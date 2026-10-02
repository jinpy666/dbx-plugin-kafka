package kafkaconn

// connection_reconnect_test.go：KAFKA-M2/M3 回归。
//   - M3：同配置幂等重连短路——保留旧 entry（admin client / SR·Glue 缓存 /
//     在途流式会话 / 消费池），配置变化才整体拆除。
//   - M2：glue 后端按连接缓存——同配置两次解析返回同一实例。

import "testing"

// KAFKA-M3：mcp/call 桥每次工具调用都携带 lifecycle 即 Connect；同配置
// 重连不得误杀该连接上的流式会话。
func TestConnectSameConfigPreservesStreams(t *testing.T) {
	svc := NewService()
	cfg := `{"bootstrap_servers": "k1:9092"}`
	if err := connectWithConfig(t, svc, "m3", cfg, `{}`); err != nil {
		t.Fatalf("connect = %v", err)
	}
	svc.Streams.inject(newTestSession("keep1", "m3", "t"))

	if err := connectWithConfig(t, svc, "m3", cfg, `{}`); err != nil {
		t.Fatalf("same-config reconnect = %v", err)
	}
	if _, err := svc.StreamStatusOf("keep1"); err != nil {
		t.Fatalf("same-config reconnect must not kill stream sessions: %v", err)
	}

	// 配置变化重连：保留整体拆除语义（旧会话停止）。
	if err := connectWithConfig(t, svc, "m3", `{"bootstrap_servers": "k2:9092"}`, `{}`); err != nil {
		t.Fatalf("changed reconnect = %v", err)
	}
	if _, err := svc.StreamStatusOf("keep1"); err == nil {
		t.Fatal("config-change reconnect must stop stream sessions")
	}
}

// KAFKA-M3：secrets 变化（同 config）不算幂等——短路判定必须覆盖凭据。
// connectedAt 是毫秒精度，同毫秒重连不可区分，改用 entry 指针身份断言。
func TestConnectSecretsChangeRebuilds(t *testing.T) {
	svc := NewService()
	if err := connectWithConfig(t, svc, "m3s", `{"bootstrap_servers": "k1:9092"}`, `{"sasl_password": "p1"}`); err != nil {
		t.Fatalf("connect = %v", err)
	}
	svc.mu.Lock()
	before := svc.conns["m3s"]
	svc.mu.Unlock()
	if err := connectWithConfig(t, svc, "m3s", `{"bootstrap_servers": "k1:9092"}`, `{"sasl_password": "p2"}`); err != nil {
		t.Fatalf("reconnect = %v", err)
	}
	svc.mu.Lock()
	after := svc.conns["m3s"]
	svc.mu.Unlock()
	if after == before {
		t.Fatal("secrets change must replace the entry (short-circuit must compare secrets)")
	}
	// 同 config + 同 secrets：entry 保留（短路生效，覆盖同毫秒重连场景）。
	kept := after
	if err := connectWithConfig(t, svc, "m3s", `{"bootstrap_servers": "k1:9092"}`, `{"sasl_password": "p2"}`); err != nil {
		t.Fatalf("idempotent reconnect = %v", err)
	}
	svc.mu.Lock()
	idem := svc.conns["m3s"]
	svc.mu.Unlock()
	if idem != kept {
		t.Fatal("same-config reconnect must keep the entry (short-circuit)")
	}
}

// KAFKA-M2：同配置两次 schemaBackendFor 返回同一 glue 后端实例（凭据链
// 解析不再逐调用重复付费）。
func TestGlueBackendCachedPerConnection(t *testing.T) {
	svc := NewService()
	glueConnect(t, svc, "m2", "static")
	b1, err := svc.schemaBackendFor("m2", "glue")
	if err != nil {
		t.Fatalf("schemaBackendFor = %v", err)
	}
	b2, err := svc.schemaBackendFor("m2", "glue")
	if err != nil {
		t.Fatalf("schemaBackendFor 2nd = %v", err)
	}
	if b1 != b2 {
		t.Fatal("glue backend must be cached per connection")
	}
}
