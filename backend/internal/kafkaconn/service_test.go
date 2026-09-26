package kafkaconn

// service_test.go：连接表生命周期、预设存储与审计回调（内存实现，不连网）。

import (
	"context"
	"testing"

	"io.dbx.kafka.plugin/internal/lifecycle"
)

// memPresetStore 内存预设存储（测试替身）。
type memPresetStore struct {
	presets []ConsumePreset
}

func (m *memPresetStore) LoadPresets() ([]ConsumePreset, error) {
	return append([]ConsumePreset{}, m.presets...), nil
}

func (m *memPresetStore) SavePresets(presets []ConsumePreset) error {
	m.presets = append([]ConsumePreset{}, presets...)
	return nil
}

func connectParams(id string) *lifecycle.Params {
	params, err := lifecycle.Parse([]byte(`{
	  "connection": {
	    "id": "` + id + `",
	    "name": "test",
	    "external_config": { "bootstrap_servers": "k1:9092", "read_only": true }
	  },
	  "runtime": { "host": "127.0.0.1", "port": 9094 }
	}`))
	if err != nil {
		panic(err)
	}
	return params
}

func TestConnectDisconnectLifecycle(t *testing.T) {
	service := NewService()

	if err := service.Connect(connectParams("conn-1")); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	statuses := service.SnapshotStatuses()
	if len(statuses) != 1 {
		t.Fatalf("statuses = %d, want 1", len(statuses))
	}
	status := statuses[0]
	if status.ConnectionID != "conn-1" || status.Status != "idle" || status.ReadOnly != true {
		t.Errorf("status = %+v", status)
	}
	if status.Bootstrap != "k1:9092" {
		t.Errorf("bootstrap = %q", status.Bootstrap)
	}

	// 重复 connect 覆盖配置（幂等）。
	if err := service.Connect(connectParams("conn-1")); err != nil {
		t.Fatalf("Connect() again error = %v", err)
	}
	if got := len(service.SnapshotStatuses()); got != 1 {
		t.Errorf("statuses after reconnect = %d, want 1", got)
	}

	// Disconnect 幂等。
	service.Disconnect("conn-1")
	service.Disconnect("conn-1")
	if got := len(service.SnapshotStatuses()); got != 0 {
		t.Errorf("statuses after disconnect = %d, want 0", got)
	}

	// 未连接的领域调用报连接不存在。
	_, err := service.ListGroups(context.Background(), "missing")
	if err == nil || err.Error() != `connection "missing" is not connected; call connection/connect first` {
		t.Errorf("ListGroups(missing) error = %v", err)
	}
}

func TestProfileOfGateFallback(t *testing.T) {
	service := NewService()
	// 未连接 → 兜底 read_only Profile（写操作一律拒绝）。
	profile := service.profileOf("ghost")
	if !profile.ReadOnly {
		t.Error("ghost profile should default to read-only")
	}
	if err := ensureWriteAllowed(profile, "produce"); err == nil {
		t.Error("ghost write should be blocked")
	}
}

func TestAuditCallback(t *testing.T) {
	service := NewService()
	var records []AuditRecord
	service.Audit = func(rec AuditRecord) {
		records = append(records, rec)
	}
	service.emitAudit("conn-1", "produce", "orders", "blocked", "read-only")
	if len(records) != 1 {
		t.Fatalf("audit records = %d", len(records))
	}
	rec := records[0]
	if rec.ConnectionID != "conn-1" || rec.Action != "produce" || rec.Target != "orders" || rec.Result != "blocked" {
		t.Errorf("record = %+v", rec)
	}
	// 凭据红线断言：审计记录结构不含凭据字段（编译期形状 + 值检查）。
	if containsAll(rec.Target+rec.Detail+rec.Action, "password", "sasl", "secret") {
		t.Errorf("audit record must not contain credential markers: %+v", rec)
	}
}

// 审查 L5 回归：skip-verify 连接在 lifecycle 配置应用（Connect）时发一条
// success + Detail 审计留痕；普通连接不发。Result 走三值契约，无 warning 态。
func TestConnectAuditsInsecureSkipVerify(t *testing.T) {
	insecureParams := func(id string) *lifecycle.Params {
		params, err := lifecycle.Parse([]byte(`{
		  "connection": {
		    "id": "` + id + `",
		    "name": "insecure-kafka",
		    "external_config": {
		      "bootstrap_servers": "k1:9092",
		      "security_protocol": "SSL",
		      "tls_insecure_skip_verify": true
		    }
		  }
		}`))
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}
		return params
	}

	service := NewService()
	var records []AuditRecord
	service.Audit = func(rec AuditRecord) { records = append(records, rec) }

	// 普通（非 skip-verify）连接：无审计。
	if err := service.Connect(connectParams("plain")); err != nil {
		t.Fatalf("Connect(plain) error = %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("plain connect must not audit, records = %+v", records)
	}

	// skip-verify 连接：单条 success 审计，Detail 注明 TLS 验证已关闭。
	if err := service.Connect(insecureParams("insecure")); err != nil {
		t.Fatalf("Connect(insecure) error = %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("insecure connect audits = %+v, want exactly one", records)
	}
	rec := records[0]
	if rec.ConnectionID != "insecure" || rec.Action != "connection-configure" ||
		rec.Target != "insecure-kafka" || rec.Result != "success" {
		t.Errorf("record = %+v", rec)
	}
	if !containsAll(rec.Detail, "TLS certificate verification is disabled", "tlsInsecureSkipVerify") {
		t.Errorf("detail should note disabled verification: %q", rec.Detail)
	}
	if containsAll(rec.Detail+rec.Target, "password", "secret") {
		t.Errorf("audit record must not contain credential markers: %+v", rec)
	}

	// Audit 回调 nil 时 Connect 不 panic（emitAudit nil 安全）。
	if err := (NewService()).Connect(insecureParams("insecure-2")); err != nil {
		t.Fatalf("Connect without audit sink error = %v", err)
	}
}

func TestPresets(t *testing.T) {
	service := NewService()
	if _, err := service.ListPresets(); err == nil {
		t.Fatal("nil Presets store should error")
	}

	store := &memPresetStore{}
	service.Presets = store

	// 新建（id 兜底）。
	saved, err := service.SavePreset(ConsumePreset{Name: "latest-orders", Params: ConsumeParams{Topic: "orders", OffsetStrategy: "latest"}})
	if err != nil {
		t.Fatalf("SavePreset() error = %v", err)
	}
	if saved.ID == "" {
		t.Error("preset id should be generated")
	}
	if len(store.presets) != 1 {
		t.Fatalf("presets = %d", len(store.presets))
	}

	// 覆盖同 id。
	saved.Name = "renamed"
	if _, err := service.SavePreset(*saved); err != nil {
		t.Fatalf("SavePreset() overwrite error = %v", err)
	}
	presets, _ := service.ListPresets()
	if len(presets) != 1 || presets[0].Name != "renamed" {
		t.Errorf("presets after overwrite = %+v", presets)
	}

	// 删除。
	if err := service.RemovePreset(saved.ID); err != nil {
		t.Fatalf("RemovePreset() error = %v", err)
	}
	if err := service.RemovePreset(saved.ID); err == nil {
		t.Error("removing missing preset should error")
	}
	if err := service.RemovePreset(""); err == nil {
		t.Error("removing empty id should error")
	}
}
