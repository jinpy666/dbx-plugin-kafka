package kafkaconn

// zk_test.go：ZK 地址解析、broker JSON 解析与不可达错误路径（不依赖真实
// ZooKeeper；broker 发现的正路径由容器/集成验证覆盖，单测覆盖解析与错误）。

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestParseZKServers(t *testing.T) {
	servers, err := parseZKServers([]string{"zk1:2181, zk2:2181/kafka", "\n", "zk3:2181"})
	if err != nil {
		t.Fatalf("parseZKServers() error = %v", err)
	}
	if len(servers) != 3 || servers[0] != "zk1:2181" || servers[1] != "zk2:2181/kafka" {
		t.Errorf("servers = %v", servers)
	}

	// 单条带空格/逗号混合。
	servers, err = parseZKServers([]string{"zk1:2181 zk2:2181"})
	if err != nil || len(servers) != 2 {
		t.Errorf("whitespace split = %v, %v", servers, err)
	}

	// 空列表 → 必填错误。
	if _, err := parseZKServers(nil); err == nil || !strings.Contains(err.Error(), "required") {
		t.Errorf("empty servers error = %v", err)
	}
	// 缺端口。
	if _, err := parseZKServers([]string{"zk1"}); err == nil || !strings.Contains(err.Error(), "host:port") {
		t.Errorf("missing port error = %v", err)
	}
	// 坏端口。
	if _, err := parseZKServers([]string{"zk1:70000"}); err == nil {
		t.Error("bad port expected error")
	}
	// 空 chroot。
	if _, err := parseZKServers([]string{"zk1:2181/"}); err == nil {
		t.Error("empty chroot expected error")
	}
}

func TestParseZKServerAddress(t *testing.T) {
	addr, err := parseZKServerAddress("zk1:2181/chroot/path")
	if err != nil || addr.Host != "zk1" || addr.Port != 2181 || addr.Chroot != "/chroot/path" {
		t.Errorf("addr = %+v, %v", addr, err)
	}
	addr, err = parseZKServerAddress(" 10.0.0.1:2181 ")
	if err != nil || addr.Host != "10.0.0.1" || addr.Port != 2181 || addr.Chroot != "" {
		t.Errorf("plain addr = %+v, %v", addr, err)
	}
	if _, err := parseZKServerAddress(""); err == nil {
		t.Error("empty addr expected error")
	}
}

func TestParseZKBrokerConfig(t *testing.T) {
	host, port, err := parseZKBrokerConfig([]byte(`{"host":"10.0.0.5","port":9092,"listener_security_protocol_map":{"PLAINTEXT":"PLAINTEXT"}}`))
	if err != nil || host != "10.0.0.5" || port != 9092 {
		t.Errorf("broker config = %q/%d, %v", host, port, err)
	}

	// 缺 host。
	if _, _, err := parseZKBrokerConfig([]byte(`{"port":9092}`)); err == nil {
		t.Error("missing host expected error")
	}
	// 缺 port。
	if _, _, err := parseZKBrokerConfig([]byte(`{"host":"h"}`)); err == nil {
		t.Error("missing port expected error")
	}
	// 非 JSON。
	if _, _, err := parseZKBrokerConfig([]byte("not-json")); err == nil {
		t.Error("bad JSON expected error")
	}
}

func TestDiscoverBrokersZKUnreachable(t *testing.T) {
	// ZK 不可达（127.0.0.1:1 立即拒绝）→ 业务错（main 层映射 -32000）。
	profile := Profile{ZKServers: []string{"127.0.0.1:1"}}
	_, err := discoverBrokersViaZKProfile(context.Background(), profile)
	if err == nil {
		t.Fatal("unreachable ZK expected error")
	}
	if !strings.Contains(err.Error(), "unreachable") {
		t.Errorf("error = %v, want unreachable marker", err)
	}

	// 多 server 时任一不可达即失败（快速预拨短路）。
	profile = Profile{ZKServers: []string{"127.0.0.1:1", "127.0.0.1:2"}}
	if _, err := discoverBrokersViaZKProfile(context.Background(), profile); err == nil {
		t.Error("second server unreachable expected error")
	}
}

func TestDiscoverBrokersZKServerListRequired(t *testing.T) {
	if _, err := discoverBrokersViaZKProfile(context.Background(), Profile{}); err == nil || !strings.Contains(err.Error(), "zkServers") {
		t.Errorf("missing zkServers error = %v", err)
	}
	// Service 路径：未连接 → 连接不存在。
	service := NewService()
	if _, err := service.discoverBrokersViaZK(context.Background(), "missing"); err == nil {
		t.Error("discover on missing connection expected error")
	}
}

// S-ZK-CTX（评审 M-7）：ZK 发现路径贯穿 ctx——取消立即失败（此前全程
// 无 ctx，预拨还串行 N×5s）；预拨并发化后最坏 1×5s。
func TestDiscoverBrokersViaZKHonorsCtx(t *testing.T) {
	profile := Profile{ZKServers: []string{"10.255.255.1:2181", "10.255.255.2:2181", "10.255.255.3:2181"}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	_, err := discoverBrokersViaZKProfile(ctx, profile)
	if err == nil {
		t.Fatal("canceled ctx must fail fast")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("canceled ctx took %v, want immediate", elapsed)
	}
}
