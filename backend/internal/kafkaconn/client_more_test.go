package kafkaconn

// client_more_test.go：client 工厂与 topics 组 Service 方法进 broker 之前的
// 离线路径——consumeClient 构造/关闭、重连与断开的 closeLocked、fieldValues
// 数值比较矩阵、statusForContract 三态、buildOauthSASLOpt 构造期（不触网）。

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestFieldValuesMatchesOperators(t *testing.T) {
	// 直接构造 regex pattern 表（fieldValueMatches 优先查缓存）。
	matcher := textMatcher{mode: "contains", patterns: map[string]*regexp.Regexp{
		`a\d+`: regexp.MustCompile(`a\d+`),
	}}

	cases := []struct {
		name   string
		source string
		query  string
		op     string
		want   bool
	}{
		{"gt hit", "10", "9", "gt", true},
		{"gt miss", "9", "10", "gt", false},
		{"gte equal", "10", "10", "gte", true},
		{"lt hit", "3", "4", "lt", true},
		{"lte equal", "4", "4", "lte", true},
		{"numeric parse fail", "abc", "4", "gt", false},
		{"query parse fail", "4", "abc", "gt", false},
		{"prefix hit", "hello world", "HEL", "prefix", true},
		{"prefix miss", "hello", "ell", "prefix", false},
		{"exact fold", "Hello", "hello", "exact", true},
		{"regex cached", "a123", `a\d+`, "regex", true},
		{"regex cached miss", "b123", `a\d+`, "regex", false},
		{"regex empty query", "x", "", "regex", true},
		{"regex uncached compile", "a99", `a\d+`, "regex", true},
		{"regex invalid", "x", "[", "regex", false},
		{"default falls to matcher", "find me", "ME", "contains", true},
	}
	for _, tc := range cases {
		if got := fieldValueMatches(tc.source, tc.query, tc.op, matcher); got != tc.want {
			t.Errorf("%s: fieldValueMatches(%q,%q,%s) = %v, want %v", tc.name, tc.source, tc.query, tc.op, got, tc.want)
		}
	}
}

func TestStatusForContract(t *testing.T) {
	cases := map[string]string{
		"connected": "connected",
		"error":     "error",
		"idle":      "idle",
		"closed":    "idle",
		"":          "idle",
	}
	for raw, want := range cases {
		if got := statusForContract(raw); got != want {
			t.Errorf("statusForContract(%q) = %q, want %q", raw, got, want)
		}
	}
}

func TestConsumeClientLifecycleOffline(t *testing.T) {
	service := NewService()
	// 未连接。
	if _, _, err := service.consumeClient("ghost"); err == nil {
		t.Error("ghost consumeClient expected error")
	}

	// 已连接（127.0.0.1:1）：client 构造离线成功（惰性拨号）。
	if err := connectWithConfig(t, service, "cc",
		`{"bootstrap_servers": "127.0.0.1:1"}`, `{}`); err != nil {
		t.Fatalf("connect: %v", err)
	}
	client, closeFn, err := service.consumeClient("cc", kgo.ConsumeTopics("t"))
	if err != nil || client == nil || closeFn == nil {
		t.Fatalf("consumeClient = %v, %v", client, err)
	}
	closeFn()
	closeFn() // kgo Close 幂等，不 panic
}

func TestReconnectAndDisconnectCloseLocked(t *testing.T) {
	service := NewService()
	if err := connectWithConfig(t, service, "rc",
		`{"bootstrap_servers": "127.0.0.1:1"}`, `{}`); err != nil {
		t.Fatalf("connect: %v", err)
	}
	// 同 id 重连：旧 entry 走 closeLocked（无 client → nil 安全路径）。
	if err := connectWithConfig(t, service, "rc",
		`{"bootstrap_servers": "127.0.0.1:2"}`, `{}`); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	// 幂等断言：二次 disconnect 不 panic。
	service.Disconnect("rc")
	service.Disconnect("rc")
	service.Disconnect("")

	// CloseAll 空表安全。
	service.CloseAll()
}

func TestTopicsServiceValidationOffline(t *testing.T) {
	// ghost 连接：门禁 / 连接检查先于 broker。
	ghost := NewService()
	if _, err := ghost.ListBrokers(context.Background(), "ghost"); err == nil {
		t.Error("ListBrokers ghost expected error")
	}
	if _, err := ghost.ListGroups(context.Background(), "ghost"); err == nil {
		t.Error("ListGroups ghost expected error")
	}
	if _, err := ghost.DescribeBrokerConfig(context.Background(), BrokerConfigRequest{ConnectionID: "ghost"}); err == nil {
		t.Error("DescribeBrokerConfig ghost expected error")
	}
	if _, err := ghost.ListTopics(context.Background(), TopicsListRequest{ConnectionID: "ghost"}); err == nil {
		t.Error("ListTopics ghost expected error")
	}
	if _, err := ghost.DescribeTopic(context.Background(), TopicsDescribeRequest{ConnectionID: "ghost", Topic: "t"}); err == nil {
		t.Error("DescribeTopic ghost expected error")
	}
	if _, err := ghost.GetTopicConfig(context.Background(), TopicConfigGetRequest{ConnectionID: "ghost"}); err == nil || !strings.Contains(err.Error(), "topic is required") {
		t.Errorf("GetTopicConfig err = %v, want topic required", err)
	}
	if _, err := ghost.ListTopicOffsets(context.Background(), TopicOffsetsListRequest{ConnectionID: "ghost"}); err == nil || !strings.Contains(err.Error(), "topics is required") {
		t.Errorf("ListTopicOffsets err = %v, want topics required", err)
	}
	if _, err := ghost.ListTopicOffsets(context.Background(), TopicOffsetsListRequest{Topics: []string{"t"}, OffsetTime: "bogus"}); err == nil || !strings.Contains(err.Error(), "offsetTime must be") {
		t.Errorf("ListTopicOffsets err = %v, want offsetTime rejection", err)
	}

	// 写门禁（ghost read_only 兜底）+ 审计。
	if _, err := ghost.CreateTopics(context.Background(), TopicsCreateRequest{Topics: []string{"t"}, Partitions: 1, ReplicationFactor: 1}); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Errorf("CreateTopics err = %v, want read-only", err)
	}
	if _, err := ghost.UpdatePartitions(context.Background(), PartitionsUpdateRequest{Partitions: map[string]int32{"t": 3}}); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Errorf("UpdatePartitions err = %v, want read-only", err)
	}
	if _, err := ghost.AlterTopicConfig(context.Background(), TopicConfigAlterRequest{Topic: "t", Config: map[string]string{"retention.ms": "1"}}); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Errorf("AlterTopicConfig err = %v, want read-only", err)
	}

	// 已连接（127.0.0.1:1）：进 broker 前的参数校验分支。
	service := NewService()
	if err := connectWithConfig(t, service, "tv",
		`{"bootstrap_servers": "127.0.0.1:1"}`, `{}`); err != nil {
		t.Fatalf("connect: %v", err)
	}
	cases := []struct {
		name       string
		call       func() error
		wantErrSub string
	}{
		{"create topics empty", func() error {
			_, err := service.CreateTopics(context.Background(), TopicsCreateRequest{ConnectionID: "tv", Partitions: 1, ReplicationFactor: 1})
			return err
		}, "topics is required"},
		{"create partitions zero", func() error {
			_, err := service.CreateTopics(context.Background(), TopicsCreateRequest{ConnectionID: "tv", Topics: []string{"t"}, ReplicationFactor: 1})
			return err
		}, "partitions must be"},
		{"create replication zero", func() error {
			_, err := service.CreateTopics(context.Background(), TopicsCreateRequest{ConnectionID: "tv", Topics: []string{"t"}, Partitions: 1})
			return err
		}, "replicationFactor must be"},
		{"create bad config key", func() error {
			_, err := service.CreateTopics(context.Background(), TopicsCreateRequest{ConnectionID: "tv", Topics: []string{"t"}, Partitions: 1, ReplicationFactor: 1, Config: map[string]string{" ": "v"}})
			return err
		}, "config key cannot be empty"},
		{"update partitions empty", func() error {
			_, err := service.UpdatePartitions(context.Background(), PartitionsUpdateRequest{ConnectionID: "tv"})
			return err
		}, "partitions is required"},
		{"update partitions zero count", func() error {
			_, err := service.UpdatePartitions(context.Background(), PartitionsUpdateRequest{ConnectionID: "tv", Partitions: map[string]int32{"t": 0}})
			return err
		}, "must be greater than 0"},
		{"alter config empty topic", func() error {
			_, err := service.AlterTopicConfig(context.Background(), TopicConfigAlterRequest{ConnectionID: "tv", Config: map[string]string{"a": "b"}})
			return err
		}, "topic is required"},
		{"alter config empty key", func() error {
			_, err := service.AlterTopicConfig(context.Background(), TopicConfigAlterRequest{ConnectionID: "tv", Topic: "t", Config: map[string]string{"": "b"}})
			return err
		}, "config key cannot be empty"},
		{"alter config empty deleteKey", func() error {
			_, err := service.AlterTopicConfig(context.Background(), TopicConfigAlterRequest{ConnectionID: "tv", Topic: "t", DeleteKeys: []string{" "}})
			return err
		}, "deleteKeys cannot contain empty"},
		{"get config connected missing conn", func() error {
			_, err := service.GetTopicConfig(context.Background(), TopicConfigGetRequest{ConnectionID: "ghost", Topic: "t"})
			return err
		}, "not connected"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
				t.Errorf("err = %v, want contains %q", err, tc.wantErrSub)
			}
		})
	}
}

func TestBuildOauthSASLOptStaticOffline(t *testing.T) {
	// 构造期不触网：static_token 形态直接出 opt。
	opt, err := buildOauthSASLOpt(Profile{OauthTokenSource: "static_token"}, connSecrets{OauthStaticToken: "tok-123"})
	if err != nil || opt == nil {
		t.Fatalf("opt = %v, %v", opt, err)
	}
	// msk_iam 形态同样只构造。
	opt, err = buildOauthSASLOpt(Profile{OauthTokenSource: "msk_iam", MSKRegion: "us-east-1"}, connSecrets{})
	if err != nil || opt == nil {
		t.Fatalf("msk opt = %v, %v", opt, err)
	}
}

// S-STATUS-LOCK（评审 M-6）：statuses 快照不得被 entry.mu 阻塞——此前
// withAdmin 在 entry 锁内执行整个 admin RPC（≤20s），statuses UI 轮询与
// 该连接全部管理面操作串行排队。
func TestSnapshotStatusesIndependentOfEntryLock(t *testing.T) {
	svc := NewService()
	if err := connectWithConfig(t, svc, "st", `{`+baseBootstrap+`}`, `{}`); err != nil {
		t.Fatalf("connect error = %v", err)
	}
	svc.mu.Lock()
	entry := svc.conns["st"]
	svc.mu.Unlock()
	if entry == nil {
		t.Fatal("entry missing")
	}
	entry.mu.Lock() // 模拟慢 admin RPC 持 entry.mu
	defer entry.mu.Unlock()

	done := make(chan []ConnectionStatus, 1)
	go func() { done <- svc.SnapshotStatuses() }()
	select {
	case statuses := <-done:
		if len(statuses) != 1 || statuses[0].ConnectionID != "st" {
			t.Fatalf("statuses = %+v", statuses)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("statuses snapshot blocked by entry lock")
	}
}
