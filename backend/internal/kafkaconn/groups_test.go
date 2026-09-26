package kafkaconn

// groups_test.go：消费组纯映射函数与校验分支（契约 §5.2）——
// committedTopics / groupOffsetRows / max64 / groupDetail 表驱动，
// 及 DescribeGroup/GetGroupOffsets/DeleteGroup/ResetGroupOffsets 进 broker
// 之前的参数校验与门禁（ghost 连接或 127.0.0.1:1 快速失败，全部离线）。

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// TestOffsetResetRowsSurfacesCommitRejection：提交被 broker 逐分区拒绝时必须
// 返回错误而不是 ok=true。Kafka 4.x 的 UNKNOWN_TOPIC_ID（OffsetCommit v10 起
// 用 topic id 取代 topic name）曾经被外层 error 吞掉，MCP reset 报告成功但
// committed offset 仍是 -1（K15 回归面）。
func TestOffsetResetRowsSurfacesCommitRejection(t *testing.T) {
	targets := kadm.Offsets{}
	targets.AddOffset("orders", 0, 6, -1)
	targets.AddOffset("orders", 1, 3, -1)

	// broker 接受：逐分区 ok、无错误。
	commits := kadm.OffsetResponses{
		"orders": {
			0: kadm.OffsetResponse{Offset: kadm.Offset{Topic: "orders", Partition: 0, At: 6}},
			1: kadm.OffsetResponse{Offset: kadm.Offset{Topic: "orders", Partition: 1, At: 3}},
		},
	}
	rows, err := offsetResetRows(targets, commits, nil)
	if err != nil || len(rows) != 2 || !rows[0].OK || !rows[1].OK {
		t.Fatalf("accepted commit: rows=%+v err=%v", rows, err)
	}
	if rows[0].Topic != "orders" || rows[1].Partition != 1 {
		t.Fatalf("rows must stay sorted by topic/partition: %+v", rows)
	}

	// 逐分区拒绝：row 带 broker 错误码，整体报错（点名 topic/partition）。
	rejected := kadm.OffsetResponses{
		"orders": {
			0: kadm.OffsetResponse{Offset: kadm.Offset{Topic: "orders", Partition: 0, At: 6}},
			1: kadm.OffsetResponse{Offset: kadm.Offset{Topic: "orders", Partition: 1, At: 3}, Err: kerr.UnknownTopicID},
		},
	}
	rows, err = offsetResetRows(targets, rejected, nil)
	if err == nil || !strings.Contains(err.Error(), "orders/1") {
		t.Fatalf("rejected partition must surface with topic/partition: %v", err)
	}
	if len(rows) != 2 || !rows[0].OK || rows[1].OK || rows[1].Error == "" {
		t.Fatalf("rejected commit rows=%+v", rows)
	}

	// 响应缺失分区：同样不得报成功。
	rows, err = offsetResetRows(targets, kadm.OffsetResponses{"orders": {
		0: kadm.OffsetResponse{Offset: kadm.Offset{Topic: "orders", Partition: 0, At: 6}},
	}}, nil)
	if err == nil || !strings.Contains(err.Error(), "orders/1") {
		t.Fatalf("missing partition must surface: %v", err)
	}

	// 传输层错误：全部分区标失败，错误原样透出。
	rows, err = offsetResetRows(targets, nil, errors.New("dial tcp 127.0.0.1:9092: connect: connection refused"))
	if err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("transport error must surface: %v", err)
	}
	if len(rows) != 2 || rows[0].OK || rows[1].OK {
		t.Fatalf("transport error rows=%+v", rows)
	}
}

func TestCommittedTopics(t *testing.T) {
	committed := kadm.OffsetResponses{
		"t-b": {
			0: kadm.OffsetResponse{Offset: kadm.Offset{Topic: "t-b", Partition: 0}},
			1: kadm.OffsetResponse{Offset: kadm.Offset{Topic: "t-b", Partition: 1}},
		},
		"t-a": {0: kadm.OffsetResponse{Offset: kadm.Offset{Topic: "t-a", Partition: 0}}},
	}
	// 无过滤：topic 全集 + 去重 + 排序。
	got := committedTopics(committed, nil)
	if len(got) != 2 || got[0] != "t-a" || got[1] != "t-b" {
		t.Errorf("committedTopics = %v, want [t-a t-b]", got)
	}
	// 带过滤：只保留请求 topic。
	got = committedTopics(committed, []string{"t-b"})
	if len(got) != 1 || got[0] != "t-b" {
		t.Errorf("filtered = %v, want [t-b]", got)
	}
	// 空 committed → 空集。
	if got := committedTopics(kadm.OffsetResponses{}, []string{"t-a"}); len(got) != 0 {
		t.Errorf("empty = %v, want empty", got)
	}
}

func TestGroupOffsetRows(t *testing.T) {
	startOffsets := kadm.ListedOffsets{
		"orders": {
			0: {Topic: "orders", Partition: 0, Offset: 0, Timestamp: -1, LeaderEpoch: -1},
			1: {Topic: "orders", Partition: 1, Offset: 10, Timestamp: -1, LeaderEpoch: -1},
			2: {Topic: "orders", Partition: 2, Err: errors.New("leader not available")},
		},
	}
	endOffsets := kadm.ListedOffsets{
		"orders": {
			0: {Topic: "orders", Partition: 0, Offset: 100, Timestamp: -1, LeaderEpoch: -1},
			1: {Topic: "orders", Partition: 1, Offset: 95, Timestamp: -1, LeaderEpoch: -1},
			2: {Topic: "orders", Partition: 2, Offset: 7, Timestamp: -1, LeaderEpoch: -1},
		},
		"payments": {
			0: {Topic: "payments", Partition: 0, Offset: 50, Timestamp: -1, LeaderEpoch: -1},
		},
	}
	committed := kadm.OffsetResponses{
		"orders": {
			0: {Offset: kadm.Offset{Topic: "orders", Partition: 0, At: 90}},
			1: {Offset: kadm.Offset{Topic: "orders", Partition: 1, At: 95}},
			2: {Offset: kadm.Offset{Topic: "orders", Partition: 2, At: -1}},
		},
	}
	rows := groupOffsetRows(startOffsets, endOffsets, committed)
	if len(rows) != 4 {
		t.Fatalf("rows = %d, want 4", len(rows))
	}
	// topic 升序、分区内分区升序：orders/0, orders/1, orders/2, payments/0。
	if rows[0].Topic != "orders" || rows[0].Partition != 0 {
		t.Fatalf("rows[0] = %+v, want orders/0", rows[0])
	}
	if rows[1].Partition != 1 || rows[2].Partition != 2 || rows[3].Topic != "payments" {
		t.Errorf("row order = %+v", rows)
	}

	// orders/0：lag = 100-90。
	if rows[0].StartOffset != 0 || rows[0].EndOffset != 100 || rows[0].CommittedOffset != 90 || rows[0].Lag != 10 || rows[0].Error != "" {
		t.Errorf("orders/0 = %+v", rows[0])
	}
	// orders/1：已消费到 end → lag 0。
	if rows[1].CommittedOffset != 95 || rows[1].Lag != 0 {
		t.Errorf("orders/1 = %+v, want committed=95 lag=0", rows[1])
	}
	// orders/2：start 列表失败 → Error 透传；committed=-1 → lag=全量 end。
	if rows[2].Error == "" || !strings.Contains(rows[2].Error, "leader not available") {
		t.Errorf("orders/2 = %+v, want start error propagated", rows[2])
	}
	if rows[2].CommittedOffset != -1 || rows[2].Lag != 7 {
		t.Errorf("orders/2 = %+v, want committed=-1 lag=7", rows[2])
	}
	// payments/0：只有 start/end 无 committed → CommittedOffset 保持 0，
	// lag = end（未提交视作全量未消费）。
	if rows[3].CommittedOffset != 0 || rows[3].Lag != 50 {
		t.Errorf("payments/0 = %+v, want lag=50 (uncommitted)", rows[3])
	}

	// 仅 committed（start/end 为空表）也要出行。
	onlyCommitted := groupOffsetRows(nil, nil, committed)
	if len(onlyCommitted) != 3 {
		t.Errorf("committed-only rows = %d, want 3", len(onlyCommitted))
	}

	// committed Err 透传（start/end 都无错时）。
	committedErr := kadm.OffsetResponses{
		"orders": {5: {Offset: kadm.Offset{Topic: "orders", Partition: 5, At: -1}, Err: errors.New("group not found")}},
	}
	rows = groupOffsetRows(nil, nil, committedErr)
	if len(rows) != 1 || !strings.Contains(rows[0].Error, "group not found") {
		t.Errorf("rows = %+v, want commit error propagated", rows)
	}

	// 空输入 → 空行。
	if rows := groupOffsetRows(nil, nil, nil); len(rows) != 0 {
		t.Errorf("empty rows = %+v", rows)
	}
}

func TestMax64(t *testing.T) {
	cases := []struct {
		a, b, want int64
	}{
		{1, 2, 2},
		{2, 1, 2},
		{-5, 0, 0},
		{-5, -1, -1},
		{7, 7, 7},
	}
	for _, tc := range cases {
		if got := max64(tc.a, tc.b); got != tc.want {
			t.Errorf("max64(%d, %d) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestGroupDetailMapping(t *testing.T) {
	instance := "static-1"
	group := kadm.DescribedGroup{
		Group:        "workers",
		State:        "Stable",
		ProtocolType: "consumer",
		Protocol:     "range",
		Coordinator:  kadm.BrokerDetail{NodeID: 2},
		Members: []kadm.DescribedGroupMember{
			{MemberID: "m-1", ClientID: "client-a", ClientHost: "/10.0.0.1", InstanceID: &instance},
			{MemberID: "m-2"},
		},
	}
	result := groupDetail(group)
	if result.Group != "workers" || result.State != "Stable" || result.ProtocolType != "consumer" || result.Protocol != "range" || result.Coordinator != 2 {
		t.Errorf("result = %+v", result)
	}
	if len(result.Members) != 2 {
		t.Fatalf("members = %d, want 2", len(result.Members))
	}
	member := result.Members[0]
	if member.MemberID != "m-1" || member.ClientID != "client-a" || member.ClientHost != "/10.0.0.1" || member.InstanceID != "static-1" {
		t.Errorf("member0 = %+v", member)
	}
	if result.Members[1].InstanceID != "" {
		t.Errorf("member1 InstanceID = %q, want empty (nil)", result.Members[1].InstanceID)
	}

	// 组级错误进 Error 字段。
	group.Err = errors.New("coordinator not available")
	if result := groupDetail(group); result.Error != "coordinator not available" {
		t.Errorf("error = %q", result.Error)
	}

	// 零值组：Members 空切片（非 nil，JSON 序列化为 []）。
	empty := groupDetail(kadm.DescribedGroup{Group: "g"})
	if empty.Members == nil || len(empty.Members) != 0 {
		t.Errorf("empty members = %#v, want non-nil empty slice", empty.Members)
	}
}

func TestDescribeGroupValidationOffline(t *testing.T) {
	service := NewService()
	// 空 group 先于连接检查。
	if _, err := service.DescribeGroup(context.Background(), GroupsDescribeRequest{ConnectionID: "ghost"}); err == nil || !strings.Contains(err.Error(), "group is required") {
		t.Errorf("error = %v, want group required", err)
	}
	// 未连接 → 连接不存在。
	if _, err := service.DescribeGroup(context.Background(), GroupsDescribeRequest{ConnectionID: "ghost", Group: "g"}); err == nil {
		t.Error("missing connection expected error")
	}
}

func TestGetGroupOffsetsValidationOffline(t *testing.T) {
	service := NewService()
	if _, err := service.GetGroupOffsets(context.Background(), GroupOffsetsListRequest{ConnectionID: "ghost"}); err == nil || !strings.Contains(err.Error(), "groupId is required") {
		t.Errorf("error = %v, want groupId required", err)
	}
	if _, err := service.GetGroupOffsets(context.Background(), GroupOffsetsListRequest{ConnectionID: "ghost", Group: "g"}); err == nil {
		t.Error("missing connection expected error")
	}
}

func TestDeleteGroupGateMatrix(t *testing.T) {
	service := NewService()
	var audits []AuditRecord
	service.Audit = func(rec AuditRecord) { audits = append(audits, rec) }

	// 空 group（先于门禁）。
	if err := service.DeleteGroup(context.Background(), GroupDeleteRequest{ConnectionID: "ghost"}); err == nil || !strings.Contains(err.Error(), "group is required") {
		t.Errorf("error = %v, want group required", err)
	}
	// ghost → read_only blocked + 审计。
	if err := service.DeleteGroup(context.Background(), GroupDeleteRequest{ConnectionID: "ghost", Group: "g"}); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Errorf("error = %v, want read-only block", err)
	}
	if len(audits) != 1 || audits[0].Action != "groups-delete" || audits[0].Result != "blocked" {
		t.Errorf("audits = %+v", audits)
	}

	// read_only=false + allow_delete=false → 与门第二层。
	service = NewService()
	if err := connectWithConfig(t, service, "gd",
		`{"bootstrap_servers": "127.0.0.1:1", "allow_delete": false}`, `{}`); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := service.DeleteGroup(context.Background(), GroupDeleteRequest{ConnectionID: "gd", Group: "g"}); err == nil || !strings.Contains(err.Error(), "does not allow delete") {
		t.Errorf("error = %v, want allow_delete block", err)
	}

	// 审查 L4：门禁放行后 confirmGroup 必须与组同名（与 topics/delete 的
	// confirmTopic 同级；不匹配 → InvalidParamsError/-32602 + blocked 审计）。
	service = NewService()
	audits = nil
	service.Audit = func(rec AuditRecord) { audits = append(audits, rec) }
	if err := connectWithConfig(t, service, "gc",
		`{"bootstrap_servers": "127.0.0.1:1", "allow_delete": true}`, `{}`); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := service.DeleteGroup(context.Background(), GroupDeleteRequest{ConnectionID: "gc", Group: "g"}); err == nil || !strings.Contains(err.Error(), "confirmGroup must match the group name") {
		t.Errorf("error = %v, want missing confirmGroup guard", err)
	}
	if err := service.DeleteGroup(context.Background(), GroupDeleteRequest{ConnectionID: "gc", Group: "g", ConfirmGroup: "other"}); err == nil || !strings.Contains(err.Error(), "does not match group") {
		t.Errorf("error = %v, want confirmGroup mismatch guard", err)
	}
	for _, rec := range audits {
		if rec.Action != "groups-delete" || rec.Result != "blocked" {
			t.Fatalf("confirm-guard audits = %+v", audits)
		}
	}
	if len(audits) != 2 {
		t.Fatalf("audits = %d, want 2 blocked records", len(audits))
	}
	// 确认一致 → 门禁放行进 broker（127.0.0.1:1 拨号失败而非参数错）。
	var paramErr *InvalidParamsError
	if err := service.DeleteGroup(context.Background(), GroupDeleteRequest{ConnectionID: "gc", Group: "g", ConfirmGroup: "g"}); err == nil || errors.As(err, &paramErr) {
		t.Errorf("confirmed delete error = %v, want broker dial error (past confirm guard)", err)
	}
}

func TestResetGroupOffsetsValidationMatrix(t *testing.T) {
	service := NewService()
	var audits []AuditRecord
	service.Audit = func(rec AuditRecord) { audits = append(audits, rec) }

	// 空 group（先于门禁）。
	if _, err := service.ResetGroupOffsets(context.Background(), GroupOffsetResetRequest{ConnectionID: "ghost", ResetTo: "latest"}); err == nil || !strings.Contains(err.Error(), "group is required") {
		t.Errorf("error = %v, want group required", err)
	}
	// ghost → 写门禁 blocked + 审计。
	if _, err := service.ResetGroupOffsets(context.Background(), GroupOffsetResetRequest{ConnectionID: "ghost", Group: "g", ResetTo: "latest"}); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Errorf("error = %v, want read-only block", err)
	}
	if len(audits) != 1 || audits[0].Action != "group-offsets-reset" || audits[0].Result != "blocked" {
		t.Errorf("audits = %+v", audits)
	}

	// 门禁放行后（127.0.0.1:1）逐个参数校验分支。
	service = NewService()
	if err := connectWithConfig(t, service, "gr",
		`{"bootstrap_servers": "127.0.0.1:1"}`, `{}`); err != nil {
		t.Fatalf("connect: %v", err)
	}
	cases := []struct {
		name       string
		req        GroupOffsetResetRequest
		wantErrSub string
	}{
		{"bogus resetTo", GroupOffsetResetRequest{ConnectionID: "gr", Group: "g", ResetTo: "bogus"}, "resetTo must be"},
		{"latest without topics", GroupOffsetResetRequest{ConnectionID: "gr", Group: "g", ResetTo: "latest"}, "topics is required"},
		{"earliest without topics", GroupOffsetResetRequest{ConnectionID: "gr", Group: "g", ResetTo: "earliest"}, "topics is required"},
		{"timestamp without topics", GroupOffsetResetRequest{ConnectionID: "gr", Group: "g", ResetTo: "timestamp"}, "topics is required"},
		{"negative timestamp", GroupOffsetResetRequest{ConnectionID: "gr", Group: "g", ResetTo: "timestamp", Topics: []string{"t"}, TimestampMs: -1}, "timestampMs must be"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := service.ResetGroupOffsets(context.Background(), tc.req)
			if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
				t.Errorf("error = %v, want contains %q", err, tc.wantErrSub)
			}
		})
	}
	// partitionOffset 模式不要求 topics。
	if _, err := service.ResetGroupOffsets(context.Background(), GroupOffsetResetRequest{
		ConnectionID: "gr", Group: "g", ResetTo: "partitionOffset",
	}); err == nil {
		t.Error("partitionOffset without partitionOffsets should reach broker (dial error), got nil")
	}
}
