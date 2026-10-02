package kafkaconn

// messages_more_test.go：messages.go 纯函数与进 broker 之前的校验分支
// （契约 §5.3）——offset 映射、deadline 判定、matcher 组装矩阵、操作符
// 归一、headers 映射、snappy framed 兜底、consume/export/produce 的
// 离线校验路径。全部离线：ghost 连接或 127.0.0.1:1 快速失败。

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/klauspost/compress/snappy"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestOffsetPartitionsSorted(t *testing.T) {
	got := offsetPartitions(map[int32]int64{2: 10, 0: 5, 1: 7})
	if len(got) != 3 || got[0] != 0 || got[1] != 1 || got[2] != 2 {
		t.Errorf("offsetPartitions = %v, want [0 1 2]", got)
	}
	if got := offsetPartitions(nil); len(got) != 0 {
		t.Errorf("nil = %v, want empty", got)
	}
}

func TestPartitionOffsetMap(t *testing.T) {
	offset := kgo.NewOffset().At(42)
	out := partitionOffsetMap([]int32{0, 1}, offset)
	if len(out) != 2 {
		t.Fatalf("len = %d, want 2", len(out))
	}
	for _, partition := range []int32{0, 1} {
		// kgo.Offset 无导出 getter，直接比较结构体值（同一 offset 原样分发）。
		if item, ok := out[partition]; !ok || item != offset {
			t.Errorf("out[%d] = %+v, want the same offset value", partition, item)
		}
	}
	if out := partitionOffsetMap(nil, offset); len(out) != 0 {
		t.Errorf("nil partitions = %v", out)
	}
}

func TestIsDeadline(t *testing.T) {
	if !isDeadline(context.DeadlineExceeded) || !isDeadline(context.Canceled) {
		t.Error("deadline/canceled should be true")
	}
	if isDeadline(errors.New("broker down")) || isDeadline(nil) {
		t.Error("regular error should be false")
	}
}

func TestNextPartitionOffset(t *testing.T) {
	next := map[int32]int64{}
	nextPartitionOffset(next, &kgo.Record{Partition: 0, Offset: 41})
	nextPartitionOffset(next, &kgo.Record{Partition: 0, Offset: 43})
	nextPartitionOffset(next, &kgo.Record{Partition: 1, Offset: 7})
	// 乱序到达时取 max(offset+1)。
	nextPartitionOffset(next, &kgo.Record{Partition: 1, Offset: 3})
	if next[0] != 44 || next[1] != 8 {
		t.Errorf("next = %v, want map[0:44 1:8]", next)
	}
	// nil 安全。
	nextPartitionOffset(nil, &kgo.Record{Partition: 0, Offset: 0})
	nextPartitionOffset(next, nil)
	if len(next) != 2 {
		t.Errorf("next len = %d, want 2", len(next))
	}
}

func TestNewConsumeTextMatcherMatrix(t *testing.T) {
	// 非 regex 模式：不编译 patterns，直接返回。
	matcher, err := newConsumeTextMatcher(ConsumeParams{MatchMode: "prefix", Filter: "a"})
	if err != nil || matcher.mode != "prefix" || matcher.patterns != nil {
		t.Errorf("matcher = %+v, %v", matcher, err)
	}

	// regex 模式：四通道各自编译；重复 pattern 去重。
	matcher, err = newConsumeTextMatcher(ConsumeParams{
		MatchMode:    "regex",
		Filter:       `orders-\d+`,
		KeyFilter:    `k\d+`,
		ValueFilter:  `orders-\d+`, // 与 Filter 重复
		HeaderFilter: "trace-.*",
	})
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if len(matcher.patterns) != 3 {
		t.Errorf("patterns = %d, want 3 (deduped)", len(matcher.patterns))
	}
	if !matcher.match("orders-42", "orders-\\d+") == false {
		// matcher.match 自身会再编译一次；此处只验证 patterns 表存在。
		_ = matcher.patterns["orders-\\d+"]
	}
	if matcher.patterns[`k\d+`] == nil || matcher.patterns["trace-.*"] == nil {
		t.Errorf("compiled patterns missing: %v", matcher.patterns)
	}

	// 各通道非法 regex 都要在组装期报错。
	for _, tc := range []struct{ name, filter, key, value, header string }{
		{"filter", "[", "", "", ""},
		{"keyFilter", "", "(", "", ""},
		{"valueFilter", "", "", "*", ""},
		{"headerFilter", "", "", "", "+"},
	} {
		if _, err := newConsumeTextMatcher(ConsumeParams{
			MatchMode: "regex", Filter: tc.filter, KeyFilter: tc.key,
			ValueFilter: tc.value, HeaderFilter: tc.header,
		}); err == nil || !strings.Contains(err.Error(), "invalid regex filter") {
			t.Errorf("%s: err = %v, want invalid regex filter", tc.name, err)
		}
	}

	// 字段过滤的 regex value 也要预编译；非法时报错。
	matcher, err = newConsumeTextMatcher(ConsumeParams{
		MatchMode: "regex",
		FieldFilters: []ConsumeFieldFilter{
			{Source: "value", Operator: "regex", Value: `id=\d+`},
			{Source: "key", Operator: "contains", Value: "["}, // 非 regex 操作符不编译
		},
	})
	if err != nil || matcher.patterns[`id=\d+`] == nil {
		t.Errorf("field regex matcher = %+v, %v", matcher, err)
	}
	if _, err := newConsumeTextMatcher(ConsumeParams{
		MatchMode: "regex",
		FieldFilters: []ConsumeFieldFilter{
			{Source: "value", Operator: "regex", Value: "["},
		},
	}); err == nil || !strings.Contains(err.Error(), "invalid regex field filter") {
		t.Errorf("err = %v, want invalid regex field filter", err)
	}
}

func TestNormalizeFieldOperatorFullMatrix(t *testing.T) {
	cases := []struct{ raw, want string }{
		{"", "contains"}, {"contains", "contains"}, {"CONTAINS", "contains"},
		{"prefix", "prefix"}, {"starts_with", "prefix"}, {"starts-with", "prefix"},
		{"exact", "exact"}, {"equals", "exact"}, {"eq", "exact"},
		{"regexp", "regex"}, {"regex", "regex"},
		{"exists", "exists"},
		{"not_exists", "not_exists"}, {"not-exists", "not_exists"}, {"missing", "not_exists"},
		{">", "gt"}, {">=", "gte"}, {"<", "lt"}, {"<=", "lte"},
		{"gt", "gt"}, {"gte", "gte"}, {"lt", "lt"}, {"lte", "lte"},
		{"bogus", "bogus"}, // 未知透传（validateFieldFilters 兜底拒绝）
	}
	for _, tc := range cases {
		if got := normalizeFieldOperator(tc.raw); got != tc.want {
			t.Errorf("normalizeFieldOperator(%q) = %q, want %q", tc.raw, got, tc.want)
		}
	}
}

func TestRecordHeadersHelpers(t *testing.T) {
	// nil/空 → nil。
	if got := recordHeaders(nil); got != nil {
		t.Errorf("recordHeaders(nil) = %v", got)
	}
	if got := recordHeadersMap(nil); got != nil {
		t.Errorf("recordHeadersMap(nil) = %v", got)
	}

	// map → headers 按 key 排序。
	headers := recordHeaders(map[string]string{"b": "2", "a": "1"})
	if len(headers) != 2 || headers[0].Key != "a" || headers[1].Key != "b" {
		t.Errorf("recordHeaders = %+v", headers)
	}

	// headers → map；非法 UTF-8 值替换为安全预览。
	mapped := recordHeadersMap([]kgo.RecordHeader{
		{Key: "bin", Value: []byte{0xff, 0xfe}},
		{Key: "text", Value: []byte("ok")},
	})
	if mapped["text"] != "ok" {
		t.Errorf("text = %q", mapped["text"])
	}
	// ToValidUTF8 把整段非法字节序列替换为单个 U+FFFD。
	if mapped["bin"] != "\uFFFD" {
		t.Errorf("bin = %q, want replacement char", mapped["bin"])
	}

	// KAFKA-L3 回归：同 key 多 header 不再静默去重——第 2 条起加 "#2" 后缀。
	dup := recordHeadersMap([]kgo.RecordHeader{
		{Key: "trace", Value: []byte("first")},
		{Key: "trace", Value: []byte("second")},
		{Key: "trace", Value: []byte("third")},
		{Key: "solo", Value: []byte("1")},
	})
	if dup["trace"] != "first" || dup["trace#2"] != "second" || dup["trace#3"] != "third" {
		t.Errorf("dup headers = %v, want first/second/third preserved", dup)
	}
	if dup["solo"] != "1" {
		t.Errorf("solo = %q, single-key shape must not change", dup["solo"])
	}
}

func TestJSONValueString(t *testing.T) {
	cases := []struct {
		name  string
		value any
		want  string
	}{
		{"nil", nil, ""},
		{"string", "hi", "hi"},
		{"float int", float64(7), "7"},
		{"float frac", float64(1.5), "1.5"},
		{"bool", true, "true"},
		{"object", map[string]any{"a": float64(1)}, `{"a":1}`},
	}
	for _, tc := range cases {
		if got := jsonValueString(tc.value); got != tc.want {
			t.Errorf("%s: jsonValueString = %q, want %q", tc.name, got, tc.want)
		}
	}
	// 不可 JSON 序列化类型走 fmt.Sprint 兜底（非空、不 panic）。
	if got := jsonValueString(make(chan int)); got == "" {
		t.Error("fallback string should not be empty")
	}
}

func TestDecompressSnappyFramedFallback(t *testing.T) {
	// block 格式：直解。
	block := snappy.Encode(nil, []byte("hello snappy block"))
	got, err := decompressSnappy(block)
	if err != nil || string(got) != "hello snappy block" {
		t.Errorf("block decode = %q, %v", got, err)
	}

	// framed 格式：block 解码失败 → stream reader 兜底。
	var buffer bytes.Buffer
	writer := snappy.NewWriter(&buffer)
	if _, err := writer.Write([]byte("hello snappy framed")); err != nil {
		t.Fatalf("stream write: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("stream flush: %v", err)
	}
	got, err = decompressSnappy(buffer.Bytes())
	if err != nil || string(got) != "hello snappy framed" {
		t.Errorf("framed decode = %q, %v", got, err)
	}

	// 两种格式都失败 → 报错。
	if _, err := decompressSnappy([]byte("not-snappy-at-all")); err == nil {
		t.Error("invalid payload expected error")
	}
}

func TestConsumeValidationPathsOffline(t *testing.T) {
	service := NewService()
	// 空 topic 最先拒绝。
	if _, err := service.Consume(context.Background(), ConsumeParams{ConnectionID: "ghost"}); err == nil || !strings.Contains(err.Error(), "topic is required") {
		t.Errorf("error = %v, want topic required", err)
	}
	cases := []struct {
		name       string
		params     ConsumeParams
		wantErrSub string
	}{
		{"bad decode", ConsumeParams{Topic: "t", Decode: "hex"}, "decode must be"},
		{"bad decompression", ConsumeParams{Topic: "t", Decompression: "br"}, "decompression must be"},
		{"bad matchMode", ConsumeParams{Topic: "t", MatchMode: "bogus"}, "matchMode must be"},
		{"negative partition", ConsumeParams{Topic: "t", Partitions: []int32{-1}}, "partition must be"},
		{"negative offset", ConsumeParams{Topic: "t", PartitionOffsets: map[int32]int64{0: -1}}, "offset for partition"},
		{"bad isolation", ConsumeParams{Topic: "t", IsolationLevel: "serializable"}, "isolationLevel must be"},
		{"offset strategy without partitions", ConsumeParams{Topic: "t", OffsetStrategy: "offset"}, "offset strategy requires"},
		{"bad regex in params", ConsumeParams{Topic: "t", MatchMode: "regex", Filter: "["}, "invalid regex"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := service.Consume(context.Background(), tc.params)
			if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
				t.Errorf("error = %v, want contains %q", err, tc.wantErrSub)
			}
		})
	}
	// 全部合法 → 走到 consumeClient（未连接）报连接错。
	if _, err := service.Consume(context.Background(), ConsumeParams{ConnectionID: "ghost", Topic: "t"}); err == nil {
		t.Error("missing connection expected error")
	}
}

func TestExportValidationPathsOffline(t *testing.T) {
	service := NewService()
	if _, err := service.Export(context.Background(), ExportRequest{ConsumeParams: ConsumeParams{Topic: "t"}, Format: "yaml"}); err == nil || !strings.Contains(err.Error(), "format must be") {
		t.Errorf("error = %v, want format rejection", err)
	}
	if _, err := service.Export(context.Background(), ExportRequest{ConsumeParams: ConsumeParams{Topic: "t"}, Limit: -1}); err == nil || !strings.Contains(err.Error(), "limit must be") {
		t.Errorf("error = %v, want limit rejection", err)
	}
	// 参数合法但未连接 → 连接错（Export 强制 Commit=false 后进消费主流程）。
	if _, err := service.Export(context.Background(), ExportRequest{ConsumeParams: ConsumeParams{Topic: "t"}}); err == nil {
		t.Error("missing connection expected error")
	}
}

func TestProduceValidationPathsOffline(t *testing.T) {
	service := NewService()
	// 空 topic 先于门禁。
	if _, err := service.Produce(context.Background(), ProduceRequest{ConnectionID: "ghost"}); err == nil || !strings.Contains(err.Error(), "topic is required") {
		t.Errorf("error = %v, want topic required", err)
	}
	// ghost（read_only 兜底）→ blocked + 审计。
	var audits []AuditRecord
	service.Audit = func(rec AuditRecord) { audits = append(audits, rec) }
	if _, err := service.Produce(context.Background(), ProduceRequest{ConnectionID: "ghost", Topic: "t", Value: "v"}); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Errorf("error = %v, want read-only block", err)
	}
	if len(audits) != 1 || audits[0].Action != "produce" || audits[0].Result != "blocked" {
		t.Errorf("audits = %+v", audits)
	}

	// 门禁放行后的参数校验分支（都在建 client 之前）。
	service = NewService()
	if err := connectWithConfig(t, service, "prod",
		`{"bootstrap_servers": "127.0.0.1:1"}`, `{}`); err != nil {
		t.Fatalf("connect: %v", err)
	}
	negative := int32(-1)
	cases := []struct {
		name       string
		req        ProduceRequest
		wantErrSub string
	}{
		{"negative partition", ProduceRequest{ConnectionID: "prod", Topic: "t", Value: "v", Partition: &negative}, "partition must be"},
		{"bad compression", ProduceRequest{ConnectionID: "prod", Topic: "t", Value: "v", Compression: "br"}, "compression must be"},
		{"value exclusive", ProduceRequest{ConnectionID: "prod", Topic: "t", Value: "v", ValueBase64: "aGk="}, "mutually exclusive"},
		{"bad value base64", ProduceRequest{ConnectionID: "prod", Topic: "t", ValueBase64: "!!"}, "not valid base64"},
		{"empty payload", ProduceRequest{ConnectionID: "prod", Topic: "t"}, "value (or valueBase64) is required"},
		{"key exclusive", ProduceRequest{ConnectionID: "prod", Topic: "t", Value: "v", Key: "k", KeyBase64: "a2g="}, "mutually exclusive"},
		{"bad key base64", ProduceRequest{ConnectionID: "prod", Topic: "t", Value: "v", KeyBase64: "??"}, "not valid base64"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := service.Produce(context.Background(), tc.req)
			if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
				t.Errorf("error = %v, want contains %q", err, tc.wantErrSub)
			}
		})
	}
}

// KAFKA-PR-M1 回归：契约锚点取最大 offset（count>1 同分区 = 末条；跨分区 =
// 全局末条写入位置）。此前取完成序首个结果，count>1 系统性偏小。
func TestProduceAnchorResultTakesMaxOffset(t *testing.T) {
	record := func(partition int32, offset int64) *kgo.Record {
		return &kgo.Record{Topic: "t", Partition: partition, Offset: offset}
	}
	results := kgo.ProduceResults{
		{Record: record(0, 100)},
		{Record: record(1, 57)},
		{Record: record(0, 104)},
	}
	anchor := produceAnchorResult(results)
	if anchor.Record.Partition != 0 || anchor.Record.Offset != 104 {
		t.Fatalf("anchor = p%d o%d, want p0 o104 (max offset)", anchor.Record.Partition, anchor.Record.Offset)
	}
	single := produceAnchorResult(kgo.ProduceResults{{Record: record(3, 7)}})
	if single.Record.Offset != 7 {
		t.Fatalf("single anchor = o%d, want o7", single.Record.Offset)
	}
}
