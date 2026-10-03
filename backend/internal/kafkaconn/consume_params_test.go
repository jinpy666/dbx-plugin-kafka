package kafkaconn

// consume_params_test.go：ConsumeParams 校验与 offset 策略（§5.3 互斥约束，
// 纯解析不连网）。

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func int64Ptr(v int64) *int64 { return &v }

func TestValidateConsumeParamsMutualExclusion(t *testing.T) {
	base := ConsumeParams{ConnectionID: "c1", Topic: "orders"}

	// 基线合法。
	if err := validateConsumeParams(base); err != nil {
		t.Fatalf("validate(base) error = %v", err)
	}

	// commit=true 必须带 groupId。
	commitNoGroup := base
	commitNoGroup.Commit = true
	if err := validateConsumeParams(commitNoGroup); err == nil || err.Error() != "commit=true requires groupId" {
		t.Errorf("commit without groupId error = %v", err)
	}

	// commit=true 禁一切过滤。
	commitWithFilter := base
	commitWithFilter.Commit = true
	commitWithFilter.GroupID = "g1"
	commitWithFilter.ValueFilter = "x"
	if err := validateConsumeParams(commitWithFilter); err == nil {
		t.Error("commit + filter expected error")
	}
	commitWithFieldFilter := commitWithFilter
	commitWithFieldFilter.ValueFilter = ""
	commitWithFieldFilter.FieldFilters = []ConsumeFieldFilter{{Source: "value", Operator: "exists"}}
	if err := validateConsumeParams(commitWithFieldFilter); err == nil {
		t.Error("commit + fieldFilter expected error")
	}
	commitWithRange := commitWithFilter
	commitWithRange.FieldFilters = nil
	commitWithRange.TimestampFrom = int64Ptr(1)
	if err := validateConsumeParams(commitWithRange); err == nil {
		t.Error("commit + range expected error")
	}

	// committed 策略必须 groupId（consumeOffset 层校验）。
	if _, err := consumeOffset("committed", "", false, false, 0); err == nil {
		t.Error("committed without groupId expected error")
	}
	// committed 策略不能与 partitions 组合。
	if _, err := consumeOffset("committed", "", true, true, 0); err == nil {
		t.Error("committed + partitions expected error")
	}

	// strategy=offset 必填 partitionOffsets（buildConsumeOpts 层语义：
	// usesExactOffsets + 归一化后 partitions 缺省回退）。
	if !usesExactOffsets("offset", nil) {
		t.Error("usesExactOffsets(offset) = false")
	}
	if !usesExactOffsets("latest", map[int32]int64{0: 1}) {
		t.Error("usesExactOffsets(with offsets) = false")
	}
	if usesExactOffsets("latest", nil) {
		t.Error("usesExactOffsets(latest, no offsets) = true")
	}

	// timestamp 策略必须 offsetTime。
	if _, err := consumeOffset("timestamp", "", false, false, 0); err == nil {
		t.Error("timestamp without offsetTime expected error")
	}
	if _, err := consumeOffset("timestamp", "1700000000000", false, false, 0); err != nil {
		t.Errorf("timestamp with ms error = %v", err)
	}

	// recent 策略：相对末端回退窗口（issue #16）。kgo.Offset 的 relative
	// 字段未导出，用 MarshalJSON 断言（relative≠0 时才带 "Relative" 键）。
	marshalOffset := func(o kgo.Offset) string {
		raw, _ := o.MarshalJSON()
		return string(raw)
	}
	recent, err := consumeOffset("recent", "", false, false, 500)
	if err != nil {
		t.Fatalf("recent error = %v", err)
	}
	if got := marshalOffset(recent); !strings.Contains(got, `"Relative":-500`) {
		t.Errorf("recent offset = %s, want relative -500 to log end", got)
	}
	// 窗口非正时钳到 1 条（至少读末端前 1 条，不退化为纯 tail）。
	clamped, err := consumeOffset("recent", "", false, false, 0)
	if err != nil {
		t.Fatalf("recent(0) error = %v", err)
	}
	if got := marshalOffset(clamped); !strings.Contains(got, `"Relative":-1`) {
		t.Errorf("recent(0) offset = %s, want relative -1", got)
	}

	// 范围 from > to 拒绝。
	badRange := base
	badRange.OffsetFrom = int64Ptr(10)
	badRange.OffsetTo = int64Ptr(5)
	if err := validateConsumeParams(badRange); err == nil {
		t.Error("offsetFrom > offsetTo expected error")
	}

	// maxScanRecords 负数拒绝。
	negative := base
	negative.MaxScanRecords = -1
	if err := validateConsumeParams(negative); err == nil {
		t.Error("negative maxScanRecords expected error")
	}
}

func TestConsumeOffsetStrategies(t *testing.T) {
	cases := []struct {
		name       string
		strategy   string
		offsetTime string
		hasGroup   bool
		partitions bool
		wantErr    bool
	}{
		{"default", "", "", false, false, false},
		{"latest", "latest", "", false, false, false},
		{"recent", "recent", "", false, false, false},
		{"recent alias last", "last", "", false, true, false},
		{"earliest", "earliest", "", false, true, false},
		{"committed with group", "committed", "", true, false, false},
		{"committed without group", "committed", "", false, false, true},
		{"committed with partitions", "committed", "", true, true, true},
		{"timestamp without time", "timestamp", "", false, false, true},
		{"timestamp with ms", "timestamp", "1700000000000", false, false, false},
		{"bogus", "bogus", "", false, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := consumeOffset(tc.strategy, tc.offsetTime, tc.hasGroup, tc.partitions, 100)
			if (err != nil) != tc.wantErr {
				t.Errorf("consumeOffset(%q) error = %v, wantErr %v", tc.strategy, err, tc.wantErr)
			}
		})
	}
}

func TestBuildConsumeOptsOffsetStrategyRequiresOffsets(t *testing.T) {
	params := ConsumeParams{Topic: "orders", OffsetStrategy: "offset"}
	// 无 partitions 无 offsets → 明确错误。
	if _, err := buildConsumeOpts(params, "orders", "", nil, nil, isolationNone()); err == nil {
		t.Error("offset strategy without offsets expected error")
	}
	// 有 offsets → exact seek，成功。
	opts, err := buildConsumeOpts(params, "orders", "", []int32{0}, map[int32]int64{0: 42}, isolationNone())
	if err != nil {
		t.Fatalf("exact offsets error = %v", err)
	}
	if len(opts) == 0 {
		t.Error("opts should not be empty")
	}
	// strategy=offset 有 partitions 但缺该分区 offset → 报错。
	// 分区列表 [1]，offsets 只给了分区 0 → 分区 1 缺 offset。
	if _, err := buildConsumeOpts(params, "orders", "", []int32{1}, map[int32]int64{0: 42}, isolationNone()); err == nil {
		t.Error("missing partition offset expected error")
	}
}

func TestNormalizeConsumePartitions(t *testing.T) {
	got, err := normalizeConsumePartitions([]int32{2, 0, 2, 1})
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if len(got) != 3 || got[0] != 0 || got[1] != 1 || got[2] != 2 {
		t.Errorf("partitions = %v, want [0 1 2]", got)
	}
	if _, err := normalizeConsumePartitions([]int32{-1}); err == nil {
		t.Error("negative partition expected error")
	}
	if got, err := normalizeConsumePartitions(nil); err != nil || got != nil {
		t.Errorf("nil partitions = %v, %v", got, err)
	}
}

func TestNormalizeConsumePartitionOffsets(t *testing.T) {
	got, err := normalizeConsumePartitionOffsets(map[int32]int64{1: 5})
	if err != nil || got[1] != 5 {
		t.Fatalf("offsets = %v, %v", got, err)
	}
	if _, err := normalizeConsumePartitionOffsets(map[int32]int64{1: -1}); err == nil {
		t.Error("negative offset expected error")
	}
}

func TestParseTimestampMillis(t *testing.T) {
	millis, err := parseTimestampMillis("1700000000000")
	if err != nil || millis != 1700000000000 {
		t.Errorf("unix ms = %d, %v", millis, err)
	}
	millis, err = parseTimestampMillis("2023-11-14T22:13:20Z")
	if err != nil || millis != 1700000000000 {
		t.Errorf("RFC3339 = %d, %v", millis, err)
	}
	if _, err := parseTimestampMillis(""); err == nil {
		t.Error("empty expected error")
	}
	if _, err := parseTimestampMillis("-1"); err == nil {
		t.Error("negative expected error")
	}
}

func TestParseOffsetTimeModes(t *testing.T) {
	mode, _, err := parseOffsetTime("latest")
	if err != nil || mode != "latest" {
		t.Errorf("latest = %q, %v", mode, err)
	}
	mode, _, err = parseOffsetTime("earliest")
	if err != nil || mode != "earliest" {
		t.Errorf("earliest = %q, %v", mode, err)
	}
	mode, millis, err := parseOffsetTime("2023-11-14T22:13:20Z")
	if err != nil || mode != "timestamp" || millis != 1700000000000 {
		t.Errorf("RFC3339 = %q/%d, %v", mode, millis, err)
	}
	if _, _, err := parseOffsetTime("bogus"); err == nil {
		t.Error("bogus expected error")
	}
}

func TestConsumeMaxScanRecords(t *testing.T) {
	if got := consumeMaxScanRecords(100, 0); got != 1000 {
		t.Errorf("default = %d, want 1000", got)
	}
	if got := consumeMaxScanRecords(200, 0); got != 2000 {
		t.Errorf("limit*10 = %d, want 2000", got)
	}
	if got := consumeMaxScanRecords(100, 50); got != 50 {
		t.Errorf("explicit = %d, want 50", got)
	}
}

func TestNormalizeProduceCount(t *testing.T) {
	if got := normalizeProduceCount(0); got != 1 {
		t.Errorf("default = %d, want 1", got)
	}
	if got := normalizeProduceCount(500); got != 500 {
		t.Errorf("500 = %d", got)
	}
	if got := normalizeProduceCount(5000); got != 1000 {
		t.Errorf("clamp = %d, want 1000", got)
	}
}

func TestProduceCompressionCodec(t *testing.T) {
	for _, method := range []string{"gzip", "lz4", "zstd", "snappy"} {
		codec, ok, err := produceCompressionCodec(method)
		if err != nil || !ok {
			t.Errorf("codec(%s) = %v, %v", method, ok, err)
		}
		_ = codec
	}
	if _, ok, err := produceCompressionCodec(""); err != nil || ok {
		t.Errorf("empty codec ok = %v, %v", ok, err)
	}
	if _, _, err := produceCompressionCodec("bogus"); err == nil {
		t.Error("bogus codec expected error")
	}
}

func TestIsolationLevelValue(t *testing.T) {
	if _, err := isolationLevelValue("read_committed"); err != nil {
		t.Errorf("read_committed error = %v", err)
	}
	if _, err := isolationLevelValue(""); err != nil {
		t.Errorf("default error = %v", err)
	}
	if _, err := isolationLevelValue("serializable"); err == nil {
		t.Error("bogus isolation expected error")
	}
}

func TestConsumeScanBatchSize(t *testing.T) {
	if got := consumeScanBatchSize(10); got != 10 {
		t.Errorf("small = %d", got)
	}
	if got := consumeScanBatchSize(10000); got != 256 {
		t.Errorf("large = %d, want 256", got)
	}
	if got := consumeScanBatchSize(0); got != 1 {
		t.Errorf("zero = %d, want 1", got)
	}
}

func TestValidateConsumeParamsTimestampRange(t *testing.T) {
	params := ConsumeParams{ConnectionID: "c1", Topic: "t"}
	params.TimestampFrom = int64Ptr(2000)
	params.TimestampTo = int64Ptr(1000)
	err := validateConsumeParams(params)
	if err == nil || err.Error() != "timestampFrom must be less than or equal to timestampTo" {
		t.Errorf("timestamp range error = %v", err)
	}
}

// isolationNone 测试辅助：默认隔离级别。
func isolationNone() kgo.IsolationLevel {
	level, _ := isolationLevelValue("")
	return level
}

// --- Phase 2：offsets 全策略与 produce 载荷二选一 ---

func TestParseOffsetTimeModesPhase2(t *testing.T) {
	for input, want := range map[string]string{
		"max-timestamp": "max-timestamp",
		"MAX_TIMESTAMP": "max-timestamp",
		"log-start":     "log-start",
		"log_start":     "log-start",
	} {
		mode, _, err := parseOffsetTime(input)
		if err != nil || mode != want {
			t.Errorf("parseOffsetTime(%q) = %q, %v; want %q", input, mode, err, want)
		}
	}
	// 负数整数是协议保留值（-1/-2/-3/-4），拒绝而不是当时间戳。
	if _, _, err := parseOffsetTime("-3"); err == nil {
		t.Error("negative integer offsetTime expected error")
	}
	if _, _, err := parseOffsetTime("1700000000000"); err != nil {
		t.Errorf("unix ms error = %v", err)
	}
}

func TestProducePayloadBytesExclusive(t *testing.T) {
	payload, err := producePayloadBytes(ProduceRequest{Value: "hello"})
	if err != nil || string(payload) != "hello" {
		t.Errorf("value path = %q, %v", payload, err)
	}
	payload, err = producePayloadBytes(ProduceRequest{ValueBase64: "aGVsbG8="})
	if err != nil || string(payload) != "hello" {
		t.Errorf("valueBase64 path = %q, %v", payload, err)
	}
	if _, err := producePayloadBytes(ProduceRequest{Value: "a", ValueBase64: "Yg=="}); err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("both given error = %v", err)
	}
	if _, err := producePayloadBytes(ProduceRequest{}); err == nil || !strings.Contains(err.Error(), "required") {
		t.Errorf("none given error = %v", err)
	}
	if _, err := producePayloadBytes(ProduceRequest{ValueBase64: "!!not-base64!!"}); err == nil {
		t.Error("bad base64 expected error")
	}
}

func TestConsumeParamsSchemaRefParsing(t *testing.T) {
	var consume ConsumeParams
	raw := []byte(`{"connectionId":"c1","topic":"t","schema":{"subject":"s-value","version":3,"format":"avro"}}`)
	if err := json.Unmarshal(raw, &consume); err != nil {
		t.Fatalf("unmarshal ConsumeParams error = %v", err)
	}
	if consume.Schema == nil || consume.Schema.Subject != "s-value" || consume.Schema.Version != 3 || consume.Schema.Format != "avro" {
		t.Errorf("schema = %+v", consume.Schema)
	}
}

// S-CAP-LIMIT（评审 H-3/SEC-002）：limit/maxScanRecords 硬上限——此前
// limit 无上界且按值预分配结果切片，单请求可要求约 200GB，打爆长驻
// sidecar。limit 上限与 export 对齐（10000）；maxScan 上限 1e6。
func TestConsumeLimitAndScanCaps(t *testing.T) {
	if got := clampConsumeLimit(0); got != 100 {
		t.Errorf("default limit = %d, want 100", got)
	}
	if got := clampConsumeLimit(500); got != 500 {
		t.Errorf("explicit limit = %d", got)
	}
	if got := clampConsumeLimit(1_000_000_000); got != consumeLimitHardCap {
		t.Errorf("clamped limit = %d, want %d", got, consumeLimitHardCap)
	}
	if got := consumeMaxScanRecords(100, 2_000_000_000); got != consumeMaxScanHardCap {
		t.Errorf("clamped maxScan = %d, want %d", got, consumeMaxScanHardCap)
	}
	if got := consumeMaxScanRecords(10000, 0); got != 100000 {
		t.Errorf("default maxScan = %d, want 100000", got)
	}
	if got := consumeMaxScanRecords(100, 0); got != 1000 {
		t.Errorf("small default maxScan = %d, want 1000", got)
	}
}

// 池复用资格与精确起点互斥（评审 H-1，2026-09-28）：带 partitionOffsets
// 的消费走精确 seek，池签名不含 offsets、reset 只回分区边界——复用会静默
// 丢弃用户请求的起点，精确-offset client 入池还会污染后续同形状请求。
func TestConsumeReuseEligibleExcludesExactOffsets(t *testing.T) {
	offsets := map[int32]int64{0: 500}

	// 带 partitionOffsets：无论策略如何一律不入池。
	for _, strategy := range []string{"", "default", "earliest", "latest"} {
		if ok, _ := consumeReuseEligible(strategy, "", offsets); ok {
			t.Errorf("strategy %q with partitionOffsets must not reuse pool", strategy)
		}
	}
	// 无 partitionOffsets：策略门保持原语义。
	if ok, atStart := consumeReuseEligible("", "", nil); !ok || !atStart {
		t.Errorf("default strategy should reuse at start, got ok=%v atStart=%v", ok, atStart)
	}
	if ok, atStart := consumeReuseEligible("latest", "", nil); !ok || atStart {
		t.Errorf("latest strategy should reuse at end, got ok=%v atStart=%v", ok, atStart)
	}
	if ok, _ := consumeReuseEligible("", "g1", nil); ok {
		t.Error("group mode must not reuse pool")
	}
	if ok, _ := consumeReuseEligible("offset", "", nil); ok {
		t.Error("offset strategy must not reuse pool")
	}
}

// digest 聚合上界（评审 M-1，2026-09-28）：留存预算路径不吃 limit 硬钳位，
// 否则 maxScanRecords 超过 1 万时扫描在钳位处提前终止，聚合分布只覆盖子集。
// H-1 回归（2026-10-02）：只有显式预算（digest）才抬升命中上界；工作台
// 默认兜底预算不得改变 limit 契约语义（返回条数上限）。
func TestConsumeEffectiveLimitDigestBypass(t *testing.T) {
	limit := clampConsumeLimit(100_000) // digest 把 Limit 设为 maxScanRecords
	maxScan := consumeMaxScanRecords(limit, 100_000)
	if maxScan != 100_000 {
		t.Fatalf("maxScan = %d, want 100000", maxScan)
	}
	if got := consumeEffectiveLimit(limit, maxScan, false); got != consumeLimitHardCap {
		t.Errorf("non-digest effective limit = %d, want %d", got, consumeLimitHardCap)
	}
	if got := consumeEffectiveLimit(limit, maxScan, true); got != maxScan {
		t.Errorf("digest effective limit = %d, want maxScan %d", got, maxScan)
	}
	// maxScan 小于钳位时 digest 也取较大者，语义一致。
	if got := consumeEffectiveLimit(100, 1000, true); got != 1000 {
		t.Errorf("digest small-scan effective limit = %d, want 1000", got)
	}
	// 工作台默认预算不抬升 limit：请求 limit=100 时命中上界就是 100。
	if got := consumeEffectiveLimit(100, 1000, false); got != 100 {
		t.Errorf("workbench effective limit = %d, want 100 (contract §5.3)", got)
	}
}

// H-1 回归：显式预算与默认兜底的推导。工作台不传 retentionByteBudget，
// 不得被推导成 digest 聚合路径。
func TestConsumeRetentionSetup(t *testing.T) {
	digest, budget := consumeRetentionSetup(0)
	if digest {
		t.Error("zero budget must not be treated as digest aggregation")
	}
	if budget != workbenchRetentionByteBudget {
		t.Errorf("workbench fallback budget = %d, want %d", budget, workbenchRetentionByteBudget)
	}
	digest, budget = consumeRetentionSetup(64 << 20)
	if !digest {
		t.Error("explicit budget must be treated as digest aggregation")
	}
	if budget != 64<<20 {
		t.Errorf("explicit budget = %d, want %d", budget, 64<<20)
	}
}

// H-1 回归（请求形状）：工作台 consume 请求不带 retentionByteBudget 时，
// 命中上界就是请求 limit（协议 §5.3 limit = 返回条数上限；此前默认预算把
// 命中上界抬到 maxScan，limit=100 最多可返回 1 万条）。
func TestConsumeDefaultParamsRespectLimitContract(t *testing.T) {
	var params ConsumeParams // 模拟前端工作台请求：不传 retentionByteBudget
	if params.RetentionByteBudget > 0 {
		t.Fatalf("frontend requests must not carry retentionByteBudget, got %d", params.RetentionByteBudget)
	}
	digest, _ := consumeRetentionSetup(params.RetentionByteBudget)
	limit := clampConsumeLimit(params.Limit)
	maxScan := consumeMaxScanRecords(limit, params.MaxScanRecords)
	if got := consumeEffectiveLimit(limit, maxScan, digest); got != 100 {
		t.Errorf("default consume hit cap = %d, want request limit 100", got)
	}
}

// KAFKA-H2 回归：响应传输预算的 wire 估算必须覆盖双通道 value、key 与
// headers（保守高估，防止预算误放行）。
func TestConsumeWireSizeEstimateCoversPayload(t *testing.T) {
	msg := ConsumedMessage{
		Topic:       "t",
		ValueText:   strings.Repeat("v", 1000),
		ValueBase64: strings.Repeat("A", 1400),
		Headers:     map[string]string{"h1": strings.Repeat("x", 100)},
	}
	wire := consumeWireSizeBytes(msg)
	payload := len("t") + len(msg.ValueText) + len(msg.ValueBase64) + len("h1") + len(msg.Headers["h1"])
	if wire < payload {
		t.Fatalf("wire estimate %d smaller than payload sum %d", wire, payload)
	}
	if cap := payload + 256 + 8*len(msg.Headers); wire > cap {
		t.Fatalf("wire estimate %d inflated beyond fixed overhead cap %d", wire, cap)
	}
}

// groupId×partitions 互斥门（评审 M-1）：分区直读路径从不注册 ConsumerGroup，
// groupID 非空 + DisableAutoCommit 会让 franz-go 拒建 client 且报错误导。
func TestValidateConsumeParamsGroupIdPartitionsExclusive(t *testing.T) {
	withGroupPartitions := ConsumeParams{ConnectionID: "c1", Topic: "orders", GroupID: "g1", Partitions: []int32{0}}
	if err := validateConsumeParams(withGroupPartitions); err == nil {
		t.Error("groupId + partitions expected error")
	}
	withGroupOffsets := ConsumeParams{ConnectionID: "c1", Topic: "orders", GroupID: "g1", PartitionOffsets: map[int32]int64{0: 5}}
	if err := validateConsumeParams(withGroupOffsets); err == nil {
		t.Error("groupId + partitionOffsets expected error")
	}
	// 仅 groupId（无分区）仍合法；仅 partitions（无 groupId）仍合法。
	onlyGroup := ConsumeParams{ConnectionID: "c1", Topic: "orders", GroupID: "g1"}
	if err := validateConsumeParams(onlyGroup); err != nil {
		t.Errorf("groupId only error = %v", err)
	}
	onlyPartitions := ConsumeParams{ConnectionID: "c1", Topic: "orders", Partitions: []int32{0}}
	if err := validateConsumeParams(onlyPartitions); err != nil {
		t.Errorf("partitions only error = %v", err)
	}
}
