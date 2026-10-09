package mcp

// consume_schema_parity_test.go：kafka_messages_digest schema ↔
// kafkaconn.ConsumeParams json tag 双向对照（架构审查 WATCH"参数形状不在契约
// 守护面内"的最小钉）。此前 groupId/isolationLevel/timestamp/offset 范围在
// 解析器里支持、schema 未声明——LLM 无法使用且测试全绿；partitionOffsets
// 更是 strategy=offset 的必填参数（枚举里有 offset 却必然报错）。
// 新增 ConsumeParams 字段或 digest 参数时本测试红灯，强制二选一：
// 声明进 schema（LLM 可见）或登记 allowlist（写明不暴露理由）。

import (
	"reflect"
	"strings"
	"testing"

	"io.dbx.kafka.plugin/internal/kafkaconn"
)

func digestSchemaProperties(t *testing.T) map[string]any {
	t.Helper()
	server := NewServer(kafkaconn.NewService(), nil)
	rawTools, ok := server.Tools("")["tools"].([]map[string]any)
	if !ok {
		t.Fatalf("tools list shape: %#v", server.Tools("")["tools"])
	}
	for _, tool := range rawTools {
		if tool["name"] != "kafka_messages_digest" {
			continue
		}
		schema, ok := tool["inputSchema"].(map[string]any)
		if !ok {
			t.Fatalf("digest inputSchema shape: %v", tool["inputSchema"])
		}
		props, ok := schema["properties"].(map[string]any)
		if !ok {
			t.Fatalf("digest properties shape: %v", schema["properties"])
		}
		return props
	}
	t.Fatal("kafka_messages_digest not in tools list")
	return nil
}

func consumeParamsJSONTags() map[string]bool {
	tags := map[string]bool{}
	paramType := reflect.TypeOf(kafkaconn.ConsumeParams{})
	for i := 0; i < paramType.NumField(); i++ {
		name := strings.Split(paramType.Field(i).Tag.Get("json"), ",")[0]
		if name != "" && name != "-" {
			tags[name] = true
		}
	}
	return tags
}

// digest 专属参数（不进 ConsumeParams，聚合/分页语义）。
var digestOnlyParams = map[string]string{
	"fields": "JSON-path projection list (digest aggregation side)",
	"format": "digest | rows output shape",
}

// ConsumeParams 中有意不进 digest schema 的字段（allowlist 必须写明理由）。
var digestStructAllowlist = map[string]string{
	"limit":               "digest 置 limit=maxScanRecords（聚合语义），不对外",
	"timeoutMs":           "扫描窗口由宿主 invoke deadline 与默认 15s 决定，不对外",
	"commit":              "digest 是只读扫描，无 commit 语义",
	"consumeId":           "取消句柄属一次性消费传输层，digest 不支持取消",
	"skipValueBase64":     "digest 固定跳过 valueBase64 通道，内部设置",
	"retentionByteBudget": "digest 固定显式预算（digestRetentionByteBudget），内部设置",
	"type":                "预设类型标记（issue #75 监控方案），仅 presets store 读写，digest 无预设语义",
	"monitor":             "监控方案载荷（issue #75），仅 presets store 读写，digest 无预设语义",
}

func TestDigestSchemaCoversConsumeParams(t *testing.T) {
	props := digestSchemaProperties(t)
	tags := consumeParamsJSONTags()

	for name := range tags {
		if _, declared := props[name]; declared {
			continue
		}
		if _, allowed := digestStructAllowlist[name]; allowed {
			continue
		}
		t.Errorf("ConsumeParams json tag %q is neither declared in the digest schema nor allowlisted — declare it in tools.go (LLM visibility) or add it to digestStructAllowlist with a reason", name)
	}
	for name := range props {
		if tags[name] {
			continue
		}
		if _, only := digestOnlyParams[name]; only {
			continue
		}
		t.Errorf("digest schema property %q has no matching ConsumeParams json tag and is not digest-only — a typo here is silently dropped by the parser", name)
	}
}

// 解析器与 schema 的三角闭环：strategy=offset 的必填参数 partitionOffsets
// 必须真的能从 digest 参数进 ConsumeParams（此前枚举有 offset、解析/schema
// 双缺，选了必然后端报错）。
func TestDigestAcceptsPartitionOffsets(t *testing.T) {
	// JSON-RPC 参数的数字面量到达时是 float64（coerceInt64 只认 float64/字符串）。
	offsets, err := parseConsumePartitionOffsets(map[string]any{
		"0": float64(120),
		"3": "55",
	})
	if err != nil {
		t.Fatalf("parse = %v", err)
	}
	if len(offsets) != 2 || offsets[0] != 120 || offsets[3] != 55 {
		t.Fatalf("offsets = %v, want {0:120, 3:55} (numeric string values tolerated)", offsets)
	}
	if _, err := parseConsumePartitionOffsets(map[string]any{"-1": 1}); err == nil {
		t.Error("negative partition must be rejected")
	}
	if _, err := parseConsumePartitionOffsets(map[string]any{"0": -5}); err == nil {
		t.Error("negative offset must be rejected")
	}
	if got, err := parseConsumePartitionOffsets(nil); err != nil || got != nil {
		t.Errorf("nil = %v, %v; want nil, nil", got, err)
	}
}
