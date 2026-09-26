package mcp

// util_test.go：缺参枚举（ssh 同款，MCP_ACCEPTANCE §3.9，第七轮拉齐）——
// 一次报出全部缺失 required 参数（按 schema required 顺序）；单缺只点名
// 其一；显式 null 视同缺失；present-but-类型错误（空串/非法值）不混入
// 枚举，由逐参数精确校验点名。

import (
	"strings"
	"testing"

	"io.dbx.kafka.plugin/internal/kafkaconn"
)

func TestMissingRequiredEnumeration(t *testing.T) {
	err := missingRequired(map[string]any{}, "connectionId", "group", "resetTo")
	if err == nil || err.Error() != "Missing required parameters: connectionId, group, resetTo" {
		t.Fatalf("triple missing must enumerate in schema order: %v", err)
	}
	err = missingRequired(map[string]any{"group": "g"}, "connectionId", "group", "resetTo")
	if err == nil || err.Error() != "Missing required parameters: connectionId, resetTo" {
		t.Fatalf("partial missing must enumerate the gaps only: %v", err)
	}
	err = missingRequired(map[string]any{"connectionId": "c", "group": nil, "resetTo": "latest"}, "connectionId", "group", "resetTo")
	if err == nil || err.Error() != "Missing required parameters: group" {
		t.Fatalf("explicit null must count as missing: %v", err)
	}
	if err := missingRequired(map[string]any{"connectionId": "c", "group": "g", "resetTo": "latest"}, "connectionId", "group", "resetTo"); err != nil {
		t.Fatalf("all present must pass: %v", err)
	}
}

// S-REQ-ENUM 工具入口接线：digest 双缺全点名、单缺只其一、空串走精确点名。
func TestMessagesDigestMissingParamsEnumerated(t *testing.T) {
	server := NewServer(kafkaconn.NewService(), nil)
	_, err := server.Call("kafka_messages_digest", map[string]any{})
	if err == nil || err.Error() != "Missing required parameters: connectionId, topic" {
		t.Fatalf("digest {} must enumerate both in schema order: %v", err)
	}
	_, err = server.Call("kafka_messages_digest", map[string]any{"connectionId": "c"})
	if err == nil || err.Error() != "Missing required parameters: topic" {
		t.Fatalf("digest without topic must name it only: %v", err)
	}
	_, err = server.Call("kafka_messages_digest", map[string]any{"connectionId": nil, "topic": "t"})
	if err == nil || !strings.Contains(err.Error(), "Missing required parameters: connectionId") {
		t.Fatalf("null connectionId must count as missing: %v", err)
	}
	// present-but-空串：精确点名（不进枚举）。
	_, err = server.Call("kafka_messages_digest", map[string]any{"connectionId": "c", "topic": "  "})
	if err == nil || !strings.Contains(err.Error(), "topic is required") {
		t.Fatalf("blank topic must hit the precise per-param check: %v", err)
	}
}

// S-REQ-ENUM offsets_reset 三缺全点名；topics_delete/topics 存在但空数组
// 走精确点名（数组参数的 present-but-invalid 形态）。
func TestWriteToolsMissingParamsEnumerated(t *testing.T) {
	server := NewServer(kafkaconn.NewService(), nil)
	_, err := server.Call("kafka_groups_offsets_reset", map[string]any{})
	if err == nil || err.Error() != "Missing required parameters: connectionId, group, resetTo" {
		t.Fatalf("reset {} must enumerate all three: %v", err)
	}
	_, err = server.Call("kafka_groups_offsets_reset", map[string]any{"connectionId": "c", "resetTo": "latest"})
	if err == nil || err.Error() != "Missing required parameters: group" {
		t.Fatalf("reset without group must name it only: %v", err)
	}
	_, err = server.Call("kafka_topics_delete", map[string]any{})
	if err == nil || err.Error() != "Missing required parameters: connectionId, topics" {
		t.Fatalf("delete {} must enumerate both: %v", err)
	}
	// topics 存在但为空数组：精确点名（不混入枚举）。
	_, err = server.Call("kafka_topics_delete", map[string]any{"connectionId": "c", "topics": []any{}})
	if err == nil || !strings.Contains(err.Error(), "topics is required") {
		t.Fatalf("empty topics array must hit the precise per-param check: %v", err)
	}
}

// S-INT-STRICT 整数参数不接受小数（评审 M：1.9 个 offset 静默截成 1 违反
// 「绝不静默折算」准确性红线——partition/timestampMs/offset 系全家一致）。
func TestCoerceIntRejectsFractionalFloats(t *testing.T) {
	for _, tc := range []struct {
		name   string
		raw    any
		want   int
		wantOK bool
	}{
		{"integral float accepted", float64(2), 2, true},
		{"fractional float rejected", float64(1.9), 0, false},
		{"negative fractional rejected", float64(-0.5), 0, false},
		{"integer string accepted", "3", 3, true},
		{"non numeric string rejected", "abc", 0, false},
		{"bool rejected", true, 0, false},
	} {
		got, ok := coerceInt(tc.raw)
		if ok != tc.wantOK || (ok && got != tc.want) {
			t.Fatalf("%s: coerceInt(%v) = (%d,%v), want (%d,%v)", tc.name, tc.raw, got, ok, tc.want, tc.wantOK)
		}
		if _, int64OK := coerceInt64(tc.raw); int64OK != tc.wantOK {
			t.Fatalf("%s: coerceInt64(%v) ok=%v, want %v", tc.name, tc.raw, int64OK, tc.wantOK)
		}
	}
	// 超出 int64 的浮点必须拒绝而非回绕。
	if _, ok := coerceInt64(float64(1) * 1e30); ok {
		t.Fatal("out-of-range float must be rejected")
	}
}

// S-INT-STRICT parsePartitionOffsets 对小数 offset 显式报错（此前
// ParseFloat+int64 截断把 1.9 静默折成 1——reset 打错目标分区位点）。
func TestParsePartitionOffsetsRejectsFractional(t *testing.T) {
	if _, err := parsePartitionOffsets(map[string]any{
		"t": map[string]any{"0": 1.9},
	}); err == nil {
		t.Fatal("fractional offset must be rejected")
	} else if !strings.Contains(err.Error(), "non-negative integer") {
		t.Fatalf("error must name the expected shape: %v", err)
	}
	offsets, err := parsePartitionOffsets(map[string]any{
		"t": map[string]any{"0": float64(5), "1": "7"},
	})
	if err != nil {
		t.Fatalf("integral offsets must parse: %v", err)
	}
	if offsets["t"][0] != 5 || offsets["t"][1] != 7 {
		t.Fatalf("integral offsets must round-trip: %v", offsets)
	}
}
