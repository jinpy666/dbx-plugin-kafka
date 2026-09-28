package kafkaconn

// export_test.go：JSON/CSV 导出序列化（纯序列化不连网）。

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func exportSample() []ConsumedMessage {
	return []ConsumedMessage{
		{
			Topic: "orders", Partition: 1, Offset: 42, Timestamp: 1700000000000,
			Key: "k-1", ValueText: `{"a":1}`, ValueBase64: "eyJhIjoxfQ==",
			Headers: map[string]string{"trace": "t-1"},
		},
		{
			Topic: "orders", Partition: 0, Offset: 7, Timestamp: 1700000000001,
			Key: "k-2", ValueText: "plain, with comma", ValueBase64: "cGxhaW4sIHdpdGggY29tbWE=",
		},
	}
}

func TestExportJSON(t *testing.T) {
	content, contentType, err := serializeConsumedMessages("json", exportSample())
	if err != nil {
		t.Fatalf("serialize json error = %v", err)
	}
	if contentType != "application/json; charset=utf-8" {
		t.Errorf("contentType = %q", contentType)
	}
	var parsed []map[string]any
	if err := json.Unmarshal([]byte(content), &parsed); err != nil {
		t.Fatalf("parse exported json: %v", err)
	}
	if len(parsed) != 2 {
		t.Fatalf("exported rows = %d", len(parsed))
	}
	// 二进制保真：valueBase64 原样进 JSON。
	if parsed[0]["valueBase64"] != "eyJhIjoxfQ==" {
		t.Errorf("valueBase64 = %v", parsed[0]["valueBase64"])
	}
	if parsed[0]["valueText"] != `{"a":1}` {
		t.Errorf("valueText = %v", parsed[0]["valueText"])
	}
}

func TestExportCSV(t *testing.T) {
	content, contentType, err := serializeConsumedMessages("csv", exportSample())
	if err != nil {
		t.Fatalf("serialize csv error = %v", err)
	}
	if contentType != "text/csv; charset=utf-8" {
		t.Errorf("contentType = %q", contentType)
	}
	lines := strings.Split(strings.TrimRight(content, "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("csv lines = %d, want header+2", len(lines))
	}
	if !strings.HasPrefix(lines[0], "topic,partition,offset,timestamp,key,value,headers") {
		t.Errorf("csv header = %q", lines[0])
	}
	// 含逗号的 value 必须被 CSV 引号转义。
	if !strings.Contains(lines[2], `"plain, with comma"`) {
		t.Errorf("csv quoting broken: %q", lines[2])
	}
	// headers 列序列化。
	if !strings.Contains(lines[1], "trace=t-1") {
		t.Errorf("headers column missing: %q", lines[1])
	}
}

func TestExportFormatAndLimit(t *testing.T) {
	if _, err := normalizeExportFormat("yaml"); err == nil {
		t.Error("bogus format expected error")
	}
	if got, _ := normalizeExportFormat("CSV"); got != "csv" {
		t.Errorf("normalize CSV = %q", got)
	}
	if got, _ := exportRecordLimit(0); got != 1000 {
		t.Errorf("default limit = %d, want 1000", got)
	}
	if got, _ := exportRecordLimit(500); got != 500 {
		t.Errorf("explicit limit = %d", got)
	}
	if got, _ := exportRecordLimit(99999); got != maxExportRecords {
		t.Errorf("clamped limit = %d, want %d", got, maxExportRecords)
	}
	if _, err := exportRecordLimit(-1); err == nil {
		t.Error("negative limit expected error")
	}
}

func TestExportFilename(t *testing.T) {
	at := time.UnixMilli(1700000000000).UTC()
	name := exportFilename("order-events", "json", at)
	if name != "order-events-20231114T221320Z.json" {
		t.Errorf("filename = %q", name)
	}
	// 特殊字符安全化。
	name = exportFilename("../../etc/passwd", "csv", at)
	if strings.Contains(name, "/") || strings.Contains(name, "..") {
		t.Errorf("unsafe filename = %q", name)
	}
	// 空名兜底。
	name = exportFilename("///", "json", at)
	if !strings.HasPrefix(name, "kafka-export-") {
		t.Errorf("fallback filename = %q", name)
	}
}

// S-CSV-FORMULA（评审 LOW-5）：CSV 导出对公式头字符中和。
func TestSerializeCSVFormulaNeutralized(t *testing.T) {
	content, _, err := serializeConsumedMessages("csv", []ConsumedMessage{
		{Topic: "t", Partition: 0, Offset: 1, Key: "-1", ValueText: "=cmd|' /C calc'!A0"},
		{Topic: "t", Partition: 0, Offset: 2, Key: "safe", ValueText: "plain"},
	})
	if err != nil {
		t.Fatalf("serialize error = %v", err)
	}
	if !strings.Contains(content, "'-1") || !strings.Contains(content, "'=cmd") {
		t.Fatalf("formula cells not neutralized: %s", content)
	}
	if strings.Contains(content, "'safe") || strings.Contains(content, "'plain") {
		t.Fatalf("safe cells must stay untouched: %s", content)
	}
}
