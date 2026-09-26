package mcp

// settings_test.go：mcp/settings 加载/校验/持久化（设计 §2 骨架 + §6.3
// kafka 域内扩展 digestScanLimit；验收用例 S-SET-*，清单见
// shared/frontend/README.zh-CN.md「MCP 两阶段/digest/cursor 验收用例清单」）。

import (
	"testing"

	"io.dbx.kafka.plugin/internal/store"
)

func TestSettingsDefaultsAndClamp(t *testing.T) {
	settings := DefaultSettings()
	if settings.ReportWaitMs != 5000 || settings.CellWidth != 120 || settings.ResponseLimitBytes != 16*1024 {
		t.Fatalf("unexpected defaults: %+v", settings)
	}
	if settings.DigestGroupLimit != 20 || settings.DigestTopN != 10 || settings.DigestSampleRows != 5 || settings.DigestRowLimit != 20 {
		t.Fatalf("digest defaults mismatch: %+v", settings)
	}
	if settings.DigestScanLimit != 1000 {
		t.Fatalf("kafka digestScanLimit default mismatch: %+v", settings)
	}
	// 越界值一律收敛回设计硬上限（损坏文件 / 手改文件不可放大限制）。
	clamped := Settings{
		ReportWaitMs:       999999,
		CellWidth:          0,
		DigestGroupLimit:   100,
		DigestTopN:         50,
		DigestSampleRows:   99,
		DigestRowLimit:     1000,
		DigestScanLimit:    1 << 30,
		ResponseLimitBytes: 1 << 30,
	}.Sanitized()
	if clamped.ReportWaitMs != 30000 || clamped.CellWidth != 1 ||
		clamped.DigestGroupLimit != 20 || clamped.DigestTopN != 10 ||
		clamped.DigestSampleRows != 5 || clamped.DigestRowLimit != 20 ||
		clamped.DigestScanLimit != 100000 ||
		clamped.ResponseLimitBytes != 1024*1024 {
		t.Fatalf("clamp mismatch: %+v", clamped)
	}
}

func TestSettingsLoadFallsBackOnMissingOrCorruptFile(t *testing.T) {
	dir := t.TempDir()
	st, err := store.OpenAt(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := LoadSettings(st); got != DefaultSettings() {
		t.Fatalf("missing file should fall back to defaults: %+v", got)
	}
	if err := st.SaveJSON(settingsFileName, map[string]any{"reportWaitMs": "bogus"}); err != nil {
		t.Fatal(err)
	}
	if got := LoadSettings(st); got.ReportWaitMs != 5000 {
		t.Fatalf("corrupt file should keep default reportWaitMs: %+v", got)
	}
}

func TestSettingsUpdateWhitelistAndClamp(t *testing.T) {
	settings := DefaultSettings()
	updated, err := applySettingsUpdate(settings, map[string]any{
		"reportWaitMs":    float64(300),
		"cellWidth":       float64(80),
		"digestScanLimit": float64(5000),
		"unknownField":    float64(1), // 白名单外字段静默忽略
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.ReportWaitMs != 300 || updated.CellWidth != 80 || updated.DigestScanLimit != 5000 {
		t.Fatalf("partial update mismatch: %+v", updated)
	}
	if _, err := applySettingsUpdate(updated, map[string]any{"reportWaitMs": float64(0)}); err == nil {
		t.Fatal("zero/negative must be rejected")
	}
	if _, err := applySettingsUpdate(updated, map[string]any{"digestGroupLimit": float64(21)}); err == nil {
		t.Fatal("above ceiling must be rejected")
	}
	if _, err := applySettingsUpdate(updated, map[string]any{"digestScanLimit": float64(100001)}); err == nil {
		t.Fatal("scan limit above ceiling must be rejected")
	}
	if _, err := applySettingsUpdate(updated, map[string]any{"responseLimitBytes": "big"}); err == nil {
		t.Fatal("non-numeric must be rejected")
	}
	// 拒绝不污染当前值：再次 set 合法值仍然成功。
	if _, err := applySettingsUpdate(updated, map[string]any{"digestRowLimit": float64(5)}); err != nil {
		t.Fatal(err)
	}
}

func TestSettingsPersistRoundtrip(t *testing.T) {
	dir := t.TempDir()
	st, err := store.OpenAt(dir)
	if err != nil {
		t.Fatal(err)
	}
	settings := DefaultSettings()
	settings.ReportWaitMs = 250
	// KAFKA-M2 回归：cursor 两个字段此前 Save 持久化但 Load 漏恢复，
	// 重启后静默回默认——往返必须全字段一致。
	settings.CursorTtlSecs = 1234
	settings.MaxCursorSessions = 16
	if err := SaveSettings(st, settings); err != nil {
		t.Fatal(err)
	}
	if got := LoadSettings(st); got.ReportWaitMs != 250 || got != settings {
		t.Fatalf("roundtrip mismatch: %+v", got)
	}
	// nil store（数据目录不可用降级）：Save 静默、Load 回默认。
	if err := SaveSettings(nil, settings); err != nil {
		t.Fatalf("nil store save should be a no-op: %v", err)
	}
	if got := LoadSettings(nil); got != DefaultSettings() {
		t.Fatalf("nil store load should return defaults: %+v", got)
	}
	// 损坏行回落：持久化文件缺 cursor 字段时保默认（逐项 >0 模式）。
	if err := st.SaveJSON(settingsFileName, map[string]any{"reportWaitMs": float64(300)}); err != nil {
		t.Fatal(err)
	}
	got := LoadSettings(st)
	if got.ReportWaitMs != 300 || got.CursorTtlSecs != DefaultSettings().CursorTtlSecs || got.MaxCursorSessions != DefaultSettings().MaxCursorSessions {
		t.Fatalf("partial file should fall back per-field: %+v", got)
	}
}
