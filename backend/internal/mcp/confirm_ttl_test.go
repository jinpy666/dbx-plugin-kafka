package mcp

// confirm_ttl_test.go：持久化 confirmTtlSecs 的启动生效（评审 M-1）。

import (
	"path/filepath"
	"testing"
	"time"

	"io.dbx.kafka.plugin/internal/kafkaconn"
	"io.dbx.kafka.plugin/internal/store"
)

// S-CONFIRM-TTL：NewServer 必须把持久化的 confirmTtlSecs 应用到 confirm
// store——此前固定默认 60s，要等下一次 settings/set 才生效，settings/get
// 报告值与实际签发 TTL 不符。
func TestNewServerAppliesPersistedConfirmTTL(t *testing.T) {
	st, err := store.OpenAt(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatalf("OpenAt() error = %v", err)
	}
	if err := st.SaveJSON(settingsFileName, map[string]any{"confirmTtlSecs": 300}); err != nil {
		t.Fatalf("SaveJSON() error = %v", err)
	}
	srv := NewServer(kafkaconn.NewService(), st)
	now := time.Now()
	_, expires, err := srv.confirms.Issue("contract-hash", now)
	if err != nil {
		t.Fatalf("Issue() error = %v", err)
	}
	if ttl := expires.Sub(now); ttl < 295*time.Second || ttl > 305*time.Second {
		t.Fatalf("issued TTL = %v, want ~300s (persisted confirmTtlSecs)", ttl)
	}
}
