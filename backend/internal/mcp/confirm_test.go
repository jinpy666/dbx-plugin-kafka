package mcp

// confirm_test.go：两阶段写确认令牌（设计 §4；验收用例 S-CONF-*，清单见
// shared/frontend/README.zh-CN.md）。

import (
	"testing"
	"time"
)

func TestConfirmHashStableAndDistinct(t *testing.T) {
	first := HashParams([]byte(`{"connectionId":"c1","topic":"orders"}`))
	second := HashParams([]byte(`{"connectionId":"c1","topic":"orders"}`))
	changed := HashParams([]byte(`{"connectionId":"c1","topic":"other"}`))
	if first == "" || first != second {
		t.Fatal("same canonical params must hash identically")
	}
	if first == changed {
		t.Fatal("changed params must change the hash")
	}
}

func TestConfirmOneTimeConsumption(t *testing.T) {
	store := NewConfirmStore()
	now := intentBase
	token, expiresAt, _ := store.Issue(HashParams([]byte(`{"a":1}`)), now)
	if !expiresAt.After(now) || expiresAt.Sub(now) != ConfirmTTL {
		t.Fatalf("unexpected TTL: %v", expiresAt.Sub(now))
	}
	if got := store.Consume(token, HashParams([]byte(`{"a":1}`)), now.Add(time.Second)); got != ConfirmOK {
		t.Fatalf("first consume should be ok: %v", got)
	}
	// 一次性：第二次消费即 unknown。
	if got := store.Consume(token, HashParams([]byte(`{"a":1}`)), now.Add(2*time.Second)); got != ConfirmUnknown {
		t.Fatalf("token must be single-use: %v", got)
	}
}

func TestConfirmExpiryAndHashMismatch(t *testing.T) {
	store := NewConfirmStore()
	now := intentBase
	expired, _, _ := store.Issue(HashParams([]byte(`{"a":1}`)), now)
	if got := store.Consume(expired, HashParams([]byte(`{"a":1}`)), now.Add(61*time.Second)); got != ConfirmExpired {
		t.Fatalf("past 60s TTL must be expired: %v", got)
	}
	token, _, _ := store.Issue(HashParams([]byte(`{"a":1}`)), now)
	if got := store.Consume(token, HashParams([]byte(`{"a":2}`)), now.Add(time.Second)); got != ConfirmHashMismatch {
		t.Fatalf("changed params must invalidate: %v", got)
	}
	// hash 失配同样作废令牌（防止用同一令牌再试改参）。
	if got := store.Consume(token, HashParams([]byte(`{"a":1}`)), now.Add(2*time.Second)); got != ConfirmUnknown {
		t.Fatalf("mismatched token must be burned: %v", got)
	}
	if got := store.Consume("c-nonexistent", "", now); got != ConfirmUnknown {
		t.Fatalf("unknown token: %v", got)
	}
}
