package mcp

// intent_test.go：UI intent 状态机（设计 §1；验收用例 S-INT-*，清单见
// shared/frontend/README.zh-CN.md）。

import (
	"testing"
	"time"
)

var intentBase = time.Date(2026, 9, 12, 10, 0, 0, 0, time.UTC)

func TestIntentReportAppliesSummary(t *testing.T) {
	store := NewIntentStore(0, 0)
	intent := store.Register("i-1", "search", map[string]any{"topic": "orders"}, intentBase)
	if intent.State != IntentPending {
		t.Fatalf("new intent must be pending: %v", intent.State)
	}
	if !store.Report("i-1", IntentApplied, map[string]any{"count": float64(3)}, "", intentBase.Add(time.Second)) {
		t.Fatal("report applied should succeed")
	}
	got, status := store.Get("i-1", intentBase.Add(2*time.Second))
	if status != LookupFound || got.State != IntentApplied || got.Summary["count"] != float64(3) {
		t.Fatalf("unexpected lookup: %+v %v", got, status)
	}
}

func TestIntentReportRejectedCarriesReason(t *testing.T) {
	store := NewIntentStore(0, 0)
	store.Register("i-2", "focus", map[string]any{"panel": "nope"}, intentBase)
	if !store.Report("i-2", IntentRejected, nil, "unknown panel", intentBase) {
		t.Fatal("report rejected should succeed")
	}
	got, _ := store.Get("i-2", intentBase)
	if got.State != IntentRejected || got.Reason != "unknown panel" {
		t.Fatalf("unexpected intent: %+v", got)
	}
	// 非法状态值（pending 回报等）一律拒绝。
	store.Register("i-3", "focus", nil, intentBase)
	if store.Report("i-3", "applied ", nil, "", intentBase) {
		t.Fatal("invalid state must be refused")
	}
}

func TestIntentTTLOnceExpiredIsUnknown(t *testing.T) {
	store := NewIntentStore(60*time.Second, 0)
	store.Register("i-4", "search", nil, intentBase)
	// TTL 内可查；过期后读取报 expired 且条目被清除（再读 unknown）。
	if _, status := store.Get("i-4", intentBase.Add(59*time.Second)); status != LookupFound {
		t.Fatalf("within TTL should be found: %v", status)
	}
	if _, status := store.Get("i-4", intentBase.Add(61*time.Second)); status != LookupExpired {
		t.Fatalf("past TTL should be expired: %v", status)
	}
	if _, status := store.Get("i-4", intentBase.Add(62*time.Second)); status != LookupUnknown {
		t.Fatalf("expired entry should be pruned: %v", status)
	}
	// 过期后 report 不复活。
	store.Register("i-5", "search", nil, intentBase)
	if store.Report("i-5", IntentApplied, nil, "", intentBase.Add(2*time.Minute)) {
		t.Fatal("late report must fail")
	}
}

func TestIntentLRUEvictsOldest(t *testing.T) {
	store := NewIntentStore(time.Hour, 3)
	for _, id := range []string{"a", "b", "c", "d"} {
		store.Register(id, "search", nil, intentBase)
	}
	if _, status := store.Get("a", intentBase); status != LookupUnknown {
		t.Fatalf("oldest should be evicted: %v", status)
	}
	for _, id := range []string{"b", "c", "d"} {
		if _, status := store.Get(id, intentBase); status != LookupFound {
			t.Fatalf("%s should survive: %v", id, status)
		}
	}
	// 同 id 重发 = 顶到最新（不会自我淘汰）。
	store.Register("b", "search", nil, intentBase)
	store.Register("e", "search", nil, intentBase)
	if _, status := store.Get("b", intentBase); status != LookupFound {
		t.Fatal("re-registered id must survive")
	}
}

func TestIntentSnapshotRoundtrip(t *testing.T) {
	store := NewIntentStore(0, 0)
	if store.Snapshot() != nil {
		t.Fatal("no snapshot initially")
	}
	snapshot := map[string]any{"panel": "messages", "count": float64(10)}
	store.SetSnapshot(snapshot)
	snapshot["mutated"] = true
	got := store.Snapshot()
	if got["panel"] != "messages" || got["mutated"] != nil {
		t.Fatalf("snapshot should be copied on write: %+v", got)
	}
	got["count"] = 999
	if store.Snapshot()["count"] != float64(10) {
		t.Fatal("snapshot should be copied on read")
	}
}

// S-INTENT-COPY（评审 H-1）：Get 返回值拷贝——此前指针逃逸使轮询方在锁外
// 读 State/Summary/Reason，与 Report 的持锁写构成数据竞争。
func TestIntentGetReturnsCopy(t *testing.T) {
	store := NewIntentStore(0, 0)
	store.Register("i-copy", "search", nil, intentBase)
	got, status := store.Get("i-copy", intentBase)
	if status != LookupFound {
		t.Fatalf("status = %v", status)
	}
	got.State = IntentRejected
	got.Reason = "tampered"
	again, _ := store.Get("i-copy", intentBase)
	if again.State != IntentPending || again.Reason != "" {
		t.Fatalf("Get must return a copy, store saw %+v", again)
	}
}

// S-INTENT-RACE：并发回报 × 轮询读同一条目（配合 go test -race 验证拷贝
// 语义消除了 Report 写 × Get 逃逸指针读的竞争窗口）。
func TestIntentConcurrentReportAndGet(t *testing.T) {
	store := NewIntentStore(0, 0)
	store.Register("i-race", "search", nil, intentBase)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			intent, status := store.Get("i-race", intentBase.Add(time.Second))
			if status != LookupFound {
				continue
			}
			_ = intent.Reason
			if intent.Summary != nil {
				_ = intent.Summary["n"]
			}
		}
	}()
	for i := 0; i < 200; i++ {
		state := IntentApplied
		reason := "ok"
		if i%2 == 1 {
			state = IntentRejected
			reason = "no"
		}
		store.Report("i-race", state, map[string]any{"n": float64(i)}, reason, intentBase.Add(time.Second))
	}
	close(stop)
	<-done
}
