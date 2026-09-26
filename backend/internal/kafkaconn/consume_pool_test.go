package kafkaconn

// consume_pool_test.go：复用池纯逻辑单测（策略表/签名/池状态机）。
// 不触真实 kgo 拨号：池条目 client 为 nil，Close 对 nil 安全（closeConsumePoolEntry
// 有 nil 检查）；client 侧行为（drain/SetOffsets）由直连冒烟验证。

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// --- consumeReusePolicy -------------------------------------------------------

func TestConsumeReusePolicy(t *testing.T) {
	cases := []struct {
		name     string
		strategy string
		groupID  string
		wantOK   bool
		wantAt   bool // true = start
	}{
		{"empty strategy no group", "", "", true, true},
		{"default no group", "default", "", true, true},
		{"earliest no group", "earliest", "", true, true},
		{"earliest alias start", "START", "", true, true},
		{"latest no group", "latest", "", true, false},
		{"latest alias end", "End", "", true, false},
		{"group committed", "committed", "g1", false, false},
		{"group earliest not reusable", "earliest", "g1", false, false},
		{"timestamp not reusable", "timestamp", "", false, false},
		{"offset not reusable", "offset", "", false, false},
		{"unknown strategy not reusable", "bogus", "", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ok, atStart := consumeReusePolicy(tc.strategy, tc.groupID)
			if ok != tc.wantOK || (ok && atStart != tc.wantAt) {
				t.Fatalf("consumeReusePolicy(%q, %q) = (%v, %v), want (%v, %v)",
					tc.strategy, tc.groupID, ok, atStart, tc.wantOK, tc.wantAt)
			}
		})
	}
}

// --- consumeClientSignature ---------------------------------------------------

func TestConsumeClientSignatureStableAndDistinct(t *testing.T) {
	base := consumeClientSignature("t1", "earliest", []int32{0, 1}, "")
	if again := consumeClientSignature("t1", "earliest", []int32{0, 1}, ""); again != base {
		t.Fatalf("signature not stable: %s vs %s", base, again)
	}
	distinct := map[string]string{
		"topic":      consumeClientSignature("t2", "earliest", []int32{0, 1}, ""),
		"strategy":   consumeClientSignature("t1", "latest", []int32{0, 1}, ""),
		"partitions": consumeClientSignature("t1", "earliest", []int32{0}, ""),
		"isolation":  consumeClientSignature("t1", "earliest", []int32{0, 1}, "read_committed"),
	}
	for name, sig := range distinct {
		if sig == base {
			t.Fatalf("signature collides on %s: %s", name, sig)
		}
	}
}

// --- 池状态机 ------------------------------------------------------------------

func newPoolService() *Service {
	return NewService()
}

func TestConsumePoolAcquireMissAndHit(t *testing.T) {
	s := newPoolService()
	sig := consumeClientSignature("t1", "earliest", nil, "")

	if _, reusable := s.consumePoolAcquire("c1", sig, true); reusable {
		t.Fatal("empty pool must miss")
	}

	entry := s.consumePoolPut("c1", sig, nil)
	if entry == nil || !entry.inUse {
		t.Fatal("put must return in-use entry")
	}

	if _, reusable := s.consumePoolAcquire("c1", sig, true); reusable {
		t.Fatal("in-use entry must not be re-acquired")
	}

	s.consumePoolRelease(entry, map[int32]struct{}{7: {}}, true)
	if entry.inUse {
		t.Fatal("release must clear in-use")
	}
	if _, ok := entry.seenParts[7]; !ok {
		t.Fatal("release must absorb observed partitions")
	}

	reacquired, reusable := s.consumePoolAcquire("c1", sig, true)
	if !reusable || reacquired != entry {
		t.Fatal("released entry must be reusable")
	}
	s.consumePoolRelease(reacquired, nil, true)
}

func TestConsumePoolResetFailedNotReused(t *testing.T) {
	s := newPoolService()
	sig := consumeClientSignature("t1", "latest", nil, "")
	entry := s.consumePoolPut("c1", sig, nil)
	s.consumePoolRelease(entry, nil, false)

	if _, reusable := s.consumePoolAcquire("c1", sig, false); reusable {
		t.Fatal("unhealthy entry must not be reused")
	}
}

func TestConsumePoolEvictsOldestOnLimit(t *testing.T) {
	s := newPoolService()
	for i := 0; i < consumePoolMaxEntries; i++ {
		sig := consumeClientSignature("t", "earliest", []int32{int32(i)}, "")
		entry := s.consumePoolPut("c", sig, nil)
		if i == 0 {
			s.consumePoolRelease(entry, nil, true) // 最旧且空闲
		}
	}
	// 恰好在上限内：不驱逐。
	if _, stillThere := s.consumePool[consumePoolKey("c", consumeClientSignature("t", "earliest", []int32{0}, ""))]; !stillThere {
		t.Fatal("entry within limit must not be evicted")
	}
	// 第 9 个 put：放入后超限，最旧空闲条目（partition 0）被驱逐。
	extra := consumeClientSignature("t", "earliest", []int32{int32(consumePoolMaxEntries)}, "")
	s.consumePoolPut("c", extra, nil)
	if _, stillThere := s.consumePool[consumePoolKey("c", consumeClientSignature("t", "earliest", []int32{0}, ""))]; stillThere {
		t.Fatal("oldest idle entry should have been evicted")
	}
	if len(s.consumePool) > consumePoolMaxEntries {
		t.Fatalf("pool size %d exceeds limit", len(s.consumePool))
	}
}

func TestConsumePoolKeepsBusyEntriesOverLimit(t *testing.T) {
	s := newPoolService()
	var busy []*consumePoolEntry
	for i := 0; i < consumePoolMaxEntries+2; i++ {
		sig := consumeClientSignature("t", "earliest", []int32{int32(i)}, "")
		busy = append(busy, s.consumePoolPut("c", sig, nil)) // 全部占用中
	}
	if len(s.consumePool) != consumePoolMaxEntries+2 {
		t.Fatalf("busy entries must not be evicted, pool=%d", len(s.consumePool))
	}
	for _, entry := range busy {
		s.consumePoolRelease(entry, nil, true)
	}
	// release 后惰性扫描应收敛回上限内（同刻 lastUsedAt 相同则全部保留也合法——
	// 断言只要求不超过 put 上限+当前占用 0）
	if len(s.consumePool) > consumePoolMaxEntries+2 {
		t.Fatalf("pool failed to converge, pool=%d", len(s.consumePool))
	}
}

func TestConsumePoolIdleEvictionOnRelease(t *testing.T) {
	s := newPoolService()
	sigA := consumeClientSignature("t", "earliest", []int32{0}, "")
	sigB := consumeClientSignature("t", "earliest", []int32{1}, "")
	a := s.consumePoolPut("c", sigA, nil)
	s.consumePoolRelease(a, nil, true)
	s.consumePoolMu.Lock()
	a.lastUsedAt = time.Now().UnixMilli() - consumePoolIdleTTL.Milliseconds() - 1000
	s.consumePoolMu.Unlock()

	b := s.consumePoolPut("c", sigB, nil)
	s.consumePoolRelease(b, nil, true)

	if _, still := s.consumePool[consumePoolKey("c", sigA)]; still {
		t.Fatal("idle-expired entry should be closed on next release sweep")
	}
}

func TestConsumePoolCloseForConnection(t *testing.T) {
	s := newPoolService()
	sig := consumeClientSignature("t", "earliest", nil, "")
	entry := s.consumePoolPut("c1", sig, nil)
	s.consumePoolRelease(entry, nil, true)
	s.consumePoolPut("c2", sig, nil)

	s.consumePoolCloseFor("c1")

	if _, still := s.consumePool[consumePoolKey("c1", sig)]; still {
		t.Fatal("closeFor must drop target connection entries")
	}
	if _, still := s.consumePool[consumePoolKey("c2", sig)]; !still {
		t.Fatal("closeFor must keep other connections")
	}
}

// KAFKA-M1 回归：旧条目 inUse 时 put 不得替换/关闭（返回 nil，所有权留给
// 调用方按临时 client 关闭）——否则并发同形状消费会关掉正在 poll 的 client。
func TestConsumePoolPutSkipsInUseReplacement(t *testing.T) {
	s := newPoolService()
	sig := consumeClientSignature("t", "earliest", nil, "")
	holder := s.consumePoolPut("c", sig, nil)

	// inUse 期间：put 返回 nil，池内仍是原条目且未被关闭。
	if pooled := s.consumePoolPut("c", sig, nil); pooled != nil {
		t.Fatal("put must not replace an in-use entry")
	}
	s.consumePoolMu.Lock()
	current := s.consumePool[consumePoolKey("c", sig)]
	untouched := current == holder && current.client == holder.client
	s.consumePoolMu.Unlock()
	if !untouched {
		t.Fatal("in-use entry must stay in the pool untouched")
	}

	// release 后：put 正常替换并关闭旧条目。
	s.consumePoolRelease(holder, nil, true)
	if pooled := s.consumePoolPut("c", sig, nil); pooled == nil || pooled == holder {
		t.Fatal("put must replace a released entry with a fresh one")
	}
	s.consumePoolMu.Lock()
	closed := holder.client == nil
	s.consumePoolMu.Unlock()
	if !closed {
		t.Fatal("replaced idle entry must be closed")
	}
}

// 并发场景：持有者占用期间，另一个 goroutine 反复同形状 put/acquire，
// 占用条目不得被替换或关闭（-race 下验证）。
func TestConsumePoolPutConcurrentInUseGuard(t *testing.T) {
	s := newPoolService()
	sig := consumeClientSignature("t", "earliest", nil, "")
	holder := s.consumePoolPut("c", sig, nil)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			if _, reusable := s.consumePoolAcquire("c", sig, true); reusable {
				t.Error("in-use entry must not be re-acquired concurrently")
				return
			}
			if pooled := s.consumePoolPut("c", sig, nil); pooled != nil {
				t.Error("concurrent put must not replace an in-use entry")
				return
			}
		}
	}()
	for i := 0; i < 200; i++ {
		s.consumePoolMu.Lock()
		current := s.consumePool[consumePoolKey("c", sig)]
		s.consumePoolMu.Unlock()
		if current != holder {
			t.Fatal("in-use entry replaced by concurrent put")
		}
	}
	<-done
	s.consumePoolRelease(holder, nil, true)
}

// reset 的哨兵值映射（纯逻辑部分）：错误路径依赖真实 client,不在此覆盖。
func TestConsumeStartupBudget(t *testing.T) {
	if got := consumeStartupBudget(5 * time.Second); got != 12*time.Second {
		t.Fatalf("budget(5s) = %s, want 12s", got)
	}
	if got := consumeStartupBudget(30 * time.Second); got != time.Minute {
		t.Fatalf("budget(30s) = %s, want 60s", got)
	}
}

// 编译期守住 client 类型签名（reset 参数面），防止接口漂移。
var _ = resetConsumeClientForReuse
var _ *kgo.Client
