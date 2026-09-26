package kafkaconn

// consume_pool_churn_test.go：消费池长期 churn 可靠性（可靠性纵深轮）：
// 不同 signature（topic/分区/方向/isolation）交替 200+ 轮后——池不串台
//（entry 与 key 的签名恒一致、异连接不共享）、不泄漏（release 后收敛在
// 上限内）、resetFailed 条目不入复用。全离线：池条目 client 为 nil
//（closeConsumePoolEntry 有 nil 检查）；client 侧行为由直连冒烟覆盖。

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// poolSignaturesChurn churn 用 signature 组：覆盖 topic/分区集/方向/
// isolation 四个签名维度。
func poolSignaturesChurn() []string {
	return []string{
		consumeClientSignature("t1", "earliest", nil, ""),
		consumeClientSignature("t1", "latest", nil, ""),
		consumeClientSignature("t2", "earliest", nil, ""),
		consumeClientSignature("t1", "earliest", []int32{0}, ""),
		consumeClientSignature("t1", "earliest", []int32{0, 1}, ""),
		consumeClientSignature("t1", "earliest", []int32{1, 0}, ""), // 分区列表按序参与签名（同请求形状必同序）
		consumeClientSignature("t1", "earliest", nil, "read_committed"),
	}
}

// S-POOL-CHURN-1 交替 churn：7 种 signature × 2 连接交替 acquire/put/release
// 200+ 轮——命中必同签名同连接（不串台），release 后池收敛在上限内。
func TestConsumePoolSignatureChurnNoCrossTalk(t *testing.T) {
	s := newPoolService()
	sigs := poolSignaturesChurn()
	// 同输入同签名（确定性）；异维度互异。
	if consumeClientSignature("t1", "earliest", nil, "") != sigs[0] {
		t.Fatal("signature must be deterministic")
	}
	for i, sigA := range sigs {
		for j, sigB := range sigs {
			if i != j && sigA == sigB {
				t.Fatalf("signature %d collides with %d", i, j)
			}
		}
	}

	released := map[string]*consumePoolEntry{} // key → 最近一次 release 的条目
	for round := 0; round < 30; round++ {
		connID := fmt.Sprintf("c%d", round%2)
		for _, sig := range sigs {
			key := consumePoolKey(connID, sig)
			entry, reusable := s.consumePoolAcquire(connID, sig, true)
			if reusable {
				if entry.sig != sig || entry.connID != connID {
					t.Fatalf("round %d: pool cross-talk: sig=%s conn=%s (want %s/%s)",
						round, entry.sig, entry.connID, sig, connID)
				}
				if released[key] != nil && entry != released[key] {
					t.Fatalf("round %d: same key must reuse the same entry", round)
				}
			} else {
				entry = s.consumePoolPut(connID, sig, nil)
			}
			// 占用中同 key 再 acquire 必 miss（排他）。
			if _, busy := s.consumePoolAcquire(connID, sig, true); busy {
				t.Fatalf("round %d: in-use entry must not be re-acquired", round)
			}
			s.consumePoolRelease(entry, nil, true)
			released[key] = entry
			// release 惰性扫描后：全空闲池必须收敛在上限内。
			if len(s.consumePool) > consumePoolMaxEntries {
				t.Fatalf("round %d: pool leaks: %d entries", round, len(s.consumePool))
			}
		}
	}
	// 终态校验：池内每个条目的 key 与签名/连接一致（不串台的静态面）。
	for key, entry := range s.consumePool {
		if key != consumePoolKey(entry.connID, entry.sig) {
			t.Fatalf("pool entry key mismatch: %q vs conn=%s sig=%s", key, entry.connID, entry.sig)
		}
	}
	if len(s.consumePool) > consumePoolMaxEntries {
		t.Fatalf("pool size %d exceeds limit", len(s.consumePool))
	}
}

// S-POOL-CHURN-2 异连接隔离：同 signature 不同 connID 永远是不同条目；
// Disconnect 挂钩（closeFor）只清目标连接。
func TestConsumePoolConnIDIsolationUnderChurn(t *testing.T) {
	s := newPoolService()
	sig := consumeClientSignature("t", "earliest", nil, "")
	a := s.consumePoolPut("conn-a", sig, nil)
	s.consumePoolRelease(a, nil, true)
	b := s.consumePoolPut("conn-b", sig, nil)
	s.consumePoolRelease(b, nil, true)

	if a == b {
		t.Fatal("same signature on different connections must not share entries")
	}
	if got := s.consumePool[consumePoolKey("conn-a", sig)]; got != a {
		t.Fatal("conn-a entry must stay")
	}
	s.consumePoolCloseFor("conn-a")
	if _, still := s.consumePool[consumePoolKey("conn-a", sig)]; still {
		t.Fatal("closeFor must drop conn-a")
	}
	if got := s.consumePool[consumePoolKey("conn-b", sig)]; got != b {
		t.Fatal("closeFor must keep conn-b")
	}
	s.consumePoolCloseAll()
	if len(s.consumePool) != 0 {
		t.Fatalf("closeAll must empty the pool: %d", len(s.consumePool))
	}
}

// S-POOL-CHURN-3 resetFailed 条目在 churn 中不再复用，且被同 key 新条目
// 替换后恢复可用（put 替换语义）。
func TestConsumePoolResetFailedReplacement(t *testing.T) {
	s := newPoolService()
	sig := consumeClientSignature("t", "latest", nil, "")
	bad := s.consumePoolPut("c", sig, nil)
	s.consumePoolRelease(bad, nil, false) // 不健康：标记 resetFailed
	if _, reusable := s.consumePoolAcquire("c", sig, false); reusable {
		t.Fatal("resetFailed entry must not be reused")
	}
	good := s.consumePoolPut("c", sig, nil) // 同 key 替换（替换条目为占用态）
	s.consumePoolRelease(good, nil, true)
	if good == bad || good.resetFailed {
		t.Fatal("replacement entry must be fresh and healthy")
	}
	if _, reusable := s.consumePoolAcquire("c", sig, false); !reusable {
		t.Fatal("replacement entry must be reusable after release")
	}
	s.consumePoolRelease(good, nil, true)
}

// S-POOL-RACE release-vs-acquire 并发压测（评审 H-2）：consumePoolRelease
// 此前在池锁外写 inUse/lastUsedAt，与本文件 :46 的不变式（inUse 由池锁保护）
// 冲突——并发同形状消费时 Acquire 的锁内读与 Release 的锁外写构成 data race。
// 本用例以多 goroutine 高频对同一条目 acquire/release 制造交错，配合
// `go test -race` 钉住锁纪律；终态断言条目回到空闲可复用。
func TestConsumePoolReleaseAcquireConcurrentRace(t *testing.T) {
	s := newPoolService()
	sig := consumeClientSignature("t", "earliest", nil, "")
	// Put 返回的条目处于占用态（所有权移交池）：先按正常持有期释放为空闲，
	// worker 才能进入 acquire/release 循环。
	if entry := s.consumePoolPut("c", sig, nil); entry == nil {
		t.Fatal("put must register the entry")
	} else {
		s.consumePoolRelease(entry, nil, true)
	}
	const workers = 8
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			observed := map[int32]struct{}{1: {}}
			for {
				select {
				case <-stop:
					return
				default:
				}
				entry, reusable := s.consumePoolAcquire("c", sig, true)
				if reusable {
					if !entry.inUse {
						t.Error("reusable entry must be marked in-use")
						return
					}
					s.consumePoolRelease(entry, observed, true)
				}
			}
		}()
	}
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	s.consumePoolMu.Lock()
	entry := s.consumePool[consumePoolKey("c", sig)]
	s.consumePoolMu.Unlock()
	if entry == nil {
		t.Fatal("entry must remain pooled after churn")
	}
	if entry.inUse {
		t.Fatal("entry must be idle after all workers stop")
	}
	if entry.resetFailed {
		t.Fatal("healthy churn must not mark resetFailed")
	}
}
