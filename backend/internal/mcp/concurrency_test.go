package mcp

// concurrency_test.go：三张表（Confirm/Cursor/Intent）的真并发压力（第七轮，
// S-CONC-*，ldap Go 版同表）：前六轮 churn 全部顺序执行，mutex 并发面从未被
// 竞争检测器验证。这里多 goroutine 并发 churn 同一张表（issue/consume/expire
// 混并发、物化/翻页/淘汰混并发、登记/回报/过期混并发），断言终态一致：无丢
// 更新、无双重消费、容量收敛。配合 `go test -race` 运行；全部离线（store 纯
// 逻辑 + 注入时钟）。

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// concClock 并发安全的注入时钟：每次 Now 原子推进 step（单调，供 prune/expire
// 驱动）。
type concClock struct {
	ms   int64
	step int64
}

func newConcClock(stepMs int64) *concClock { return &concClock{step: stepMs} }

func (c *concClock) Now() time.Time {
	return intentBase.Add(time.Duration(atomic.AddInt64(&c.ms, c.step)) * time.Millisecond)
}

// concErrs 并发安全的错误收集器（子 goroutine 内失败不中断，收尾统一断言）。
type concErrs struct {
	mu   sync.Mutex
	list []string
}

func (e *concErrs) add(message string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.list = append(e.list, message)
}

func (e *concErrs) fail(t *testing.T) {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, message := range e.list {
		t.Error(message)
	}
}

// S-CONC-CONF-1 ConfirmStore 并发压力：16 goroutine × 40 轮混合
// issue→即时消费（必须 OK）/ 4 方竞争消费同一令牌（恰好一方 OK，其余
// unknown——一次性语义在并发下不得双重消费）/ 远期消费（expired）/ 弃单。
// 阶段 2：短 TTL 弃单洪泛，验证 Issue 的 prune 在并发下把表收敛到窗口内。
func TestConfirmStoreConcurrentStress(t *testing.T) {
	store := NewConfirmStore()
	clock := newConcClock(1) // 1ms/次：全程假时 << 60s TTL，新令牌窗口内必 OK
	hash := HashParams([]byte(`{"action":"delete","topics":["t"]}`))

	const workers = 16
	const rounds = 40
	var wg sync.WaitGroup
	errs := &concErrs{}
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for round := 0; round < rounds; round++ {
				switch round % 4 {
				case 0: // 即时消费：新令牌必须一次 OK。
					token, _, _ := store.Issue(hash, clock.Now())
					if got := store.Consume(token, hash, clock.Now()); got != ConfirmOK {
						errs.add("fresh token must consume ok: got " + string(got))
					}
				case 1: // 4 方竞争消费同一令牌：恰好一方 OK（一次性语义）。
					token, _, _ := store.Issue(hash, clock.Now())
					var race sync.WaitGroup
					var oks atomic.Int32
					for racer := 0; racer < 4; racer++ {
						race.Add(1)
						go func() {
							defer race.Done()
							if store.Consume(token, hash, clock.Now()) == ConfirmOK {
								oks.Add(1)
							}
						}()
					}
					race.Wait()
					if n := oks.Load(); n != 1 {
						errs.add("worker " + strconv.Itoa(worker) + " round " + strconv.Itoa(round) +
							": raced token consumed " + strconv.Itoa(int(n)) + " times (want exactly 1)")
					}
				case 2: // 远期消费：expired 且删除。
					token, _, _ := store.Issue(hash, clock.Now())
					if got := store.Consume(token, hash, intentBase.Add(2*ConfirmTTL)); got != ConfirmExpired {
						errs.add("stale token must expire: got " + string(got))
					}
				default: // 弃单：签发后从不消费，靠 Issue 的 prune 收敛。
					_, _, _ = store.Issue(hash, clock.Now())
				}
			}
		}(worker)
	}
	wg.Wait()

	// 阶段 2：短 TTL（10s）+ 4 goroutine 弃单洪泛；收尾跨 TTL 签发一条触发
	// prune——并发下全部过期令牌必须被清掉（收敛为 1）。
	store.SetTTL(10 * time.Second)
	var flood sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		flood.Add(1)
		go func() {
			defer flood.Done()
			for round := 0; round < 75; round++ {
				_, _, _ = store.Issue(hash, clock.Now())
			}
		}()
	}
	flood.Wait()
	finalToken, _, _ := store.Issue(hash, intentBase.Add(10*time.Minute)) // 触发 prune
	if len(store.items) != 1 {
		errs.add("prune must clear all expired tokens under concurrency, got " + strconv.Itoa(len(store.items)))
	}
	if got := store.Consume(finalToken, hash, intentBase.Add(10*time.Minute)); got != ConfirmOK {
		errs.add("surviving token must consume ok: got " + string(got))
	}
	errs.fail(t)
}

// S-CONC-CUR-1 CursorStore 并发压力：物化/翻页/淘汰混并发——24 goroutine 各
// 物化 8 个会话（16 行确定性内容），每会话 4 个读者按固定窗口并发翻页：
// 命中必须逐行精确（Put 复制行、Next 只读切片，并发下不得串行/丢行/串行
// 错位），被淘汰报 unknown 可接受；终态 sessions/order 一致且收敛在容量内。
func TestCursorStoreConcurrentStress(t *testing.T) {
	const capacity = 4
	store := NewCursorStore(time.Hour, capacity, 64)
	clock := newConcClock(1)

	const workers = 24
	const putsPerWorker = 8
	const rowsPerSession = 16
	var wg sync.WaitGroup
	errs := &concErrs{}
	var putSeq atomic.Int64
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := 0; round < putsPerWorker; round++ {
				seq := int(putSeq.Add(1))
				rows := make([]CursorRow, rowsPerSession)
				for i := range rows {
					rows[i] = kafkaRow("stress", int64(seq*1000+i))
				}
				session := store.Put(rows, "stress", clock.Now())
				// 同会话 4 读者固定窗口并发翻页：命中内容必须精确。
				var readers sync.WaitGroup
				for part := 0; part < 4; part++ {
					readers.Add(1)
					go func(part int) {
						defer readers.Done()
						offset := part * (rowsPerSession / 4)
						result, status := store.Next(session.ID, NextRequest{N: rowsPerSession / 4, Offset: offset}, clock.Now())
						switch status {
						case LookupFound:
							if len(result.Rows) != rowsPerSession/4 {
								errs.add("seq " + strconv.Itoa(seq) + " part " + strconv.Itoa(part) + ": short page")
								return
							}
							for i, got := range result.Rows {
								want := int64(seq*1000 + offset + i)
								if got.Topic != "stress" || got.Offset != want {
									errs.add("seq " + strconv.Itoa(seq) + " part " + strconv.Itoa(part) +
										" row " + strconv.Itoa(i) + ": corrupted row " + got.Anchor() +
										" (want offset " + strconv.FormatInt(want, 10) + ")")
								}
							}
						case LookupUnknown: // 容量淘汰，并发下合法。
						default:
							errs.add("seq " + strconv.Itoa(seq) + ": unexpected status " + string(status))
						}
					}(part)
				}
				readers.Wait()
			}
		}()
	}
	wg.Wait()
	if n := putSeq.Load(); n != workers*putsPerWorker {
		t.Fatalf("lost puts: %d", n)
	}
	if len(store.sessions) != len(store.order) || len(store.sessions) != capacity {
		t.Fatalf("store must converge to capacity %d: sessions=%d order=%d", capacity, len(store.sessions), len(store.order))
	}
	errs.fail(t)
}

// S-CONC-INT-1 IntentStore 并发压力：登记/回报/过期/快照混并发——12
// goroutine 各登记 50 条（id 唯一）并把每条投递两次给回报池（双回报竞争），
// 期间快照写入方持续覆盖。断言：回报成功且仍存活的 intent 终态必不是
// pending（无丢更新回落）、表收敛在容量内、快照读取与 churn 互不污染。
func TestIntentStoreConcurrentStress(t *testing.T) {
	store := NewIntentStore(60*time.Second, 32)
	clock := newConcClock(5) // 5ms/次：全程假时 < 60s，存活条目不因 TTL 误过期

	const workers = 12
	const perWorker = 50
	jobs := make(chan string, workers*perWorker*2)
	var registerWG, reportWG sync.WaitGroup
	var seq atomic.Int64
	errs := &concErrs{}

	for worker := 0; worker < workers; worker++ {
		registerWG.Add(1)
		go func() {
			defer registerWG.Done()
			for round := 0; round < perWorker; round++ {
				id := "i-" + strconv.FormatInt(seq.Add(1), 10)
				store.Register(id, "search", map[string]any{"n": round}, clock.Now())
				jobs <- id // 双回报：两个消费者并发竞争同一条
				jobs <- id
			}
		}()
	}
	for reporter := 0; reporter < 6; reporter++ {
		reportWG.Add(1)
		go func() {
			defer reportWG.Done()
			for id := range jobs {
				store.Report(id, IntentApplied, map[string]any{"ok": true}, "", clock.Now())
			}
		}()
	}
	// 快照写入方：与登记/回报并发覆盖，验证快照隔离（读侧永远拿到完整形状）。
	snapshotStop := make(chan struct{})
	var snapshotWG sync.WaitGroup
	snapshotWG.Add(1)
	go func() {
		defer snapshotWG.Done()
		for {
			select {
			case <-snapshotStop:
				return
			default:
				store.SetSnapshot(map[string]any{"panel": "messages", "count": 42})
			}
		}
	}()

	registerWG.Wait()
	close(jobs)
	reportWG.Wait()
	close(snapshotStop)
	snapshotWG.Wait()

	// 终态一致性：容量收敛 + 双表一致；仍存活的条目终态必不是 pending
	//（登记后经双回报竞争，未存活即已被淘汰，不参与断言）。
	if len(store.items) != len(store.order) || len(store.items) > 32 {
		t.Fatalf("intent table must converge to capacity: items=%d order=%d", len(store.items), len(store.order))
	}
	for _, id := range store.order {
		intent, status := store.Get(id, clock.Now())
		if status == LookupFound && intent.State == IntentPending {
			errs.add("intent " + id + " still pending after concurrent reports (lost update)")
		}
	}
	// 快照隔离：并发 churn 后读取必须得到完整写形状。
	if snapshot := store.Snapshot(); snapshot == nil || snapshot["panel"] != "messages" || snapshot["count"] != 42 {
		errs.add("snapshot corrupted by concurrent churn")
	}
	errs.fail(t)
	// 过期驱动 prune：跨 TTL 后登记一条，表收敛为 1。
	store.Register("i-final", "search", nil, intentBase.Add(time.Hour))
	if len(store.items) != 1 {
		t.Fatalf("prune must clear expired intents under concurrency, got %d", len(store.items))
	}
}
