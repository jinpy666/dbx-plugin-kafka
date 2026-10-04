package kafkaconn

// ringbuffer_test.go：流式 ring buffer 覆盖最旧、分页快照与空闲回收（§5.5）。

import (
	"fmt"
	"testing"
	"time"
)

func TestRingBufferOverwriteOldest(t *testing.T) {
	rb := newRingBuffer(5)
	for i := 0; i < 8; i++ {
		rb.append(ConsumedMessage{Offset: int64(i), ValueText: fmt.Sprintf("m%d", i)})
	}
	if rb.Len() != 5 || rb.Cap() != 5 {
		t.Fatalf("len/cap = %d/%d, want 5/5", rb.Len(), rb.Cap())
	}
	page := rb.Page(0, 5)
	if len(page) != 5 {
		t.Fatalf("page len = %d", len(page))
	}
	// 覆盖最旧：首条应为 offset 3。
	if page[0].Offset != 3 || page[4].Offset != 7 {
		t.Errorf("oldest = %d, newest = %d, want 3/7", page[0].Offset, page[4].Offset)
	}
}

func TestRingBufferPage(t *testing.T) {
	rb := newRingBuffer(10)
	for i := 0; i < 4; i++ {
		rb.append(ConsumedMessage{Offset: int64(i)})
	}
	if got := rb.Page(1, 2); len(got) != 2 || got[0].Offset != 1 || got[1].Offset != 2 {
		t.Errorf("page(1,2) offsets = %d/%d", got[0].Offset, got[1].Offset)
	}
	// offset 越界 → nil。
	if got := rb.Page(10, 2); got != nil {
		t.Errorf("out-of-range page = %v", got)
	}
	// 负 offset 截 0。
	if got := rb.Page(-5, 2); len(got) != 2 || got[0].Offset != 0 {
		t.Errorf("negative offset page = %v", got)
	}
	// limit 超尾截断。
	if got := rb.Page(2, 100); len(got) != 2 || got[1].Offset != 3 {
		t.Errorf("tail page = %v", got)
	}
	// 拷贝语义：修改副本不影响原数据。
	page := rb.Page(0, 4)
	page[0].ValueText = "mutated"
	if rb.Page(0, 1)[0].ValueText == "mutated" {
		t.Error("Page must return copies")
	}
}

func TestRingBufferWrapAroundPage(t *testing.T) {
	rb := newRingBuffer(4)
	for i := 0; i < 6; i++ {
		rb.append(ConsumedMessage{Offset: int64(i)})
	}
	// 覆盖后 base = (head - size + cap) % cap，分页必须跨环绕边界连续。
	page := rb.Page(0, 4)
	for i := 0; i < 4; i++ {
		if page[i].Offset != int64(i+2) {
			t.Fatalf("wrap page[%d].Offset = %d, want %d", i, page[i].Offset, i+2)
		}
	}
}

func TestRegistryEvictIdle(t *testing.T) {
	registry := NewStreamRegistry()
	now := int64(1_000_000_000)
	session := &streamSession{
		sessionID:           "s1",
		connectionID:        "c1",
		closeClient:         func() {},
		ring:                newRingBuffer(4),
		partitionOffsets:    map[int32]int64{},
		lastActivityUnixMs:  now,
		lastAttentionUnixMs: now, // 回收以客户端关注为唯一信号（架构评审 H-1）
	}
	registry.mu.Lock()
	registry.sessions["s1"] = session
	registry.mu.Unlock()

	// 新鲜会话不回收。
	if got := registry.EvictIdle(now); len(got) != 0 {
		t.Errorf("fresh session evicted: %v", got)
	}
	// 超时回收（> 30min）。
	evicted := registry.EvictIdle(now + (31 * 60 * 1000))
	if len(evicted) != 1 || evicted[0] != "s1" {
		t.Fatalf("evicted = %v, want [s1]", evicted)
	}
	if registry.lookup("s1") != nil {
		t.Error("session should be removed from registry")
	}
}

// TestRegistryEvictLoop 覆盖空闲回收调度端（此前 EvictIdle 无生产调度方，
// §5.5「空闲 30 分钟回收」只在测试里生效）：注入 tick 驱动回收，Close 后循环
// 必须退出。
func TestRegistryEvictLoop(t *testing.T) {
	registry := &StreamRegistry{sessions: map[string]*streamSession{}, evictStop: make(chan struct{})}
	tick := make(chan time.Time)
	done := make(chan struct{})
	go func() {
		registry.evictLoop(tick)
		close(done)
	}()

	session := &streamSession{
		sessionID:           "idle-loop-1",
		closeClient:         func() {},
		lastActivityUnixMs:  time.Now().UnixMilli() - (31 * 60 * 1000), // 已空闲 >30min
		lastAttentionUnixMs: time.Now().UnixMilli() - (31 * 60 * 1000), // 无人轮询 >30min
	}
	registry.mu.Lock()
	registry.sessions["idle-loop-1"] = session
	registry.mu.Unlock()

	tick <- time.Now()
	deadline := time.Now().Add(2 * time.Second)
	for {
		registry.mu.Lock()
		remaining := len(registry.sessions)
		registry.mu.Unlock()
		if remaining == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("evictLoop did not reclaim idle session")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Close（close evictStop）后循环退出。
	registry.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("evictLoop did not stop after Close")
	}
}

func TestRegistryCloseIdempotent(t *testing.T) {
	registry := NewStreamRegistry()
	registry.Close()
	registry.Close() // 幂等不 panic
	// Close 只停后台循环，不动会话表：手动回收仍可用。
	if got := registry.EvictIdle(time.Now().UnixMilli()); len(got) != 0 {
		t.Fatalf("EvictIdle after Close: unexpected evictions %v", got)
	}
	// evictStop 为 nil 的直构夹具（不经 NewStreamRegistry）Close 为 no-op。
	(&StreamRegistry{sessions: map[string]*streamSession{}}).Close()
}

func TestRegistryMaxSessions(t *testing.T) {
	// 常量契约守卫（§5.5）。
	if StreamMaxSessions != 20 {
		t.Errorf("StreamMaxSessions = %d, want 20", StreamMaxSessions)
	}
	if StreamRingCapacity != 10000 {
		t.Errorf("StreamRingCapacity = %d, want 10000", StreamRingCapacity)
	}
	if StreamIdleTimeout.Minutes() != 30 {
		t.Errorf("StreamIdleTimeout = %v, want 30m", StreamIdleTimeout)
	}
	if StreamBatchFlush.Milliseconds() != 200 || StreamBatchSize != 50 {
		t.Errorf("throttle = %v/%d, want 200ms/50", StreamBatchFlush, StreamBatchSize)
	}
}

// 字节预算逐出（评审 M-2）：超预算从最旧端弹出直至回落；单条超预算不留存。
func TestRingBufferTrimToBytes(t *testing.T) {
	rb := newRingBuffer(10)
	// 3 条 × ~356B（两通道 + 256 保守底数），预算 800 → 弹出最旧 2 条后回落。
	small := ConsumedMessage{ValueText: "abcdefgh", ValueBase64: "YWJjZGVmZ2g="}
	for i := 0; i < 3; i++ {
		rb.append(small)
	}
	before := rb.Len()
	evicted := rb.trimToBytes(800)
	if evicted == 0 || rb.Len() >= before {
		t.Fatalf("trim evicted=%d len=%d->%d, want eviction below budget", evicted, before, rb.Len())
	}
	if rb.Bytes() > 800 {
		t.Errorf("bytes=%d still above budget 800", rb.Bytes())
	}
	// 覆盖最旧路径的字节记账：写满 10 条（3 已在）后 bytes 应与重算一致。
	for i := 0; i < 9; i++ {
		rb.append(small)
	}
	if rb.Len() != rb.Cap() {
		t.Fatalf("len=%d want full %d", rb.Len(), rb.Cap())
	}
	var want int64
	for i := 0; i < rb.Len(); i++ {
		want += int64(consumeWireSizeBytes(rb.Page(i, 1)[0]))
	}
	if rb.Bytes() != want {
		t.Errorf("bytes=%d, recomputed=%d", rb.Bytes(), want)
	}
	// 单条超预算：trim 把 ring 弹空（不留存超预算巨条）。
	huge := ConsumedMessage{ValueText: string(make([]byte, 4096))}
	rb2 := newRingBuffer(4)
	rb2.append(huge)
	rb2.append(small)
	rb2.trimToBytes(100)
	if rb2.Len() != 0 || rb2.Bytes() != 0 {
		t.Errorf("oversized trim len=%d bytes=%d, want 0/0", rb2.Len(), rb2.Bytes())
	}
}

// paused 不续命（架构评审 C1 残余）：flush 对 paused 会话只入 ring 不刷
// lastActivityUnixMs——繁忙 topic 上的暂停会话（含孤儿）也要满足 30 分钟
// 空闲回收；running 会话照旧续命。
func TestRegistryFlushPausedDoesNotRefreshActivity(t *testing.T) {
	registry := NewStreamRegistry()
	now := int64(1_000_000_000)
	session := &streamSession{
		sessionID:          "s1",
		connectionID:       "c1",
		closeClient:        func() {},
		ring:               newRingBuffer(4),
		partitionOffsets:   map[int32]int64{},
		lastActivityUnixMs: now,
		paused:             true,
	}
	registry.mu.Lock()
	registry.sessions["s1"] = session
	registry.mu.Unlock()

	batch := []ConsumedMessage{{Topic: "t", ValueText: "x"}}
	registry.flush(session, &batch)
	if got := session.lastActivityUnixMs; got != now {
		t.Errorf("paused flush refreshed activity: %d -> %d, want unchanged", now, got)
	}
	if session.ring.Len() != 1 {
		t.Errorf("paused flush should still buffer, ring len = %d", session.ring.Len())
	}

	session.paused = false
	batch = []ConsumedMessage{{Topic: "t", ValueText: "x"}} // flush 会清空 batch，重填
	registry.flush(session, &batch)
	if got := session.lastActivityUnixMs; got == now {
		t.Error("running flush should refresh activity")
	}
}
