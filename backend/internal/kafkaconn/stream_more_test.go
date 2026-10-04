package kafkaconn

// stream_more_test.go：流式会话注册表离线矩阵（契约 §5.4/§5.5）——
// ring 批量写入、pause/resume/status/messages、stop/stopAll/按连接停止、
// 空闲回收、flush 节流 emit（fake emitter）、StartStream 进 broker 之前的
// 校验分支。runLoop 依赖真实 client（broker 依赖），不在离线范围。

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// fakeStreamEmitter 捕获 emit 事件。
type fakeStreamEmitter struct {
	batches []StreamMessageBatch
	errors  []string
}

func (f *fakeStreamEmitter) EmitStreamMessages(batch StreamMessageBatch) {
	f.batches = append(f.batches, batch)
}
func (f *fakeStreamEmitter) EmitStreamError(sessionID, message string) {
	f.errors = append(f.errors, sessionID+":"+message)
}

// newTestSession 构造注入用会话（无 client，不 runLoop）。
func newTestSession(id, connectionID, topic string) *streamSession {
	ctx, cancel := context.WithCancel(context.Background())
	return &streamSession{
		sessionID:          id,
		connectionID:       connectionID,
		topic:              topic,
		ctx:                ctx,
		cancel:             cancel,
		ring:               newRingBuffer(StreamRingCapacity),
		partitionOffsets:   map[int32]int64{},
		lastActivityUnixMs: 0,
	}
}

func (r *StreamRegistry) inject(session *streamSession) {
	r.mu.Lock()
	r.sessions[session.sessionID] = session
	r.mu.Unlock()
}

func TestRingAppendBatch(t *testing.T) {
	rb := newRingBuffer(3)
	rb.appendBatch([]ConsumedMessage{{ValueText: "1"}, {ValueText: "2"}, {ValueText: "3"}, {ValueText: "4"}})
	if rb.Len() != 3 || rb.Cap() != 3 {
		t.Fatalf("len/cap = %d/%d, want 3/3", rb.Len(), rb.Cap())
	}
	// 覆盖最旧后最旧在前是 2,3,4。
	page := rb.Page(0, 10)
	if len(page) != 3 || page[0].ValueText != "2" || page[2].ValueText != "4" {
		t.Errorf("page = %+v", page)
	}
	rb.appendBatch(nil) // 空批安全
	if rb.Len() != 3 {
		t.Errorf("len = %d after empty batch", rb.Len())
	}
}

func TestStreamRegistryLifecycleMatrix(t *testing.T) {
	service := NewService()
	emitter := &fakeStreamEmitter{}
	service.Streams.Emitter = emitter

	service.Streams.inject(newTestSession("s1", "conn-a", "orders"))
	service.Streams.inject(newTestSession("s2", "conn-b", "payments"))

	// status 未找到报错。
	if _, err := service.StreamStatusOf("ghost"); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("err = %v, want not found", err)
	}

	// pause/resume + status 快照。
	status, err := service.PauseStream("s1")
	if err != nil || !status.Paused || status.SessionID != "s1" || status.Topic != "orders" {
		t.Fatalf("pause status = %+v, %v", status, err)
	}
	if status.BufferCapacity != StreamRingCapacity {
		t.Errorf("capacity = %d, want %d", status.BufferCapacity, StreamRingCapacity)
	}
	status, err = service.ResumeStream("s1")
	if err != nil || status.Paused {
		t.Errorf("resume status = %+v, %v", status, err)
	}
	if _, err := service.PauseStream("ghost"); err == nil {
		t.Error("pause ghost expected error")
	}

	// messages 分页 + 未找到（ring 空 → Page 返回 nil）。
	if _, err := service.StreamMessages("ghost", 0, 10); err == nil {
		t.Error("messages ghost expected error")
	}
	msgs, err := service.StreamMessages("s1", 0, 100)
	if err != nil || msgs.Total != 0 || msgs.Offset != 0 || msgs.Limit != 100 {
		t.Errorf("messages = %+v, %v", msgs, err)
	}

	// Stop 幂等；停止后查不到。
	service.StopStream("s1", false)
	service.StopStream("s1", false)
	if _, err := service.StreamStatusOf("s1"); err == nil {
		t.Error("stopped session expected error")
	}
	// all:true 停全部。
	service.StopStream("s2", true)
	if _, err := service.StreamStatusOf("s2"); err == nil {
		t.Error("stop-all should remove s2")
	}
}

func TestStreamStopAllForConnectionAndEvict(t *testing.T) {
	service := NewService()
	service.Streams.inject(newTestSession("a1", "conn-a", "t1"))
	service.Streams.inject(newTestSession("a2", "conn-a", "t2"))
	service.Streams.inject(newTestSession("b1", "conn-b", "t3"))

	service.Streams.StopAllForConnection("conn-a")
	if _, err := service.StreamStatusOf("a1"); err == nil {
		t.Error("a1 should be stopped")
	}
	if _, err := service.StreamStatusOf("a2"); err == nil {
		t.Error("a2 should be stopped")
	}
	if _, err := service.StreamStatusOf("b1"); err != nil {
		t.Errorf("b1 should survive: %v", err)
	}

	// 空闲回收（双信号）：b1 此前被 status 命中（关注已续命到当前时点），推进
	// 到「活动与关注同时超时」的未来时点才回收。
	ids := service.Streams.EvictIdle(time.Now().UnixMilli() + StreamIdleTimeout.Milliseconds() + 1)
	if len(ids) != 1 || ids[0] != "b1" {
		t.Errorf("evicted = %v, want [b1]", ids)
	}
	if ids := service.Streams.EvictIdle(0); len(ids) != 0 {
		t.Errorf("second evict = %v, want empty", ids)
	}
}

// 空闲回收可观测（架构评审 H-1 回归）：EvictIdle 对每个被回收会话发
// kafka/stream/error——此前回收完全静默，前端把已回收会话误读成 Running。
func TestRegistryEvictIdleEmitsError(t *testing.T) {
	service := NewService()
	emitter := &fakeStreamEmitter{}
	service.Streams.Emitter = emitter
	session := newTestSession("e1", "conn-a", "orders")
	service.Streams.inject(session)

	ids := service.Streams.EvictIdle(time.Now().UnixMilli() + StreamIdleTimeout.Milliseconds() + 1)
	if len(ids) != 1 || ids[0] != "e1" {
		t.Fatalf("evicted = %v, want [e1]", ids)
	}
	if len(emitter.errors) != 1 || !strings.HasPrefix(emitter.errors[0], "e1:") {
		t.Fatalf("errors = %v, want exactly one idle-timeout event for e1", emitter.errors)
	}
}

// 关注续命（架构评审 H-1 回归）：回收以客户端关注为唯一信号——status 命中
// 续命的会话不被回收；繁忙 topic 上无人轮询的孤儿（活动新鲜也无效）到点
// 回收。关注随后过期才轮到它被回收。
func TestRegistryAttentionKeepsSessionAlive(t *testing.T) {
	service := NewService()
	service.Streams.inject(newTestSession("att", "conn-a", "quiet"))
	service.Streams.inject(newTestSession("busy", "conn-a", "hot"))

	far := time.Now().UnixMilli() + StreamIdleTimeout.Milliseconds() + 1
	service.Streams.mu.Lock()
	// att：面板在轮询——status 命中把关注刷到阈值内（活动已停更）。
	service.Streams.sessions["att"].lastAttentionUnixMs = far - StreamIdleTimeout.Milliseconds() + 1000
	// busy：繁忙 topic 上的孤儿——非 paused flush 一直续命活动，但无人轮询。
	service.Streams.sessions["busy"].lastActivityUnixMs = far - 1000
	service.Streams.mu.Unlock()

	ids := service.Streams.EvictIdle(far)
	if len(ids) != 1 || ids[0] != "busy" {
		t.Fatalf("evicted = %v, want [busy] (attended session must survive, busy orphan must go)", ids)
	}
	// 关注同样过期后（面板关掉），att 在下一轮扫描被回收。
	if ids := service.Streams.EvictIdle(far + StreamIdleTimeout.Milliseconds()); len(ids) != 1 || ids[0] != "att" {
		t.Fatalf("second evict = %v, want [att]", ids)
	}
}

func TestStreamFlushThrottleAndEmit(t *testing.T) {
	service := NewService()
	emitter := &fakeStreamEmitter{}
	service.Streams.Emitter = emitter
	session := newTestSession("f1", "conn-a", "orders")
	service.Streams.inject(session)

	batch := []ConsumedMessage{{ValueText: "x"}}
	// 运行中：写 ring + 推送。
	service.Streams.flush(session, &batch)
	if session.ring.Len() != 1 || len(emitter.batches) != 1 {
		t.Fatalf("ring=%d batches=%d", session.ring.Len(), len(emitter.batches))
	}
	if got := emitter.batches[0]; got.SessionID != "f1" || got.Paused || len(got.Messages) != 1 {
		t.Errorf("batch = %+v", got)
	}
	// 空批不发事件。
	empty := []ConsumedMessage{}
	service.Streams.flush(session, &empty)
	if len(emitter.batches) != 1 {
		t.Errorf("empty batch emitted: %d", len(emitter.batches))
	}
	// paused：入 ring 但不推送。
	session.mu.Lock()
	session.paused = true
	session.mu.Unlock()
	batch = []ConsumedMessage{{ValueText: "y"}}
	service.Streams.flush(session, &batch)
	if session.ring.Len() != 2 || len(emitter.batches) != 1 {
		t.Errorf("paused flush: ring=%d batches=%d", session.ring.Len(), len(emitter.batches))
	}
	// nil batch 安全。
	service.Streams.flush(session, nil)

	// Emitter nil 安全。
	service.Streams.Emitter = nil
	session.mu.Lock()
	session.paused = false
	session.mu.Unlock()
	batch = []ConsumedMessage{{ValueText: "z"}}
	service.Streams.flush(session, &batch)

	// emitError：Emitter nil 安全 + 事件内容。
	service.Streams.emitError(session, "boom")
	service.Streams.Emitter = emitter
	service.Streams.emitError(session, "boom")
	if len(emitter.errors) != 1 || emitter.errors[0] != "f1:boom" {
		t.Errorf("errors = %v", emitter.errors)
	}
}

func TestStartStreamValidationOffline(t *testing.T) {
	service := NewService()
	// 未连接。
	if _, err := service.StartStream(ConsumeParams{ConnectionID: "ghost", Topic: "t"}); err == nil || !strings.Contains(err.Error(), "not connected") {
		t.Errorf("err = %v, want connection not found", err)
	}
	// 连接后：空 topic / commit×read_only / 参数错。
	service2 := NewService()
	if err := connectWithConfig(t, service2, "ss",
		`{"bootstrap_servers": "127.0.0.1:1", "read_only": true}`, `{}`); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := service2.StartStream(ConsumeParams{ConnectionID: "ss"}); err == nil || !strings.Contains(err.Error(), "topic is required") {
		t.Errorf("err = %v, want topic required", err)
	}
	if _, err := service2.StartStream(ConsumeParams{ConnectionID: "ss", Topic: "t", Commit: true, GroupID: "g"}); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Errorf("err = %v, want read-only commit block", err)
	}
	if _, err := service2.StartStream(ConsumeParams{ConnectionID: "ss", Topic: "t", MatchMode: "bogus"}); err == nil {
		t.Error("bad matchMode expected error")
	}
	// KAFKA-M4 回归：decode/decompression 非法值必须在建 client 前拒绝，
	// 不产生占名额的僵尸会话。
	if _, err := service2.StartStream(ConsumeParams{ConnectionID: "ss", Topic: "t", Decompression: "brotli"}); err == nil || !strings.Contains(err.Error(), "decompression must be") {
		t.Errorf("err = %v, want decompression validation error", err)
	}
	if _, err := service2.StartStream(ConsumeParams{ConnectionID: "ss", Topic: "t", Decode: "hex"}); err == nil || !strings.Contains(err.Error(), "decode must be") {
		t.Errorf("err = %v, want decode validation error", err)
	}
	service2.Streams.mu.Lock()
	remaining := len(service2.Streams.sessions)
	service2.Streams.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("failed starts must not leave sessions behind, got %d", remaining)
	}
}

// S-STREAM-CAP（评审 M-2）：名额 check+reserve 必须同临界区——check-then-act
// 之间隔着 profile/client 构建，并发 start 可超限。预留期间名额计入
// reserved，注册（admit）与失败释放（release）均锁内结转。
func TestStreamRegistryReserveAdmit(t *testing.T) {
	r := &StreamRegistry{sessions: map[string]*streamSession{}}
	for i := 0; i < StreamMaxSessions; i++ {
		r.inject(newTestSession(sprintf("s-%d", i), "c", "t"))
	}
	if _, ok := r.reserveStreamSlot(); ok {
		t.Fatal("full registry must refuse reserve")
	}
	r.mu.Lock()
	delete(r.sessions, "s-0")
	r.mu.Unlock()
	id, ok := r.reserveStreamSlot()
	if !ok {
		t.Fatal("slot must be reservable after stop")
	}
	if _, ok := r.reserveStreamSlot(); ok {
		t.Fatal("reserved slot must count against the cap")
	}
	r.releaseStreamSlot()
	if _, ok := r.reserveStreamSlot(); !ok {
		t.Fatal("release must free the slot")
	}
	r.admitStreamSlot(id, newTestSession(id, "c", "t"))
	if _, ok := r.reserveStreamSlot(); ok {
		t.Fatal("admitted session must count against the cap")
	}
}

// S-STREAM-COMMIT（评审 M-3）：流式会话不提交 offset（runLoop 无提交点），
// commit=true 此前静默无效——诚实拒绝；只读拦截文案不变。
func TestStartStreamCommitRejected(t *testing.T) {
	svc := NewService()
	_, err := svc.StartStream(ConsumeParams{ConnectionID: "ghost", Topic: "t", Commit: true})
	if err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("err = %v, want read-only commit block", err)
	}
	if err := connectWithConfig(t, svc, "sc", `{"bootstrap_servers": "127.0.0.1:1"}`, `{}`); err != nil {
		t.Fatalf("connect error = %v", err)
	}
	_, err = svc.StartStream(ConsumeParams{ConnectionID: "sc", Topic: "t", Commit: true})
	if err == nil || !strings.Contains(err.Error(), "commit") {
		t.Fatalf("err = %v, want explicit commit rejection", err)
	}
}

// KAFKA-H2 回归：flush 事件携带发送时点的 ring 存量（bufferSize），前端
// 据此估算被覆盖丢弃的行数——此前真实后端不发送该字段，前端 droppedRows
// 估算只在 mock 中生效（活体漂移）。
func TestStreamFlushCarriesBufferSize(t *testing.T) {
	service := NewService()
	emitter := &fakeStreamEmitter{}
	service.Streams.Emitter = emitter
	session := newTestSession("bs1", "conn-a", "orders")
	service.Streams.inject(session)

	batch := []ConsumedMessage{{ValueText: "x"}, {ValueText: "y"}}
	service.Streams.flush(session, &batch)
	if len(emitter.batches) != 1 {
		t.Fatalf("batches = %d", len(emitter.batches))
	}
	if got := emitter.batches[0].BufferSize; got != 2 {
		t.Errorf("bufferSize = %d, want ring size 2", got)
	}
}

// KAFKA-H2 回归：批次推送判定双阈值（条数上限 / 字节预算），空批恒不 flush。
func TestStreamBatchShouldFlushMatrix(t *testing.T) {
	if streamBatchShouldFlush(0, 0, StreamBatchByteBudget) {
		t.Error("empty batch must never flush")
	}
	if !streamBatchShouldFlush(StreamBatchSize, 0, 0) {
		t.Error("count overflow must flush")
	}
	if streamBatchShouldFlush(StreamBatchSize-1, 0, 0) {
		t.Error("below count cap must not flush")
	}
	if !streamBatchShouldFlush(1, StreamBatchByteBudget, 1) {
		t.Error("byte overflow must flush")
	}
	if streamBatchShouldFlush(1, StreamBatchByteBudget-1, 1) {
		t.Error("within byte budget must not flush")
	}
}

// KAFKA-M1 回归：runLoop 任何错误退出都必须把会话从注册表摘除——此前
// 入口校验失败/熔断只发事件就 return，会话以「活跃」状态滞留至 30 分钟
// 空闲回收，status 一直返回冻结快照。
func TestStreamRunLoopErrorExitRemovesSession(t *testing.T) {
	service := NewService()
	emitter := &fakeStreamEmitter{}
	service.Streams.Emitter = emitter
	session := newTestSession("z1", "conn-a", "orders")
	// 非法正则让 runLoop 在触碰 client 之前于入口校验处退出。
	session.req.MatchMode = "regex"
	session.req.ValueFilter = "["
	service.Streams.inject(session)

	service.Streams.runLoop(session)

	if len(emitter.errors) == 0 {
		t.Fatal("expected error event before exit")
	}
	service.Streams.mu.Lock()
	_, ok := service.Streams.sessions["z1"]
	service.Streams.mu.Unlock()
	if ok {
		t.Fatal("zombie session left in registry after runLoop error exit")
	}
}

// KAFKA-EVT 回归（架构审查"事件通道不在契约守护面内"最小钉）：事件载荷
// 顶层键与协议文档 §6.1 严格一致——此前 bufferSize 漂移（前端/mock 有、
// 真实后端无）导致 droppedRows 估算在生产从不工作且所有测试全绿。
func TestStreamMessageBatchEventShape(t *testing.T) {
	batch := StreamMessageBatch{
		SessionID:    "s1",
		Messages:     []ConsumedMessage{},
		TotalScanned: 1,
		TotalMatched: 1,
		Paused:       false,
		BufferSize:   2,
	}
	data, err := json.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal = %v", err)
	}
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(data, &keys); err != nil {
		t.Fatalf("unmarshal = %v", err)
	}
	want := []string{"sessionId", "messages", "totalScanned", "totalMatched", "paused", "bufferSize"}
	if len(keys) != len(want) {
		t.Fatalf("event keys = %v, want exactly %v", keys, want)
	}
	for _, key := range want {
		if _, ok := keys[key]; !ok {
			t.Errorf("event key %q missing (doc §6.1 drift)", key)
		}
	}
}
