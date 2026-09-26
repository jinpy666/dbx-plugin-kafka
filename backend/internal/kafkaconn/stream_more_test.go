package kafkaconn

// stream_more_test.go：流式会话注册表离线矩阵（契约 §5.4/§5.5）——
// ring 批量写入、pause/resume/status/messages、stop/stopAll/按连接停止、
// 空闲回收、flush 节流 emit（fake emitter）、StartStream 进 broker 之前的
// 校验分支。runLoop 依赖真实 client（broker 依赖），不在离线范围。

import (
	"context"
	"strings"
	"testing"
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

	// 空闲回收：超时被回收，活跃保留。
	ids := service.Streams.EvictIdle(StreamIdleTimeout.Milliseconds() + 1)
	if len(ids) != 1 || ids[0] != "b1" {
		t.Errorf("evicted = %v, want [b1]", ids)
	}
	if ids := service.Streams.EvictIdle(0); len(ids) != 0 {
		t.Errorf("second evict = %v, want empty", ids)
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
