package kafkaconn

// stream.go：流式消费会话（契约 §5.2/§5.4/§5.5）。
// 逻辑对照 tiny-rdm kafka_stream_service.go 重写：ring buffer 10000、
// 会话上限 20、空闲 30 分钟回收、200ms/批 50 节流 emit `kafka/stream/messages`、
// fetch 错误指数退避 500ms→30s、pause/resume/stop；read_only 禁止 commit。
//
// 事件通道经 StreamEmitter 注入（main 适配 SDK emitter），kafkaconn 不依赖
// SDK；tinyrdm 的 string(record.Value) 二进制损坏 bug 由 messageFromRecord
// 的保真形状规避。

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// 流式会话常量（§5.5）。
const (
	StreamMaxSessions    = 20
	StreamIdleTimeout    = 30 * time.Minute
	StreamBatchFlush     = 200 * time.Millisecond
	StreamBatchSize      = 50
	StreamRingCapacity   = 10000
	StreamMinRetryDelay  = 500 * time.Millisecond
	StreamMaxRetryDelay  = 30 * time.Second
	StreamBackoffFactor  = 2.0
	StreamEvictScanEvery = 5 * time.Minute
)

// StreamEmitter 是流式事件出口（main 注入 SDK emitter 适配器）。
type StreamEmitter interface {
	// EmitStreamMessages 发送 kafka/stream/messages 事件。
	EmitStreamMessages(batch StreamMessageBatch)
	// EmitStreamError 发送 kafka/stream/error 事件。
	EmitStreamError(sessionID, message string)
}

// StreamMessageBatch 是 kafka/stream/messages 事件载荷（§5.4）。
type StreamMessageBatch struct {
	SessionID    string            `json:"sessionId"`
	Messages     []ConsumedMessage `json:"messages"`
	TotalScanned int64             `json:"totalScanned"`
	TotalMatched int64             `json:"totalMatched"`
	Paused       bool              `json:"paused"`
}

// StreamStatus 是 kafka/stream/status 返回（§5.2）。
type StreamStatus struct {
	SessionID        string          `json:"sessionId"`
	Topic            string          `json:"topic"`
	Paused           bool            `json:"paused"`
	TotalScanned     int64           `json:"totalScanned"`
	TotalMatched     int64           `json:"totalMatched"`
	BufferSize       int             `json:"bufferSize"`
	BufferCapacity   int             `json:"bufferCapacity"`
	PartitionOffsets map[int32]int64 `json:"partitionOffsets,omitempty"`
}

// StreamMessagesResult 是 kafka/stream/messages 返回（ring buffer 历史分页）。
type StreamMessagesResult struct {
	Total    int               `json:"total"`
	Offset   int               `json:"offset"`
	Limit    int               `json:"limit"`
	Messages []ConsumedMessage `json:"messages"`
}

// ringBuffer 定容环形缓冲（非并发安全；调用方持锁）。
type ringBuffer struct {
	data     []ConsumedMessage
	head     int // 下一写入位
	size     int
	capacity int
}

func newRingBuffer(capacity int) *ringBuffer {
	if capacity <= 0 {
		capacity = StreamRingCapacity
	}
	return &ringBuffer{data: make([]ConsumedMessage, capacity), capacity: capacity}
}

// append 写入单条，满则覆盖最旧。
func (rb *ringBuffer) append(msg ConsumedMessage) {
	rb.data[rb.head] = msg
	rb.head = (rb.head + 1) % rb.capacity
	if rb.size < rb.capacity {
		rb.size++
	}
}

// appendBatch 批量写入。
func (rb *ringBuffer) appendBatch(msgs []ConsumedMessage) {
	for i := range msgs {
		rb.append(msgs[i])
	}
}

// Len 当前条数。
func (rb *ringBuffer) Len() int { return rb.size }

// Cap 容量。
func (rb *ringBuffer) Cap() int { return rb.capacity }

// Page 返回 [offset, offset+limit) 快照（拷贝副本，最旧在前）。
func (rb *ringBuffer) Page(offset, limit int) []ConsumedMessage {
	if offset < 0 {
		offset = 0
	}
	if offset >= rb.size {
		return nil
	}
	end := offset + limit
	if end > rb.size {
		end = rb.size
	}
	count := end - offset
	base := (rb.head - rb.size + rb.capacity) % rb.capacity
	out := make([]ConsumedMessage, count)
	for i := 0; i < count; i++ {
		out[i] = rb.data[(base+offset+i)%rb.capacity]
	}
	return out
}

// streamSession 单个流式会话状态。
type streamSession struct {
	sessionID    string
	connectionID string
	topic        string

	client      *kgo.Client
	closeClient func()

	req    ConsumeParams
	ctx    context.Context
	cancel context.CancelFunc
	// schemaClient 非 nil 时按 req.Schema 挂载 SR 解码（StartStream 时构建）。
	schemaClient *schemaRegistryClient

	mu                 sync.Mutex
	ring               *ringBuffer
	partitionOffsets   map[int32]int64
	totalScanned       int64
	totalMatched       int64
	paused             bool
	lastActivityUnixMs int64
}

// StreamRegistry 管理全部流式会话。
type StreamRegistry struct {
	mu       sync.Mutex
	next     int64
	sessions map[string]*streamSession
	Emitter  StreamEmitter // nil 安全

	// evictStop 关闭即令 evictLoop 退出（Close 幂等；nil 安全——直构字面量的
	// 测试夹具没有该通道时 Close 为 no-op）。只在进程收尾（Service.CloseAll）
	// 关闭；stream/stop all:true 走 StopAll，不影响回收循环。
	evictStop chan struct{}
}

// NewStreamRegistry 创建会话注册表，并启动空闲回收循环（§5.5「空闲 30 分钟
// 回收」的调度端：此前 EvictIdle 仅测试可达，长驻 sidecar 中过期会话（如
// topic 被删后只出不进的会话）只累积到 StreamMaxSessions 上限才报错）。
func NewStreamRegistry() *StreamRegistry {
	r := &StreamRegistry{sessions: map[string]*streamSession{}, evictStop: make(chan struct{})}
	ticker := time.NewTicker(StreamEvictScanEvery)
	go func() {
		defer ticker.Stop()
		r.evictLoop(ticker.C)
	}()
	return r
}

// evictLoop 周期扫描空闲会话；tick 注入便于测试。
func (r *StreamRegistry) evictLoop(tick <-chan time.Time) {
	for {
		select {
		case <-r.evictStop:
			return
		case <-tick:
			r.EvictIdle(time.Now().UnixMilli())
		}
	}
}

// Close 停止空闲回收循环（Service.CloseAll 进程收尾调用；幂等）。
func (r *StreamRegistry) Close() {
	if r.evictStop == nil {
		return
	}
	select {
	case <-r.evictStop:
	default:
		close(r.evictStop)
	}
}

// StartStream 创建并启动流式会话（§5.2 kafka/stream/start）。
func (s *Service) StartStream(params ConsumeParams) (*StreamStatus, error) {
	entry := s.lookup(params.ConnectionID)
	if entry == nil {
		return nil, errConnectionNotFound(params.ConnectionID)
	}
	entry.mu.Lock()
	profile := entry.profile
	entry.mu.Unlock()

	topic := trimSpace(params.Topic)
	if topic == "" {
		return nil, errf("topic is required")
	}
	if err := validateConsumeParams(params); err != nil {
		return nil, err
	}
	// 解码/解压方法预校验（对照一次性消费路径先校验）：此前非法值要到
	// runLoop 才报错，会话已注册进名额——僵尸会话占 20 上限、等 30 分钟
	// 空闲回收。建 client 前拒绝，失败不留任何注册状态。
	if _, err := normalizeDecodeMethod(params.Decode); err != nil {
		return nil, err
	}
	if _, err := normalizeDecompressMethod(params.Decompression); err != nil {
		return nil, err
	}
	// §5.5：只读策略下禁止 commit。
	if params.Commit && profile.ReadOnly {
		return nil, errf("kafka profile %q is read-only; commit is blocked", profile.Name)
	}

	s.Streams.mu.Lock()
	if len(s.Streams.sessions) >= StreamMaxSessions {
		s.Streams.mu.Unlock()
		return nil, errf("maximum %d concurrent stream sessions reached", StreamMaxSessions)
	}
	s.Streams.next++
	sessionID := sprintf("kafka-stream-%d-%d", time.Now().UnixNano(), s.Streams.next)
	s.Streams.mu.Unlock()

	partitions, err := normalizeConsumePartitions(params.Partitions)
	if err != nil {
		return nil, err
	}
	partitionOffsets, err := normalizeConsumePartitionOffsets(params.PartitionOffsets)
	if err != nil {
		return nil, err
	}
	if len(partitions) == 0 && len(partitionOffsets) > 0 {
		partitions = offsetPartitions(partitionOffsets)
	}
	isolation, err := isolationLevelValue(params.IsolationLevel)
	if err != nil {
		return nil, err
	}
	groupID := trimSpace(params.GroupID)

	consumeOpts, err := buildConsumeOpts(params, topic, groupID, partitions, partitionOffsets, isolation)
	if err != nil {
		return nil, err
	}
	// schema 挂载：SR 客户端在会话创建期构建一次（失败即 start 失败）。
	// wire format 解码仅支持 Confluent：provider=glue → 业务错（Phase 3 门禁）。
	var schemaClient *schemaRegistryClient
	if params.Schema != nil {
		if err := s.schemaMountSupported(params.ConnectionID, params.Schema.Registry, "consume"); err != nil {
			return nil, err
		}
		schemaClient, err = s.confluentClientFor(params.ConnectionID)
		if err != nil {
			return nil, err
		}
	}
	client, closeClient, err := s.consumeClient(params.ConnectionID, consumeOpts...)
	if err != nil {
		return nil, err
	}

	sessionCtx, cancel := context.WithCancel(context.Background())
	now := time.Now().UnixMilli()
	session := &streamSession{
		sessionID:          sessionID,
		connectionID:       params.ConnectionID,
		topic:              topic,
		client:             client,
		closeClient:        closeClient,
		req:                params,
		ctx:                sessionCtx,
		cancel:             cancel,
		schemaClient:       schemaClient,
		ring:               newRingBuffer(StreamRingCapacity),
		partitionOffsets:   map[int32]int64{},
		lastActivityUnixMs: now,
	}

	s.Streams.mu.Lock()
	s.Streams.sessions[sessionID] = session
	s.Streams.mu.Unlock()

	go s.Streams.runLoop(session)
	return s.Streams.Status(sessionID)
}

// StopStream 停止单个会话（或 all:true 全停，§5.2 kafka/stream/stop）。
func (s *Service) StopStream(sessionID string, all bool) {
	if all {
		s.Streams.StopAll()
		return
	}
	s.Streams.Stop(sessionID)
}

// PauseStream 暂停推送（消费继续进 ring，§5.5）。
func (s *Service) PauseStream(sessionID string) (*StreamStatus, error) {
	if err := s.Streams.setPaused(sessionID, true); err != nil {
		return nil, err
	}
	return s.Streams.Status(sessionID)
}

// ResumeStream 恢复推送。
func (s *Service) ResumeStream(sessionID string) (*StreamStatus, error) {
	if err := s.Streams.setPaused(sessionID, false); err != nil {
		return nil, err
	}
	return s.Streams.Status(sessionID)
}

// StreamStatusOf 查询会话状态（kafka/stream/status）。
func (s *Service) StreamStatusOf(sessionID string) (*StreamStatus, error) {
	return s.Streams.Status(sessionID)
}

// StreamMessages ring buffer 历史分页（kafka/stream/messages）。
func (s *Service) StreamMessages(sessionID string, offset, limit int) (*StreamMessagesResult, error) {
	session := s.Streams.lookup(sessionID)
	if session == nil {
		return nil, errf("stream session %q not found", sessionID)
	}
	if limit <= 0 {
		limit = 100
	}
	session.mu.Lock()
	total := session.ring.Len()
	msgs := session.ring.Page(offset, limit)
	session.mu.Unlock()
	return &StreamMessagesResult{
		Total:    total,
		Offset:   offset,
		Limit:    limit,
		Messages: msgs,
	}, nil
}

// --- registry 内部 ---

func (r *StreamRegistry) lookup(sessionID string) *streamSession {
	r.mu.Lock()
	session := r.sessions[sessionID]
	r.mu.Unlock()
	return session
}

// shutdownSession 取消上下文并关闭 client（均 nil 安全，测试/生产复用）。
func shutdownSession(session *streamSession) {
	if session == nil {
		return
	}
	if session.cancel != nil {
		session.cancel()
	}
	if session.closeClient != nil {
		session.closeClient()
	}
}

// Stop 取消并移除会话；幂等。
func (r *StreamRegistry) Stop(sessionID string) {
	r.mu.Lock()
	session := r.sessions[sessionID]
	delete(r.sessions, sessionID)
	r.mu.Unlock()
	if session == nil {
		return
	}
	shutdownSession(session)
}

// StopAll 停止全部会话（CloseAll / stream/stop all:true）。
func (r *StreamRegistry) StopAll() {
	r.mu.Lock()
	sessions := make([]*streamSession, 0, len(r.sessions))
	for _, session := range r.sessions {
		sessions = append(sessions, session)
	}
	r.sessions = map[string]*streamSession{}
	r.mu.Unlock()
	for _, session := range sessions {
		shutdownSession(session)
	}
}

// StopAllForConnection 停止某连接的全部会话（重连/断开时调用）。
func (r *StreamRegistry) StopAllForConnection(connectionID string) {
	r.mu.Lock()
	sessions := make([]*streamSession, 0)
	for _, session := range r.sessions {
		if session.connectionID == connectionID {
			sessions = append(sessions, session)
			delete(r.sessions, session.sessionID)
		}
	}
	r.mu.Unlock()
	for _, session := range sessions {
		shutdownSession(session)
	}
}

// EvictIdle 回收空闲会话（idle 超时；nowMs 注入便于测试）。
func (r *StreamRegistry) EvictIdle(nowMs int64) []string {
	r.mu.Lock()
	var evict []*streamSession
	var ids []string
	for id, session := range r.sessions {
		session.mu.Lock()
		last := session.lastActivityUnixMs
		session.mu.Unlock()
		if nowMs-last > StreamIdleTimeout.Milliseconds() {
			ids = append(ids, id)
			evict = append(evict, session)
			delete(r.sessions, id)
		}
	}
	r.mu.Unlock()
	for _, session := range evict {
		shutdownSession(session)
	}
	return ids
}

// setPaused 暂停/恢复推送。
func (r *StreamRegistry) setPaused(sessionID string, paused bool) error {
	session := r.lookup(sessionID)
	if session == nil {
		return errf("stream session %q not found", sessionID)
	}
	session.mu.Lock()
	session.paused = paused
	session.mu.Unlock()
	return nil
}

// Status 会话状态快照。
func (r *StreamRegistry) Status(sessionID string) (*StreamStatus, error) {
	session := r.lookup(sessionID)
	if session == nil {
		return nil, errf("stream session %q not found", sessionID)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	offsets := make(map[int32]int64, len(session.partitionOffsets))
	for k, v := range session.partitionOffsets {
		offsets[k] = v
	}
	return &StreamStatus{
		SessionID:        session.sessionID,
		Topic:            session.topic,
		Paused:           session.paused,
		TotalScanned:     session.totalScanned,
		TotalMatched:     session.totalMatched,
		BufferSize:       session.ring.Len(),
		BufferCapacity:   session.ring.Cap(),
		PartitionOffsets: offsets,
	}, nil
}

// StatusesFor 返回指定连接的全部流式会话状态快照（MCP kafka_ui_state 的
// stream 附加段用；connectionId 空 = 全部连接）。按 sessionId 升序稳定排序。
func (r *StreamRegistry) StatusesFor(connectionID string) []StreamStatus {
	r.mu.Lock()
	targets := make([]*streamSession, 0, len(r.sessions))
	for _, session := range r.sessions {
		if connectionID == "" || session.connectionID == connectionID {
			targets = append(targets, session)
		}
	}
	r.mu.Unlock()

	statuses := make([]StreamStatus, 0, len(targets))
	for _, session := range targets {
		status, err := r.Status(session.sessionID)
		if err != nil {
			continue
		}
		statuses = append(statuses, *status)
	}
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].SessionID < statuses[j].SessionID })
	return statuses
}

// runLoop 消费主循环：节流 emit + ring 写入 + 指数退避（tinyrdm
// processStream :413 重写；计数与 ring 仅经 session.mu 保护读写）。
func (r *StreamRegistry) runLoop(session *streamSession) {
	matcher, err := newConsumeTextMatcher(session.req)
	if err != nil {
		r.emitError(session, err.Error())
		return
	}
	decodeMethod, err := normalizeDecodeMethod(session.req.Decode)
	if err != nil {
		r.emitError(session, err.Error())
		return
	}
	decompressMethod, err := normalizeDecompressMethod(session.req.Decompression)
	if err != nil {
		r.emitError(session, err.Error())
		return
	}
	// schema 挂载（Phase 2）：per-session 解码器（SR 客户端 + 元数据缓存）。
	var schemaDec *schemaDecoder
	if session.req.Schema != nil && session.schemaClient != nil {
		schemaDec = newSchemaDecoder(session.schemaClient, session.req.Schema)
	}

	batch := make([]ConsumedMessage, 0, StreamBatchSize)
	ticker := time.NewTicker(StreamBatchFlush)
	defer ticker.Stop()
	defer func() { r.flush(session, &batch) }()

	retryDelay := StreamMinRetryDelay
	for {
		select {
		case <-session.ctx.Done():
			return
		case <-ticker.C:
			if len(batch) > 0 {
				r.flush(session, &batch)
			}
		default:
		}

		// poll 以 200ms 为上限：PollRecords 无记录时会一直阻塞在 ctx 上，
		// 若直接用 session.ctx，ticker 分支永远得不到调度，batch 无法按时
		// flush（Phase 1 遗留：ring/事件只在"消息持续流动"时才推送）。
		pollCtx, pollCancel := context.WithTimeout(session.ctx, StreamBatchFlush)
		fetches := session.client.PollRecords(pollCtx, StreamBatchSize)
		pollCancel()
		if fetchErr := fetches.Err(); fetchErr != nil && !isDeadline(fetchErr) {
			r.emitError(session, fetchErr.Error())
			select {
			case <-session.ctx.Done():
				return
			case <-time.After(retryDelay):
			}
			retryDelay = time.Duration(float64(retryDelay) * StreamBackoffFactor)
			if retryDelay > StreamMaxRetryDelay {
				retryDelay = StreamMaxRetryDelay
			}
			continue
		}
		retryDelay = StreamMinRetryDelay

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			session.mu.Lock()
			session.totalScanned++
			nextPartitionOffset(session.partitionOffsets, record)
			session.mu.Unlock()

			if !recordMatches(session.req, matcher, record) {
				continue
			}
			value := record.Value
			decoded := false
			decodeErr := ""
			valueDecoded := false
			var recordSchemaInfo *schemaValueInfo
			ensureValueDecoded := func() {
				if valueDecoded {
					return
				}
				valueDecoded = true
				value, decoded, decodeErr = decodeConsumeValue(record.Value, decodeMethod, decompressMethod)
				if schemaDec != nil {
					out, info, schemaErr := schemaDec.decode(session.ctx, value)
					if schemaErr != nil {
						if decodeErr == "" {
							decodeErr = "schema: " + schemaErr.Error()
						} else {
							decodeErr = decodeErr + "; schema: " + schemaErr.Error()
						}
						return
					}
					value = out
					decoded = true
					info.Subject = firstNonEmpty(info.Subject, trimSpace(session.req.Schema.Subject))
					recordSchemaInfo = &info
				}
			}
			if fieldFiltersNeedValue(session.req.FieldFilters) {
				ensureValueDecoded()
			}
			if !fieldFiltersMatch(session.req.FieldFilters, matcher, record, value) {
				continue
			}
			session.mu.Lock()
			session.totalMatched++
			session.mu.Unlock()

			ensureValueDecoded()
			// 流式会话是工作台路径：保持 valueText/valueBase64 双通道（评审
			// H-1 的 skipValueBase64 仅用于 digest 一次性消费）。
			batch = append(batch, messageFromRecordWithSchema(record, value, decoded, decodeErr, false, recordSchemaInfo, false))
			if len(batch) >= StreamBatchSize {
				r.flush(session, &batch)
			}
		}
		if session.ctx.Err() != nil {
			return
		}
	}
}

// flush 写 ring + 节流 emit（paused 时只入 ring 不推送）。
func (r *StreamRegistry) flush(session *streamSession, batch *[]ConsumedMessage) {
	if batch == nil || len(*batch) == 0 {
		return
	}
	msgs := *batch
	*batch = make([]ConsumedMessage, 0, StreamBatchSize)

	session.mu.Lock()
	session.lastActivityUnixMs = time.Now().UnixMilli()
	session.ring.appendBatch(msgs)
	paused := session.paused
	totalScanned := session.totalScanned
	totalMatched := session.totalMatched
	session.mu.Unlock()

	if r.Emitter == nil || paused {
		return
	}
	r.Emitter.EmitStreamMessages(StreamMessageBatch{
		SessionID:    session.sessionID,
		Messages:     msgs,
		TotalScanned: totalScanned,
		TotalMatched: totalMatched,
		Paused:       paused,
	})
}

// emitError 发送会话错误事件。
func (r *StreamRegistry) emitError(session *streamSession, message string) {
	if r.Emitter == nil {
		return
	}
	r.Emitter.EmitStreamError(session.sessionID, message)
}
