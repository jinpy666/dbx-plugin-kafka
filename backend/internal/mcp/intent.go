package mcp

// intent.go：UI intent 状态表（设计 §1）。
//
// sidecar 进程内 map[intentId]{params, status, summary, expiry}：
// TTL 60s、只留最近 20 条（LRU）。前端经 `kafka/ui/state/report` 回报
// applied/rejected + summary；无 intentId 的 report 是快照型（前端在关键
// 动作后主动上报），`kafka_ui_state` 不带 intentId 时返回最新快照。
//
// 纯逻辑：时间由调用方注入（now 参数），便于单测覆盖 TTL/LRU 边界。

import (
	"sync"
	"time"
)

// IntentState intent 生命周期三态（pending → applied|rejected；expired 由
// 读取方按 expiry 推导，不落存储）。
type IntentState string

const (
	IntentPending  IntentState = "pending"
	IntentApplied  IntentState = "applied"
	IntentRejected IntentState = "rejected"
)

// Intent 单条 intent 记录。
type Intent struct {
	ID        string         `json:"intentId"`
	Action    string         `json:"action"`
	Params    map[string]any `json:"params,omitempty"`
	State     IntentState    `json:"state"`
	Summary   map[string]any `json:"summary,omitempty"`
	Reason    string         `json:"reason,omitempty"`
	ExpiresAt time.Time      `json:"-"` // TTL 判定用，不出工具响应
}

// IntentStore intent 状态表 + 最新 UI 快照。并发安全。
type IntentStore struct {
	mu       sync.Mutex
	ttl      time.Duration
	capacity int
	items    map[string]*Intent
	order    []string // 插入序（LRU 淘汰最旧）
	snapshot map[string]any
}

// NewIntentStore 创建 intent 状态表（ttl ≤0 或 capacity ≤0 用设计默认：
// 60s / 20 条）。
func NewIntentStore(ttl time.Duration, capacity int) *IntentStore {
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	if capacity <= 0 {
		capacity = 20
	}
	return &IntentStore{
		ttl:      ttl,
		capacity: capacity,
		items:    map[string]*Intent{},
	}
}

// Register 登记 pending intent；同 id 重发覆盖并视为最新（LRU 顶到队尾）。
func (s *IntentStore) Register(id, action string, params map[string]any, now time.Time) *Intent {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(now)
	if _, exists := s.items[id]; exists {
		s.removeLocked(id)
	}
	intent := &Intent{
		ID:        id,
		Action:    action,
		Params:    params,
		State:     IntentPending,
		ExpiresAt: now.Add(s.ttl),
	}
	s.items[id] = intent
	s.order = append(s.order, id)
	for len(s.order) > s.capacity {
		delete(s.items, s.order[0])
		s.order = s.order[1:]
	}
	return intent
}

// Report 回报 intent 终态（applied/rejected）；不存在或已过期返回 false。
func (s *IntentStore) Report(id string, state IntentState, summary map[string]any, reason string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	intent, ok := s.items[id]
	if !ok || !now.Before(intent.ExpiresAt) {
		if ok {
			s.removeLocked(id)
		}
		return false
	}
	if state != IntentApplied && state != IntentRejected {
		return false
	}
	intent.State = state
	intent.Summary = summary
	intent.Reason = reason
	return true
}

// LookupStatus Get 的三态结果。
type LookupStatus string

const (
	LookupFound   LookupStatus = "found"
	LookupExpired LookupStatus = "expired"
	LookupUnknown LookupStatus = "unknown"
)

// Get 读取 intent：found（含终态/pending）、expired（存在但过 TTL，读取时
// 顺手清除）、unknown（从未登记）。返回值拷贝（评审 H-1）：指针逃逸会让
// 轮询方在锁外读 State/Summary/Reason，与 Report 的持锁写构成数据竞争；
// Summary/Params 发布后只被整体替换、不在原 map 上改写，浅拷贝即安全。
func (s *IntentStore) Get(id string, now time.Time) (Intent, LookupStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	intent, ok := s.items[id]
	if !ok {
		return Intent{}, LookupUnknown
	}
	if !now.Before(intent.ExpiresAt) {
		s.removeLocked(id)
		return Intent{}, LookupExpired
	}
	return *intent, LookupFound
}

// SetSnapshot 覆盖最新 UI 快照（快照型 report，无 intentId）；写入即复制，
// 调用方后续改动不影响已存快照。
func (s *IntentStore) SetSnapshot(snapshot map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if snapshot == nil {
		s.snapshot = nil
		return
	}
	copied := make(map[string]any, len(snapshot))
	for key, value := range snapshot {
		copied[key] = value
	}
	s.snapshot = copied
}

// Snapshot 返回最新 UI 快照（无则 nil）。
func (s *IntentStore) Snapshot() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshot == nil {
		return nil
	}
	out := make(map[string]any, len(s.snapshot))
	for key, value := range s.snapshot {
		out[key] = value
	}
	return out
}

// pruneLocked 清除过期条目（调用方持锁）。
func (s *IntentStore) pruneLocked(now time.Time) {
	kept := s.order[:0]
	for _, id := range s.order {
		if intent, ok := s.items[id]; ok && now.Before(intent.ExpiresAt) {
			kept = append(kept, id)
		} else {
			delete(s.items, id)
		}
	}
	s.order = kept
}

// removeLocked 按命中删除（调用方持锁）。
func (s *IntentStore) removeLocked(id string) {
	delete(s.items, id)
	for index, existing := range s.order {
		if existing == id {
			s.order = append(s.order[:index], s.order[index+1:]...)
			break
		}
	}
}
