package mcp

// cursor.go：digest 会话翻页游标（设计 §3 原语 4）。
//
// `kafka_messages_digest` 成功后把定位字段（topic-partition-offset + 可选
// key/投影字段）物化进进程内会话（上限 1 万条，TTL 10 分钟，LRU ≤8 会话）；
// AI 用 `kafka_cursor_next {cursorId, n≤20}` 分批取行——条件不重发、远端
// 不重扫。纯逻辑：时间由调用方注入，便于单测覆盖 TTL/LRU/上限边界。

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"sync"
	"time"
)

// CursorRow 物化的一行（定位字段 topic/partition/offset 不截断；key 与
// fields 为投影字段，物化时已过单元格截断宽度）。
type CursorRow struct {
	Topic     string            `json:"topic"`
	Partition int32             `json:"partition"`
	Offset    int64             `json:"offset"`
	Key       string            `json:"key,omitempty"`
	Fields    map[string]string `json:"fields,omitempty"`
}

// anchor 返回定位串（partition/offset），供摘要/调试。
func (row CursorRow) Anchor() string {
	return fmt.Sprintf("%s[%d]@%d", row.Topic, row.Partition, row.Offset)
}

// CursorSession 一次 digest 物化的会话。
type CursorSession struct {
	ID        string      `json:"id"`
	Topic     string      `json:"topic,omitempty"`
	Rows      []CursorRow `json:"rows"`
	Truncated bool        `json:"truncated"` // 物化时超 maxRows 截断
	Offset    int         `json:"-"`         // 下一批起点（会话内续读）
	ExpiresAt time.Time   `json:"-"`
}

// CursorStore digest 会话表。并发安全。
type CursorStore struct {
	mu       sync.Mutex
	ttl      time.Duration
	capacity int
	maxRows  int
	sessions map[string]*CursorSession
	order    []string
}

// NewCursorStore 创建游标表（参数 ≤0 时用设计默认：TTL 10 分钟、8 会话、
// 1 万行上限）。
func NewCursorStore(ttl time.Duration, capacity, maxRows int) *CursorStore {
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	if capacity <= 0 {
		capacity = 8
	}
	if maxRows <= 0 {
		maxRows = 10000
	}
	return &CursorStore{
		ttl:      ttl,
		capacity: capacity,
		maxRows:  maxRows,
		sessions: map[string]*CursorSession{},
	}
}

// Put 物化一次 digest 的行（超 maxRows 截断并置 Truncated）；新会话把最旧
// 会话按 LRU 淘汰。id 冲突概率可忽略（16 字节随机），冲突时旧会话被覆盖。
func (s *CursorStore) Put(rows []CursorRow, topic string, now time.Time) *CursorSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(now)

	truncated := false
	if len(rows) > s.maxRows {
		rows = append([]CursorRow(nil), rows[:s.maxRows]...)
		truncated = true
	} else {
		rows = append([]CursorRow(nil), rows...)
	}
	session := &CursorSession{
		ID:        newCursorID(),
		Topic:     topic,
		Rows:      rows,
		Truncated: truncated,
		ExpiresAt: now.Add(s.ttl),
	}
	s.sessions[session.ID] = session
	s.order = append(s.order, session.ID)
	for len(s.order) > s.capacity {
		delete(s.sessions, s.order[0])
		s.order = s.order[1:]
	}
	return session
}

// NextRequest 分批取行参数（n ≤0 时用缺省 20；n >20 clamp 到 20）。
type NextRequest struct {
	N      int
	Offset int // <0 = 续读会话内游标
}

// NextResult 一批行 + 续读位置；done=true 表示没有更多行。
type NextResult struct {
	Rows       []CursorRow
	Offset     int
	NextOffset int
	Done       bool
}

// SetTTL 运行时调整会话 TTL（mcp/settings/set cursorTtlSecs 后生效于
// 新物化的会话；既有会话的 ExpiresAt 已物化，不回溯）。
func (s *CursorStore) SetTTL(ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ttl = ttl
}

// SetCapacity 运行时调整 LRU 容量：立即淘汰最旧会话收敛到新上限
// （mcp/settings/set maxCursorSessions 后生效）。
func (s *CursorStore) SetCapacity(capacity int) {
	if capacity <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.capacity = capacity
	for len(s.order) > s.capacity {
		delete(s.sessions, s.order[0])
		s.order = s.order[1:]
	}
}

// Next 分批取行：offset<0 时续读会话内游标（AI 不需要自己记 offset）。
// 命中即顶到淘汰序队尾（真 LRU：持续翻页的活跃会话不被纯插入序淘汰，
// ldap 同构）。会话过期（expired，读取时顺手清除）或不存在（unknown）
// 时不返回行。
func (s *CursorStore) Next(id string, req NextRequest, now time.Time) (*NextResult, LookupStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	session, ok := s.sessions[id]
	if !ok {
		return nil, LookupUnknown
	}
	if !now.Before(session.ExpiresAt) {
		s.removeLocked(id)
		return nil, LookupExpired
	}
	s.touchLocked(id)
	n := req.N
	if n <= 0 {
		n = 20
	}
	if n > 20 {
		n = 20
	}
	offset := req.Offset
	if offset < 0 {
		offset = session.Offset
	}
	if offset > len(session.Rows) {
		offset = len(session.Rows)
	}
	end := offset + n
	if end > len(session.Rows) {
		end = len(session.Rows)
	}
	rows := append([]CursorRow(nil), session.Rows[offset:end]...)
	session.Offset = end
	return &NextResult{
		Rows:       rows,
		Offset:     offset,
		NextOffset: end,
		Done:       end >= len(session.Rows),
	}, LookupFound
}

// pruneLocked 清除过期会话（调用方持锁）。
func (s *CursorStore) pruneLocked(now time.Time) {
	kept := s.order[:0]
	for _, id := range s.order {
		if session, ok := s.sessions[id]; ok && now.Before(session.ExpiresAt) {
			kept = append(kept, id)
		} else {
			delete(s.sessions, id)
		}
	}
	s.order = kept
}

// removeLocked 按命中删除（调用方持锁）。
func (s *CursorStore) removeLocked(id string) {
	delete(s.sessions, id)
	for index, existing := range s.order {
		if existing == id {
			s.order = append(s.order[:index], s.order[index+1:]...)
			break
		}
	}
}

// touchLocked 命中续读时把会话顶到淘汰序队尾（真 LRU：容量淘汰看最近
// 使用而非纯插入序，活跃会话不被误逐）。调用方持锁。
func (s *CursorStore) touchLocked(id string) {
	for index, existing := range s.order {
		if existing == id {
			s.order = append(s.order[:index], s.order[index+1:]...)
			break
		}
	}
	s.order = append(s.order, id)
}

// newCursorID 生成 "cur-<hex16>" 会话 id。crypto/rand 失败退化为纳秒
// 时间戳（cursorId 是进程内会话键而非安全令牌，同纳秒冲突仅覆盖会话）。
func newCursorID() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return "cur-" + strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return "cur-" + hex.EncodeToString(buf)
}
