package mcp

// confirm.go：两阶段写确认令牌（设计 §4）。
//
// 强制两阶段写（kafka：topics/delete、groups/offsets/reset、
// topics/records/clear）：无 confirmToken 返回 preview + 一次性 confirmToken
//（60s TTL，参数 hash 绑定）；带 token 且 hash 一致才执行；参数被改即作废
// 重开预览。
// 纯逻辑：时间由调用方注入，便于单测覆盖过期/hash 失配/一次性消费。

import (
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"time"
)

// ConfirmTTL 设计 §4：一次性令牌 60 秒。
const ConfirmTTL = 60 * time.Second

// ConfirmResult Consume 的四种结果。
type ConfirmResult string

const (
	ConfirmOK           ConfirmResult = "ok"
	ConfirmUnknown      ConfirmResult = "unknown"
	ConfirmExpired      ConfirmResult = "expired"
	ConfirmHashMismatch ConfirmResult = "hash_mismatch"
)

// ConfirmEntry 已签发的令牌。
type ConfirmEntry struct {
	ParamHash string
	ExpiresAt time.Time
}

// ConfirmStore 令牌表（进程内，一次性消费）。并发安全。
type ConfirmStore struct {
	mu    sync.Mutex
	ttl   time.Duration
	items map[string]ConfirmEntry
}

// NewConfirmStore 创建令牌表（TTL 缺省 60s；mcp/settings/set 的
// confirmTtlSecs 可调 10–600，ldap/files 同名同范围）。
func NewConfirmStore() *ConfirmStore {
	return &ConfirmStore{ttl: ConfirmTTL, items: map[string]ConfirmEntry{}}
}

// SetTTL 运行期调整令牌 TTL（ldap 同构）：下一次 Issue 生效，已签发令牌
// 的 ExpiresAt 不追溯（MCP_ACCEPTANCE §5/§8）。≤0 的值被忽略（防误配清零）。
func (s *ConfirmStore) SetTTL(ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ttl = ttl
}

// TTL 当前令牌 TTL（过期报文携带实际生效值用）。
func (s *ConfirmStore) TTL() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ttl
}

// HashParams 计算 write 请求的绑定 hash（sha256 hex）。输入为归一化后的
// canonical JSON 字节（Go encoding/json 对 struct 确定性输出）。
func HashParams(canonical []byte) string {
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}

// Issue 签发一次性令牌（c-<hex12>，TTL 见 SetTTL/缺省 60s）。签发前顺手
// 清理已过期未消费的令牌（churn 防线：大量「只要预览不确认」的调用不能
// 无界撑大令牌表——Consume 只在显式消费时删除，过期即弃的令牌没有别的
// 删除路径；ldap 同构）。
func (s *ConfirmStore) Issue(paramHash string, now time.Time) (string, time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(now)
	random, err := randomHex(6)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("issue confirmToken: %w", err)
	}
	token := "c-" + random
	expiresAt := now.Add(s.ttl)
	s.items[token] = ConfirmEntry{ParamHash: paramHash, ExpiresAt: expiresAt}
	return token, expiresAt, nil
}

// pruneLocked 清除已过期的未消费令牌（调用方持锁；map 遍历中删除安全）。
func (s *ConfirmStore) pruneLocked(now time.Time) {
	for token, entry := range s.items {
		if !now.Before(entry.ExpiresAt) {
			delete(s.items, token)
		}
	}
}

// Consume 校验并消费令牌：
//   - ok：token 存在、未过期、hash 一致（消费后即删除，一次性）；
//   - expired：存在但过 TTL（消费删除）；
//   - hash_mismatch：token 有效但参数被改（消费删除，须重开预览）；
//   - unknown：token 不存在（从未签发/已消费/已过期清理）。
func (s *ConfirmStore) Consume(token, paramHash string, now time.Time) ConfirmResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.items[token]
	if !ok {
		return ConfirmUnknown
	}
	delete(s.items, token) // 一次性：无论哪种失败都作废
	if !now.Before(entry.ExpiresAt) {
		return ConfirmExpired
	}
	if entry.ParamHash != paramHash {
		return ConfirmHashMismatch
	}
	return ConfirmOK
}

// randomHex n 字节随机 hex。crypto/rand 失败返回 error 而非回退时间戳
// （评审 L：时间戳回退对 confirmToken 是可猜值——宁可拒签，不降级随机源）。
func randomHex(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := io.ReadFull(cryptorand.Reader, buf); err != nil {
		return "", fmt.Errorf("crypto/rand unavailable: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
