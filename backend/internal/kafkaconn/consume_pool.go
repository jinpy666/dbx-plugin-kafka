package kafkaconn

// consume_pool.go：一次性消费 client 复用池（消费启动提速）。
//
// 背景：kgo client 冷启动要完整走 TCP + TLS 握手 + SASL + metadata +
// ListOffsets，跨境 SASL_SSL 链路实测 5-11s，而一次性消费默认扫描窗口仅 5s，
// 启动没完成就被掐掉（表现为「一条都拉不到」）。admin 类调用已有共享 client
//（client.go），consume 因携带 per-request 消费 opts 一直用完即关；本池按
//「连接 + 消费形状」缓存消费 client，复用前 drain 残留缓冲并经 SetOffsets
// 重置回策略起点，重复消费降到亚秒级。
//
// 复用面（保守）：非 group 模式且 offsetStrategy ∈ {空, default, earliest,
// latest}。group 模式（再均衡/提交语义）与 timestamp/committed/offset（需
// 服务端解析起点）维持 per-request 新建。重置目标经 kadm 全分区边界
// offset（具体值 + leader epoch）SetOffsets——哨兵值/部分分区的 SetOffsets
// 在 franz-go 直接消费下静默不生效（见 resetConsumeClientForReuse 注释）。
//
// 并发：entry.inUse 排他——命中但占用中、或未命中，调用方都走「新建临时
// client」路径（不等待、不入池）。seenParts/atStart/resetFailed 只在 inUse
// 持有期间访问（池锁仅管 inUse 标记与表操作），无需条目级锁。

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// 池参数。
const (
	// consumePoolIdleTTL 空闲回收（release 时惰性扫描，无复用收益即关）。
	consumePoolIdleTTL = 2 * time.Minute
	// consumePoolMaxEntries 全服务池上限（不同连接/形状组合）；全占用时允许
	// 临时超限，release 再收敛。
	consumePoolMaxEntries = 8
	// consumeResetListTimeout 复用重置时 ListOffsets 的独立预算（reset 不吃
	// 用户扫描窗口，也不应无限等——失败即放弃该条目 fallback 新建）。
	consumeResetListTimeout = 10 * time.Second
)

// consumePoolEntry 池条目。inUse 由池锁保护；其余字段仅 inUse 持有者访问。
type consumePoolEntry struct {
	client *kgo.Client
	connID string
	sig    string

	// inUse 占用标记（池锁内改；其余字段仅 inUse 持有者访问）。
	inUse bool
	// atStart reset 目标（true=start/earliest，false=end/latest），acquire 刷新。
	atStart bool
	// seenParts 该 client 上已观察到有记录的分区（诊断记录；重置已改为
	// kadm 全分区边界 seek，不再依赖此集合）。
	seenParts map[int32]struct{}
	// resetFailed 复用重置失败后不再入池复用。
	resetFailed bool
	// lastUsedAt unix ms（驱逐判据）。
	lastUsedAt int64
}

// consumeReusePolicy 判定消费请求是否走复用池及 reset 目标。
// 返回 (ok, atStart)；ok=false 维持 per-request 新建。
func consumeReusePolicy(offsetStrategy, groupID string) (bool, bool) {
	if trimSpace(groupID) != "" {
		return false, false
	}
	switch normalizeOffsetStrategyName(offsetStrategy) {
	case "", "default", "earliest", "start":
		return true, true
	case "latest", "end":
		return true, false
	default:
		return false, false
	}
}

// consumeClientSignature 消费形状签名（不含连接配置——断连即清池）。
// 仅对 reusable 输入有意义（timestamp/committed/offset 不入池）。
func consumeClientSignature(topic, offsetStrategy string, partitions []int32, isolationLevel string) string {
	parts := make([]string, 0, 4+len(partitions))
	parts = append(parts,
		topic,
		normalizeOffsetStrategyName(offsetStrategy),
		isolationLevel,
		strconv.Itoa(len(partitions)),
	)
	for _, partition := range partitions {
		parts = append(parts, strconv.FormatInt(int64(partition), 10))
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:8])
}

func consumePoolKey(connID, signature string) string {
	return connID + "\x00" + signature
}

// consumePoolAcquire 取池条目（不管是否占用——调用方据 inUse 决定复用或
// 新建临时 client；置位也在这里完成，保证「查+占」原子）。
// 返回 (entry, reusable)；entry 非 nil 且 reusable=true 表示已置为占用。
func (s *Service) consumePoolAcquire(connID, signature string, atStart bool) (*consumePoolEntry, bool) {
	s.consumePoolMu.Lock()
	defer s.consumePoolMu.Unlock()
	entry := s.consumePool[consumePoolKey(connID, signature)]
	if entry == nil || entry.inUse || entry.resetFailed {
		return entry, false
	}
	entry.inUse = true
	entry.atStart = atStart
	entry.lastUsedAt = time.Now().UnixMilli()
	return entry, true
}

// consumePoolPut 新建 client 入池并返回占用中的条目（所有权移交池）。
// 同键旧条目空闲则替换（关闭旧 client）；旧条目 inUse 时不替换也不入池、
// 返回 nil——所有权留给调用方按临时 client 用完即关（KAFKA-M1：此前不看
// inUse 直接关旧条目，并发同形状消费会关掉正在 poll 的 client；对齐
// evictConsumePoolLocked 跳过 inUse 的语义）。池超限时先驱逐最旧空闲条目。
func (s *Service) consumePoolPut(connID, signature string, client *kgo.Client) *consumePoolEntry {
	s.consumePoolMu.Lock()
	defer s.consumePoolMu.Unlock()

	key := consumePoolKey(connID, signature)
	if old := s.consumePool[key]; old != nil {
		if old.inUse {
			return nil
		}
		s.closeConsumePoolEntry(old, key)
	}
	entry := &consumePoolEntry{
		client:     client,
		connID:     connID,
		sig:        signature,
		seenParts:  map[int32]struct{}{},
		inUse:      true,
		lastUsedAt: time.Now().UnixMilli(),
	}
	s.consumePool[key] = entry
	s.evictConsumePoolLocked(key)
	return entry
}

// closeConsumePoolEntry 关闭条目 client 并从池摘除（须持池锁）。
func (s *Service) closeConsumePoolEntry(entry *consumePoolEntry, key string) {
	if s.consumePool[key] == entry {
		delete(s.consumePool, key)
	}
	if entry.client != nil {
		entry.client.Close()
		entry.client = nil
	}
}

// evictConsumePoolLocked 池超限时驱逐最旧空闲条目（须持池锁；put 后调用，
// 保证空闲态池大小收敛到上限；占用中不动，全占用时允许临时超限）。
func (s *Service) evictConsumePoolLocked(preserveKey string) {
	for len(s.consumePool) > consumePoolMaxEntries {
		oldestKey := ""
		var oldestAt int64 = 1 << 62
		for key, entry := range s.consumePool {
			if key == preserveKey || entry.inUse {
				continue
			}
			if entry.lastUsedAt < oldestAt {
				oldestAt = entry.lastUsedAt
				oldestKey = key
			}
		}
		if oldestKey == "" {
			return
		}
		s.closeConsumePoolEntry(s.consumePool[oldestKey], oldestKey)
	}
}

// consumePoolRelease 释放占用条目：吸收本轮观察到的分区；不健康（reset
// 失败等）标记不再复用；顺手回收空闲超 TTL 条目。
//
// 字段写入全部在池锁内完成（评审 H-2：inUse/lastUsedAt 此前在锁外写，与
// Acquire/Put/evict 的锁内读写构成 data race；seenParts 虽按不变式仅持有期
// 访问，一并收拢在锁内使「释放」成为单一线性化点）。
func (s *Service) consumePoolRelease(entry *consumePoolEntry, observed map[int32]struct{}, healthy bool) {
	if entry == nil {
		return
	}
	s.consumePoolMu.Lock()
	defer s.consumePoolMu.Unlock()
	for partition := range observed {
		entry.seenParts[partition] = struct{}{}
	}
	if !healthy {
		entry.resetFailed = true
	}
	entry.inUse = false
	entry.lastUsedAt = time.Now().UnixMilli()
	now := entry.lastUsedAt
	for key, candidate := range s.consumePool {
		if candidate.inUse || now-candidate.lastUsedAt <= consumePoolIdleTTL.Milliseconds() {
			continue
		}
		s.closeConsumePoolEntry(candidate, key)
	}
}

// consumePoolCloseFor 关闭某连接全部池条目（Disconnect 挂钩）。
func (s *Service) consumePoolCloseFor(connID string) {
	s.consumePoolMu.Lock()
	defer s.consumePoolMu.Unlock()
	for key, entry := range s.consumePool {
		if entry.connID == connID {
			s.closeConsumePoolEntry(entry, key)
		}
	}
}

// consumePoolCloseAll 关闭全部池条目（CloseAll 挂钩）。
func (s *Service) consumePoolCloseAll() {
	s.consumePoolMu.Lock()
	defer s.consumePoolMu.Unlock()
	for key, entry := range s.consumePool {
		s.closeConsumePoolEntry(entry, key)
	}
}

// consumeStartupBudget 启动期预算：与用户扫描窗口分离，取 max(2×窗口, 12s)
// ——跨境链路冷启动实测 5-11s，12s 下限保证最差链路也能完成首次建连。
func consumeStartupBudget(fetchWindow time.Duration) time.Duration {
	budget := 2 * fetchWindow
	if budget < 12*time.Second {
		budget = 12 * time.Second
	}
	return budget
}

// resetConsumeClientForReuse 复用前重置：drain 丢弃缓冲记录（50ms 上限——
// 目标只是清掉 client 内存里已 fetch 未 poll 的残留，不需要等新数据），再经
// kadm 拉目标 topic **全部分区**的边界 offset 后一次性 SetOffsets。fetch 错误
// 返回 error（调用方放弃该条目 fallback 新建）。
//
// 必须全部分区覆盖 + 具体 offset，不能按"已见分区"+ 哨兵值 seek：franz-go
// v1.20.x 直接消费下，SetOffsets 的 map 未覆盖全部在消费分区时整个 seek
// 静默丢失（2026-09-13 真集群最小复现：只 seek 有记录的分区 → 第二次
// earliest 扫描恒 0 条且无任何错误；全覆盖具体 offset → 正常重扫）。
// ListOffsets 复用同一 client 的连接（kadm 适配层），代价一次 broker 往返。
func resetConsumeClientForReuse(client *kgo.Client, topic string, atStart bool) error {
	deadline := time.Now().Add(50 * time.Millisecond)
	var drainErr error
	for time.Now().Before(deadline) {
		pollCtx, cancel := context.WithDeadline(context.Background(), deadline)
		fetches := client.PollRecords(pollCtx, 512)
		cancel()
		fetches.EachError(func(_ string, _ int32, err error) {
			// poll 窗口到期会作为 fetch 错误出现，属正常退出条件而非故障。
			if !isDeadline(err) {
				drainErr = err
			}
		})
		if fetches.NumRecords() == 0 {
			break
		}
	}
	if drainErr != nil {
		return drainErr
	}
	admin := kadm.NewClient(client)
	ctx, cancel := context.WithTimeout(context.Background(), consumeResetListTimeout)
	defer cancel()
	var listed kadm.ListedOffsets
	var listErr error
	if atStart {
		listed, listErr = admin.ListStartOffsets(ctx, topic)
	} else {
		listed, listErr = admin.ListEndOffsets(ctx, topic)
	}
	if listErr != nil {
		return errf("list %s offsets for consumer reset: %w", map[bool]string{true: "start", false: "end"}[atStart], listErr)
	}
	parts := make(map[int32]kgo.EpochOffset)
	listed.Each(func(listed kadm.ListedOffset) {
		// Offset 即目标边界（start 或 end 的具体 offset，>=0；-1 表示该
		// 分区无可消费边界，跳过——空分区无数据可扫）；LeaderEpoch 是该
		// offset 处的 leader epoch，比哨兵 -1 更精确。
		if listed.Err != nil || listed.Offset < 0 {
			return
		}
		parts[listed.Partition] = kgo.EpochOffset{Epoch: listed.LeaderEpoch, Offset: listed.Offset}
	})
	if len(parts) == 0 {
		return errf("no partitions listed for topic %q; cannot reset pooled consumer", topic)
	}
	client.SetOffsets(map[string]map[int32]kgo.EpochOffset{topic: parts})
	return nil
}
