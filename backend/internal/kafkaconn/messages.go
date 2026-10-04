package kafkaconn

// messages.go：消息生产/消费/过滤/解码/导出（契约 §5.2/§5.3）。
// 逻辑对照 tiny-rdm kafka_service.go：Produce :923、consumeMessages :1091、
// offset 策略族 :1994-2172、校验 :2174、匹配器 :2233、过滤引擎 :2508-2930、
// 解码解压 :2986-3069、导出序列化 :2356-2507。
//
// 关键改造（tinyrdm 已知 bug 补齐）：value 不做 string() 直转——
// valueText 恒为 UTF-8 安全预览（非法字节替换 U+FFFD，512KB 上限截断）、
// valueBase64 同用 512KB 上限截断，双双标记 truncated；key 非法 UTF-8 时以
// keyBase64 透出。解压路径带 maxDecodedBytes 上限（解压炸弹防护）。

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/twmb/franz-go/pkg/kgo"
)

// 消息常量（契约 §5.3）。
const (
	// maxMessageBytes 单条消息体上限（超出截断并标记 truncated）。
	maxMessageBytes = 512 * 1024
	// maxProduceCount 单次生产上限。
	maxProduceCount = 1000
	// maxExportRecords 导出上限。
	maxExportRecords = 10000
	// maxDecodedBytes 解压输出上限（KAFKA-H1 解压炸弹防护）：单条恶意消息
	// 经 gzip/lz4/zstd/snappy 可膨胀上万倍，无上限 ReadAll 可把 sidecar 打到
	// OOM。超出即报错，按现有 DecodeError 语义进消息不中断消费。
	maxDecodedBytes = 16 * 1024 * 1024
	// produceTimeout 是 produce 的两阶段统一超时预算（KAFKA-L5）：schema
	// 编码与 ProduceSync 此前一处 adminTimeout(20s) 一处硬编码 30s。30s 覆盖
	// acks=all 的最慢副本同步，宿主 invoke 超时（默认 10s+）之外由 ctx 取消
	// 兜底。
	produceTimeout = 30 * time.Second
)

// ConsumeParams 是一次性与流式消费共用参数（契约 §5.3 全字段）。
type ConsumeParams struct {
	ConnectionID string `json:"connectionId"`
	Topic        string `json:"topic"`
	GroupID      string `json:"groupId,omitempty"`
	// OffsetStrategy：latest | earliest | committed | timestamp | offset。
	OffsetStrategy string `json:"offsetStrategy,omitempty"`
	// OffsetTime：RFC3339 或 unix ms（strategy=timestamp 时必填）。
	OffsetTime string  `json:"offsetTime,omitempty"`
	Partitions []int32 `json:"partitions,omitempty"`
	// PartitionOffsets strategy=offset 时必填：partition → offset。
	PartitionOffsets map[int32]int64 `json:"partitionOffsets,omitempty"`
	Limit            int             `json:"limit,omitempty"`
	TimeoutMs        int             `json:"timeoutMs,omitempty"`
	MaxScanRecords   int             `json:"maxScanRecords,omitempty"`
	// IsolationLevel：read_uncommitted（默认）| read_committed。
	IsolationLevel string `json:"isolationLevel,omitempty"`
	// Commit 为 true 时禁一切过滤且必须 groupId（§5.3 互斥）。
	Commit bool `json:"commit,omitempty"`

	// 过滤：filter 全文（key+value+headers 拼接），其余分通道；
	// matchMode：contains | prefix | exact | regex。
	Filter        string               `json:"filter,omitempty"`
	KeyFilter     string               `json:"keyFilter,omitempty"`
	ValueFilter   string               `json:"valueFilter,omitempty"`
	HeaderFilter  string               `json:"headerFilter,omitempty"`
	MatchMode     string               `json:"matchMode,omitempty"`
	FieldFilters  []ConsumeFieldFilter `json:"fieldFilters,omitempty"`
	TimestampFrom *int64               `json:"timestampFrom,omitempty"`
	TimestampTo   *int64               `json:"timestampTo,omitempty"`
	OffsetFrom    *int64               `json:"offsetFrom,omitempty"`
	OffsetTo      *int64               `json:"offsetTo,omitempty"`

	// 解码：decode none|base64（二次解码）；decompression 一次解压。
	Decode        string `json:"decode,omitempty"`
	Decompression string `json:"decompression,omitempty"`

	// ConsumeID 是本次消费的取消句柄（前端生成的 uuid，可选）：非空时登记
	// 进取消注册表，kafka/messages/consume/cancel 可提前中断扫描窗口。
	ConsumeID string `json:"consumeId,omitempty"`

	// SkipValueBase64 跳过 valueBase64 通道（评审 H-1：digest 聚合只读
	// valueText，base64 是纯冤枉驻留；工作台消费保持双通道不变）。
	SkipValueBase64 bool `json:"skipValueBase64,omitempty"`
	// RetentionByteBudget 命中消息留存的 value 字节预算（评审 H-1：0 = 无
	// 预算即契约原语义；超出即停止留存但 matched 计数不受影响，由
	// ConsumeResult.RetentionTruncated 标记——digest 大扫描防 OOM）。
	RetentionByteBudget int `json:"retentionByteBudget,omitempty"`

	// Schema 可选（Phase 2）：SR 挂载 —— 解码 Confluent wire format 载荷为
	// JSON 文本；命中消息附加 schemaId/schemaSubject/schemaVersion 字段，
	// 解码失败置 decodeError（不中断消费）。
	Schema *SchemaRef `json:"schema,omitempty"`
}

// ConsumeFieldFilter 字段级过滤（三通道 + JSON path + 数值比较）。
type ConsumeFieldFilter struct {
	// Source：value | key | header | topic | partition | offset | timestamp。
	Source string `json:"source,omitempty"`
	// Path JSON path（value/key，如 $.user.id 或 items[0]）；header 时为
	// header 名。
	Path string `json:"path,omitempty"`
	// Operator：contains|prefix|exact|regex|exists|not_exists|gt|gte|lt|lte。
	Operator string `json:"operator,omitempty"`
	Value    string `json:"value,omitempty"`
	Enabled  *bool  `json:"enabled,omitempty"`
}

// ConsumedMessage 消息形状（契约 §5.3 二进制保真）。
type ConsumedMessage struct {
	Topic       string `json:"topic"`
	Partition   int32  `json:"partition"`
	Offset      int64  `json:"offset"`
	Timestamp   int64  `json:"timestamp,omitempty"`
	LeaderEpoch int32  `json:"leaderEpoch,omitempty"`
	// Key 为合法 UTF-8 时输出；否则 keyBase64。
	Key       string `json:"key,omitempty"`
	KeyBase64 string `json:"keyBase64,omitempty"`
	// ValueText 恒为 UTF-8 安全预览（非法字节替换；超 512KB 截断）；
	// ValueBase64 同用 512KB 上限截断。双通道与 Truncated 标志一致。
	ValueText   string            `json:"valueText"`
	ValueBase64 string            `json:"valueBase64"`
	Headers     map[string]string `json:"headers,omitempty"`
	// Truncated 标记 value 超上限被截断。
	Truncated bool `json:"truncated,omitempty"`
	// Committed 标记本次请求带 commit。
	Committed bool `json:"committed,omitempty"`
	// DecodeError 保存解码/解压失败信息。
	DecodeError string `json:"decodeError,omitempty"`
	// Schema 定位信息（Phase 2：ConsumeParams.schema 命中时填充）。
	SchemaID      int64  `json:"schemaId,omitempty"`
	SchemaSubject string `json:"schemaSubject,omitempty"`
	SchemaVersion int64  `json:"schemaVersion,omitempty"`
}

// ConsumeResult 对应 kafka/messages/consume。
type ConsumeResult struct {
	Messages             []ConsumedMessage `json:"messages"`
	Scanned              int               `json:"scanned"`
	Matched              int               `json:"matched"`
	Limited              bool              `json:"limited"`
	HasMore              bool              `json:"hasMore"`
	NextPartitionOffsets map[int32]int64   `json:"nextPartitionOffsets"`
	// RetentionTruncated 标记留存超 RetentionByteBudget 预算（命中计数完整，
	// messages 为预算内子集——cursor/样本行只覆盖留存部分）。
	RetentionTruncated bool `json:"retentionTruncated,omitempty"`
	// TimedOut 标记扫描窗口（timeoutMs）到点退出：hasMore 为真可能只是超时
	// 而非「还有更多」——调用方（UI/MCP）据此给出可行动提示而非静默空结果。
	TimedOut bool `json:"timedOut,omitempty"`
	// Cancelled 标记消费被 consume/cancel 提前中断（工作台停止按钮）：
	// messages 为停止前的部分结果，scanned/matched 计数完整。
	Cancelled bool `json:"cancelled,omitempty"`
}

// ProduceRequest 对应 kafka/messages/produce。
type ProduceRequest struct {
	ConnectionID string            `json:"connectionId"`
	Topic        string            `json:"topic"`
	Key          string            `json:"key,omitempty"`
	Value        string            `json:"value"`
	Headers      map[string]string `json:"headers,omitempty"`
	Partition    *int32            `json:"partition,omitempty"`
	// Count 批量条数（≤1000，默认 1）。
	Count int `json:"count,omitempty"`
	// Compression：none（默认）| gzip | lz4 | zstd | snappy。
	Compression string `json:"compression,omitempty"`

	// --- Phase 2 扩展（冻结契约） ---
	// KeyBase64 可选：key 的 base64（与 key 二选一，二进制 key 保真）。
	KeyBase64 string `json:"keyBase64,omitempty"`
	// ValueBase64 可选：未编码载荷的 base64（与 value 二选一）。
	ValueBase64 string `json:"valueBase64,omitempty"`
	// Schema 可选：SR 挂载 —— 有 schema 时 value/valueBase64 是未编码载荷，
	// sidecar 按 SR 元数据编码为载荷并打包 Confluent wire format
	// （magic byte 0 + 4 字节大端 schemaID + 载荷；PROTOBUF 另含 message
	// index 数组段）。
	Schema *SchemaRef `json:"schema,omitempty"`

	// Acks 投递确认级别："all"（默认，等待全部 ISR）| "1"（仅 leader）。
	// "0"（fire-and-forget）不支持：生产走同步 ProduceSync 语义，franz-go
	// 的 promise 依赖 broker 响应，acks=0 会等到请求超时才失败。
	Acks string `json:"acks,omitempty"`
	// EnableIdempotence 幂等生产开关（Kafka 服务端去重）。缺省/true =
	// franz-go 默认行为（幂等开，要求 acks=all）；false 关闭幂等
	// （DisableIdempotentWrite）。acks=1 时必须显式 false。
	EnableIdempotence *bool `json:"enableIdempotence,omitempty"`

	// Source 操作来源标注（MCP 设计 §4：MCP 写路径 "mcp"；工作台不携带）。
	Source string `json:"source,omitempty"`
}

// ProduceResult 对应 kafka/messages/produce 返回。
type ProduceResult struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Timestamp int64  `json:"timestamp"`
}

// ExportRequest 对应 kafka/messages/export（consume 参数 + format/limit）。
type ExportRequest struct {
	ConsumeParams
	// Format：json（默认）| csv。
	Format string `json:"format,omitempty"`
	// Limit 导出上限（≤10000；同时覆盖 params.Limit）。
	Limit int `json:"limit,omitempty"`
}

// ExportResult 对应 kafka/messages/export。
type ExportResult struct {
	Content     string `json:"content"`
	Filename    string `json:"filename"`
	ContentType string `json:"contentType"`
	Scanned     int    `json:"scanned"`
	Matched     int    `json:"matched"`
	Exported    int    `json:"exported"`
	HasMore     bool   `json:"hasMore"`
	// RetentionTruncated 留存超预算（评审 H-1）：导出为「预算内子集」，
	// 计数（Matched）完整——前端据此提示改小范围分批导出。
	RetentionTruncated bool `json:"retentionTruncated,omitempty"`
}

// Produce 实现 kafka/messages/produce（批量 ≤1000 / headers / 压缩 / 指定分区；
// Phase 2：valueBase64/keyBase64 保真输入 + schema 挂载编码 wire format）。
func (s *Service) Produce(ctx context.Context, req ProduceRequest) (*ProduceResult, error) {
	profile := s.profileOf(req.ConnectionID)
	topic := trimSpace(req.Topic)
	if topic == "" {
		return nil, errf("topic is required")
	}
	if err := ensureWriteAllowed(profile, "messages/produce"); err != nil {
		s.emitAuditSource(req.Source, req.ConnectionID, "produce", topic, "blocked", err.Error())
		return nil, err
	}
	count := normalizeProduceCount(req.Count)
	if req.Partition != nil && *req.Partition < 0 {
		return nil, errf("partition must be greater than or equal to 0")
	}
	codec, ok, err := produceCompressionCodec(req.Compression)
	if err != nil {
		return nil, err
	}
	acks, err := normalizeProduceAcks(req.Acks)
	if err != nil {
		return nil, err
	}
	deliveryOpts, err := produceDeliveryOpts(acks, req.EnableIdempotence)
	if err != nil {
		return nil, err
	}

	// 载荷/键解析：value 与 valueBase64 二选一（同给报错）；key 同理。
	payload, err := producePayloadBytes(req)
	if err != nil {
		return nil, err
	}
	keyBytes := []byte(req.Key)
	if keyB64 := trimSpace(req.KeyBase64); keyB64 != "" {
		if trimSpace(req.Key) != "" {
			return nil, errf("key and keyBase64 are mutually exclusive")
		}
		decoded, decodeErr := base64.StdEncoding.DecodeString(keyB64)
		if decodeErr != nil {
			return nil, fmt.Errorf("keyBase64 is not valid base64: %w", decodeErr)
		}
		keyBytes = decoded
	}

	// schema 挂载：按 SR 元数据编码载荷并打包 wire format（Phase 2）。
	// wire format 编解码仅支持 Confluent：provider=glue → 业务错（Phase 3
	// 门禁，tinyrdm 同款语义）；双配置歧义 → -32602。
	var schemaClient *schemaRegistryClient
	if req.Schema != nil {
		if err := s.schemaMountSupported(req.ConnectionID, req.Schema.Registry, "produce"); err != nil {
			return nil, err
		}
		schemaClient, err = s.confluentClientFor(req.ConnectionID)
		if err != nil {
			return nil, err
		}
	}
	var schemaAuditDetail string

	var extraOpts []kgo.Opt
	if req.Partition != nil {
		extraOpts = append(extraOpts, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	}
	if ok {
		extraOpts = append(extraOpts, kgo.ProducerBatchCompression(codec))
	}
	extraOpts = append(extraOpts, deliveryOpts...)

	client, closeClient, err := s.consumeClient(req.ConnectionID, extraOpts...)
	if err != nil {
		s.emitAuditSource(req.Source, req.ConnectionID, "produce", topic, "error", err.Error())
		return nil, err
	}
	defer closeClient()

	if req.Schema != nil {
		produceCtx, cancel := context.WithTimeout(ctx, produceTimeout)
		var schemaResult SchemaGetResult
		payload, schemaResult, err = encodeForProduce(produceCtx, schemaClient, req.Schema, payload)
		cancel()
		if err != nil {
			s.emitAuditSource(req.Source, req.ConnectionID, "produce", topic, "error", err.Error())
			return nil, err
		}
		schemaAuditDetail = sprintf(" schema=subject:%s,id:%d,version:%d", schemaResult.Subject, schemaResult.ID, schemaResult.Version)
	}

	records := make([]*kgo.Record, 0, count)
	headers := recordHeaders(req.Headers)
	for i := 0; i < count; i++ {
		record := &kgo.Record{
			Topic:   topic,
			Key:     keyBytes,
			Value:   payload,
			Headers: headers,
		}
		if req.Partition != nil {
			record.Partition = *req.Partition
		}
		records = append(records, record)
	}

	produceCtx, cancel := context.WithTimeout(ctx, produceTimeout)
	defer cancel()
	// KAFKA-PR-H1（第二轮审查）：ProduceSync 返回完成序结果，First() 只看
	// 首个完成批次——count>1 按 key 分散多分区（或共享 produceTimeout 中途
	// 到期）时，非首完成批次的失败被整体吞掉：调用方拿到 success、审计记
	// success，落库却缺数据（静默丢数 + 审计假阳性）。FirstErr() = 任一批
	// 次失败即报，并带失败计数。
	results := client.ProduceSync(produceCtx, records...)
	if err := results.FirstErr(); err != nil {
		failed := 0
		for i := range results {
			if results[i].Err != nil {
				failed++
			}
		}
		s.emitAuditSource(req.Source, req.ConnectionID, "produce", topic, "error",
			sprintf("%d/%d failed: %v", failed, count, err))
		return nil, err
	}
	// 契约 §produce：首条消息定位；count>1 时为末条 offset（KAFKA-PR-M1：
	// 此前取完成序首个结果，count>1 时系统性偏小且多分区下字段可来自不同
	// 记录）。锚点 = 最大 offset 的结果。
	written := produceAnchorResult(results)
	s.emitAuditSource(req.Source, req.ConnectionID, "produce", topic, "success", sprintf("count=%d%s", count, schemaAuditDetail))
	return &ProduceResult{
		Topic:     written.Record.Topic,
		Partition: written.Record.Partition,
		Offset:    written.Record.Offset,
		Timestamp: written.Record.Timestamp.UnixMilli(),
	}, nil
}

// produceAnchorResult 从全部成功的结果集中取契约锚点（最大 offset 的结果；
// count=1 即该条；count>1 同分区为末条，跨分区为全局末条写入位置）。
// 空集由调用方保证不发生（ProduceSync 至少返回 count 条）。
func produceAnchorResult(results kgo.ProduceResults) kgo.ProduceResult {
	anchor := results[0]
	for i := 1; i < len(results); i++ {
		if results[i].Record.Offset > anchor.Record.Offset {
			anchor = results[i]
		}
	}
	return anchor
}

// producePayloadBytes 解析 produce 载荷：value 与 valueBase64 二选一
// （同给/均空报错；base64 路径支持二进制载荷保真）。
func producePayloadBytes(req ProduceRequest) ([]byte, error) {
	valueText := req.Value
	valueB64 := trimSpace(req.ValueBase64)
	if valueB64 != "" {
		if trimSpace(valueText) != "" {
			return nil, errf("value and valueBase64 are mutually exclusive")
		}
		decoded, err := base64.StdEncoding.DecodeString(valueB64)
		if err != nil {
			return nil, fmt.Errorf("valueBase64 is not valid base64: %w", err)
		}
		return decoded, nil
	}
	if trimSpace(valueText) == "" {
		return nil, errf("value (or valueBase64) is required")
	}
	return []byte(valueText), nil
}

// Consume 实现 kafka/messages/consume（一次性）。
func (s *Service) Consume(ctx context.Context, params ConsumeParams) (*ConsumeResult, error) {
	result, err := s.consumeMessages(ctx, params)
	if err != nil {
		return nil, err
	}
	return &ConsumeResult{
		Messages:             result.messages,
		Scanned:              result.scanned,
		Matched:              result.matched,
		Limited:              result.limited,
		HasMore:              result.hasMore,
		NextPartitionOffsets: result.nextPartitionOffsets,
		RetentionTruncated:   result.retentionTruncated,
		TimedOut:             result.timedOut,
		Cancelled:            result.cancelled,
	}, nil
}

// Export 实现 kafka/messages/export。
func (s *Service) Export(ctx context.Context, req ExportRequest) (*ExportResult, error) {
	format, err := normalizeExportFormat(req.Format)
	if err != nil {
		return nil, err
	}
	limit, err := exportRecordLimit(req.Limit)
	if err != nil {
		return nil, err
	}
	// 导出禁 commit（tinyrdm 同款：导出是只读回放）；取消句柄同样只属于
	// 一次性消费路径（契约 §4），导出不登记。
	consumeParams := req.ConsumeParams
	consumeParams.Commit = false
	consumeParams.ConsumeID = ""
	consumeParams.Limit = limit

	result, err := s.consumeMessages(ctx, consumeParams)
	if err != nil {
		return nil, err
	}
	content, contentType, err := serializeConsumedMessages(format, result.messages)
	if err != nil {
		return nil, err
	}
	return &ExportResult{
		Content:            content,
		Filename:           exportFilename(trimSpace(consumeParams.Topic), format, time.Now()),
		ContentType:        contentType,
		Scanned:            result.scanned,
		Matched:            result.matched,
		Exported:           len(result.messages),
		HasMore:            result.hasMore,
		RetentionTruncated: result.retentionTruncated,
	}, nil
}

// consumeResult 是 consumeMessages 的内部结果。
type consumeResult struct {
	messages             []ConsumedMessage
	scanned              int
	matched              int
	limited              bool
	hasMore              bool
	timedOut             bool
	cancelled            bool
	retentionTruncated   bool
	nextPartitionOffsets map[int32]int64
}

// consumeRetentionTracker digest 大扫描的留存预算（评审 H-1）：命中消息的
// value 字节累计超预算即拒绝留存（matched 计数不受影响，由调用方置
// RetentionTruncated）。budget<=0 = 无预算（契约原语义）；首条消息恒
// admitted——预算小于单条消息时保证 digest 至少有 1 条样本可用。
type consumeRetentionTracker struct {
	budget   int
	retained int
}

// workbenchRetentionByteBudget 工作台 consume/export 的留存兜底预算：256MiB
// 覆盖满额单条（512KB value）约 500 条、常规消息数万条，不改变典型行为；
// 最坏驻留 ≈ 预算 + 序列化瞬时副本（<1GB），对长驻 sidecar 安全。
const workbenchRetentionByteBudget = 256 << 20

// consumeResponseByteBudget 工作台 consume/export 的响应传输预算（KAFKA-H2）：
// SDK 单行 JSON 上限 8MiB（shared/sdk/go/dbx-plugin-sdk/sdk.go maxJSONBytes），
// 超限整个响应被丢弃——连错误响应都发不出，调用方挂到宿主超时。按 6MiB
// 兜底：valueText + valueBase64 双通道 ≈ 2.4×value/条，再加 headers 与 JSON
// 结构开销。digest 聚合路径响应是聚合结果（远小于留存样本），不套用本预算。
const consumeResponseByteBudget = 6 << 20

// consumeRetentionSetup 由调用方显式传入的留存预算推导（digestAggregation,
// budget）：显式预算只来自 digest 聚合路径（mcp/server.go
// digestRetentionByteBudget），此时命中计数不吃 limit 硬钳位；其余调用方
// （工作台 consume/export、MCP consume 工具）不传预算，只做内存兜底，
// limit 契约语义（返回条数上限，§5.3）不变。
func consumeRetentionSetup(explicitBudget int) (digestAggregation bool, budget int) {
	if explicitBudget > 0 {
		return true, explicitBudget
	}
	return false, workbenchRetentionByteBudget
}

// consumeWireSizeBytes 单条留存消息的响应序列化字节估算：双通道 value、
// key 与 headers 全量计入，另加每条 ~256B 的 JSON 结构/数字字段开销。
// 估算偏保守（略高估），只用于传输预算判断。
func consumeWireSizeBytes(msg ConsumedMessage) int {
	size := len(msg.Topic) + len(msg.Key) + len(msg.KeyBase64) +
		len(msg.ValueText) + len(msg.ValueBase64) +
		len(msg.DecodeError) + 256
	for k, v := range msg.Headers {
		size += len(k) + len(v) + 8
	}
	return size
}

func (t *consumeRetentionTracker) admit(valueBytes int) bool {
	if t.budget <= 0 || t.retained == 0 {
		t.retained += valueBytes
		return true
	}
	if t.retained+valueBytes > t.budget {
		return false
	}
	t.retained += valueBytes
	return true
}

// consumeMessages 一次性消费主循环（tinyrdm consumeMessages :1091 重写：
// 共享缓存 admin client 不适用——消费需要 per-request opts，这里经
// consumeClient 新建）。
func (s *Service) consumeMessages(ctx context.Context, params ConsumeParams) (consumeResult, error) {
	var result consumeResult
	topic := trimSpace(params.Topic)
	if topic == "" {
		return result, errf("topic is required")
	}
	if err := validateConsumeParams(params); err != nil {
		return result, err
	}
	// 留存预算推导（评审 H-1 回归修复）：显式预算 = digest 聚合路径；工作台
	// consume/export 与 MCP consume 工具不传（0=契约原语义），只做内存兜底。
	// 此前把「显式预算」和「默认兜底」混在一个字段里，导致 consumeEffectiveLimit
	// 对所有调用方把命中上界抬到 maxScan，limit 契约（返回条数上限）失真。
	digestAggregation, retentionBudget := consumeRetentionSetup(params.RetentionByteBudget)
	// §5.5：只读策略下禁止 commit（与 stream/start 同门禁）。此前一次性
	// 消费路径漏检——read_only 连接可经本路径为消费组提交 offset 且不留
	// 审计。未连接连接按 profileOf 只读兜底拒绝。
	profile := s.profileOf(params.ConnectionID)
	if params.Commit && profile.ReadOnly {
		s.emitAudit(params.ConnectionID, "messages-consume-commit", topic, "blocked", "read-only profile")
		return result, errf("kafka profile %q is read-only; commit is blocked", profile.Name)
	}
	limit := clampConsumeLimit(params.Limit)
	maxScan := consumeMaxScanRecords(limit, params.MaxScanRecords)
	timeout := time.Duration(params.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		// 15s：跨境/冷启动链路 5s 窗口极易到点空手而归（前端表单默认同值）。
		timeout = 15 * time.Second
	}

	groupID := trimSpace(params.GroupID)
	decodeMethod, err := normalizeDecodeMethod(params.Decode)
	if err != nil {
		return result, err
	}
	decompressMethod, err := normalizeDecompressMethod(params.Decompression)
	if err != nil {
		return result, err
	}
	// schema 挂载（Phase 2）：per-consume 解码器（SR 客户端 + 元数据缓存）。
	// wire format 解码仅支持 Confluent：provider=glue → 业务错（Phase 3 门禁）。
	var schemaDec *schemaDecoder
	matcher, err := newConsumeTextMatcher(params)
	if err != nil {
		return result, err
	}
	partitions, err := normalizeConsumePartitions(params.Partitions)
	if err != nil {
		return result, err
	}
	partitionOffsets, err := normalizeConsumePartitionOffsets(params.PartitionOffsets)
	if err != nil {
		return result, err
	}
	if len(partitions) == 0 && len(partitionOffsets) > 0 {
		partitions = offsetPartitions(partitionOffsets)
	}
	isolation, err := isolationLevelValue(params.IsolationLevel)
	if err != nil {
		return result, err
	}

	consumeOpts, err := buildConsumeOpts(params, topic, groupID, partitions, partitionOffsets, isolation)
	if err != nil {
		return result, err
	}
	// 消费 client 获取：池优先（consume_pool.go，消除冷启动握手/元数据开销）；
	// 不适用（group/精确起点）、未命中或占用中 → per-request 新建。
	// 池条目所有权移交池，closeClient 变 no-op，统一走 release。
	reuseOK, resetAtStart := consumeReuseEligible(params.OffsetStrategy, groupID, partitionOffsets)
	var signature string
	var pooled *consumePoolEntry
	var observed map[int32]struct{}
	var client *kgo.Client
	var closeClient func()
	if reuseOK {
		signature = consumeClientSignature(topic, params.OffsetStrategy, partitions, params.IsolationLevel)
		if entry, reusable := s.consumePoolAcquire(params.ConnectionID, signature, resetAtStart); reusable {
			if resetErr := resetConsumeClientForReuse(entry.client, topic, resetAtStart); resetErr != nil {
				s.consumePoolRelease(entry, nil, false)
			} else {
				pooled = entry
				client = entry.client
				closeClient = func() {}
				observed = map[int32]struct{}{}
			}
		}
	}
	if client == nil {
		client, closeClient, err = s.consumeClient(params.ConnectionID, consumeOpts...)
		if err != nil {
			return result, err
		}
		if reuseOK {
			// put 返回 nil = 同键旧条目占用中（并发同形状消费）：本次 client
			// 不入池，按临时 client 语义保留真实 closeClient（defer 关闭）。
			if pooled = s.consumePoolPut(params.ConnectionID, signature, client); pooled != nil {
				observed = map[int32]struct{}{}
				closeClient = func() {}
			}
		}
	}
	healthy := true
	defer func() {
		if pooled != nil {
			s.consumePoolRelease(pooled, observed, healthy)
		} else if closeClient != nil {
			closeClient()
		}
	}()

	// schema 挂载放在 client 就绪与 defer 注册之后（评审 L-10）：此前 SR
	// client 先建，后续 consumeClient 失败把它连同保活连接丢给 GC；现在
	// 失败路径统一经 defer 释放消费 client。
	if params.Schema != nil {
		if err := s.schemaMountSupported(params.ConnectionID, params.Schema.Registry, "consume"); err != nil {
			return result, err
		}
		schemaClient, schemaErr := s.confluentClientFor(params.ConnectionID)
		if schemaErr != nil {
			return result, schemaErr
		}
		schemaDec = newSchemaDecoder(schemaClient, params.Schema)
	}

	// 取消句柄（工作台停止按钮）：consumeId 非空时在 Ping 之前登记——冷启动
	// （跨境 TLS/SASL/metadata）同样可能久等，停止必须对启动期同样生效。
	// cancel 根同时覆盖启动与扫描两段；请求结束即注销。
	cancelRoot, cancelRootFn := context.WithCancel(ctx)
	defer cancelRootFn()
	var userCancelled atomic.Bool
	if consumeID := trimSpace(params.ConsumeID); consumeID != "" {
		handle := s.consumeCancels.register(consumeID, func() {
			userCancelled.Store(true)
			cancelRootFn()
		})
		defer s.consumeCancels.deregister(consumeID, handle)
	}

	// 启动期预算与扫描窗口分离：冷启动的 TLS/SASL/metadata/ListOffsets 不吃
	// 用户 timeoutMs（5s 窗口在跨境链路上连启动都跑不完）；Ping 等 metadata
	// ready 后才开扫描窗口。
	pingCtx, pingCancel := context.WithTimeout(cancelRoot, consumeStartupBudget(timeout))
	pingErr := client.Ping(pingCtx)
	pingCancel()
	if pingErr != nil {
		if userCancelled.Load() {
			// 启动期被取消：返回空结果（cancelled 标记）而非报错——拨号本身
			// 没有失败，client 保持 healthy 可回池。
			result.cancelled = true
			result.messages = []ConsumedMessage{}
			result.nextPartitionOffsets = map[int32]int64{}
			return result, nil
		}
		healthy = false
		return result, fmt.Errorf("kafka cluster not ready within %s: %w", consumeStartupBudget(timeout), pingErr)
	}
	consumeCtx, cancel := context.WithTimeout(cancelRoot, timeout)
	defer cancel()

	// 预分配按 min(limit, maxScan) 收敛：留存条数同时受两者约束，不再
	// 按未上界的入参做虚拟预留。
	messages := make([]ConsumedMessage, 0, min(limit, maxScan))
	// digest 聚合路径的留存由显式字节预算兜底，命中计数不吃 limit 硬钳位
	//（否则 maxScanRecords 超过钳位时扫描在 1 万命中处提前终止，聚合分布
	// 只覆盖子集——评审 M-1）；预分配仍按钳位锚定，追加交给 append 自然扩容。
	limit = consumeEffectiveLimit(limit, maxScan, digestAggregation)
	nextPartitionOffsets := map[int32]int64{}
	retention := consumeRetentionTracker{budget: retentionBudget}
	// 响应传输预算（KAFKA-H2）：只约束把原始消息发回调用方的路径；digest
	// 聚合的响应是聚合结果，留存样本按自己的显式预算走。0 = 不限。
	responseBudget := consumeResponseByteBudget
	if digestAggregation {
		responseBudget = 0
	}
	responseBytes := 0
	scanned := 0
	matched := 0
	committed := params.Commit && groupID != ""

	for scanned < maxScan && matched < limit {
		remaining := maxScan - scanned
		fetches := client.PollRecords(consumeCtx, consumeScanBatchSize(remaining))
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			scanned++
			if observed != nil {
				observed[record.Partition] = struct{}{}
			}
			nextPartitionOffset(nextPartitionOffsets, record)
			if !recordMatches(params, matcher, record) {
				continue
			}
			// 字段过滤需要解码 value 时惰性解码。
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
					out, info, schemaErr := schemaDec.decode(consumeCtx, value)
					if schemaErr != nil {
						// 解码失败不中断消费：保留原值 + decodeError。
						if decodeErr == "" {
							decodeErr = "schema: " + schemaErr.Error()
						} else {
							decodeErr = decodeErr + "; schema: " + schemaErr.Error()
						}
						return
					}
					value = out
					decoded = true
					info.Subject = firstNonEmpty(info.Subject, trimSpace(params.Schema.Subject))
					recordSchemaInfo = &info
				}
			}
			if fieldFiltersNeedValue(params.FieldFilters) {
				ensureValueDecoded()
			}
			if !fieldFiltersMatch(params.FieldFilters, matcher, record, value) {
				continue
			}
			matched++
			if len(messages) >= limit {
				continue
			}
			ensureValueDecoded()
			// 留存预算（评审 H-1）：超预算即停止留存，扫描与命中计数继续
			//（RetentionTruncated 让调用方区分「预算内子集」与完整命中面）。
			if !retention.admit(len(value)) {
				result.retentionTruncated = true
				continue
			}
			msg := messageFromRecordWithSchema(record, value, decoded, decodeErr, committed, recordSchemaInfo, params.SkipValueBase64)
			// 响应传输预算（KAFKA-H2）：序列化估算超 SDK 单行上限即停止
			// 留存而非让整个响应被丢弃。首条恒 admitted（与留存预算同语义：
			// 预算小于单条消息时保证至少 1 条样本可用）。
			wire := consumeWireSizeBytes(msg)
			if responseBudget > 0 && responseBytes > 0 && responseBytes+wire > responseBudget {
				result.retentionTruncated = true
				continue
			}
			responseBytes += wire
			messages = append(messages, msg)
		}
		if err := fetches.Err(); err != nil {
			if isDeadline(err) {
				result.cancelled, result.timedOut = consumeExitFlags(userCancelled.Load())
				break
			}
			healthy = false
			return result, err
		}
		if consumeCtx.Err() != nil {
			result.cancelled, result.timedOut = consumeExitFlags(userCancelled.Load())
			break
		}
	}
	limited := matched >= limit
	hasMore := limited || scanned >= maxScan || result.timedOut || result.cancelled

	if params.Commit {
		// commit 不复用扫描窗口 ctx（窗口超时退出是 commit 型消费最常见
		// 的退出方式）；成功留审计（offset 提交是写操作，§5.4）。
		if err := commitConsumeOffsets(ctx, client.CommitUncommittedOffsets); err != nil {
			healthy = false
			s.emitAudit(params.ConnectionID, "messages-consume-commit", topic, "error", err.Error())
			return result, err
		}
		s.emitAudit(params.ConnectionID, "messages-consume-commit", topic, "success", sprintf("group=%s", groupID))
	}
	result.messages = messages
	result.matched = matched
	result.scanned = scanned
	result.limited = limited
	result.hasMore = hasMore
	result.nextPartitionOffsets = nextPartitionOffsets
	return result, nil
}

// --- 消费参数校验与 kgo opts（tinyrdm :1994-2172 / :3894 重写） ---

// validateConsumeParams 参数校验：commit×过滤互斥、partitions×groupId 互斥、
// strategy=offset 必填 partitionOffsets、范围字段大小关系。
func validateConsumeParams(params ConsumeParams) error {
	// partitions/partitionOffsets 与 groupId 互斥（与 MCP 工具 schema 声明
	// 一致）：分区直读路径从不注册 ConsumerGroup，groupID 非空时
	// DisableAutoCommit 会让 franz-go 拒建 client，报错与真实原因无关。
	if trimSpace(params.GroupID) != "" && (len(params.Partitions) > 0 || len(params.PartitionOffsets) > 0) {
		return errf("groupId cannot be combined with partitions or partitionOffsets")
	}
	if err := validateRange(params.TimestampFrom, params.TimestampTo, "timestamp"); err != nil {
		return err
	}
	if err := validateRange(params.OffsetFrom, params.OffsetTo, "offset"); err != nil {
		return err
	}
	if params.MaxScanRecords < 0 {
		return errf("maxScanRecords must be greater than or equal to 0")
	}
	if params.Commit {
		if trimSpace(params.GroupID) == "" {
			return errf("commit=true requires groupId")
		}
		if hasConsumeFilter(params) {
			return errf("commit=true cannot be combined with filters")
		}
	}
	if err := validateFieldFilters(params.FieldFilters); err != nil {
		return err
	}
	_, err := newConsumeTextMatcher(params)
	return err
}

// hasConsumeFilter 报告参数中是否带任何过滤（含范围与字段过滤）。
func hasConsumeFilter(params ConsumeParams) bool {
	return trimSpace(params.Filter) != "" ||
		trimSpace(params.KeyFilter) != "" ||
		trimSpace(params.ValueFilter) != "" ||
		trimSpace(params.HeaderFilter) != "" ||
		hasFieldFilters(params.FieldFilters) ||
		params.TimestampFrom != nil || params.TimestampTo != nil ||
		params.OffsetFrom != nil || params.OffsetTo != nil
}

func validateRange(from, to *int64, name string) error {
	if from != nil && to != nil && *from > *to {
		return errf("%sFrom must be less than or equal to %sTo", name, name)
	}
	return nil
}

// normalizeConsumePartitions 分区列表归一（去重排序、非负校验）。
func normalizeConsumePartitions(partitions []int32) ([]int32, error) {
	if len(partitions) == 0 {
		return nil, nil
	}
	seen := make(map[int32]struct{}, len(partitions))
	result := make([]int32, 0, len(partitions))
	for _, partition := range partitions {
		if partition < 0 {
			return nil, errf("partition must be greater than or equal to 0")
		}
		if _, ok := seen[partition]; ok {
			continue
		}
		seen[partition] = struct{}{}
		result = append(result, partition)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result, nil
}

// normalizeConsumePartitionOffsets per-partition offset 归一。
func normalizeConsumePartitionOffsets(offsets map[int32]int64) (map[int32]int64, error) {
	if len(offsets) == 0 {
		return nil, nil
	}
	result := make(map[int32]int64, len(offsets))
	for partition, offset := range offsets {
		if partition < 0 {
			return nil, errf("partition must be greater than or equal to 0")
		}
		if offset < 0 {
			return nil, errf("offset for partition %d must be greater than or equal to 0", partition)
		}
		result[partition] = offset
	}
	return result, nil
}

func offsetPartitions(offsets map[int32]int64) []int32 {
	partitions := make([]int32, 0, len(offsets))
	for partition := range offsets {
		partitions = append(partitions, partition)
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
	return partitions
}

// normalizeOffsetStrategyName 别名归一（offset/absolute 等）。
func normalizeOffsetStrategyName(strategy string) string {
	switch strings.ToLower(trimSpace(strategy)) {
	case "offset", "absolute", "absolute-offset", "absolute_offset":
		return "offset"
	default:
		return strings.ToLower(trimSpace(strategy))
	}
}

// usesExactOffsets 报告是否按精确 offset seek。
func usesExactOffsets(strategy string, offsets map[int32]int64) bool {
	return len(offsets) > 0 || normalizeOffsetStrategyName(strategy) == "offset"
}

// parseTimestampMillis 解析 timestamp 策略的 offsetTime。
func parseTimestampMillis(value string) (int64, error) {
	raw := trimSpace(value)
	if raw == "" {
		return 0, errf("offsetTime is required for timestamp offset strategy")
	}
	if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil {
		if parsed < 0 {
			return 0, errf("offsetTime must be greater than or equal to 0")
		}
		return parsed, nil
	}
	if ts, err := time.Parse(time.RFC3339, raw); err == nil {
		millis := ts.UnixMilli()
		if millis < 0 {
			return 0, errf("offsetTime must be greater than or equal to 0")
		}
		return millis, nil
	}
	return 0, errf("offsetTime must be unix milliseconds or RFC3339")
}

// consumeOffset 解析 offset 策略为 kgo.Offset（tinyrdm kafkaConsumeOffset :2125）。
// recent = 每分区从「日志末端回退 recentWindow 条」起读（浏览型查询默认值：
// latest 只尾巴等待新消息，历史消息永远扫不到，见 issue #16）。
func consumeOffset(strategy, offsetTime string, hasGroup, directPartitions bool, recentWindow int64) (kgo.Offset, error) {
	raw := strings.ToLower(trimSpace(strategy))
	if raw == "" || raw == "default" {
		if directPartitions {
			return kgo.NewOffset().AtStart(), nil
		}
		return kgo.Offset{}, nil
	}
	var offset kgo.Offset
	switch raw {
	case "latest", "end":
		offset = kgo.NewOffset().AtEnd()
	case "recent", "last":
		if recentWindow < 1 {
			recentWindow = 1
		}
		offset = kgo.NewOffset().AtEnd().Relative(-recentWindow)
	case "earliest", "start":
		offset = kgo.NewOffset().AtStart()
	case "timestamp", "time", "by-time", "by_time":
		millis, err := parseTimestampMillis(offsetTime)
		if err != nil {
			return kgo.Offset{}, err
		}
		offset = kgo.NewOffset().AfterMilli(millis)
	case "committed", "none":
		if !hasGroup {
			return kgo.Offset{}, errf("committed offset strategy requires groupId")
		}
		if directPartitions {
			return kgo.Offset{}, errf("committed offset strategy cannot be combined with partitions")
		}
		offset = kgo.NewOffset().AtCommitted()
	default:
		return kgo.Offset{}, errf("offsetStrategy must be latest, recent, earliest, committed, timestamp, or offset")
	}
	return offset, nil
}

// buildConsumeOpts 组装消费 opts（tinyrdm buildKgoConsumeOpts :3894 重写）。
// strategy=offset 而 partitionOffsets 缺失时返回错误（对齐 tinyrdm
// kafkaConsumePartitionExactOffsets 的必填校验）。
func buildConsumeOpts(params ConsumeParams, topic, groupID string, partitions []int32, partitionOffsets map[int32]int64, isolation kgo.IsolationLevel) ([]kgo.Opt, error) {
	var opts []kgo.Opt
	recentWindow := int64(consumeMaxScanRecords(params.Limit, params.MaxScanRecords))
	opts = append(opts, kgo.FetchIsolationLevel(isolation))
	// 注意：DisableAutoCommit 必须与 ConsumerGroup 同生共死（franz-go 对
	// 「禁自动提交但未指定 group」直接拒建 client）。group 模式仅存在于
	// 无分区分支；partitions/partitionOffsets 直读路径与 groupId 互斥，
	// 由 validateConsumeParams 把门。
	if len(partitions) > 0 {
		if usesExactOffsets(params.OffsetStrategy, partitionOffsets) {
			topicPartitions := make(map[int32]kgo.Offset, len(partitions))
			for _, partition := range partitions {
				offset, ok := partitionOffsets[partition]
				if !ok {
					if normalizeOffsetStrategyName(params.OffsetStrategy) == "offset" {
						return nil, errf("offset for partition %d is required", partition)
					}
					// 带 partitionOffsets 的其他策略：未列分区回退 start。
					topicPartitions[partition] = kgo.NewOffset().AtStart()
					continue
				}
				topicPartitions[partition] = kgo.NewOffset().At(offset)
			}
			opts = append(opts, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
				topic: topicPartitions,
			}))
		} else {
			offset, err := consumeOffset(params.OffsetStrategy, params.OffsetTime, false, true, recentWindow)
			if err != nil {
				return nil, err
			}
			opts = append(opts, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
				topic: partitionOffsetMap(partitions, offset),
			}))
		}
	} else {
		if normalizeOffsetStrategyName(params.OffsetStrategy) == "offset" && len(partitionOffsets) == 0 {
			return nil, errf("offset strategy requires partitions or partitionOffsets")
		}
		opts = append(opts, kgo.ConsumeTopics(topic))
		if groupID != "" {
			opts = append(opts, kgo.ConsumerGroup(groupID), kgo.DisableAutoCommit())
		}
		if trimSpace(params.OffsetStrategy) != "" && normalizeOffsetStrategyName(params.OffsetStrategy) != "default" {
			offset, err := consumeOffset(params.OffsetStrategy, params.OffsetTime, groupID != "", false, recentWindow)
			if err != nil {
				return nil, err
			}
			opts = append(opts, kgo.ConsumeStartOffset(offset), kgo.ConsumeResetOffset(offset))
		}
	}
	return opts, nil
}

func partitionOffsetMap(partitions []int32, offset kgo.Offset) map[int32]kgo.Offset {
	out := make(map[int32]kgo.Offset, len(partitions))
	for _, partition := range partitions {
		out[partition] = offset
	}
	return out
}

// isolationLevelValue 解析 isolationLevel。
func isolationLevelValue(level string) (kgo.IsolationLevel, error) {
	switch strings.ToLower(trimSpace(level)) {
	case "", "read_uncommitted":
		return kgo.ReadUncommitted(), nil
	case "read_committed":
		return kgo.ReadCommitted(), nil
	default:
		return kgo.IsolationLevel{}, errf("isolationLevel must be read_uncommitted or read_committed")
	}
}

func isDeadline(err error) bool {
	// errors.Is（评审 L-8）：包装错误（fmt.Errorf("%w")）同样识别。
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

// --- 匹配器与过滤引擎（tinyrdm :2213-2300 / :2508-2930 重写） ---

// textMatcher 按 matchMode 匹配文本；regex 模式预编译缓存。
type textMatcher struct {
	mode     string
	patterns map[string]*regexp.Regexp
}

// normalizeMatchMode 归一化 matchMode。
func normalizeMatchMode(mode string) (string, error) {
	switch strings.ToLower(trimSpace(mode)) {
	case "", "contains":
		return "contains", nil
	case "prefix", "starts_with", "starts-with":
		return "prefix", nil
	case "exact", "equals":
		return "exact", nil
	case "regex", "regexp":
		return "regex", nil
	default:
		return "", errf("matchMode must be contains, prefix, exact, or regex")
	}
}

// newConsumeTextMatcher 构建匹配器（regex 模式编译所有查询串）。
func newConsumeTextMatcher(params ConsumeParams) (textMatcher, error) {
	mode, err := normalizeMatchMode(params.MatchMode)
	if err != nil {
		return textMatcher{}, err
	}
	matcher := textMatcher{mode: mode}
	if mode != "regex" {
		return matcher, nil
	}
	patterns := map[string]*regexp.Regexp{}
	for _, query := range []string{params.Filter, params.KeyFilter, params.ValueFilter, params.HeaderFilter} {
		query = trimSpace(query)
		if query == "" {
			continue
		}
		if _, ok := patterns[query]; ok {
			continue
		}
		compiled, err := regexp.Compile(query)
		if err != nil {
			return textMatcher{}, fmt.Errorf("invalid regex filter %q: %w", query, err)
		}
		patterns[query] = compiled
	}
	for _, filter := range params.FieldFilters {
		if !fieldFilterEnabled(filter) || normalizeFieldOperator(filter.Operator) != "regex" {
			continue
		}
		query := trimSpace(filter.Value)
		if query == "" {
			continue
		}
		if _, ok := patterns[query]; ok {
			continue
		}
		compiled, err := regexp.Compile(query)
		if err != nil {
			return textMatcher{}, fmt.Errorf("invalid regex field filter %q: %w", query, err)
		}
		patterns[query] = compiled
	}
	matcher.patterns = patterns
	return matcher, nil
}

// match 匹配（query 空 = 恒真，语义：未设该通道过滤）。
func (m textMatcher) match(value, query string) bool {
	query = trimSpace(query)
	if query == "" {
		return true
	}
	switch m.mode {
	case "prefix":
		return strings.HasPrefix(strings.ToLower(value), strings.ToLower(query))
	case "exact":
		return strings.EqualFold(value, query)
	case "regex":
		// 先查 newConsumeTextMatcher 的预编译缓存（KAFKA-M3：热路径每记录
		// 每通道重复 Compile 比匹配贵；对齐 fieldValueMatches 的查表写法），
		// miss 再现编译（调用链已保证合法，非法一律 false）。
		if compiled := m.patterns[query]; compiled != nil {
			return compiled.MatchString(value)
		}
		compiled, err := regexp.Compile(query)
		if err != nil {
			return false
		}
		return compiled.MatchString(value)
	default:
		return strings.Contains(strings.ToLower(value), strings.ToLower(query))
	}
}

// matchBytes 是 match 的字节通道版本（评审 M）：valueFilter/keyFilter 直接在
// record.Value/record.Key 上匹配，省去 string(record.Value) 整串拷贝——大
// value × 高扫描量下每记录一份拷贝是留存预算之外的第二个内存放大器。语义
// 与 match 完全一致；contains/prefix 对「纯 ASCII 且无大写」的 value 走零
// 分配快路径（此时 ToLower 是恒等变换），其余回退与 match 相同的降写路径。
func (m textMatcher) matchBytes(value []byte, query string) bool {
	query = trimSpace(query)
	if query == "" {
		return true
	}
	switch m.mode {
	case "prefix":
		if isPlainLowerASCII(value) {
			return bytes.HasPrefix(value, []byte(strings.ToLower(query)))
		}
		return strings.HasPrefix(strings.ToLower(string(value)), strings.ToLower(query))
	case "exact":
		return bytes.EqualFold(value, []byte(query))
	case "regex":
		return m.match(string(value), query)
	default:
		lowered := []byte(strings.ToLower(query))
		if isPlainLowerASCII(value) {
			return bytes.Contains(value, lowered)
		}
		return bytes.Contains(bytes.ToLower(value), lowered)
	}
}

// isPlainLowerASCII 报告字节序列是否纯 ASCII 且不含大写字母（contains/
// prefix 快路径判据：ToLower 恒等，无需整串降写拷贝）。
func isPlainLowerASCII(data []byte) bool {
	for _, b := range data {
		if b >= utf8.RuneSelf || (b >= 'A' && b <= 'Z') {
			return false
		}
	}
	return true
}

// recordMatches 全量过滤链：分区白名单 → 时间/offset 范围 → 全文/分通道 →
// 字段过滤由 fieldFiltersMatch 处理。
func recordMatches(params ConsumeParams, matcher textMatcher, record *kgo.Record) bool {
	if !partitionAllowed(params.Partitions, record.Partition) {
		return false
	}
	if params.TimestampFrom != nil && record.Timestamp.UnixMilli() < *params.TimestampFrom {
		return false
	}
	if params.TimestampTo != nil && record.Timestamp.UnixMilli() > *params.TimestampTo {
		return false
	}
	if params.OffsetFrom != nil && record.Offset < *params.OffsetFrom {
		return false
	}
	if params.OffsetTo != nil && record.Offset > *params.OffsetTo {
		return false
	}
	if filter := trimSpace(params.Filter); filter != "" {
		// filter 通道走复用缓冲 + 字节匹配（评审 L-3）：此前每条扫描记录
		// string(record.Value) 整串拷贝拼检索文本，高扫描量下是第三个内存
		// 放大器；字节级拼接与原实现逐字节等价。
		bufp := searchTextBufPool.Get().(*[]byte)
		buf := appendRecordSearchText((*bufp)[:0], record)
		matched := matcher.matchBytes(buf, filter)
		*bufp = buf[:0]
		searchTextBufPool.Put(bufp)
		if !matched {
			return false
		}
	}
	// key/value 通道走字节匹配（matchBytes）：免 string(record.Key/Value)
	// 整串拷贝，语义与字符串路径一致。
	if key := trimSpace(params.KeyFilter); key != "" && !matcher.matchBytes(record.Key, key) {
		return false
	}
	if value := trimSpace(params.ValueFilter); value != "" && !matcher.matchBytes(record.Value, value) {
		return false
	}
	if header := trimSpace(params.HeaderFilter); header != "" && !matcher.match(headersText(record.Headers), header) {
		return false
	}
	return true
}

func partitionAllowed(partitions []int32, partition int32) bool {
	if len(partitions) == 0 {
		return true
	}
	for _, candidate := range partitions {
		if candidate == partition {
			return true
		}
	}
	return false
}

// searchTextBufPool 复用检索文本缓冲（评审 L-3）。
var searchTextBufPool = sync.Pool{New: func() any { return new([]byte) }}

// appendRecordSearchText 追加全文检索文本（topic/partition/offset/ts/key/
// value/headers 空格拼接；字节级等价于原 strings.Join 实现）。
func appendRecordSearchText(dst []byte, record *kgo.Record) []byte {
	if record == nil {
		return dst
	}
	dst = append(dst, record.Topic...)
	dst = append(dst, ' ')
	dst = strconv.AppendInt(dst, int64(record.Partition), 10)
	dst = append(dst, ' ')
	dst = strconv.AppendInt(dst, record.Offset, 10)
	dst = append(dst, ' ')
	dst = strconv.AppendInt(dst, record.Timestamp.UnixMilli(), 10)
	dst = append(dst, ' ')
	dst = append(dst, record.Key...)
	dst = append(dst, ' ')
	dst = append(dst, record.Value...)
	dst = append(dst, ' ')
	return append(dst, headersText(record.Headers)...)
}

func headersText(headers []kgo.RecordHeader) string {
	if len(headers) == 0 {
		return ""
	}
	var builder strings.Builder
	for _, header := range headers {
		if builder.Len() > 0 {
			builder.WriteByte(' ')
		}
		builder.WriteString(header.Key)
		builder.WriteByte('=')
		builder.Write(header.Value)
	}
	return builder.String()
}

func nextPartitionOffset(next map[int32]int64, record *kgo.Record) {
	if next == nil || record == nil {
		return
	}
	offset := record.Offset + 1
	if current, ok := next[record.Partition]; !ok || offset > current {
		next[record.Partition] = offset
	}
}

// --- 字段过滤（tinyrdm :2554-2753 重写） ---

func fieldFilterEnabled(filter ConsumeFieldFilter) bool {
	return filter.Enabled == nil || *filter.Enabled
}

// validateFieldFilters 校验字段过滤（source/operator/regex/数值/path）。
func validateFieldFilters(filters []ConsumeFieldFilter) error {
	for i, filter := range filters {
		if !fieldFilterEnabled(filter) {
			continue
		}
		source, err := normalizeFieldSource(filter.Source)
		if err != nil {
			return fmt.Errorf("fieldFilters[%d]: %w", i, err)
		}
		operator := normalizeFieldOperator(filter.Operator)
		switch operator {
		case "contains", "prefix", "exact", "regex", "exists", "not_exists", "gt", "gte", "lt", "lte":
		default:
			return fmt.Errorf("fieldFilters[%d]: operator must be contains, prefix, exact, regex, exists, not_exists, gt, gte, lt, or lte", i)
		}
		if operator == "regex" && trimSpace(filter.Value) != "" {
			if _, err := regexp.Compile(trimSpace(filter.Value)); err != nil {
				return fmt.Errorf("fieldFilters[%d]: invalid regex field filter %q: %w", i, trimSpace(filter.Value), err)
			}
		}
		if isNumericOperator(operator) {
			if _, err := strconv.ParseFloat(trimSpace(filter.Value), 64); err != nil {
				return fmt.Errorf("fieldFilters[%d]: value must be numeric for %s", i, operator)
			}
		}
		if (source == "value" || source == "key") && trimSpace(filter.Path) != "" {
			if _, err := parseJSONPath(filter.Path); err != nil {
				return fmt.Errorf("fieldFilters[%d]: invalid path %q: %w", i, filter.Path, err)
			}
		}
	}
	return nil
}

func hasFieldFilters(filters []ConsumeFieldFilter) bool {
	for _, filter := range filters {
		if fieldFilterEnabled(filter) {
			return true
		}
	}
	return false
}

// fieldFiltersNeedValue 报告是否有启用过滤需要解码 value。
func fieldFiltersNeedValue(filters []ConsumeFieldFilter) bool {
	for _, filter := range filters {
		if !fieldFilterEnabled(filter) {
			continue
		}
		source, _ := normalizeFieldSource(filter.Source)
		if source == "value" {
			return true
		}
	}
	return false
}

func fieldFiltersMatch(filters []ConsumeFieldFilter, matcher textMatcher, record *kgo.Record, value []byte) bool {
	for _, filter := range filters {
		if !fieldFilterEnabled(filter) {
			continue
		}
		sourceValue, exists := fieldFilterValue(filter, record, value)
		operator := normalizeFieldOperator(filter.Operator)
		switch operator {
		case "exists":
			if !exists {
				return false
			}
			continue
		case "not_exists":
			if exists {
				return false
			}
			continue
		}
		if !exists {
			return false
		}
		if !fieldValueMatches(sourceValue, filter.Value, operator, matcher) {
			return false
		}
	}
	return true
}

func fieldFilterValue(filter ConsumeFieldFilter, record *kgo.Record, decodedValue []byte) (string, bool) {
	if record == nil {
		return "", false
	}
	source, err := normalizeFieldSource(filter.Source)
	if err != nil {
		return "", false
	}
	path := trimSpace(filter.Path)
	switch source {
	case "key":
		return jsonPathValue(record.Key, path)
	case "header":
		return headerPathValue(record.Headers, path)
	case "topic":
		return record.Topic, true
	case "partition":
		return strconv.FormatInt(int64(record.Partition), 10), true
	case "offset":
		return strconv.FormatInt(record.Offset, 10), true
	case "timestamp":
		return strconv.FormatInt(record.Timestamp.UnixMilli(), 10), true
	default:
		return jsonPathValue(decodedValue, path)
	}
}

func normalizeFieldSource(source string) (string, error) {
	switch strings.ToLower(trimSpace(source)) {
	case "", "value", "payload", "message":
		return "value", nil
	case "key":
		return "key", nil
	case "header", "headers":
		return "header", nil
	case "topic", "partition", "offset", "timestamp":
		return strings.ToLower(trimSpace(source)), nil
	default:
		return "", errf("source must be value, key, header, topic, partition, offset, or timestamp")
	}
}

func normalizeFieldOperator(operator string) string {
	switch strings.ToLower(trimSpace(operator)) {
	case "", "contains":
		return "contains"
	case "prefix", "starts_with", "starts-with":
		return "prefix"
	case "exact", "equals", "eq":
		return "exact"
	case "regexp":
		return "regex"
	case "not_exists", "not-exists", "missing":
		return "not_exists"
	case ">":
		return "gt"
	case ">=":
		return "gte"
	case "<":
		return "lt"
	case "<=":
		return "lte"
	default:
		return strings.ToLower(trimSpace(operator))
	}
}

func isNumericOperator(operator string) bool {
	switch operator {
	case "gt", "gte", "lt", "lte":
		return true
	default:
		return false
	}
}

func fieldValueMatches(sourceValue, query, operator string, matcher textMatcher) bool {
	query = trimSpace(query)
	switch operator {
	case "prefix":
		return strings.HasPrefix(strings.ToLower(sourceValue), strings.ToLower(query))
	case "exact":
		return strings.EqualFold(sourceValue, query)
	case "regex":
		if query == "" {
			return true
		}
		if compiled := matcher.patterns[query]; compiled != nil {
			return compiled.MatchString(sourceValue)
		}
		compiled, err := regexp.Compile(query)
		return err == nil && compiled.MatchString(sourceValue)
	case "gt", "gte", "lt", "lte":
		left, leftErr := strconv.ParseFloat(trimSpace(sourceValue), 64)
		right, rightErr := strconv.ParseFloat(query, 64)
		if leftErr != nil || rightErr != nil {
			return false
		}
		switch operator {
		case "gt":
			return left > right
		case "gte":
			return left >= right
		case "lt":
			return left < right
		default:
			return left <= right
		}
	default:
		return matcher.match(sourceValue, query)
	}
}

// --- JSON path（tinyrdm :2755-2865 重写） ---

type jsonPathToken struct {
	key     string
	index   int
	isIndex bool
}

func jsonPathValue(data []byte, path string) (string, bool) {
	if trimSpace(path) == "" || trimSpace(path) == "$" {
		return string(data), data != nil
	}
	var decoded any
	if err := json.Unmarshal(data, &decoded); err != nil {
		return "", false
	}
	tokens, err := parseJSONPath(path)
	if err != nil {
		return "", false
	}
	current := decoded
	for _, token := range tokens {
		if token.isIndex {
			items, ok := current.([]any)
			if !ok || token.index < 0 || token.index >= len(items) {
				return "", false
			}
			current = items[token.index]
			continue
		}
		object, ok := current.(map[string]any)
		if !ok {
			return "", false
		}
		next, ok := object[token.key]
		if !ok {
			return "", false
		}
		current = next
	}
	return jsonValueString(current), true
}

func headerPathValue(headers []kgo.RecordHeader, path string) (string, bool) {
	if trimSpace(path) == "" || trimSpace(path) == "$" {
		return headersText(headers), len(headers) > 0
	}
	for _, header := range headers {
		if header.Key == path {
			return string(header.Value), true
		}
	}
	return "", false
}

func parseJSONPath(path string) ([]jsonPathToken, error) {
	path = trimSpace(path)
	path = strings.TrimPrefix(path, "$")
	path = strings.TrimPrefix(path, ".")
	if path == "" {
		return nil, nil
	}
	var tokens []jsonPathToken
	for _, part := range strings.Split(path, ".") {
		if part == "" {
			return nil, errf("empty path segment")
		}
		for part != "" {
			bracket := strings.IndexByte(part, '[')
			if bracket < 0 {
				tokens = append(tokens, jsonPathToken{key: part})
				break
			}
			if bracket > 0 {
				tokens = append(tokens, jsonPathToken{key: part[:bracket]})
			}
			end := strings.IndexByte(part[bracket:], ']')
			if end <= 1 {
				return nil, errf("invalid array index")
			}
			indexText := part[bracket+1 : bracket+end]
			index, err := strconv.Atoi(indexText)
			if err != nil || index < 0 {
				return nil, fmt.Errorf("invalid array index %q", indexText)
			}
			tokens = append(tokens, jsonPathToken{index: index, isIndex: true})
			part = part[bracket+end+1:]
			if part != "" && part[0] != '[' {
				return nil, fmt.Errorf("unexpected path suffix %q", part)
			}
		}
	}
	return tokens, nil
}

func jsonValueString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(typed)
	default:
		encoded, err := json.Marshal(typed)
		if err != nil {
			return fmt.Sprint(typed)
		}
		return string(encoded)
	}
}

// --- 解码/解压（tinyrdm :2986-3069 重写） ---

func normalizeDecodeMethod(decode string) (string, error) {
	method := strings.ToLower(trimSpace(decode))
	switch method {
	case "", "none", "base64":
		return method, nil
	default:
		return "", errf("decode must be none or base64")
	}
}

func normalizeDecompressMethod(decompression string) (string, error) {
	method := strings.ToLower(trimSpace(decompression))
	switch method {
	case "", "none", "gzip", "lz4", "zstd", "snappy":
		return method, nil
	default:
		return "", errf("decompression must be none, gzip, lz4, zstd, or snappy")
	}
}

// decodeConsumeValue 先二次 base64 解码再解压；失败保留原值 + 错误信息。
func decodeConsumeValue(value []byte, decodeMethod, decompressMethod string) ([]byte, bool, string) {
	raw := value
	decoded := false
	if decodeMethod == "base64" {
		decodedBytes, err := base64.StdEncoding.DecodeString(string(value))
		if err != nil {
			return raw, false, err.Error()
		}
		value = decodedBytes
		decoded = true
	}
	if decompressMethod == "" || decompressMethod == "none" {
		return value, decoded, ""
	}
	decompressed, err := decompressPayload(value, decompressMethod)
	if err != nil {
		return value, decoded, err.Error()
	}
	return decompressed, true, ""
}

// sharedZstdDecoder 包级共享解码器（评审 L-2）：DecodeAll 并发安全，窗口表
// 初始化不便宜，构建一次全进程复用；与进程同寿，不 Close。
var sharedZstdDecoder = sync.OnceValue(func() *zstd.Decoder {
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(maxDecodedBytes))
	if err != nil {
		// 仅 option 非法才会失败：返回 nil，调用方退化为显式错误而非 panic。
		return nil
	}
	return decoder
})

func decompressPayload(value []byte, method string) ([]byte, error) {
	switch strings.ToLower(trimSpace(method)) {
	case "gzip":
		reader, err := gzip.NewReader(bytes.NewReader(value))
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		return readBounded(reader)
	case "lz4":
		reader := lz4.NewReader(bytes.NewReader(value))
		return readBounded(reader)
	case "zstd":
		// WithDecoderMaxMemory 兜底 DecodeAll 内部分配；输出长度再显式校验
		//（KAFKA-H1：恶意帧可声明超大内容尺寸）。解码器为包级共享单例。
		decoder := sharedZstdDecoder()
		if decoder == nil {
			return nil, errf("zstd decoder unavailable")
		}
		decoded, decodeAllErr := decoder.DecodeAll(value, nil)
		if decodeAllErr != nil {
			return nil, decodeAllErr
		}
		if len(decoded) > maxDecodedBytes {
			return nil, errDecodedTooLarge()
		}
		return decoded, nil
	case "snappy":
		return decompressSnappy(value)
	default:
		return nil, errf("decompression must be none, gzip, lz4, zstd, or snappy")
	}
}

// readBounded 读取解压流并在超过 maxDecodedBytes 时报错（LimitReader 读
// max+1 字节即可判定超限，不物化超出部分）。
func readBounded(reader io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, maxDecodedBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxDecodedBytes {
		return nil, errDecodedTooLarge()
	}
	return data, nil
}

// errDecodedTooLarge 解压超限统一错误（进消息 decodeError，不中断消费）。
func errDecodedTooLarge() error {
	return errf("decompressed payload exceeds %d bytes (decompression bomb guard)", maxDecodedBytes)
}

// decompressSnappy block 优先、framed 兜底（tinyrdm :3057 同款）。block 路径
// 先经 DecodedLen 预检声明长度（恶意 block 头可声明超大解码尺寸）。
func decompressSnappy(value []byte) ([]byte, error) {
	if declared, err := snappy.DecodedLen(value); err == nil && declared > maxDecodedBytes {
		return nil, errDecodedTooLarge()
	}
	decoded, blockErr := snappy.Decode(nil, value)
	if blockErr == nil {
		return decoded, nil
	}
	bounded, streamErr := readBounded(snappy.NewReader(bytes.NewReader(value)))
	if streamErr == nil {
		return bounded, nil
	}
	return nil, fmt.Errorf("snappy decompression failed: block: %v; framed: %v", blockErr, streamErr)
}

// --- 消息形状与扫描辅助 ---

// messageFromRecord 构建保真消息（valueText UTF-8 安全预览 + valueBase64
// 完整；512KB 截断标记；key 非法 UTF-8 → keyBase64）。
func messageFromRecord(record *kgo.Record, value []byte, decoded bool, decodeErr string, committed bool) ConsumedMessage {
	return messageFromRecordWithSchema(record, value, decoded, decodeErr, committed, nil, false)
}

// messageFromRecordWithSchema 是 messageFromRecord 的 schema 感知变体
// （Phase 2：schema 解码命中时附 schemaId/schemaSubject/schemaVersion）；
// skipValueBase64 为 digest 大扫描省略 base64 通道（评审 H-1）。
func messageFromRecordWithSchema(record *kgo.Record, value []byte, decoded bool, decodeErr string, committed bool, schemaInfo *schemaValueInfo, skipValueBase64 bool) ConsumedMessage {
	// 双通道同用 maxMessageBytes 截断并共用 Truncated 标志（KAFKA-H1：
	// valueText 此前不截断，digest 高扫描量 × 双字段留存可达 GB 级驻留）。
	valueText := safeUTF8Preview(boundedValue(value))
	valueBase64 := ""
	truncated := false
	if !skipValueBase64 {
		valueBase64, truncated = encodeBase64WithLimit(value, maxMessageBytes)
	} else {
		truncated = len(value) > maxMessageBytes
	}
	message := ConsumedMessage{
		Topic:       record.Topic,
		Partition:   record.Partition,
		Offset:      record.Offset,
		Timestamp:   record.Timestamp.UnixMilli(),
		LeaderEpoch: record.LeaderEpoch,
		ValueText:   valueText,
		ValueBase64: valueBase64,
		Headers:     recordHeadersMap(record.Headers),
		Truncated:   truncated,
		Committed:   committed,
		DecodeError: decodeErr,
	}
	_ = decoded
	if schemaInfo != nil {
		message.SchemaID = int64(schemaInfo.ID)
		message.SchemaSubject = schemaInfo.Subject
		message.SchemaVersion = schemaInfo.Version
	}
	if isProbablyUTF8(record.Key) {
		message.Key = string(record.Key)
	} else {
		message.KeyBase64 = base64.StdEncoding.EncodeToString(record.Key)
	}
	return message
}

// isProbablyUTF8 报告字节序列是否为合法 UTF-8 且不含 NUL。
func isProbablyUTF8(data []byte) bool {
	if !utf8.Valid(data) {
		return false
	}
	return !bytes.ContainsRune(data, 0)
}

// safeUTF8Preview UTF-8 安全预览（非法字节替换 U+FFFD）。
func safeUTF8Preview(data []byte) string {
	if utf8.Valid(data) {
		return string(data)
	}
	return strings.ToValidUTF8(string(data), "\uFFFD")
}

// boundedValue 截断到 maxMessageBytes（截断边界落在多字节字符中间时由
// safeUTF8Preview 以 U+FFFD 收尾，预览语义可接受）。
func boundedValue(data []byte) []byte {
	if len(data) > maxMessageBytes {
		return data[:maxMessageBytes]
	}
	return data
}

// encodeBase64WithLimit base64 编码并在 limit 字节处截断（truncated 标记）。
func encodeBase64WithLimit(data []byte, limit int) (string, bool) {
	truncated := false
	payload := data
	if len(payload) > limit {
		payload = payload[:limit]
		truncated = true
	}
	return base64.StdEncoding.EncodeToString(payload), truncated
}

func recordHeaders(headers map[string]string) []kgo.RecordHeader {
	if len(headers) == 0 {
		return nil
	}
	keys := make([]string, 0, len(headers))
	for key := range headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]kgo.RecordHeader, 0, len(keys))
	for _, key := range keys {
		result = append(result, kgo.RecordHeader{Key: key, Value: []byte(headers[key])})
	}
	return result
}

func recordHeadersMap(headers []kgo.RecordHeader) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	result := make(map[string]string, len(headers))
	seen := make(map[string]int, len(headers))
	for _, header := range headers {
		// Kafka 允许同 key 多 header（KIP-82）：map 形状下后者覆盖即静默
		// 丢数据。冲突时第 2 条起加 "#2" 序号后缀（#3 顺延），单 key 场景
		// 形状不变（KAFKA-L3）。
		seen[header.Key]++
		key := header.Key
		if n := seen[header.Key]; n > 1 {
			key = sprintf("%s#%d", header.Key, n)
		}
		result[key] = safeUTF8Preview(header.Value)
	}
	return result
}

// normalizeProduceCount 批量条数归一（≤1000，默认 1）。
func normalizeProduceCount(count int) int {
	if count <= 0 {
		count = 1
	}
	if count > maxProduceCount {
		return maxProduceCount
	}
	return count
}

// produceCompressionCodec 解析压缩选项。
func produceCompressionCodec(method string) (kgo.CompressionCodec, bool, error) {
	switch strings.ToLower(trimSpace(method)) {
	case "", "none":
		return kgo.CompressionCodec{}, false, nil
	case "gzip":
		return kgo.GzipCompression(), true, nil
	case "lz4":
		return kgo.Lz4Compression(), true, nil
	case "zstd":
		return kgo.ZstdCompression(), true, nil
	case "snappy":
		return kgo.SnappyCompression(), true, nil
	default:
		return kgo.CompressionCodec{}, false, errf("compression must be none, gzip, lz4, zstd, or snappy")
	}
}

// normalizeProduceAcks 归一化投递确认级别（"all" | "1"；空 = all）。acks=0
// 显式拒绝：生产走同步 ProduceSync，franz-go 的 promise 依赖 broker 响应，
// acks=0（broker 不回）会拖到请求超时，不是可用的 fire-and-forget。
func normalizeProduceAcks(value string) (string, error) {
	switch strings.ToLower(trimSpace(value)) {
	case "", "all", "-1":
		return "all", nil
	case "1", "leader":
		return "1", nil
	case "0", "none":
		return "", errf("acks=0 is not supported: synchronous produce waits for broker responses (use acks \"all\" or \"1\")")
	default:
		return "", errf("acks must be \"all\" or \"1\"")
	}
}

// produceDeliveryOpts 把 acks/enableIdempotence 映射为 kgo producer opts。
// franz-go v1.20 默认即幂等生产 + acks=all，故默认分支零 opt；
// acks=1 / 关幂等需 DisableIdempotentWrite（kgo 校验：幂等开启时 acks 必为 all）。
func produceDeliveryOpts(acks string, enableIdempotence *bool) ([]kgo.Opt, error) {
	idempotent := enableIdempotence == nil || *enableIdempotence
	if acks == "1" {
		if idempotent {
			return nil, errf("idempotent producer requires acks=all (set enableIdempotence=false for acks=1)")
		}
		return []kgo.Opt{kgo.DisableIdempotentWrite(), kgo.RequiredAcks(kgo.LeaderAck())}, nil
	}
	if !idempotent {
		return []kgo.Opt{kgo.DisableIdempotentWrite(), kgo.RequiredAcks(kgo.AllISRAcks())}, nil
	}
	return nil, nil
}

// consumeCommitBudget commit 独立预算：不复用扫描窗口 ctx——窗口耗尽
// （timeoutMs 到点）是 commit 型消费最常见的退出方式，复用已超时的
// consumeCtx 会让提交必然失败并丢弃全部已扫结果。
const consumeCommitBudget = 10 * time.Second

// commitConsumeOffsets 以独立预算执行 offset 提交：ctx 从父请求派生
// （保持取消传播），deadline 重新起算，提交错误原样上抛。
func commitConsumeOffsets(ctx context.Context, commit func(context.Context) error) error {
	commitCtx, cancel := context.WithTimeout(ctx, consumeCommitBudget)
	defer cancel()
	return commit(commitCtx)
}

// consumeLimitHardCap / consumeMaxScanHardCap 消费内存上界（评审 H-3）：
// limit 曾无上界且按值预分配结果切片，单请求即可要求约 200GB（OOM 长驻
// sidecar）。limit 上限与 export 对齐（maxExportRecords=10000）；扫描上限
// 1e6（与 digest maxScanRecords 上限同量级）。
const (
	consumeLimitHardCap   = maxExportRecords
	consumeMaxScanHardCap = 1_000_000
)

// clampConsumeLimit 归一化返回条数上限：<=0 默认 100，>硬上限按 export 语义
// 收敛到上限。
func clampConsumeLimit(limit int) int {
	if limit <= 0 {
		return 100
	}
	return min(limit, consumeLimitHardCap)
}

// consumeEffectiveLimit 扫描循环的命中上界：digest 聚合路径的留存由显式
// RetentionByteBudget 兜底，命中计数不吃 limit 硬钳位（否则 maxScanRecords
// 超过钳位时扫描提前终止，聚合分布只覆盖子集）；扫描面仍受 maxScan 上界。
// 非 digest 路径（工作台 consume/export、MCP consume 工具）保持 limit 契约
// 语义：返回条数上限就是 limit。
func consumeEffectiveLimit(limit, maxScan int, digestAggregation bool) int {
	if digestAggregation {
		return max(limit, maxScan)
	}
	return limit
}

// consumeMaxScanRecords 扫描上限（默认 max(1000, limit×10)，§5.3；硬上限
// consumeMaxScanHardCap 防无界扫描）。
func consumeMaxScanRecords(limit, maxScan int) int {
	if maxScan > 0 {
		return min(maxScan, consumeMaxScanHardCap)
	}
	scanLimit := limit * 10
	if scanLimit < 1000 {
		scanLimit = 1000
	}
	if scanLimit < limit {
		scanLimit = limit
	}
	return min(scanLimit, consumeMaxScanHardCap)
}

// consumeScanBatchSize 单轮 poll 大小（上限 256）。
func consumeScanBatchSize(remaining int) int {
	const maxBatch = 256
	if remaining < maxBatch {
		if remaining > 0 {
			return remaining
		}
		return 1
	}
	return maxBatch
}

// --- 导出（tinyrdm :2327-2507 重写） ---

func normalizeExportFormat(format string) (string, error) {
	switch strings.ToLower(trimSpace(format)) {
	case "", "json":
		return "json", nil
	case "csv":
		return "csv", nil
	default:
		return "", errf("format must be json or csv")
	}
}

// exportRecordLimit 导出上限（默认 1000，硬上限 10000）。
func exportRecordLimit(limit int) (int, error) {
	if limit < 0 {
		return 0, errf("limit must be greater than or equal to 0")
	}
	effective := limit
	if effective <= 0 {
		effective = 1000
	}
	if effective > maxExportRecords {
		effective = maxExportRecords
	}
	return effective, nil
}

func serializeConsumedMessages(format string, messages []ConsumedMessage) (string, string, error) {
	if messages == nil {
		messages = []ConsumedMessage{}
	}
	switch format {
	case "csv":
		content, err := serializeCSV(messages)
		return content, "text/csv; charset=utf-8", err
	default:
		content, err := serializeJSON(messages)
		return content, "application/json; charset=utf-8", err
	}
}

func serializeJSON(messages []ConsumedMessage) (string, error) {
	encoded, err := json.MarshalIndent(messages, "", "  ")
	if err != nil {
		return "", err
	}
	return string(encoded) + "\n", nil
}

// serializeCSV 列：topic,partition,offset,timestamp,key,value,headers
// （value 用 valueText 预览；完整二进制经 JSON 导出保留 valueBase64）。
func serializeCSV(messages []ConsumedMessage) (string, error) {
	buffer := &bytes.Buffer{}
	writer := csv.NewWriter(buffer)
	if err := writer.Write([]string{"topic", "partition", "offset", "timestamp", "key", "value", "headers"}); err != nil {
		return "", err
	}
	for _, message := range messages {
		// 公式注入中和（评审 LOW-5）：每个单元格过 csvFormulaSafe。
		row := []string{
			csvFormulaSafe(message.Topic),
			strconv.FormatInt(int64(message.Partition), 10),
			strconv.FormatInt(message.Offset, 10),
			strconv.FormatInt(message.Timestamp, 10),
			csvFormulaSafe(firstNonEmpty(message.Key, message.KeyBase64)),
			csvFormulaSafe(message.ValueText),
			csvFormulaSafe(headersExportText(message.Headers)),
		}
		if err := writer.Write(row); err != nil {
			return "", err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return "", err
	}
	return buffer.String(), nil
}

// csvFormulaSafe 中和公式注入（评审 LOW-5）：以 = + - @ TAB CR 开头的
// 单元格在 Excel/Sheets 打开时会被当公式执行——前缀 ' 中和（业界通行
// 约定；与前端 messageExport.ts 同款）。
func csvFormulaSafe(value string) string {
	if value == "" {
		return value
	}
	switch value[0] {
	case '=', '+', '-', '@', '\t', '\r':
		return "'" + value
	}
	return value
}

func headersExportText(headers map[string]string) string {
	if len(headers) == 0 {
		return ""
	}
	keys := make([]string, 0, len(headers))
	for key := range headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+headers[key])
	}
	return strings.Join(parts, "; ")
}

func exportFilename(topic, format string, at time.Time) string {
	stamp := at.UTC().Format("20060102T150405Z")
	return sanitizeFilenamePart(topic) + "-" + stamp + "." + format
}

// sanitizeFilenamePart 文件名安全化（保留常见分隔语义；折叠连续点防目录
// 遍历形状，如 `../../etc/passwd` → `etc-passwd`）。
func sanitizeFilenamePart(value string) string {
	value = strings.TrimSpace(value)
	var builder strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '-', r == '_', r == '.':
			builder.WriteRune(r)
		default:
			builder.WriteRune('-')
		}
	}
	name := builder.String()
	for strings.Contains(name, "..") {
		name = strings.ReplaceAll(name, "..", "-")
	}
	name = strings.Trim(name, "-.")
	if name == "" {
		name = "kafka-export"
	}
	return name
}
