package mcp

// digest.go：`kafka_messages_digest` 的本地聚合（设计 §3 原语 2/3 + §6.3）。
//
// 扫描复用 kafkaconn.Consume 的 maxScanRecords 语义（MCP 不订阅 stream，
// stream 由用户在 UI 操作）；过滤各通道在 Consume 内完成，扫描到的消息全文
// 只留在 sidecar 进程内——本文件对命中消息做本地聚合：per-partition 计数、
// key groupBy（≤20 组）、时间直方图（≤12 桶）、（schema 解码后）指定字段
// 投影 distinct/topN（≤10）。聚合结论替代数据本体成为 MCP 返回。
// 纯函数：输入 []kafkaconn.ConsumedMessage + 参数，输出 digest 结构，便于单测。

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"io.dbx.kafka.plugin/internal/kafkaconn"
)

// DigestCellTruncate 单元格截断（§3：每单元格 120 字符；topic-partition-offset
// 定位字段不截断，截断在调用方按字段判断）。width ≤0 不截断。
func DigestCellTruncate(value string, width int) string {
	if width <= 0 {
		return value
	}
	runes := []rune(value)
	if len(runes) <= width {
		return value
	}
	return string(runes[:width]) + "…"
}

// TimeHistogram 时间直方图（桶数 ≤12；无时间戳消息单独计数）。
type TimeHistogram struct {
	// Buckets 各桶 [from,to) 边界（unix ms，含 from 不含 to；末桶含 to）。
	Buckets []TimeBucket `json:"buckets"`
	// NoTimestamp 无时间戳消息数（Timestamp==0）。
	NoTimestamp int `json:"noTimestamp,omitempty"`
}

// TimeBucket 直方图单桶。
type TimeBucket struct {
	From  int64 `json:"from"`
	To    int64 `json:"to"`
	Count int   `json:"count"`
}

// timeHistogramMaxBuckets 设计硬上限：直方图桶数 ≤12。
const timeHistogramMaxBuckets = 12

// digestDecodeErrorWidth 样本行 decodeError 的截断宽度（诊断信息，宽于
// 数据单元格——SR 错误的 subject 路径 + HTTP 状态码在前 120 字符常放不下）。
const digestDecodeErrorWidth = 200

// buildTimeHistogram 把带时间戳的命中消息按 [min,max] 均分 ≤12 桶。
// 桶数 = min(12, 时间跨度)（跨度不足时少分桶，避免空桶）；单一时间点
// （min==max）产出单桶。无时间戳消息不进桶。
func buildTimeHistogram(timestamps []int64) *TimeHistogram {
	if len(timestamps) == 0 {
		return nil
	}
	minTs, maxTs := timestamps[0], timestamps[0]
	for _, ts := range timestamps {
		if ts < minTs {
			minTs = ts
		}
		if ts > maxTs {
			maxTs = ts
		}
	}
	span := maxTs - minTs + 1
	bucketCount := int(span)
	if bucketCount > timeHistogramMaxBuckets {
		bucketCount = timeHistogramMaxBuckets
	}
	if bucketCount < 1 {
		bucketCount = 1
	}
	width := (span + int64(bucketCount) - 1) / int64(bucketCount)
	if width < 1 {
		width = 1
	}
	buckets := make([]TimeBucket, 0, bucketCount)
	for index := 0; index < bucketCount; index++ {
		from := minTs + int64(index)*width
		to := from + width
		if index == bucketCount-1 {
			to = maxTs + 1 // 末桶吸收余数并含 max
		}
		buckets = append(buckets, TimeBucket{From: from, To: to})
	}
	for _, ts := range timestamps {
		index := int((ts - minTs) / width)
		if index >= bucketCount {
			index = bucketCount - 1
		}
		buckets[index].Count++
	}
	return &TimeHistogram{Buckets: buckets}
}

// jsonPathLookup 在解析后的 JSON 值上按 path 取值（支持 $.user.id、
// items[0].sku、[0] 片段；大小写敏感）。命中返回字符串化值。
func jsonPathLookup(root any, path string) (string, bool) {
	normalized := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(path), "$"))
	normalized = strings.TrimPrefix(normalized, ".")
	segments := []string{}
	if normalized != "" {
		segments = strings.Split(normalized, ".")
	}
	current := root
	for _, segment := range segments {
		name, index, hasIndex := splitIndexSuffix(segment)
		if name != "" {
			object, ok := current.(map[string]any)
			if !ok {
				return "", false
			}
			next, ok := object[name]
			if !ok {
				return "", false
			}
			current = next
		}
		if hasIndex {
			array, ok := current.([]any)
			if !ok || index < 0 || index >= len(array) {
				return "", false
			}
			current = array[index]
		}
		if name == "" && !hasIndex {
			return "", false
		}
	}
	return jsonValueString(current), true
}

// splitIndexSuffix 拆分 `name[2]` / `[2]` / `name` 片段。
func splitIndexSuffix(segment string) (name string, index int, hasIndex bool) {
	open := strings.Index(segment, "[")
	if open < 0 || !strings.HasSuffix(segment, "]") {
		return segment, 0, false
	}
	inner := strings.TrimSpace(segment[open+1 : len(segment)-1])
	number, err := strconv.Atoi(inner)
	if err != nil {
		return segment, 0, false
	}
	return segment[:open], number, true
}

// jsonValueString JSON 值的字符串化（标量直出；容器压缩为 JSON 文本，
// 单元格截断由调用方完成）。
func jsonValueString(value any) string {
	switch typed := value.(type) {
	case nil:
		return "null"
	case string:
		return typed
	case bool:
		return strconv.FormatBool(typed)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	default:
		data, err := json.Marshal(typed)
		if err != nil {
			return fmt.Sprintf("%v", typed)
		}
		return string(data)
	}
}

// projectFieldValues 从消息 valueText（decode/decompression/schema 解码后的
// JSON 文本）提取全部投影字段的值。
func projectFieldValues(valueText string, fields []string) map[string]string {
	if len(fields) == 0 || strings.TrimSpace(valueText) == "" {
		return nil
	}
	var root any
	if err := json.Unmarshal([]byte(valueText), &root); err != nil {
		return nil // 非 JSON 载荷不做字段投影
	}
	out := make(map[string]string, len(fields))
	for _, field := range fields {
		if value, ok := jsonPathLookup(root, field); ok {
			out[field] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// FieldStats 单个投影字段的值域统计（distinct 计数 + top ≤DigestTopN）。
type FieldStats struct {
	Field      string         `json:"field"`
	ValueCount int            `json:"valueCount"` // 全量 distinct 值数
	Values     map[string]int `json:"values"`     // top ≤ TopN（按出现次数）
	Truncated  bool           `json:"truncated,omitempty"`
}

// DigestStats 聚合结论（digest 的 stats 段）。
type DigestStats struct {
	PerPartition      map[string]int `json:"perPartition"`
	PerPartitionLimit bool           `json:"perPartitionLimit,omitempty"`
	Keys              map[string]int `json:"keys"`
	KeysLimit         bool           `json:"keysLimit,omitempty"`
	// NullKeyCount key 为空/二进制的消息数（key groupBy 之外单列，防 key 维度失真）。
	NullKeyCount  int            `json:"nullKeyCount,omitempty"`
	TimeHistogram *TimeHistogram `json:"timeHistogram,omitempty"`
	Fields        []FieldStats   `json:"fields,omitempty"`
}

// topGroups 组计数取前 limit 组（按计数降序、组名升序稳定排序）。
func topGroups(counts map[string]int, limit int) (map[string]int, bool) {
	if len(counts) == 0 || limit <= 0 {
		return map[string]int{}, false
	}
	names := make([]string, 0, len(counts))
	for name := range counts {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		if counts[names[i]] != counts[names[j]] {
			return counts[names[i]] > counts[names[j]]
		}
		return names[i] < names[j]
	})
	truncated := len(names) > limit
	if truncated {
		names = names[:limit]
	}
	out := make(map[string]int, len(names))
	for _, name := range names {
		out[name] = counts[name]
	}
	return out, truncated
}

// DigestInput 聚合参数。
type DigestInput struct {
	Messages []kafkaconn.ConsumedMessage
	Topic    string
	// Fields 字段投影（JSON path；valueText 解析后取值）。
	Fields []string
	// Width 单元格截断宽度（partition/offset 定位字段不截断）；≤0 不截断。
	Width int
	// GroupLimit / TopN / SampleRows 设计硬上限内的可调值。
	GroupLimit int
	TopN       int
	SampleRows int
}

// DigestResult 聚合输出。
type DigestResult struct {
	Matched int
	Stats   DigestStats
	Sample  []map[string]any
	// Rows format:"rows" 的定位字段行（物化 cursor 同形）。
	Rows []CursorRow
}

// AggregateDigest 对命中消息做本地聚合。
func AggregateDigest(input DigestInput) DigestResult {
	partitionCounts := map[string]int{}
	keyCounts := map[string]int{}
	nullKeyCount := 0
	noTimestampCount := 0
	timestamps := make([]int64, 0, len(input.Messages))
	fieldCounts := map[string]map[string]int{}
	for _, field := range input.Fields {
		fieldCounts[field] = map[string]int{}
	}

	// 单遍融合（评审 M-7）：stats 聚合与 cursor 行物化此前对每条命中各做
	// 一次完整 json.Unmarshal（10 万命中 × 512KB 时 CPU/带宽翻倍）——现在
	// 每条消息只解析一次，投影结果同时喂计数与行物化。
	rows := make([]CursorRow, 0, len(input.Messages))
	for _, message := range input.Messages {
		partitionCounts[strconv.FormatInt(int64(message.Partition), 10)]++
		key := message.Key
		if key == "" && message.KeyBase64 != "" {
			key = "(binary key)"
		}
		if strings.TrimSpace(key) == "" {
			nullKeyCount++
		} else {
			keyCounts[key]++
		}
		if message.Timestamp > 0 {
			timestamps = append(timestamps, message.Timestamp)
		} else {
			noTimestampCount++
		}
		projected := projectFieldValues(message.ValueText, input.Fields)
		for field, value := range projected {
			fieldCounts[field][value]++
		}
		rows = append(rows, cursorRowOf(message, projected, input.Width))
	}

	stats := DigestStats{PerPartition: map[string]int{}, Keys: map[string]int{}}
	stats.PerPartition, stats.PerPartitionLimit = topGroups(partitionCounts, input.GroupLimit)
	stats.Keys, stats.KeysLimit = topGroups(keyCounts, input.GroupLimit)
	stats.NullKeyCount = nullKeyCount
	stats.TimeHistogram = buildTimeHistogram(timestamps)
	if stats.TimeHistogram != nil {
		stats.TimeHistogram.NoTimestamp = noTimestampCount
	} else if noTimestampCount > 0 {
		stats.TimeHistogram = &TimeHistogram{NoTimestamp: noTimestampCount}
	}
	if len(input.Fields) > 0 {
		fields := make([]FieldStats, 0, len(input.Fields))
		for _, field := range input.Fields {
			counts := fieldCounts[field]
			values, truncated := topGroups(counts, input.TopN)
			fields = append(fields, FieldStats{Field: field, ValueCount: len(counts), Values: values, Truncated: truncated})
		}
		stats.Fields = fields
	}

	sampleRows := input.SampleRows
	if sampleRows <= 0 {
		sampleRows = 5
	}
	if sampleRows > len(input.Messages) {
		sampleRows = len(input.Messages)
	}
	sample := make([]map[string]any, 0, sampleRows)
	for _, message := range input.Messages[:sampleRows] {
		sample = append(sample, projectMessage(message, input.Fields, input.Width))
	}

	return DigestResult{Matched: len(input.Messages), Stats: stats, Sample: sample, Rows: rows}
}

// cursorRowOf 物化 cursor 行：定位字段（topic/partition/offset）不截断；
// key 与投影字段值过截断宽度。projected 由调用方单遍融合时传入（评审 M-7，
// 不再重复解析）。
func cursorRowOf(message kafkaconn.ConsumedMessage, projected map[string]string, width int) CursorRow {
	key := message.Key
	if key == "" && message.KeyBase64 != "" {
		key = "(binary key)"
	}
	if projected != nil {
		for field, value := range projected {
			projected[field] = DigestCellTruncate(value, width)
		}
	}
	return CursorRow{
		Topic:     message.Topic,
		Partition: message.Partition,
		Offset:    message.Offset,
		Key:       DigestCellTruncate(key, width),
		Fields:    projected,
	}
}

// projectMessage 样本行投影：定位字段不截断；key/value 过截断宽度；
// 大 value（512KB 截断标记）不出原文，占位 + 定位指引。
func projectMessage(message kafkaconn.ConsumedMessage, fields []string, width int) map[string]any {
	out := map[string]any{
		"topic":     message.Topic,
		"partition": message.Partition,
		"offset":    message.Offset,
	}
	if message.Timestamp > 0 {
		out["timestamp"] = message.Timestamp
	}
	key := message.Key
	if key == "" && message.KeyBase64 != "" {
		key = "(binary key)"
	}
	out["key"] = DigestCellTruncate(key, width)
	if message.Truncated {
		// 大 value 原文不出 MCP（设计 §3）：占位 + 定位指引。
		out["value"] = fmt.Sprintf("(value truncated at 512KB; locate via topic=%s partition=%d offset=%d)", message.Topic, message.Partition, message.Offset)
		out["valueOmitted"] = true
	} else {
		out["value"] = DigestCellTruncate(message.ValueText, width)
	}
	if len(fields) > 0 {
		projected := projectFieldValues(message.ValueText, fields)
		for field, value := range projected {
			projected[field] = DigestCellTruncate(value, width)
		}
		if projected != nil {
			out["fields"] = projected
		}
	}
	// schema 挂载定位信息（解码命中时填充）：AI 据此确认挂载生效与
	// wire id → subject/version 的解析结果。
	if message.SchemaID > 0 {
		out["schemaId"] = message.SchemaID
		if message.SchemaSubject != "" {
			out["schemaSubject"] = message.SchemaSubject
		}
		if message.SchemaVersion > 0 {
			out["schemaVersion"] = message.SchemaVersion
		}
	}
	// 解码失败可见性（schema 挂载/解压失败等）：错误不截断丢弃——静默
	// 吞掉时 AI 会把乱码原值误读成数据本身。截断用独立上限（比数据单元格
	// 宽：SR 错误含 subject/version 路径 + HTTP 状态码，120 字符常截掉
	// 定位与状态信息）。
	if message.DecodeError != "" {
		out["decodeError"] = DigestCellTruncate(message.DecodeError, digestDecodeErrorWidth)
	}
	return out
}
