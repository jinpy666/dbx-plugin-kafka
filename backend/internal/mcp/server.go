package mcp

// server.go：`mcp/call` 分派 + `mcp/settings/get|set` + UI intent 等待
//（设计 §1/§2/§4/§6.3）。main.go 只做方法表转发；本文件持有全部 MCP 状态：
// settings（持久化）、intent 状态表、cursor 会话、confirmToken 表。
// MCP 不订阅 stream（stream 由用户在 UI 操作）；digest 走一次性 Consume。

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"io.dbx.kafka.plugin/internal/kafkaconn"
	"io.dbx.kafka.plugin/internal/store"
)

// Server MCP 工具面状态与分派。并发安全（SDK 每请求一个 goroutine）。
type Server struct {
	svc      *kafkaconn.Service
	st       *store.Store // nil = 数据目录不可用，设置不持久化
	intents  *IntentStore
	cursors  *CursorStore
	confirms *ConfirmStore

	mu       sync.Mutex
	settings Settings

	// emit 把 kafka/ui/intent 事件交还 main.go 的当前 Emitter（nil 时跳过）。
	emit func(method string, params any)

	now func() time.Time
}

// NewServer 构造 MCP Server（settings 从数据目录加载，损坏回落默认；
// cursor 会话的 TTL/LRU 容量跟随 settings——同族 files cursorTtlSecs/
// maxCursorSessions 同款语义）。
func NewServer(svc *kafkaconn.Service, st *store.Store) *Server {
	settings := LoadSettings(st)
	return &Server{
		svc:      svc,
		st:       st,
		intents:  NewIntentStore(0, 0),
		cursors:  NewCursorStore(time.Duration(settings.CursorTtlSecs)*time.Second, settings.MaxCursorSessions, 0),
		confirms: NewConfirmStore(),
		settings: settings,
		now:      time.Now,
	}
}

// SetEmitter 注入事件回调（main.go 持锁转发到当前 Emitter）。
func (s *Server) SetEmitter(emit func(method string, params any)) {
	s.emit = emit
}

// SettingsGet 返回 `mcp/settings/get` 响应（当前生效值）。
func (s *Server) SettingsGet() map[string]any {
	s.mu.Lock()
	settings := s.settings
	s.mu.Unlock()
	return map[string]any{
		"settings":           settings,
		"responseLimitBytes": settings.ResponseLimitBytes,
	}
}

// SettingsSet 处理 `mcp/settings/set`：白名单部分更新 + 持久化。
func (s *Server) SettingsSet(updates map[string]any) (map[string]any, error) {
	if updates == nil {
		return nil, errors.New("updates object is required")
	}
	s.mu.Lock()
	settings, err := applySettingsUpdate(s.settings, updates)
	if err == nil {
		s.settings = settings
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	// cursor 会话参数同步到运行中的 store：TTL 生效于新物化会话，容量
	// 立即收敛（同族 files cursorTtlSecs/maxCursorSessions 语义）；confirm
	// 令牌 TTL 生效于下一次签发（已签发令牌不追溯，ldap 同构）。
	s.cursors.SetTTL(time.Duration(settings.CursorTtlSecs) * time.Second)
	s.cursors.SetCapacity(settings.MaxCursorSessions)
	s.confirms.SetTTL(time.Duration(settings.ConfirmTtlSecs) * time.Second)
	if err := SaveSettings(s.st, settings); err != nil {
		return nil, fmt.Errorf("persist mcp settings: %w", err)
	}
	return s.SettingsGet(), nil
}

// ReportUIState 处理 `kafka/ui/state/report`（前端回调）：
// 带 intentId = intent 回报（applied/rejected + summary）；
// 无 intentId = 快照型 report（sidecar 缓存最新快照）。
func (s *Server) ReportUIState(params map[string]any) error {
	intentID := strings.TrimSpace(stringField(params, "intentId"))
	summary, _ := params["summary"].(map[string]any)
	if summary == nil {
		summary = map[string]any{}
	}
	state := strings.TrimSpace(stringField(params, "status"))
	if intentID == "" {
		s.intents.SetSnapshot(summary)
		return nil
	}
	var intentState IntentState
	switch state {
	case "applied":
		intentState = IntentApplied
	case "rejected":
		intentState = IntentRejected
	default:
		return fmt.Errorf("status must be applied or rejected")
	}
	if !s.intents.Report(intentID, intentState, summary, strings.TrimSpace(stringField(params, "reason")), s.now()) {
		return fmt.Errorf("intent %q is unknown or expired", intentID)
	}
	return nil
}

// Call 处理 `mcp/call`：工具分派 + 16 KiB 响应上限（超出截断置 truncated）。
func (s *Server) Call(tool string, arguments map[string]any) (map[string]any, error) {
	if arguments == nil {
		arguments = map[string]any{}
	}
	var (
		result map[string]any
		err    error
	)
	switch tool {
	case "kafka_ui_search":
		result, err = s.uiSearch(arguments)
	case "kafka_ui_focus":
		result, err = s.uiFocus(arguments)
	case "kafka_ui_select":
		result, err = s.uiSelect(arguments)
	case "kafka_ui_state":
		result, err = s.uiState(arguments)
	case "kafka_ui_topics":
		result, err = s.uiTopics(arguments)
	case "kafka_messages_digest":
		result, err = s.messagesDigest(arguments)
	case "kafka_cursor_next":
		result, err = s.cursorNext(arguments)
	case "kafka_messages_produce":
		result, err = s.messagesProduce(arguments)
	case "kafka_topics_delete":
		result, err = s.topicsDelete(arguments)
	case "kafka_groups_offsets_reset":
		result, err = s.groupsOffsetsReset(arguments)
	case "kafka_topics_records_clear":
		result, err = s.topicsRecordsClear(arguments)
	default:
		return nil, errors.New(unknownToolMessage(tool))
	}
	if err != nil {
		return nil, err
	}
	return s.enforceResponseLimit(result), nil
}

// toolNameList 全部注册工具名（unknown tool 建议与计数用；单一来源取自
// 注册表，包初始化时展开一次）。
var toolNameList = func() []string {
	names := make([]string, 0, len(allToolDefinitions()))
	for _, tool := range allToolDefinitions() {
		names = append(names, tool["name"].(string))
	}
	return names
}()

// unknownToolMessage 未注册工具名的可行动错误（ssh unknown_tool_message
// 同构）：分隔符/大小写变体（kafka-messages-digest、KAFKA_MESSAGES_DIGEST）
// 建议注册名；全部 miss 时仍指向工具发现面。
func unknownToolMessage(name string) string {
	compact := func(text string) string {
		return strings.ToLower(strings.Map(func(r rune) rune {
			if r == '-' || r == '_' || r == ' ' {
				return -1
			}
			return r
		}, text))
	}
	query := compact(name)
	suggestion := ""
	for _, tool := range toolNameList {
		candidate := compact(tool)
		if candidate == query {
			suggestion = tool
			break
		}
		if len(query) >= 4 && (strings.Contains(candidate, query) || strings.Contains(query, candidate)) {
			suggestion = tool
		}
	}
	hint := ""
	if suggestion != "" {
		hint = fmt.Sprintf("Did you mean '%s'? ", suggestion)
	}
	return fmt.Sprintf("unknown tool: %s. %sUse mcp/tools to list the %d available tools", name, hint, len(toolNameList))
}

// --- UI intent 工具（设计 §1/§6.3） ---

// uiSearch / uiFocus / uiSelect 共用 intent 发起流程：
// 生成 intentId → 状态表登记 pending → 发 `kafka/ui/intent` 事件 → 等
// report（ReportWaitMs）→ 返回 {intentId, state, summary}。
func (s *Server) uiSearch(args map[string]any) (map[string]any, error) {
	if err := missingRequired(args, "topic"); err != nil {
		return nil, err
	}
	topic := strings.TrimSpace(stringField(args, "topic"))
	if topic == "" {
		return nil, errors.New("topic is required")
	}
	// partitions 宽容解析（JSON number / 整数字符串 / 逗号分隔串）；非法项
	// 报错而非静默丢弃（静默丢分区会改变消费语义）。
	partitions, err := parseIntList(args["partitions"])
	if err != nil {
		return nil, err
	}
	params := map[string]any{
		"topic":          topic,
		"connectionId":   strings.TrimSpace(stringField(args, "connectionId")),
		"offsetStrategy": strings.ToLower(strings.TrimSpace(stringField(args, "offsetStrategy"))),
		"limit":          args["limit"],
		"filter":         strings.TrimSpace(stringField(args, "filter")),
		"keyFilter":      strings.TrimSpace(stringField(args, "keyFilter")),
		"valueFilter":    strings.TrimSpace(stringField(args, "valueFilter")),
		"headerFilter":   strings.TrimSpace(stringField(args, "headerFilter")),
		"matchMode":      strings.ToLower(strings.TrimSpace(stringField(args, "matchMode"))),
		"groupId":        strings.TrimSpace(stringField(args, "groupId")),
		"partitions":     partitions,
		"offsetTime":     strings.TrimSpace(stringField(args, "offsetTime")),
	}
	return s.runIntent("search", params), nil
}

func (s *Server) uiFocus(args map[string]any) (map[string]any, error) {
	panel := strings.TrimSpace(stringField(args, "panel"))
	if panel == "" {
		return nil, errors.New("panel is required (messages | topics | groups | schemas)")
	}
	panel = strings.ToLower(panel)
	switch panel {
	case "messages", "topics", "groups", "schemas":
	default:
		// 前端在线时未知面板会 rejected，但离线/stdio 下只会 pending——
		// 在 sidecar 侧提前给出明确错误。
		return nil, fmt.Errorf("panel must be one of messages | topics | groups | schemas (got %q)", panel)
	}
	return s.runIntent("focus", map[string]any{
		"panel":        panel,
		"connectionId": strings.TrimSpace(stringField(args, "connectionId")),
	}), nil
}

func (s *Server) uiSelect(args map[string]any) (map[string]any, error) {
	partition, offset, err := locatorOf(args)
	if err != nil {
		return nil, err
	}
	return s.runIntent("select", map[string]any{
		"topic":        strings.TrimSpace(stringField(args, "topic")),
		"partition":    partition,
		"offset":       offset,
		"connectionId": strings.TrimSpace(stringField(args, "connectionId")),
	}), nil
}

// locatorOf 解析 partition+offset 定位参数（schema required =
// [partition, offset]：双缺一次枚举全点名，ssh 同款；整数必填且非负、
// 整数字符串宽容接受——前端 Number.parseInt(String(...)) 同语义；
// present-but-非法值仍逐参数精确点名）。
func locatorOf(args map[string]any) (int, int, error) {
	if err := missingRequired(args, "partition", "offset"); err != nil {
		return 0, 0, err
	}
	partition, ok := coerceInt(args["partition"])
	if !ok || partition < 0 {
		return 0, 0, fmt.Errorf("partition must be a non-negative integer (got %v)", args["partition"])
	}
	offset, ok := coerceInt(args["offset"])
	if !ok || offset < 0 {
		return 0, 0, fmt.Errorf("offset must be a non-negative integer (got %v)", args["offset"])
	}
	return partition, offset, nil
}

// runIntent intent 发起 + 等待 report。
func (s *Server) runIntent(action string, params map[string]any) map[string]any {
	randomID, err := randomHex(8)
	if err != nil {
		// intentId 是会话内相关性 id（非安全令牌）：纳秒兜底可接受。
		randomID = strconv.FormatInt(s.now().UnixNano(), 16)
	}
	intentID := "i-" + randomID
	s.intents.Register(intentID, action, params, s.now())
	if s.emit != nil {
		s.emit("kafka/ui/intent", map[string]any{
			"intentId": intentID,
			"action":   action,
			"params":   params,
		})
	}
	return s.waitIntent(intentID)
}

// waitIntent 轮询 intent 状态表直到终态或超时（默认 5s，settings 可调）。
func (s *Server) waitIntent(intentID string) map[string]any {
	s.mu.Lock()
	wait := time.Duration(s.settings.ReportWaitMs) * time.Millisecond
	s.mu.Unlock()
	deadline := s.now().Add(wait)
	for {
		time.Sleep(25 * time.Millisecond)
		intent, status := s.intents.Get(intentID, s.now())
		if status == LookupExpired {
			return map[string]any{"intentId": intentID, "state": "expired"}
		}
		if status == LookupFound && intent.State != IntentPending {
			out := map[string]any{"intentId": intentID, "state": string(intent.State)}
			if intent.Summary != nil {
				out["summary"] = intent.Summary
			}
			if intent.Reason != "" {
				out["reason"] = intent.Reason
			}
			return out
		}
		if !s.now().Before(deadline) {
			return map[string]any{
				"intentId": intentID,
				"state":    "pending",
				"hint":     "workbench not open or frontend did not respond; fall back to kafka_messages_digest (re-check later with kafka_ui_state)",
			}
		}
	}
}

// uiState 读 intent 结果（带 intentId）或最新 UI 快照（不带）；
// kafka 域内扩展：快照响应附带 stream 会话状态（设计 §6.3）。
func (s *Server) uiState(args map[string]any) (map[string]any, error) {
	intentID := strings.TrimSpace(stringField(args, "intentId"))
	if intentID == "" {
		out := map[string]any{"snapshot": s.intents.Snapshot()}
		if connectionID := strings.TrimSpace(stringField(args, "connectionId")); connectionID != "" {
			out["streams"] = s.svc.Streams.StatusesFor(connectionID)
		}
		return out, nil
	}
	intent, status := s.intents.Get(intentID, s.now())
	switch status {
	case LookupFound:
		out := map[string]any{"intentId": intentID, "state": string(intent.State)}
		if intent.Summary != nil {
			out["summary"] = intent.Summary
		}
		if intent.Reason != "" {
			out["reason"] = intent.Reason
		}
		return out, nil
	case LookupExpired:
		return map[string]any{"intentId": intentID, "state": "expired"}, nil
	default:
		return nil, fmt.Errorf("unknown intentId: %s", intentID)
	}
}

// --- 元发现 ---

// uiTopicsLimit `kafka_ui_topics` 的硬上限（设计 §2：limit 硬上限 50，纯定位用）。
const uiTopicsLimit = 50

// uiTopics 工具 `kafka_ui_topics`：topic 名清单（limit 硬上限 50；帮 AI
// 在 digest/ui_search 前定位真实 topic 名）。
func (s *Server) uiTopics(args map[string]any) (map[string]any, error) {
	if err := missingRequired(args, "connectionId"); err != nil {
		return nil, err
	}
	connectionID := strings.TrimSpace(stringField(args, "connectionId"))
	if connectionID == "" {
		return nil, errors.New("connectionId is required")
	}
	result, err := s.svc.ListTopics(getContext(), kafkaconn.TopicsListRequest{ConnectionID: connectionID})
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(result.Topics))
	for _, topic := range result.Topics {
		names = append(names, topic.Name)
	}
	sortStrings(names)
	truncated := len(names) > uiTopicsLimit
	names = clampStrings(names, uiTopicsLimit)
	return map[string]any{
		"topics":    names,
		"count":     len(names),
		"truncated": truncated,
		"limit":     uiTopicsLimit,
	}, nil
}

// --- 本地读（设计 §3/§6.3） ---

// messagesDigest 工具 `kafka_messages_digest`：复用 consume 的
// maxScanRecords 扫描语义与 filter 各通道，sidecar 本地聚合 + 物化 cursor。
func (s *Server) messagesDigest(args map[string]any) (map[string]any, error) {
	// 缺参一次枚举（schema required = [connectionId, topic]，按声明顺序
	// 全点名，ssh 同款）；present-but-空串仍由下方逐参数精确点名。
	if err := missingRequired(args, "connectionId", "topic"); err != nil {
		return nil, err
	}
	connectionID := strings.TrimSpace(stringField(args, "connectionId"))
	if connectionID == "" {
		return nil, errors.New("connectionId is required")
	}
	topic := strings.TrimSpace(stringField(args, "topic"))
	if topic == "" {
		return nil, errors.New("topic is required")
	}
	format := strings.ToLower(strings.TrimSpace(stringField(args, "format")))
	if format == "" {
		format = "digest"
	}
	if format != "digest" && format != "rows" {
		return nil, fmt.Errorf("format must be \"digest\" or \"rows\" (got %q)", stringField(args, "format"))
	}

	s.mu.Lock()
	settings := s.settings
	s.mu.Unlock()

	// 扫描语义缺省 earliest（digest 面向存量数据；显式 offsetStrategy 覆盖）。
	offsetStrategy := strings.TrimSpace(stringField(args, "offsetStrategy"))
	if offsetStrategy == "" {
		offsetStrategy = "earliest"
	}
	params := kafkaconn.ConsumeParams{
		ConnectionID:   connectionID,
		Topic:          topic,
		OffsetStrategy: offsetStrategy,
		MaxScanRecords: digestScanLimit(args, settings.DigestScanLimit),
		Filter:         strings.TrimSpace(stringField(args, "filter")),
		KeyFilter:      strings.TrimSpace(stringField(args, "keyFilter")),
		ValueFilter:    strings.TrimSpace(stringField(args, "valueFilter")),
		HeaderFilter:   strings.TrimSpace(stringField(args, "headerFilter")),
		MatchMode:      strings.TrimSpace(stringField(args, "matchMode")),
		IsolationLevel: strings.TrimSpace(stringField(args, "isolationLevel")),
		Decode:         strings.TrimSpace(stringField(args, "decode")),
		Decompression:  strings.TrimSpace(stringField(args, "decompression")),
	}
	if partitions, err := parseIntList(args["partitions"]); err != nil {
		return nil, err
	} else if len(partitions) > 0 {
		params.Partitions = partitions
	}
	if groupId := strings.TrimSpace(stringField(args, "groupId")); groupId != "" {
		params.GroupID = groupId
	}
	if offsetTime := strings.TrimSpace(stringField(args, "offsetTime")); offsetTime != "" {
		params.OffsetTime = offsetTime
	}
	for _, key := range []string{"timestampFrom", "timestampTo", "offsetFrom", "offsetTo"} {
		value, present, err := optionalInt64Arg(args, key)
		if err != nil {
			return nil, err
		}
		if !present {
			continue
		}
		bound := value
		switch key {
		case "timestampFrom":
			params.TimestampFrom = &bound
		case "timestampTo":
			params.TimestampTo = &bound
		case "offsetFrom":
			params.OffsetFrom = &bound
		case "offsetTo":
			params.OffsetTo = &bound
		}
	}
	if fieldFilters, err := parseFieldFilters(args["fieldFilters"]); err != nil {
		return nil, err
	} else if len(fieldFilters) > 0 {
		params.FieldFilters = fieldFilters
	}
	// schema 挂载（可选）：SR 解码 wire format 载荷后再做字段投影（与
	// produce 的 schema 同形状：{registry?, subject?, version?, format?}；
	// digest 允许缺 subject——按 wire id 查 SR）。不存在/未配置 SR 的连接
	// 带 schema 时由 kafkaconn 门禁显式报错（不静默丢弃参数）。
	if rawSchema, present := args["schema"]; present && rawSchema != nil {
		schemaRef, err := parseSchemaRef(rawSchema, false)
		if err != nil {
			return nil, err
		}
		params.Schema = schemaRef
	}
	// 保留上限 = 扫描上限：命中消息全量留存做本地聚合（仍不出 sidecar）。
	// 留存预算（评审 H-1）：聚合只读 valueText——跳过 valueBase64 通道，并
	// 给留存加字节预算：超预算即停止留存（matched 计数完整，cursor/样本行
	// 覆盖预算内子集），payload 以 retentionTruncated 提示调小 maxScanRecords。
	params.Limit = params.MaxScanRecords
	params.SkipValueBase64 = true
	params.RetentionByteBudget = digestRetentionByteBudget
	result, err := s.svc.Consume(getContext(), params)
	if err != nil {
		return nil, annotateClusterError(err)
	}
	// franz-go 对不存在 topic 的消费是静默空结果（与空 topic 无差别），
	// AI 会把"名字打错"误读成"没有数据"——扫描为 0 时补一次存在性校验。
	// 非空路径零额外成本；空 topic 多付一次元数据往返；校验本身因网络等
	// 原因失败时不掩盖消费结果（只对明确的 topic 不存在报错）。
	if result.Scanned == 0 {
		if _, describeErr := s.svc.DescribeTopic(getContext(), kafkaconn.TopicsDescribeRequest{ConnectionID: connectionID, Topic: topic}); describeErr != nil && topicNotFoundish(describeErr.Error()) {
			return nil, fmt.Errorf("topic %q does not exist on this connection; verify the name — kafka_ui_topics lists valid topic names (workbench mode; standalone stdio has no topic listing, use a topic name from the user or cluster docs)", topic)
		}
	}
	fields := stringSlice(args["fields"])
	aggregated := AggregateDigest(DigestInput{
		Messages:   result.Messages,
		Topic:      topic,
		Fields:     fields,
		Width:      settings.CellWidth,
		GroupLimit: settings.DigestGroupLimit,
		TopN:       settings.DigestTopN,
		SampleRows: settings.DigestSampleRows,
	})
	session := s.cursors.Put(aggregated.Rows, topic, s.now())

	payload := map[string]any{
		"connectionId":       connectionID,
		"topic":              topic,
		"matched":            aggregated.Matched,
		"scanned":            result.Scanned,
		"scanTruncated":      result.HasMore,
		"retentionTruncated": result.RetentionTruncated,
		"cursorId":           session.ID,
		"cursorTruncated":    session.Truncated,
	}
	if format == "rows" {
		rowLimit := settings.DigestRowLimit
		if rowLimit > len(aggregated.Rows) {
			rowLimit = len(aggregated.Rows)
		}
		payload["rows"] = aggregated.Rows[:rowLimit]
		return payload, nil
	}
	// 解码失败可见性：decodeError 静默吞掉时 AI 会把乱码 wire 字节误读成
	// 数据本身——计数 + 指引（样本行带 per-message decodeError）。
	if decodeFailures, decodeNote := digestNotes(result.Messages, aggregated); decodeFailures > 0 {
		payload["decodeFailures"] = decodeFailures
		payload["decodeNote"] = decodeNote
	}
	// 投影字段全不命中提示：matched>0 且每个请求字段都 0 命中时给路径核对
	// 指引（纯函数便于单测）。
	if note := fieldsNoteOf(aggregated, fields); note != "" {
		payload["fieldsNote"] = note
	}
	payload["stats"] = aggregated.Stats
	payload["sample"] = aggregated.Sample
	return payload, nil
}

// digestNotes 计算 decode 失败计数与指引（schema 挂载/解压失败等）。
func digestNotes(messages []kafkaconn.ConsumedMessage, aggregated DigestResult) (int, string) {
	decodeFailures := 0
	for _, message := range messages {
		if message.DecodeError != "" {
			decodeFailures++
		}
	}
	if decodeFailures == 0 {
		return 0, ""
	}
	return decodeFailures, fmt.Sprintf("%d of %d matched messages failed decode; sample rows carry per-message decodeError (verify schema.subject/version when a schema is mounted)", decodeFailures, aggregated.Matched)
}

// fieldsNoteOf 投影字段全不命中提示：matched>0 且每个请求字段都 0 命中时
// 给路径核对指引（sidecar 无法区分"路径错"与"载荷非 JSON"，提示两者都查）。
func fieldsNoteOf(aggregated DigestResult, fields []string) string {
	if len(fields) == 0 || aggregated.Matched == 0 {
		return ""
	}
	for _, fieldStat := range aggregated.Stats.Fields {
		if fieldStat.ValueCount > 0 {
			return ""
		}
	}
	return "projected fields matched 0 values across all matched messages; verify the JSON paths and that payloads are JSON (field projection skips non-JSON payloads)"
}

// digestScanLimit 扫描上限：显式 maxScanRecords（clamp ≤100000）优先，
// 否则用 settings.DigestScanLimit。
func digestScanLimit(args map[string]any, fallback int) int {
	limit := intArg(args["maxScanRecords"])
	if limit <= 0 {
		limit = fallback
	}
	if limit <= 0 {
		limit = 1000
	}
	if limit > 100000 {
		limit = 100000
	}
	return limit
}

// digestRetentionByteBudget digest 命中消息的留存字节预算（评审 H-1）：
// maxScanRecords=100000 × 大消息的双通道全量驻留理论可达百 GB 级（sidecar
// OOM）；聚合/样本/cursor 行只消费 valueText 与定位字段，64 MiB 预算把最坏
// 驻留压到常数级——超预算停止留存并置 retentionTruncated（计数完整）。
const digestRetentionByteBudget = 64 << 20

// cursorNext 工具 `kafka_cursor_next`：分批取定位字段行（n ≤20/批）。
func (s *Server) cursorNext(args map[string]any) (map[string]any, error) {
	if err := missingRequired(args, "cursorId"); err != nil {
		return nil, err
	}
	cursorID := strings.TrimSpace(stringField(args, "cursorId"))
	if cursorID == "" {
		return nil, errors.New("cursorId is required")
	}
	// n：缺失走会话缺省 20；存在但非法/非正报错（整数字符串宽容接受）。
	n := 0
	if raw, present := args["n"]; present && raw != nil {
		value, ok := coerceInt(raw)
		if !ok || value <= 0 {
			return nil, fmt.Errorf("n must be a positive integer (got %v)", raw)
		}
		n = value
	}
	result, status := s.cursors.Next(cursorID, NextRequest{N: n, Offset: offsetArg(args)}, s.now())
	switch status {
	case LookupExpired:
		// 过期报文携带实际生效 TTL（settings 可调，同族 files 同款语义），
		// 引导重发 digest。
		s.mu.Lock()
		ttl := s.settings.CursorTtlSecs
		s.mu.Unlock()
		return nil, fmt.Errorf("cursor expired (TTL %ds); re-run kafka_messages_digest", ttl)
	case LookupUnknown:
		// 淘汰/过期分不清是设计内（游标是进程内短会话）：报文带实际生效
		// TTL、会话容量与重建指引（ldap 同款质量，MCP_ACCEPTANCE §4）。
		s.mu.Lock()
		ttl := s.settings.CursorTtlSecs
		sessions := s.settings.MaxCursorSessions
		s.mu.Unlock()
		return nil, fmt.Errorf("unknown cursorId: %s — the cursor may have expired (TTL %ds) or been evicted (at most %d digest sessions are kept); re-run kafka_messages_digest", cursorID, ttl, sessions)
	}
	rows := make([]map[string]any, 0, len(result.Rows))
	for _, row := range result.Rows {
		out := map[string]any{
			"topic":     row.Topic,
			"partition": row.Partition,
			"offset":    row.Offset,
		}
		if row.Key != "" {
			out["key"] = row.Key
		}
		if row.Fields != nil {
			out["fields"] = row.Fields
		}
		rows = append(rows, out)
	}
	return map[string]any{
		"rows":       rows,
		"offset":     result.Offset,
		"nextOffset": result.NextOffset,
		"done":       result.Done,
	}, nil
}

// --- 写（设计 §4 两阶段） ---

const sourceMCP = "mcp"

// produceArgs 两阶段 hash 绑定的 canonical 形状（produce 单阶段也走结构体
// 归一，形状对齐）。
type produceArgs struct {
	ConnectionID string               `json:"connectionId"`
	Topic        string               `json:"topic"`
	Key          string               `json:"key,omitempty"`
	Value        string               `json:"value,omitempty"`
	KeyBase64    string               `json:"keyBase64,omitempty"`
	ValueBase64  string               `json:"valueBase64,omitempty"`
	Headers      map[string]string    `json:"headers,omitempty"`
	Partition    *int32               `json:"partition,omitempty"`
	Compression  string               `json:"compression,omitempty"`
	Schema       *kafkaconn.SchemaRef `json:"schema,omitempty"`
}

// messagesProduce 工具 `kafka_messages_produce`：单条小消息单阶段直执行
// （设计 §4：非破坏、可逆）；照常过连接只读门并审计 source:"mcp"。
// value/valueBase64 合计 ≤64 KiB（MCP 侧建议上限，超出提示走工作台）。
func (s *Server) messagesProduce(args map[string]any) (map[string]any, error) {
	// 缺参一次枚举（schema required = [connectionId, topic]，ssh 同款）。
	if err := missingRequired(args, "connectionId", "topic"); err != nil {
		return nil, err
	}
	connectionID := strings.TrimSpace(stringField(args, "connectionId"))
	topic := strings.TrimSpace(stringField(args, "topic"))
	if connectionID == "" {
		return nil, errors.New("connectionId is required")
	}
	if topic == "" {
		return nil, errors.New("topic is required")
	}
	if err := s.ensureWritable(connectionID); err != nil {
		return nil, err
	}
	req := produceArgs{ConnectionID: connectionID, Topic: topic}
	req.Key = stringField(args, "key")
	req.Value = stringField(args, "value")
	req.KeyBase64 = stringField(args, "keyBase64")
	req.ValueBase64 = stringField(args, "valueBase64")
	req.Compression = stringField(args, "compression")
	// headers：object（header 名 → 标量值）；存在但形状非法显式报错——
	// 数组/标量形状静默丢弃会让"想带 header"的消息无 header 落盘。边界
	// 输入（空键/超长键/大量 header）按 MCP 头部预算显式拒绝（对抗输入
	// 的报错质量：带实际值/数量与上限），不做静默截断。
	if rawHeaders, present := args["headers"]; present && rawHeaders != nil {
		headers, ok := rawHeaders.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("headers must be an object (header name -> scalar value, got %v)", rawHeaders)
		}
		if len(headers) > mcpHeaderMaxCount {
			return nil, fmt.Errorf("too many headers (%d > %d); use the workbench for header-heavy messages", len(headers), mcpHeaderMaxCount)
		}
		req.Headers = make(map[string]string, len(headers))
		for name, raw := range headers {
			if name == "" {
				return nil, errors.New("headers key cannot be empty (every header needs a non-empty name)")
			}
			if len(name) > mcpHeaderKeyMaxBytes {
				return nil, fmt.Errorf("header name exceeds %d bytes (got %d bytes)", mcpHeaderKeyMaxBytes, len(name))
			}
			switch raw.(type) {
			case string, float64, bool, nil:
				req.Headers[name] = fmt.Sprintf("%v", raw)
			default:
				return nil, fmt.Errorf("headers[%q] must be a scalar value (string/number/boolean, got %v)", name, raw)
			}
		}
	}
	if raw, present := args["partition"]; present && raw != nil {
		partitionValue, ok := coerceInt(raw)
		if !ok || partitionValue < 0 {
			return nil, fmt.Errorf("partition must be a non-negative integer (got %v)", raw)
		}
		partition := int32(partitionValue)
		req.Partition = &partition
	}
	if schema, ok := args["schema"].(map[string]any); ok {
		schemaRef, err := parseSchemaRef(schema, true)
		if err != nil {
			return nil, err
		}
		req.Schema = schemaRef
	} else if _, present := args["schema"]; present && args["schema"] != nil {
		return nil, errors.New("schema must be an object ({subject, version?, format?, registry?})")
	}
	if size := len(req.Value) + len(req.ValueBase64); size > mcpProduceMaxBytes {
		return nil, fmt.Errorf("value exceeds the MCP produce budget (%d > %d bytes); use the workbench for large payloads", size, mcpProduceMaxBytes)
	}

	result, err := s.svc.Produce(getContext(), kafkaconn.ProduceRequest{
		ConnectionID: connectionID,
		Topic:        topic,
		Key:          req.Key,
		Value:        req.Value,
		KeyBase64:    req.KeyBase64,
		ValueBase64:  req.ValueBase64,
		Headers:      req.Headers,
		Partition:    req.Partition,
		Compression:  req.Compression,
		Schema:       req.Schema,
		Source:       sourceMCP,
	})
	if err != nil {
		// produce 到不存在 topic 等：附可行动指引（与读路径同一套标注）。
		return nil, annotateClusterError(err)
	}
	return map[string]any{
		"success":   true,
		"topic":     result.Topic,
		"partition": result.Partition,
		"offset":    result.Offset,
		"timestamp": result.Timestamp,
	}, nil
}

// mcpProduceMaxBytes MCP 单条消息载荷预算（value 或 valueBase64 解码前
// 合计 ≤64 KiB；大载荷走工作台/生产面板）。
const mcpProduceMaxBytes = 64 * 1024

// MCP 头部预算（对抗输入边界）：header 名非空、≤1024 字节、条数 ≤64。
// 超限显式报错（带实际值与上限），不静默截断/丢弃。
const (
	mcpHeaderKeyMaxBytes = 1024
	mcpHeaderMaxCount    = 64
)

// deleteArgs 两阶段 hash 绑定的 canonical 形状（结构体输出确定，hash 稳定）。
// ConfirmTopic/ConfirmTopics 由 MCP 侧按 topics 填充，第二阶段执行时仍满足
// kafkaconn 的 confirmTopic 门禁（防误删，纵深防御）。
type deleteArgs struct {
	ConnectionID  string   `json:"connectionId"`
	Topics        []string `json:"topics"`
	ConfirmTopic  string   `json:"confirmTopic,omitempty"`
	ConfirmTopics []string `json:"confirmTopics,omitempty"`
}

// offsetsResetArgs 两阶段 hash 绑定的 canonical 形状。
type offsetsResetArgs struct {
	ConnectionID     string                     `json:"connectionId"`
	Group            string                     `json:"group"`
	Topics           []string                   `json:"topics,omitempty"`
	ResetTo          string                     `json:"resetTo"`
	TimestampMs      int64                      `json:"timestampMs,omitempty"`
	PartitionOffsets map[string]map[int32]int64 `json:"partitionOffsets,omitempty"`
}

// clearArgs 两阶段 hash 绑定的 canonical 形状。
type clearArgs struct {
	ConnectionID string `json:"connectionId"`
	Topic        string `json:"topic"`
}

// topicsDelete 工具 `kafka_topics_delete`：强制两阶段（preview +
// confirmToken；60s TTL、参数 hash 绑定）。confirmTopic 门禁在第二阶段
// 执行时仍走 kafkaconn 原语义（纵深防御）。
func (s *Server) topicsDelete(args map[string]any) (map[string]any, error) {
	// 缺参一次枚举（schema required = [connectionId, topics]，ssh 同款）；
	// topics 存在但为空数组由下方精确点名（不混入枚举）。
	if err := missingRequired(args, "connectionId", "topics"); err != nil {
		return nil, err
	}
	connectionID := strings.TrimSpace(stringField(args, "connectionId"))
	topics := stringSlice(args["topics"])
	if connectionID == "" {
		return nil, errors.New("connectionId is required")
	}
	if len(topics) == 0 {
		return nil, errors.New("topics is required")
	}
	if err := s.ensureDeleteAllowed(connectionID, "topics/delete"); err != nil {
		return nil, err
	}
	req := deleteArgs{ConnectionID: connectionID, Topics: topics}
	if len(topics) == 1 {
		req.ConfirmTopic = topics[0]
	} else {
		req.ConfirmTopics = topics
	}
	return s.twoPhase(connectionID, req, args, func() (any, error) {
		results, err := s.svc.DeleteTopics(getContext(), kafkaconn.TopicsDeleteRequest{
			ConnectionID:  connectionID,
			Topics:        topics,
			ConfirmTopic:  req.ConfirmTopic,
			ConfirmTopics: req.ConfirmTopics,
			Source:        sourceMCP,
		})
		if err != nil {
			return nil, err
		}
		return map[string]any{"success": true, "action": "delete", "results": results}, nil
	})
}

// groupsOffsetsReset 工具 `kafka_groups_offsets_reset`：强制两阶段。
func (s *Server) groupsOffsetsReset(args map[string]any) (map[string]any, error) {
	// 缺参一次枚举（schema required = [connectionId, group, resetTo]，按
	// 声明顺序全点名，ssh 同款）；resetTo 模式配套参数（topics/timestampMs/
	// partitionOffsets）由 validateResetRequest 按模式精确点名。
	if err := missingRequired(args, "connectionId", "group", "resetTo"); err != nil {
		return nil, err
	}
	connectionID := strings.TrimSpace(stringField(args, "connectionId"))
	group := strings.TrimSpace(stringField(args, "group"))
	if connectionID == "" {
		return nil, errors.New("connectionId is required")
	}
	if group == "" {
		return nil, errors.New("group is required")
	}
	if err := s.ensureWritable(connectionID); err != nil {
		return nil, err
	}
	req := offsetsResetArgs{
		ConnectionID: connectionID,
		Group:        group,
		Topics:       stringSlice(args["topics"]),
		ResetTo:      strings.TrimSpace(stringField(args, "resetTo")),
	}
	// timestampMs 宽容解析（整数字符串接受）；存在但非法即报错——
	// 静默折算为 0 会把 reset 变成"重置到纪元"（准确性红线）。
	if raw, present := args["timestampMs"]; present && raw != nil {
		value, ok := coerceInt64(raw)
		if !ok {
			return nil, fmt.Errorf("timestampMs must be an integer (unix milliseconds, got %v)", raw)
		}
		req.TimestampMs = value
	}
	if req.ResetTo == "" {
		return nil, errors.New("resetTo is required (earliest | latest | timestamp | partitionOffset)")
	}
	if partitionOffsets, err := parsePartitionOffsets(args["partitionOffsets"]); err != nil {
		return nil, err
	} else if partitionOffsets != nil {
		req.PartitionOffsets = partitionOffsets
	}
	// 预览前体检：避免"预览成功 → 确认时才报参数缺"白烧一次性令牌
	//（归一规则与 kafkaconn normalizeResetMode 同源，见 groups.go）。
	if err := validateResetRequest(req); err != nil {
		return nil, err
	}
	return s.twoPhase(connectionID, req, args, func() (any, error) {
		result, err := s.svc.ResetGroupOffsets(getContext(), kafkaconn.GroupOffsetResetRequest{
			ConnectionID:     connectionID,
			Group:            group,
			Topics:           req.Topics,
			ResetTo:          req.ResetTo,
			TimestampMs:      req.TimestampMs,
			PartitionOffsets: req.PartitionOffsets,
			Source:           sourceMCP,
		})
		if err != nil {
			return nil, err
		}
		return map[string]any{"success": true, "action": "offsetsReset", "rows": result.Rows}, nil
	})
}

// validateResetRequest resetTo 模式与配套参数的预检（预览前拒绝，不签发
// 令牌）：earliest/latest/timestamp 必须给 topics；timestamp 必须给正的
// timestampMs（0 等价重置到纪元，几乎必是参数缺失的产物）；partitionOffset
// 必须给 partitionOffsets。归一与大小写别名与 kafkaconn normalizeResetMode
// 保持一致（groups.go），漂移时以那边为准。
func validateResetRequest(req offsetsResetArgs) error {
	switch strings.ToLower(req.ResetTo) {
	case "earliest", "latest":
		if len(req.Topics) == 0 {
			return fmt.Errorf("topics is required for resetTo=%s (topic name list)", req.ResetTo)
		}
	case "timestamp":
		if len(req.Topics) == 0 {
			return errors.New("topics is required for resetTo=timestamp (topic name list)")
		}
		if req.TimestampMs <= 0 {
			return errors.New("timestampMs is required (unix milliseconds > 0) for resetTo=timestamp")
		}
	case "partitionoffset", "partition_offset", "partitionoffsets":
		if len(req.PartitionOffsets) == 0 {
			return errors.New("partitionOffsets is required for resetTo=partitionOffset (topic -> partition -> offset)")
		}
	default:
		return fmt.Errorf("resetTo must be earliest, latest, timestamp, or partitionOffset (got %q)", req.ResetTo)
	}
	return nil
}

// topicsRecordsClear 工具 `kafka_topics_records_clear`：强制两阶段
// （purge 同级红线：read_only/allow_delete 与门在预览与执行两道都拒绝）。
func (s *Server) topicsRecordsClear(args map[string]any) (map[string]any, error) {
	if err := missingRequired(args, "connectionId", "topic"); err != nil {
		return nil, err
	}
	connectionID := strings.TrimSpace(stringField(args, "connectionId"))
	topic := strings.TrimSpace(stringField(args, "topic"))
	if connectionID == "" {
		return nil, errors.New("connectionId is required")
	}
	if topic == "" {
		return nil, errors.New("topic is required")
	}
	if err := s.ensureDeleteAllowed(connectionID, "topics/records/clear"); err != nil {
		return nil, err
	}
	req := clearArgs{ConnectionID: connectionID, Topic: topic}
	return s.twoPhase(connectionID, req, args, func() (any, error) {
		result, err := s.svc.ClearTopicRecords(getContext(), kafkaconn.TopicRecordsClearRequest{
			ConnectionID: connectionID,
			Topic:        topic,
			// MCP 面无 confirmTopic 参数（两阶段令牌即确认）；执行层仍带
			// 同名确认满足 kafkaconn 防误清空门禁（纵深防御）。
			ConfirmTopic: topic,
			Source:       sourceMCP,
		})
		if err != nil {
			return nil, err
		}
		return map[string]any{"success": true, "action": "recordsClear", "rows": result.Rows}, nil
	})
}

// twoPhase 两阶段通用骨架：
// 无 confirmToken → preview + 一次性令牌（60s TTL，hash 绑定，不执行写）；
// 带 token → Consume（一次性 + hash 一致）→ 执行。
func (s *Server) twoPhase(connectionID string, req any, args map[string]any, execute func() (any, error)) (map[string]any, error) {
	canonical, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	paramHash := HashParams(canonical)
	token := strings.TrimSpace(stringField(args, "confirmToken"))
	if token == "" {
		issued, expiresAt, issueErr := s.confirms.Issue(paramHash, s.now())
		if issueErr != nil {
			return nil, issueErr
		}
		return map[string]any{
			"preview":      req,
			"confirmToken": issued,
			"expiresAt":    expiresAt.UTC().Format(time.RFC3339),
			"note":         "nothing written yet; repeat the same arguments with confirmToken to execute",
		}, nil
	}
	switch result := s.confirms.Consume(token, paramHash, s.now()); result {
	case ConfirmOK:
		out, err := execute()
		if err != nil {
			return nil, err
		}
		if typed, ok := out.(map[string]any); ok {
			return typed, nil
		}
		return map[string]any{"success": true}, nil
	case ConfirmExpired:
		return nil, fmt.Errorf("confirmToken expired (TTL %ds); request a new preview", int(s.confirms.TTL().Seconds()))
	case ConfirmHashMismatch:
		return nil, errors.New("arguments changed since the preview; request a new confirmToken")
	default:
		return nil, errors.New("confirmToken unknown or already used; request a new preview")
	}
}

// ensureWritable mcp/call 侧只读门（纵深防御：工具清单已剔除写工具，
// 直接调用仍拒绝）。消息附可行动出路：stdio 内联连接默认 readOnly=true，
// AI 调用方最常见的卡点就是不知道如何解除（真机 agent 实测，2026-09-14）。
func (s *Server) ensureWritable(connectionID string) error {
	readOnly, _ := s.svc.PolicyOf(connectionID)
	if readOnly {
		return fmt.Errorf("connection %q is read-only; write tools are refused — standalone stdio inline connections open read-only by default: re-call with \"readOnly\": false on the inline connection (for a saved connection, change the setting in the DBX workbench)", connectionID)
	}
	return nil
}

// ensureDeleteAllowed 删除类门（read_only ∥ !allow_delete）；出路指引同
// ensureWritable（delete 族还叠加 allowDelete 开关）。
func (s *Server) ensureDeleteAllowed(connectionID, action string) error {
	readOnly, allowDelete := s.svc.PolicyOf(connectionID)
	if readOnly {
		return fmt.Errorf("connection %q is read-only; %s is refused — re-call with \"readOnly\": false (and \"allowDelete\": true for delete-class tools) on the inline connection", connectionID, action)
	}
	if !allowDelete {
		return fmt.Errorf("connection %q does not allow delete operations (allow_delete=false); %s is refused — re-call with \"allowDelete\": true on the inline connection", connectionID, action)
	}
	return nil
}

// --- 参数解析辅助（kafkaconn 形状对齐） ---

// topicNotFoundish 报告错误文本是否表示 topic 不存在（kafkaconn "topic %q
// not found" 与 kadm UNKNOWN_TOPIC_OR_PARTITION 两种来源；匹配保持宽松，
// 文本随上游版本变化时在此单点调整）。
func topicNotFoundish(message string) bool {
	lowered := strings.ToLower(message)
	return strings.Contains(lowered, "unknown topic") ||
		strings.Contains(lowered, "unknown_topic") ||
		strings.Contains(lowered, "topic not found") ||
		strings.Contains(lowered, "not found") ||
		strings.Contains(lowered, "does not host") ||
		strings.Contains(lowered, "does not exist")
}

// annotateClusterError 集群操作失败附加可行动指引（digest/produce 等
// 读写共用）：连接未注册 → 检查 connectionId / stdio 内联参数；topic 疑似
// 不存在 → kafka_ui_topics 定位。匹配不上就原样返回。
func annotateClusterError(err error) error {
	if err == nil {
		return nil
	}
	if topicNotFoundish(err.Error()) {
		return fmt.Errorf("%w; verify the topic name — kafka_ui_topics lists valid topic names (workbench mode; standalone stdio has no topic listing, use a topic name from the user or cluster docs)", err)
	}
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "is not connected") {
		return fmt.Errorf("%w; verify the connectionId (or pass inline connection parameters in standalone stdio mode)", err)
	}
	return err
}

// parseIntList 整数数组参数宽容解析：JSON number 数组、整数字符串数组、
// 单个逗号/空白分隔字符串（UI 表单 textarea 语义）都接受；存在但含非法/负数
// 项时报错——静默丢弃会改变扫描/消费语义（准确性）。
func parseIntList(raw any) ([]int32, error) {
	switch value := raw.(type) {
	case nil:
		return nil, nil
	case []any:
		out := make([]int32, 0, len(value))
		for index, item := range value {
			number, ok := coerceInt(item)
			if !ok || number < 0 {
				return nil, fmt.Errorf("partitions[%d] must be a non-negative integer (got %v)", index, item)
			}
			out = append(out, int32(number))
		}
		return out, nil
	case string:
		out := []int32{}
		for _, piece := range strings.FieldsFunc(value, func(r rune) bool { return r == ',' || r == ' ' || r == '\n' || r == '\r' || r == '\t' }) {
			number, err := strconv.Atoi(strings.TrimSpace(piece))
			if err != nil || number < 0 {
				return nil, fmt.Errorf("partitions must be non-negative integers (got %q)", piece)
			}
			out = append(out, int32(number))
		}
		return out, nil
	default:
		return nil, fmt.Errorf("partitions must be an array of non-negative integers (got %v)", raw)
	}
}

// optionalInt64Arg 可选整数参数三态：缺失 → (0,false,nil)；JSON number 或
// 整数字符串 → (值,true,nil)；存在但非法 → 报错（不静默忽略）。
func optionalInt64Arg(args map[string]any, key string) (int64, bool, error) {
	raw, present := args[key]
	if !present || raw == nil {
		return 0, false, nil
	}
	value, ok := coerceInt64(raw)
	if !ok {
		return 0, false, fmt.Errorf("%s must be an integer (got %v)", key, raw)
	}
	return value, true, nil
}

// parseFieldFilters 解析字段级过滤（ConsumeFieldFilter 形状直通）。
func parseFieldFilters(raw any) ([]kafkaconn.ConsumeFieldFilter, error) {
	items, ok := raw.([]any)
	if !ok || len(items) == 0 {
		return nil, nil
	}
	out := make([]kafkaconn.ConsumeFieldFilter, 0, len(items))
	for index, item := range items {
		object, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("fieldFilters[%d] must be an object", index)
		}
		filter := kafkaconn.ConsumeFieldFilter{
			Source:   stringField(object, "source"),
			Path:     stringField(object, "path"),
			Operator: stringField(object, "operator"),
			Value:    stringField(object, "value"),
		}
		if enabled, ok := object["enabled"].(bool); ok {
			filter.Enabled = &enabled
		}
		out = append(out, filter)
	}
	return out, nil
}

// parseSchemaRef 解析 schema 挂载参数（produce/digest 共形）：对象
// {registry?, subject, version?, format?}。produce 必须给 subject（编码按
// subject+version 取元数据）；digest 允许缺省（按 wire id 查 SR）。version
// 缺省 = 最新；宽容整数字符串，存在但非法/负数即报错——静默折 0 会把
// "version 打错"变成"取最新"（准确性）。
func parseSchemaRef(raw any, requireSubject bool) (*kafkaconn.SchemaRef, error) {
	object, ok := raw.(map[string]any)
	if !ok {
		return nil, errors.New("schema must be an object ({subject, version?, format?, registry?})")
	}
	ref := &kafkaconn.SchemaRef{
		Registry: strings.TrimSpace(stringField(object, "registry")),
		Subject:  strings.TrimSpace(stringField(object, "subject")),
		Format:   strings.TrimSpace(stringField(object, "format")),
	}
	if requireSubject && ref.Subject == "" {
		return nil, errors.New("schema.subject is required")
	}
	if rawVersion, present := object["version"]; present && rawVersion != nil {
		version, ok := coerceInt64(rawVersion)
		if !ok || version < 0 {
			return nil, fmt.Errorf("schema.version must be a non-negative integer (got %v)", rawVersion)
		}
		ref.Version = version
	}
	return ref, nil
}

// parsePartitionOffsets 解析 resetTo=partitionOffset 的嵌套映射
// （topic → partition → offset）。
func parsePartitionOffsets(raw any) (map[string]map[int32]int64, error) {
	object, ok := raw.(map[string]any)
	if !ok || len(object) == 0 {
		return nil, nil
	}
	out := make(map[string]map[int32]int64, len(object))
	for topic, partitions := range object {
		partitionMap, ok := partitions.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("partitionOffsets[%q] must be an object", topic)
		}
		inner := make(map[int32]int64, len(partitionMap))
		for partition, offset := range partitionMap {
			// 整数 strictly（评审 M：ParseFloat+int64 静默截断把 1.9 折成
			// 1——与 timestampMs/schema.version 同一条「绝不静默折算」红线，
			// 存在但非法必须精确点名报错）。
			offsetNumber, ok := coerceInt64(offset)
			if !ok || offsetNumber < 0 {
				return nil, fmt.Errorf("partitionOffsets[%q][%q] must be a non-negative integer (got %v)", topic, partition, offset)
			}
			partitionNumber, err := strconv.ParseInt(strings.TrimSpace(partition), 10, 32)
			if err != nil || partitionNumber < 0 {
				return nil, fmt.Errorf("partitionOffsets[%q] partition %q must be a non-negative integer", topic, partition)
			}
			inner[int32(partitionNumber)] = offsetNumber
		}
		out[topic] = inner
	}
	return out, nil
}

// sortStrings 就地排序（topic 名清单稳定输出）。
func sortStrings(names []string) {
	sort.Strings(names)
}

// --- 响应上限（设计 §3：单工具响应 16 KiB） ---

// enforceResponseLimit 超限时按 sample → rows → stats 顺序丢弃重字段并置
// truncated；仍超限返回占位（指引调小请求或调大 responseLimitBytes）。
func (s *Server) enforceResponseLimit(result map[string]any) map[string]any {
	s.mu.Lock()
	limit := s.settings.ResponseLimitBytes
	s.mu.Unlock()
	if payloadSize(result) <= limit {
		return result
	}
	trimmed := cloneMap(result)
	for _, key := range []string{"sample", "rows", "stats"} {
		delete(trimmed, key)
		trimmed["truncated"] = true
		if payloadSize(trimmed) <= limit {
			return trimmed
		}
	}
	return map[string]any{
		"truncated": true,
		"note":      "response exceeded the configured size limit; narrow the request or raise responseLimitBytes via mcp/settings/set",
	}
}
