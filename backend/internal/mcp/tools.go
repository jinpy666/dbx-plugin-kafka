package mcp

// tools.go：`mcp/tools` 工具注册（JSON Schema，形状照 ssh mcp.rs
// tool_definitions / ldap Go 版）。命名 <域>_<动作>、字段 camelCase。
//
// 只读 / allow_delete=false 连接：`mcp/tools {connectionId}` 显式给到时写
// 工具不进清单（设计 §4），omittedWriteTools 说明原因；未给 connectionId
// 时全量列出（调用时仍按连接策略门拒绝，纵深防御）。

// connectionProperty 各连接类工具共享的 connectionId 声明（凭据由宿主
// lifecycle 转发，参数里不出现密码）。
func connectionProperty() map[string]any {
	return map[string]any{
		"type":        "string",
		"description": "DBX saved Kafka connection id; credentials are resolved by the DBX host and never travel in tool arguments",
	}
}

// toolEntry 单个工具定义。required 为空时省略键：nil 切片的 JSON 形状是
// "required":null，严格校验的 MCP 宿主（zcode tools/list zod 校验）会据此
// 拒收整个服务器（2026-09-14 真机接入实测）。
func toolEntry(name, description string, required []string, properties map[string]any) map[string]any {
	schema := map[string]any{
		"type":       "object",
		"properties": properties,
	}
	if len(required) > 0 {
		schema["required"] = required
	}
	return map[string]any{
		"name":        name,
		"description": description,
		"inputSchema": schema,
	}
}

// writeToolNames 写类工具名单（连接策略不满足时不进工具清单）：
//   - produce / groups_offsets_reset：只读即剔除；
//   - topics_delete / topics_records_clear：只读 或 allow_delete=false 剔除。
var writeToolNames = []string{
	"kafka_messages_produce",
	"kafka_topics_delete",
	"kafka_groups_offsets_reset",
	"kafka_topics_records_clear",
}

// allToolDefinitions 全量工具定义（M3 设计 §2/§6.3：UI 驱动 4 + 元发现 1 +
// 本地读 2 + 写 4）。
func allToolDefinitions() []map[string]any {
	return []map[string]any{
		toolEntry(
			"kafka_ui_search",
			"Fill the Kafka workbench consume form with the given conditions (topic, offset strategy, limit, filter channels) and trigger the consume. Results stay visible in the Messages panel (the user can keep working with them); returns the applied state plus a small summary (count + first rows with partition/offset anchors). Requires the DBX workbench to be open: without a frontend the call reports state=pending with a hint, fall back to kafka_messages_digest instead.",
			[]string{"topic"},
			map[string]any{
				"connectionId":   connectionProperty(),
				"topic":          map[string]any{"type": "string", "description": "Topic to consume"},
				"offsetStrategy": map[string]any{"type": "string", "enum": []string{"latest", "recent", "earliest", "committed", "timestamp", "offset"}, "description": "Offset strategy (default latest; recent starts each partition scanWindow records before the log end)"},
				"limit":          map[string]any{"type": "integer", "description": "Max matched messages fetched"},
				"filter":         map[string]any{"type": "string", "description": "Full-text filter (key+value+headers)"},
				"keyFilter":      map[string]any{"type": "string", "description": "Key channel filter"},
				"valueFilter":    map[string]any{"type": "string", "description": "Value channel filter"},
				"headerFilter":   map[string]any{"type": "string", "description": "Header channel filter"},
				"matchMode":      map[string]any{"type": "string", "enum": []string{"contains", "prefix", "exact", "regex"}, "description": "Filter match mode (default contains)"},
				"groupId":        map[string]any{"type": "string", "description": "Consumer group id (mutually exclusive with partitions)"},
				"partitions":     map[string]any{"type": "array", "items": map[string]any{"type": "integer"}, "description": "Explicit partition list (integers; numeric strings or one comma-separated string also accepted)"},
				"offsetTime":     map[string]any{"type": "string", "description": "RFC3339 / unix ms (offsetStrategy=timestamp)"},
			},
		),
		toolEntry(
			"kafka_ui_focus",
			"Focus a workbench panel (messages | topics | groups | schemas). Requires the DBX workbench to be open; without a frontend the call reports state=pending.",
			nil,
			map[string]any{
				"connectionId": connectionProperty(),
				"panel":        map[string]any{"type": "string", "enum": []string{"messages", "topics", "groups", "schemas"}, "description": "Panel to focus (case-insensitive)"},
			},
		),
		toolEntry(
			"kafka_ui_select",
			"Locate a message by partition+offset in the current Messages panel results and open its detail drawer. Requires the DBX workbench to be open with matching results; reports rejected when the locator is not in the current results.",
			[]string{"partition", "offset"},
			map[string]any{
				"connectionId": connectionProperty(),
				"topic":        map[string]any{"type": "string", "description": "Optional topic check (locator = topic-partition-offset)"},
				"partition":    map[string]any{"type": "integer", "description": "Partition of the message (non-negative; numeric strings tolerated)"},
				"offset":       map[string]any{"type": "integer", "description": "Offset of the message (non-negative; numeric strings tolerated)"},
			},
		),
		toolEntry(
			"kafka_ui_state",
			"Read a UI intent result by intentId, or (without intentId) the latest workbench snapshot reported by the frontend (current panel, consume form values, result count, selected locator). The snapshot response additionally lists the connection's stream sessions. Use it to re-check an intent that returned pending.",
			nil,
			map[string]any{
				"connectionId": connectionProperty(),
				"intentId":     map[string]any{"type": "string", "description": "intentId from a previous kafka_ui_* call; omit to read the latest snapshot (streams are appended when connectionId is given)"},
			},
		),
		toolEntry(
			"kafka_ui_topics",
			"List topic names on the connection (hard limit 50, locator-only) so a valid topic name can be picked before kafka_ui_search or kafka_messages_digest.",
			[]string{"connectionId"},
			map[string]any{"connectionId": connectionProperty()},
		),
		toolEntry(
			"kafka_messages_digest",
			"Consume a topic with the consume filter channels (server-side offsets, local filters) and aggregate the matches locally in the sidecar — MCP never subscribes to a stream. Default format=digest returns counts and distributions only (per-partition counts, key group-by, time histogram, optional JSON-field projection distinct/topN, <=5 sample rows with partition/offset anchors); format=rows returns at most 20 locator rows. Every digest also materializes a cursor for kafka_cursor_next so pages are fetched without re-consuming. Message bodies never leave the sidecar (large values become placeholders plus a partition/offset locator).",
			[]string{"connectionId", "topic"},
			map[string]any{
				"connectionId":     connectionProperty(),
				"topic":            map[string]any{"type": "string", "description": "Topic to scan"},
				"offsetStrategy":   map[string]any{"type": "string", "enum": []string{"latest", "recent", "earliest", "committed", "timestamp", "offset"}, "description": "Offset strategy (default earliest; recent starts each partition scanWindow records before the log end)"},
				"offsetTime":       map[string]any{"type": "string", "description": "RFC3339 / unix ms (offsetStrategy=timestamp)"},
				"partitions":       map[string]any{"type": "array", "items": map[string]any{"type": "integer"}, "description": "Explicit partition list (integers; numeric strings or one comma-separated string also accepted)"},
				"partitionOffsets": map[string]any{"type": "object", "description": "Explicit offsets per partition (offsetStrategy=offset): {\"0\": 120, \"3\": \"55\"}; numeric strings tolerated"},
				"maxScanRecords":   map[string]any{"type": "integer", "description": "Scan budget (default 1000, max 100000; consume maxScanRecords semantics; numeric strings tolerated)"},
				"filter":           map[string]any{"type": "string", "description": "Full-text filter (key+value+headers)"},
				"keyFilter":        map[string]any{"type": "string", "description": "Key channel filter"},
				"valueFilter":      map[string]any{"type": "string", "description": "Value channel filter"},
				"headerFilter":     map[string]any{"type": "string", "description": "Header channel filter"},
				"matchMode":        map[string]any{"type": "string", "enum": []string{"contains", "prefix", "exact", "regex"}, "description": "Filter match mode (default contains)"},
				"groupId":          map[string]any{"type": "string", "description": "Consumer group id — offsetStrategy=committed starts from the group's committed offsets (mutually exclusive with partitions/partitionOffsets)"},
				"timestampFrom":    map[string]any{"type": "integer", "description": "Unix ms lower bound on message timestamp (inclusive; numeric strings tolerated)"},
				"timestampTo":      map[string]any{"type": "integer", "description": "Unix ms upper bound on message timestamp (inclusive; numeric strings tolerated)"},
				"offsetFrom":       map[string]any{"type": "integer", "description": "Offset lower bound (inclusive; numeric strings tolerated)"},
				"offsetTo":         map[string]any{"type": "integer", "description": "Offset upper bound (inclusive; numeric strings tolerated)"},
				"isolationLevel":   map[string]any{"type": "string", "enum": []string{"read_uncommitted", "read_committed"}, "description": "Transaction isolation (default read_uncommitted)"},
				"fieldFilters":     map[string]any{"type": "array", "items": map[string]any{"type": "object", "properties": map[string]any{"source": map[string]any{"type": "string"}, "path": map[string]any{"type": "string"}, "operator": map[string]any{"type": "string"}, "value": map[string]any{"type": "string"}}}, "description": "Field-level filters (value/key/header channels with JSON path)"},
				"decode":           map[string]any{"type": "string", "enum": []string{"none", "base64"}, "description": "Value second-pass decode"},
				"decompression":    map[string]any{"type": "string", "description": "Decompression (gzip | lz4 | zstd | snappy)"},
				"schema":           map[string]any{"type": "object", "properties": map[string]any{"registry": map[string]any{"type": "string"}, "subject": map[string]any{"type": "string"}, "version": map[string]any{"type": "integer"}, "format": map[string]any{"type": "string"}}, "description": "Schema Registry mount to decode Confluent wire-format values into JSON before projection ({subject?, version?}; subject optional — resolved from the wire schema id; requires schemaRegistry=confluent on the connection)"},
				"fields":           map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "JSON-path projection fields aggregated as distinct/topN (after schema decode when mounted), e.g. [\"$.user.id\"]"},
				"format":           map[string]any{"type": "string", "enum": []string{"digest", "rows"}, "description": "digest (default) = counts + sample; rows = at most 20 locator rows"},
			},
		),
		toolEntry(
			"kafka_cursor_next",
			"Fetch the next batch (n<=20) of locator rows from a digest session: topic-partition-offset (+ optional projected fields) only, no filter re-send and no re-scan. Expired cursors return an explicit error (with the effective TTL, default 10 minutes, adjustable via mcp/settings/set cursorTtlSecs) suggesting a fresh kafka_messages_digest.",
			[]string{"cursorId"},
			map[string]any{
				"cursorId": map[string]any{"type": "string", "description": "cursorId returned by kafka_messages_digest"},
				"n":        map[string]any{"type": "integer", "description": "Batch size (default 20, max 20; numeric strings tolerated)"},
				"offset":   map[string]any{"type": "integer", "description": "Start offset; omit to continue where the previous batch stopped"},
			},
		),
		toolEntry(
			"kafka_messages_produce",
			"Produce one small message (single-phase, executed directly; audited with source=mcp). value/valueBase64 together are capped at 64 KiB — larger payloads belong in the workbench. Refused on read-only connections.",
			[]string{"connectionId", "topic"},
			map[string]any{
				"connectionId": connectionProperty(),
				"topic":        map[string]any{"type": "string", "description": "Target topic"},
				"key":          map[string]any{"type": "string", "description": "Message key (or keyBase64)"},
				"value":        map[string]any{"type": "string", "description": "Message value (or valueBase64)"},
				"keyBase64":    map[string]any{"type": "string", "description": "Binary key as base64"},
				"valueBase64":  map[string]any{"type": "string", "description": "Binary value as base64"},
				"headers":      map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string"}, "description": "Message headers"},
				"partition":    map[string]any{"type": "integer", "description": "Explicit partition (optional; non-negative, numeric strings tolerated)"},
				"compression":  map[string]any{"type": "string", "enum": []string{"none", "gzip", "lz4", "zstd", "snappy"}, "description": "Compression (default none)"},
				"schema":       map[string]any{"type": "object", "properties": map[string]any{"registry": map[string]any{"type": "string"}, "subject": map[string]any{"type": "string"}, "version": map[string]any{"type": "integer"}, "format": map[string]any{"type": "string"}}, "description": "Schema Registry mount for wire-format encoding"},
			},
		),
		toolEntry(
			"kafka_topics_delete",
			"Delete topics (two-phase: the first call without confirmToken returns a preview plus a one-time confirmToken (60s TTL, bound to the parameter hash) and writes nothing — repeat the same arguments with the token to execute). Read-only or allow_delete=false connections never list this tool. Every write is audited with source=mcp.",
			[]string{"connectionId", "topics"},
			map[string]any{
				"connectionId": connectionProperty(),
				"topics":       map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "Topic names to delete"},
				"confirmToken": map[string]any{"type": "string", "description": "One-time token returned by the preview call"},
			},
		),
		toolEntry(
			"kafka_groups_offsets_reset",
			"Reset consumer group offsets (two-phase preview/confirm like kafka_topics_delete). resetTo: earliest | latest | timestamp | partitionOffset (case-insensitive). Per mode the preview also requires: topics for earliest/latest/timestamp; a positive timestampMs (unix ms) for timestamp; partitionOffsets (topic -> partition -> offset) for partitionOffset. Read-only connections never list this tool. Audited with source=mcp.",
			[]string{"connectionId", "group", "resetTo"},
			map[string]any{
				"connectionId":     connectionProperty(),
				"group":            map[string]any{"type": "string", "description": "Consumer group id"},
				"topics":           map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "Topics to reset (required except for partitionOffset)"},
				"resetTo":          map[string]any{"type": "string", "enum": []string{"earliest", "latest", "timestamp", "partitionOffset"}, "description": "Reset target (case-insensitive)"},
				"timestampMs":      map[string]any{"type": "integer", "description": "resetTo=timestamp: target unix ms (>0; numeric strings tolerated)"},
				"partitionOffsets": map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "object"}, "description": "resetTo=partitionOffset: topic -> partition -> offset"},
				"confirmToken":     map[string]any{"type": "string", "description": "One-time token returned by the preview call"},
			},
		),
		toolEntry(
			"kafka_topics_records_clear",
			"Clear all records of a topic (truncate to the current high watermark; two-phase preview/confirm like kafka_topics_delete). Read-only or allow_delete=false connections never list this tool. Audited with source=mcp.",
			[]string{"connectionId", "topic"},
			map[string]any{
				"connectionId": connectionProperty(),
				"topic":        map[string]any{"type": "string", "description": "Topic to clear"},
				"confirmToken": map[string]any{"type": "string", "description": "One-time token returned by the preview call"},
			},
		),
	}
}

// Tools 返回工具清单：给出 connectionId 且该连接策略不满足时剔除对应写工具
// 并附原因（设计 §4：不注册而非注册了再报错）。
func (s *Server) Tools(connectionID string) map[string]any {
	tools := allToolDefinitions()
	result := map[string]any{"tools": tools}
	if connectionID == "" {
		return result
	}
	readOnly, allowDelete := s.svc.PolicyOf(connectionID)
	if !readOnly && allowDelete {
		return result
	}
	omitted := make([]map[string]any, 0, len(writeToolNames))
	omit := func(name, reason string) {
		omitted = append(omitted, map[string]any{"name": name, "reason": reason})
	}
	for _, name := range writeToolNames {
		switch name {
		case "kafka_topics_delete", "kafka_topics_records_clear":
			if readOnly || !allowDelete {
				omit(name, "connection is read-only or disallows delete (allow_delete=false); write tools are not registered (design §4)")
			}
		default:
			if readOnly {
				omit(name, "connection is configured read-only; write tools are not registered (design §4)")
			}
		}
	}
	filtered := make([]map[string]any, 0, len(tools))
	for _, tool := range tools {
		name := tool["name"].(string)
		excluded := false
		for _, omittedEntry := range omitted {
			if omittedEntry["name"] == name {
				excluded = true
				break
			}
		}
		if !excluded {
			filtered = append(filtered, tool)
		}
	}
	return map[string]any{"tools": filtered, "omittedWriteTools": omitted}
}
