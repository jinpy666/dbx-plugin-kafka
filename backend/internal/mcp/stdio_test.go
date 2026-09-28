package mcp

// stdio_test.go：standalone `--mcp` stdio 模式纯逻辑单测（S-STDIO-*，
// ldap Go 版同构）：JSON-RPC 协议循环、UNAVAILABLE 分支、内联凭据池化键、
// 连接解析门。全部离线（不拨号：连接类断言停在解析/注册层，注册是惰性
// 建连；bootstrap 指向假地址不触发任何调用）。

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func newTestStdioServer() *StdioServer {
	return NewStdioServer("0.0.0-test", nil, nil)
}

// decodeResponse 把响应帧解为通用 map。
func decodeResponse(t *testing.T, response map[string]any) map[string]any {
	t.Helper()
	payload, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}

func errorOf(t *testing.T, decoded map[string]any) map[string]any {
	t.Helper()
	errorFrame, _ := decoded["error"].(map[string]any)
	return errorFrame
}

// S-STDIO-1 initialize：协议版本 + serverInfo（name/版本注入）。
func TestStdioInitialize(t *testing.T) {
	server := newTestStdioServer()
	decoded := decodeResponse(t, server.handleLine([]byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`)))
	if errorOf(t, decoded) != nil {
		t.Fatalf("initialize must not fail: %v", decoded)
	}
	result := decoded["result"].(map[string]any)
	if result["protocolVersion"] != MCPProtocolVersion {
		t.Fatalf("protocolVersion: %v", result["protocolVersion"])
	}
	info := result["serverInfo"].(map[string]any)
	if info["name"] != "io.dbx.kafka" || info["version"] != "0.0.0-test" {
		t.Fatalf("serverInfo: %v", info)
	}
	if decoded["id"] != float64(1) {
		t.Fatalf("id roundtrip: %v", decoded["id"])
	}
}

// S-STDIO-2 通知不回包；ping 空结果；未知方法 -32601；坏 JSON -32700。
func TestStdioProtocolSemantics(t *testing.T) {
	server := newTestStdioServer()
	if response := server.handleLine([]byte(`{"jsonrpc":"2.0","method":"notifications/initialized"}`)); response != nil {
		t.Fatalf("notification must stay silent, got %v", response)
	}
	decoded := decodeResponse(t, server.handleLine([]byte(`{"jsonrpc":"2.0","id":2,"method":"ping"}`)))
	if _, ok := decoded["result"].(map[string]any); !ok {
		t.Fatalf("ping result: %v", decoded)
	}
	decoded = decodeResponse(t, server.handleLine([]byte(`{"jsonrpc":"2.0","id":3,"method":"mcp/nonexistent","params":{}}`)))
	if got := errorOf(t, decoded); got == nil || got["code"] != float64(-32601) {
		t.Fatalf("unknown method must be -32601: %v", decoded)
	}
	decoded = decodeResponse(t, server.handleLine([]byte(`not json`)))
	if got := errorOf(t, decoded); got == nil || got["code"] != float64(-32700) {
		t.Fatalf("parse error must be -32700: %v", decoded)
	}
}

// S-STDIO-3 tools/list：复用注册表全量 11 工具；连接类工具补内联参数声明
// 并把 required 的 connectionId 放宽为 anyOf；UI 工具 schema 不动。
func TestStdioToolsList(t *testing.T) {
	server := newTestStdioServer()
	decoded := decodeResponse(t, server.handleLine([]byte(`{"jsonrpc":"2.0","id":4,"method":"tools/list"}`)))
	tools := decoded["result"].(map[string]any)["tools"].([]any)
	if len(tools) != 11 {
		t.Fatalf("tool count: %d", len(tools))
	}
	byName := map[string]map[string]any{}
	for _, raw := range tools {
		tool := raw.(map[string]any)
		byName[tool["name"].(string)] = tool
	}
	digest := byName["kafka_messages_digest"]["inputSchema"].(map[string]any)
	properties := digest["properties"].(map[string]any)
	for _, key := range []string{"brokers", "securityProtocol", "saslMechanism", "saslUsername", "saslPassword", "schemaRegistry", "schemaRegistryUrl", "schemaRegistryPassword", "readOnly", "allowDelete"} {
		if _, ok := properties[key]; !ok {
			t.Fatalf("inline property %q missing from digest schema", key)
		}
	}
	for _, entry := range digest["required"].([]any) {
		if entry == "connectionId" {
			t.Fatalf("connectionId must be relaxed out of required: %v", digest["required"])
		}
	}
	if digest["anyOf"] == nil {
		t.Fatal("anyOf selector requirement missing")
	}
	ui := byName["kafka_ui_topics"]["inputSchema"].(map[string]any)
	if _, ok := ui["properties"].(map[string]any)["brokers"]; ok {
		t.Fatal("UI tool schema must stay untouched")
	}
	if _, ok := ui["anyOf"]; ok {
		t.Fatal("UI tool must not gain anyOf")
	}
}

// S-STDIO-4 UNAVAILABLE 分支：UI 类工具（含元发现 kafka_ui_topics）
// isError content 明确不假死。
func TestStdioUIToolsUnavailable(t *testing.T) {
	server := newTestStdioServer()
	for _, name := range []string{"kafka_ui_focus", "kafka_ui_search", "kafka_ui_select", "kafka_ui_state", "kafka_ui_topics"} {
		decoded := decodeResponse(t, server.handleLine([]byte(
			`{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"`+name+`","arguments":{}}}`)))
		result := decoded["result"].(map[string]any)
		if result["isError"] != true {
			t.Fatalf("%s must return isError=true: %v", name, result)
		}
		content := result["content"].([]any)[0].(map[string]any)
		text := content["text"].(string)
		if !strings.Contains(text, "UNAVAILABLE") || !strings.Contains(text, "此工具需要 DBX 工作台") {
			t.Fatalf("%s text: %s", name, text)
		}
	}
}

// S-STDIO-5 连接解析门：缺参引导 / 未知 connectionId 走桥兜底（无桥时
// fail-closed 引导错误）/ 已池化 id 直通（不触网）。
func TestStdioConnectionGate(t *testing.T) {
	server := newTestStdioServer()
	server.bridgeEnsureWait = 50 * time.Millisecond
	missing := server.callTool("kafka_messages_digest", map[string]any{"topic": "t"})
	if missing["isError"] != true || !strings.Contains(missing["content"].([]map[string]any)[0]["text"].(string), "brokers (required)") {
		t.Fatalf("missing-params guidance: %v", missing)
	}
	// 未知 connectionId：桥未发布（空 app-data）→ fail-closed 合并错误
	// （桥失败原因 + 内联凭据出路），不假死。
	t.Setenv("DBX_APP_DATA_DIR", t.TempDir())
	t.Setenv("DBX_APP_LAUNCH_CMD", ":")
	unknown := server.callTool("kafka_messages_digest", map[string]any{"connectionId": "mcp-nope", "topic": "t"})
	text := unknown["content"].([]map[string]any)[0]["text"].(string)
	if unknown["isError"] != true || !strings.Contains(text, "DBX app bridge") || !strings.Contains(text, "inline connection parameters") {
		t.Fatalf("unknown connectionId guidance: %v", unknown)
	}
	// 已池化 id：解析直通（不注入不覆盖，更不走桥）。
	id, err := server.pooledConnectionID(inlineConn{Brokers: []string{"127.0.0.1:9092"}})
	if err != nil {
		t.Fatal(err)
	}
	args := map[string]any{"connectionId": id}
	forwarded, handled, err := server.resolveConnectionOrForward("kafka_messages_digest", args)
	if err != nil || handled || forwarded != nil {
		t.Fatalf("pooled id must stay local: %v %v %v", forwarded, handled, err)
	}
	if args["connectionId"] != id {
		t.Fatalf("pooled id must stay untouched: %v", args)
	}
}

// S-STDIO-6 内联凭据池化键：同参数同键（键序无关）、异凭据异键、表单默认
// 归一（缺省 readOnly=true/allowDelete=false 与显式同值同键）、presence 判定。
func TestStdioInlinePoolKey(t *testing.T) {
	base := inlineConn{Brokers: []string{"b:9092"}, SASLPassword: "p"}
	if base.poolKey() != (inlineConn{Brokers: []string{"b:9092"}, SASLPassword: "p"}).poolKey() {
		t.Fatal("same params must share a pool key")
	}
	if base.poolKey() == (inlineConn{Brokers: []string{"b:9092"}, SASLPassword: "other"}).poolKey() {
		t.Fatal("different credentials must not collide")
	}
	a, okA, errA := parseInlineConn(map[string]any{"brokers": []any{"b:9092"}, "securityProtocol": "PLAINTEXT", "readOnly": true})
	b, okB, errB := parseInlineConn(map[string]any{"securityProtocol": "PLAINTEXT", "brokers": []any{"b:9092"}})
	if errA != nil || errB != nil {
		t.Fatalf("parse must not fail: %v %v", errA, errB)
	}
	if !okA || !okB || a.poolKey() != b.poolKey() {
		t.Fatalf("argument order / form defaults must not change the pool key: %v %v", a, b)
	}
	if _, present, err := parseInlineConn(map[string]any{"saslPassword": "x"}); present || err != nil {
		t.Fatal("absent brokers must report not-present without error")
	}
	// 字符串布尔宽容解析；非法值显式报错（静默回落默认会翻转读写语义）。
	// 第二轮对齐 ssh 基线：yes/no/on/off 变体也接受。
	coerced, okStr, errStr := parseInlineConn(map[string]any{"brokers": "b:9092", "readOnly": "false", "allowDelete": "TRUE"})
	if errStr != nil || !okStr || coerced.ReadOnly || !coerced.AllowDelete {
		t.Fatalf("string booleans must coerce: %+v %v %v", coerced, okStr, errStr)
	}
	coerced2, _, errYes := parseInlineConn(map[string]any{"brokers": "b:9092", "readOnly": "yes"})
	if errYes != nil || !coerced2.ReadOnly {
		t.Fatalf("yes variant must coerce to true: %+v %v", coerced2, errYes)
	}
	if _, _, err := parseInlineConn(map[string]any{"brokers": "b:9092", "readOnly": "maybe"}); err == nil || !strings.Contains(err.Error(), "true or false") {
		t.Fatalf("invalid boolean must be refused: %v", err)
	}
}

// S-STDIO-7 brokers 解析：数组、逗号/换行分隔字符串、混合；toLifecycle
// 折算标准 lifecycle 形状（bootstrap_servers + read_only/allow_delete +
// secrets）。
func TestStdioBrokersAndLifecycle(t *testing.T) {
	if got := parseBrokers([]any{"a:9092", " b:9092 "}); len(got) != 2 || got[0] != "a:9092" || got[1] != "b:9092" {
		t.Fatalf("array brokers: %v", got)
	}
	if got := parseBrokers("a:9092, b:9092\nc:9092"); len(got) != 3 || got[2] != "c:9092" {
		t.Fatalf("string brokers: %v", got)
	}
	if got := parseBrokers(nil); got != nil {
		t.Fatalf("absent brokers: %v", got)
	}
	inline, present, err := parseInlineConn(map[string]any{
		"brokers": "a:9092,b:9092", "securityProtocol": "SASL_PLAINTEXT",
		"saslMechanism": "SCRAM-SHA-256", "saslUsername": "u", "saslPassword": "p",
		"schemaRegistry": "confluent", "schemaRegistryUrl": "http://sr:8081",
		"schemaRegistryPassword": "sp", "readOnly": false, "allowDelete": true,
	})
	if !present {
		t.Fatal("inline params must be present")
	}
	if err != nil {
		t.Fatalf("parse must not fail: %v", err)
	}
	// 显式 readOnly=false 允许写；缺省（表单默认）为只读。
	if inline.ReadOnly {
		t.Fatalf("explicit readOnly=false must resolve: %+v", inline)
	}
	params := inline.toLifecycle("mcp-x")
	if params.ConnectionID() != "mcp-x" {
		t.Fatalf("connection id: %+v", params.Connection)
	}
	if got := params.ConfigStringSlice("bootstrap_servers"); len(got) != 2 || got[0] != "a:9092" {
		t.Fatalf("bootstrap_servers: %v", got)
	}
	if params.ConfigString("security_protocol") != "SASL_PLAINTEXT" ||
		params.ConfigString("sasl_mechanism") != "SCRAM-SHA-256" ||
		params.ConfigString("sasl_username") != "u" ||
		params.ConfigString("schema_registry") != "confluent" ||
		params.ConfigString("sr_url") != "http://sr:8081" ||
		params.ConfigBool("read_only") || !params.ConfigBool("allow_delete") {
		t.Fatalf("external_config: %+v", params.Connection.ExternalConfig)
	}
	if params.SecretString("sasl_password") != "p" || params.SecretString("sr_password") != "sp" {
		t.Fatalf("secrets: %+v", params.Connection.Secrets)
	}
}

// S-STDIO-8 池淘汰：上限 8，最旧淘汰且底层连接断开（svc.Disconnect 幂等）。
func TestStdioPoolCapEviction(t *testing.T) {
	server := newTestStdioServer()
	ids := make([]string, 0, inlinePoolCap+1)
	for index := 0; index <= inlinePoolCap; index++ {
		id, err := server.pooledConnectionID(inlineConn{Brokers: []string{fmt.Sprintf("h-%d:9092", index)}})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if len(server.hash) != inlinePoolCap || len(server.ids) != inlinePoolCap {
		t.Fatalf("pool size: %d/%d", len(server.hash), len(server.ids))
	}
	if server.poolHas(ids[0]) {
		t.Fatal("oldest entry must be evicted")
	}
	if !server.poolHas(ids[inlinePoolCap]) {
		t.Fatal("newest entry must stay")
	}
	// 命中复用：同参数再次池化返回同一 id 且不扩张。
	dup, err := server.pooledConnectionID(inlineConn{Brokers: []string{fmt.Sprintf("h-%d:9092", inlinePoolCap)}})
	if err != nil || dup != ids[inlinePoolCap] || len(server.hash) != inlinePoolCap {
		t.Fatalf("pool hit: %v %v %d", dup, err, len(server.hash))
	}
}

// S-STDIO-9 会话类工具直通 Server.Call（cursor 语义复用，不触连接门）。
func TestStdioCursorNextReusesCall(t *testing.T) {
	server := newTestStdioServer()
	result := server.callTool("kafka_cursor_next", map[string]any{"cursorId": "cur-nope"})
	if result["isError"] != true {
		t.Fatalf("cursor gate: %v", result)
	}
	if !strings.Contains(result["content"].([]map[string]any)[0]["text"].(string), "unknown cursorId") {
		t.Fatalf("cursor text: %v", result)
	}
}

// S-STDIO-10 Serve 循环：喂行读响应；通知不回包；响应合法 NDJSON 且按 id
// 关联（乱序容忍）。
func TestStdioServeLoop(t *testing.T) {
	server := newTestStdioServer()
	input := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","id":2,"method":"ping"}`,
		`not json`,
		`{"jsonrpc":"2.0","id":3,"method":"mcp/nonexistent","params":{}}`,
		`{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"kafka_ui_focus","arguments":{"panel":"topics"}}}`,
		"",
	}, "\n") + "\n"
	var out bytes.Buffer
	if err := server.Serve(strings.NewReader(input), &out); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 5 {
		t.Fatalf("expect 5 responses (notification silent), got %d:\n%s", len(lines), out.String())
	}
	byID := map[string]map[string]any{}
	for _, line := range lines {
		var decoded map[string]any
		if err := json.Unmarshal([]byte(line), &decoded); err != nil {
			t.Fatalf("response line not JSON: %q", line)
		}
		id := "\"null\""
		if raw, err := json.Marshal(decoded["id"]); err == nil {
			id = string(raw)
		}
		byID[id] = decoded
	}
	if got := byID[`1`]["result"].(map[string]any)["protocolVersion"]; got != MCPProtocolVersion {
		t.Fatalf("initialize via loop: %v", got)
	}
	if _, ok := byID[`2`]["result"]; !ok {
		t.Fatalf("ping via loop: %v", byID[`2`])
	}
	if got := errorOf(t, byID[`null`]); got == nil || got["code"] != float64(-32700) {
		t.Fatalf("parse error via loop: %v", byID[`null`])
	}
	if got := errorOf(t, byID[`3`]); got == nil || got["code"] != float64(-32601) {
		t.Fatalf("unknown method via loop: %v", byID[`3`])
	}
	if got := byID[`4`]["result"].(map[string]any)["isError"]; got != true {
		t.Fatalf("ui unavailable via loop: %v", byID[`4`])
	}
}

// S-STDIO-BACKPRESSURE（评审 M-6）：槽满后的积压行仍须全部处理、EOF 后
// Serve 正常返回——读入/分派分离重构的回归钉。
func TestStdioServeProcessesBacklogUnderBackpressure(t *testing.T) {
	server := newTestStdioServer()
	var input strings.Builder
	const total = maxConcurrentRequests + 8
	for id := 1; id <= total; id++ {
		fmt.Fprintf(&input, `{"jsonrpc":"2.0","id":%d,"method":"ping"}`+"\n", id)
	}
	var out bytes.Buffer
	if err := server.Serve(strings.NewReader(input.String()), &out); err != nil {
		t.Fatalf("Serve error = %v", err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != total {
		t.Fatalf("responses = %d, want %d", len(lines), total)
	}
}
