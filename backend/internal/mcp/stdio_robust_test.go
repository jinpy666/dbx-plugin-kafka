package mcp

// stdio_robust_test.go：stdio 传输层健壮性单测（可靠性纵深轮，S-STDIO-R*，
// ldap Go 版同表）：非法 JSON / 非法 UTF-8、通知静默、请求形状分档
//（-32700/-32600/-32602）、超长行、pipelining、空行/CRLF 容忍。
// 每条破坏性断言后跟一次 ping 验证「连接继续可用」（进程逻辑存活）。

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// mustErrCode 断言响应是 error 帧且码匹配，返回 error 帧。
func mustErrCode(t *testing.T, decoded map[string]any, code float64) map[string]any {
	t.Helper()
	got := errorOf(t, decoded)
	if got == nil || got["code"] != code {
		t.Fatalf("expect error code %v, got: %v", code, decoded)
	}
	return got
}

// S-STDIO-R1 非法 JSON / 非法 UTF-8 字节流 → -32700（id null），随后 ping
// 仍成功（进程存活）。注意 go json 对「JSON 字符串内的非法 UTF-8」会替换
// U+FFFD 后成功——所以只有顶层结构坏才算 parse error；纯非法字节流必然
// 顶层结构坏。
func TestStdioRobustMalformedBytes(t *testing.T) {
	server := newTestStdioServer()
	for name, line := range map[string][]byte{
		"garbage":           []byte(`not json`),
		"truncated":         []byte(`{"jsonrpc":"2.0","id":1,"method":`),
		"raw-bytes":         {0xff, 0xfe, 0x7b, 0x7d}, // ÿþ{}
		"nul-bytes":         {0x00, 0x01, 0x02},
		"json-then-garbage": []byte(`{"jsonrpc":"2.0","id":1} trailing garbage`),
	} {
		decoded := decodeResponse(t, server.handleLine(line))
		mustErrCode(t, decoded, -32700)
		if got, _ := json.Marshal(decoded["id"]); string(got) != "null" {
			t.Fatalf("%s: parse error must carry null id: %s", name, got)
		}
	}
	// 健康检查：破坏输入后 ping 照常。
	decoded := decodeResponse(t, server.handleLine([]byte(`{"jsonrpc":"2.0","id":1,"method":"ping"}`)))
	if _, ok := decoded["result"]; !ok {
		t.Fatalf("ping after malformed input must work: %v", decoded)
	}
}

// S-STDIO-R2 通知（无 id）不响应：notifications/* 前缀（含未知通知名）保持
// 静默，且连接继续可用。
func TestStdioRobustNotificationSilence(t *testing.T) {
	server := newTestStdioServer()
	for name, line := range map[string]string{
		"initialized":    `{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		"unknown-notify": `{"jsonrpc":"2.0","method":"notifications/custom/x","params":{"a":1}}`,
		"notify-with-id": `{"jsonrpc":"2.0","id":9,"method":"notifications/progress"}`,
	} {
		if response := server.handleLine([]byte(line)); response != nil {
			t.Fatalf("%s must stay silent, got %v", name, response)
		}
	}
	// 通知之后连接继续可用。
	decoded := decodeResponse(t, server.handleLine([]byte(`{"jsonrpc":"2.0","id":1,"method":"ping"}`)))
	if _, ok := decoded["result"]; !ok {
		t.Fatalf("ping after notifications must work: %v", decoded)
	}
}

// S-STDIO-R3 请求形状分档（对照 MCP_ACCEPTANCE §2）：缺 id / id 为 object/
// array/布尔 / method 缺失或非字符串 / jsonrpc 版本非法 → 全部 -32600 结构化
// 报错（不崩、不 -32700 误档）；显式 "id":null 是合法请求；每条之后 ping
// 验证存活。
func TestStdioRobustRequestShapes(t *testing.T) {
	server := newTestStdioServer()
	for name, line := range map[string]string{
		"missing-id":     `{"jsonrpc":"2.0","method":"ping"}`,
		"id-object":      `{"jsonrpc":"2.0","id":{"a":1},"method":"ping"}`,
		"id-array":       `{"jsonrpc":"2.0","id":[1,2],"method":"ping"}`,
		"id-true":        `{"jsonrpc":"2.0","id":true,"method":"ping"}`,
		"missing-method": `{"jsonrpc":"2.0","id":1}`,
		"method-empty":   `{"jsonrpc":"2.0","id":1,"method":""}`,
		"method-number":  `{"jsonrpc":"2.0","id":1,"method":42}`,
		"method-object":  `{"jsonrpc":"2.0","id":1,"method":{"x":1}}`,
		"jsonrpc-wrong":  `{"jsonrpc":"1.0","id":1,"method":"ping"}`,
		"jsonrpc-number": `{"jsonrpc":2.0,"id":1,"method":"ping"}`,
		"empty-body":     `{}`,
	} {
		decoded := decodeResponse(t, server.handleLine([]byte(line)))
		mustErrCode(t, decoded, -32600)
		// 非法/缺失 id 的回包 id 必须是 null（无法关联）。
		if strings.Contains(name, "id-") {
			if got, _ := json.Marshal(decoded["id"]); string(got) != "null" {
				t.Fatalf("%s: invalid id must answer with null id: %s", name, got)
			}
		}
	}
	// 显式 "id": null：合法的 null-id 请求（result 正常返回，id 回 null）。
	decoded := decodeResponse(t, server.handleLine([]byte(`{"jsonrpc":"2.0","id":null,"method":"ping"}`)))
	if _, ok := decoded["result"]; !ok {
		t.Fatalf("explicit null-id request must be answered: %v", decoded)
	}
	// 健康检查：ping 存活 + id 回显。
	decoded = decodeResponse(t, server.handleLine([]byte(`{"jsonrpc":"2.0","id":"str-id","method":"ping"}`)))
	if decoded["id"] != "str-id" {
		t.Fatalf("string id must roundtrip: %v", decoded)
	}
}

// S-STDIO-R4 超长行：8 MiB arguments 的 tools/call → 工具层结构化报错
// （连接门引导错误，不 panic 不挂死）；超 maxRequestLineBytes 的行 → -32700
// 拒绝；之后 ping 存活。
func TestStdioRobustOversizedLines(t *testing.T) {
	server := newTestStdioServer()
	huge := strings.Repeat("x", 8<<20)
	line := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"kafka_messages_digest","arguments":{"topic":"t%s"}}}`, huge)
	decoded := decodeResponse(t, server.handleLine([]byte(line)))
	result, ok := decoded["result"].(map[string]any)
	if !ok || result["isError"] != true {
		t.Fatalf("huge arguments must degrade to a tool-level error: %v", decoded)
	}
	// 超上限行：经 Serve 循环整体拒绝（-32700），进程不崩。
	oversized := strings.Repeat("a", maxRequestLineBytes+1)
	input := "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"ping\"}\n" + oversized + "\n"
	var out bytes.Buffer
	if err := server.Serve(strings.NewReader(input), &out); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expect 2 responses (oversized line refused but answered), got %d:\n%s", len(lines), out.String())
	}
	byID := map[string]map[string]any{}
	for _, responseLine := range lines {
		var decodedLine map[string]any
		if err := json.Unmarshal([]byte(responseLine), &decodedLine); err != nil {
			t.Fatalf("response line not JSON: %q", responseLine)
		}
		id, _ := json.Marshal(decodedLine["id"])
		byID[string(id)] = decodedLine
	}
	mustErrCode(t, byID["null"], -32700)
	if got := byID["2"]["result"]; got == nil {
		t.Fatalf("ping before the oversized line must succeed: %v", byID["2"])
	}
}

// S-STDIO-R5 pipelining：不等待响应连发 5 个不同 id 请求（含 initialize/
// ping/未知方法/UI 工具/再次 ping）→ 5 条响应、与 id 一一对应、每条合法
// NDJSON（响应可能乱序，JSON-RPC 以 id 关联）。
func TestStdioRobustPipelining(t *testing.T) {
	server := newTestStdioServer()
	requests := []string{
		`{"jsonrpc":"2.0","id":101,"method":"initialize","params":{}}`,
		`{"jsonrpc":"2.0","id":102,"method":"mcp/nonexistent","params":{}}`,
		`{"jsonrpc":"2.0","id":103,"method":"tools/call","params":{"name":"kafka_ui_focus","arguments":{"panel":"topics"}}}`,
		`{"jsonrpc":"2.0","id":104,"method":"ping"}`,
		`{"jsonrpc":"2.0","id":"105","method":"ping"}`,
	}
	input := strings.Join(requests, "\n") + "\n"
	var out bytes.Buffer
	if err := server.Serve(strings.NewReader(input), &out); err != nil {
		t.Fatal(err)
	}
	byID := map[string]map[string]any{}
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		var decoded map[string]any
		if err := json.Unmarshal([]byte(line), &decoded); err != nil {
			t.Fatalf("response line not JSON: %q", line)
		}
		id, _ := json.Marshal(decoded["id"])
		byID[string(id)] = decoded
	}
	if len(byID) != len(requests) {
		t.Fatalf("expect one response per pipelined request: got %d ids %v", len(byID), byID)
	}
	for _, id := range []string{"101", "102", "103", "104", `"105"`} {
		if _, ok := byID[id]; !ok {
			t.Fatalf("missing response for id %s in %v", id, byID)
		}
	}
	if _, ok := byID["101"]["result"]; !ok {
		t.Fatal("pipelined initialize must succeed")
	}
	if got := errorOf(t, byID["102"]); got == nil || got["code"] != float64(-32601) {
		t.Fatalf("pipelined unknown method must be -32601: %v", byID["102"])
	}
	if got := byID["103"]["result"].(map[string]any)["isError"]; got != true {
		t.Fatalf("pipelined UI tool must be UNAVAILABLE: %v", byID["103"])
	}
}

// S-STDIO-R6 空行 / CRLF 容忍：\r\n 行尾、请求间空行、纯空白行都跳过，
// 响应数与合法请求一一对应。
func TestStdioRobustEmptyLinesAndCRLF(t *testing.T) {
	server := newTestStdioServer()
	input := "\r\n" +
		`{"jsonrpc":"2.0","id":1,"method":"ping"}` + "\r\n" +
		"\n" +
		"   \n" +
		`{"jsonrpc":"2.0","id":2,"method":"ping"}` + "\r\n" +
		"\r\n"
	var out bytes.Buffer
	if err := server.Serve(strings.NewReader(input), &out); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("blank/CRLF lines must be tolerated: got %d responses\n%s", len(lines), out.String())
	}
	for _, line := range lines {
		var decoded map[string]any
		if err := json.Unmarshal([]byte(line), &decoded); err != nil {
			t.Fatalf("CRLF response not JSON: %q", line)
		}
		if _, ok := decoded["result"]; !ok {
			t.Fatalf("CRLF ping must succeed: %v", decoded)
		}
	}
}

// S-STDIO-R4b 有界读行边界（评审 M）：超限判定不得误伤恰好等于上限的合法
// 请求；无换行、以 EOF 结尾的超限行仍要 -32700（不挂死）；超限后后续请求
// 存活。
func TestStdioRobustBoundedLineReadBoundaries(t *testing.T) {
	server := newTestStdioServer()
	// 恰好 maxRequestLineBytes 的合法 ping：不超限，正常应答。
	prefix := `{"jsonrpc":"2.0","id":1,"method":"ping","params":{"pad":"`
	line := prefix + strings.Repeat("a", maxRequestLineBytes-len(prefix)-len(`"}}`)) + `"}}` + "\n"
	if len(strings.TrimRight(strings.TrimSpace(line), "\n")) != maxRequestLineBytes {
		t.Fatalf("test setup: line must be exactly maxRequestLineBytes (got %d)", len(strings.TrimSpace(line)))
	}
	input := line + strings.Repeat("b", maxRequestLineBytes+1) + "\n" + `{"jsonrpc":"2.0","id":2,"method":"ping"}` + "\n"
	var out bytes.Buffer
	if err := server.Serve(strings.NewReader(input), &out); err != nil {
		t.Fatal(err)
	}
	byID := map[string]map[string]any{}
	for _, responseLine := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		var decoded map[string]any
		if err := json.Unmarshal([]byte(responseLine), &decoded); err != nil {
			t.Fatalf("response line not JSON: %q", responseLine)
		}
		id, _ := json.Marshal(decoded["id"])
		byID[string(id)] = decoded
	}
	if byID["1"] == nil || byID["1"]["result"] == nil {
		t.Fatalf("line exactly at the limit must be served: %v", byID["1"])
	}
	mustErrCode(t, byID["null"], -32700)
	if byID["2"] == nil || byID["2"]["result"] == nil {
		t.Fatalf("request after oversized line must survive: %v", byID["2"])
	}

	// EOF 结尾的无换行超限行：同样 -32700，不挂死不丢响应。
	var outEOF bytes.Buffer
	if err := server.Serve(strings.NewReader(strings.Repeat("c", maxRequestLineBytes+1)), &outEOF); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(outEOF.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("expect exactly one refusal for unterminated oversized line, got %d", len(lines))
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &decoded); err != nil {
		t.Fatalf("refusal not JSON: %v", err)
	}
	mustErrCode(t, decoded, -32700)
}
