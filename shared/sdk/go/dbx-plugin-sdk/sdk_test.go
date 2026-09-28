package dbxpluginsdk

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"testing"
)

func TestServerInitializesAndDispatches(t *testing.T) {
	input := bytes.NewBufferString(
		"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"plugin/initialize\",\"params\":{\"host\":{\"protocolVersions\":[1]}}}\n" +
			"{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"sample/ping\",\"params\":{\"name\":\"DBX\"}}\n",
	)
	var output bytes.Buffer
	server := NewServer(
		Metadata{ID: "sample.plugin", Version: "1.0.0", Capabilities: []string{"commands"}},
		HandlerFunc(func(_ RequestContext, method string, _ json.RawMessage, _ *Emitter) (any, *PluginError) {
			if method != "sample/ping" {
				return nil, MethodNotFound(method)
			}
			return map[string]any{"ok": true}, nil
		}),
	).WithIO(input, &output, &bytes.Buffer{})
	if err := server.Serve(); err != nil {
		t.Fatal(err)
	}
	var responses []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(output.Bytes()), []byte{'\n'}) {
		var response map[string]any
		if err := json.Unmarshal(line, &response); err != nil {
			t.Fatal(err)
		}
		responses = append(responses, response)
	}
	if len(responses) != 2 {
		t.Fatalf("expected 2 responses, got %d", len(responses))
	}
	initialize := responses[0]["result"].(map[string]any)
	if initialize["protocolVersion"] != float64(ProtocolVersion) {
		t.Fatalf("unexpected initialize response: %#v", initialize)
	}
	pong := responses[1]["result"].(map[string]any)
	if pong["ok"] != true {
		t.Fatalf("unexpected handler response: %#v", pong)
	}
}

func TestEmitterWritesEvents(t *testing.T) {
	var output bytes.Buffer
	emitter := &Emitter{writer: &output, mutex: &sync.Mutex{}}
	if pluginError := emitter.Event("sample/progress", map[string]any{"value": 1}); pluginError != nil {
		t.Fatal(pluginError.Message)
	}
	var event map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &event); err != nil {
		t.Fatal(err)
	}
	if event["method"] != "sample/progress" {
		t.Fatalf("unexpected event: %#v", event)
	}
}

// S-PANIC-2（评审 H-2）：handler panic 不得带崩插件进程——带 id 的请求
// 得到 -32603 错误响应，Serve 正常返回。
func TestServeHandlerPanicReturnsInternalError(t *testing.T) {
	input := bytes.NewBufferString("{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"sample/boom\",\"params\":{}}\n")
	var output bytes.Buffer
	server := NewServer(
		Metadata{ID: "sample.plugin", Version: "1.0.0"},
		HandlerFunc(func(_ RequestContext, _ string, _ json.RawMessage, _ *Emitter) (any, *PluginError) {
			panic("handler exploded")
		}),
	).WithIO(input, &output, &bytes.Buffer{})
	if err := server.Serve(); err != nil {
		t.Fatalf("Serve error = %v", err)
	}
	var response map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &response); err != nil {
		t.Fatalf("decode response: %v (raw=%q)", err, output.String())
	}
	errObj, ok := response["error"].(map[string]any)
	if !ok {
		t.Fatalf("expected error response, got %#v", response)
	}
	if errObj["code"] != float64(-32603) {
		t.Fatalf("error code = %v, want -32603", errObj["code"])
	}
	if response["id"] != float64(7) {
		t.Fatalf("response id = %v, want 7", response["id"])
	}
}

// S-SDK-ROBUST（评审 M-5）：带可关联 id 的畸形请求必须回结构化错误——此前
// 静默丢弃（stderr 一行），同步等响应的宿主永久挂起。解析失败 -32700、
// 信封非法 -32600，且连接继续可用。
func TestServeRepliesStructuredErrorsForMalformedLines(t *testing.T) {
	input := bytes.NewBufferString(
		"{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\n" +
			"{\"id\":4,\"method\":\"sample/ping\"}\n" +
			"{\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"sample/ping\",\"params\":{}}\n")
	var output bytes.Buffer
	server := NewServer(
		Metadata{ID: "sample.plugin", Version: "1.0.0"},
		HandlerFunc(func(_ RequestContext, _ string, _ json.RawMessage, _ *Emitter) (any, *PluginError) {
			return map[string]any{"ok": true}, nil
		}),
	).WithIO(input, &output, &bytes.Buffer{})
	if err := server.Serve(); err != nil {
		t.Fatalf("Serve error = %v", err)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("responses = %d, want 3 (raw=%q)", len(lines), output.String())
	}
	var first, second, third map[string]any
	for i, line := range [][]byte{[]byte(lines[0]), []byte(lines[1]), []byte(lines[2])} {
		var response map[string]any
		if err := json.Unmarshal(line, &response); err != nil {
			t.Fatalf("decode response %d: %v", i, err)
		}
		switch i {
		case 0:
			first = response
		case 1:
			second = response
		default:
			third = response
		}
	}
	// JSON-RPC 2.0 规约：解析错误无法可靠检测 id，响应 id 必须为 null
	// （与 mcp stdio 模式同款）——关键是必须"有响应"，宿主不再挂起。
	errObj, _ := first["error"].(map[string]any)
	if first["id"] != nil || errObj == nil || errObj["code"] != float64(-32700) {
		t.Fatalf("parse-error response = %#v, want -32700 with null id", first)
	}
	errObj, _ = second["error"].(map[string]any)
	if second["id"] != float64(4) || errObj == nil || errObj["code"] != float64(-32600) {
		t.Fatalf("invalid-request response = %#v, want -32600 echoing id 4", second)
	}
	if _, hasErr := third["error"]; hasErr || third["id"] != float64(5) {
		t.Fatalf("healthy ping broken: %#v", third)
	}
}

// S-SDK-OVERSIZE（评审 M-5）：单行超过 maxJSONBytes 时拒绝该行（-32700）
// 并继续服务——此前 bufio.Scanner 报 ErrTooLong 终止 Serve，main 层
// log.Fatal 带崩进程。
func TestServeSurvivesOversizedLine(t *testing.T) {
	input := bytes.NewBuffer(nil)
	input.WriteString("{\"jsonrpc\":\"2.0\",\"id\":8,\"pad\":\"")
	input.Write(bytes.Repeat([]byte("x"), maxJSONBytes+1024))
	input.WriteString("\"}\n")
	input.WriteString("{\"jsonrpc\":\"2.0\",\"id\":9,\"method\":\"sample/ping\",\"params\":{}}\n")
	var output bytes.Buffer
	server := NewServer(
		Metadata{ID: "sample.plugin", Version: "1.0.0"},
		HandlerFunc(func(_ RequestContext, _ string, _ json.RawMessage, _ *Emitter) (any, *PluginError) {
			return map[string]any{"ok": true}, nil
		}),
	).WithIO(input, &output, &bytes.Buffer{})
	if err := server.Serve(); err != nil {
		t.Fatalf("Serve must survive oversized line, error = %v", err)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("responses = %d, want 2 (rejected oversized + ping)", len(lines))
	}
	var rejected map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rejected); err != nil {
		t.Fatalf("decode rejection: %v", err)
	}
	if errObj, ok := rejected["error"].(map[string]any); !ok || errObj["code"] != float64(-32700) {
		t.Fatalf("oversized line response = %#v, want -32700", rejected)
	}
	var ping map[string]any
	if err := json.Unmarshal([]byte(lines[1]), &ping); err != nil {
		t.Fatalf("decode ping: %v", err)
	}
	if ping["id"] != float64(9) {
		t.Fatalf("ping id = %v, want 9", ping["id"])
	}
}
