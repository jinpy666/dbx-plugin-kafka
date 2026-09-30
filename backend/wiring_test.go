package main

// wiring_test.go：main.go 接线层的离线面（方法分派矩阵、参数门禁、公共
// helper、presets/审计落盘、Stream Emitter 适配）。域方法只走 decode/门禁
// 分支与"无连接即快速失败"的路径，全程不触网；真实拨号路径由 kafkaconn
// 的 service_test（本地 listener）与容器 smoke 覆盖。

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"

	dbxpluginsdk "github.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk"

	"io.dbx.kafka.plugin/internal/kafkaconn"
	"io.dbx.kafka.plugin/internal/mcp"
	"io.dbx.kafka.plugin/internal/store"
)

// newTestHandler 按 main() 的装配同构组装离线 handler：临时目录 store +
// kafkaconn.Service + mcp Server，注入 Audit 回调、PresetStore 与 Stream
// Emitter，全部状态落在 t.TempDir()。
func newTestHandler(t *testing.T) *pluginHandler {
	t.Helper()
	st, err := store.OpenAt(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatalf("open temp store: %v", err)
	}
	svc := kafkaconn.NewService()
	h := &pluginHandler{svc: svc, st: st}
	svc.Audit = h.auditRecord
	svc.Presets = newPresetStore(st)
	svc.Streams.Emitter = h
	h.mcpSrv = mcp.NewServer(svc, st)
	return h
}

// callHandle 以固定 RequestContext 调 Handle（emitter 可空）。
func callHandle(h *pluginHandler, method, params string, emitter *dbxpluginsdk.Emitter) (any, *dbxpluginsdk.PluginError) {
	return h.Handle(dbxpluginsdk.RequestContext{}, method, json.RawMessage(params), emitter)
}

// captureServeEmitter 用 SDK Server.Serve 真实分发一轮请求，捕获宿主注入
// handler 的 Emitter（writer 即传入的 w），不触碰 SDK 私有字段。
func captureServeEmitter(t *testing.T, w io.Writer) *dbxpluginsdk.Emitter {
	t.Helper()
	var captured *dbxpluginsdk.Emitter
	server := dbxpluginsdk.NewServer(
		dbxpluginsdk.Metadata{ID: "io.dbx.kafka", Version: "test"},
		dbxpluginsdk.HandlerFunc(func(_ dbxpluginsdk.RequestContext, _ string, _ json.RawMessage, em *dbxpluginsdk.Emitter) (any, *dbxpluginsdk.PluginError) {
			captured = em
			return map[string]any{}, nil
		}),
	)
	server.WithIO(strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"probe/emitter"}`+"\n"), w, io.Discard)
	if err := server.Serve(); err != nil {
		t.Fatalf("serve probe: %v", err)
	}
	if captured == nil {
		t.Fatal("handler not invoked; no emitter captured")
	}
	return captured
}

type failingWriter struct{}

func (failingWriter) Write(p []byte) (int, error) { return 0, errors.New("disk full") }

func TestHandleDispatchMatrix(t *testing.T) {
	h := newTestHandler(t)
	domainGate := -32602 // `{}` → 缺 connectionId/sessionId/connection.id 的参数门禁
	cases := []struct {
		method   string
		params   string
		wantCode int // 0 = 离线即可成功
	}{
		{"connection/test", `{}`, domainGate},
		{"connection/connect", `{}`, -32000}, // 空 profile：connection.id is required（plain errf）
		{"connection/disconnect", `{}`, domainGate},

		{"kafka/brokers/list", `{}`, domainGate},
		{"kafka/brokers/config", `{}`, domainGate},
		{"kafka/topics/list", `{}`, domainGate},
		{"kafka/topics/describe", `{}`, domainGate},
		{"kafka/topics/create", `{}`, domainGate},
		{"kafka/topics/delete", `{}`, domainGate},
		{"kafka/topics/partitions/update", `{}`, domainGate},
		{"kafka/topics/config/get", `{}`, domainGate},
		{"kafka/topics/config/alter", `{}`, domainGate},
		{"kafka/topics/offsets/list", `{}`, domainGate},
		{"kafka/topics/records/clear", `{}`, domainGate},

		{"kafka/groups/list", `{}`, domainGate},
		{"kafka/groups/describe", `{}`, domainGate},
		{"kafka/groups/offsets/list", `{}`, domainGate},
		{"kafka/groups/delete", `{}`, domainGate},
		{"kafka/groups/offsets/reset", `{}`, domainGate},

		{"kafka/acls/list", `{}`, domainGate},
		{"kafka/acls/create", `{}`, domainGate},
		{"kafka/acls/delete", `{}`, domainGate},

		{"kafka/messages/produce", `{}`, domainGate},
		{"kafka/messages/consume", `{}`, domainGate},
		{"kafka/messages/consume/cancel", `{}`, domainGate},                                              // 缺 connectionId/consumeId
		{"kafka/messages/consume/cancel", `{"connectionId":"c1","consumeId":"unknown-id"}`, 0},           // 未知 id 幂等 success:false
		{"kafka/messages/export", `{}`, domainGate},

		{"kafka/stream/start", `{}`, domainGate},
		{"kafka/stream/stop", `{}`, domainGate}, // 缺 sessionId（或 all=true）
		{"kafka/stream/pause", `{}`, domainGate},
		{"kafka/stream/resume", `{}`, domainGate},
		{"kafka/stream/status", `{}`, domainGate},
		{"kafka/stream/messages", `{}`, domainGate},

		{"kafka/presets/list", `{}`, 0},
		{"kafka/presets/save", `{}`, -32000}, // SavePreset：preset name is required
		{"kafka/presets/remove", `{}`, domainGate},
		{"kafka/connections/statuses", `{}`, 0},

		{"kafka/ui/state/report", `{}`, 0}, // 无 intentId = 快照型，SetSnapshot 成功
		{"mcp/tools", `{}`, 0},
		{"mcp/settings/get", `{}`, 0},
		{"mcp/settings/set", `{}`, 0}, // 空对象 = 白名单空更新
		{"mcp/call", `{}`, domainGate},

		{"kafka/schema/test", `{}`, domainGate},
		{"kafka/schema/subjects/list", `{}`, domainGate},
		{"kafka/schema/versions/list", `{}`, domainGate},
		{"kafka/schema/get", `{}`, domainGate},
		{"kafka/schema/versions/compare", `{}`, domainGate},
		{"kafka/schema/compatibility/get", `{}`, domainGate},
		{"kafka/schema/compatibility/set", `{}`, domainGate},
		{"kafka/schema/compatibility/check", `{}`, domainGate},
		{"kafka/schema/register", `{}`, domainGate},
		{"kafka/schema/delete", `{}`, domainGate},
		{"kafka/schema/delete/version", `{}`, domainGate},
		{"kafka/schema/delete/version", `{"connectionId":"c1","subject":"s","version":0}`, domainGate}, // version 门禁
		{"kafka/schema/delete/version", `{"connectionId":"c1","subject":"s","version":1}`, -32000},     // 未知连接快速失败

		{"no/such/method", `{}`, -32601},
	}
	for _, tc := range cases {
		_, perr := callHandle(h, tc.method, tc.params, nil)
		switch {
		case tc.wantCode == 0 && perr != nil:
			t.Errorf("%s: expected success, got code=%d msg=%q", tc.method, perr.Code, perr.Message)
		case tc.wantCode != 0 && perr == nil:
			t.Errorf("%s: expected error code=%d, got success", tc.method, tc.wantCode)
		case tc.wantCode != 0 && perr.Code != tc.wantCode:
			t.Errorf("%s: code=%d, want %d (msg=%q)", tc.method, perr.Code, tc.wantCode, perr.Message)
		}
	}
}

func TestConnectionLifecycleOffline(t *testing.T) {
	h := newTestHandler(t)

	// connect 仅登记连接（idle），不拨号。
	res, perr := callHandle(h, "connection/connect",
		`{"connection":{"id":"c1","external_config":{"bootstrap_servers":"127.0.0.1:9092"}}}`, nil)
	if perr != nil {
		t.Fatalf("connect: code=%d msg=%q", perr.Code, perr.Message)
	}
	if got := res.(map[string]any)["success"]; got != true {
		t.Fatalf("connect success = %v, want true", got)
	}

	res, perr = callHandle(h, "kafka/connections/statuses", `{}`, nil)
	if perr != nil {
		t.Fatalf("statuses: %v", perr)
	}
	statuses := res.(map[string]any)["statuses"].([]kafkaconn.ConnectionStatus)
	if len(statuses) != 1 || statuses[0].ConnectionID != "c1" {
		t.Fatalf("statuses = %+v, want exactly [c1]", statuses)
	}

	res, perr = callHandle(h, "connection/disconnect", `{"connection":{"id":"c1"}}`, nil)
	if perr != nil || res.(map[string]any)["success"] != true {
		t.Fatalf("disconnect: res=%v perr=%v", res, perr)
	}

	// 断开后 statuses 清空。
	res, _ = callHandle(h, "kafka/connections/statuses", `{}`, nil)
	if got := len(res.(map[string]any)["statuses"].([]kafkaconn.ConnectionStatus)); got != 0 {
		t.Fatalf("statuses after disconnect = %d entries, want 0", got)
	}
}

func TestPresetsLifecycleOffline(t *testing.T) {
	h := newTestHandler(t)

	res, perr := callHandle(h, "kafka/presets/save", `{"preset":{"name":"hourly","params":{"topic":"t1"}}}`, nil)
	if perr != nil {
		t.Fatalf("save: %v", perr)
	}
	saved := res.(map[string]any)["preset"].(*kafkaconn.ConsumePreset)
	if saved.ID == "" || saved.Name != "hourly" {
		t.Fatalf("saved preset = %+v, want id assigned + name hourly", saved)
	}

	res, _ = callHandle(h, "kafka/presets/list", `{}`, nil)
	presets := res.(map[string]any)["presets"].([]kafkaconn.ConsumePreset)
	if len(presets) != 1 || presets[0].Name != "hourly" {
		t.Fatalf("list = %+v, want 1×hourly", presets)
	}

	// 同 id 再存 = 覆盖。
	if _, perr = callHandle(h, "kafka/presets/save",
		`{"preset":{"id":"`+saved.ID+`","name":"renamed"}}`, nil); perr != nil {
		t.Fatalf("save overwrite: %v", perr)
	}
	res, _ = callHandle(h, "kafka/presets/list", `{}`, nil)
	if presets = res.(map[string]any)["presets"].([]kafkaconn.ConsumePreset); len(presets) != 1 || presets[0].Name != "renamed" {
		t.Fatalf("list after overwrite = %+v, want 1×renamed", presets)
	}

	res, perr = callHandle(h, "kafka/presets/remove", `{"id":"`+saved.ID+`"}`, nil)
	if perr != nil || res.(map[string]any)["success"] != true {
		t.Fatalf("remove: res=%v perr=%v", res, perr)
	}
	if _, perr = callHandle(h, "kafka/presets/remove", `{"id":"`+saved.ID+`"}`, nil); perr == nil || perr.Code != -32000 {
		t.Fatalf("remove missing: perr=%v, want -32000", perr)
	}
}

func TestMCPFaceOffline(t *testing.T) {
	h := newTestHandler(t)

	// 工具清单离线可取（无 connectionId = 全量）。
	res, perr := callHandle(h, "mcp/tools", `{}`, nil)
	if perr != nil {
		t.Fatalf("mcp/tools: %v", perr)
	}
	if _, ok := res.(map[string]any)["tools"]; !ok {
		t.Fatalf("mcp/tools result missing tools: %+v", res)
	}

	// settings 白名单更新 + 回读。
	if _, perr = callHandle(h, "mcp/settings/set", `{"responseLimitBytes":2048}`, nil); perr != nil {
		t.Fatalf("settings/set 2048: %v", perr)
	}
	res, _ = callHandle(h, "mcp/settings/get", `{}`, nil)
	if got := res.(map[string]any)["responseLimitBytes"]; got != 2048 {
		t.Fatalf("responseLimitBytes = %v, want 2048", got)
	}
	if _, perr = callHandle(h, "mcp/settings/set", `{"responseLimitBytes":0}`, nil); perr == nil || perr.Code != -32000 {
		t.Fatalf("settings/set 0: perr=%v, want -32000 (below min)", perr)
	}
	// 数字字符串宽容（评审 L-6）：与 coerceInt64 风格一致，"2048" 接受。
	if _, perr = callHandle(h, "mcp/settings/set", `{"responseLimitBytes":"2048"}`, nil); perr != nil {
		t.Fatalf("settings/set string: %v", perr)
	}
	res, _ = callHandle(h, "mcp/settings/get", `{}`, nil)
	if got := res.(map[string]any)["responseLimitBytes"]; got != 2048 {
		t.Fatalf("responseLimitBytes = %v, want 2048", got)
	}
	// 非数字字符串仍拒绝。
	if _, perr = callHandle(h, "mcp/settings/set", `{"responseLimitBytes":"abc"}`, nil); perr == nil || perr.Code != -32000 {
		t.Fatalf("settings/set bad string: perr=%v, want -32000", perr)
	}

	// mcp/call：缺 tool / 未注册工具 / lifecycle、arguments 坏 JSON。
	if _, perr = callHandle(h, "mcp/call", `{"tool":"  "}`, nil); perr == nil || perr.Code != -32602 {
		t.Fatalf("mcp/call blank tool: perr=%v, want -32602", perr)
	}
	if _, perr = callHandle(h, "mcp/call", `{"tool":"kafka-messages-digest"}`, nil); perr == nil || perr.Code != -32000 ||
		!strings.Contains(perr.Message, "kafka_messages_digest") {
		t.Fatalf("mcp/call variant name: perr=%v, want -32000 with suggestion", perr)
	}
	if _, perr = callHandle(h, "mcp/call", `{"tool":"kafka_ui_state","lifecycle":123}`, nil); perr == nil || perr.Code != -32602 {
		t.Fatalf("mcp/call bad lifecycle: perr=%v, want -32602", perr)
	}
	if _, perr = callHandle(h, "mcp/call", `{"tool":"kafka_ui_state","arguments":123}`, nil); perr == nil || perr.Code != -32602 {
		t.Fatalf("mcp/call bad arguments: perr=%v, want -32602", perr)
	}

	// mcp/call 成功信封：kafka_ui_state 无 intentId 读快照，离线确定成功。
	res, perr = callHandle(h, "mcp/call", `{"tool":"kafka_ui_state"}`, nil)
	if perr != nil {
		t.Fatalf("mcp/call kafka_ui_state: %v", perr)
	}
	envelope := res.(map[string]any)
	if envelope["isError"] != false {
		t.Fatalf("isError = %v, want false", envelope["isError"])
	}
	content := envelope["content"].([]map[string]any)
	if content[0]["type"] != "text" || !strings.Contains(content[0]["text"].(string), "snapshot") {
		t.Fatalf("content = %+v, want text with snapshot", content)
	}

	// ui/state/report：快照成功；intent 回报非法状态/未知 intent 拒绝。
	if _, perr = callHandle(h, "kafka/ui/state/report", `{"summary":{"panel":"messages"}}`, nil); perr != nil {
		t.Fatalf("ui/state/report snapshot: %v", perr)
	}
	if _, perr = callHandle(h, "kafka/ui/state/report", `{"intentId":"i1","status":"bogus"}`, nil); perr == nil || perr.Code != -32000 {
		t.Fatalf("ui/state/report bogus status: perr=%v, want -32000", perr)
	}
	if _, perr = callHandle(h, "kafka/ui/state/report", `{"intentId":"i1","status":"applied"}`, nil); perr == nil || perr.Code != -32000 {
		t.Fatalf("ui/state/report unknown intent: perr=%v, want -32000", perr)
	}
}

func TestStreamGatesOffline(t *testing.T) {
	h := newTestHandler(t)

	// stop all=true 离线成功（无会话即空操作）。
	res, perr := callHandle(h, "kafka/stream/stop", `{"all":true}`, nil)
	if perr != nil || res.(map[string]any)["success"] != true {
		t.Fatalf("stream/stop all: res=%v perr=%v", res, perr)
	}
	// 未知 session 的状态/暂停/恢复 → 业务错误 -32000。
	for _, method := range []string{"kafka/stream/status", "kafka/stream/pause", "kafka/stream/resume"} {
		if _, perr = callHandle(h, method, `{"sessionId":"none"}`, nil); perr == nil || perr.Code != -32000 {
			t.Fatalf("%s unknown session: perr=%v, want -32000", method, perr)
		}
	}
}

func TestAuditRecordWritesStoreAndEvent(t *testing.T) {
	h := newTestHandler(t)
	var buf bytes.Buffer
	if _, perr := callHandle(h, "kafka/connections/statuses", `{}`, captureServeEmitter(t, &buf)); perr != nil {
		t.Fatalf("statuses with emitter: %v", perr)
	}

	h.auditRecord(kafkaconn.AuditRecord{
		ConnectionID: "c1",
		Action:       "topics/delete",
		Target:       "t1",
		Result:       "blocked",
		Source:       "mcp",
	})
	lines, err := h.st.ReadAuditLines()
	if err != nil {
		t.Fatalf("read audit: %v", err)
	}
	if len(lines) != 1 {
		t.Fatalf("audit lines = %d, want 1", len(lines))
	}
	if lines[0].Action != "kafka/topics/delete" || lines[0].Result != "denied" || lines[0].Target != "t1" || lines[0].Source != "mcp" {
		t.Fatalf("audit line = %+v, want kafka/topics/delete × denied × t1 × mcp", lines[0])
	}
	for _, want := range []string{"kafka/audit", "t1"} {
		if !strings.Contains(buf.String(), want) {
			t.Fatalf("audit event buffer missing %q: %s", want, buf.String())
		}
	}

	// store 不可用不阻断：h.st = nil 只丢落盘通道，不 panic。
	h.st = nil
	h.auditRecord(kafkaconn.AuditRecord{Action: "topics/list", Result: "success"})
}

func TestStreamEmitterAdapters(t *testing.T) {
	// emitter 未挂载：两条 emit 都是空操作。
	idle := &pluginHandler{}
	idle.EmitStreamMessages(kafkaconn.StreamMessageBatch{SessionID: "s1"})
	idle.EmitStreamError("s1", "boom")

	var buf bytes.Buffer
	h := &pluginHandler{emitter: captureServeEmitter(t, &buf)}
	h.EmitStreamMessages(kafkaconn.StreamMessageBatch{SessionID: "s1", TotalScanned: 3, TotalMatched: 1})
	for _, want := range []string{"kafka/stream/messages", `"sessionId":"s1"`} {
		if !strings.Contains(buf.String(), want) {
			t.Fatalf("stream messages buffer missing %q: %s", want, buf.String())
		}
	}

	buf.Reset()
	h.EmitStreamError("s2", "boom")
	for _, want := range []string{"kafka/stream/error", `"sessionId":"s2"`, "boom"} {
		if !strings.Contains(buf.String(), want) {
			t.Fatalf("stream error buffer missing %q: %s", want, buf.String())
		}
	}

	// writer 写失败只记日志，不 panic。
	loose := &pluginHandler{emitter: captureServeEmitter(t, failingWriter{})}
	loose.EmitStreamMessages(kafkaconn.StreamMessageBatch{SessionID: "s1"})
	loose.EmitStreamError("s1", "boom")
}

func TestDecodeParamsGates(t *testing.T) {
	if perr := decodeParams(json.RawMessage(`{bad`), &struct{}{}); perr == nil || perr.Code != -32602 {
		t.Fatalf("bad json: perr=%v, want -32602", perr)
	}
	if perr := decodeParams(json.RawMessage(`{}`), &struct{}{}); perr == nil || perr.Code != -32602 ||
		perr.Message != "Missing connectionId" {
		t.Fatalf("missing connectionId: perr=%v, want Missing connectionId", perr)
	}
	var out struct {
		ConnectionID string `json:"connectionId"`
		Topic        string `json:"topic"`
	}
	if perr := decodeParams(json.RawMessage(`{"connectionId":"c1","topic":"t1"}`), &out); perr != nil {
		t.Fatalf("valid params: %v", perr)
	}
	if out.ConnectionID != "c1" || out.Topic != "t1" {
		t.Fatalf("decoded = %+v", out)
	}
}

func TestRequireSessionID(t *testing.T) {
	if _, perr := requireSessionID(json.RawMessage(`{bad`)); perr == nil || perr.Code != -32602 {
		t.Fatalf("bad json: perr=%v, want -32602", perr)
	}
	if _, perr := requireSessionID(json.RawMessage(`{"sessionId":"  "}`)); perr == nil || perr.Code != -32602 {
		t.Fatalf("blank session: perr=%v, want -32602", perr)
	}
	got, perr := requireSessionID(json.RawMessage(`{"sessionId":" s1 "}`))
	if perr != nil || got != "s1" {
		t.Fatalf("got=%q perr=%v, want s1", got, perr)
	}
}

func TestErrorAndAuditHelpers(t *testing.T) {
	if perr := invalidParams(errors.New("boom")); perr.Code != -32602 || perr.Message != "boom" {
		t.Fatalf("invalidParams = %+v", perr)
	}
	if perr := bizError(errors.New("offline")); perr.Code != -32000 {
		t.Fatalf("bizError plain = %+v, want -32000", perr)
	}
	if perr := bizError(&kafkaconn.InvalidParamsError{Msg: "bad combo"}); perr.Code != -32602 {
		t.Fatalf("bizError InvalidParams = %+v, want -32602", perr)
	}

	actionCases := map[string]string{
		"topics/delete": "kafka/topics/delete",
		"kafka/ping":    "kafka/ping",
		"":              "kafka/",
	}
	for in, want := range actionCases {
		if got := auditAction(in); got != want {
			t.Fatalf("auditAction(%q) = %q, want %q", in, got, want)
		}
	}
	resultCases := map[string]string{
		"success": "ok",
		"blocked": "denied",
		"error":   "error",
		"":        "",
	}
	for in, want := range resultCases {
		if got := auditResultForStore(in); got != want {
			t.Fatalf("auditResultForStore(%q) = %q, want %q", in, got, want)
		}
	}

	if getContext() == nil {
		t.Fatal("getContext returned nil")
	}
	if newPresetStore(nil) != nil {
		t.Fatal("newPresetStore(nil) should be nil")
	}
}
