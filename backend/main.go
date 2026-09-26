// dbx-plugin-kafka sidecar 入口（A 路）。
//
// 入口互斥：`--mcp` 进入独立 stdio MCP 服务器模式（MCP 2024-11-05，ssh
// 同款；internal/mcp stdio.go），不启动 Emitter/插件协议循环/流式会话；
// 不带标志则按下述 DBX 插件协议模式运行。同一进程只跑其中一种。
//
// 装配：dbxpluginsdk.NewServer + Handler switch。方法表按实施文档 §5.2 全量：
//
//	connection/test | connection/connect | connection/disconnect      （生命周期）
//	kafka/brokers/list | kafka/brokers/config
//	kafka/topics/list | describe | create | delete | partitions/update |
//	  config/get | config/alter | offsets/list | records/clear  （Phase 3
//	  records/clear 为 critical 门禁）
//	kafka/groups/list | describe | offsets/list | delete | offsets/reset
//	kafka/acls/list | create | delete
//	kafka/messages/produce | consume | export
//	kafka/stream/start | stop | pause | resume | status | messages
//	kafka/presets/list | save | remove
//	kafka/connections/statuses
//	kafka/schema/test | subjects/list | versions/list | get |
//	  versions/compare | compatibility/get | compatibility/set |
//	  compatibility/check | register | delete | delete/version  （Phase 2；
//	  Phase 3 起全部支持可选 registry:"confluent"|"glue"）
//	kafka/ui/state/report                                            （MCP intent 回报，前端回调）
//	mcp/tools | mcp/call | mcp/settings/get | mcp/settings/set       （M3 MCP 工具面，internal/mcp）
//
// 公共约定：参数/返回 camelCase；领域方法必填 connectionId
// （kafka/connections/statuses 为全局视图可省）；参数错 -32602、业务错
// -32000、未注册 -32601（SDK MethodNotFound）。凭据不进日志/事件/审计。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"strings"
	"sync"

	dbxpluginsdk "github.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk"

	"io.dbx.kafka.plugin/internal/kafkaconn"
	"io.dbx.kafka.plugin/internal/lifecycle"
	"io.dbx.kafka.plugin/internal/mcp"
	"io.dbx.kafka.plugin/internal/store"
)

// 身份版本：默认仅 go run/go test 兜底；scripts/build.sh 打包时用
// -ldflags "-X main.version=..." 从 manifest.json 注入，保证与 manifest 一致。
var version = "0.0.0-dev"

// --- 独立 stdio MCP 服务器模式（`--mcp`） ---

// mcpStdioRequested 报告 args 是否携带 `--mcp` 标志（精确匹配，照 ssh
// mcp 入口约定）。true 时进程进入 stdio MCP 服务器模式，不启动插件协议。
func mcpStdioRequested(args []string) bool {
	for _, arg := range args {
		if arg == "--mcp" {
			return true
		}
	}
	return false
}

// runMcpStdio 独立 stdio MCP 服务器（internal/mcp stdio.go）：自带连接
// service 与工具面 Server；审计只落 audit.jsonl（无 Emitter，不发事件）。
// 日志走 stderr（stdout 是协议通道）。
func runMcpStdio() {
	st, err := store.Open()
	if err != nil {
		log.Printf("[dbx-plugin-kafka] store disabled: %v", err)
		st = nil
	}
	server := mcp.NewStdioServer(version, st, func(rec kafkaconn.AuditRecord) {
		if st == nil {
			return
		}
		if err := st.AppendAudit(store.AuditRecord{
			ConnectionID: rec.ConnectionID,
			Action:       auditAction(rec.Action),
			Target:       rec.Target,
			Result:       auditResultForStore(rec.Result),
			Source:       rec.Source,
		}); err != nil {
			log.Printf("[dbx-plugin-kafka] audit write failed: %v", err)
		}
	})
	defer server.Close()
	if err := server.Serve(os.Stdin, os.Stdout); err != nil {
		log.Fatal(err)
	}
}

// pluginHandler 实现 dbxpluginsdk.Handler。
type pluginHandler struct {
	svc    *kafkaconn.Service
	st     *store.Store
	mcpSrv *mcp.Server

	mu      sync.Mutex
	emitter *dbxpluginsdk.Emitter // Serve 期间单例，用于 kafka/audit、stream 与 kafka/ui/intent 事件
}

func main() {
	// `--mcp`：独立 stdio MCP 服务器模式（与插件协议模式互斥，见文件头）。
	if mcpStdioRequested(os.Args[1:]) {
		runMcpStdio()
		return
	}

	st, err := store.Open()
	if err != nil {
		// 数据目录不可用不阻断连接能力：审计/预设降级为不可用。
		log.Printf("[dbx-plugin-kafka] store disabled: %v", err)
		st = nil
	}

	svc := kafkaconn.NewService()
	handler := &pluginHandler{svc: svc, st: st}
	svc.Audit = handler.auditRecord
	svc.Presets = newPresetStore(st)
	svc.Streams.Emitter = handler

	// MCP 工具面（M3）：mcp/tools|call|settings + kafka/ui/state/report。
	// intent 事件经当前 Emitter 下发（与 audit/stream 同一条持锁通道）。
	mcpSrv := mcp.NewServer(svc, st)
	mcpSrv.SetEmitter(func(method string, params any) {
		handler.mu.Lock()
		emitter := handler.emitter
		handler.mu.Unlock()
		if emitter != nil {
			if err := emitter.Event(method, params); err != nil {
				log.Printf("[dbx-plugin-kafka] %s event failed: %v", method, err)
			}
		}
	})
	handler.mcpSrv = mcpSrv

	metadata := dbxpluginsdk.Metadata{
		ID:           "io.dbx.kafka",
		Version:      version,
		Capabilities: []string{"connections", "events"},
	}
	server := dbxpluginsdk.NewServer(metadata, handler)

	// stdin EOF（进程生命周期结束）→ Serve 返回 → 清理全部连接与流式会话（M0 §3.2）。
	defer svc.CloseAll()
	if err := server.Serve(); err != nil {
		log.Fatal(err)
	}
}

// Handle 方法分发（每请求一个 goroutine，Handler 须并发安全：全部状态在
// kafkaconn.Service 内加锁，本结构体仅 emitter 一个可变字段且持锁访问）。
func (h *pluginHandler) Handle(
	_ dbxpluginsdk.RequestContext,
	method string,
	params json.RawMessage,
	emitter *dbxpluginsdk.Emitter,
) (any, *dbxpluginsdk.PluginError) {
	h.mu.Lock()
	h.emitter = emitter
	h.mu.Unlock()

	switch method {
	case "connection/test":
		return h.connectionTest(params)
	case "connection/connect":
		return h.connectionConnect(params)
	case "connection/disconnect":
		return h.connectionDisconnect(params)

	case "kafka/brokers/list":
		return h.brokersList(params)
	case "kafka/brokers/config":
		return h.brokersConfig(params)

	case "kafka/topics/list":
		return h.topicsList(params)
	case "kafka/topics/describe":
		return h.topicsDescribe(params)
	case "kafka/topics/create":
		return h.topicsCreate(params)
	case "kafka/topics/delete":
		return h.topicsDelete(params)
	case "kafka/topics/partitions/update":
		return h.partitionsUpdate(params)
	case "kafka/topics/config/get":
		return h.topicConfigGet(params)
	case "kafka/topics/config/alter":
		return h.topicConfigAlter(params)
	case "kafka/topics/offsets/list":
		return h.topicOffsetsList(params)
	case "kafka/topics/records/clear":
		return h.topicsRecordsClear(params)

	case "kafka/groups/list":
		return h.groupsList(params)
	case "kafka/groups/describe":
		return h.groupsDescribe(params)
	case "kafka/groups/offsets/list":
		return h.groupOffsetsList(params)
	case "kafka/groups/delete":
		return h.groupDelete(params)
	case "kafka/groups/offsets/reset":
		return h.groupOffsetsReset(params)

	case "kafka/acls/list":
		return h.aclsList(params)
	case "kafka/acls/create":
		return h.aclsCreate(params)
	case "kafka/acls/delete":
		return h.aclsDelete(params)

	case "kafka/messages/produce":
		return h.messagesProduce(params)
	case "kafka/messages/consume":
		return h.messagesConsume(params)
	case "kafka/messages/export":
		return h.messagesExport(params)

	case "kafka/stream/start":
		return h.streamStart(params)
	case "kafka/stream/stop":
		return h.streamStop(params)
	case "kafka/stream/pause":
		return h.streamPause(params)
	case "kafka/stream/resume":
		return h.streamResume(params)
	case "kafka/stream/status":
		return h.streamStatus(params)
	case "kafka/stream/messages":
		return h.streamMessages(params)

	case "kafka/presets/list":
		return h.presetsList()
	case "kafka/presets/save":
		return h.presetsSave(params)
	case "kafka/presets/remove":
		return h.presetsRemove(params)

	case "kafka/connections/statuses":
		return h.connectionStatuses()

	case "kafka/ui/state/report":
		return h.uiStateReport(params)

	case "mcp/tools":
		return h.mcpTools(params)
	case "mcp/call":
		return h.mcpCall(params)
	case "mcp/settings/get":
		return h.mcpSrv.SettingsGet(), nil
	case "mcp/settings/set":
		return h.mcpSettingsSet(params)

	// --- Phase 2/3：schema registry（Confluent 兼容 REST + AWS Glue，
	// registry 参数显式选择后端，缺省自动探测） ---
	case "kafka/schema/test":
		return h.schemaTest(params)
	case "kafka/schema/subjects/list":
		return h.schemaSubjectsList(params)
	case "kafka/schema/versions/list":
		return h.schemaVersionsList(params)
	case "kafka/schema/get":
		return h.schemaGet(params)
	case "kafka/schema/versions/compare":
		return h.schemaVersionsCompare(params)
	case "kafka/schema/compatibility/get":
		return h.schemaCompatibilityGet(params)
	case "kafka/schema/compatibility/set":
		return h.schemaCompatibilitySet(params)
	case "kafka/schema/compatibility/check":
		return h.schemaCompatibilityCheck(params)
	case "kafka/schema/register":
		return h.schemaRegister(params)
	case "kafka/schema/delete":
		return h.schemaDelete(params)
	case "kafka/schema/delete/version":
		return h.schemaDeleteVersion(params)

	default:
		return nil, dbxpluginsdk.MethodNotFound(method)
	}
}

// --- 生命周期方法（§5.1） ---

func (h *pluginHandler) connectionTest(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	parsed, err := lifecycle.Parse(params)
	if err != nil {
		return nil, invalidParams(err)
	}
	if parsed.ConnectionID() == "" {
		return nil, dbxpluginsdk.NewError(-32602, "Missing connection.id")
	}
	message, err := h.svc.Test(getContext(), parsed)
	if err != nil {
		return nil, bizError(err)
	}
	return map[string]any{"success": true, "message": message}, nil
}

func (h *pluginHandler) connectionConnect(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	parsed, err := lifecycle.Parse(params)
	if err != nil {
		return nil, invalidParams(err)
	}
	if err := h.svc.Connect(parsed); err != nil {
		return nil, bizError(err)
	}
	return map[string]any{"success": true}, nil
}

func (h *pluginHandler) connectionDisconnect(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	parsed, err := lifecycle.Parse(params)
	if err != nil {
		return nil, invalidParams(err)
	}
	id := parsed.ConnectionID()
	if id == "" {
		return nil, dbxpluginsdk.NewError(-32602, "Missing connection id")
	}
	h.svc.Disconnect(id)
	return map[string]any{"success": true}, nil
}

// --- brokers ---

func (h *pluginHandler) brokersList(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req struct {
		ConnectionID string `json:"connectionId"`
	}
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.ListBrokers(getContext(), req.ConnectionID)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

func (h *pluginHandler) brokersConfig(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.BrokerConfigRequest
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.DescribeBrokerConfig(getContext(), req)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

// --- topics ---

func (h *pluginHandler) topicsList(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.TopicsListRequest
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.ListTopics(getContext(), req)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

func (h *pluginHandler) topicsDescribe(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.TopicsDescribeRequest
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.DescribeTopic(getContext(), req)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

func (h *pluginHandler) topicsCreate(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.TopicsCreateRequest
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	results, err := h.svc.CreateTopics(getContext(), req)
	if err != nil {
		return nil, bizError(err)
	}
	return map[string]any{"results": results}, nil
}

func (h *pluginHandler) topicsDelete(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.TopicsDeleteRequest
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	results, err := h.svc.DeleteTopics(getContext(), req)
	if err != nil {
		return nil, bizError(err)
	}
	return map[string]any{"results": results}, nil
}

func (h *pluginHandler) partitionsUpdate(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.PartitionsUpdateRequest
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	results, err := h.svc.UpdatePartitions(getContext(), req)
	if err != nil {
		return nil, bizError(err)
	}
	return map[string]any{"results": results}, nil
}

func (h *pluginHandler) topicConfigGet(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.TopicConfigGetRequest
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.GetTopicConfig(getContext(), req)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

func (h *pluginHandler) topicConfigAlter(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.TopicConfigAlterRequest
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	results, err := h.svc.AlterTopicConfig(getContext(), req)
	if err != nil {
		return nil, bizError(err)
	}
	return map[string]any{"results": results}, nil
}

func (h *pluginHandler) topicOffsetsList(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.TopicOffsetsListRequest
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.ListTopicOffsets(getContext(), req)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

// topicsRecordsClear 实现 kafka/topics/records/clear（Phase 3，critical 门禁
// 在 Service 层：read_only/allow_delete 与门 + confirmTopic → -32602）。
func (h *pluginHandler) topicsRecordsClear(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.TopicRecordsClearRequest
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.ClearTopicRecords(getContext(), req)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

// --- groups ---

func (h *pluginHandler) groupsList(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req struct {
		ConnectionID string `json:"connectionId"`
	}
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.ListGroups(getContext(), req.ConnectionID)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

func (h *pluginHandler) groupsDescribe(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.GroupsDescribeRequest
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.DescribeGroup(getContext(), req)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

func (h *pluginHandler) groupOffsetsList(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.GroupOffsetsListRequest
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.GetGroupOffsets(getContext(), req)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

func (h *pluginHandler) groupDelete(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.GroupDeleteRequest
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	if err := h.svc.DeleteGroup(getContext(), req); err != nil {
		return nil, bizError(err)
	}
	return map[string]any{"success": true}, nil
}

func (h *pluginHandler) groupOffsetsReset(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.GroupOffsetResetRequest
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.ResetGroupOffsets(getContext(), req)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

// --- acls ---

func (h *pluginHandler) aclsList(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.ACLsListRequest
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.ListACLs(getContext(), req)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

func (h *pluginHandler) aclsCreate(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.ACLsCreateRequest
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	if err := h.svc.CreateACL(getContext(), req); err != nil {
		return nil, bizError(err)
	}
	return map[string]any{"success": true}, nil
}

func (h *pluginHandler) aclsDelete(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.ACLsDeleteRequest
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.DeleteACLs(getContext(), req)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

// --- messages ---

func (h *pluginHandler) messagesProduce(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.ProduceRequest
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.Produce(getContext(), req)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

func (h *pluginHandler) messagesConsume(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.ConsumeParams
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.Consume(getContext(), req)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

func (h *pluginHandler) messagesExport(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.ExportRequest
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.Export(getContext(), req)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

// --- stream ---

func (h *pluginHandler) streamStart(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.ConsumeParams
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	status, err := h.svc.StartStream(req)
	if err != nil {
		return nil, bizError(err)
	}
	return map[string]any{"sessionId": status.SessionID, "status": status}, nil
}

func (h *pluginHandler) streamStop(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req struct {
		SessionID string `json:"sessionId"`
		All       bool   `json:"all,omitempty"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, invalidParams(err)
	}
	if !req.All && strings.TrimSpace(req.SessionID) == "" {
		return nil, dbxpluginsdk.NewError(-32602, "Missing sessionId (or all=true)")
	}
	h.svc.StopStream(req.SessionID, req.All)
	return map[string]any{"success": true}, nil
}

func (h *pluginHandler) streamPause(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	sessionID, perr := requireSessionID(params)
	if perr != nil {
		return nil, perr
	}
	status, err := h.svc.PauseStream(sessionID)
	if err != nil {
		return nil, bizError(err)
	}
	return map[string]any{"status": status}, nil
}

func (h *pluginHandler) streamResume(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	sessionID, perr := requireSessionID(params)
	if perr != nil {
		return nil, perr
	}
	status, err := h.svc.ResumeStream(sessionID)
	if err != nil {
		return nil, bizError(err)
	}
	return map[string]any{"status": status}, nil
}

func (h *pluginHandler) streamStatus(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	sessionID, perr := requireSessionID(params)
	if perr != nil {
		return nil, perr
	}
	status, err := h.svc.StreamStatusOf(sessionID)
	if err != nil {
		return nil, bizError(err)
	}
	return map[string]any{"status": status}, nil
}

func (h *pluginHandler) streamMessages(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req struct {
		SessionID string `json:"sessionId"`
		Offset    int    `json:"offset"`
		Limit     int    `json:"limit"`
	}
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.StreamMessages(req.SessionID, req.Offset, req.Limit)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

// --- presets / statuses ---

func (h *pluginHandler) presetsList() (any, *dbxpluginsdk.PluginError) {
	presets, err := h.svc.ListPresets()
	if err != nil {
		return nil, bizError(err)
	}
	return map[string]any{"presets": presets}, nil
}

func (h *pluginHandler) presetsSave(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var body struct {
		Preset kafkaconn.ConsumePreset `json:"preset"`
	}
	if err := json.Unmarshal(params, &body); err != nil {
		return nil, invalidParams(err)
	}
	preset, err := h.svc.SavePreset(body.Preset)
	if err != nil {
		return nil, bizError(err)
	}
	return map[string]any{"success": true, "preset": preset}, nil
}

func (h *pluginHandler) presetsRemove(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var body struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(params, &body); err != nil {
		return nil, invalidParams(err)
	}
	if body.ID == "" {
		return nil, dbxpluginsdk.NewError(-32602, "Missing preset id")
	}
	if err := h.svc.RemovePreset(body.ID); err != nil {
		return nil, bizError(err)
	}
	return map[string]any{"success": true}, nil
}

func (h *pluginHandler) connectionStatuses() (any, *dbxpluginsdk.PluginError) {
	return map[string]any{"statuses": h.svc.SnapshotStatuses()}, nil
}

// --- Phase 2：schema registry 方法臂（kafka/schema/*） ---

func (h *pluginHandler) schemaTest(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req struct {
		ConnectionID string `json:"connectionId"`
		Registry     string `json:"registry,omitempty"`
	}
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.TestSchema(getContext(), req.ConnectionID, req.Registry)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

func (h *pluginHandler) schemaSubjectsList(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req struct {
		ConnectionID string `json:"connectionId"`
		Registry     string `json:"registry,omitempty"`
	}
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.ListSchemaSubjects(getContext(), req.ConnectionID, req.Registry)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

func (h *pluginHandler) schemaVersionsList(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req struct {
		ConnectionID string `json:"connectionId"`
		Subject      string `json:"subject"`
		Registry     string `json:"registry,omitempty"`
	}
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.ListSchemaVersions(getContext(), req.ConnectionID, req.Subject, req.Registry)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

func (h *pluginHandler) schemaGet(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req struct {
		ConnectionID string `json:"connectionId"`
		Subject      string `json:"subject"`
		Version      int64  `json:"version,omitempty"`
		Registry     string `json:"registry,omitempty"`
	}
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.GetSchema(getContext(), req.ConnectionID, req.Subject, req.Version, req.Registry)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

func (h *pluginHandler) schemaVersionsCompare(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.SchemaVersionsCompareRequest
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.CompareSchemaVersions(getContext(), req)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

func (h *pluginHandler) schemaCompatibilityGet(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req struct {
		ConnectionID string `json:"connectionId"`
		Subject      string `json:"subject,omitempty"`
		Registry     string `json:"registry,omitempty"`
	}
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.GetSchemaCompatibility(getContext(), req.ConnectionID, req.Subject, req.Registry)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

func (h *pluginHandler) schemaCompatibilitySet(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.SchemaCompatibilityRequest
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.SetSchemaCompatibility(getContext(), req)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

func (h *pluginHandler) schemaCompatibilityCheck(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.SchemaCompatibilityCheckRequest
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.CheckSchemaCompatibility(getContext(), req)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

func (h *pluginHandler) schemaRegister(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.SchemaRegisterRequest
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.RegisterSchema(getContext(), req)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

func (h *pluginHandler) schemaDelete(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.SchemaDeleteRequest
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.DeleteSchema(getContext(), req)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

// schemaDeleteVersion 与 schemaDelete 同一服务方法：version>0 走
// /versions/<v> 删除臂（critical 门禁一致）。
func (h *pluginHandler) schemaDeleteVersion(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.SchemaDeleteRequest
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	if req.Version <= 0 {
		return nil, dbxpluginsdk.NewError(-32602, "version is required")
	}
	result, err := h.svc.DeleteSchema(getContext(), req)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

// --- MCP 工具面（M3，internal/mcp） ---

// uiStateReport 处理 kafka/ui/state/report（前端回调）：带 intentId 回报
// intent 终态；无 intentId 为快照型（sidecar 缓存最新快照）。
func (h *pluginHandler) uiStateReport(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var body map[string]any
	if err := json.Unmarshal(params, &body); err != nil {
		return nil, invalidParams(err)
	}
	if err := h.mcpSrv.ReportUIState(body); err != nil {
		return nil, bizError(err)
	}
	return map[string]any{"success": true}, nil
}

// mcpTools 处理 mcp/tools（DBX MCP 桥工具发现）：可选 connectionId，连接
// 策略不满足的写工具不进清单（设计 §4）。
func (h *pluginHandler) mcpTools(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var body struct {
		ConnectionID string `json:"connectionId"`
	}
	_ = json.Unmarshal(params, &body)
	return h.mcpSrv.Tools(strings.TrimSpace(body.ConnectionID)), nil
}

// mcpCall 处理 mcp/call（DBX MCP 桥 dbx_call_plugin_tool）：注册 lifecycle
// payload（凭据由宿主转发，参数不携带）→ 分派工具。响应统一 MCP content
// 信封（对齐 ssh / ldap mcp/call）。
func (h *pluginHandler) mcpCall(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var body struct {
		Tool      string          `json:"tool"`
		Arguments json.RawMessage `json:"arguments"`
		Lifecycle json.RawMessage `json:"lifecycle"`
	}
	if err := json.Unmarshal(params, &body); err != nil {
		return nil, invalidParams(err)
	}
	if strings.TrimSpace(body.Tool) == "" {
		return nil, dbxpluginsdk.NewError(-32602, "Missing tool")
	}
	// 桥转发附带 lifecycle payload：注册/刷新连接配置（幂等覆盖），之后工具
	// 调用只引用 connectionId。解析失败即拒绝（不静默丢凭据上下文）。
	if len(body.Lifecycle) > 0 {
		parsed, err := lifecycle.Parse(body.Lifecycle)
		if err != nil {
			return nil, invalidParams(err)
		}
		if err := h.svc.Connect(parsed); err != nil {
			return nil, bizError(err)
		}
	}
	var arguments map[string]any
	if len(body.Arguments) > 0 {
		if err := json.Unmarshal(body.Arguments, &arguments); err != nil {
			return nil, invalidParams(err)
		}
	}
	result, err := h.mcpSrv.Call(strings.TrimSpace(body.Tool), arguments)
	if err != nil {
		return nil, bizError(err)
	}
	payload, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		return nil, dbxpluginsdk.NewError(-32603, marshalErr.Error())
	}
	return map[string]any{
		"content": []map[string]any{{"type": "text", "text": string(payload)}},
		"isError": false,
	}, nil
}

// mcpSettingsSet 处理 mcp/settings/set（白名单部分更新 + 持久化）。
func (h *pluginHandler) mcpSettingsSet(params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var body map[string]any
	if err := json.Unmarshal(params, &body); err != nil {
		return nil, invalidParams(err)
	}
	result, err := h.mcpSrv.SettingsSet(body)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}

// --- StreamEmitter 适配（kafka/stream/messages、kafka/stream/error） ---

// EmitStreamMessages 实现 kafkaconn.StreamEmitter。
func (h *pluginHandler) EmitStreamMessages(batch kafkaconn.StreamMessageBatch) {
	h.mu.Lock()
	emitter := h.emitter
	h.mu.Unlock()
	if emitter == nil {
		return
	}
	if err := emitter.Event("kafka/stream/messages", batch); err != nil {
		log.Printf("[dbx-plugin-kafka] stream event failed: %v", err)
	}
}

// EmitStreamError 实现 kafkaconn.StreamEmitter。
func (h *pluginHandler) EmitStreamError(sessionID, message string) {
	h.mu.Lock()
	emitter := h.emitter
	h.mu.Unlock()
	if emitter == nil {
		return
	}
	payload := map[string]string{"sessionId": sessionID, "error": message}
	if err := emitter.Event("kafka/stream/error", payload); err != nil {
		log.Printf("[dbx-plugin-kafka] stream error event failed: %v", err)
	}
}

// --- 审计与公共 helper ---

// auditRecord 是 kafkaconn.Service.Audit 回调：audit.jsonl 落盘 +
// kafka/audit 事件（§5.4；两条通道携带同一份非敏感数据；rec.Target 只含
// 资源名，不含消息值/凭据）。
func (h *pluginHandler) auditRecord(rec kafkaconn.AuditRecord) {
	if h.st != nil {
		if err := h.st.AppendAudit(store.AuditRecord{
			ConnectionID: rec.ConnectionID,
			Action:       auditAction(rec.Action),
			Target:       rec.Target,
			Result:       auditResultForStore(rec.Result),
			Source:       rec.Source,
		}); err != nil {
			log.Printf("[dbx-plugin-kafka] audit write failed: %v", err)
		}
	}
	h.mu.Lock()
	emitter := h.emitter
	h.mu.Unlock()
	if emitter != nil {
		if err := emitter.Event("kafka/audit", rec); err != nil {
			log.Printf("[dbx-plugin-kafka] audit event failed: %v", err)
		}
	}
}

// auditAction 统一审计 action 命名（kafka/<action>）。
func auditAction(action string) string {
	if strings.HasPrefix(action, "kafka/") {
		return action
	}
	return "kafka/" + action
}

// auditResultForStore 把内部 result（success|blocked|error）折算为
// audit.jsonl 的 ok|denied|error（M0 §4 格式，对齐 ssh-sftp / ldap）。
func auditResultForStore(result string) string {
	switch result {
	case "success":
		return "ok"
	case "blocked":
		return "denied"
	default:
		return result
	}
}

// decodeParams 解析领域方法参数并校验必填 connectionId
// （kafka/connections/statuses 为全局视图，不经此函数）。
func decodeParams(params json.RawMessage, out any) *dbxpluginsdk.PluginError {
	if err := json.Unmarshal(params, out); err != nil {
		return invalidParams(err)
	}
	var holder struct {
		ConnectionID string `json:"connectionId"`
	}
	if err := json.Unmarshal(params, &holder); err == nil && holder.ConnectionID == "" {
		return dbxpluginsdk.NewError(-32602, "Missing connectionId")
	}
	return nil
}

// requireSessionID 解析并校验 sessionId 必填。
func requireSessionID(params json.RawMessage) (string, *dbxpluginsdk.PluginError) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.Unmarshal(params, &body); err != nil {
		return "", invalidParams(err)
	}
	sessionID := strings.TrimSpace(body.SessionID)
	if sessionID == "" {
		return "", dbxpluginsdk.NewError(-32602, "Missing sessionId")
	}
	return sessionID, nil
}

func invalidParams(err error) *dbxpluginsdk.PluginError {
	return dbxpluginsdk.NewError(-32602, err.Error())
}

// bizError 业务错误统一 -32000（blocked 语义，对齐 ssh-sftp / ldap）；
// InvalidParamsError（如 schema registry 参数歧义/未知 registry）映射 -32602。
func bizError(err error) *dbxpluginsdk.PluginError {
	var paramErr *kafkaconn.InvalidParamsError
	if errors.As(err, &paramErr) {
		return dbxpluginsdk.NewError(-32602, err.Error())
	}
	return dbxpluginsdk.NewError(-32000, err.Error())
}

// getContext 返回请求级 context（jsonl SDK 无 ctx 传递，超时由
// kafkaconn 层按操作控制）。
func getContext() context.Context {
	return context.Background()
}

// presetStore 把 store.Store 适配为 kafkaconn.PresetStore（presets.json 全量
// 读写；预设明文、不含凭据）。
type presetStore struct {
	st *store.Store
}

func newPresetStore(st *store.Store) kafkaconn.PresetStore {
	if st == nil {
		return nil
	}
	return &presetStore{st: st}
}

func (p *presetStore) LoadPresets() ([]kafkaconn.ConsumePreset, error) {
	var doc struct {
		Presets []kafkaconn.ConsumePreset `json:"presets"`
	}
	exists, err := p.st.LoadJSON("presets.json", &doc)
	if err != nil {
		return nil, err
	}
	if !exists || doc.Presets == nil {
		return []kafkaconn.ConsumePreset{}, nil
	}
	return doc.Presets, nil
}

func (p *presetStore) SavePresets(presets []kafkaconn.ConsumePreset) error {
	if presets == nil {
		presets = []kafkaconn.ConsumePreset{}
	}
	return p.st.SaveJSON("presets.json", map[string]any{"presets": presets})
}
