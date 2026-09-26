package mcp

// stdio.go：standalone `--mcp` 独立 stdio MCP 服务器模式（设计 §0.2/§5
// stdio 行；ssh backend/src/mcp.rs `run_mcp_stdio` 的 Go 移植，ldap Go 版
// 同构）。main.go 检测 `--mcp` 标志后进入本模式，与 DBX 插件协议模式互斥
// （同一进程只跑其中一种；本模式不启动 Emitter/插件协议循环/流式会话）。
// 协议为 MCP 2024-11-05，换行分隔 JSON-RPC 2.0 over stdio。
//
// 工具语义全部复用 Server.Call（stdio 只是新入口，不复制 digest/cursor/
// 两阶段写逻辑；MCP 仍不订阅 stream）：
//   - UI 类工具（kafka_ui_*，含元发现 kafka_ui_topics）照常出现在
//     tools/list，tools/call 返回明确 UNAVAILABLE（需要 DBX 工作台），
//     不假死（设计 §5 stdio 行）；
//   - 其余工具：连接参数随每次调用内联（不落盘、不持久化），按参数 hash
//     进程内池化——以池化 id 经 svc.Connect 注册到与工作台同一套底层连接
//     service，再注入 connectionId 走既有分派路径。
//
// 本轮实现桥接兜底（connectionId 转发，同 ssh/ldap）：工具收到本会话未
// 池化的 `connectionId` 时，经 appbridge.go 转发运行中 DBX 应用的本地 TCP
// 桥（应用自己的 sidecar 持有保存连接的凭据/只读标志/策略）；桥不可达
// 立刻返回含 "DBX app bridge" 失败原因与内联凭据出路的可行动错误
// （fail-closed，不假死），见 docs/MCP.zh-CN.md「桥接兜底」。

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"io.dbx.kafka.plugin/internal/kafkaconn"
	"io.dbx.kafka.plugin/internal/lifecycle"
	"io.dbx.kafka.plugin/internal/store"
)

// MCPProtocolVersion 本插件 MCP 服务器声明的协议版本（照 ssh mcp.rs）。
const MCPProtocolVersion = "2024-11-05"

// inlinePoolCap 进程内内联凭据连接池上限（参数 hash → 池化 connectionId；
// 淘汰最旧并断开，防 AI 连续换凭据撑大连接表；与 cursor LRU ≤8 同量级）。
const inlinePoolCap = 8

// uiToolPrefix UI 类工具名前缀（含元发现 kafka_ui_topics）：stdio 模式下
// tools/call 一律 UNAVAILABLE（此类工具靠 Emitter 把 intent 交给前端，
// stdio 无工作台无意义）。
const uiToolPrefix = "kafka_ui_"

// connectionBoundTools 需要连接的工具（连接解析门适用；cursor_next 只依赖
// cursor 会话、UI 类工具直接 UNAVAILABLE，都不在此列）。
var connectionBoundTools = map[string]struct{}{
	"kafka_messages_digest":      {},
	"kafka_messages_produce":     {},
	"kafka_topics_delete":        {},
	"kafka_groups_offsets_reset": {},
	"kafka_topics_records_clear": {},
}

// inlineRequiredKey anyOf 二选一中内联凭据侧的必填键（tools/list schema
// 放宽声明用）。
const inlineRequiredKey = "brokers"

// maxRequestLineBytes 单行请求上限（16 MiB）：超限行直接 -32700 拒绝——
// bufio 逐行读无上限会随单行长度无界占内存，16 MiB 已远超任何合法 MCP
// 请求（大 arguments 属工作台职责），超限必须优雅报错：不 panic 不挂死、
// 进程继续服务后续请求（可靠性纵深轮，ldap 同构）。
const maxRequestLineBytes = 16 << 20

// maxConcurrentRequests 在途请求并发上限（评审 M：每请求一 goroutine 原本
// 无界——被控客户端可并发灌入大量慢请求放大内存；主循环在槽满时暂停读入
// 形成背压，超限行/空行不受影响仍即时处置）。量级对齐桌面单会话 AI 场景
// 的合法并发（digest/ping/tools-list 交错远低于此）。
const maxConcurrentRequests = 32

// StdioServer 独立 stdio 模式的 MCP 服务器：包装工具面 Server + 内联凭据
// 连接池 + DBX 桥转发兜底。并发安全（每请求一个 goroutine）。
type StdioServer struct {
	srv     *Server
	svc     *kafkaconn.Service
	version string

	// bridgeEnsureWait 桥兜底里 launch 后等端口就绪的预算（默认 30s；
	// 测试/smoke 引导错误用例可缩短，fail-closed 语义不变）。
	bridgeEnsureWait time.Duration

	mu    sync.Mutex
	ids   map[string]struct{} // 已池化 connectionId 存在性集合（poolHas/淘汰用；评审 L：原值从未被读取）
	hash  map[string]string   // 参数 hash → 池化 connectionId
	order []string            // 参数 hash 淘汰序（FIFO）
}

// NewStdioServer 构造 stdio MCP 服务器（自带底层连接 service 与工具面
// Server；settings 从数据目录加载，损坏回落默认）。audit 为写操作审计
// 落盘回调（main 注入 store.AppendAudit 适配；nil 跳过）。
func NewStdioServer(version string, st *store.Store, audit func(kafkaconn.AuditRecord)) *StdioServer {
	svc := kafkaconn.NewService()
	svc.Audit = audit
	return &StdioServer{
		srv:              NewServer(svc, st),
		svc:              svc,
		version:          version,
		bridgeEnsureWait: DefaultBridgeEnsureWait,
		ids:              map[string]struct{}{},
		hash:             map[string]string{},
	}
}

// Close 释放全部底层连接与流式会话（进程退出前，M0 §3.2）。
func (s *StdioServer) Close() {
	s.svc.CloseAll()
}

// Serve 逐行读 JSON-RPC 请求并写回响应，直到输入 EOF。每个请求一个
// goroutine（慢的 digest 不阻塞 ping/tools/list；响应可能乱序，JSON-RPC
// 以 id 关联），EOF 后 drain 在途请求至多 300s（照 ssh run_mcp_stdio）。
// 单行超 maxRequestLineBytes 时直接 -32700 拒绝该行并继续（进程存活）。
func (s *StdioServer) Serve(in io.Reader, out io.Writer) error {
	reader := bufio.NewReaderSize(in, 64*1024)
	var writeMu sync.Mutex
	var wg sync.WaitGroup
	slots := make(chan struct{}, maxConcurrentRequests)
	for {
		line, tooLong, err := readLineBounded(reader)
		if tooLong {
			payload, _ := json.Marshal(rpcFailure(json.RawMessage("null"), -32700,
				fmt.Sprintf("Parse error: request line exceeds %d bytes", maxRequestLineBytes)))
			writeMu.Lock()
			_, _ = out.Write(append(payload, '\n'))
			writeMu.Unlock()
		} else if trimmed := strings.TrimSpace(string(line)); trimmed != "" {
			request := trimmed
			wg.Add(1)
			// 并发背压（评审 M）：槽满时主循环暂停读入，在途请求收敛在
			// maxConcurrentRequests 内；超限行处置不经过槽（拒绝必须即时）。
			slots <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-slots }()
				if response := s.handleLine([]byte(request)); response != nil {
					payload, marshalErr := json.Marshal(response)
					if marshalErr != nil {
						payload, _ = json.Marshal(map[string]any{
							"jsonrpc": "2.0", "id": nil,
							"error": map[string]any{"code": -32603, "message": marshalErr.Error()},
						})
					}
					writeMu.Lock()
					_, _ = out.Write(append(payload, '\n'))
					writeMu.Unlock()
				}
			}()
		}
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
	}
	drained := make(chan struct{})
	go func() {
		wg.Wait()
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(300 * time.Second):
	}
	return nil
}

// readLineBounded 有界读一行（评审 M：原 ReadString 先整行缓冲再判超限——
// 超限行在拒绝前已全额进内存，防的是「无界处理」不是「无界缓冲」）。增量
// 累积到 maxRequestLineBytes+1 即判超限（内容立即丢弃），继续读至换行/EOF
// 后返回 tooLong——拒绝报文与后续行的服务都不受影响。
func readLineBounded(reader *bufio.Reader) (line []byte, tooLong bool, err error) {
	var buf []byte
	for {
		chunk, ferr := reader.ReadSlice('\n')
		switch {
		case tooLong:
			// 已判超限：仅消费到行尾，内容不再保留。
			switch ferr {
			case nil, io.EOF:
				return nil, true, nil
			case bufio.ErrBufferFull:
				continue
			default:
				return nil, true, ferr
			}
		case len(buf)+len(chunk) > maxRequestLineBytes+1:
			// 触发超限：换行/EOF 已随本 chunk 消费则到此为止（不得继续读，
			// 否则会把下一行合法请求当超限尾部吞掉）；chunk 未含换行才继续
			// 丢弃余下部分。
			switch ferr {
			case nil, io.EOF:
				return nil, true, nil
			case bufio.ErrBufferFull:
				tooLong = true
				buf = nil
				continue
			default:
				return nil, true, ferr
			}
		default:
			buf = append(buf, chunk...)
			switch ferr {
			case nil:
				return bytes.TrimRight(buf, "\r\n"), false, nil
			case io.EOF:
				// EOF 结尾的最后一行：行内容照常返回，err 保留 io.EOF 交给
				// Serve 结束读循环（吞掉 EOF 会让 Serve 在输入耗尽后无限自旋）。
				return bytes.TrimRight(buf, "\r\n"), false, io.EOF
			case bufio.ErrBufferFull:
				continue
			default:
				return nil, false, ferr
			}
		}
	}
}

// handleLine 处理一行 JSON-RPC：返回要写回的响应；通知类（notifications/*，
// 含未知通知名与缺 id 通知）返回 nil 不回包。请求形状按 JSON-RPC 分档
// （MCP_ACCEPTANCE §2）：解析失败 -32700、非法请求（缺 id / method 缺失或
// 非字符串 / id 为 object/array / jsonrpc 版本非 2.0）-32600——全部结构化
// 报错，进程不崩（ldap 同构）。纯分派、无 I/O，单测直接喂行。
func (s *StdioServer) handleLine(line []byte) map[string]any {
	var raw struct {
		JSONRPC json.RawMessage `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Method  json.RawMessage `json:"method"`
		Params  json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(line, &raw); err != nil {
		return rpcFailure(json.RawMessage("null"), -32700, fmt.Sprintf("Parse error: %v", err))
	}
	method, methodErr := decodeMethod(raw.Method)
	if methodErr == nil && strings.HasPrefix(method, "notifications/") {
		// notifications/initialized 等通知不回包（MCP 规约），未知通知名容忍。
		return nil
	}
	id := json.RawMessage("null")
	switch {
	case len(raw.ID) == 0:
		// 无 id 且非通知：invalid request（ssh 基线同款——无法关联响应的
		// 请求回 null id 报 -32600，绝不静默当 null-id 请求应答）。
		return rpcFailure(json.RawMessage("null"), -32600, "Invalid request: request is missing an id")
	case !validRequestID(raw.ID):
		return rpcFailure(json.RawMessage("null"), -32600,
			"Invalid request: id must be a string, number, or null")
	default:
		id = raw.ID
	}
	if methodErr != nil {
		return rpcFailure(id, -32600, "Invalid request: "+methodErr.Error())
	}
	// jsonrpc 版本：存在且非 "2.0" → invalid request；字段缺失容忍（ssh
	// 基线同款宽松，避免误伤极简客户端）。
	if len(raw.JSONRPC) > 0 {
		var version string
		if err := json.Unmarshal(raw.JSONRPC, &version); err != nil || version != "2.0" {
			return rpcFailure(id, -32600, fmt.Sprintf("Invalid request: jsonrpc must be \"2.0\" (got %s)", raw.JSONRPC))
		}
	}
	switch method {
	case "initialize":
		return rpcResult(id, map[string]any{
			"protocolVersion": MCPProtocolVersion,
			"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
			"serverInfo":      map[string]any{"name": "io.dbx.kafka", "version": s.version},
		})
	case "ping":
		return rpcResult(id, map[string]any{})
	case "tools/list":
		return rpcResult(id, s.stdioToolList())
	case "tools/call":
		var body struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		}
		if err := json.Unmarshal(raw.Params, &body); err != nil {
			return rpcFailure(id, -32602, fmt.Sprintf("Invalid params: %v", err))
		}
		return rpcResult(id, s.callTool(strings.TrimSpace(body.Name), body.Arguments))
	default:
		return rpcFailure(id, -32601, fmt.Sprintf("Method not found: %s", method))
	}
}

// decodeMethod 解析 method 字段：缺失/空串/非字符串都算无效请求（-32600
// 档，而非 -32700 解析档——JSON 合法但请求形状不合法）。
func decodeMethod(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "", errors.New("method is required")
	}
	var method string
	if err := json.Unmarshal(raw, &method); err != nil {
		return "", fmt.Errorf("method must be a string (got %s)", raw)
	}
	if strings.TrimSpace(method) == "" {
		return "", errors.New("method is required")
	}
	return method, nil
}

// validRequestID JSON-RPC 2.0 id 合法形状：string/number/null。object/array/
// true/false 非法（回包 id 置 null）；字面量 null 是合法的显式 null-id 请求。
func validRequestID(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] == '{' || trimmed[0] == '[' {
		return false
	}
	if bytes.Equal(trimmed, []byte("true")) || bytes.Equal(trimmed, []byte("false")) {
		return false
	}
	var value any
	return json.Unmarshal(trimmed, &value) == nil
}

func rpcResult(id json.RawMessage, result map[string]any) map[string]any {
	return map[string]any{"jsonrpc": "2.0", "id": id, "result": result}
}

func rpcFailure(id json.RawMessage, code int, message string) map[string]any {
	return map[string]any{
		"jsonrpc": "2.0", "id": id,
		"error": map[string]any{"code": code, "message": message},
	}
}

// stdioToolList tools/list：复用注册表全量清单（工具名/描述/语义与
// mcp/tools 完全一致），并为连接类工具补充内联连接参数声明——严格校验的
// MCP 宿主会丢弃未声明参数（ssh 同因在 inputSchema 显式声明），同时把
// required 的 connectionId 放宽为 anyOf 二选一（connectionId 或内联凭据）。
// UI 类工具保持原 schema（stdio 下调用即 UNAVAILABLE）。
func (s *StdioServer) stdioToolList() map[string]any {
	list := s.srv.Tools("")
	tools, ok := list["tools"].([]map[string]any)
	if !ok {
		return list
	}
	for _, tool := range tools {
		name, _ := tool["name"].(string)
		if _, bound := connectionBoundTools[name]; !bound {
			continue
		}
		schema, ok := tool["inputSchema"].(map[string]any)
		if !ok {
			continue
		}
		properties, ok := schema["properties"].(map[string]any)
		if !ok {
			continue
		}
		for key, value := range stdioConnectionProperties() {
			properties[key] = value
		}
		// required 来自注册表，形状是 []string（toolEntry）；这里宽容处理
		// 两种切片形状，剔除 connectionId 后保留其余必填项。
		switch required := schema["required"].(type) {
		case []string:
			relaxed := make([]any, 0, len(required))
			for _, entry := range required {
				if entry != "connectionId" {
					relaxed = append(relaxed, entry)
				}
			}
			schema["required"] = relaxed
		case []any:
			relaxed := make([]any, 0, len(required))
			for _, entry := range required {
				if entry == "connectionId" {
					continue
				}
				relaxed = append(relaxed, entry)
			}
			schema["required"] = relaxed
		}
		schema["anyOf"] = []any{
			map[string]any{"required": []any{"connectionId"}},
			map[string]any{"required": []any{inlineRequiredKey}},
		}
	}
	return list
}

// stdioConnectionProperties 内联连接参数声明（camelCase，与 kafka 连接表单
// 字段对齐；随每次调用传入，不落盘不持久化，按参数 hash 池化）。
func stdioConnectionProperties() map[string]any {
	return map[string]any{
		"connectionId": map[string]any{
			"type":        "string",
			"description": "Connection id: either pooled from an earlier inline-credential call in this stdio session (mcp-…), or a DBX saved connection id (forwarded through the running DBX app's local bridge). Omit to connect by inline parameters instead; in DBX workbench/bridge mode credentials are resolved by the host and never travel in tool arguments",
		},
		"brokers":                map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "Bootstrap broker list (also accepts a single comma/newline-separated string), e.g. [\"broker1:9092\"]; inline mode is keyed by a hash of these parameters"},
		"securityProtocol":       map[string]any{"type": "string", "enum": []string{"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}, "description": "Security protocol (default PLAINTEXT)"},
		"saslMechanism":          map[string]any{"type": "string", "enum": []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}, "description": "SASL mechanism (required for SASL_* protocols; GSSAPI/OAUTHBEARER need the workbench form)"},
		"saslUsername":           map[string]any{"type": "string", "description": "SASL username"},
		"saslPassword":           map[string]any{"type": "string", "description": "SASL password (inline credentials stay in process memory only)"},
		"tlsCaCert":              map[string]any{"type": "string", "description": "PEM CA certificate(s) (SSL protocols; empty = system roots)"},
		"tlsClientCert":          map[string]any{"type": "string", "description": "PEM client certificate for mTLS"},
		"tlsClientKey":           map[string]any{"type": "string", "description": "PEM client key for mTLS"},
		"tlsInsecureSkipVerify":  map[string]any{"type": "boolean", "description": "Skip TLS certificate verification"},
		"schemaRegistry":         map[string]any{"type": "string", "enum": []string{"none", "confluent"}, "description": "Schema Registry backend (default none; aws_glue needs the workbench form)"},
		"schemaRegistryUrl":      map[string]any{"type": "string", "description": "Confluent-compatible Schema Registry URL (schemaRegistry=confluent)"},
		"schemaRegistryUsername": map[string]any{"type": "string", "description": "Schema Registry basic-auth username"},
		"schemaRegistryPassword": map[string]any{"type": "string", "description": "Schema Registry basic-auth password"},
		"clientId":               map[string]any{"type": "string", "description": "Kafka client.id (default dbx-kafka-plugin)"},
		"readOnly":               map[string]any{"type": "boolean", "description": "Open read-only (default true, matching the connection form; write tools refused). Accepts boolean or the strings \"true\"/\"false\"/\"1\"/\"0\"/\"yes\"/\"no\"/\"on\"/\"off\""},
		"allowDelete":            map[string]any{"type": "boolean", "description": "Allow delete-class operations (default false; delete tools refused without it). Accepts boolean or the strings \"true\"/\"false\"/\"1\"/\"0\"/\"yes\"/\"no\"/\"on\"/\"off\""},
		"timeoutSecs":            map[string]any{"type": "number", "description": "Bridge-forward timeout in seconds (5-300, default 300); applies when connectionId is forwarded to the running DBX app's local bridge"},
	}
}

// callTool tools/call 分派（stdio 入口）：
//  1. UI 类工具 → UNAVAILABLE（isError content，明确不假死）；
//  2. 连接类工具 → 连接解析门：内联凭据（池化注册）或已池化 connectionId
//     走 Server.Call 既有分派（digest/cursor/两阶段写语义完全复用）；
//     未池化 connectionId 经 DBX 桥转发（appbridge.go，桥不可达 fail-closed）；
//  3. 其余（cursor_next 等会话类工具）→ 直接 Server.Call。
//
// 工具执行错误按 MCP 规约以 isError:true content 返回（非协议级错误）。
func (s *StdioServer) callTool(name string, args map[string]any) map[string]any {
	if args == nil {
		args = map[string]any{}
	}
	if strings.HasPrefix(name, uiToolPrefix) {
		return unavailableResult(name)
	}
	if _, bound := connectionBoundTools[name]; bound {
		forwarded, handled, err := s.resolveConnectionOrForward(name, args)
		if handled {
			return forwarded
		}
		if err != nil {
			return toolErrorResult(err.Error())
		}
	}
	result, err := s.srv.Call(name, args)
	if err != nil {
		return toolErrorResult(err.Error())
	}
	payload, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		return toolErrorResult(marshalErr.Error())
	}
	return map[string]any{
		"content": []map[string]any{{"type": "text", "text": string(payload)}},
		"isError": false,
	}
}

// unavailableResult UI 类工具在 stdio 模式的固定响应（设计 §5 stdio 行：
// 明确 UNAVAILABLE，不假死）。
func unavailableResult(name string) map[string]any {
	return map[string]any{
		"content": []map[string]any{{"type": "text", "text": "UNAVAILABLE: 此工具需要 DBX 工作台（工作台模式可用，请经 DBX MCP 桥调用）。独立 stdio 模式请改用 kafka_messages_digest / kafka_cursor_next 与写族工具。/ This tool requires the DBX workbench and is unavailable in standalone stdio mode (" + name + ")."}},
		"isError": true,
	}
}

func toolErrorResult(message string) map[string]any {
	return map[string]any{
		"content": []map[string]any{{"type": "text", "text": message}},
		"isError": true,
	}
}

// resolveConnectionOrForward 连接类工具的连接解析门：
//   - 内联凭据在场 → 按参数 hash 池化注册，注入池化 connectionId（handled=false）；
//   - 仅 connectionId 且已池化 → 本地路径直通（handled=false）；
//   - 仅 connectionId 且未池化 → DBX 桥转发（handled=true 时 forwarded 即
//     应用侧 MCP envelope；桥不可达返回 fail-closed 合并错误）；
//   - 两者皆无 → 报缺哪些字段。
func (s *StdioServer) resolveConnectionOrForward(name string, args map[string]any) (forwarded map[string]any, handled bool, err error) {
	inline, present, err := parseInlineConn(args)
	if err != nil {
		return nil, false, err
	}
	if present {
		id, poolErr := s.pooledConnectionID(inline)
		if poolErr != nil {
			return nil, false, poolErr
		}
		args["connectionId"] = id
		return nil, false, nil
	}
	if id := strings.TrimSpace(stringField(args, "connectionId")); id != "" {
		if s.poolHas(id) {
			return nil, false, nil
		}
		result, forwardErr := s.forwardViaBridge(id, name, args)
		if forwardErr == nil {
			return result, true, nil
		}
		return nil, false, fmt.Errorf(
			"unknown connectionId %q: not pooled in this stdio session and the bridge fallback failed (%v). Re-send the inline connection parameters (brokers, securityProtocol, sasl*, schemaRegistry*, …) or start the DBX app so its saved connections can be used",
			id, forwardErr)
	}
	return nil, false, fmt.Errorf("connection parameters required: pass connectionId (pooled earlier in this stdio session, or a DBX saved connection id when the app is running) or inline parameters — brokers (required), plus securityProtocol, saslMechanism, saslUsername, saslPassword, tlsCaCert, tlsClientCert, tlsClientKey, tlsInsecureSkipVerify, schemaRegistry, schemaRegistryUrl, schemaRegistryUsername, schemaRegistryPassword, clientId, readOnly, allowDelete")
}

// forwardViaBridge 把一次未池化 connectionId 的连接类工具调用转发给运行中
// DBX 应用的本地桥（ssh bridge_forward_plan / ldap forwardViaBridge 同构；
// kafka 工具参数只有显式 connectionId 一条转发路，无额外选择器）。
// ensure 失败 / HTTP 非 200 / 非法 JSON 都原样上抛——由调用方并成带内联
// 凭据出路的 fail-closed 引导错误。转发的超时跟随 arguments.timeoutSecs
// （clamp 5–300，缺省 300，照 ssh forward_tool_via_bridge）。
func (s *StdioServer) forwardViaBridge(connectionID, name string, args map[string]any) (map[string]any, error) {
	port, err := ensureBridge(s.bridgeEnsureWait)
	if err != nil {
		return nil, err
	}
	timeout := 300 * time.Second
	if secs := intArg(args["timeoutSecs"]); secs > 0 {
		if secs < 5 {
			secs = 5
		}
		if secs > 300 {
			secs = 300
		}
		timeout = time.Duration(secs) * time.Second
	}
	result, err := callPluginTool(port, connectionID, name, args, timeout)
	if err != nil {
		return nil, err
	}
	// 应用侧返回的是宿主 mcp/call 的 MCP content envelope（{content,
	// isError}），逐字透传——再包一层会把应用侧答案埋深一层 JSON。
	if _, hasContent := result["content"]; hasContent {
		if _, hasIsError := result["isError"]; hasIsError {
			return result, nil
		}
	}
	// 非 envelope 形状（防御性）：按成功 content 包装原始 JSON。
	payload, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		return nil, fmt.Errorf("DBX app bridge returned unencodable payload: %v", marshalErr)
	}
	return map[string]any{
		"content": []map[string]any{{"type": "text", "text": string(payload)}},
		"isError": false,
	}, nil
}

func (s *StdioServer) poolHas(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.ids[id]
	return ok
}

// pooledConnectionID 按参数 hash 池化：命中直接复用（底层 service 惰性
// 建连 + 指纹失效重建语义保持）；未命中经 svc.Connect 注册（幂等、惰性
// 拨号，与工作台同一套底层驱动），池满淘汰最旧并断开。
func (s *StdioServer) pooledConnectionID(inline inlineConn) (string, error) {
	id := inline.poolKey()
	s.mu.Lock()
	if _, ok := s.hash[id]; ok {
		s.mu.Unlock()
		return id, nil
	}
	var evicted []string
	for len(s.hash) >= inlinePoolCap {
		oldest := s.order[0]
		s.order = s.order[1:]
		if staleID, ok := s.hash[oldest]; ok {
			delete(s.hash, oldest)
			delete(s.ids, staleID)
			evicted = append(evicted, staleID)
		}
	}
	s.hash[id] = id
	s.ids[id] = struct{}{}
	s.order = append(s.order, id)
	s.mu.Unlock()
	for _, stale := range evicted {
		s.svc.Disconnect(stale)
	}
	params := inline.toLifecycle(id)
	if err := s.svc.Connect(params); err != nil {
		// 注册失败不残留池项（下次同参数调用重试）。
		s.mu.Lock()
		delete(s.hash, id)
		delete(s.ids, id)
		s.mu.Unlock()
		return "", err
	}
	return id, nil
}

// --- 内联连接参数（camelCase，与 kafka 连接表单字段对齐） ---

// inlineConn 内联凭据的归一化形状：poolKey 的 canonical 序列化输入
// （结构体字段序固定 → hash 稳定）。凭据（saslPassword/tlsClientKey/
// schemaRegistryPassword）只进 hash 输入与 svc.Connect 的内存连接表，
// 不落盘不进日志/审计。
//
// readOnly/allowDelete 按连接表单默认解析（缺省 readOnly=true、
// allowDelete=false，显式传值等同缺省值时池键一致）。
type inlineConn struct {
	Brokers                []string `json:"brokers"`
	SecurityProtocol       string   `json:"securityProtocol,omitempty"`
	SASLMechanism          string   `json:"saslMechanism,omitempty"`
	SASLUsername           string   `json:"saslUsername,omitempty"`
	SASLPassword           string   `json:"saslPassword,omitempty"`
	TLSCACert              string   `json:"tlsCaCert,omitempty"`
	TLSClientCert          string   `json:"tlsClientCert,omitempty"`
	TLSClientKey           string   `json:"tlsClientKey,omitempty"`
	TLSInsecureSkipVerify  bool     `json:"tlsInsecureSkipVerify,omitempty"`
	SchemaRegistry         string   `json:"schemaRegistry,omitempty"`
	SchemaRegistryURL      string   `json:"schemaRegistryUrl,omitempty"`
	SchemaRegistryUsername string   `json:"schemaRegistryUsername,omitempty"`
	SchemaRegistryPassword string   `json:"schemaRegistryPassword,omitempty"`
	ClientID               string   `json:"clientId,omitempty"`
	ReadOnly               bool     `json:"readOnly"`
	AllowDelete            bool     `json:"allowDelete"`
}

// parseInlineConn 从工具参数提取内联连接参数。present = brokers 非空。
// readOnly/allowDelete 接受布尔或字符串 "true"/"false"/"1"/"0"（LLM 常见的
// 字符串布尔变体）；其余值报错——静默回落表单默认会把写连接降级成只读
// （或把只读连接升成可写），必须显式失败。
func parseInlineConn(args map[string]any) (inlineConn, bool, error) {
	inline := inlineConn{
		Brokers:                parseBrokers(args["brokers"]),
		SecurityProtocol:       strings.TrimSpace(stringField(args, "securityProtocol")),
		SASLMechanism:          strings.TrimSpace(stringField(args, "saslMechanism")),
		SASLUsername:           strings.TrimSpace(stringField(args, "saslUsername")),
		SASLPassword:           stringField(args, "saslPassword"),
		TLSCACert:              stringField(args, "tlsCaCert"),
		TLSClientCert:          stringField(args, "tlsClientCert"),
		TLSClientKey:           stringField(args, "tlsClientKey"),
		TLSInsecureSkipVerify:  boolArg(args["tlsInsecureSkipVerify"]),
		SchemaRegistry:         strings.TrimSpace(stringField(args, "schemaRegistry")),
		SchemaRegistryURL:      strings.TrimSpace(stringField(args, "schemaRegistryUrl")),
		SchemaRegistryUsername: strings.TrimSpace(stringField(args, "schemaRegistryUsername")),
		SchemaRegistryPassword: stringField(args, "schemaRegistryPassword"),
		ClientID:               strings.TrimSpace(stringField(args, "clientId")),
		// 表单默认：readOnly=true、allowDelete=false（显式同值不影响池键）。
		ReadOnly:    true,
		AllowDelete: false,
	}
	for _, field := range []struct {
		key    string
		target *bool
	}{
		{"readOnly", &inline.ReadOnly},
		{"allowDelete", &inline.AllowDelete},
	} {
		raw, present := args[field.key]
		if !present || raw == nil {
			continue
		}
		value, ok := coerceBool(raw)
		if !ok {
			return inline, false, fmt.Errorf("%s must be true or false (got %v)", field.key, raw)
		}
		*field.target = value
	}
	return inline, len(inline.Brokers) > 0, nil
}

// parseBrokers 解析 bootstrap 列表：JSON 数组或单个字符串（逗号/换行分隔，
// 与连接表单 textarea 语义一致）。
func parseBrokers(raw any) []string {
	var pieces []string
	switch value := raw.(type) {
	case nil:
		return nil
	case []any:
		for _, item := range value {
			pieces = append(pieces, fmt.Sprintf("%v", item))
		}
	case []string:
		pieces = value
	case string:
		pieces = []string{value}
	default:
		pieces = []string{fmt.Sprintf("%v", value)}
	}
	out := []string{}
	for _, piece := range pieces {
		for _, part := range strings.FieldsFunc(piece, func(r rune) bool { return r == ',' || r == '\n' || r == '\r' }) {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				out = append(out, trimmed)
			}
		}
	}
	return out
}

// poolKey 参数 hash → 池化 connectionId（前 16 字节 hex；同参数必同键，
// 键顺序无关——canonical 输入是结构体而非原始 map）。
func (c inlineConn) poolKey() string {
	canonical, err := json.Marshal(c)
	if err != nil {
		// 结构体序列化不失败；防御性兜底仍产出稳定键。
		canonical = []byte(fmt.Sprintf("%+v", c))
	}
	sum := sha256.Sum256(canonical)
	return "mcp-" + hex.EncodeToString(sum[:16])
}

// toLifecycle 把内联参数折算为标准 lifecycle params（与宿主转发的形状
// 同构，直接走 NewProfileFromLifecycle → svc.Connect 同一条路径；kafka
// 无 runtime 隧道，bootstrap 即拨号地址）。
func (c inlineConn) toLifecycle(id string) *lifecycle.Params {
	external := map[string]any{
		"bootstrap_servers": strings.Join(c.Brokers, ","),
		"read_only":         c.ReadOnly,
		"allow_delete":      c.AllowDelete,
	}
	if c.SecurityProtocol != "" {
		external["security_protocol"] = c.SecurityProtocol
	}
	if c.SASLMechanism != "" {
		external["sasl_mechanism"] = c.SASLMechanism
	}
	if c.SASLUsername != "" {
		external["sasl_username"] = c.SASLUsername
	}
	if c.TLSCACert != "" {
		external["tls_ca_cert"] = c.TLSCACert
	}
	if c.TLSClientCert != "" {
		external["tls_client_cert"] = c.TLSClientCert
	}
	if c.TLSInsecureSkipVerify {
		external["tls_insecure_skip_verify"] = true
	}
	if c.SchemaRegistry != "" {
		external["schema_registry"] = c.SchemaRegistry
	}
	if c.SchemaRegistryURL != "" {
		external["sr_url"] = c.SchemaRegistryURL
	}
	if c.SchemaRegistryUsername != "" {
		external["sr_username"] = c.SchemaRegistryUsername
	}
	if c.ClientID != "" {
		external["client_id"] = c.ClientID
	}
	secrets := map[string]any{}
	if c.SASLPassword != "" {
		secrets["sasl_password"] = c.SASLPassword
	}
	if c.TLSClientKey != "" {
		secrets["tls_client_key"] = c.TLSClientKey
	}
	if c.SchemaRegistryPassword != "" {
		secrets["sr_password"] = c.SchemaRegistryPassword
	}
	return &lifecycle.Params{
		Connection: lifecycle.Connection{
			ID:             id,
			Name:           "mcp-stdio-inline",
			ExternalConfig: external,
			Secrets:        secrets,
		},
	}
}
