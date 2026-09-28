package mcp

// appbridge.go：DBX 桌面应用本地 TCP 桥的客户端（设计 shared/
// IMPL_PLAN_PLUGIN_MCP.zh-CN.md §5 stdio 行「凭据内联/桥接兜底，同 ssh
// 模式」；ldap appbridge.go 的 kafka 同构移植，源头是 ssh
// backend/src/app_bridge.rs 的 Go 移植）。
//
// standalone `--mcp` stdio 模式没有内嵌 Emitter，工具收到一个本会话未池化
// 的 `connectionId` 时，经本客户端把调用转发给运行中的 DBX 应用本地桥：
// 应用监听 `127.0.0.1:<port>` 并把端口写在 `<app_data_dir>/mcp-bridge-port`，
// `POST /call-plugin-tool` 把调用中继到应用自己的插件 sidecar（与工作台同一
// 进程），保存的连接因此可用且凭据从不出现在工具参数里。
//
// fail-closed 契约：端口文件缺失/损坏/端口不可达/HTTP 非 200 都立刻返回带
// "DBX app bridge" 前缀的可行动错误（调用方并成一条含内联凭据出路的引导
// 错误），绝不假死、绝不静默按内联凭据重拨。

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// bridgePortFileName 宿主把桥端口写进 app-data 目录的发现文件名（宿主
// mcp_bridge.rs MCP_BRIDGE_PORT_FILE，同 ssh app_bridge.rs）。
const bridgePortFileName = "mcp-bridge-port"

// bridgePluginID 本插件在桥契约里的身份（宿主 /call-plugin-tool 用它把
// connection 解析到正确插件的 sidecar）。
const bridgePluginID = "io.dbx.kafka"

// bridgeDefaultAppDataSubpath DBX_APP_DATA_DIR 未设置时的 macOS 兜底
// app-data 目录（与宿主 resolve 相同，照 ssh DEFAULT_APP_DATA_SUBPATH）。
const bridgeDefaultAppDataSubpath = "Library/Application Support/com.dbx.app"

// 本地 TCP 一跳的预算与读超时余量（宿主可能在工作台里挂审批弹窗，读超时
// 必须 ≥ 转发的 timeout_ms，照 ssh APPROVAL_READ_MARGIN）。
const (
	bridgeConnectTimeout = 3 * time.Second
	bridgeProbeTimeout   = 2 * time.Second
	bridgeReadMargin     = 150 * time.Second
	// bridgeMaxRequestBytes 宿主用单次 64 KiB 读整个请求，请求必须一次写完。
	bridgeMaxRequestBytes = 64 * 1024
	// bridgeEnsurePollInterval 端口文件轮询间隔（launch 后等应用起来）。
	bridgeEnsurePollInterval = 500 * time.Millisecond
)

// DefaultBridgeEnsureWait ensureBridge 在拉起尝试后继续轮询端口文件的预算
// （照 ssh DEFAULT_ENSURE_WAIT = 30s）。
const DefaultBridgeEnsureWait = 30 * time.Second

// bridgeAppDataDir `<app_data_dir>` 解析顺序：env DBX_APP_DATA_DIR（非空），
// 然后 $HOME/Library/Application Support/com.dbx.app。
func bridgeAppDataDir() (string, bool) {
	if dir := strings.TrimSpace(os.Getenv("DBX_APP_DATA_DIR")); dir != "" {
		return dir, true
	}
	home := strings.TrimSpace(os.Getenv("HOME"))
	if home == "" {
		return "", false
	}
	return filepath.Join(home, bridgeDefaultAppDataSubpath), true
}

// bridgePort 读宿主发布的桥端口（十进制、容忍首尾空白）。文件缺失/损坏
// 返回 false——端口文件比进程活得久，绝不猜端口。
func bridgePort(appDataDir string) (int, bool) {
	raw, err := os.ReadFile(filepath.Join(appDataDir, bridgePortFileName))
	if err != nil {
		return 0, false
	}
	port, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || port <= 0 || port > 65535 {
		return 0, false
	}
	return port, true
}

// bridgePortAlive 端口文件存在且 TCP connect 探测证明确有监听者（端口文件
// 比被杀的应用活得久，否则会永远交出一个陈旧端口）。
func bridgePortAlive(appDataDir string) (int, bool) {
	port, ok := bridgePort(appDataDir)
	if !ok {
		return 0, false
	}
	conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), bridgeProbeTimeout)
	if err != nil {
		return 0, false
	}
	_ = conn.Close()
	return port, true
}

// launchAppDBX 尽力拉起 DBX 应用（缺省 macOS `open -a DBX.app`）；失败不
// 致命——下面的端口文件轮询才是事实来源。DBX_APP_LAUNCH_CMD 仅支持 ":"
// 哨兵（跳过拉起，smoke/CI 用来避免 UI 弹出）；不透传任意命令到 exec，
// env 值不构成命令注入面。
// bridgeResponseLimit 桥响应体读取上限（评审 L-2）。
const bridgeResponseLimit = 8 * 1024 * 1024

func launchAppDBX() {
	if strings.TrimSpace(os.Getenv("DBX_APP_LAUNCH_CMD")) == ":" {
		return
	}
	if proc := exec.Command("open", "-a", "DBX.app"); proc != nil {
		if err := proc.Start(); err == nil {
			go func() { _ = proc.Wait() }() // 回收子进程，防僵尸累积（评审 L-3）
		}
	}
}

// ensureBridge 等到可达的桥端口：在场立即验证返回；否则拉起应用后每 500ms
// 重读端口文件 + 重探测（应用重启后新端口也能捡到），直到 wait 用尽。
// 超时错误带所有桥失败统一的 "DBX app bridge" 前缀（调用方与 smoke 靠它
// grep 一个可行动标记）。
func ensureBridge(wait time.Duration) (int, error) {
	appDataDir, ok := bridgeAppDataDir()
	if !ok {
		return 0, fmt.Errorf("DBX app bridge app-data dir unresolved: set DBX_APP_DATA_DIR or HOME; start the DBX app and retry")
	}
	if port, alive := bridgePortAlive(appDataDir); alive {
		return port, nil
	}
	launchAppDBX()
	deadline := time.Now().Add(wait)
	for {
		time.Sleep(bridgeEnsurePollInterval)
		if port, alive := bridgePortAlive(appDataDir); alive {
			return port, nil
		}
		if !time.Now().Before(deadline) {
			return 0, fmt.Errorf("DBX app bridge unreachable after %s: no reachable mcp-bridge-port in %s; start the DBX app and retry", wait, appDataDir)
		}
	}
}

// bridgeRequestBody /call-plugin-tool 的 JSON body（snake_case，宿主桥契约；
// 与 ssh request_body 同构，仅 plugin_id 换本插件）。
func bridgeRequestBody(connectionID, tool string, arguments map[string]any, timeoutMS int64) (map[string]any, error) {
	if arguments == nil {
		arguments = map[string]any{}
	}
	return map[string]any{
		"plugin_id":     bridgePluginID,
		"connection_id": connectionID,
		"tool":          tool,
		"arguments":     arguments,
		"timeout_ms":    timeoutMS,
	}, nil
}

// callPluginTool 经桥转发一次工具调用。调用方先 ensureBridge 拿到端口并
// 通过（ensure 预算由调用方持有，测试/引导场景可缩短）。200 body 是宿主侧
// 插件 `mcp/call` 的 MCP content envelope，逐字返回；任何其他结果（应用没
// 起、旧版应用没这条路由、连接不存在、插件报错）都变成带 "DBX app bridge"
// 前缀的错误。
func callPluginTool(port int, connectionID, tool string, arguments map[string]any, timeout time.Duration) (map[string]any, error) {
	body, err := bridgeRequestBody(connectionID, tool, arguments, timeout.Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("failed to encode the DBX app bridge request: %v", err)
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to encode the DBX app bridge request: %v", err)
	}
	if len(payload) > bridgeMaxRequestBytes {
		return nil, fmt.Errorf("DBX app bridge request is %d bytes, above the %d-byte single-write ceiling", len(payload), bridgeMaxRequestBytes)
	}
	url := fmt.Sprintf("http://127.0.0.1:%d/call-plugin-tool", port)
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("DBX app bridge request build failed: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Connection", "close")
	client := &http.Client{Timeout: timeout + bridgeReadMargin}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("DBX app bridge call to 127.0.0.1:%d failed: %v", port, err)
	}
	defer func() { _ = response.Body.Close() }()
	var raw bytes.Buffer
	// 响应体上限（评审 L-2）：桥/宿主异常时无界读取可拉满 sidecar 内存。
	if _, err := raw.ReadFrom(io.LimitReader(response.Body, bridgeResponseLimit)); err != nil {
		return nil, fmt.Errorf("DBX app bridge read failed: %v", err)
	}
	text := strings.TrimSpace(raw.String())
	if response.StatusCode != http.StatusOK {
		if len(text) > 500 {
			text = text[:500]
		}
		return nil, fmt.Errorf("DBX app bridge returned HTTP %d: %s", response.StatusCode, text)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(text), &decoded); err != nil {
		return nil, fmt.Errorf("DBX app bridge returned invalid JSON: %v", err)
	}
	return decoded, nil
}
