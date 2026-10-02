// Package lifecycle 解析宿主 → sidecar 的连接生命周期 params。
//
// JSON 形状契约见 shared/IMPL_PLAN_M0_COMMON.zh-CN.md §3.1
// （与 ssh-sftp backend/src/model.rs::from_lifecycle_params 同构）：
//
//	{
//	  "provider":  { "id": "io.dbx.kafka.connection", "databaseType": "kafka" },
//	  "connection": {
//	    "id": "uuid", "name": "prod-kafka", "host": "", "port": 0,
//	    "username": "", "password": "",
//	    "external_config":     { "bootstrap_servers": "…", "security_protocol": "…" },  // binding: config
//	    "connection_secrets":  { "sasl_password": "…", "tls_client_key": "…" }          // binding: secret
//	  },
//	  "runtime": { "host": "127.0.0.1", "port": 9092 },   // DBX 传输层拨号端点（bootstrap 空时兜底）
//	  "operationId": "…"                                  // Host 1.1 才有，缺失本地 uuid 兜底
//	}
//
// binding 落点：config → external_config.<field_key>（snake_case）；
// secret → connection_secrets.<field_key>。凭据只允许经 SecretString 读取，
// 禁止进入日志/审计/事件（M0 文档 §4 红线）。
//
// 本包照抄 ldap/internal/lifecycle（kafka 无标准 host/port 字段，连接参数
// 全走 external_config 的 bootstrap_servers textarea；其余解析逻辑一致）。
package lifecycle

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// Params 是 connection/test|connect|disconnect 的 params 顶层结构。
type Params struct {
	Provider    Provider   `json:"provider,omitempty"`
	Connection  Connection `json:"connection"`
	Runtime     Runtime    `json:"runtime,omitempty"`
	OperationID string     `json:"operationId,omitempty"`
}

// Provider 标识贡献该连接的 connection-provider。
type Provider struct {
	ID           string `json:"id,omitempty"`
	DatabaseType string `json:"databaseType,omitempty"`
}

// Connection 是宿主连接对象（标准字段 + config/secret 两个绑定落点）。
type Connection struct {
	ID       string `json:"id,omitempty"`
	Name     string `json:"name,omitempty"`
	Host     string `json:"host,omitempty"`
	Port     int    `json:"port,omitempty"`
	Username string `json:"username,omitempty"`
	// Password 是 binding:"password" 的标准密码字段（manifest 中无 password
	// binding 字段时恒为空；凭据红线：不得写入日志/审计/事件）。
	Password string `json:"password,omitempty"`
	// ReadOnly 是宿主 ConnectionConfig.read_only（连接级通用只读设置）。
	ReadOnly bool `json:"read_only,omitempty"`
	// ExternalConfig 存 binding:"config" 字段，key = manifest field key
	// （snake_case，如 bootstrap_servers / security_protocol）。
	ExternalConfig map[string]any `json:"external_config,omitempty"`
	// Secrets 存 binding:"secret" 字段，由宿主加密存储、传输时下发明文。
	Secrets map[string]any `json:"connection_secrets,omitempty"`
}

// Runtime 是 DBX 传输层拨号端点。Host 1.0 只保证 host/port：它们是
// transport layers 建立后的最终本地端点，插件不得自行重建隧道。较新的
// Host 可额外提供 proxy route，用于需要对 Kafka advertised.listeners 的
// 每个 broker 都执行 SOCKS5 拨号的多 broker 连接；proxy 是可选扩展，旧
// Host/无代理请求仍只包含 host/port。
type Runtime struct {
	Host  string        `json:"host,omitempty"`
	Port  int           `json:"port,omitempty"`
	Proxy *RuntimeProxy `json:"proxy,omitempty"`
}

// RuntimeProxy describes a host-managed proxy route. Credentials may be
// hydrated by the host for this lifecycle request and must never be logged or
// returned by the plugin. Type is intentionally a string for forward
// compatibility; Kafka currently accepts socks5/socks5h only.
type RuntimeProxy struct {
	Type     string `json:"type,omitempty"`
	Kind     string `json:"kind,omitempty"`
	Host     string `json:"host,omitempty"`
	Port     int    `json:"port,omitempty"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

// Parse 解析 lifecycle params。宿主不同版本可能省略 provider/runtime，
// 仅 connection/disconnect 可能只带 {connection:{id}}，故各段均可缺省。
func Parse(raw json.RawMessage) (*Params, error) {
	var params Params
	if len(raw) == 0 {
		return &params, nil
	}
	if err := json.Unmarshal(raw, &params); err != nil {
		return nil, fmt.Errorf("parse lifecycle params: %w", err)
	}
	return &params, nil
}

// ConnectionID 返回 connection.id（去空白）。
func (p *Params) ConnectionID() string {
	return strings.TrimSpace(p.Connection.ID)
}

// OperationIDOrUUID 返回 operationId；宿主未携带（Host API 1.0）时用 uuid 兜底。
func (p *Params) OperationIDOrUUID() string {
	if id := strings.TrimSpace(p.OperationID); id != "" {
		return id
	}
	return uuid.NewString()
}

// ConfigString 取 external_config 中的字符串值（兼容 number/bool 的字符串化）。
func (p *Params) ConfigString(key string) string {
	return anyToString(p.Connection.ExternalConfig[key])
}

// ConfigBool 取 external_config 中的布尔值：标准 Go 形态（1/t/T/TRUE…）
// 之外，宽容接受 coerceBool 同族的 yes/on / no/off（大小写不敏感，与
// mcp/stdio 的布尔宽容口径一致）——KAFKA-LC-L1（第二轮审查）：此前
// "read_only": "yes" 静默按 false 处理，只读门禁 fail-open；门禁类解析
// 失败方向必须保守。
func (p *Params) ConfigBool(key string) bool {
	switch value := p.Connection.ExternalConfig[key].(type) {
	case bool:
		return value
	case string:
		trimmed := strings.TrimSpace(value)
		if parsed, err := strconv.ParseBool(trimmed); err == nil {
			return parsed
		}
		switch strings.ToLower(trimmed) {
		case "yes", "on":
			return true
		case "no", "off":
			return false
		}
		return false
	default:
		return false
	}
}

// ConfigInt 取 external_config 中的整数值。
func (p *Params) ConfigInt(key string) int {
	switch value := p.Connection.ExternalConfig[key].(type) {
	case float64:
		return int(value)
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return 0
		}
		return parsed
	case json.Number:
		parsed, err := strconv.Atoi(value.String())
		if err != nil {
			return 0
		}
		return parsed
	default:
		return 0
	}
}

// ConfigStringSlice 取列表值；textarea 多行字符串按行拆分（逗号分隔的
// bootstrap 列表同样支持：每行剥首尾逗号后按逗号再拆），空项丢弃。
func (p *Params) ConfigStringSlice(key string) []string {
	switch value := p.Connection.ExternalConfig[key].(type) {
	case []any:
		out := make([]string, 0, len(value))
		for _, item := range value {
			if s := anyToString(item); s != "" {
				out = append(out, s)
			}
		}
		return out
	case []string:
		return value
	case string:
		return splitBootstrapLines(value)
	default:
		return nil
	}
}

// SecretString 取 connection_secrets 中的字符串值。
func (p *Params) SecretString(key string) string {
	return anyToString(p.Connection.Secrets[key])
}

func anyToString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(typed)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case json.Number:
		return typed.String()
	case bool:
		return strconv.FormatBool(typed)
	default:
		return ""
	}
}

// splitBootstrapLines 拆分 bootstrap 列表：支持换行 + 逗号两种分隔
// （manifest bootstrap_servers 为 textarea，用户可能输入逗号或换行分隔）。
func splitBootstrapLines(value string) []string {
	out := []string{}
	for _, line := range strings.Split(value, "\n") {
		for _, part := range strings.Split(line, ",") {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				out = append(out, trimmed)
			}
		}
	}
	return out
}
