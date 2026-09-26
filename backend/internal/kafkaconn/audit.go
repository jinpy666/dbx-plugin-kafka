package kafkaconn

// audit.go：写操作审计（IMPL_PLAN §5.4 kafka/audit 事件 + §6 审计基线）。
// 与 ldap 同构：领域层只产出 AuditRecord，落盘与事件由 main 注入回调完成；
// Result 语义 success | blocked | error（main 层折算 audit.jsonl 的
// ok|denied|error）。族内三值契约：不引入第四种 result——insecure-TLS 之类
// 的警示语义走 success + Detail 留痕（见 Service.Connect 的 skip-verify 审计）。
// 红线：Target 只含资源名（topic/group/acl 描述），Detail 不含消息值/凭据。

// AuditRecord 写操作审计记录（§5.4 kafka/audit 事件 + audit.jsonl 同条落盘）。
type AuditRecord struct {
	ConnectionID string `json:"connectionId"`
	Action       string `json:"action"` // produce | topics-create | topics-delete | ... （main 层统一加 kafka/ 前缀）
	Target       string `json:"target"`
	Result       string `json:"result"` // success | blocked | error
	Detail       string `json:"detail,omitempty"`
	// Source 操作来源标注（MCP 设计 §4）：MCP 写路径记 "mcp"；工作台路径
	// 不携带（旧记录/事件无此字段，additive 兼容）。
	Source string `json:"source,omitempty"`
}
