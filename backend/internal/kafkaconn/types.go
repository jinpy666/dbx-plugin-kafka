// Package kafkaconn 是 dbx-kafka-plugin 的连接与领域层，逻辑参照
// tiny-rdm backend/services/kafka_service.go / kafka_stream_service.go 重写
// （迁移映射见 docs/IMPL_PLAN_DBX_KAFKA.zh-CN.md §1/§5）。
//
// types.go：请求/响应/消息类型。相对 tiny-rdm types/kafka.go 的改造：
//   - ProfileID → ConnectionID（宿主连接模型，§5 契约）；
//   - Profile 收敛为 manifest §4 字段（bootstrap/security_protocol/sasl/tls/
//     read_only/allow_delete），凭据（sasl_password/tls_client_key）与 Profile
//     分离，只存内存 connSecrets；
//   - 删除 SchemaRegistry / Connector / Kerberos / Transport 等宿主替代或
//     Phase 2 延后字段；
//   - 消息形状按 §5.3 二进制保真：valueText（UTF-8 安全预览）恒出 +
//     valueBase64（完整）恒出，key 为非法 UTF-8 时以 keyBase64 透出；
//   - JSON tag 全部 camelCase。
package kafkaconn

import (
	"fmt"
	"strings"
)

// 安全协议与 SASL 机制取值面（manifest §4；GSSAPI=Kerberos 为 Phase 2 新增）。
const (
	SecurityProtocolPlaintext     = "PLAINTEXT"
	SecurityProtocolSSL           = "SSL"
	SecurityProtocolSASLPlaintext = "SASL_PLAINTEXT"
	SecurityProtocolSASLSSL       = "SASL_SSL"

	SASLMechanismPlain       = "PLAIN"
	SASLMechanismSCRAMSHA256 = "SCRAM-SHA-256"
	SASLMechanismSCRAMSHA512 = "SCRAM-SHA-512"
	SASLMechanismGSSAPI      = "GSSAPI"
	// SASLMechanismOAUTHBEARER（Phase 3，IMPL_PLAN §12.2.3）：机制层用
	// franz-go pkg/sasl/oauth；token 来源见 OauthTokenSource 取值面。
	// 仅允许 SASL_SSL（validateRequiredCombination 约束）。
	SASLMechanismOAUTHBEARER = "OAUTHBEARER"

	// ConnectionSource 取值面（connection_source 字段）。
	ConnectionSourceBootstrap = "bootstrap"
	ConnectionSourceZookeeper = "zookeeper"

	// SchemaRegistry 取值面（schema_registry 决策字段；空 = 旧版本连接，
	// 运行时按 sr_url/glue_region 自动探测回退，见 NormalizeSchemaRegistry）。
	SchemaRegistryNone      = "none"
	SchemaRegistryConfluent = "confluent"
	SchemaRegistryAWSGlue   = "aws_glue"

	// GlueAuthMode 取值面（glue_auth_mode 字段；tinyrdm 的 aws-profile 模式
	// sidecar 不提供）。
	GlueAuthModeDefault = "default"
	GlueAuthModeStatic  = "static"

	// OauthTokenSource 取值面（oauth_token_source 字段，Phase 3 OAUTHBEARER）：
	// msk_iam = AWS MSK IAM 签名 token（signer.GenerateAuthToken，凭据默认链
	// 或显式 AK/SK 覆盖，照 §11.5 Glue auth_mode=default 范式）；
	// static_token = 静态 token 直供（oauth_static_token secret）。
	OauthTokenSourceMSKIAM = "msk_iam"
	OauthTokenSourceStatic = "static_token"
)

// NormalizeSchemaRegistry 归一化 schema_registry 开关；空保留为空（旧连接
// 向后兼容：按 sr_url/glue_region 自动探测），未知值保守回退 none。
func NormalizeSchemaRegistry(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case SchemaRegistryConfluent:
		return SchemaRegistryConfluent
	case SchemaRegistryAWSGlue:
		return SchemaRegistryAWSGlue
	case SchemaRegistryNone:
		return SchemaRegistryNone
	case "":
		return ""
	default:
		return SchemaRegistryNone
	}
}

// NormalizeConnectionSource 归一化连接来源；空值/未知回退 bootstrap。
func NormalizeConnectionSource(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case ConnectionSourceZookeeper:
		return ConnectionSourceZookeeper
	default:
		return ConnectionSourceBootstrap
	}
}

// NormalizeSecurityProtocol 归一化安全协议；空值/未知回退 PLAINTEXT。
func NormalizeSecurityProtocol(value string) string {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "":
		return SecurityProtocolPlaintext
	case SecurityProtocolSSL, SecurityProtocolSASLPlaintext, SecurityProtocolSASLSSL:
		return strings.ToUpper(strings.TrimSpace(value))
	default:
		return SecurityProtocolPlaintext
	}
}

// NormalizeSASLMechanism 归一化 SASL 机制；未知返回空串。
func NormalizeSASLMechanism(value string) string {
	switch strings.TrimSpace(value) {
	case SASLMechanismPlain:
		return SASLMechanismPlain
	case SASLMechanismSCRAMSHA256:
		return SASLMechanismSCRAMSHA256
	case SASLMechanismSCRAMSHA512:
		return SASLMechanismSCRAMSHA512
	case SASLMechanismGSSAPI:
		return SASLMechanismGSSAPI
	case SASLMechanismOAUTHBEARER:
		return SASLMechanismOAUTHBEARER
	default:
		return ""
	}
}

// NormalizeOauthTokenSource 归一化 oauth_token_source；空值回退 msk_iam
// （主用 MSK IAM；仅显式选择 static_token 才走静态 token），未知值返回空串
// （validateRequiredCombination 报参数错）。
func NormalizeOauthTokenSource(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return OauthTokenSourceMSKIAM
	case OauthTokenSourceMSKIAM:
		return OauthTokenSourceMSKIAM
	case OauthTokenSourceStatic:
		return OauthTokenSourceStatic
	default:
		return ""
	}
}

// Profile 是 sidecar 内存的连接配置（由 lifecycle 从宿主 params 构造）。
// 凭据字段（SASL 密码、客户端私钥、SR 密码）不在此结构，见 connSecrets。
type Profile struct {
	ID               string   `json:"id"`
	Name             string   `json:"name"`
	BootstrapServers []string `json:"bootstrapServers"`
	// SecurityProtocol：PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL（§4 默认 PLAINTEXT）。
	SecurityProtocol string `json:"securityProtocol"`
	// SASLMechanism：PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512 | GSSAPI |
	// OAUTHBEARER（含 SASL 时必填）。
	SASLMechanism         string `json:"saslMechanism,omitempty"`
	Username              string `json:"username,omitempty"`
	TLSCACert             string `json:"tlsCaCert,omitempty"`
	TLSClientCert         string `json:"tlsClientCert,omitempty"`
	TLSInsecureSkipVerify bool   `json:"tlsInsecureSkipVerify,omitempty"`
	ClientID              string `json:"clientId,omitempty"`
	// ReadOnly 收敛门禁：连接表单 read_only ∥ 宿主标准 read_only。
	ReadOnly bool `json:"readOnly"`
	// AllowDelete 允许删除类操作；read_only 下强制无效（§6 与门）。
	AllowDelete bool `json:"allowDelete,omitempty"`

	// --- Phase 2（IMPL_PLAN §0.2）：连接来源 / ZK 发现 / Kerberos / SR ---

	// ConnectionSource：bootstrap（默认）| zookeeper（经 ZK 发现 broker）。
	ConnectionSource string `json:"connectionSource,omitempty"`
	// ZKServers：host:port[/chroot] 逗号分隔（仅 zookeeper 源使用）。
	ZKServers []string `json:"zkServers,omitempty"`
	// Kerberos 配置（GSSAPI 机制；keytab 只收文件路径不收内容）。
	KerberosServiceName  string `json:"kerberosServiceName,omitempty"`
	KerberosRealm        string `json:"kerberosRealm,omitempty"`
	KerberosPrincipal    string `json:"kerberosPrincipal,omitempty"`
	KerberosKeytabPath   string `json:"kerberosKeytabPath,omitempty"`
	KerberosKrb5ConfPath string `json:"kerberosKrb5ConfPath,omitempty"`
	// Schema Registry（Confluent 兼容 REST；空 URL = SR 能力禁用）。
	// SchemaRegistry 是 manifest schema_registry 决策开关（none | confluent |
	// aws_glue；空 = 旧连接按 sr_url/glue_region 自动探测回退）。SR provider
	// 解析以本开关为准（resolveSchemaProvider），sr_url/glue_* 仅是该后端的
	// 连接参数。
	SchemaRegistry string `json:"schemaRegistry,omitempty"`
	SRURL          string `json:"srUrl,omitempty"`
	SRUsername     string `json:"srUsername,omitempty"`
	// AWS Glue Schema Registry（Phase 3）：region+registryName 齐备 = 启用。
	// 凭据（secret key/token）不在此结构，见 connSecrets；auth_mode 取值
	// default | static（tinyrdm 的 aws-profile 模式 sidecar 不提供）。
	GlueRegion       string `json:"glueRegion,omitempty"`
	GlueRegistryName string `json:"glueRegistryName,omitempty"`
	GlueAuthMode     string `json:"glueAuthMode,omitempty"`
	GlueAccessKeyID  string `json:"glueAccessKeyId,omitempty"`

	// --- Phase 3（IMPL_PLAN §12.2.3）：OAUTHBEARER 连接参数。非凭据字段入
	// Profile；凭据（msk_secret_access_key/msk_session_token/oauth_static_token）
	// 不在此结构，见 connSecrets。 ---
	// OauthTokenSource：msk_iam（默认）| static_token。
	OauthTokenSource string `json:"oauthTokenSource,omitempty"`
	// MSKRegion：MSK IAM 签名所需 AWS region（token_source=msk_iam 必填）。
	MSKRegion string `json:"mskRegion,omitempty"`
	// MSKAccessKeyID：可选显式覆盖默认凭据链（照 §11.5 Glue auth_mode=default
	// 范式；与 MSKSecretAccessKey 须成对出现）。
	MSKAccessKeyID string `json:"mskAccessKeyID,omitempty"`

	// --- Lane 3（conn-properties）：粘贴 properties 导入。合并结果直接落在
	// 上方结构化字段；这里只存解析摘要（计数+键名，无任何值，见 props.go）。 ---
	PropertiesImport *PropertiesImportSummary `json:"propertiesImport,omitempty"`
}

// kerberosEnabled 报告连接是否启用 GSSAPI。
func (p Profile) kerberosEnabled() bool {
	return p.SASLMechanism == SASLMechanismGSSAPI
}

// srEnabled 报告 Confluent SR 能力是否启用：以 schema_registry 开关为准；
// 开关未设（旧连接）按 sr_url 非空自动探测回退。
func (p Profile) srEnabled() bool {
	switch p.SchemaRegistry {
	case SchemaRegistryConfluent:
		return true
	case SchemaRegistryNone, SchemaRegistryAWSGlue:
		return false
	}
	return strings.TrimSpace(p.SRURL) != ""
}

// glueEnabled 报告 AWS Glue SR 是否启用：以 schema_registry 开关为准；
// 开关未设（旧连接）按 glue_region + glue_registry_name 齐备自动探测回退。
func (p Profile) glueEnabled() bool {
	switch p.SchemaRegistry {
	case SchemaRegistryAWSGlue:
		return true
	case SchemaRegistryNone, SchemaRegistryConfluent:
		return false
	}
	return strings.TrimSpace(p.GlueRegion) != "" && strings.TrimSpace(p.GlueRegistryName) != ""
}

// NormalizeProfile 归一化 Profile（trim + 协议/机制归一）。
func NormalizeProfile(p Profile) Profile {
	p.ID = strings.TrimSpace(p.ID)
	p.Name = strings.TrimSpace(p.Name)
	p.ClientID = strings.TrimSpace(p.ClientID)
	p.Username = strings.TrimSpace(p.Username)
	p.TLSCACert = strings.TrimSpace(p.TLSCACert)
	p.TLSClientCert = strings.TrimSpace(p.TLSClientCert)
	p.SecurityProtocol = NormalizeSecurityProtocol(p.SecurityProtocol)
	p.SASLMechanism = NormalizeSASLMechanism(p.SASLMechanism)
	p.ConnectionSource = NormalizeConnectionSource(p.ConnectionSource)
	p.SchemaRegistry = NormalizeSchemaRegistry(p.SchemaRegistry)
	p.KerberosServiceName = strings.TrimSpace(p.KerberosServiceName)
	p.KerberosRealm = strings.TrimSpace(p.KerberosRealm)
	p.KerberosPrincipal = strings.TrimSpace(p.KerberosPrincipal)
	p.KerberosKeytabPath = strings.TrimSpace(p.KerberosKeytabPath)
	p.KerberosKrb5ConfPath = strings.TrimSpace(p.KerberosKrb5ConfPath)
	p.SRURL = strings.TrimSpace(p.SRURL)
	p.SRUsername = strings.TrimSpace(p.SRUsername)
	p.GlueRegion = strings.TrimSpace(p.GlueRegion)
	p.GlueRegistryName = strings.TrimSpace(p.GlueRegistryName)
	p.GlueAuthMode = strings.TrimSpace(p.GlueAuthMode)
	p.GlueAccessKeyID = strings.TrimSpace(p.GlueAccessKeyID)
	// Phase 3：OAUTHBEARER 参数（token_source 归一化到取值面）。
	p.OauthTokenSource = NormalizeOauthTokenSource(p.OauthTokenSource)
	p.MSKRegion = strings.TrimSpace(p.MSKRegion)
	p.MSKAccessKeyID = strings.TrimSpace(p.MSKAccessKeyID)

	servers := make([]string, 0, len(p.BootstrapServers))
	for _, server := range p.BootstrapServers {
		server = strings.TrimSpace(server)
		if server == "" {
			continue
		}
		servers = append(servers, server)
	}
	p.BootstrapServers = servers
	zkServers := make([]string, 0, len(p.ZKServers))
	for _, server := range p.ZKServers {
		server = strings.TrimSpace(server)
		if server == "" {
			continue
		}
		zkServers = append(zkServers, server)
	}
	p.ZKServers = zkServers
	return p
}

// hasSASL 报告安全协议是否含 SASL。
func (p Profile) hasSASL() bool {
	return p.SecurityProtocol == SecurityProtocolSASLPlaintext || p.SecurityProtocol == SecurityProtocolSASLSSL
}

// hasTLS 报告安全协议是否含 SSL。
func (p Profile) hasTLS() bool {
	return p.SecurityProtocol == SecurityProtocolSSL || p.SecurityProtocol == SecurityProtocolSASLSSL
}

// Validate 基础校验（bootstrap/SASL 参数齐备性；zookeeper 源时 bootstrap 可
// 由 ZK 发现替代）。required_when 矩阵对应的必填组合校验见
// validateRequiredCombination（含 secrets，返回 -32602 参数错）。
func (p Profile) Validate() error {
	if p.ID == "" {
		return errf("connection.id is required")
	}
	if p.ConnectionSource == ConnectionSourceZookeeper {
		if len(p.ZKServers) == 0 {
			return errf("zkServers is required for zookeeper connection source")
		}
	} else if len(p.BootstrapServers) == 0 {
		return errf("bootstrapServers is required")
	}
	if p.hasSASL() && p.SASLMechanism == "" {
		return errf("saslMechanism is required for %s", p.SecurityProtocol)
	}
	return nil
}

// validateRequiredCombination 是 manifest required_when 矩阵的后端兜底校验
// （宿主连接对话框会拦可见必填字段，但 MCP/import 等非对话框写路径与纵深
// 防御仍需 sidecar 复核；矩阵与 manifest §9 一致）。缺失 →
// *InvalidParamsError（main.go 映射 -32602）。
//
// 矩阵：
//   - schema_registry=confluent → sr_url 必填且必须带 http(s) scheme
//   - schema_registry=aws_glue → glue_region/glue_registry_name 必填；
//     glue_auth_mode=static 时 AK/SK 必填（token 可选）
//   - mTLS：tls_client_cert / tls_client_key（secret）必须成对出现
//   - SASL 非 GSSAPI/OAUTHBEARER → sasl username/password 必填
//   - GSSAPI → kerberos principal/keytab 必填
//   - OAUTHBEARER（Phase 3）→ security_protocol 必须为 SASL_SSL；
//     token_source=msk_iam → mskRegion 必填；static_token → oauthStaticToken
//     必填；msk 显式 AK/SK 须成对（可选覆盖默认链），mskSessionToken 只随
//     显式 AK/SK 有效（单独提供会被默认链静默丢弃 → 参数错）
func validateRequiredCombination(p Profile, s connSecrets) error {
	switch p.SchemaRegistry {
	case SchemaRegistryConfluent:
		if strings.TrimSpace(p.SRURL) == "" {
			return &InvalidParamsError{Msg: "srUrl is required when schemaRegistry is \"confluent\""}
		}
		// 无 scheme 的 URL 会在首次 REST 请求时才报 "unsupported protocol
		// scheme"；提前为参数错，错误信息直接指向表单字段。
		if scheme := strings.ToLower(p.SRURL); !strings.HasPrefix(scheme, "http://") && !strings.HasPrefix(scheme, "https://") {
			return &InvalidParamsError{Msg: fmt.Sprintf("srUrl must start with http:// or https:// (got %q)", p.SRURL)}
		}
	case SchemaRegistryAWSGlue:
		if strings.TrimSpace(p.GlueRegion) == "" {
			return &InvalidParamsError{Msg: "glueRegion is required when schemaRegistry is \"aws_glue\""}
		}
		if strings.TrimSpace(p.GlueRegistryName) == "" {
			return &InvalidParamsError{Msg: "glueRegistryName is required when schemaRegistry is \"aws_glue\""}
		}
		if strings.EqualFold(strings.TrimSpace(p.GlueAuthMode), GlueAuthModeStatic) {
			if strings.TrimSpace(p.GlueAccessKeyID) == "" {
				return &InvalidParamsError{Msg: "glueAccessKeyId is required when glueAuthMode is \"static\""}
			}
			if strings.TrimSpace(s.GlueSecretAccessKey) == "" {
				return &InvalidParamsError{Msg: "glueSecretAccessKey is required when glueAuthMode is \"static\""}
			}
		}
	}
	// mTLS 半配置（cert 与 secret key 单边出现）会在拨号期才报 PEM 解析错
	// （"load tls client certificate/key: ..."，不指向表单字段）；提前为
	// -32602，字段名与连接表单一一对应。证书内容合法性仍在拨号期校验。
	if (strings.TrimSpace(p.TLSClientCert) == "") != (strings.TrimSpace(s.TLSClientKey) == "") {
		return &InvalidParamsError{Msg: "tls_client_cert and tls_client_key must be provided together for mutual TLS"}
	}
	// Inactive SASL values remain in saved forms so switching back restores
	// credentials. Only validate OAuth while a SASL transport is selected.
	if p.hasSASL() && p.SASLMechanism == SASLMechanismOAUTHBEARER {
		if p.SecurityProtocol != SecurityProtocolSASLSSL {
			return &InvalidParamsError{Msg: fmt.Sprintf("OAUTHBEARER requires security_protocol SASL_SSL (got %s)", p.SecurityProtocol)}
		}
		switch p.OauthTokenSource {
		case OauthTokenSourceMSKIAM:
			if strings.TrimSpace(p.MSKRegion) == "" {
				return &InvalidParamsError{Msg: "mskRegion is required when oauthTokenSource is \"msk_iam\""}
			}
			// 显式静态凭据可选覆盖默认链；给了一半 → 参数错。
			if (strings.TrimSpace(p.MSKAccessKeyID) == "") != (strings.TrimSpace(s.MSKSecretAccessKey) == "") {
				return &InvalidParamsError{Msg: "mskAccessKeyID and mskSecretAccessKey must be provided together to override the default AWS credential chain"}
			}
			// 会话 token 是 STS 临时凭据的一部分，只随显式 AK/SK 生效；
			// 单独提供时默认链不会消费它（静默丢弃 → 排障困惑）→ 参数错。
			if strings.TrimSpace(s.MSKSessionToken) != "" && strings.TrimSpace(p.MSKAccessKeyID) == "" {
				return &InvalidParamsError{Msg: "mskSessionToken requires mskAccessKeyID and mskSecretAccessKey (STS session credentials are never used alone)"}
			}
		case OauthTokenSourceStatic:
			if strings.TrimSpace(s.OauthStaticToken) == "" {
				return &InvalidParamsError{Msg: "oauthStaticToken is required when oauthTokenSource is \"static_token\""}
			}
		default:
			return &InvalidParamsError{Msg: "oauthTokenSource must be msk_iam or static_token"}
		}
	}
	if p.hasSASL() {
		switch p.SASLMechanism {
		case SASLMechanismGSSAPI:
			if strings.TrimSpace(p.KerberosPrincipal) == "" {
				return &InvalidParamsError{Msg: "kerberosPrincipal is required for GSSAPI"}
			}
			if strings.TrimSpace(p.KerberosKeytabPath) == "" {
				return &InvalidParamsError{Msg: "kerberosKeytabPath is required for GSSAPI"}
			}
		case SASLMechanismOAUTHBEARER:
			// 不需要 SASL 账密（token 即凭据；矩阵见上方 OAUTHBEARER 块）。
		default:
			if p.Username == "" {
				return &InvalidParamsError{Msg: fmt.Sprintf("sasl username is required for %s", p.SecurityProtocol)}
			}
			if strings.TrimSpace(s.SASLPassword) == "" {
				return &InvalidParamsError{Msg: fmt.Sprintf("sasl password is required for %s", p.SecurityProtocol)}
			}
		}
	}
	return nil
}

// --- 连接状态（kafka/connections/statuses，照 ldap 形态） ---

// SchemaRegistryStatus SR 能力摘要（不含凭据）。provider 取值
// confluent | glue | both | none（both = 旧连接双配置待显式 registry；none 时
// enabled=false 且 url/registryName 均空）。Mode 是 schema_registry 开关的
// 归一值 none | confluent | aws_glue（旧连接未设开关时省略，此时 provider
// 来自自动探测）。
type SchemaRegistryStatus struct {
	Enabled      bool   `json:"enabled"`
	Provider     string `json:"provider"`
	Mode         string `json:"mode,omitempty"`
	URL          string `json:"url,omitempty"`
	RegistryName string `json:"registryName,omitempty"`
}

// KerberosStatus Kerberos 摘要（不含凭据/路径细节）。
type KerberosStatus struct {
	Enabled bool `json:"enabled"`
}

// ConnectionStatus 连接状态。
type ConnectionStatus struct {
	ConnectionID string `json:"connectionId"`
	Name         string `json:"name"`
	Bootstrap    string `json:"bootstrap"`
	Status       string `json:"status"` // connected | idle | error
	ReadOnly     bool   `json:"readOnly,omitempty"`
	ConnectedAt  int64  `json:"connectedAt,omitempty"`
	LastUsedAt   int64  `json:"lastUsedAt,omitempty"`
	Error        string `json:"error,omitempty"`
	// ConnectionSource：bootstrap | zookeeper（Phase 2）。
	ConnectionSource string                `json:"connectionSource,omitempty"`
	SchemaRegistry   *SchemaRegistryStatus `json:"schemaRegistry,omitempty"`
	Kerberos         *KerberosStatus       `json:"kerberos,omitempty"`
	// PropertiesImport 是「粘贴 properties 导入」摘要（Lane 3；仅键名/计数，
	// 值不透出；未使用导入时省略）。
	PropertiesImport *PropertiesImportSummary `json:"propertiesImport,omitempty"`
}

// --- brokers ---

// BrokerInfo 对应 kafka/brokers/list 返回（契约 §5.2）。
type BrokerInfo struct {
	NodeID int32  `json:"nodeId"`
	Host   string `json:"host"`
	Port   int32  `json:"port"`
	Rack   string `json:"rack,omitempty"`
}

// BrokersListResult 对应 kafka/brokers/list。
type BrokersListResult struct {
	Brokers []BrokerInfo `json:"brokers"`
	// ConnectionSource：bootstrap | zookeeper（broker 列表来源，Phase 2）。
	ConnectionSource string `json:"connectionSource,omitempty"`
}

// BrokerConfigRequest 对应 kafka/brokers/config。
type BrokerConfigRequest struct {
	ConnectionID string `json:"connectionId"`
	BrokerID     int32  `json:"brokerId"`
}

// ConfigEntry 配置条目（brokers/config 与 topics/config/get 共用）。
type ConfigEntry struct {
	Name      string `json:"name"`
	Value     string `json:"value,omitempty"`
	Source    string `json:"source,omitempty"`
	Sensitive bool   `json:"sensitive,omitempty"`
	IsDefault bool   `json:"isDefault,omitempty"`
}

// ConfigEntriesResult 对应配置类方法返回。
type ConfigEntriesResult struct {
	Entries []ConfigEntry `json:"entries"`
}

// --- topics ---

// TopicsListRequest 对应 kafka/topics/list。
type TopicsListRequest struct {
	ConnectionID    string `json:"connectionId"`
	IncludeInternal bool   `json:"includeInternal,omitempty"`
}

// TopicInfo topic 概要。
type TopicInfo struct {
	Name              string `json:"name"`
	TopicID           string `json:"topicId,omitempty"`
	IsInternal        bool   `json:"isInternal,omitempty"`
	PartitionCount    int    `json:"partitionCount"`
	ReplicationFactor int    `json:"replicationFactor"`
	Error             string `json:"error,omitempty"`
	// isHealthy / unhealthyPartitions（Phase 3 §12.2.4，additive 无开关）：
	// 分区级判定复用 partitionInfos 同款规则（leader 有效 + ISR=replicas +
	// 无 offline），topic 级 = 全分区健康；unhealthyPartitions = 不健康分区数
	//（树徽标 title 用）。topic 元数据加载失败（Error 非空）时 isHealthy=false。
	IsHealthy           bool `json:"isHealthy"`
	UnhealthyPartitions int  `json:"unhealthyPartitions"`
}

// TopicsListResult 对应 kafka/topics/list。
type TopicsListResult struct {
	Topics []TopicInfo `json:"topics"`
}

// TopicsDescribeRequest 对应 kafka/topics/describe。
type TopicsDescribeRequest struct {
	ConnectionID string `json:"connectionId"`
	Topic        string `json:"topic"`
}

// PartitionInfo 分区健康视图（isHealthy：ISR 覆盖全部 replicas 且无 offline）。
type PartitionInfo struct {
	Partition       int32   `json:"partition"`
	Leader          int32   `json:"leader"`
	LeaderEpoch     int32   `json:"leaderEpoch,omitempty"`
	Replicas        []int32 `json:"replicas,omitempty"`
	ISR             []int32 `json:"isr,omitempty"`
	OfflineReplicas []int32 `json:"offlineReplicas,omitempty"`
	IsHealthy       bool    `json:"isHealthy"`
	Error           string  `json:"error,omitempty"`
}

// TopicDescribeResult 对应 kafka/topics/describe。
type TopicDescribeResult struct {
	Topic      string          `json:"topic"`
	Partitions []PartitionInfo `json:"partitions"`
}

// TopicsCreateRequest 对应 kafka/topics/create。
type TopicsCreateRequest struct {
	ConnectionID      string            `json:"connectionId"`
	Topics            []string          `json:"topics"`
	Partitions        int32             `json:"partitions"`
	ReplicationFactor int16             `json:"replicationFactor"`
	Config            map[string]string `json:"config,omitempty"`
}

// TopicsDeleteRequest 对应 kafka/topics/delete（critical 门禁：confirmTopic
// 必须与待删 topic 一致，防误删；多 topic 时要求全部同名或用 confirmTopics）。
type TopicsDeleteRequest struct {
	ConnectionID string   `json:"connectionId"`
	Topics       []string `json:"topics"`
	// ConfirmTopic 单 topic 删除的确认字段（§6：与 topic 同名才放行）。
	ConfirmTopic string `json:"confirmTopic,omitempty"`
	// ConfirmTopics 多 topic 删除的确认列表（与 Topics 逐一同名）。
	ConfirmTopics []string `json:"confirmTopics,omitempty"`
	// Source 操作来源标注（MCP 设计 §4：MCP 写路径 "mcp"；工作台不携带）。
	Source string `json:"source,omitempty"`
}

// PartitionsUpdateRequest 对应 kafka/topics/partitions/update（只增）。
type PartitionsUpdateRequest struct {
	ConnectionID string           `json:"connectionId"`
	Partitions   map[string]int32 `json:"partitions"`
}

// TopicConfigGetRequest 对应 kafka/topics/config/get。
type TopicConfigGetRequest struct {
	ConnectionID string `json:"connectionId"`
	Topic        string `json:"topic"`
}

// TopicConfigAlterRequest 对应 kafka/topics/config/alter。
type TopicConfigAlterRequest struct {
	ConnectionID string            `json:"connectionId"`
	Topic        string            `json:"topic"`
	Config       map[string]string `json:"config,omitempty"`
	DeleteKeys   []string          `json:"deleteKeys,omitempty"`
}

// TopicOffsetsListRequest 对应 kafka/topics/offsets/list。
type TopicOffsetsListRequest struct {
	ConnectionID string   `json:"connectionId"`
	Topics       []string `json:"topics"`
	// OffsetTime：earliest | latest | max-timestamp | log-start | RFC3339 |
	// unix ms（§5.2 Phase 2 全策略；默认 latest）。
	OffsetTime string `json:"offsetTime,omitempty"`
}

// TopicOffsetRow offset 行。
type TopicOffsetRow struct {
	Topic       string `json:"topic"`
	Partition   int32  `json:"partition"`
	Offset      int64  `json:"offset"`
	Timestamp   int64  `json:"timestamp,omitempty"`
	LeaderEpoch int32  `json:"leaderEpoch,omitempty"`
	Error       string `json:"error,omitempty"`
}

// TopicOffsetsListResult 对应 kafka/topics/offsets/list。
type TopicOffsetsListResult struct {
	Rows []TopicOffsetRow `json:"rows"`
}

// TopicRecordsClearRequest 对应 kafka/topics/records/clear（Phase 3，
// critical 门禁：与 topics/delete 同级——read_only/allow_delete 与门 +
// confirmTopic 单 topic 确认）。
type TopicRecordsClearRequest struct {
	ConnectionID string `json:"connectionId"`
	Topic        string `json:"topic"`
	// ConfirmTopic 必须与 topic 同名（复用 ensureTopicDeleteConfirm 单 topic
	// 语义，防误清空）。
	ConfirmTopic string `json:"confirmTopic,omitempty"`
	// Source 操作来源标注（MCP 设计 §4：MCP 写路径 "mcp"；工作台不携带）。
	Source string `json:"source,omitempty"`
}

// TopicRecordsClearRow 单分区清空结果行（Phase 3 §12.2.1）。
// Deleted/LowWatermark 为 long|null：任一段 offset 取不到时置 null
// （指针 nil → JSON null），不影响其他分区。
type TopicRecordsClearRow struct {
	Partition    int32  `json:"partition"`
	Deleted      *int64 `json:"deleted"`
	LowWatermark *int64 `json:"lowWatermark"`
	OK           bool   `json:"ok"`
	Error        string `json:"error,omitempty"`
}

// TopicRecordsClearResult 对应 kafka/topics/records/clear。
type TopicRecordsClearResult struct {
	Rows []TopicRecordsClearRow `json:"rows"`
}

// --- groups ---

// GroupsListResult 对应 kafka/groups/list。
type GroupsListResult struct {
	Groups []GroupInfo `json:"groups"`
}

// GroupInfo 消费组概要。
type GroupInfo struct {
	Group        string `json:"group"`
	State        string `json:"state,omitempty"`
	ProtocolType string `json:"protocolType,omitempty"`
	Coordinator  int32  `json:"coordinator,omitempty"`
}

// GroupsDescribeRequest 对应 kafka/groups/describe。
type GroupsDescribeRequest struct {
	ConnectionID string `json:"connectionId"`
	Group        string `json:"group"`
}

// GroupMemberInfo 组成员。
type GroupMemberInfo struct {
	MemberID    string             `json:"memberId"`
	InstanceID  string             `json:"instanceId,omitempty"`
	ClientID    string             `json:"clientId,omitempty"`
	ClientHost  string             `json:"clientHost,omitempty"`
	Assignments map[string][]int32 `json:"assignments,omitempty"`
}

// GroupDescribeResult 对应 kafka/groups/describe。
type GroupDescribeResult struct {
	Group        string            `json:"group"`
	State        string            `json:"state,omitempty"`
	ProtocolType string            `json:"protocolType,omitempty"`
	Protocol     string            `json:"protocol,omitempty"`
	Coordinator  int32             `json:"coordinator,omitempty"`
	Members      []GroupMemberInfo `json:"members,omitempty"`
	Error        string            `json:"error,omitempty"`
}

// GroupOffsetsListRequest 对应 kafka/groups/offsets/list（topics 空 =
// committed 全量）。
type GroupOffsetsListRequest struct {
	ConnectionID string   `json:"connectionId"`
	Group        string   `json:"group"`
	Topics       []string `json:"topics,omitempty"`
}

// GroupOffsetRow 消费组 offset 行（lag = endOffset - committedOffset）。
type GroupOffsetRow struct {
	Topic           string `json:"topic"`
	Partition       int32  `json:"partition"`
	StartOffset     int64  `json:"startOffset"`
	EndOffset       int64  `json:"endOffset"`
	CommittedOffset int64  `json:"committedOffset"`
	Lag             int64  `json:"lag"`
	Error           string `json:"error,omitempty"`
}

// GroupOffsetsListResult 对应 kafka/groups/offsets/list。
// Option 语义：组从未提交过 offset 时 hasCommitted=false（与零 lag 区分）。
type GroupOffsetsListResult struct {
	Rows         []GroupOffsetRow `json:"rows"`
	TotalLag     int64            `json:"totalLag"`
	HasCommitted bool             `json:"hasCommitted"`
}

// GroupDeleteRequest 对应 kafka/groups/delete（critical 门禁）。
type GroupDeleteRequest struct {
	ConnectionID string `json:"connectionId"`
	Group        string `json:"group"`
}

// OffsetResetMode 是 kafka/groups/offsets/reset 的 resetTo 取值。
type OffsetResetMode string

const (
	OffsetResetEarliest         OffsetResetMode = "earliest"
	OffsetResetLatest           OffsetResetMode = "latest"
	OffsetResetTimestamp        OffsetResetMode = "timestamp"
	OffsetResetPartitionOffsets OffsetResetMode = "partitionOffset"
)

// GroupOffsetResetRequest 对应 kafka/groups/offsets/reset。
type GroupOffsetResetRequest struct {
	ConnectionID string   `json:"connectionId"`
	Group        string   `json:"group"`
	Topics       []string `json:"topics,omitempty"`
	ResetTo      string   `json:"resetTo"`
	// TimestampMs 仅 resetTo=timestamp 时使用。
	TimestampMs int64 `json:"timestampMs,omitempty"`
	// PartitionOffsets 仅 resetTo=partitionOffset 时使用：
	// topic → partition → offset（分区号 JSON 序列化为字符串 key）。
	PartitionOffsets map[string]map[int32]int64 `json:"partitionOffsets,omitempty"`
	// Source 操作来源标注（MCP 设计 §4：MCP 写路径 "mcp"；工作台不携带）。
	Source string `json:"source,omitempty"`
}

// OffsetResetRow 重置结果行。
type OffsetResetRow struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	OK        bool   `json:"ok"`
	Error     string `json:"error,omitempty"`
}

// OffsetResetResult 对应 kafka/groups/offsets/reset。
type OffsetResetResult struct {
	Rows []OffsetResetRow `json:"rows"`
}

// --- acls ---

// ACLFilter ACL 过滤/定义（§5.2 filter{} 拒绝过宽：至少一个资源维度非空）。
type ACLFilter struct {
	ResourceType string `json:"resourceType,omitempty"`
	ResourceName string `json:"resourceName,omitempty"`
	PatternType  string `json:"patternType,omitempty"`
	Principal    string `json:"principal,omitempty"`
	Host         string `json:"host,omitempty"`
	Operation    string `json:"operation,omitempty"`
	Permission   string `json:"permission,omitempty"`
}

// ACLBinding 一条 ACL（list 行 / create 请求体）。
type ACLBinding struct {
	ResourceType string `json:"resourceType"`
	ResourceName string `json:"resourceName"`
	PatternType  string `json:"patternType,omitempty"`
	Principal    string `json:"principal"`
	Host         string `json:"host,omitempty"`
	Operation    string `json:"operation"`
	Permission   string `json:"permission"`
	Error        string `json:"error,omitempty"`
}

// ACLsListRequest 对应 kafka/acls/list。
type ACLsListRequest struct {
	ConnectionID string    `json:"connectionId"`
	Filter       ACLFilter `json:"filter"`
}

// ACLsListResult 对应 kafka/acls/list。
type ACLsListResult struct {
	ACLs []ACLBinding `json:"acls"`
}

// ACLsCreateRequest 对应 kafka/acls/create。
type ACLsCreateRequest struct {
	ConnectionID string     `json:"connectionId"`
	ACL          ACLBinding `json:"acl"`
}

// ACLsDeleteRequest 对应 kafka/acls/delete。
type ACLsDeleteRequest struct {
	ConnectionID string    `json:"connectionId"`
	Filter       ACLFilter `json:"filter"`
}

// ACLsDeleteResult 对应 kafka/acls/delete。
type ACLsDeleteResult struct {
	Matched []ACLBinding `json:"matched"`
}

// --- 预设（kafka/presets/*，照 ldap/presets 形态；存 store presets.json） ---

// ConsumePreset 消费/过滤预设（Params 为值拷贝，不含凭据）。
type ConsumePreset struct {
	ID     string        `json:"id"`
	Name   string        `json:"name"`
	Params ConsumeParams `json:"params"`
}

// PresetStore 是预设持久化接口（main.go 注入 store-backed 实现；
// Service.Presets 为 nil 时 kafka/presets/* 返回业务错误）。
type PresetStore interface {
	LoadPresets() ([]ConsumePreset, error)
	SavePresets(presets []ConsumePreset) error
}

// errf 是包内 fmt.Errorf 的短别名（types/policy 层错误统一走业务 -32000）。
func errf(format string, args ...any) error {
	return fmt.Errorf(format, args...)
}
