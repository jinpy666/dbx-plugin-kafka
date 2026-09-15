package kafkaconn

// client.go：kgo client 构建与按 connectionId 缓存（IMPL_PLAN §5.2 改造点
// "客户端连接复用"——tinyrdm 每调用重建 client；本插件 admin 类调用复用缓存
// client，指纹=bootstrap+auth+TLS 摘要，失效重建；consume/stream 因携带
// per-request 的消费 opts，每次新建 client 用完即关）。
//
// 拨号语义（D5 方案 kafka 侧）：连接参数里的 bootstrap 就是宿主改写后的
// 地址，直接拨 bootstrap_servers，无需自建隧道；bootstrap 缺失时以
// runtime.host:port 兜底（单 seed）。
//
// TLS（CA/client cert/insecure skip verify）+ SASL（PLAIN/SCRAM-SHA-256/
// SCRAM-SHA-512/GSSAPI/OAUTHBEARER）取值面与 tinyrdm kafkaSASLOpt / kafkaDialer
// 对齐（GSSAPI/Kerberos 为 Phase 2 接入，见 kerberos.go；OAUTHBEARER 为
// Phase 3 接入，见 oauth.go）。

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"io.dbx.kafka.plugin/internal/lifecycle"
)

// connSecrets 是 binding:secret 的连接凭据（仅存内存，禁止落日志/审计/事件）。
type connSecrets struct {
	SASLPassword        string
	TLSClientKey        string
	SRPassword          string
	GlueSecretAccessKey string // AWS Glue static 凭据（Phase 3）
	GlueSessionToken    string // AWS Glue 会话 token（可选）
	// OAUTHBEARER（Phase 3 §12.2.3）：凭据走 secret binding，不进 Profile。
	MSKSecretAccessKey string // msk_iam 显式覆盖默认链的 SK（可选，与 AK 成对）
	MSKSessionToken    string // msk_iam 会话 token（可选，STS 临时凭据）
	OauthStaticToken   string // static_token 来源的静态 bearer token
}

// connTarget 是一次拨号需要的运行时端点（bootstrap 空时兜底）。
type connTarget struct {
	Host string
	Port int
}

// connEntry 是单个宿主连接的 sidecar 内状态。
type connEntry struct {
	mu sync.Mutex

	profile     Profile     // 已 NormalizeProfile 的连接配置（不含凭据）
	secrets     connSecrets // binding: secret
	target      connTarget  // runtime 兜底端点
	client      *kgo.Client // admin 类调用共享 client（nil = 未建）
	fingerprint string      // 建 client 时的配置指纹

	connectedAt int64 // unix ms
	lastUsedAt  int64 // unix ms
	status      string
	lastError   string
}

// computeFingerprint 计算连接配置摘要（凭据进摘要但不落任何日志；配置任一
// 维度变化即失效重建 client）。
func (e *connEntry) computeFingerprint() string {
	parts := []string{
		strings.Join(e.profile.BootstrapServers, ","),
		e.profile.SecurityProtocol,
		e.profile.SASLMechanism,
		e.profile.Username,
		e.secrets.SASLPassword,
		e.profile.TLSCACert,
		e.profile.TLSClientCert,
		e.secrets.TLSClientKey,
		fmt.Sprintf("%t", e.profile.TLSInsecureSkipVerify),
		e.profile.ClientID,
		// Phase 2：GSSAPI 参数进指纹（换 keytab/principal 重建）。
		e.profile.ConnectionSource,
		strings.Join(e.profile.ZKServers, ","),
		e.profile.KerberosServiceName,
		e.profile.KerberosRealm,
		e.profile.KerberosPrincipal,
		e.profile.KerberosKeytabPath,
		e.profile.KerberosKrb5ConfPath,
		// Schema Registry 开关/参数变化（含 glue 参数）→ 摘要变化。
		e.profile.SchemaRegistry,
		e.profile.SRURL,
		e.profile.SRUsername,
		e.profile.GlueRegion,
		e.profile.GlueRegistryName,
		e.profile.GlueAuthMode,
		e.profile.GlueAccessKeyID,
		// Phase 3：OAUTHBEARER 参数（token 来源/region/显式 AK + secret）进
		// 摘要（换凭据/换 token 源 → 失效重建；凭据只进摘要不落日志）。
		e.profile.OauthTokenSource,
		e.profile.MSKRegion,
		e.profile.MSKAccessKeyID,
		e.secrets.MSKSecretAccessKey,
		e.secrets.MSKSessionToken,
		e.secrets.OauthStaticToken,
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

// seedBrokers 返回拨号地址列表：bootstrap 原样（宿主改写后的地址），
// 空时 runtime.host:port 兜底。
func (e *connEntry) seedBrokers() []string {
	// Selecting ZooKeeper must not keep using a previous bootstrap list (or
	// runtime endpoint) retained in the form's inactive branch.
	if e.profile.ConnectionSource == ConnectionSourceZookeeper {
		return nil
	}
	if len(e.profile.BootstrapServers) > 0 {
		return e.profile.BootstrapServers
	}
	if e.target.Host != "" && e.target.Port > 0 {
		return []string{fmt.Sprintf("%s:%d", e.target.Host, e.target.Port)}
	}
	return nil
}

// buildClientOpts 组装 kgo opts（TLS + SASL + SeedBrokers + ClientID）。
// extraOpts 由调用方追加（consume/stream 的 ConsumePartitions 等）。
// 本函数不主动拨号（单测覆盖 TLS/SASL 矩阵）。
func (e *connEntry) buildClientOpts(extraOpts ...kgo.Opt) ([]kgo.Opt, error) {
	seeds := e.seedBrokers()
	if len(seeds) == 0 && e.profile.ConnectionSource == ConnectionSourceZookeeper {
		// zookeeper 源且未给 bootstrap：经 ZK 发现 broker 作为拨号种子
		//（discoverBrokersViaZK 是纯 profile 实现，不触 entry/service）。
		resolved, err := zkSeedsForTest(e.profile)
		if err != nil {
			return nil, err
		}
		seeds = resolved
	}
	if len(seeds) == 0 {
		return nil, errf("bootstrap servers or runtime endpoint is required")
	}
	return e.buildClientOptsWithSeeds(seeds, extraOpts...)
}

// buildClientOptsWithSeeds 以给定种子组装 kgo opts（connection/test 的 ZK
// 模式与常规路径共用 TLS/SASL 组装）。
func (e *connEntry) buildClientOptsWithSeeds(seeds []string, extraOpts ...kgo.Opt) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(seeds...),
		// 默认生产者参数对 admin/consume 无影响；关闭 kgo 默认的自动
		// transactional id 等，保持 tinyrdm 同款最小 client。
		kgo.ClientID(firstNonEmpty(e.profile.ClientID, "dbx-kafka-plugin")),
		kgo.RequestTimeoutOverhead(5 * time.Second),
	}

	if e.profile.hasTLS() {
		tlsConfig, err := buildTLSConfig(e.profile, e.secrets)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	if e.profile.hasSASL() {
		saslOpt, err := buildSASLOpt(e.profile, e.secrets)
		if err != nil {
			return nil, err
		}
		opts = append(opts, saslOpt)
	}

	opts = append(opts, extraOpts...)
	return opts, nil
}

// buildTLSConfig 由 profile + secret 构建 TLS 配置（纯函数，单测覆盖）。
// 仅在安全协议含 SSL 时调用。
// 注：不共享 ClientSessionCache 做 TLS 会话恢复——实测部分中间盒/网关环境
// （如内网代理终结 TLS 的集群入口）对 session 重放直接断连
// （"broker closed the connection immediately after a dial"），风险大于
// 冷启动省 1 RTT 的收益；冷启动提速由消费 client 复用池承担。
func buildTLSConfig(profile Profile, secrets connSecrets) (*tls.Config, error) {
	config := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}
	// CA 证书（PEM textarea）：空 = 系统信任链。
	if caCert := strings.TrimSpace(profile.TLSCACert); caCert != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(caCert)) {
			return nil, errf("tlsCaCert contains no valid PEM certificate")
		}
		config.RootCAs = pool
	}
	// 客户端证书双向认证（cert 为 config，key 为 secret）。
	if certPEM := strings.TrimSpace(profile.TLSClientCert); certPEM != "" || strings.TrimSpace(secrets.TLSClientKey) != "" {
		pair, err := tls.X509KeyPair([]byte(profile.TLSClientCert), []byte(secrets.TLSClientKey))
		if err != nil {
			return nil, fmt.Errorf("load tls client certificate/key: %w", err)
		}
		config.Certificates = []tls.Certificate{pair}
	}
	config.InsecureSkipVerify = profile.TLSInsecureSkipVerify
	return config, nil
}

// buildSASLOpt 由 profile + secret 构建 SASL 机制（纯函数，单测覆盖矩阵）。
func buildSASLOpt(profile Profile, secrets connSecrets) (kgo.Opt, error) {
	switch profile.SASLMechanism {
	case SASLMechanismPlain:
		return kgo.SASL(plain.Auth{
			User: profile.Username,
			Pass: secrets.SASLPassword,
		}.AsMechanism()), nil
	case SASLMechanismSCRAMSHA256:
		return kgo.SASL(scram.Auth{
			User: profile.Username,
			Pass: secrets.SASLPassword,
		}.AsSha256Mechanism()), nil
	case SASLMechanismSCRAMSHA512:
		return kgo.SASL(scram.Auth{
			User: profile.Username,
			Pass: secrets.SASLPassword,
		}.AsSha512Mechanism()), nil
	case SASLMechanismGSSAPI:
		// Kerberos/GSSAPI（Phase 2）：参数构造在此完成（keytab/krb5.conf
		// 落盘读取），真实认证发生在 kgo 拨号时。
		params, err := buildKerberosParams(profile)
		if err != nil {
			return nil, err
		}
		auth, err := buildKerberosSASLMechanism(params)
		if err != nil {
			return nil, err
		}
		return kgo.SASL(auth.AsMechanismWithClose()), nil
	case SASLMechanismOAUTHBEARER:
		// OAUTHBEARER（Phase 3 §12.2.3）：token provider 按来源构造
		// （msk_iam 签名在拨号时按会话调用，构造期不触网）。
		return buildOauthSASLOpt(profile, secrets)
	case "":
		return nil, errf("saslMechanism is required for %s", profile.SecurityProtocol)
	default:
		return nil, errf("unsupported SASL mechanism %q (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI, OAUTHBEARER)", profile.SASLMechanism)
	}
}

// closeLocked 关闭共享 client（调用方须持 entry.mu）。
func (e *connEntry) closeLocked() {
	if e.client != nil {
		e.client.Close()
		e.client = nil
		e.fingerprint = ""
	}
	e.status = "closed"
}

// adminClientLocked 返回共享 admin client：指纹匹配直接复用，否则重建
// （调用方须持 entry.mu；失败不改动已有 client）。同一连接的领域操作经
// entry.mu 串行化，client 无并发争用。
func (e *connEntry) adminClientLocked() (*kgo.Client, error) {
	want := e.computeFingerprint()
	if e.client != nil && e.fingerprint == want {
		return e.client, nil
	}
	if e.client != nil {
		e.client.Close()
		e.client = nil
	}
	opts, err := e.buildClientOpts()
	if err != nil {
		return nil, err
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	e.client = client
	e.fingerprint = want
	return client, nil
}

// withAdmin 在 entry 锁内执行 admin 回调（共享 client 串行使用 + 指纹失效
// 重建）。回调内禁止再取 entry.mu。
func (s *Service) withAdmin(connectionID string, fn func(client *kgo.Client) error) error {
	entry := s.lookup(connectionID)
	if entry == nil {
		return errConnectionNotFound(connectionID)
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()

	client, err := entry.adminClientLocked()
	if err != nil {
		entry.status = "error"
		entry.lastError = err.Error()
		return err
	}
	err = fn(client)
	entry.lastUsedAt = time.Now().UnixMilli()
	if err == nil {
		entry.status = "connected"
		entry.lastError = ""
		return nil
	}
	entry.status = "error"
	entry.lastError = err.Error()
	return err
}

// consumeClient 为一次性消费/生产等 per-request 场景新建 client（带
// extraOpts），调用方负责 Close。与缓存 client 解耦，避免消费 opts 污染
// admin 复用通道。
func (s *Service) consumeClient(connectionID string, extraOpts ...kgo.Opt) (*kgo.Client, func(), error) {
	entry := s.lookup(connectionID)
	if entry == nil {
		return nil, nil, errConnectionNotFound(connectionID)
	}
	entry.mu.Lock()
	opts, err := entry.buildClientOpts(extraOpts...)
	entry.mu.Unlock()
	if err != nil {
		return nil, nil, err
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("create kafka client: %w", err)
	}
	return client, func() { client.Close() }, nil
}

// firstNonEmpty 返回第一个非空字符串。
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

// NewProfileFromLifecycle 把 lifecycle params 映射为 Profile（manifest §4
// 字段表 → binding 落点，M0 文档 §3.1）。第二个返回值是连接表内保存的凭据
// （不进 Profile，只存 connEntry）。
func NewProfileFromLifecycle(params *lifecycle.Params) (Profile, connSecrets, error) {
	profile := Profile{
		ID:               params.ConnectionID(),
		Name:             firstNonEmpty(params.Connection.Name, "Kafka cluster"),
		BootstrapServers: params.ConfigStringSlice("bootstrap_servers"),
		SecurityProtocol: NormalizeSecurityProtocol(params.ConfigString("security_protocol")),
		SASLMechanism:    params.ConfigString("sasl_mechanism"),
		Username:         firstNonEmpty(params.ConfigString("sasl_username"), params.Connection.Username),
		TLSCACert:        params.ConfigString("tls_ca_cert"),
		TLSClientCert:    params.ConfigString("tls_client_cert"),
		ClientID:         params.ConfigString("client_id"),
		// --- Phase 2（manifest 新字段，key/取值面见冻结契约） ---
		ConnectionSource:     params.ConfigString("connection_source"),
		ZKServers:            params.ConfigStringSlice("zk_servers"),
		KerberosServiceName:  params.ConfigString("kerberos_service_name"),
		KerberosRealm:        params.ConfigString("kerberos_realm"),
		KerberosPrincipal:    params.ConfigString("kerberos_principal"),
		KerberosKeytabPath:   params.ConfigString("kerberos_keytab_path"),
		KerberosKrb5ConfPath: params.ConfigString("kerberos_krb5_conf_path"),
		// --- Schema Registry 决策开关（none | confluent | aws_glue；空 = 旧
		// 连接自动探测回退）+ Confluent SR 参数 ---
		SchemaRegistry: params.ConfigString("schema_registry"),
		SRURL:          params.ConfigString("sr_url"),
		SRUsername:     params.ConfigString("sr_username"),
		// --- Phase 3（AWS Glue Schema Registry，冻结契约 1） ---
		GlueRegion:       params.ConfigString("glue_region"),
		GlueRegistryName: params.ConfigString("glue_registry_name"),
		GlueAuthMode:     params.ConfigString("glue_auth_mode"),
		GlueAccessKeyID:  params.ConfigString("glue_access_key_id"),
		// --- Phase 3（OAUTHBEARER，§12.2.3）：非凭据参数入 Profile ---
		OauthTokenSource: params.ConfigString("oauth_token_source"),
		MSKRegion:        params.ConfigString("msk_region"),
		MSKAccessKeyID:   params.ConfigString("msk_access_key_id"),
	}
	profile.TLSInsecureSkipVerify = params.ConfigBool("tls_insecure_skip_verify")
	// 只读门禁收敛：连接表单 read_only ∥ 宿主标准 read_only。
	profile.ReadOnly = params.ConfigBool("read_only") || params.Connection.ReadOnly
	profile.AllowDelete = params.ConfigBool("allow_delete")

	secrets := connSecrets{
		SASLPassword:        params.SecretString("sasl_password"),
		TLSClientKey:        params.SecretString("tls_client_key"),
		SRPassword:          params.SecretString("sr_password"),
		GlueSecretAccessKey: params.SecretString("glue_secret_access_key"),
		GlueSessionToken:    params.SecretString("glue_session_token"),
		// OAUTHBEARER 凭据（secret binding，红线：不落日志/审计/回显）。
		MSKSecretAccessKey: params.SecretString("msk_secret_access_key"),
		MSKSessionToken:    params.SecretString("msk_session_token"),
		OauthStaticToken:   params.SecretString("oauth_static_token"),
	}
	// Lane 3（conn-properties）：properties_import 只从 secret binding 读取
	//（manifest textarea + binding:secret，粘贴文本中的密码经宿主加密存储，
	// 绝不明文持久化）。非空时解析并合并进结构化字段（paste 非空值覆盖表单
	// 值），合并发生在 Normalize/Validate 之前——粘贴驱动的 SASL_SSL + jaas
	// 凭据组合会通过 required_when 兜底校验；解析摘要（仅键名）随 statuses 透出。
	if paste := params.SecretString("properties_import"); paste != "" {
		profile.PropertiesImport = applyPropertiesImport(&profile, &secrets, paste)
	}
	profile = NormalizeProfile(profile)
	if err := profile.Validate(); err != nil {
		return Profile{}, connSecrets{}, err
	}
	// required_when 矩阵兜底校验（缺失 → *InvalidParamsError → -32602）。
	if err := validateRequiredCombination(profile, secrets); err != nil {
		return Profile{}, connSecrets{}, err
	}
	return profile, secrets, nil
}
