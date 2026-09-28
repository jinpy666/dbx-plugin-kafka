package kafkaconn

// client.go：kgo client 构建与按 connectionId 缓存（IMPL_PLAN §5.2 改造点
// "客户端连接复用"——tinyrdm 每调用重建 client；本插件 admin 类调用复用缓存
// client，指纹=bootstrap+auth+TLS 摘要，失效重建；consume/stream 因携带
// per-request 的消费 opts，每次新建 client 用完即关）。
//
// 拨号语义（D5 方案 kafka 侧）：无 structured proxy route 时优先拨宿主
// lifecycle 传入的 runtime.host:port；没有 runtime 端点才回退到 bootstrap。
// 多 broker 场景若 Host 传入 runtime.proxy，则保留逻辑 bootstrap/metadata
// 地址，并由统一 SOCKS5 dialer 负责每个 broker 的连接。
//
// TLS（CA/client cert/insecure skip verify）+ SASL（PLAIN/SCRAM-SHA-256/
// SCRAM-SHA-512/GSSAPI/OAUTHBEARER）取值面与 tinyrdm kafkaSASLOpt / kafkaDialer
// 对齐（GSSAPI/Kerberos 为 Phase 2 接入，见 kerberos.go；OAUTHBEARER 为
// Phase 3 接入，见 oauth.go）。

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	xproxy "golang.org/x/net/proxy"

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
	Host  string
	Port  int
	Proxy *lifecycle.RuntimeProxy
}

// connEntry 是单个宿主连接的 sidecar 内状态。
type connEntry struct {
	mu sync.Mutex

	// profile/secrets/target 发布后不可变（Connect/Test 每次构造全新
	// entry 原子替换），statuses 快照等只读方无需持 mu。
	profile     Profile     // 已 NormalizeProfile 的连接配置（不含凭据）
	secrets     connSecrets // binding: secret
	target      connTarget  // runtime 兜底端点
	client      *kgo.Client // admin 类调用共享 client（nil = 未建）
	fingerprint string      // 建 client 时的配置指纹

	// SR HTTP 客户端缓存（评审 L-2）：此前每次 produce/consume/stream 新建
	// http.Client+Transport，TLS 下无连接复用、每请求重做握手。
	srClient      *schemaRegistryClient
	srFingerprint string

	// 状态面用独立细粒度锁（评审 M-6）：此前与 mu 共锁，withAdmin 在 mu 内
	// 执行整个 admin RPC（≤20s），statuses UI 轮询被串行阻塞。锁序：
	// mu → statusMu，禁反向。
	statusMu    sync.Mutex
	connectedAt int64 // unix ms
	lastUsedAt  int64 // unix ms
	status      string
	lastError   string
}

// setStatus 更新状态面（statusMu；lastUsedAt 记本次活动时间）。
func (e *connEntry) setStatus(status, lastErr string) {
	e.statusMu.Lock()
	e.status = status
	e.lastError = lastErr
	e.lastUsedAt = time.Now().UnixMilli()
	e.statusMu.Unlock()
}

// statusSnapshot 原子读取状态面三元组（statuses 快照用）。
func (e *connEntry) statusSnapshot() (status, lastErr string, lastUsedAt int64) {
	e.statusMu.Lock()
	defer e.statusMu.Unlock()
	return e.status, e.lastError, e.lastUsedAt
}

// computeFingerprint 计算连接配置摘要（凭据进摘要但不落任何日志；配置任一
// 维度变化即失效重建 client）。
func (e *connEntry) computeFingerprint() string {
	parts := []string{
		strings.Join(e.profile.BootstrapServers, ","),
		e.target.Host,
		fmt.Sprintf("%d", e.target.Port),
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
	if e.target.Proxy != nil {
		parts = append(parts,
			e.target.Proxy.Type,
			e.target.Proxy.Kind,
			e.target.Proxy.Host,
			fmt.Sprintf("%d", e.target.Proxy.Port),
			e.target.Proxy.Username,
			e.target.Proxy.Password,
		)
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

// seedBrokers 返回逻辑 broker 地址。
//
// A structured proxy route must keep the original bootstrap list: franz-go
// uses broker metadata to discover every advertised broker, and the dialer
// below sends each of those addresses through the same SOCKS5 route. Without
// a proxy route, runtime.host:port is the final Host-managed endpoint and is
// preferred over the raw bootstrap list. This is important for SSH, SOCKS5,
// HTTP CONNECT, and HTTP tunnel layers represented by a local forward port.
func (e *connEntry) seedBrokers() []string {
	// Selecting ZooKeeper must not keep using a previous bootstrap list (or
	// runtime endpoint) retained in the form's inactive branch.
	if e.profile.ConnectionSource == ConnectionSourceZookeeper {
		return nil
	}
	if e.target.Proxy != nil && len(e.profile.BootstrapServers) > 0 {
		return e.profile.BootstrapServers
	}
	if e.target.Host != "" && e.target.Port > 0 {
		return []string{fmt.Sprintf("%s:%d", e.target.Host, e.target.Port)}
	}
	if len(e.profile.BootstrapServers) > 0 {
		return e.profile.BootstrapServers
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
		//（discoverBrokersViaZKProfile 是纯 profile 实现，不触 entry/service）。
		// 评审 M-7：此路径暂无 ctx 可贯穿（buildClientOpts 签名波及池化面），
		// ZK 会话仍受 zkDialTimeout/zkSessionTimeout 上界约束；请求级入口
		// （brokers/list、connection/test）已贯穿 ctx 可取消。
		resolved, err := zkSeedsForTest(context.Background(), e.profile)
		if err != nil {
			return nil, err
		}
		seeds = resolved
	}
	if len(seeds) == 0 {
		return nil, errf("bootstrap servers or runtime endpoint is required")
	}
	opts, _, err := e.buildClientOptsWithSeeds(seeds, false, extraOpts...)
	return opts, err
}

// buildClientOptsWithSeeds 以给定种子组装 kgo opts（connection/test 的 ZK
// 模式与常规路径共用 TLS/SASL 组装）。第二个返回值是生效的 TLS 配置（未启用
// TLS 时为 nil）。
//
// withProbeDialer 标记调用方随后会追加自己的探针拨号器（connection/test）：
// 此时不得再追加 kgo.DialTLSConfig——kgo 校验拒绝 Dialer 与 DialTLSConfig
// 并存（config.go validate），TLS 改由探针拨号器按返回的 tlsConfig 自行完成
// （语义对齐内置路径：Clone config，ServerName 为空时由拨号地址推导 SNI）。
func (e *connEntry) buildClientOptsWithSeeds(seeds []string, withProbeDialer bool, extraOpts ...kgo.Opt) ([]kgo.Opt, *tls.Config, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(seeds...),
		// 默认生产者参数对 admin/consume 无影响；关闭 kgo 默认的自动
		// transactional id 等，保持 tinyrdm 同款最小 client。
		kgo.ClientID(firstNonEmpty(e.profile.ClientID, "dbx-kafka-plugin")),
		kgo.RequestTimeoutOverhead(5 * time.Second),
	}

	var tlsConfig *tls.Config
	if e.profile.hasTLS() {
		built, err := buildTLSConfig(e.profile, e.secrets)
		if err != nil {
			return nil, nil, err
		}
		tlsConfig = built
		// When Host hands us a local forward endpoint, preserve the logical
		// broker name for certificate verification instead of using 127.0.0.1.
		// A structured proxy route leaves ServerName empty here so the custom
		// dialer can derive SNI from each advertised broker address.
		if e.target.Proxy == nil && e.target.Host != "" && e.target.Port > 0 {
			if host := e.logicalTLSSeedHost(seeds); host != "" {
				tlsConfig.ServerName = host
			}
		}
		if e.target.Proxy == nil && !withProbeDialer {
			opts = append(opts, kgo.DialTLSConfig(tlsConfig))
		}
	}

	if e.profile.hasSASL() {
		saslOpt, err := buildSASLOpt(e.profile, e.secrets)
		if err != nil {
			return nil, nil, err
		}
		opts = append(opts, saslOpt)
	}

	if e.target.Proxy != nil {
		dial, err := e.runtimeProxyDialer(tlsConfig)
		if err != nil {
			return nil, nil, err
		}
		opts = append(opts, kgo.Dialer(dial))
	}

	opts = append(opts, extraOpts...)
	return opts, tlsConfig, nil
}

// runtimeProxyDialer creates the one dialer shared by every franz-go client
// shape. Keeping it at the kgo option boundary means admin, produce, consume,
// and stream clients cannot accidentally diverge in their transport behavior.
func (e *connEntry) runtimeProxyDialer(tlsConfig *tls.Config) (func(context.Context, string, string) (net.Conn, error), error) {
	proxyConfig := e.target.Proxy
	if proxyConfig == nil {
		return nil, errf("runtime proxy is not configured")
	}
	typeName := strings.ToLower(strings.TrimSpace(proxyConfig.Type))
	if typeName == "" {
		typeName = strings.ToLower(strings.TrimSpace(proxyConfig.Kind))
	}
	if typeName != "socks5" && typeName != "socks5h" {
		return nil, errf("unsupported runtime proxy type %q (only socks5 is supported)", typeName)
	}
	if strings.TrimSpace(proxyConfig.Host) == "" || proxyConfig.Port <= 0 || proxyConfig.Port > 65535 {
		return nil, errf("runtime SOCKS5 proxy host and port are required")
	}
	auth := (*xproxy.Auth)(nil)
	if proxyConfig.Username != "" || proxyConfig.Password != "" {
		auth = &xproxy.Auth{User: proxyConfig.Username, Password: proxyConfig.Password}
	}
	dialer, err := xproxy.SOCKS5("tcp", net.JoinHostPort(proxyConfig.Host, fmt.Sprintf("%d", proxyConfig.Port)), auth, &net.Dialer{Timeout: 10 * time.Second})
	if err != nil {
		return nil, errf("create runtime SOCKS5 dialer: %v", err)
	}
	rawDial := contextDialer(dialer)
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := rawDial(ctx, network, address)
		if err != nil || tlsConfig == nil {
			return conn, err
		}
		config := tlsConfig.Clone()
		if config.ServerName == "" {
			config.ServerName = kafkaSeedHost([]string{address})
		}
		tlsConn := tls.Client(conn, config)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = conn.Close()
			return nil, err
		}
		return tlsConn, nil
	}, nil
}

func (e *connEntry) logicalTLSSeedHost(seeds []string) string {
	if e.target.Proxy == nil && len(e.profile.BootstrapServers) > 0 {
		return kafkaSeedHost(e.profile.BootstrapServers)
	}
	return kafkaSeedHost(seeds)
}

// contextDialer adapts x/net/proxy's legacy Dialer to kgo's cancellable
// DialContext contract. The result channel is buffered so a canceled request
// cannot strand a successful connection; the late result is closed instead.
func contextDialer(dialer xproxy.Dialer) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		type result struct {
			conn net.Conn
			err  error
		}
		results := make(chan result, 1)
		go func() {
			conn, err := dialer.Dial(network, address)
			select {
			case results <- result{conn: conn, err: err}:
			case <-ctx.Done():
				if conn != nil {
					_ = conn.Close()
				}
			}
		}()
		select {
		case result := <-results:
			return result.conn, result.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// kafkaSeedHost extracts a logical hostname from the first seed for TLS SNI.
// Kafka seed syntax is host:port; the small scheme trim also tolerates common
// Kafka properties imports such as SSL://broker:9093.
func kafkaSeedHost(seeds []string) string {
	if len(seeds) == 0 {
		return ""
	}
	seed := strings.TrimSpace(seeds[0])
	if scheme := strings.Index(seed, "://"); scheme >= 0 {
		seed = seed[scheme+3:]
	}
	host, _, err := net.SplitHostPort(seed)
	if err == nil {
		return strings.Trim(host, "[]")
	}
	return strings.TrimSpace(seed)
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
		// issue #26：kgo 把拨号地址直接传给 GSSAPI 机制，runtime 兜底端点
		// 或 metadata 广告 IP 会让服务主体变成 kafka/<IP>；包装一层按 Java
		// getHostName 语义反解为域名（失败回退原地址）。
		return kgo.SASL(&canonicalKerberosMechanism{
			inner:    auth.AsMechanismWithClose(),
			resolver: net.DefaultResolver,
		}), nil
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

// closeLocked 关闭共享 client（调用方须持 entry.mu；状态面走 statusMu，
// 锁序 mu → statusMu）。
func (e *connEntry) closeLocked() {
	if e.client != nil {
		e.client.Close()
		e.client = nil
		e.fingerprint = ""
	}
	e.statusMu.Lock()
	e.status = "closed"
	e.statusMu.Unlock()
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

// withAdmin 执行 admin 回调（共享 client 指纹失效重建）。mu 仅覆盖 client
// 获取/重建；RPC 期间不持 mu（评审 M-6）：kgo.Client 并发安全，Close 与
// 在途请求并发也有文档保证（在途请求收 ErrClientClosed）——慢 admin RPC
// 不再串行阻塞 statuses 轮询与同连接的 client 构建。回调内禁止再取 entry.mu。
func (s *Service) withAdmin(connectionID string, fn func(client *kgo.Client) error) error {
	entry := s.lookup(connectionID)
	if entry == nil {
		return errConnectionNotFound(connectionID)
	}
	entry.mu.Lock()
	client, err := entry.adminClientLocked()
	entry.mu.Unlock()
	if err != nil {
		entry.setStatus("error", err.Error())
		return err
	}
	err = fn(client)
	if err == nil {
		entry.setStatus("connected", "")
	} else {
		entry.setStatus("error", err.Error())
	}
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
