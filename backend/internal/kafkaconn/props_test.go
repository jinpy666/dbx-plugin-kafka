package kafkaconn

// props_test.go：「粘贴 properties 导入」表驱动单测（Lane 3 conn-properties）。
// 覆盖：Java properties 解析（注释/=/:/空白分隔、续行奇偶、\uXXXX 与控制
// 转义、重复键）、jaas 提取（PLAIN/SCRAM/GSSAPI/OAUTHBEARER）、密码路由
//（只进 connSecrets 不进 Profile）、paste-wins 优先级、取值面外键忽略、
// SR basic auth 门控、lifecycle secret 通道端到端 + statuses 摘要透出。

import (
	"reflect"
	"strings"
	"testing"

	"io.dbx.kafka.plugin/internal/lifecycle"
)

func TestParseJavaProperties(t *testing.T) {
	tests := []struct {
		name string
		text string
		want map[string]string
	}{
		{
			name: "equals separator with comments and blanks",
			text: "# comment\n! also comment\n\na=b\n\nkey.value = trimmed value",
			want: map[string]string{"a": "b", "key.value": "trimmed value"},
		},
		{
			name: "colon separator keeps colons in value",
			text: "bootstrap.servers:k1:9092,k2:9092",
			want: map[string]string{"bootstrap.servers": "k1:9092,k2:9092"},
		},
		{
			name: "whitespace separator (java semantics)",
			text: "key value",
			want: map[string]string{"key": "value"},
		},
		{
			name: "escaped separators inside key",
			text: `a\=b\:c = v`,
			want: map[string]string{"a=b:c": "v"},
		},
		{
			name: "continuation odd trailing backslashes with leading whitespace skipped",
			text: "bootstrap.servers=b1:9092,\\\n  b2:9092,\\\n\tb3:9092",
			want: map[string]string{"bootstrap.servers": "b1:9092,b2:9092,b3:9092"},
		},
		{
			name: "even trailing backslashes are literal escapes not continuation",
			text: `path=C:\\dir`,
			want: map[string]string{"path": `C:\dir`},
		},
		{
			name: "unicode escape",
			text: `greeting=\u4f60\u597d world`,
			want: map[string]string{"greeting": "你好 world"},
		},
		{
			name: "invalid unicode escape kept literal",
			text: `bad=\uZZZZ value`,
			want: map[string]string{"bad": `\uZZZZ value`},
		},
		{
			name: "control escapes",
			text: "multi=a\\tb\\nc\\rd",
			want: map[string]string{"multi": "a\tb\nc\rd"},
		},
		{
			name: "duplicate key last wins",
			text: "k=first\nk=second",
			want: map[string]string{"k": "second"},
		},
		{
			name: "empty value preserved by parser",
			text: "empty=",
			want: map[string]string{"empty": ""},
		},
		{
			name: "crlf line endings",
			text: "a=1\r\nb=2\r\n",
			want: map[string]string{"a": "1", "b": "2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseJavaProperties(tt.text)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("parse = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestApplyPropertiesImportRoutesSecrets 校验核心映射表与凭据红线：
// password 类值只进 connSecrets（Profile 无密码字段）；paste-wins；
// 摘要只含键名；合并后通过 required_when 兜底校验。
func TestApplyPropertiesImportRoutesSecrets(t *testing.T) {
	paste := strings.Join([]string{
		"# cluster",
		"bootstrap.servers=b1:9092, b2:9092",
		"security.protocol=SASL_SSL",
		"sasl.mechanism=SCRAM-SHA-512",
		`sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="alice" password="s3cret!";`,
		"ssl.endpoint.identification.algorithm=none",
		"schema.registry.url=https://sr.example.com",
		"basic.auth.credentials.source=USER_INFO",
		"basic.auth.user.info=sruser:srpass:with:colons",
		"client.id=imported-client",
		"# unsupported java-keystore keys land in the ignore list",
		"ssl.truststore.location=/etc/kafka/truststore.jks",
		"ssl.truststore.password=changeit",
		"ssl.keystore.location=/etc/kafka/keystore.jks",
		"ssl.key.password=keypass",
		"request.timeout.ms=30000",
	}, "\n")

	profile := Profile{ID: "c1", SecurityProtocol: SecurityProtocolPlaintext, SASLMechanism: SASLMechanismPlain, Username: "form-user"}
	secrets := connSecrets{SASLPassword: "form-pass"}
	summary := applyPropertiesImport(&profile, &secrets, paste)
	profile = NormalizeProfile(profile)

	if got := strings.Join(profile.BootstrapServers, ","); got != "b1:9092,b2:9092" {
		t.Fatalf("bootstrap = %q", got)
	}
	if profile.SecurityProtocol != SecurityProtocolSASLSSL {
		t.Fatalf("securityProtocol = %q, want SASL_SSL (paste wins)", profile.SecurityProtocol)
	}
	if profile.SASLMechanism != SASLMechanismSCRAMSHA512 {
		t.Fatalf("saslMechanism = %q", profile.SASLMechanism)
	}
	if profile.Username != "alice" {
		t.Fatalf("username = %q (jaas extraction)", profile.Username)
	}
	// 密码只进 secrets；Profile 结构体没有密码字段（编译期保证），这里
	// 校验 secrets 路由 + 覆盖语义。
	if secrets.SASLPassword != "s3cret!" {
		t.Fatalf("sasl password routing = %q", secrets.SASLPassword)
	}
	if !profile.TLSInsecureSkipVerify {
		t.Fatal("endpoint identification none should map to skip verify")
	}
	if profile.SRURL != "https://sr.example.com" || profile.SchemaRegistry != SchemaRegistryConfluent {
		t.Fatalf("sr mapping = %q / %q", profile.SRURL, profile.SchemaRegistry)
	}
	if profile.SRUsername != "sruser" {
		t.Fatalf("sr username = %q", profile.SRUsername)
	}
	if secrets.SRPassword != "srpass:with:colons" {
		t.Fatalf("sr password routing = %q (first-colon split)", secrets.SRPassword)
	}
	if profile.ClientID != "imported-client" {
		t.Fatalf("clientId = %q", profile.ClientID)
	}

	if summary.Mapped != 9 {
		t.Fatalf("mapped = %d (%v)", summary.Mapped, summary.MappedKeys)
	}
	wantIgnored := []string{
		"request.timeout.ms",
		"ssl.key.password",
		"ssl.keystore.location",
		"ssl.truststore.location",
		"ssl.truststore.password",
	}
	if !reflect.DeepEqual(summary.IgnoredKeys, wantIgnored) || summary.Ignored != len(wantIgnored) {
		t.Fatalf("ignored = %d %v, want %v", summary.Ignored, summary.IgnoredKeys, wantIgnored)
	}
	// 摘要红线：任何值都不出现在摘要键名里。
	for _, key := range append(append([]string{}, summary.MappedKeys...), summary.IgnoredKeys...) {
		if strings.Contains(key, "s3cret") || strings.Contains(key, "srpass") || strings.Contains(key, "9092") {
			t.Fatalf("summary leaked a value: %q", key)
		}
	}
	// required_when 兜底校验：粘贴驱动的 SASL_SSL + SCRAM + jaas 凭据组合应通过。
	if err := validateRequiredCombination(profile, secrets); err != nil {
		t.Fatalf("validateRequiredCombination after import: %v", err)
	}
}

func TestApplyPropertiesImportCases(t *testing.T) {
	tests := []struct {
		name  string
		paste map[string]string
		check func(t *testing.T, profile Profile, secrets connSecrets, summary *PropertiesImportSummary)
	}{
		{
			name:  "invalid security protocol ignored and form value kept",
			paste: map[string]string{"security.protocol": "TLS_1_2"},
			check: func(t *testing.T, p Profile, _ connSecrets, s *PropertiesImportSummary) {
				if p.SecurityProtocol != SecurityProtocolSASLSSL {
					t.Fatalf("protocol = %q, want form value SASL_SSL kept", p.SecurityProtocol)
				}
				if len(s.IgnoredKeys) != 1 || s.IgnoredKeys[0] != "security.protocol" {
					t.Fatalf("ignored = %v", s.IgnoredKeys)
				}
			},
		},
		{
			name:  "invalid sasl mechanism ignored",
			paste: map[string]string{"sasl.mechanism": "OAUTHBEARER-X"},
			check: func(t *testing.T, p Profile, _ connSecrets, s *PropertiesImportSummary) {
				if p.SASLMechanism != SASLMechanismSCRAMSHA256 {
					t.Fatalf("mechanism = %q, want form value kept", p.SASLMechanism)
				}
				if s.Ignored != 1 {
					t.Fatalf("ignored = %d", s.Ignored)
				}
			},
		},
		{
			name:  "empty value skipped without counting",
			paste: map[string]string{"bootstrap.servers": "  ", "security.protocol": ""},
			check: func(t *testing.T, _ Profile, _ connSecrets, s *PropertiesImportSummary) {
				if s.Mapped+s.Ignored != 0 {
					t.Fatalf("empty values counted: mapped=%d ignored=%d", s.Mapped, s.Ignored)
				}
			},
		},
		{
			name:  "endpoint algorithm https keeps verification on",
			paste: map[string]string{"ssl.endpoint.identification.algorithm": "HTTPS"},
			check: func(t *testing.T, p Profile, _ connSecrets, s *PropertiesImportSummary) {
				if p.TLSInsecureSkipVerify {
					t.Fatal("HTTPS algorithm must not disable verification")
				}
				if s.Mapped != 1 {
					t.Fatalf("mapped = %d", s.Mapped)
				}
			},
		},
		{
			name: "PEM inline TLS keys routed (key to secrets)",
			paste: map[string]string{
				// properties 值内不能有物理换行（续行/转义另测）；PEM 实体
				// 以单行占位验证路由即可。
				"ssl.truststore.certificates":    "-----BEGIN CERTIFICATE----- CA -----END CERTIFICATE-----",
				"ssl.keystore.certificate.chain": "-----BEGIN CERTIFICATE----- CLIENT -----END CERTIFICATE-----",
				"ssl.keystore.key":               "-----BEGIN PRIVATE KEY----- KEY -----END PRIVATE KEY-----",
			},
			check: func(t *testing.T, p Profile, s connSecrets, summary *PropertiesImportSummary) {
				if !strings.Contains(p.TLSCACert, "CA") || !strings.Contains(p.TLSClientCert, "CLIENT") {
					t.Fatalf("PEM routing: ca=%q cert=%q", p.TLSCACert, p.TLSClientCert)
				}
				if !strings.Contains(s.TLSClientKey, "PRIVATE KEY") {
					t.Fatalf("client key not routed to secrets: %q", s.TLSClientKey)
				}
				if summary.Mapped != 3 {
					t.Fatalf("mapped = %d", summary.Mapped)
				}
			},
		},
		{
			name: "jaas for GSSAPI extracts principal and keytab",
			paste: map[string]string{
				"sasl.mechanism":              "GSSAPI",
				"sasl.jaas.config":            `com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true keyTab="/etc/security/kafka/client.keytab" principal="kafka-client@EXAMPLE.COM";`,
				"ssl.truststore.certificates": "CA",
			},
			check: func(t *testing.T, p Profile, _ connSecrets, s *PropertiesImportSummary) {
				if p.KerberosPrincipal != "kafka-client@EXAMPLE.COM" || p.KerberosKeytabPath != "/etc/security/kafka/client.keytab" {
					t.Fatalf("kerberos extraction: principal=%q keytab=%q", p.KerberosPrincipal, p.KerberosKeytabPath)
				}
				if s.Mapped != 3 { // mechanism + truststore cert + jaas
					t.Fatalf("mapped = %d (%v)", s.Mapped, s.MappedKeys)
				}
			},
		},
		{
			name: "jaas for OAUTHBEARER has no mapping target",
			paste: map[string]string{
				"sasl.mechanism":   "OAUTHBEARER",
				"sasl.jaas.config": `software.amazon.msk.auth.iam.IAMLoginModule required;`,
			},
			check: func(t *testing.T, _ Profile, _ connSecrets, s *PropertiesImportSummary) {
				if len(s.IgnoredKeys) != 1 || s.IgnoredKeys[0] != "sasl.jaas.config" {
					t.Fatalf("ignored = %v, want sasl.jaas.config", s.IgnoredKeys)
				}
			},
		},
		{
			name:  "jaas without usable fields is ignored",
			paste: map[string]string{"sasl.jaas.config": `org.apache.kafka.common.security.plain.PlainLoginModule required;`},
			check: func(t *testing.T, _ Profile, _ connSecrets, s *PropertiesImportSummary) {
				if s.Ignored != 1 || s.Mapped != 0 {
					t.Fatalf("mapped=%d ignored=%d", s.Mapped, s.Ignored)
				}
			},
		},
		{
			name: "basic auth user info gated by unsupported source",
			paste: map[string]string{
				"basic.auth.credentials.source": "URL",
				"basic.auth.user.info":          "u:p",
			},
			check: func(t *testing.T, p Profile, s connSecrets, summary *PropertiesImportSummary) {
				if p.SRUsername != "" || s.SRPassword != "" {
					t.Fatalf("user.info must be gated by USER_INFO source: user=%q pass=%q", p.SRUsername, s.SRPassword)
				}
				if summary.Ignored != 2 {
					t.Fatalf("ignored = %d (%v)", summary.Ignored, summary.IgnoredKeys)
				}
			},
		},
		{
			name: "single-quote jaas values and case-insensitive keys",
			paste: map[string]string{
				"Bootstrap.Servers": "b9:9092",
				"sasl.jaas.config":  `PlainLoginModule required username='bob' password='pw';`,
			},
			check: func(t *testing.T, p Profile, s connSecrets, _ *PropertiesImportSummary) {
				if !reflect.DeepEqual(p.BootstrapServers, []string{"b9:9092"}) {
					t.Fatalf("bootstrap = %v", p.BootstrapServers)
				}
				if p.Username != "bob" || s.SASLPassword != "pw" {
					t.Fatalf("jaas single-quote extraction: user=%q pass=%q", p.Username, s.SASLPassword)
				}
			},
		},
		{
			name:  "unicode escaped password in jaas",
			paste: map[string]string{"sasl.jaas.config": `PlainLoginModule required username="u" password="p\u0040ss";`},
			check: func(t *testing.T, _ Profile, s connSecrets, _ *PropertiesImportSummary) {
				if s.SASLPassword != "p@ss" {
					t.Fatalf("unicode in jaas value = %q", s.SASLPassword)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var text strings.Builder
			for key, value := range tt.paste {
				text.WriteString(key + "=" + value + "\n")
			}
			profile := Profile{ID: "c1", SecurityProtocol: SecurityProtocolSASLSSL, SASLMechanism: SASLMechanismSCRAMSHA256}
			secrets := connSecrets{}
			summary := applyPropertiesImport(&profile, &secrets, text.String())
			tt.check(t, profile, secrets, summary)
		})
	}
}

// TestPropertiesImportLifecycleEndToEnd：properties_import 从 lifecycle
// secret 通道（connection_secrets）进入合并流程并经 statuses 透出；config
// 通道中的同名键不被消费（红线：粘贴文本绝不走明文 config 落点）。
func TestPropertiesImportLifecycleEndToEnd(t *testing.T) {
	paste := strings.Join([]string{
		"bootstrap.servers=eb1:9092,eb2:9092",
		"security.protocol=SASL_SSL",
		"sasl.mechanism=SCRAM-SHA-256",
		`sasl.jaas.config=ScramLoginModule required username="eu" password="ep";`,
	}, "\n")

	params := &lifecycle.Params{
		Connection: lifecycle.Connection{
			ID:   "conn-props",
			Name: "imported",
			ExternalConfig: map[string]any{
				"bootstrap_servers": "form-broker:9092",
				"security_protocol": "PLAINTEXT",
			},
			Secrets: map[string]any{
				"properties_import": paste,
			},
		},
	}
	profile, secrets, err := NewProfileFromLifecycle(params)
	if err != nil {
		t.Fatalf("NewProfileFromLifecycle: %v", err)
	}
	if !reflect.DeepEqual(profile.BootstrapServers, []string{"eb1:9092", "eb2:9092"}) {
		t.Fatalf("bootstrap = %v (paste wins over form)", profile.BootstrapServers)
	}
	if profile.SecurityProtocol != SecurityProtocolSASLSSL || profile.SASLMechanism != SASLMechanismSCRAMSHA256 {
		t.Fatalf("protocol/mechanism = %s/%s", profile.SecurityProtocol, profile.SASLMechanism)
	}
	if secrets.SASLPassword != "ep" || profile.Username != "eu" {
		t.Fatalf("jaas routing: user=%q pass=%q", profile.Username, secrets.SASLPassword)
	}
	if profile.PropertiesImport == nil || profile.PropertiesImport.Mapped != 4 || profile.PropertiesImport.Ignored != 0 {
		t.Fatalf("summary = %+v", profile.PropertiesImport)
	}

	// statuses 摘要透出（Connect 复用同一解析路径）。
	service := NewService()
	if err := service.Connect(params); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	statuses := service.SnapshotStatuses()
	if len(statuses) != 1 || statuses[0].PropertiesImport == nil || statuses[0].PropertiesImport.Mapped != 4 {
		t.Fatalf("statuses summary = %+v", statuses)
	}

	// 红线：properties_import 只认 secret 通道；出现在 config 通道的明文
	// 粘贴不被消费（引导宿主/调用方走 secret binding，杜绝明文持久化）。
	paramsConfigChannel := &lifecycle.Params{
		Connection: lifecycle.Connection{
			ID:   "conn-cfg",
			Name: "plaintext-channel",
			ExternalConfig: map[string]any{
				"bootstrap_servers": "form-broker:9092",
				"properties_import": paste,
			},
		},
	}
	profileConfigChannel, _, err := NewProfileFromLifecycle(paramsConfigChannel)
	if err != nil {
		t.Fatalf("NewProfileFromLifecycle (config channel): %v", err)
	}
	if !reflect.DeepEqual(profileConfigChannel.BootstrapServers, []string{"form-broker:9092"}) {
		t.Fatalf("config-channel paste must not be consumed: %v", profileConfigChannel.BootstrapServers)
	}
	if profileConfigChannel.PropertiesImport != nil {
		t.Fatalf("config-channel paste must not produce a summary: %+v", profileConfigChannel.PropertiesImport)
	}
}
