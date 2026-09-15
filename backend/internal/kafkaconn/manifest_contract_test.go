// manifest 契约守卫：连接 provider 字段与七语 label 必须与后端消费的
// binding/取值面对齐（IMPL_PLAN §4）。并行实施期 manifest 由 C 路产出，
// 文件缺失时 t.Skip("manifest not ready")（收口主线重跑本测试）。
package kafkaconn

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type manifestField struct {
	Key         string `json:"key"`
	Label       string `json:"label"`
	Binding     string `json:"binding"`
	Required    bool   `json:"required"`
	VisibleWhen *struct {
		Field string   `json:"field"`
		OneOf []string `json:"one_of"`
	} `json:"visible_when"`
	RequiredWhen *struct {
		Field string   `json:"field"`
		OneOf []string `json:"one_of"`
	} `json:"required_when"`
	Options []struct {
		Label string `json:"label"`
		Value string `json:"value"`
	} `json:"options"`
	Default any `json:"default"`
}

type manifestDoc struct {
	ID            string `json:"id"`
	Contributions []struct {
		Type         string          `json:"type"`
		ID           string          `json:"id"`
		DatabaseType string          `json:"database_type"`
		Capabilities []string        `json:"capabilities"`
		Fields       []manifestField `json:"fields"`
	} `json:"contributions"`
	Localizations map[string]struct {
		Contributions map[string]struct {
			Fields map[string]struct {
				Label       string            `json:"label"`
				Description string            `json:"description"`
				Options     map[string]string `json:"options"`
			} `json:"fields"`
		} `json:"contributions"`
	} `json:"localizations"`
}

func loadKafkaManifest(t *testing.T) *manifestDoc {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "manifest.json"))
	if err != nil {
		if os.IsNotExist(err) || strings.Contains(err.Error(), "no such file") {
			t.Skip("manifest not ready")
		}
		t.Fatalf("read manifest.json: %v", err)
	}
	var doc manifestDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("parse manifest.json: %v", err)
	}
	return &doc
}

func kafkaProviderFields(doc *manifestDoc) map[string]manifestField {
	fields := map[string]manifestField{}
	for _, contribution := range doc.Contributions {
		if contribution.Type != "connection-provider" {
			continue
		}
		for _, field := range contribution.Fields {
			fields[field.Key] = field
		}
	}
	return fields
}

func TestManifestConnectionProvider(t *testing.T) {
	doc := loadKafkaManifest(t)
	var provider *struct {
		ID           string
		DatabaseType string
		Capabilities []string
	}
	for _, contribution := range doc.Contributions {
		if contribution.Type == "connection-provider" {
			provider = &struct {
				ID           string
				DatabaseType string
				Capabilities []string
			}{contribution.ID, contribution.DatabaseType, contribution.Capabilities}
		}
	}
	if provider == nil {
		t.Fatal("connection-provider contribution missing")
	}
	if provider.ID != "io.dbx.kafka.connection" {
		t.Errorf("provider id = %q, want io.dbx.kafka.connection", provider.ID)
	}
	if provider.DatabaseType != "kafka" {
		t.Errorf("database_type = %q, want kafka", provider.DatabaseType)
	}
	for _, capability := range []string{"test", "connect", "disconnect"} {
		found := false
		for _, item := range provider.Capabilities {
			if item == capability {
				found = true
			}
		}
		if !found {
			t.Errorf("capability %q missing", capability)
		}
	}
}

// TestManifestBackendFieldContract 守卫后端 NewProfileFromLifecycle 消费的
// 字段 key/binding 与 manifest 一致（config vs secret 落点，§4）。
func TestManifestBackendFieldContract(t *testing.T) {
	doc := loadKafkaManifest(t)
	fields := kafkaProviderFields(doc)

	// 后端 ConfigString/ConfigStringSlice/ConfigBool 消费的 config 字段。
	wantConfig := []string{
		"bootstrap_servers", "security_protocol", "sasl_mechanism", "sasl_username",
		"tls_ca_cert", "tls_client_cert", "tls_insecure_skip_verify", "client_id",
		"read_only", "allow_delete",
		// Phase 2（IMPL_PLAN §0.2）：连接来源 / ZK / Kerberos / SR；
		// Phase 3（AWS Glue SR）：region/registry/auth_mode/access_key_id。
		// schema_registry 为 SR 决策开关（agent I：条件显隐 + 必填校验）。
		"connection_source", "zk_servers", "schema_registry",
		"kerberos_service_name", "kerberos_realm", "kerberos_principal",
		"kerberos_keytab_path", "kerberos_krb5_conf_path",
		"sr_url", "sr_username",
		"glue_region", "glue_registry_name", "glue_auth_mode", "glue_access_key_id",
		// Phase 3（OAUTHBEARER，§12.2.3）：token 来源/MSK region/显式 AK。
		"oauth_token_source", "msk_region", "msk_access_key_id",
	}
	for _, key := range wantConfig {
		field, ok := fields[key]
		if !ok {
			t.Fatalf("manifest field %q missing", key)
		}
		if field.Binding != "config" {
			t.Errorf("%s binding = %q, want config", key, field.Binding)
		}
		if field.Label == "" {
			t.Errorf("%s label empty", key)
		}
	}

	// 凭据红线：sasl_password / tls_client_key / sr_password /
	// glue_secret_access_key / glue_session_token / msk_secret_access_key /
	// msk_session_token / oauth_static_token 必须 secret binding；
	// properties_import（Lane 3 粘贴导入）同样 secret binding —— 粘贴文本中
	// 的密码绝不能以明文持久化。
	for _, key := range []string{"sasl_password", "tls_client_key", "sr_password",
		"glue_secret_access_key", "glue_session_token",
		"msk_secret_access_key", "msk_session_token", "oauth_static_token",
		"properties_import"} {
		field, ok := fields[key]
		if !ok {
			t.Fatalf("manifest field %q missing", key)
		}
		if field.Binding != "secret" {
			t.Fatalf("%s binding = %q, want secret (凭据红线)", key, field.Binding)
		}
	}

	// display_name 是 name binding 且 required。
	if field, ok := fields["display_name"]; !ok || field.Binding != "name" || !field.Required {
		t.Errorf("display_name = %+v, want name binding + required", fields["display_name"])
	}

	// security_protocol 取值面与后端 NormalizeSecurityProtocol 一致。
	protocolValues := map[string]bool{}
	for _, option := range fields["security_protocol"].Options {
		protocolValues[option.Value] = true
	}
	for _, want := range []string{"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"} {
		if !protocolValues[want] {
			t.Errorf("security_protocol options missing %q", want)
		}
	}
	// sasl_mechanism 取值面与 buildSASLOpt 一致。
	mechanismValues := map[string]bool{}
	for _, option := range fields["sasl_mechanism"].Options {
		mechanismValues[option.Value] = true
	}
	for _, want := range []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI", "OAUTHBEARER"} {
		if !mechanismValues[want] {
			t.Errorf("sasl_mechanism options missing %q", want)
		}
	}

	// Phase 2：connection_source 取值面与 NormalizeConnectionSource 一致。
	sourceValues := map[string]bool{}
	for _, option := range fields["connection_source"].Options {
		sourceValues[option.Value] = true
	}
	for _, want := range []string{"bootstrap", "zookeeper"} {
		if !sourceValues[want] {
			t.Errorf("connection_source options missing %q", want)
		}
	}
	// schema_registry 取值面与 NormalizeSchemaRegistry / resolveSchemaProvider
	// 一致；default none（开关未显式选择时不启用任何 SR 后端）。
	registryValues := map[string]bool{}
	for _, option := range fields["schema_registry"].Options {
		registryValues[option.Value] = true
	}
	for _, want := range []string{"none", "confluent", "aws_glue"} {
		if !registryValues[want] {
			t.Errorf("schema_registry options missing %q", want)
		}
	}
	if fields["schema_registry"].Default != "none" {
		t.Errorf("schema_registry default = %v, want \"none\"", fields["schema_registry"].Default)
	}
	// Phase 3：glue_auth_mode 取值面与 newGlueClient 一致（default | static；
	// tinyrdm 的 aws-profile 模式 sidecar 不提供）。
	authModeValues := map[string]bool{}
	for _, option := range fields["glue_auth_mode"].Options {
		authModeValues[option.Value] = true
	}
	for _, want := range []string{"default", "static"} {
		if !authModeValues[want] {
			t.Errorf("glue_auth_mode options missing %q", want)
		}
	}
	// --- 显隐/必填矩阵（agent I）：单字段条件，与宿主 pluginFieldConditions
	// 语义（visible_when/required_when ∈ {field, one_of[]}）对齐 ---
	// schema_registry=confluent → sr_* 三件可见，sr_url 必填。
	for _, key := range []string{"sr_url", "sr_username", "sr_password"} {
		cond := fields[key].VisibleWhen
		if cond == nil || cond.Field != "schema_registry" || len(cond.OneOf) != 1 || cond.OneOf[0] != "confluent" {
			t.Errorf("%s visible_when = %+v, want schema_registry one_of [confluent]", key, cond)
		}
	}
	if rw := fields["sr_url"].RequiredWhen; rw == nil || rw.Field != "schema_registry" ||
		len(rw.OneOf) != 1 || rw.OneOf[0] != "confluent" {
		t.Errorf("sr_url required_when = %+v, want schema_registry one_of [confluent]", fields["sr_url"].RequiredWhen)
	}
	for _, key := range []string{"sr_username", "sr_password"} {
		if fields[key].RequiredWhen != nil {
			t.Errorf("%s required_when = %+v, want nil (可选)", key, fields[key].RequiredWhen)
		}
	}
	// schema_registry=aws_glue → glue 三件可见，region/registry_name 必填。
	for _, key := range []string{"glue_region", "glue_registry_name", "glue_auth_mode"} {
		cond := fields[key].VisibleWhen
		if cond == nil || cond.Field != "schema_registry" || len(cond.OneOf) != 1 || cond.OneOf[0] != "aws_glue" {
			t.Errorf("%s visible_when = %+v, want schema_registry one_of [aws_glue]", key, cond)
		}
	}
	for _, key := range []string{"glue_region", "glue_registry_name"} {
		rw := fields[key].RequiredWhen
		if rw == nil || rw.Field != "schema_registry" || len(rw.OneOf) != 1 || rw.OneOf[0] != "aws_glue" {
			t.Errorf("%s required_when = %+v, want schema_registry one_of [aws_glue]", key, rw)
		}
	}
	if fields["glue_auth_mode"].RequiredWhen != nil {
		t.Errorf("glue_auth_mode required_when = %+v, want nil", fields["glue_auth_mode"].RequiredWhen)
	}
	// glue_auth_mode=static → AK/SK/token 可见，AK/SK 必填，token 可选。
	for _, key := range []string{"glue_access_key_id", "glue_secret_access_key", "glue_session_token"} {
		cond := fields[key].VisibleWhen
		if cond == nil || cond.Field != "glue_auth_mode" || len(cond.OneOf) != 1 || cond.OneOf[0] != "static" {
			t.Errorf("%s visible_when = %+v, want glue_auth_mode one_of [static]", key, cond)
		}
	}
	for _, key := range []string{"glue_access_key_id", "glue_secret_access_key"} {
		rw := fields[key].RequiredWhen
		if rw == nil || rw.Field != "glue_auth_mode" || len(rw.OneOf) != 1 || rw.OneOf[0] != "static" {
			t.Errorf("%s required_when = %+v, want glue_auth_mode one_of [static]", key, rw)
		}
	}
	if fields["glue_session_token"].RequiredWhen != nil {
		t.Errorf("glue_session_token required_when = %+v, want nil (可选)", fields["glue_session_token"].RequiredWhen)
	}
	// Phase 3（OAUTHBEARER，§12.2.3）：联动链 sasl_mechanism ∈ [OAUTHBEARER]
	// → oauth_token_source → msk_* 组（msk_iam）/ oauth_static_token
	//（static_token）。
	oauthSource := fields["oauth_token_source"]
	if oauthSource.VisibleWhen == nil || oauthSource.VisibleWhen.Field != "sasl_mechanism" ||
		len(oauthSource.VisibleWhen.OneOf) != 1 || oauthSource.VisibleWhen.OneOf[0] != "OAUTHBEARER" {
		t.Errorf("oauth_token_source visible_when = %+v, want sasl_mechanism one_of [OAUTHBEARER]", oauthSource.VisibleWhen)
	}
	if rw := oauthSource.RequiredWhen; rw == nil || rw.Field != "sasl_mechanism" || !slicesEqual(rw.OneOf, []string{"OAUTHBEARER"}) {
		t.Errorf("oauth_token_source must require an explicit token source for OAUTHBEARER")
	}
	// 不声明 default：宿主条件求值会拿字段 default 参与下游 visible_when，
	// 回填 msk_iam 会让 msk_region 在纯 PLAINTEXT/SCRAM 表单上幽灵必填
	//（宿主旧版不级联可见性时直接死锁保存按钮）；后端空值语义仍回退
	// msk_iam（NormalizeOauthTokenSource）。
	if oauthSource.Default != nil {
		t.Errorf("oauth_token_source default = %v, want nil (must stay unset)", oauthSource.Default)
	}
	tokenSourceValues := map[string]bool{}
	for _, option := range fields["oauth_token_source"].Options {
		tokenSourceValues[option.Value] = true
	}
	for _, want := range []string{"msk_iam", "static_token"} {
		if !tokenSourceValues[want] {
			t.Errorf("oauth_token_source options missing %q", want)
		}
	}
	for _, key := range []string{"msk_region", "msk_access_key_id", "msk_secret_access_key", "msk_session_token"} {
		cond := fields[key].VisibleWhen
		if cond == nil || cond.Field != "oauth_token_source" || len(cond.OneOf) != 1 || cond.OneOf[0] != "msk_iam" {
			t.Errorf("%s visible_when = %+v, want oauth_token_source one_of [msk_iam]", key, cond)
		}
	}
	if rw := fields["msk_region"].RequiredWhen; rw == nil || rw.Field != "oauth_token_source" ||
		len(rw.OneOf) != 1 || rw.OneOf[0] != "msk_iam" {
		t.Errorf("msk_region required_when = %+v, want oauth_token_source one_of [msk_iam]", fields["msk_region"].RequiredWhen)
	}
	for _, key := range []string{"msk_access_key_id", "msk_secret_access_key", "msk_session_token"} {
		if fields[key].RequiredWhen != nil {
			t.Errorf("%s required_when = %+v, want nil (可选)", key, fields[key].RequiredWhen)
		}
	}
	staticToken := fields["oauth_static_token"]
	if staticToken.VisibleWhen == nil || staticToken.VisibleWhen.Field != "oauth_token_source" ||
		len(staticToken.VisibleWhen.OneOf) != 1 || staticToken.VisibleWhen.OneOf[0] != "static_token" {
		t.Errorf("oauth_static_token visible_when = %+v, want oauth_token_source one_of [static_token]", staticToken.VisibleWhen)
	}
	if rw := staticToken.RequiredWhen; rw == nil || rw.Field != "oauth_token_source" ||
		len(rw.OneOf) != 1 || rw.OneOf[0] != "static_token" {
		t.Errorf("oauth_static_token required_when = %+v, want oauth_token_source one_of [static_token]", staticToken.RequiredWhen)
	}
	// zk_servers 挂在 connection_source ∈ [zookeeper]。
	zk := fields["zk_servers"]
	if zk.VisibleWhen == nil || zk.VisibleWhen.Field != "connection_source" ||
		len(zk.VisibleWhen.OneOf) != 1 || zk.VisibleWhen.OneOf[0] != "zookeeper" {
		t.Errorf("zk_servers visible_when = %+v, want connection_source one_of [zookeeper]", zk.VisibleWhen)
	}
	// Kerberos 字段挂在 sasl_mechanism ∈ [GSSAPI]。
	for _, key := range []string{"kerberos_service_name", "kerberos_realm", "kerberos_principal",
		"kerberos_keytab_path", "kerberos_krb5_conf_path"} {
		field := fields[key]
		if field.VisibleWhen == nil || field.VisibleWhen.Field != "sasl_mechanism" ||
			len(field.VisibleWhen.OneOf) != 1 || field.VisibleWhen.OneOf[0] != "GSSAPI" {
			t.Errorf("%s visible_when = %+v, want sasl_mechanism one_of [GSSAPI]", key, field.VisibleWhen)
		}
	}

	// visible_when：SASL 字段挂在 security_protocol ∈ SASL 两态上。
	saslProtocols := map[string]bool{"SASL_PLAINTEXT": true, "SASL_SSL": true}
	for _, key := range []string{"sasl_mechanism"} {
		field := fields[key]
		if field.VisibleWhen == nil || field.VisibleWhen.Field != "security_protocol" {
			t.Errorf("%s visible_when missing security_protocol", key)
			continue
		}
		for _, value := range field.VisibleWhen.OneOf {
			if !saslProtocols[value] {
				t.Errorf("%s visible_when contains non-SASL protocol %q", key, value)
			}
		}
	}
	// required_when：SASL 账密挂在 sasl_mechanism ∈ PLAIN/SCRAM（GSSAPI 不需要账密）。
	saslCredMechanisms := []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}
	for _, key := range []string{"sasl_username", "sasl_password"} {
		visible := fields[key].VisibleWhen
		if visible == nil || visible.Field != "sasl_mechanism" || !slicesEqual(visible.OneOf, saslCredMechanisms) {
			t.Errorf("%s must only appear for password-based SASL mechanisms", key)
		}
		rw := fields[key].RequiredWhen
		if rw == nil || rw.Field != "sasl_mechanism" || !slicesEqual(rw.OneOf, saslCredMechanisms) {
			t.Errorf("%s required_when = %+v, want sasl_mechanism one_of %v", key, rw, saslCredMechanisms)
		}
	}
	if fields["sasl_mechanism"].RequiredWhen != nil {
		t.Errorf("sasl_mechanism required_when = %+v, want nil", fields["sasl_mechanism"].RequiredWhen)
	}
	// required_when：GSSAPI 时 principal/keytab 必填（service_name 有默认值、
	// realm/krb5 可选）。
	for _, key := range []string{"kerberos_principal", "kerberos_keytab_path"} {
		rw := fields[key].RequiredWhen
		if rw == nil || rw.Field != "sasl_mechanism" || len(rw.OneOf) != 1 || rw.OneOf[0] != "GSSAPI" {
			t.Errorf("%s required_when = %+v, want sasl_mechanism one_of [GSSAPI]", key, rw)
		}
	}
	for _, key := range []string{"kerberos_service_name", "kerberos_realm", "kerberos_krb5_conf_path"} {
		if fields[key].RequiredWhen != nil {
			t.Errorf("%s required_when = %+v, want nil (可选)", key, fields[key].RequiredWhen)
		}
	}
}

// slicesEqual 是测试内 string 切片比较（顺序敏感）。
func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestManifestSevenLanguages 七语 localizations 完整性（M0 硬性规则）。
func TestManifestSevenLanguages(t *testing.T) {
	doc := loadKafkaManifest(t)
	wantLocales := []string{"en", "zh-CN", "zh-TW", "es", "it", "ja", "pt-BR"}
	if len(doc.Localizations) != len(wantLocales) {
		t.Fatalf("localizations = %d blocks, want %d", len(doc.Localizations), len(wantLocales))
	}
	for _, lang := range wantLocales {
		loc, ok := doc.Localizations[lang]
		if !ok {
			t.Fatalf("localization %q missing", lang)
		}
		connFields := loc.Contributions["io.dbx.kafka.connection"].Fields
		for _, key := range append([]string(nil),
			"display_name", "bootstrap_servers", "security_protocol", "sasl_mechanism",
			"sasl_username", "sasl_password", "tls_ca_cert", "tls_client_cert",
			"tls_client_key", "tls_insecure_skip_verify", "client_id", "read_only", "allow_delete",
			"connection_source", "zk_servers", "kerberos_service_name", "kerberos_realm",
			"kerberos_principal", "kerberos_keytab_path", "kerberos_krb5_conf_path",
			"sr_url", "sr_username", "sr_password",
			"glue_region", "glue_registry_name", "glue_auth_mode",
			"glue_access_key_id", "glue_secret_access_key", "glue_session_token") {
			entry, ok := connFields[key]
			if !ok || entry.Label == "" {
				t.Fatalf("localization %s/%s label missing", lang, key)
			}
		}
		securityLoc := connFields["security_protocol"]
		for _, want := range []string{"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"} {
			if securityLoc.Options[want] == "" {
				t.Fatalf("localization %s security_protocol option %q label missing", lang, want)
			}
		}
		mechanismLoc := connFields["sasl_mechanism"]
		for _, want := range []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI", "OAUTHBEARER"} {
			if mechanismLoc.Options[want] == "" {
				t.Fatalf("localization %s sasl_mechanism option %q label missing", lang, want)
			}
		}
		sourceLoc := connFields["connection_source"]
		for _, want := range []string{"bootstrap", "zookeeper"} {
			if sourceLoc.Options[want] == "" {
				t.Fatalf("localization %s connection_source option %q label missing", lang, want)
			}
		}
		// Phase 2/3 新字段逐字段七语 label/description（schema_registry 开关
		// 同样七语；agent I 条件显隐改造；Phase 3 OAUTHBEARER 六字段；
		// Lane 3 properties_import 粘贴导入字段）。
		for _, key := range []string{"connection_source", "zk_servers", "schema_registry",
			"kerberos_service_name", "kerberos_realm", "kerberos_principal",
			"kerberos_keytab_path", "kerberos_krb5_conf_path",
			"sr_url", "sr_username", "sr_password",
			"glue_region", "glue_registry_name", "glue_auth_mode",
			"glue_access_key_id", "glue_secret_access_key", "glue_session_token",
			"oauth_token_source", "msk_region", "msk_access_key_id",
			"msk_secret_access_key", "msk_session_token", "oauth_static_token",
			"properties_import"} {
			entry, ok := connFields[key]
			if !ok || entry.Label == "" || entry.Description == "" {
				t.Fatalf("localization %s/%s label/description missing", lang, key)
			}
		}
		// oauth_token_source 选项七语。
		tokenSourceLoc := connFields["oauth_token_source"]
		for _, want := range []string{"msk_iam", "static_token"} {
			if tokenSourceLoc.Options[want] == "" {
				t.Fatalf("localization %s oauth_token_source option %q label missing", lang, want)
			}
		}
		// glue_auth_mode 选项七语。
		authModeLoc := connFields["glue_auth_mode"]
		for _, want := range []string{"default", "static"} {
			if authModeLoc.Options[want] == "" {
				t.Fatalf("localization %s glue_auth_mode option %q label missing", lang, want)
			}
		}
		// schema_registry 选项七语（none/confluent/aws_glue）。
		registryLoc := connFields["schema_registry"]
		for _, want := range []string{"none", "confluent", "aws_glue"} {
			if registryLoc.Options[want] == "" {
				t.Fatalf("localization %s schema_registry option %q label missing", lang, want)
			}
		}
	}
}

// TestManifestIDMatchesPluginID manifest 顶层 id 与 sidecar Metadata 一致。
func TestManifestIDMatchesPluginID(t *testing.T) {
	doc := loadKafkaManifest(t)
	if doc.ID != "io.dbx.kafka" {
		t.Errorf("manifest id = %q, want io.dbx.kafka", doc.ID)
	}
}
