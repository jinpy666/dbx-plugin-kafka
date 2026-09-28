package kafkaconn

// oauth_test.go：OAUTHBEARER 单测（Phase 3 §12.2.3 / §12.4 G 矩阵）：
// normalize 扩展、token provider 选择与 static/msk_iam 行为、SASL_SSL 约束
//（完整矩阵见 required_matrix_test.go）、lifecycle 映射与凭据红线。
// 全部离线：msk_iam 只验证构造与参数防御，签名发生在拨号期（不触网）。

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"io.dbx.kafka.plugin/internal/lifecycle"
)

func TestNormalizeSASLMechanismOAUTHBEARER(t *testing.T) {
	if got := NormalizeSASLMechanism(" OAUTHBEARER "); got != SASLMechanismOAUTHBEARER {
		t.Errorf("NormalizeSASLMechanism = %q, want %q", got, SASLMechanismOAUTHBEARER)
	}
	// 大小写不敏感（评审 L-12）：小写归一到规范形式，不再落进忽略清单。
	if got := NormalizeSASLMechanism("oauthbearer"); got != SASLMechanismOAUTHBEARER {
		t.Errorf("NormalizeSASLMechanism lowercase = %q, want %q", got, SASLMechanismOAUTHBEARER)
	}
	if got := NormalizeSASLMechanism("KERBEROS"); got != "" {
		t.Errorf("NormalizeSASLMechanism(unknown) = %q, want \"\"", got)
	}
}

func TestNormalizeOauthTokenSource(t *testing.T) {
	cases := map[string]string{
		"":               OauthTokenSourceMSKIAM, // 缺省 = msk_iam
		"msk_iam":        OauthTokenSourceMSKIAM,
		" MSK_IAM ":      OauthTokenSourceMSKIAM,
		"static_token":   OauthTokenSourceStatic,
		" STATIC_TOKEN ": OauthTokenSourceStatic,
		"static-token":   "", // 连字符不归一（与 manifest select 精确值对齐）
		"oidc":           "", // 未知 → 参数错（validateRequiredCombination -32602）
	}
	for input, want := range cases {
		if got := NormalizeOauthTokenSource(input); got != want {
			t.Errorf("NormalizeOauthTokenSource(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestNewOauthTokenProviderSelection(t *testing.T) {
	// static_token → 静态 provider；msk_iam/缺省 → MSK provider。
	provider, err := newOauthTokenProvider(
		Profile{OauthTokenSource: OauthTokenSourceStatic},
		connSecrets{OauthStaticToken: "tok-1"})
	if err != nil {
		t.Fatalf("static provider error = %v", err)
	}
	if _, ok := provider.(*staticOauthTokenProvider); !ok {
		t.Errorf("provider = %T, want *staticOauthTokenProvider", provider)
	}
	provider, err = newOauthTokenProvider(Profile{}, connSecrets{})
	if err != nil {
		t.Fatalf("default provider error = %v", err)
	}
	if _, ok := provider.(*mskIAMTokenProvider); !ok {
		t.Errorf("provider = %T, want *mskIAMTokenProvider", provider)
	}
	// 未知来源 → 拒绝。
	if _, err := newOauthTokenProvider(Profile{OauthTokenSource: "oidc"}, connSecrets{}); err == nil {
		t.Error("unknown token source accepted")
	}
	// msk 显式凭据只给一半 → 拒绝。
	if _, err := newOauthTokenProvider(
		Profile{MSKAccessKeyID: "ak"},
		connSecrets{}); err == nil || !strings.Contains(err.Error(), "together") {
		t.Errorf("partial AK/SK error = %v, want together error", err)
	}
}

func TestStaticOauthTokenProvider(t *testing.T) {
	provider := &staticOauthTokenProvider{token: "bearer-token"}
	token, expiry, err := provider.Token(context.Background())
	if err != nil {
		t.Fatalf("Token() error = %v", err)
	}
	if token != "bearer-token" {
		t.Errorf("token = %q", token)
	}
	// Expiration=0 表示不过期（§12.2.3）。
	if expiry != 0 {
		t.Errorf("expiry = %d, want 0 (不过期)", expiry)
	}
	if _, _, err := (&staticOauthTokenProvider{}).Token(context.Background()); err == nil {
		t.Error("empty static token accepted")
	}
}

func TestMSKIAMTokenProviderValidation(t *testing.T) {
	// region 缺失（防御；normalize/校验层已拦）→ 快速失败。
	provider := &mskIAMTokenProvider{}
	if _, _, err := provider.Token(context.Background()); err == nil || !strings.Contains(err.Error(), "mskRegion") {
		t.Errorf("Token() without region error = %v, want mskRegion error", err)
	}
	// 显式凭据路径构造（签名在拨号期，构造不触网）。
	provider = &mskIAMTokenProvider{region: "us-east-1", accessKeyID: "ak", secretAccessKey: "sk", sessionToken: "st"}
	if provider.region != "us-east-1" || provider.accessKeyID == "" || provider.secretAccessKey == "" {
		t.Errorf("provider = %+v", provider)
	}
}

func TestBuildSASLOptOAUTHBEARER(t *testing.T) {
	profile := Profile{SecurityProtocol: SecurityProtocolSASLSSL, SASLMechanism: SASLMechanismOAUTHBEARER}
	// static_token：机制可构建。
	staticProfile := profile
	staticProfile.OauthTokenSource = OauthTokenSourceStatic
	opt, err := buildSASLOpt(staticProfile, connSecrets{OauthStaticToken: "tok"})
	if err != nil || opt == nil {
		t.Errorf("buildSASLOpt(static token) = %v, %v", opt, err)
	}
	// static_token 缺 token：构造期放行（必填由 connect 期 -32602 矩阵拦截），
	// token 获取期拒绝。
	provider, perr := newOauthTokenProvider(
		Profile{OauthTokenSource: OauthTokenSourceStatic}, connSecrets{})
	if perr != nil {
		t.Fatalf("static provider construction error = %v", perr)
	}
	if _, _, err := provider.Token(context.Background()); err == nil {
		t.Error("empty static token accepted at token fetch")
	}
	// msk_iam：机制可构建（签名延迟到 SASL 会话）。
	profile.OauthTokenSource = OauthTokenSourceMSKIAM
	profile.MSKRegion = "us-east-1"
	opt, err = buildSASLOpt(profile, connSecrets{})
	if err != nil || opt == nil {
		t.Errorf("buildSASLOpt(msk_iam) = %v, %v", opt, err)
	}
}

func TestNewProfileFromLifecycleOAUTHBEARER(t *testing.T) {
	params, err := lifecycle.Parse([]byte(`{
	  "connection": {
	    "id": "conn-msk",
	    "external_config": {
	      "bootstrap_servers": "k1:9092",
	      "security_protocol": "SASL_SSL",
	      "sasl_mechanism": "OAUTHBEARER",
	      "oauth_token_source": "msk_iam",
	      "msk_region": "us-west-2",
	      "msk_access_key_id": "AKIA-TEST"
	    },
	    "connection_secrets": {
	      "msk_secret_access_key": "SK-TEST",
	      "msk_session_token": "STS-TEST"
	    }
	  }
	}`))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	profile, secrets, err := NewProfileFromLifecycle(params)
	if err != nil {
		t.Fatalf("NewProfileFromLifecycle() error = %v", err)
	}
	if profile.SASLMechanism != SASLMechanismOAUTHBEARER {
		t.Errorf("saslMechanism = %q", profile.SASLMechanism)
	}
	if profile.OauthTokenSource != OauthTokenSourceMSKIAM || profile.MSKRegion != "us-west-2" || profile.MSKAccessKeyID != "AKIA-TEST" {
		t.Errorf("profile oauth fields = %+v", profile)
	}
	// 凭据红线：secret 只进 connSecrets，不进 Profile。
	if secrets.MSKSecretAccessKey != "SK-TEST" || secrets.MSKSessionToken != "STS-TEST" {
		t.Errorf("secrets = %+v", secrets)
	}
	// 空 oauth_token_source 缺省归一为 msk_iam（Profile json 透出该值）。
	params, _ = lifecycle.Parse([]byte(`{
	  "connection": {
	    "id": "conn-default-src",
	    "external_config": {
	      "bootstrap_servers": "k1:9092",
	      "security_protocol": "SASL_SSL",
	      "sasl_mechanism": "OAUTHBEARER",
	      "msk_region": "us-east-1"
	    }
	  }
	}`))
	profile, _, err = NewProfileFromLifecycle(params)
	if err != nil {
		t.Fatalf("NewProfileFromLifecycle(default source) error = %v", err)
	}
	if profile.OauthTokenSource != OauthTokenSourceMSKIAM {
		t.Errorf("default token source = %q, want msk_iam", profile.OauthTokenSource)
	}
}

// TestOauthProfileJSONShape 守卫 Profile 新字段 camelCase json 名（§12.2.3
// 冻结形状：oauthTokenSource/mskRegion/mskAccessKeyID）。
func TestOauthProfileJSONShape(t *testing.T) {
	raw, err := json.Marshal(Profile{
		OauthTokenSource: "msk_iam",
		MSKRegion:        "us-east-1",
		MSKAccessKeyID:   "AK",
	})
	if err != nil {
		t.Fatalf("marshal profile: %v", err)
	}
	for _, want := range []string{`"oauthTokenSource"`, `"mskRegion"`, `"mskAccessKeyID"`} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("profile JSON %s missing key %s", raw, want)
		}
	}
	// 凭据字段不得出现在 Profile JSON（红线的编译期形状守卫）。
	for _, forbidden := range []string{"StaticToken", "SecretAccessKey", "SessionToken"} {
		if strings.Contains(string(raw), forbidden) {
			t.Errorf("profile JSON must not contain %s: %s", forbidden, raw)
		}
	}
}

// S-SASL-CASE（评审 L-12）：机制归一化大小写不敏感——粘贴 "scram-sha-256"
// 不应整键落进忽略清单。
func TestNormalizeSASLMechanismCaseInsensitive(t *testing.T) {
	for raw, want := range map[string]string{
		"plain":          "PLAIN",
		"SCRAM-sha-256":  "SCRAM-SHA-256",
		" scram-sha-512": "SCRAM-SHA-512",
		"OauthBearer":    "OAUTHBEARER",
	} {
		if got := NormalizeSASLMechanism(raw); got != want {
			t.Errorf("NormalizeSASLMechanism(%q) = %q, want %q", raw, got, want)
		}
	}
	if got := NormalizeSASLMechanism("bogus"); got != "" {
		t.Errorf("bogus mechanism = %q, want empty", got)
	}
}

// S-DEADLINE-WRAP（评审 L-8）：isDeadline 用 errors.Is 识别包装错误——
// fmt.Errorf("%w") 包装的超时此前不被识别。
func TestIsDeadlineWrapped(t *testing.T) {
	if !isDeadline(fmt.Errorf("poll: %w", context.DeadlineExceeded)) {
		t.Fatal("wrapped DeadlineExceeded must be recognized")
	}
	if !isDeadline(fmt.Errorf("poll: %w", context.Canceled)) {
		t.Fatal("wrapped Canceled must be recognized")
	}
	if isDeadline(errors.New("broker said no")) {
		t.Fatal("plain error must not match")
	}
}
