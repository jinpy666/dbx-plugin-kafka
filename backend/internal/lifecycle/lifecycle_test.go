package lifecycle

import (
	"encoding/json"
	"testing"
)

func mustParse(t *testing.T, raw string) *Params {
	t.Helper()
	params, err := Parse(json.RawMessage(raw))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	return params
}

// 完整形状对照 M0 文档 §3.1（kafka 字段按 manifest §4）。
const fullParams = `{
  "provider": { "id": "io.dbx.kafka.connection", "databaseType": "kafka" },
  "connection": {
    "id": " conn-1 ",
    "name": "prod-kafka",
    "username": "",
    "password": "",
    "external_config": {
      "bootstrap_servers": "k1:9092, k2:9092\nk3:9092",
      "security_protocol": "SASL_SSL",
      "sasl_mechanism": "SCRAM-SHA-256",
      "sasl_username": "app",
      "tls_insecure_skip_verify": true,
      "read_only": true,
      "allow_delete": false
    },
    "connection_secrets": { "sasl_password": " s3cret " }
  },
  "runtime": { "host": "127.0.0.1", "port": 9092 },
  "operationId": "op-42"
}`

func TestParseFullShape(t *testing.T) {
	params := mustParse(t, fullParams)

	if got := params.Provider.ID; got != "io.dbx.kafka.connection" {
		t.Errorf("provider.id = %q", got)
	}
	if got := params.Provider.DatabaseType; got != "kafka" {
		t.Errorf("provider.databaseType = %q", got)
	}
	if got := params.ConnectionID(); got != "conn-1" {
		t.Errorf("connection.id trim = %q", got)
	}
	if got := params.Runtime.Host; got != "127.0.0.1" {
		t.Errorf("runtime.host = %q", got)
	}
	if got := params.Runtime.Port; got != 9092 {
		t.Errorf("runtime.port = %d", got)
	}
	if got := params.OperationID; got != "op-42" {
		t.Errorf("operationId = %q", got)
	}
}

func TestConfigGetters(t *testing.T) {
	params := mustParse(t, fullParams)

	if got := params.ConfigString("security_protocol"); got != "SASL_SSL" {
		t.Errorf("ConfigString(security_protocol) = %q", got)
	}
	if got := params.ConfigString("missing"); got != "" {
		t.Errorf("ConfigString(missing) = %q", got)
	}
	if !params.ConfigBool("tls_insecure_skip_verify") {
		t.Error("ConfigBool(tls_insecure_skip_verify) = false, want true")
	}
	if params.ConfigBool("allow_delete") {
		t.Error("ConfigBool(allow_delete) = true, want false")
	}

	// textarea 换行 + 逗号混拆，空项丢弃。
	got := params.ConfigStringSlice("bootstrap_servers")
	want := []string{"k1:9092", "k2:9092", "k3:9092"}
	if len(got) != len(want) {
		t.Fatalf("ConfigStringSlice(bootstrap_servers) = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("ConfigStringSlice()[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	// 数组形式（[]any）同样支持。
	arrayParams := mustParse(t, `{"connection":{"external_config":{"bootstrap_servers":["a:9092"," b:9092 ",""]}}}`)
	arrayGot := arrayParams.ConfigStringSlice("bootstrap_servers")
	if len(arrayGot) != 2 || arrayGot[0] != "a:9092" || arrayGot[1] != "b:9092" {
		t.Errorf("ConfigStringSlice(array) = %v", arrayGot)
	}
}

func TestSecretString(t *testing.T) {
	params := mustParse(t, fullParams)
	if got := params.SecretString("sasl_password"); got != "s3cret" {
		t.Errorf("SecretString(sasl_password) = %q", got)
	}
	if got := params.SecretString("missing"); got != "" {
		t.Errorf("SecretString(missing) = %q", got)
	}
}

func TestOperationIDFallbackUUID(t *testing.T) {
	params := mustParse(t, `{"connection": {"id": "c1"}}`)
	fallback := params.OperationIDOrUUID()
	if fallback == "" || fallback == "op-42" {
		t.Fatalf("OperationIDOrUUID fallback = %q", fallback)
	}
	if len(fallback) != 36 || fallback[8] != '-' {
		t.Errorf("fallback is not a uuid: %q", fallback)
	}

	withOp := mustParse(t, `{"operationId": "op-7"}`)
	if got := withOp.OperationIDOrUUID(); got != "op-7" {
		t.Errorf("OperationIDOrUUID = %q, want op-7", got)
	}
}

func TestParseDisconnectMinimal(t *testing.T) {
	// connection/disconnect 可能只带 {connection:{id}}。
	params := mustParse(t, `{"connection": {"id": "c9"}}`)
	if got := params.ConnectionID(); got != "c9" {
		t.Errorf("ConnectionID = %q", got)
	}
	if params.Runtime.Host != "" {
		t.Errorf("Runtime.Host = %q, want empty", params.Runtime.Host)
	}
}

func TestParseEmptyAndInvalid(t *testing.T) {
	params, err := Parse(nil)
	if err != nil {
		t.Fatalf("Parse(nil) error = %v", err)
	}
	if params.ConnectionID() != "" {
		t.Errorf("empty params ConnectionID = %q", params.ConnectionID())
	}

	if _, err := Parse(json.RawMessage("{not json")); err == nil {
		t.Error("Parse(invalid) expected error, got nil")
	}
}

func TestConfigBoolStringAndFallback(t *testing.T) {
	// bool 的字符串形态（textarea 输入）与非法值兜底。
	params := mustParse(t, `{"connection":{"external_config":{
		"read_only": "true",
		"allow_delete": " false ",
		"bad_flag": "not-a-bool"
	}}}`)
	if !params.ConfigBool("read_only") {
		t.Error(`ConfigBool("true") = false`)
	}
	if params.ConfigBool("allow_delete") {
		t.Error(`ConfigBool(" false ") = true`)
	}
	if params.ConfigBool("bad_flag") || params.ConfigBool("missing") {
		t.Error("invalid/missing bool should be false")
	}
}

func TestConfigIntMatrix(t *testing.T) {
	params := mustParse(t, `{"connection":{"external_config":{
		"timeout_ms": 5000,
		"retries_s": "30",
		"bad_int": "abc"
	}}}`)
	if got := params.ConfigInt("timeout_ms"); got != 5000 {
		t.Errorf("ConfigInt(number) = %d, want 5000", got)
	}
	if got := params.ConfigInt("retries_s"); got != 30 {
		t.Errorf(`ConfigInt("30") = %d, want 30`, got)
	}
	if got := params.ConfigInt("bad_int"); got != 0 {
		t.Errorf("ConfigInt(bad) = %d, want 0", got)
	}
	if got := params.ConfigInt("missing"); got != 0 {
		t.Errorf("ConfigInt(missing) = %d, want 0", got)
	}
}

func TestAnyToStringMatrix(t *testing.T) {
	cases := []struct {
		name  string
		value any
		want  string
	}{
		{"nil", nil, ""},
		{"string", "  v  ", "v"},
		{"int float", float64(42), "42"},
		{"frac float", float64(1.25), "1.25"},
		{"bool true", true, "true"},
		{"bool false", false, "false"},
		{"unsupported", []int{1}, ""},
	}
	for _, tc := range cases {
		if got := anyToString(tc.value); got != tc.want {
			t.Errorf("%s: anyToString = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// KAFKA-LC-L1 回归：门禁类布尔的宽容口径（yes/on 认 true，no/off 认 false，
// 未知串保守 false）——此前 "read_only": "yes" 静默按 false，门禁 fail-open。
func TestConfigBoolTolerantForms(t *testing.T) {
	params := &Params{Connection: Connection{ExternalConfig: map[string]any{
		"a": true,
		"b": "yes",
		"c": "ON",
		"d": "no",
		"e": "off",
		"f": "true",
		"g": "0",
		"h": "junk",
		"i": float64(1), // JSON number 形态不在契约内：保守 false
	}}}
	cases := map[string]bool{"a": true, "b": true, "c": true, "d": false, "e": false, "f": true, "g": false, "h": false, "i": false}
	for key, want := range cases {
		if got := params.ConfigBool(key); got != want {
			t.Errorf("ConfigBool(%q) = %v, want %v", key, got, want)
		}
	}
	if got := params.ConfigBool("missing"); got {
		t.Error("ConfigBool(missing) = true, want false")
	}
}
