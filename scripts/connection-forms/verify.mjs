// Standalone Kafka connection-form contract verifier.
//
// The monorepo version imports the DBX host's TypeScript condition evaluator.
// This copy intentionally keeps only the small, host-compatible evaluator and
// Kafka assertions needed by this repository, so clean clones do not need
// ../host or ../shared. Replace this file with the future public
// form-contract package when that package is available; keep the scenarios
// below as the Kafka regression contract.
//
// Usage: node scripts/connection-forms/verify.mjs [kafka]
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const plugin = process.argv[2] ?? "kafka";
if (plugin !== "kafka") {
  console.error(`unknown plugin ${JSON.stringify(plugin)}; this verifier only covers kafka`);
  process.exit(1);
}

const root = new URL("../../", import.meta.url);
const manifest = JSON.parse(readFileSync(new URL("manifest.json", root), "utf8"));
const provider = manifest.contributions.find((item) => item.type === "connection-provider");
assert(provider, "Kafka manifest must define a connection provider");
const fields = provider.fields;
const byKey = Object.fromEntries(fields.map((field) => [field.key, field]));
const defaults = Object.fromEntries(fields.map((field) => [field.key, field.default]));
const locales = ["en", "zh-CN", "zh-TW", "es", "it", "ja", "pt-BR"];

assert.equal(new Set(fields.map((field) => field.key)).size, fields.length, "duplicate field keys");

function conditionMatches(condition, value) {
  if (!condition) return true;
  if (value === undefined || value === null || (typeof value === "string" && value.trim() === "")) return false;
  return condition.one_of.includes(String(value));
}

function isVisible(field, values, seen = new Set([field.key])) {
  const condition = field.visible_when;
  if (!condition || !conditionMatches(condition, values[condition.field])) return !condition;
  const target = byKey[condition.field];
  if (!target || seen.has(target.key)) return true;
  seen.add(target.key);
  return isVisible(target, values, seen);
}

function isRequired(field, values) {
  return Boolean(field.required)
    || Boolean(field.required_when && conditionMatches(field.required_when, values[field.required_when.field]));
}

for (const [index, field] of fields.entries()) {
  for (const condition of [field.visible_when, field.required_when].filter(Boolean)) {
    const target = byKey[condition.field];
    assert(target, `${field.key}: unknown condition field ${condition.field}`);
    assert(fields.indexOf(target) < index, `${field.key}: condition target must precede dependent field`);
    const values = target.type === "boolean" ? ["true", "false"] : target.options?.map((option) => option.value);
    if (values) assert(condition.one_of.every((value) => values.includes(value)), `${field.key}: invalid condition value`);
  }
  if (field.type === "select" && field.default !== undefined) {
    assert(field.options.some((option) => option.value === field.default), `${field.key}: invalid default`);
  }
  for (const locale of locales) {
    const localized = manifest.localizations[locale]?.contributions?.[provider.id]?.fields?.[field.key]
      ?? (locale === "en" ? field : undefined);
    assert(localized?.label?.trim(), `${locale}/${field.key}: missing label`);
    for (const option of field.options ?? []) {
      const label = Array.isArray(localized.options)
        ? localized.options.find((item) => item.value === option.value)?.label
        : localized.options?.[option.value];
      assert(label?.trim(), `${locale}/${field.key}/${option.value}: missing option label`);
    }
  }
}

const options = (key) => byKey[key].options.map((option) => option.value);
let scenarios = 0;
function state(overrides) {
  scenarios++;
  const values = { ...defaults, ...overrides };
  const visible = new Set(fields.filter((field) => isVisible(field, values)).map((field) => field.key));
  const required = new Set(fields.filter((field) => visible.has(field.key) && isRequired(field, values)).map((field) => field.key));
  return {
    visible(key, expected) { assert.equal(visible.has(key), expected, `${key} visibility: ${JSON.stringify(overrides)}`); },
    required(key, expected) { assert.equal(required.has(key), expected, `${key} required: ${JSON.stringify(overrides)}`); },
  };
}

// Kafka connection matrix: broker discovery x security protocol x SASL
// mechanism (incl. Kerberos/OAuth sub-forms) x Schema Registry flavor
// x read-only/delete gate. Full cross product keeps the cascade honest.
//
// Cross-field semantic conflicts (which the host's single-field
// visible_when/required_when evaluator cannot express) are guarded on the
// backend in validateRequiredCombination (backend/internal/kafkaconn/types.go,
// matrix regression: required_matrix_test.go) and surface as -32602:
//   - OAUTHBEARER requires security_protocol=SASL_SSL
//   - msk_access_key_id + msk_secret_access_key must pair; msk_session_token
//     is only valid alongside an explicit pair
//   - tls_client_cert + tls_client_key (secret) must pair for mTLS
//   - schema_registry=confluent requires sr_url with an http(s) scheme
// Dormant values of hidden fields are intentionally kept (switching back
// restores credentials); the backend ignores them unless their branch is
// active (glue static keys, zk/bootstrap, SASL sub-forms).
for (const connection_source of options("connection_source")) {
  for (const security_protocol of options("security_protocol")) {
    for (const sasl_mechanism of options("sasl_mechanism")) {
      for (const oauth_token_source of options("oauth_token_source")) {
        for (const schema_registry of options("schema_registry")) {
          for (const glue_auth_mode of options("glue_auth_mode")) {
            for (const read_only of [false, true]) {
              const current = state({
                connection_source, security_protocol, sasl_mechanism,
                oauth_token_source, schema_registry, glue_auth_mode, read_only,
              });
              const sasl = ["SASL_PLAINTEXT", "SASL_SSL"].includes(security_protocol);
              const ssl = ["SSL", "SASL_SSL"].includes(security_protocol);
              const scramOrPlain = ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"].includes(sasl_mechanism);

              current.visible("display_name", true);
              current.required("display_name", true);
              current.visible("connection_source", true);
              current.visible("bootstrap_servers", connection_source === "bootstrap");
              current.required("bootstrap_servers", connection_source === "bootstrap");
              current.visible("zk_servers", connection_source === "zookeeper");
              current.required("zk_servers", connection_source === "zookeeper");
              current.visible("security_protocol", true);
              current.visible("sasl_mechanism", sasl);

              current.visible("sasl_username", sasl && scramOrPlain);
              current.required("sasl_username", sasl && scramOrPlain);
              current.visible("sasl_password", sasl && scramOrPlain);
              current.required("sasl_password", sasl && scramOrPlain);

              const kerberos = sasl && sasl_mechanism === "GSSAPI";
              current.visible("kerberos_principal", kerberos);
              current.required("kerberos_principal", kerberos);
              current.visible("kerberos_keytab_path", kerberos);
              current.required("kerberos_keytab_path", kerberos);
              current.visible("kerberos_realm", kerberos);
              current.visible("kerberos_service_name", kerberos);
              current.visible("kerberos_krb5_conf_path", kerberos);

              const oauth = sasl && sasl_mechanism === "OAUTHBEARER";
              current.visible("oauth_token_source", oauth);
              current.required("oauth_token_source", oauth);
              const mskIam = oauth && oauth_token_source === "msk_iam";
              current.visible("msk_region", mskIam);
              current.required("msk_region", mskIam);
              current.visible("msk_access_key_id", mskIam);
              current.visible("msk_secret_access_key", mskIam);
              current.visible("msk_session_token", mskIam);
              current.visible("oauth_static_token", oauth && oauth_token_source === "static_token");
              current.required("oauth_static_token", oauth && oauth_token_source === "static_token");

              current.visible("tls_ca_cert", ssl);
              current.visible("tls_client_cert", ssl);
              current.visible("tls_client_key", ssl);
              current.visible("tls_insecure_skip_verify", ssl);

              current.visible("schema_registry", true);
              const confluent = schema_registry === "confluent";
              current.visible("sr_url", confluent);
              current.required("sr_url", confluent);
              current.visible("sr_username", confluent);
              current.visible("sr_password", confluent);
              const glue = schema_registry === "aws_glue";
              current.visible("glue_region", glue);
              current.required("glue_region", glue);
              current.visible("glue_registry_name", glue);
              current.required("glue_registry_name", glue);
              current.visible("glue_auth_mode", glue);
              const glueStatic = glue && glue_auth_mode === "static";
              current.visible("glue_access_key_id", glueStatic);
              current.required("glue_access_key_id", glueStatic);
              current.visible("glue_secret_access_key", glueStatic);
              current.required("glue_secret_access_key", glueStatic);
              current.visible("glue_session_token", glueStatic);

              current.visible("client_id", true);
              current.visible("read_only", true);
              current.visible("allow_delete", read_only === false);
              current.visible("properties_import", true);
              current.required("properties_import", false);
            }
          }
        }
      }
    }
  }
}

assert.equal(byKey.allow_delete.type, "boolean");
assert.equal(byKey.tls_insecure_skip_verify.type, "boolean");

// Lane 3（conn-properties）：粘贴 properties 导入字段契约 —— textarea +
// secret binding（粘贴文本中的密码经宿主加密存储，绝不明文持久化）、
// 无条件常显、恒可选（不参与 required_when 矩阵）。
const propsImport = byKey.properties_import;
assert(propsImport, "properties_import field missing");
assert.equal(propsImport.type, "textarea", "properties_import must be a textarea");
assert.equal(propsImport.binding, "secret", "properties_import must be secret-bound (pasted passwords are never persisted in plain text)");
assert.equal(propsImport.visible_when, undefined, "properties_import must be always visible");
assert.equal(propsImport.required_when, undefined, "properties_import must be optional");
for (const locale of locales) {
  const localized = manifest.localizations[locale]?.contributions?.[provider.id]?.fields?.properties_import;
  assert(localized?.description?.trim(), `${locale}/properties_import: missing description`);
}
console.log("PASS Kafka properties-import field: textarea + secret binding, unconditional and optional, seven-language labels");
console.log(`PASS Kafka connection form: ${scenarios} combinations; field ordering and seven-language labels/options`);
