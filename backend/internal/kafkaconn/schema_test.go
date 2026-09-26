package kafkaconn

// schema_test.go：SR REST 客户端（httptest 假 Registry，无外网）、wire format
// roundtrip、载荷编解码、LCS diff、per-consume 缓存、Service 层门禁与审计。

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"io.dbx.kafka.plugin/internal/lifecycle"
)

// --- httptest 假 Confluent 兼容 Registry ---

type fakeRegistry struct {
	mu            sync.Mutex
	nextID        int
	subjects      map[string]map[int64]SchemaMeta // subject -> version -> meta
	compatGlobal  string
	compatSubject map[string]string
	byIDHits      int    // /schemas/ids/<id> 命中计数（缓存测试）
	lastNormalize string // 最近一次 register POST 的 normalize 查询参数（"" = 未携带）
}

func newFakeRegistry() *fakeRegistry {
	return &fakeRegistry{
		nextID:        10,
		subjects:      map[string]map[int64]SchemaMeta{},
		compatGlobal:  "BACKWARD",
		compatSubject: map[string]string{},
	}
}

func (f *fakeRegistry) latestVersion(subject string) int64 {
	var latest int64
	for version := range f.subjects[subject] {
		if version > latest {
			latest = version
		}
	}
	return latest
}

// register 注册一个新版本（调用方须持 f.mu：handler 路径持锁；直调路径为
// 测试前置，无并发）。
func (f *fakeRegistry) register(subject, schema, schemaType string) (int, int64) {
	f.nextID++
	id := f.nextID
	if f.subjects[subject] == nil {
		f.subjects[subject] = map[int64]SchemaMeta{}
	}
	version := f.latestVersion(subject) + 1
	f.subjects[subject][version] = SchemaMeta{
		Subject: subject, Version: version, ID: id, Schema: schema, SchemaType: schemaType,
	}
	return id, version
}

// handler 是假 Registry 的全部路由（手工 switch，路径语义对齐 Confluent REST）。
func (f *fakeRegistry) handler(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.Split(path, "/")

	writeJSON := func(payload any) {
		_ = json.NewEncoder(w).Encode(payload)
	}
	switch {
	case path == "subjects" && r.Method == http.MethodGet:
		names := []string{}
		for subject := range f.subjects {
			names = append(names, subject)
		}
		writeJSON(names)
		return
	case path == "config" && r.Method == http.MethodGet:
		writeJSON(map[string]string{"compatibilityLevel": f.compatGlobal})
		return
	case path == "config" && r.Method == http.MethodPut:
		var body struct {
			Compatibility string `json:"compatibility"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.compatGlobal = body.Compatibility
		writeJSON(map[string]string{"compatibilityLevel": f.compatGlobal})
		return
	case len(parts) == 2 && parts[0] == "config" && r.Method == http.MethodGet && parts[1] != "config":
		level, ok := f.compatSubject[parts[1]]
		if !ok {
			http.Error(w, `{"error_code":40401,"message":"not found"}`, http.StatusNotFound)
			return
		}
		writeJSON(map[string]string{"compatibilityLevel": level})
		return
	case len(parts) == 2 && parts[0] == "config" && r.Method == http.MethodPut && parts[1] != "config":
		var body struct {
			Compatibility string `json:"compatibility"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.compatSubject[parts[1]] = body.Compatibility
		writeJSON(map[string]string{"compatibilityLevel": body.Compatibility})
		return
	case len(parts) == 5 && parts[0] == "compatibility" && parts[1] == "subjects" && parts[3] == "versions":
		writeJSON(map[string]any{"is_compatible": true, "messages": []string{}})
		return
	case len(parts) == 3 && parts[0] == "schemas" && parts[1] == "ids":
		f.byIDHits++
		id := int(atoi64(parts[2]))
		for _, versions := range f.subjects {
			for _, meta := range versions {
				if meta.ID == id {
					writeJSON(map[string]any{"schema": meta.Schema, "schemaType": meta.SchemaType})
					return
				}
			}
		}
		http.Error(w, `{"error_code":40403,"message":"schema not found"}`, http.StatusNotFound)
		return
	}

	// /subjects/{subject}/versions[...]（len>=2：subject 自身删除臂为两段）
	if len(parts) >= 2 && parts[0] == "subjects" {
		subject := parts[1]
		switch {
		case len(parts) == 2 && r.Method == http.MethodDelete:
			versions := f.subjects[subject]
			if versions == nil {
				http.Error(w, `{"error_code":40401,"message":"not found"}`, http.StatusNotFound)
				return
			}
			deleted := []int64{}
			for version := range versions {
				deleted = append(deleted, version)
			}
			delete(f.subjects, subject)
			writeJSON(deleted)
			return
		case len(parts) == 3 && parts[2] == "versions" && r.Method == http.MethodGet:
			versions := []int{}
			for version := range f.subjects[subject] {
				versions = append(versions, int(version))
			}
			writeJSON(versions)
			return
		case len(parts) == 3 && parts[2] == "versions" && r.Method == http.MethodPost:
			f.lastNormalize = r.URL.Query().Get("normalize")
			var body struct {
				Schema     string `json:"schema"`
				SchemaType string `json:"schemaType"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			id, _ := f.register(subject, body.Schema, body.SchemaType)
			writeJSON(map[string]int{"id": id})
			return
		case len(parts) == 4 && parts[2] == "versions" && r.Method == http.MethodGet:
			version := int64(0)
			if parts[3] != "latest" {
				version = atoi64(parts[3])
			} else {
				version = f.latestVersion(subject)
			}
			meta, ok := f.subjects[subject][version]
			if !ok {
				http.Error(w, `{"error_code":40402,"message":"version not found"}`, http.StatusNotFound)
				return
			}
			writeJSON(meta)
			return
		case len(parts) == 4 && parts[2] == "versions" && r.Method == http.MethodDelete:
			version := atoi64(parts[3])
			if _, ok := f.subjects[subject][version]; !ok {
				http.Error(w, `{"error_code":40402,"message":"version not found"}`, http.StatusNotFound)
				return
			}
			delete(f.subjects[subject], version)
			writeJSON([]int64{version})
			return
		}
	}
	http.Error(w, `{"error_code":500,"message":"unsupported route"}`, http.StatusInternalServerError)
}

func atoi64(value string) int64 {
	var out int64
	_, _ = fmt.Sscanf(value, "%d", &out)
	return out
}

const testAvroSchema = `{"type":"record","name":"R","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}`

// newTestHTTPServer 启动httptest 服务（无外网；测试结束自动关闭）。
func newTestHTTPServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

// schemaConnect 用 lifecycle params 建一条带 SR 配置的连接。
func schemaConnect(t *testing.T, service *Service, id, srURL, srPassword string, readOnly, allowDelete bool) {
	t.Helper()
	params, err := lifecycle.Parse([]byte(`{
	  "connection": {
	    "id": "` + id + `",
	    "name": "sr-conn",
	    "external_config": {
	      "bootstrap_servers": "k1:9092",
	      "sr_url": "` + srURL + `",
	      "read_only": ` + boolText(readOnly) + `,
	      "allow_delete": ` + boolText(allowDelete) + `
	    },
	    "connection_secrets": { "sr_password": "` + srPassword + `" }
	  }
	}`))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if err := service.Connect(params); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
}

func boolText(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

// --- wire format ---

func TestWireFrameRoundtrip(t *testing.T) {
	payload := []byte{0x01, 0x02, 0x03}
	wire := encodeWireFrame(42, payload)
	if wire[0] != confluentWireMagicByte {
		t.Fatalf("magic byte = %d", wire[0])
	}
	id, decoded, err := decodeWireFrame(wire)
	if err != nil {
		t.Fatalf("decodeWireFrame() error = %v", err)
	}
	if id != 42 || string(decoded) != string(payload) {
		t.Errorf("roundtrip = id %d payload %v, want 42 %v", id, decoded, payload)
	}

	if _, _, err := decodeWireFrame([]byte{0x00, 0x00}); err == nil {
		t.Error("short frame expected error")
	}
	if _, _, err := decodeWireFrame([]byte{0x07, 0, 0, 0, 1, 0x01}); err == nil || !strings.Contains(err.Error(), "magic") {
		t.Errorf("bad magic error = %v", err)
	}
}

// --- SR REST（httptest） ---

func newSchemaTestService(t *testing.T) (*Service, *fakeRegistry) {
	t.Helper()
	registry := newFakeRegistry()
	server := newTestHTTPServer(t, registry.handler)
	service := NewService()
	schemaConnect(t, service, "sr-rw", server.URL, "sr-secret", false, true)
	return service, registry
}

func TestSchemaServiceRESTFlow(t *testing.T) {
	service, registry := newSchemaTestService(t)
	ctx := context.Background()

	// test：SR 可达。
	result, err := service.TestSchema(ctx, "sr-rw", "")
	if err != nil {
		t.Fatalf("TestSchema() error = %v", err)
	}
	if !result.OK || len(result.CompatibleFormats) != 2 {
		t.Errorf("TestSchema() = %+v", result)
	}

	// register → id/version。
	registered, err := service.RegisterSchema(ctx, SchemaRegisterRequest{
		ConnectionID: "sr-rw", Subject: "s-value", Format: "avro", Schema: testAvroSchema,
	})
	if err != nil {
		t.Fatalf("RegisterSchema() error = %v", err)
	}
	if registered.ID == 0 || registered.Version != 1 {
		t.Errorf("RegisterSchema() = %+v", registered)
	}

	// subjects/list。
	subjects, err := service.ListSchemaSubjects(ctx, "sr-rw", "")
	if err != nil {
		t.Fatalf("ListSchemaSubjects() error = %v", err)
	}
	if len(subjects.Subjects) != 1 || subjects.Subjects[0].Subject != "s-value" {
		t.Errorf("subjects = %+v", subjects.Subjects)
	}
	if subjects.Subjects[0].Formats[0] != "AVRO" || subjects.Subjects[0].LatestVersion != 1 {
		t.Errorf("subject info = %+v", subjects.Subjects[0])
	}

	// versions/list。
	versions, err := service.ListSchemaVersions(ctx, "sr-rw", "s-value", "")
	if err != nil {
		t.Fatalf("ListSchemaVersions() error = %v", err)
	}
	if len(versions.Versions) != 1 || versions.Versions[0].Version != 1 || versions.Versions[0].ID != registered.ID {
		t.Errorf("versions = %+v", versions.Versions)
	}

	// get（latest）。
	got, err := service.GetSchema(ctx, "sr-rw", "s-value", 0, "")
	if err != nil {
		t.Fatalf("GetSchema() error = %v", err)
	}
	if got.Version != 1 || got.ID != registered.ID || got.Format != "AVRO" || got.Schema != testAvroSchema {
		t.Errorf("GetSchema() = %+v", got)
	}

	// compare（相同 schema → 无 hunks）。
	diff, err := service.CompareSchemaVersions(ctx, SchemaVersionsCompareRequest{
		ConnectionID: "sr-rw", Subject: "s-value", FromVersion: 1, ToVersion: 1,
	})
	if err != nil {
		t.Fatalf("CompareSchemaVersions() error = %v", err)
	}
	if len(diff.Hunks) != 0 || diff.Summary.Unchanged == 0 {
		t.Errorf("diff = %+v", diff)
	}

	// compatibility get/set（全局 + subject）。
	compat, err := service.GetSchemaCompatibility(ctx, "sr-rw", "", "")
	if err != nil {
		t.Fatalf("GetSchemaCompatibility(global) error = %v", err)
	}
	if compat.Level != "BACKWARD" || compat.Scope != "global" {
		t.Errorf("compat global = %+v", compat)
	}
	compat, err = service.SetSchemaCompatibility(ctx, SchemaCompatibilityRequest{
		ConnectionID: "sr-rw", Subject: "s-value", Level: "FULL_TRANSITIVE",
	})
	if err != nil {
		t.Fatalf("SetSchemaCompatibility() error = %v", err)
	}
	if compat.Level != "FULL_TRANSITIVE" || compat.Scope != "subject" {
		t.Errorf("compat set = %+v", compat)
	}
	if got := registry.compatSubject["s-value"]; got != "FULL_TRANSITIVE" {
		t.Errorf("registry subject compat = %q", got)
	}

	// compatibility/check。
	check, err := service.CheckSchemaCompatibility(ctx, SchemaCompatibilityCheckRequest{
		ConnectionID: "sr-rw", Subject: "s-value", Format: "avro", Schema: testAvroSchema,
	})
	if err != nil {
		t.Fatalf("CheckSchemaCompatibility() error = %v", err)
	}
	if !check.IsCompatible {
		t.Errorf("check = %+v", check)
	}

	// delete/version + delete subject。
	deleted, err := service.DeleteSchema(ctx, SchemaDeleteRequest{ConnectionID: "sr-rw", Subject: "s-value", Version: 1})
	if err != nil {
		t.Fatalf("DeleteSchema(version) error = %v", err)
	}
	if len(deleted.DeletedVersions) != 1 || deleted.DeletedVersions[0] != 1 {
		t.Errorf("deleted version = %+v", deleted)
	}
	if _, err := service.RegisterSchema(ctx, SchemaRegisterRequest{
		ConnectionID: "sr-rw", Subject: "s-value", Format: "avro", Schema: testAvroSchema,
	}); err != nil {
		t.Fatalf("re-register error = %v", err)
	}
	deleted, err = service.DeleteSchema(ctx, SchemaDeleteRequest{ConnectionID: "sr-rw", Subject: "s-value"})
	if err != nil {
		t.Fatalf("DeleteSchema(subject) error = %v", err)
	}
	if len(deleted.DeletedVersions) == 0 {
		t.Errorf("deleted subject = %+v", deleted)
	}
}

// register 的 normalize 可选参数：缺省/false 不携带查询参数；true 以
// POST /subjects/{subject}/versions?normalize=true 注册（SR 侧归一化文本）。
func TestSchemaRegisterNormalizeParam(t *testing.T) {
	for _, tc := range []struct {
		name      string
		normalize bool
		wantQuery string
	}{
		{name: "omitted", normalize: false, wantQuery: ""},
		{name: "explicit_false", normalize: false, wantQuery: ""},
		{name: "true", normalize: true, wantQuery: "true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, registry := newSchemaTestService(t)
			registered, err := service.RegisterSchema(context.Background(), SchemaRegisterRequest{
				ConnectionID: "sr-rw", Subject: "s-value", Format: "avro", Schema: testAvroSchema,
				Normalize: tc.normalize,
			})
			if err != nil {
				t.Fatalf("RegisterSchema() error = %v", err)
			}
			if registered.ID == 0 || registered.Version != 1 {
				t.Errorf("RegisterSchema() = %+v", registered)
			}
			registry.mu.Lock()
			got := registry.lastNormalize
			registry.mu.Unlock()
			if got != tc.wantQuery {
				t.Errorf("normalize query = %q, want %q", got, tc.wantQuery)
			}
		})
	}
}

func TestSchemaServicePolicyGates(t *testing.T) {
	registry := newFakeRegistry()
	server := newTestHTTPServer(t, registry.handler)
	service := NewService()
	var audits []AuditRecord
	service.Audit = func(rec AuditRecord) { audits = append(audits, rec) }

	// read_only 连接：register / compatibility set → blocked。
	schemaConnect(t, service, "sr-ro", server.URL, "", true, false)
	_, err := service.RegisterSchema(context.Background(), SchemaRegisterRequest{
		ConnectionID: "sr-ro", Subject: "s-value", Format: "avro", Schema: testAvroSchema,
	})
	if err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Errorf("register under read-only error = %v", err)
	}
	_, err = service.SetSchemaCompatibility(context.Background(), SchemaCompatibilityRequest{
		ConnectionID: "sr-ro", Level: "NONE",
	})
	if err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Errorf("compat set under read-only error = %v", err)
	}
	// read_only 下 delete 同样 blocked（allow_delete 与门第二层）。
	_, err = service.DeleteSchema(context.Background(), SchemaDeleteRequest{ConnectionID: "sr-ro", Subject: "s-value"})
	if err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Errorf("delete under read-only error = %v", err)
	}

	// 可写但 allow_delete=false：register 放行、delete 仍拒绝。
	schemaConnect(t, service, "sr-nodelete", server.URL, "", false, false)
	if _, err := service.RegisterSchema(context.Background(), SchemaRegisterRequest{
		ConnectionID: "sr-nodelete", Subject: "s-value", Format: "avro", Schema: testAvroSchema,
	}); err != nil {
		t.Fatalf("register error = %v", err)
	}
	_, err = service.DeleteSchema(context.Background(), SchemaDeleteRequest{ConnectionID: "sr-nodelete", Subject: "s-value"})
	if err == nil || !strings.Contains(err.Error(), "delete") {
		t.Errorf("delete without allow_delete error = %v", err)
	}

	// blocked 审计已记（register×1 + compat set×1 + delete×2）。
	blocked := 0
	for _, rec := range audits {
		if rec.Result == "blocked" {
			blocked++
		}
		if strings.Contains(rec.Detail, "sr-secret") || strings.Contains(rec.Target, "sr-secret") {
			t.Fatalf("audit leaked secret: %+v", rec)
		}
	}
	if blocked != 4 {
		t.Errorf("blocked audits = %d, want 4 (%+v)", blocked, audits)
	}
}

// 审查 L3 回归：SR REST 通道复用连接的 TLS 信任面（buildTLSConfig）——
// https/skip-verify/CA 按连接配置进 transport，默认 transport 不再吞掉 TLS 配置。
func TestSchemaRegistryClientTLSTransport(t *testing.T) {
	// https + skip-verify → transport 携带 InsecureSkipVerify。
	client, err := newSchemaRegistryClient(Profile{
		SRURL:                 "https://sr.example:8081",
		TLSInsecureSkipVerify: true,
	}, connSecrets{})
	if err != nil {
		t.Fatal(err)
	}
	transport, ok := client.http.Transport.(*http.Transport)
	if !ok || transport.TLSClientConfig == nil || !transport.TLSClientConfig.InsecureSkipVerify {
		t.Fatalf("https+skipVerify transport = %+v, want TLSClientConfig with InsecureSkipVerify", client.http.Transport)
	}
	// 纯 http 且无任何 TLS 项 → 默认 transport（零行为变化）。
	plain, err := newSchemaRegistryClient(Profile{SRURL: "http://127.0.0.1:8081"}, connSecrets{})
	if err != nil {
		t.Fatal(err)
	}
	if plain.http.Transport != nil {
		t.Fatalf("plain http transport = %v, want nil", plain.http.Transport)
	}
	// CA 配置经 buildTLSConfig 生效：非法 PEM 显式报错（复用路径的证据）。
	if _, err := newSchemaRegistryClient(Profile{
		SRURL:     "https://sr.example:8081",
		TLSCACert: "not-a-pem",
	}, connSecrets{}); err == nil || !strings.Contains(err.Error(), "tlsCaCert") {
		t.Fatalf("invalid CA error = %v, want tlsCaCert build failure", err)
	}
	// schemaUsesTLS 取值面。
	cases := []struct {
		url     string
		profile Profile
		want    bool
	}{
		{"http://sr:8081", Profile{}, false},
		{"https://sr:8081", Profile{}, true},
		{"http://sr:8081", Profile{TLSCACert: "x"}, true},
		{"http://sr:8081", Profile{TLSInsecureSkipVerify: true}, true},
	}
	for _, tc := range cases {
		if got := schemaUsesTLS(tc.url, tc.profile); got != tc.want {
			t.Errorf("schemaUsesTLS(%q, %+v) = %v, want %v", tc.url, tc.profile, got, tc.want)
		}
	}
}

func TestSchemaServiceNotConfigured(t *testing.T) {
	service := NewService()
	// 无 sr_url（旧连接，开关未设）→ 业务错（不 panic）。
	schemaConnect(t, service, "sr-off", "", "", false, false)
	if _, err := service.TestSchema(context.Background(), "sr-off", ""); err == nil || !strings.Contains(err.Error(), "not enabled") {
		t.Errorf("TestSchema without sr_url error = %v", err)
	}
	// 未连接 → 连接不存在。
	if _, err := service.TestSchema(context.Background(), "missing", ""); err == nil {
		t.Error("TestSchema(missing) expected error")
	}
}

// --- 载荷编解码 ---

func TestSchemaPayloadAvroRoundtrip(t *testing.T) {
	payload := []byte(`{"id":7,"name":"dbx"}`)
	encoded, err := encodeSchemaPayload(payload, testAvroSchema, "AVRO", "")
	if err != nil {
		t.Fatalf("encodeSchemaPayload(avro) error = %v", err)
	}
	decoded, err := decodeSchemaPayload(encoded, testAvroSchema, "AVRO", "")
	if err != nil {
		t.Fatalf("decodeSchemaPayload(avro) error = %v", err)
	}
	var back map[string]any
	if err := json.Unmarshal(decoded, &back); err != nil {
		t.Fatalf("decoded payload is not JSON: %v", err)
	}
	if back["id"].(float64) != 7 || back["name"] != "dbx" {
		t.Errorf("decoded = %s", decoded)
	}
}

func TestSchemaPayloadJSONValidation(t *testing.T) {
	schema := `{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}`
	if _, err := encodeSchemaPayload([]byte(`{"id":1}`), schema, "JSON", ""); err != nil {
		t.Errorf("valid json payload rejected: %v", err)
	}
	if _, err := encodeSchemaPayload([]byte(`{"id":"nope"}`), schema, "JSON", ""); err == nil {
		t.Error("invalid json payload accepted")
	}
	if _, err := encodeSchemaPayload([]byte(`{bad json`), schema, "JSON", ""); err == nil {
		t.Error("non-JSON payload accepted")
	}
	// PROTOBUF 分支：schema 非 base64/FDSet → 参数级错误（有效向量见
	// protobuf_test.go，单测直接复用 kafka-seed fixture）。
	if _, err := encodeSchemaPayload([]byte(`{}`), "not-base64!!", "PROTOBUF", ""); err == nil {
		t.Error("protobuf encode with invalid FDSet accepted")
	}
	if _, err := decodeSchemaPayload([]byte{0x01}, "not-base64!!", "PROTOBUF", ""); err == nil {
		t.Error("protobuf decode with invalid FDSet accepted")
	}
	if _, err := encodeSchemaPayload([]byte("{}"), "{}", "YAML", ""); err == nil {
		t.Error("bogus format accepted")
	}
}

// --- per-consume 缓存（同 schemaID 只打一次 /schemas/ids） ---

func TestSchemaDecoderCacheByID(t *testing.T) {
	registry := newFakeRegistry()
	id, _ := registry.register("s-value", testAvroSchema, "AVRO")
	server := newTestHTTPServer(t, registry.handler)
	client, err := newSchemaRegistryClient(Profile{SRURL: server.URL}, connSecrets{})
	if err != nil {
		t.Fatalf("newSchemaRegistryClient() error = %v", err)
	}
	decoder := newSchemaDecoder(client, nil)
	ctx := context.Background()

	payload := []byte(`{"id":1,"name":"a"}`)
	encoded, err := encodeSchemaPayload(payload, testAvroSchema, "AVRO", "")
	if err != nil {
		t.Fatalf("encode payload error = %v", err)
	}
	wire := encodeWireFrame(id, encoded)

	for i := 0; i < 3; i++ {
		decoded, info, err := decoder.decode(ctx, wire)
		if err != nil {
			t.Fatalf("decode[%d] error = %v", i, err)
		}
		// byID 路径（请求未指定 subject）：/schemas/ids 响应只含
		// schema/schemaType，Subject/Version 留空、ID 来自 wire id。
		if info.ID != id || info.Subject != "" || info.Version != 0 {
			t.Errorf("decode[%d] info = %+v", i, info)
		}
		if !strings.Contains(string(decoded), `"name":"a"`) {
			t.Errorf("decode[%d] = %s", i, decoded)
		}
	}
	if registry.byIDHits != 1 {
		t.Errorf("byID hits = %d, want 1 (cache)", registry.byIDHits)
	}

	// 非 wire format → 明确错误（不中断消费语义）。
	if _, _, err := decoder.decode(ctx, []byte("plain-text")); err == nil {
		t.Error("plain value expected error")
	}
}

// --- LCS diff ---

func TestSchemaTextDiff(t *testing.T) {
	before := "a\nb\nc\nd"
	after := "a\nX\nc\nd\nY"
	hunks, summary := schemaTextDiff(before, after)
	if summary.Removed != 1 || summary.Added != 2 || summary.Unchanged != 3 {
		t.Fatalf("summary = %+v", summary)
	}
	if summary.BeforeLines != 4 || summary.AfterLines != 5 {
		t.Errorf("line counts = %+v", summary)
	}
	var modify *SchemaDiffHunk
	for i := range hunks {
		if hunks[i].Op == "modify" {
			modify = &hunks[i]
		}
	}
	if modify == nil || modify.Before != "b" || !strings.Contains(modify.After, "X") {
		t.Fatalf("modify hunk = %+v", hunks)
	}

	// 纯新增。
	hunks, summary = schemaTextDiff("a", "a\nb")
	if len(hunks) != 1 || hunks[0].Op != "add" || hunks[0].After != "b" {
		t.Errorf("add-only hunks = %+v", hunks)
	}
	if summary.Added != 1 || summary.Removed != 0 {
		t.Errorf("add-only summary = %+v", summary)
	}

	// 纯删除。
	hunks, summary = schemaTextDiff("a\nb", "a")
	if len(hunks) != 1 || hunks[0].Op != "remove" || hunks[0].Before != "b" {
		t.Errorf("remove-only hunks = %+v", hunks)
	}

	// 完全相同 → 无 hunks。
	hunks, _ = schemaTextDiff("same", "same")
	if len(hunks) != 0 {
		t.Errorf("identical text hunks = %+v", hunks)
	}
}

// --- format 归一化辅助 ---

func TestNormalizeConfluentSchemaType(t *testing.T) {
	if got := normalizeConfluentSchemaType(""); got != "AVRO" {
		t.Errorf("empty schemaType = %q", got)
	}
	if got := normalizeConfluentSchemaType("json"); got != "JSON" {
		t.Errorf("json schemaType = %q", got)
	}
	if got := normalizeConfluentSchemaType("weird"); got != "WEIRD" {
		t.Errorf("unknown schemaType = %q", got)
	}
	if _, err := normalizeKafkaSchemaFormat("yaml"); err == nil {
		t.Error("bogus format expected error")
	}
}

func TestNormalizeCompatibilityAndReferences(t *testing.T) {
	for _, pair := range [][2]string{
		{"BACKWARD", "BACKWARD"},
		{"backward_all", "BACKWARD_TRANSITIVE"},
		{"NONE", "NONE"},
		{"", "NONE"},
		{"FULL", "FULL"},
	} {
		got, err := normalizeConfluentCompatibility(pair[0])
		if err != nil || got != pair[1] {
			t.Errorf("normalize(%q) = %q, %v", pair[0], got, err)
		}
	}
	if _, err := normalizeConfluentCompatibility("SIDEWAYS"); err == nil {
		t.Error("bogus compatibility level expected error")
	}

	if _, err := normalizeSchemaReferences([]SchemaReference{{Name: "n", Subject: "s", Version: 0}}); err == nil {
		t.Error("reference without version expected error")
	}
	refs, err := normalizeSchemaReferences([]SchemaReference{{Name: "n", Subject: "s", Version: 2}})
	if err != nil || len(refs) != 1 {
		t.Errorf("refs = %v, %v", refs, err)
	}
}

// --- 删除响应形状兼容（Confluent 数组 / Redpanda 单数字） ---

func TestDecodeDeletedVersionsShapes(t *testing.T) {
	list, err := decodeDeletedVersions(json.RawMessage(`[1,2,3]`))
	if err != nil || len(list) != 3 || list[2] != 3 {
		t.Errorf("array shape = %v, %v", list, err)
	}
	single, err := decodeDeletedVersions(json.RawMessage(`2`))
	if err != nil || len(single) != 1 || single[0] != 2 {
		t.Errorf("single number shape = %v, %v", single, err)
	}
	if _, err := decodeDeletedVersions(json.RawMessage(`{"bad":1}`)); err == nil {
		t.Error("object shape expected error")
	}
}
