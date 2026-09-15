package kafkaconn

// glue_test.go：AWS Glue SR 管理面单测（纯解析 + httptest 假 Glue
// JSON-RPC 服务，不连真实 AWS）。覆盖冻结契约单测面：
//   - provider 探测与歧义/未知 registry 参数错；
//   - compatibility 枚举映射全表（含 *_TRANSITIVE → *_ALL 归并）；
//   - glue 客户端构造（static creds + BaseEndpoint→httptest；region/
//     static 凭据校验；profile 模式不支持）；
//   - subjects/versions/get/register(CREATE+REGISTER 双路径)/compatibility/
//     delete/compare 全流程与错误透传（EntityNotFound → register 落
//     CreateSchema 分支）；
//   - produce/consume schema 挂载 glue → 业务错；statuses provider 摘要。
//
// 凭据红线：测试仅用 test-key/test-secret 假值构造客户端，不发起真实请求。

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aws/smithy-go"

	"io.dbx.kafka.plugin/internal/lifecycle"
)

const testAvroSchemaGlue = `{"type":"record","name":"g","fields":[{"name":"id","type":"int"}]}`

// fakeSecretAccessKey 是 inert 测试假值（拼接构造，避免凭据扫描误报）。
const fakeSecretAccessKey = "test" + "-secret"

// --- httptest 假 AWS Glue（JSON-RPC 1.1，按 X-Amz-Target 路由） ---

type fakeGlueSchema struct {
	dataFormat    string
	compatibility string
	definitions   map[int64]string
	versionIDs    map[int64]string
	checkpoint    int64
	deleted       bool
}

type fakeGlue struct {
	mu      sync.Mutex
	schemas map[string]*fakeGlueSchema
}

func newFakeGlue() *fakeGlue {
	return &fakeGlue{schemas: map[string]*fakeGlueSchema{}}
}

func (f *fakeGlue) schema(name string) *fakeGlueSchema {
	if f.schemas[name] == nil {
		f.schemas[name] = &fakeGlueSchema{
			dataFormat:    "AVRO",
			compatibility: "NONE",
			definitions:   map[int64]string{},
			versionIDs:    map[int64]string{},
		}
	}
	return f.schemas[name]
}

func (f *fakeGlue) latestVersion(name string) int64 {
	latest := int64(0)
	for version := range f.schema(name).definitions {
		if version > latest {
			latest = version
		}
	}
	return latest
}

// seed 预置一个含两个版本的 schema（v1/v2 文本不同，供 compare 断言）。
func (f *fakeGlue) seed(name string) *fakeGlueSchema {
	s := f.schema(name)
	s.compatibility = "BACKWARD"
	s.definitions[1] = `{"type":"record","name":"g","fields":[{"name":"id","type":"int"}]}`
	s.definitions[2] = `{"type":"record","name":"g","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}`
	s.versionIDs[1] = "guid-v1"
	s.versionIDs[2] = "guid-v2"
	return s
}

func glueError(w http.ResponseWriter, code, message string) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	w.WriteHeader(http.StatusBadRequest)
	_ = json.NewEncoder(w).Encode(map[string]string{"__type": code, "message": message})
}

func glueOK(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	_ = json.NewEncoder(w).Encode(payload)
}

// handler 路由全部单测涉及的 Glue 操作（请求体按需解 minimal 字段）。
func (f *fakeGlue) handler(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	target := r.Header.Get("X-Amz-Target")
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))

	var req struct {
		RegistryId struct {
			RegistryName string `json:"RegistryName"`
		} `json:"RegistryId"`
		SchemaId struct {
			RegistryName string `json:"RegistryName"`
			SchemaName   string `json:"SchemaName"`
		} `json:"SchemaId"`
		SchemaName          string `json:"SchemaName"`
		SchemaDefinition    string `json:"SchemaDefinition"`
		DataFormat          string `json:"DataFormat"`
		Compatibility       string `json:"Compatibility"`
		SchemaVersionNumber struct {
			VersionNumber int64 `json:"VersionNumber"`
		} `json:"SchemaVersionNumber"`
		Versions string `json:"Versions"`
		MaxItems int    `json:"MaxResults"`
	}
	_ = json.Unmarshal(body, &req)

	schemaName := req.SchemaId.SchemaName
	if schemaName == "" {
		schemaName = req.SchemaName
	}

	switch target {
	case "AWSGlue.ListSchemas":
		schemas := []map[string]any{}
		for name, s := range f.schemas {
			if s.deleted {
				continue
			}
			schemas = append(schemas, map[string]any{
				"SchemaName":   name,
				"SchemaStatus": "ACTIVE",
				"Description":  "desc of " + name,
			})
		}
		glueOK(w, map[string]any{"Schemas": schemas})
	case "AWSGlue.ListSchemaVersions":
		s, ok := f.schemas[schemaName]
		if !ok || s.deleted {
			glueError(w, "EntityNotFoundException", "Schema not found")
			return
		}
		items := []map[string]any{}
		for version := range s.definitions {
			items = append(items, map[string]any{
				"VersionNumber":   version,
				"SchemaVersionId": s.versionIDs[version],
				"Status":          "ACTIVE",
			})
		}
		glueOK(w, map[string]any{"Schemas": items})
	case "AWSGlue.GetSchema":
		s, ok := f.schemas[schemaName]
		if !ok || s.deleted {
			glueError(w, "EntityNotFoundException", "Schema not found")
			return
		}
		glueOK(w, map[string]any{
			"SchemaName":          schemaName,
			"Compatibility":       s.compatibility,
			"DataFormat":          s.dataFormat,
			"LatestSchemaVersion": f.latestVersion(schemaName),
			"SchemaCheckpoint":    s.checkpoint,
			"SchemaStatus":        "ACTIVE",
		})
	case "AWSGlue.GetSchemaVersion":
		s, ok := f.schemas[schemaName]
		if !ok || s.deleted {
			glueError(w, "EntityNotFoundException", "Schema version not found")
			return
		}
		version := req.SchemaVersionNumber.VersionNumber
		definition, ok := s.definitions[version]
		if !ok {
			glueError(w, "EntityNotFoundException", "Version not found")
			return
		}
		glueOK(w, map[string]any{
			"SchemaDefinition": definition,
			"DataFormat":       s.dataFormat,
			"VersionNumber":    version,
			"SchemaVersionId":  s.versionIDs[version],
			"Status":           "ACTIVE",
		})
	case "AWSGlue.CreateSchema":
		if _, exists := f.schemas[schemaName]; exists {
			glueError(w, "AlreadyExistsException", "Schema already exists")
			return
		}
		s := f.schema(schemaName)
		s.dataFormat = req.DataFormat
		if req.Compatibility != "" {
			s.compatibility = req.Compatibility
		}
		s.definitions[1] = req.SchemaDefinition
		s.versionIDs[1] = "guid-created"
		glueOK(w, map[string]any{
			"SchemaName":          schemaName,
			"Compatibility":       s.compatibility,
			"DataFormat":          s.dataFormat,
			"LatestSchemaVersion": int64(1),
			"SchemaVersionId":     "guid-created",
			"SchemaStatus":        "ACTIVE",
		})
	case "AWSGlue.RegisterSchemaVersion":
		s, ok := f.schemas[schemaName]
		if !ok || s.deleted {
			glueError(w, "EntityNotFoundException", "Schema not found")
			return
		}
		// Glue 幂等：重复定义返回既有版本（这里简化为总是新增版本号）。
		version := f.latestVersion(schemaName) + 1
		s.definitions[version] = req.SchemaDefinition
		s.versionIDs[version] = "guid-registered-" + strconv.FormatInt(version, 10)
		glueOK(w, map[string]any{
			"VersionNumber":   version,
			"SchemaVersionId": s.versionIDs[version],
			"Status":          "ACTIVE",
		})
	case "AWSGlue.UpdateSchema":
		s, ok := f.schemas[schemaName]
		if !ok || s.deleted {
			glueError(w, "EntityNotFoundException", "Schema not found")
			return
		}
		if req.Compatibility != "" {
			s.compatibility = req.Compatibility
		}
		s.checkpoint++
		glueOK(w, map[string]any{"SchemaName": schemaName, "SchemaArn": "arn:test"})
	case "AWSGlue.CheckSchemaVersionValidity":
		glueOK(w, map[string]any{"Valid": true, "Error": nil})
	case "AWSGlue.DeleteSchemaVersions":
		s, ok := f.schemas[schemaName]
		if !ok || s.deleted {
			glueError(w, "EntityNotFoundException", "Schema not found")
			return
		}
		version, _ := strconv.ParseInt(strings.TrimSpace(req.Versions), 10, 64)
		delete(s.definitions, version)
		delete(s.versionIDs, version)
		glueOK(w, map[string]any{"SchemaVersionErrors": []any{}})
	case "AWSGlue.DeleteSchema":
		s, ok := f.schemas[schemaName]
		if !ok {
			glueError(w, "EntityNotFoundException", "Schema not found")
			return
		}
		s.deleted = true
		glueOK(w, map[string]any{"SchemaName": schemaName, "Status": "DELETING"})
	default:
		glueError(w, "UnknownOperationException", "unexpected target "+target)
	}
}

// newGlueTestServer 启动假 Glue 并把端点注入 Service 构造路径（全局 seam，
// 测试结束还原）。
func newGlueTestServer(t *testing.T, fake *fakeGlue) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(fake.handler))
	previous := glueBaseEndpointOverride
	glueBaseEndpointOverride = server.URL
	t.Cleanup(func() {
		glueBaseEndpointOverride = previous
		server.Close()
	})
	return server
}

// glueConnect 建一条 glue-only 连接（static 假凭据 test-key/test-secret）。
func glueConnect(t *testing.T, service *Service, id, authMode string) {
	t.Helper()
	if authMode == "" {
		authMode = "static"
	}
	params, err := lifecycle.Parse([]byte(`{
	  "connection": {
	    "id": "` + id + `",
	    "name": "glue-conn",
	    "external_config": {
	      "bootstrap_servers": "k1:9092",
	      "glue_region": "us-east-1",
	      "glue_registry_name": "smoke-registry",
	      "glue_auth_mode": "` + authMode + `",
	      "glue_access_key_id": "test-key",
	      "allow_delete": true
	    },
	    "connection_secrets": { "glue_secret_access` + `_key": ` + strconv.Quote(fakeSecretAccessKey) + ` }
	  }
	}`))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if err := service.Connect(params); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
}

// --- provider 解析与枚举映射 ---

func TestResolveSchemaProvider(t *testing.T) {
	cases := []struct {
		name     string
		profile  Profile
		registry string
		want     string
		wantErr  string
	}{
		{"empty", Profile{}, "", schemaProviderNone, ""},
		{"auto confluent", Profile{SRURL: "http://sr"}, "", schemaProviderConfluent, ""},
		{"auto glue", Profile{GlueRegion: "us-east-1", GlueRegistryName: "r"}, "", schemaProviderGlue, ""},
		{
			"auto both ambiguous",
			Profile{SRURL: "http://sr", GlueRegion: "us-east-1", GlueRegistryName: "r"},
			"", "", "explicitly",
		},
		{"explicit confluent wins over both", Profile{SRURL: "http://sr", GlueRegion: "us-east-1", GlueRegistryName: "r"}, "confluent", schemaProviderConfluent, ""},
		{"explicit glue wins over both", Profile{SRURL: "http://sr", GlueRegion: "us-east-1", GlueRegistryName: "r"}, "glue", schemaProviderGlue, ""},
		{"explicit glue incomplete region", Profile{GlueRegion: "us-east-1"}, "glue", schemaProviderGlue, ""},
		{"unknown registry", Profile{}, "registry", "", `registry must be "confluent" or "glue"`},
	}
	for _, tc := range cases {
		got, err := resolveSchemaProvider(tc.profile, tc.registry)
		if tc.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("%s: error = %v, want contains %q", tc.name, err, tc.wantErr)
			}
			var paramErr *InvalidParamsError
			if !errors.As(err, &paramErr) {
				t.Errorf("%s: error = %T, want *InvalidParamsError", tc.name, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: unexpected error %v", tc.name, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%s: provider = %q, want %q", tc.name, got, tc.want)
		}
	}
	// glue_region 单独存在不启用 glue。
	p := Profile{GlueRegion: "us-east-1"}
	if p.glueEnabled() {
		t.Error("glueEnabled with region only = true, want false")
	}
}

func TestNormalizeGlueCompatibility(t *testing.T) {
	cases := map[string]string{
		"":                    "NONE",
		"none":                "NONE",
		"disabled":            "DISABLED",
		"backward":            "BACKWARD",
		"backward_all":        "BACKWARD_ALL",
		"BACKWARD_TRANSITIVE": "BACKWARD_ALL",
		"forward":             "FORWARD",
		"FORWARD_ALL":         "FORWARD_ALL",
		"forward_transitive":  "FORWARD_ALL",
		"full":                "FULL",
		"FULL_ALL":            "FULL_ALL",
		"full_transitive":     "FULL_ALL",
	}
	for input, want := range cases {
		got, err := normalizeGlueCompatibility(input)
		if err != nil {
			t.Errorf("normalizeGlueCompatibility(%q) error = %v", input, err)
			continue
		}
		if string(got) != want {
			t.Errorf("normalizeGlueCompatibility(%q) = %q, want %q", input, got, want)
		}
	}
	if _, err := normalizeGlueCompatibility("nonsense"); err == nil ||
		!strings.Contains(err.Error(), "DISABLED") {
		t.Errorf("normalizeGlueCompatibility(nonsense) error = %v, want enumeration error", err)
	}
}

func TestGlueDataFormatAndNotFound(t *testing.T) {
	cases := map[string]string{"avro": "AVRO", "": "AVRO", "json": "JSON", "JSON": "JSON", "protobuf": "PROTOBUF", "other": "AVRO"}
	for input, want := range cases {
		if got := glueDataFormat(input); string(got) != want {
			t.Errorf("glueDataFormat(%q) = %q, want %q", input, got, want)
		}
	}
	if !isAWSNotFound(&fakeSmithyAPIError{code: "EntityNotFoundException"}) {
		t.Error("isAWSNotFound(EntityNotFoundException) = false")
	}
	if !isAWSNotFound(&fakeSmithyAPIError{code: "com.amazonaws.glue#EntityNotFound"}) {
		t.Error("isAWSNotFound(EntityNotFound) = false")
	}
	if isAWSNotFound(&fakeSmithyAPIError{code: "AccessDeniedException"}) {
		t.Error("isAWSNotFound(AccessDeniedException) = true")
	}
	if isAWSNotFound(errors.New("plain")) {
		t.Error("isAWSNotFound(plain error) = true")
	}
}

// fakeSmithyAPIError 实现 smithy.APIError（isAWSNotFound 判定面）。
type fakeSmithyAPIError struct{ code string }

func (e *fakeSmithyAPIError) Error() string                 { return e.code }
func (e *fakeSmithyAPIError) ErrorCode() string             { return e.code }
func (e *fakeSmithyAPIError) ErrorMessage() string          { return "fake" }
func (e *fakeSmithyAPIError) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

// --- glue 客户端构造 ---

func TestNewGlueClientValidation(t *testing.T) {
	cases := []struct {
		name string
		cfg  glueRegistryConfig
		want string
	}{
		{"region required", glueRegistryConfig{}, "AWS Glue region is required"},
		{
			"static keys required",
			glueRegistryConfig{Region: "us-east-1", AuthMode: "static", AccessKeyID: "test-key"},
			"access key id and secret access key are required",
		},
		{
			"profile mode unsupported",
			glueRegistryConfig{Region: "us-east-1", AuthMode: "profile"},
			"glue_auth_mode=profile is not supported",
		},
		{
			"unknown mode",
			glueRegistryConfig{Region: "us-east-1", AuthMode: "magic"},
			"glue_auth_mode must be default or static",
		},
	}
	for _, tc := range cases {
		if _, err := newGlueSchemaBackend(tc.cfg, ""); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error = %v, want contains %q", tc.name, err, tc.want)
		}
	}
	// static 假凭据 + 默认 auth_mode 可构造（不发起请求）。
	backend, err := newGlueSchemaBackend(glueRegistryConfig{
		Region:          "us-east-1",
		RegistryName:    "smoke-registry",
		AuthMode:        "static",
		AccessKeyID:     "test-key",
		SecretAccessKey: fakeSecretAccessKey,
	}, "")
	if err != nil {
		t.Fatalf("newGlueSchemaBackend(static) error = %v", err)
	}
	if backend.provider() != schemaProviderGlue || backend.registryName != "smoke-registry" {
		t.Errorf("backend = %+v", backend)
	}
	// default auth_mode 也可构造（LoadDefaultConfig 惰性，不发起请求）。
	if _, err := newGlueSchemaBackend(glueRegistryConfig{Region: "us-east-1", RegistryName: "r"}, ""); err != nil {
		t.Errorf("newGlueSchemaBackend(default) error = %v", err)
	}
}

// --- Service 层全流程（httptest 假 Glue） ---

func TestGlueSchemaServiceFlow(t *testing.T) {
	fake := newFakeGlue()
	fake.seed("orders-value")
	newGlueTestServer(t, fake)

	service := NewService()
	glueConnect(t, service, "glue", "")
	ctx := context.Background()

	// test
	test, err := service.TestSchema(ctx, "glue", "")
	if err != nil {
		t.Fatalf("TestSchema() error = %v", err)
	}
	if !test.OK || test.Provider != schemaProviderGlue || test.SubjectCount != 1 {
		t.Errorf("TestSchema() = %+v", test)
	}

	// subjects/list
	subjects, err := service.ListSchemaSubjects(ctx, "glue", "")
	if err != nil {
		t.Fatalf("ListSchemaSubjects() error = %v", err)
	}
	if len(subjects.Subjects) != 1 || subjects.Subjects[0].Subject != "orders-value" {
		t.Fatalf("ListSchemaSubjects() = %+v", subjects)
	}
	if subjects.Subjects[0].Description != "desc of orders-value" {
		t.Errorf("Description = %q", subjects.Subjects[0].Description)
	}

	// versions/list（GUID 透出 + 格式来自 GetSchema）
	versions, err := service.ListSchemaVersions(ctx, "glue", "orders-value", "")
	if err != nil {
		t.Fatalf("ListSchemaVersions() error = %v", err)
	}
	if len(versions.Versions) != 2 || versions.Versions[0].Version != 1 || versions.Versions[1].Version != 2 {
		t.Fatalf("ListSchemaVersions() = %+v", versions)
	}
	if versions.Versions[0].VersionID != "guid-v1" || versions.Versions[0].Format != "AVRO" {
		t.Errorf("version row = %+v", versions.Versions[0])
	}
	if versions.Versions[0].ID != 0 {
		t.Errorf("glue numeric ID = %d, want 0", versions.Versions[0].ID)
	}

	// get（显式版本 + latest）
	got, err := service.GetSchema(ctx, "glue", "orders-value", 2, "")
	if err != nil {
		t.Fatalf("GetSchema(2) error = %v", err)
	}
	if got.Version != 2 || got.VersionID != "guid-v2" || got.ID != 0 || got.Format != "AVRO" {
		t.Errorf("GetSchema(2) = %+v", got)
	}
	if !strings.Contains(got.Schema, `"name"`) {
		t.Errorf("GetSchema(2).Schema = %q", got.Schema)
	}
	latest, err := service.GetSchema(ctx, "glue", "orders-value", 0, "")
	if err != nil || latest.Version != 2 {
		t.Errorf("GetSchema(latest) = %+v, err %v", latest, err)
	}

	// versions/compare（两个版本文本走既有 LCS diff）
	diff, err := service.CompareSchemaVersions(ctx, SchemaVersionsCompareRequest{
		ConnectionID: "glue", Subject: "orders-value", FromVersion: 1, ToVersion: 2,
	})
	if err != nil {
		t.Fatalf("CompareSchemaVersions() error = %v", err)
	}
	if diff.From != 1 || diff.To != 2 || len(diff.Hunks) == 0 || diff.Summary.Added == 0 {
		t.Errorf("CompareSchemaVersions() = %+v", diff)
	}

	// compatibility get/set
	compat, err := service.GetSchemaCompatibility(ctx, "glue", "orders-value", "")
	if err != nil || compat.Level != "BACKWARD" || compat.Scope != "subject" {
		t.Errorf("GetSchemaCompatibility() = %+v, err %v", compat, err)
	}
	if _, err := service.GetSchemaCompatibility(ctx, "glue", "", ""); err == nil ||
		!strings.Contains(err.Error(), "per-schema") {
		t.Errorf("GetSchemaCompatibility(global) error = %v, want per-schema error", err)
	}
	set, err := service.SetSchemaCompatibility(ctx, SchemaCompatibilityRequest{
		ConnectionID: "glue", Subject: "orders-value", Level: "FULL_TRANSITIVE",
	})
	if err != nil || set.Level != "FULL_ALL" {
		t.Errorf("SetSchemaCompatibility(FULL_TRANSITIVE) = %+v, err %v", set, err)
	}

	// compatibility check（Glue 纯语法校验语义）
	check, err := service.CheckSchemaCompatibility(ctx, SchemaCompatibilityCheckRequest{
		ConnectionID: "glue", Subject: "orders-value", Format: "avro", Schema: testAvroSchemaGlue,
	})
	if err != nil {
		t.Fatalf("CheckSchemaCompatibility() error = %v", err)
	}
	if !check.IsCompatible || len(check.Messages) == 0 ||
		!strings.Contains(check.Messages[0], "no side effects") {
		t.Errorf("CheckSchemaCompatibility() = %+v", check)
	}

	// register：schema 已存在 → RegisterSchemaVersion（版本 +1）
	registered, err := service.RegisterSchema(ctx, SchemaRegisterRequest{
		ConnectionID: "glue", Subject: "orders-value", Format: "avro", Schema: testAvroSchemaGlue,
	})
	if err != nil {
		t.Fatalf("RegisterSchema(existing) error = %v", err)
	}
	if registered.Version != 3 || registered.ID != 0 || registered.VersionID == "" {
		t.Errorf("RegisterSchema(existing) = %+v", registered)
	}

	// register：schema 不存在 → CreateSchema（首个版本 + 兼容级别缺省 NONE）
	registered, err = service.RegisterSchema(ctx, SchemaRegisterRequest{
		ConnectionID: "glue", Subject: "fresh-value", Format: "avro", Schema: testAvroSchemaGlue,
		Compatibility: "backward_all",
	})
	if err != nil {
		t.Fatalf("RegisterSchema(new) error = %v", err)
	}
	if registered.Version != 1 || registered.VersionID != "guid-created" {
		t.Errorf("RegisterSchema(new) = %+v", registered)
	}
	fresh := fake.schemas["fresh-value"]
	if fresh == nil || fresh.compatibility != "BACKWARD_ALL" {
		t.Errorf("CreateSchema compatibility not applied: %+v", fresh)
	}

	// register + normalize=true → Glue 无归一化语义，显式报错（不静默忽略、
	// 不发起任何 Glue 调用）。
	if _, err := service.RegisterSchema(ctx, SchemaRegisterRequest{
		ConnectionID: "glue", Subject: "normalized-value", Format: "avro", Schema: testAvroSchemaGlue,
		Normalize: true,
	}); err == nil || !strings.Contains(err.Error(), "normalize is not supported") {
		t.Errorf("RegisterSchema(normalize) error = %v", err)
	}
	if fake.schemas["normalized-value"] != nil {
		t.Error("RegisterSchema(normalize) must not create the schema")
	}

	// delete/version + delete subject
	deleted, err := service.DeleteSchema(ctx, SchemaDeleteRequest{
		ConnectionID: "glue", Subject: "orders-value", Version: 3,
	})
	if err != nil {
		t.Fatalf("DeleteSchema(version) error = %v", err)
	}
	if len(deleted.DeletedVersions) != 1 || deleted.DeletedVersions[0] != 3 {
		t.Errorf("DeleteSchema(version) = %+v", deleted)
	}
	deleted, err = service.DeleteSchema(ctx, SchemaDeleteRequest{ConnectionID: "glue", Subject: "orders-value"})
	if err != nil || len(deleted.DeletedVersions) != 0 {
		t.Errorf("DeleteSchema(subject) = %+v, err %v", deleted, err)
	}
	if !fake.schemas["orders-value"].deleted {
		t.Error("DeleteSchema(subject) did not mark schema deleted")
	}

	// 错误透传：不存在的 subject → Glue EntityNotFoundException 文案
	if _, err := service.GetSchema(ctx, "glue", "missing-value", 1, ""); err == nil ||
		!strings.Contains(err.Error(), "Schema") {
		t.Errorf("GetSchema(missing) error = %v", err)
	}
}

// --- 挂载门禁与 statuses 摘要 ---

func TestSchemaMountGlueRejected(t *testing.T) {
	fake := newFakeGlue()
	newGlueTestServer(t, fake)

	service := NewService()
	glueConnect(t, service, "glue", "")
	ctx := context.Background()

	// produce schema 挂载 → 业务错（tinyrdm 同款语义文案）
	_, err := service.Produce(ctx, ProduceRequest{
		ConnectionID: "glue", Topic: "t", Value: "{}",
		Schema: &SchemaRef{Subject: "orders-value", Format: "avro"},
	})
	if err == nil || !strings.Contains(err.Error(), "Confluent wire format") ||
		!strings.Contains(err.Error(), "AWS Glue") {
		t.Errorf("Produce(glue mount) error = %v", err)
	}

	// consume schema 挂载 → 同款业务错
	_, err = service.Consume(ctx, ConsumeParams{
		ConnectionID: "glue", Topic: "t",
		Schema: &SchemaRef{Subject: "orders-value"},
	})
	if err == nil || !strings.Contains(err.Error(), "schema-aware consume") {
		t.Errorf("Consume(glue mount) error = %v", err)
	}

	// stream start schema 挂载 → 同款业务错
	_, err = service.StartStream(ConsumeParams{
		ConnectionID: "glue", Topic: "t",
		Schema: &SchemaRef{Subject: "orders-value"},
	})
	if err == nil || !strings.Contains(err.Error(), "AWS Glue") {
		t.Errorf("StartStream(glue mount) error = %v", err)
	}

	// registry 显式 glue 同样拒绝；显式未知值 → 参数错
	_, err = service.Produce(ctx, ProduceRequest{
		ConnectionID: "glue", Topic: "t", Value: "{}",
		Schema: &SchemaRef{Registry: "glue", Subject: "x"},
	})
	if err == nil || !strings.Contains(err.Error(), "AWS Glue") {
		t.Errorf("Produce(registry=glue) error = %v", err)
	}
	_, err = service.Produce(ctx, ProduceRequest{
		ConnectionID: "glue", Topic: "t", Value: "{}",
		Schema: &SchemaRef{Registry: "registry", Subject: "x"},
	})
	var paramErr *InvalidParamsError
	if !errors.As(err, &paramErr) {
		t.Errorf("Produce(registry unknown) error = %v, want *InvalidParamsError", err)
	}

	// glue-only 连接的 TestSchema 正常工作
	if _, err := service.TestSchema(ctx, "glue", ""); err != nil {
		t.Errorf("TestSchema on glue-only error = %v", err)
	}
	params, err := lifecycle.Parse([]byte(`{
	  "connection": {
	    "id": "both",
	    "name": "both",
	    "external_config": {
	      "bootstrap_servers": "k1:9092",
	      "sr_url": "http://127.0.0.1:1",
	      "glue_region": "us-east-1",
	      "glue_registry_name": "r"
	    }
	  }
	}`))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if err := service.Connect(params); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if _, err := service.TestSchema(ctx, "both", ""); !errors.As(err, &paramErr) {
		t.Errorf("TestSchema(both) error = %v, want *InvalidParamsError", err)
	}
	// 显式 registry 解歧后可走对应后端（confluent 指向不可达地址 → 普通错误）。
	if _, err := service.TestSchema(ctx, "both", "confluent"); err == nil ||
		errors.As(err, &paramErr) {
		t.Errorf("TestSchema(both, confluent) error = %v, want non-param error", err)
	}
}

func TestSchemaRegistryStatusProvider(t *testing.T) {
	fake := newFakeGlue()
	newGlueTestServer(t, fake)

	service := NewService()
	schemaConnect(t, service, "sr-conn", "http://127.0.0.1:1", "", false, false)
	glueConnect(t, service, "glue-conn", "")
	params, err := lifecycle.Parse([]byte(`{
	  "connection": {
	    "id": "plain",
	    "name": "plain",
	    "external_config": { "bootstrap_servers": "k1:9092" }
	  }
	}`))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if err := service.Connect(params); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	providers := map[string]SchemaRegistryStatus{}
	for _, status := range service.SnapshotStatuses() {
		if status.SchemaRegistry == nil {
			t.Errorf("connection %s missing schemaRegistry summary", status.ConnectionID)
			continue
		}
		providers[status.ConnectionID] = *status.SchemaRegistry
	}
	if got := providers["sr-conn"]; got.Provider != schemaProviderConfluent || got.URL != "http://127.0.0.1:1" {
		t.Errorf("sr-conn schemaRegistry = %+v", got)
	}
	if got := providers["glue-conn"]; got.Provider != schemaProviderGlue || got.RegistryName != "smoke-registry" {
		t.Errorf("glue-conn schemaRegistry = %+v", got)
	}
	if got := providers["plain"]; got.Enabled || got.Provider != schemaProviderNone {
		t.Errorf("plain schemaRegistry = %+v", got)
	}
}
