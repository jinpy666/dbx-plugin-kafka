package kafkaconn

// schema.go：Confluent 兼容 Schema Registry（Phase 2，IMPL_PLAN §0.2/§5）。
// 逻辑参照 tiny-rdm backend/services/kafka_schema_registry.go 重写：
//   - REST 客户端（net/http，basic auth 可空，10s 超时；仅 Confluent 兼容
//     REST —— 含 Redpanda 内置 SR；AWS Glue 登记 Phase 3 不做）；
//   - Confluent wire format：magic byte 0 + 4 字节大端 schemaID + 载荷；
//   - AVRO 编解码（goavro：JSON 文本 ⇄ 二进制）、JSON Schema 校验
//     （jsonschema-go）、PROTOBUF 编解码（Phase 3，§12.2.2：FDSet 动态消息
//     ⇄ JSON，protojson 渲染）；
//   - 版本 LCS 逐行 diff（语义 hunks/summary）；
//   - 兼容性 get/set/check、register/delete、subjects/versions 列表；
//   - per-consume 元数据缓存（tinyrdm metadata cache 思路：一次消费内按
//     schemaID / subject+version 去重，避免逐消息打 SR）。
//
// 凭据红线：sr_password 仅存 connSecrets，只用于 basic auth 头，不落日志/
// 审计/事件（§6）。

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	jsonschema "github.com/google/jsonschema-go/jsonschema"
	goavro "github.com/linkedin/goavro/v2"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// confluentWireMagicByte 是 Confluent wire format 魔数字节。
const confluentWireMagicByte byte = 0

// schemaRegistryTimeout SR REST 请求超时（任务契约 10s）。
const schemaRegistryTimeout = 10 * time.Second

// SchemaReference schema 引用（Confluent references[]）。
type SchemaReference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

// SchemaMeta 是一条 subject/version 元数据（SR REST 形状；Glue 后端复用：
// ID 恒 0（无数字 id）、VersionID 为 Glue GUID）。
type SchemaMeta struct {
	Subject    string            `json:"subject"`
	Version    int64             `json:"version"`
	ID         int               `json:"id"`
	Schema     string            `json:"schema"`
	SchemaType string            `json:"schemaType,omitempty"`
	References []SchemaReference `json:"references,omitempty"`
	VersionID  string            `json:"versionId,omitempty"`
}

// schemaRegistryClient 是 Confluent 兼容 SR 的最小 REST 客户端。
type schemaRegistryClient struct {
	baseURL  string
	username string
	password string
	http     *http.Client
}

// newSchemaRegistryClient 由 Profile 构建 SR 客户端（sr_url 空 = SR 未配置，
// 业务错）；password 来自 connSecrets，仅进 Authorization 头。
func newSchemaRegistryClient(profile Profile, srPassword string) (*schemaRegistryClient, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(profile.SRURL), "/")
	if baseURL == "" {
		return nil, errf("schema registry is not configured for connection %q (srUrl is empty)", profile.Name)
	}
	if _, err := url.ParseRequestURI(baseURL); err != nil {
		return nil, fmt.Errorf("schema registry URL is invalid: %w", err)
	}
	return &schemaRegistryClient{
		baseURL:  baseURL,
		username: profile.SRUsername,
		password: srPassword,
		http:     &http.Client{Timeout: schemaRegistryTimeout},
	}, nil
}

// request 执行一次 SR REST 调用（payload 非 nil 时序列化为 JSON body）。
// 非 2xx 把响应体（截断）带进错误信息（tinyrdm 同款）。
func (c *schemaRegistryClient) request(ctx context.Context, method, path string, payload any, target any) error {
	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json, application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	}
	if strings.TrimSpace(c.username) != "" || c.password != "" {
		req.SetBasicAuth(strings.TrimSpace(c.username), c.password)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024*1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("schema registry %s %s failed (HTTP %d): %s", method, path, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if target == nil || len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("decode schema registry response: %w", err)
	}
	return nil
}

// --- SR 领域操作（schemaService.go 层方法使用） ---

func (c *schemaRegistryClient) listSubjects(ctx context.Context) ([]string, error) {
	var subjects []string
	if err := c.request(ctx, http.MethodGet, "/subjects", nil, &subjects); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(subjects))
	for _, subject := range subjects {
		subject = strings.TrimSpace(subject)
		// SR 对形如 "a/b" 的 subject 返回以 / 结尾的探测条目，跳过。
		if subject == "" || strings.HasSuffix(subject, "/") {
			continue
		}
		names = append(names, subject)
	}
	sort.Strings(names)
	return names, nil
}

func (c *schemaRegistryClient) listVersions(ctx context.Context, subject string) ([]int, error) {
	var versions []int
	if err := c.request(ctx, http.MethodGet, "/subjects/"+url.PathEscape(subject)+"/versions", nil, &versions); err != nil {
		return nil, err
	}
	sort.Ints(versions)
	return versions, nil
}

// getSchema 取 subject 指定版本元数据；version<=0 取 latest。
func (c *schemaRegistryClient) getSchema(ctx context.Context, subject string, version int64) (SchemaMeta, error) {
	vPath := "latest"
	if version > 0 {
		vPath = strconv.FormatInt(version, 10)
	}
	var meta SchemaMeta
	if err := c.request(ctx, http.MethodGet, "/subjects/"+url.PathEscape(subject)+"/versions/"+vPath, nil, &meta); err != nil {
		return SchemaMeta{}, err
	}
	if meta.Subject == "" {
		meta.Subject = subject
	}
	return meta, nil
}

func (c *schemaRegistryClient) getSchemaByID(ctx context.Context, id int) (SchemaMeta, error) {
	var meta SchemaMeta
	if err := c.request(ctx, http.MethodGet, "/schemas/ids/"+strconv.Itoa(id), nil, &meta); err != nil {
		return SchemaMeta{}, err
	}
	meta.ID = id
	return meta, nil
}

func (c *schemaRegistryClient) registerSchema(ctx context.Context, subject string, format, schema string, refs []SchemaReference) (SchemaMeta, error) {
	var registered struct {
		ID int `json:"id"`
	}
	err := c.request(ctx, http.MethodPost, "/subjects/"+url.PathEscape(subject)+"/versions", schemaRegisterRequest{
		Schema:     schema,
		SchemaType: format,
		References: refs,
	}, &registered)
	if err != nil {
		return SchemaMeta{}, err
	}
	meta, err := c.getSchema(ctx, subject, 0)
	if err != nil {
		// 注册成功但回读 latest 失败：返回注册 id、版本未知（0）。
		return SchemaMeta{Subject: subject, ID: registered.ID, Schema: schema, SchemaType: format}, nil
	}
	meta.ID = registered.ID
	return meta, nil
}

func (c *schemaRegistryClient) deleteSubject(ctx context.Context, subject string) ([]int64, error) {
	var raw json.RawMessage
	if err := c.request(ctx, http.MethodDelete, "/subjects/"+url.PathEscape(subject), nil, &raw); err != nil {
		return nil, err
	}
	return decodeDeletedVersions(raw)
}

func (c *schemaRegistryClient) deleteVersion(ctx context.Context, subject string, version int64) ([]int64, error) {
	var raw json.RawMessage
	if err := c.request(ctx, http.MethodDelete,
		"/subjects/"+url.PathEscape(subject)+"/versions/"+strconv.FormatInt(version, 10), nil, &raw); err != nil {
		return nil, err
	}
	return decodeDeletedVersions(raw)
}

// decodeDeletedVersions 兼容 Confluent（DELETE subject → 版本号数组）与
// Redpanda（DELETE version → 单个版本号数字）两种响应形状。
func decodeDeletedVersions(raw json.RawMessage) ([]int64, error) {
	trimmed := bytes.TrimLeft(raw, " \t\r\n")
	if len(trimmed) > 0 && trimmed[0] == '[' {
		var list []int64
		if err := json.Unmarshal(raw, &list); err != nil {
			return nil, fmt.Errorf("decode schema registry response: %w", err)
		}
		return list, nil
	}
	var single int64
	if err := json.Unmarshal(raw, &single); err != nil {
		return nil, fmt.Errorf("decode schema registry response: %w", err)
	}
	return []int64{single}, nil
}

// compatibilityResponse 兼容 Confluent 两种字段名（compatibility /
// compatibilityLevel，Redpanda 用后者）。
type compatibilityResponse struct {
	Compatibility      string `json:"compatibility,omitempty"`
	CompatibilityLevel string `json:"compatibilityLevel,omitempty"`
}

func (r compatibilityResponse) level() string {
	if v := strings.TrimSpace(r.Compatibility); v != "" {
		return v
	}
	return strings.TrimSpace(r.CompatibilityLevel)
}

func (c *schemaRegistryClient) getCompatibility(ctx context.Context, subject string) (compatibilityResponse, error) {
	path := "/config"
	if subject != "" {
		path = "/config/" + url.PathEscape(subject)
	}
	var out compatibilityResponse
	if err := c.request(ctx, http.MethodGet, path, nil, &out); err != nil {
		return compatibilityResponse{}, err
	}
	return out, nil
}

func (c *schemaRegistryClient) setCompatibility(ctx context.Context, subject, level string) (compatibilityResponse, error) {
	path := "/config"
	if subject != "" {
		path = "/config/" + url.PathEscape(subject)
	}
	var out compatibilityResponse
	if err := c.request(ctx, http.MethodPut, path, map[string]string{"compatibility": level}, &out); err != nil {
		return compatibilityResponse{}, err
	}
	return out, nil
}

func (c *schemaRegistryClient) checkCompatibility(ctx context.Context, subject string, version int64, format, schema string, refs []SchemaReference) (bool, []string, error) {
	vPath := "latest"
	if version > 0 {
		vPath = strconv.FormatInt(version, 10)
	}
	var out struct {
		IsCompatible bool     `json:"is_compatible"`
		Messages     []string `json:"messages,omitempty"`
	}
	err := c.request(ctx, http.MethodPost,
		"/compatibility/subjects/"+url.PathEscape(subject)+"/versions/"+vPath,
		schemaRegisterRequest{Schema: schema, SchemaType: format, References: refs}, &out)
	if err != nil {
		return false, nil, err
	}
	return out.IsCompatible, out.Messages, nil
}

// --- 请求/返回类型（契约 §5 schema 方法族，camelCase） ---

// SchemaRef 是 produce/consume 的 schema 挂载参数。
type SchemaRef struct {
	// Registry 显式指定 SR 后端（confluent | glue；缺省自动探测——
	// glue 挂载不支持，见 schemaMountSupported）。produce/consume/stream。
	Registry string `json:"registry,omitempty"`
	// Subject 必填；consume 时为空则按 wire format 里的 schemaID 反查。
	Subject string `json:"subject,omitempty"`
	// Version 可空（=latest；consume 时配合 subject 定位元数据）。
	Version int64 `json:"version,omitempty"`
	// Format：avro | json | protobuf（可空 = 按注册元数据的 schemaType）。
	Format string `json:"format,omitempty"`
}

// SchemaTestResult 对应 kafka/schema/test。
type SchemaTestResult struct {
	OK                bool     `json:"ok"`
	Version           string   `json:"version"`
	CompatibleFormats []string `json:"compatibleFormats"`
	SubjectCount      int      `json:"subjectCount,omitempty"`
	// Provider 是本次探测命中的后端：confluent | glue（连接未配置任何
	// SR 时方法直接 -32000 业务错，provider=none 仅用于 statuses 摘要）。
	Provider string `json:"provider"`
}

// SubjectInfo 对应 subjects/list 行。
type SubjectInfo struct {
	Subject            string   `json:"subject"`
	Formats            []string `json:"formats"`
	LatestVersion      int64    `json:"latestVersion,omitempty"`
	CompatibilityLevel string   `json:"compatibilityLevel,omitempty"`
	// Description 仅 Glue 后端填充（ListSchemas 原生字段）。
	Description string `json:"description,omitempty"`
}

// SubjectsListResult 对应 kafka/schema/subjects/list。
type SubjectsListResult struct {
	Subjects []SubjectInfo `json:"subjects"`
}

// SchemaVersionInfo 对应 versions/list 行。
type SchemaVersionInfo struct {
	Version int64  `json:"version"`
	ID      int    `json:"id"`
	Format  string `json:"format"`
	// VersionID 仅 Glue 后端填充（Glue GUID；Confluent 无此概念）。
	VersionID string `json:"versionId,omitempty"`
}

// SchemaVersionsListResult 对应 kafka/schema/versions/list。
type SchemaVersionsListResult struct {
	Versions []SchemaVersionInfo `json:"versions"`
}

// SchemaGetResult 对应 kafka/schema/get。
type SchemaGetResult struct {
	Subject    string            `json:"subject"`
	Version    int64             `json:"version"`
	ID         int               `json:"id"`
	Schema     string            `json:"schema"`
	Format     string            `json:"format"`
	References []SchemaReference `json:"references,omitempty"`
	// VersionID 仅 Glue 后端填充。
	VersionID string `json:"versionId,omitempty"`
}

// SchemaVersionsCompareRequest 对应 kafka/schema/versions/compare。
type SchemaVersionsCompareRequest struct {
	ConnectionID string `json:"connectionId"`
	Subject      string `json:"subject"`
	FromVersion  int64  `json:"fromVersion"`
	ToVersion    int64  `json:"toVersion"`
	// Registry 可选（confluent | glue；缺省自动探测，双配置须显式）。
	Registry string `json:"registry,omitempty"`
}

// SchemaDiffHunk 一个语义 diff 块（op: add | remove | modify）。
type SchemaDiffHunk struct {
	Op     string `json:"op"`
	Path   string `json:"path"`
	Before string `json:"before,omitempty"`
	After  string `json:"after,omitempty"`
}

// SchemaDiffSummary diff 统计。
type SchemaDiffSummary struct {
	Added       int `json:"added"`
	Removed     int `json:"removed"`
	Unchanged   int `json:"unchanged"`
	BeforeLines int `json:"beforeLines"`
	AfterLines  int `json:"afterLines"`
}

// SchemaDiffResult 对应 kafka/schema/versions/compare。
type SchemaDiffResult struct {
	Subject string            `json:"subject"`
	From    int64             `json:"from"`
	To      int64             `json:"to"`
	Hunks   []SchemaDiffHunk  `json:"hunks"`
	Summary SchemaDiffSummary `json:"summary"`
}

// SchemaCompatibilityRequest 对应 compatibility/get|set（subject 空 = 全局；
// Glue 后端 subject 必填——per-schema 级别，无全局）。
type SchemaCompatibilityRequest struct {
	ConnectionID string `json:"connectionId"`
	Subject      string `json:"subject,omitempty"`
	// Level 仅 set 必填。
	Level string `json:"level,omitempty"`
	// Registry 可选（confluent | glue；缺省自动探测）。
	Registry string `json:"registry,omitempty"`
	// Version 仅 glue set 使用：UpdateSchema 版本检查点（缺省 = latest）。
	Version int64 `json:"version,omitempty"`
}

// SchemaCompatibilityResult 对应 compatibility/get|set。
type SchemaCompatibilityResult struct {
	Level string `json:"level"`
	Scope string `json:"scope"` // global | subject
}

// SchemaCompatibilityCheckRequest 对应 compatibility/check。
type SchemaCompatibilityCheckRequest struct {
	ConnectionID string            `json:"connectionId"`
	Subject      string            `json:"subject"`
	Format       string            `json:"format"`
	Schema       string            `json:"schema"`
	Version      int64             `json:"version,omitempty"`
	References   []SchemaReference `json:"references,omitempty"`
	// Registry 可选（confluent | glue；缺省自动探测。glue 为纯语法
	// 有效性校验，version/references 不参与）。
	Registry string `json:"registry,omitempty"`
}

// SchemaCompatibilityCheckResult 对应 compatibility/check。
type SchemaCompatibilityCheckResult struct {
	IsCompatible bool     `json:"isCompatible"`
	Messages     []string `json:"messages"`
}

// SchemaRegisterRequest 对应 kafka/schema/register（写，过 read_only）。
type SchemaRegisterRequest struct {
	ConnectionID string            `json:"connectionId"`
	Subject      string            `json:"subject"`
	Format       string            `json:"format"`
	Schema       string            `json:"schema"`
	References   []SchemaReference `json:"references,omitempty"`
	// Registry 可选（confluent | glue；缺省自动探测）。
	Registry string `json:"registry,omitempty"`
	// Compatibility 仅 glue CreateSchema（schema 不存在时）使用；
	// 取值面见 normalizeGlueCompatibility，缺省 NONE。
	Compatibility string `json:"compatibility,omitempty"`
}

// SchemaRegisterResult 对应 kafka/schema/register。
type SchemaRegisterResult struct {
	ID      int   `json:"id"`
	Version int64 `json:"version"`
	// VersionID 仅 glue 后端填充（Glue GUID）；Confluent 无此概念。
	VersionID string `json:"versionId,omitempty"`
}

// SchemaDeleteRequest 对应 kafka/schema/delete 与 delete/version（critical）。
type SchemaDeleteRequest struct {
	ConnectionID string `json:"connectionId"`
	Subject      string `json:"subject"`
	Version      int64  `json:"version,omitempty"`
	// Registry 可选（confluent | glue；缺省自动探测。glue：version>0 删
	// 单版本，version=0 删整个 schema——Glue 不返回被删版本号清单）。
	Registry string `json:"registry,omitempty"`
}

// SchemaDeleteResult 对应 kafka/schema/delete 与 delete/version。
type SchemaDeleteResult struct {
	DeletedVersions []int64 `json:"deletedVersions"`
}

// --- Service 方法（11 个 schema 方法；门禁见 policy.go；provider 分发见
// schemaBackend 接口与 glueBackend 实现） ---

// schemaBackend 是 provider 无关的 SR 管理面（Confluent REST 与 AWS Glue
// 双实现；Service 层方法只面向此接口）。
type schemaBackend interface {
	provider() string
	test(ctx context.Context) (subjectCount int, version string, err error)
	listSubjects(ctx context.Context) ([]SubjectInfo, error)
	listVersions(ctx context.Context, subject string) ([]SchemaVersionInfo, error)
	getSchema(ctx context.Context, subject string, version int64) (SchemaMeta, error)
	getCompatibility(ctx context.Context, subject string) (string, error)
	setCompatibility(ctx context.Context, subject, level string, version int64) (string, error)
	checkCompatibility(ctx context.Context, subject string, version int64, format, schema string, refs []SchemaReference) (bool, []string, error)
	registerSchema(ctx context.Context, subject, format, schema string, refs []SchemaReference, compatibility string) (SchemaMeta, error)
	deleteSchema(ctx context.Context, subject string, version int64) ([]int64, error)
}

// confluentBackend 是 schemaRegistryClient 的 schemaBackend 适配器
// （Confluent 兼容 REST；兼容级别归一化收敛在适配器内）。
type confluentBackend struct{ client *schemaRegistryClient }

func (b *confluentBackend) provider() string { return schemaProviderConfluent }

func (b *confluentBackend) test(ctx context.Context) (int, string, error) {
	subjects, err := b.client.listSubjects(ctx)
	if err != nil {
		return 0, "", err
	}
	// version：Redpanda 内置 SR 暴露 /v1/metadata/id（含版本），Confluent
	// 无此端点——失败不阻断测试（version 留空 = 未知）。
	var meta struct {
		Version string `json:"version"`
	}
	if err := b.client.request(ctx, http.MethodGet, "/v1/metadata/id", nil, &meta); err != nil {
		return len(subjects), "", nil
	}
	return len(subjects), strings.TrimSpace(meta.Version), nil
}

func (b *confluentBackend) listSubjects(ctx context.Context) ([]SubjectInfo, error) {
	subjects, err := b.client.listSubjects(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]SubjectInfo, 0, len(subjects))
	for _, subject := range subjects {
		item := SubjectInfo{Subject: subject, Formats: []string{}}
		// 逐条最新版补 format/latestVersion；兼容级别逐条查（404 = 未覆盖，
		// 继承全局，省略）。
		if meta, err := b.client.getSchema(ctx, subject, 0); err == nil {
			item.Formats = []string{normalizeConfluentSchemaType(meta.SchemaType)}
			item.LatestVersion = meta.Version
		} else {
			item.Formats = []string{"UNKNOWN"}
		}
		if compat, err := b.client.getCompatibility(ctx, subject); err == nil {
			item.CompatibilityLevel = compat.level()
		}
		out = append(out, item)
	}
	return out, nil
}

func (b *confluentBackend) listVersions(ctx context.Context, subject string) ([]SchemaVersionInfo, error) {
	versions, err := b.client.listVersions(ctx, subject)
	if err != nil {
		return nil, err
	}
	out := make([]SchemaVersionInfo, 0, len(versions))
	for _, version := range versions {
		format := "UNKNOWN"
		if meta, err := b.client.getSchema(ctx, subject, int64(version)); err == nil {
			format = normalizeConfluentSchemaType(meta.SchemaType)
		}
		out = append(out, SchemaVersionInfo{
			Version: int64(version),
			Format:  format,
			ID:      schemaIDForVersion(b.client, subject, int64(version)),
		})
	}
	return out, nil
}

// schemaIDForVersion 尽力补 id（失败返回 0，不阻断列表）。
func schemaIDForVersion(client *schemaRegistryClient, subject string, version int64) int {
	ctx, cancel := context.WithTimeout(context.Background(), schemaRegistryTimeout)
	defer cancel()
	meta, err := client.getSchema(ctx, subject, version)
	if err != nil {
		return 0
	}
	return meta.ID
}

func (b *confluentBackend) getSchema(ctx context.Context, subject string, version int64) (SchemaMeta, error) {
	return b.client.getSchema(ctx, subject, version)
}

func (b *confluentBackend) getCompatibility(ctx context.Context, subject string) (string, error) {
	compat, err := b.client.getCompatibility(ctx, subject)
	if err != nil {
		return "", err
	}
	level := compat.level()
	if level == "" {
		return "", errf("schema compatibility level is empty")
	}
	return level, nil
}

func (b *confluentBackend) setCompatibility(ctx context.Context, subject, level string, _ int64) (string, error) {
	normalized, err := normalizeConfluentCompatibility(level)
	if err != nil {
		return "", err
	}
	compat, err := b.client.setCompatibility(ctx, subject, normalized)
	if err != nil {
		return "", err
	}
	if applied := compat.level(); applied != "" {
		return applied, nil
	}
	return normalized, nil
}

func (b *confluentBackend) checkCompatibility(ctx context.Context, subject string, version int64, format, schema string, refs []SchemaReference) (bool, []string, error) {
	return b.client.checkCompatibility(ctx, subject, version, format, schema, refs)
}

func (b *confluentBackend) registerSchema(ctx context.Context, subject, format, schema string, refs []SchemaReference, _ string) (SchemaMeta, error) {
	return b.client.registerSchema(ctx, subject, format, schema, refs)
}

func (b *confluentBackend) deleteSchema(ctx context.Context, subject string, version int64) ([]int64, error) {
	if version > 0 {
		return b.client.deleteVersion(ctx, subject, version)
	}
	return b.client.deleteSubject(ctx, subject)
}

// schemaBackendFor 解析 registry 参数并构建对应后端（confluent 客户端密码
// 与 glue 凭据均从连接表 connSecrets 读，不入 Profile/日志）。
func (s *Service) schemaBackendFor(connectionID, registry string) (schemaBackend, error) {
	entry := s.lookup(connectionID)
	if entry == nil {
		return nil, errConnectionNotFound(connectionID)
	}
	entry.mu.Lock()
	profile := entry.profile
	srPassword := entry.secrets.SRPassword
	glueSecret := entry.secrets.GlueSecretAccessKey
	glueToken := entry.secrets.GlueSessionToken
	entry.mu.Unlock()

	provider, err := resolveSchemaProvider(profile, registry)
	if err != nil {
		return nil, err
	}
	switch provider {
	case schemaProviderConfluent:
		client, err := newSchemaRegistryClient(profile, srPassword)
		if err != nil {
			return nil, err
		}
		return &confluentBackend{client: client}, nil
	case schemaProviderGlue:
		return newGlueSchemaBackend(glueRegistryConfig{
			Region:          profile.GlueRegion,
			RegistryName:    profile.GlueRegistryName,
			AuthMode:        profile.GlueAuthMode,
			AccessKeyID:     profile.GlueAccessKeyID,
			SecretAccessKey: glueSecret,
			SessionToken:    glueToken,
		}, glueBaseEndpointOverride)
	default:
		return nil, errf("schema registry is not enabled for connection %q (set schemaRegistry to confluent or aws_glue)", profile.Name)
	}
}

// confluentClientFor 取连接的 Confluent SR 客户端（produce/consume/stream
// 的 schema 挂载专用——wire format 编解码仅 Confluent；glue 门禁见
// schemaMountSupported，调用方须先过门禁）。密码从连接表 connSecrets 读，
// 不入 Profile/日志。
func (s *Service) confluentClientFor(connectionID string) (*schemaRegistryClient, error) {
	entry := s.lookup(connectionID)
	if entry == nil {
		return nil, errConnectionNotFound(connectionID)
	}
	entry.mu.Lock()
	profile := entry.profile
	srPassword := entry.secrets.SRPassword
	entry.mu.Unlock()
	return newSchemaRegistryClient(profile, srPassword)
}

// schemaMountSupported 是 produce/consume/stream 的 schema{} 挂载门禁：
// wire format 编解码仅支持 Confluent（tinyrdm 同款语义）；provider=glue →
// 业务错；双配置歧义/未知 registry → 参数错；未配置任何 SR → 业务错。
func (s *Service) schemaMountSupported(connectionID, registry, action string) error {
	entry := s.lookup(connectionID)
	if entry == nil {
		return errConnectionNotFound(connectionID)
	}
	entry.mu.Lock()
	profile := entry.profile
	entry.mu.Unlock()
	provider, err := resolveSchemaProvider(profile, registry)
	if err != nil {
		return err
	}
	switch provider {
	case schemaProviderGlue:
		return errf("schema-aware %s currently supports Confluent wire format; AWS Glue schema management is available", action)
	case schemaProviderNone:
		return errf("schema registry is not enabled for connection %q (schemaRegistry is none)", profile.Name)
	}
	return nil
}

// TestSchema 实现 kafka/schema/test（SR 可达性探测 + 支持的格式清单）。
func (s *Service) TestSchema(ctx context.Context, connectionID, registry string) (*SchemaTestResult, error) {
	backend, err := s.schemaBackendFor(connectionID, registry)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, schemaRegistryTimeout)
	defer cancel()

	subjectCount, version, err := backend.test(ctx)
	if err != nil {
		return nil, err
	}
	return &SchemaTestResult{
		OK:                true,
		Version:           version,
		CompatibleFormats: []string{"AVRO", "JSON"},
		SubjectCount:      subjectCount,
		Provider:          backend.provider(),
	}, nil
}

// ListSchemaSubjects 实现 kafka/schema/subjects/list。
func (s *Service) ListSchemaSubjects(ctx context.Context, connectionID, registry string) (*SubjectsListResult, error) {
	backend, err := s.schemaBackendFor(connectionID, registry)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, adminTimeout)
	defer cancel()

	subjects, err := backend.listSubjects(ctx)
	if err != nil {
		return nil, err
	}
	return &SubjectsListResult{Subjects: subjects}, nil
}

// ListSchemaVersions 实现 kafka/schema/versions/list。
func (s *Service) ListSchemaVersions(ctx context.Context, connectionID, subject, registry string) (*SchemaVersionsListResult, error) {
	subject = trimSpace(subject)
	if subject == "" {
		return nil, errf("subject is required")
	}
	backend, err := s.schemaBackendFor(connectionID, registry)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, adminTimeout)
	defer cancel()

	versions, err := backend.listVersions(ctx, subject)
	if err != nil {
		return nil, err
	}
	return &SchemaVersionsListResult{Versions: versions}, nil
}

// GetSchema 实现 kafka/schema/get。
func (s *Service) GetSchema(ctx context.Context, connectionID, subject string, version int64, registry string) (*SchemaGetResult, error) {
	subject = trimSpace(subject)
	if subject == "" {
		return nil, errf("subject is required")
	}
	backend, err := s.schemaBackendFor(connectionID, registry)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, adminTimeout)
	defer cancel()

	meta, err := backend.getSchema(ctx, subject, version)
	if err != nil {
		return nil, err
	}
	return &SchemaGetResult{
		Subject:    meta.Subject,
		Version:    meta.Version,
		ID:         meta.ID,
		Schema:     meta.Schema,
		Format:     normalizeConfluentSchemaType(meta.SchemaType),
		References: meta.References,
		VersionID:  meta.VersionID,
	}, nil
}

// CompareSchemaVersions 实现 kafka/schema/versions/compare（LCS 逐行 diff；
// glue 后端同样拉两版 schema 文本走既有 diff）。
func (s *Service) CompareSchemaVersions(ctx context.Context, req SchemaVersionsCompareRequest) (*SchemaDiffResult, error) {
	subject := trimSpace(req.Subject)
	if subject == "" {
		return nil, errf("subject is required")
	}
	if req.FromVersion <= 0 || req.ToVersion <= 0 {
		return nil, errf("fromVersion and toVersion must be positive version numbers")
	}
	backend, err := s.schemaBackendFor(req.ConnectionID, req.Registry)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, adminTimeout)
	defer cancel()

	fromMeta, err := backend.getSchema(ctx, subject, req.FromVersion)
	if err != nil {
		return nil, err
	}
	toMeta, err := backend.getSchema(ctx, subject, req.ToVersion)
	if err != nil {
		return nil, err
	}
	hunks, summary := schemaTextDiff(fromMeta.Schema, toMeta.Schema)
	return &SchemaDiffResult{
		Subject: subject,
		From:    fromMeta.Version,
		To:      toMeta.Version,
		Hunks:   hunks,
		Summary: summary,
	}, nil
}

// GetSchemaCompatibility 实现 kafka/schema/compatibility/get（subject 空 =
// 全局；glue 后端 subject 必填，per-schema 级别）。
func (s *Service) GetSchemaCompatibility(ctx context.Context, connectionID, subject, registry string) (*SchemaCompatibilityResult, error) {
	backend, err := s.schemaBackendFor(connectionID, registry)
	if err != nil {
		return nil, err
	}
	subject = trimSpace(subject)
	ctx, cancel := context.WithTimeout(ctx, adminTimeout)
	defer cancel()

	level, err := backend.getCompatibility(ctx, subject)
	if err != nil {
		return nil, err
	}
	return &SchemaCompatibilityResult{Level: level, Scope: compatibilityScope(subject)}, nil
}

// SetSchemaCompatibility 实现 kafka/schema/compatibility/set（写门禁 + 审计；
// 兼容级别归一化在 provider 适配器内：confluent → *_TRANSITIVE 语义，
// glue → *_ALL/DISABLED 枚举）。
func (s *Service) SetSchemaCompatibility(ctx context.Context, req SchemaCompatibilityRequest) (*SchemaCompatibilityResult, error) {
	subject := trimSpace(req.Subject)
	if err := ensureWriteAllowed(s.profileOf(req.ConnectionID), "schema/compatibility/set"); err != nil {
		s.emitAudit(req.ConnectionID, "schema-compatibility-set", subjectOrGlobal(subject), "blocked", err.Error())
		return nil, err
	}
	backend, err := s.schemaBackendFor(req.ConnectionID, req.Registry)
	if err != nil {
		s.emitAudit(req.ConnectionID, "schema-compatibility-set", subjectOrGlobal(subject), "error", err.Error())
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, adminTimeout)
	defer cancel()

	applied, err := backend.setCompatibility(ctx, subject, req.Level, req.Version)
	if err != nil {
		s.emitAudit(req.ConnectionID, "schema-compatibility-set", subjectOrGlobal(subject), "error", err.Error())
		return nil, err
	}
	s.emitAudit(req.ConnectionID, "schema-compatibility-set", subjectOrGlobal(subject), "success", sprintf("level=%s", applied))
	return &SchemaCompatibilityResult{Level: applied, Scope: compatibilityScope(subject)}, nil
}

// CheckSchemaCompatibility 实现 kafka/schema/compatibility/check（glue 后端
// 为纯语法有效性校验：无副作用，不做注册兼容判定）。
func (s *Service) CheckSchemaCompatibility(ctx context.Context, req SchemaCompatibilityCheckRequest) (*SchemaCompatibilityCheckResult, error) {
	subject := trimSpace(req.Subject)
	if subject == "" {
		return nil, errf("subject is required")
	}
	format, err := normalizeKafkaSchemaFormat(req.Format)
	if err != nil {
		return nil, err
	}
	if trimSpace(req.Schema) == "" {
		return nil, errf("schema is required")
	}
	refs, err := normalizeSchemaReferences(req.References)
	if err != nil {
		return nil, err
	}
	backend, err := s.schemaBackendFor(req.ConnectionID, req.Registry)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, adminTimeout)
	defer cancel()

	compatible, messages, err := backend.checkCompatibility(ctx, subject, req.Version, format, req.Schema, refs)
	if err != nil {
		return nil, err
	}
	if messages == nil {
		messages = []string{}
	}
	return &SchemaCompatibilityCheckResult{IsCompatible: compatible, Messages: messages}, nil
}

// RegisterSchema 实现 kafka/schema/register（写门禁 + 审计；非 allow_delete 级）。
func (s *Service) RegisterSchema(ctx context.Context, req SchemaRegisterRequest) (*SchemaRegisterResult, error) {
	subject := trimSpace(req.Subject)
	if err := ensureWriteAllowed(s.profileOf(req.ConnectionID), "schema/register"); err != nil {
		s.emitAudit(req.ConnectionID, "schema-register", subject, "blocked", err.Error())
		return nil, err
	}
	if subject == "" {
		return nil, errf("subject is required")
	}
	format, err := normalizeKafkaSchemaFormat(req.Format)
	if err != nil {
		return nil, err
	}
	if trimSpace(req.Schema) == "" {
		return nil, errf("schema is required")
	}
	refs, err := normalizeSchemaReferences(req.References)
	if err != nil {
		return nil, err
	}
	backend, err := s.schemaBackendFor(req.ConnectionID, req.Registry)
	if err != nil {
		s.emitAudit(req.ConnectionID, "schema-register", subject, "error", err.Error())
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, adminTimeout)
	defer cancel()

	meta, err := backend.registerSchema(ctx, subject, format, req.Schema, refs, req.Compatibility)
	if err != nil {
		s.emitAudit(req.ConnectionID, "schema-register", subject, "error", err.Error())
		return nil, err
	}
	s.emitAudit(req.ConnectionID, "schema-register", subject, "success", sprintf("id=%d version=%d format=%s", meta.ID, meta.Version, format))
	return &SchemaRegisterResult{ID: meta.ID, Version: meta.Version, VersionID: meta.VersionID}, nil
}

// DeleteSchema 实现 kafka/schema/delete 与 kafka/schema/delete/version
// （critical：allow_delete + read_only 与门 + 审计）。
func (s *Service) DeleteSchema(ctx context.Context, req SchemaDeleteRequest) (*SchemaDeleteResult, error) {
	subject := trimSpace(req.Subject)
	if err := ensureDeleteAllowed(s.profileOf(req.ConnectionID), "schema/delete"); err != nil {
		s.emitAudit(req.ConnectionID, "schema-delete", subjectOrVersion(subject, req.Version), "blocked", err.Error())
		return nil, err
	}
	if subject == "" {
		return nil, errf("subject is required")
	}
	backend, err := s.schemaBackendFor(req.ConnectionID, req.Registry)
	if err != nil {
		s.emitAudit(req.ConnectionID, "schema-delete", subjectOrVersion(subject, req.Version), "error", err.Error())
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, adminTimeout)
	defer cancel()

	deleted, err := backend.deleteSchema(ctx, subject, req.Version)
	if err != nil {
		s.emitAudit(req.ConnectionID, "schema-delete", subjectOrVersion(subject, req.Version), "error", err.Error())
		return nil, err
	}
	if deleted == nil {
		deleted = []int64{}
	}
	s.emitAudit(req.ConnectionID, "schema-delete", subjectOrVersion(subject, req.Version), "success", sprintf("versions=%v", deleted))
	return &SchemaDeleteResult{DeletedVersions: deleted}, nil
}

// --- wire format（magic byte 0 + 4 字节大端 schemaID + 载荷） ---

// encodeWireFrame 打包 Confluent wire format 帧。
func encodeWireFrame(schemaID int, payload []byte) []byte {
	wire := make([]byte, 5+len(payload))
	wire[0] = confluentWireMagicByte
	binary.BigEndian.PutUint32(wire[1:5], uint32(schemaID))
	copy(wire[5:], payload)
	return wire
}

// decodeWireFrame 解包 wire format；非 magic-0 / 长度不足报错。
func decodeWireFrame(value []byte) (schemaID int, payload []byte, err error) {
	if len(value) < 5 {
		return 0, nil, errf("value is too short to use the Confluent wire format")
	}
	if value[0] != confluentWireMagicByte {
		return 0, nil, errf("value does not use the Confluent wire format (magic byte %d != %d)", value[0], confluentWireMagicByte)
	}
	return int(binary.BigEndian.Uint32(value[1:5])), value[5:], nil
}

// --- 载荷编解码（AVRO 二进制 ⇄ JSON 文本；JSON Schema 校验透传） ---

// normalizeKafkaSchemaFormat 归一化 schema 格式（AVRO/JSON/PROTOBUF，
// tinyrdm normalizeKafkaSchemaFormat 同款）。
func normalizeKafkaSchemaFormat(format string) (string, error) {
	switch strings.ToUpper(strings.TrimSpace(format)) {
	case "", "AVRO":
		return "AVRO", nil
	case "JSON", "JSON_SCHEMA", "JSONSCHEMA":
		return "JSON", nil
	case "PROTOBUF", "PROTO":
		return "PROTOBUF", nil
	default:
		return "", errf("schema format must be AVRO, JSON, or PROTOBUF")
	}
}

// normalizeConfluentSchemaType 归一化注册元数据里的 schemaType（空 = AVRO，
// 未知类型原样大写透出）。
func normalizeConfluentSchemaType(schemaType string) string {
	if strings.TrimSpace(schemaType) == "" {
		return "AVRO"
	}
	format, err := normalizeKafkaSchemaFormat(schemaType)
	if err != nil {
		return strings.ToUpper(strings.TrimSpace(schemaType))
	}
	return format
}

// encodeSchemaPayload 把 JSON 文本载荷编码为对应格式的二进制载荷
// （AVRO：goavro；JSON：Schema 校验后原样；PROTOBUF：protojson → 动态消息）。
// subject 仅 PROTOBUF 分支用于 message 消歧，可空（单 message FDSet 不需要）。
func encodeSchemaPayload(text []byte, schema, format, subject string) ([]byte, error) {
	switch strings.ToUpper(strings.TrimSpace(format)) {
	case "", "AVRO":
		codec, err := goavro.NewCodec(schema)
		if err != nil {
			return nil, fmt.Errorf("parse Avro schema: %w", err)
		}
		native, _, err := codec.NativeFromTextual(text)
		if err != nil {
			return nil, fmt.Errorf("encode Avro JSON: %w", err)
		}
		return codec.BinaryFromNative(nil, native)
	case "JSON":
		if err := validateJSONSchemaPayload(text, schema); err != nil {
			return nil, err
		}
		return text, nil
	case "PROTOBUF":
		return encodeProtobufPayload(text, schema, subject)
	default:
		return nil, errf("schema format must be AVRO, JSON, or PROTOBUF")
	}
}

// decodeSchemaPayload 把二进制载荷解码回 JSON 文本（AVRO：goavro；JSON：
// Schema 校验后原样；PROTOBUF：动态消息 → protojson）。
// subject 仅 PROTOBUF 分支用于 message 消歧，可空。
func decodeSchemaPayload(payload []byte, schema, format, subject string) ([]byte, error) {
	switch strings.ToUpper(strings.TrimSpace(format)) {
	case "", "AVRO":
		codec, err := goavro.NewCodec(schema)
		if err != nil {
			return nil, fmt.Errorf("parse Avro schema: %w", err)
		}
		native, _, err := codec.NativeFromBinary(payload)
		if err != nil {
			return nil, fmt.Errorf("decode Avro payload: %w", err)
		}
		return codec.TextualFromNative(nil, native)
	case "JSON":
		if err := validateJSONSchemaPayload(payload, schema); err != nil {
			return nil, err
		}
		return payload, nil
	case "PROTOBUF":
		return decodeProtobufPayload(payload, schema, subject)
	default:
		return nil, errf("schema format must be AVRO, JSON, or PROTOBUF")
	}
}

// --- PROTOBUF 载荷编解码（Phase 3，IMPL_PLAN §12.2.2） ---
//
// SR 元数据事实：PROTOBUF subject 的 schema 字段 = base64(FileDescriptorSet)，
// SchemaMeta.SchemaType="PROTOBUF"。解码走 dynamicpb 动态消息 + protojson
// 渲染；编码走 protojson 反序列化 + proto.Marshal（wire frame 打包由调用方
// encodeWireFrame(meta.ID) 复用）。int64/枚举的 JSON 表示与 Confluent 序列化
// 器存在已知差异（protojson 输出 proto 字段名），登记不强行对齐（§12.7）。

// decodeProtobufPayload 把 protobuf 二进制载荷解码为 JSON 文本。
func decodeProtobufPayload(payload []byte, fdsetB64, subject string) ([]byte, error) {
	desc, err := resolveProtobufMessage(fdsetB64, subject)
	if err != nil {
		return nil, err
	}
	msg := dynamicpb.NewMessage(desc)
	if err := proto.Unmarshal(payload, msg); err != nil {
		return nil, fmt.Errorf("decode protobuf payload: %w", err)
	}
	out, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("render protobuf payload as JSON: %w", err)
	}
	return out, nil
}

// encodeProtobufPayload 把 JSON 文本载荷（protojson 语义）编码为 protobuf
// 二进制。
func encodeProtobufPayload(text []byte, fdsetB64, subject string) ([]byte, error) {
	desc, err := resolveProtobufMessage(fdsetB64, subject)
	if err != nil {
		return nil, err
	}
	msg := dynamicpb.NewMessage(desc)
	if err := (protojson.UnmarshalOptions{}).Unmarshal(text, msg); err != nil {
		return nil, fmt.Errorf("encode protobuf JSON payload: %w", err)
	}
	out, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("encode protobuf payload: %w", err)
	}
	return out, nil
}

// resolveProtobufMessage 从 base64(FileDescriptorSet) + subject 约定选出目标
// message 描述符。
func resolveProtobufMessage(fdsetB64, subject string) (protoreflect.MessageDescriptor, error) {
	files, err := protobufFilesFromSchema(fdsetB64)
	if err != nil {
		return nil, err
	}
	return selectProtobufMessage(collectProtobufMessages(files), subject)
}

// selectProtobufMessage 从收集的 message 描述符按 subject 约定选出目标
// message（消歧按序三分支，§12.2.2）：
//  1. FDSet 恰含 1 个 message → 用之；
//  2. subject 约定匹配：剥 "-key"/"-value" 后缀取尾段，PascalCase 后唯一
//     命中 message 全名尾段 → 用之；
//  3. 仍无法唯一 → 报错并列出候选全名（进 decodeError，不中断消费）。
func selectProtobufMessage(messages []protoreflect.MessageDescriptor, subject string) (protoreflect.MessageDescriptor, error) {
	if len(messages) == 0 {
		return nil, errf("protobuf FileDescriptorSet contains no message")
	}
	if len(messages) == 1 {
		return messages[0], nil
	}
	// 分支 2：subject 约定匹配（-key/-value 后缀 + PascalCase 尾段）。
	if tail := protobufSubjectTail(subject); tail != "" {
		want := pascalCase(tail)
		var matched []protoreflect.MessageDescriptor
		for _, message := range messages {
			full := string(message.FullName())
			if nameTail := messageFullNameTail(full); nameTail == want {
				matched = append(matched, message)
			}
		}
		if len(matched) == 1 {
			return matched[0], nil
		}
	}
	names := make([]string, 0, len(messages))
	for _, message := range messages {
		names = append(names, string(message.FullName()))
	}
	sort.Strings(names)
	return nil, errf("protobuf schema contains multiple messages and subject %q does not identify exactly one (strip -key/-value suffix, PascalCase tail match); candidates: %s",
		subject, strings.Join(names, ", "))
}

// protobufFilesFromSchema base64(FileDescriptorSet) → protoregistry.Files。
func protobufFilesFromSchema(fdsetB64 string) (*protoregistry.Files, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(fdsetB64))
	if err != nil {
		return nil, fmt.Errorf("decode protobuf FileDescriptorSet base64: %w", err)
	}
	var fdset descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(raw, &fdset); err != nil {
		return nil, fmt.Errorf("parse protobuf FileDescriptorSet: %w", err)
	}
	files, err := protodesc.NewFiles(&fdset)
	if err != nil {
		return nil, fmt.Errorf("build protobuf descriptors: %w", err)
	}
	return files, nil
}

// collectProtobufMessages 遍历 FDSet 全部文件收集 message 描述符（含嵌套）。
func collectProtobufMessages(files *protoregistry.Files) []protoreflect.MessageDescriptor {
	var appendNested func(md protoreflect.MessageDescriptor)
	var messages []protoreflect.MessageDescriptor
	appendNested = func(md protoreflect.MessageDescriptor) {
		for i := 0; i < md.Messages().Len(); i++ {
			nested := md.Messages().Get(i)
			messages = append(messages, nested)
			appendNested(nested)
		}
	}
	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		for i := 0; i < fd.Messages().Len(); i++ {
			md := fd.Messages().Get(i)
			messages = append(messages, md)
			appendNested(md)
		}
		return true
	})
	return messages
}

// protobufSubjectTail 剥 subject 的 "-key"/"-value" 后缀并取尾段
// （"orders-value" → "orders"、"com.dbx.test.orders-key" → "orders"）。
func protobufSubjectTail(subject string) string {
	base := strings.TrimSpace(subject)
	for _, suffix := range []string{"-key", "-value"} {
		if strings.HasSuffix(base, suffix) {
			base = strings.TrimSuffix(base, suffix)
			break
		}
	}
	if i := strings.LastIndexAny(base, "./"); i >= 0 {
		base = base[i+1:]
	}
	return strings.TrimSpace(base)
}

// messageFullNameTail 取 message 全名尾段（"com.dbx.test.Order" → "Order"）。
func messageFullNameTail(full string) string {
	if i := strings.LastIndex(full, "."); i >= 0 {
		return full[i+1:]
	}
	return full
}

// pascalCase 把 snake/kebab/空格分隔词转 PascalCase（"orders" → "Orders"、
// "order_items" → "OrderItems"），用于 subject 约定匹配 message 名。
func pascalCase(value string) string {
	words := strings.FieldsFunc(value, func(r rune) bool {
		return r == '_' || r == '-' || r == ' ' || r == '.'
	})
	var b strings.Builder
	for _, word := range words {
		runes := []rune(word)
		if len(runes) == 0 {
			continue
		}
		b.WriteRune(unicode.ToUpper(runes[0]))
		b.WriteString(string(runes[1:]))
	}
	return b.String()
}

// validateJSONSchemaPayload 校验 JSON 载荷是否符合 JSON Schema
// （tinyrdm kafkaValidateJSONSchemaPayload 同款）。
func validateJSONSchemaPayload(text []byte, schema string) error {
	if !json.Valid(text) {
		return errf("JSON schema payload must be valid JSON")
	}
	var schemaDoc jsonschema.Schema
	if err := json.Unmarshal([]byte(schema), &schemaDoc); err != nil {
		return fmt.Errorf("parse JSON schema: %w", err)
	}
	resolved, err := schemaDoc.Resolve(nil)
	if err != nil {
		return fmt.Errorf("resolve JSON schema: %w", err)
	}
	var value any
	if err := json.Unmarshal(text, &value); err != nil {
		return fmt.Errorf("decode JSON schema payload: %w", err)
	}
	if err := resolved.Validate(value); err != nil {
		return fmt.Errorf("validate JSON schema payload: %w", err)
	}
	return nil
}

// --- per-consume schema 元数据缓存（tinyrdm metadata cache 思路） ---

// schemaMetaCache 一次消费/一个流式会话内的元数据缓存（并发安全）。
type schemaMetaCache struct {
	mu               sync.Mutex
	byID             map[int]SchemaMeta
	bySubjectVersion map[string]SchemaMeta
}

func newSchemaMetaCache() *schemaMetaCache {
	return &schemaMetaCache{
		byID:             map[int]SchemaMeta{},
		bySubjectVersion: map[string]SchemaMeta{},
	}
}

func subjectVersionKey(subject string, version int64) string {
	if version > 0 {
		return subject + "\x00" + strconv.FormatInt(version, 10)
	}
	return subject + "\x00latest"
}

func (c *schemaMetaCache) lookupByID(id int) (SchemaMeta, bool) {
	if c == nil || id <= 0 {
		return SchemaMeta{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	meta, ok := c.byID[id]
	return meta, ok
}

func (c *schemaMetaCache) lookupBySubjectVersion(subject string, version int64) (SchemaMeta, bool) {
	if c == nil || strings.TrimSpace(subject) == "" {
		return SchemaMeta{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	meta, ok := c.bySubjectVersion[subjectVersionKey(subject, version)]
	return meta, ok
}

func (c *schemaMetaCache) store(meta SchemaMeta) {
	if c == nil || meta.ID <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byID[meta.ID] = meta
	if strings.TrimSpace(meta.Subject) != "" && meta.Version > 0 {
		c.bySubjectVersion[subjectVersionKey(meta.Subject, meta.Version)] = meta
	}
}

func (c *schemaMetaCache) storeSubjectLookup(subject string, version int64, meta SchemaMeta) {
	if c == nil || strings.TrimSpace(subject) == "" || meta.ID <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bySubjectVersion[subjectVersionKey(subject, version)] = meta
	c.byID[meta.ID] = meta
}

// schemaDecoder 是 per-consume 的 wire format 解码器（SR 客户端 + 缓存 +
// 请求参数）。schema 参数为 nil 时 decode 直接透传。
type schemaDecoder struct {
	client *schemaRegistryClient
	cache  *schemaMetaCache
	req    *SchemaRef
}

// schemaValueInfo 是解码后附带进消息的 schema 定位信息。
type schemaValueInfo struct {
	ID      int
	Subject string
	Version int64
}

func newSchemaDecoder(client *schemaRegistryClient, req *SchemaRef) *schemaDecoder {
	return &schemaDecoder{client: client, cache: newSchemaMetaCache(), req: req}
}

// decode 解包 wire format 并解码载荷为 JSON 文本。
// 返回 decoded 载荷 + schema 定位信息 + 错误（错误只进消息 decodeError，
// 不中断消费）。
func (d *schemaDecoder) decode(ctx context.Context, value []byte) ([]byte, schemaValueInfo, error) {
	schemaID, payload, err := decodeWireFrame(value)
	if err != nil {
		return nil, schemaValueInfo{}, err
	}
	req := d.req
	var meta SchemaMeta
	if req != nil && strings.TrimSpace(req.Subject) != "" {
		// 指定 subject：按 subject(+version) 取元数据（wire id 与元数据
		// 不一致时以 wire id 兜底填充）。
		if cached, ok := d.cache.lookupBySubjectVersion(trimSpace(req.Subject), req.Version); ok {
			meta = cached
		} else {
			meta, err = d.client.getSchema(ctx, trimSpace(req.Subject), req.Version)
			if err != nil {
				return nil, schemaValueInfo{}, err
			}
			if meta.ID == 0 {
				meta.ID = schemaID
			}
			d.cache.storeSubjectLookup(trimSpace(req.Subject), req.Version, meta)
		}
	} else {
		if cached, ok := d.cache.lookupByID(schemaID); ok {
			meta = cached
		} else {
			meta, err = d.client.getSchemaByID(ctx, schemaID)
			if err != nil {
				return nil, schemaValueInfo{}, err
			}
			d.cache.store(meta)
		}
	}
	format := ""
	if req != nil && trimSpace(req.Format) != "" {
		format = normalizeConfluentSchemaType(req.Format)
	} else {
		format = normalizeConfluentSchemaType(meta.SchemaType)
	}
	// PROTOBUF：Confluent wire format 在 schemaID 与载荷之间还有 message
	// index 数组段，按深度剥离后再解码（AVRO/JSON 原样传）。
	wirePayload := payload
	stripped := false
	if format == "PROTOBUF" {
		if indexes, indexErr := protobufMessageIndexesFromSchema(meta.Schema, meta.Subject); indexErr == nil {
			if rest, ok := stripProtobufMessageIndexes(payload, len(indexes)); ok {
				wirePayload = rest
				stripped = true
			}
		}
	}
	decoded, err := decodeSchemaPayload(wirePayload, meta.Schema, format, meta.Subject)
	if err != nil && stripped {
		// 历史兼容：早期版本 produce 侧漏写 message index 段，剥段后解码
		// 失败时按原始载荷重试一次（仍失败以剥段路径的错误为准）。
		if fallback, fallbackErr := decodeSchemaPayload(payload, meta.Schema, format, meta.Subject); fallbackErr == nil {
			decoded, err = fallback, nil
		}
	}
	if err != nil {
		return nil, schemaValueInfo{}, err
	}
	info := schemaValueInfo{ID: meta.ID, Subject: meta.Subject, Version: meta.Version}
	return decoded, info, nil
}

// encodeForProduce 把未编码载荷按 schema 元数据编码并打包 wire format。
// PROTOBUF：除 schemaID 帧外还补 Confluent message index 数组段（AVRO/JSON
// 无此段）。
func encodeForProduce(ctx context.Context, client *schemaRegistryClient, ref *SchemaRef, payload []byte) ([]byte, SchemaGetResult, error) {
	subject := trimSpace(ref.Subject)
	if subject == "" {
		return nil, SchemaGetResult{}, errf("schema.subject is required")
	}
	meta, err := client.getSchema(ctx, subject, ref.Version)
	if err != nil {
		return nil, SchemaGetResult{}, err
	}
	format := normalizeConfluentSchemaType(firstNonEmpty(ref.Format, meta.SchemaType))
	encoded, err := encodeSchemaPayload(payload, meta.Schema, format, subject)
	if err != nil {
		return nil, SchemaGetResult{}, err
	}
	if format == "PROTOBUF" {
		indexes, indexErr := protobufMessageIndexesFromSchema(meta.Schema, subject)
		if indexErr != nil {
			return nil, SchemaGetResult{}, indexErr
		}
		encoded = append(encodeProtobufMessageIndexes(indexes), encoded...)
	}
	return encodeWireFrame(meta.ID, encoded), SchemaGetResult{
		Subject: meta.Subject,
		Version: meta.Version,
		ID:      meta.ID,
		Format:  format,
	}, nil
}

// --- PROTOBUF wire framing（Confluent message index 段）---
//
// Confluent PROTOBUF 的 wire format 在 4 字节 schemaID 之后、protobuf 载荷
// 之前还有一段 message index 数组：目标 message 的声明序号路径（顶层序号 →
// 逐级嵌套序号）各以一个 varint 紧密拼接，无长度前缀（单顶层 message =
// 单字节 0x00）。AVRO/JSON 无此段。produce 侧按选中 message 补齐该段，
// consume 侧按深度剥离；剥离失败/推导失败的载荷交给解码（历史版本 produce
// 侧漏写该段，解码带一次按原始载荷的回退重试）。

// protobufMessageIndexesFromSchema 解析 FDSet 并返回选中 message 的
// message index 路径（顶层序号 → 逐级嵌套序号）。
func protobufMessageIndexesFromSchema(fdsetB64, subject string) ([]int, error) {
	files, err := protobufFilesFromSchema(fdsetB64)
	if err != nil {
		return nil, err
	}
	desc, err := selectProtobufMessage(collectProtobufMessages(files), subject)
	if err != nil {
		return nil, err
	}
	return protobufMessageIndexPath(desc), nil
}

// protobufMessageIndexPath 计算目标 message 的声明序号路径
// （顶层 message 在文件中的序号起，逐级嵌套到目标为止）。
func protobufMessageIndexPath(desc protoreflect.MessageDescriptor) []int {
	var reversed []int
	current := desc
	for {
		reversed = append(reversed, protobufSiblingIndex(current))
		parent, ok := current.Parent().(protoreflect.MessageDescriptor)
		if !ok {
			// 非 message parent = 文件级（顶层），路径收集完毕。
			path := make([]int, len(reversed))
			for i, index := range reversed {
				path[len(reversed)-1-i] = index
			}
			return path
		}
		current = parent
	}
}

// protobufSiblingIndex 取 message 在其 parent（文件或外层 message）内的
// 声明序号（按 FullName 比对；未命中返回 0，defensive）。
func protobufSiblingIndex(desc protoreflect.MessageDescriptor) int {
	switch parent := desc.Parent().(type) {
	case protoreflect.MessageDescriptor:
		for i := 0; i < parent.Messages().Len(); i++ {
			if parent.Messages().Get(i).FullName() == desc.FullName() {
				return i
			}
		}
	case protoreflect.FileDescriptor:
		for i := 0; i < parent.Messages().Len(); i++ {
			if parent.Messages().Get(i).FullName() == desc.FullName() {
				return i
			}
		}
	}
	return 0
}

// encodeProtobufMessageIndexes 把 index 路径编码为 Confluent message index
// 数组（每个元素一个 varint，紧密拼接，无长度前缀）。
func encodeProtobufMessageIndexes(indexes []int) []byte {
	var out []byte
	for _, index := range indexes {
		out = binary.AppendUvarint(out, uint64(index))
	}
	return out
}

// stripProtobufMessageIndexes 从 wire 载荷头部剥掉 depth 个 varint index
// （Confluent message index 数组）；varint 不完整/溢出返回 false。
func stripProtobufMessageIndexes(payload []byte, depth int) ([]byte, bool) {
	if depth <= 0 {
		return payload, true
	}
	rest := payload
	for i := 0; i < depth; i++ {
		_, n := binary.Uvarint(rest)
		if n <= 0 {
			return nil, false
		}
		rest = rest[n:]
	}
	return rest, true
}

// --- LCS 逐行 diff（tinyrdm kafkaSchemaTextDiff 重写为语义 hunks） ---

// schemaDiffOp 是 LCS diff 的单行操作（包级类型，joinDiffLines 复用）。
type schemaDiffOp struct {
	kind string // equal | remove | add
	line int    // 1-based（remove 用 before 行号，add 用 after 行号）
	text string
}

// schemaTextDiff 逐行 LCS diff，输出语义 hunks（相邻 remove/add 归并为
// modify 块）与统计 summary。
func schemaTextDiff(before, after string) ([]SchemaDiffHunk, SchemaDiffSummary) {
	beforeLines := splitSchemaLines(before)
	afterLines := splitSchemaLines(after)

	summary := SchemaDiffSummary{
		BeforeLines: len(beforeLines),
		AfterLines:  len(afterLines),
	}

	// DP 求 LCS 长度表（逐行，行数有限、schema 文本量级小）。
	dp := make([][]int, len(beforeLines)+1)
	for i := range dp {
		dp[i] = make([]int, len(afterLines)+1)
	}
	for i := len(beforeLines) - 1; i >= 0; i-- {
		for j := len(afterLines) - 1; j >= 0; j-- {
			if beforeLines[i] == afterLines[j] {
				dp[i][j] = dp[i+1][j+1] + 1
			} else if dp[i+1][j] >= dp[i][j+1] {
				dp[i][j] = dp[i+1][j]
			} else {
				dp[i][j] = dp[i][j+1]
			}
		}
	}

	type diffOp = schemaDiffOp
	ops := make([]diffOp, 0, len(beforeLines)+len(afterLines))
	i, j := 0, 0
	for i < len(beforeLines) && j < len(afterLines) {
		switch {
		case beforeLines[i] == afterLines[j]:
			ops = append(ops, diffOp{kind: "equal", line: i + 1, text: beforeLines[i]})
			i++
			j++
		case dp[i+1][j] >= dp[i][j+1]:
			ops = append(ops, diffOp{kind: "remove", line: i + 1, text: beforeLines[i]})
			i++
		default:
			ops = append(ops, diffOp{kind: "add", line: j + 1, text: afterLines[j]})
			j++
		}
	}
	for i < len(beforeLines) {
		ops = append(ops, diffOp{kind: "remove", line: i + 1, text: beforeLines[i]})
		i++
	}
	for j < len(afterLines) {
		ops = append(ops, diffOp{kind: "add", line: j + 1, text: afterLines[j]})
		j++
	}

	// 相邻 remove/add 归并：成对出现 → modify；单独出现 → remove/add。
	hunks := []SchemaDiffHunk{}
	for k := 0; k < len(ops); {
		if ops[k].kind == "equal" {
			summary.Unchanged++
			k++
			continue
		}
		var removed, added []diffOp
		for k < len(ops) && ops[k].kind == "remove" {
			removed = append(removed, ops[k])
			k++
		}
		for k < len(ops) && ops[k].kind == "add" {
			added = append(added, ops[k])
			k++
		}
		switch {
		case len(removed) > 0 && len(added) > 0:
			summary.Removed += len(removed)
			summary.Added += len(added)
			hunks = append(hunks, SchemaDiffHunk{
				Op:     "modify",
				Path:   schemaDiffPath(removed[0].line),
				Before: joinDiffLines(removed),
				After:  joinDiffLines(added),
			})
		case len(removed) > 0:
			summary.Removed += len(removed)
			hunks = append(hunks, SchemaDiffHunk{
				Op:     "remove",
				Path:   schemaDiffPath(removed[0].line),
				Before: joinDiffLines(removed),
			})
		default:
			summary.Added += len(added)
			hunks = append(hunks, SchemaDiffHunk{
				Op:    "add",
				Path:  schemaDiffPath(added[0].line),
				After: joinDiffLines(added),
			})
		}
	}
	return hunks, summary
}

func joinDiffLines(ops []schemaDiffOp) string {
	texts := make([]string, 0, len(ops))
	for _, op := range ops {
		texts = append(texts, op.text)
	}
	return strings.Join(texts, "\n")
}

func schemaDiffPath(line int) string {
	return "line " + strconv.Itoa(line)
}

func splitSchemaLines(text string) []string {
	if text == "" {
		return nil
	}
	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")
	return strings.Split(text, "\n")
}

// --- 兼容性取值面与辅助 ---

// normalizeConfluentCompatibility 归一化兼容级别（NONE/BACKWARD(_TRANSITIVE)/
// FORWARD(_TRANSITIVE)/FULL(_TRANSITIVE)；tinyrdm 同款，*_ALL 归并为
// *_TRANSITIVE）。
func normalizeConfluentCompatibility(value string) (string, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "NONE", "DISABLED", "":
		return "NONE", nil
	case "BACKWARD":
		return "BACKWARD", nil
	case "BACKWARD_ALL", "BACKWARD_TRANSITIVE":
		return "BACKWARD_TRANSITIVE", nil
	case "FORWARD":
		return "FORWARD", nil
	case "FORWARD_ALL", "FORWARD_TRANSITIVE":
		return "FORWARD_TRANSITIVE", nil
	case "FULL":
		return "FULL", nil
	case "FULL_ALL", "FULL_TRANSITIVE":
		return "FULL_TRANSITIVE", nil
	default:
		return "", errf("level must be one of BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE, FULL, FULL_TRANSITIVE, or NONE")
	}
}

func compatibilityScope(subject string) string {
	if trimSpace(subject) == "" {
		return "global"
	}
	return "subject"
}

func subjectOrGlobal(subject string) string {
	if trimSpace(subject) == "" {
		return "<global>"
	}
	return trimSpace(subject)
}

func subjectOrVersion(subject string, version int64) string {
	if version > 0 {
		return sprintf("%s@%d", trimSpace(subject), version)
	}
	return subjectOrGlobal(subject)
}

// normalizeSchemaReferences 校验 references（name/subject/version 必填）。
func normalizeSchemaReferences(refs []SchemaReference) ([]SchemaReference, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	out := make([]SchemaReference, 0, len(refs))
	for _, ref := range refs {
		if strings.TrimSpace(ref.Name) == "" || strings.TrimSpace(ref.Subject) == "" || ref.Version <= 0 {
			return nil, errf("schema reference name, subject, and version are required")
		}
		out = append(out, SchemaReference{
			Name:    strings.TrimSpace(ref.Name),
			Subject: strings.TrimSpace(ref.Subject),
			Version: ref.Version,
		})
	}
	return out, nil
}

// schemaRegisterRequest SR REST 注册/兼容性检查共用请求体。
type schemaRegisterRequest struct {
	Schema     string            `json:"schema"`
	SchemaType string            `json:"schemaType,omitempty"`
	References []SchemaReference `json:"references,omitempty"`
}
