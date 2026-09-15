package kafkaconn

// glue.go：AWS Glue Schema Registry 管理面（Phase 3，补齐 tinyrdm 字面功能
// 缺口）。逻辑参照 tiny-rdm backend/services/kafka_schema_registry.go 的
// glue 分支重写：
//   - aws-sdk-go-v2/service/glue 客户端；auth_mode：default = LoadDefaultConfig
//     默认凭据链，static = NewStaticCredentialsProvider（tinyrdm 的 aws-profile
//     模式 sidecar 场景不提供，协议文档登记后续）；
//   - API 映射照 tinyrdm：ListSchemas / ListSchemaVersions / GetSchemaVersion /
//     GetSchema / RegisterSchemaVersion + CreateSchema / UpdateSchema(Compatibility) /
//     CheckSchemaVersionValidity / DeleteSchemaVersions / DeleteSchema；
//   - Glue 无数字 schemaID、无全局兼容级别：ID 置 0（VersionID 为 Glue GUID），
//     兼容性为 per-schema，subject 必填；
//   - compatibility 枚举映射：Confluent *_TRANSITIVE 输入归并为 Glue *_ALL，
//     另支持 Glue 特有 DISABLED。
//
// 凭据红线：glue_secret_access_key / glue_session_token 走 secret binding，
// 仅进 SigV4 签名，不落日志/审计/事件（§6）。

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awscredentials "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/smithy-go"
)

// --- provider 取值面与解析（冻结契约 2：registry 参数 + 自动探测） ---

// provider 取值（kafka/schema/test 返回与 statuses 的 schemaRegistry.provider）。
const (
	schemaProviderConfluent = "confluent"
	schemaProviderGlue      = "glue"
	schemaProviderNone      = "none"
)

// schemaProviderBoth statuses 专用：双配置（方法层会要求显式 registry）。
const schemaProviderBoth = "both"

// InvalidParamsError 标记参数级错误（协议参数错 -32602；main.go 的 bizError
// 据此映射，与既有业务错 -32000 区分）。
type InvalidParamsError struct{ Msg string }

func (e *InvalidParamsError) Error() string { return e.Msg }

// glueEnabled / srEnabled 判定与 schema_registry 开关语义统一收口在
// types.go（Profile 方法）。

// schemaProviderConfigured 返回连接配置的 provider 概览（statuses 摘要用）：
// confluent | glue | both | none（both 仅出现在旧连接：无 schema_registry
// 开关且 sr_url 与 glue_* 双配置）。
func (p Profile) schemaProviderConfigured() string {
	confluent, glue := p.srEnabled(), p.glueEnabled()
	switch {
	case confluent && glue:
		return schemaProviderBoth
	case confluent:
		return schemaProviderConfluent
	case glue:
		return schemaProviderGlue
	default:
		return schemaProviderNone
	}
}

// resolveSchemaProvider 解析 schema 方法/挂载的 registry 参数（以
// schema_registry 开关为准）：
//   - 缺省 registry：开关 confluent → confluent、aws_glue → glue、none →
//     none（SR 禁用）；开关未设（旧连接，向后兼容）按 sr_url/glue_region
//     自动探测回退，双配置须显式 registry。
//   - 显式 registry 与开关冲突（none/另一后端）→ 参数错；旧连接（开关空）
//     保持原直取语义。
func resolveSchemaProvider(p Profile, registry string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(registry)) {
	case "":
	case schemaProviderConfluent:
		switch p.SchemaRegistry {
		case SchemaRegistryNone:
			return "", &InvalidParamsError{Msg: "schema registry is disabled for this connection (schemaRegistry=\"none\")"}
		case SchemaRegistryAWSGlue:
			return "", &InvalidParamsError{Msg: "confluent registry is not enabled for this connection (schemaRegistry=\"aws_glue\")"}
		}
		return schemaProviderConfluent, nil
	case schemaProviderGlue:
		switch p.SchemaRegistry {
		case SchemaRegistryNone:
			return "", &InvalidParamsError{Msg: "schema registry is disabled for this connection (schemaRegistry=\"none\")"}
		case SchemaRegistryConfluent:
			return "", &InvalidParamsError{Msg: "AWS Glue registry is not enabled for this connection (schemaRegistry=\"confluent\")"}
		}
		return schemaProviderGlue, nil
	default:
		return "", &InvalidParamsError{Msg: `registry must be "confluent" or "glue"`}
	}
	switch p.SchemaRegistry {
	case SchemaRegistryConfluent:
		return schemaProviderConfluent, nil
	case SchemaRegistryAWSGlue:
		return schemaProviderGlue, nil
	case SchemaRegistryNone:
		return schemaProviderNone, nil
	}
	// 旧连接（schema_registry 未设）：按 sr_url/glue 配置自动探测回退。
	switch p.schemaProviderConfigured() {
	case schemaProviderBoth:
		return "", &InvalidParamsError{Msg: "connection has both srUrl and AWS Glue configured; pass registry \"confluent\" or \"glue\" explicitly"}
	case schemaProviderConfluent:
		return schemaProviderConfluent, nil
	case schemaProviderGlue:
		return schemaProviderGlue, nil
	default:
		return schemaProviderNone, nil
	}
}

// --- Glue 客户端（aws-sdk-go-v2） ---

// glueRegistryConfig 是一次 Glue 后端构造需要的配置切片（来自 Profile +
// connSecrets，凭据只经此处进 SigV4）。
type glueRegistryConfig struct {
	Region          string
	RegistryName    string
	AuthMode        string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

// glueBackend 是 schemaBackend 的 AWS Glue 实现。
type glueBackend struct {
	client       *glue.Client
	registryName string
}

// glueListPageCap 是 ListSchemas/ListSchemaVersions 分页上限（防御性，
// 超大 registry 截断；tinyrdm list 侧默认 200，这里放宽到 1000）。
const glueListPageCap = 1000

// glueBaseEndpointOverride 是单测注入 httptest 端点的 seam（生产恒空，
// Service 构造 Glue 后端时走 AWS 默认端点解析）。
var glueBaseEndpointOverride string

// newGlueSchemaBackend 构建 Glue 后端；baseEndpoint 非空时覆盖服务端点
// （仅单测 httptest 注入；生产恒为空，走 AWS 默认端点解析）。
func newGlueSchemaBackend(cfg glueRegistryConfig, baseEndpoint string) (*glueBackend, error) {
	client, err := newGlueClient(context.Background(), cfg, baseEndpoint)
	if err != nil {
		return nil, err
	}
	return &glueBackend{client: client, registryName: strings.TrimSpace(cfg.RegistryName)}, nil
}

// newGlueClient 构建 aws-sdk-go-v2 glue.Client（tinyrdm kafkaGlueClient 同款
// 语义，profile 模式按契约不提供）。
func newGlueClient(ctx context.Context, cfg glueRegistryConfig, baseEndpoint string) (*glue.Client, error) {
	region := strings.TrimSpace(cfg.Region)
	if region == "" {
		return nil, errf("AWS Glue region is required")
	}
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
	switch strings.ToLower(strings.TrimSpace(cfg.AuthMode)) {
	case "", "default":
		// 默认凭据链（env / shared config / IMDS 等，LoadDefaultConfig）。
	case "static":
		if strings.TrimSpace(cfg.AccessKeyID) == "" || strings.TrimSpace(cfg.SecretAccessKey) == "" {
			return nil, errf("AWS access key id and secret access key are required for glue_auth_mode=static")
		}
		opts = append(opts, awsconfig.WithCredentialsProvider(awscredentials.NewStaticCredentialsProvider(
			strings.TrimSpace(cfg.AccessKeyID), strings.TrimSpace(cfg.SecretAccessKey), strings.TrimSpace(cfg.SessionToken))))
	case "profile":
		return nil, errf("glue_auth_mode=profile is not supported in the sidecar; use default or static")
	default:
		return nil, errf("glue_auth_mode must be default or static")
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	glueOpts := []func(*glue.Options){}
	if endpoint := strings.TrimSpace(baseEndpoint); endpoint != "" {
		glueOpts = append(glueOpts, func(o *glue.Options) { o.BaseEndpoint = &endpoint })
	}
	return glue.NewFromConfig(awsCfg, glueOpts...), nil
}

// requireRegistry 校验 registryName（防御：构造路径之外仍可能为空）。
func (b *glueBackend) requireRegistry() error {
	if b == nil || b.registryName == "" {
		return errf("AWS Glue registryName is required")
	}
	return nil
}

// schemaID 组装 Glue SchemaId（registryName + schemaName，tinyrdm 同款）。
func (b *glueBackend) schemaID(schemaName string) *gluetypes.SchemaId {
	registry, name := b.registryName, schemaName
	return &gluetypes.SchemaId{RegistryName: &registry, SchemaName: &name}
}

// glueCompatibilityFromGlue 读回 GetSchema 输出的兼容级别（空 = NONE）。
func glueCompatibilityFromGlue(out *glue.GetSchemaOutput) string {
	if out == nil || out.Compatibility == "" {
		return string(gluetypes.CompatibilityNone)
	}
	return string(out.Compatibility)
}

// normalizeGlueCompatibility 归一化兼容级别到 Glue 枚举（tinyrdm 同款：
// Confluent 的 *_TRANSITIVE 输入归并为 *_ALL；NONE 为缺省；Glue 另有
// DISABLED）。
func normalizeGlueCompatibility(value string) (gluetypes.Compatibility, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "", "NONE":
		return gluetypes.CompatibilityNone, nil
	case "DISABLED":
		return gluetypes.CompatibilityDisabled, nil
	case "BACKWARD":
		return gluetypes.CompatibilityBackward, nil
	case "BACKWARD_ALL", "BACKWARD_TRANSITIVE":
		return gluetypes.CompatibilityBackwardAll, nil
	case "FORWARD":
		return gluetypes.CompatibilityForward, nil
	case "FORWARD_ALL", "FORWARD_TRANSITIVE":
		return gluetypes.CompatibilityForwardAll, nil
	case "FULL":
		return gluetypes.CompatibilityFull, nil
	case "FULL_ALL", "FULL_TRANSITIVE":
		return gluetypes.CompatibilityFullAll, nil
	default:
		return "", errf("level must be one of BACKWARD, BACKWARD_ALL, FORWARD, FORWARD_ALL, FULL, FULL_ALL, NONE, or DISABLED")
	}
}

// glueDataFormat 把归一化后的格式（AVRO/JSON/PROTOBUF）映射为 Glue DataFormat
// （tinyrdm 同款；其余一律按 AVRO）。
func glueDataFormat(format string) gluetypes.DataFormat {
	switch strings.ToUpper(strings.TrimSpace(format)) {
	case "JSON":
		return gluetypes.DataFormatJson
	case "PROTOBUF":
		return gluetypes.DataFormatProtobuf
	default:
		return gluetypes.DataFormatAvro
	}
}

// isAWSNotFound 判定 Glue "实体不存在" 类错误（EntityNotFound，
// register 的 GetSchema 探测用；tinyrdm 同款语义）。
func isAWSNotFound(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	code := strings.ToLower(apiErr.ErrorCode())
	return strings.Contains(code, "notfound") || strings.Contains(code, "entitynotfound")
}

// --- Glue 指针/取值小工具（SDK 全指针入参） ---

func awsStringPtr(value string) *string { return &value }
func awsInt64Ptr(value int64) *int64    { return &value }
func awsInt32Ptr(value int32) *int32    { return &value }

func awsStringValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func awsInt64Value(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

// --- schemaBackend 的 Glue 实现（接口定义见 schema.go） ---

func (b *glueBackend) provider() string { return schemaProviderGlue }

// test 以一次分页 ListSchemas 探活并计数（Glue 无 registry 级版本号，version 恒空）。
func (b *glueBackend) test(ctx context.Context) (subjectCount int, version string, err error) {
	subjects, err := b.listSubjects(ctx)
	if err != nil {
		return 0, "", err
	}
	return len(subjects), "", nil
}

// listSubjects = glue ListSchemas（分页聚合；Formats/LatestVersion/
// CompatibilityLevel 在列表级不可得——Glue 逐条需额外 API，留空由
// versions/list、get 补齐；Description 为 ListSchemas 原生字段）。
func (b *glueBackend) listSubjects(ctx context.Context) ([]SubjectInfo, error) {
	if err := b.requireRegistry(); err != nil {
		return nil, err
	}
	out := make([]SubjectInfo, 0, 8)
	var nextToken *string
	for {
		page, err := b.client.ListSchemas(ctx, &glue.ListSchemasInput{
			RegistryId: &gluetypes.RegistryId{RegistryName: &b.registryName},
			MaxResults: awsInt32Ptr(100),
			NextToken:  nextToken,
		})
		if err != nil {
			return nil, err
		}
		for _, item := range page.Schemas {
			name := awsStringValue(item.SchemaName)
			// 与 Confluent 列表同款：跳过探测条目与空名。
			if name == "" || strings.HasSuffix(name, "/") {
				continue
			}
			out = append(out, SubjectInfo{
				Subject:     name,
				Formats:     []string{},
				Description: awsStringValue(item.Description),
			})
		}
		if page.NextToken == nil || strings.TrimSpace(*page.NextToken) == "" || len(out) >= glueListPageCap {
			break
		}
		nextToken = page.NextToken
	}
	sort.Slice(out, func(i, j int) bool { return strings.ToLower(out[i].Subject) < strings.ToLower(out[j].Subject) })
	return out, nil
}

// listVersions = glue ListSchemaVersions（分页聚合）+ 一次 GetSchema 取
// DataFormat（同一 schema 全版本同格式）；versionId 为 Glue GUID。
func (b *glueBackend) listVersions(ctx context.Context, subject string) ([]SchemaVersionInfo, error) {
	if err := b.requireRegistry(); err != nil {
		return nil, err
	}
	subject = strings.TrimSpace(subject)
	if subject == "" {
		return nil, errf("subject is required")
	}
	format := "UNKNOWN"
	if out, err := b.client.GetSchema(ctx, &glue.GetSchemaInput{SchemaId: b.schemaID(subject)}); err == nil {
		format = normalizeConfluentSchemaType(string(out.DataFormat))
	}
	var out []SchemaVersionInfo
	var nextToken *string
	for {
		page, err := b.client.ListSchemaVersions(ctx, &glue.ListSchemaVersionsInput{
			SchemaId:  b.schemaID(subject),
			NextToken: nextToken,
		})
		if err != nil {
			return nil, err
		}
		for _, item := range page.Schemas {
			version := awsInt64Value(item.VersionNumber)
			if version <= 0 {
				continue
			}
			out = append(out, SchemaVersionInfo{
				Version:   version,
				ID:        0,
				Format:    format,
				VersionID: awsStringValue(item.SchemaVersionId),
			})
		}
		if page.NextToken == nil || strings.TrimSpace(*page.NextToken) == "" || len(out) >= glueListPageCap {
			break
		}
		nextToken = page.NextToken
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

// getSchema：version<=0 先 GetSchema 取 LatestSchemaVersion，再
// GetSchemaVersion 取定义。ID 恒 0（Glue 无数字 id），VersionID 为 GUID。
func (b *glueBackend) getSchema(ctx context.Context, subject string, version int64) (SchemaMeta, error) {
	if err := b.requireRegistry(); err != nil {
		return SchemaMeta{}, err
	}
	subject = strings.TrimSpace(subject)
	if subject == "" {
		return SchemaMeta{}, errf("subject is required")
	}
	if version <= 0 {
		out, err := b.client.GetSchema(ctx, &glue.GetSchemaInput{SchemaId: b.schemaID(subject)})
		if err != nil {
			return SchemaMeta{}, err
		}
		version = awsInt64Value(out.LatestSchemaVersion)
	}
	vout, err := b.client.GetSchemaVersion(ctx, &glue.GetSchemaVersionInput{
		SchemaId:            b.schemaID(subject),
		SchemaVersionNumber: &gluetypes.SchemaVersionNumber{VersionNumber: awsInt64Ptr(version)},
	})
	if err != nil {
		return SchemaMeta{}, err
	}
	return SchemaMeta{
		Subject:    subject,
		Version:    awsInt64Value(vout.VersionNumber),
		VersionID:  awsStringValue(vout.SchemaVersionId),
		Schema:     awsStringValue(vout.SchemaDefinition),
		SchemaType: normalizeConfluentSchemaType(string(vout.DataFormat)),
	}, nil
}

// getCompatibility = glue GetSchema（兼容级别为 per-schema，subject 必填）。
func (b *glueBackend) getCompatibility(ctx context.Context, subject string) (string, error) {
	if err := b.requireRegistry(); err != nil {
		return "", err
	}
	subject = strings.TrimSpace(subject)
	if subject == "" {
		return "", errf("subject is required for AWS Glue compatibility (per-schema; no global level)")
	}
	out, err := b.client.GetSchema(ctx, &glue.GetSchemaInput{SchemaId: b.schemaID(subject)})
	if err != nil {
		return "", err
	}
	return glueCompatibilityFromGlue(out), nil
}

// setCompatibility = glue UpdateSchema（需版本检查点：缺省取
// LatestSchemaVersion），随后 GetSchema 回读生效值。
func (b *glueBackend) setCompatibility(ctx context.Context, subject, level string, version int64) (string, error) {
	if err := b.requireRegistry(); err != nil {
		return "", err
	}
	subject = strings.TrimSpace(subject)
	if subject == "" {
		return "", errf("subject is required for AWS Glue compatibility (per-schema; no global level)")
	}
	compatibility, err := normalizeGlueCompatibility(level)
	if err != nil {
		return "", err
	}
	if version <= 0 {
		schema, err := b.client.GetSchema(ctx, &glue.GetSchemaInput{SchemaId: b.schemaID(subject)})
		if err != nil {
			return "", err
		}
		version = awsInt64Value(schema.LatestSchemaVersion)
	}
	if version <= 0 {
		return "", errf("schema version checkpoint is required")
	}
	if _, err := b.client.UpdateSchema(ctx, &glue.UpdateSchemaInput{
		SchemaId:            b.schemaID(subject),
		Compatibility:       compatibility,
		SchemaVersionNumber: &gluetypes.SchemaVersionNumber{VersionNumber: awsInt64Ptr(version)},
	}); err != nil {
		return "", err
	}
	// 回读生效值；失败时返回 NONE（Update 已成功，不因回读失败报错）。
	if out, err := b.client.GetSchema(ctx, &glue.GetSchemaInput{SchemaId: b.schemaID(subject)}); err == nil {
		return glueCompatibilityFromGlue(out), nil
	}
	return glueCompatibilityFromGlue(nil), nil
}

// checkCompatibility = glue CheckSchemaVersionValidity（纯语法有效性校验，
// 无副作用、不做注册兼容判定——tinyrdm 同款语义，messages 说明这一点）。
func (b *glueBackend) checkCompatibility(ctx context.Context, subject string, version int64, format, schema string, refs []SchemaReference) (bool, []string, error) {
	if err := b.requireRegistry(); err != nil {
		return false, nil, err
	}
	if strings.TrimSpace(subject) == "" {
		return false, nil, errf("subject is required")
	}
	if _, err := normalizeKafkaSchemaFormat(format); err != nil {
		return false, nil, err
	}
	if strings.TrimSpace(schema) == "" {
		return false, nil, errf("schema is required")
	}
	out, err := b.client.CheckSchemaVersionValidity(ctx, &glue.CheckSchemaVersionValidityInput{
		DataFormat:       glueDataFormat(format),
		SchemaDefinition: awsStringPtr(schema),
	})
	if err != nil {
		return false, nil, err
	}
	valid := out.Valid
	messages := []string{"AWS Glue validity check has no side effects; registry compatibility is enforced when registering the schema version."}
	if msg := awsStringValue(out.Error); msg != "" {
		messages = append(messages, msg)
	}
	return valid, messages, nil
}

// registerSchema：先 GetSchema 探测——不存在 → CreateSchema（首个版本，
// 兼容级别取请求 compatibility，缺省 NONE）；存在 → RegisterSchemaVersion
// （幂等：Glue 对重复定义返回既有版本）。references 为 Confluent 概念，
// Glue 忽略。normalize 为 Confluent REST 语义，Glue 无归一化端点 → 显式
// 报错（诚实拒绝，不静默忽略）。
func (b *glueBackend) registerSchema(ctx context.Context, subject, format, schema string, refs []SchemaReference, compatibility string, normalize bool) (SchemaMeta, error) {
	if normalize {
		return SchemaMeta{}, errf("normalize is not supported by the AWS Glue schema registry backend (Confluent-compatible registries only)")
	}
	if err := b.requireRegistry(); err != nil {
		return SchemaMeta{}, err
	}
	subject = strings.TrimSpace(subject)
	if subject == "" {
		return SchemaMeta{}, errf("subject is required")
	}
	if _, err := normalizeKafkaSchemaFormat(format); err != nil {
		return SchemaMeta{}, err
	}
	if strings.TrimSpace(schema) == "" {
		return SchemaMeta{}, errf("schema is required")
	}
	_, err := b.client.GetSchema(ctx, &glue.GetSchemaInput{SchemaId: b.schemaID(subject)})
	if err != nil {
		if !isAWSNotFound(err) {
			return SchemaMeta{}, err
		}
		compat, err := normalizeGlueCompatibility(compatibility)
		if err != nil {
			return SchemaMeta{}, err
		}
		out, err := b.client.CreateSchema(ctx, &glue.CreateSchemaInput{
			RegistryId:       &gluetypes.RegistryId{RegistryName: &b.registryName},
			SchemaName:       awsStringPtr(subject),
			DataFormat:       glueDataFormat(format),
			Compatibility:    compat,
			SchemaDefinition: awsStringPtr(schema),
		})
		if err != nil {
			return SchemaMeta{}, err
		}
		return SchemaMeta{
			Subject:    subject,
			Version:    awsInt64Value(out.LatestSchemaVersion),
			VersionID:  awsStringValue(out.SchemaVersionId),
			Schema:     schema,
			SchemaType: normalizeConfluentSchemaType(format),
		}, nil
	}
	out, err := b.client.RegisterSchemaVersion(ctx, &glue.RegisterSchemaVersionInput{
		SchemaId:         b.schemaID(subject),
		SchemaDefinition: awsStringPtr(schema),
	})
	if err != nil {
		return SchemaMeta{}, err
	}
	return SchemaMeta{
		Subject:    subject,
		Version:    awsInt64Value(out.VersionNumber),
		VersionID:  awsStringValue(out.SchemaVersionId),
		Schema:     schema,
		SchemaType: normalizeConfluentSchemaType(format),
	}, nil
}

// deleteSchema：version>0 → DeleteSchemaVersions（单版本区间；SDK 本版未
// 建模 VersionNumbers，按错误清单折算：无错误 = 该版本已删）；version=0 →
// DeleteSchema（整个 schema，Glue 不返回被删版本号清单 → deletedVersions 为空）。
func (b *glueBackend) deleteSchema(ctx context.Context, subject string, version int64) ([]int64, error) {
	if err := b.requireRegistry(); err != nil {
		return nil, err
	}
	subject = strings.TrimSpace(subject)
	if subject == "" {
		return nil, errf("subject is required")
	}
	if version > 0 {
		out, err := b.client.DeleteSchemaVersions(ctx, &glue.DeleteSchemaVersionsInput{
			SchemaId: b.schemaID(subject),
			Versions: awsStringPtr(strconv.FormatInt(version, 10)),
		})
		if err != nil {
			return nil, err
		}
		if len(out.SchemaVersionErrors) > 0 {
			item := out.SchemaVersionErrors[0]
			message := ""
			if item.ErrorDetails != nil {
				message = awsStringValue(item.ErrorDetails.ErrorMessage)
			}
			return nil, errf("delete schema version %d: %s", version, message)
		}
		return []int64{version}, nil
	}
	out, err := b.client.DeleteSchema(ctx, &glue.DeleteSchemaInput{SchemaId: b.schemaID(subject)})
	if err != nil {
		return nil, err
	}
	_ = out // status 仅审计语义，不透出（SchemaDeleteResult 只有版本号清单）
	return []int64{}, nil
}
