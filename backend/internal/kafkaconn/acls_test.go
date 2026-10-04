package kafkaconn

// acls_test.go：ACL builder 组装与映射（契约 §5.2）全离线矩阵——
// builderFromFilter/FromACL 校验路径、资源类型分发、权限挂载、
// described→binding 映射、审计 target 拼接，及 Service 方法进 broker
// 之前的门禁/校验分支（ghost 连接或 127.0.0.1:1 快速失败）。

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestACLBuilderFromFilterMatrix(t *testing.T) {
	cases := []struct {
		name       string
		filter     ACLFilter
		wantErrSub string // 空 = 期望成功
		wantAny    bool   // 期望 builder 带 "any" 宽匹配（operation 默认/显式 any）
	}{
		{name: "topic by name", filter: ACLFilter{ResourceType: "topic", ResourceName: "orders"}, wantAny: true},
		{name: "group only", filter: ACLFilter{ResourceType: "group"}, wantAny: true},
		{name: "any type by name plus principal", filter: ACLFilter{ResourceType: "any", ResourceName: "orders", Principal: "alice"}, wantAny: true},
		{name: "any type by name plus operation", filter: ACLFilter{ResourceType: "any", ResourceName: "orders", Operation: "read"}, wantAny: true}, // patternType 空 = ANY（评审 H-1 同族：不限语义）
		{name: "explicit any operation", filter: ACLFilter{ResourceType: "topic"}, wantAny: true},
		{name: "resourceType required", filter: ACLFilter{ResourceName: "x"}, wantErrSub: "resourceType must be"},
		{name: "any without name too broad", filter: ACLFilter{ResourceType: "any"}, wantErrSub: "filter is too broad"},
		{name: "any name without principal/operation too broad", filter: ACLFilter{ResourceType: "any", ResourceName: "x"}, wantErrSub: "filter is too broad"},
		{name: "bogus resourceType", filter: ACLFilter{ResourceType: "bogus"}, wantErrSub: "resourceType must be"},
		{name: "bogus operation", filter: ACLFilter{ResourceType: "topic", Operation: "bogus"}, wantErrSub: "operation must be"},
		{name: "bogus pattern", filter: ACLFilter{ResourceType: "topic", PatternType: "bogus"}, wantErrSub: "patternType must be"},
		{name: "bogus permission", filter: ACLFilter{ResourceType: "topic", Permission: "bogus"}, wantErrSub: "permission must be"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			builder, err := aclBuilderFromFilter(tc.filter, false)
			if tc.wantErrSub != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("error = %v, want contains %q", err, tc.wantErrSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !builder.HasResource() {
				t.Error("builder should carry a resource")
			}
			if err := builder.ValidateFilter(); err != nil {
				t.Errorf("ValidateFilter = %v, want nil", err)
			}
			if tc.wantAny != builder.HasAnyFilter() {
				t.Errorf("HasAnyFilter = %v, want %v", builder.HasAnyFilter(), tc.wantAny)
			}
		})
	}
}

func TestACLBuilderFromACLMatrix(t *testing.T) {
	cases := []struct {
		name       string
		acl        ACLBinding
		create     bool
		wantErrSub string // 空 = 期望成功
	}{
		{name: "valid create defaults", acl: ACLBinding{ResourceType: "topic", ResourceName: "orders", Operation: "read", Principal: "User:alice"}, create: true},
		{name: "prefixed create", acl: ACLBinding{ResourceType: "topic", ResourceName: "orders-", PatternType: "prefixed", Operation: "write", Principal: "alice"}, create: true},
		{name: "create rejects any resourceType", acl: ACLBinding{ResourceType: "any", ResourceName: "x", Operation: "read", Principal: "a"}, create: true, wantErrSub: "must be a concrete type"},
		{name: "create rejects empty name", acl: ACLBinding{ResourceType: "topic", Operation: "read", Principal: "a"}, create: true, wantErrSub: "resourceName is required"},
		{name: "create rejects any operation", acl: ACLBinding{ResourceType: "topic", ResourceName: "x", Operation: "any", Principal: "a"}, create: true, wantErrSub: "must be a concrete operation"},
		{name: "create rejects any permission", acl: ACLBinding{ResourceType: "topic", ResourceName: "x", Operation: "read", Permission: "any", Principal: "a"}, create: true, wantErrSub: "must be a concrete permission"},
		{name: "create rejects missing principal", acl: ACLBinding{ResourceType: "topic", ResourceName: "x", Operation: "read"}, create: true, wantErrSub: "principal is required"},
		{name: "bogus operation", acl: ACLBinding{ResourceType: "topic", ResourceName: "x", Operation: "bogus", Principal: "a"}, create: true, wantErrSub: "operation must be"},
		{name: "bogus pattern", acl: ACLBinding{ResourceType: "topic", ResourceName: "x", PatternType: "bogus", Operation: "read", Principal: "a"}, create: true, wantErrSub: "patternType must be"},
		{name: "bogus permission", acl: ACLBinding{ResourceType: "topic", ResourceName: "x", Permission: "bogus", Operation: "read", Principal: "a"}, create: true, wantErrSub: "permission must be"},
		{name: "non-create tolerates any", acl: ACLBinding{ResourceType: "any", Operation: "any", Principal: "a"}, create: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			builder, err := aclBuilderFromACL(tc.acl, tc.create)
			if tc.wantErrSub != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("error = %v, want contains %q", err, tc.wantErrSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.create {
				// create 语义：literal/prefixed + 具体操作 → ValidateCreate 通过。
				if err := builder.ValidateCreate(); err != nil {
					t.Errorf("ValidateCreate = %v, want nil", err)
				}
			}
			if !builder.HasPrincipals() {
				t.Error("builder should carry principals")
			}
		})
	}
}

func TestApplyACLResourceMatrix(t *testing.T) {
	// 每种资源类型挂载后 builder 都应持有资源（方法族不可参数化，逐类分发）。
	cases := []struct {
		name         string
		resourceType kmsg.ACLResourceType
	}{
		{"topic", kmsg.ACLResourceTypeTopic},
		{"group", kmsg.ACLResourceTypeGroup},
		{"cluster", kmsg.ACLResourceTypeCluster},
		{"transactionalId", kmsg.ACLResourceTypeTransactionalId},
		{"delegationToken", kmsg.ACLResourceTypeDelegationToken},
		{"any fallback", kmsg.ACLResourceTypeAny},
		{"user fallback", kmsg.ACLResourceTypeUser},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			builder := kadm.NewACLs()
			applyACLResource(builder, tc.resourceType, "orders", kmsg.ACLResourcePatternTypeLiteral)
			if !builder.HasResource() {
				t.Errorf("resourceType %v: HasResource = false, want true", tc.resourceType)
			}
		})
	}
}

func TestApplyACLPermissionAllowAndDeny(t *testing.T) {
	// allow 与 deny 两条路径都要让 builder 持有 principal/host。
	builder := kadm.NewACLs()
	applyACLPermission(builder, kmsg.ACLPermissionTypeAllow, "User:alice", "host-1")
	if !builder.HasPrincipals() || !builder.HasHosts() {
		t.Errorf("allow: principals=%v hosts=%v, want true/true", builder.HasPrincipals(), builder.HasHosts())
	}

	builder = kadm.NewACLs()
	applyACLPermission(builder, kmsg.ACLPermissionTypeDeny, "User:mallory", "")
	if !builder.HasPrincipals() {
		t.Error("deny: HasPrincipals = false, want true")
	}

	// unknown permission 兜底走 any 语义（双侧挂载，零参 = any）。
	builder = kadm.NewACLs()
	applyACLPermission(builder, kmsg.ACLPermissionType(0), "User:x", "")
	if !builder.HasPrincipals() {
		t.Error("default: HasPrincipals = false, want true")
	}
}

// recordingACLBuilder 记录挂载调用序列（评审 H-1 回归钉的测试缝）：kadm 的
// 导出 getter（HasAnyFilter 等）区分不了「钉死空串」与「零参 any」——正是
// 旧缺陷逃过全部测试的原因，所以在挂载层直接断言。
type recordingACLBuilder struct{ calls []string }

func (r *recordingACLBuilder) record(name string, args ...string) {
	r.calls = append(r.calls, name+"("+strings.Join(args, ",")+")")
}

func (r *recordingACLBuilder) AnyResource(name ...string) *kadm.ACLBuilder {
	r.record("AnyResource", name...)
	return nil
}
func (r *recordingACLBuilder) Topics(t ...string) *kadm.ACLBuilder {
	r.record("Topics", t...)
	return nil
}
func (r *recordingACLBuilder) Groups(g ...string) *kadm.ACLBuilder {
	r.record("Groups", g...)
	return nil
}
func (r *recordingACLBuilder) Clusters() *kadm.ACLBuilder { r.record("Clusters"); return nil }
func (r *recordingACLBuilder) TransactionalIDs(x ...string) *kadm.ACLBuilder {
	r.record("TransactionalIDs", x...)
	return nil
}
func (r *recordingACLBuilder) DelegationTokens(t ...string) *kadm.ACLBuilder {
	r.record("DelegationTokens", t...)
	return nil
}
func (r *recordingACLBuilder) ResourcePatternType(pattern kmsg.ACLResourcePatternType) *kadm.ACLBuilder {
	r.record("ResourcePatternType", pattern.String())
	return nil
}
func (r *recordingACLBuilder) Allow(principals ...string) *kadm.ACLBuilder {
	r.record("Allow", principals...)
	return nil
}
func (r *recordingACLBuilder) AllowHosts(hosts ...string) *kadm.ACLBuilder {
	r.record("AllowHosts", hosts...)
	return nil
}
func (r *recordingACLBuilder) Deny(principals ...string) *kadm.ACLBuilder {
	r.record("Deny", principals...)
	return nil
}
func (r *recordingACLBuilder) DenyHosts(hosts ...string) *kadm.ACLBuilder {
	r.record("DenyHosts", hosts...)
	return nil
}
func (r *recordingACLBuilder) PrefixUserExcept(except ...string) {
	r.record("PrefixUserExcept", except...)
}

// TestACLFilterEmptyFieldsMountAsAny 钉死 H-1 修复的挂载决策：空 name/
// principal/host 必须落成零参调用（kadm any），绝不以空串实参出现——空串
// 会被钉成字面精确匹配，acls/list 恒空、acls/delete 静默 no-op；permission
// 过滤空 = ANY（双侧），create 的空 host 展开为 kadm 文档默认 "*"。
func TestACLFilterEmptyFieldsMountAsAny(t *testing.T) {
	rec := &recordingACLBuilder{}
	applyACLResource(rec, kmsg.ACLResourceTypeTopic, "", kmsg.ACLResourcePatternTypeAny)
	applyACLPermission(rec, kmsg.ACLPermissionTypeAny, "", "")
	want := strings.Join([]string{
		"ResourcePatternType(ANY)",
		"Topics()",
		"Allow()",
		"AllowHosts()",
		"Deny()",
		"DenyHosts()",
		"PrefixUserExcept(User:,Group:,ANONYMOUS)",
	}, " ")
	if got := strings.Join(rec.calls, " "); got != want {
		t.Fatalf("calls = %q, want %q", got, want)
	}

	// 全字段钉死：精确匹配逐字出现，permission=deny 只挂 deny 侧。
	rec = &recordingACLBuilder{}
	applyACLResource(rec, kmsg.ACLResourceTypeTopic, "orders", kmsg.ACLResourcePatternTypeLiteral)
	applyACLPermission(rec, kmsg.ACLPermissionTypeDeny, "User:mallory", "host-1")
	want = strings.Join([]string{
		"ResourcePatternType(LITERAL)",
		"Topics(orders)",
		"Deny(User:mallory)",
		"DenyHosts(host-1)",
		"PrefixUserExcept(User:,Group:,ANONYMOUS)",
	}, " ")
	if got := strings.Join(rec.calls, " "); got != want {
		t.Fatalf("pinned calls = %q, want %q", got, want)
	}

	// permission=any + 钉死 principal：Allow+Deny 两侧都钉（broker 无「双侧」
	// 单值），host 保持零参 any。
	rec = &recordingACLBuilder{}
	applyACLPermission(rec, kmsg.ACLPermissionTypeAny, "User:alice", "")
	want = strings.Join([]string{
		"Allow(User:alice)",
		"AllowHosts()",
		"Deny(User:alice)",
		"DenyHosts()",
		"PrefixUserExcept(User:,Group:,ANONYMOUS)",
	}, " ")
	if got := strings.Join(rec.calls, " "); got != want {
		t.Fatalf("any-permission calls = %q, want %q", got, want)
	}
}

// TestACLFilterMapperDefaults 钉死过滤路径的空值映射：permission/patternType
// 空 = 不限（create 路径保持「空 = allow/literal」默认）。
func TestACLFilterMapperDefaults(t *testing.T) {
	permission, err := aclFilterPermissionType("")
	if err != nil || permission != kmsg.ACLPermissionTypeAny {
		t.Fatalf("filter permission(\"\") = %v, %v; want ANY", permission, err)
	}
	if permission, err = aclFilterPermissionType("deny"); err != nil || permission != kmsg.ACLPermissionTypeDeny {
		t.Fatalf("filter permission(deny) = %v, %v; want DENY", permission, err)
	}
	pattern, err := aclFilterPatternType("")
	if err != nil || pattern != kmsg.ACLResourcePatternTypeAny {
		t.Fatalf("filter patternType(\"\") = %v, %v; want ANY", pattern, err)
	}
	if pattern, err = aclFilterPatternType("prefixed"); err != nil || pattern != kmsg.ACLResourcePatternTypePrefixed {
		t.Fatalf("filter patternType(prefixed) = %v, %v; want PREFIXED", pattern, err)
	}
}

func TestACLBindingFromDescribed(t *testing.T) {
	// kmsg 枚举 String() 输出大写协议名（TOPIC/READ/...）。
	binding := aclBindingFromDescribed(kadm.DescribedACL{
		Principal:  "User:alice",
		Host:       "*",
		Type:       kmsg.ACLResourceTypeTopic,
		Name:       "orders",
		Pattern:    kmsg.ACLResourcePatternTypePrefixed,
		Operation:  kmsg.ACLOperationRead,
		Permission: kmsg.ACLPermissionTypeDeny,
	})
	want := ACLBinding{
		ResourceType: "TOPIC",
		ResourceName: "orders",
		PatternType:  "PREFIXED",
		Principal:    "User:alice",
		Host:         "*",
		Operation:    "READ",
		Permission:   "DENY",
	}
	if binding != want {
		t.Errorf("binding = %+v, want %+v", binding, want)
	}
}

func TestACLTargetStrings(t *testing.T) {
	if got := aclTarget(ACLBinding{ResourceType: "topic", ResourceName: "orders", Principal: "User:a"}); got != "topic/orders/User:a" {
		t.Errorf("aclTarget = %q", got)
	}
	got := aclFilterTarget(ACLFilter{ResourceType: "group", ResourceName: "g1", Principal: "User:b"})
	if got != "group/g1/User:b" {
		t.Errorf("aclFilterTarget = %q", got)
	}
}

func TestListACLsRejectsBroadFilterBeforeBroker(t *testing.T) {
	// ghost 连接 + 过宽 filter：filter 校验在 withAdmin 之前 → 校验错优先。
	service := NewService()
	_, err := service.ListACLs(context.Background(), ACLsListRequest{
		ConnectionID: "ghost",
		Filter:       ACLFilter{ResourceType: "any"},
	})
	if err == nil || !strings.Contains(err.Error(), "too broad") {
		t.Fatalf("error = %v, want too-broad rejection", err)
	}
	// 合法 filter + 未连接 → 连接不存在。
	_, err = service.ListACLs(context.Background(), ACLsListRequest{
		ConnectionID: "ghost",
		Filter:       ACLFilter{ResourceType: "topic"},
	})
	if err == nil {
		t.Fatal("missing connection expected error")
	}
}

func TestCreateACLGateAndValidation(t *testing.T) {
	// ghost 连接（read_only 兜底）→ 写门禁 blocked + 审计。
	service := NewService()
	var audits []AuditRecord
	service.Audit = func(rec AuditRecord) { audits = append(audits, rec) }
	err := service.CreateACL(context.Background(), ACLsCreateRequest{
		ConnectionID: "ghost",
		ACL:          ACLBinding{ResourceType: "topic", ResourceName: "orders", Operation: "read", Principal: "User:a"},
	})
	if err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("error = %v, want read-only block", err)
	}
	if len(audits) != 1 || audits[0].Result != "blocked" || audits[0].Action != "acls-create" {
		t.Errorf("audits = %+v, want one blocked acls-create", audits)
	}
	if audits[0].Target != "topic/orders/User:a" {
		t.Errorf("audit target = %q, want topic/orders/User:a", audits[0].Target)
	}

	// 门禁放行（127.0.0.1:1 快速失败）→ 必填校验在 broker 之前。
	service = NewService()
	if err := connectWithConfig(t, service, "acl-create",
		`{"bootstrap_servers": "127.0.0.1:1"}`, `{}`); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := service.CreateACL(context.Background(), ACLsCreateRequest{
		ConnectionID: "acl-create",
		ACL:          ACLBinding{ResourceType: "topic", Operation: "read", Principal: "User:a"},
	}); err == nil || !strings.Contains(err.Error(), "resourceName is required") {
		t.Errorf("error = %v, want resourceName required", err)
	}
	if err := service.CreateACL(context.Background(), ACLsCreateRequest{
		ConnectionID: "acl-create",
		ACL:          ACLBinding{ResourceType: "topic", ResourceName: "orders", Operation: "read"},
	}); err == nil || !strings.Contains(err.Error(), "principal is required") {
		t.Errorf("error = %v, want principal required", err)
	}
}

func TestDeleteACLsGateOffline(t *testing.T) {
	// ghost 连接（read_only 兜底）→ 与门第一层 blocked。
	service := NewService()
	var audits []AuditRecord
	service.Audit = func(rec AuditRecord) { audits = append(audits, rec) }
	_, err := service.DeleteACLs(context.Background(), ACLsDeleteRequest{
		ConnectionID: "ghost",
		Filter:       ACLFilter{ResourceType: "topic", ResourceName: "orders"},
	})
	if err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("error = %v, want read-only block", err)
	}
	if len(audits) != 1 || audits[0].Result != "blocked" || audits[0].Action != "acls-delete" {
		t.Errorf("audits = %+v, want one blocked acls-delete", audits)
	}
	if audits[0].Target != "topic/orders/" {
		t.Errorf("audit target = %q, want topic/orders/", audits[0].Target)
	}

	// read_only=false + allow_delete=false → 与门第二层 blocked。
	service = NewService()
	if err := connectWithConfig(t, service, "acl-delete",
		`{"bootstrap_servers": "127.0.0.1:1", "allow_delete": false}`, `{}`); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := service.DeleteACLs(context.Background(), ACLsDeleteRequest{
		ConnectionID: "acl-delete",
		Filter:       ACLFilter{ResourceType: "topic"},
	}); err == nil || !strings.Contains(err.Error(), "does not allow delete") {
		t.Fatalf("error = %v, want allow_delete block", err)
	}

	// 门禁放行 + 过宽 filter → builder 校验错（普通业务错，非 InvalidParams）。
	service = NewService()
	if err := connectWithConfig(t, service, "acl-delete-ok",
		`{"bootstrap_servers": "127.0.0.1:1", "allow_delete": true}`, `{}`); err != nil {
		t.Fatalf("connect: %v", err)
	}
	_, err = service.DeleteACLs(context.Background(), ACLsDeleteRequest{
		ConnectionID: "acl-delete-ok",
		Filter:       ACLFilter{ResourceType: "any"},
	})
	if err == nil || !strings.Contains(err.Error(), "too broad") {
		t.Fatalf("error = %v, want too-broad rejection", err)
	}
	var paramErr *InvalidParamsError
	if errors.As(err, &paramErr) {
		t.Error("filter-too-broad is a business error, not InvalidParamsError")
	}
}
