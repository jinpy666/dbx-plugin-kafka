package kafkaconn

// acls.go：ACL 管理（契约 §5.2，逻辑对照 tiny-rdm kafka_service.go
// ListACLs :800 / CreateACL :831 / DeleteACL :873 / kafkaACLBuilder :3071
// 及 acl 归一化族 :3093-3443 收敛重写）。
// 过宽拒绝（IMPL_PLAN §5.2 filter{} 拒绝过宽）：list/delete 要求
// resourceType 与 operation 至少显式给出其一之外，还要求过滤条件非全空
// —— 实现取 resourceType 必填（tinyrdm validateExactKafkaACLFilter 同义）。

import (
	"context"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ListACLs 实现 kafka/acls/list。
func (s *Service) ListACLs(ctx context.Context, req ACLsListRequest) (*ACLsListResult, error) {
	builder, err := aclBuilderFromFilter(req.Filter, false)
	if err != nil {
		return nil, err
	}
	var result ACLsListResult
	err = s.withAdmin(req.ConnectionID, func(client *kgo.Client) error {
		admin := kadm.NewClient(client)
		ctx, cancel := context.WithTimeout(ctx, adminTimeout)
		defer cancel()

		described, err := admin.DescribeACLs(ctx, builder)
		if err != nil {
			return err
		}
		result.ACLs = []ACLBinding{}
		for _, filterResult := range described {
			if filterResult.Err != nil {
				result.ACLs = append(result.ACLs, ACLBinding{Error: filterResult.Err.Error()})
				continue
			}
			for _, acl := range filterResult.Described {
				result.ACLs = append(result.ACLs, aclBindingFromDescribed(acl))
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// CreateACL 实现 kafka/acls/create（写门禁）。
func (s *Service) CreateACL(ctx context.Context, req ACLsCreateRequest) error {
	profile := s.profileOf(req.ConnectionID)
	target := aclTarget(req.ACL)
	if err := ensureWriteAllowed(profile, "acls/create"); err != nil {
		s.emitAudit(req.ConnectionID, "acls-create", target, "blocked", err.Error())
		return err
	}
	if req.ACL.ResourceName == "" {
		return errf("acl.resourceName is required")
	}
	if req.ACL.Principal == "" {
		return errf("acl.principal is required")
	}
	builder, err := aclBuilderFromACL(req.ACL, true)
	if err != nil {
		return err
	}

	err = s.withAdmin(req.ConnectionID, func(client *kgo.Client) error {
		admin := kadm.NewClient(client)
		ctx, cancel := context.WithTimeout(ctx, adminTimeout)
		defer cancel()

		results, err := admin.CreateACLs(ctx, builder)
		if err != nil {
			return err
		}
		for _, result := range results {
			if result.Err != nil {
				return result.Err
			}
		}
		return nil
	})
	if err != nil {
		s.emitAudit(req.ConnectionID, "acls-create", target, "error", err.Error())
		return err
	}
	s.emitAudit(req.ConnectionID, "acls-create", target, "success", "")
	return nil
}

// DeleteACLs 实现 kafka/acls/delete（critical 门禁）。
func (s *Service) DeleteACLs(ctx context.Context, req ACLsDeleteRequest) (*ACLsDeleteResult, error) {
	profile := s.profileOf(req.ConnectionID)
	if err := ensureDeleteAllowed(profile, "acls/delete"); err != nil {
		s.emitAudit(req.ConnectionID, "acls-delete", aclFilterTarget(req.Filter), "blocked", err.Error())
		return nil, err
	}
	builder, err := aclBuilderFromFilter(req.Filter, false)
	if err != nil {
		return nil, err
	}
	var result ACLsDeleteResult
	err = s.withAdmin(req.ConnectionID, func(client *kgo.Client) error {
		admin := kadm.NewClient(client)
		ctx, cancel := context.WithTimeout(ctx, adminTimeout)
		defer cancel()

		deleted, err := admin.DeleteACLs(ctx, builder)
		if err != nil {
			return err
		}
		result.Matched = []ACLBinding{}
		for _, filterResult := range deleted {
			if filterResult.Err != nil {
				result.Matched = append(result.Matched, ACLBinding{Error: filterResult.Err.Error()})
				continue
			}
			for _, acl := range filterResult.Deleted {
				item := ACLBinding{
					ResourceType: acl.Type.String(),
					ResourceName: acl.Name,
					PatternType:  acl.Pattern.String(),
					Principal:    acl.Principal,
					Host:         acl.Host,
					Operation:    acl.Operation.String(),
					Permission:   acl.Permission.String(),
				}
				if acl.Err != nil {
					item.Error = acl.Err.Error()
				}
				result.Matched = append(result.Matched, item)
			}
		}
		return nil
	})
	if err != nil {
		s.emitAudit(req.ConnectionID, "acls-delete", aclFilterTarget(req.Filter), "error", err.Error())
		return nil, err
	}
	s.emitAudit(req.ConnectionID, "acls-delete", aclFilterTarget(req.Filter), "success", "")
	return &result, nil
}

// aclBuilderFromFilter 组装 describe/delete 过滤 builder（tinyrdm
// applyKafkaACLAllowFilter 语义：空 principal/host = 任意——评审 H-1 起才
// 真正成立，此前空串被 kadm MaybeX("") 钉成字面精确匹配）。
func aclBuilderFromFilter(filter ACLFilter, create bool) (*kadm.ACLBuilder, error) {
	resourceType, err := aclResourceType(filter.ResourceType)
	if err != nil {
		return nil, err
	}
	if resourceType == kmsg.ACLResourceTypeAny && trimSpace(filter.ResourceName) == "" {
		return nil, errf("filter is too broad: resourceType (or resourceName) is required")
	}
	if resourceType == kmsg.ACLResourceTypeAny {
		// 只给了 resourceName：仍要求 principal 或 operation 至少一项限定，
		// 避免全库扫 ACL。
		if trimSpace(filter.Principal) == "" && trimSpace(filter.Operation) == "" {
			return nil, errf("filter is too broad: principal or operation is required alongside resourceName")
		}
	}

	operation, err := aclOperationType(filter.Operation)
	if err != nil {
		return nil, err
	}
	pattern, err := aclFilterPatternType(filter.PatternType)
	if err != nil {
		return nil, err
	}
	permission, err := aclFilterPermissionType(filter.Permission)
	if err != nil {
		return nil, err
	}

	builder := kadm.NewACLs()
	applyACLResource(builder, resourceType, filter.ResourceName, pattern)
	builder.Operations(operation)
	applyACLPermission(builder, permission, filter.Principal, filter.Host)
	return builder, nil
}

// aclBuilderFromACL 组装 create builder（必填校验 + 默认 literal/allow）。
func aclBuilderFromACL(acl ACLBinding, create bool) (*kadm.ACLBuilder, error) {
	resourceType, err := aclResourceType(acl.ResourceType)
	if err != nil {
		return nil, err
	}
	if create && resourceType == kmsg.ACLResourceTypeAny {
		return nil, errf("acl.resourceType must be a concrete type (not any)")
	}
	if create && trimSpace(acl.ResourceName) == "" {
		return nil, errf("acl.resourceName is required")
	}
	operation, err := aclOperationType(acl.Operation)
	if err != nil {
		return nil, err
	}
	if create && operation == kmsg.ACLOperationAny {
		return nil, errf("acl.operation must be a concrete operation (not any)")
	}
	pattern, err := aclPatternType(acl.PatternType)
	if err != nil {
		return nil, err
	}
	permission, err := aclPermissionType(acl.Permission)
	if err != nil {
		return nil, err
	}
	if create && permission == kmsg.ACLPermissionTypeAny {
		// 创建「不限权限」的 ACL 无意义：kadm 的 any 展开成 allow 侧（评审
		// M-1），显式拒绝，与 operation=any 同款门禁。
		return nil, errf("acl.permission must be a concrete permission (not any)")
	}
	if trimSpace(acl.Principal) == "" {
		return nil, errf("acl.principal is required")
	}

	builder := kadm.NewACLs()
	applyACLResource(builder, resourceType, acl.ResourceName, pattern)
	builder.Operations(operation)
	applyACLPermission(builder, permission, acl.Principal, acl.Host)
	return builder, nil
}

// aclBuilderMounts 是 kadm.ACLBuilder 挂载面的最小子集（*kadm.ACLBuilder
// 天然满足，生产路径零适配）。抽接口只为给离线回归钉一个记录调用序列的
// 测试缝：断言「空字段 = 零参 any」这一挂载决策（评审 H-1）。
type aclBuilderMounts interface {
	AnyResource(name ...string) *kadm.ACLBuilder
	Topics(t ...string) *kadm.ACLBuilder
	Groups(g ...string) *kadm.ACLBuilder
	Clusters() *kadm.ACLBuilder
	TransactionalIDs(x ...string) *kadm.ACLBuilder
	DelegationTokens(t ...string) *kadm.ACLBuilder
	ResourcePatternType(pattern kmsg.ACLResourcePatternType) *kadm.ACLBuilder
	Allow(principals ...string) *kadm.ACLBuilder
	AllowHosts(hosts ...string) *kadm.ACLBuilder
	Deny(principals ...string) *kadm.ACLBuilder
	DenyHosts(hosts ...string) *kadm.ACLBuilder
	PrefixUserExcept(except ...string)
}

// mountNames 名称维度统一挂载：空 = 零参调用（任意），非空 = 精确匹配
// （评审 H-1：kadm v1.19.0 的 Topics("") 会把空串原样钉进请求过滤的
// ResourceName——broker 端按字面匹配，空的 principal/host 永不相中，
// acls/list 恒空、acls/delete 静默 no-op）。
func mountNames(mount func(...string) *kadm.ACLBuilder, name string) {
	if trimSpace(name) == "" {
		mount()
		return
	}
	mount(name)
}

// applyACLResource 按资源类型挂载（builder 方法族不可参数化，逐类分发）。
func applyACLResource(builder aclBuilderMounts, resourceType kmsg.ACLResourceType, name string, pattern kmsg.ACLResourcePatternType) {
	builder.ResourcePatternType(pattern)
	switch resourceType {
	case kmsg.ACLResourceTypeTopic:
		mountNames(builder.Topics, name)
	case kmsg.ACLResourceTypeGroup:
		mountNames(builder.Groups, name)
	case kmsg.ACLResourceTypeCluster:
		builder.Clusters() // cluster 无名称维度
	case kmsg.ACLResourceTypeTransactionalId:
		mountNames(builder.TransactionalIDs, name)
	case kmsg.ACLResourceTypeDelegationToken:
		mountNames(builder.DelegationTokens, name)
	default: // any / user
		mountNames(builder.AnyResource, name)
	}
}

// applyACLPermission 权限与 principal/host：空 = 任意（零参），非空 = 精确。
// Allow/Deny 只挂对应侧；Any 两侧都挂——kadm 在「双侧全 any」时折叠成单条
// PermissionType=Any 的过滤，钉了 principal/host 时展开为 Allow+Deny 两条
// （broker 的 permission 过滤没有「双侧」单值）。create 的 host 留空走零参，
// kadm 建档时按文档默认展开为 "*"。
func applyACLPermission(builder aclBuilderMounts, permission kmsg.ACLPermissionType, principal, host string) {
	principalArg := []string(nil)
	if p := trimSpace(principal); p != "" {
		principalArg = []string{p}
	}
	hostArg := []string(nil)
	if h := trimSpace(host); h != "" {
		hostArg = []string{h}
	}
	switch permission {
	case kmsg.ACLPermissionTypeDeny:
		builder.Deny(principalArg...)
		builder.DenyHosts(hostArg...)
	case kmsg.ACLPermissionTypeAllow:
		builder.Allow(principalArg...)
		builder.AllowHosts(hostArg...)
	default: // any（含 unknown 兜底）
		builder.Allow(principalArg...)
		builder.AllowHosts(hostArg...)
		builder.Deny(principalArg...)
		builder.DenyHosts(hostArg...)
	}
	builder.PrefixUserExcept("User:", "Group:", "ANONYMOUS")
}

func aclBindingFromDescribed(acl kadm.DescribedACL) ACLBinding {
	return ACLBinding{
		ResourceType: acl.Type.String(),
		ResourceName: acl.Name,
		PatternType:  acl.Pattern.String(),
		Principal:    acl.Principal,
		Host:         acl.Host,
		Operation:    acl.Operation.String(),
		Permission:   acl.Permission.String(),
	}
}

func aclTarget(acl ACLBinding) string {
	return acl.ResourceType + "/" + acl.ResourceName + "/" + acl.Principal
}

func aclFilterTarget(filter ACLFilter) string {
	return aclTarget(ACLBinding{
		ResourceType: filter.ResourceType,
		ResourceName: filter.ResourceName,
		Principal:    filter.Principal,
	})
}
