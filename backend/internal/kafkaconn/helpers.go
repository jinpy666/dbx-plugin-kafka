package kafkaconn

// helpers.go：请求归一化与 mutation 结果映射（tinyrdm
// normalizeKafkaTopicNames :1878 / normalizeKafkaPartitionUpdates :1973 /
// kafkaAlterConfigsFromMap :1903 / kafkaCreateTopicConfigsFromMap :1958 /
// kafkaMutationResp 系 :3463-3540 收敛重写）。

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// MutationResult 单条管理面变更结果（契约 results[]{topic,ok,error}）。
type MutationResult struct {
	Topic string `json:"topic"`
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// trimSpace 短别名。
func trimSpace(value string) string {
	return strings.TrimSpace(value)
}

// sprintf 短别名。
func sprintf(format string, args ...any) string {
	return fmt.Sprintf(format, args...)
}

// joinNames 逗号拼接 topic 名（审计/错误信息用）。
func joinNames(names []string) string {
	return strings.Join(names, ",")
}

// joinMapKeys 逗号拼接 map key（审计 target 用，顺序稳定）。
func joinMapKeys(m map[string]int32) string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

// createTopicConfigsFromMap 建 topic 配置 map（值为指针；空 value 删除项）。
func createTopicConfigsFromMap(config map[string]string) (map[string]*string, error) {
	if len(config) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(config))
	for key := range config {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make(map[string]*string, len(config))
	for _, key := range keys {
		key = trimSpace(key)
		if key == "" {
			return nil, errf("config key cannot be empty")
		}
		value := config[key]
		out[key] = &value
	}
	return out, nil
}

// alterConfigsFromMap 组装增量配置变更（config → Set，deleteKeys → Delete）。
func alterConfigsFromMap(config map[string]string, deleteKeys []string) ([]kadm.AlterConfig, error) {
	configs := make([]kadm.AlterConfig, 0, len(config)+len(deleteKeys))
	keys := make([]string, 0, len(config))
	for key := range config {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		key = trimSpace(key)
		if key == "" {
			return nil, errf("config key cannot be empty")
		}
		value := config[key]
		configs = append(configs, kadm.AlterConfig{Op: kadm.SetConfig, Name: key, Value: &value})
	}
	for _, key := range deleteKeys {
		key = trimSpace(key)
		if key == "" {
			return nil, errf("deleteKeys cannot contain empty keys")
		}
		configs = append(configs, kadm.AlterConfig{Op: kadm.DeleteConfig, Name: key})
	}
	return configs, nil
}

// normalizePartitionUpdates 归一化扩分区请求（map topic→新分区数，按目标
// 分区数分组合并后逐组 UpdatePartitions，tinyrdm 同款）。
func normalizePartitionUpdates(partitions map[string]int32) (map[int][]string, error) {
	if len(partitions) == 0 {
		return nil, errf("partitions is required")
	}
	result := make(map[int][]string)
	for topic, count := range partitions {
		topic = trimSpace(topic)
		if topic == "" {
			return nil, errf("topic is required")
		}
		if count <= 0 {
			return nil, errf("partition count for %q must be greater than 0", topic)
		}
		result[int(count)] = append(result[int(count)], topic)
	}
	for count := range result {
		sort.Strings(result[count])
	}
	return result, nil
}

// parseOffsetTime 解析 offsetTime（Phase 2 全策略；常量对齐 Kafka ListOffsets
// timestamp 语义：-1 latest / -2 earliest / -3 max timestamp / -4 log start，
// 实现走 kadm 对应方法）。
func parseOffsetTime(value string) (mode string, millis int64, err error) {
	raw := trimSpace(value)
	normalized := strings.ToLower(raw)
	switch normalized {
	case "", "latest", "end":
		return "latest", 0, nil
	case "earliest", "start", "beginning":
		return "earliest", 0, nil
	case "max-timestamp", "max_timestamp", "maxtimestamp", "max":
		return "max-timestamp", 0, nil
	case "log-start", "log_start", "logstart":
		return "log-start", 0, nil
	}
	if parsed, parseErr := strconv.ParseInt(raw, 10, 64); parseErr == nil {
		// 负数整数与策略名冲突（-1/-2/-3/-4 是协议保留值），拒绝。
		if parsed < 0 {
			return "", 0, errf("offsetTime must be earliest, latest, max-timestamp, log-start, unix milliseconds, or RFC3339")
		}
		return "timestamp", parsed, nil
	}
	if ts, parseErr := time.Parse(time.RFC3339, raw); parseErr == nil {
		return "timestamp", ts.UnixMilli(), nil
	}
	return "", 0, errf("offsetTime must be earliest, latest, max-timestamp, log-start, unix milliseconds, or RFC3339")
}

// mutationResultsTopic create/update 类响应映射（resource=topic）。
func mutationResultsTopic(responses kadm.CreateTopicResponses) []MutationResult {
	results := make([]MutationResult, 0, len(responses))
	for _, response := range responses.Sorted() {
		item := MutationResult{Topic: response.Topic, OK: response.Err == nil}
		if response.Err != nil {
			item.Error = response.Err.Error()
		}
		results = append(results, item)
	}
	return results
}

// mutationResultsCreatePartitions 扩分区响应映射（CreatePartitionsResponses）。
func mutationResultsCreatePartitions(responses kadm.CreatePartitionsResponses) []MutationResult {
	results := make([]MutationResult, 0, len(responses))
	for _, response := range responses.Sorted() {
		item := MutationResult{Topic: response.Topic, OK: response.Err == nil}
		if response.Err != nil {
			item.Error = response.Err.Error()
		}
		results = append(results, item)
	}
	return results
}

// mutationResultsTopicDelete delete 类响应映射。
func mutationResultsTopicDelete(responses kadm.DeleteTopicResponses) []MutationResult {
	results := make([]MutationResult, 0, len(responses))
	for _, response := range responses.Sorted() {
		item := MutationResult{Topic: response.Topic, OK: response.Err == nil}
		if response.Err != nil {
			item.Error = response.Err.Error()
		}
		results = append(results, item)
	}
	return results
}

// alterConfigResults 配置变更响应映射（AlterConfigsResponses 为 slice）。
func alterConfigResults(responses kadm.AlterConfigsResponses, resource string) []MutationResult {
	results := make([]MutationResult, 0, len(responses))
	for _, response := range responses {
		item := MutationResult{Topic: resource, OK: response.Err == nil}
		if response.Err != nil {
			item.Error = response.Err.Error()
		}
		results = append(results, item)
	}
	return results
}

// aclResourceType 归一化 ACL 资源类型（接受 kmsg 枚举名/任意大小写词）。
func aclResourceType(value string) (kmsg.ACLResourceType, error) {
	switch strings.ToLower(trimSpace(value)) {
	case "any":
		return kmsg.ACLResourceTypeAny, nil
	case "topic":
		return kmsg.ACLResourceTypeTopic, nil
	case "group":
		return kmsg.ACLResourceTypeGroup, nil
	case "cluster":
		return kmsg.ACLResourceTypeCluster, nil
	case "transactionalid", "transactional_id":
		return kmsg.ACLResourceTypeTransactionalId, nil
	case "delegationtoken", "delegation_token":
		return kmsg.ACLResourceTypeDelegationToken, nil
	case "user":
		return kmsg.ACLResourceTypeUser, nil
	default:
		return kmsg.ACLResourceType(0), errf("resourceType must be any, topic, group, cluster, transactionalId, delegationToken, or user")
	}
}

// aclPatternType 归一化 ACL pattern。
func aclPatternType(value string) (kmsg.ACLResourcePatternType, error) {
	switch strings.ToLower(trimSpace(value)) {
	case "", "literal":
		return kmsg.ACLResourcePatternTypeLiteral, nil
	case "prefixed", "prefix":
		return kmsg.ACLResourcePatternTypePrefixed, nil
	case "any":
		return kmsg.ACLResourcePatternTypeAny, nil
	case "match":
		return kmsg.ACLResourcePatternTypeMatch, nil
	default:
		return kmsg.ACLResourcePatternType(0), errf("patternType must be literal, prefixed, any, or match")
	}
}

// aclOperationType 归一化 ACL operation（list/delete filter 空 = any 任意；
// create 路径由 aclBuilderFromACL 拒绝 any）。
func aclOperationType(value string) (kmsg.ACLOperation, error) {
	switch strings.ToLower(trimSpace(value)) {
	case "", "any":
		return kmsg.ACLOperationAny, nil
	case "all":
		return kmsg.ACLOperationAll, nil
	case "read":
		return kmsg.ACLOperationRead, nil
	case "write":
		return kmsg.ACLOperationWrite, nil
	case "create":
		return kmsg.ACLOperationCreate, nil
	case "delete":
		return kmsg.ACLOperationDelete, nil
	case "alter":
		return kmsg.ACLOperationAlter, nil
	case "describe":
		return kmsg.ACLOperationDescribe, nil
	case "clusteraction", "cluster_action":
		return kmsg.ACLOperationClusterAction, nil
	case "describeconfigs", "describe_configs":
		return kmsg.ACLOperationDescribeConfigs, nil
	case "alterconfigs", "alter_configs":
		return kmsg.ACLOperationAlterConfigs, nil
	case "idempotentwrite", "idempotent_write":
		return kmsg.ACLOperationIdempotentWrite, nil
	case "createtokens", "create_tokens":
		return kmsg.ACLOperationCreateTokens, nil
	case "describetokens", "describe_tokens":
		return kmsg.ACLOperationDescribeTokens, nil
	default:
		return kmsg.ACLOperation(0), errf("operation must be any, all, read, write, create, delete, alter, describe, clusterAction, describeConfigs, alterConfigs, idempotentWrite, createTokens, or describeTokens")
	}
}

// aclPermissionType 归一化 ACL permission（create 路径语义：空 = 默认 allow；
// 过滤路径用 aclFilterPermissionType——空 = 不限）。
func aclPermissionType(value string) (kmsg.ACLPermissionType, error) {
	switch strings.ToLower(trimSpace(value)) {
	case "any":
		return kmsg.ACLPermissionTypeAny, nil
	case "", "allow":
		return kmsg.ACLPermissionTypeAllow, nil
	case "deny":
		return kmsg.ACLPermissionTypeDeny, nil
	default:
		return kmsg.ACLPermissionType(0), errf("permission must be any, allow, or deny")
	}
}

// aclFilterPermissionType 过滤路径的 permission：空 = 不限（ANY）。UI「不限」
// 选项发的是缺省值，沿用 create 路径的「空 = allow」会让 DENY ACL 在默认
// 过滤下不可见、acls/delete 删不到（评审 H-1 同族）。
func aclFilterPermissionType(value string) (kmsg.ACLPermissionType, error) {
	if trimSpace(value) == "" {
		return kmsg.ACLPermissionTypeAny, nil
	}
	return aclPermissionType(value)
}

// aclFilterPatternType 过滤路径的 patternType：空 = ANY——沿用 create 的
// 「空 = literal」会使 PREFIXED ACL 在默认过滤下不可见（评审 H-1 同族）。
func aclFilterPatternType(value string) (kmsg.ACLResourcePatternType, error) {
	if trimSpace(value) == "" {
		return kmsg.ACLResourcePatternTypeAny, nil
	}
	return aclPatternType(value)
}
