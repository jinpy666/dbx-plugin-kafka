package kafkaconn

// policy.go：安全策略（IMPL_PLAN §6）。
//   - read_only=true：produce/create/alter/reset/ACL 写一律拒绝
//     （错误码语义 blocked，经 main 层 bizError 统一映射 -32000，与 ldap
//     policy 一致）。
//   - allow_delete=false：topics/delete、topics/records/clear（Phase 3）、
//     groups/delete、acls/delete 额外拒绝；read_only 下 allow_delete 无效
//     （两者与门）。
//   - topics/delete 要求 confirmTopic 与待删 topic 同名（防误删）；
//     topics/records/clear 复用同一单 topic 确认语义（防误清空）；
//     groups/delete 要求 confirmGroup 与待删组同名（同款强度）。
//   - 凭据（sasl_password、tls_client_key）不落日志、不进审计、不回显。

import (
	"fmt"
	"strings"
)

// ensureWriteAllowed 写操作门禁（read_only 与门第一层）。
// action 形如 "produce" / "topics/create"，用于错误提示与审计。
func ensureWriteAllowed(profile Profile, action string) error {
	if profile.ReadOnly {
		return fmt.Errorf("kafka profile %q is read-only; %s is blocked", profile.Name, action)
	}
	return nil
}

// ensureDeleteAllowed 删除类门禁（read_only ∥ !allow_delete 任一即拒；与门）。
func ensureDeleteAllowed(profile Profile, action string) error {
	if err := ensureWriteAllowed(profile, action); err != nil {
		return err
	}
	if !profile.AllowDelete {
		return fmt.Errorf("kafka profile %q does not allow delete operations; %s is blocked", profile.Name, action)
	}
	return nil
}

// ensureTopicDeleteConfirm 校验 topics/delete 的确认字段（§6：confirmTopic
// 与待删 topic 同名防误删）。多 topic 用 confirmTopics 逐一对齐。
// 失败路径返回 *InvalidParamsError（§3.2 冻结语义：confirm 不匹配是参数错
// → -32602，与 Phase 3 topics/records/clear 对齐）。
func ensureTopicDeleteConfirm(req TopicsDeleteRequest) error {
	topics := normalizeTopicNames(req.Topics)
	if len(topics) == 0 {
		return &InvalidParamsError{Msg: "topics is required"}
	}
	confirm := req.ConfirmTopic
	if len(topics) > 1 {
		if len(req.ConfirmTopics) != len(topics) {
			return &InvalidParamsError{Msg: fmt.Sprintf("confirmTopics must list exactly the topics to delete (got %d, want %d)", len(req.ConfirmTopics), len(topics))}
		}
		confirmed := map[string]struct{}{}
		for _, name := range normalizeTopicNames(req.ConfirmTopics) {
			confirmed[name] = struct{}{}
		}
		for _, topic := range topics {
			if _, ok := confirmed[topic]; !ok {
				return &InvalidParamsError{Msg: fmt.Sprintf("confirmTopics mismatch: topic %q not confirmed (confirmTopic guard)", topic)}
			}
		}
		return nil
	}
	if strings.TrimSpace(confirm) == "" {
		return &InvalidParamsError{Msg: "confirmTopic must match the topic name to delete (confirmTopic guard)"}
	}
	if strings.TrimSpace(confirm) != topics[0] {
		return &InvalidParamsError{Msg: fmt.Sprintf("confirmTopic %q does not match topic %q (confirmTopic guard)", strings.TrimSpace(confirm), topics[0])}
	}
	return nil
}

// ensureGroupDeleteConfirm 校验 groups/delete 的确认字段（审查 L4：与
// topics/delete 的 confirmTopic 同级——confirmGroup 与待删组同名防误删，
// 2026-09-26 补齐）。失败返回 *InvalidParamsError（§3.2 冻结语义：confirm
// 不匹配是参数错 → -32602）。
func ensureGroupDeleteConfirm(req GroupDeleteRequest) error {
	group := trimSpace(req.Group)
	confirm := trimSpace(req.ConfirmGroup)
	if confirm == "" {
		return &InvalidParamsError{Msg: "confirmGroup must match the group name to delete (confirmGroup guard)"}
	}
	if confirm != group {
		return &InvalidParamsError{Msg: fmt.Sprintf("confirmGroup %q does not match group %q (confirmGroup guard)", confirm, group)}
	}
	return nil
}

// normalizeTopicNames 归一化 topic 名列表（trim + 去空 + 去重）。
func normalizeTopicNames(topics []string) []string {
	out := make([]string, 0, len(topics))
	seen := map[string]struct{}{}
	for _, topic := range topics {
		topic = strings.TrimSpace(topic)
		if topic == "" {
			continue
		}
		if _, ok := seen[topic]; ok {
			continue
		}
		seen[topic] = struct{}{}
		out = append(out, topic)
	}
	return out
}
