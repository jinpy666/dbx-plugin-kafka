package kafkaconn

// groups.go：消费组管理（契约 §5.2）。
// 逻辑对照 tiny-rdm kafka_service.go：ListGroups :316、DescribeGroup :686、
// kafkaFetchGroupOffsets :3588、DeleteGroup :764。
// host 补齐能力 kafka/groups/offsets/reset（tinyrdm 无）：基于
// kadm FetchOffsets + CommitOffsets 自实现（kadm v1.17 无内建 reset）。

import (
	"context"
	"sort"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ListGroups 实现 kafka/groups/list。
func (s *Service) ListGroups(ctx context.Context, connectionID string) (*GroupsListResult, error) {
	var result GroupsListResult
	err := s.withAdmin(connectionID, func(client *kgo.Client) error {
		admin := kadm.NewClient(client)
		ctx, cancel := context.WithTimeout(ctx, adminTimeout)
		defer cancel()

		groups, err := admin.ListGroups(ctx)
		if err != nil {
			return err
		}
		result.Groups = make([]GroupInfo, 0, len(groups))
		for _, group := range groups.Sorted() {
			result.Groups = append(result.Groups, GroupInfo{
				Group:        group.Group,
				State:        group.State,
				ProtocolType: group.ProtocolType,
				Coordinator:  group.Coordinator,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// DescribeGroup 实现 kafka/groups/describe。
func (s *Service) DescribeGroup(ctx context.Context, req GroupsDescribeRequest) (*GroupDescribeResult, error) {
	group := trimSpace(req.Group)
	if group == "" {
		return nil, errf("group is required")
	}
	var result GroupDescribeResult
	err := s.withAdmin(req.ConnectionID, func(client *kgo.Client) error {
		admin := kadm.NewClient(client)
		ctx, cancel := context.WithTimeout(ctx, adminTimeout)
		defer cancel()

		described, err := admin.DescribeGroups(ctx, group)
		if err != nil {
			return err
		}
		describedGroup, ok := described[group]
		if !ok {
			return errf("group %q not found", group)
		}
		result = groupDetail(describedGroup)
		if describedGroup.Err != nil {
			return describedGroup.Err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// GetGroupOffsets 实现 kafka/groups/offsets/list（topics 空 = committed 全量；
// Option 语义：无 committed 数据 → hasCommitted=false，与零 lag 区分）。
func (s *Service) GetGroupOffsets(ctx context.Context, req GroupOffsetsListRequest) (*GroupOffsetsListResult, error) {
	group := trimSpace(req.Group)
	if group == "" {
		return nil, errf("groupId is required")
	}
	topics := normalizeTopicNames(req.Topics)

	var result GroupOffsetsListResult
	err := s.withAdmin(req.ConnectionID, func(client *kgo.Client) error {
		admin := kadm.NewClient(client)
		ctx, cancel := context.WithTimeout(ctx, adminTimeout)
		defer cancel()

		// committed offsets（topics 指定时过滤到指定 topic）。
		var committed kadm.OffsetResponses
		var commitErr error
		if len(topics) > 0 {
			committed, commitErr = admin.FetchOffsetsForTopics(ctx, group, topics...)
		} else {
			committed, commitErr = admin.FetchOffsets(ctx, group)
		}
		if commitErr != nil {
			return commitErr
		}
		// Option 语义：committed 响应全空 = 组从未提交过。
		result.HasCommitted = len(committed) > 0

		// committed 涉及的 topic 全集（空 committed 时回退请求 topics）。
		scope := committedTopics(committed, topics)
		if len(scope) == 0 {
			return nil
		}

		startOffsets, err := admin.ListStartOffsets(ctx, scope...)
		if err != nil {
			return err
		}
		endOffsets, err := admin.ListEndOffsets(ctx, scope...)
		if err != nil {
			return err
		}

		result.Rows = groupOffsetRows(startOffsets, endOffsets, committed)
		for _, row := range result.Rows {
			result.TotalLag += row.Lag
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// DeleteGroup 实现 kafka/groups/delete（critical 门禁：allow_delete 与门 +
// confirmGroup 与 topics/delete 的 confirmTopic 同级——审查 L4 补齐）。
func (s *Service) DeleteGroup(ctx context.Context, req GroupDeleteRequest) error {
	profile := s.profileOf(req.ConnectionID)
	group := trimSpace(req.Group)
	if group == "" {
		return errf("group is required")
	}
	if err := ensureDeleteAllowed(profile, "groups/delete"); err != nil {
		s.emitAudit(req.ConnectionID, "groups-delete", group, "blocked", err.Error())
		return err
	}
	if err := ensureGroupDeleteConfirm(req); err != nil {
		s.emitAudit(req.ConnectionID, "groups-delete", group, "blocked", err.Error())
		return err
	}

	err := s.withAdmin(req.ConnectionID, func(client *kgo.Client) error {
		admin := kadm.NewClient(client)
		ctx, cancel := context.WithTimeout(ctx, adminTimeout)
		defer cancel()

		responses, err := admin.DeleteGroups(ctx, group)
		if err != nil {
			return err
		}
		for _, response := range responses.Sorted() {
			if response.Err != nil {
				return response.Err
			}
		}
		return nil
	})
	if err != nil {
		s.emitAudit(req.ConnectionID, "groups-delete", group, "error", err.Error())
		return err
	}
	s.emitAudit(req.ConnectionID, "groups-delete", group, "success", "")
	return nil
}

// ResetGroupOffsets 实现 kafka/groups/offsets/reset（host 补齐能力；
// resetTo: earliest/latest/timestamp/partitionOffset）。
// 只读策略下拒绝（offset 重置是写操作）。
func (s *Service) ResetGroupOffsets(ctx context.Context, req GroupOffsetResetRequest) (*OffsetResetResult, error) {
	profile := s.profileOf(req.ConnectionID)
	group := trimSpace(req.Group)
	if group == "" {
		return nil, errf("group is required")
	}
	if err := ensureWriteAllowed(profile, "groups/offsets/reset"); err != nil {
		s.emitAuditSource(req.Source, req.ConnectionID, "group-offsets-reset", group, "blocked", err.Error())
		return nil, err
	}

	mode, err := normalizeResetMode(req.ResetTo)
	if err != nil {
		return nil, err
	}
	topics := normalizeTopicNames(req.Topics)
	if mode != OffsetResetPartitionOffsets && len(topics) == 0 {
		return nil, errf("topics is required for %s reset", mode)
	}
	if mode == OffsetResetTimestamp && req.TimestampMs < 0 {
		return nil, errf("timestampMs must be greater than or equal to 0")
	}

	var result OffsetResetResult
	err = s.withAdmin(req.ConnectionID, func(client *kgo.Client) error {
		admin := kadm.NewClient(client)
		ctx, cancel := context.WithTimeout(ctx, adminTimeout)
		defer cancel()

		// 目标 offsets：先列出再提交。
		var targets kadm.Offsets
		switch mode {
		case OffsetResetEarliest:
			listed, listErr := admin.ListStartOffsets(ctx, topics...)
			if listErr != nil {
				return listErr
			}
			targets = listed.Offsets()
		case OffsetResetLatest:
			listed, listErr := admin.ListEndOffsets(ctx, topics...)
			if listErr != nil {
				return listErr
			}
			targets = listed.Offsets()
		case OffsetResetTimestamp:
			listed, listErr := admin.ListOffsetsAfterMilli(ctx, req.TimestampMs, topics...)
			if listErr != nil {
				return listErr
			}
			targets = listed.Offsets()
		case OffsetResetPartitionOffsets:
			targets = kadm.Offsets{}
			if len(req.PartitionOffsets) == 0 {
				return errf("partitionOffsets is required for partitionOffset reset")
			}
			for topic, partitions := range req.PartitionOffsets {
				for partition, offset := range partitions {
					if partition < 0 {
						return errf("partition must be greater than or equal to 0")
					}
					if offset < 0 {
						return errf("offset for %s/%d must be greater than or equal to 0", topic, partition)
					}
					targets.AddOffset(topic, partition, offset, -1)
				}
			}
		}

		// CommitOffsets 只在传输层失败时返回 error；broker 的逐分区错误码
		// （UNKNOWN_TOPIC_ID / UNKNOWN_MEMBER_ID / COORDINATOR_LOAD_IN_PROGRESS
		// 等）落在 responses 里。只看外层 error 会把被拒绝的提交误报为
		// ok=true，调用方随后读到未提交的 offset（K15 回归面）。
		commits, commitErr := admin.CommitOffsets(ctx, group, targets)
		rows, commitFailure := offsetResetRows(targets, commits, commitErr)
		result.Rows = rows
		if commitFailure != nil {
			return commitFailure
		}
		return nil
	})
	if err != nil {
		s.emitAuditSource(req.Source, req.ConnectionID, "group-offsets-reset", group, "error", err.Error())
		return nil, err
	}
	s.emitAuditSource(req.Source, req.ConnectionID, "group-offsets-reset", group, "success", sprintf("resetTo=%s", mode))
	return &result, nil
}

// offsetResetRows 把一次提交的目标 offset 与 broker 响应合并成逐分区结果。
// 任一分区被拒绝即返回错误（reset 未落盘绝不能报告成功）；错误信息点名
// topic/partition 与 broker 错误码，便于定位。
func offsetResetRows(targets kadm.Offsets, commits kadm.OffsetResponses, commitErr error) ([]OffsetResetRow, error) {
	rows := []OffsetResetRow{}
	var failure error
	targets.Each(func(offset kadm.Offset) {
		row := OffsetResetRow{Topic: offset.Topic, Partition: offset.Partition, OK: true}
		switch {
		case commitErr != nil:
			row.OK = false
			row.Error = commitErr.Error()
			if failure == nil {
				failure = commitErr
			}
		default:
			// 响应缺失同样按失败处理：无法确认落盘的 reset 不得报成功
			// （kadm 对缺失分区填 errOffsetCommitMissing，这里再兜一层）。
			response, ok := commits.Lookup(offset.Topic, offset.Partition)
			switch {
			case !ok:
				row.OK = false
				row.Error = "partition missing from commit response"
				if failure == nil {
					failure = errf("commit offset for %s/%d was not acknowledged by the broker", offset.Topic, offset.Partition)
				}
			case response.Err != nil:
				row.OK = false
				row.Error = response.Err.Error()
				if failure == nil {
					failure = errf("commit offset for %s/%d was rejected: %v", offset.Topic, offset.Partition, response.Err)
				}
			}
		}
		rows = append(rows, row)
	})
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Topic != rows[j].Topic {
			return rows[i].Topic < rows[j].Topic
		}
		return rows[i].Partition < rows[j].Partition
	})
	return rows, failure
}

// normalizeResetMode 归一化 resetTo。
func normalizeResetMode(value string) (OffsetResetMode, error) {
	switch strings.ToLower(trimSpace(value)) {
	case "earliest":
		return OffsetResetEarliest, nil
	case "latest":
		return OffsetResetLatest, nil
	case "timestamp":
		return OffsetResetTimestamp, nil
	case "partitionoffset", "partition_offset", "partitionoffsets":
		return OffsetResetPartitionOffsets, nil
	default:
		return "", errf("resetTo must be earliest, latest, timestamp, or partitionOffset")
	}
}

// committedTopics 汇总 committed offsets 涉及的 topic（带请求过滤）。
func committedTopics(committed kadm.OffsetResponses, topics []string) []string {
	filter := map[string]struct{}{}
	for _, topic := range topics {
		filter[topic] = struct{}{}
	}
	seen := map[string]struct{}{}
	out := []string{}
	committed.Each(func(response kadm.OffsetResponse) {
		if _, ok := seen[response.Topic]; ok {
			return
		}
		if len(filter) > 0 {
			if _, ok := filter[response.Topic]; !ok {
				return
			}
		}
		seen[response.Topic] = struct{}{}
		out = append(out, response.Topic)
	})
	sort.Strings(out)
	return out
}

// groupOffsetRows 合并 start/end/committed 三张表（tinyrdm
// kafkaGroupOffsetsSummary :3647 收敛重写）。
func groupOffsetRows(startOffsets, endOffsets kadm.ListedOffsets, committed kadm.OffsetResponses) []GroupOffsetRow {
	// 收集 (topic, partition) 全集。
	type key struct {
		topic     string
		partition int32
	}
	keys := []key{}
	seen := map[key]struct{}{}
	add := func(topic string, partition int32) {
		k := key{topic, partition}
		if _, ok := seen[k]; ok {
			return
		}
		seen[k] = struct{}{}
		keys = append(keys, k)
	}
	startOffsets.Each(func(item kadm.ListedOffset) { add(item.Topic, item.Partition) })
	endOffsets.Each(func(item kadm.ListedOffset) { add(item.Topic, item.Partition) })
	committed.Each(func(response kadm.OffsetResponse) { add(response.Topic, response.Partition) })

	rows := make([]GroupOffsetRow, 0, len(keys))
	for _, k := range keys {
		row := GroupOffsetRow{Topic: k.topic, Partition: k.partition}
		if item, ok := startOffsets.Lookup(k.topic, k.partition); ok {
			row.StartOffset = max64(item.Offset, 0)
			if item.Err != nil {
				row.Error = item.Err.Error()
			}
		}
		if item, ok := endOffsets.Lookup(k.topic, k.partition); ok {
			row.EndOffset = max64(item.Offset, 0)
			if item.Err != nil && row.Error == "" {
				row.Error = item.Err.Error()
			}
		}
		if item, ok := committed.Lookup(k.topic, k.partition); ok {
			row.CommittedOffset = max64(item.At, -1)
			if item.Err != nil && row.Error == "" {
				row.Error = item.Err.Error()
			}
		}
		row.Lag = groupLag(row.EndOffset, row.CommittedOffset)
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Topic != rows[j].Topic {
			return rows[i].Topic < rows[j].Topic
		}
		return rows[i].Partition < rows[j].Partition
	})
	return rows
}

// groupLag lag = end - committed（未提交 committed=-1 时视作未消费全量）。
func groupLag(end, committed int64) int64 {
	if committed < 0 {
		return end
	}
	if end <= committed {
		return 0
	}
	return end - committed
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// groupDetail 映射组详情（tinyrdm kafkaGroupDetail :3829 同构）。
func groupDetail(group kadm.DescribedGroup) GroupDescribeResult {
	result := GroupDescribeResult{
		Group:        group.Group,
		State:        group.State,
		ProtocolType: group.ProtocolType,
		Protocol:     group.Protocol,
		Coordinator:  group.Coordinator.NodeID,
		Members:      make([]GroupMemberInfo, 0, len(group.Members)),
	}
	if group.Err != nil {
		result.Error = group.Err.Error()
	}
	for _, member := range group.Members {
		item := GroupMemberInfo{
			MemberID:   member.MemberID,
			ClientID:   member.ClientID,
			ClientHost: member.ClientHost,
		}
		if member.InstanceID != nil {
			item.InstanceID = *member.InstanceID
		}
		if assignment, ok := member.Assigned.AsConsumer(); ok {
			item.Assignments = make(map[string][]int32, len(assignment.Topics))
			for _, topic := range assignment.Topics {
				item.Assignments[topic.Topic] = append([]int32(nil), topic.Partitions...)
			}
		}
		result.Members = append(result.Members, item)
	}
	return result
}
