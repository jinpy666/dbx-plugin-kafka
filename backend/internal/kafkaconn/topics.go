package kafkaconn

// topics.go：broker/topic 管理面方法（契约 §5.2）。
// 逻辑对照 tiny-rdm kafka_service.go：ListBrokers :251、DescribeBrokerConfig
// :351、ListTopics :290、CreateTopics :462、DeleteTopics :514、
// CreatePartitions :550、GetTopicOffsets :597、DescribeTopicConfig :729、
// AlterTopicConfig :419、DescribeTopic（describe 主机补齐：分区健康视图）。

import (
	"context"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// adminTimeout 领域管理面统一超时（tinyrdm 15-20s 取中）。
const adminTimeout = 20 * time.Second

// ListBrokers 实现 kafka/brokers/list。connection_source=zookeeper 时经 ZK
// 发现 broker（ZK 不可达 → 业务错 -32000）；bootstrap 源走 Kafka metadata。
func (s *Service) ListBrokers(ctx context.Context, connectionID string) (*BrokersListResult, error) {
	profile := s.profileOf(connectionID)
	if profile.ConnectionSource == ConnectionSourceZookeeper {
		brokers, err := s.discoverBrokersViaZK(ctx, connectionID)
		if err != nil {
			return nil, err
		}
		return &BrokersListResult{Brokers: brokers, ConnectionSource: ConnectionSourceZookeeper}, nil
	}

	var result BrokersListResult
	err := s.withAdmin(connectionID, func(client *kgo.Client) error {
		admin := kadm.NewClient(client)
		ctx, cancel := context.WithTimeout(ctx, adminTimeout)
		defer cancel()

		brokers, err := admin.ListBrokers(ctx)
		if err != nil {
			return err
		}
		result.Brokers = make([]BrokerInfo, 0, len(brokers))
		for _, broker := range brokers {
			result.Brokers = append(result.Brokers, BrokerInfo{
				NodeID: broker.NodeID,
				Host:   broker.Host,
				Port:   broker.Port,
				Rack:   firstNonEmpty(deref(broker.Rack), ""),
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	result.ConnectionSource = ConnectionSourceBootstrap
	return &result, nil
}

// DescribeBrokerConfig 实现 kafka/brokers/config。
func (s *Service) DescribeBrokerConfig(ctx context.Context, req BrokerConfigRequest) (*ConfigEntriesResult, error) {
	var result ConfigEntriesResult
	err := s.withAdmin(req.ConnectionID, func(client *kgo.Client) error {
		admin := kadm.NewClient(client)
		ctx, cancel := context.WithTimeout(ctx, adminTimeout)
		defer cancel()

		configs, err := admin.DescribeBrokerConfigs(ctx, req.BrokerID)
		if err != nil {
			return err
		}
		result.Entries = []ConfigEntry{}
		if len(configs) > 0 {
			result.Entries = configEntries(configs[0].Configs)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// ListTopics 实现 kafka/topics/list（includeInternal 控制 internal topic）。
func (s *Service) ListTopics(ctx context.Context, req TopicsListRequest) (*TopicsListResult, error) {
	var result TopicsListResult
	err := s.withAdmin(req.ConnectionID, func(client *kgo.Client) error {
		admin := kadm.NewClient(client)
		ctx, cancel := context.WithTimeout(ctx, adminTimeout)
		defer cancel()

		var topics kadm.TopicDetails
		var err error
		if req.IncludeInternal {
			topics, err = admin.ListTopicsWithInternal(ctx)
		} else {
			topics, err = admin.ListTopics(ctx)
		}
		if err != nil {
			return err
		}
		result.Topics = topicInfos(topics)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// DescribeTopic 实现 kafka/topics/describe（分区健康视图：isHealthy = ISR
// 覆盖全部 replicas 且无 offline，host 补齐能力，IMPL_PLAN §0.1）。
func (s *Service) DescribeTopic(ctx context.Context, req TopicsDescribeRequest) (*TopicDescribeResult, error) {
	topic := trimSpace(req.Topic)
	if topic == "" {
		return nil, errf("topic is required")
	}
	var result TopicDescribeResult
	err := s.withAdmin(req.ConnectionID, func(client *kgo.Client) error {
		admin := kadm.NewClient(client)
		ctx, cancel := context.WithTimeout(ctx, adminTimeout)
		defer cancel()

		details, err := admin.ListTopics(ctx, topic)
		if err != nil {
			return err
		}
		detail, ok := details[topic]
		if !ok {
			return errf("topic %q not found", topic)
		}
		if detail.Err != nil {
			return detail.Err
		}
		result.Topic = topic
		result.Partitions = partitionInfos(detail)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// CreateTopics 实现 kafka/topics/create（写门禁 + 每条 results）。
func (s *Service) CreateTopics(ctx context.Context, req TopicsCreateRequest) ([]MutationResult, error) {
	if err := ensureWriteAllowed(s.profileOf(req.ConnectionID), "topics/create"); err != nil {
		s.emitAudit(req.ConnectionID, "topics-create", joinNames(req.Topics), "blocked", err.Error())
		return nil, err
	}
	topics := normalizeTopicNames(req.Topics)
	if len(topics) == 0 {
		return nil, errf("topics is required")
	}
	if req.Partitions <= 0 {
		return nil, errf("partitions must be greater than 0")
	}
	if req.ReplicationFactor <= 0 {
		return nil, errf("replicationFactor must be greater than 0")
	}
	createConfigs, err := createTopicConfigsFromMap(req.Config)
	if err != nil {
		return nil, err
	}

	var results []MutationResult
	err = s.withAdmin(req.ConnectionID, func(client *kgo.Client) error {
		admin := kadm.NewClient(client)
		ctx, cancel := context.WithTimeout(ctx, adminTimeout)
		defer cancel()

		responses, err := admin.CreateTopics(ctx, req.Partitions, req.ReplicationFactor, createConfigs, topics...)
		if err != nil {
			return err
		}
		results = mutationResultsTopic(responses)
		return nil
	})
	if err != nil {
		s.emitAudit(req.ConnectionID, "topics-create", joinNames(topics), "error", err.Error())
		return nil, err
	}
	s.emitAudit(req.ConnectionID, "topics-create", joinNames(topics), "success",
		sprintf("partitions=%d replicationFactor=%d", req.Partitions, req.ReplicationFactor))
	return results, nil
}

// DeleteTopics 实现 kafka/topics/delete（critical：allow_delete + confirmTopic）。
func (s *Service) DeleteTopics(ctx context.Context, req TopicsDeleteRequest) ([]MutationResult, error) {
	profile := s.profileOf(req.ConnectionID)
	if err := ensureDeleteAllowed(profile, "topics/delete"); err != nil {
		s.emitAuditSource(req.Source, req.ConnectionID, "topics-delete", joinNames(req.Topics), "blocked", err.Error())
		return nil, err
	}
	if err := ensureTopicDeleteConfirm(req); err != nil {
		s.emitAuditSource(req.Source, req.ConnectionID, "topics-delete", joinNames(req.Topics), "blocked", err.Error())
		return nil, err
	}
	topics := normalizeTopicNames(req.Topics)

	var results []MutationResult
	err := s.withAdmin(req.ConnectionID, func(client *kgo.Client) error {
		admin := kadm.NewClient(client)
		ctx, cancel := context.WithTimeout(ctx, adminTimeout)
		defer cancel()

		responses, err := admin.DeleteTopics(ctx, topics...)
		if err != nil {
			return err
		}
		results = mutationResultsTopicDelete(responses)
		return nil
	})
	if err != nil {
		s.emitAuditSource(req.Source, req.ConnectionID, "topics-delete", joinNames(topics), "error", err.Error())
		return nil, err
	}
	s.emitAuditSource(req.Source, req.ConnectionID, "topics-delete", joinNames(topics), "success", "")
	return results, nil
}

// ClearTopicRecords 实现 kafka/topics/records/clear（Phase 3 §12.2.1，
// critical：read_only/allow_delete 与门 + confirmTopic 与 topics/delete 同级）。
// 实现：ListEndOffsets 取各分区 high watermark → DeleteRecords(offset=hw)
// （KIP-107；broker <0.11 不支持时分区行内业务错透传，最低 broker 版本见
// 协议文档）。deleted = 删除前 hw − 删除后 lowWatermark（任一段取不到 →
// 行内 null，不影响其他分区）。
func (s *Service) ClearTopicRecords(ctx context.Context, req TopicRecordsClearRequest) (*TopicRecordsClearResult, error) {
	topic := trimSpace(req.Topic)
	if err := ensureDeleteAllowed(s.profileOf(req.ConnectionID), "topics/records/clear"); err != nil {
		s.emitAuditSource(req.Source, req.ConnectionID, "topics.records.clear", topic, "blocked", err.Error())
		return nil, err
	}
	if topic == "" {
		return nil, errf("topic is required")
	}
	if err := ensureTopicDeleteConfirm(TopicsDeleteRequest{Topics: []string{topic}, ConfirmTopic: req.ConfirmTopic}); err != nil {
		// §12.2.1 冻结契约：confirmTopic 不匹配 → -32602 参数错。
		s.emitAuditSource(req.Source, req.ConnectionID, "topics.records.clear", topic, "blocked", err.Error())
		return nil, &InvalidParamsError{Msg: err.Error()}
	}

	var result TopicRecordsClearResult
	err := s.withAdmin(req.ConnectionID, func(client *kgo.Client) error {
		admin := kadm.NewClient(client)
		cctx, cancel := context.WithTimeout(ctx, adminTimeout)
		defer cancel()

		// 1) 删除前：各分区 high watermark（要清空的右开边界）。
		highs, err := admin.ListEndOffsets(cctx, topic)
		if err != nil {
			return err
		}
		deleteOffsets := kadm.Offsets{}
		before := map[int32]int64{}
		beforeErr := map[int32]error{}
		for _, item := range highs[topic] {
			if item.Err != nil {
				beforeErr[item.Partition] = item.Err
				continue
			}
			at := item.Offset
			if at < 0 {
				at = 0
			}
			before[item.Partition] = at
			deleteOffsets.Add(kadm.Offset{Topic: topic, Partition: item.Partition, At: at})
		}
		// 2) 删除：offset=hw（清空全部已可见记录；不删除未来写入）。
		deleted, err := admin.DeleteRecords(cctx, deleteOffsets)
		if err != nil {
			return err
		}
		// 3) 行合并（纯函数，rows 形状/long|null 单测覆盖）。
		result.Rows = clearRecordsRows(topic, before, beforeErr, deleted)
		return nil
	})
	if err != nil {
		s.emitAuditSource(req.Source, req.ConnectionID, "topics.records.clear", topic, "error", err.Error())
		return nil, err
	}
	s.emitAuditSource(req.Source, req.ConnectionID, "topics.records.clear", topic, "success", sprintf("partitions=%d", len(result.Rows)))
	return &result, nil
}

// clearRecordsRows 合并删除前 hw 快照与 DeleteRecords 响应为响应行
// （纯函数；分区升序；任一段 offset 取不到 → Deleted/LowWatermark=null，
// 行内 error 不影响其他分区）。
func clearRecordsRows(topic string, before map[int32]int64, beforeErr map[int32]error, deleted kadm.DeleteRecordsResponses) []TopicRecordsClearRow {
	partitions := make([]int32, 0, len(before)+len(beforeErr))
	for p := range before {
		partitions = append(partitions, p)
	}
	for p := range beforeErr {
		partitions = append(partitions, p)
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })

	rows := make([]TopicRecordsClearRow, 0, len(partitions))
	for _, p := range partitions {
		row := TopicRecordsClearRow{Partition: p}
		if err := beforeErr[p]; err != nil {
			row.Error = err.Error()
			rows = append(rows, row)
			continue
		}
		resp, ok := deleted.Lookup(topic, p)
		if !ok {
			row.Error = "no DeleteRecords response for partition"
			rows = append(rows, row)
			continue
		}
		if resp.Err != nil {
			row.Error = resp.Err.Error()
			rows = append(rows, row)
			continue
		}
		low := resp.LowWatermark
		deletedCount := max64(before[p]-low, 0) // 并发 DeleteRecords 下 low 可能越过 before（评审 L-6）
		row.LowWatermark = &low
		row.Deleted = &deletedCount
		row.OK = true
		rows = append(rows, row)
	}
	return rows
}

// UpdatePartitions 实现 kafka/topics/partitions/update（只增）。
func (s *Service) UpdatePartitions(ctx context.Context, req PartitionsUpdateRequest) ([]MutationResult, error) {
	if err := ensureWriteAllowed(s.profileOf(req.ConnectionID), "topics/partitions/update"); err != nil {
		s.emitAudit(req.ConnectionID, "partitions-update", joinMapKeys(req.Partitions), "blocked", err.Error())
		return nil, err
	}
	byCount, err := normalizePartitionUpdates(req.Partitions)
	if err != nil {
		return nil, err
	}

	var results []MutationResult
	err = s.withAdmin(req.ConnectionID, func(client *kgo.Client) error {
		admin := kadm.NewClient(client)
		ctx, cancel := context.WithTimeout(ctx, adminTimeout)
		defer cancel()

		results = []MutationResult{}
		counts := make([]int, 0, len(byCount))
		for count := range byCount {
			counts = append(counts, count)
		}
		sort.Ints(counts)
		for _, count := range counts {
			topics := byCount[count]
			responses, err := admin.UpdatePartitions(ctx, count, topics...)
			if err != nil {
				return err
			}
			results = append(results, mutationResultsCreatePartitions(responses)...)
		}
		sort.Slice(results, func(i, j int) bool { return results[i].Topic < results[j].Topic })
		return nil
	})
	if err != nil {
		s.emitAudit(req.ConnectionID, "partitions-update", joinMapKeys(req.Partitions), "error", err.Error())
		return nil, err
	}
	s.emitAudit(req.ConnectionID, "partitions-update", joinMapKeys(req.Partitions), "success", "")
	return results, nil
}

// GetTopicConfig 实现 kafka/topics/config/get。
func (s *Service) GetTopicConfig(ctx context.Context, req TopicConfigGetRequest) (*ConfigEntriesResult, error) {
	topic := trimSpace(req.Topic)
	if topic == "" {
		return nil, errf("topic is required")
	}
	var result ConfigEntriesResult
	err := s.withAdmin(req.ConnectionID, func(client *kgo.Client) error {
		admin := kadm.NewClient(client)
		ctx, cancel := context.WithTimeout(ctx, adminTimeout)
		defer cancel()

		configs, err := admin.DescribeTopicConfigs(ctx, topic)
		if err != nil {
			return err
		}
		result.Entries = []ConfigEntry{}
		if len(configs) > 0 {
			result.Entries = configEntries(configs[0].Configs)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// AlterTopicConfig 实现 kafka/topics/config/alter（写门禁）。
func (s *Service) AlterTopicConfig(ctx context.Context, req TopicConfigAlterRequest) ([]MutationResult, error) {
	if err := ensureWriteAllowed(s.profileOf(req.ConnectionID), "topics/config/alter"); err != nil {
		s.emitAudit(req.ConnectionID, "topic-config-alter", trimSpace(req.Topic), "blocked", err.Error())
		return nil, err
	}
	topic := trimSpace(req.Topic)
	if topic == "" {
		return nil, errf("topic is required")
	}
	alterConfigs, err := alterConfigsFromMap(req.Config, req.DeleteKeys)
	if err != nil {
		return nil, err
	}

	var results []MutationResult
	err = s.withAdmin(req.ConnectionID, func(client *kgo.Client) error {
		admin := kadm.NewClient(client)
		ctx, cancel := context.WithTimeout(ctx, adminTimeout)
		defer cancel()

		responses, err := admin.AlterTopicConfigs(ctx, alterConfigs, topic)
		if err != nil {
			return err
		}
		results = alterConfigResults(responses, topic)
		return nil
	})
	if err != nil {
		s.emitAudit(req.ConnectionID, "topic-config-alter", topic, "error", err.Error())
		return nil, err
	}
	s.emitAudit(req.ConnectionID, "topic-config-alter", topic, "success", "")
	return results, nil
}

// ListTopicOffsets 实现 kafka/topics/offsets/list（Phase 2 全策略：
// earliest(-2)/latest(-1)/max-timestamp(-3)/log-start(-4)/RFC3339/unix ms；
// 单分区失败在行上标 error，不整体失败）。
func (s *Service) ListTopicOffsets(ctx context.Context, req TopicOffsetsListRequest) (*TopicOffsetsListResult, error) {
	topics := normalizeTopicNames(req.Topics)
	if len(topics) == 0 {
		return nil, errf("topics is required")
	}
	mode, millis, err := parseOffsetTime(req.OffsetTime)
	if err != nil {
		return nil, err
	}

	var result TopicOffsetsListResult
	err = s.withAdmin(req.ConnectionID, func(client *kgo.Client) error {
		admin := kadm.NewClient(client)
		ctx, cancel := context.WithTimeout(ctx, adminTimeout)
		defer cancel()

		var offsets kadm.ListedOffsets
		switch mode {
		case "earliest":
			offsets, err = admin.ListStartOffsets(ctx, topics...)
		case "latest":
			offsets, err = admin.ListEndOffsets(ctx, topics...)
		case "max-timestamp":
			offsets, err = admin.ListMaxTimestampOffsets(ctx, topics...)
		case "log-start":
			offsets, err = admin.ListLocalLogStartOffsets(ctx, topics...)
		default:
			offsets, err = admin.ListOffsetsAfterMilli(ctx, millis, topics...)
		}
		if err != nil {
			return err
		}
		result.Rows = listedOffsetRows(offsets)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// --- 映射辅助（tinyrdm kafkaTopicInfos :1825 / kafkaConfigEntries :1864 /
// kafkaTopicOffsetInfos :3541 收敛重写） ---

func topicInfos(details kadm.TopicDetails) []TopicInfo {
	topics := details.Sorted()
	result := make([]TopicInfo, 0, len(topics))
	for _, topic := range topics {
		// 健康聚合（§12.2.4）：分区元数据已在 ListTopics 加载，零额外请求；
		// 判定规则复用 partitionInfos 同款（partitionIsHealthy）。
		unhealthy := 0
		for _, partition := range topic.Partitions {
			if !partitionIsHealthy(partition.Leader, partition.ISR, partition.Replicas, partition.OfflineReplicas, partition.Err) {
				unhealthy++
			}
		}
		item := TopicInfo{
			TopicID:             topic.ID.String(),
			Name:                topic.Topic,
			IsInternal:          topic.IsInternal,
			PartitionCount:      len(topic.Partitions),
			ReplicationFactor:   topic.Partitions.NumReplicas(),
			IsHealthy:           topic.Err == nil && unhealthy == 0,
			UnhealthyPartitions: unhealthy,
		}
		if topic.Err != nil {
			item.Error = topic.Err.Error()
		}
		result = append(result, item)
	}
	return result
}

// partitionIsHealthy 单分区健康判定：leader 有效 + ISR 覆盖全部 replicas +
// 无 offline 副本（partitionInfos 与 topicInfos 健康聚合共用）。
func partitionIsHealthy(leader int32, isr, replicas, offline []int32, err error) bool {
	return err == nil &&
		leader >= 0 &&
		len(offline) == 0 &&
		len(isr) == len(replicas)
}

func partitionInfos(detail kadm.TopicDetail) []PartitionInfo {
	partitions := make([]PartitionInfo, 0, len(detail.Partitions))
	for _, partition := range detail.Partitions.Sorted() {
		item := PartitionInfo{
			Partition:       partition.Partition,
			Leader:          partition.Leader,
			LeaderEpoch:     partition.LeaderEpoch,
			Replicas:        append([]int32(nil), partition.Replicas...),
			ISR:             append([]int32(nil), partition.ISR...),
			OfflineReplicas: append([]int32(nil), partition.OfflineReplicas...),
		}
		if partition.Err != nil {
			item.Error = partition.Err.Error()
		}
		// 健康判定：leader 有效 + ISR 覆盖全部 replicas + 无 offline 副本。
		item.IsHealthy = partitionIsHealthy(partition.Leader, partition.ISR, partition.Replicas, partition.OfflineReplicas, partition.Err)
		partitions = append(partitions, item)
	}
	return partitions
}

func configEntries(configs []kadm.Config) []ConfigEntry {
	entries := make([]ConfigEntry, 0, len(configs))
	for _, config := range configs {
		entry := ConfigEntry{
			Name:      config.Key,
			Value:     config.MaybeValue(),
			Sensitive: config.Sensitive,
			Source:    config.Source.String(),
		}
		if entry.Source == "DEFAULT_CONFIG" || entry.Source == "INHERITED" {
			entry.IsDefault = true
		}
		entries = append(entries, entry)
	}
	return entries
}

func listedOffsetRows(offsets kadm.ListedOffsets) []TopicOffsetRow {
	rows := make([]TopicOffsetRow, 0, len(offsets))
	offsets.Each(func(item kadm.ListedOffset) {
		row := TopicOffsetRow{
			Topic:       item.Topic,
			Partition:   item.Partition,
			Offset:      item.Offset,
			Timestamp:   item.Timestamp,
			LeaderEpoch: item.LeaderEpoch,
		}
		if row.Offset < 0 {
			row.Offset = 0
		}
		if row.Timestamp < 0 {
			row.Timestamp = 0
		}
		if row.LeaderEpoch < 0 {
			row.LeaderEpoch = 0
		}
		if item.Err != nil {
			row.Error = item.Err.Error()
		}
		rows = append(rows, row)
	})
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Topic != rows[j].Topic {
			return rows[i].Topic < rows[j].Topic
		}
		return rows[i].Partition < rows[j].Partition
	})
	return rows
}

// deref 安全解引用字符串指针。
func deref(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
