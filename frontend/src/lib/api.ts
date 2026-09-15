/**
 * Sidecar call wrapper for the Kafka workbench.
 *
 * All `kafka/*` domain methods take only `connectionId` (the workbench never
 * receives credentials — the host drives `connection/test|connect|disconnect`
 * itself). The current connection id comes from the host context delivered
 * over `window.dbxPlugin`. Errors surface through `showError(cause)`.
 * 方法契约：IMPL_PLAN_DBX_KAFKA §5（新方法必须同步 PROTOCOL_KAFKA 文档）。
 */

export type OffsetStrategy = "latest" | "earliest" | "committed" | "timestamp" | "offset";
export type IsolationLevel = "read_uncommitted" | "read_committed";
export type MatchMode = "contains" | "prefix" | "exact" | "regex";
export type DecodeMode = "none" | "base64";
export type Decompression = "none" | "gzip" | "lz4" | "zstd" | "snappy";
export type Compression = "none" | "gzip" | "lz4" | "zstd" | "snappy";

export interface KafkaMessage {
  topic: string;
  partition: number;
  offset: number;
  /** unix ms（sidecar JSON number）。 */
  timestamp: number;
  leaderEpoch?: number;
  /** UTF-8 安全预览（非法字节已替换），可能与原始字节不一致。 */
  key?: string;
  /** 恒完整的 key base64（valueBase64 同理保真，见 §5.3 二进制保真约定）。 */
  keyBase64?: string;
  valueText?: string;
  valueBase64?: string;
  headers?: Record<string, string>;
  committed?: boolean;
  decodeError?: string;
  truncated?: boolean;
  /** SR 解码挂载信息（consume/stream 附带；produce 挂载后回填）。 */
  schemaId?: number;
  schemaSubject?: string;
  schemaVersion?: number;
}

/** Schema Registry 解码挂载（consume/stream/produce 共用；version 省略 = latest）。
 *  Phase 3 F1：format 枚举随后端扩展为 avro|json|protobuf（前端仅透传/提示，
 *  PROTOBUF 前端生成与树视图不做，见 §12.2.5/12.2.7/12.6）。 */
export type SchemaFormat = "avro" | "json" | "protobuf";

export interface SchemaAttach {
  subject: string;
  version?: number;
  format: SchemaFormat;
}

export interface FieldFilter {
  source: "value" | "key" | "header" | "topic" | "partition" | "offset" | "timestamp";
  path?: string;
  operator: "contains" | "prefix" | "exact" | "regex" | "exists" | "not_exists" | "gt" | "gte" | "lt" | "lte";
  value: string;
  /** 前端行开关；上线载荷中剥离（仅启用的行会携带）。 */
  enabled?: boolean;
}

export interface ConsumeParams {
  topic: string;
  groupId?: string;
  offsetStrategy: OffsetStrategy;
  offsetTime?: string | number;
  partitions?: number[];
  partitionOffsets?: Record<string, number>;
  limit?: number;
  timeoutMs?: number;
  maxScanRecords?: number;
  isolationLevel?: IsolationLevel;
  commit?: boolean;
  filter?: string;
  keyFilter?: string;
  valueFilter?: string;
  headerFilter?: string;
  matchMode?: MatchMode;
  fieldFilters?: FieldFilter[];
  timestampFrom?: number;
  timestampTo?: number;
  offsetFrom?: number;
  offsetTo?: number;
  decode?: DecodeMode;
  decompression?: Decompression;
  /** SR 解码挂载（Phase 2；与 decode 内层解码可叠加，后端先 SR 再内层）。 */
  schema?: SchemaAttach;
}

export interface ConsumeResult {
  messages: KafkaMessage[];
  scanned: number;
  matched: number;
  limited: boolean;
  hasMore: boolean;
  nextPartitionOffsets?: Record<string, number>;
}

export interface KafkaBroker {
  nodeId: number;
  host: string;
  port: number;
  rack?: string;
}

/** `kafka/brokers/list` 顶层返回；ZK 模式下带 `connectionSource=zookeeper`。 */
export interface BrokersListResult {
  brokers: KafkaBroker[];
  connectionSource?: "kafka" | "zookeeper" | string;
}

export interface ConfigEntry {
  name: string;
  value: string;
  source?: string;
  sensitive?: boolean;
  isDefault?: boolean;
}

export interface KafkaTopic {
  name: string;
  topicId?: string;
  isInternal?: boolean;
  partitionCount: number;
  replicationFactor: number;
  error?: string;
  /** F6-4 后端半件（Phase 3，旧 sidecar 可能缺省）：topic 级健康度与不健康分区数。 */
  isHealthy?: boolean;
  unhealthyPartitions?: number;
}

export interface TopicPartitionInfo {
  partition: number;
  leader: number;
  leaderEpoch?: number;
  replicas: number[];
  isr: number[];
  offlineReplicas: number[];
  isHealthy?: boolean;
}

export interface TopicOffsetRow {
  topic: string;
  partition: number;
  offset: number;
  timestamp?: number;
  leaderEpoch?: number;
}

export interface KafkaGroup {
  group: string;
  state?: string;
  protocolType?: string;
  coordinator?: number | string;
}

export interface GroupMember {
  memberId: string;
  instanceId?: string;
  clientId?: string;
  clientHost?: string;
  assignments?: Record<string, number[]>;
}

export interface GroupOffsetRow {
  topic: string;
  partition: number;
  startOffset?: number;
  endOffset?: number;
  committedOffset?: number | null;
  lag?: number;
  hasCommitted?: boolean;
}

export interface KafkaAcl {
  resourceType: string;
  resourceName: string;
  patternType?: string;
  principal: string;
  host?: string;
  operation: string;
  permission: string;
}

export interface AclFilter {
  resourceType?: string;
  resourceName?: string;
  patternType?: string;
  principal?: string;
  host?: string;
  operation?: string;
  permission?: string;
}

export interface ProduceResult {
  partition: number;
  offset: number;
  timestamp?: number;
}

export interface StreamStatus {
  paused?: boolean;
  totalScanned?: number;
  totalMatched?: number;
  bufferSize?: number;
  partitionOffsets?: Record<string, number>;
}

export interface KafkaPreset {
  id: string;
  name: string;
  /**
   * 消费预设：序列化的 ConsumeParams（不含 topic，应用时回填当前选中 topic）。
   * 监控预设：附加 `type:"monitor"` 与 `monitor` 载荷（Phase 2，同一 store 复用）。
   */
  params: ConsumeParams & {
    type?: "consume" | "monitor";
    monitor?: MonitorPresetParams;
  };
}

/** MonitorPanel 监控方案（走 kafka/presets/*，type=monitor）。 */
export interface MonitorPresetParams {
  group: string;
  topics: string[];
  intervalSec: number;
  threshold: number;
}

export interface KafkaConnectionStatus {
  connectionId: string;
  /** 后端 KafkaConnectionStatus.Status（契约三态：connected | idle | error）。 */
  status: "connected" | "idle" | "error";
  /** 策略层只读门禁（表单 read_only ∥ 宿主 read_only），旧 sidecar 可能缺省。 */
  readOnly?: boolean;
  /** 删除类操作门禁（allow_delete 表单；read_only 下后端强制无效）。旧 sidecar 可能缺省。 */
  allowDelete?: boolean;
  lastError?: string;
  /** unix 毫秒时间戳（sidecar JSON number）。 */
  lastUsedAt?: number;
  /** Schema Registry 摘要（Phase 2；旧 sidecar 可能缺省 = 未启用）。
   *  Phase P：provider 标注当前 SR 提供方（confluent|glue；缺省按 confluent 处理）。 */
  schemaRegistry?: { enabled: boolean; url?: string; provider?: SchemaRegistryProvider };
  /** Kerberos/GSSAPI 摘要（Phase 2）。 */
  kerberos?: { enabled: boolean };
  /** 连接元数据来源（ZK 模式 = zookeeper）。 */
  connectionSource?: string;
  /** 粘贴 properties 导入摘要（Lane 3；仅计数+键名，值不透出；旧 sidecar 缺省 = 未使用导入）。 */
  propertiesImport?: {
    mapped: number;
    mappedKeys?: string[];
    ignored: number;
    ignoredKeys?: string[];
  };
}

// -- Schema Registry（Phase 2 冻结契约，方法与形状见任务书） -------------------------

/** Schema Registry 提供方（Phase P：kafka/schema/* 全族可选 registry 参数）。 */
export type SchemaRegistryProvider = "confluent" | "glue";
/** schema/test 探测结果（none = 该 registry 未配置/不可达）。 */
export type SchemaRegistryTestProvider = SchemaRegistryProvider | "none";

export interface SchemaSubject {
  subject: string;
  formats: string[];
  latestVersion?: number;
  compatibilityLevel?: string;
}

export interface SchemaVersionRow {
  version: number;
  id: number;
  format: string;
}

export interface SchemaReference {
  name?: string;
  subject?: string;
  version?: number;
  [key: string]: unknown;
}

export interface SchemaDetail {
  subject: string;
  version: number;
  id: number;
  /** 原始 schema 文本（JSON 序列化）。 */
  schema: string;
  format: string;
  references: SchemaReference[];
}

export interface SchemaDiffHunk {
  op: "add" | "remove" | "modify";
  path: string;
  before?: string;
  after?: string;
}

export interface SchemaDiff {
  hunks: SchemaDiffHunk[];
  summary: string;
}

export interface SchemaCompatibility {
  level: string;
  scope?: string;
}

export interface SchemaCompatibilityCheckResult {
  isCompatible: boolean;
  messages: string[];
}

export interface SchemaRegisterResult {
  id: number;
  version: number;
}

export type SchemaCompatibilityLevel =
  | "NONE"
  | "BACKWARD"
  | "BACKWARD_TRANSITIVE"
  | "FORWARD"
  | "FORWARD_TRANSITIVE"
  | "FULL"
  | "FULL_TRANSITIVE"
  // AWS Glue 兼容性枚举（provider=glue；NONE/DISABLED 语义与 Confluent NONE 不同，
  // 枚举原样透传展示，见 IMPL_PLAN Phase P 冻结契约）。
  | "DISABLED"
  | "BACKWARD_ALL"
  | "FORWARD_ALL"
  | "FULL_ALL";

/** Current connection id, injected by App.vue once the host context resolves. */
let currentConnectionId = "";

export function setKafkaConnectionId(connectionId: string) {
  currentConnectionId = String(connectionId || "");
}

export function getKafkaConnectionId(): string {
  return currentConnectionId;
}

function requireConnectionId(): string {
  if (!currentConnectionId) {
    throw new Error("Kafka connection context is not ready (missing connectionId)");
  }
  return currentConnectionId;
}

// params 放宽为 object（接口类型无 index signature，调用面更贴合方法契约），
// 组装载荷时收窄为记录展开。
async function callKafka<T>(method: string, params: object = {}, options?: { timeoutMs?: number }): Promise<T> {
  const api = window.dbxPlugin;
  if (!api) throw new Error("DBX Host API unavailable");
  const invoke = (api.invoke ?? api.request).bind(api);
  return invoke<T>(method, { connectionId: requireConnectionId(), ...(params as Record<string, unknown>) }, options);
}

// -- domain methods (§5.2 of IMPL_PLAN_DBX_KAFKA) -----------------------------

export const kafkaApi = {
  // brokers
  brokersList() {
    return callKafka<BrokersListResult>("kafka/brokers/list");
  },
  brokersConfig(brokerId: number) {
    return callKafka<{ entries: ConfigEntry[] }>("kafka/brokers/config", { brokerId });
  },

  // topics
  topicsList(includeInternal = false) {
    return callKafka<{ topics: KafkaTopic[] }>("kafka/topics/list", includeInternal ? { includeInternal: true } : {});
  },
  topicsDescribe(topic: string) {
    return callKafka<{ partitions: TopicPartitionInfo[] }>("kafka/topics/describe", { topic });
  },
  topicsCreate(topics: string[], partitions: number, replicationFactor: number, config?: Record<string, string>) {
    return callKafka<{ results: Array<{ topic: string; ok: boolean; error?: string }> }>(
      "kafka/topics/create",
      { topics, partitions, replicationFactor, ...(config && Object.keys(config).length > 0 ? { config } : {}) },
    );
  },
  topicsDelete(topics: string[], confirmTopic: string) {
    return callKafka<{ results: Array<{ topic: string; ok: boolean; error?: string }> }>(
      "kafka/topics/delete",
      { topics, confirmTopic },
    );
  },
  topicsPartitionsUpdate(partitions: Record<string, number>) {
    return callKafka<{ results: Array<{ topic: string; ok: boolean; error?: string }> }>(
      "kafka/topics/partitions/update",
      { partitions },
    );
  },
  topicsConfigGet(topic: string) {
    return callKafka<{ entries: ConfigEntry[] }>("kafka/topics/config/get", { topic });
  },
  topicsConfigAlter(topic: string, config: Record<string, string>, deleteKeys: string[] = []) {
    return callKafka<{ entries: ConfigEntry[] }>("kafka/topics/config/alter", { topic, config, deleteKeys });
  },
  topicsOffsetsList(topics: string[], offsetTime?: string | number) {
    return callKafka<{ rows: TopicOffsetRow[] }>(
      "kafka/topics/offsets/list",
      offsetTime !== undefined ? { topics, offsetTime } : { topics },
    );
  },

  // groups
  groupsList() {
    return callKafka<{ groups: KafkaGroup[] }>("kafka/groups/list");
  },
  groupsDescribe(group: string) {
    return callKafka<{ members: GroupMember[] }>("kafka/groups/describe", { group });
  },
  groupsOffsetsList(group: string, topics?: string[]) {
    return callKafka<{ rows: GroupOffsetRow[]; totalLag?: number }>(
      "kafka/groups/offsets/list",
      topics?.length ? { group, topics } : { group },
    );
  },
  groupsDelete(group: string) {
    return callKafka<{ success: boolean }>("kafka/groups/delete", { group });
  },
  groupsOffsetsReset(
    group: string,
    topics: string[],
    resetTo: "earliest" | "latest" | "timestamp" | "partitionOffset",
    extra: { timestampMs?: number; partitionOffsets?: Record<string, Record<string, number>> } = {},
  ) {
    return callKafka<{ rows: Array<{ topic: string; partition: number; ok: boolean; error?: string }> }>(
      "kafka/groups/offsets/reset",
      { group, topics, resetTo, ...extra },
    );
  },

  // acls
  aclsList(filter: AclFilter) {
    return callKafka<{ acls: KafkaAcl[] }>("kafka/acls/list", { filter });
  },
  aclsCreate(acl: KafkaAcl) {
    return callKafka<{ success: boolean }>("kafka/acls/create", { acl });
  },
  aclsDelete(filter: AclFilter) {
    return callKafka<{ matched: number }>("kafka/acls/delete", { filter });
  },

  // messages
  messagesProduce(params: {
    topic: string;
    key?: string;
    value?: string;
    /** key 的 base64 保真载荷（keyBase64 与 key 二选一，Phase 2）。 */
    keyBase64?: string;
    /** value 的 base64 保真载荷（与 value 二选一，Phase 2）。 */
    valueBase64?: string;
    headers?: Record<string, string>;
    partition?: number;
    count?: number;
    compression?: Compression;
    schema?: SchemaAttach;
  }) {
    return callKafka<ProduceResult>("kafka/messages/produce", params);
  },
  messagesConsume(params: ConsumeParams, options?: { timeoutMs?: number }) {
    return callKafka<ConsumeResult>("kafka/messages/consume", params, options);
  },
  messagesExport(params: ConsumeParams & { format: "json" | "csv"; limit: number }) {
    return callKafka<{ content: string; filename: string; contentType: string }>("kafka/messages/export", params);
  },

  // stream
  streamStart(params: ConsumeParams) {
    return callKafka<{ sessionId: string }>("kafka/stream/start", params);
  },
  streamStop(sessionId?: string, all = false) {
    return callKafka<{ success: boolean }>("kafka/stream/stop", sessionId && !all ? { sessionId } : { all: true });
  },
  streamPause(sessionId: string) {
    return callKafka<{ status: StreamStatus }>("kafka/stream/pause", { sessionId });
  },
  streamResume(sessionId: string) {
    return callKafka<{ status: StreamStatus }>("kafka/stream/resume", { sessionId });
  },
  streamStatus(sessionId: string) {
    return callKafka<{ status: StreamStatus }>("kafka/stream/status", { sessionId });
  },
  streamMessages(sessionId: string, offset: number, limit: number) {
    return callKafka<{ messages: KafkaMessage[]; total?: number }>("kafka/stream/messages", { sessionId, offset, limit });
  },

  // presets + statuses（照 ldap/presets、ldap/connections/statuses 形态）
  presetsList() {
    return callKafka<{ presets: KafkaPreset[] }>("kafka/presets/list");
  },
  presetsSave(preset: KafkaPreset) {
    return callKafka<{ presets: KafkaPreset[] }>("kafka/presets/save", { preset });
  },
  presetsRemove(id: string) {
    return callKafka<{ presets: KafkaPreset[] }>("kafka/presets/remove", { id });
  },
  connectionStatuses() {
    return callKafka<{ statuses: KafkaConnectionStatus[] }>("kafka/connections/statuses");
  },

  // schema registry（Phase 2 冻结契约 + Phase P registry 参数；只读/写门禁与
  // delete 双门禁同 §6。registry 省略 = 连接默认提供方，与旧 sidecar 兼容）
  schemaTest(registry?: SchemaRegistryProvider) {
    return callKafka<{ success: boolean; provider?: SchemaRegistryTestProvider }>(
      "kafka/schema/test",
      registry ? { registry } : {},
    );
  },
  schemaSubjectsList(registry?: SchemaRegistryProvider) {
    return callKafka<{ subjects: SchemaSubject[] }>(
      "kafka/schema/subjects/list",
      registry ? { registry } : {},
    );
  },
  schemaVersionsList(subject: string, registry?: SchemaRegistryProvider) {
    return callKafka<{ versions: SchemaVersionRow[] }>(
      "kafka/schema/versions/list",
      registry ? { subject, registry } : { subject },
    );
  },
  schemaGet(subject: string, version?: number, registry?: SchemaRegistryProvider) {
    const base: Record<string, unknown> = version !== undefined ? { subject, version } : { subject };
    return callKafka<SchemaDetail>("kafka/schema/get", registry ? { ...base, registry } : base);
  },
  schemaVersionsCompare(subject: string, fromVersion: number, toVersion: number, registry?: SchemaRegistryProvider) {
    return callKafka<SchemaDiff>(
      "kafka/schema/versions/compare",
      registry ? { subject, fromVersion, toVersion, registry } : { subject, fromVersion, toVersion },
    );
  },
  schemaCompatibilityGet(subject?: string, registry?: SchemaRegistryProvider) {
    const base: Record<string, unknown> = subject ? { subject } : {};
    return callKafka<SchemaCompatibility>("kafka/schema/compatibility/get", registry ? { ...base, registry } : base);
  },
  schemaCompatibilitySet(subject: string | undefined, level: SchemaCompatibilityLevel, registry?: SchemaRegistryProvider) {
    const base: Record<string, unknown> = subject ? { subject, level } : { level };
    return callKafka<SchemaCompatibility>("kafka/schema/compatibility/set", registry ? { ...base, registry } : base);
  },
  schemaCompatibilityCheck(
    subject: string,
    format: "avro" | "json",
    schema: string,
    version?: number,
    registry?: SchemaRegistryProvider,
  ) {
    const base: Record<string, unknown> =
      version !== undefined ? { subject, format, schema, version } : { subject, format, schema };
    return callKafka<SchemaCompatibilityCheckResult>(
      "kafka/schema/compatibility/check",
      registry ? { ...base, registry } : base,
    );
  },
  schemaRegister(subject: string, format: SchemaFormat, schema: string, registry?: SchemaRegistryProvider) {
    return callKafka<SchemaRegisterResult>(
      "kafka/schema/register",
      registry ? { subject, format, schema, registry } : { subject, format, schema },
    );
  },
  schemaDelete(subject: string, registry?: SchemaRegistryProvider) {
    return callKafka<{ success: boolean }>(
      "kafka/schema/delete",
      registry ? { subject, registry } : { subject },
    );
  },
  schemaDeleteVersion(subject: string, version: number, registry?: SchemaRegistryProvider) {
    return callKafka<{ success: boolean }>(
      "kafka/schema/delete/version",
      registry ? { subject, version, registry } : { subject, version },
    );
  },
};

// -- events (§5.4) ------------------------------------------------------------

export interface KafkaStreamMessagesEvent {
  sessionId: string;
  messages: KafkaMessage[];
  totalScanned?: number;
  totalMatched?: number;
  paused?: boolean;
  /** 当前 sidecar ring buffer 内的消息数（前端 dropped 估算用）。 */
  bufferSize?: number;
}

export interface KafkaStreamErrorEvent {
  sessionId: string;
  error: string;
}

export interface KafkaAuditEvent {
  connectionId: string;
  action: string;
  target: string;
  result: "ok" | "denied" | "error";
}
