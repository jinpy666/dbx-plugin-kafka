/**
 * Visual fixture host bridge for the Kafka workbench (mock.html).
 *
 * Mirrors the ldap mockDbxHost pattern (which mirrors the real bridge shape):
 * in-memory cluster + `window.dbxPlugin` implementation so the workbench runs
 * in a plain browser (pnpm dev → /mock.html). URL params:
 *   ?theme=light|dark    appearance scheme (default dark)
 *   ?locale=zh-CN|en|…   workbench locale (default zh-CN)
 *   ?err=1               kafka/* domain calls always reject (error fixture)
 *   ?noconn=1            host context has no connectionId (init error fixture)
 *   ?ro=1                readOnly connection（写操作先发 denied kafka/audit 事件
 *                        再抛错，镜像后端 policy.go 分支）
 *   ?nodelete=1          allowDelete=false（删除类额外拒绝）
 *   ?nosave=1            模拟旧宿主（无 saveFile 桥）：导出走 Blob 网页下载兜底；
 *                        默认镜像桌面端 v0.6.14+ 的 host.saveFile 另存为桥
 *   ?glue=1              Glue-only 模式：statuses.provider=glue、仅 Glue subjects、
 *                        confluent 探测返回 none、schema 挂载 -32000 拒绝
 *                        （默认模式 = Confluent + Glue 双 registry 配置，可切换）
 *   ?msk=1               OAUTHBEARER/MSK IAM 模式（Phase 3 F2 前端半件）：连接摘要
 *                        携带 oauth_token_source=msk_iam + msk_region/msk_access_key_id，
 *                        connection_secrets 含 msk_secret_access_key/msk_session_token
 *                        （ConnectionsPanel 摘要徽标路径；照 ?glue=1 开关范式）
 *   ?big=1               大数据量压测 fixture：+500 topic、+200 group、
 *                        big-throughput topic 4 分区共 5000 条消息（纯内存生成，
 *                        无凭据）；consume 未显式 limit 时默认取 5000。
 *                        仅追加种子数据，不改真实桥形状与非 big 模式行为。
 *   ?audit=denied        审计链夹具：宿主监听就绪后注入 1 条 denied + 1 条 ok
 *                        kafka/audit 事件（真实链路只在策略拒绝时产生，ro 模式
 *                        下写入口已禁用而不可达；此参数使 AuditFeed 展示/自动
 *                        展开/denied 徽标与错误横幅链路可在 mock 中验证）。
 */
import "./style.css";
import { compress as lz4Compress } from "lz4js";
import { compress as snappyCompress } from "snappyjs";

const eventListeners = new Set<(event: DbxPluginEvent) => void>();
const appearanceListeners = new Set<(appearance: DbxPluginAppearance) => void>();
const contextListeners = new Set<(context: Record<string, unknown>) => void>();

const params = new URLSearchParams(location.search);
const readOnly = params.get("ro") === "1";
const allowDelete = params.get("nodelete") !== "1";
const injectError = params.get("err") === "1";
const glueOnly = params.get("glue") === "1";
const bigMode = params.get("big") === "1";
// OAUTHBEARER/MSK 连接摘要夹具（?msk=1）：照 ?glue=1 开关范式。
const mskMode = params.get("msk") === "1";
// 审计链夹具（?audit=denied）：见文件头注释。
const auditDeniedInject = params.get("audit") === "denied";
// 连接默认 SR provider（无 registry 参数的 kafka/schema/* 调用落到这里）。
const defaultSrProvider: "confluent" | "glue" = glueOnly ? "glue" : "confluent";

const context: Record<string, unknown> = {
  connectionId: params.get("noconn") === "1" ? "" : "visual-connection",
  workbenchId: "visual-workbench",
  restored: false,
  connection: {
    name: "Demo Kafka",
    host: "dbx-kafka-test",
    port: 9092,
    username: "kafka-app",
    color: "#e11d48",
    readOnly,
    allowDelete,
    // Phase P：manifest glue_* 连接字段（连接摘要展示用；secret 走 binding 名单）。
    // schema_registry 决策开关（none | confluent | aws_glue）随 glue 参数联动。
    external_config: {
      schema_registry: glueOnly ? "aws_glue" : "confluent",
      glue_region: "us-east-1",
      glue_registry_name: "dbx-kafka-registry",
      glue_auth_mode: "access_key",
      glue_access_key_id: "mock-access-key-id",
      // Phase 3 F2 前端半件（?msk=1）：OAUTHBEARER/MSK 字段（snake_case，
      // 镜像 host 对 manifest 字段的 external_config 形状；camelCase 直挂
      // connection 的双源兼容由 ConnectionsPanel.pickConnectionField 覆盖）。
      ...(mskMode
        ? {
            security_protocol: "SASL_SSL",
            sasl_mechanism: "OAUTHBEARER",
            oauth_token_source: "msk_iam",
            msk_region: "us-east-1",
            msk_access_key_id: "mock-access-key-id",
          }
        : {}),
    },
    // secret binding 名单：仅名字，不含值（前端据此显示「已配置」）。
    connection_secrets: mskMode ? ["glue_secret_access_key", "msk_secret_access_key", "msk_session_token"] : ["glue_secret_access_key"],
  },
};

const light = params.get("theme") === "light";
// 与 DBX globals.css 的 :root（pearl 浅色）和 .dark 规范块保持一致。
const appearance: DbxPluginAppearance = {
  colorScheme: light ? "light" : "dark",
  colors: light
    ? { background: "rgb(255 255 255)", foreground: "rgb(10 10 10)", muted: "rgb(245 245 245)", mutedForeground: "rgb(115 115 115)", accent: "rgb(245 245 245)", accentForeground: "rgb(23 23 23)", border: "rgb(229 229 229)", destructive: "rgb(231 0 11)" }
    : { background: "rgb(19 20 22)", foreground: "rgb(215 215 219)", muted: "rgb(42 42 45)", mutedForeground: "rgb(151 152 157)", accent: "rgb(46 47 51)", accentForeground: "rgb(221 221 226)", border: "rgb(110 110 114 / 0.28)", destructive: "rgb(243 98 95)" },
  terminal: { fontFamily: "Cascadia Mono, Consolas, monospace", fontSize: 13 },
};

// 镜像宿主 1.1 theme 通道形状（colors 反查 --color-* 令牌），与真实宿主一致。
const theme: DbxPluginTheme = {
  appearance: appearance.colorScheme,
  tokens: Object.fromEntries(
    Object.entries(appearance.colors).map(([key, value]) => [`--color-${key.replace(/([A-Z])/g, (c) => `-${c.toLowerCase()}`)}`, value]),
  ),
};

// -- in-memory cluster ----------------------------------------------------------

interface MockMessage {
  topic: string;
  partition: number;
  offset: number;
  timestamp: number;
  leaderEpoch?: number;
  key?: string;
  keyBase64?: string;
  valueText?: string;
  valueBase64?: string;
  headers?: Record<string, string>;
  committed?: boolean;
  schemaId?: number;
  schemaSubject?: string;
  schemaVersion?: number;
}

interface MockTopic {
  name: string;
  topicId: string;
  isInternal: boolean;
  partitionCount: number;
  replicationFactor: number;
  /** 分区 → 消息数组（offset = 数组下标）。 */
  partitions: MockMessage[][];
  /** Phase 3 F6-4 后端半件镜像（topics/list 下发；缺省 = 健康）。 */
  isHealthy?: boolean;
  unhealthyPartitions?: number;
}

function utf8ToBase64(text: string): string {
  return btoa(String.fromCharCode(...new TextEncoder().encode(text)));
}

function makeMessage(topic: string, partition: number, offset: number, key: string, value: string, headers: Record<string, string> = {}): MockMessage {
  return {
    topic,
    partition,
    offset,
    timestamp: Date.now(),
    leaderEpoch: 0,
    key,
    keyBase64: utf8ToBase64(key),
    valueText: value,
    valueBase64: utf8ToBase64(value),
    headers,
  };
}

const brokers: KafkaBrokerFixture[] = [
  { nodeId: 1, host: "dbx-kafka-test", port: 9092, rack: "rack-a" },
  { nodeId: 2, host: "dbx-kafka-test-2", port: 9092, rack: "rack-b" },
];

interface KafkaBrokerFixture {
  nodeId: number;
  host: string;
  port: number;
  rack?: string;
}

const brokerConfigs: ConfigEntryFixture[] = [
  { name: "num.partitions", value: "1", source: "DEFAULT_CONFIG", sensitive: false, isDefault: true },
  { name: "log.retention.ms", value: "604800000", source: "DYNAMIC_BROKER_CONFIG", sensitive: false, isDefault: false },
  { name: "sasl.jaas.config", value: "hidden", source: "STATIC_BROKER_CONFIG", sensitive: true, isDefault: false },
];

interface ConfigEntryFixture {
  name: string;
  value: string;
  source?: string;
  sensitive?: boolean;
  isDefault?: boolean;
}

const topics = new Map<string, MockTopic>();

function putTopic(topic: MockTopic) {
  topics.set(topic.name, topic);
}

function seedTopic(name: string, partitionCount: number, isInternal: boolean, samples: Array<{ key: string; value: string; headers?: Record<string, string> }>) {
  const topic: MockTopic = {
    name,
    topicId: `id-${name}`,
    isInternal,
    partitionCount,
    replicationFactor: 1,
    partitions: Array.from({ length: partitionCount }, () => [] as MockMessage[]),
  };
  samples.forEach((sample, index) => {
    const partition = index % partitionCount;
    const message = makeMessage(name, partition, topic.partitions[partition].length, sample.key, sample.value, sample.headers);
    topic.partitions[partition].push(message);
  });
  putTopic(topic);
}

seedTopic("order-events", 2, false, [
  { key: "o-1", value: '{"orderId":"A-1001","amount":42}', headers: { "trace-id": "t-1" } },
  { key: "o-2", value: '{"orderId":"A-1002","amount":7}' },
  { key: "o-3", value: '{"orderId":"A-1003","amount":129}' },
]);
seedTopic("user-signup", 1, false, [
  { key: "u-1", value: '{"userId":"u1","name":"Ada"}' },
  { key: "u-2", value: "binary\u0001payload" },
]);
seedTopic("payment-gateway", 1, false, [{ key: "p-1", value: "5" }]);
seedTopic("connect-offsets", 1, true, [{ key: "c", value: "state" }]);
seedTopic("_schemas", 1, true, [{ key: "s-1", value: '{"schema":"x"}' }]);

// Phase 3 F6-5：不健康 topic 行夹具（isHealthy:false + unhealthyPartitions=1，
// partition 1 leader 缺失）——树红点徽标与 describe 健康列可在 mock 验证。
function seedUnhealthyTopic(name: string, partitionCount: number) {
  seedTopic(name, partitionCount, false, []);
  const topic = topics.get(name)!;
  topic.isHealthy = false;
  topic.unhealthyPartitions = 1;
}
seedUnhealthyTopic("degraded-topic", 2);

// Phase P：codec-lab 假 topic——四种压缩算法各一条消息（详情抽屉二次解码走查用）。
// gzip/zstd 用固定 base64 常量（浏览器无同步 gzip 压缩器；fzstd 仅解码），
// snappy/lz4 用各库 compress API 现场构造。
const GZIP_N7_PAYLOAD_B64 = "H4sIAAAAAAAAE6tWylOyMq8FAPicEYIHAAAA"; // gzip('{"n":7}')
const ZSTD_ORDER_PAYLOAD_B64 =
  "KLUv/SQxiQEAeyJvcmRlcklkIjoiQS0xMDAxIiwiYW1vdW50Ijo0MiwiY3VycmVuY3kiOiJVU0QifbQxJQg=";

function bytesToBase64(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
}

const codecSamples: Array<{ label: string; payloadB64: string }> = [
  { label: "gzip", payloadB64: GZIP_N7_PAYLOAD_B64 },
  { label: "zstd", payloadB64: ZSTD_ORDER_PAYLOAD_B64 },
  {
    label: "snappy",
    payloadB64: bytesToBase64(snappyCompress(new TextEncoder().encode('{"algo":"snappy","ok":true}'))),
  },
  {
    label: "lz4",
    payloadB64: bytesToBase64(lz4Compress(new TextEncoder().encode('{"algo":"lz4","ok":true}'))),
  },
  // XML 明文样本:详情抽屉 XML 高亮/格式化自动识别走查用(属性/嵌套/自闭合)。
  {
    label: "xml",
    payloadB64: bytesToBase64(
      new TextEncoder().encode(
        '<order id="A-1009" currency="USD"><customer><name>Alice</name><tier>gold</tier></customer><items><item sku="S-1" qty="2"/><item sku="S-2" qty="1"/></items><paid>true</paid></order>',
      ),
    ),
  },
];
const codecTopic: MockTopic = {
  name: "codec-lab",
  topicId: "id-codec-lab",
  isInternal: false,
  partitionCount: 1,
  replicationFactor: 1,
  partitions: [[]],
};
codecSamples.forEach((sample, index) => {
  codecTopic.partitions[0].push({
    topic: "codec-lab",
    partition: 0,
    offset: index,
    timestamp: Date.now(),
    leaderEpoch: 0,
    key: sample.label,
    keyBase64: utf8ToBase64(sample.label),
    valueText: `(compressed payload: ${sample.label})`,
    valueBase64: sample.payloadB64,
  });
});
topics.set(codecTopic.name, codecTopic);

interface MockGroup {
  group: string;
  state: string;
  protocolType: string;
  coordinator: number;
  members: Array<{ memberId: string; instanceId?: string; clientId?: string; clientHost?: string; assignments?: Record<string, number[]> }>;
  committed: Map<string, Map<number, number>>; // topic → partition → committed offset
}

const groups = new Map<string, MockGroup>();
const orderGroup: MockGroup = {
  group: "billing-consumer",
  state: "Stable",
  protocolType: "consumer",
  coordinator: 1,
  members: [
    { memberId: "member-1", clientId: "billing-1", clientHost: "/10.0.0.4", assignments: { "order-events": [0, 1] } },
  ],
  committed: new Map([
    ["order-events", new Map([[0, 1], [1, 0]])],
  ]),
};
groups.set(orderGroup.group, orderGroup);

// ?big=1 大数据量压测种子：500 topic + big-throughput 5000 条消息 + 200 group。
// 纯内存生成（无凭据），非 big 模式完全不受影响。
function seedBigFixtures() {
  for (let index = 1; index <= 500; index += 1) {
    seedTopic(`bulk-topic-${String(index).padStart(4, "0")}`, index % 3 === 0 ? 2 : 1, false, []);
  }
  const bigTopicName = "big-throughput";
  seedTopic(bigTopicName, 4, false, []);
  const bigTopic = topics.get(bigTopicName)!;
  for (let sequence = 0; sequence < 5000; sequence += 1) {
    const partition = sequence % 4;
    bigTopic.partitions[partition].push(
      makeMessage(bigTopicName, partition, bigTopic.partitions[partition].length, `k-${sequence}`, JSON.stringify({ seq: sequence, payload: "x".repeat(64) }), { seq: String(sequence) }),
    );
  }
  for (let index = 1; index <= 200; index += 1) {
    const empty = index % 7 === 0;
    const group: MockGroup = {
      group: `bulk-consumer-${String(index).padStart(3, "0")}`,
      state: empty ? "Empty" : "Stable",
      protocolType: "consumer",
      coordinator: (index % 2) + 1,
      members: empty
        ? []
        : [{ memberId: `member-${index}-1`, clientId: `client-${index}`, clientHost: "/10.0.0.4", assignments: { [bigTopicName]: [0, 1] } }],
      committed: new Map([[bigTopicName, new Map([[0, 100 + index], [1, 50 + index]])]]),
    };
    groups.set(group.group, group);
  }
}
if (bigMode) seedBigFixtures();

const acls: KafkaAclFixture[] = [
  { resourceType: "TOPIC", resourceName: "order-events", patternType: "LITERAL", principal: "User:billing", host: "*", operation: "READ", permission: "ALLOW" },
];

interface KafkaAclFixture {
  resourceType: string;
  resourceName: string;
  patternType?: string;
  principal: string;
  host?: string;
  operation: string;
  permission: string;
}

// -- schema registry fixture（Phase 2：SR 假数据 + schema roundtrip 假编解码）-------

interface SchemaVersionFixture {
  version: number;
  id: number;
  format: "avro" | "json" | "protobuf";
  schema: string;
}

interface SchemaSubjectFixture {
  subject: string;
  formats: Array<"avro" | "json" | "protobuf">;
  versions: SchemaVersionFixture[];
  compatibilityLevel: string;
  /** Phase P：subject 所属 registry（confluent | glue）。 */
  provider: "confluent" | "glue";
}

let schemaIdSeq = 100;

function makeSchemaVersion(subject: SchemaSubjectFixture, format: "avro" | "json" | "protobuf", schema: string): SchemaVersionFixture {
  schemaIdSeq += 1;
  return { version: subject.versions.length + 1, id: schemaIdSeq, format, schema };
}

const schemaSubjects = new Map<string, SchemaSubjectFixture>();

/** registry 参数解析：缺省落到连接默认 provider（与后端语义一致）。 */
function resolveProvider(input: Record<string, unknown>): "confluent" | "glue" {
  return input.registry === "glue" || input.registry === "confluent" ? input.registry : defaultSrProvider;
}

function providerSubjects(provider: "confluent" | "glue"): SchemaSubjectFixture[] {
  return [...schemaSubjects.values()].filter((subject) => subject.provider === provider);
}

function findSubject(name: string, provider: "confluent" | "glue"): SchemaSubjectFixture | undefined {
  const subject = schemaSubjects.get(name);
  return subject && subject.provider === provider ? subject : undefined;
}

/** Glue 挂载拒绝（契约 2：消息编解码仅 Confluent wire format，后端 -32000 业务错）。 */
function rejectGlueSchemaAttach(schemaParam: { subject?: string } | undefined | null) {
  if (schemaParam?.subject && glueOnly) {
    throw new Error(
      "-32000: schema attach is not supported for AWS Glue Schema Registry — message codec implements the Confluent wire format only (fixture)",
    );
  }
}

const orderSchemaV1 = JSON.stringify({ type: "record", name: "Order", fields: [{ name: "orderId", type: "string" }, { name: "amount", type: "int" }] }, null, 2);
const orderSchemaV2 = JSON.stringify({ type: "record", name: "Order", fields: [{ name: "orderId", type: "string" }, { name: "amount", type: "int" }, { name: "currency", type: "string", default: "USD" }] }, null, 2);
const userSchemaV1 = JSON.stringify({ type: "object", properties: { userId: { type: "string" }, name: { type: "string" } }, required: ["userId"] }, null, 2);

const orderSubject: SchemaSubjectFixture = {
  subject: "order-events-value",
  formats: ["avro"],
  versions: [],
  compatibilityLevel: "BACKWARD",
  provider: "confluent",
};
orderSubject.versions.push(makeSchemaVersion(orderSubject, "avro", orderSchemaV1));
orderSubject.versions.push(makeSchemaVersion(orderSubject, "avro", orderSchemaV2));
schemaSubjects.set(orderSubject.subject, orderSubject);

const userSubject: SchemaSubjectFixture = {
  subject: "user-signup-value",
  formats: ["json"],
  versions: [],
  compatibilityLevel: "NONE",
  provider: "confluent",
};
userSubject.versions.push(makeSchemaVersion(userSubject, "json", userSchemaV1));
schemaSubjects.set(userSubject.subject, userSubject);

// Phase 3 F1/F5：PROTOBUF subject 样例（subjects/list formats 含 protobuf）——
// SchemasPanel 树视图「文本 + 行内提示」路径与 ProducePanel Flow schema_random
// 「命中非 AVRO subject → 改用 template」提示路径可在 mock 验证。
// fixture 用 proto3 源文本（真实 SR 元数据为 base64 FileDescriptorSet，G 路域）。
const ordersProtoSchema = `syntax = "proto3";

package demo.v1;

message Order {
  string order_id = 1;
  int32 amount = 2;
}`;
const ordersProtoSubject: SchemaSubjectFixture = {
  subject: "orders-proto-value",
  formats: ["protobuf"],
  versions: [],
  compatibilityLevel: "BACKWARD",
  provider: "confluent",
};
ordersProtoSubject.versions.push(makeSchemaVersion(ordersProtoSubject, "protobuf", ordersProtoSchema));
schemaSubjects.set(ordersProtoSubject.subject, ordersProtoSubject);

// Phase P：AWS Glue 假 subjects（管理面假数据；兼容性枚举为 Glue 命名）。
const glueOrderSubject: SchemaSubjectFixture = {
  subject: "orders-value",
  formats: ["avro"],
  versions: [],
  compatibilityLevel: "BACKWARD_ALL",
  provider: "glue",
};
glueOrderSubject.versions.push(makeSchemaVersion(glueOrderSubject, "avro", orderSchemaV1));
schemaSubjects.set(glueOrderSubject.subject, glueOrderSubject);

const gluePaymentSubject: SchemaSubjectFixture = {
  subject: "payments-value",
  formats: ["json"],
  versions: [],
  compatibilityLevel: "FULL_ALL",
  provider: "glue",
};
gluePaymentSubject.versions.push(makeSchemaVersion(gluePaymentSubject, "json", userSchemaV1));
schemaSubjects.set(gluePaymentSubject.subject, gluePaymentSubject);

let globalCompatibility = { level: "BACKWARD", scope: "GLOBAL" };

/** fixture 级版本 diff：pretty JSON 行集合对比 → hunks（与后端形状一致）。 */
function diffSchemaVersions(subject: SchemaSubjectFixture, fromVersion: number, toVersion: number) {
  const from = subject.versions.find((entry) => entry.version === fromVersion);
  const to = subject.versions.find((entry) => entry.version === toVersion);
  if (!from || !to) throw new Error("schema version not found (fixture)");
  const beforeLines = from.schema.split("\n");
  const afterLines = to.schema.split("\n");
  const hunks: Array<{ op: "add" | "remove" | "modify"; path: string; before?: string; after?: string }> = [];
  beforeLines.forEach((line, index) => {
    if (!afterLines.includes(line)) {
      const counterpart = afterLines[index];
      if (counterpart !== undefined && !beforeLines.includes(counterpart)) {
        hunks.push({ op: "modify", path: `line ${index + 1}`, before: line.trim(), after: counterpart.trim() });
      } else {
        hunks.push({ op: "remove", path: `line ${index + 1}`, before: line.trim() });
      }
    }
  });
  afterLines.forEach((line, index) => {
    if (!beforeLines.includes(line)) {
      const already = hunks.some((hunk) => hunk.op === "modify" && hunk.after === line.trim());
      if (!already) hunks.push({ op: "add", path: `line ${index + 1}`, after: line.trim() });
    }
  });
  const summary = `${subject.subject}: +${hunks.filter((hunk) => hunk.op === "add").length} -${hunks.filter((hunk) => hunk.op === "remove").length} ~${hunks.filter((hunk) => hunk.op === "modify").length}`;
  return { hunks, summary };
}

// -- stream sessions（fixture 级 ring buffer + 定时发事件）-------------------------

interface StreamSession {
  id: string;
  topic: string;
  buffer: MockMessage[];
  produced: number;
  paused: boolean;
  timer?: number;
  /** Phase 2：SR 挂载（tick 时给消息附 schemaId/subject/version）。 */
  schema?: { subject: string; version?: number; format?: string };
}

const streams = new Map<string, StreamSession>();
let streamSeq = 0;

function startStream(topicName: string, schema?: { subject: string; version?: number; format?: string }): StreamSession {
  streamSeq += 1;
  const session: StreamSession = { id: `stream-${streamSeq}`, topic: topicName, buffer: [], produced: 0, paused: false, schema };
  session.timer = window.setInterval(() => {
    if (session.paused) return;
    injectError ? failStream(session) : tickStream(session);
  }, 500);
  streams.set(session.id, session);
  return session;
}

/** fixture 假 SR 解码：按挂载 subject/version 取 schemaId 附到消息上。
 *  挂载编解码仅 Confluent 侧（Glue 仅管理面，produce/consume/stream 已拒绝）。 */
function attachSchema(message: MockMessage, schema?: { subject: string; version?: number }): MockMessage {
  if (!schema?.subject) return message;
  const subject = schemaSubjects.get(schema.subject);
  if (!subject || subject.provider !== "confluent") return message;
  const version = schema.version !== undefined ? subject.versions.find((entry) => entry.version === schema.version) : subject.versions[subject.versions.length - 1];
  if (!version) return message;
  return { ...message, schemaId: version.id, schemaSubject: subject.subject, schemaVersion: version.version };
}

let tickCount = 0;
function tickStream(session: StreamSession) {
  tickCount += 1;
  const batch: MockMessage[] = [];
  for (let index = 0; index < 3; index += 1) {
    session.produced += 1;
    batch.push(attachSchema(makeMessage(session.topic, 0, session.produced, `live-${session.produced}`, `{"tick":${tickCount},"n":${session.produced}}`), session.schema));
  }
  session.buffer.push(...batch);
  if (session.buffer.length > 10000) session.buffer.splice(0, session.buffer.length - 10000);
  emitEvent("kafka/stream/messages", {
    sessionId: session.id,
    messages: batch,
    totalScanned: session.produced,
    totalMatched: session.produced,
    paused: session.paused,
    bufferSize: session.buffer.length,
  });
}

function failStream(session: StreamSession) {
  emitEvent("kafka/stream/error", { sessionId: session.id, error: "connection lost (fixture error injection)" });
}

function stopStream(sessionId?: string) {
  for (const [id, session] of streams) {
    if (!sessionId || sessionId === id || sessionId === "all") {
      if (session.timer) window.clearInterval(session.timer);
      streams.delete(id);
    }
  }
}

// -- audit / policy ---------------------------------------------------------------

function emitEvent(method: string, eventParams: Record<string, unknown>) {
  for (const listener of eventListeners) listener({ method, params: eventParams } as DbxPluginEvent);
}

// readOnly / allowDelete 下写操作被策略拒绝：先发 denied audit 事件（App.vue
// 横幅与 audit.jsonl 的 denied 语义对应），再抛业务错误（与后端 policy.go 分支
// 行为一致：AuditRecord{Action:"write-policy", Result:"denied"}）。
function denyWrite(action: string, target: string, reason: string): never {
  emitEvent("kafka/audit", {
    connectionId: context.connectionId,
    action,
    target,
    result: "denied",
    detail: reason,
  });
  throw new Error(reason);
}

function guardWrite(action: string, target: string, options: { critical?: boolean; confirmTopic?: string } = {}) {
  if (readOnly) denyWrite(action, target, "connection is read-only (fixture)");
  if (options.critical && !allowDelete) denyWrite(action, target, "allow_delete=false rejects delete operations (fixture)");
  if (options.critical && options.confirmTopic !== undefined && options.confirmTopic !== target) {
    denyWrite(action, target, "confirmTopic mismatch (fixture)");
  }
}

function requireTopic(name: string): MockTopic {
  const topic = topics.get(name);
  if (!topic) throw new Error(`unknown topic: ${name} (fixture)`);
  return topic;
}

// -- filter evaluation（fixture 级 contains/prefix/exact/regex）---------------------

function matchValue(candidate: string | undefined, pattern: string, mode: string): boolean {
  const haystack = candidate ?? "";
  switch (mode) {
    case "prefix":
      return haystack.startsWith(pattern);
    case "exact":
      return haystack === pattern;
    case "regex":
      try {
        return new RegExp(pattern).test(haystack);
      } catch {
        return false;
      }
    default:
      return haystack.includes(pattern);
  }
}

function messageMatches(message: MockMessage, input: Record<string, unknown>): boolean {
  const mode = String(input.matchMode ?? "contains");
  const blob = `${message.key ?? ""}${message.valueText ?? ""}${JSON.stringify(message.headers ?? {})}`;
  if (typeof input.filter === "string" && input.filter && !matchValue(blob, input.filter, mode)) return false;
  if (typeof input.keyFilter === "string" && input.keyFilter && !matchValue(message.key, input.keyFilter, mode)) return false;
  if (typeof input.valueFilter === "string" && input.valueFilter && !matchValue(message.valueText, input.valueFilter, mode)) return false;
  if (typeof input.headerFilter === "string" && input.headerFilter) {
    const headerBlob = JSON.stringify(message.headers ?? {});
    if (!matchValue(headerBlob, input.headerFilter, mode)) return false;
  }
  const from = typeof input.timestampFrom === "number" ? input.timestampFrom : null;
  const to = typeof input.timestampTo === "number" ? input.timestampTo : null;
  if (from !== null && message.timestamp < from) return false;
  if (to !== null && message.timestamp > to) return false;
  const offsetFrom = typeof input.offsetFrom === "number" ? input.offsetFrom : null;
  const offsetTo = typeof input.offsetTo === "number" ? input.offsetTo : null;
  if (offsetFrom !== null && message.offset < offsetFrom) return false;
  if (offsetTo !== null && message.offset > offsetTo) return false;
  return true;
}

function scanTopic(topic: MockTopic, input: Record<string, unknown>): { messages: MockMessage[]; scanned: number } {
  // big 压测模式：未显式 limit 时默认取 5000（一次 consume 返回全量大批次）。
  const limit = Number(input.limit ?? (bigMode ? 5000 : 100)) || (bigMode ? 5000 : 100);
  const wantedPartitions = Array.isArray(input.partitions) ? (input.partitions as number[]) : null;
  const strategy = String(input.offsetStrategy ?? "latest");
  const scannedAll: Array<{ message: MockMessage; index: number; partition: number }> = [];
  let scanned = 0;
  topic.partitions.forEach((partitionMessages, partition) => {
    if (wantedPartitions && !wantedPartitions.includes(partition)) return;
    const partitionOffsets = (input.partitionOffsets ?? {}) as Record<string, number>;
    let startIndex: number;
    if (strategy === "offset" && partitionOffsets[partition] !== undefined) startIndex = partitionOffsets[partition];
    else if (strategy === "earliest") startIndex = 0;
    else if (strategy === "committed") {
      // 按请求的 groupId 取对应组的 committed（此前硬编码 orderGroup，
      // 其余组在 committed 策略下语义错误——回落 earliest）。
      const committed = groups.get(String(input.groupId ?? ""))?.committed.get(topic.name)?.get(partition);
      startIndex = committed === undefined ? 0 : committed;
    } else if (strategy === "timestamp") {
      const after = typeof input.offsetTime === "number" ? input.offsetTime : Date.parse(String(input.offsetTime ?? "")) || 0;
      startIndex = partitionMessages.findIndex((message) => message.timestamp >= after);
      if (startIndex < 0) startIndex = partitionMessages.length;
    } else startIndex = Math.max(0, partitionMessages.length - limit);
    for (let index = startIndex; index < partitionMessages.length; index += 1) {
      scanned += 1;
      scannedAll.push({ message: partitionMessages[index], index, partition });
    }
  });
  const matched = scannedAll.filter((entry) => messageMatches(entry.message, input)).map((entry) => entry.message);
  return { messages: matched.slice(0, limit), scanned };
}

// mock 预设存取（localStorage 损坏/禁存储时静默兜底，与其他 lib 的
// best-effort 存取范式一致）。
function readMockPresets(): Array<Record<string, unknown>> {
  try {
    const parsed = JSON.parse(localStorage.getItem("kafka-mock-presets") ?? "[]");
    return Array.isArray(parsed) ? (parsed as Array<Record<string, unknown>>) : [];
  } catch {
    return [];
  }
}

function writeMockPresets(presets: Array<Record<string, unknown>>): void {
  try {
    localStorage.setItem("kafka-mock-presets", JSON.stringify(presets));
  } catch {
    /* 存储不可用：内存态即可 */
  }
}

// -- request / invoke ---------------------------------------------------------------

const request: DbxPluginApi["request"] = async <T = unknown>(method: string) =>
  (method === "host.getContext" ? context : null) as T;

const invoke: DbxPluginApi["invoke"] = async <T = unknown>(method: string, rawParams?: unknown) => {
  const input = (rawParams ?? {}) as Record<string, unknown>;
  let result: unknown = { success: true };
  if (injectError && method !== "kafka/connections/statuses" && method !== "host.getContext") {
    throw new Error("connection lost (fixture error injection)");
  }
  if (method === "kafka/brokers/list") {
    result = { brokers, connectionSource: "kafka" };
  } else if (method === "kafka/brokers/config") {
    result = { entries: brokerConfigs };
  } else if (method === "kafka/topics/list") {
    const includeInternal = input.includeInternal === true;
    const list = [...topics.values()]
      .filter((topic) => includeInternal || !topic.isInternal)
      .map((topic) => ({
        name: topic.name,
        topicId: topic.topicId,
        isInternal: topic.isInternal,
        partitionCount: topic.partitionCount,
        replicationFactor: topic.replicationFactor,
        // Phase 3 F6-4 镜像：健康度字段（12.2.4；缺省 = 全分区健康）。
        isHealthy: topic.isHealthy !== false,
        ...(topic.unhealthyPartitions !== undefined ? { unhealthyPartitions: topic.unhealthyPartitions } : {}),
      }));
    result = { topics: list };
  } else if (method === "kafka/topics/describe") {
    const topic = requireTopic(String(input.topic ?? ""));
    result = {
      partitions: topic.partitions.map((partition, index) => {
        // degraded-topic：partition 1（超 0 时末位）标不健康（offline replica）。
        const unhealthyIndex = topic.isHealthy === false ? topic.partitions.length - 1 : -1;
        const isUnhealthy = index === unhealthyIndex;
        return {
          partition: index,
          leader: brokers[index % brokers.length].nodeId,
          leaderEpoch: 0,
          replicas: [brokers[index % brokers.length].nodeId],
          isr: isUnhealthy ? [] : [brokers[index % brokers.length].nodeId],
          offlineReplicas: isUnhealthy ? [brokers[index % brokers.length].nodeId] : [],
          isHealthy: !isUnhealthy,
        };
      }),
    };
  } else if (method === "kafka/topics/create") {
    const names = (input.topics ?? []) as string[];
    const partitionCount = Number(input.partitions ?? 1);
    const replicationFactor = Number(input.replicationFactor ?? 1);
    const config = (input.config ?? {}) as Record<string, string>;
    guardWrite("topics/create", names.join(","));
    const results = names.map((name) => {
      if (topics.has(name)) return { topic: name, ok: false, error: "topic already exists" };
      seedTopic(name, partitionCount, false, []);
      const seeded = topics.get(name)!;
      seeded.replicationFactor = replicationFactor;
      void config;
      return { topic: name, ok: true };
    });
    result = { results };
  } else if (method === "kafka/topics/delete") {
    const names = (input.topics ?? []) as string[];
    for (const name of names) {
      guardWrite("topics/delete", name, { critical: true, confirmTopic: String(input.confirmTopic ?? "") });
    }
    result = {
      results: names.map((name) => {
        const existed = topics.delete(name);
        return { topic: name, ok: existed, error: existed ? undefined : "unknown topic" };
      }),
    };
  } else if (method === "kafka/topics/partitions/update") {
    const wanted = (input.partitions ?? {}) as Record<string, number>;
    guardWrite("topics/partitions/update", JSON.stringify(wanted));
    const results: Array<{ topic: string; ok: boolean; error?: string }> = [];
    for (const [name, nextCount] of Object.entries(wanted)) {
      const topic = topics.get(name);
      if (!topic) {
        results.push({ topic: name, ok: false, error: "unknown topic" });
        continue;
      }
      if (nextCount <= topic.partitionCount) {
        results.push({ topic: name, ok: false, error: "partition count must be larger" });
        continue;
      }
      while (topic.partitions.length < nextCount) topic.partitions.push([]);
      topic.partitionCount = nextCount;
      results.push({ topic: name, ok: true });
    }
    result = { results };
  } else if (method === "kafka/topics/config/get" || method === "kafka/brokers/config") {
    result = {
      entries: [
        { name: "retention.ms", value: "604800000", source: "DYNAMIC_TOPIC_CONFIG", sensitive: false, isDefault: false },
        { name: "cleanup.policy", value: "delete", source: "DEFAULT_CONFIG", sensitive: false, isDefault: true },
        { name: "password.secret", value: "hidden", source: "DYNAMIC_TOPIC_CONFIG", sensitive: true, isDefault: false },
      ],
    };
  } else if (method === "kafka/topics/config/alter") {
    guardWrite("topics/config/alter", String(input.topic ?? ""));
    result = { entries: brokerConfigs };
  } else if (method === "kafka/topics/offsets/list") {
    const names = (input.topics ?? []) as string[];
    const rows: Array<Record<string, unknown>> = [];
    for (const name of names) {
      const topic = topics.get(name);
      if (!topic) continue;
      topic.partitions.forEach((partition, index) => {
        const last = partition[partition.length - 1];
        rows.push({ topic: name, partition: index, offset: partition.length, timestamp: last?.timestamp, leaderEpoch: 0 });
      });
    }
    result = { rows };
  } else if (method === "kafka/groups/list") {
    result = {
      groups: [...groups.values()].map((group) => ({ group: group.group, state: group.state, protocolType: group.protocolType, coordinator: group.coordinator })),
    };
  } else if (method === "kafka/groups/describe") {
    const group = groups.get(String(input.group ?? ""));
    result = { members: group?.members ?? [] };
  } else if (method === "kafka/groups/offsets/list") {
    const group = groups.get(String(input.group ?? ""));
    const rows: Array<Record<string, unknown>> = [];
    let totalLag = 0;
    if (group) {
      for (const [topicName, partitions] of group.committed) {
        const topic = topics.get(topicName);
        topic?.partitions.forEach((partition, index) => {
          const committedOffset = partitions.get(index);
          const endOffset = partition.length;
          const lag = committedOffset === undefined ? endOffset : Math.max(0, endOffset - committedOffset);
          totalLag += lag;
          rows.push({
            topic: topicName,
            partition: index,
            startOffset: 0,
            endOffset,
            committedOffset: committedOffset ?? null,
            lag,
            hasCommitted: committedOffset !== undefined,
          });
        });
      }
    }
    result = { rows, totalLag };
  } else if (method === "kafka/groups/delete") {
    const group = String(input.group ?? "");
    guardWrite("groups/delete", group, { critical: true });
    result = { success: groups.delete(group) };
  } else if (method === "kafka/groups/offsets/reset") {
    const group = groups.get(String(input.group ?? ""));
    guardWrite("groups/offsets/reset", String(input.group ?? ""));
    const topicsArg = ((input.topics ?? []) as string[]).filter(Boolean);
    const rows: Array<Record<string, unknown>> = [];
    if (group) {
      for (const [topicName, partitions] of group.committed) {
        if (topicsArg.length > 0 && !topicsArg.includes(topicName)) continue;
        const topic = topics.get(topicName);
        topic?.partitions.forEach((partition, index) => {
          const resetTo = String(input.resetTo ?? "earliest");
          let next = 0;
          if (resetTo === "latest") next = partition.length;
          else if (resetTo === "timestamp") next = partition.filter((message) => message.timestamp >= Number(input.timestampMs ?? 0)).length;
          else if (resetTo === "partitionOffset") {
            const wanted = (input.partitionOffsets ?? {}) as Record<string, number>;
            next = wanted[index] ?? 0;
          }
          partitions.set(index, next);
          rows.push({ topic: topicName, partition: index, ok: true });
        });
      }
    }
    result = { rows };
  } else if (method === "kafka/acls/list") {
    const filter_ = (input.filter ?? {}) as Record<string, string>;
    const matched = acls.filter((acl) =>
      Object.entries(filter_).every(([key, value]) => {
        if (!value) return true;
        const candidate = (acl as unknown as Record<string, string>)[key] ?? "";
        return candidate === value || value === "ANY";
      }),
    );
    result = { acls: matched };
  } else if (method === "kafka/acls/create") {
    const acl = (input.acl ?? {}) as KafkaAclFixture;
    guardWrite("acls/create", `${acl.resourceType}:${acl.resourceName}`);
    acls.push(acl);
    result = { success: true };
  } else if (method === "kafka/acls/delete") {
    const filter_ = (input.filter ?? {}) as Record<string, string>;
    guardWrite("acls/delete", JSON.stringify(filter_), { critical: true });
    const before = acls.length;
    for (let index = acls.length - 1; index >= 0; index -= 1) {
      const acl = acls[index] as unknown as Record<string, string>;
      const matches = Object.entries(filter_).every(([key, value]) => !value || acl[key] === value || value === "ANY");
      if (matches) acls.splice(index, 1);
    }
    result = { matched: before - acls.length };
  } else if (method === "kafka/messages/produce") {
    const topicName = String(input.topic ?? "");
    const topic = requireTopic(topicName);
    guardWrite("messages/produce", topicName);
    const count = Math.min(Number(input.count ?? 1) || 1, 1000);
    const partition = typeof input.partition === "number" ? input.partition : 0;
    // Phase 2：keyBase64/valueBase64 保真载荷（与 key/value 二选一），base64 → 文本。
    const key = typeof input.keyBase64 === "string" ? atob(input.keyBase64) : typeof input.key === "string" ? input.key : "";
    const value = typeof input.valueBase64 === "string" ? atob(input.valueBase64) : String(input.value ?? "");
    const headers = (input.headers ?? {}) as Record<string, string>;
    const schemaParam = input.schema as { subject: string; version?: number } | undefined;
    rejectGlueSchemaAttach(schemaParam);
    let schemaId: number | undefined;
    if (schemaParam?.subject) {
      const subject = schemaSubjects.get(schemaParam.subject);
      const version =
        schemaParam.version !== undefined
          ? subject?.versions.find((entry) => entry.version === schemaParam.version)
          : subject?.versions[subject.versions.length - 1];
      if (subject && version) schemaId = version.id;
    }
    let lastOffset = -1;
    for (let index = 0; index < count; index += 1) {
      lastOffset = topic.partitions[partition]?.length ?? 0;
      const message = attachSchema(
        makeMessage(topicName, partition, lastOffset, count > 1 ? `${key}-${index}` : key, value, headers),
        schemaParam?.subject ? { subject: schemaParam.subject, version: schemaParam.version } : undefined,
      );
      if (schemaId !== undefined && message.schemaId === undefined) message.schemaId = schemaId;
      topic.partitions[partition]?.push(message);
    }
    result = { partition, offset: lastOffset, timestamp: Date.now() };
  } else if (method === "kafka/messages/consume") {
    const topic = requireTopic(String(input.topic ?? ""));
    const schemaParam = input.schema as { subject: string; version?: number } | undefined;
    rejectGlueSchemaAttach(schemaParam);
    const scan = scanTopic(topic, input);
    result = {
      messages: schemaParam?.subject ? scan.messages.map((message) => attachSchema(message, schemaParam)) : scan.messages,
      scanned: scan.scanned,
      matched: scan.messages.length,
      limited: scan.messages.length >= Number(input.limit ?? (bigMode ? 5000 : 100)),
      hasMore: false,
      nextPartitionOffsets: {},
    };
  } else if (method === "kafka/messages/export") {
    const topic = requireTopic(String(input.topic ?? ""));
    const scan = scanTopic(topic, input);
    const format = String(input.format ?? "json");
    if (format === "csv") {
      const lines = ["topic,partition,offset,timestamp,key,value,headers"];
      for (const message of scan.messages) {
        lines.push([message.topic, String(message.partition), String(message.offset), String(message.timestamp), message.key ?? "", message.valueText ?? "", JSON.stringify(message.headers ?? {})].map(quoteCsv).join(","));
      }
      result = { content: lines.join("\r\n"), filename: "kafka-messages.csv", contentType: "text/csv" };
    } else {
      result = {
        content: JSON.stringify(
          scan.messages.map((message) => ({ topic: message.topic, partition: message.partition, offset: message.offset, timestamp: message.timestamp, key: message.key, value: message.valueText, headers: message.headers ?? {} })),
          null,
          2,
        ),
        filename: "kafka-messages.json",
        contentType: "application/json",
      };
    }
  } else if (method === "kafka/stream/start") {
    const topicName = String(input.topic ?? "");
    requireTopic(topicName);
    if (readOnly && input.commit === true) guardWrite("stream/start", topicName);
    const schemaParam = input.schema as { subject: string; version?: number } | undefined;
    rejectGlueSchemaAttach(schemaParam);
    const session = startStream(topicName, schemaParam);
    result = { sessionId: session.id };
  } else if (method === "kafka/stream/stop") {
    stopStream(input.all === true ? "all" : typeof input.sessionId === "string" ? input.sessionId : undefined);
    result = { success: true };
  } else if (method === "kafka/stream/pause" || method === "kafka/stream/resume") {
    const session = streams.get(String(input.sessionId ?? ""));
    if (!session) throw new Error("stream session not found (fixture)");
    session.paused = method === "kafka/stream/pause";
    result = {
      status: { paused: session.paused, totalScanned: session.produced, totalMatched: session.produced, bufferSize: session.buffer.length, partitionOffsets: { "0": session.buffer.length } },
    };
  } else if (method === "kafka/stream/status") {
    const session = streams.get(String(input.sessionId ?? ""));
    if (!session) throw new Error("stream session not found (fixture)");
    result = {
      status: { paused: session.paused, totalScanned: session.produced, totalMatched: session.produced, bufferSize: session.buffer.length, partitionOffsets: { "0": session.buffer.length } },
    };
  } else if (method === "kafka/stream/messages") {
    const session = streams.get(String(input.sessionId ?? ""));
    if (!session) throw new Error("stream session not found (fixture)");
    const offset = Number(input.offset ?? 0) || 0;
    const limit = Number(input.limit ?? 100) || 100;
    result = { messages: session.buffer.slice(offset, offset + limit), total: session.buffer.length };
  } else if (method === "kafka/presets/list") {
    result = { presets: readMockPresets() };
  } else if (method === "kafka/presets/save") {
    const presets = readMockPresets();
    const incoming = (input.preset ?? {}) as Record<string, unknown>;
    const index = presets.findIndex((preset) => (preset as Record<string, unknown>).id === incoming.id);
    if (index >= 0) presets[index] = incoming;
    else presets.push(incoming);
    writeMockPresets(presets);
    // 照真实 sidecar main.go presetsSave 返回操作结果（而非全量列表）。
    result = { success: true, preset: incoming };
  } else if (method === "kafka/presets/remove") {
    const id = String(input.id ?? "");
    const presets = readMockPresets().filter((preset) => preset.id !== id);
    writeMockPresets(presets);
    result = { success: true };
  } else if (method === "kafka/connections/statuses") {
    result = {
      statuses: [
        {
          connectionId: String(context.connectionId),
          status: "connected",
          readOnly,
          allowDelete,
          lastUsedAt: Date.now(),
          schemaRegistry: {
            enabled: true,
            url: glueOnly ? "glue://us-east-1/dbx-kafka-registry (fixture)" : "http://schema-registry:8081 (fixture)",
            provider: defaultSrProvider,
          },
          kerberos: { enabled: false },
          connectionSource: "kafka",
        },
        {
          // schema_registry=none 的对照行：SR 徽标应显示「未启用」（srOff）。
          connectionId: "visual-connection-nosr",
          status: "idle",
          readOnly: true,
          allowDelete: false,
          schemaRegistry: { enabled: false, provider: "none" },
          kerberos: { enabled: false },
          connectionSource: "kafka",
        },
      ],
    };
  } else if (method === "kafka/schema/test") {
    // Phase P：按 registry 参数探测（glue-only 模式下 confluent 返回 none）。
    const wanted = resolveProvider(input);
    result =
      wanted === "confluent" && glueOnly
        ? { success: false, provider: "none" }
        : { success: true, provider: wanted };
  } else if (method === "kafka/schema/subjects/list") {
    const wanted = resolveProvider(input);
    result = {
      subjects: providerSubjects(wanted).map((subject) => ({
        subject: subject.subject,
        formats: subject.formats,
        latestVersion: subject.versions.length,
        compatibilityLevel: subject.compatibilityLevel,
      })),
    };
  } else if (method === "kafka/schema/versions/list") {
    const subject = findSubject(String(input.subject ?? ""), resolveProvider(input));
    if (!subject) throw new Error("schema subject not found (fixture)");
    result = { versions: subject.versions.map((version) => ({ version: version.version, id: version.id, format: version.format })) };
  } else if (method === "kafka/schema/get") {
    const subject = findSubject(String(input.subject ?? ""), resolveProvider(input));
    if (!subject) throw new Error("schema subject not found (fixture)");
    const wanted = typeof input.version === "number" ? input.version : subject.versions.length;
    const version = subject.versions.find((entry) => entry.version === wanted);
    if (!version) throw new Error("schema version not found (fixture)");
    result = { subject: subject.subject, version: version.version, id: version.id, schema: version.schema, format: version.format, references: [] };
  } else if (method === "kafka/schema/versions/compare") {
    const subject = findSubject(String(input.subject ?? ""), resolveProvider(input));
    if (!subject) throw new Error("schema subject not found (fixture)");
    result = diffSchemaVersions(subject, Number(input.fromVersion ?? 1), Number(input.toVersion ?? 1));
  } else if (method === "kafka/schema/compatibility/get") {
    const subjectName = typeof input.subject === "string" ? String(input.subject) : "";
    const subject = subjectName ? findSubject(subjectName, resolveProvider(input)) : undefined;
    result = subject ? { level: subject.compatibilityLevel, scope: "SUBJECT" } : { ...globalCompatibility };
  } else if (method === "kafka/schema/compatibility/set") {
    guardWrite("schema/compatibility/set", String(input.subject ?? "(global)"));
    const level = String(input.level ?? "NONE");
    const subjectName = typeof input.subject === "string" ? String(input.subject) : "";
    if (subjectName) {
      const subject = findSubject(subjectName, resolveProvider(input));
      if (!subject) throw new Error("schema subject not found (fixture)");
      subject.compatibilityLevel = level;
    } else {
      globalCompatibility = { level, scope: "GLOBAL" };
    }
    result = { level, scope: subjectName ? "SUBJECT" : "GLOBAL" };
  } else if (method === "kafka/schema/compatibility/check") {
    const subject = findSubject(String(input.subject ?? ""), resolveProvider(input));
    if (!subject) throw new Error("schema subject not found (fixture)");
    // fixture 级检查：候选 schema 能解析为 JSON 即视为向后兼容（不做 avro 真校验）。
    const messages: string[] = [];
    let isCompatible = true;
    try {
      JSON.parse(String(input.schema ?? ""));
      messages.push("no incompatible changes detected (fixture)");
    } catch (cause) {
      isCompatible = false;
      messages.push(`schema is not valid JSON: ${cause instanceof Error ? cause.message : String(cause)}`);
    }
    result = { isCompatible, messages };
  } else if (method === "kafka/schema/register") {
    const subjectName = String(input.subject ?? "");
    const format = String(input.format ?? "avro") as "avro" | "json" | "protobuf";
    const schemaText = String(input.schema ?? "");
    const provider = resolveProvider(input);
    guardWrite("schema/register", subjectName);
    // PROTOBUF 注册体是 proto3 文本（非 JSON）；AVRO/JSON 才做 JSON 语法校验。
    if (format !== "protobuf") {
      try {
        JSON.parse(schemaText);
      } catch (cause) {
        throw new Error(`schema is not valid JSON: ${cause instanceof Error ? cause.message : String(cause)}`);
      }
    }
    let subject = findSubject(subjectName, provider);
    if (!subject) {
      subject = { subject: subjectName, formats: [format], versions: [], compatibilityLevel: globalCompatibility.level, provider };
      schemaSubjects.set(subjectName, subject);
    }
    const version = makeSchemaVersion(subject, format, schemaText);
    subject.versions.push(version);
    if (!subject.formats.includes(format)) subject.formats.push(format);
    result = { id: version.id, version: version.version };
  } else if (method === "kafka/schema/delete") {
    const subjectName = String(input.subject ?? "");
    guardWrite("schema/delete", subjectName, { critical: true });
    const subject = findSubject(subjectName, resolveProvider(input));
    result = { success: subject ? schemaSubjects.delete(subjectName) : false };
  } else if (method === "kafka/schema/delete/version") {
    const subjectName = String(input.subject ?? "");
    guardWrite("schema/delete/version", subjectName, { critical: true });
    const subject = findSubject(subjectName, resolveProvider(input));
    if (!subject) throw new Error("schema subject not found (fixture)");
    const wanted = Number(input.version ?? 0);
    const before = subject.versions.length;
    subject.versions = subject.versions.filter((entry) => entry.version !== wanted);
    result = { success: subject.versions.length < before };
  } else if (method === "kafka/ui/state/report") {
    // MCP UI intent 回报（M3）：镜像 sidecar 校验——带 intentId 时 status
    // 必须是 applied|rejected；无 intentId 为快照型（恒 success）。
    const status = String(input.status ?? "");
    const intentId = String(input.intentId ?? "").trim();
    if (intentId && status !== "applied" && status !== "rejected") throw new Error("status must be applied or rejected");
    result = { success: true };
  }
  // kafka/audit 不在路由里：它是 sidecar→宿主的事件通道（emitter.Event），
  // mock 侧同样经 emitEvent 注入，方法契约（methodContract.json）只管方法面。
  return result as T;
};

function quoteCsv(value: string): string {
  if (/[",\n\r]/.test(value)) return `"${value.replace(/"/g, '""')}"`;
  return value;
}

// window.dbxPlugin 组装（与宿主 Host API 1.2 面一致；invoke ?? request 双方法，
// capabilities/storage 为 1.2 新增，mock 镜像真实桥 storage 命名空间同形）。
window.dbxPlugin = {
  ready: Promise.resolve(context),
  context,
  appearance,
  theme,
  locale: params.get("locale") || "zh-CN",
  request,
  invoke,
  notify: async () => undefined,
  sendBinary: async () => undefined,
  onEvent: (listener) => {
    eventListeners.add(listener);
    return () => eventListeners.delete(listener);
  },
  onBinary: () => () => undefined,
  onAppearanceChange: (listener) => {
    appearanceListeners.add(listener);
    listener(appearance);
    return () => appearanceListeners.delete(listener);
  },
  onLocaleChange: () => () => undefined,
  onContextChange: (listener) => {
    contextListeners.add(listener);
    listener(context);
    return () => contextListeners.delete(listener);
  },
  decodeBase64: (value) => Uint8Array.from(atob(value), (character) => character.charCodeAt(0)),
  encodeBase64: (value) => {
    const bytes = value instanceof Uint8Array ? value : new Uint8Array(value as ArrayBuffer);
    let binary = "";
    for (const byte of bytes) binary += String.fromCharCode(byte);
    return btoa(binary);
  },
  workbenchState: { set: async () => undefined },
  clipboard: { readText: async () => "", writeText: async () => undefined },
  // 宿主 host.storage mock（Host API 1.2，pluginHostBridge storage 命名空间同形）：
  // 与真实 web 宿主同形由 localStorage 兜底（键名不变；字符串值原样、对象 JSON
  // 编码），刷新/重开不丢；opaque origin 等不可用场景退化为内存 Map。
  // get 未命中返回 null，set(undefined) 归一化为 null。
  capabilities: { storage: true },
  storage: (() => {
    let ls: Storage | null = null;
    try {
      window.localStorage.setItem("__dbx_mock_storage_probe__", "1");
      window.localStorage.removeItem("__dbx_mock_storage_probe__");
      ls = window.localStorage;
    } catch {
      ls = null;
    }
    const mem = new Map<string, string>();
    const write = (key: string, value: unknown) => {
      const raw = typeof value === "string" ? value : JSON.stringify(value);
      if (ls) ls.setItem(key, raw);
      else mem.set(key, raw);
    };
    const read = (key: string): unknown => {
      const raw = ls ? ls.getItem(key) : (mem.get(key) ?? null);
      if (raw === null) return null;
      try {
        const parsed = JSON.parse(raw);
        return parsed !== null && typeof parsed === "object" ? parsed : raw;
      } catch {
        return raw;
      }
    };
    return {
      get: async (key: string) => read(key),
      set: async (key: string, value: unknown) => {
        write(key, value === undefined ? null : value);
        return null;
      },
      delete: async (key: string) => {
        if (ls) ls.removeItem(key);
        else mem.delete(key);
        return null;
      },
    };
  })(),
  // 宿主另存为桥（桌面端 v0.6.14+）：mock 镜像宿主 Web 模式行为——宿主页
  // 发起 anchor 下载并回 { path }；?nosave=1 时不挂该成员模拟旧宿主（导出
  // 落到 lib/download 的 Blob 网页下载兜底）。
  ...(params.get("nosave") === "1"
    ? {}
    : {
        saveFile: async (options: { fileName?: string; contentType?: string }, data: Uint8Array | ArrayBuffer | string): Promise<{ path: string } | null> => {
          const bytes =
            typeof data === "string"
              ? Uint8Array.from(atob(data), (character) => character.charCodeAt(0))
              : data instanceof Uint8Array
                ? data
                : new Uint8Array(data);
          const blob = new Blob([new TextDecoder().decode(bytes)], { type: `${options.contentType || "application/octet-stream"};charset=utf-8` });
          const url = URL.createObjectURL(blob);
          const anchor = document.createElement("a");
          anchor.href = url;
          anchor.download = options.fileName || "download.bin";
          anchor.click();
          window.setTimeout(() => URL.revokeObjectURL(url), 10_000);
          return { path: options.fileName || "download.bin" };
        },
      }),
};

// 走查注入：ui_test 经 window.dbxPlugin.emitKafkaUiIntent 发 kafka/ui/intent
// （vitest 直接走模块导出）。mock 专用钩子不属于宿主桥契约面，用
// Object.assign 挂载避免污染 DbxPluginApi 类型（函数声明提升，此处引用安全）。
Object.assign(window.dbxPlugin, { emitKafkaUiIntent });

export { context, appearance };

/** 测试/走查注入：按 sidecar `kafka/ui/intent` 事件形状发一条 intent
 * （mock 与真实 emitter.Event 同面；useUiIntent 消费后回报
 * kafka/ui/state/report）。 */
export function emitKafkaUiIntent(message: { intentId: string; action: string; params?: Record<string, unknown> }) {
  emitEvent("kafka/ui/intent", {
    intentId: message.intentId,
    action: message.action,
    params: message.params ?? {},
  });
}

// -- audit 链夹具（?audit=denied）--------------------------------------------------
// 宿主 onEvent 监听就绪后再注入（App.initialize 挂监听前的事件会丢失）：
// 先 denied（触发 AuditFeed 自动展开 + denied 徽标 + 错误横幅），后 ok（对照行）。
if (auditDeniedInject) {
  const poll = window.setInterval(() => {
    if (eventListeners.size === 0) return;
    window.clearInterval(poll);
    window.setTimeout(() => {
      emitEvent("kafka/audit", {
        connectionId: context.connectionId,
        action: "topics/delete",
        target: "order-events",
        result: "denied",
        detail: "connection is read-only (fixture audit injection)",
      });
    }, 200);
    window.setTimeout(() => {
      emitEvent("kafka/audit", {
        connectionId: context.connectionId,
        action: "messages/produce",
        target: "order-events",
        result: "ok",
      });
    }, 900);
  }, 100);
  // 兜底：宿主一直不挂监听就放弃注入，避免孤儿 interval。
  window.setTimeout(() => window.clearInterval(poll), 15000);
}
