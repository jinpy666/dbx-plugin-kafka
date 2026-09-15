# Kafka Studio 插件协议文档（PROTOCOL_KAFKA）

> 版本基准：kafka v0.2.0（Phase 1 + Phase 2 商用化）。素材来源与唯一权威：
> `docs/IMPL_PLAN_DBX_KAFKA.zh-CN.md` §0.2/§5（实现必须与本文件同步；
> 按 AGENTS.md 规则 2，**新增/变更方法必须同步本文档**）。

## 1. 公共约定

- **传输**：stdio-jsonl —— SDK 帧协议，stdin/stdout 每行一条 JSON-RPC 2.0
  消息（`\n` 结尾），stderr 为诊断通道。
- **方法命名**：`<域>/<动作>`（如 `kafka/topics/list`）。
- **字段命名**：camelCase。
- **connectionId 必填**：除生命周期三方法与事件外，所有领域方法的
  `params.connectionId` 必填；宿主按 workbench 上下文注入。缺失或未知
  连接 → `-32602` / 业务错。
- **错误码**：
  | 码 | 语义 | 例 |
  | --- | --- | --- |
  | `-32602` | 参数错（缺字段/类型不符/取值非法） | `connectionId` 缺失、`partitions` 非法、consume 参数互斥冲突 |
  | `-32000` | 业务错（PluginError） | 连接失败、broker 拒绝、**策略拒绝（blocked）**、未知连接、SR 不可达、ZK 不可达 |
  | `-32601` | 方法未注册/未实现 | 并行开发期，smoke 对其 SKIP 而非 FAIL |
- **响应形状**：成功统一 `{ "ok": true, "data": <payload> }`；失败走
  PluginError（JSON-RPC `error`，`message` + `code`，`data` 附加上下文）。
  下文各方法只描述 `data` 载荷。
- **凭据红线**：`sasl_password`、`tls_client_key`、`sr_password`、
  `glue_secret_access_key`/`glue_session_token`、`msk_secret_access_key`/
  `msk_session_token`/`oauth_static_token` 等 secret binding 字段不落日志、
  不进审计 result、不回显。

## 2. 生命周期方法（SDK/ldap 同款）

| 方法 | 请求 | 返回（data） | 错误语义 |
| --- | --- | --- | --- |
| `connection/test` | `provider{id,databaseType:"kafka"}`、`connection{id,name,external_config,connection_secrets}`、`runtime{host,port}` | `{success:true}` | 拨号/认证失败 → `-32000`；连接参数缺失/非法 → `-32602` |
| `connection/connect` | 同上 | `{success:true}` | 同上；成功后按 `connectionId` 缓存 client（指纹失效重建） |
| `connection/disconnect` | `connection{id}` | `{success:true}` | 释放缓存 client 与流式会话 |

配置字段来源：`external_config` 为 manifest config binding 字段
（bootstrap_servers、security_protocol、sasl_*、tls_*、client_id、
read_only、allow_delete，以及 Phase 2 的 connection_source、zk_servers、
kerberos_* 、sr_url、sr_username，Phase 3 的 schema_registry、glue_*、
oauth_token_source、msk_region、msk_access_key_id），`connection_secrets`
为 secret binding 字段（sasl_password、tls_client_key、sr_password、
glue_secret_access_key、glue_session_token、msk_secret_access_key、
msk_session_token、oauth_static_token）。

## 3. 领域方法

### 3.1 brokers

**`kafka/brokers/list`**

- 请求：无附加字段（`connectionId` 必填）。
- 返回：`brokers[]{nodeId:int, host:string, port:int, rack:string?}`、
  `connectionSource:string`（`bootstrap` = Kafka metadata；`zookeeper` =
  经 ZK `/brokers/ids` 发现，Phase 2）。
- 错误：连接不可用 → `-32000`；`connection_source=zookeeper` 时 ZK 不可达 /
  `/brokers/ids` 为空 → `-32000`（不崩溃）。
- ZK 模式（Phase 2）：`zk_servers` 支持 `host:port[/chroot]`（chroot 由
  zk 客户端自动前缀）；`zk_servers` 未配置 → `-32000`。

**`kafka/brokers/config`**

- 请求：`brokerId:int`（必填）。
- 返回：`entries[]{name, value:string, source, sensitive:bool, isDefault:bool}`；
  `sensitive=true` 的条目 value 以掩码返回。
- 错误：`brokerId` 缺失 → `-32602`；broker 不存在 → `-32000`。

### 3.2 topics

**`kafka/topics/list`**

- 请求：`includeInternal?:bool`（默认 false，隐藏 `__consumer_offsets` 等内部 topic）。
- 返回：`topics[]{name, topicId:string?, isInternal:bool, partitionCount:int, replicationFactor:int, isHealthy:bool, unhealthyPartitions:int, error?:string}`。
  Phase 3 增量（additive 无开关）：`isHealthy` = 全分区健康（分区判定与
  topics/describe 同款：leader 有效 + ISR 覆盖 replicas + 无 offline）；
  `unhealthyPartitions` = 不健康分区数；topic 元数据加载失败时
  `isHealthy=false` 且 `error` 非空。零额外请求（复用 list 元数据）。
- 错误：连接不可用 → `-32000`。

**`kafka/topics/describe`**

- 请求：`topic:string`（必填）。
- 返回：`partitions[]{partition:int, leader:int, leaderEpoch:int?, replicas:int[], isr:int[], offlineReplicas:int[], isHealthy:bool}`
  （isHealthy = ISR 覆盖 replicas 且无 offline）。
- 错误：`topic` 缺失 → `-32602`；topic 不存在 → `-32000`。

**`kafka/topics/create`**

- 请求：`topics:string[]`（≥1）、`partitions:int`（≥1）、
  `replicationFactor:int`（≥1，受集群 broker 数上限）、
  `config?:map<string,string>`（逐 topic 应用）。
- 返回：`results[]{topic:string, ok:bool, error?:string}`（逐条成败）。
- 错误：read_only → `-32000`（blocked）；参数非法 → `-32602`；审计记一条。

**`kafka/topics/delete`**（critical 门禁）

- 请求：`topics:string[]`、`confirmTopic:string`（必填，与被删 topic 同名的
  确认字段，防误删；任一条不匹配 → `-32602`）。
- 返回：`results[]{topic, ok, error?}`。
- 错误：read_only 或 `allow_delete=false` → `-32000`（blocked）；审计。

**`kafka/topics/records/clear`**（critical 门禁，Phase 3）

- 请求：`topic:string`（必填）、`confirmTopic:string`（必填，与 topic 同名，
  复用 topics/delete 单 topic 确认语义；不匹配 → `-32602`）。
- 语义：把各分区已可见记录清空——取每分区 high watermark 后
  `DeleteRecords(offset=hw)`（KIP-107，**最低 broker 版本 0.11**；旧 broker
  分区行内业务错透传）。清空后 latest==earliest；不影响删除后新写入。
- 返回：`rows[]{partition:int, deleted:long|null, lowWatermark:long|null,
  ok:bool, error?:string}`；`deleted` = 删除前 hw − 删除后 lowWatermark
  （任一段 offset 取不到 → 该行 `deleted`/`lowWatermark` 置 `null`，不影响
  其他分区；行按 partition 升序）。
- 错误：read_only 或 `allow_delete=false` → `-32000`（blocked）；
  `confirmTopic` 不匹配 → `-32602`；审计 action `topics.records.clear`，
  detail `partitions=N`。

**`kafka/topics/partitions/update`**

- 请求：`partitions:map<topic,int>`（新分区数，**只增**——小于当前值 → `-32602`）。
- 返回：`results[]{topic, ok, error?}`。
- 错误：read_only → `-32000`（blocked）；审计。

**`kafka/topics/config/get`**

- 请求：`topic:string`。
- 返回：`entries[]`（同 brokers/config 形状）。

**`kafka/topics/config/alter`**

- 请求：`topic:string`、`config:map<string,string>`（set）、
  `deleteKeys?:string[]`（还原默认）。
- 返回：`entries[]`（alter 后的当前配置）。
- 错误：read_only → `-32000`（blocked）；审计。

**`kafka/topics/offsets/list`**

- 请求：`topics:string[]`、`offsetTime?:string`
  （`earliest`(-2) | `latest`(-1) | `max-timestamp`(-3) | `log-start`(-4) |
  RFC3339 | unix 毫秒；默认 `latest`。Phase 2 全策略；负数整数是协议保留值
  会被拒绝）。
- 返回：`rows[]{topic, partition:int, offset:int, timestamp:int?, leaderEpoch:int?, error?:string}`
  （单分区失败在行上标 `error`，不整体失败）。
- 错误：时间解析失败 → `-32602`。

### 3.3 groups

**`kafka/groups/list`**

- 请求：无附加字段。
- 返回：`groups[]{group:string, state:string, protocolType:string, coordinator:int?}`。

**`kafka/groups/describe`**

- 请求：`group:string`。
- 返回：`members[]{memberId, instanceId?, clientId, clientHost, assignments:map<topic,int[]>}`。
- 错误：组不存在 → `-32000`。

**`kafka/groups/offsets/list`**

- 请求：`group:string`、`topics?:string[]`（空/缺省 = committed 全量）。
- 返回：`rows[]{topic, partition:int, startOffset:int, endOffset:int, committedOffset:int, lag:int}`、
  `totalLag:int`、`hasCommitted:bool`。
  **Option 语义**：组从未提交（`__consumer_offsets` 无记录）→
  `hasCommitted:false` 且 `rows` 为空，与"已提交且零 lag"明确区分。
- 错误：组不存在 → `-32000`。

**`kafka/groups/delete`**（critical 门禁）

- 请求：`group:string`。
- 返回：空 `data`。
- 错误：read_only 或 `allow_delete=false` → `-32000`（blocked）；审计。

**`kafka/groups/offsets/reset`**

- 请求：`group:string`、`topics:string[]`、
  `resetTo:"earliest"|"latest"|"timestamp"|"partitionOffset"`、
  `timestampMs?:int`（resetTo=timestamp 必填）、
  `partitionOffsets?:map<topic,map<partition,int>>`（resetTo=partitionOffset 必填；
  嵌套 topic 维度以支持多 topic 组；单 topic 场景可只写一个键）。
- 返回：`rows[]{topic, partition:int, ok:bool, error?}`。
- 错误：read_only → `-32000`（blocked）；组合参数缺失/冲突 → `-32602`；
  审计。

### 3.4 acls

**`kafka/acls/list`**

- 请求：`filter{}`（`resourceType?:topic|group|cluster|transactionalId|delegationToken|user`、
  `resourceName?`、`patternType?`、`principal?`、`host?`、`operation?`、
  `permissionType?`）。**过宽过滤拒绝**：filter 为空对象 → `-32602`
  （防全量枚举打爆 controller）。
- 返回：`acls[]{resourceType, resourceName, patternType, principal, host, operation, permission}`。

**`kafka/acls/create`**

- 请求：`acl{resourceType, resourceName, patternType, principal, host, operation, permission}`（全必填）。
- 返回：空 `data`。
- 错误：read_only → `-32000`（blocked）；字段非法 → `-32602`；审计。

**`kafka/acls/delete`**（critical 门禁）

- 请求：`filter{}`（同 list 的形状，同样拒绝过宽）。
- 返回：`matched[]{...同 acl 条目}`（删除前匹配到的条目）。
- 错误：read_only 或 `allow_delete=false` → `-32000`（blocked）；审计。

### 3.5 messages

**`kafka/messages/produce`**

- 请求：`topic:string`、`key?:string`、`value:string`（必填，与
  `valueBase64` **二选一**，同给 → `-32602`）、`keyBase64?:string`
  （二进制 key 保真，与 `key` 二选一）、`headers?:map<string,string>`、
  `partition?:int`、`count?:int`（批量条数，≤1000，默认 1）、
  `compression?:"gzip"|"lz4"|"zstd"|"snappy"`、
  `acks?:"all"|"1"`（投递确认级别，缺省 `all`；`1` = 仅 leader 确认。
  **`0` 不支持**：生产为同步 ProduceSync 语义，客户端 promise 依赖 broker
  响应，acks=0 会拖到请求超时才失败 → 传 `0`/`none` 返回 `-32602`）、
  `enableIdempotence?:bool`（幂等生产 / Kafka 服务端去重，缺省 `true` =
  客户端默认幂等开；`false` 显式关闭。`acks=1` 必须配合
  `enableIdempotence=false`，否则 `-32602`）、
  `schema?:{subject, version?:int, format?:"avro"|"json"|"protobuf"}`（Phase 2
  avro/json；Phase 3 增 protobuf）。
- **schema 挂载语义**（Phase 2）：提供 `schema` 时 `value`/`valueBase64`
  是**未编码载荷**（Avro = JSON 文本；JSON = JSON 文本；PROTOBUF = JSON
  文本，protojson 语义），sidecar 按 SR 元数据（subject + version，缺省
  latest）把载荷编码为 Avro 二进制 / 校验后的 JSON / protobuf 二进制
  （protojson → 动态消息），并打包 Confluent wire format
  （magic byte 0 + 4 字节大端 schemaID + 载荷）后生产。SR 未配置（sr_url
  空）或元数据不存在 → `-32000`。
  **PROTOBUF wire framing**：schemaID 与 protobuf 载荷之间还有 Confluent
  message index 数组段——目标 message 的声明序号路径（顶层序号 → 逐级嵌套
  序号）各以一个 varint 紧密拼接，无长度前缀（单顶层 message = 单字节
  `0x00`）；message 消歧沿用 subject 约定（剥 `-key`/`-value` 后缀取尾段
  PascalCase 唯一命中，否则报错列出候选）。AVRO/JSON 无此段；消费侧对
  早期版本漏写该段的历史记录自动回退按原始载荷解码。
- 返回：`{partition:int, offset:int, timestamp:int}`（首条消息定位；
  count>1 时为末条 offset）。
- 错误：read_only → `-32000`（blocked）；count 超限 / acks 取值非法 /
  acks=1 与幂等冲突 → `-32602`；
  topic 不存在且未自动创建 → `-32000`；审计。

**`kafka/messages/consume`**

- 请求：`ConsumeParams`（见 §4，`topic` 必填；Phase 2 新增 `schema?`）。
- **schema 解码语义**（Phase 2）：提供 `schema` 时按 Confluent wire format
  解包（魔数字节 0 + schemaID），解析 SR 元数据（指定 `subject` 时按
  subject+version 取，否则按 schemaID 反查 `/schemas/ids/<id>`），把载荷
  解码为 JSON 文本（Avro 二进制 → JSON；JSON 透传校验；PROTOBUF →
  剥 message index 段 → 动态消息 → protojson 渲染，Phase 3；对早期版本
  漏写 index 段的历史记录自动回退按原始载荷解码）。命中消息附加
  `schemaId`、`schemaSubject`、`schemaVersion` 字段；解码失败**不中断
  消费**，置 `decodeError`。元数据按 schemaID / subject+version 在本次
  消费（或流式会话）内缓存，同 ID 只请求一次 SR。
- 返回：`{messages:MessageView[], scanned:int, matched:int, limited:bool, hasMore:bool, nextPartitionOffsets:map<partition,int>}`
  （`nextPartitionOffsets` 可作续读游标）。
- 错误：参数互斥冲突（见 §4）→ `-32602`；连接不可用 → `-32000`。

**`kafka/messages/export`**

- 请求：`ConsumeParams` + `format:"json"|"csv"`、`limit?:int`（≤10000，默认 1000）。
- 返回：`{content:string, filename:string, contentType:string}`；
  filename 形如 `dbx-kafka-<topic>-<yyyyMMdd-HHmmss>.json|.csv`。
  宿主 1.0 无 save-file 能力，前端以 Blob URL 下载。
- 错误：format 非法 / limit 超限 → `-32602`。

### 3.6 stream（流式消费会话）

**`kafka/stream/start`**

- 请求：`ConsumeParams`（同一次性消费）。
- 返回：`{sessionId:string}`。
- 会话上限 20，超出 → `-32000`；参数互斥冲突 → `-32602`。

**`kafka/stream/stop`**

- 请求：`sessionId?:string`（与 `all:true` 二选一；都没有 → `-32602`）。
- 返回：空 `data`。

**`kafka/stream/pause` / `kafka/stream/resume`**

- 请求：`sessionId:string`。
- 返回：`{status:string}`（`"paused"` / `"running"`）。
- 错误：未知 sessionId → `-32000`。

**`kafka/stream/status`**

- 请求：`sessionId:string`。
- 返回：`{paused:bool, totalScanned:int, totalMatched:int, bufferSize:int, partitionOffsets:map<partition,int>}`。

**`kafka/stream/messages`**（方法，非事件）

- 请求：`sessionId:string`、`offset:int`、`limit:int`（ring buffer 历史分页）。
- 返回：`{messages:MessageView[], total:int, offset:int}`。

### 3.7 presets 与连接状态（照 ldap/presets、ldap/connections/statuses 形态）

**`kafka/presets/list`** — 无附加字段；返回 store 持久化的消费/过滤预设列表。

**`kafka/presets/save`** — `preset{id?, name, payload}`；返回 `{id}`。新增落 store。

**`kafka/presets/remove`** — `id:string`；返回空 `data`；未知 id → `-32000`。

**`kafka/connections/statuses`** — 无附加字段；返回 `statuses[]`，每条含
`{connectionId, name, bootstrap, status, readOnly, connectedAt?, lastUsedAt?, error?}`，
以及 Phase 2/3 摘要：`connectionSource`（`bootstrap`|`zookeeper`）、
`schemaRegistry:{enabled:bool, provider:"confluent"|"glue"|"both"|"none",
mode?:"none"|"confluent"|"aws_glue", url?, registryName?}`（凭据不出；
`mode` 为连接表单 `schema_registry` 开关的归一值，旧连接未设开关时省略、
provider 来自自动探测；provider=both 仅出现在旧连接双配置、方法层要求显式
registry；开关=none 或未配置任何 SR 时 enabled=false 且 provider=none）、
`kerberos:{enabled:bool}`。

### 3.8 schema registry（Phase 2 Confluent 兼容 REST + Phase 3 AWS Glue）

**范围**：Confluent 兼容 REST（含 Redpanda 内置 SR）与 AWS Glue Schema
Registry 管理面（Phase 3，见文末 **Phase 3（AWS Glue）** 小节）。所有方法
`connectionId` 必填；未配置任何 SR → `-32000`
（"schema registry is not configured"）。wire format：magic byte 0 +
4 字节大端 schemaID + 载荷。`format` 取值 `avro` | `json` | `protobuf`
（大小写不敏感；SR 返回大写 `AVRO`/`JSON`/`PROTOBUF`）。

**PROTOBUF 编解码（Phase 3）**：SR PROTOBUF subject 的 `schema` 字段 =
`base64(FileDescriptorSet)`（SR 元数据事实）。解码：base64 → FDSet →
`protodesc` 动态描述符 → `dynamicpb` 动态消息 → protojson 渲染 JSON
（proto 字段名；int64/枚举的 JSON 表示与 Confluent 序列化器存在已知差异，
登记不强行对齐）。编码：protojson 反序列化 → protobuf 二进制 → wire frame。
**message 消歧**（FDSet 含多 message 时，按序）：
1. FDSet 恰含 1 个 message → 用之；
2. subject 约定匹配：剥 `-key`/`-value` 后缀取尾段，PascalCase 化后与
   message 全名尾段严格相等（如 subject `order-value` → `Order`），
   唯一命中 → 用之；
3. 仍无法唯一 → 报错列出候选 message 全名（consume 进 `decodeError`
   不中断消费；produce → `-32000`）。

**`kafka/schema/test`**

- 请求：无附加字段。
- 返回：`{ok:true, version:string, compatibleFormats:["AVRO","JSON"], subjectCount:int}`。
  `version` 为 Redpanda 内置 SR 的 `/v1/metadata/id` 版本（Confluent 无此
  端点时为空串）；`subjectCount` 为 subject 总数。
- 错误：SR 不可达 → `-32000`。

**`kafka/schema/subjects/list`**

- 请求：无附加字段。
- 返回：`subjects[]{subject:string, formats:string[], latestVersion:int?,
  compatibilityLevel?:string}`（compatibilityLevel 为该 subject 级别覆盖值，
  未覆盖时省略）。subject 按字典序。

**`kafka/schema/versions/list`**

- 请求：`subject:string`（必填，缺失 → `-32602`）。
- 返回：`versions[]{version:int, id:int, format:string}`（version 升序）。

**`kafka/schema/get`**

- 请求：`subject:string`、`version?:int`（缺省/0 = latest）。
- 返回：`{subject, version:int, id:int, schema:string, format:string,
  references?:[{name, subject, version}]}`。

**`kafka/schema/versions/compare`**

- 请求：`subject:string`、`fromVersion:int`、`toVersion:int`（均 ≥1，
  缺失 → `-32602`）。
- 返回：`{subject, from:int, to:int, hunks:[], summary:{}}`。
  `hunks[]{op:"add"|"remove"|"modify", path:string, before?:string, after?:string}`
  —— LCS 逐行 diff，相邻删除+新增归并为 `modify` 块（before/after 为
  换行拼接的行文本），`path` 为 `line <N>`（before 行号）；
  `summary{added:int, removed:int, unchanged:int, beforeLines:int, afterLines:int}`。

**`kafka/schema/compatibility/get`**

- 请求：`subject?:string`（空 = 全局级别）。
- 返回：`{level:string, scope:"global"|"subject"}`。

**`kafka/schema/compatibility/set`**（写，过 read_only）

- 请求：`subject?:string`（空 = 全局）、`level:string`
  （`BACKWARD` / `BACKWARD_TRANSITIVE` / `FORWARD` / `FORWARD_TRANSITIVE` /
  `FULL` / `FULL_TRANSITIVE` / `NONE`，大小写不敏感；`*_ALL` 归并为
  `*_TRANSITIVE`）。
- 返回：`{level:string, scope:string}`（level 为 SR 回读值）。
- 错误：read_only → `-32000`（blocked）；审计。

**`kafka/schema/compatibility/check`**

- 请求：`subject:string`、`format:string`、`schema:string`、
  `version?:int`（缺省 = latest）、`references?:[{name, subject, version}]`。
- 返回：`{isCompatible:bool, messages:string[]}`。

**`kafka/schema/register`**（写，过 read_only；非 allow_delete 级）

- 请求：`subject:string`、`format:string`、`schema:string`、`references?`、
  `normalize?:bool`（缺省 `false`；`true` 时以
  `POST /subjects/{subject}/versions?normalize=true` 注册，由 SR 归一化
  存储文本——仅 confluent 后端支持）。幂等：SR 对重复 schema 返回既有
  id。create/update 同一方法：为新 subject 注册即建第一版，为已有
  subject 注册即追加新版本（前端"克隆"= 用选中版本内容预填注册表单，
  无独立后端方法）。
- 返回：`{id:int, version:int, versionId?:string}`（version 为注册后
  latest；回读失败时为 0；`versionId` 仅 glue 后端填充）。
- 错误：read_only → `-32000`（blocked）；SR 拒绝（schema 无效/
  兼容性不过）→ `-32000`；glue 后端 + `normalize=true` → `-32000`
  （明确不支持报错，不静默忽略）；审计。

**`kafka/schema/delete`**（critical：过 allow_delete + read_only）

- 请求：`subject:string`。
- 返回：`{deletedVersions:int[]}`（软删除的全部版本号）。

**`kafka/schema/delete/version`**（critical：同上）

- 请求：`subject:string`、`version:int`（缺失 → `-32602`）。
- 返回：`{deletedVersions:int[]}`。
- 错误：read_only 或 `allow_delete=false` → `-32000`（blocked）；审计。

**Phase 3（AWS Glue）**

**registry 参数与自动探测**：全部 `kafka/schema/*` 方法请求增加可选
`registry?: "confluent"|"glue"`；缺省自动探测：`sr_url` 非空 → confluent，
`glue_region` + `glue_registry_name` 非空 → glue；**两者都配置且未显式指定
→ `-32602`** 要求显式 registry。`kafka/schema/test` 返回增加
`provider:"confluent"|"glue"`（连接未配置任何 SR 时仍为 `-32000` 业务错；
`"none"` 值仅出现在 `kafka/connections/statuses` 的 `schemaRegistry.provider`）。

**Glue manifest 字段**（与 `sr_url` 平级，留空即不启用；无 visible_when）：
`glue_region`（text）、`glue_registry_name`（text）、`glue_auth_mode`
（select：`default`|`static`，默认 `default`）、`glue_access_key_id`（text，
static 时使用）、`glue_secret_access_key`（password，**secret** binding）、
`glue_session_token`（password，**secret** binding，可选）。auth_mode 归一化
取值 `default|static`；aws-profile 凭据模式 sidecar 场景不提供
（Phase 3 后续），传 `profile` → `-32000`。

**Glue 后端语义**（Glue API 集合：ListSchemas / ListSchemaVersions /
GetSchema / GetSchemaVersion / RegisterSchemaVersion / CreateSchema /
UpdateSchema(Compatibility) / CheckSchemaVersionValidity /
DeleteSchemaVersions / DeleteSchema）：

- **subjects/list** = ListSchemas（分页聚合，上限 1000）。行形状同
  confluent；Glue 列表级无 DataFormat/版本/兼容级别 → `formats` 为空数组、
  `latestVersion`/`compatibilityLevel` 省略，`description` 为 ListSchemas
  原生字段。
- **versions/list** = ListSchemaVersions（分页聚合）+ 一次 GetSchema 取
  DataFormat。行 `id` 恒为 **0**（Glue 无数字 schemaID），`versionId` 为
  Glue GUID（`versionId?:string`，confluent 无此字段）。
- **get**：version≤0 先 GetSchema 取 LatestSchemaVersion 再
  GetSchemaVersion；返回同上（`id:0` + `versionId`）。
- **versions/compare**：拉两个版本的 schema 文本走既有 LCS diff，形状不变。
- **compatibility/get|set**：Glue 为 **per-schema** 级别、无全局——
  subject 为空 → `-32000`（"per-schema; no global level"）。set = UpdateSchema
  （需版本检查点：请求可选 `version`，缺省取 LatestSchemaVersion），返回
  回读值。兼容级别取值面归一：`NONE`/`DISABLED`/`BACKWARD`/`BACKWARD_ALL`/
  `FORWARD`/`FORWARD_ALL`/`FULL`/`FULL_ALL`（confluent 的 `*_TRANSITIVE`
  输入归并为 `*_ALL`）；未知值 → `-32000`。
- **compatibility/check** = CheckSchemaVersionValidity：**纯语法有效性校验**
  （无副作用、不做注册兼容判定，注册时由 Glue 强制）。`isCompatible`=
  Glue `Valid`；`messages[0]` 恒为无副作用说明，校验失败时附 Glue 错误。
  `version`/`references` 不参与。
- **register**：先 GetSchema 探测——schema 不存在 → CreateSchema（首个
  版本；请求可选 `compatibility` 指定初始级别，缺省 `NONE`；`references`
  为 Confluent 概念、Glue 忽略）；存在 → RegisterSchemaVersion（幂等：
  Glue 对重复定义返回既有版本）。返回 `{id:0, version, versionId}`。
  `normalize=true` → `-32000`（Glue 无归一化语义，明确报错不静默忽略，
  且不发起任何 Glue 调用）。
- **delete/version** = DeleteSchemaVersions（单版本区间）；返回
  `{deletedVersions:[version]}`（SDK 未建模被删版本号清单，按错误清单折算，
  Glue 报版本删除错误 → `-32000` 透传）。**delete**（subject 整删）=
  DeleteSchema；Glue 不返回被删版本号清单 → `deletedVersions` 为空数组。
- **凭据红线**：`glue_secret_access_key`/`glue_session_token` 仅存
  connSecrets，只进 SigV4 签名，不落日志/审计/事件。

**produce/consume/stream 的 schema 挂载**：`schema{}` 增加 `registry?`
（同上探测规则）。wire format 编解码**仅支持 Confluent**（语义与既有口径
一致，不发明 Glue wire format）：provider=glue 时 `-32000` 业务错
"schema-aware produce/consume currently supports Confluent wire format;
AWS Glue schema management is available"；双配置歧义/未知 registry →
`-32602`；未配置任何 SR → `-32000`。

### 3.10 MCP UI intent 回报（M3）

`kafka/ui/state/report`（前端 → sidecar；§6.4 事件的回报通道）：

- **intent 回报**：`{intentId, status: "applied"|"rejected", summary?,
  reason?}`。summary = `{count, truncated?, rows[≤5], anchor?, reason?}`，
  anchor 形如 `orders-p0-o42`（定位字段不截断）。未知/已过期 intentId →
  -32000；status 非 applied/rejected → -32602。
- **快照型**：无 `intentId`、`status:"snapshot"` —— `{summary:{panel?,
  topic?, count?, anchor?}}`，sidecar 覆盖最新快照。
- 返回 `{success: true}`。

### 3.9 流式会话约束（IMPL_PLAN §5.5）

- ring buffer 固定容量 **10000** 条（事件 + `kafka/stream/messages` 分页共用）；
- 并发会话上限 **20**；
- 空闲 **30 分钟**自动回收；
- `kafka/stream/stop` 可取消进行中的 fetch；
- fetch 错误指数退避 **500ms → 30s**；
- **read_only 策略下禁止 commit**（`ConsumeParams.commit=true` → `-32000`）。

## 4. ConsumeParams 完整字段表

一次性消费（`kafka/messages/consume`、`kafka/messages/export`）与流式
（`kafka/stream/start`）共用：

| 字段 | 类型 | 默认 | 说明 |
| --- | --- | --- | --- |
| `topic` | string | 必填 | 目标 topic |
| `groupId` | string? | — | 消费组 id；**与 `partitions` 互斥**（同给 → `-32602`） |
| `offsetStrategy` | enum | `latest` | `latest` / `earliest` / `committed` / `timestamp` / `offset` |
| `offsetTime` | string? | — | strategy=timestamp：RFC3339 或 unix 毫秒 |
| `partitions` | int[]? | — | 指定分区（有值时禁 `groupId`） |
| `partitionOffsets` | map<partition,int>? | — | strategy=offset 时**必填** |
| `limit` | int | 100 | 返回条数上限 |
| `timeoutMs` | int | 5000 | 扫描窗口（客户端启动/metadata 就绪另有独立预算 max(2×窗口, 12s)，不计入本值） |
| `maxScanRecords` | int | max(1000, limit×10) | 扫描上限（过滤不过 early-stop） |
| `isolationLevel` | enum | `read_uncommitted` | `read_uncommitted` / `read_committed` |
| `commit` | bool | false | true 时**禁一切过滤且必须 groupId**（否则 → `-32602`）；read_only 下拒绝 → `-32000` |
| `filter` | string? | — | 全文过滤（key+value+headers 拼接） |
| `keyFilter` | string? | — | key 过滤 |
| `valueFilter` | string? | — | value 过滤 |
| `headerFilter` | string? | — | headers 序列化后过滤 |
| `matchMode` | enum | `contains` | `contains` / `prefix` / `exact` / `regex`（非法 regex → `-32602`） |
| `fieldFilters` | FieldFilter[]? | — | 字段级过滤，见下 |
| `timestampFrom` / `timestampTo` | int? | — | 消息时间戳范围（unix 毫秒，闭区间） |
| `offsetFrom` / `offsetTo` | int? | — | offset 范围 |
| `decode` | enum | `none` | `none` / `base64`（对 valueText 的二次解码展示） |
| `decompression` | enum | — | `gzip` / `lz4` / `zstd` / `snappy`（payload 先解压再解码） |
| `schema` | object? | — | **Phase 2**：`{subject?, version?, format?}`（SR 挂载，语义见 §3.5 consume；format 取值 `avro`/`json`/`protobuf`，可空 = 按注册元数据；解码失败置 `decodeError`，不中断消费） |

`fieldFilters[]`：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `source` | enum | `value` / `key` / `header` / `topic` / `partition` / `offset` / `timestamp` |
| `path` | string? | JSON path（source=value/key 且 payload 为 JSON 时按路径取值） |
| `operator` | enum | `contains` / `prefix` / `exact` / `regex` / `exists` / `not_exists` / `gt` / `gte` / `lt` / `lte` |
| `value` | string? | 比较值（exists/not_exists 可省；数值比较按数值解析） |
| `enabled` | bool | 默认 true；false 的条目跳过 |

`commit=true` 与任何过滤条件（filter/keyFilter/valueFilter/headerFilter/
fieldFilters/时间戳或 offset 范围）同给 → `-32602`（commit 与过滤互斥）。

## 5. 消息形状（MessageView，二进制保真）

```
{
  "topic": string,
  "partition": int,
  "offset": int,
  "timestamp": int,
  "leaderEpoch": int?,
  "key": string?,          // UTF-8 安全预览（非法字节替换 U+FFFD）
  "keyBase64": string?,    // 恒完整
  "valueText": string?,    // UTF-8 安全预览（非法字节替换），恒有
  "valueBase64": string,   // 恒完整（base64 保真，二进制不损坏）
  "headers": map<string,string>,
  "committed": bool?,
  "decodeError": string?,  // 二次解码/解压/wire format 解码失败原因
  "truncated": bool,       // 单条消息体超 512KB 时截断并标记
  "schemaId": int?,        // Phase 2：ConsumeParams.schema 命中时填充
  "schemaSubject": string?,
  "schemaVersion": int?
}
```

- 单条消息体上限 **512KB**，超出截断且 `truncated:true`；
- `valueText` 恒为 UTF-8 安全预览、`valueBase64` 恒完整（二进制消息不被
  `string(record.Value)` 类直转损坏）。

## 6. 事件（sidecar → 宿主 notification）

### 6.1 `kafka/stream/messages`

流式消费批量推送（节流：200ms 或 50 条一批）：

```
{ "sessionId": string, "messages": MessageView[], "totalScanned": int,
  "totalMatched": int, "paused": bool }
```

### 6.2 `kafka/stream/error`

```
{ "sessionId": string, "error": string }
```

fetch 循环按指数退避（500ms→30s）重试；不可恢复错误（topic 删除、
会话被回收）随后由宿主主动 stop。

### 6.3 `kafka/audit`

M0 审计记录（同 ldap/audit 形状）：全部写操作 + 策略拒绝事件，
同时落 `store.AppendAudit`（audit.jsonl）并推送本事件；凭据字段
（sasl_password、tls_client_key）不进 result。MCP 写路径（M3 起）同条
携带 `source:"mcp"`（additive 字段；工作台路径不携带）。

### 6.4 `kafka/ui/intent`（MCP UI intent 通道，M3）

sidecar 收到 MCP UI 驱动类工具（`kafka_ui_search` / `kafka_ui_focus` /
`kafka_ui_select`）时下发；intent 状态表 TTL 60s、LRU 20 条：

```
{ "intentId": string, "action": "search" | "focus" | "select", "params": {} }
```

| action | params | 前端行为 |
| --- | --- | --- |
| `search` | `topic` 必填；`offsetStrategy?`、`limit?`、`filter?`、`keyFilter?`、`valueFilter?`、`headerFilter?`、`matchMode?`、`groupId?`、`partitions?[]`、`offsetTime?` | 消息面板 consume 表单填条件并触发消费 |
| `focus` | `panel`（messages \| topics \| groups \| schemas） | 切换/聚焦面板 |
| `select` | `partition`、`offset` 必填；`topic?` | 当前结果中按 partition+offset 定位并打开详情 |

前端消费统一走 `shared/frontend/uiIntent.ts` 的 `useUiIntent("kafka",
handlers)`；回报走 §3.10 的 `kafka/ui/state/report`。工具面全表与两阶段
语义见 `docs/MCP.zh-CN.md`。

## 7. 策略语义（policy.go，错误均 `-32000` blocked）

| 配置 | 效果 |
| --- | --- |
| `read_only=true` | produce / topics create/alter/delete / partitions update / groups delete / offsets reset / acls create/delete / **schema register、schema compatibility/set** 一律拒绝；**禁止 commit** |
| `allow_delete=false` | topics/delete、**topics/records/clear**、groups/delete、acls/delete、**schema/delete、schema/delete/version** 额外拒绝；read_only 下本项无效（两者与门） |
| `confirmTopic` | topics/delete 与 topics/records/clear 必须携带与 topic 同名的确认字段，否则 `-32602` |

schema 写操作审计 action：`kafka/schema-register`、`kafka/schema-compatibility-set`、
`kafka/schema-delete`（detail 只含 subject@version/id/level，不含 schema 内容
与凭据）。topics/records/clear 审计 action：`kafka/topics.records.clear`
（detail `partitions=N`）。

## 8. 与实现的同步约定

- 后端方法注册表与本文件及 `IMPL_PLAN_DBX_KAFKA.zh-CN.md` §5.2 三方一致；
  `backend/internal/kafkaconn/manifest_contract_test.go` 会读取
  `manifest.json` 做七语与字段契约校验（含 Phase 2 新字段/GSSAPI 选项）。
- smoke（`scripts/smoke_test.py`）按本文件场景编号 S1-S11（S11 = Phase 2
  SR 场景，SR 不可达时 SKIP）；未注册方法（`-32601`）单场景 SKIP。
  MCP 工具面场景 K1-K11 见 `scripts/smoke_mcp.py`（离线 + 容器场景，
  未注册 SKIP）。

## 9. manifest 连接字段（Phase 1 + Phase 2 + Phase 3(Glue) 汇总）

条件显隐/必填采用宿主 1.1 特性 `visible_when` / `required_when`（单字段条件
`{field, one_of[]}`，无 AND 组合；宿主缺位时 optional 降级为不显隐、由
sidecar 兜底校验）。SR 后端由新增决策字段 **`schema_registry`**（select：
`none`/`confluent`/`aws_glue`，默认 `none`）统一裁决，sr_url/glue_* 降级为
对应后端的连接参数。

| key | type | binding | 默认 | visible_when | 说明 |
| --- | --- | --- | --- | --- | --- |
| display_name | text | name | "Kafka cluster" | — | 连接名（required） |
| bootstrap_servers | textarea | config | — | — | `host:port` 逗号/换行分隔（required） |
| security_protocol | select | config | PLAINTEXT | — | PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL |
| sasl_mechanism | select | config | PLAIN | security_protocol ∈ SASL | PLAIN/SCRAM-SHA-256/SCRAM-SHA-512/**GSSAPI**/**OAUTHBEARER**（Phase 3） |
| sasl_username / sasl_password | text / password | config / **secret** | — | security_protocol ∈ SASL | SASL 凭据；required_when 见下方矩阵（GSSAPI/OAUTHBEARER 不需要账密） |
| tls_ca_cert / tls_client_cert | textarea | config | — | security_protocol ∈ SSL | PEM |
| tls_client_key | password | **secret** | — | security_protocol ∈ SSL | PEM 私钥 |
| tls_insecure_skip_verify | boolean | config | false | security_protocol ∈ SSL | 仅开发用 |
| client_id | text | config | — | — | 上报 client.id |
| read_only | boolean | config | true | — | 写门禁 |
| allow_delete | boolean | config | false | — | 删除门禁 |
| connection_source | select | config | bootstrap | — | **bootstrap / zookeeper**（Phase 2） |
| zk_servers | textarea | config | — | connection_source ∈ [zookeeper] | `host:port[/chroot]` 逗号分隔 |
| kerberos_service_name | text | config | kafka | sasl_mechanism ∈ [GSSAPI] | GSSAPI 服务名（有默认值，不必填） |
| kerberos_realm | text | config | — | sasl_mechanism ∈ [GSSAPI] | 空则从 principal/krb5.conf 推导（可选） |
| kerberos_principal | text | config | — | sasl_mechanism ∈ [GSSAPI] | `user@REALM`；required_when 见矩阵 |
| kerberos_keytab_path | text | config | — | sasl_mechanism ∈ [GSSAPI] | keytab **文件路径**（不收内容）；required_when 见矩阵 |
| kerberos_krb5_conf_path | text | config | — | sasl_mechanism ∈ [GSSAPI] | 空则回落 `$KRB5_CONFIG` → `/etc/krb5.conf`（可选） |
| **schema_registry** | select | config | **none** | —（常显，决策开关） | **none / confluent / aws_glue**；裁决 SR 后端，none = 禁用 schema 能力 |
| sr_url | text | config | — | schema_registry ∈ [confluent] | Confluent 兼容 SR URL；required_when 见矩阵 |
| sr_username | text | config | — | schema_registry ∈ [confluent] | SR basic auth 用户名（可选） |
| sr_password | password | **secret** | — | schema_registry ∈ [confluent] | SR basic auth 密码（可选） |
| glue_region | text | config | — | schema_registry ∈ [aws_glue] | **AWS Glue 区域**；required_when 见矩阵 |
| glue_registry_name | text | config | — | schema_registry ∈ [aws_glue] | **AWS Glue registry 名称**；required_when 见矩阵 |
| glue_auth_mode | select | config | default | schema_registry ∈ [aws_glue] | **default / static**（profile 模式不提供） |
| glue_access_key_id | text | config | — | glue_auth_mode ∈ [static] | AWS Access Key ID；required_when 见矩阵 |
| glue_secret_access_key | password | **secret** | — | glue_auth_mode ∈ [static] | AWS Secret Access Key；required_when 见矩阵（凭据红线：secret binding） |
| glue_session_token | password | **secret** | — | glue_auth_mode ∈ [static] | AWS 会话令牌（可选，STS 临时凭据） |
| **oauth_token_source** | select | config | —（不声明 default） | sasl_mechanism ∈ [OAUTHBEARER] | **msk_iam / static_token**（Phase 3 OAUTHBEARER token 来源）；表单须显式选择，后端空值仍回退 msk_iam——不声明 default 是因为宿主条件求值会把 default 代入下游 visible_when，回填 msk_iam 会让 msk_region 在普通 PLAINTEXT/SCRAM 表单上幽灵必填（宿主旧版不级联可见性时直接死锁保存按钮） |
| msk_region | text | config | — | oauth_token_source ∈ [msk_iam] | MSK 集群 AWS 区域（签名 IAM token）；required_when 见矩阵 |
| msk_access_key_id | text | config | — | oauth_token_source ∈ [msk_iam] | 可选：显式覆盖 AWS 默认凭据链（与 msk_secret_access_key 成对） |
| msk_secret_access_key | password | **secret** | — | oauth_token_source ∈ [msk_iam] | 显式凭据 SK（与 AK 成对；凭据红线：secret binding） |
| msk_session_token | password | **secret** | — | oauth_token_source ∈ [msk_iam] | 可选 STS 会话令牌 |
| oauth_static_token | password | **secret** | — | oauth_token_source ∈ [static_token] | 静态 bearer token；required_when 见矩阵（凭据红线：secret binding） |
| **properties_import** | textarea | **secret** | — | —（常显，恒可选） | **粘贴 properties 导入**（Lane 3）：Kafka 客户端 properties 片段直贴对话框；语义见 §9.3 |

### 9.1 required_when 矩阵（agent I，与 sidecar 兜底校验一一对应）

| 字段 | required_when | sidecar 校验（connection/connect 与 connection/test，缺失 → `-32602`） |
| --- | --- | --- |
| sr_url | schema_registry ∈ [confluent] | `srUrl is required when schemaRegistry is "confluent"` |
| glue_region | schema_registry ∈ [aws_glue] | `glueRegion is required when schemaRegistry is "aws_glue"` |
| glue_registry_name | schema_registry ∈ [aws_glue] | `glueRegistryName is required when schemaRegistry is "aws_glue"` |
| glue_access_key_id | glue_auth_mode ∈ [static] | `glueAccessKeyId is required when glueAuthMode is "static"` |
| glue_secret_access_key | glue_auth_mode ∈ [static] | `glueSecretAccessKey is required when glueAuthMode is "static"`（session_token 恒可选） |
| sasl_username / sasl_password | sasl_mechanism ∈ [PLAIN, SCRAM-SHA-256, SCRAM-SHA-512] | `sasl username/password is required for <protocol>`（GSSAPI/OAUTHBEARER 不需要账密） |
| kerberos_principal | sasl_mechanism ∈ [GSSAPI] | `kerberosPrincipal is required for GSSAPI` |
| kerberos_keytab_path | sasl_mechanism ∈ [GSSAPI] | `kerberosKeytabPath is required for GSSAPI`（service_name 有默认值、realm/krb5 可选） |
| msk_region | oauth_token_source ∈ [msk_iam] | `mskRegion is required when oauthTokenSource is "msk_iam"`；另 OAUTHBEARER ⇒ security_protocol=SASL_SSL（违者 `OAUTHBEARER requires security_protocol SASL_SSL`），AK/SK 只给一半 → `must be provided together`（均 `-32602`） |
| oauth_static_token | oauth_token_source ∈ [static_token] | `oauthStaticToken is required when oauthTokenSource is "static_token"`（`-32602`） |

SR provider 解析以 `schema_registry` 开关为准（`resolveSchemaProvider`）：
缺省 registry 时开关 `confluent`→confluent、`aws_glue`→glue、`none`→none
（SR 禁用，schema 方法族返回 `-32000` not enabled）；显式 `registry` 参数与
开关冲突 → `-32602` 参数错。**向后兼容**：旧连接 profile 无 `schema_registry`
字段（空值）时，沿用 Phase 2/3 的自动探测回退（`sr_url` 非空 → confluent、
`glue_region+glue_registry_name` 齐备 → glue、双配置须显式 registry），
`provider="both"` 仅在旧连接出现；statuses 摘要新增 `schemaRegistry.mode`
（开关归一值，旧连接省略）。

### 9.2 宿主契约核对结论（required_when 消费，2026-09 核对）

- **schema 层**：`host/plugins/manifest.schema.json` L142-167 定义
  `fieldCondition = {field, one_of[]}`，`formField.visible_when` /
  `required_when` 同构；单字段条件、无 AND/OR 组合。
- **宿主前端消费**（`host/apps/desktop/src/lib/plugins/pluginFieldConditions.ts`）：
  `pluginFieldIsRequired` = static `required` OR 匹配的 `required_when`；
  条件匹配要求兄弟字段当前值非空且在 `one_of` 内。连接对话框据此渲染必填
  标记并阻断提交（`PluginConnectionFields` 组件），binding=password 的
  required 字段还会触发共享密码提示
  （`lib/connection/connectionPassword.ts`）。
- **宿主 Rust 核心消费**（`host/crates/dbx-core/src/plugins/host.rs`）：
  连接建立校验对「可见（visible_when 命中）+ required（required_when 命中）
  + 值为空」的字段报 `Plugin connection field '<label>' is required`，
  与前端语义镜像；隐藏字段不参与必填校验（对 MCP/import 等非对话框写路径
  友好）。
- **结论**：`required_when` 宿主已真实消费（非仅 schema 校验），manifest
  矩阵在宿主侧即生效；sidecar 的 `validateRequiredCombination`（上表右列，
  `-32602`）为纵深防御 + 非对话框写路径兜底，两者矩阵一致。
- **降级注意**：宿主 <1.1 不识别两条件时按 optional 降级（全部字段平铺、
  无前端必填拦截），此时 sidecar 兜底校验仍保证必填组合不缺（规则 3
  "宿主 1.1 特性 optional 降级"的落地形态）。

### 9.3 粘贴 properties 导入（Lane 3，properties_import）

对标 Confluent 插件「粘贴即连」：用户把 Kafka 客户端 properties 片段直接
粘进连接对话框的 `properties_import` 字段，保存/测试时 sidecar 解析并合并
进结构化连接字段。

- **凭据红线**：字段 binding 为 **secret**——粘贴文本（可能内嵌
  jaas/basic.auth 密码）经宿主 secret binding 加密存储、仅在
  connection/connect|test 时经 `connection_secrets.properties_import` 下发
  明文到 sidecar，插件不持久化、不进日志/审计。**config 通道中的同名键
  一律不消费**（防非对话框写路径明文持久化，`props_test.go` 有回归）。
- **解析语法**（`backend/internal/kafkaconn/props.go`，java.util.Properties
  语义子集）：`#`/`!` 注释行；第一个未转义 `=` / `:` / 空白为分隔符；
  行尾奇数反斜杠续行（续行前导空白跳过）；`\t \n \r \f \\ \uXXXX` 转义
  （非法 `\u` 保守保留原文）；重复键后者覆盖；空值键跳过不覆盖表单值。
- **映射表**（paste-wins：粘贴非空值覆盖表单值；取值面外的值整键忽略）：
  `bootstrap.servers`→bootstrap_servers；`security.protocol`→security_protocol；
  `sasl.mechanism`→sasl_mechanism；`sasl.jaas.config`→按机制提取
  （PLAIN/SCRAM→sasl_username + sasl_password(secret)；GSSAPI→principal/
  keyTab；OAUTHBEARER 无映射目标，token 须走表单 oauth_token_source）；
  `sasl.kerberos.service.name`→kerberos_service_name；
  `ssl.endpoint.identification.algorithm`（none→tls_insecure_skip_verify）；
  `ssl.truststore.certificates`→tls_ca_cert；`ssl.keystore.certificate.chain`→
  tls_client_cert；`ssl.keystore.key`→tls_client_key(secret)；
  `schema.registry.url`→sr_url + schema_registry=confluent；
  `basic.auth.credentials.source`（仅 USER_INFO 支持）+
  `basic.auth.user.info` / `schema.registry.basic.auth.user.info`→
  sr_username + sr_password(secret)；`client.id`→client_id。
  **Java keystore 路径类键**（`ssl.truststore.location/password`、
  `ssl.keystore.location/password`、`ssl.key.password` 等）没有对应字段
  （TLS 走 PEM 内联模型），进忽略清单。
- **合并时机**：发生在 `NormalizeProfile`/`Validate`/
  `validateRequiredCombination` 之前——粘贴驱动的 SASL_SSL + jaas 凭据
  组合直接通过 required_when 兜底校验；半粘贴（缺凭据等）仍按矩阵报
  `-32602`。
- **解析摘要**：`kafka/connections/statuses` 每连接新增可选
  `propertiesImport: {mapped, mappedKeys?, ignored, ignoredKeys?}`（仅计数
  与键名，值一律不透出）；未使用导入时省略。工作台连接面板对当前连接
  展示「已映射 N 项 / 已忽略 M 项」。
