# B-KAFKA 路交付报告（io.dbx.kafka backend / Phase 1 全量 + Phase 2 商用化）

> 路线：IMPL_PLAN_DBX_KAFKA.zh-CN.md §9 A 路 backend —— 自研 Kafka
> Go sidecar（stdio-jsonl，与 ldap 同构，franz-go + kadm）。
> Phase 1：仅 `kafka/backend/**` 与本文件。Phase 2（§6 小节）：另含
> `kafka/manifest.json`、`docker-compose.kafka-test.yml`、
> `docs/PROTOCOL_KAFKA.zh-CN.md`、`scripts/smoke_test.py`（追加 S11）、
> `scripts/sidecar_client_jsonl.py`（超时/诊断修复）。`kafka/frontend/**`
> 属并行路，未改动。无 git commit/push。

## 0. 验证终值

| 套件 | Phase 1 | Phase 2（2026-09-05） |
|---|---|---|
| `CGO_ENABLED=0 go vet ./...` | ✅ 0 告警 | ✅ 0 告警 |
| `CGO_ENABLED=0 go test ./...` | **74 passed / 0 failed** | **98 passed / 0 failed**（`-count=1 -v` 计数；kafkaconn 85 + lifecycle + store） |
| 方法注册 | **34/34** | **45/45**（Phase 2 新增 11 个 `kafka/schema/*`） |
| manifest 契约测试 | 真实 PASS | 真实 PASS（新增 10 字段/绑定/七语/GSSAPI 选项守卫） |
| 依赖 | franz-go v1.20.7 + kadm v1.17.2 | + go-zookeeper/zk v1.0.4、gokrb5/v8 v8.4.4、franz-go pkg/sasl/kerberos v1.1.0、goavro/v2 v2.15.0、jsonschema-go v0.4.2（全部进 go.sum，公网拉取成功；CGO_ENABLED=0） |
| smoke（真容器） | 未跑 | **S1-S11：10 PASS / 0 FAIL / 1 合法 SKIP**（apache/kafka 9092 + redpanda 19081，详见 §6.5） |

复现命令：

```bash
cd /Users/Jinpy/btroot/dbx-plugins/kafka/backend
CGO_ENABLED=0 go vet ./... && CGO_ENABLED=0 go test -count=1 ./...
# ok  io.dbx.kafka.plugin/internal/kafkaconn
# ok  io.dbx.kafka.plugin/internal/lifecycle
# ok  io.dbx.kafka.plugin/internal/store
python3 -c "import json;d=json.load(open('../manifest.json'));ks={f['key'] for f in d['contributions'][0]['fields']};need={'connection_source','zk_servers','kerberos_service_name','kerberos_realm','kerberos_principal','kerberos_keytab_path','kerberos_krb5_conf_path','sr_url','sr_username','sr_password'};assert need<=ks, need-ks;print('manifest fields ok')"
# manifest fields ok
# smoke（双容器）：
#   docker compose -f docker-compose.kafka-test.yml up -d
#   bash scripts/kafka-seed/seed-topics.sh
#   DBX_PLUGIN_SIDECAR=$PWD/backend/bin/dbx-plugin-kafka python3 scripts/smoke_test.py
```

## 1. 交付总览

```
kafka/backend/
├── go.mod / go.sum              # module io.dbx.kafka.plugin，go 1.26.0；SDK replace 照 ldap
├── main.go                      # SDK server + 34 方法 switch + 审计/流式事件 emitter 注入
└── internal/
    ├── lifecycle/               # 照 ldap 抄改（bootstrap textarea 换行+逗号双拆）
    ├── store/                   # 照 ldap 抄改（DefaultDirName=io.dbx.kafka，audit.jsonl）
    └── kafkaconn/
        ├── types.go             # §5 契约类型（camelCase；消息二进制保真形状）
        ├── client.go            # TLS/SASL 纯构建 + admin client 指纹缓存 + per-request 消费 client
        ├── service.go           # 连接表生命周期 + 预设 + 审计回调 + 状态快照
        ├── topics.go            # brokers 2 方法 + topics 8 方法（含 isHealthy 分区健康）
        ├── groups.go            # groups 5 方法（offsets/reset 自实现，kadm v1.17 无内建）
        ├── acls.go              # acls 3 方法（builder 过滤；过宽拒绝）
        ├── messages.go          # produce/consume/export + 过滤引擎 + 解码解压 + CSV/JSON
        ├── stream.go            # ring buffer 10000 / 会话上限 20 / 30min 回收 / 200ms·50 节流 emit
        ├── policy.go            # read_only × allow_delete 与门 + confirmTopic 守卫
        ├── audit.go             # AuditRecord（main 落盘 + kafka/audit 事件）
        ├── helpers.go           # 归一化与 mutation 结果映射
        └── 7 个 *_test.go + manifest_contract_test.go
```

## 2. 方法注册清单（34，与 IMPL_PLAN §5.2 一致）

- 生命周期：`connection/test`、`connection/connect`、`connection/disconnect`
- brokers：`kafka/brokers/list`、`kafka/brokers/config`
- topics：`kafka/topics/list|describe|create|delete|partitions/update|config/get|config/alter|offsets/list`
- groups：`kafka/groups/list|describe|offsets/list|delete|offsets/reset`
- acls：`kafka/acls/list|create|delete`
- messages：`kafka/messages/produce|consume|export`
- stream：`kafka/stream/start|stop|pause|resume|status|messages`
- presets：`kafka/presets/list|save|remove`
- 全局：`kafka/connections/statuses`

未匹配臂统一 `dbxpluginsdk.MethodNotFound`（-32601）；参数错 -32602、业务错
-32000（blocked 语义经消息文案携带，与 ldap 一致）。

## 3. 关键实现决策（现象 → 改动 → 验证）

### ① 客户端连接复用（修复每调用重建 client 的已知缺陷）
- **现象**：`newKafkaClient` 每次调用新建 + Close；管理面轮询浪费。
- **改动**：admin 类调用（brokers/topics/groups/acls/offsets）复用
  `connEntry.client`，指纹 = SHA256(bootstrap+securityProtocol+SASL 机制/
  用户名/密码+CA/cert/key+insecure+clientID)，指纹失效重建；同一连接操作经
  `entry.mu` 串行。consume/stream 携带 per-request 消费 opts，仍每次新建
  client（`consumeClient`）用完即关——避免 ConsumePartitions 等选项污染
  共享通道。
- **验证**：`TestFingerprintStableAndSensitive`（稳定 + 密码/bootstrap 变化
  触发失效）；`TestSeedBrokersFallback`（bootstrap 缺失时 runtime.host:port
  兜底，任务书"拨号支持 runtime.host:port 语义"落地）。

### ② 二进制保真（修复 `string(record.Value)` 已知缺陷）
- **改动**：消息形状 `valueText` 恒为 UTF-8 安全预览（非法字节替换
  U+FFFD）、`valueBase64` 恒完整（512KB 上限截断并置 `truncated:true`）；
  key 合法 UTF-8 走 `key` 字段，否则 `keyBase64`；headers 值做 UTF-8 安全
  替换。
- **验证**：`TestMessageBinaryFidelity`（恶意二进制样本 roundtrip
  valueBase64/keyBase64 逐字节相等）；`TestMessageTruncationAtLimit`；
  `TestMessageTextKeyUsesKeyField`；导出 JSON 断言 valueBase64 原样透传
  （`TestExportJSON`）。

### ③ consume 全参数（§5.3）
- **改动**：5 种 offset 策略（latest/earliest/committed/timestamp/offset）、
  per-partition 精确 seek、`commit × 过滤` 互斥（含 fieldFilters/范围过滤）、
  `partitions × groupId` 互斥、matchMode 四态、fieldFilters 七 source 十
  operator（含 JSON path `$.a.b[0].c` 与 gt/gte/lt/lte 数值比较）、base64
  二次解码 + gzip/lz4/zstd/snappy 解压（snappy block/framed 双兜底）、
  scanned/matched/limited/hasMore/nextPartitionOffsets、limit 默认 100、
  maxScanRecords 默认 max(1000, limit×10)。
- **验证**：`consume_params_test.go`（互斥矩阵 11 case + buildConsumeOpts
  缺 offset 报错）；`filter_test.go`（matchMode/通道/字段过滤/JSON path）；
  `codec_test.go`（四种解压 roundtrip + base64+gzip 组合）。

### ④ 流式会话（§5.5）
- **改动**：ring buffer 10000（满覆盖最旧）、会话上限 20、空闲 30min 回收
  （`EvictIdle`，连接断开/重连即 `StopAllForConnection`）、200ms/批 50 节流
  emit `kafka/stream/messages`、fetch 错误指数退避 500ms→30s、pause 时消费
  继续入 ring 仅停推送、read_only 禁 commit（StartStream 校验层拒绝）。
  事件经 `StreamEmitter` 接口注入（main 适配 SDK emitter），kafkaconn 不
  依赖 SDK。
- **验证**：`ringbuffer_test.go`（覆盖最旧、跨环绕分页连续、拷贝语义、
  回收阈值、常量契约 20/10000/30min/200ms/50）。

### ⑤ 安全策略与审计（§6）
- **改动**：`ensureWriteAllowed`（read_only 拒 produce/create/alter/reset/
  ACL 写）、`ensureDeleteAllowed`（read_only ∥ !allow_delete 与门拒
  topics/groups/acls delete）、`ensureTopicDeleteConfirm`（单 topic
  confirmTopic 同名、多 topic confirmTopics 逐一对齐）。写操作成功/拒绝均
  走 `emitAudit` → audit.jsonl（success→ok、blocked→denied）+ `kafka/audit`
  事件；Target 只含资源名。
- **验证**：`policy_test.go`（2×2 门禁矩阵 + confirmTopic 四态）；
  `TestAuditCallback`（含"审计记录不含凭据标记"断言）。

### ⑥ kafka/groups/offsets/reset（host 补齐能力）
- **改动**：kadm v1.17.2 无内建 OffsetReset，基于 `FetchOffsets` +
  `CommitOffsets` 自实现：earliest/latest/timestamp 用 ListOffsets 后提交；
  partitionOffset 用请求 `partitionOffsets{topic:{partition:offset}}` 直接
  构造（leaderEpoch=-1）。逐分区返回 `rows[]{topic,partition,ok,error}`。
  只读策略下拒绝（写操作）。
- **验证**：`TestResetModeNormalization`；reset 主流程属连网路径，留待
  smoke 容器场景覆盖（见 §5 遗留）。

### ⑦ groups/offsets/list Option 语义
- **改动**：`hasCommitted` 区分"组从未提交 offset"（FetchOffsets 结果为空
  → false）与零 lag；行值 = start/end/committed 三表合并，lag= end-
  committed（未提交时视作全量未消费，committed=-1）。
- **验证**：`TestGroupLag`；`groupOffsetRows` 属连网路径，容器场景覆盖。

## 4. 改动清单

新增（全部为本路范围内新文件）：

- `kafka/backend/go.mod`、`go.sum`
- `kafka/backend/main.go`
- `kafka/backend/internal/lifecycle/lifecycle.go` + `lifecycle_test.go`
- `kafka/backend/internal/store/store.go` + `store_test.go`
- `kafka/backend/internal/kafkaconn/`：`types.go`、`client.go`、
  `service.go`、`topics.go`、`groups.go`、`acls.go`、`messages.go`、
  `stream.go`、`policy.go`、`audit.go`、`helpers.go`；测试
  `client_test.go`、`consume_params_test.go`、`filter_test.go`、
  `codec_test.go`、`export_test.go`、`policy_test.go`、`ringbuffer_test.go`、
  `helpers_test.go`、`service_test.go`、`manifest_contract_test.go`
- `kafka/docs/PROGRESS-B-KAFKA.zh-CN.md`（本文件）

依赖取值面（§3 白名单内）：franz-go（kgo/kadm/kmsg/sasl plain+scram）、
google/uuid、klauspost/compress（zstd/snappy，franz-go 同源）、
pierrec/lz4/v4。无 sarama、无 gokrb5（Kerberos 按契约 Phase 2）。

## 5. 遗留与风险

1. **连网路径未真机验证**：单测全部纯解析（无容器依赖）；produce/consume/
   stream/reset 等真实 Kafka 交互由 smoke（`dbx-kafka-test` KRaft 容器）与
   收口阶段覆盖，本路未启动容器。
2. **`kafka/groups/offsets/reset` 的 partitionOffsets 形状**：实现为
   `{topic: {partition: offset}}`；IMPL_PLAN §5.2 未细化到分区维度，收口时
   需与 C 路 `PROTOCOL_KAFKA.zh-CN.md` 核对，若协议定义为扁平
   `{partition: offset}` 需同步调整（改动点集中在
   `GroupOffsetResetRequest` + `ResetGroupOffsets`）。
3. **stream 事件与宿主桥的实测**：`kafka/stream/messages` 为 JSON 载荷
   事件，`pluginHandler` 的 emitter 引用沿用 ldap 的"Serve 期间单例"模式；
   真机多事件流背压（ring 满 + 前端 droppedRows）需 e2e 复核。
4. **admin client 复用在断线场景**：共享 client 断线后 franz-go 自带重连，
   未做"失败重建一次"的 ldap WithConn 同构逻辑（kgo client 常驻自愈，
   语义等价）；若 smoke 发现长断连场景状态不刷新，再补指纹强制重建入口。
5. **`kafka/messages/produce` 的 value 为文本字段**（契约 §5.2 形状）；
   二进制生产（base64 入参）契约未定义，Phase 2 如需可加 `valueBase64`
   入参字段。
6. manifest 契约测试当前真实 PASS（manifest 已在位）；若 manifest 后续
   调整字段，以本测试为守卫（文件缺失时才 Skip）。

## 6. Phase 2 商用化（2026-09-05 增补）

### 6.0 范围与交付

IMPL_PLAN §0.2 三项全部落地：**Schema Registry**（Confluent 兼容 REST，
含 Redpanda 内置 SR；AWS Glue 登记 Phase 3 不做）、**Kerberos/GSSAPI**
（gokrb5 + franz-go pkg/sasl/kerberos）、**ZooKeeper 发现**
（go-zookeeper/zk，支持 chroot）。另含冻结契约要求的既有方法扩展
（produce/consume/stream 的 schema 挂载与 valueBase64/keyBase64、offsets
全策略、statuses 摘要）与 manifest/协议文档同步。

### 6.1 新方法注册清单（+11，Phase 1 34 → Phase 2 45）

`kafka/schema/test`、`kafka/schema/subjects/list`、
`kafka/schema/versions/list`、`kafka/schema/get`、
`kafka/schema/versions/compare`、`kafka/schema/compatibility/get`、
`kafka/schema/compatibility/set`（写，过 read_only）、
`kafka/schema/compatibility/check`、`kafka/schema/register`
（写，过 read_only，非 allow_delete 级）、`kafka/schema/delete`、
`kafka/schema/delete/version`（两者 critical：allow_delete + read_only 与门）。
写操作均走 emitAudit：`kafka/schema-register` / `kafka/schema-compatibility-set` /
`kafka/schema-delete`。

新文件：`internal/kafkaconn/schema.go`（SR REST 客户端 10s 超时、wire format
magic-0 编解码、AVRO/JSON 载荷编解码、LCS 逐行 diff、per-consume 元数据
缓存）、`kerberos.go`、`zk.go` + 对应 `_test.go`（httptest 假 SR、合成
keytab v2/krb5.conf、ZK 解析与不可达路径；无外网、无 KDC）。

### 6.2 既有方法扩展（冻结契约逐项）

- `messages/produce`：`valueBase64`（与 value 二选一，同给 -32602）、
  `keyBase64`、`schema{subject,version?,format}` —— 有 schema 时 value 是
  未编码载荷，按 SR 元数据编码（AVRO→goavro 二进制 / JSON→Schema 校验
  透传）并打包 wire format 后生产。
- `messages/consume` + `stream/start`：`schema` 解码 wire format 为 JSON
  文本，命中消息附 `schemaId/schemaSubject/schemaVersion`，解码失败置
  `decodeError` 不中断；元数据按 schemaID / subject+version 在本次消费或
  会话内缓存（同 ID 只请求一次 SR，单测计数断言）。
- `topics/offsets/list`：`offsetTime` 支持 `earliest`(-2)/`latest`(-1)/
  `max-timestamp`(-3)/`log-start`(-4)/RFC3339/unix ms（kadm
  ListStartOffsets/ListEndOffsets/ListMaxTimestampOffsets/
  ListLocalLogStartOffsets/ListOffsetsAfterMilli；负数整数按协议保留值拒绝）。
- `brokers/list`：`connection_source=zookeeper` 时经 ZK `/brokers/ids` 发现
  （快速 TCP 预拨 + chroot 支持；ZK 不可达 → -32000 业务错），返回新增
  `connectionSource` 字段；connection/test 同语义（ZK 模式先发现再探活）。
- `connections/statuses`：新增 `connectionSource`、`schemaRegistry{enabled,url}`、
  `kerberos{enabled}` 摘要（凭据不出）。
- manifest 新字段 10 个（connection_source/zk_servers/kerberos_*/sr_*，
  sr_password 为 secret binding）+ sasl_mechanism 增加 GSSAPI 选项，七语
  label/description 全量，`manifest_contract_test.go` 同步守卫。

### 6.3 真跑容器暴露并修复的 Phase 1 遗留缺陷（单测不可见）

首轮 smoke FAIL 暴露的问题，全部已修并复验：

1. **无 group 消费直接建 client 失败**：`buildConsumeOpts` 无条件
   `kgo.DisableAutoCommit()`，franz-go v1.20.7 对无 group 的
   DisableAutoCommit 在 NewClient 即报 "invalid autocommit options"——
   S4/S7/S8/S11 全炸。改为仅 groupID != "" 时附加（commit 场景必有
   groupId，语义安全）。
2. **acls/list 空 operation 误报**：`aclOperationType("")` 落 default 报错；
   过滤语义空 = any，补 `case ""`（create 路径仍有 "not any" 保护）。
3. **connection/test 无参错误码**：`connection.id is required` 走了
   -32000，契约要求参数错 -32602；main 层补空 id 判定。
4. **流式会话永不出事件（bufferSize 恒 0）**：runLoop 的
   `PollRecords(session.ctx)` 无记录时无限阻塞，ticker 分支永远无法调度，
   ring/事件只在"消息持续流动"时才 flush。改为 poll 以 200ms
   （StreamBatchFlush）为上限 + `isDeadline` 过滤超时错误。真机验证
   `bufferSize` 从 0 → 6、`kafka/stream/messages` 事件正常到达。
5. **smoke 基础设施**（scripts/sidecar_client_jsonl.py）：`_read_line` 的
   `readline()` 无超时（deadline 仅前置检查），stream 空转时 sidecar 不发
   行导致整条套件挂死——改为 raw fd select + 超时；`stderr=PIPE` 无人读取
   存在管道写满阻塞 sidecar 的隐患——加后台 drain 线程 + 保留 200 行尾部
   供诊断。
6. **smoke S10 场景顺序**（scripts/smoke_test.py，既有场景一行修正）：
   S9 把 CURRENT_CONNECTION 切到只读连接后 S10 直接复用导致 create 必被
   拒——S10 开头恢复可写连接。

### 6.4 S11（Phase 2 SR 场景）与 Redpanda 兼容点

`docker-compose.kafka-test.yml` 新增 `redpanda-sr-test` 服务
（redpandadata/redpanda 单节点 dev-container、SASL 关闭、无任何凭据字面量；
容器名 dbx-kafka-sr-test，broker 19092→9092，SR 19081→8081，与主 kafka
9092 不冲突）。S11 流程：schema/test → topics/create → subjects/list →
register → produce(schema) → consume(schema) roundtrip（断言 schemaId/
schemaSubject/JSON 内容）→ compatibility global get / subject set /
check → delete/version + delete subject。实现差异处理：Redpanda 的
DELETE version 响应是**单个数字**（Confluent 为数组）→ 后端
`decodeDeletedVersions` 双形状兼容（含单测）；Redpanda 在 subject 最后
一个版本删除后即移除 subject（40401）→ S11 对该形状容忍。

### 6.5 Phase 2 验证证据

```
CGO_ENABLED=0 go vet ./...          # 0 告警
CGO_ENABLED=0 go test -count=1 ./...
# ok  io.dbx.kafka.plugin/internal/kafkaconn    （85 个测试函数）
# ok  io.dbx.kafka.plugin/internal/lifecycle
# ok  io.dbx.kafka.plugin/internal/store
# 合计 98 passed / 0 failed

# 真容器 smoke（apache/kafka 9092 + redpanda 19081）：
# No.   Scenario                                         Status
# S1    initialize + connection/test without params      PASS
# S2    connection/test fake cluster -> business error   PASS
# S3    connect + topics/list seeded topics              SKIP  (合法：__consumer_offsets 未建)
# S4    produce -> consume round-trip fidelity           PASS
# S5    consume valueFilter subset                       PASS
# S6    groups/list + acls/list shapes                   PASS
# S7    stream start -> events -> stop                   PASS
# S8    export json + csv                                PASS
# S9    read-only write rejection                        PASS
# S10   topics create + delete confirmTopic gate         PASS
# S11   schema registry .../compat/delete                PASS
# total=11 PASS=10 FAIL=0 SKIP=1
```

### 6.6 Phase 2 遗留与风险

1. **Kerberos 无真实 KDC 验证**：按契约仅做参数构造/校验单测（principal
   解析、keytab 必填、合成 keytab v2 + krb5.conf 下机制可构建、krb5.conf
   回落顺序）；真实 GSSAPI 握手需 KDC/keytab 环境做 e2e 复核。
2. **ZooKeeper 发现正路径**：单测覆盖地址/broker JSON 解析与不可达错误
   路径；`/brokers/ids` 正路径由容器/集成验证（本容器组无 ZK，未跑），
   chroot 走 go-zookeeper 原生语义。
3. **PROTOBUF 载荷编解码未实现**（明确 -32000 报错），SR 的 protobuf
   subject 仅支持元数据/浏览/diff；如需编解码登记后续期。
4. **JSON Schema 校验依赖 jsonschema-go**；`format` 等
   关键字的覆盖面以该库为准。
5. **AWS Glue SR**：按契约登记 Phase 3。
6. scripts/sidecar_client_jsonl.py 的两处基础设施修复（读超时/stderr
   drain）超出"仅追加 S11"字面范围：不修则 S7 永远挂死（select 前
   readline 无界），属让套件可真实运行的必要修正，已在此报备。

## 7. Phase 3：AWS Glue Schema Registry 接入（2026-09-05 增补）

按冻结契约补齐字面功能缺口：AWS Glue SR **管理面**。Glue API 逐一映射实现
（kafkaGlueClient / ListSchemas / ListSchemaVersions / GetSchemaVersion /
RegisterSchema / Get·Set·CheckSchemaCompatibility / DeleteSchema）。

### 7.1 交付内容

1. **`backend/internal/kafkaconn/glue.go`（新增）**：aws-sdk-go-v2
   （config/credentials/service/glue + smithy-go）Glue 客户端与后端实现。
   auth_mode 归一化 `default|static`（static = NewStaticCredentialsProvider；
   aws-profile 凭据模式 sidecar 场景不提供，协议文档登记 Phase 3 后续，
   传 `profile` 明确报错）；`glueBaseEndpointOverride` 包级 seam 供单测注入
   httptest 端点（生产恒空）。compatibility 枚举映射 `normalizeGlueCompatibility`
   （`*_TRANSITIVE` 输入归并 `*_ALL`，另支持 Glue 特有 `DISABLED`）；
   `isAWSNotFound`（smithy APIError code 含 notfound）供 register 探测分流。
2. **provider 抽象**（schema.go）：新增 `schemaBackend` 接口 +
   `confluentBackend` 适配器 + `glueBackend` 双实现，11 个 `kafka/schema/*`
   Service 方法全部改为 backend 分发；`resolveSchemaProvider` 实现 registry
   参数解析（显式 confluent|glue / 自动探测 / 双配置歧义与未知值 →
   `InvalidParamsError`，main.go `bizError` 据此映射 **-32602**，其余业务错
   仍 -32000）。
3. **挂载门禁**（messages.go/stream.go）：produce/consume/stream start 的
   `schema{}` 增加 `registry?`；provider=glue → 业务错
   "schema-aware produce/consume currently supports Confluent wire format;
   AWS Glue schema management is available"（语义与既有口径一致，不发明 Glue
   wire format）；`schemaClientFor` 正名为 `confluentClientFor`。
4. **Profile/secrets**：`glue_region/glue_registry_name/glue_auth_mode/
   glue_access_key_id`（config）+ `glue_secret_access_key/glue_session_token`
   （secret，凭据红线：只进 SigV4，不落日志/审计/事件）。
5. **statuses**：`schemaRegistry` 改为恒出 `{enabled, provider, url?,
   registryName?}`，provider 取值 `confluent|glue|both|none`。
6. **manifest.json**：六新字段（与 sr_url 平级、无 visible_when；
   glue_secret_access_key/glue_session_token 为 secret binding）+ 七语
   label/description（glue_auth_mode options 亦七语）；
   `manifest_contract_test.go` 同步（config/secret 落点、取值面、无
   visible_when 断言、七语完整性）。
7. **协议文档**（PROTOCOL_KAFKA §3.7/§3.8/§9）：registry 参数与自动探测、
   Glue manifest 字段、Glue 各方法语义（id=0 + versionId、per-schema 兼容
   级别无全局、check 纯语法校验、register Create/Register 双路径、delete
   单版本/整 schema 的版本清单差异）、挂载 glue 业务错。
8. **smoke**（scripts/smoke_test.py，仅追加）：S12 —— 仅当
   `GLUE_TEST_REGION`+`GLUE_TEST_REGISTRY` 存在才执行（本地无 Glue 容器，
   否则 SKIP）；static 模式凭据全部经 `GLUE_TEST_*` 环境变量透传，脚本零
   字面量。覆盖 test(provider=glue)/subjects/versions/get/compare/
   compat get·set·check（含全局 get 必败断言）/register create+version/
   delete version+subject/挂载业务错。

### 7.2 实现差异与取舍（Glue 后端语义备注）

- **数字 schemaID**：Glue 无数字 id，`id` 恒 0，GUID 走新增
  `versionId?:string` 字段（SchemaVersionInfo/SchemaGetResult/
  SchemaRegisterResult/SchemaMeta 增量字段，confluent 不受影响）。
- **全局兼容级别**：Glue 仅 per-schema，compatibility get/set 的
  subject 为空 → `-32000`；set 增加可选 `version`（UpdateSchema 版本
  检查点，缺省 LatestSchemaVersion）。
- **compatibility/check**：映射 CheckSchemaVersionValidity
  （纯语法校验、无副作用），`isCompatible`=Glue Valid，messages 首条为
  无副作用说明。
- **register**：GetSchema 探测 404（EntityNotFound）→ CreateSchema
  （`compatibility` 缺省 NONE；`references` 为 Confluent 概念、忽略），
  存在 → RegisterSchemaVersion。
- **delete**：version>0 → DeleteSchemaVersions（本版 SDK 未建模
  VersionNumbers，按 SchemaVersionErrors 折算：无错误 = 已删）；subject
  整删 → `deletedVersions` 空数组（Glue 不返回清单）。
- **subjects/list**：Glue 列表级无 DataFormat/版本/兼容级别（不逐条回查），
  formats 空数组、description 透出；versions/list 补一次
  GetSchema 取全版本同款 DataFormat。

### 7.3 Phase 3 验证证据

```
CGO_ENABLED=0 go vet ./...            # 0 告警
CGO_ENABLED=0 go test -count=1 ./...  # 全绿（118 个测试函数，较 Phase 2 +20）

# glue 专项（httptest 假 Glue JSON-RPC，不连真实 AWS）：
# --- PASS: TestResolveSchemaProvider            （探测/歧义 -32602/未知值）
# --- PASS: TestNormalizeGlueCompatibility       （枚举映射全表）
# --- PASS: TestGlueDataFormatAndNotFound        （DataFormat/EntityNotFound）
# --- PASS: TestNewGlueClientValidation          （region/static 校验、profile 拒绝）
# --- PASS: TestGlueSchemaServiceFlow            （subjects/versions/get/compare/
#                                                 compat/register 双路径/delete/
#                                                 错误透传）
# --- PASS: TestSchemaMountGlueRejected          （produce/consume/stream 挂载
#                                                 glue 业务错；双配置 -32602）
# --- PASS: TestSchemaRegistryStatusProvider     （statuses provider 摘要）
# manifest：glue fields ok（六字段 + 七语，契约测试全绿）

python3 scripts/smoke_test.py         # 无 Glue 环境：S12 SKIP（其余场景不受影响）
```

### 7.4 Phase 3 遗留与风险

1. **无真实 AWS Glue 环境**：正路径全部由 httptest 假 Glue（JSON-RPC 按
   X-Amz-Target 路由，含 EntityNotFound 错误形状）覆盖；签名请求的真实
   AWS 往返需 `GLUE_TEST_REGION`+`GLUE_TEST_REGISTRY` 环境跑 S12 复核。
2. **aws-profile 凭据模式**：sidecar 场景不提供（shared
   config 的 default 链已覆盖本机 profile 场景），协议文档已登记。
3. **produce/consume 的 schema 编解码不含 Glue**：仅 Confluent wire format，
   Glue 消息解码如需支持须引入 Glue 的
   schema registry 消息头格式，登记后续期。
4. **SDK DeleteSchemaVersionsOutput 未建模 VersionNumbers**：单版本删除
   结果按 SchemaVersionErrors 折算（无错误即成功），与 AWS 控制台语义
   一致；若 SDK 后续补全字段可改为回读精确清单。

## 8. 连接表单条件显隐 + 必填校验（2026-09-05 增补，实施路 agent I）

### 8.1 背景与交付

用户反馈：连接表单 AWS Glue 等字段全部平铺、无条件显隐、无必填校验。
本轮以宿主 1.1 的 `visible_when` / `required_when`（单字段条件
`{field, one_of[]}`）落地，并新增 **`schema_registry` 决策字段**
（select：`none`/`confluent`/`aws_glue`，默认 `none`，binding config，
置于 `sr_url` 之前）统一裁决 SR 后端：

- **显隐矩阵**：`sr_*` 三件挂 `schema_registry ∈ [confluent]`；
  `glue_region/glue_registry_name/glue_auth_mode` 挂 `[aws_glue]`；
  `glue_access_key_id/glue_secret_access_key/glue_session_token` 挂
  `glue_auth_mode ∈ [static]`；SASL 账密/Kerberos 五件套/TLS 字段维持
  原 `security_protocol`/`sasl_mechanism` 挂点。
- **必填矩阵**：`sr_url`、`glue_region`、`glue_registry_name`、
  `glue_access_key_id`、`glue_secret_access_key`（static 时 AK/SK）、
  `sasl_username`/`sasl_password`（PLAIN/SCRAM 时）、
  `kerberos_principal`/`kerberos_keytab_path`（GSSAPI 时）；
  `sr_username`/`sr_password`/`glue_session_token`/`kerberos_service_name`
  （有默认值）/`kerberos_realm`/`kerberos_krb5_conf_path` 恒可选。
  全部新增 required 字段的七语 description 注明必填条件。
- **backend**：`Profile.SchemaRegistry` 解析（缺省空 = 旧连接按
  `sr_url`/`glue_*` 自动探测回退，向后兼容）；`resolveSchemaProvider`
  以开关为准（none 禁用 SR、开关与显式 registry 参数冲突 → `-32602`）；
  `validateRequiredCombination` 兜底校验矩阵（缺失 → `*InvalidParamsError`
  → `-32602`，Connect/Test 均过）；`kafka/connections/statuses` 的
  `schemaRegistry` 摘要新增 `mode`（开关归一值，旧连接省略）；指纹纳入
  开关与 SR/Glue 参数。
- **frontend**：`mockDbxHost.ts` connection fixture `external_config` 加
  `schema_registry`（默认 confluent，`?glue=1` 时 aws_glue），statuses
  fixture 增补一条 `schema_registry=none` 对照行（SR 徽标显示「未启用」）；
  `ConnectionsPanel` 徽标逻辑核对无需改动（`enabled=false` → srOff 文案，
  provider=none 已兼容）。

### 8.2 宿主契约核对结论（required_when 真实消费）

- `host/plugins/manifest.schema.json` L142-167：`fieldCondition={field,one_of[]}`，
  `visible_when`/`required_when` 同构、单字段无 AND。
- 宿主前端 `pluginFieldConditions.ts`：effective required = static `required`
  OR 匹配的 `required_when`；连接对话框渲染必填标记并阻断提交；
  binding=password 的 required 字段触发共享密码提示（`connectionPassword.ts`）。
- 宿主 Rust 核心 `host.rs`：连接校验对「可见 + required + 空」字段报错，
  与前端语义镜像；隐藏字段不参与必填校验（MCP/import 路径友好）。
- **结论**：`required_when` 宿主已实现并被消费；sidecar 兜底校验为纵深
  防御 + 非对话框写路径保障，矩阵与宿主一致（详见 PROTOCOL §9.2）。

### 8.3 验证证据（真实跑）

```text
backend：CGO_ENABLED=0 go vet ./... 通过；
         CGO_ENABLED=0 go test -count=1 ./... 全绿
         （新增 required_matrix_test.go：TestRequiredCombinationMatrix 14 例
         必填矩阵、TestSchemaRegistrySwitchResolvesProvider 11 例开关语义、
         TestSchemaRegistryStatusesExposeMode、TestNormalizeSchemaRegistry；
         manifest_contract_test.go 同步显隐/必填矩阵与 schema_registry 七语）。
frontend：pnpm typecheck 通过；pnpm test 6 files / 52 tests 全绿。
manifest：python 断言脚本「manifest conditional fields ok; fields total = 30」
         （schema_registry 存在、sr_*/glue_* visible_when、required_when
         矩阵、七语 options 全量）。
```

### 8.4 遗留

1. **真机表单复核**：显隐矩阵在真实 DBX.app 连接对话框的交互表现（切换
   `schema_registry` 时字段淡入淡出、必填星号）建议随下次宿主 e2e 批次
   目验（本轮以宿主源码契约 + 单测 + manifest 断言为据）。
2. **旧连接 statuses 的 `mode` 省略**：前端类型未消费 `mode`，仅协议文档
   登记；如后续徽标需区分「旧连接自动探测」与「显式开关」，再补前端读取。
3. glue 连接 fixture 的 `glue_auth_mode: "access_key"` 为 mock 装饰值
   （取值面实为 default/static），与真表单无关，本轮未改（保持既有视觉
   断言稳定）。

## 9. Phase 3 特性追赶（2026-09-07，G 路 worktree 并发实施）

> 分支 `phase3/kafka-backend`（commit `4449e36`），契约依据 IMPL_PLAN
> §12.2（冻结）；对标对象与裁决记录见 IMPL_PLAN §12.0/§12.8。

### 9.1 交付（4 特性 + manifest/协议/fixture）

- **F1 PROTOBUF 编解码**（schema.go）：`decodeSchemaPayload`/
  `encodeSchemaPayload` 增 protobuf 分支（替换"未实现"报错）；解码
  base64(FDSet)→`protodesc.NewFiles`→`dynamicpb`→`protojson` 渲染；
  编码 protojson→`proto.Marshal`→`encodeWireFrame`；消歧三分支（单
  message / subject 剥 `-key`/`-value` 后缀 PascalCase 尾段唯一命中 /
  报错列候选全名）；`SchemaRef.Format` 扩为 `avro|json|protobuf`。
  新依赖 `google.golang.org/protobuf v1.36.12`。
- **F2 OAUTHBEARER**（新 `oauth.go` + types/client）：取值面 +
  `NormalizeSASLMechanism`；validateProfile 增 SASL_SSL 约束与
  token_source 矩阵；TokenProvider 双实现——`msk_iam`
  （aws-msk-iam-sasl-signer-go v1.0.4，默认链 + 显式 AK/SK 覆盖，照
  §11.5 Glue 范式）、`static_token`（Expiration=0）；franz-go
  `pkg/sasl/oauth` 机制层；单次取 token 10s 超时收敛；connSecrets 增
  msk_secret_access_key/msk_session_token/oauth_static_token（secret
  binding，不落日志/审计，指纹纳入）。
- **F3 `kafka/topics/records/clear`**（topics.go + main.go + policy）：
  ListEndOffsets 取 hw → DeleteRecords(offset=hw)；KIP-107 旧 broker
  业务错透传；confirmTopic 门禁（复用 ensureTopicDeleteConfirm 语义，
  不匹配 -32602）；审计 `topics.records.clear`。
- **12.2.4**：TopicInfo 增 `isHealthy`/`unhealthyPartitions`（topicInfos
  内按 partitionInfos 同款规则聚合，零额外请求）。
- manifest：sasl_mechanism +OAUTHBEARER、6 新字段七语全量、visible_when
  联动链；manifest_contract_test/required_matrix_test 同步扩展（+8 行
  矩阵）。PROTOCOL：§3.2 records/clear 行、topics/list 健康度、Format
  枚举 protobuf、§3.8 消歧语义。
- fixture：`cmd/gen-protobuf-fixture`（无 protoc，descriptorpb 程序化
  生成）→ `scripts/kafka-seed/protobuf/orders.proto` + `orders_fdset.b64`。

### 9.2 验证

- 基线 109 例全绿 → 最终 128 顶层用例 + 19 子用例全绿（新增 33 例）：
  protobuf 6（roundtrip 含 int64 字符串已知差异断言 / wire frame 全链路 /
  消歧三分支 / 坏 FDSet / 非 wire format / 尾段助手）、clear+健康度 5
  （门禁 6 子用例矩阵 / rows 形状 / 健康聚合）、oauth 8（normalize /
  provider 选择 / static / msk 校验 / SASL 构建 / lifecycle / JSON 形状）。
- smoke（主线）：S13 真跑 PASS（confirmTopic 门禁 + 收空 + offsets 收敛
  + rows 形状）；S15 开门验证 static_token+SASL_SSL 对 PLAINTEXT broker
  得 -32000 TLS 业务错（参数链/secret binding 贯通）；S14 有据 SKIP
  （见 §9.3）。全套 test.sh 全绿（package `io.dbx.kafka-0.1.13` dbxp）。

### 9.3 裁决与遗留

- **采纳**：secret 三字段入 connSecrets 而非 Profile（§6 红线，G 路提案
  经主线裁决采纳）；消歧 PascalCase 严格相等（复数 subject 不命中，
  PROTOCOL §3.8）；protojson int64 字符串（§12.7 既定，测试锁定）。
- **S14 降级**：Redpanda 内置 SR 仅接受 .proto 文本形态，base64(FDSet)
  被当文本解析存储（POST 200 但回读不符）→ 本地无法真跑 roundtrip；F1
  按 Confluent SR 正确形态实现。遗留：sidecar 双形态兼容（.proto 文本或
  `GET /schemas/ids/{id}/schema?format=serialized`）以复跑 S14；Confluent
  references 多文件 schema 的 FDSet 依赖解析。
- **观察登记**：既有 topics/delete confirmTopic 不匹配 -32000 与
  PROTOCOL §3.2 -32602 的偏差（Phase 1 遗留）——维持现状，待对齐。
- 真实 MSK 往返（S15）待真环境；无 IMDS 环境超时实测待做。

## 10. 测试覆盖完善轮（2026-09-07，backend 覆盖 agent）

### 10.1 交付

- 总覆盖率 **55.1% → 73.0%**（kafkaconn 72.8% / lifecycle 86.8% / store
  76.1%）；新增 7 测试文件 + helpers_test/lifecycle_test 扩展，共 49 个
  测试函数：`acls_test.go`（builder/枚举矩阵/门禁与审计 9）、
  `groups_test.go`（三表合并/lag/Option 语义/校验矩阵 8）、
  `messages_more_test.go`（四通道 matcher/操作符全枚举/离线校验 12）、
  `stream_more_test.go`（appendBatch/会话控制/节流 emit 5）、
  `topics_more_test.go`（configEntries/listedOffsetRows/confirm 守卫 4）、
  `client_more_test.go`（filter 数值/状态三态/离线构造/关闭幂等 6）、
  `service_more_test.go`（Test 拨号路径/CloseAll/缓存矩阵/挂载门禁 5）、
  helpers +5、lifecycle +3。

### 10.2 契约修复

- `ensureTopicDeleteConfirm` 全部失败路径 `fmt.Errorf` →
  `*InvalidParamsError`（-32602，PROTOCOL §3.2 冻结语义，与
  `topics/records/clear` 对齐）；policy_test 类型断言 +
  `TestDeleteTopicsConfirmGateMapsInvalidParams` Service 层锁定；smoke
  S10 断言同步（`!= -32602`）并 PASS；ClearTopicRecords 既有 errors.As
  包装行为不变。§12.8 遗留第 4 条关闭。

### 10.3 仍 <50% 清单（全部 broker/网络依赖，不强测）

topics/groups/acls 的 withAdmin 回调体（List/Describe/Create/Delete/
Reset 17%-48%）、Produce 的 ProduceSync 拨号后路径（37%）、
consumeMessages 的 PollRecords 主循环（35%）、stream runLoop（0%）、zk
discoverBrokersViaZK 读取体、buildClientOpts(WithSeeds) 44%/47%、
closeLocked 40%、buildOauthSASLOpt authFn 闭包（36%）。方法头部离线
校验/门禁分支均已覆盖。后续路线：内存 Kafka testcontainer 或 withAdmin
client 接口抽象（超出本轮"不重构"约束，登记不实施）。

### 10.4 验证

`CGO_ENABLED=0 go vet ./...` 通过；`go test ./... -count=1` 3 包 ok
（kafkaconn 169 用例全 PASS）；cover total 73.0%；smoke 全套
`total=15 PASS=11 FAIL=0 SKIP=4`（S10/S13 -32602 断言 PASS）。

## 11. 连接表单幽灵必填修复：oauth_token_source 不再声明 default（2026-09-07）

- **症状（用户报告）**：新建连接表单默认就要求填 AWS region，PLAINTEXT/SCRAM
  普通 Kafka 集群无法添加连接。
- **根因（宿主 × manifest 叠加）**：宿主 `hasRequiredConnectionTarget` 要求
  "所有字段：不可见或非必填或有值"。manifest 中 `oauth_token_source` 声明
  `default: "msk_iam"`，而宿主条件求值（`pluginFieldConditions.ts`）在表单值
  缺失时回退字段 default 代入下游 `visible_when`/`required_when` —— 于是
  `msk_region` 在 sasl_mechanism 仍为 PLAIN（其自身及 oauth_token_source 均
  不可见）时被判为"可见 + 必填 + 无值"，保存/测试按钮全部禁用。联动链
  security_protocol → sasl_mechanism → oauth_token_source → msk_region 的
  级联可见性宿主侧不成立（条件只看被引用字段的值，不看该字段是否可见）。
- **修复（两处）**：
  1. 宿主（二开补丁，见 `shared/PROGRESS-HOST-SUBREPO.zh-CN.md` §22）：
     `pluginFieldIsVisible` 支持级联（传入 sibling resolver，条件引用的
     字段自身不可见则条件不成立，带 seen 防环）；两个调用方
     （PluginConnectionFields.vue / ConnectionDialog.vue）传入 resolver。
  2. 插件 manifest 兜底（旧宿主不级联时也不被卡死）：`oauth_token_source`
     移除 `default`（不声明时旧宿主条件求值得 undefined → msk_region 隐藏）；
     后端空值语义不变（`NormalizeOauthTokenSource` 空值回退 msk_iam，且仅在
     OAUTHBEARER 分支校验 msk_region），已存连接不受影响。副作用：MSK 用户
     需显式选一次 token source（更符合"显式优于隐式"）。
- **守护**：`manifest_contract_test.go` 新增断言 `oauth_token_source.default
  必须为 nil`（防止回填复发）；宿主 `pluginFieldConditions.spec.ts` 新增
  kafka 形联动链 dormant/复活用例 + 未知字段/自环用例；
  `PluginConnectionFields.spec.ts` 新增 msk_region 幽灵必填回归用例。
- **协议同步**：`PROTOCOL_KAFKA.zh-CN.md` §9 字段表 default 列改"—（不声明
  default）"并注明原因；manifest version 0.1.14 → 0.1.15。
- **验证**：宿主相关 spec（pluginFieldConditions 8 + PluginConnectionFields 8
  + connectionPassword/connectionStore 30 + connection 目录 10）全绿；
  kafka `scripts/test.sh` all green（前端三件套 + go vet/test + 打包
  io.dbx.kafka-0.1.15-darwin-arm64.dbxp + smoke `total=15 PASS=11 FAIL=0
  SKIP=4`，SKIP 均为设计内）。
- **遗留**：用户期望的"连接类型选择器"（原生 / Confluent / AWS MSK 一键
  预设）受宿主条件模型限制（`visible_when` 仅单字段 one_of，预设需要多条件
  AND），本期不实施；现表单已可通过 security_protocol × sasl_mechanism ×
  schema_registry 组合表达全部形态，如需一键预设须先扩展宿主条件模型。

## 12. 消息页 UI 微调：topic 选中态与空态居中（2026-09-07）

- **反馈（用户）**：① 消息页左侧 topic 树选中行是整行实底色块，"高亮和底色
  反了"不好看；② 右侧空态「暂无消息——请调整条件后重新消费」不居中。
- **修复**：
  1. `style.css` `.tree-row.selected`：整行 `var(--accent)` 实底 →
     `--primary` 14% 轻底 + 文字/图标（icon-violet/icon-neutral）转主色，
     选中靠"轻底 + 主色高亮"表达而非实色块；hover（accent 65%）不变。
  2. `MessagesPanel.vue` 未消费空态（P2-21 两态空态）包进
     `grid-box grid-box--fill` 与结果区同容器；`style.css` 增
     `.grid-box > .empty { flex: 1 1 auto }`——grid-box 是 row 向 flex 容器，
     空 p 作为 flex item 默认按内容宽靠左，撑满后自身 justify-content 才
     生效（结果区空态 1126 同路径一并修正）。
- **验证**：mock 页（?theme=dark/light）浏览器实测——选中行计算样式
  primary 14% 底 + rgb(59,130,246) 字，空态几何中心与容器双向偏差 <2px；
  前端 typecheck + 219 单测全绿（MessagesPanel.spec 空态文案断言不受
  容器包裹影响）。纯样式/模板结构调整，协议与 sidecar 无涉。

## 13. 消费启动提速（复用池 + 超时分层）与流式面板 AG Grid 化（2026-09-07）

- **根因（TSS-MEP-INT 实测取证）**：kgo client 冷启动要完整走 TCP+TLS 握手
  +SASL+metadata+ListOffsets，跨境 SASL_SSL 链路实测单次 5-11s；而一次性
  消费前端默认 `timeoutMs=5000` 且该超时覆盖整个请求——启动没完成就被掐掉
  （earliest 同样 0 条必现；`latest` 默认策略在无新流量的 topic 上再叠加
  一层 0 条）。流式会话 `context.Background()` 长跑，启动开销只付一次，
  故「流式能拉到、一次性拉不到」。
- **修复（backend）**：
  1. 消费 client 复用池 `consume_pool.go`：一次性消费按「连接+消费形状」
     缓存 kgo client，复用前 drain（50ms，仅清内存缓冲）+ `SetOffsets`
     重置回策略起点（-2/-1 哨兵值经 franz-go directConsumer.applySetOffsets
     原样进 Offset.at，已核对源码）。复用面保守：非 group 且 strategy ∈
     {空, default, earliest, latest}；group/精确起点策略、占用中、重置失败
     均回退 per-request 新建。池上限 8 条，空闲 2min 惰性回收，断连/退出
     清池。inUse 排他防并发争用。
  2. 超时分层 `consumeMessages`：启动期预算 `max(2×窗口, 12s)` 内
     `client.Ping` 等 metadata ready，用户 `timeoutMs` 只约束扫描窗口——
     5s 窗口不再被启动吃掉。Ping 失败显式报错（原来 5s 静默空结果）。
  3. TLS 会话缓存共享方案**评估后放弃**：共享 `ClientSessionCache` 在
     中间盒/网关终结 TLS 的集群入口实测触发 broker 立即断连
     （dial 后 closed，提示 TLS misconfigured），风险大于省 1 RTT，
     已回滚并在 `buildTLSConfig` 注释留档。
- **修复（frontend）**：`StreamPanel.vue` 消息列表从自绘 `v-for` 行迁移到
  `DbxAgGrid`（与 MessagesPanel 同一套 `toMessageRows`/`messageColumns`/
  `MINIMAL_MESSAGE_FIELDS` 列模型；quickFilter 防抖透传 grid；autoScroll
  经 `goToLatest()` 跳末页；环形缓冲「更早/更新」分页保留；行点击无详情
  抽屉故 `emit-row-click=false`）。清理 `stream-log/stream-row/stream-scroll`
  死样式与七语死键 `stream.quickFilterNoMatch`（过滤无命中由 ag-grid
  内建 noRowsToShow 七语空态接管）。
- **验证（TSS-MEP-INT 直连 sidecar 实测）**：
  - 修复前（0.1.17）：earliest+5s → 0 条必现（纯超时）；earliest+20s →
    10 条，wall 5.3-10.7s，三次重复无复用收益。
  - 修复后：earliest+5s cold → 34-47 条（启动不吃窗口）；hot → 44-100 条，
    wall ≈5s（窗口语义，凑满 limit 提前返回）；本地低 RTT 集群复用路径
    0.24s（drain 200ms 版）→ 0.07s（50ms 版）。
  - `scripts/test.sh` all green：前端 typecheck + 219 单测（StreamPanel.spec
    改 mock DbxAgGrid 桩断言落表内容/quickFilter 透传/分页 clamp）+
    go vet/test（新增 consume_pool 策略表/签名/池状态机 7 用例）+ 打包
    io.dbx.kafka-0.1.19 + smoke `total=15 PASS=11 FAIL=0 SKIP=4`（设计内）。
- **遗留**：① 消费空结果仍无法区分「无数据」与「窗口不足」（ConsumeResult
  未透出 timedOut/启动耗时，需协议增字段，另行立项）；② 前端默认
  `offsetStrategy=latest` 未改（用户未拍板；浏览历史消息场景建议 earliest）；
  ③ 流式面板无详情抽屉（行点击不弹 JSON 详情，与消息面板不对称）。

## 14. 消息面板条件抽屉化 + topic 下拉（2026-09-07）

- **反馈（用户）**：① 消费条件占满面板上部，太占空间，希望做成抽屉样式；
  ② 条件区 topic 只读不支持下拉，左侧树占地方，不如下拉框实用。
- **改造（frontend）**：
  1. 消息面板顶部改为**单行消费条**（consume-bar，常驻 ~40px）：topic 下拉
     （props.topics 与左侧树同源 App.topics，emit selectTopic 走 App.selectTopic
     双向联动）+「条件」开关（SlidersHorizontal icon，活跃过滤条件数 badge）
     + 摘要 chips（策略/条数/消费组/解码）+ 主消费按钮；原收起态摘要条取消。
  2. 条件全量收进 **consume-drawer 浮层**（absolute 面板内，从消费条之下展开
     max-height 72%，backdrop 点击收起）：基础（去掉 readonly topic 字段）/
     定位/时间与范围/过滤/解码五分组 + 预设 + 收起 + 消费原样搬入；
     `v-show` 保 DOM（消费表单既有单测无需先开抽屉）。
  3. 开合语义调整：`dbx.kafka.ui.msgFormOpen` 记忆保留，默认收起（原默认
     展开）；消费成功后自动收起抽屉（原"有结果后收起为摘要条"逻辑取消）。
- **验证**：mock 页浏览器实测——消费条单行、抽屉展开不挤压表格、下拉选择
  user-signup 后侧栏高亮与头部 badge 同步切换、消费成功抽屉自动收起且结果
  落地（已扫描 3 · 命中 3）；typecheck + 221 单测全绿（摘要条重消费按钮
  定位更新为 consume-bar 的 data-testid=consume-run）；smoke all green
  （PASS=11 FAIL=0 SKIP=4）。纯前端改动，协议与 sidecar 无涉。
- **说明**：左侧 topic 树为全部面板共享的 App 级布局，本次未动；消息面板
  已有下拉 + 侧栏自带折叠按钮，若要全局以下拉取代树需另行立项。

## 15. 详情抽屉二轮：XML 高亮/格式化 + 折叠区块 + headers 限高（2026-09-07）

- **反馈（用户）**：① value 缺 XML 等格式的高亮配色与格式化；② Headers/Value
  应为可折叠区块；③ headers 多时高度失控，应限高滚动。
- **改造（frontend）**：
  1. **XML 支持**：`ValueFormat` 增 `xml`；`prettyXml`（kafkaModel，词法级
     缩进，不做 DOM 解析）——含 DOCTYPE/ENTITY 的载荷**原样返回不重排**
     （安全红线：不解析不展开不可信实体），良构性词法校验（标签栈匹配）
     不平衡即放弃；`<a>text</a>` 叶子同行、自闭合/注释/CDATA/PI 保序。
     自动识别：`looksLikeXml`（`<` 起始）优先于 JSON，打开详情即选对格式。
     CodeEditor 增 `xml` 语言（新增依赖 @codemirror/lang-xml 6.1.0，
     HighlightStyle 走 --cm-* 变量对齐明暗两套）。mock codec-lab 增 XML
     明文样本（详情走查 fixture 先例）。
  2. **折叠区块**：Headers/Value 标题行整行可点（ChevronDown 旋转 + aria-
     expanded），body v-show；打开详情默认全展开。
  3. **headers 限高**：表格包 `.kv-scroll`（max-height 160px 内部滚动，
     表头 sticky）；JSON 视图同步 220px 上限。
- **验证**：mock 实测——XML 消息自动识别 format=XML、pretty 缩进（11 行
  结构化输出）+ 标签/属性高亮、折叠头 chevron 左置、Headers 收起 body
  隐藏；`prettyXml` 单测 4 例（良构缩进/不良构原样/DOCTYPE+ENTITY 红线/
  实体保留）；typecheck + 225 单测全绿；smoke all green（PASS=11 FAIL=0
  SKIP=4）；打包 io.dbx.kafka-0.1.20。纯前端改动，协议与 sidecar 无涉。

## 16. 详情抽屉三轮：标题行合并操作 + 编辑器吃满底部（2026-09-07）

- **反馈（用户）**：① Headers/Value 标题行与下一行的切换/复制按钮应合并成
  一行，省高度；② value 高亮展示区应吃满抽屉底部剩余高度，而不是固定
  320px 后留白。
- **改造（frontend，纯样式/模板）**：
  1. 区块头单行化：`detail-block__head` = 左侧折叠热区（chevron+标题的
     `detail-block__toggle` button）+ 右侧 `detail-block__actions`
     （表格/JSON 切换、完整值、下载、复制 icon）；折叠收起时 actions 一并
     隐藏。body 里不再有独立操作行（每区块省 ~24px）。
  2. 编辑器吃满：drawer-body `overflow: hidden` + value 区块
     `flex: 1 1 auto; min-height: 220px` + CodeEditor 去 max-height
     （flex: 1 1 auto, min-height: 0），CodeMirror 内部滚动；错误/完整值
     pre 视图同样 flex 拉伸。修复过程中发现并清掉一条后置
     `.drawer .dbx-code-editor { flex: 0 0 auto }` 旧规则（同特异性后到
     覆盖导致 flex 拉伸失效）。
- **验证**：mock 实测 evaluate 量高——valueBody 448px、编辑器 402px
  flex=1 1 auto 拉伸到底，截图确认；typecheck + 225 单测全绿；smoke all
  green（PASS=11 FAIL=0 SKIP=4）；打包 io.dbx.kafka-0.1.22。

## 17. 代理路由传输层测试 + main 接线层离线测试（2026-09-20）

- **背景**：DBX 宿主 structured proxy route（runtime.proxy，SOCKS5）此前只有
  种子保留 / 类型校验 / 诊断标签 3 个浅层断言，传输行为无任何实拨验证；
  main.go 接线层（MCP 轮新增的根包）自上次覆盖轮后完全无测试（0.7%）。
- **新增 `internal/kafkaconn/proxy_test.go`（9 用例）**：内嵌迷你 SOCKS5
  服务器（RFC 1928 + 可选 RFC 1929 认证，CONNECT 地址回显记录 + 可固定
  转发）做端到端实拨——明文路由回环实拨（CONNECT 地址逐字透传）、
  用户名/密码认证正误两向、TLS 包装 + SNI 按拨号地址推导（服务端
  ConnectionState 断言 `db-broker.internal`，域名形态 CONNECT 不预解析）、
  kgo 选项互斥回归（代理路由下 Dialer/DialTLSConfig 不得并存，连接测试
  探针路径同查，即 issue #28 修复的代理侧护栏）、seedBrokers 优先级补遗
  （代理+无 bootstrap→runtime 端点；直连时 runtime 优先 bootstrap）、
  computeFingerprint 代理敏感（换代理或凭据轮换必换指纹→失效重建）、
  lifecycle 解析 runtime.proxy 回环。
- **新增 `wiring_test.go`（根包 0.7%→61.9%，全仓 68.9%→74.2%）**：Handle
  全方法分派矩阵（每方法 `{}` 门禁错误码逐一断言 + 未知方法 -32601）、
  离线成功链（connect→statuses→disconnect、presets 增删改查、mcp/tools、
  settings 白名单边界、mcp/call 成功信封与四类拒绝、ui/state/report 快照
  与 intent 拒绝、stream stop-all 与未知 session）、审计落盘 + kafka/audit
  事件双通道断言、Stream Emitter 适配（nil no-op / 实发事件 / writer 故障
  只记日志；SDK Emitter 经 Server.Serve 真实分发捕获，不触私有字段）。
- **烟测 S18**（scripts/smoke_test.py）：进程内 SOCKS5 double + 宿主形态
  `lifecycle_params(runtime_extra)`（模拟宿主注入 runtime.proxy）——明文
  路由 connection/test + topics/list 数据路径过隧道、认证强制与错误凭据
  拒绝（服务端 rejected_auth 断言）、不可达代理快速失败、http 类型参数级
  拒绝。全量 `total=18 PASS=14 FAIL=0 SKIP=4`（SKIP 均设计内/既有观察：
  S3 internal-flag 竞态、S12 Glue、S14 redpanda FDSet、S15 MSK）；
  smoke_mcp 19/19 全绿。
- **观察登记（不实施）**：zookeeper 连接源 + 代理路由组合时，ZK 发现拨号
  不经代理（`zkSeedsForTest` 直连），broker 拨号仍走代理；如需支持须先
  设计讨论。
- **验证**：`go vet` / `go test ./... -count=1` 全绿（kafkaconn 217 用例、
  根包 11 用例）；打包链路 `scripts/build.sh` 复跑通过。

## 18. 2026-09-26 审查修复（族 REVIEW-FAMILY-2026-09-26 的 kafka 部分）

> 依据 `shared/REVIEW-FAMILY-2026-09-26.zh-CN.md`，本插件 HIGH 1 + MEDIUM 4 +
> LOW 5 共 10 项全部修复（L4 含前端/i18n/协议文档同步；MCP 未暴露组删除工具，
> 无 MCP 两阶段改动）。main 分支工作区直改，无 commit/push。

| 编号 | 修复 | 位置 |
|---|---|---|
| KAFKA-H1 | `maxDecodedBytes`（16MiB）解压上限：gzip/lz4/snappy-framed 走 `readBounded`（LimitReader max+1），zstd `DecodeAll` 显式长度校验 + `WithDecoderMaxMemory` 兜底，snappy block `DecodedLen` 预检；超限报错按既有 DecodeError 语义进消息不中断消费。`valueText` 与 `valueBase64` 同用 `maxMessageBytes` 截断并共用 `truncated` 标志（digest 双字段 GB 级驻留收口） | `kafkaconn/messages.go`（decompressPayload/readBounded/messageFromRecordWithSchema） |
| KAFKA-M1 | `consumePoolPut` 旧条目 `inUse` 时不替换不入池、返回 nil；调用方对 nil 保留真实 `closeClient`（临时 client 语义），与驱逐函数跳过 inUse 对齐 | `kafkaconn/consume_pool.go` + `messages.go` 调用点 |
| KAFKA-M2 | `LoadSettings` 补 `cursorTtlSecs`/`maxCursorSessions` 恢复分支（既有 >0 模式）；往返用例覆盖 | `mcp/settings.go` |
| KAFKA-M3 | `textMatcher.match` regex 分支先查 `patterns` 预编译缓存，miss 再编译（对齐同文件 `fieldValueMatches`） | `kafkaconn/messages.go` |
| KAFKA-M4 | `StartStream` 建 client 前预校验 decode/decompression，非法值返回业务错误，不再产生占名额僵尸会话 | `kafkaconn/stream.go` |
| KAFKA-L3 | SR REST 客户端按连接 TLS 配置复用 `buildTLSConfig` 构建 transport（CA/skip-verify/mTLS 生效于 SR 通道；纯 http 且无 TLS 项保持默认 transport，代理语义对齐 DefaultTransport） | `kafkaconn/schema.go` |
| KAFKA-L5 | skip-verify 必留痕：`Connect`（lifecycle 配置应用单次 emit 点）对 `tls_insecure_skip_verify=true` 发 `Result:"success"`（store 折算 ok）+ Detail 注明 TLS 验证已关闭；`audit.go` 注释修订为三值契约（success/blocked/error），删除 "warning 保留" 语义 | `kafkaconn/service.go`、`audit.go` |
| KAFKA-L4 | `DeleteGroup` 补 `confirmGroup` 同名门禁（`ensureGroupDeleteConfirm`，-32602 + blocked 审计，与 confirmTopic 同级）；前端 GroupsPanel 删除弹窗补输入组名确认；`groups.deleteConfirmLabel` 七语 key；协议文档 `PROTOCOL_KAFKA`/`IMPL_PLAN` 参数表同步 | `kafkaconn/types.go`、`policy.go`、`groups.go`；`frontend` GroupsPanel/api/i18n |

**测试**：新增 `TestDecompressBombGuard`（全解压分支超限）、`TestMessageTruncationAtLimit` 扩展（valueText 截断）、`TestConsumePoolPutSkipsInUseReplacement` + `TestConsumePoolPutConcurrentInUseGuard`（-race）、`TestTextMatcherRegexUsesPrecompiledCache`、`TestSettingsPersistRoundtrip` 扩展、`TestStartStreamValidationOffline` 扩展（无僵尸会话断言）、`TestSchemaRegistryClientTLSTransport`、`TestConnectAuditsInsecureSkipVerify`、`TestDeleteGroupGateMatrix` 扩展（confirmGroup 矩阵）；GroupsPanel spec 补输入确认门禁/参数上送断言。

**验证终值**：`go build/vet/gofmt` 0 告警、`go test ./...` 全绿（kafkaconn/mcp/lifecycle/store；池并发用例 `-race -count=2` 通过）；前端 `vue-tsc` 通过、vitest 346/346（含 i18n 七语对齐 spec）；smoke `total=18 PASS=13 FAIL=0 SKIP=5`（SKIP 均环境性：Glue/PROTOBUF/OAUTH/SASL/TLS matrix）；smoke_mcp `19/19`（K18 消费池 churn 50 轮压 M1 路径）。S17 TLS matrix SKIP 的 TLS 生效性由 L3/L5 单测覆盖。
