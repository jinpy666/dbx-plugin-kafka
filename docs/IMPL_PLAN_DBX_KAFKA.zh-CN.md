# IMPL PLAN — DBX Kafka 插件（io.dbx.kafka）

> 状态：Phase 1（基础版）+ Phase 2（商用化）已实施，收口验证见 §11；
> Phase 3（对标 Confluent for IntelliJ）已立项登记，实施方案见 §12。
> 决策记录：本文件是 kafka 插件**唯一工作来源**。工作区 AGENTS.md 原有
> "聚焦三插件、不做任何新插件" 约束，经用户于 2026-09-05 明确指令新增
> kafka 插件而解除；根 README.md / AGENTS.md 随本次任务同步修订登记。
> 2026-09-05 目标升级：商用特性全量覆盖 + host 特有能力（取长补短）、
> 商用级鉴权/加密矩阵、ag-grid 表格过滤检索、docker 覆盖测试——即本文件
> §0.2 的 Phase 2 项全部落地，统一记入 §11。

## 0. 目标与非目标

### 0.1 目标

构建 DBX 插件形态的 Kafka 控制台：

- **Go sidecar**（stdio-jsonl，与 `ldap/` 同构），Kafka 客户端用
  `github.com/twmb/franz-go` + `pkg/kadm`（生态成熟，迁移成本最低）。
- **核心语义能力**：消费语义（5 种 offset 策略、per-partition 精确 seek、
  commit 与过滤互斥、续读游标）、Go 侧字段级过滤（三通道 + matchMode +
  JSON path + 数值比较）、base64/四种解压解码、流式消费会话（ring buffer、
  暂停/恢复、空闲回收）、导出 JSON/CSV、Confluent properties 导入（前端）、
  高危操作分级门禁。
- **修复已知实现缺陷**：二进制值保真（`string(record.Value)` 直转会损坏
  二进制消息；本插件 value 一律 `base64 保真 + text 预览` 双字段）；
  **客户端连接复用**（本插件按 connectionId
  缓存 client，指纹失效重建）。
- **参考 host 补齐**（host 的 Kafka 是 Java agent，消息浏览仅 peek ≤100 条、
  无实时消费/过滤/导出——插件正好补齐）：消费组 lag 快照（Option 语义区分
  "无数据/零 lag"）、topic 分区健康视图（leader/replicas/ISR/offline）、
  消费组 offset 重置（host 有 `mq_reset_consumer_group_offsets`，插件同样提供）。
- **不重复宿主**（M0 红线）：连接 profile 持久化、凭据 secret binding、
  SSH 隧道/代理传输层（sidecar 默认经 `runtime.host:port` 拨号；多 broker
  场景若 Host 提供 `runtime.proxy`，sidecar 只使用该结构化 SOCKS5 route，
  不读取或重建宿主的 tunnel profile）、read_only 治理与审计基线全部走宿主。

### 0.2 非目标

原 Phase 2 项（Schema Registry、Kerberos、ZooKeeper 发现）已于 2026-09-05
商用化轮次全部落地（见 §11）；原登记的 PROTOBUF 载荷编解码、
OAUTHBEARER/AWS MSK IAM 已于 2026-09-06 对标 Confluent for IntelliJ 后
立项进入 Phase 3 范围（见 §12）。当前剩余非目标：

- 通用 OIDC 的 OAUTHBEARER token 回调通道（sidecar 无回调面；MSK IAM 与
  静态 token 两类 token 源已覆盖主流场景，见 §12.2.3）。
- 幂等/事务生产参数、ACL 之外的 quota/reassignment/log dir（维持登记不动，
  本轮对标未提升其优先级）。
- 与宿主 MQ 控制台（topics/groups/ACL 治理面）的互通。
- CCloud/IDE 专属能力（CCloud OAuth、Scaffold 代码脚手架、Spring gutter
  建连、语言依赖检测、遥测上报）：理由存档 §12.6。

## 1. 三方功能对标（范围依据）

| 能力 | 早期参照实现 | host | 本插件 Phase 1 |
| --- | --- | --- | --- |
| 连接 profile 持久化 | sqlite 自管 | 宿主 ConnectionConfig | **宿主**（manifest connection-provider） |
| 凭据存储 | sqlite 明文 | 加密 secret | **宿主 secret binding** |
| SSH 隧道/代理 | 自建 dialer | 宿主传输层 | **宿主**（sidecar 经 runtime.host:port） |
| 认证 | PLAIN/SCRAM-256/512/Kerberos | PLAIN/SCRAM/Kerberos(Java) | PLAIN/SCRAM-256/SCRAM-512 + TLS；Kerberos Phase 2 |
| broker 列表/config | ✅ | ✅(describe_cluster) | ✅ brokers/list + brokers/config |
| topics 列表/创建/删除/扩分区/配置 | ✅（副本因子可配） | ✅（副本硬编码 1） | ✅（replicationFactor 可配） |
| 分区元数据与健康 | ✅ leader/ISR | ✅ partitionStats | ✅ + isHealthy(isr/replicas) |
| offset 查询 | earliest/latest/max-timestamp/按时间 | begin/end | earliest/latest/按时间戳 |
| 消息生产 | ✅ 批量≤1000/headers/压缩/指定分区 | ✅ 单条 | ✅ 全量实现 |
| 消息消费（一次性） | ✅ 5 策略+过滤+解码+导出 | peek ≤100 无过滤 | ✅ 全量实现 |
| 消息消费（流式） | ✅ ring buffer 会话 | ❌ | ✅（sidecar 事件通道） |
| 二进制保真 | ❌（string 直转） | base64 | ✅ base64+text 双字段 |
| 消费组 list/describe/lag | ✅ | ✅ 快照+Option 语义 | ✅ 两者合并 |
| 消费组删除/offset 重置 | 删除✅/重置❌ | 重置✅ | ✅ 都做 |
| ACL | ✅ 全枚举 | grant/revoke 简化 | ✅ 全量实现 |
| 导出 | JSON/CSV | ❌ | ✅ JSON/CSV |
| 只读/写门禁 | 审批弹窗（进程内） | read_only+生产确认 | **宿主 read_only + allowDelete 策略** |

## 2. 仓库结构

```
kafka/
├── manifest.json              # id=io.dbx.kafka，connection-provider database_type=kafka
├── dbx-plugin.toml            # language=go，binary=dbx-plugin-kafka，include=[assets,ui]
├── README.md
├── .gitignore                 # backend/bin、ui/、dist 等（照 ldap/.gitignore）
├── assets/plugin.svg          # 64×64 图标
├── docker-compose.kafka-test.yml   # apache/kafka KRaft 单机，dbx-kafka-test:9092
├── backend/
│   ├── go.mod / go.sum        # module io.dbx.kafka.plugin；franz-go+kadm+uuid；SDK replace 照 ldap
│   ├── main.go                # SDK server + 方法 switch（同 ldap/main.go 模式）
│   └── internal/
│       ├── kafkaconn/         # 领域包：types.go / service.go / client.go(拨号+SASL+TLS+缓存) /
│       │                      #   topics.go / groups.go / messages.go(produce+consume+filter) /
│       │                      #   stream.go(流式会话) / acls.go / policy.go / audit.go
│       │                      #   + 每文件 *_test.go + manifest_contract_test.go
│       ├── lifecycle/         # 照抄 ldap/internal/lifecycle（provider id/databaseType 改 kafka）
│       └── store/             # 照抄 ldap/internal/store（DefaultDirName="io.dbx.kafka"，audit.jsonl）
├── frontend/                  # Vue3+Vite+vitest，目录同 ldap/frontend
├── scripts/
│   ├── build.sh / test.sh / ui_test.mjs
│   ├── sidecar_client_jsonl.py / smoke_test.py / smoke_container.py
│   └── kafka-seed/            # 建测试 topic 脚本（无凭据）
└── docs/
    ├── IMPL_PLAN_DBX_KAFKA.zh-CN.md   # 本文件
    ├── PROTOCOL_KAFKA.zh-CN.md        # 协议文档（§5 展开；新方法必须同步此处）
    └── PROGRESS-B-KAFKA.zh-CN.md      # backend 路（frontend 路另有 P 文档）
```

## 3. Go 依赖

- `github.com/twmb/franz-go`（kgo）、`github.com/twmb/franz-go/pkg/kadm`
- `github.com/google/uuid`、SDK `github.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk`
  （replace 照 ldap 指向 `../../../dbx-plugin-host-worktree/plugins/sdk/go/dbx-plugin-sdk`）
- 压缩解压：franz-go 自带 gzip/snappy/lz4/zstd 依赖可直接复用
- 全部进 go.sum；`CGO_ENABLED=0`

## 4. manifest 贡献点

`connection-provider`（id `io.dbx.kafka.connection`，database_type `kafka`，
capabilities `["test","connect","disconnect"]`，workbench `io.dbx.kafka.workbench`）字段：

| key | label | type | binding | 说明 |
| --- | --- | --- | --- | --- |
| display_name | 连接名 | text | name | required，默认 "Kafka cluster" |
| bootstrap_servers | Bootstrap servers | textarea | config | required，`host:port` 逗号/换行分隔 |
| security_protocol | Security protocol | select(PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL) | config | 默认 PLAINTEXT |
| sasl_mechanism | SASL mechanism | select(PLAIN/SCRAM-SHA-256/SCRAM-SHA-512) | config | visible_when security_protocol 含 SASL |
| sasl_username / sasl_password | 用户名/密码 | text/password | config / secret | visible_when 含 SASL |
| tls_ca_cert | CA 证书 (PEM) | textarea | config | visible_when 含 SSL |
| tls_client_cert / tls_client_key | 客户端证书/私钥 | textarea/password | config / secret | visible_when 含 SSL |
| tls_insecure_skip_verify | 跳过 TLS 校验 | boolean | config | 默认 false，visible_when 含 SSL |
| client_id | Client ID | text | config | 可选 |
| read_only | 只读模式 | boolean | config | 默认 true |
| allow_delete | 允许删除类操作 | boolean | config | 默认 false；read_only 下强制无效 |

七语 localizations 全量（zh-CN/zh-TW/en/es/it/ja/pt-BR），含每个字段的
label/description。`engines: { dbx: ">=0.5.77", host_api: ">=1.0.0" }`，
permissions `["host.events","host.workbench"]`。

## 5. Sidecar 方法契约

公共约定：stdio-jsonl（SDK 帧）、方法 `<域>/<动作>`、字段 camelCase、
必填 `connectionId`、参数错 `-32602`、业务错 `-32000`、未注册 `-32601`。
响应统一 `{ ok: true, data }`；错误走 PluginError。**完整字段表见
`docs/PROTOCOL_KAFKA.zh-CN.md`（实现必须与其同步）**。

### 5.1 生命周期（SDK/ldap 同款）

- `connection/test` / `connection/connect` / `connection/disconnect`

### 5.2 领域方法

| 方法 | 请求要点 | 返回要点 |
| --- | --- | --- |
| `kafka/brokers/list` | — | `brokers[]{nodeId,host,port,rack}` |
| `kafka/brokers/config` | `brokerId` | `entries[]{name,value,source,sensitive,isDefault}` |
| `kafka/topics/list` | `includeInternal?` | `topics[]{name,topicId,isInternal,partitionCount,replicationFactor,error?}` |
| `kafka/topics/describe` | `topic` | `partitions[]{partition,leader,leaderEpoch,replicas[],isr[],offlineReplicas[],isHealthy}` |
| `kafka/topics/create` | `topics[]`, `partitions`, `replicationFactor`, `config?` | 每条 `results[]{topic,ok,error}` |
| `kafka/topics/delete` | `topics[]` | 同上（critical 门禁） |
| `kafka/topics/partitions/update` | `partitions`（map topic→新分区数，只增） | 同上 |
| `kafka/topics/config/get` | `topic` | `entries[]` 同 brokers/config |
| `kafka/topics/config/alter` | `topic`, `config{}`, `deleteKeys[]` | 同上 |
| `kafka/topics/offsets/list` | `topics[]`, `offsetTime?`(earliest/latest/RFC3339/unix ms) | `rows[]{topic,partition,offset,timestamp,leaderEpoch}` |
| `kafka/groups/list` | — | `groups[]{group,state,protocolType,coordinator}` |
| `kafka/groups/describe` | `group` | `members[]{memberId,instanceId,clientId,clientHost,assignments{topic:[]partition}}` |
| `kafka/groups/offsets/list` | `group`, `topics?`(空=committed 全量) | `rows[]{topic,partition,startOffset,endOffset,committedOffset,lag}` + `totalLag`（Option 语义：无 committed 数据→`hasCommitted:false`，与零 lag 区分） |
| `kafka/groups/delete` | `group`, `confirmGroup`（与组同名，-32602 门禁） | —（critical 门禁） |
| `kafka/groups/offsets/reset` | `group`, `topics[]`, `resetTo`(earliest/latest/timestamp/partitionOffset), `timestampMs?`, `partitionOffsets?` | `rows[]{topic,partition,ok,error}` |
| `kafka/acls/list` | `filter{}`（拒绝过宽） | `acls[]{resourceType,resourceName,patternType,principal,host,operation,permission}` |
| `kafka/acls/create` | `acl{}` | — |
| `kafka/acls/delete` | `filter{}` | `matched[]` |
| `kafka/messages/produce` | `topic`, `key?`, `value`, `headers?{}`, `partition?`, `count?`(≤1000), `compression?`(gzip/lz4/zstd/snappy) | `partition,offset,timestamp` |
| `kafka/messages/consume` | 见下 | `messages[]`,`scanned`,`matched`,`limited`,`hasMore`,`nextPartitionOffsets{}` |
| `kafka/messages/export` | 同 consume + `format`(json/csv), `limit`(≤10000) | `content`,`filename`,`contentType` |
| `kafka/stream/start` | 同 consume 参数 | `sessionId` |
| `kafka/stream/stop` | `sessionId`（或 `all:true`） | — |
| `kafka/stream/pause` / `kafka/stream/resume` | `sessionId` | `status` |
| `kafka/stream/status` | `sessionId` | `status{paused,totalScanned,totalMatched,bufferSize,partitionOffsets}` |
| `kafka/stream/messages` | `sessionId`, `offset`, `limit` | ring buffer 历史分页 |
| `kafka/presets/list|save|remove` | 消费/过滤预设 | 照 ldap/presets 形态（store 持久化） |
| `kafka/connections/statuses` | — | 照 ldap/connections/statuses 形态 |

### 5.3 consume 参数（一次性与流式共用 `ConsumeParams`）

`topic`、`groupId?`、`offsetStrategy`(latest/earliest/committed/timestamp/offset)、
`offsetTime?`（RFC3339 或 unix ms）、`partitions?[]`、`partitionOffsets?{partition:offset}`
（有 partitions 时禁 groupId；strategy=offset 时必填）、`limit`(默认100)、
`timeoutMs`(默认5000)、`maxScanRecords`(默认 max(1000, limit×10))、
`isolationLevel`(read_uncommitted/read_committed)、`commit`(true 时禁一切过滤且必须 groupId)、
过滤：`filter?`（全文=key+value+headers 拼接）、`keyFilter?`、`valueFilter?`、
`headerFilter?`、`matchMode`(contains/prefix/exact/regex)、
`fieldFilters?[]{source(value/key/header/topic/partition/offset/timestamp), path?, operator(contains/prefix/exact/regex/exists/not_exists/gt/gte/lt/lte), value, enabled}`,
`timestampFrom?/timestampTo?/offsetFrom?/offsetTo?`、
解码：`decode`(none/base64)、`decompression`(gzip/lz4/zstd/snappy)。

消息形状（二进制保真）：`{topic,partition,offset,timestamp,leaderEpoch?,key?/keyBase64?,valueText?,valueBase64?,headers{},committed?,decodeError?,truncated?}`
—— value 超过 8KB 只给 `valueBase64` + `truncated:true`？否：**valueText 恒为
UTF-8 安全预览（非法字节替换），valueBase64 恒完整**；消息体上限单条 512KB
（超出截断并标记）。

### 5.4 事件

| 事件 | 载荷 |
| --- | --- |
| `kafka/stream/messages` | `{sessionId, messages[], totalScanned, totalMatched, paused}`（200ms/批 50 节流） |
| `kafka/stream/error` | `{sessionId, error}` |
| `kafka/audit` | M0 审计记录（同 ldap/audit 形状） |

### 5.5 流式会话约束

ring buffer 10000 条；会话上限 20；空闲 30 分钟回收；`StopStream` 可取消；
fetch 错误指数退避 500ms→30s；**只读策略下禁止 commit**。

## 6. 安全策略（policy.go）

- `read_only=true`：produce/create/alter/reset/ACL 写一律 `-32000` 拒绝
  （错误码语义 `blocked`，与 ldap policy 一致）。
- `allow_delete=false`：topics/delete、groups/delete、acls/delete 额外拒绝；
  read_only 下 allow_delete 无效（两者与门）。
- topics/delete 要求请求携带 `confirmTopic`（与 topic 同名的确认字段，防误删）。
- 凭据（sasl_password、tls_client_key）不落日志、不进审计 result、不回显。
- 审计：全部写操作 + 拒绝事件 → store.AppendAudit + `kafka/audit` 事件。

## 7. 前端实施

目录同 ldap/frontend（App.vue 单页多面板 + components/ + lib/）：

- `lib/api.ts`：`callKafka<T>(method, params)` 注入 connectionId，导出 `kafkaApi`。
- `lib/kafkaModel.ts`（纯函数 + spec）：消息格式化/解码二次转换
  （Base64/GZip/Hex/JSON pretty/BitSet）、topic 业务排序（内部 topic 沉底）、
  lag 计算、CSV/JSON 导出序列化、Confluent properties 粘贴解析（填表单用）。
- `lib/i18n.ts` 七语 + `i18n.spec.ts` 七语完整性守卫（照 ldap）。
- `lib/appearance.ts`/`hostTheme.ts`：先照 ldap 抄，后续收敛 shared/frontend。
- 组件（Phase 1）：
  - `TopicTree.vue`（左栏：topic/分组/internal 标记 + 过滤框）
  - `MessagesPanel.vue`（一次性消费表单 + 消息表 + 详情抽屉 + 导出；
    消费表单覆盖 §5.3 全参数）
  - `StreamPanel.vue`（流式：start/stop/pause/resume + 实时表 + 自动滚动 +
    用户上滚暂停滚动 + droppedRows 提示，收 `kafka/stream/messages` 事件）
  - `ProducePanel.vue`（key/value/headers JSON 校验/partition/count/压缩）
  - `TopicsPanel.vue`（create/delete(confirmTopic)/扩分区/config 查看/编辑/offsets）
  - `GroupsPanel.vue`（组列表 + lag 徽章 + describe + offsets 表 + reset/delete）
  - `BrokersPanel.vue`（broker 列表 + config）
  - `AclsPanel.vue`（list/create/delete）
  - `ConnectionsPanel.vue` / `AuditFeedPanel.vue`（照 ldap 改）
- mock：`mockDbxHost.ts` 内存假桥必须镜像真实桥当前形状（binary 事件双形状，
  经 `shared/frontend/binaryEvent.ts` 归一化——本插件消息走 invoke 返回值，
  事件是 JSON 载荷，无二进制通道，但仍按规范引 shared）。
- 导出下载：Blob URL 兜底（宿主 1.0 无 save-file）。

## 8. 测试计划

- **单测**（纯解析，不连网）：client 构建/TLS+SASL 参数矩阵、consume 参数
  校验（commit×过滤互斥、partitions×groupId 互斥）、过滤引擎（matchMode、
  fieldFilters、JSON path、数值比较）、解码/解压、CSV/JSON 序列化、
  policy 门禁矩阵、manifest_contract_test（manifest 七语与方法表对齐）、
  前端 kafkaModel/i18n spec。
- **smoke**（`scripts/smoke_test.py`，sidecar_client_jsonl.py 直驱 sidecar）：
  S1 initialize+connection/test 无连接参数错误码正确；S2 假连接 connection/test
  返回业务错（非崩溃）；S3-S10 容器场景（KRaft）：connect → produce →
  consume(roundtrip) → topics/list 含种子 topic → groups/acls 容器不支持则
  SKIP → stream start/收事件/stop → export → policy read_only 拒绝写。
  SKIP 三层语义照 ldap：容器不可达整套 SKIP（`KAFKA_TEST_REQUIRE=1` 转
  FAIL）、sidecar 缺失 SKIP、未注册方法单场景 SKIP。
- **容器**：`docker-compose.kafka-test.yml`（apache/kafka KRaft 单机，
  container_name `dbx-kafka-test`，9092；无凭据字面量，探活发真实
  metadata 请求而非端口探测）；`smoke_container.py` 编排同 ldap 模式。
- **收口**：`scripts/test.sh` 全绿（SKIP 允许）才算完成定义达成。

## 9. 里程碑与并发分路

三路 agent 并行（契约以本文件为准，接口冻结）：

| 路 | 范围 | 交付 |
| --- | --- | --- |
| A backend | `kafka/backend/**` + `docs/PROGRESS-B-KAFKA.zh-CN.md` | go vet/test 过；方法全注册；单测齐 |
| B frontend | `kafka/frontend/**` + `docs/PROGRESS-P-KAFKA.zh-CN.md` | typecheck/test/build 过；七语齐 |
| C scaffold | manifest/dbx-plugin.toml/README/.gitignore/assets/scripts/docker-compose/docs(PROTOCOL) + 根 README/AGENTS 修订 | scripts 可跑；协议文档完整 |

收口（主线）：build.sh → test.sh → 修复 → 完成定义四件套核验。

## 10. 风险与备注

- franz-go 依赖需公网拉取进 go.sum；SDK replace 本地路径不影响打包
  （CLI 用 DBX_PLUGIN_SDK_ROOT+go.work 覆盖）。
- 宿主 bridge binary 事件契约与本插件无关（无二进制通道），但前端仍统一走
  `shared/frontend/binaryEvent.ts` 消费任何事件载荷字节（防未来演进）。
- Go 1.24 工具链与 CLI 捆绑 go.work 1.22 冲突：打包走原生 CLI 二进制
  （照 ldap/build.sh 的 PATH 兜底方案）。
- 大消息（512KB 上限）与流式背压：ring buffer 固定容量，前端 droppedRows
  计数提示，避免 OOM。
- SR/Glue/Kerberos/Connector 导入不阻塞 Phase 1 验收，
  全部登记 Phase 2。

## 11. Phase 2 商用化落地记录（2026-09-05）

三路并发（D=backend+manifest+PROTOCOL、E=frontend+ag-grid、主线收口联调），
契约冻结后并行，无目录交叉。

### 11.1 交付

- **后端 45 方法**（Phase 1 的 34 + schema 族 11）：Confluent 兼容 SR REST
  （wire format 编解码、LCS diff、兼容性 get/set/check、注册/删除、
  per-consume 元数据缓存）、Kerberos/GSSAPI（gokrb5 + franz-go sasl/kerberos，
  keytab 只收路径）、ZooKeeper 发现（go-zookeeper/zk，chroot）、produce/consume
  的 `valueBase64/keyBase64/schema{}` 挂载、offsets 五策略
  （earliest/latest/max-timestamp/log-start/时间戳）。
- **manifest 新字段 10 个**（connection_source/zk_servers/kerberos 五件套/
  sr_url/sr_username/sr_password，sr_password 走 secret binding），
  sasl_mechanism 增加 GSSAPI，七语逐字段全量。
- **前端**：引入 `ag-grid-community`（36.1.0，MIT；理由：商用检索要求列内
  过滤，用户点名 table 组件）——消息/组/offsets/ACL/topics 六表 ag-grid 化
  （排序+列过滤+分页持久化+窄容器降级，`DbxAgGrid.vue` 统一封装对齐 DBX
  主题令牌）；新增 `SchemasPanel`（版本 diff/兼容性/注册/删除双确认）与
  `MonitorPanel`（lag 采样+趋势 SVG+阈值告警+方案 presets）；Confluent
  properties 导入助手（密码掩码不落盘）；七语 440 键/语。
- **测试**：backend 单测 74→98，前端 38→49；docker 双容器覆盖
  （apache/kafka KRaft + Redpanda SR）smoke S1-S11。

### 11.2 联调修复（收口主线）

- **reset `partitionOffsets` 形状三方不一致**（Phase 1 遗留）：backend 嵌套
  `map<topic,map<partition,int>>`、前端/PROTOCOL 扁平 → 以嵌套为准，
  前端新增 `parseGroupOffsetTargetsText`（`topic:0=100` 显式语法，单 topic
  可裸写 `0=100`，歧义报错）、api.ts 类型、PROTOCOL §3.3、七语 hint 同步。
- **打包失败**：全局 npm CLI wrapper 注入的捆绑 go.work 锁 go 1.22 与模块
  1.24 冲突 → `test.sh` package 段照 `build.sh` 直调原生 CLI 二进制
  （`env -u DBX_PLUGIN_SDK_ROOT`）。
- 容器真跑暴露并已修（D 路，详 PROGRESS-B §6.3）：无 group 消费建 client
  失败、流式 poll 无限期阻塞导致事件不出、ACL 空 operation 误报、
  sidecar_client readline 无超时挂死、Redpanda DELETE version 单数字兼容。

### 11.3 验证证据

- `bash scripts/build.sh` → `dist/io.dbx.kafka-0.1.0-darwin-arm64.dbxp`。
- `bash scripts/test.sh`（含 package 步骤修复后复跑）→ 前端三件套 +
  go vet/test + package + smoke `total=11 PASS=10 FAIL=0 SKIP=1`
  （S3 合法 SKIP：新集群无消费组提交，`__consumer_offsets` 未建）。
- ui_test.mjs（Playwright+mock 桥）：ag-grid 过滤/分页持久化、Schemas diff、
  Monitor 采样/阈值/方案、`?ro=1` 只读禁用、暗色主题可读。

### 11.4 完成定义四件套核验

| 项 | 状态 |
| --- | --- |
| 单测（纯解析不连远端） | go 3 包 ok（98 例）+ vitest 6 文件 49 例 |
| smoke（未实现方法 SKIP 不 FAIL） | S1-S11，SKIP 语义三层齐 |
| 对标/任务清单更新 | 本文件 §11 + PROGRESS-B/-P 各 Phase 2 小节 |
| 七语文案 | manifest 逐字段 × 前端 440 键 × 7 语，spec 守卫绿 |

### 11.5 AWS Glue Schema Registry 落地（2026-09-05 第三轮，补齐字面功能缺口）

- **后端**：`internal/kafkaconn/glue.go`（aws-sdk-go-v2/service/glue，auth_mode
  default/static；API 集合：ListSchemas/ListSchemaVersions/
  GetSchemaVersion/RegisterSchemaVersion/CreateRegistry/UpdateSchema
  compatibility/CheckSchemaVersionValidity/DeleteSchema*），11 个 schema 方法
  改 backend 分发，请求 `registry?: "confluent"|"glue"`（缺省自动探测，歧义
  -32602）；compatibility 枚举含 Glue 8 档；**消息编解码挂载在 glue 下返回
  固定业务错**（语义约束：schema-aware 编解码仅 Confluent
  wire format，Glue 仅管理面）。
- **manifest**：glue_region/glue_registry_name/glue_auth_mode/glue_access_key_id/
  glue_secret_access_key(secret)/glue_session_token(secret)，七语全量。
- **前端**：SchemasPanel registry 徽章+双后端切换（枚举 7/8 档联动）、
  ConnectionsPanel Glue 摘要（secret 只显已配置态）、三面板 schema 挂载在
  glue 下禁用+提示、`?glue=1` mock 模式、i18n 454 键/语。
- **消息二次解码补齐**：fzstd/snappyjs/lz4js（MIT/ISC 纯 JS）落地，详情侧
  gzip/zstd/snappy/lz4 四解压全支持（Phase 2 降级
  标注撤销）。
- **验证**：backend 105 例（httptest 假 Glue JSON-RPC： subjects/versions/get/
  register/compat/delete/错误透传/探测歧义/枚举全表）；前端 52 例（zstd CLI
  固定向量 + snappy/lz4 roundtrip + 失败降级）；smoke S12（Glue 需
  GLUE_TEST_REGION/GLUE_TEST_REGISTRY，本地无 Glue 容器即 SKIP）；收口
  test.sh 全绿。
- 遗留：真实 AWS Glue 往返需凭据环境跑 S12；Glue DISABLED 与 Confluent NONE
  语义差异文档化；proto 载荷/幂等生产参数仍 Phase 3。

## 12. Phase 3 实施计划（2026-09-06 立项：对标 Confluent for IntelliJ）

> 对标对象：`github.com/confluentinc/intellij`（原 JetBrains Big Data Tools
> Kafka 客户端，2026 年更名 "Confluent"，Apache 2 开源）。依据：其仓库
> `resources/META-INF/plugin.xml` 动作/扩展点全表、`docs/architecture-overview.md`、
> 各包源码结构与官方文档页（未实机运行）。本节是 Phase 3 唯一工作来源；
> §12.2 为冻结契约，实施偏差回主线裁决，不得各路私改形状。

### 12.0 对标结论摘要

- **我方领先（保持不动）**：ACL 全 CRUD（对方无 ACL 面）、字段级过滤引擎
  （三通道 + matchMode + JSON path + 数值比较）、流式会话语义（ring
  buffer/暂停恢复/会话分页）、Lag 趋势监控告警（MonitorPanel）、宿主治理
  （read_only/审计/分级删除门禁）、broker config 视图、消息二进制保真。
- **差距（本轮范围 F1-F6）**：PROTOBUF 编解码、OAUTHBEARER/MSK IAM、
  Clear Topic 清空消息、Schema 随机测试数据生成（Flow）、Schema 克隆/模板/
  树视图、六项便利性 UI（§12.2.6）。
- **不跟进（§12.6 存档）**：CCloud OAuth/环境浏览/Scaffold 脚手架、
  Spring gutter 建连、语言依赖检测、遥测上报。

### 12.1 特性总表

| id | 特性 | 归属路 | 后端改动 | 前端改动 | smoke |
| --- | --- | --- | --- | --- | --- |
| F1 | PROTOBUF 载荷编解码（Confluent SR） | G | codec 增分支 + FDSet 解析 | Format 枚举/提示透传 | S14 真跑 |
| F2 | OAUTHBEARER（MSK IAM + 静态 token） | G | SASL 扩展 + manifest 6 字段 | 连接表单联动 | S15 env 门 |
| F3 | Clear Topic 清空消息 | G | 新方法 + critical 门禁 | TopicsPanel 双确认 | S13 真跑 |
| F4 | Flow 随机测试数据生成 | H | 无（复用 produce schema 挂载） | ProducePanel + 生成器 | 前端 spec |
| F5 | Schema 三件套（克隆/模板/树视图） | H | 无 | SchemasPanel | 前端 spec |
| F6 | UI 体验打磨族（6 项） | G+H | topics/list 增 isHealthy | §12.2.6 全件 | 前端 spec |

### 12.2 契约明细（冻结）

#### 12.2.1 F3 `kafka/topics/records/clear`

- 请求 `{connectionId, topic, confirmTopic}`；confirmTopic 必须与 topic 同名
  （复用 `ensureTopicDeleteConfirm` 单 topic 语义）。
- 门禁分级与 topics/delete 一致：read_only → -32000 blocked；
  allow_delete=false → -32000 blocked；confirmTopic 不匹配 → -32602。
- 实现：kadm `ListOffsets(WatermarkHigh)` 取各分区 hw →
  `DeleteRecords(offset=hw)`；旧 broker（<0.11）不支持时业务错透传，
  文档标注最低 broker 版本。
- 响应 `{rows:[{partition:int, deleted:long|null, lowWatermark:long|null,
  ok:bool, error?:string}]}`；deleted = 删除前 hw − 删除后 lowWatermark
  （任一段取不到置 null）。
- 审计：action `topics.records.clear`，detail `partitions=N`。
- PROTOCOL §3 topics 域新增行（G 路同步）。

#### 12.2.2 F1 PROTOBUF codec（schema.go）

- 新依赖 `google.golang.org/protobuf`（纯 Go，CGO=0 不受影响）。
- SR 元数据事实：PROTOBUF subject 的 schema 字段 =
  base64(FileDescriptorSet)，`SchemaMeta.SchemaType="PROTOBUF"`。
- `decodeSchemaPayload`/`encodeSchemaPayload` 增 protobuf 分支（删除现有
  "PROTOBUF 未实现" 显式报错分支）：
  - 解码：base64 → `descriptorpb.FileDescriptorSet` → `protodesc.NewFiles`
    → 动态消息 `proto.Unmarshal` → `protojson` 渲染 JSON；
  - 消息消歧（按序）：① FDSet 恰含 1 个 message → 用之；② subject 约定
    匹配（剥 `-key`/`-value` 后缀，PascalCase 匹配 message 全名尾段）；
    ③ 仍无法唯一 → 报错并列出候选全名（进 decodeError，不中断消费）；
  - 编码：`protojson.Unmarshal`（JSON → dynamicpb）→ `proto.Marshal` →
    `encodeWireFrame(meta.ID)`；
  - `SchemaRef.Format` 枚举 `avro|json` → **`avro|json|protobuf`**（可空 =
    按注册元数据；字段注释同步）。
- 契约不变：消息形状、schemaId/schemaSubject/schemaVersion 字段、
  per-consume 元数据缓存、Glue 下挂载仍业务错（编解码仅 Confluent wire
  format，§11.5 语义不变）。

#### 12.2.3 F2 OAUTHBEARER

- 新依赖 `github.com/aws/aws-msk-iam-sasl-signer-go`（Apache-2，纯 Go；
  aws-sdk-go-v2 已在依赖树）；机制层用 franz-go `pkg/sasl/oauth`。
- types.go：`SASLMechanismOAUTHBEARER="OAUTHBEARER"` 入取值面 +
  NormalizeSASLMechanism；validateProfile 增约束：OAUTHBEARER ⇒
  security_protocol=SASL_SSL（否则 -32602）；token_source=msk_iam ⇒
  mskRegion 必填；static_token ⇒ oauthStaticToken 必填。
- TokenProvider 两实现（client.go SASL 构建矩阵扩展 + 单测）：
  - `msk_iam`：`signer.GenerateAuthToken(ctx, region, creds, "kafka")`；
    凭据走 default 链（照 §11.5 Glue auth_mode=default 范式），
    `mskAccessKeyID`/`mskSecretAccessKey`/`mskSessionToken`（均 secret）
    可选显式覆盖；
  - `static_token`：`oauthStaticToken`（secret binding）直供，Expiration=0
    表示不过期。
- Profile 新字段（json camelCase）：`oauthTokenSource` / `mskRegion` /
  `mskAccessKeyID` / `mskSecretAccessKey` / `mskSessionToken` /
  `oauthStaticToken`。
- manifest 新增 6 字段 + sasl_mechanism 枚举 +1（七语逐字段
  label/description；visible_when 联动链：OAUTHBEARER →
  oauth_token_source → 各字段组）。
- 凭据红线照 §6：全部走 secret binding，不落日志/审计/回显。
- OIDC token endpoint 交换仍非目标（§0.2）。

#### 12.2.4 F6 后端半件：topics/list 增健康度

- `TopicInfo` 增 `isHealthy bool` + `unhealthyPartitions int`（additive
  无开关；分区元数据已在 ListTopics 加载，零额外请求）。
- 判定复用 `partitionInfos` 同款（leader 有效 + ISR=replicas + 无
  offline）；topic 级 = 全分区健康；unhealthyPartitions = 不健康分区数
  （树徽标 title 用）。
- PROTOCOL topics/list 响应行同步两字段。

#### 12.2.5 F4 Flow 随机测试数据生成（前端）

- 契约：零后端改动——复用 `kafka/messages/produce` + `schema` 挂载
  （SchemaRef.Version 缺省 = latest，后端已有语义）。
- ProducePanel 新「测试数据生成」组：
  - mode：manual（现状）| flow；flow 参数：来源 `schema_random` | `template`、
    countPerSend 1..100（默认 1）、intervalMs 250..10000（默认 1000）、
    启动/停止按钮 + 运行徽标 + 累计发送计数与最近 1 条回显；
  - 自动停止条件：read_only、校验失败、发送连续失败 ≥3；
  - schema_random：subject 发现按 `kafka/schema/subjects/list` 行前缀匹配
    `<topic>-key` / `<topic>-value`（比裸猜名稳），取该 subject 的 latest
    schema 生成 JSON；命中 PROTOBUF/JSON Schema subject → 行内提示改用
    template（PROTOBUF 前端生成不做，§12.6）；
  - template：JSON 模板 + 占位符 `{uuid}` `{now}` `{int:min,max}`
    `{float:min,max}` `{pick:a|b|c}`。
- 生成器落 `lib/kafkaModel.ts`：`generateAvroRandom(schema, rng)` 递归覆盖
  record/array/map/union（非 null 首支）/enum/fixed + 逻辑类型
  date/timestamp-millis/uuid/decimal；RNG 用 mulberry32 固定种子，spec
  固定向量断言（照 §11.5 zstd 固定向量范式）。

#### 12.2.6 F6 UI 体验打磨族（前端）

| # | 项 | 落点 | 契约/要点 |
| --- | --- | --- | --- |
| 1 | 即时搜索 | Messages/Stream 表 | ag-grid quickFilterText 输入框（防抖 150ms，只过滤已加载行），七语 placeholder |
| 2 | 复制族 | 详情抽屉 + 消息表 | 抽屉：复制 key/value/headers/整条 JSON 四按钮；表：行操作「复制 JSON」；navigator.clipboard + execCommand 兜底 |
| 3 | 时间戳时区切换 | MessagesPanel 工具栏 | 本地/UTC toggle（localStorage `kafka.ts.tz`），formatTimestamp 带 tz 参数，单元格 title 显完整 ISO |
| 4 | 生产面板分区数 | ProducePanel | App 把选中 topic 的 partitionCount（topics/list 已有）传入；头部显示「分区数 N」，partition 超界行内校验（第 4 轮扫描观察项收口） |
| 5 | 树健康徽标 | TopicTree | isHealthy===false 红点 + title「N 个分区不健康」；无额外请求（消费 12.2.4） |
| 6 | 列头筛选增强 | DbxAgGrid/kafkaColumns | timestamp 列 date filter、offset/lag/endOffset 列 number filter（ag-grid community 自带）；`AG_GRID_LOCALE_KEYS` 补 date/number filter 键（七语；照 UI_SCAN P2-19 范式从包内核对实际消费键名） |

#### 12.2.7 F5 Schema 三件套（前端）

- 克隆：版本表行操作「克隆」→ 注册弹窗预填 schema 文本（GetSchema 已有
  数据），subject 默认原值可改。
- 模板：注册弹窗 format 选定后「插入模板」——AVRO/JSON Schema/Protobuf
  三段静态模板（代码常量，非 i18n）。
- 树视图：详情区增 树/文本 toggle；AVRO/JSON 递归渲染（record/array/
  union/类型/默认值，可折叠）；PROTOBUF 保持文本 + 提示（后端 FDSet
  树化登记后续）。

### 12.3 并发分路（契约冻结后并行，无目录交叉）

| 路 | 范围 | 目录边界 | 交付 |
| --- | --- | --- | --- |
| G | F1+F2+F3+12.2.4；manifest/PROTOCOL 增量；protobuf seed fixture | `backend/**`、`manifest.json`、`docs/PROTOCOL`、`scripts/kafka-seed/protobuf/` | go vet/test 过；新方法注册；单测齐 |
| H | F4+F5+F6 前端全件 + 七语新键 | `frontend/**` | typecheck/test/build 过；spec 齐 |
| 主线 | 契约裁决、smoke S13-S15、对标清单/PROGRESS 回填、四件套核验 | `scripts/smoke*`、`docs/IMPL_PLAN`、`docs/PROGRESS-*` | test.sh 全绿 |

- manifest 归 G 独占；H 消费新字段只改 ConnectionsPanel 表单联动与 mock 桥
  （镜像真实桥新形状，工作区规则 7）。
- mock 夹具归 H：unhealthy topic 行（12.2.4）、PROTOBUF subject 样例
  （SchemasPanel 树视图提示路径）在 mockDbxHost.ts 落地；G 不碰 frontend。
- 收口合并顺序：G/H 各自全绿 → 主线联调（真跑 S13/S14）→ 四件套核验。

### 12.4 测试计划

- G 单测：protobuf codec 向量（roundtrip / 消歧三分支 / 坏 FDSet / 非 wire
  format 载荷）、clear 门禁矩阵（read_only × allow_delete × confirmTopic ×
  rows 形状）、oauth provider（static / msk_iam mock / SASL_SSL 约束 /
  normalize 扩展）、TopicInfo 健康聚合。
- H spec：生成器固定向量 + 占位符展开、Flow 启停与自动停止、SchemasPanel
  克隆预填/模板插入/树渲染、quick filter 防抖、复制兜底、tz 切换持久化、
  produce 分区校验、树徽标；七语完整性守卫自动覆盖新键。
- smoke：
  - S13 Clear Topic：produce 5 → clear → offsets/list latest==earliest →
    consume 0（容器真跑）；
  - S14 PROTOBUF roundtrip：seed 注册 PROTOBUF subject（fixture 见下）→
    produce(schema 挂载) → consume decode（容器真跑；若 Redpanda SR 不支持
    PROTOBUF 则降级登记 SKIP 并记 PROGRESS）；
  - S15 OAUTHBEARER：默认 SKIP（无 MSK 环境），env 门范式照 S12（Glue）。
- fixture：`scripts/kafka-seed/protobuf/orders.proto` + 离线 protoc 预生成
  的 `orders_fdset.b64`（提交入库；运行时不需要 protoc）。
- 收口：`scripts/test.sh` 全绿（SKIP 允许）。

### 12.5 完成定义四件套映射

| 项 | 本轮要求 |
| --- | --- |
| 单测 | G 新增 ≥15 例、H 新增 ≥15 例，全绿 |
| smoke | S1-S15（S13/S14 真跑、S15 SKIP 门），SKIP 语义照 §8 |
| 对标/清单 | §12 回填实施记录；PROGRESS-B/-P 各增 Phase 3 小节 |
| 七语 | manifest 6 字段 ×7 + 前端新键 ×7（spec 守卫绿） |

### 12.6 明确不跟进（存档）

- CCloud OAuth 登录、环境/资源浏览、Scaffold 代码脚手架（CCloud API 模板
  生成各语言工程）：Confluent Cloud 生态 + IDE 生成器形态，与 DBX GUI
  定位不符。
- Spring Boot gutter 一键建连、spark/flink/akka/.NET/python 依赖检测：IDE
  专属；等价能力 = 既有 Confluent properties 导入助手。
- PROTOBUF 前端 schema_random 生成与树视图渲染（12.2.5/12.2.7 注记）：本轮
  PROTOBUF 仅做后端编解码。
- 多标签并行消费（IDE 编辑器多 tab 天然能力）：以消费条件预设覆盖，观察
  后续轮次。
- 幂等/事务生产参数、quota/reassignment/log dir：维持 §0.2 登记不动。

### 12.7 风险与备注

- 两个新 Go 依赖需公网拉取进 go.sum（google.golang.org/protobuf、
  aws-msk-iam-sasl-signer-go），均纯 Go；打包照 §11.2 原生 CLI 二进制路径。
- Redpanda 内置 SR 的 PROTOBUF 支持度需 S14 真跑确认（降级路径见 12.4）。
- protojson 输出采用 proto 字段名；与 Confluent 序列化器的 JSON 渲染在
  int64/枚举表示上可能存在已知差异——登记为已知差异，不强行对齐。
- msk signer 在无 IMDS/无凭据环境的失败要快速收敛（config load 超时收紧），
  错误串入 friendlyKafkaError 映射（H 路同步）。
- clear 依赖 AdminClient DeleteRecords（KIP-107），文档标注最低 broker
  版本；不支持时业务错透传。

### 12.8 实施记录与收口（2026-09-07，worktree 双路并发 + 主线收口）

**交付**：
- **G 路**（branch `phase3/kafka-backend`，commit `4449e36`）：F1 PROTOBUF
  codec（protobuf 分支替换"未实现"报错、消歧三分支、Format 枚举扩展）、
  F2 OAUTHBEARER（`oauth.go`：static_token/msk_iam 双 TokenProvider，
  franz-go `pkg/sasl/oauth` + aws-msk-iam-sasl-signer-go，单次取 token
  10s 超时）、F3 `kafka/topics/records/clear`（KIP-107 DeleteRecords +
  confirmTopic 门禁 + 审计）、12.2.4 topics/list `isHealthy`/
  `unhealthyPartitions`；manifest 6 字段七语 + PROTOCOL 同步 +
  `cmd/gen-protobuf-fixture`（无 protoc，descriptorpb 程序化生成 FDSet
  fixture）。
- **H 路**（branch `phase3/kafka-frontend`，commit `8062ab8`+`8d79bca`）：
  F4 Flow（mulberry32 固定向量、generateAvroRandom、expandTemplate、
  subjects/list 前缀发现、启停/自动停止）、F5 Schema 三件套（克隆预填、
  三格式模板、SchemaTree.vue 树视图）、F6 六项（quickFilter 防抖、复制族
  + execCommand 兜底、tz 切换持久化、生产分区数徽标、树健康徽标、
  date/number 列头筛选 + AG_GRID_LOCALE_KEYS 13 键从包内核对）、
  OAUTH/MSK 表单联动 + `?msk=1` 夹具、friendlyKafkaError msk 映射、
  50 键 ×7 语。
- **主线**：smoke S13/S14/S15（smoke_test.py +309 行）；合并两分支
  （`6b73c3b`/`a4b4fab`）。

**验证证据**：
- backend：go vet 干净；kafkaconn/lifecycle/store 3 包 ok（基线 109 例 →
  128 顶层用例 + 19 子用例，新增 33 例）。
- frontend：typecheck 0 错；vitest 17 文件 180 用例全绿（基线 132，新增
  48 例）；build ✓。
- smoke：`total=15 PASS=11 FAIL=0 SKIP=4`（S3 `__consumer_offsets` 既有
  条件 SKIP、S12 Glue env 门、S14 降级登记见下、S15 OAUTH env 门）；
  S1-S12 无回归。S15 开门验证：static_token+SASL_SSL+OAUTHBEARER 对本地
  PLAINTEXT broker 得 `-32000` TLS 业务错（非 -32602），字段链/secret
  binding/SASL 矩阵贯通。
- `scripts/test.sh` 全绿：前端三件套 + go vet/test + package
  `io.dbx.kafka-0.1.13-darwin-arm64.dbxp` + smoke。

**主线裁决记录**：
1. F2 的 3 个 secret 字段（msk_secret_access_key/msk_session_token/
   oauth_static_token）入 connSecrets 而非 Profile 结构体——按 §6 凭据
   红线与 §11.5 Glue 范式，采纳 G 路提案。
2. PROTOBUF 消歧 PascalCase 为严格相等（`order-value`↔`Order` 命中，
   复数不命中）——已入 PROTOCOL §3.8。
3. protojson int64 输出字符串——§12.7 既定已知差异，测试锁定形状。
4. G 路观察项：既有 topics/delete confirmTopic 不匹配实际 -32000 与
   PROTOCOL §3.2 的 -32602 存在偏差（Phase 1 遗留，契约外）——维持现状
   登记遗留；clear 按冻结契约 -32602。

**S14 降级登记（§12.4 预判路径）**：Redpanda 内置 SR 对 PROTOBUF 仅接受
.proto 源文本形态；对契约冻结的 base64(FileDescriptorSet) 形态会当作
文本解析存储（证据：POST 注册 200 但 GET 回读为 `syntax = "proto2";`
非提交内容）。本地容器无法真跑 PROTOBUF roundtrip，S14 有据 SKIP；F1
编解码按 Confluent SR 正确形态实现（单测覆盖 roundtrip/消歧三分支/坏
FDSet/非 wire format 载荷）。

**完成定义四件套**：单测（backend +33 / frontend +48）✓；smoke（15 场景，
SKIP 语义三层）✓；对标/清单（本节 + PROGRESS-B §9 + PROGRESS-P §9）✓；
七语（manifest 6 字段 ×7 + 前端 50 键 ×7，spec 守卫绿）✓。

**遗留与后续**：
- F1：sidecar 兼容 SR .proto 文本形态（或 `GET /schemas/ids/{id}/schema?format=serialized`）
  以复跑 S14；Confluent references 多文件 schema 的 FDSet 依赖解析。
- F2：真实 MSK 往返（S15 env 门待真环境）；msk signer 无 IMDS 环境超时实测。
- H：StreamPanel 时区显示跟随缺省 local（未统一 tz toggle）；decimal
  逻辑类型 goavro 编码限制（已知）。
- topics/delete confirmTopic 错误码对齐（-32000 vs -32602，契约外遗留）。
  ✅ 已于 §12.9 收口。

### 12.9 测试覆盖完善轮（2026-09-07，双路并发 + 主线收口）

- **backend**：总覆盖率 55.1% → **73.0%**（kafkaconn 72.8% / lifecycle
  86.8% / store 76.1%）；新增 7 个测试文件 + 2 处既有扩展共 49 个测试
  函数，acls/groups/messages/stream/topics/client/service 的映射纯函数、
  枚举矩阵与离线校验分支全补（明细见 PROGRESS-B §10）。仍 <50% 者均为
  broker 依赖路径（admin 回调体 / PollRecords 主循环 / stream runLoop），
  后续可用内存 broker 或 withAdmin client 接口抽象覆盖（登记不实施）。
- **契约遗留收口**：topics/delete confirmTopic 不匹配从 -32000 对齐为
  **-32602**（`InvalidParamsError`，PROTOCOL §3.2 冻结语义，与 clear
  一致）；policy/topics Service 层测试锁定映射；smoke S10 断言同步并
  PASS——§12.8 遗留第 4 条关闭。
- **frontend**：接入 `@vitest/coverage-v8`（新增 `pnpm test:coverage`，
  既有 `test` script 不动）；statements 65.98% → **72.82%**（branches
  61.08% / funcs 63.31% / lines 75.39%）；补齐 6 个无 spec 面板
  （Monitor/Stream/Acls/Brokers/AuditFeed/DbxAgGrid），+39 用例 →
  23 文件 **219 用例**全绿；新增 `kafka/frontend/.gitignore` 忽略
  coverage/ 产物。下一轮低覆盖目标：hostTheme/TopicsPanel/SchemasPanel/
  App/TopicTree/MessagesPanel（清单见 PROGRESS-P §10）。
- **验证**：`scripts/test.sh` 全绿（前端三件套 + go vet/test 3 包 +
  package + smoke `total=15 PASS=11 FAIL=0 SKIP=4`，S10/S13 按 -32602
  断言 PASS）。


## 13. MCP 工具面（M3，2026-09-12 落地）

> 设计来源：`shared/IMPL_PLAN_PLUGIN_MCP.zh-CN.md`（v2）§1–§4/§6.3；
> 本节为设计文档 §6.3 对 kafka IMPL_PLAN 的补录（此前 kafka 是三插件中
> 唯一无 MCP 规划段落的）。实现与运行形态详见 `docs/MCP.zh-CN.md`，
> 协议形状见 `PROTOCOL_KAFKA.zh-CN.md` §6.4 / §3.10。

### 13.1 规划要点（设计 §2/§3/§4/§6.3 摘录）

- **读：UI 优先 + 强本地化**。MCP 不订阅 stream；`kafka_messages_digest`
  复用一次性 Consume 的 `maxScanRecords` 扫描语义与 filter 各通道，
  sidecar 本地聚合（per-partition 计数、key groupBy ≤20、时间直方图
  ≤12 桶、`fields` JSON-path 投影 distinct/topN ≤10），默认
  `format:"digest"`，`rows` clamp ≤20；`kafka_cursor_next` 会话翻页
  （TTL 10 分钟 / LRU ≤8 / 物化 ≤1 万行，定位字段
  topic-partition-offset 不截断）。
- **UI intent**：事件 `kafka/ui/intent`（search/focus/select）+
  方法 `kafka/ui/state/report`（intent 回报 + 快照型）；前端统一走
  `shared/frontend/useUiIntent("kafka")`（公共层单点），落表挂
  MessagesPanel consume 表单，锚点 = partition+offset；`kafka_ui_state`
  快照附带 stream 会话状态段。
- **写：两阶段确认**。`kafka_messages_produce` 单阶段直执行（MCP 载荷
  ≤64 KiB）；`kafka_topics_delete`、`kafka_groups_offsets_reset`、
  `kafka_topics_records_clear` 强制 preview + 一次性 confirmToken
  （60s TTL、参数 hash 绑定）。审计 `source:"mcp"`；连接只读（及
  allow_delete 对删除类）时写工具不进 `mcp/tools` 清单。
- **骨架**：`mcp/tools`、`mcp/call`、`mcp/settings/get|set`
  （8 字段，`digestScanLimit` 为 kafka 域内扩展）；单响应 16 KiB 截断。

### 13.2 工具表（11 个）

UI 驱动 4（`kafka_ui_focus/search/select/state`）+ 元发现 1
（`kafka_ui_topics`，硬上限 50）+ 本地读 2（`kafka_messages_digest` /
`kafka_cursor_next`）+ 写 4（`kafka_messages_produce` /
`kafka_topics_delete` / `kafka_groups_offsets_reset` /
`kafka_topics_records_clear`）。参数/语义全表见 `docs/MCP.zh-CN.md`。

### 13.3 验收（完成定义四件套）

- 单测：`backend/internal/mcp/*_test.go`（对照 shared/frontend/README
  「MCP 验收用例清单」S-SET/S-INT/S-CUR/S-CONF/S-DIG(kafka 变体)/S-SRV）。
- smoke：`scripts/smoke_mcp.py` K1–K11（未注册 SKIP；容器场景无集群
  SKIP；dev 集群在跑时全 PASS，含两阶段写与 audit source=mcp 断言）。
- 前端：`useUiIntent` 接线用例 + mock 契约镜像 + 七语 `intent.*` 文案。
- 文档：本节 + `docs/MCP.zh-CN.md`（新增）+ PROTOCOL §6.4/§3.10 +
  PROGRESS-P M3 记录。
