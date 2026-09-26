# MCP 集成（M3）

kafka 插件的 MCP 工具面（设计来源 `shared/IMPL_PLAN_PLUGIN_MCP.zh-CN.md`
v2，ssh `mcp.rs` 骨架 → ldap Go 版 → kafka 同构移植）。实现在 sidecar
`internal/mcp/`（Go），有**两种被 AI 客户端调用的方式**：

- **方式一（推荐）：DBX MCP 桥**——经 `dbx_list_plugin_tools` /
  `dbx_call_plugin_tool` 调用插件协议方法 `mcp/tools`、`mcp/call`、
  `mcp/settings/get|set`。桥按 `connectionId` 转发标准 lifecycle payload
  （`mcp/call` 的 `lifecycle` 字段），凭据由宿主解析，**工具参数里不出现
  任何密码**。
- **方式二：独立 stdio 模式（`--mcp`）**——sidecar 二进制自起 MCP 服务器，
  无需 DBX 在场，凭据随调用内联传入（见下文专节）。

核心设计（详见设计文档）：

1. **读：UI 优先 + 强本地化**——千万级消息量不进 MCP；AI 用 UI intent 把
   consume 条件填进消息面板（用户可视可继续操作），或用 digest 在 sidecar
   本地聚合 + cursor 翻页。**MCP 不订阅 stream**（stream 由用户在 UI 操作）；
   digest 复用 consume 的 `maxScanRecords` 扫描语义与 filter 各通道，
   扫描过程数据一条不出 sidecar。
2. **写：入 MCP 面 + 两阶段确认**——`kafka_messages_produce`（单条小消息）
   单阶段直执行；`kafka_topics_delete`、`kafka_groups_offsets_reset`、
   `kafka_topics_records_clear` 强制 preview → confirmToken。

   > **安全边界须知（评审 2026-09-26）**：两阶段确认（preview → confirmToken）
   > 与只读门是**防误操作的机制**，不是授权边界。stdio 独立模式下 preview、
   > token、confirm 在同一个 agent 对话流内完成——签发者与消费者是同一个
   > LLM，`readOnly`/`allowDelete` 也只是连接参数本身；真正的授权边界是
   > **用户选择把哪些凭据/连接交给这个 agent**（只读连接 + `allow_delete=false`
   > 是推荐的默认姿态）。桥模式（工作台）则由应用侧持有凭据与策略，宿主可
   > 在工作台加带外审批（appbridge 的 bridgeReadMargin 为此设计）。把生产
   > 集群的可写凭据配进完全自主的 agent 循环，任何插件侧机制都拦不住。
3. **token 经济**——默认 `format:"digest"`（计数 + 分布 + 样本），显式要
   `rows` 才出行且 clamp ≤20 行；单响应上限 16 KiB；大 value 原文不出
   MCP（占位符 + partition/offset 定位指引）。

## 方式二：独立 stdio 模式（`--mcp`）

sidecar 二进制直接作为 MCP 服务器运行（MCP `2024-11-05`，换行分隔
JSON-RPC 2.0 over stdio；ssh `run_mcp_stdio` 的 Go 移植，ldap Go 版同构）：

```bash
dbx-plugin-kafka --mcp
```

- **入口互斥**：`--mcp` 与 DBX 插件协议模式互斥，同一进程只跑其中一种；
  stdio 模式不启动 Emitter/插件协议循环/流式会话（MCP 仍不订阅 stream）。
- **协议方法**：`initialize`（serverInfo.name=`io.dbx.kafka`，version 取
  构建版本）、`notifications/initialized`（通知不回包）、`tools/list`、
  `tools/call`、`ping`；未知方法返回 JSON-RPC 错误（-32601）不崩。请求逐
  个 goroutine 处理，慢 digest 不阻塞 `ping`/`tools/list`（响应可能乱序，
  以 id 关联）；stdin EOF 后 drain 在途请求至多 300s 再退出。
- **请求形状分档（2026-09-13 可靠性纵深轮，ldap 同构）**：解析失败
  -32700（id null）；非法请求 -32600——缺 id（非通知）、`id` 为
  object/array/布尔、`method` 缺失/空/非字符串、`jsonrpc` 存在且非
  `"2.0"`（字段缺失容忍，照 ssh 基线）；`tools/call` params 非对象
  -32602。全部结构化报错，进程不崩、后续合法请求照常服务；非法 UTF-8
  字节流按顶层结构损坏归 -32700。
- **单行上限 16 MiB**：超限行直接 -32700 拒绝该行并继续服务（防单行无界
  占内存；8 MiB 级 arguments 走正常分派、工具层优雅报错）。smoke K17 钉住
  全矩阵（非法 JSON/UTF-8、通知静默、形状分档、8 MiB 行、pipelining 3+、
  空行/CRLF）。
- **tools/list**：复用 `mcp/tools` 既有注册表（工具名/描述/语义不变），
  并为连接类工具（digest/produce/topics_delete/groups_offsets_reset/
  topics_records_clear）在 inputSchema 里**显式声明内联连接参数**（严格
  校验的 MCP 宿主会丢弃未声明参数，ssh 同因声明），`required` 的
  `connectionId` 放宽为 anyOf 二选一（`connectionId` 或内联 `brokers`）。
  UI 类工具 schema 保持原样。

### 内联凭据连接（与 kafka 连接表单字段对齐，camelCase）

连接参数随每次工具调用内联传入（**不落盘、不持久化**；凭据只进进程内存
连接表与参数 hash 输入），按参数 hash 池化为进程内 connectionId
（`mcp-<hash 前 32 hex>`）：同参数后续调用自动复用（底层 admin client 与
工作台同一套 service，指纹失效重建语义保持）；池上限 8，满后淘汰最旧并
断开。也可以传先前调用用过的池化 `connectionId` 复用；传 DBX 保存连接的
id 则走**桥接兜底**（见下节）。

| 参数 | 表单字段 | 说明 |
| --- | --- | --- |
| `brokers` 必填 | `bootstrap_servers` | 数组或单个逗号/换行分隔字符串 |
| `securityProtocol` | `security_protocol` | 缺省 `PLAINTEXT` |
| `saslMechanism` | `sasl_mechanism` | SASL_* 协议必填（PLAIN/SCRAM-SHA-256/SCRAM-SHA-512） |
| `saslUsername` / `saslPassword` | `sasl_username` / `sasl_password`（secret） | SASL 凭据 |
| `tlsCaCert` / `tlsClientCert` / `tlsClientKey` | `tls_ca_cert` / `tls_client_cert` / `tls_client_key`（key 为 secret） | SSL 协议 TLS 材料（PEM 原文） |
| `tlsInsecureSkipVerify` | `tls_insecure_skip_verify` | 缺省 false |
| `schemaRegistry` | `schema_registry` | `none`（缺省）\| `confluent` |
| `schemaRegistryUrl` | `sr_url` | confluent 必填 |
| `schemaRegistryUsername` / `schemaRegistryPassword` | `sr_username` / `sr_password`（secret） | SR basic auth |
| `clientId` | `client_id` | 缺省 `dbx-kafka-plugin` |
| `readOnly` | `read_only` | **缺省 true（表单默认）**，写工具拒绝；显式 `false` 才允许写 |
| `allowDelete` | `allow_delete` | **缺省 false**，删除类两工具（topics/delete、topics/records/clear）拒绝；缺省值与显式同值池键一致 |

**stdio 未覆盖的表单字段**（本轮内联不支持，需工作台保存连接）：
ZooKeeper 源（`connection_source=zk` + `zk_servers`）、Kerberos/GSSAPI
（`kerberos_*`）、OAUTHBEARER/MSK IAM（`oauth_*`/`msk_*`）、AWS Glue SR
（`glue_*`）。

### stdio 语义差异

- **UI 类工具（`kafka_ui_*` 全部 5 个，含元发现 `kafka_ui_topics`）照常
  列出，但 `tools/call` 返回明确 `UNAVAILABLE`**：`isError:true` + 内容
  「UNAVAILABLE: 此工具需要 DBX 工作台（工作台模式可用，请经 DBX MCP 桥
  调用）……」，不假死、不长时间挂起（设计 §5 stdio 行）。
- digest/cursor/写族语义与方式一完全同构（同一 `mcp/call` 分派路径，只是
  新入口）：`maxScanRecords` 扫描语义 + 本地聚合、cursor 翻页、produce
  单阶段、三写两阶段 confirmToken（一次性/60s/hash 绑定）、写审计
  `source:"mcp"` 照常落 audit.jsonl。同上节：stdio 的两阶段确认是思考
  停顿而非授权边界——凭据经内联参数进入本进程，客户端侧的会话日志/追踪
  通常会记录完整 arguments（含 saslPassword 等），共享凭据前先评估这条
  泄漏面。
- 工具执行错误按 MCP 规约以 `isError:true` content 返回（非协议级错误）。
- 数据目录沿用 `DBX_PLUGIN_DATA_DIR`，`mcp-settings.json` 与 `audit.jsonl`
  都在其中生效。

### 桥接兜底（同 ssh/ldap 模式，2026-09-13 第三轮落地）

stdio 模式带「未注册 `connectionId` 自动转发运行中 DBX 应用本地 TCP 桥」
的兜底（ssh `app_bridge.rs` 的 Go 移植，ldap `appbridge.go` 的 kafka
同构移植，`internal/mcp/appbridge.go`）：

- **转发决策**（优先级与 ldap 同构）：连接类工具（digest / produce /
  topics_delete / groups_offsets_reset / topics_records_clear）的连接解析门
  为——内联凭据在场（`brokers` 非空）优先，按 hash 池化本地连接（不触桥）；
  其次本会话已池化的 `connectionId` 走本地路径；只有**未池化**的
  `connectionId`（典型是 DBX 保存连接的 id）才经桥转发给应用自己的 sidecar
  （保存连接的凭据/只读标志/策略由应用侧持有，工具参数里无凭据）。
  `kafka_cursor_next` 等会话类工具不触桥（unknown cursorId 是本地错误）。
- **发现与契约**：应用把端口写在 `<app_data_dir>/mcp-bridge-port`
  （`DBX_APP_DATA_DIR` 覆盖，缺省 macOS `~/Library/Application Support/
  com.dbx.app`），`POST /call-plugin-tool` body 为 snake_case 五字段
  `{plugin_id:"io.dbx.kafka", connection_id, tool, arguments, timeout_ms}`；
  200 body（应用侧 MCP content envelope）逐字透传。转发超时跟随
  `timeoutSecs`（clamp 5–300，缺省 300，整数字符串宽容折算）+ 150s 审批
  读余量。
- **fail-closed**：端口文件缺失/损坏/TCP 探测失败/HTTP 非 200 都立即返回
  可行动错误（带 `DBX app bridge` 失败原因 + 内联凭据出路：改传
  `brokers`/`securityProtocol`/`sasl*`/`schemaRegistry*` 或启动 DBX 应用），
  不假死、不静默重拨。应用未起时先尽力拉起（缺省 `open -a DBX.app`；
  `DBX_APP_LAUNCH_CMD` 仅支持 `:` 哨兵跳过拉起，smoke/CI 用）并每 500ms
  轮询端口至多 30s（`ensure` 预算，TCP 探测防陈旧端口）。
- **两阶段写透传**：preview/confirm 全部在应用侧 sidecar 完成一次性与
  hash 绑定语义（本地 stdio 会话不参与，token 不跨侧通用）。
- 测试：Go 单测 mock 桥（httptest，契约形状/envelope 透传/404 表面化/
  fail-closed/非 envelope 包装/会话类工具不触桥，9 用例）；smoke K16
  （空 app-data fail-closed + 本地 mock 桥转发契约 + 404 表面化）。

### 接入 ZCode（stdio 客户端）

```json
{
  "mcp": {
    "servers": {
      "dbx-kafka": {
        "type": "stdio",
        "command": "/绝对路径/backend/bin/dbx-plugin-kafka",
        "args": ["--mcp"]
      }
    }
  }
}
```

真机回环验证：`python3 scripts/smoke_mcp.py`（K12 离线 stdio / K13 容器
内联凭据全流程；dev 集群 `bash scripts/dev-cluster.sh up`）；手工冒烟
`echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}' | dbx-plugin-kafka --mcp`。

## 事件：`kafka/ui/intent`（sidecar → 前端）

sidecar 收到 UI 驱动类工具调用时：生成 `intentId` → intent 状态表登记
（进程内 map，TTL 60s，LRU 20 条，`pending`）→ 发本事件 → 等待前端
report（默认 5s，`mcp/settings/set` 的 `reportWaitMs` 可调）。

```json
{ "intentId": "i-1a2b3c4d", "action": "search",
  "params": { "topic": "orders", "offsetStrategy": "earliest", "limit": 100,
              "valueFilter": "paid", "matchMode": "contains",
              "connectionId": "…" } }
```

| action | params | 前端行为 |
| --- | --- | --- |
| `search` | `topic` 必填；`offsetStrategy?`、`limit?`、`filter?`、`keyFilter?`、`valueFilter?`、`headerFilter?`、`matchMode?`、`groupId?`、`partitions?[]`、`offsetTime?` | 消息面板 consume 表单填条件（App 先切选中 topic）→ 触发消费 → 结果进消息表 |
| `focus` | `panel`（messages \| topics \| groups \| schemas） | 切换/聚焦面板 |
| `select` | `partition`、`offset` 必填；`topic?` | 当前消息结果中按 partition+offset 定位命中行并打开详情抽屉 |

前端消费统一走 `shared/frontend/uiIntent.ts` 的 `useUiIntent("kafka",
handlers)`（公共层单点维护，插件不各抄一份）；落表/定位在
`MessagesPanel.vue` 的 `applyIntentConsume` / `applyIntentSelect`（经
defineExpose 由 App 装配）。

## 方法：`kafka/ui/state/report`（前端 → sidecar）

- **intent 回报**：`{intentId, status: "applied"|"rejected", summary,
  reason?}`。summary = `{count, truncated?, rows[≤5], anchor?, reason?}`，
  每 cell 截 120 字符（`cellWidth` 可调），**partition+offset 定位字段
  不截断**（anchor 形如 `orders-p0-o42`）。
- **快照型**：无 `intentId`、`status:"snapshot"`——前端在关键动作后
  （面板切换、topic 选中、消费完成）主动上报 `{panel, topic?, count?,
  anchor?}`，sidecar 缓存最新快照；`kafka_ui_state` 不带 intentId 时返回。
- 未知/已过期 intentId 回报报业务错误（-32000）。

## 工具一览（11 个）

`mcp/tools` 可带可选 `{connectionId}`：该连接配置为只读时全部 4 个写工具
**不进清单**；`allow_delete=false` 时仅剔除删除类两工具（topics/delete、
topics/records/clear），并附 `omittedWriteTools` 原因说明；未带
connectionId 时全量列出（调用时仍有策略门拒绝，纵深防御）。Scoped AI
会话由宿主禁 `dbx_call_plugin_tool`。

| 工具 | 参数（camelCase） | 语义 |
| --- | --- | --- |
| `kafka_ui_search` | `topic` 必填；`connectionId?`、`offsetStrategy?`（latest/earliest/committed/timestamp/offset）、`limit?`、`filter?`、`keyFilter?`、`valueFilter?`、`headerFilter?`、`matchMode?`、`groupId?`、`partitions?[]`、`offsetTime?` | 填 consume 条件并触发消费，结果留 UI；返回 `{intentId, state, summary}`。前端未响应 → `state:"pending"` + 引导走 digest |
| `kafka_ui_focus` | `panel` 必填（messages/topics/groups/schemas）；`connectionId?` | 聚焦面板；同 intent 回报语义 |
| `kafka_ui_select` | `partition`、`offset` 必填；`topic?`、`connectionId?` | 消息结果按 partition+offset 定位并打开详情；未命中 → `rejected` + reason |
| `kafka_ui_state` | `intentId?`、`connectionId?` | 带 intentId 读 intent 结果；不带读最新 UI 快照，且 `connectionId` 给出时附带该连接的 stream 会话状态段（`streams`，设计 §6.3） |
| `kafka_ui_topics` | `connectionId` 必填 | topic 名清单（硬上限 50，纯定位用；帮 AI 在 digest/ui_search 前选定真实 topic 名） |
| `kafka_messages_digest` | `connectionId`、`topic` 必填；`offsetStrategy?`（缺省 earliest）、`offsetTime?`、`partitions?[]`、`maxScanRecords?`（缺省 1000，上限 100000）、`filter?`/`keyFilter?`/`valueFilter?`/`headerFilter?`/`matchMode?`/`fieldFilters?`（consume 同名通道）、`decode?`、`decompression?`、`schema?`（SR 挂载，见下）、`fields?[]`（JSON path 投影，schema 解码后生效）、`format?`（digest 缺省 / rows） | **本地读核心**：一次性 Consume（maxScanRecords 语义）+ sidecar 本地聚合 `{matched, scanned, scanTruncated, stats:{perPartition、keys（含 nullKeyCount）、timeHistogram（≤12 桶）、fields[]（distinct/topN ≤10）}, sample[≤5], cursorId}`；`format:"rows"` 出 ≤20 行定位字段行。消息体不离开 sidecar；超 512KB 被截断的 value 在样本中以占位符 + 定位指引出现；扫描为 0 时自动补 topic 存在性校验（不存在的 topic 明确报错 + 引导 `kafka_ui_topics`，与空 topic 区分）。**schema 挂载**（MCP 第二轮）：`schema:{subject?, version?, format?, registry?}` 解码 Confluent wire format 后再做投影（subject 可缺省——按 wire schema id 查 SR；version 缺省=最新）；命中样本带 `schemaId`/`schemaSubject`/`schemaVersion`；逐条解码失败不中断消费，聚合层带 `decodeFailures` 计数 + `decodeNote` 指引（校验 subject/version），样本行带截断至 200 字符的 `decodeError`。**投影零命中提示**：请求了 `fields` 且全部字段 0 命中时带 `fieldsNote`（核对 JSON path 与载荷是否为 JSON——投影跳过非 JSON 载荷）。未配置 SR 的连接带 `schema` 由 kafkaconn 门禁显式报错（参数不静默丢弃） |
| `kafka_cursor_next` | `cursorId` 必填；`n?`（≤20，缺省 20）、`offset?`（缺省续读） | digest 会话翻页：只取 topic-partition-offset（+ 投影字段）行，条件不重发、远端不重扫。会话 TTL（`cursorTtlSecs` 可调，缺省 600s）、LRU（`maxCursorSessions` 可调，缺省 8）、物化上限 1 万行；过期报错带实际生效 TTL 并建议重发 digest |
| `kafka_messages_produce` | `connectionId`、`topic` 必填；`key?`/`keyBase64?`、`value?`/`valueBase64?`、`headers?`、`partition?`、`compression?`、`schema?` | 单条小消息**单阶段直执行**（value/valueBase64 合计 ≤64 KiB，超出提示走工作台）；审计 `source:"mcp"`。`headers` 必须是对象（header 名 → 标量值，标量折字符串）；数组/标量形状或嵌套值显式报错，不静默丢 header。`schema` 形状校验与 produce 同源（`schema.subject` 必填，`schema.version` 非法报错） |
| `kafka_topics_delete` | `connectionId`、`topics[]` 必填；`confirmToken?` | **强制两阶段**，见下节 |
| `kafka_groups_offsets_reset` | `connectionId`、`group`、`resetTo` 必填；`topics?[]`、`timestampMs?`、`partitionOffsets?`、`confirmToken?` | **强制两阶段**，见下节；预览前按模式预检（earliest/latest/timestamp 必须给 `topics`；timestamp 还须正的 `timestampMs`；partitionOffset 必须给 `partitionOffsets`），参数缺时直接报错、不签发令牌 |
| `kafka_topics_records_clear` | `connectionId`、`topic` 必填；`confirmToken?` | **强制两阶段**，见下节 |

### 容错语义（LLM 常见输入变体）

原则：**能无歧义折算的宽容接受；存在但非法的显式报错（带原值），绝不静默
改语义执行**。约定与实现单点在 `internal/mcp`（util.go 的 coerce* 与
server.go 各参数通道），由 Go 单测与 smoke K5/K7/K8/K10/K11/K12 覆盖：

- **整数字符串**：`partition`、`offset`、`n`、`maxScanRecords`、
  `timestampMs`、`partitions[]` 等整数字段接受 JSON number 或整数字符串
  （如 `"0"`、`"1700000000000"`）；`partitions` 还接受单个逗号/空白分隔
  字符串（`"0,2"`，UI 表单语义）。存在但非法（如 `"abc"`、负数）报错并列出
  原值——不会静默丢弃分区项、不会把字符串 `timestampMs` 折算为 0 执行。
- **字符串布尔**：stdio 内联 `readOnly` / `allowDelete` 接受布尔或字符串
  `"true"/"false"/"1"/"0"/"yes"/"no"/"on"/"off"`（大小写不敏感；变体面
  与 ssh `arg_bool` 同族一致）；其余值报错（静默回落表单默认会把写连接
  降级成只读或反之）。
- **枚举大小写归一**：`format`、`panel`、`offsetStrategy`、`matchMode`、
  `resetTo` 大小写不敏感（统一 lower 后传递/校验；在线钉桩 K19：
  `offsetStrategy:"EARLIEST"` 正常消费）；`ui_focus` 的 panel、
  `groups_offsets_reset` 的 resetTo+配套参数在 sidecar 侧提前校验，给出
  列出合法值的错误（不等前端 rejected / stdio pending / 二阶段执行才失败）。
- **缺参枚举（2026-09-14 第七轮，对齐 ssh §3.9）**：`required` 参数缺失
  （键不存在或显式 `null`）时**一次枚举全部缺口**，按 schema `required`
  声明顺序：`Missing required parameters: connectionId, topic`（digest/
  produce/topics_records_clear 双缺）、`…: partition, offset`（ui_select）、
  `…: connectionId, group, resetTo`（offsets_reset 三缺）——LLM 一轮补齐
  所有缺口而不是逐个 fail-fast 往返；present-but-类型错误（空串/空数组/
  非法值）不混入枚举，仍由逐参数校验精确点名（如 `topics: []` →
  `topics is required`、`partition:"abc"` → `partition must be a
  non-negative integer (got "abc")`）。resetTo 各模式的配套参数
  （topics/timestampMs/partitionOffsets）属于条件必填，保持按模式精确
  点名（K10/K15 不变）。
- **可行动错误**：连接未注册 → hint 检查 connectionId / stdio 改传内联
  参数；topic 不存在（digest 扫描为 0 时自动做存在性校验，produce 到
  不存在 topic 同样标注）→ hint 用 `kafka_ui_topics` 列出真实 topic 名，
  并注明 stdio 无 topic 列表能力（topic 名从用户/集群文档获取）；
  只读门 → 点名出路：stdio 内联连接默认 `readOnly:true`，写需
  `"readOnly": false` 重发（删除类叠加 `"allowDelete": true`；真机
  agent 实测最常见卡点，2026-09-14 补）；
  produce 载荷超 64 KiB → 报错带实际字节数与限额，提示走工作台；
  cursor 过期 → 报文携带实际生效 TTL 并 hint 重发 digest；未注册工具名
  → 分隔符/大小写变体给 `Did you mean` 建议（ssh 同构）并指向 `mcp/tools`。
- **schema 挂载参数**（produce/digest 同形 `{subject, version?, format?,
  registry?}`）：`version` 接受整数字符串、存在但非法/负数报错（静默折 0
  会把"版本打错"变成"取最新"）；produce 必须给 `subject`，digest 允许
  缺省（按 wire id 查 SR）。

## 写路径与两阶段确认（§4）

```
第一次  kafka_topics_delete {connectionId, topics:["orders"]}   （无 confirmToken）
  → {preview:{connectionId, topics, confirmTopic…},
     confirmToken:"c-…", expiresAt, note}                       （不执行任何写）
第二次  同参数 + confirmToken 且参数 hash 一致 → 执行 + 审计
```

- **单阶段直执行**：`kafka_messages_produce`（非破坏、可逆；照常过
  read_only 门）。`headers` 对象另有 MCP 头部预算（2026-09-13 可靠性纵深
  轮）：header 名非空、≤1024 字节、条数 ≤64，超限显式报错带实际上限/实测
  值，不静默截断或丢弃。
- **强制两阶段**：`kafka_topics_delete`、`kafka_groups_offsets_reset`、
  `kafka_topics_records_clear`。confirmToken 一次性、60s TTL（`confirmTtlSecs`
  可调 10–600，ldap/files 同名同范围；已签发令牌不追溯）、与请求参数
  hash 绑定——参数被改即作废（`arguments changed…`），过期（`expired`，
  报文携带实际生效 TTL）或复用（`unknown or already used`）都要求重开
  预览。reset 的模式/配套参数（含 `timestampMs > 0`，0 等价重置到纪元
  几乎必是参数缺失）在**签发令牌前预检**，不白烧一次性令牌。
- MCP 面没有 `confirmTopic` 参数（两阶段令牌即确认）；第二阶段执行时
  sidecar 内部仍按 topics 填充同名确认字段，kafkaconn 的防误删/防误清空
  门禁保持有效（纵深防御）。
- **只读/删除门**：只读连接上写工具不进 `mcp/tools` 清单，
  `allow_delete=false` 追加剔除删除类两工具；`mcp/call` 侧再拒绝一道
  （`PolicyOf` 同源）。
- **审计**：MCP 写路径审计记 `source:"mcp"`（`kafka/audit` 事件与
  audit.jsonl 同条携带；additive 字段，工作台路径不携带该字段，M0 审计
  事件形状不变）。
- **响应上限**：单工具响应 16 KiB（`mcp/settings/set` 的
  `responseLimitBytes`，1 KiB–1 MiB），超限按 sample→rows→stats 顺序丢弃
  并置 `truncated:true`，仍超限返回占位响应。

## mcp/settings（可调参数，`mcp-settings.json` 持久化）

| 字段 | 默认 | 范围 | 说明 |
| --- | --- | --- | --- |
| `reportWaitMs` | 5000 | 1–30000 | UI intent report 等待时长 |
| `cellWidth` | 120 | 1–2000 | 单元格截断宽度（partition/offset 定位字段不截断） |
| `digestGroupLimit` | 20 | 1–20 | per-partition / key groupBy 组数上限 |
| `digestTopN` | 10 | 1–10 | 字段投影 distinct/topN 取样上限 |
| `digestSampleRows` | 5 | 1–5 | digest 样本行数 |
| `digestRowLimit` | 20 | 1–20 | `format:"rows"` 行数 |
| `digestScanLimit` | 1000 | 1–100000 | digest 扫描上限（kafka 域内扩展，maxScanRecords 语义） |
| `cursorTtlSecs` | 600 | 10–3600 | digest 会话翻页游标 TTL（同族 files 同款语义；生效于新物化会话） |
| `maxCursorSessions` | 8 | 1–32 | cursor 会话 LRU 容量（set 后立即收敛） |
| `confirmTtlSecs` | 60 | 10–600 | confirmToken TTL（files/ldap 同名同范围；生效于下一次签发，已签发令牌不追溯——2026-09-13 补齐 ldap 同款键族） |
| `responseLimitBytes` | 16384 | 1024–1048576 | 单响应上限 |

cursor 淘汰为**真 LRU**（命中续读把会话顶到淘汰序队尾，持续翻页的活跃
会话不被纯插入序误逐；2026-09-13 可靠性纵深轮）；`kafka_cursor_next` 的
unknown cursorId 报文携带实际 TTL/容量与「重发 digest」指引（对齐 ldap
报文质量）；confirmToken 表在每次签发时顺手清理已过期未消费令牌（大量
「只要预览不确认」的调用不再无界撑表）。

## 降级矩阵（设计 §5）

| 场景 | `kafka_ui_*` | digest / cursor | 写工具 |
| --- | --- | --- | --- |
| 工作台打开、前端在线 | applied + summary | 可用 | 可用 |
| 前端在线但面板无该 action | rejected + reason | 可用 | 可用 |
| 工作台未打开 / 前端未响应 | pending → 超时 hint（引导走 digest） | 可用 | 可用 |
| 独立 stdio `--mcp` | 明确 `UNAVAILABLE`（"此工具需要 DBX 工作台"），不假死 | 可用（凭据内联/池化；未池化 `connectionId` 自动桥接兜底） | 可用（同左；表单默认门生效） |
| 连接只读 | 不受影响 | 可用 | 不注册进工具清单 |
| allow_delete=false | 不受影响 | 可用 | 仅删除类两工具不注册 |
| Scoped AI 会话 | 宿主禁 `dbx_call_plugin_tool` | 同左 | 同左 |

## 状态机验收用例

digest/cursor/confirmToken/intent 纯逻辑的验收用例清单（三插件同表，防
形状漂移）单点维护在 `shared/frontend/README.zh-CN.md`「MCP 两阶段/
digest/cursor 验收用例清单」，kafka 侧对应
`backend/internal/mcp/*_test.go`（S-SET/S-INT/S-CUR/S-CONF + S-DIG 的
kafka 变体：per-partition 计数、key groupBy、时间直方图、字段投影聚合；
S-SRV 编排）。

## smoke

```bash
DBX_PLUGIN_SIDECAR=/path/to/dbx-plugin-kafka python3 scripts/smoke_mcp.py
# 场景 K1–K19：离线 11 个（settings/tools/只读与 allow_delete 门/intent
# pending 与 applied/快照+streams/门禁/未知方法 + K12 stdio `--mcp`
# initialize/tools/UNAVAILABLE/连接门/字符串布尔宽变体与非法值门）+
# 容器场景（K11 digest 聚合 + cursor 翻页 + 字符串 partition/n 容错 +
# produce + 三个两阶段写全流程 + token 复用拒绝 + audit source=mcp；
# K13 stdio 内联凭据 produce/digest/cursor + 两阶段 delete + audit；
# K14 schema 挂载投影——raw 通道、wire format 解码 + JSON path
# distinct/topN、投影零命中 fieldsNote、坏版本 decodeFailures/decodeError、
# 未配置 SR 门禁，需 Schema Registry；K15 聚合 clamp——keys 45→20
# keysLimit/topN 45→10 truncated/直方图 ≤12 桶 + cursor 翻页到 done +
# reset timestampMs 真实落点核验 + produce 边界错误）+ K16 stdio 桥接兜底
# （空 app-data fail-closed + 本地 mock 桥转发契约 + 404 表面化；约 30s
# ensure 预算）。可靠性纵深轮（2026-09-13）新增：K17 stdio 健壮性（非法
# JSON/UTF-8、通知静默、请求形状分档 -32600、8 MiB 行、pipelining、CRLF，
# 全程进程存活，离线）+ K18 消费池 churn（earliest/latest 交替 50 轮
# matched 稳定——第四轮 P0 修复的回归面，需 dev 集群）。第七轮
# （2026-09-14）新增：K6/K8 缺参枚举断言（ui_select 双缺全点名、digest
# 单缺只其一、双缺按 schema 顺序）+ K19 enum 非法值在线钉桩（真实连接下
# offsetStrategy/resetTo 传 bogus，报错列出 schema enum 全部合法值；
# offsetStrategy="EARLIEST" 大小写归一正常消费）。
# 未注册方法 SKIP 不 FAIL（M0 §5.2）；dev 测试集群用
# `bash scripts/dev-cluster.sh up` 拉起。
```
