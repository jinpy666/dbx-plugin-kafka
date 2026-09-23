# PROGRESS — DBX Kafka 前端路（P）

> 状态：frontend 路交付（2026-09-05）。
> 唯一工作来源：`docs/IMPL_PLAN_DBX_KAFKA.zh-CN.md` §7（前端实施）+ §4/§5（契约）。
> 本路只写 `kafka/frontend/**` 与本文档，未触碰工作区其他文件。

## 1. 交付范围

蓝本照 `ldap/frontend`（Vue3 + Vite + vitest + vue-tsc，`packageManager:
pnpm@11.24.0`），依赖集与 ldap 完全一致（**零新 npm 依赖**）：vue、
@lucide/vue、@vitejs/plugin-vue、@vue/test-utils、happy-dom、typescript、
vite、vitest、vue-tsc。

### 1.1 脚手架（照抄 ldap，改名）

- `package.json`（name `@xynanan/dbx-kafka-ui`）、`vite.config.ts`、`build.mjs`
  （自包含单文件产物 → `../ui/index.html`，ui 目录规则不变）、`tsconfig.json`
  （types 保持 `["vite/client"]`，与 ldap 基线一致）、`index.html`、`mock.html`、
  `src/main.ts`、`src/env.d.ts`。

### 1.2 lib/

| 文件 | 内容 |
| --- | --- |
| `src/lib/api.ts` | `callKafka<T>(method, params)`（`invoke ?? request` + connectionId 注入）；`kafkaApi` 覆盖 IMPL_PLAN §5.2 全部方法：brokers/list+config、topics/list/describe/create/delete/partitions-update/config-get/config-alter/offsets-list、groups/list/describe/offsets-list/delete/offsets-reset、acls/list/create/delete、messages/produce/consume/export、stream/start/stop/pause/resume/status/messages、presets/list/save/remove、connections/statuses；事件类型 `kafka/stream/messages`（含 bufferSize）、`kafka/stream/error`、`kafka/audit` |
| `src/lib/kafkaModel.ts` | 纯函数：消息二次解码/格式化管线（valueBase64 → 内层 base64 → GZip inflate（浏览器 DecompressionStream；lz4/zstd/snappy 标注降级）→ raw/JSON pretty/hex/BitSet）、`appendStreamRows` 环形上限裁剪、topic 业务评分排序（internal 沉底）、lag 聚合、CSV（RFC 4180）/JSON 导出序列化、Confluent properties 解析（注释/续行/转义）→ 连接表单字段映射、`validateConsumeForm`（§5.3 互斥规则）、partition/partitionOffsets 解析、offsetTime 解析（unix ms/datetime-local/RFC3339） |
| `src/lib/i18n.ts` | 七语（zh-CN/zh-TW/en/es/it/ja/pt-BR），**每语 325 个 key，集合经 spec 断言完全一致** |
| `src/lib/i18n.spec.ts` | 七语完整性守卫（7 locale 集合相等 + 无空值 + locale 家族回退 + 模板替换） |
| `src/lib/kafkaErrors.ts` | `friendlyKafkaError`：门禁类（read-only/allow-delete/confirmTopic）优先，其次 SASL/TLS/网络/超时，未知透传 |
| `src/lib/auditFeed.ts` | 照 ldap 改（默认 action `kafka`） |
| `src/lib/appearance.ts` / `hostTheme.ts` | 照 ldap 抄（宿主外观契约解析 / 1.1 theme 通道适配），后续收敛 shared/frontend 候选 |
| `src/lib/sharedBridge.spec.ts` | shared/frontend 公共层薄 spec（README 约定）：相对引用 `../../../../shared/frontend/binaryEvent` 并断言 binary 双形状归一化。本插件事件为 JSON 载荷、无二进制通道，按规范仍统一走 shared |

### 1.3 App.vue + components/（Phase 1 组件清单）

- `App.vue`：工作台外壳（toolbar 身份/只读·禁删徽章/连接面板入口/刷新、
  七面板 tab 栏、横幅/通知、`waitForHostApi` + ready/request(host.getContext)
  竞速、appearance/theme/locale/context 事件订阅、`kafka/audit` → 审计面板、
  `kafka/stream/*` → StreamPanel.pushEvent 转发、`kafka/connections/statuses`
  读回后端 readOnly/allowDelete 门禁）。
- `TopicTree.vue`：业务 topic 评分排序 + internal（`_` 前缀/后端标记）沉底、
  过滤框、分区数/内部徽章、选中态（selectedTopic 单一来源）。
- `MessagesPanel.vue`：一次性消费表单覆盖 §5.3 全参数——topic、groupId、
  offsetStrategy 五选、offsetTime（datetime/unix ms）、partitions、
  partitionOffsets（`0=100,1:200`）、limit/timeoutMs/maxScanRecords、
  isolationLevel、commit（与过滤互斥、必须 groupId，前端先校验后禁用）、
  filter/keyFilter/valueFilter/headerFilter + matchMode、fieldFilters 编辑器
  （source/path/operator/值/启用开关）、timestampFrom/To、offsetFrom/To、
  decode/decompression；消息表 + 详情抽屉（本地二次 decode/decompression/
  format 切换、valueBase64 完整查看、下载 value）+ JSON/CSV 导出（后端
  `kafka/messages/export`，Blob URL 下载兜底）+ 消费预设（presets 存取）。
- `StreamPanel.vue`：start/stop/pause/resume、收 `kafka/stream/messages` 事件
  实时追加、`kafka/stream/error` 横幅、自动滚动 + 用户上滚暂停、前端展示上限
  1000 行溢出丢最旧并显示 droppedRows 提示、环形缓冲历史分页（Older/Newer →
  `kafka/stream/messages` invoke）、5s 轮询 status。
- `ProducePanel.vue`：key/value/headers（JSON 校验）/partition/count≤1000/
  compression 下拉；发送回显 partition/offset；read_only 整体禁用 + 提示。
- `TopicsPanel.vue`：list（业务排序）/create（partitions/replicationFactor/
  config JSON）/delete（**输 topic 名 confirmTopic 确认**）/扩分区（只增）/
  config get+alter（kv 编辑 + 删除键）/offsets 查询（earliest/latest/自定义
  时间戳）；分区健康视图（leader/replicas/ISR/offline/isHealthy）。
- `GroupsPanel.vue`：组列表（state/protocol/coordinator）+ offsets 表
  （start/end/committed/lag、`hasCommitted:false` → "无已提交数据" 标注）+
  totalLag 聚合徽章 + describe members（assignments）+ reset（earliest/
  latest/timestamp/partitionOffset）+ delete。
- `BrokersPanel.vue`：broker 列表 + config 查看（sensitive 条目掩码）。
- `AclsPanel.vue`：list（过宽过滤前端预检拒绝：必须 resourceName 或
  principal）/create/delete（回显 matched 数）；枚举值为 Kafka 协议术语保持原文。
- `ConnectionsPanel.vue` / `AuditFeedPanel.vue`：照 ldap 改（statuses 增加
  allowDelete 门禁徽标；审计收 `kafka/audit`）。

### 1.4 只读/allowDelete 门禁（前端先禁用 + 提示）

- `readOnly`（宿主 context ∥ `kafka/connections/statuses.readOnly`）→
  工具栏"只读"徽章；produce/commit/topics create·alter·扩分组、groups reset
  等写操作禁用。
- `allowDelete === false`（statuses 字段，缺省不主动禁用、后端仍拒绝）→
  "禁删"徽章；topics/delete（含 confirmTopic 输入确认）、groups/delete、
  acls/delete 禁用。

### 1.5 mock（src/mockDbxHost.ts）

内存假桥，**镜像真实桥当前形状**（与 ldap mock 同面）：`invoke ?? request`、
onEvent/onAppearanceChange/onLocaleChange/onContextChange、decodeBase64/
encodeBase64、workbenchState/clipboard；本插件事件为 JSON 载荷（无 binary 通道）。
覆盖全部 kafka/* 方法：内存集群（2 broker、5 topic 含 2 internal、消费组、
ACL）、produce 追加并回显 offset、consume 按策略/过滤扫描、stream 会话
（定时器发 `kafka/stream/messages` 事件 + ring buffer + pause/resume/status/
messages 分页）、写门禁（ro=1 / nodelete=1 时先发 denied `kafka/audit` 事件再
抛错，镜像后端 policy 分支）。URL 参数：`?theme` `?locale` `?err=1` `?noconn=1`
`?ro=1` `?nodelete=1`。

## 2. 验证证据（真实运行）

环境：`PATH="$HOME/.nvm/versions/node/v22.21.0/bin:$HOME/Library/pnpm:$PATH"`，
`pnpm@11.24.0`。

```
=== pnpm install ===
+ @lucide/vue 1.17.0, vue 3.5.42, @vitejs/plugin-vue 6.0.7, @vue/test-utils 2.5.0,
  happy-dom 15.11.7, typescript 6.0.3, vite 8.0.16, vitest 4.1.11, vue-tsc 3.3.11
Done in 4.4s using pnpm v11.24.0

=== pnpm typecheck ===
$ vue-tsc --noEmit        # 退出码 0，无输出

=== pnpm test ===
 ✓ src/lib/sharedBridge.spec.ts (1 test)
 ✓ src/lib/kafkaModel.spec.ts (24 tests)
 ✓ src/lib/kafkaErrors.spec.ts (3 tests)
 ✓ src/lib/i18n.spec.ts (5 tests)
 ✓ src/components/TopicTree.spec.ts (5 tests)
 Test Files  5 passed (5)
      Tests  38 passed (38)

=== pnpm build ===
✓ 1769 modules transformed.
Wrote self-contained plugin UI to .../kafka/ui/index.html
（268,058 字节，单 <script> 自包含，产物落 ../ui/index.html，ui/ 已 gitignore）
```

### 七语 key 数

- 7 个 locale（zh-CN/zh-TW/en/es/it/ja/pt-BR），**每语 325 key**，扁平化集合
  两两一致（i18n.spec 断言 + 独立脚本复核），无空值。

### 浏览器可视化验证（pnpm dev → /mock.html，Playwright 截图核对后即删）

- 深色 zh-CN 初始渲染：toolbar 身份/徽章、七 tab、topic 树业务排序 +
  internal 沉底、消费表单全参数 ✓
- 选中 order-events → 消费：`已扫描 3 · 命中 3`，消息表分区/offset/时间/key/
  value/headers（trace-id=t-1）✓
- 详情抽屉：JSON 自动识别并 pretty、内层解码/解压/展示格式切换、完整值
  （base64）/下载 value ✓
- 流式：start 后实时追加（stream-1，`已扫描/命中/缓冲 123` 同步）、自动滚动、
  暂停/停止、会话提示 ✓
- 消费组：billing-consumer（Stable/consumer/coordinator 1）、`总 lag: 2`、
  offsets 表（最早/最新/已提交/LAG）、成员 + assignments ✓
- Broker：2 broker 列表 + 配置入口 ✓
- `?ro=1&nodelete=1`：toolbar 只读/禁删徽章、生产面板"只读连接：已禁用生产"、
  Topic 行 扩分区/删除 按钮 disabled（title 提示只读/禁删）✓
- ACL：过滤表单 + "过宽条件会被拒绝"提示、列表空态 ✓
- 验证过程截图与 `.playwright-mcp/` 已全部删除（仓库卫生规约 4）。

## 3. 契约对齐说明

- 方法名/参数/返回形状均按 IMPL_PLAN §5；`enabled` 仅作为 fieldFilters 前端
  行开关，上线载荷剥离（仅启用行下发）。`kafka/stream/messages` 事件消费
  `bufferSize` 字段做 dropped 估算（§5.4/§5.5）。
- 消息二进制保真：valueText 恒 UTF-8 安全预览、valueBase64 恒完整（§5.3），
  抽屉/导出/下载均取 base64 通道。
- 宿主 1.1 特性 optional 降级：appearance 缺失走 theme 通道，再缺失走本地
  规范色板；导出走 Blob URL（宿主 1.0 无 save-file）。
- **遗留提醒（给收口主线）**：前端契约以本文件 + IMPL_PLAN §5 为准；backend
  路（PROGRESS-B）若对 `ConsumeParams`/事件字段做增删，需同步
  `docs/PROTOCOL_KAFKA.zh-CN.md` 并回改 `src/lib/api.ts` 类型与 mock。

## 4. 遗留与风险

1. **GZip 以外解压算法**：浏览器 DecompressionStream 仅支持 gzip/deflate；
   lz4/zstd/snappy 在详情抽屉选择时会展示"不可用"降级提示（消费管线走后端
   decode/decompression，不受影响）。引入 wasm 解压器登记 Phase 2。
2. **消费预设编辑/删除 UI 较简**：删除以每个预设旁的 ✕ 呈现，后续可收敛为
   管理弹层（ldap 同款问题，非阻塞）。
3. **StreamPanel 状态轮询为 5s 定时器**：`kafka/stream/status` 为兜底刷新，
   主要状态靠事件载荷；后端事件若缺 totalScanned 等字段显示为 0，联调时对齐。
4. **ACL 枚举未翻译**（TOPIC/READ/LITERAL 等协议术语保持原文，七语一致），
   如需本地化需扩 key。
5. 工具链备注：spec 不引 node 专有模块（zlib/Buffer），gzip 样本用预生成
   base64 常量（`H4sIAAAAAAAAE6tWylOyMq8FAPicEYIHAAAA` = gzip('{"n":7}')），
   保持 tsconfig types 与 ldap 基线一致、零新依赖。

## 5. Phase 2 商用化（2026-09-05，frontend 路 E）

### 5.1 现象与目标

Phase 1 面板表格为零依赖自绘网格（无列过滤/排序能力有限）、SR（Schema
Registry）/Kerberos/ZK 连接面缺失、监控只有单次快照无趋势与告警。本期按
Phase 2 冻结契约（任务书 11 个 `kafka/schema/*` 方法 + produce/consume/stream
扩展 + statuses 扩展）做商用化补齐，覆盖商用控制台的表格检索、Schema 管理、
监控与生产/连接配置能力面，视觉维持 DBX 设计系统。

### 5.2 改动清单

**新依赖（仅 1 个，进 pnpm-lock）**

- `ag-grid-community@36.1.0`：当前最新稳定 MIT 社区版（用户点名要求表格
  列过滤检索能力；v33+ Theming API 支持 `--ag-*` CSS 变量定制，无需企业版、
  无需 legacy CSS 主题，正好用 DBX 令牌对齐）。不引 `ag-grid-vue3`，以
  vanilla `createGrid` 薄封装自控（少一个版本强耦合依赖）。

**新增文件**

| 文件 | 内容 |
| --- | --- |
| `src/components/DbxAgGrid.vue` | ag-grid 封装：排序/列内过滤（文本+数值）/分页 + 页大小 localStorage 持久化（`dbx-kafka-grid-pagesize-<tableKey>`）、窄容器（<560px，ResizeObserver）降级 minimal 列集、单行选择（点击选中）、行点击/选择事件、`rowClassRules` 透传（Monitor 阈值行高亮）、ag 内置文案随工作台 locale 七语切换。行 id：VM 带 `id` 用之，否则 WeakMap 按对象身份分配（重复空 id 会导致行合并，见 5.4） |
| `src/lib/kafkaColumns.ts` | 全部表格列定义集中点：messages/groups/groupOffsets/members/acls/topics/partitions/topicOffsets/subjects/schemaVersions/lag 十一组 builder（`t()` 实时取 locale）+ `to*Rows()` VM 映射（预览截断/时间格式化/健康着色/schema 徽章文本）+ `minimalColumns()` + 页大小持久化存取 + ag-grid 内置 chrome 文案七语表（20 键 ×7） |
| `src/lib/kafkaColumns.spec.ts` | 10 用例：列字段/过滤类型/排序断言、VM 映射、minimalColumns、页大小持久化 roundtrip、ag 文案七语键齐 |
| `src/components/SchemasPanel.vue` | SR 面板：subject 表（ag-grid）→ 版本表（ag-grid）→ schema pretty 查看 → 版本对比（`versions/compare` hunks 渲染 add/remove/modify 徽章 + before/after 色块 + summary）、兼容性 get/set（GLOBAL/SUBJECT 作用域显示）、compatibility check 弹层（粘贴候选 schema + JSON 预校验 + isCompatible/messages 回显）、register 弹层（subject/format avro·json/schema JSON 校验）、delete subject/version（**双门禁双确认**：canDelete + 输入名称 + 二次确认页） |
| `src/components/MonitorPanel.vue` | lag 监控：组 + topics 过滤 → per-partition lag 快照表（ag-grid，超阈值行 `dbx-row-alert` 红调高亮）→ 开始/停止采样（interval 钳位 5–60s）→ 总 lag 迷你趋势 SVG（polyline + 阈值虚线）→ 阈值上穿横幅告警（回落再上穿才重复触发，横幅不被逐轮采样清空）→ 监控方案保存/加载/删除（复用 `kafka/presets/*`，`params.type="monitor"` + `monitor` 载荷；MessagesPanel 预设列表过滤 `type!=="monitor"` 互不混显） |

**改造文件**

- `src/lib/api.ts`：+11 个 `kafka/schema/*` 方法（test/subjects·list/versions·list/
  get/versions·compare/compatibility·get/set/check/register/delete/delete·version）
  与类型（`SchemaSubject/SchemaVersionRow/SchemaDetail/SchemaDiffHunk/SchemaDiff/
  SchemaCompatibility(+CheckResult)/SchemaRegisterResult/SchemaCompatibilityLevel/
  SchemaAttach`）；`ConsumeParams.schema?`、`KafkaMessage.schemaId/schemaSubject/
  schemaVersion`、produce 参数 `valueBase64?/keyBase64?/schema?`（`value` 转可选，
  与 valueBase64 二选一）；`KafkaPreset.params` 扩展 `type?:"monitor"` + `monitor?`
  载荷；`KafkaConnectionStatus` + `schemaRegistry/kerberos/connectionSource`；
  `brokersList` 返回 `connectionSource?`。
- `src/lib/kafkaModel.ts`：+`buildPropertyMappings()`（properties → 只读映射行，
  密码/secret 掩码占位 `••••••`，映射目标为 manifest 连接字段名：bootstrap_servers/
  security_protocol/sasl_mechanism（GSSAPI 提示 kerberos_*）/sasl_username/
  sasl_password/kerberos_principal/kerberos_keytab_path/kerberos_service_name/
  sr_url/sr_username/sr_password）。
- 表格 ag-grid 化：`MessagesPanel`（消息表，行点击开详情抽屉）、`GroupsPanel`
  （组表/offsets 表/成员表三张，行选择驱动 describe；reset/delete 收敛为工具栏
  动作作用于选中组）、`TopicsPanel`（topic 表/分区健康表/offsets 表三张 + 选中
  行工具栏动作；offsets 查询策略全量 earliest/latest/max-timestamp/log-start/
  custom）、`AclsPanel`（ACL 表 + 行点击详情抽屉）。均保留列排序/列内过滤/
  分页持久化/窄容器降级。
- 表单扩展：`MessagesPanel`/`StreamPanel` 消费表单加「SR 解码挂载」区（subject
  下拉来自 `schema/subjects/list`，version 空 = latest，format 随 subject formats
  回填，SR 不可达时静默降级不阻断消费）；`ProducePanel` 加 key/value base64
  直发切换（走 `keyBase64`/`valueBase64`）与 schema 挂载。
- `ConnectionsPanel.vue`：连接摘要加 SR/Kerberos/ZK（connectionSource）状态徽标
  （statuses 缺省字段全部 optional 降级）；新增「properties 导入助手」弹层：粘贴
  → `parsePropertiesText` → `buildPropertyMappings` → 只读键值表（属性/值/宿主
  表单字段三列，密码掩码），纯内存不落 localStorage。
- `App.vue`：+schemas/monitor 两个 tab 与面板挂载；MonitorPanel `alert` 事件 →
  顶部告警横幅。
- `src/mockDbxHost.ts`：SR 假数据（order-events-value avro v1/v2 + user-signup-value
  json v1、版本 diff 假算法、全局/subject 兼容性级别、register/delete 真 mutating），
  11 个 schema 方法假实现（set/register 走 guardWrite、delete 走 critical 门禁）；
  produce/consume/stream 支持 base64 载荷与 schema 挂载（消息附
  schemaId/schemaSubject/schemaVersion）；statuses 返回 schemaRegistry/kerberos/
  connectionSource。
- `src/lib/i18n.ts`：+115 键/语（tabs.schemas·monitor、messages SR 挂载 5 键、
  produce base64 2 键、topics offsets 策略 2 键、connections SR·Kerberos·ZK·导入
  助手 15 键、schemas 61 键、monitor 27 键），七语全量人工翻译，`i18n.spec`
  守卫保持绿。

### 5.3 验证证据（真实运行，2026-09-05）

```
=== pnpm install ===
+ ag-grid-community 36.1.0
Done in 2.6s using pnpm v11.24.0

=== pnpm typecheck ===
$ vue-tsc --noEmit        # 退出码 0（TYPECHECK OK）

=== pnpm test ===
 ✓ src/lib/sharedBridge.spec.ts (1 test)
 ✓ src/lib/kafkaModel.spec.ts (24 tests)
 ✓ src/lib/kafkaErrors.spec.ts (3 tests)
 ✓ src/lib/i18n.spec.ts (5 tests)
 ✓ src/lib/kafkaColumns.spec.ts (10 tests)   ← 新增
 ✓ src/components/TopicTree.spec.ts (5 tests)
 Test Files  6 passed (6)
      Tests  48 passed (48)

=== pnpm build ===
✓ Wrote self-contained plugin UI to .../kafka/ui/index.html
（仅 chunk >500kB 提示：ag-grid 体量 + 自包含单文件产物，预期内；
  assetsInlineLimit 10MB / inlineDynamicImports 不变）
```

七语 key 数：**440 键/语 × 7 语**（Phase 1 为 325），集合一致性由
`i18n.spec.ts` 断言 + tsx 脚本复核（`en keys: 440`）。

### 5.4 mock 浏览器走查（pnpm dev → /mock.html，Playwright 截图核对后即删）

- 暗色 zh-CN：ag-grid 消息表渲染 7 列（分区/Offset/时间/Key/Value/Headers/Schema）、
  分页器已本地化（「每页条数： 50 · 1 to 3 of 3」）✓
- **列内过滤**：Value 列头菜单 → 过滤操作符「包含」（本地化）→ 输入 `A-1002` →
  结果 `1 to 1 of 3`（截屏核对后清除）✓
- **页大小持久化**：`dbx-kafka-grid-pagesize-messages=20` 写入 → 重建表格分页器
  显示 20，且跨 theme/locale 会话生效 ✓
- **SchemasPanel**：subject 两行渲染 → 选中 order-events-value → 版本表 v1(101)/
  v2(102) → schema pretty 查看（含 currency 字段）→ 「对比」v1→v2 → hunks 渲染
  「新增」徽章 + line 14/15 变更后绿色块 + 摘要 `+3 -0 ~0`；兼容性 SUBJECT
  BACKWARD 徽章 ✓
- **MonitorPanel**：选 billing-consumer、阈值 1、开始采样 → 每轮 `groups/offsets/
  list` 快照（分区 lag 明细行超阈值红调高亮）、趋势 SVG 蓝线 + 红色虚线阈值线、
  「已采样 6 次」、告警横幅「Lag 超过阈值：总 lag 2 > 1」✓
- **监控方案**：保存 `billing-lag-watch` → 出现在方案下拉 + 删除按钮 +
  「方案已保存」通知 ✓
- **properties 导入助手**：粘贴含 SCRAM+SR 的 properties → 解析出
  bootstrap_servers/sasl_mechanism/sasl_username（kafka-app）/
  **sasl_password（•••••• 已掩码，明文不出现）**/sr_url/sr_username/sr_password
  （掩码）目标字段表 ✓
- **light 主题 ag-grid 可读**：白底/边框/过滤图标/分页器全部随 DBX 令牌切换 ✓
- **`?ro=1` 写门禁**：Schema 面板「注册」「应用（兼容性）」按钮 disabled 且
  title 提示「只读连接：已禁用 schema 写操作」；delete 按钮随 canDelete=false
  禁用 ✓
- 走查中发现并修复 2 个真 bug：① `getRowId` 对无 `id` 字段的 VM 返回空串导致
  subject 表两行合并（改为 WeakMap 稳定 id）；② 采样循环开头 `emit("error","")`
  把上一轮阈值告警横幅清掉（清空挪到 startSampling 一次性执行）。
- 验证截图与 `.playwright-mcp/` 已全部删除（仓库卫生规约 4）。

### 5.5 遗留与风险（Phase 2 增补）

1. **ag-grid 包体**：自包含单文件 UI 体积显著增大（chunk >500kB 提示）；宿主
   webview 加载本地文件，无网络成本，暂不拆包；如需瘦身可评估按面板动态
   import（与 build.mjs `inlineDynamicImports: true` 冲突，需一并调整）。
2. **ag-grid enterprise 专属能力未用**（行分组/透视等），MIT 社区版能力
   （排序/过滤/分页）即满足本期需求；filter 菜单内建文案已七语，日期过滤器
   等长尾文案未覆盖（表格均未启用 date filter）。
3. **SR 解码为后端职责**：前端只负责挂载参数与结果展示（decodeError 透传）；
   avro/json 真编解码、`references` 引用解析联调时与 backend 路对齐
   （PROGRESS-B），如契约有出入回改 `api.ts` 类型 + mock。
4. **MonitorPanel 采样定时器**：切走 tab（v-show）不停止采样（后台继续算
   lag），仅卸载/停止按钮/换连接清理；如宿主对后台 invoke 有限流再收敛为
   隐藏时暂停。
5. **offsets 查询 `log-start`/`max-timestamp`**：mock 返回与 earliest/latest 同
   形数据；真机联调时确认 franz-go 侧 OffsetTime 语义（契约已冻结）。

## 6. AWS Glue SR UI + 消息二次解码补齐（2026-09-05，frontend 路 H）

### 6.1 现象与目标

Phase P 冻结契约（与 backend 路 G 共用）落地前端三件事：① `kafka/schema/*`
全族增加可选 `registry?: "confluent"|"glue"`、`schema/test` 返回 `provider`、
statuses 的 `schemaRegistry` 增加 `provider`——Schemas 面板需要 registry 徽章
与双后端切换；② produce/consume/stream 的 schema 挂载在 provider=glue 时后端
`-32000` 拒绝（消息编解码仅 Confluent wire format，Glue 仅管理面）——前端
schema 挂载区需禁用 + 提示（保留 discoverability）；③ 新 manifest 连接字段
`glue_region/glue_registry_name/glue_auth_mode/glue_access_key_id/
glue_secret_access_key/glue_session_token`——连接摘要展示 Glue 徽标（secret
只显示配置态）。此外消息详情二次解码此前仅支持 gzip（lz4/zstd/snappy 标注
降级），本期以轻量纯 JS 解压器补齐全四种。

### 6.2 改动清单

**新依赖（3 个，均进 pnpm-lock）**

- `fzstd@0.1.1`（MIT）：zstd 解码，纯 JS、零依赖；仅提供解码器（与 Kafka
  生产端 zstd 帧兼容），单测向量用 `zstd` CLI 预生成 base64 常量。
- `snappyjs@0.7.0`（MIT）：snappy 压缩/解压，纯 JS。
- `lz4js@0.2.0`（ISC）：lz4 frame 压缩/解压，纯 JS。
- 三者均为无 wasm 的轻量实现，符合"浏览器端消息详情二次解码"场景；质量
  评估：fzstd/snappyjs 有成熟使用面，lz4js API 简单（frame 格式与 Kafka lz4
  兼容），未发现需替换同类的必要。snappyjs/lz4js 无类型定义，新增
  `src/vendor-decompress.d.ts` 环境声明（仅声明用到的 compress/uncompress）。

**改造文件**

- `src/lib/api.ts`：+`SchemaRegistryProvider`/`SchemaRegistryTestProvider`
  类型；`schemaTest` 返回加 `provider`；11 个 `kafka/schema/*` 方法全部追加
  可选尾参 `registry`（省略 = 连接默认提供方，旧 sidecar 忽略该参数不受影响）；
  `KafkaConnectionStatus.schemaRegistry` 加 `provider?`；
  `SchemaCompatibilityLevel` 联合类型扩展 Glue 枚举
  （`DISABLED/BACKWARD_ALL/FORWARD_ALL/FULL_ALL`）。
- `src/lib/kafkaModel.ts`：删除 `UNSUPPORTED_DECOMPRESSION` 降级表，新增
  `inflateZstd/inflateSnappy/inflateLz4/inflateDecompression`（同步解压 + 带
  算法前缀的 error 降级，不抛异常）；`formatMessageValue` 管线改为
  gzip（DecompressionStream）∥ 其余三种（纯 JS）分发，头部注释同步更新。
- `src/components/SchemasPanel.vue`：顶部 registry 徽章（`schemas.registryLabel`
  + 当前 provider 名）+ 双后端均配置时的 Confluent/AWS Glue 下拉（挂载
  onMounted 逐 provider `schemaTest` 探测；用户未手动切换时跟随 statuses
  provider）；切换即清空选中态并整表重载（registry = 命名空间）；兼容性
  下拉按 registry 切换枚举集（Confluent 7 档 / Glue 8 档）；全部 11 个方法
  调用透传 registry。
- `src/components/ConnectionsPanel.vue`：+`connection` prop（宿主 context
  connection 摘要）；SR 徽标旁 provider=glue 时显示「AWS Glue」徽章；当前
  连接行下渲染 Glue 摘要徽标：region/registryName/authMode/accessKeyId
  （双源取值：connection 直挂 camelCase ∥ `external_config` snake_case，照
  ldap `base_dn` 模式），`glue_secret_access_key`/`glue_session_token` 只显示
  已配置/未配置（值在 external_config 非空 ∥ `connection_secrets` 名单即视为
  已配置，任何来源不展示值）。
- `MessagesPanel/StreamPanel/ProducePanel.vue`：+`srProvider` prop；
  `glueSchemaDisabled` computed（provider=glue）→ schema 挂载 checkbox 与
  subject/version/format 下拉禁用、下方/表单下方七语提示
  （`messages.schemaGlueDisabled`）、`buildSchemaAttach` 直接返回 undefined
  （预置/预设残留挂载也不会下发）、provider 变 glue 时自动取消勾选。
- `App.vue`：`refreshBackendPolicy` 读 `statuses.schemaRegistry.provider` →
  `srProvider` ref 下发四个面板；ConnectionsPanel 传入 connection 摘要。
- `src/mockDbxHost.ts`：fixture subjects 增加 `provider` 字段（confluent 2 个
  + Glue 假 subjects `orders-value`(avro, BACKWARD_ALL)/`payments-value`
  (json, FULL_ALL)）；`resolveProvider`（registry 参数缺省落连接默认 provider）、
  `providerSubjects/findSubject` 按 provider 过滤；`schema/test` 按 registry
  返回 `{success, provider}`；`?glue=1` 模式：statuses.provider=glue、仅 Glue
  subjects、confluent 探测返回 `{success:false, provider:"none"}`、
  produce/consume/stream 带 schema 挂载时抛
  `-32000: schema attach is not supported for AWS Glue Schema Registry…`
  （镜像后端业务错，默认模式双 registry 可切换）；context.connection 增加
  `external_config.glue_*` 与 `connection_secrets: ["glue_secret_access_key"]`
  （secret 只给名字不给值）；新增 `codec-lab` 假 topic（gzip/zstd 固定 base64
  向量 + snappyjs/lz4js compress 现场构造，详情抽屉四种解压走查用）。
- `src/lib/i18n.ts`：+14 键/语（`messages.schemaGlueDisabled`、connections
  `glueBadge/glueRegion/glueRegistryName/glueAuthMode/glueAccessKeyId/
  glueSecretLabel/glueSessionTokenLabel/glueConfigured/glueNotConfigured`、
  schemas `registryLabel/registryConfluent/registryGlue`），七语人工翻译，
  `i18n.spec` 守卫保持绿。

### 6.3 验证证据（真实运行，2026-09-05）

```
=== pnpm install ===
+ fzstd 0.1.1 + lz4js 0.2.0 + snappyjs 0.7.0
Done in 1.3s using pnpm v11.24.0

=== pnpm typecheck ===
$ vue-tsc --noEmit        # 退出码 0，无输出

=== pnpm test ===
 ✓ src/lib/sharedBridge.spec.ts (1 test)
 ✓ src/lib/kafkaErrors.spec.ts (3 tests)
 ✓ src/lib/kafkaModel.spec.ts (28 tests)   ← +4（zstd 固定向量、snappy/lz4
   库内 compress roundtrip、zstd/snappy/lz4 失败降级三算法断言）
 ✓ src/lib/i18n.spec.ts (5 tests)
 ✓ src/lib/kafkaColumns.spec.ts (10 tests)
 ✓ src/components/TopicTree.spec.ts (5 tests)
 Test Files  6 passed (6) / Tests  52 passed (52)

=== pnpm build ===
✓ Wrote self-contained plugin UI to .../kafka/ui/index.html
（chunk >500kB 提示：ag-grid + 三个解压器进自包含单文件，预期内）
```

七语 key 数：**454 键/语 × 7 语**（Phase 2 为 440，+14），`tsx` 脚本逐 locale
计数一致（en/zh-CN/zh-TW/es/it/ja/pt-BR 全部 454），i18n.spec 集合相等断言绿。

### 6.4 mock 浏览器走查（pnpm dev → /mock.html，Playwright 截图核对后即删）

- **registry 切换**（默认模式=双 registry）：Schema 面板顶部「注册表:
  Confluent」徽章 + Confluent/AWS Glue 下拉 → 切 Glue 后列表刷新为
  orders-value(BACKWARD_ALL)/payments-value(FULL_ALL)，选中 subject 版本表/
  schema pretty/兼容性 SUBJECT 徽章均走 glue 通道 ✓
- **Glue 兼容性枚举**：provider=glue 时下拉为
  NONE/DISABLED/BACKWARD/BACKWARD_ALL/FORWARD/FORWARD_ALL/FULL/FULL_ALL
  （DOM 选项全量断言）✓
- **glue 禁用提示**（`?glue=1`）：消息/流式/生产三面板 schema 挂载 checkbox
  disabled + 「AWS Glue 连接：消息编解码仅支持 Confluent wire format…」
  提示（checkbox.disabled DOM 断言 true + 三面板 hint 文案断言）✓
- **连接摘要 Glue 徽标**（`?glue=1`）：连接弹窗显示「Schema Registry 已启用」
  +「AWS Glue」徽章 + 区域 us-east-1/注册表 dbx-kafka-registry/认证
  access_key/Access Key ID 值 + Secret Access Key: 已配置（绿色，取自
  connection_secrets 名单，值不出现）/会话令牌: 未配置（黄色）✓
- **四种解压详情解码**：codec-lab topic 消费 4 条 → 详情抽屉分别选
  gzip→`{"n":7}`、zstd→`{"orderId":"A-1001","amount":42,"currency":"USD"}`、
  snappy→`{"algo":"snappy","ok":true}`、lz4→`{"algo":"lz4","ok":true}`
  （DOM 逐条读取解码文本断言）✓
- **明暗主题**：dark/light 两套下 registry 徽章、切换下拉、ag-grid、连接
  弹窗均随 DBX 令牌渲染，无新造视觉语言 ✓
- 走查截图、`.playwright-mcp/` 与 dev server 已全部清理（仓库卫生规约 4）。

### 6.5 遗留与风险（Phase P 增补）

1. **`?glue=1` 拒绝路径 UI 不可达**：前端禁用挂载后正常操作触发不到 -32000
   （mock 已实现该分支，供契约联调/脚本验证用）。
2. **secret 配置态判定是前端启发**：宿主不下发 secret 值时以
   `connection_secrets` 名单判定「已配置」；若宿主后续提供权威的
   secret-binding 状态字段，应替换该判定（PROGRESS-P 本节为准）。
3. **SchemasPanel 双 provider 探测**：每次挂载发 2 个 `schema/test`；旧
   sidecar 忽略 registry 参数时两路都返回 confluent 成功 → 只显示徽章不出
   切换下拉（optional 降级，符合规则 3）。
4. **fzstd 仅解码**：zstd 压缩仍由生产端/后端负责（Kafka 场景前端只需解码）；
   单测向量由 `zstd` CLI 预生成（常量见 kafkaModel.spec.ts 头注释）。
5. **Glue 兼容性 set/check 语义**：前端按枚举透传；Glue 的 DISABLED 与
   Confluent NONE 语义差异、真机 check 行为待与 backend 路联调确认。

## 7. UI 体验升级轮（2026-09-05 第四轮：L/M/N/P/Q 并发 + 主线收口）

用户反馈驱动的四项改造 + 持续扫描闭环，五路并发（文件归属隔离：i18n 主线预置、
PROGRESS 主线合并）：

### 7.1 交付（按路）

- **L 路（ProducePanel 重构 + CodeMirror）**：引入 CodeMirror 6
  （@codemirror/{state,view,commands,language,lang-json}；理由：monaco ~5MB+worker
  与自包含单文件 UI 冲突，CM6 ~300KB 无 worker）——`CodeEditor.vue` 薄封装
  （行号/wrap/JSON 高亮+错误行 gutter 标记/invalid 红框/disabled 只读，主题全走
  DBX 令牌、明暗跟随）；ProducePanel 竖排大块重构（零 inline style：topic →
  key → value 大编辑器 flex-grow ≥240px → headers → 发送选项栅格 → 成功条 +
  大号主色发送按钮）；i18n 清理 13 个死 key ×7 语 = 91 条；走查修复 2 个真 bug
  （produce-field-full 抢 flex、CM6 gutter lineMarkerChange 缺失致 "!" 标记残留）。
- **M 路（TopicTree 侧栏 + 消费筛选体验）**：侧栏可拖宽（180–480px，
  `dbx.kafka.ui.treeWidth` 记忆，双击重置）/可折叠（40px 竖条，记忆）/`/` 快捷键/
  计数徽章；消费表单五分组卡片（基础/定位常展开，时间与范围/过滤/解码可折叠
  记忆 `dbx.kafka.ui.msgFilters`）；时间选择器（datetime-local step=1 + 「现在」
  + datetime↔unix ms 切换，经既有 offsetTimeToParam，起止倒挂校验禁用消费）；
  fieldFilters 行式编辑器（source/operator/path/value/启用/删除 + 数值校验红框）。
  走查 11 项全过（含拖宽/折叠/记忆/倒挂禁用/明暗/窄容器）。
- **N 路（持续 UI 扫描）**：`docs/UI_SCAN_FINDINGS.zh-CN.md`——12 个走查对象 ×
  视口（720/1280/1440）× 主题 × 参数（ro/glue/err/noconn）矩阵，含键盘与弹层
  路径；发现 P0×0 / P1×4 / P2×15，P1 全部转入修复。
- **P 路（P1 修复）**：P1-1 消费后首屏让位（结果区 flex 食满 + 表单上限 46% +
  smooth 滚动到统计行）；P1-2 抽屉/连接弹窗 Esc 关闭（遮罩 v-if 卸载无残留、
  高层弹窗在场时让位）；P1-3 焦点管理（打开进面板/关闭归还触发元素/Tab 简单
  陷阱，纯逻辑 `decideModalKeydown` 入 kafkaModel + 5 条 spec）。走查 18/18。
- **Q 路（P1-4 修复）**：`positiveInt` number 型 ref 调 `.trim()` 的运行时异常
  （签名改 unknown + String 归一，partition 输入同病一并修）；发送门禁
  （value 空/headers 非法 → disabled + title/aria-label 原因，复用既有七语 key）。
  走查 8 步 0 控制台异常。主线随后把 MessagesPanel/StreamPanel 的同形
  `positiveInt` 一并防御归一（其 ref 为 string 型属预防性加固）。

### 7.2 验证证据

- `pnpm typecheck` 0 错误；`pnpm test` 7 文件 74 用例全绿（+CodeEditor 8、
  +decideModalKeydown 5）；`pnpm build` 产物自包含单文件 UI（ag-grid + CM6）。
- 四路 Playwright 走查（独立会话/端口互不冲突，截图核对后删除）：L 修复 2 bug
  复测通过、M 11 项、P 18/18、Q 8 步 0 异常；N 二轮复核确认 P1 修复落地。
- 收口 `bash scripts/test.sh` 全绿（前端三件套 + UI 走查 + go + package +
  容器 smoke）。

### 7.3 遗留

- P2×15 见 `docs/UI_SCAN_FINDINGS.zh-CN.md`（后续打磨 backlog：错误文案透传
  原文、ag-grid 焦点指示、Stream 分页按钮热区、抽屉标题语义等）。
- 连接弹窗内「导入助手」子弹层 Esc 会整层关闭（ConnectionsPanel 不在本轮
  授权范围）；secret 配置态判定仍是前端启发（宿主出权威 binding 状态字段后替换）。

## 8. UI 持续优化轮·五（2026-09-05：R/S/T 三路并发 + 主线收口）

### 8.1 交付（按路）

- **R 路（消息页布局压缩 + 大数据量防护）**：消费表单默认收起为一行摘要条
  （chips：topic/策略/上限/过滤数/groupId/decode + mini 消费按钮；消费成功自动
  收起、展开态记忆 `dbx.kafka.ui.msgFormOpen`），表格成为主体（1280×800 首屏
  ~25 行可见）；性能四件套——`capRows` 内存上限 10000 行裁头部（裁剪徽章
  uiRowsCapped）、行数组 shallowRef+triggerRef（5000 行不走深响应代理）、
  ag-grid 一次性批量 rowData 提交、详情抽屉 value 截断预览（头尾 16384 字符
  + ⋯ N chars ⋯，512KB 不再整段塞 DOM）；「跳到最新」兼容 ag-grid v36 三代
  viewport 类名。`?big=1` 压测：5000 行 consume 落地 93ms、20 帧滚动 154ms、
  longtask=0、pageerror=0。
- **S 路（全局 P2 打磨批，8 项）**：错误文案本地化归一（kafkaErrors 扩充
  连接类规则，Stream/Connections/Groups 错误点改走 friendlyKafkaError）；
  Topics 表与侧栏排序统一（sortTopics 同源）；Stream 分页按钮热区 36×20→52×32；
  ag-grid 单元格键盘焦点指示（主题令牌 outline，明暗两套）；导入助手子弹层
  Esc 分层关闭；只读/Glue 禁用态统一视觉（grayscale/opacity/not-allowed +
  title）；ACL 空态引导；Schema 兼容级别徽标语义化。
- **T 路（启动/加载性能 + 大数据压测）**：App 层懒挂载（visited set：首访才
  v-if 实例化 + 已访问 v-show 保状态）+ 7 个重面板 defineAsyncComponent；
  stream 事件背压（面板不可见时有界队列 800 丢最旧，切回按序补发）；
  `?big=1` 压测 fixture（500 topics/5000 消息/200 组）；build.mjs 换
  codeSplitting:false（产物 byte 级一致、deprecation 告警消除）。实测：
  产物 FCP 364→124ms、tab 就绪 384→119ms、DOM 788→221、heap 18.2→13.4MB；
  切 tab 10 轮 heap delta=0；CodeMirror 组件级懒加载经评估**回退**
  （spec 同步断言 + 单文件产物动态 import 被内联提前求值，无净收益，
  模块链推迟由 Produce 面板异步化达成——produce 首挂 145ms 就绪）。
- **主线接线**：groups.state* 六态七语预置 + kafkaColumns 状态列
  valueFormatter 本地化（Stable/Empty/PreparingRebalance/…，未映射枚举原文
  兜底）；确认 TopicTree 错误已统一走 showError→friendlyKafkaError（S 报告的
  英文原文属未命中规则兜底，规则已扩充）。

### 8.2 验证证据

- `pnpm typecheck` 0 错误；`pnpm test` 79/79 绿（+capRows/truncatedValuePreview
  等 5 例）；收口 `bash scripts/test.sh` 全绿（含 pnpm build 产物 1.78MB 自包含、
  UI 走查 2/2、容器 smoke 12 场景 10 PASS/0 FAIL/2 SKIP）。
- 三路走查（R：big 模式摘要条/裁剪/截断/记忆；S：逐项程序化断言 + 明暗/ro/glue
  矩阵；T：性能指标前后对比 + 10 轮切 tab 零泄漏）截图核对后均已删除。

### 8.3 遗留

- 「跳到最新」当前滚动分页视口，跳末页需 DbxAgGrid 暴露 gridApi（跨组件契约，
  下一轮）；Monitor 采样在面板内部降频未做（App 层已背压 stream 事件侧）；
  App 层缓冲丢弃为静默计数；P2-2 错误横幅遮挡 tab 栏、P2-11 light 工具栏泛红、
  P2-13 窄视口死空间（均在 App.vue/style.css，待小修轮）。

## 主题令牌桥（2026-09-05）

- 接入 `shared/frontend/themeSync.ts`：`main.ts` 挂载前 `installHostThemeBridge()`，
  插件变量桥接宿主 `--color-*` 令牌——首绘即命中宿主主题（不再等 init 后 JS 回写），
  主题切换自动跟随，primary/radius/字体纳入同步面。宿主无令牌（mock/旧宿主）回退
  暗色规范值，行为不变。
- 验证：`vue-tsc` 0 错；`vitest run` 8 文件 82 用例全绿（含新增
  `themeSync.spec.ts` 薄 spec）；v0.1.4 发版。

## 白色主题配色标准化（2026-09-05 第二轮）

四插件联合审查白色主题配色错误，语义令牌与明暗分支在
`shared/frontend/themeSync.ts` 单点收敛（详见该文件与 shared/frontend/README）。

- kafka 本轮替换：`badge-ok`/`badge-warn`/`dbx-cell-ok`/`dbx-row-warn`
  （#10b981/#d97706 → `--success`/`--warning`）、`.state-dot.connected`
  （#10b981 → `--success`）、ProducePanel 发送成功横幅与 SchemasPanel diff
  after 行（#10b981 → `--success`）、modal/drawer 遮罩（50%/35% 黑 → 统一
  `--overlay`）、CodeEditor 暗色分支双属性化（`data-theme` +
  `data-dbx-theme`，收窄暗色宿主首绘窗口期）、图标 dark 变体双属性化。
- 验证：`vue-tsc` 0 错；`vitest run` 8 文件 83 用例全绿（themeSync 薄 spec
  增补语义令牌/遮罩/light 回退断言）。无新增文案，七语不受影响。

## UI 持续优化轮·六（2026-09-05：弹层键盘可达 + 细节专业化收口）

针对 §8.3 与 `docs/UI_SCAN_FINDINGS.zh-CN.md` 遗留 P1/P2 的收口轮。

- **连接弹窗键盘可达（P1-2/P1-3 收口）**：ConnectionsPanel 主弹窗补 Esc 关闭 +
  Tab 焦点陷阱 + 关闭归还触发元素（决策复用 `kafkaModel.decideModalKeydown`，
  与消息抽屉同源；打开时 nextTick 后焦点进首个控件，modal 加 `tabindex=-1`/
  `role=dialog`/`aria-label`）。助手子弹层打开时主弹窗监听让位（子弹层捕获阶段
  已拦截 Esc），分层关闭语义不变。
- **跳到最新收口（§8.3 遗留）**：DbxAgGrid `defineExpose({ goToLatest })`——
  分页表先 `paginationGoToLastPage()` 再 `ensureIndexVisible(last, "bottom")`；
  MessagesPanel `jumpToLatest` 改走 gridApi，删除跨 ag-grid 版本脆弱的
  viewport 类名 DOM 滚动 hack（分页模式下 viewport 不含未渲染页，原实现跳不到
  末页）。
- **P2 细节批**：错误横幅 `top` 42→70px（工具栏 37 + tab 栏 31 之下，不再遮挡
  页签，P2-2）；浅色主题工具栏连接色染色 10%→5%（dark 维持 10%，颜色主线索由
  identity 前 4px 色条承担，P2-11 泛红误读）；窄视口（≤900px）侧栏高度改内容
  自适应 `height:auto; max-height:46%; min-height:120px`（少 topic 不再留大块
  死空间，P2-13）；抽屉标题语义化 `topic · 分区 N · Offset N`（复用既有
  `messages.colPartition/colOffset` 文案键，七语无新增，P2-5）；mock.html 内联
  SVG data-icon 消除 favicon 404（P2-14）。
- 验证：`pnpm typecheck` 0 错；`pnpm test` 8 文件 83 用例全绿；mock 夹具
  playwright 走查 12 项 PASS（连接弹窗焦点进弹窗/Tab×20 陷阱/Esc 关闭/焦点
  归还/助手子弹层分层 Esc/横幅 top=70 不遮 tab（实测 bannerTop=70 vs
  tabBarBottom=66）/抽屉标题 `order-events · 分区 0 · Offset 0`/抽屉 Esc 回归/
  跳到最新回归/窄视口侧栏 gap=0/light 染色 0.05 vs dark 0.10/favicon data-icon）；
  收口 `bash scripts/test.sh` 全绿（前端三件套 + UI 走查 2/2 + 容器 smoke
  10 PASS/0 FAIL/2 SKIP）。
- 至此 UI_SCAN P1×4 全部关闭（P1-1 R 路、P1-2/P1-3 本轮、P1-4 归 L 已修）；
  P2 余 P2-15（图标按钮 title 依赖，走查接受现状）一项保留观察。

## UI 持续优化轮·七（2026-09-05：P2 批量收口 + 禁用态/页签栏专业化）

UI_SCAN 遗留 P2 的批量收口轮（P2-1/3/4/6/7/8/9/10/12，均在途代码本轮落地），
另做两处页签栏新打磨。收口状态矩阵回填见
`docs/UI_SCAN_FINDINGS.zh-CN.md` §五。

- **P2 批量**：错误文案兜底（网络类规则覆盖夹具串，正文本地化、原文留 title）；
  Topics 管理表与侧栏树同源 `sortTopics` 排序（P2-3）；Stream 环形缓冲分页
  按钮 ≥32px 热区（P2-4）；Schema 当前兼容级别徽标加语义前缀（P2-6）；只读
  发送按钮禁用降饱和（P2-7）；ACL 空态附过滤引导（P2-9）；ag-grid 键盘焦点
  `.ag-cell-focus` primary 描边，仅键盘聚焦时显示（P2-10，CSS 已落、宿主真机
  复核待做）；消费组状态列 `groups.state*` 七语映射、未知枚举原文兜底（P2-12）。
- **禁用态统一收敛（P2-8 补全）**：cursor not-allowed + `.checkbox` 复选框禁用
  透明度从 Stream/Produce/Groups 三处 scoped 重复块收敛到全局 style.css 一份
  （面板特例如发送按钮降饱和保留 scoped 层），顺带补齐 MessagesPanel 等
  未覆盖面板——Glue 下解码组 SR 挂载复选框自此有可见禁用态。
- **页签栏专业化**：选中 topic 徽标限宽 220px 省略 + title 悬停（长 topic 名
  不再撑爆 9 页签同排的页签栏）；页签按钮 `flex-shrink:0` + `tab-bar`
  overflow-x:auto（窄视口/长语言不压缩变形，可横向滚动）。
- 文案：全部复用既有键，七语无新增。
- 验证：`pnpm typecheck` 0 错；`pnpm test` 8 文件 83 用例全绿；`pnpm build`
  通过；mock 夹具 playwright 走查 8 项 PASS（徽标 220px 限宽/省略裁切/title
  绑定真实 topic/页签 flex-shrink=0/720px overflow-x auto/无页面错误×2/Glue
  复选框禁用态 cursor=not-allowed 透明度 0.45/0.55）；既有 ui_test 2/2 回归
  PASS。本轮纯前端样式层改动，未动 sidecar，容器 smoke 沿用轮·六结论。

## UI 扫描第 2 轮修复轮（2026-09-06：P1-5 弹层行为下沉 + P2-12/16/17）

对应 `docs/UI_SCAN_FINDINGS.zh-CN.md` 第 2 轮场景化扫描（第六章），本轮修复
P1 与明确回归项；状态回填见该文档 §6.7。

- **P1-5 弹层 Esc/焦点管理覆盖面**：新建
  `frontend/src/lib/modalBehavior.ts`——`useModalBehavior` 组合式函数把
  App 壳层已验证的 `decideModalKeydown`/焦点陷阱/归还逻辑下沉为插件内共享
  实现：模块级层栈仅栈顶响应 Esc/Tab（照连接弹窗导入助手子弹层的捕获态
  语义，逐层关闭不透传）；打开时焦点进容器首个可交互控件（无控件兜底容器，
  需 `tabindex="-1"`）；关闭时焦点归还触发元素。接入全部 13 处弹层：
  AclsPanel 详情抽屉/创建/删除、TopicsPanel 创建/删除/扩分区/配置、
  SchemasPanel 注册/兼容检查/删除、GroupsPanel 重置/删除、BrokersPanel
  配置（容器统一补 `tabindex="-1" role="dialog" aria-modal="true"`）。
  App 壳层连接弹窗、ConnectionsPanel、MessagesPanel 抽屉的已验证实现不动。
- **P2-12 回归**：`kafkaColumns.groupColumns()` 状态列查找键
  `groups.state*` → `messages.state*`（以 i18n 实际存在的键为准），
  未知枚举原文兜底不变。
- **P2-16**：新增 `acls.createInvalid` 七语键，ACL 空名校验不再复用
  topic 专属的「分区数与副本因子」文案。
- **P2-17**：`topics.created` 七语补键（此前成功提示显示原始键名）。
- **防回归测试**：`modalBehavior.spec.ts` ×5（开焦点/Esc 归还/Tab 双向
  回绕含越界兜底/层栈逐层 Esc/子弹层在场时下层让位）；
  `kafkaColumns.spec` 增 P2-12 断言（`messages.state*` 七语键存在性 +
  zh-CN 格式化冒烟 + 全列头「未解析点分键」护栏——该护栏顺带抓到
  `topics.colOffset` 缺键，topicOffsetColumns 已改引用既有
  `messages.colOffset`）。
- **既有测试红转绿（本轮暴露的组件/夹具缺陷，限 kafka/frontend）**：
  GroupsPanel 行级失败横幅被 reload 起手清错误 emit 立即冲掉
  （submitReset 先刷新详情再上抛结果）；`resetTimestampMs` String 归一
  （同 P1-4B 范式）；partitionOffset 校验分支顺序（无效条目优先于必填，
  `0=abc` 不再误报「必填」）；GroupsPanel.spec mock 桥补 `{error}` 信封
  →异常拒绝（镜像真实桥形态，工作区规则 7）+ 弹窗断言逐步重查
  （teleport stub 重渲染替换弹窗元素，过期 wrapper 失效）。
- 验证：`pnpm typecheck` 0 错；`pnpm test` 11 文件 115 用例全绿；
  playwright（playwright-core + 系统 Chrome headless `--disable-gpu`，
  vite :5294 mock 夹具）18 项 PASS、0 pageerror——13 处弹层逐一
  （焦点入层/Tab×8 不出层/Esc 关闭/焦点归还触发钮；ACL 抽屉焦点归还
  网格）+ 连接弹窗回归对照 + P2-12 状态列「稳定」+ P2-16 横幅
  「资源名与主体均为必填」+ P2-17 通知「Topic 已创建: scan-topic-fix」；
  复验截图即删未入库。P2-18/P2-19 不在本轮范围，留待下轮。

## UI 扫描第 3 轮修复轮（2026-09-06：P2-18/19 收尾 + AuditFeed denied 夹具）

对应 `docs/UI_SCAN_FINDINGS.zh-CN.md` §6.7 留待项与 §6.5 遗留，状态回填见该
文档 §6.8。全部改动限 `kafka/frontend/` 内。

- **P2-18 树错误区原文透传**：`TopicTree.vue` 展示层接入 `friendlyKafkaError`
  （与 App 错误横幅同一条映射规则）：`.tree-error` 正文渲染友好化文案，
  友好化结果与原始串不同时原始串挂 title 悬停供排查。上层 `loadTopics`
  仍存原始串，不动数据面。
- **P2-19 ag-grid 分页文案中英混排**：从 ag-grid 36.1.0 包内核对分页条实际
  消费键（`to`/`of`/`page`/`more`/`number`/`firstPage`/`previousPage`/
  `nextPage`/`lastPage`/`ariaPageSizeSelectorLabel`；扫描报告建议的
  `paginationFirst` 等键名 v36 不存在），全部纳入 `AG_GRID_LOCALE_KEYS` 并
  补七语内联字典（未新增依赖）。zh 组合：行摘要「1 至 50 / 共 201」、
  页摘要「第 N / 共 5」；en「1 to 50 of 201 / Page of 5」全英文。
- **AuditFeed denied 事件链夹具（§6.5 遗留）**：`mockDbxHost.ts` 新增
  `?audit=denied`——宿主 `onEvent` 监听就绪后（轮询 eventListeners 非空）
  注入 1 条 denied + 900ms 后 1 条 ok 的 `kafka/audit` 事件（镜像
  AuditRecord JSON 面），15s 兜底放弃防孤儿 interval。ro 模式写入口禁用
  导致 denied 链不可达的问题自此可在 mock 中验证（自动展开/denied 徽标/
  错误横幅/ok 对照行）。
- **防回归测试**：`TopicTree.spec` ×2（夹具串本地化 + title 原文；未覆盖
  错误原文透传无 title）；`kafkaColumns.spec` ×1（分页组合键七语冒烟，
  键齐由既有 `AG_GRID_LOCALE_KEYS` 键集测试自动守护）。
- 验证：`pnpm typecheck` 0 错；`pnpm test` 11 文件 118 用例全绿
  （基线 115）；playwright（playwright-core + 系统 Chrome headless
  `--disable-gpu`，vite :5294）15 项 PASS、0 console error / 0 pageerror
  ——`?err=1`（zh/en）树错误区本地化 + title 原文 + 与横幅同源、
  `?big=1&locale=en` / `?big=1` 分页条无混排、`?audit=denied` 审计链全链路
  （2 条事件 · 1 条被拒绝、自动展开、已拒绝徽标、横幅）、默认页回归；
  复验截图即删、/tmp 夹具目录已清理。遗留维持：P2-10 宿主真机复核、
  P2-15 观察保留。

## UI 扫描第 5 轮修复轮（2026-09-06：第 4 轮专家深度测试 P1×2 + P2×5 全收口）

对应 `docs/UI_SCAN_FINDINGS.zh-CN.md` §7.2 全部发现，状态回填见该文档 §7.6。
全部改动限 `kafka/frontend/` 内。

- **P1-6 消费 offset 范围过滤不可用**：`MessagesPanel.optionalNumber` 入参
  String 归一（`String(value ?? "").trim()`，P1-4B/GroupsPanel `resetTimestampMs`
  同范式收口——Vue 3 number 型 v-model 直接 `.trim()` 抛 TypeError 且请求不发出）。
- **P1-7 慢响应竞态跨 topic 串台**：`runConsume` 请求序号守卫 `consumeSeq`——
  响应（成功/失败）落地前比对，不一致即丢弃；topic 切换 watch 自增序号 +
  复位 `consuming`（新 topic 立即可重发，旧请求 finally 序号不匹配不再抢先
  解锁在途新消费）。
- **P2-20 扩分区报错文案错位**：`TopicsPanel.submitExpand` 改用专用键
  `topics.expandCountInvalid`（含 `{count}` 当前值插值），七语补键，不再复用
  `err.partition`。
- **P2-21 空集群空态误导**：MessagesPanel 空态两态——未选 topic 用新键
  `messages.uiNoTopicSelected`（七语），已选 topic 无匹配维持原文案。
- **P2-22 树过滤键盘死角**：过滤框 Enter 选中首个匹配项；清除钮
  `tabindex="-1"` 移出 Tab 序（Esc 仍为键盘清空路径）。roving tabindex /
  listbox 化留作后续增强（本轮选改动小可验证方案）。
- **P2-23 数值单元格千分位**：`kafkaColumns.numberColumn` 统一
  `valueFormatter`（`Intl.NumberFormat`，按工作台 locale 缓存实例；字符串
  数字兼容、非数值占位与空值透传），与分页条「共 5,000」同屏一致。
- **P2-24 异步反馈读屏可感知**：App 错误横幅 `role="alert"`、成功通知
  `role="status" aria-live="polite"`（与 ProducePanel 成功条约定收敛）。
- **i18n**：新增 `messages.uiNoTopicSelected`、`topics.expandCountInvalid`
  两键 ×7 语（键集一致性由既有 i18n.spec 守护）。
- **防回归测试（+14 用例）**：`MessagesPanel.spec` ×4（offset 范围端到端
  发请求 / 仅 offsetTo / deferred promise 手控时序的竞态丢弃+重发落地 /
  空态两态）；`TopicsPanel.spec` ×3（扩分区小值/相等值专用文案且零请求 /
  合法值提交）；`App.spec` ×2（role=alert / role=status+aria-live）； 
  `TopicTree.spec` ×3（Enter 选中首匹配 / 无匹配不误发 / 清除钮 tabindex）；
  `kafkaColumns.spec` ×2（千分位 / 占位透传）。
- 验证：`pnpm typecheck` 0 错；`pnpm test` 14 文件 132 用例全绿（基线
  118）；playwright（playwright-core + 系统 Chrome headless `--disable-gpu`，
  `/tmp/uiscan-kafka-r4`，vite :5294）8 项 PASS、0 console error /
  0 pageerror——P1-6 invoke 参数捕获含 offsetFrom/offsetTo、P1-7 延迟 1.2s
  竞态落地 payment-gateway 详情抽屉、P2-20 横幅文案 + 零 update 调用、
  P2-21 defineProperty 空集群夹具、P2-22 activeElement 断言、P2-23
  `?big=1` 位点表 `1,250/1,149/1,199`、P2-24 双 live region；复验截图即删、
  dev server 已 kill。第 4 轮观察项 4 条维持不计级未动。

## 9. Phase 3 特性追赶（2026-09-07，H 路 worktree 并发实施）

> 分支 `phase3/kafka-frontend`（commit `8062ab8` + `8d79bca`），契约依据
> IMPL_PLAN §12.2.5-12.2.7（冻结）；对标对象与裁决记录见 IMPL_PLAN
> §12.0/§12.8。

### 9.1 交付

- **F4 Flow 随机测试数据生成**：`kafkaModel.ts` 新增 `mulberry32`（固定
  种子 RNG）、`generateAvroRandom`（record/array/map/union 非 null 首支/
  enum/fixed + date/timestamp-millis/uuid/decimal）、`expandTemplate`
  （`{uuid}` `{now}` `{int:min,max}` `{float:min,max}` `{pick:a|b|c}`）、
  `matchingSchemaSubjects`（`<topic>-key/-value` 前缀发现）、
  `clampFlowCount/clampFlowIntervalMs`；ProducePanel「测试数据生成」组
  （flow 开关、来源 schema_random|template、countPerSend/intervalMs、
  启停 + 运行徽标 + 累计计数 + 最近 1 条回显）；自动停止三条件
  （read_only watcher、校验失败、连续失败 ≥3）；schema_random 走
  subjects/list 前缀发现 + schema/get latest + produce schema 挂载
  （version 缺省），零后端改动；PROTOBUF subject 命中 → 行内提示改
  template。
- **F5 Schema 三件套**：版本表行「克隆」（schemaGet 预填注册弹窗、
  subject 可改）；format 选定后「插入模板」（AVRO/JSON/Protobuf 三段
  代码常量，protobuf 免 JSON 校验）；详情区 树/文本 toggle + 新组件
  `SchemaTree.vue`（递归可折叠；`buildSchemaTree` 支持 AVRO 与 JSON
  Schema properties/required `*`/default；PROTOBUF 返回 null → 文本 +
  行内提示）。
- **F6 六项**：① Messages/Stream 表 quickFilter 防抖 150ms（DbxAgGrid
  增 quickFilter prop）；② 详情抽屉复制 key/value/headers/整条 JSON 四
  按钮 + 消息表行操作「复制 JSON」（clipboard API + execCommand 兜底，
  `uiHelpers.ts`）；③ 本地/UTC toggle（localStorage `kafka.ts.tz`，
  timestamp 列 tz 感知比较器 + 完整 ISO title）；④ 生产面板分区数徽标 +
  `partitionInputIssue` 超界行内校验（App 透传 partitionCount，第 4 轮
  观察项收口）；⑤ TopicTree `isHealthy===false` 红点 + title「N 个分区
  不健康」（消费 topics/list 新字段，无额外请求）；⑥ timestamp 列
  `agDateColumnFilter`、offset/lag/endOffset 列 number filter，
  `AG_GRID_LOCALE_KEYS` 补 13 键（从 ag-grid 36.1.0 dist 核对实际消费
  点，七语内联字典）。
- **F2 前端半件**：ConnectionsPanel OAUTH/MSK 摘要徽标（token source/
  region/access key，secret 只显已配置态，照 glue 形态）；
  `oauthFormVisibility`/`oauthRequiresSaslSsl` 联动链纯函数；
  `friendlyKafkaError` msk 无凭据/region 缺失映射；三面板 schema 挂载
  format 枚举扩 `avro|json|protobuf`（§12.1 F1 前端半件）。
- **mock 桥**：`?msk=1` 开关（照 `?glue=1` 范式，external_config 注入
  oauth/msk 字段 + connection_secrets）；`degraded-topic` unhealthy 夹具
  （isHealthy:false + unhealthyPartitions:1）；`orders-proto-value`
  PROTOBUF subject 夹具；schema register 支持 protobuf。
- **七语**：新增 50 键 ×7（err 2/tree 1/messages 10/stream 1/produce 23/
  schemas 6/connections 7）；`i18n.spec.ts` 守卫绿。

### 9.2 验证

- typecheck 0 错；vitest **17 文件 180 用例全绿**（基线 132，新增 48 例）：
  `kafkaModel.spec` +16（mulberry32 序列锁定 / 生成器固定向量 seed 7 /
  占位符 / 前缀发现 / Flow 夹持 / OAUTH 联动链 / SASL_SSL 约束 / 分区
  校验 / 树模型 / tz / date 比较器 / 流式过滤）、`uiHelpers.spec` 新 6
  （clipboard 双路径失败降级 / debounce）、`ProducePanel.spec` 新 7
  （fake timers 启停 / 连续失败自停 / PROTOBUF 提示零发送 / schema_random
  带挂载 / 分区徽标 / 超界拦截 / 界内可发）、`SchemasPanel.spec` 新 4
  （树渲染 toggle / PROTOBUF 提示 / 模板插入 / 克隆预填）；既有 spec
  扩展 +15。build ✓。
- 收口（主线）：test.sh 全绿（含 UI walkthrough 2/2、smoke 15 场景）。

### 9.3 遗留

- StreamPanel 时区显示仍跟随 `formatTimestamp` 缺省 local（契约只要求
  MessagesPanel toggle），后续统一。
- decimal 逻辑类型生成器输出数值形状，goavro `NativeFromTextual` 编码
  需 `*big.Rat`——生成值大概率编码失败（已知差异，登记不动）。
- 第 4 轮观察项「生产面板不显示分区数」「时间戳时区标注（Messages）」
  由本轮收口（F6-4/F6-3）；其余观察项维持。

## 10. 测试覆盖完善轮（2026-09-07，frontend 覆盖 agent）

### 10.1 覆盖率工具接入

- devDependencies 新增 `@vitest/coverage-v8@^4.1.11`（与 vitest 4.1.11
  同线）；scripts 新增 `test:coverage`；**既有 `test` script 未动**
  （test.sh 依赖面不变）；新增 `kafka/frontend/.gitignore` 忽略
  `coverage/`（工作区规则 4）。零 vitest config（v8 provider 默认约定）。

### 10.2 覆盖率与新增 spec

- 总覆盖 statements 65.98% → **72.82%**（branches 56.00 → 61.08 / funcs
  55.81 → 63.31 / lines 68.33 → 75.39）；23 文件 **219 用例**全绿
  （基线 17 文件 180，+6 文件 +39）。
- 六个无 spec 面板补齐：`MonitorPanel.spec`（7：禁用态/lag 表/阈值告警
  每轮上穿一次/方案存取删）、`StreamPanel.spec`（8：启停桥参数/暂停恢复
  /session 过滤/error friendlyKafkaError 归一/quickFilter 防抖/Older-
  Newer clamp/无 topic 禁用）、`AclsPanel.spec`（7：过宽拦截/渲染/创建
  校验/删除确认/canWrite·canDelete/抽屉）、`BrokersPanel.spec`（4）、
  `AuditFeedPanel.spec`（6：denied 自动展开/计数徽标/clear）、
  `DbxAgGrid.spec`（7：vi.mock ag-grid 锁定 quickFilter 透传/分页持久化
  /窄容器降级/rowClick/goToLatest）。
- 文件级亮点：AuditFeedPanel 100%、MonitorPanel 88% lines、
  BrokersPanel 93.47% lines；DbxAgGrid 0 → 80.55% lines。

### 10.3 下一轮低覆盖目标（lines 升序）

hostTheme 30 / TopicsPanel 44.55 / SchemasPanel 55.83 / App 57.14 /
TopicTree 57.89 / MessagesPanel 61.75 / api 70.17 / ProducePanel 72.86。

### 10.4 行为锁定（现行为认知点，非 bug）

- StreamPanel 事件内嵌错误经 friendlyKafkaError 归一（设计行为）；
- MonitorPanel `monitor.needGroup` 分支因按钮 disabled 在 UI 不可达
  （防御性代码）；
- AuditFeedPanel 清空后若从未展开则整个 section 消失、手动展开过则保留
  空态摘要。

### 10.5 验证

`pnpm typecheck` 0 错；`pnpm test` 23 文件 219 用例全绿；`pnpm build`
通过（chunk warning 为既有现象）；改动仅 `kafka/frontend/**`。

## 工作台滚动条隐藏：条体不再常驻显示（2026-09-09）

`style.css` 全局滚动条由"6px thin 常驻"改为全部隐藏（`scrollbar-width: none` +
`::-webkit-scrollbar { display: none }`），滚动仍由滚轮/触控板/键盘驱动；`.tab-bar`
横向滚动残留的 `scrollbar-width: thin` 同步移除。原先对 webkit 伪元素定制宽高会把
滚动条从悬浮态固化为占位常驻态，与宿主观感不符。改动仅 `kafka/frontend/src/style.css`；
验证：`pnpm typecheck` 0 错、`pnpm test` 23 文件 225 用例全绿。
## 插件数据目录 fallback 由 $TMPDIR 改为持久化路径（2026-09-09）

根因：DBX 宿主拉起 sidecar 时从未注入 `DBX_PLUGIN_DATA_DIR`，插件一直走
`os.TempDir()/dbx-plugin-data/io.dbx.kafka` 兜底；macOS 的 `$TMPDIR` 在重启时
清空，prefs/presets/audit 全部丢失（ssh 插件先发现，kafka 同构）。

修复：`internal/store` 新增纯函数 `resolveDataDir(getenv, goos)`，按序取第一个
可用项：① `DBX_PLUGIN_DATA_DIR` 原样使用（宿主显式注入，未来方案 A 接入点）；
② `DBX_DATA_DIR` 非空 → `<root>/plugin-data/io.dbx.kafka`（便携/web 模式，
`plugin-data/` 避开安装器注册树）；③ 平台标准用户数据目录下
`dbx-plugin-data/io.dbx.kafka`（darwin `$HOME/Library/Application Support`、
其他 unix `${XDG_DATA_HOME:-$HOME/.local/share}`、windows `%APPDATA%`）；
④ 全缺才回落 `os.TempDir()`，永不失败。不用 `os.UserConfigDir()`（Linux 上
语义是 config 非 data），保证四插件路径一致；`Open()` 传 `os.Getenv` 与
`runtime.GOOS`。backend 中无第二处同语义目录解析（其余 TempDir 均为测试
临时文件用途）。

验证（TDD）：先写 `TestResolveDataDir` 11 个平台分支用例（fake getenv +
显式 goos，不用 `t.Setenv` 测平台分支）确认失败（`undefined: resolveDataDir`），
实现后全绿；`go test ./...`（kafkaconn/lifecycle/store）全过，`go vet`、
`go build` 通过；本机 darwin 实际解析到
`~/Library/Application Support/dbx-plugin-data/io.dbx.kafka`（0700）。改动仅
`kafka/backend/internal/store/{store.go,store_test.go}`。

## CodeEditor 暗色配色提亮 + shared/editorTheme.ts 调色板收敛（2026-09-10）

用户反馈多插件代码编辑器语法高亮"不够明显，有点暗"（暗色主题下）。

- `shared/frontend/editorTheme.ts`（新）：四插件共用调色板 `EDITOR_TOKEN_COLORS`
  （明暗两套，暗色以 GitHub Dark 系提亮，浅色 VS Code Light+ 同源）与注入式高亮
  扩展 `dbxSyntaxHighlight(scheme, runtime)`（按 scheme 记忆化；shared 零运行时
  依赖约定不变，codemirror 系对象由插件注入）。
- kafka `CodeEditor.vue`：暗色 `--cm-*` 六值替换为提亮色板（key `#4fc1ff`、
  string `#ffb86c`、number `#c3e88d`、null `#e5c07b`、punct `#dcdcdc`、prop
  `#7cc7ff`），浅色保持历史值；`lib/editorTheme.spec.ts` 以组件源码断言与
  shared 调色板一致（CSS 变量驱动无法 import 色值，属已说明的镜像 + 防漂移测试）。
- ssh/files `TextPreview.vue` 同步接入 `dbxSyntaxHighlight`（basicSetup 之后追加，
  其内置 defaultHighlightStyle 为 fallback 自动让位，暗色下语法色明显提亮）；
  两插件 `@lezer/highlight` 由传递依赖显式化为 ^1.2.3（与 kafka 同版本，无新代码）。
- 验证：kafka `pnpm typecheck` 0 错、`pnpm test` 24 文件 227 用例全绿、build 通过；
  ssh/files 合并态 typecheck + test（289/185 用例）+ build 全绿。

## 持续优化轮·1（2026-09-11：review + 流丢弃可见化 / 会话 topic 标注 + 后端空闲回收调度修复）

> 第 1 轮 review + 持续优化（cron 巡检派发）；完整报告
> `.goal-state/report-kafka-round1.md`。改动限 `kafka/` 内，零新增依赖。

- **后端 P1 修复（§5.5 契约缺口）**：`StreamRegistry.EvictIdle` 此前无生产
  调度方（`StreamEvictScanEvery` 仅测试可达），长驻 sidecar 中过期流式会话
  只累积到 StreamMaxSessions=20 上限。修复：`NewStreamRegistry` 启动
  `evictLoop`（5min 周期扫描），新增幂等 `Close()` 并接入 `CloseAll`；
  `stream/stop all:true` 语义不变。测试：`TestRegistryEvictLoop`（tick 注入
  + 退出断言）、`TestRegistryCloseIdempotent`，`-race` 全绿。
- **前端 P2 修复（§8.3 遗留「App 层缓冲丢弃为静默计数」收口）**：App.vue
  `flushStreamEvents` 补发完成后一次性提示丢弃条数；新键
  `stream.bufferDropped` ×7 语；App.spec +2（丢弃通知出现 / 无丢弃不弹）。
- **前端防误读**：StreamPanel 运行中在会话 ID 旁显示会话发起时的 topic
  （start 时定格，切走树选中不再误读会话归属；title 复用 `messages.topic`，
  零新增文案）。
- **review 核实记录**：① §9.3「StreamPanel 时区未统一」实际已由
  `workbenchTimestampTz` 收口；② i18n 全量审计（463 字面键）0 缺失；
  ③ ACL PatternType `TYPE` 维持豁免（后端 helper 不接受且 CreateAcl 语义
  非法，联动成本 > 价值）；④ P2-22 roving tabindex、MonitorPanel 隐藏采样
  维持登记。
- 验证：`pnpm typecheck` 0 错；`pnpm test` 24 文件 **229 用例**全绿（基线
  227 + 2）；`pnpm build` 通过；`go test ./...` + `go vet` 全绿。**SKIP**：
  容器 smoke（本环境无集群，协议/请求形状未动；建议发版前补跑
  `scripts/test.sh`）。浏览器级目验（mock.html `?big=1` 丢弃提示、会话
  topic chip）待人工/浏览器复核。

## 持续优化轮·2（2026-09-11：容器 smoke 补跑 + TopicsPanel/hostTheme 覆盖补测 + TopicTree 键盘化评估）

> 第 2 轮 review + 持续优化（cron 巡检派发）；完整报告
> `.goal-state/report-kafka-round2.md`。零生产代码改动，仅测试与文档。

- **容器 smoke 补跑（第 1 轮遗留 1 收口）**：`dev-cluster.sh up`（KRaft
  PLAINTEXT 9092 + redpanda SR 19081）后 `scripts/test.sh` 全套 exit 0；
  **S1-S15 PASS=11 / FAIL=0 / SKIP=4**（S3 无消费组提交条件跳过、S12 Glue
  env 门、S14 redpanda SR 不支持 FDSet、S15 OAUTH env 门，均合法）。第 1
  轮后端 evictLoop/CloseAll 关键路径 S7（stream 往返）与 S4 PASS，无回归。
  集群已 `dev-cluster.sh down` 清理。
- **TopicsPanel 覆盖 44.55% → 89.1% lines**（§10.3 遗留 2 主项）：3 例 →
  16 例，新增 describe/offsets/create/delete 确认门/config 编辑行路由/
  只读门禁/选中刷新保持与清空共 13 例，全部行为断言（invoke 参数 + 事件 +
  可见面）。
- **hostTheme 30% → 100% lines**（§10.3 遗留 2 次项）：新增
  `hostTheme.spec.ts` 7 例（类型守卫矩阵 / env detail 归一 / token 映射
  剔除规则 / 事件订阅过滤与退订）。
- **测试基建发现（P2）**：teleport stub 下弹窗重渲染会重建 DOM 子树，
  一次性抓取的 `.modal` DOMWrapper 引用变 detached 死树——旧引用上的
  disabled 断言与 click 全部落空（假阴性，产品行为实际正确）。本轮用例
  已全部改为顶层 wrapper 逐次重查（含既有 3 例扩分区用例的隐性同患）；
  其余面板 spec 的弹窗用例建议下轮统一巡检（已列入报告遗留项）。
- **TopicTree roving tabindex 评估（第 1 轮遗留 3）**：核心 script ~50 行
  虽低于 100 行线，但须变更 Tab/ARIA 交互语义 + spec ~80 行，总量超预算 →
  按边界只做方案设计（行内 roving tabindex 变体，listbox/option +
  activeKey 回落 + Arrow/Home/End + scrollIntoView，详见 round2 报告），
  留后续轮实施。其余遗留（MonitorPanel 采样、P2-10 真机、ACL TYPE、
  Phase 3）原样维持。
- 验证：`pnpm typecheck` 0 错；`pnpm test` **25 文件 249 用例**全绿（229
  基线 + 20）；coverage 全项目 lines 79.52%（前值 75.39%）；test.sh 内
  go vet/test/打包/UI walkthrough 全绿。

## 持续优化轮·3（2026-09-11：TopicTree roving tabindex 实施 + stale 引用巡检 + 收敛判定）

> 第 3 轮 review + 持续优化（cron 巡检派发，收敛评估轮）；完整报告
> `.goal-state/report-kafka-round3.md`。

- **TopicTree roving tabindex / listbox 化（round2 遗留 1 落地）**：按
  round2 评审方案实施——`.tree-node` 加 `role="listbox"` + aria-label
  （复用 `tree.title`，零新增 i18n 键），行 button 加 `role="option"` +
  `aria-selected` + roving tabindex（仅 active 行 0，其余 -1，big 模式
  507 行收敛为单 tab stop）；新增 `activeKey`（初始 selectedTopic ?? 首行，
  watch selectedTopic 同步 / watch visible 失联回落）；ArrowDown/Up/
  Home/End 移动 active + emit select + 焦点/scrollIntoView 跟随，边界不
  重复 emit。鼠标点击路径与视觉样式零改动（焦点描边走既有全局
  `button:focus-visible`）。**Tab 语义变更待真机/读屏确认后关闭 P2-22**。
- **弹窗 spec stale 引用巡检（round2 遗留 2 收口）**：GroupsPanel 1 例
  「捕获 `.modal` 后跨 setValue/click 复用旧引用」实证为 detached 死树
  （`isConnected=false`），且 stale click 可经残留监听触发 handler
  （幽灵路径，假阳性风险）——已改 `modal = () =>` 逐次重查，断言不变。
  其余面板 spec 一次性捕获均为「捕获后仅读、无中间交互」安全形态，维持
  不动；顺带删除 SchemasPanel 一处遗留调试 `console.log`。
- **复核结论（第 1 轮 evictLoop + 第 2 轮补测面）**：锁序单向无死锁面、
  Close 幂等/nil 安全有覆盖，round2 真实 broker smoke 仍有效——无新
  P1/P2。两条观察登记不实施：① 空闲按「消息活动」计（空 topic 挂机
  30min 会被回收，§5.5 字面实现）；② evict 回收无前端事件通知。
- 验证：`pnpm typecheck` 0 错；`pnpm test` **25 文件 255 用例**全绿（249
  基线 + 6 键盘导航例，含 507 节点单 tab stop 防回归断言）；`pnpm build`
  通过。**SKIP**：容器 smoke（本轮生产改动仅前端键盘导航、Go 零改动，
  round2 结论仍有效）；浏览器/读屏目验（待真机）。
- **收敛判定：无剩余可执行项（仅剩人工/真机复核项与维持豁免项）**——
  MonitorPanel 采样降频、P2-10 真机复核、ACL TYPE 豁免、Phase 3 登记、
  S3 常绿化均维持原状；插件进入收敛状态。

## 持续优化轮·4（2026-09-12：fresh review 未扫面五轴 + 授权错误映射 + 状态一致性收敛）

> 第 4 轮 review + 持续优化（换视角轮，不复现 round1-3 已修项）；完整报告
> `.goal-state/report-kafka-round4.md`。

- **五个未扫面结论**：①状态一致性——TopicsPanel 管理表空态/加载态/错误态
  混淆（P2 已修：新增 `error` prop + 三态收敛 error→loading→empty，App
  下发 `topicsError`），MessagesPanel 消费在途整块空白（P2 已修：在途态
  显示 `messages.running`）；②错误可操作性——`friendlyKafkaError` 缺授权
  类映射（P1 已修：新增 `err.forbidden` 规则 + 七语文案，覆盖 franz-go
  TOPIC/GROUP/CLUSTER_AUTHORIZATION_FAILED 原文案，SASL/x509 不误吞）；
  ③大数据边界无发现（capRows 10000 + 预览 120 字符 + CodeMirror 虚拟渲染
  + quickFilter 防抖均在案）；④i18n 七语占位符脚本审计 issues 0；⑤mock↔
  真实桥契约 41 方法 + 3 事件逐条对照无发现。
- 验证：`pnpm typecheck` 0 错；`pnpm test` **25 文件 260 用例**全绿（255
  基线 + 5）；`pnpm build` 通过。SKIP：容器 smoke（仅前端文案/渲染改动，
  Go 与协议零改动）。观察登记：showFullBase64 裸 pre 无上限、
  truncatedValuePreview 生产死代码（有护栏）。
- 收敛判定不变：无剩余 subagent 可执行项，仅剩真机复核/维持豁免项。


## M3 MCP 工具面（2026-09-12：io.dbx.kafka MCP 全栈落地）

> 设计来源 `shared/IMPL_PLAN_PLUGIN_MCP.zh-CN.md`（v2）§1–§4/§6.3；形状
> 对齐 ldap M1 Go 版（`ldap/backend/internal/mcp/`）与 files M2 Rust 版。
> 本轮落在 `kafka/backend/**`（internal/mcp + kafkaconn 最小增量 + main 接线）、
> `kafka/frontend/**`（useUiIntent 接线）、`kafka/scripts/smoke_mcp.py`、
> `kafka/docs/MCP.zh-CN.md`（新增）+ `PROTOCOL_KAFKA.zh-CN.md`（§6.4 事件、
> §3.10 方法、审计 source 标注）。

### 后端（internal/mcp，11 工具）

- 骨架：`mcp/tools`（11 工具 JSON Schema）、`mcp/call`（分派 + MCP content
  信封）、`mcp/settings/get|set`（8 字段白名单持久化，`digestScanLimit` 为
  kafka 域内扩展）。UI intent 4 工具（`kafka_ui_focus/search/select/state`）
  + 元发现 `kafka_ui_topics`（硬上限 50）。
- 本地读：`kafka_messages_digest`（一次性 Consume 复用 `maxScanRecords`
  扫描语义与 filter 全通道，本地聚合 per-partition 计数 / key groupBy ≤20 /
  时间直方图 ≤12 桶 / `fields` JSON-path 投影 distinct+topN ≤10；默认
  digest，`rows` clamp ≤20；超 512KB value 出占位符 + partition/offset
  定位指引，正文不出 sidecar；MCP 不订阅 stream）+ `kafka_cursor_next`
  （TTL 10 分钟 / LRU ≤8 / 物化 ≤1 万行）。
- 写：`kafka_messages_produce` 单阶段直执行（MCP 载荷 ≤64 KiB）；
  `kafka_topics_delete`、`kafka_groups_offsets_reset`、
  `kafka_topics_records_clear` 强制两阶段（preview + 一次性 confirmToken，
  60s TTL、参数 hash 绑定；执行层内部仍带同名 confirmTopic 满足
  kafkaconn 防误删门禁）。只读连接 4 写工具全剔除、`allow_delete=false`
  追加剔除删除类两工具（`omittedWriteTools` 附原因，`mcp/call` 侧
  `PolicyOf` 纵深防御）。
- kafkaconn 最小增量（additive）：`AuditRecord`/`store.AuditRecord` 增
  `source` 字段；`ProduceRequest`/`TopicsDeleteRequest`/
  `GroupOffsetResetRequest`/`TopicRecordsClearRequest` 增 `Source`（MCP
  写路径 `"mcp"`，对应 emitAudit 调用点改 `emitAuditSource`）；
  `Service.PolicyOf`、`StreamRegistry.StatusesFor`（kafka_ui_state 快照
  附带 stream 状态段）。
- 单测（对照 shared/frontend/README「MCP 验收用例清单」编号）：S-SET ×4、
  S-INT ×5、S-CUR ×5、S-CONF ×3、S-DIG kafka 变体 ×8（per-partition、
  key groupBy 截断、直方图桶、字段投影 distinct/topN、非 JSON 跳过、
  单元格截断与定位字段保真、大 value 占位、sample/rows clamp）、S-SRV ×8
  （settings 往返、16KiB 截断序、intent/快照/streams、cursor 错误、定位
  参数校验、只读与 allow_delete 剔除矩阵、两阶段 preview/hash 绑定/一次性）。
  `go vet` 0 告警，`go test ./...` 全绿。

### 前端（照 ldap 样板接线）

- `App.vue` 挂 `useUiIntent("kafka", handlers)`：focus 切 messages/topics/
  groups/schemas 面板；search → `MessagesPanel.applyIntentConsume`（表单
  填入 + 触发消费，回报 summary 含 count + 前 5 行 + `topic-p{o}` 锚点）；
  select → `applyIntentSelect`（partition+offset 定位 + 详情抽屉）。
  面板切换 / topic 选中后 `reportSnapshot` 上报快照；卸载 stop()。
- `MessagesPanel.vue`：`buildParams(topicOverride)` 时序兜底、
  `applyIntentConsume/applyIntentSelect` 经 defineExpose 暴露（复用既有
  校验/竞态守卫/capRows 落地路径）。
- `mockDbxHost.ts` 镜像新事件/方法（`kafka/ui/intent` 经
  `emitKafkaUiIntent` 注入；`kafka/ui/state/report` 校验同 sidecar）；
  `env.d.ts` 同步 `DbxPluginUiIntentEvent`/`DbxPluginUiStateReport`。
- 七语文案：`intent.*` 6 键 × 7 语言（en/zh-CN/zh-TW/es/it/ja/pt-BR）。
- 新增用例：`lib/uiIntent.spec.ts`（5）、`App.spec.ts` MCP 接线（2）、
  `MessagesPanel.spec.ts` intent 集成（3）；`pnpm typecheck` 0 错，
  `pnpm test` **271 用例全绿**。

### smoke 与验证证据

- `scripts/smoke_mcp.py`（K1–K11）：**dev 测试集群在跑时 11/11 PASS**——
  K11 真实集群覆盖 digest 聚合（perPartition/keys/timeHistogram/fields
  投影）、cursor 翻页定位字段行、单阶段 produce、topics/delete 与
  groups/offsets/reset 与 topics/records/clear 三个两阶段全流程（含参数
  改动作废、token 复用 unknown）、audit.jsonl 四动作 `source:"mcp"` 且
  工作台路径（topics/create）不带 source。无容器时 K11 SKIP、其余 10 离线
  场景全 PASS。
- 复跑：`go vet ./... && go test ./...`（backend）、`pnpm typecheck && 
  pnpm test`（frontend）全绿。
- 遗留/风险：`scripts/test.sh` 未追加 smoke_mcp 调用（与 ldap 保持同构，
  手动跑）；digest 扫描依赖一次性 Consume 超时窗（timeoutMs 缺省 5s，超大
  扫描量场景建议在 `maxScanRecords` 内收敛）；ui_test.mjs 未覆盖 intent
  真机流（与 ldap 同待 host-e2e）。

### 补充（2026-09-12 晚）：真机 host-e2e 验收

- smoke_mcp 纳入 `scripts/test.sh`（smoke 段后自动重建 sidecar 并运行，
  dev 集群用例自动 SKIP）；实测 10 PASS + 1 SKIP（无集群）。
- `ui_test.mjs` 新增 intent 走查用例（3/3 全绿）：mockDbxHost 暴露
  `emitKafkaUiIntent`，用例验证 emit → MessagesPanel consume 表单 →
  `kafka/ui/state/report` applied 全链（topic=codec-lab, earliest）。
- 重新打包 v0.1.29 装入隔离 app-data；插件中心显示 DBX Kafka v0.1.29
  兼容。桌面 HTTP MCP 工具面验证待服务开启，
  见 shared/PROGRESS-HOST-SUBREPO.zh-CN.md §27。

### 补充（2026-09-12）：standalone `--mcp` stdio 模式（设计 §0.2/§5）

真机验证确认独立 stdio 是插件 MCP 工具被 AI 客户端调用的现实暴露路径
（ssh 基线同款；ldap Go 版先行，kafka 同构移植）：

- **入口与互斥**：`main.go` 新增 `--mcp` 标志分发（`mcpStdioRequested`
  精确匹配）→ `runMcpStdio` 进入 stdio MCP 服务器模式，不装配
  dbxpluginsdk.Server/Emitter/流式会话（同一进程只跑插件协议或 stdio MCP
  其一；MCP 仍不订阅 stream）。
- **`internal/mcp/stdio.go`**（ssh `run_mcp_stdio` 的 Go 移植，与 ldap
  同构）：MCP 2024-11-05 换行分隔 JSON-RPC；initialize
  （serverInfo.name=io.dbx.kafka）、notifications/initialized（不回包）、
  tools/list、tools/call、ping；未知方法 -32601、坏行 -32700 不崩；每请求
  一个 goroutine（慢 digest 不阻塞 ping/tools/list），stdin EOF 后 drain
  ≤300s。
- **tools/list 复用注册表**：11 工具照常列出；连接类工具
  （messages_digest/messages_produce/topics_delete/groups_offsets_reset/
  topics_records_clear）inputSchema 显式补内联连接参数（严格 MCP 宿主会
  丢未声明参数，ssh 同因），required 的 connectionId 放宽为 anyOf
  （connectionId ∥ 内联 brokers）；UI 工具 schema 不动。
- **UI 工具 UNAVAILABLE**：`kafka_ui_*` 5 个（含元发现 kafka_ui_topics）
  tools/call 一律 isError content「UNAVAILABLE: 此工具需要 DBX 工作台
  （工作台模式可用）…」，不假死。
- **内联凭据连接**：camelCase 字段（brokers 数组/逗号换行分隔/
  securityProtocol/saslMechanism/saslUsername/saslPassword/tlsCaCert/
  tlsClientCert/tlsClientKey/tlsInsecureSkipVerify/schemaRegistry/
  schemaRegistryUrl/schemaRegistryUsername/schemaRegistryPassword/clientId/
  readOnly/allowDelete）→ 归一结构体 canonical JSON sha256 池化
  （`mcp-<hash>`，上限 8 FIFO 淘汰即 svc.Disconnect）；readOnly/allowDelete
  按表单默认解析（缺省 true/false，显式同值同池键）；toLifecycle 折算标准
  lifecycle params 走 `svc.Connect` 同一条路径（admin client 指纹失效重建
  语义保持）；凭据不落盘不持久化。本轮不做宿主 TCP 桥接兜底（未知
  connectionId 报引导错误，见 docs/MCP.zh-CN.md「桥接兜底」）。
- **工具语义零复制**：stdio tools/call 注入池化 connectionId 后直接走
  `Server.Call`（digest maxScanRecords/cursor/produce 单阶段/三写两阶段
  confirmToken/审计 source=mcp 全部复用）；写审计经 runMcpStdio 注入回调
  落 audit.jsonl（无 Emitter）。
- 单测：`internal/mcp/stdio_test.go`（S-STDIO-1..10：协议循环/UNAVAILABLE/
  池化键+表单默认归一/brokers 解析+toLifecycle/池淘汰/连接门/Serve 端到端）
  + `main_test.go`（--mcp 分发互斥）。
- smoke：`scripts/smoke_mcp.py` 新增 K12（离线 stdio：initialize/tools-list
  11 工具+内联 schema/ui UNAVAILABLE/连接门/未知方法）与 K13（容器：stdio
  内联凭据 produce×2 → digest matched=2 → cursor 翻页 → 两阶段 delete 真删
  → token 一次性 → audit source=mcp；scratch topic 经工作台协议预建——
  stdio 工具面无 topic/create）。
- 文档：`docs/MCP.zh-CN.md` 新增「方式二：独立 stdio 模式」章节（用法/
  凭据参数表/stdio 未覆盖字段/语义差异/桥接兜底未做/ZCode 接入），
  降级矩阵补 stdio 行，smoke 段更新 K1–K13。

**验证（真实输出）**：`go vet ./...` 通过；`go test ./...` 全绿（mcp 包
+10 用例）；dev 测试集群在跑时 `python3 scripts/smoke_mcp.py`
**13/13 PASS**（K13 stdio produce=2 → digest matched=2 → 两阶段 delete
真删 + token 单次 + audit source=mcp）。未跑 test.sh 打包段（无 manifest/
构建链变更，打包不受影响）。未尽：stdio 桥接兜底（connectionId 转发
DBX app）、ZK 源/Kerberos/OAUTHBEARER/Glue 族表单字段的内联支持；未执行
任何 git 提交。

## MCP 测试覆盖专项（2026-09-13：AI agent 调用易用性/准确性/容错性审计与修复）

对本轮 M3 MCP 工具面（11 工具）做六维覆盖审计（参数校验/错误消息质量/
成功路径/降级路径/两阶段确认/文档一致性），以"补测试 → 跑出缺口 → 修实现
→ 立即回归"循环收敛。digest clamp 全链核对结论：样本 ≤5 / rows ≤20 /
单元格 120 字符 / 响应 16 KiB / cursor 1 万条·TTL 10min·LRU ≤8 / 直方图
≤12 桶 / groupBy ≤20 组 / topN ≤10 全部与文档一致（settings.Sanitized
双保险），未动。两阶段清单（topics/delete、groups/offsets/reset、
topics/records/clear）与 confirmToken 一次性/60s/hash 绑定均验证无缺口。

**修复的问题（现象 → 根因 → 修复）**：

1. **字符串数字静默改语义（准确性，最重要）**：LLM 常把整数写成字符串。
   `partition:"0"` 在 produce 里被静默忽略 → 消息落错分区；`partitions:
   ["0","1"]` 在 digest 里被静默丢弃 → 变成全分区扫描；`timestampMs:"17e11"`
   在 offsets reset 里被 `int64(intArg())` 折算为 **0** → 等价重置到纪元；
   `n:"5"` 静默变 20；`kafka_ui_select` 的字符串数字被误导性报错
   "partition is required"（前端 `Number.parseInt(String(...))` 本可接受）。
   修复：`util.go` 新增 `coerceInt/coerceInt64/coerceBool` 宽容解析；
   `parseIntList`（JSON number 数组/整数字符串数组/逗号分隔串三形态）替代
   静默丢弃的 `intSlice`；存在但非法一律带原值报错，绝不静默折算后执行。
   `uiSearch.partitions` 原走 `stringSlice`（数字数组反而全被丢弃），同修。
2. **digest 对不存在 topic 静默 matched=0（准确性/易用性）**：franz-go
   消费不存在的 topic 返回空结果，与空 topic 无差别，AI 会把"名字打错"误读
   成"没有数据"。修复：扫描为 0 时自动补 `DescribeTopic` 存在性校验
   （kadm 层报 `UNKNOWN_TOPIC_OR_PARTITION`，与 kafkaconn `topic %q not
   found` 两种文本都匹配；非空路径零额外成本，校验自身网络失败不掩盖结果），
   报错附 `kafka_ui_topics` 定位指引。真机探针确认修复前后行为。
3. **offsets reset 二阶段才失败白烧令牌（易用性/容错性）**：resetTo 配套
   参数（topics/timestampMs/partitionOffsets）原在执行层（确认之后）才被
   kafkaconn 校验——预览签发令牌 → 确认烧掉令牌 → 才报"topics is required"。
   修复：新增 `validateResetRequest` 预检（归一规则与 kafkaconn
   `normalizeResetMode` 同源），预览前拒绝且不签发令牌；`timestampMs<=0`
   明确拒绝（0 等价重置到纪元，几乎必是参数缺失产物）。
4. **stdio 字符串布尔静默回落表单默认（容错性）**：`readOnly:"false"` /
   `allowDelete:"true"` 字符串被忽略 → 写连接静默降级只读（或反之），报错
   与原因脱节。修复：`parseInlineConn` 接受 `"true"/"false"/"1"/"0"`
   （大小写不敏感），其余值报错（签名加 error 返回）。
5. **可行动错误消息（易用性）**：未知连接 digest 报错附 "verify the
   connectionId (or pass inline connection parameters in standalone stdio
   mode)"；topic 疑似不存在附 `kafka_ui_topics` 指引（`annotateReadError`
   + `topicNotFoundish` 单点判定）；`format` 报错回显原值；produce 超限报错
   带实际字节数（`65537 > 65536`）；`kafka_ui_focus` panel、digest offset
   时间窗等枚举/字段在 sidecar 侧提前校验（枚举大小写归一：format/panel/
   offsetStrategy/matchMode/resetTo 统一 lower）。
6. **测试环境封闭性**：`TestServerTwoPhasePreviewAndConsume` 隐式依赖
   "本机 9092 无 broker"（dev 集群拉起后该测试真的对集群执行了 delete；
   幸而 `orders` topic 不存在无副作用）。修复：`connect2At` 显式 bootstrap，
   执行类断言指向 `127.0.0.1:1`；`TestServerProducePartitionVariants` 借
   "超预算值在解析后、拨号前被拦"的顺序做封闭断言（不付 30s 拨号超时）。
7. 杂项：`confirm.go` 头注释残留 ldap 语言（delete recursive/modifyDn）
   改为 kafka 三写工具；tools.go/stdioConnectionProperties 的 schema 描述
   同步容错语义（numeric strings tolerated / string booleans accepted /
   resetTo 各模式配套要求）。

**新增覆盖**：Go 单测 `tolerance_test.go` 9 个（coerce 变体/定位字符串
数字/digest 参数变体含 partitions 报错与时间窗校验/cursor 字符串 n/panel
枚举/reset 预检+字符串 timestampMs 折算进 canonical/produce partition
变体/读错误指引/超预算报错带字节数），mcp 包 45→54 个；smoke 新增断言：
K5 未知面板报错、K7 字符串定位、K8 非法 partitions/cursor 字符串 n/未知
连接 hint/format 回显、K10 reset 四模式预检、K11 字符串 partition 钉分区
（produce partition="1" → 落分区 1）+ 不存在 topic 指引 + cursor 字符串 n
+ reset 字符串 timestampMs 折算、K12 字符串布尔报错。文档
`docs/MCP.zh-CN.md` 新增「容错语义」小节 + digest/reset 工具行更新 +
smoke 段更新。

**验证（真实输出）**：`go vet ./...` 通过；`go test ./...` 全绿（mcp 包
`-count=1` 54/54 PASS）；`CGO_ENABLED=0 go build -trimpath -o
bin/dbx-plugin-kafka .` 通过；dev 集群（`scripts/dev-cluster.sh up`）
在跑时 `DBX_PLUGIN_SIDECAR=… python3 scripts/smoke_mcp.py` **13/13 PASS
（FAIL=0 SKIP=0）**，其中 K11/K13 为容器场景。未注册方法/工具路径保持
SKIP 语义（smoke 框架 `is_method_not_registered`，本轮未触发）。

**剩余风险**：① 三插件同构观察——ldap/files 的 digest/cursor 尚未对齐
本轮 kafka 的宽容解析与预检语义（shared/ 只读未动，建议后续在公共验收
清单单点收敛）；② `annotateReadError`/`topicNotFoundish` 依赖上游错误
文本匹配（已单点化 + 宽松匹配，franz-go 大版本升级时需复核）；③ stdio
桥接兜底（未知 connectionId 转发 DBX app）仍为后续项；④ 未执行任何 git
提交（工作区含他人 WIP，未回退未触碰）。

## MCP 测试覆盖专项·第二轮（2026-09-13：真集群深水区实测 + 消费池复用 P0 修复）

基于第一轮工作树继续，目标从"参数容错/预检"推进到"真集群行为 + 聚合边界
+ 会话容量 + 边界矩阵"，六项任务全部落地。**本轮最重要的产出是任务外发现的
P0 准确性 bug**：消费池复用路径的 seek 失效——同一 topic 第二次
earliest digest 恒静默返回 0 条（详见下文问题 1）。

**新增真集群覆盖（smoke K14/K15，均需容器，K14 另需 Schema Registry）**：

- **K14 schema 解码投影**：SR 注册 JSON schema → MCP produce 挂载编码
  wire format 入集群 → digest 断言六段：① 无 schema 走 raw 通道（wire
  字节原样、matched 照常计数）；② 挂载解码后 `fields` JSON path 投影
  distinct/topN 命中真实值（`$.user.id` valueCount=2、top u0=3）且样本
  value 是解码 JSON、带 `schemaId`/`schemaSubject`/`schemaVersion`；
  ③ 投影字段全不命中（raw 字节非 JSON）→ `fieldsNote` 指引；④ 坏
  schema 版本（version=999）→ `decodeFailures` 计数 + `decodeNote`
  指引 + 样本行 `decodeError`（含 HTTP 404）；⑤ 未配置 SR 的连接挂
  schema → 门禁显式报错；⑥ 投影部分命中时无 fieldsNote。
- **K15 聚合边界 + reset 落点 + 边界矩阵**：4 分区 45 条真实多 key 数据
  断言 keys 45→20 且 `keysLimit:true`、topN 45→10 且 `truncated:true`、
  直方图 ≤12 桶、perPartition 求和=total、cursor 不截断；cursor 以缺省
  n=20 连续翻页到 `done`（45=20+20+5，`nextOffset`=总数）；**reset
  timestampMs 真实落点**——两批消息 + 分界时间戳 cut_ms → 两阶段 reset
  → `kafka/groups/offsets/list` 核验 committedOffset 精确落在 batch1
  条数（与 broker ListOffsetsAfterMilli 语义一致；字符串 timestampMs
  宽容折算同验）；produce partition=99 与不存在 topic 的错误文本断言。

**发现并修复的问题（现象 → 根因 → 修复 → 验证）**：

1. **消费池复用 seek 失效（P0，准确性）**：同连接同 signature 的第二次
   earliest `kafka_messages_digest` 恒 `matched=0` 且无任何错误——AI 会把
   "第二次扫描"误读成"数据没了"（第一轮加的存在性校验还因 topic 存在而
   不报错，完全静默）。根因：`resetConsumeClientForReuse` 用哨兵值
   （-2=start）**只 seek `seenParts` 里"有记录的分区"**；franz-go v1.20.7
   直接消费下 `SetOffsets` 的 map 未覆盖全部在消费分区时**整个 seek 静默
   丢失**（真集群最小复现：全分区 seek → 重扫成功；只 seek p0 → 0 记录
   0 错误）。修复：reset 改为经 kadm `ListStartOffsets/ListEndOffsets`
   拉 topic **全部分区**的具体边界 offset（含 leader epoch）后一次性
   `SetOffsets`；一次 broker 往返换正确性，池化提速语义保留。验证：真机
   连续 4 次 earliest digest 均 matched=6（修复前 #2 起全 0）；latest
   复用 reset 到新 end（新消息被正确跳过，语义与新建 client 一致）。
2. **digest 无法挂载 schema（功能缺口）**：工具描述与 digest.go 注释承诺
   "schema 解码后投影"，但 `kafka_messages_digest` 没有 `schema` 参数、
   `messagesDigest` 也不解析——wire format 消息的投影全部静默 0，传入的
   schema 参数被静默丢弃。修复：新增 `parseSchemaRef`（produce/digest
   共形；digest 允许缺 subject 按 wire id 查 SR，version 宽容字符串数字、
   非法/负数报错——静默折 0 会把"版本打错"变"取最新"），digest 接入
   `ConsumeParams.Schema`；样本行带 `schemaId`/`schemaSubject`/
   `schemaVersion` 与 `decodeError`（独立 200 字符截断上限——SR 错误的
   subject 路径 + HTTP 状态码在 120 单元格宽度下常被截掉）；聚合层带
   `decodeFailures`/`decodeNote`。真机探针前后对照确认。
3. **投影零命中静默（易用性）**：`fields` 全不命中时 valueCount=0 无任何
   提示，AI 无法区分"路径错"与"载荷非 JSON"。修复：matched>0 且全部请求
   字段 0 命中时带 `fieldsNote`（提示两者都查；部分命中不提示）。
4. **produce headers 非法形状静默丢弃（容错红线）**：数组/标量形状的
   `headers` 被类型断言静默忽略，"想带 header"的消息无 header 落盘；嵌套
   值被 `fmt.Sprintf` 折成 `[a b c]` 乱码。修复：headers 存在但非 object
   或值非标量（string/number/boolean/null）显式报错并回显原值。
5. **produce schema.version 静默折 0**：沿用 `intArg`（非法→0=取最新），
   与第一轮"绝不静默折算"原则冲突。修复：统一走 `parseSchemaRef` 报错。
6. **cursor TTL/容量硬编码（同族漂移）**：files 的 `cursorTtlSecs`/
   `maxCursorSessions` 是可调 settings 且过期报文携带实际生效 TTL；
   kafka 硬编码 "10 minutes"。修复：settings 新增 `cursorTtlSecs`
   （600，10–3600）/`maxCursorSessions`（8，1–32），`SettingsSet` 同步
   运行中 store（新 `SetTTL`/`SetCapacity`，容量 set 后立即收敛），过期
   报文改为 `cursor expired (TTL <n>s)`。
7. **同族一致性对齐 ssh 基线（任务 6）**：① 字符串布尔变体面——ssh
   `arg_bool` 接受 `yes/no/on/off`，kafka `coerceBool` 原只有
   `true/false/1/0`，补齐（stdio 内联 readOnly/allowDelete 同步，非法值
   仍报错）；② 未注册工具名——ssh 有 `Unknown tool` + `Did you mean`
   建议，kafka 原只有裸文本，补 `unknownToolMessage`（分隔符/大小写
   compact 变体建议注册名 + 指向 `mcp/tools` 与工具数）。
8. **produce 不存在 topic 无定位指引**：读路径第一轮已加 `kafka_ui_topics`
   指引，写路径没有。修复：`annotateReadError` 更名 `annotateClusterError`
   （读写共用），produce 错误同样标注。

**别家形状漂移（只读对照 ssh/ldap/files MCP 文档，未动别家）**：ldap/
files 的 digest/cursor/两阶段/settings 形状与 kafka 同构（16 KiB 响应、
120 字符 cell、≤20 rows、confirmToken 一次性/60s/hash 绑定）；ldap
`ldap_search_digest` 尚无 kafka 本轮的 schema/投影零命中提示等价物
（其 distinctAttr 语义本身闭环，无静默缺口）；files/ldap stdio 均已有
桥接兜底，kafka 仍为后续项（见第一轮剩余风险 ③）。

**新增覆盖**：Go 单测 `boundary_test.go`（MCP 第二轮：parseSchemaRef
变体/digest schema 参数校验/decodeFailures+fieldsNote 纯函数/cursor
TTL 与 LRU 的 Server 层注入时钟报文/produce 边界矩阵/unknown tool 建议/
coerceBool 宽变体）9 个 + 既有用例更新（tolerance_test 的 annotate
改名与 yes 变体、stdio_test 的布尔变体、cursor TTL 报文随 settings），
mcp 包 54→**63** 个全绿；smoke K14/K15 新增（13→15 场景）+ K12 字符串
布尔宽变体断言更新。文档 `docs/MCP.zh-CN.md` 同步：digest 工具行
（schema 挂载/decodeFailures/fieldsNote）、cursor 行（TTL/LRU 可调）、
produce 行（headers/schema 形状）、容错语义小节（字符串布尔变体、
schema 参数、unknown tool 建议）、settings 表两行、smoke 段 K14/K15。

**验证（真实输出）**：`go vet ./...` 通过；`go test ./... -count=1` 全绿
（mcp 包 63/63 PASS）；`CGO_ENABLED=0 go build -trimpath -o
bin/dbx-plugin-kafka .` 通过；dev 集群在跑时 `DBX_PLUGIN_SIDECAR=…
python3 scripts/smoke_mcp.py` **15/15 PASS（FAIL=0 SKIP=0）**。测试探针
与遗留数据已清理（/tmp 探针目录删除；dev 集群 probe/smoke 一次性 topic
与 SR 测试 subject 删除，仅剩 seed 基础 topic：dbx-smoke-events/binary/
export/filter/stream）。集群按脚本设计保留运行（127.0.0.1:9092 +
Schema Registry :19081，docker Up 9h+）。

**剩余风险**：① franz-go 升级时 reset 的 kadm 全分区 seek 语义需复核
（本轮实测钉死 v1.20.7 行为；上游 CHANGELOG 提及 SetOffsets 部分分区
语义变更史）；② cursor TTL 调整只影响新物化会话（既有会话 ExpiresAt
已物化，文档未另行承诺）；③ stdio 宿主 TCP 桥接兜底仍为后续项；
④ K14 依赖 SR 的 `/subjects` 探测，Redpanda/AWS Glue 兼容实现上
Glue 走工作台表单（与既有 schemaMountSupported 门禁一致）；⑤ 未执行
任何 git 提交（工作区含他人 WIP，未回退未触碰）。

## MCP 测试覆盖专项·第三轮（2026-09-13：stdio 桥接兜底收敛，对齐 ssh/ldap 同构）

**主题**：把 stdio `--mcp` 模式「未知 connectionId → DBX 本地桥转发」兜底
补齐到 ssh/ldap 同构水平（消第二轮剩余风险 ③）。ldap
`appbridge.go`/`appbridge_test.go`（wave 2 刚落地）的 kafka 同构移植，
源头是 ssh `backend/src/app_bridge.rs`（只读参照）。

### 交付

1. **`backend/internal/mcp/appbridge.go`（新增）**：DBX 桌面应用本地 TCP
   桥客户端——端口发现（`<app_data_dir>/mcp-bridge-port`，
   `DBX_APP_DATA_DIR` 覆盖 / macOS 缺省）、TCP 探测防陈旧端口、
   `DBX_APP_LAUNCH_CMD` 尽力拉起、500ms 轮询 ensure 30s 预算、
   `POST /call-plugin-tool` snake_case 五字段契约
   （`plugin_id:"io.dbx.kafka"`）、200 envelope 逐字透传、64 KiB 单写上限、
   超时随转发 timeout + 150s 审批读余量、fail-closed 错误统一
   `DBX app bridge` 前缀。与 ldap 逐行为同构，仅 pluginID 与注释域措辞
   kafka 化。
2. **`backend/internal/mcp/stdio.go`**：`resolveConnection` →
   `resolveConnectionOrForward`（优先级 ldap 同构：内联凭据 > 已池化 id >
   未池化 id 桥转发 > fail-closed 合并错误）；新增 `forwardViaBridge`
   （timeoutSecs clamp 5–300 缺省 300，整数字符串宽容折算走既有
   `intArg`；envelope 逐字 / 非 envelope 防御性包装）；`StdioServer` 增
   `bridgeEnsureWait`（缺省 `DefaultBridgeEnsureWait` 30s）；未池化
   connectionId 错误改为桥失败原因 + 内联凭据出路（brokers/securityProtocol/
   sasl*/schemaRegistry*）合并文案；connectionId schema 描述同步（pooled
   or DBX saved connection id）。kafka 特化差异仅一处：内联凭据出路文案
   用 kafka 连接参数族（ldap 为 host/tlsMode/bindDn 族）。
3. **测试**：`backend/internal/mcp/appbridge_test.go`（新增 9 用例，对齐
   ldap appbridge_test 用例集：端口解析/垃圾文件拒绝/五字段契约/ensure
   fail-closed 预算/端口发布拾取/mock 桥转发契约+envelope/非 envelope
   包装/会话类工具不触桥；转发集成用例 envelope 形状与 ldap 同表）； 
   `stdio_test.go` S-STDIO-5 连接门用例更新（无桥 fail-closed 断言 + 
   `resolveConnectionOrForward` 直通断言）。mcp 包 63→**72** 用例全绿。
4. **smoke**：`scripts/smoke_mcp.py` 新增 **K16** stdio 桥接兜底场景
   （离线段：空 app-data + no-op launch → 30s ensure 预算跑满 → fail-closed
   可行动错误含 `mcp-nope` 回显；mock 段：本地 MockBridge 转发契约
   `/call-plugin-tool` + 五字段 + `plugin_id=io.dbx.kafka` + envelope 逐字
   + timeoutSecs:"30"→timeout_ms==30000；404 表面化段）；`McpStdioClient.start`
   增 `extra_env`（ldap 同款）；K12 摘除旧「no DBX bridge fallback」断言
   （对齐 ldap M11/M14 分工，未知 connectionId 归 K16 专场景）。15→**16**
   场景，SKIP 语义不变（未注册方法仍 SKIP）。
5. **顺手核对（任务 6，只查未改）**：cursor 过期报文已带实际生效 TTL——
   `server.go` cursorNext 的 `LookupExpired` 分支取 `settings.CursorTtlSecs`
   实际值（第二轮已修），`boundary_test.go` `TestServerCursorTTLExpiryMessage`
   覆盖缺省 600s 与 settings 调整后 30s 两种报文；无漏网。
6. **文档**：`docs/MCP.zh-CN.md`「桥接兜底」节由「本轮未做」改写为落地
   描述（对照 ldap/docs/MCP.zh-CN.md 写法：转发决策/发现与契约/fail-closed/
   两阶段透传/测试）、内联凭据节补「保存连接 id 走桥接兜底」、降级矩阵
   stdio 行补桥接兜底、smoke 段 K1–K15→K1–K16。

### 验证（真实输出）

- `cd backend && go vet ./...` 通过。
- `go test ./... -count=1` 全绿：`io.dbx.kafka.plugin` /
  `internal/kafkaconn` / `internal/lifecycle` / `internal/mcp`（72 用例，
  含 Bridge 9 用例）/ `internal/store` 全 ok。
- `CGO_ENABLED=0 go build -trimpath -o bin/dbx-plugin-kafka .` 通过。
- dev 集群在跑（dbx-kafka-test 127.0.0.1:9092 + SR :19081）时
  `DBX_PLUGIN_SIDECAR=$PWD/backend/bin/dbx-plugin-kafka python3
  scripts/smoke_mcp.py`：**total=16 PASS=16 FAIL=0 SKIP=0**（K1–K16 全
  PASS，K16 detail「fail-closed without the app; mock-bridge forward
  contract + envelope verbatim; 404 surfaced」）。

### 剩余风险

① 真实 DBX.app 桥回环未在本轮验证（本机无运行中的 DBX.app；契约形状由
mock 桥按宿主 `/call-plugin-tool` 契约钉死，与 ldap wave 2 同一契约表，
宿主契约若有异动按 AGENTS.md 规则 7 走 `shared/frontend`/桥客户端单点
跟进）；② 桥转发路径的 kafka 工具参数不含 `timeoutSecs` 声明（转发的
timeoutSecs 仅客户端折算用，会随 arguments 透传给应用侧；应用侧 mcp/call
按键取值、未知键无害，与 ldap 传法一致）；③ 未执行任何 git 提交。

## 第四轮（2026-09-13）桥回环：真机 DBX.app 端到端验证

（接上节遗留项①）本机隔离 app-data（`shared/host-e2e/app-data`）+
测试 DBX.app（host debug bundle，经 launch.sh 注入 `DBX_DATA_DIR`），
kafka 0.1.35 随四插件装入同一 app-data；桥端口发布后 TCP 探测通过。

- **转发契约（核心验收）**：standalone `dbx-plugin-kafka --mcp`
  （`DBX_APP_DATA_DIR` 指向隔离 app-data）`tools/call
  kafka_messages_digest {connectionId:"no-such-kafka-conn",
  topic:"probe"}` → 未池化 connectionId 经桥转发 → 宿主
  resolve_connection 404 `{"error":"Connection with id
  'no-such-kafka-conn' not found"}` 并入引导错误——转发路径端到端通。
  注意 `kafka_ui_*` 族在 standalone 判定下先短路 UNAVAILABLE（设计如此，
  文案含「经 DBX MCP 桥调用」引导），不参与桥兜底，回环探针须选
  `kafka_messages_digest` 这类连接级本地读工具。
- **fail-closed 对照**：同调用换空 `DBX_APP_DATA_DIR` → `DBX app bridge
  unreachable after 30s` 本地 fail-closed，与「app 侧 returned HTTP
  404」明确区分。
- **app-data 内无 kafka 保存连接**（仅 ssh 的 vagrant 固件），「转发 →
  宿主 → kafka workbench sidecar → 真实结果」全链路未覆盖，待后续
  seed（可用 kafka dev 测试集群地址）后补。
- **沉淀**：`shared/host-e2e/mcp_bridge_e2e.sh`（四插件统一回环脚本，
  本轮真机 4/4 全绿；kafka 探针为其中一环）。遗留项①闭环。
- 本轮插件源码只读，未改代码。

**剩余风险**：全链路真实结果层待 seed 连接补齐；偶发观察到跨插件转发
调用偶发 90s 无响应、单独重跑立即成功（疑似宿主侧/GUI 渲染竞态）。

## 第五轮（2026-09-13）可靠性纵深：stdio 传输 / 会话 churn / 写门对抗输入

三类主题（对照本轮任务书与 `shared/MCP_ACCEPTANCE.zh-CN.md` §2/§5/§8）：

**① stdio 传输层健壮性（实现 + 单测 + smoke K17）**

- `internal/mcp/stdio.go` `handleLine` 重构为 RawMessage 形状分派（ldap
  同构）：解析失败 -32700（id null）；非法请求 -32600——缺 id（非通知）、
  id 为 object/array/布尔、method 缺失/空/非字符串、jsonrpc 存在且非
  "2.0"（字段缺失容忍，照 ssh 基线）。原实现缺 method 折 -32601、method
  非字符串误档 -32700、id object 原样回显，全部修正。`Serve` 增加 16 MiB
  单行上限（超限 -32700 拒该行继续服务）。已知家族差异（登记）：ssh 对
  缺 method 仍回 -32601，本轮 ldap/kafka 按 -32600 分档，待家族拉齐。
- 新增 `stdio_robust_test.go` 6 用例（S-STDIO-R1..R6，ldap 同表）。
- smoke K17（真进程离线）覆盖同表全矩阵。

**② 会话/存储 churn（churn_test.go，S-CHURN-*）**

- **ConfirmStore 修复真缺陷**：过期未消费令牌（preview 弃单）原无任何
  清理路径，大量「只要预览不确认」的调用无界撑大令牌表——Issue 时顺手
  prune（S-CHURN-CONF-1）。
- **confirmTtlSecs 补齐（契约缺口）**：MCP_ACCEPTANCE §8 声明
  files/ldap/kafka 同构键族，kafka 此前缺 `confirmTtlSecs`——Settings
  增字段（缺省 60，10–600 与 ldap/files 同范围）+ `SettingsSet` 接线
  `confirms.SetTTL` + `ConfirmStore` 增 ttl/SetTTL/TTL（≤0 忽略）；
  `twoPhase` 过期报文改携实际生效 TTL（原硬编码 "expired (60s)"）。
  不追溯契约钉测（S-CHURN-CONF-2：旧令牌按原 60s 过期、新令牌按新 TTL）。
- **CursorStore 修复语义偏差**：淘汰原为纯插入序 FIFO（活跃会话误逐），
  `Next` 命中 `touchLocked` 顶队尾改真 LRU（S-CHURN-CUR-1/2）；
  `kafka_cursor_next` 的 unknown cursorId 报文对齐 ldap 质量（带 TTL/
  容量/重发 digest 指引，原仅 "unknown cursorId: x"）。
- IntentStore churn 500 轮收敛 + 快照不污染（S-CHURN-INT-1，ldap 同构）。
- **消费池 churn（kafkaconn/consume_pool_churn_test.go，S-POOL-CHURN-*）**：
  7 种 signature × 2 连接交替 30 轮（200+ 次 acquire/put/release）——
  key 与 entry 的签名/连接恒一致（不串台）、release 后收敛在 8 上限
  （不泄漏）、resetFailed 条目不复用且被同 key 替换恢复；异连接同签名
  永不共享条目。全离线（client nil，Close 对 nil 安全）。
- smoke K18（dev 集群）：同一连接 earliest/latest 交替 **50 轮** digest，
  earliest 恒 matched=造数条数、latest 恒 0——第四轮 P0 修复（复用前
  kadm 全分区边界重置）的回归面，收尾两阶段删除 churn topic。

**③ 写门对抗输入（writegate_test.go，S-WGATE-1..4）**

- 写门策略矩阵：只读连接 produce/clear/reset/delete 全拒（补 reset 只读
  拒绝）；allow_delete=false 拒 delete/clear 而 reset（写非删）进两阶段；
  未注册连接按只读兜底且不签发令牌（S-WGATE-1）。
- reset timestampMs 越界：0/负数预检拒绝（0 等价重置到纪元，几乎必是
  参数缺失；不白烧令牌）、非整数点名带原值、远未来（2100 年）合法进
  两阶段且确认到达执行层（S-WGATE-2）。
- records_clear 两阶段：参数被改 hash mismatch 作废、正确确认到达执行层、
  token 一次性（S-WGATE-3）。
- **produce 头部预算（新增）**：header 名空/超 1024 字节/条数超 64 显式
  报错带实际上限与实测值（不静默截断丢弃）；≤64 个与空对象 headers 通过
  解析（借 64 KiB 预算检查封闭验证，不触拨号）；produce 单阶段永不签发
  令牌（S-WGATE-4）。

**回归（真实输出）**

- `cd backend && go vet ./...` 通过；`go test ./... -count=1` 全绿
  （internal/mcp 由 72 函数增至 **87 函数**，本轮新增 15：stdio_robust 6
  + churn 5 + writegate 4；internal/kafkaconn 增 churn 3）；
  `CGO_ENABLED=0 go build -trimpath -o bin/dbx-plugin-kafka .` 成功。
- dev 集群（dbx-kafka-test 127.0.0.1:9092 + SR :19081）：
  `DBX_PLUGIN_SIDECAR=$PWD/backend/bin/dbx-plugin-kafka python3
  scripts/smoke_mcp.py` → **total=18 PASS=18 FAIL=0 SKIP=0**
  （K1–K18 全 PASS，新增 K17/K18；K18 detail「50 alternating rounds:
  earliest matched=10 stable, latest matched=0 (pool reuse resets)」）。

**smoke 客户端两处修复（与 ldap M18 同源，测试面缺陷而非 sidecar 缺陷）**

1. McpStdioClient 响应读取丢弃不匹配 id 的帧——逐请求 goroutine 响应
   乱序时先到的帧被丢，后续读取永远等不到（挂死）。改为 `pending`
   缓冲（通知帧除外）。
2. `select` 直接探测 BufferedReader 的 fd：上一帧已把后续数据拉进用户态
   缓冲时管道为空，select 满超时假报"响应丢失"。改为 `os.read` + 自管
   行缓冲，超时语义为真。

**剩余风险**：(1) ssh 侧缺 method 的 -32601 分档差异待家族拉齐（shared/
契约表本轮不可改）；(2) K18 的 50 轮交替在慢集群上会线性拉长 smoke 时长
（每轮一次 digest 往返）；(3) stdio `pending` 缓冲只服务单客户端顺序
消费，多路复用同一 stdio 的客户端理论可乱序取帧（现状无影响）。

### 2026-09-14 ZCode MCP 接入与真机 agent 调用测试（MCP 集成会话）

- **ZCode 接入**：用户级 zcode config 新增 `dbx-kafka`（`backend/bin/
  dbx-plugin-kafka --mcp` + 专属 `DBX_PLUGIN_DATA_DIR`；与 dbx-files/
  dbx-ldap 同轮接入，形状镜像既有 dbx-ssh 条目）。
- **只读/删除门与未知 topic 错误补可行动提示**（真机 agent 实测卡点）：
  stdio 内联连接默认 `readOnly:true`，produce/写族被拒后 AI 调用方无从
  知道如何解除——`ensureWritable`/`ensureDeleteAllowed` 拒绝消息点名
  `"readOnly": false`（删除类叠加 `"allowDelete": true`）；未知 topic
  的 `kafka_ui_topics` hint 补注 stdio 无 topic 列表能力（annotate 与
  digest 存在性校验两处）。`writegate_test.go`/`tolerance_test.go` 加
  提示文案断言；go test mcp 包全绿。
- **真机 agent 调用测试**：dev 集群（dev-cluster.sh up）+ 内联 brokers
  全链路——digest/produce（readOnly:false 后）/cursor/两阶段 topics_delete
  /未注册 topic/错误 broker/只读拒绝，均按预期；改进后复验提示文案生效。
  smoke 终态 **total=18 PASS=18 FAIL=0 SKIP=0**（K17 修复见上文，本轮
  复跑两次全绿；首次全量运行曾复现 K17 挂死，即上节客户端缺陷实证）。

### 2026-09-14 续：zcode 真机接入发现 required:null 拒收并修复

- 同 ldap 轮：`toolEntry` nil `required` → `"required":null` 被 zcode
  tools/list zod 校验拒收（整个服务器不出现）；`tools[1]` 即
  `kafka_ui_focus` 触发。修复空 required 省略键 + `tools_schema_test.go`
  红线测试；strict-shape 全字段检查通过；mcp 包单测全绿，smoke
  **18/18**，二进制已重建。`dbx-kafka` 待会话重启后以 `mcp__dbx-kafka__*`
  出现。
- 同轮 zcode 内真实工具身份首跑（dbx-files，schema/isError 均为新态）：
  内联 localFs digest → write → rows+cursor 翻页 → 两阶段 delete →
  UI UNAVAILABLE（isError 经 zcode 以 "MCP tool returned an error" 透出，
  文本可行动）全链路按预期。

## 第七轮（2026-09-14）并发安全 -race 验证 + 缺参枚举口径拉齐 + enum 在线钉桩

三类主题（任务书：并发安全与口径拉齐轮；对齐 `shared/MCP_ACCEPTANCE.zh-CN.md`
§3.3/§3.9；ldap 同表落地）：

**① go test -race 全量首验（本轮最高优先级）**

- `cd backend && go test -race -count=1 ./...` 首跑即全绿、**race 零告警**
  （6 包：根/kafkaconn/lifecycle/mcp/store + gen-protobuf-fixture 无测试）。
  前六轮 churn/边界测试全部顺序执行，本轮以真并发补上竞争检测器验证面。
- **补真并发压力测试**（`internal/mcp/concurrency_test.go`，S-CONC-*，3 个
  ——Confirm/Cursor/Intent 三张表各一，与 ldap 逐字同构仅行形状差异；
  全部离线 + 原子注入时钟 `concClock` + 互斥错误收集器 `concErrs`）：
  - S-CONC-CONF-1：16 goroutine × 40 轮混合 issue→即时消费（必须 OK）/
    4 方竞争消费同一令牌（**恰好一方 OK，其余 unknown——一次性语义并发下
    不得双重消费**）/ 远期消费（expired）/ 弃单；阶段 2 短 TTL 弃单洪泛，
    收敛后跨 TTL 签发触发 prune → 表收敛为 1（>2000 次操作）。
  - S-CONC-CUR-1：24 goroutine × 8 会话物化（16 行确定性
    topic/partition/offset）× 每会话 4 读者固定窗口并发翻页——命中必须
    逐行精确（Anchor 逐项核对，不得串行/丢行/错位），被淘汰报 unknown
    合法；终态 sessions/order 一致且收敛在容量 4（960 次操作）。
  - S-CONC-INT-1：12 goroutine × 50 登记双投递给 6 回报池（双回报竞争）+
    快照写入方持续覆盖——存活条目终态必不是 pending（无丢更新）、表收敛在
    容量 32、快照读取永得完整写形状；跨 TTL prune 收敛为 1
    （600 登记 + 1200 回报）。
- 三者在 -race 下全过，无数据竞争、无双重消费、容量收敛。

**② 缺参报错枚举式拉齐（对齐 ssh，§3.9）**

- `util.go` 新增 `missingRequired(args, keys...)`（ldap 同构）：一次枚举
  全部缺失 required 参数（`Missing required parameters: a, b`，按 schema
  `required` 声明顺序；缺失判定 = 键不存在或显式 null）。present-but-类型
  错误（空串/空数组/非法值）不混入枚举，仍由逐参数校验精确点名。
- 落地点（server.go 九处）：`uiSearch`（topic）、`locatorOf`/ui_select
  （partition+offset）、`uiTopics`（connectionId）、`messagesDigest`
  （connectionId+topic）、`cursorNext`（cursorId）、`messagesProduce`
  （connectionId+topic）、`topicsDelete`（connectionId+topics）、
  `groupsOffsetsReset`（connectionId+group+resetTo）、
  `topicsRecordsClear`（connectionId+topic）。
- **边界保持**：`topics: []`（present-but-空数组）仍精确报
  `topics is required`；resetTo 各模式配套参数（topics/timestampMs/
  partitionOffsets）属条件必填，保持 validateResetRequest 按模式精确点名
  （K10/K15 预检语义不变）；`ui_focus` 的 panel 不在 schema required 内，
  维持原提前校验语义。
- 单测 +3（新建 util_test.go）：helper 三缺全点名/单缺只其一/null 视同
  缺失；digest 入口接线（双缺按 schema 顺序 + 空串精确点名）；
  写族入口（reset 三缺全点名、topics_delete 空数组精确点名）。
  既有 `TestServerLocatorValidation` 断言同步更新为枚举语义。
- smoke K6（ui_select 双缺全点名）+ K8（digest 单缺/双缺、ui_topics）
  断言同步更新。

**③ enum 非法值在线冒烟（离线探针不可达段，§3.3）**

- kafkaconn 对 `offsetStrategy` 非法值的报错已列全 schema enum
  （`offsetStrategy must be latest, earliest, committed, timestamp, or
  offset`），`resetTo` 同（`resetTo must be earliest, latest, timestamp,
  or partitionOffset (got …)`）——本轮核对无需改文案，补在线钉桩。
- smoke 新增 **K19**（dev 集群在线段）：真实连接下
  `offsetStrategy:"bogus"` / `resetTo:"bogus"` 报错逐项列出 schema enum
  全部合法值（含 got 实际值）；`offsetStrategy:"EARLIEST"` 大小写归一
  在线钉桩（自建 topic produce 1 条 → matched≥1；收尾两阶段删除保持
  dev 集群整洁）。

**回归（真实输出）**

- `go vet ./...` 通过；`go test -race -count=1 ./...` 全绿 race 零告警；
  internal/mcp 测试函数 88 → **94**（本轮 +6：并发压力 3 + 缺参枚举 3），
  kafkaconn 184 不变。
- `CGO_ENABLED=0 go build -trimpath -o bin/dbx-plugin-kafka .` 成功
  （-race 只影响测试，构建不变）。
- `python3 shared/mcp_schema_check.py --binary backend/bin/dbx-plugin-kafka`
  → **RESULT: CLEAN**（schema required 与枚举报错一致性无漂移）。
- dev 集群（127.0.0.1:9092 + SR :19081，任务前已在跑）：
  `scripts/smoke_mcp.py` → **total=19 PASS=19 FAIL=0 SKIP=0**
  （K1–K19 全 PASS，新增 K6/K8 枚举断言 + K19 在线钉桩段）。

**剩余风险**：(1) 并发压力测试只覆盖三张 store 表的纯逻辑面，Server 层
settings 混合流量并发未单列（settings 读写已由 s.mu 串行化，且被 store
并发面间接覆盖）；(2) consume 池（kafkaconn consume_pool）的并发面由
既有单测 + K18 churn 覆盖，本轮未追加 -race 专项多 goroutine 压测
（-race 全量跑已把 K18 纳入检测范围）；(3) dev 集群为共享资源，K19
造数自建自清，不触碰种子 topic。

**重启复验（同日）**：会话重启后 `mcp__dbx-kafka__*` 全套工具出现；zcode
内真实任务通过——digest → produce（readOnly:false）→ 回读+`$.from` 投影
聚合 → 两阶段 topics_delete → 只读拒绝（新提示生效）→ 未知 topic（stdio
注记生效）。至此 files/ldap/kafka 三插件均完成 zcode 真实链路验证。

## 第八轮（2026-09-14）终验：dev 集群全量冒烟

MCP 专项收口轮：第七轮代码之后对在跑 dev 集群（127.0.0.1:9092 + SR
:19081）做最终全量终验。本插件源码本轮只读。

- `DBX_PLUGIN_SIDECAR=backend/bin/dbx-plugin-kafka python3
  scripts/smoke_mcp.py` → **total=19 PASS=19 FAIL=0 SKIP=0**
  （K1–K19 全 PASS；**K19 enum 在线断言通过**——`offsetStrategy/resetTo`
  非法值报错逐项列出 schema enum 合法值、`EARLIEST` 归一化在线
  matched≥1）。
- 深水区照例全绿：K14（schema 挂载 + `$.user.id` 投影 distinct/topN +
  坏版本 decodeFailures + SR 门）、K15（聚合钳制 45→20/45→10 + cursor
  翻到 done + reset timestampMs 落点 p0@6 + 边界报错）、K18（消费池
  earliest/latest 交替 50 轮 matched 稳定）。
- 结论：第七轮全部改动（并发安全、缺参枚举口径、K19 在线钉桩）在真实
  集群组合下无回归，第七轮记录的 dev 集群数据（K1–K19 全 PASS）本轮
  复现成立。本插件无独立性能脚本，不做基线采集（digest 扫描/翻页由
  K15/K18 断言覆盖正确性面）。

## 测试覆盖续轮：组合式函数专测（2026-09-20）

- **`useConsumeForm.spec.ts` 新增（29 用例，此前 490 行组合式无专属 spec，
  51.8% 覆盖）**：buildParams 全分支——groupId×partitions 互斥兜底锁定
  「双填时 partitions 优先、groupId 落选」（此前误读为两侧都丢）；commit
  互斥短路；过滤通道 + fieldFilters 仅「启用且有值」上送（gt+非法数值行
  仅 UI 标红仍上送，行为锁定）；timestamp/offset 策略与时间、offset 范围；
  schema 挂载（version 非法省略、glue provider watch 复位开关——computed
  依赖必须 ref 承载，闭包变量驱动不了重算）。开合记忆（msgFormOpen/
  msgFilters 落盘与损坏 JSON 回退，watch 异步需 nextTick）；摘要 chips；
  时间双模式换算；fieldFilters 行校验；预设 load/save/apply/remove 全
  矩阵（含 monitor 型过滤、空名不发请求、桥错误走 error 回调）。
- **行为锁定**：applyPreset 先设 version 再设 subject，subject watch 会把
  schemaVersionText 复位（subjects 未加载时预设版本号被清、format 保留）。
  如需保留预设版本号须调整 applyPreset 写入顺序，登记不实施。
- **`useMessageDetailDrawer.spec.ts` 新增（8 用例，65.2%）**：格式探测
  （XML/JSON/raw）、打开视图重置、headers 表格/JSON 双视图与关闭复位、
  renderView 手动切换重渲染、Esc 关闭焦点归还触发元素、Tab 双向回绕、
  更高层弹窗（modal-backdrop）在场让位。焦点断言前提：mount 须
  `attachTo: document.body`（modalBehavior.spec 同款）。
- **验证**：`vue-tsc --noEmit` 0 错；38 文件 337 用例全绿（基线 36 文件
  300 用例）；`vite build` 通过（chunk warning 为既有现象）。纯测试改动，
  零组件/协议变更。

## 前端持久化迁移 host.storage（2026-09-24）

- **背景**：工作台 iframe 是 sandbox opaque origin，直接读 `localStorage` 抛
  SecurityError，此前各调用点的 best-effort localStorage 持久化（表单开合、
  树宽/折叠、收藏、时区、页大小）在真机上全部静默失效。宿主 Host API 1.2
  提供 `window.dbxPlugin.storage`（get/set/delete + 能力位
  `capabilities.storage`，manifest 须声明 `host.storage` 权限），桌面端落
  `plugin-data/<id>/ui-storage.json`，web 宿主落顶层文档 localStorage。
  迁移统一走 `shared/frontend/pluginStorage.ts` 适配器（files 插件同款），
  新增 `src/lib/pluginStore.ts` 单点接线，调用点保持
  `getItem/setItem/removeItem` 同名语义，零 async 改造。
- **键清单（键名一律不变，共 8 键）**：`dbx.kafka.ui.msgFormOpen`、
  `dbx.kafka.ui.msgFilters`（useConsumeForm）；`dbx.kafka.ui.treeWidth`、
  `dbx.kafka.ui.treeCollapsed`、`dbx.kafka.ui.showInternal`（TopicTree）；
  `dbx.kafka.ui.topicFavorites`（topicFavorites）；`kafka.ts.tz`
  （kafkaColumns，历史键名沿用）；`dbx.kafka.ui.gridPageSizes`（新收敛键，
  见下）。键集合在 `pluginStore.ts` 显式声明（宿主 storage 无列键方法），
  `main.ts` 挂载前 `await pluginStore.ready` 完成启动水合（含 localStorage
  旧键一次性惰性搬家）。
- **动态键收敛决策**：分页页大小原为 `dbx-kafka-grid-pagesize-<tableKey>`
  动态拼键（12 张表），收敛为单一 `dbx.kafka.ui.gridPageSizes` 固定键 +
  JSON map（tableKey → size）；`loadPreferredPageSize/savePreferredPageSize`
  导出签名不变，内部布局换掉。旧动态键不做迁移：工作台 opaque origin 下
  localStorage 从未写成功过，无存量可搬。
- **降级链**：宿主桥 storage → guarded localStorage（web 直连/dev mock 场景）
  → 内存（仅当前会话）；store 不抛错，调用点原有 try/catch 兜底结构保留
  （无害）。mock（`mockDbxHost.ts`）补齐 storage mock + 
  `capabilities.storage=true`（内存 Map，get 未命中 null、set(undefined)→null），
  组装注释从「Host API 1.0 面一致」更新为 1.2；`env.d.ts` 的 DbxPluginApi
  内联补 `capabilities`/`storage` 声明；`manifest.json` permissions 增加
  `host.storage`（版本号不动）。
- **spec 适配**：播种/断言全部改走 pluginStore 实例（happy-dom 下 store 在
  模块导入时已水合，之后改 localStorage 读不到）——useConsumeForm/
  TopicTree/DbxAgGrid/MessagesPanel/topicFavorites 五个 spec；DbxAgGrid 页
  大小断言改查 gridPageSizes JSON map。新增薄 spec
  `lib/pluginStorage.spec.ts`（键集合完整 + 键名常量锁定 + node 环境通道为
  memory + 内存档 Web Storage 语义）。mockDbxHost 内部的
  `kafka-mock-presets` localStorage 属 mock 走查壳自身存储，不在工作台
  UI 键清单内，未迁移。
- **web/docker 注意点**：web 宿主把 host.storage 落到顶层文档 localStorage
  （非插件 iframe），插件侧代码不变；桌面端 opaque origin 下 localStorage
  搬家自然跳过。配额由宿主端强制（单值 256 KiB / 总量 1 MiB / 1024 键），
  超限仅 console.warn 不阻断 UI；凭据仍走连接表单 binding:"secret"，本迁移
  只涉及非敏感 UI 状态。
- **验证**：`vue-tsc --noEmit` 0 错；`vitest run` 40 文件 345 用例全绿
  （基线 39 文件 343 用例）。零新依赖、零用户可见文案（无七语改动）、
  不动 backend/、不动键名。

- **mock 兜底语义修正（收尾统一改动）**：storage mock 初版为纯内存 Map，
  页面刷新即丢，背离真实宿主（web 宿主由顶层 localStorage 兜底、桌面端落
  `plugin-data/<id>/ui-storage.json`）——ldap ui_test walkthrough 的
  「expanding the compact bar … persists」用例即因此失败（用例 109 行裸
  localStorage 断言，且 walkthrough 共用 page 导致后续 builder 用例连坐，
  一度 30/35）。统一改为 localStorage 兜底（键名不变；字符串值原样、对象
  JSON 编码；opaque origin 不可用时退化内存），dev/`?mock=1` 恢复刷新持久化，
  walkthrough 断言无需改动；修正后 本插件 vitest 40 文件 345 用例复验全绿。
