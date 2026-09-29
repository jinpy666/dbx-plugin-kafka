# 全方位代码评审报告（2026-09-30）

- **分支**：`codex/kafka/full-review-audit`（自 main @ 1d12604 切出，含工作树未提交修改，按现状评审）
- **方法**：三条独立评审 lane 并行（后端 Go / 前端 Vue+TS / 架构与契约交叉），主线交叉复核关键发现并运行 agent-flow.yml 全量本地验证链
- **范围**：backend（Go，~33 文件）、frontend + shared/frontend（~50 文件）、shared/sdk/go、manifest.json、docs、scripts；核心生产代码逐文件人工评审，测试/生成物不在深度评审范围

## 本地验证链结果（agent-flow.yml validation.local 全项）

| 项 | 结果 |
| --- | --- |
| `python3 scripts/validate_repo.py` | PASS |
| `node scripts/connection-forms/verify.mjs kafka` | PASS（960 组合） |
| 后端 `gofmt -l` / `go vet ./...` | 合规 / 干净 |
| 后端 `go test ./...` | 全部 ok |
| 前端 `vue-tsc --noEmit` | 无错误 |
| 前端 `vitest --run` | 42 文件 382 用例全通过 |
| 前端 `pnpm build` | 成功；⚠ 主 bundle 1.96 MB（gzip 581 KB）超 500 KB 警告线 |

i18n 脚本化比对：7 locale × 593 key 完全对齐；全部静态 `t()` key 存在。

## 总体结论：REQUEST CHANGES

无 CRITICAL、无安全阻断缺陷（写门禁、凭据红线、两阶段确认、注入防护均核实扎实）。判定依据：2 个 HIGH（1 个内存放大风险 + 1 个本次改动引入的必现运行时错误）与 2 个确认的功能性逻辑错误，应在合入前修复。架构状态 **WATCH**（无 BLOCK），核心关切是契约守护链对「请求参数类型」与「嵌套字段形状」无机制性覆盖——本次 `timedOut` 漏登记与 `offsetTime` 类型错误恰好都落在盲区里。

---

## CRITICAL (0)

（none）

## HIGH (2)

### H1. consume/export 路径消息留存无字节级预算 → 单请求 OOM 风险
- **位置**：`backend/internal/kafkaconn/messages.go:587,656-660,1827`（配合 393-401、1921-1927）
- **问题**：工作台 consume/export 只有条数上限（limit≤10000、maxScan≤1e6），每条命中消息驻留 valueText(≤512KB)+valueBase64(≈683KB)+headers ≈ 1.2MB；limit=10000 时单请求驻留理论上可达 ~12GB，export 再经 `serializeJSON` 全量 MarshalIndent 瞬时翻倍。digest 路径已有 `RetentionByteBudget=64MiB` 兜底（`mcp/server.go:632`、`messages.go:594`），consume/export 未覆盖。
- **风险**：长驻 sidecar 单请求即可 OOM。
- **修复**：非 digest 消费路径同样接入字节预算（或按 limit×单条上限收敛）；export 序列化改流式/分块。

### H2. offsetTime 以 JSON number 上送，后端 string 字段必拒（本次 diff 新增的一等入口）【已主线复核证实】
- **位置**：`frontend/src/lib/consumeForm.ts:61` → `frontend/src/composables/useConsumeForm.ts:397,417`；后端 `backend/internal/kafkaconn/messages.go:59`（`OffsetTime string`）；协议 `docs/PROTOCOL_KAFKA.zh-CN.md` §555/§177 亦为 string
- **问题**：`offsetTimeToParam` 对 `^\d{10,}$` 输入（unix 毫秒）返回 `Number(trimmed)`，`buildParams` 原样放入 `params.offsetTime`。Go `encoding/json` 解 `number → string` 直接报错 → -32602。本次 diff 新加的 datetime/unix 双模式输入与 "now" 按钮把该路径变成主入口。
- **风险**：timestamp 策略的 unix 模式消费必现失败；契约守护四层（methodContract 只查响应顶层键 / mock 不挑类型 / smoke 无参数断言 / typecheck 自家 union 放行）全部拦不住。
- **修复**：api.ts 类型与实现改为 string（unix ms 透传为字符串），或后端宽容解析；同时给 methodContract 增加请求参数类型登记。

## MEDIUM (8)

1. **预设应用静默丢弃时间/offset 范围过滤**【已复核证实】`frontend/src/composables/useConsumeForm.ts:478-512` — `applyPreset` 未恢复 `timestampFrom/timestampTo/offsetFrom/offsetTo`，而保存侧 `buildParams()`（436-443）明确含这四个字段。重放查询与保存时语义不一致、filterCount 徽标漏计。修复成本极低（补四字段回填，时间字段用 `unixMsToDatetimeLocal` 归一）。
2. **切换连接后数据面不刷新，跨连接数据串台** `frontend/src/App.vue:404-407,419-422` — 宿主 context 重推只更新 connectionId + 刷新策略，不重拉 topics、不清面板状态；面板 `visitedPanels`+v-show 常驻，旧连接的 Stream 会话/Monitor 采样/Messages 结果冠以新连接名展示。建议 connectionId 变化时 `loadTopics()` + 广播 reset，或以 `:key="connectionId"` 重建面板。
3. **SDK 层每请求一 goroutine 无背压**【已复核证实】`shared/sdk/go/dbx-plugin-sdk/sdk.go:200-219` — 同构问题已在 `mcp/stdio.go:74-78`（`maxConcurrentRequests=32` 槽位背压，注释自证系评审修复）解决，SDK Serve 路径漏改。宿主灌入慢请求时 goroutine/内存无界累积，并与 H1 相乘。
4. **partition 参数 int64→int32 静默截断** `backend/internal/mcp/server.go:778,1135,1145` — `coerceInt` 后直接 int32 截断：4294967301 静默折为 5。与同文件 `parsePartitionOffsets`（server.go:1243，`ParseInt(…,32)` 正确报错）不一致，违反本仓「绝不静默折算」红线。
5. **cursor 翻页 offset 非法值静默折 0** `backend/internal/mcp/util.go:100-103` + `server.go:652` — `offset:"abc"`/小数被 `intArg` 折为 0，翻页静默回到开头；digestScanLimit 同病。违反「present-but-非法必须精确报错」约定。
6. **SR listVersions 串行 + 重复请求** `backend/internal/kafkaconn/schema.go:616-641` — 每版本取 format 与 schemaID 重复调两次 `getSchema` 且无并发，版本多的 subject 最坏 2×N 次串行 REST，外层 20s adminTimeout（schema.go:840）必然截断。复用单次结果 + 有界并发（照 listSubjects 的 sem 模式）。
7. **ConnectionStatus 字段双漂移**【已复核证实】后端 `types.go:454` 下发 `error`、从不下发 `allowDelete`；前端 `api.ts:279-280` 读 `lastError`（`ConnectionsPanel.vue:303` 永不显示连接错误）与 `allowDelete`（`App.vue:208` 删除门禁恒放行——该处有注释自认降级，后端服务层门禁仍在，非安全洞）。前端改读 `error`；allowDelete 要么后端补发要么前端删字段。`mockDbxHost.ts:1051/1066` 按前端口径造假，掩盖了漂移。
8. **methodContract.json 漏登记 timedOut** `frontend/src/lib/methodContract.json`（consume 条目）— 本次 diff 给 consume 同步了 `timedOut` 于 4 处（messages.go:155、api.ts:98、PROTOCOL §consume×4），唯独自称 "single source of truth" 的契约文件未更新，mock 也不发（MessagesPanel.spec.ts:655 手搭响应才测到）。

## LOW（精选 15 条）

**后端**
1. `mcp/stdio.go:178,216-227` EOF 前等待槽位期间 pending 切片可无界增长；设上限或积压超限时暂停接收。
2. `mcp/digest.go:73-99` `buildTimeHistogram` 恶意极端时间戳（跨度>2^63）int64 回绕产生负索引 → panic（被 recover 兜成单请求 -32603）；索引前补 `index<0` 防御。
3. `main.go:918` mcpTools 的 `_ = json.Unmarshal(params, &body)` 吞畸形参数错误，垃圾输入按全量清单返回而非 -32602。
4. `mcp/stdio.go:580-592` + `appbridge.go:166-208` 仅给 connectionId 但携带残参时，saslPassword/tlsClientKey 等凭据类键经明文 loopback HTTP 转发 DBX 桥（桥无 token，仅 127.0.0.1 绑定）；转发前剥离凭据键，桥鉴权属宿主侧跟进项。
5. `mcp/util.go:144-150` + `server.go:1263-1282` payloadSize 序列化失败返回 0 → 恒放行绕过截断上限；超限路径最多 4 次全量 Marshal。
6. `mcp/stdio.go:138-155` `eofSeen` 通道 close 后无读方（死代码）。
7. `kafkaconn/stream.go:599-611` fetch 错误退避重试无次数上限，topic 永久不可达时每周期向前端 spam 错误直至 30 分钟空闲回收；加连续错误熔断。

**前端**
8. `composables/useMessageDetailDrawer.ts:49-62` gzip 解压真实异步，`renderView` 无序号守卫，快速切换两条消息时旧结果覆盖新视图；加 `renderSeq`。
9. `components/ProducePanel.vue:484-486,398-460` Flow 的 `setInterval` 不避让在途 async tick，慢于 250ms 间隔时并发双计 `flowFailures` → 提前自动停止；加 in-flight 守卫或改 setTimeout 串行链。
10. `lib/timestamps.ts:57-71` tz=utc 时 date-filter 用 `getFullYear()/getMonth()/getDate()`（本地分量）比较按 UTC 解析的值，跨时区按天过滤整体错位一天；改 `getUTC*`。
11. `lib/kafkaColumns.ts:428-438` group state=Unknown 查 `messages.stateUnknown`，七语均缺该 key（有原文兜底不崩，但中文界面混英文）；补 key。
12. `lib/timestamps.ts:15,26` `if (!ms)` 把合法的 epoch `0` 渲染为 "—"；改 `ms == null` 判空。
13. `components/TopicTree.vue:340-376` 星标/快捷生产/消费是 `tabindex="-1"` 的 span，键盘用户无法触达（注释声明刻意单 tab-stop）；提供键盘路径或文档明示取舍。
14. `lib/api.ts:531-533` `messagesExport` 已无调用点（导出改前端序列化），与后端旧口径并存易误用；删除或注明仅契约保留。`lib/consumeForm.ts:172-188` `matchText` 死代码。

**构建**
15. 主 bundle 1.96 MB（gzip 581 KB）超 500 KB 警告线（ag-grid 等大依赖）；对自包含插件 UI 可接受，但可评估按面板动态 import 分包。

## 架构 WATCHLIST

- **契约守护链系统性盲区（本次最强反方论点）**：方法名级 42 个方法三方零漂移且有 AST 守护，但字段/类型级漂移本次实际发生 6 处；守护只查响应顶层键、不查请求参数类型与嵌套形状，smoke 无 statuses 断言。H2 正是四层检查交集盲区。**建议**：methodContract 扩展 params 类型登记；smoke 增加 statuses 形状断言。在此之前合入，"契约四方同步"仍靠人肉。
- `.github/agent-flow.yml` ownership.contract 指向不存在的 `docs/PROTOCOL.zh-CN.md`（实际为 `PROTOCOL_KAFKA.zh-CN.md`）。
- `README.md:23,60` / `README.en.md:25,62` 称 "5 种 offset 策略"，实际 6 种（漏 `recent`，且它是表单默认值）。
- manifest/README 宣称 "Read-only by default / 默认只读"，但 `types.go:232-260` `NormalizeProfile` 不强制 ReadOnly、manifest `read_only` 无 default——新连接实为读写；加默认值或改措辞。
- `frontend/src/mockDbxHost.ts:1064` mock 发 `connectionSource:"kafka"`，后端常量是 `"bootstrap"`（types.go:40）。
- `backend/internal/store/store.go:174-189` `LoadPrefs/SavePrefs` 零调用（prefs.json 死代码）；文件头目录注释未列 `mcp-settings.json`。
- 超大文件拆分建议（非缺陷）：`i18n.ts` 4496 行（按语言/域拆资源）、`kafkaColumns.ts` 1033 行、`mcp/server.go` 1282 行（digest 段可同 digest.go 先例拆出）、`main.go` 1140 行（51 case 按域拆）、`mockDbxHost.ts` 1345 行。

## 已核实为干净的面（三 lane 交叉确认）

- **凭据红线**：sasl/tls/sr/glue/oauth 凭据只进 connSecrets 与指纹摘要，全部日志路径 grep 无泄漏（diag.go:147 仅输出机制名）；tls MinVersion 1.2，skip-verify 留审计。
- **写门禁双层不可绕过**：MCP `ensureWritable/ensureDeleteAllowed` + kafkaconn `ensureWriteAllowed/ensureDeleteAllowed` 与门；confirmTopic/confirmGroup 同名校验（policy.go:44-91）；confirmToken 一次性 + hash 绑定 + 10–600s TTL。
- **注入与内容安全**：全仓无 v-html/innerHTML；Kafka 消息体只经文本插值与 ag-grid 默认转义；SR URL 全 PathEscape；CSV/TSV 公式注入已中和；导出文件名穿越已中和；解压炸弹前后端对齐 16MiB 防护。
- **并发与状态**：意图表/游标/令牌表锁内值拷贝与 LRU/TTL 正确；ResetGroupOffsets 逐分区 commit 确认；后端包依赖方向无环；前端组件不绕桥；shared 双语言层带 vendored-sync 戳并有校验。

## 修复优先级建议

1. **随本次改动必修**：H2（offsetTime）、M1（预设丢字段）——均为本次 diff 直接引入/放大且修复成本极低。
2. **合入前修**：H1（字节预算）、M2（换连接串台）、M7（lastError 漂移，一行改动）。
3. **随后跟进**：M3-M6、M8、LOW 与 WATCHLIST 契约守护强化。
