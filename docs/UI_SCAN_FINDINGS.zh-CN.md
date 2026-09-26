# Kafka 插件前端 UI 体验扫描报告（UI_SCAN_FINDINGS）

> 扫描角色：持续 UI 体验扫描 agent N（只读扫描 + 报告，不实施修复）。
> 扫描对象：`kafka/frontend`（mock.html 可视化夹具）。本报告只记录发现与方向建议，不含代码修改。

## 一、扫描环境

| 项 | 值 |
| --- | --- |
| 轮次 | 第 1 轮基线走查（约 09:37–09:55，含 L 已在工作树的 ProducePanel/CodeEditor 半成品改动）＋ 第 2 轮收尾复核（约 09:58–10:05，L 的改动持续就位，dev server 热加载） |
| Dev server | 复用已在跑的 `vite --port 5283`（IPv6 ::1，进程 71489，kafka/frontend 工作目录），未新起、未跑 build |
| 自动化 | playwright-core + 系统 Chrome，独立实例装于 `/tmp/uiscan-n`（未进项目依赖；共享 Playwright MCP 浏览器被并行 agent 占用，故独立起会话避免互踩） |
| 视口矩阵 | 1280×900（默认）、720×900（窄）、1440×900（宽） |
| URL 参数矩阵 | 默认 / `?theme=light`（默认即 dark，dark 由默认路径覆盖）/ `?ro=1` / `?glue=1` / `?err=1`（错误注入）/ `?noconn=1` |
| 交互核验 | Tab 顺序、`/` 快捷键、Esc 关闭弹层、backdrop 点击关闭、按钮禁用态、行内校验 |
| 证据 | 截图 30+ 张（走查核对后已全部删除，未入库） |
| typecheck | `pnpm typecheck` 退出码 0（工作区健康，L 半成品代码可编译） |

## 二、发现清单

统计：**P0 × 0，P1 × 4，P2 × 15**。无阻断使用级问题。

### P1（明显可感知的体验债）

**P1-1 消息面板首屏被消费表单占满，消息数据整体在首屏之外**
- 位置：MessagesPanel · 消费表单（基础/定位/时间与范围/过滤 四组）+ 结果表
- 复现：选中 topic → 打开"消息"页 → 点"消费"。1280×900 与 1440×900 下结论一致。
- 期望 vs 实际：期望消费后立刻能看到消息表（哪怕首行）；实际四个表单组默认全部展开占满约 830px 高度，"已扫描 3 · 命中 3"统计与表头贴在视口底边，数据行要滚一整屏才能看到。空态时同样只见表单一整屏。
- 建议：非核心组（定位/时间与范围/过滤）默认折叠或记忆上次折叠态；或压缩表单纵向密度，把结果区上移；"消费"后自动滚到结果区兜底。

**P1-2 弹层不响应 Esc：消息详情抽屉、连接弹窗**
- 位置：MessagesPanel · 消息详情抽屉（含 `.drawer-backdrop` 模态遮罩）；ConnectionsPanel 弹窗
- 复现：点击消息行打开抽屉 → 按 Esc。截图前后完全一致，遮罩仍拦截全页点击；连接弹窗同样。
- 期望 vs 实际：期望 Esc 关闭当前弹层并归还焦点；实际 Esc 无效，关闭途径仅 ✕ / 关闭按钮 / 点遮罩（抽屉点遮罩可关，已验证）。
- 建议：弹层组件统一挂 keydown Esc → 关闭 + 焦点归还；注意抽屉与连接弹窗两处都要。

**P1-3 连接弹窗无焦点陷阱，关闭后焦点不归还**
- 位置：ConnectionsPanel
- 复现：点击工具栏"连接"打开弹窗后连续 Tab。实际焦点序列全部落在背景元素（OUT: 消息/流式/生产/…tab 按钮），没有任何一步进入弹窗内控件；关闭后焦点停留在被 Tab 移到的任意位置。
- 期望 vs 实际：期望打开时焦点进弹窗首个控件、Tab 在弹窗内循环、关闭归还到触发按钮；实际三者皆无。
- 建议：open 时 focus 弹窗容器或首个交互控件 + 简易 focus trap；close 时归还触发按钮。消息详情抽屉同理（打开时焦点也未移入抽屉）。

**P1-4 生产面板校验态与主操作不联动 + 1 处运行时异常（归属 L，只记录）**
- 位置：ProducePanel（L 正在实施改动的文件）
- 复现 A：Headers 编辑器输入 `{bad json` → 行内红字"Headers JSON 不合法：…"出现，但"发送"按钮仍为可点击态（isDisabled=false）。
- 复现 B：第 2 轮在生产面板交互中出现未捕获异常 `PAGEERROR: value_.trim is not a function`（源码定位 `ProducePanel.vue:105` 附近 `Number.parseInt(value_.trim(), 10)`，入参非字符串时抛出）。typecheck 通过（说明是运行时类型问题）。
- 期望 vs 实际：期望校验失败时主按钮禁用或点击被拦截并聚焦错误字段；实际按钮可用，用户可能点发送后才发现失败。运行时异常属功能性缺陷。
- 建议：校验结果驱动发送按钮 disabled；`value_` 入参做 String 归一。此两条归 L 的实施范围，本报告不修。

### P2（打磨项）

**P2-1 错误文案透传原始串**：`?err=1` 下 Topic 树错误区与顶部错误横幅均直接显示 `connection lost (fixture error injection)` 英文原文；`friendlyKafkaError` 未覆盖的串原样透传。建议错误映射兜底 + 中性化文案 + 横幅里保留原始串于悬停（现已有 title，但正文仍是原文）。
**P2-2 错误横幅遮挡导航**：横幅 fixed 居中悬于 tab 栏上方，出现时盖住"生产/Topic"等页签。建议下移或让出 tab 栏高度。
**P2-3 Topic 管理表排序与侧栏树不一致**：树序 order-events/payment-gateway/user-signup/codec-lab/_schemas/connect-offsets；表序 order-events/user-signup/payment-gateway/connect-offsets/_schemas/codec-lab。两处规则不统一，建议同一默认排序。
**P2-4 Stream 面板"更早/更新"按钮 36×20px**：低于 24px 最小可用热区，实际点击易脱靶。建议加大 padding 或热区。
**P2-5 消息详情抽屉标题语义不明**：标题呈 `order-events · 0 / 0`，分区与 offset 未标注，易读作"0/0 进度"。建议 `分区 0 · offset 0`。
**P2-6 Schema 面板当前兼容级别徽标显示"—"**：位于兼容性下拉与"应用"之间，warn 样式小胶囊，形似禁用按钮；初次使用不知其含义。建议加前缀文案"当前：—"。
**P2-7 只读模式"发送"禁用态不醒目**：`?ro=1` 生产面板"发送"仍呈蓝色主按钮，禁用对比弱（面板顶部有"只读连接：已禁用生产"提示文字兜底）。建议强化 disabled 视觉。
**P2-8 Glue 模式"SR 解码挂载"复选框无禁用视觉**：消息面板底部有小字说明"SR 解码挂载不可用"，但复选框本身看不出 disabled。建议同步 disabled 态 + 行内提示。
**P2-9 ACL 面板空态裸**：查询前/后均为一行灰字"没有匹配过滤条件的 ACL"，无表头无引导。建议空态给列骨架或"点击查询"引导。
**P2-10 数据网格单元格无可见焦点指示**：Tab 进入 ag-grid 后 activeElement 为单元格 DIV，computed outline 为 none 且无 box-shadow，键盘用户无法定位焦点位置（ag-grid 默认行为，需复核主题覆盖）。
**P2-11 浅色主题工具栏整体泛红**：默认连接色为红色系，light 下 `color 10% alpha` 染红整条工具栏+左侧红竖条，易误读为错误/告警态。建议降饱和或中性兜底色。
**P2-12 Groups 面板状态值未本地化**：消费组状态列直接显示 `Stable` 英文枚举。
**P2-13 720px 窄视口侧栏下方留大块死空间**：窄视口布局纵向堆叠后 Topic 列表区与主区之间有大段空带，可用但松散；建议侧栏列表高度自适应内容。
**P2-14 mock 页 favicon 404**：每次加载控制台报一条 404，噪音；可在 mock.html 内联 data-icon。
**P2-15 Topics 工具栏图标按钮依赖悬停提示**：位点/配置/扩分区等按钮在未选中行时禁用，仅靠 title 说明，可发现性一般（title 齐全，接受现状）。

### 已被并行实施路覆盖的项（避免重复实施）

| 项 | 归属 | 本扫描动作 |
| --- | --- | --- |
| ProducePanel/CodeEditor 编辑器体验（换行、行号、字数统计 32 字符、headers 校验红框） | agent L | 仅记录 P1-4 两条，不实施 |
| TopicTree 侧栏（过滤 1/6 计数、清除按钮、折叠侧栏、`/` 聚焦过滤框） | agent M 已交付 | 复核通过：`/` 聚焦成功、过滤即时生效、折叠按钮存在 |
| style.css 主题令牌/明暗切换 | agent M 已交付 | 复核通过：dark/light 两套渲染正常、无拼色 |

## 三、面板逐一走查矩阵

走查对象 12 个（9 页签 + TopicTree 侧栏 + 连接弹窗 + 消息详情抽屉；AuditFeed 为事件驱动随面板出现）。

| 面板 | 默认 dark | light | ro=1 | glue=1 | 720px | 1440px | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| TopicTree 侧栏 | ✓ | ✓ | ✓ | ✓ | ✓(死空间) | ✓ | `/` 快捷键、过滤、折叠均正常 |
| Messages 消息 | ✓ | ✓ | ✓ | ✓(提示齐) | ✓ | ✓ | P1-1/P1-2/P1-3、抽屉标题 P2-5 |
| Stream 流式 | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | 空态/统计行清晰；P2-4 小按钮 |
| Produce 生产 | ✓ | ✓ | ✓(禁发+提示) | ✓ | ✓ | ✓ | L 改动区：P1-4；行内校验 UI 本身良好 |
| Topics 管理 | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | 6 行齐全；P2-3 排序、P2-15 |
| Groups 消费组 | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | P2-12 Stable 英文 |
| Brokers | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | 无发现 |
| ACL | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | P2-9 空态裸 |
| Schema | ✓ | ✓ | ✓ | ✓(徽标正确) | ✓ | ✓ | P2-6；Glue 下注册表徽标/枚举正确 |
| Monitor 监控 | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | 空态文案引导明确 |
| Connections 弹窗 | ✓ | ✓ | ✓(只读/禁删徽标) | — | ✓ | ✓ | P1-2 Esc、P1-3 焦点 |
| AuditFeed 审计条 | ✓(默认隐藏) | — | — | — | — | — | mock 未产生 kafka/audit 事件，可视化未验证（遗留） |
| 错误态 `?err=1` | ✓ | — | — | — | — | — | P2-1/P2-2 |
| 初始化态 `?noconn=1` | ✓ | — | — | — | — | — | 本地化文案清晰，无发现 |

## 四、验证与遗留

- `pnpm typecheck` 退出码 0（只读确认，L 半成品可编译）。
- 走查面板数：12+2（错误态/初始化态夹具）；发现条数：P0=0、P1=4、P2=15。
- 遗留未验证：AuditFeed 实际事件到达时的自动展开/高亮（mock 夹具未推 `kafka/audit`，需真机或补夹具验证）；ag-grid 焦点指示是否被插件主题覆盖（P2-10 需在宿主真机复核）。
- 走查用截图已全部删除，未入工作区；`/tmp/uiscan-n` 下的脚本与截图为扫描工具产物，不入库。

## 五、收口状态（实施侧回填，2026-09-05）

P1 × 4 全部关闭；P2 除 P2-15（观察保留）与 P2-10（CSS 修复已落，宿主真机复核待做）外全部关闭。

| 项 | 状态 | 收口轮次与方式 |
| --- | --- | --- |
| P1-1 | ✅ 关闭 | 轮·五「R 路」：消费表单分组折叠 + 首屏让位给结果区 |
| P1-2 / P1-3 | ✅ 关闭 | 轮·六：弹层 Esc + 焦点陷阱 + 焦点归还（`decideModalKeydown` 单测） |
| P1-4 | ✅ 关闭 | L 路：校验联动发送按钮 + `value_` String 归一 |
| P2-1 | ✅ 关闭 | `friendlyKafkaError` 网络类规则覆盖夹具串；横幅正文本地化、原文留在 title 悬停 |
| P2-2 | ✅ 关闭 | 轮·六：横幅 top 70px，让出工具栏+页签栏 |
| P2-3 | ✅ 关闭 | TopicsPanel 与侧栏树同源 `sortTopics` 排序 |
| P2-4 | ✅ 关闭 | `.stream-pager` ≥32px 热区 + 禁用态 title |
| P2-5 | ✅ 关闭 | 轮·六：抽屉标题 `topic · 分区 N · Offset N` |
| P2-6 | ✅ 关闭 | 徽标加 `当前兼容级别:` 前缀（复用既有文案键，七语无新增） |
| P2-7 | ✅ 关闭 | 发送按钮禁用降饱和 + 全局 not-allowed 光标 |
| P2-8 | ✅ 关闭 | 轮·七：禁用态规则收敛至全局 style.css 后覆盖 MessagesPanel 解码组复选框（此前仅 Stream/Produce 面板局部生效），走查实测 cursor=not-allowed、透明度 0.45/0.55 |
| P2-9 | ✅ 关闭 | ACL 空态附过滤引导行（复用既有文案键） |
| P2-10 | 🟡 CSS 已落 | DbxAgGrid `.ag-cell-focus` primary 描边（键盘聚焦才显示）；宿主真机复核待做 |
| P2-11 | ✅ 关闭 | 轮·六：light 连接色染色 10%→5%，主线索由 4px 色条承担 |
| P2-12 | ✅ 关闭 | 消费组状态列 `groups.state*` 七语映射（未知枚举原文兜底） |
| P2-13 | ✅ 关闭 | 轮·六：窄视口侧栏高度内容自适应 |
| P2-14 | ✅ 关闭 | 轮·六：mock 内联 data-icon |
| P2-15 | 👀 观察保留 | title 齐全，接受现状 |

### 轮·七新增打磨（本轮）

- **页签栏选中 topic 徽标限宽**：长 topic 名（>220px）省略号裁切 + title 悬停看全名，不再撑爆页签栏（9 页签 + 徽标同排）。
- **页签栏窄视口/长语言兜底**：页签按钮 `flex-shrink:0` 不压缩变形，`tab-bar` overflow-x:auto 可横向滚动。
- **禁用态统一收敛**：cursor not-allowed + `.checkbox` 复选框禁用透明度从 Stream/Produce/Groups 三个面板的 scoped 重复块收敛到全局 style.css 一份（面板特例如发送按钮降饱和保留在 scoped 层），并顺带补齐 MessagesPanel/AclsPanel/SchemasPanel/ConnectionsPanel 等未覆盖面板。

## 六、第 2 轮扫描（场景驱动全走查，2026-09-06）

> 扫描角色：持续 UI 体验扫描 agent（只读扫描 + 报告，不实施修复）。本轮按用户场景端到端走查：第 1 轮 P1 逐条复现复核 + 第 1 轮未深入路径补齐（Stream/Monitor/ACL 编辑流/CodeEditor 深度交互/Schemas 与 Topics 编辑流）+ 新 fixture 模式覆盖。

### 6.1 扫描环境

| 项 | 值 |
| --- | --- |
| 轮次 | 第 2 轮场景驱动走查（单轮完成，独立端口不复用 5283 旧进程） |
| Dev server | `vite --port 5294 --strictPort`（kafka/frontend 工作目录，IPv6 ::1，结束时已 kill） |
| 自动化 | playwright-core + 系统 Chrome（headless + `--disable-gpu`），独立实例装于 `/tmp/uiscan-kafka`（未进项目依赖） |
| 视口矩阵 | 1280×900（默认）、720×900（窄） |
| URL 参数矩阵 | 默认 / `?theme=light` / `?ro=1` / `?glue=1`（夹具复核，本轮走查以源码为准）/ `?err=1` / `?noconn=1`（第 1 轮已验，未重复）/ **`?nodelete=1`、`?big=1`、`?locale=en`（第 1 轮未覆盖的新夹具）** |
| 交互核验 | Tab 顺序、`/` 快捷键、Esc 逐层关闭（含子弹层层级）、Enter/按钮禁用态、表单校验、生产→消费端到端 |
| 证据 | 截图 12 张（分析核对后已全部删除，未入库） |
| 控制台健康度 | 全程 0 console error / 0 pageerror（除一次性环境级渲染进程崩溃，见 6.5） |

### 6.2 第 1 轮发现复核结果

**P1 × 4 全部确认已修复**（收口声明与实测一致）：

| 项 | 结论 | 实测证据 |
| --- | --- | --- |
| P1-1 | ✅ 已修复 | 选中 topic → 消费后表单自动折叠为摘要条（top 66px/35px 高），结果统计条 top 101、网格 top 134、首行 top 162，全部在 900px 首屏内 |
| P1-2 | ✅ 已修复 | 消息详情抽屉：打开时焦点移入抽屉关闭钮，Esc 关闭、焦点回网格单元格；连接弹窗 Esc 关闭同样通过 |
| P1-3 | ✅ 已修复 | 弹窗打开焦点进首个控件；连 Tab 10/25 次均循环在弹窗内不出背景；Esc 后焦点归还工具栏「连接」触发按钮 |
| P1-4 | ✅ 已修复 | A：headers 输入 `{bad json` → 行内红字 + 发送按钮 `disabled=true`；B：7 个 number 输入逐一空/负/超大值填入，0 pageerror（`value_` 崩溃不复现）；C：合法 headers 后正常发送（生产 5 条成功返回 offset 6） |

**P2 抽测复核**（复现第 1 轮场景逐项验证）：

| 项 | 结论 |
| --- | --- |
| P2-3 排序一致 | ✅ 侧栏树与 Topic 管理表同为 order-events/payment-gateway/user-signup/codec-lab/_schemas/connect-offsets |
| P2-4 流分页热区 | ✅ 实测 52×32px |
| P2-6 兼容级别徽标 | ✅ 显示「级别: BACKWARD」前缀 |
| P2-7 ro 发送禁用视觉 | ✅ disabled + 降饱和；ro 下编辑器 contenteditable=false、顶部只读提示在位 |
| P2-9 ACL 空态 | ✅ 空态附「资源名 / 主体 → 查询」引导行；空过滤查询有「过滤条件过宽：请填写资源名或主体」行内拦截 |
| P2-10 网格焦点描边 | ✅ fixture 层面 cell focus 后 outline solid 2px（宿主真机复核仍待做，维持 🟡） |
| P2-11 light 工具栏染色 | ✅ 实测 `rgba(225,29,72,0.05)`，前景 rgb(10,10,10) 对比正常 |
| P2-13 窄视口死空间 | ✅ 720px 下树高 237px 内容自适应、无横向溢出（scrollWidth=720）、页签栏 overflow-x:auto |
| P2-14 favicon | ✅ 内联 data-icon，无 404 |
| **P2-12 组状态本地化** | ❌ **未生效（回归）**：zh-CN 下消费组状态列仍显示英文 `Stable`。根因：`groups.state*` 文案键实际挂在 `i18n` 的 `messages.state*` 命名空间（七语皆是），而 `kafkaColumns.groupColumns()` 的 valueFormatter 按 `groups.state${Pascal}` 查找 → `t()` 返回键名 → 回退原文。收口声明「groups.state* 七语映射」与实际键位不符，整条链路未打通 |
| P2-15 | 👀 维持观察 |

### 6.3 第 2 轮新发现

统计：**P0 × 0，P1 × 1，P2 × 4**。

### P1-5 弹层 Esc/焦点管理只覆盖 2 处，其余 9 个弹层（含 1 个抽屉）均无 Esc、无焦点陷阱
- 位置与复现：
  - AclsPanel × 3：行详情抽屉、创建弹窗、删除弹窗——`mock.html` → ACL 页 → 查询 → 点行开抽屉/点「创建」「删除」。实测三处 Esc 均无响应（抽屉关闭仅能点遮罩，且遮罩拦截全页点击；创建弹窗打开后焦点仍留在触发按钮，Tab 直接落到背景）。
  - TopicsPanel × 4：创建 / 删除 / 扩分区 / 配置弹窗。
  - SchemasPanel × 3：注册 / 兼容检查 / 删除弹窗。
  - GroupsPanel × 2：重置位点 / 删除组弹窗。
  - BrokersPanel × 1：broker 配置弹窗。
- 对照组（已正确实现，可作为统一方案参照）：MessagesPanel 抽屉、ConnectionsPanel 主弹窗（App 壳层 `decideModalKeydown`）、ConnectionsPanel 导入助手子弹层（捕获态拦截，实测 Esc 层级 2→1→0 正确：只关子弹层不透传）。
- 期望 vs 实际：第 1 轮 P1-2/P1-3 的收口只覆盖了消息抽屉与连接弹窗；同类弹层在其他 6 个面板全部缺失，破坏「Esc 关闭当前弹层 + 焦点圈定/归还」的一致性预期。删除/重置类危险确认弹窗尤其依赖 Esc 快速退出。
- 建议：把 `decideModalKeydown` + 焦点陷阱下沉为共享弹层行为（如 `shared/frontend/` 适配层或本插件 lib 内组合式函数），各面板 modal/drawer 统一挂载；注意子弹层（导入助手）已单独处理，合并时保留其捕获态语义。

### P2-16 ACL 创建校验复用 topic 专属文案，语义错位
- 位置：AclsPanel `submitCreate`（`components/AclsPanel.vue`）。
- 复现：ACL 页 → 创建 → 资源名/主体留空直接保存。
- 现象：错误横幅显示「名称、分区数与副本因子均为必填」（`topics.createInvalid`），而 ACL 表单根本没有分区数/副本因子字段。
- 建议：新增或改用 ACL 语义的必填校验文案（资源名、主体必填），七语同步。

### P2-17 `topics.created` 文案键七语全缺，成功提示显示原始键名
- 位置：TopicsPanel 创建成功通知（`components/TopicsPanel.vue:131` → `t("topics.created")`）。
- 复现：Topics 页 → 创建 → 填名保存。
- 现象：通知条显示 `topics.created: scan-topic-x`（键名 + 冒号拼接），zh-CN 与 `?locale=en` 下同样复现；对比删除通知「Topic 已删除: …」正常。违反工作区七语文案硬性规则。
- 建议：`topics.created` 七语补键（模板含 `{name}` 占位），或复用既有键调整拼接方式。

### P2-18 错误注入下 Topic 树错误区显示英文原文且无悬停原文
- 位置：TopicTree `.tree-error`（`components/TopicTree.vue:175`，直接渲染 App 传入的 `topicsError` 原始串）。
- 复现：`?err=1` → 观察 Topic 树区域。
- 现象：显示 `connection lost (fixture error injection)` 英文原文，且无 title 悬停。第 1 轮 P2-1 收口只本地化了 App 错误横幅（`friendlyKafkaError`），树区错误未走同一映射。
- 建议：`loadTopics` 的 catch 或 TopicTree 展示层接入 `friendlyKafkaError`，与横幅同源规则。
- 附带确认：`?err=1` 下流面板「开始」按钮因无 topic 正确禁用，无白屏/报错。

### P2-19 ag-grid 分页文案中英混排
- 复现：`?big=1` → 消费组页（100 组，每页 50）。
- 现象：分页条显示「每页条数： 50 1 to 50 of 100 页 of 2」——「页」已翻译，"1 to 50 of 100" 保持英文，中英混排观感差。
- 建议：`agGridLocaleText()` 补齐 pagination 相关键（`paginationFirst`/`paginationLast`/`paginationTotal`/`paginationPageSizeSelectorLabel` 等），七语同步。

### 6.4 本轮确认无问题的路径（第 1 轮未深入，本轮补齐）

- **StreamPanel**：开始/暂停（「已暂停」徽标）/恢复/停止全链路；500ms tick 消息持续到达；切走面板再切回，App 背压缓冲按序补发（已扫描 30 无丢失）；「更早/更新」分页按钮可用；`?err=1` 下降级正常。
- **MonitorPanel**：未选消费组时「开始采样」禁用；选 billing-consumer 后采样（总 lag：2）、lag 表 2 行；≥2 个样本后趋势 SVG + 阈值线渲染；阈值调 1 触发「总 lag」badge-danger + 本地化告警横幅「Lag 超过阈值：总 lag 2 > 1」；方案保存/加载；停止采样恢复。
- **ACL 编辑流**：空过滤行内拦截（防过宽查询/删除）；查询→详情→创建（空名校验拦截 → 合法创建成功「ACL 已创建」并自动按新 ACL 过滤刷新）；删除确认弹窗展示将要匹配的过滤 JSON（危险操作语义清晰）。
- **CodeEditor 深度交互**：行号（26px）与字数统计正常；编辑器内 Tab 缩进、Esc+Tab 退出到外部控件（CodeMirror 惯例可用）；坏 JSON 行内提示与 aria 标注。
- **生产→消费端到端**：发送 5 条 → 通知「消息已发送 · 分区 0，offset 6」→ 消息面板消费「已扫描 8 · 命中 8」（3 种子 + 5 新产）；JSON/CSV 导出按钮启用且导出有「已导出 JSON」反馈。
- **Schemas 编辑流**：subject 选择 → 版本表 → v1↔v2 对比（摘要 `+3 -0 ~0` + detail 区）；注册坏 JSON 双重提示（横幅 + 弹窗内联）且弹窗保持、合法 JSON 注册成功「已注册 v1（id 106）」；Confluent/Glue 注册表切换徽标正确。
- **Topics 编辑流**：空名创建被横幅校验拦截；创建成功入树；删除确认弹窗要求输入完整 topic 名——不匹配时删除钮 disabled（防误删）、匹配后可删、成功通知。
- **ConnectionsPanel**：状态行 SR 徽标（已启用/「SR 未启用」对照行）、只读/禁删徽标、Esc 关闭；导入助手子弹层 Esc 层级正确。
- **新夹具模式**：`?nodelete=1` 工具栏「禁删」徽标；`?big=1` 507 个 topic 树 2.2s 渲染、heap 29MB、消费 100 条上限徽标「已达条数上限」、组表分页正常——无卡死无报错。
- **交互细节**：首屏 Tab 顺序合理（连接 → 刷新 → 页签 → 面板）；`/` 聚焦树过滤、Esc 清空过滤并保留焦点。

### 6.5 环境异常与遗留

- headless Chrome 渲染进程崩溃 2 次（一次发生在 `?big=1` 场景 GPU 进程退出，一次为孤发 "Target crashed"；与并行 agent 的资源争用疑似相关）。加 `--disable-gpu` 后全程稳定复跑无复现，判定为环境异常而非产品缺陷，未计入发现。
- AuditFeed 可视化仍未验证（与第 1 轮相同）：mock 夹具的 denied audit 事件只在「策略层拒绝」时产生，而 ro 模式下前端已把所有写入口禁用，事件链不可达；需真机（后端 policy 真实拒绝）或补夹具直推事件。
- P2-10 的宿主真机复核维持待办（本轮仅 fixture 层确认描边存在）。
- 消息面板「条件预设」保存/应用流本轮未走通自动化（选择器未命中，夹具 API 本身存在），留待第 3 轮或真机。

### 6.6 第 2 轮统计

| 类别 | 数量 | 条目 |
| --- | --- | --- |
| 第 1 轮 P1 复核 | 4/4 已修复 | P1-1、P1-2、P1-3、P1-4 |
| 第 1 轮 P2 抽测 | 9 通过 + 1 未生效 + 1 维持 | P2-12 ❌ 未生效（键位错挂 `messages.*`）；P2-10 维持 🟡；P2-15 维持观察 |
| 新发现 P0 | 0 | — |
| 新发现 P1 | 1 | P1-5（弹层 Esc/焦点管理覆盖面） |
| 新发现 P2 | 4 | P2-16（ACL 校验文案错位）、P2-17（`topics.created` 七语缺键）、P2-18（树错误区原文透传）、P2-19（分页文案混排） |
| 面板覆盖 | 9 页签 + 侧栏 + 连接弹窗 + 导入助手 + 消息抽屉 + ACL 抽屉 | 生产/消费端到端、流、监控、Schema、ACL、Topics 编辑流全部走通 |

### 6.7 修复状态回填（实施侧，2026-09-06；只做状态标注，不改写上文）

本轮实施范围为 P1-5 与明确回归项 P2-12，附 P2-16/P2-17 两个文案键项。

| 项 | 状态 | 修复方式与复验结论 |
| --- | --- | --- |
| P1-5 | ✅ 关闭 | 弹层行为下沉为 `kafka/frontend/src/lib/modalBehavior.ts`（`useModalBehavior` 组合式函数：模块级层栈仅栈顶响应 Esc/Tab——照导入助手捕获态语义；决策复用 `kafkaModel.decideModalKeydown`；打开聚焦首控件、关闭归还触发元素），接入全部 13 处弹层：AclsPanel 抽屉/创建/删除、TopicsPanel 创建/删除/扩分区/配置、SchemasPanel 注册/兼容检查/删除、GroupsPanel 重置/删除、BrokersPanel 配置（容器补 `tabindex="-1" role="dialog" aria-modal="true"`）。App 壳层连接弹窗与 MessagesPanel 抽屉的已验证实现保持原样。新增 `modalBehavior.spec.ts` ×5（开焦点/Esc 归还/Tab 双向回绕/层栈逐层 Esc/让位语义）。playwright 复验 13 层 + 连接弹窗对照全部 PASS：焦点入层、Tab×8 不出层、Esc 关闭、焦点归还触发钮（抽屉归还网格单元格） |
| P2-12 | ✅ 真关闭（回归修复） | 键位对齐以 i18n 实际存在的键为准：`kafkaColumns.groupColumns()` 查找 `groups.state*` → `messages.state*`。复验：消费组状态列实测「稳定」（非 Stable 原文）。kafkaColumns.spec 增防回归断言（`messages.state*` 七语键存在性 + 格式化链路 zh-CN 冒烟 + 列头「未解析点分键」护栏）；该护栏顺带抓到并修复 `topics.colOffset` 缺键（topicOffsetColumns 改引用既有 `messages.colOffset`） |
| P2-16 | ✅ 关闭 | 新增 `acls.createInvalid` 七语键，AclsPanel 空名校验不再复用 `topics.createInvalid`。复验：空名保存横幅实测「资源名与主体均为必填」，无分区数/副本因子字样 |
| P2-17 | ✅ 关闭 | `topics.created` 七语补键（en/zh-CN/zh-TW/es/it/ja/pt-BR）。复验：创建成功通知实测「Topic 已创建: scan-topic-fix」，无原始键名 |
| P2-18 | ✅ 关闭（第 3 轮，见 §6.8） | TopicTree 展示层接入 `friendlyKafkaError`（与横幅同源规则），友好化正文 + 原始串留在 title 悬停 |
| P2-19 | ✅ 关闭（第 3 轮，见 §6.8） | `AG_GRID_LOCALE_KEYS` 扩展 ag-grid v36 分页实际消费键（to/of/page/more/firstPage 等），七语内联字典补齐 |

附带（既有测试红转绿过程中暴露并修复，均限 kafka/frontend 内）：
GroupsPanel 行级失败横幅被 reload 起手清错误 emit 立即冲掉（submitReset 改为先刷新详情再上抛结果）；`resetTimestampMs` String 归一（同 ProducePanel P1-4B 范式，部分环境 number input value 非 string）；partitionOffset 校验分支顺序（无效条目优先于必填，`0=abc` 不再误报「必填」）；GroupsPanel.spec mock 桥补 `{error}` 信封→异常拒绝（镜像真实桥形态，符合工作区规则 7）与弹窗断言逐步重查（teleport stub 重渲染替换弹窗元素，过期 wrapper 失效）。

验证：`pnpm typecheck` 0 错；`pnpm test` 11 文件 115 用例全绿；playwright（playwright-core + 系统 Chrome headless `--disable-gpu`，vite :5294）18 项 PASS、0 pageerror；复验截图即删未入库。

### 6.8 第 3 轮修复回填（实施侧，2026-09-06；只加状态标注，不改写上文）

本轮实施范围为 §6.7 留待的 P2-18/P2-19，并补 §6.5 遗留的 AuditFeed denied 事件链夹具。

| 项 | 状态 | 修复方式与复验结论 |
| --- | --- | --- |
| P2-18 | ✅ 关闭 | `TopicTree.vue` 展示层接入 `friendlyKafkaError`（不只在 loadTopics catch——树错误区与 App 错误横幅同一条规则映射）：`.tree-error` 正文渲染友好化文案，友好化结果与原始串不同时原始串挂 title 悬停。复验：`?err=1` 下树错误区实测「无法连接 Kafka broker：请检查 bootstrap servers 与网络」（zh-CN）/「Cannot reach the Kafka brokers: check bootstrap servers and network」（`?locale=en`），title 均为原文 `connection lost (fixture error injection)`，与触发的错误横幅文案逐字一致 |
| P2-19 | ✅ 关闭 | 从 ag-grid 36.1.0 包内确认分页条（PageSummaryComp/RowSummaryComp/paginationComp）实际消费键为 `to`/`of`/`page`/`more`/`number`/`firstPage`/`previousPage`/`nextPage`/`lastPage`/`ariaPageSizeSelectorLabel`（§6.3 建议的 `paginationFirst` 等键名 v36 不存在），全部入 `AG_GRID_LOCALE_KEYS` 并补七语内联字典；zh 组合：行摘要「1 至 50 / 共 201」、页摘要「第 N / 共 5」。复验：`?big=1&locale=en` 分页条实测全英文「Page size: 50 1 to 50 of 201 Page of 5」（无中英混排）；`?big=1`（zh-CN）实测「每页条数： 50 1 至 50 / 共 201 第 / 共 5」，无英文残留 |
| AuditFeed denied 链（§6.5 遗留） | ✅ 夹具补齐 | `mockDbxHost.ts` 新增 `?audit=denied`：宿主 `onEvent` 监听就绪后（轮询 eventListeners 非空）注入 1 条 denied + 900ms 后 1 条 ok 的 `kafka/audit` 事件（镜像 AuditRecord JSON 面；15s 兜底放弃防孤儿 interval）。复验：审计条实测「2 条事件 · 1 条被拒绝」、denied 到达自动展开（aria-expanded=true）、「已拒绝」徽标高亮 + 错误横幅弹出、ok 行对照「成功」；0 console error |
| P2-10 | 🟡 维持 | CSS 已落（§五），宿主真机复核仍待做，本轮未动 |
| P2-15 | 👀 维持观察 | 维持 §五 结论 |

验证：`pnpm typecheck` 0 错；`pnpm test` 11 文件 118 用例全绿（基线 115 + TopicTree.spec ×2、kafkaColumns.spec ×1 新增防回归断言；`AG_GRID_LOCALE_KEYS` 七语键齐由既有键集测试自动守护）；playwright（playwright-core + 系统 Chrome headless `--disable-gpu`，vite :5294）15 项 PASS、0 console error / 0 pageerror；复验截图即删、/tmp 夹具目录已清理。

## 七、第 4 轮（专家视角深度测试，2026-09-06）

> 扫描角色：Kafka 平台工程师 + 软件测试专家双视角（只读扫描 + 报告，不实施修复）。前 3 轮已收口项不重复发现，本轮聚焦：性能压力实测、Kafka 语义正确性、慢响应/断连健壮性、键盘全流程、i18n/a11y 全量走查。

### 7.1 扫描环境

| 项 | 值 |
| --- | --- |
| 轮次 | 第 4 轮专家视角深度测试（单轮完成） |
| Dev server | 复用 :5294 上已存活的 vite 进程（cwd 即 kafka/frontend，dev 模式热加载当前源码；扫描结束时已 kill，端口释放） |
| 自动化 | playwright-core 1.63.0 + 系统 Chrome（headless + `--disable-gpu`），独立实例装于 `/tmp/uiscan-kafka-r3`（未进项目依赖） |
| 视口 | 1280×900 |
| 夹具矩阵 | 默认 / `?big=1`（507 topic + 5000 消息 + 200 组）/ `?err=1`；另用 initScript 注入两类自定义夹具：`invoke` 延迟 1.2s（慢响应竞态）、`defineProperty` 陷阱返回空 topics/groups（空集群空态） |
| 源码交叉验证 | mockDbxHost.ts（夹具语义）、kafkaModel.ts / kafkaColumns.ts（格式化/校验）、kafkaErrors.ts（错误映射）、各面板 Vue（根因定位） |
| 证据 | 中间截图 1 张（dbg1.png，走查后已删）；全部结论以 DOM 断言 + invoke 参数捕获为准 |
| 控制台健康度 | 全部 7 个测试场景 0 console error / 0 pageerror |

### 7.2 新发现清单

统计：**P0 × 0，P1 × 2，P2 × 5**，另记观察项 4 条（不计级）。

### P1-6 消费「起始/结束 offset」范围过滤整体不可用：填值即抛内部异常，请求不发出
- 位置：MessagesPanel `optionalNumber(value: string)`（`components/MessagesPanel.vue` ~L336）→ `value.trim()`；`buildParams` 中 `offsetFrom`/`offsetTo` 两字段。
- 复现：消息页 → 「时间与范围」组 → 「起始 offset」填 1（或只填「结束 offset」2，任一组合）→ 点「消费」。
- 现象：错误横幅显示原始内部错误 `value.trim is not a function`，`kafka/messages/consume` 请求**根本未发出**（invoke 捕获证实只有 `kafka/presets/list`），结果区不更新。
- 根因：Vue 3 对 `<input type="number">` 的 v-model 返回 **number**，`optionalNumber` 直接 `.trim()` 抛 TypeError（try 块内被 `emit("error")` 捕获后透传原文）。与第 1 轮 P1-4B（ProducePanel）和 §6.7 附带修复（GroupsPanel `resetTimestampMs`）完全同范式——这两处都做了 String 归一，唯独 MessagesPanel 的 offset 范围入口漏掉了。
- 影响：offset 范围定位这一消费手段 100% 不可用；且横幅透传内部异常串而非可行动文案。
- 建议：`optionalNumber` 入参 String 归一（`String(value ?? "").trim()`）；补一条「offset 范围消费」端到端单测防回归（前两处修复均无对应 spec 覆盖此面板）。

### P1-7 慢响应竞态跨 topic 串台：在途消费结果落地到新选中的 topic 名下，且无从分辨
- 位置：MessagesPanel `runConsume`（`applyResult(await kafkaApi.messagesConsume(...))` 无请求守卫）+ `watch(() => props.topic)` 只在切换瞬间清一次旧结果。
- 复现（invoke 注入 1.2s 延迟）：选中 order-events → 点消费 → 300ms 内点选 payment-gateway → 等待响应落地。
- 实测落地状态：页签徽标与摘要条均显示 `payment-gateway`，但结果区统计「已扫描 3 · 命中 3」、首行 `o-1 {"orderId":"A-1001"...}` —— **order-events 的消息完整呈现在 payment-gateway 名下**。
- 加重因素：消息表列集无 topic 列（partition/offset/timestamp/key/value/headers/schema）；真实 sidecar 消费超时窗为 5s（timeoutMs 默认 5000），竞态窗口真实存在；在途期间「消费」按钮保持禁用（`consuming` 不随 topic 切换复位），新 topic 无法立即重发。
- 影响：Kafka 控制台最忌「看错 topic 的数据」——用户可能基于串台结果做导出/判断；属可信度级缺陷。
- 建议：`runConsume` 记录发起时的 topic（或递增请求序号），响应落地前与 `props.topic` 比对，不一致即丢弃；顺带评估：结果统计条标注结果所属 topic，或消息表补 topic 列（导出场景同样受益）。

### P2-20 扩分区填更小/相等值时报错文案语义错位
- 位置：TopicsPanel `submitExpand`（`components/TopicsPanel.vue` ~L187）客户端校验复用 `t("err.partition")`。
- 复现：Topic 页 → 选中 order-events（2 分区）→ 扩分区 → 填 1 → 确认。
- 现象：横幅「分区无效或 offset 超出范围」——与「新分区数必须大于当前」的真实原因错位（弹窗 label 虽有「必须大于当前」小字，但报错语义仍混淆）。
- 建议：改用专用文案（如「新分区数必须大于当前值 {n}」），七语同步。

### P2-21 空集群（无任何 topic）时消息面板空态文案误导
- 复现：注入空 topics 夹具 → 树空态正确显示「集群内暂无 topic（0/0）」、消费按钮正确禁用；但消息主区空态为「暂无消息——请调整条件后重新消费」——真实原因是**未选 topic**，该文案引导用户去调消费条件（此场景下无意义）。
- 建议：区分「未选 topic」（引导先在左侧树选择）与「有 topic 无匹配」两种空态。

### P2-22 树过滤结果键盘死角：Enter 不能选首个匹配，逐 Tab 穿树在 big 模式下有 507 个 tab stop
- 复现：`/` 聚焦过滤框 → 输入 order-events → Tab。实测首站是「清除」按钮（按 Enter 会误清过滤），再 Tab 才进入树行按钮列表；Enter 在过滤框内无选中行为。?big=1 下全量树行均为独立 button，键盘用户选中目标需最多 507 次 Tab。
- 建议：过滤框 Enter 选择唯一/首个匹配（唯一时直接选中）；树行容器考虑 roving tabindex 或 listbox 语义，避免全量 tab stop。

### P2-23 数值单元格无千分位，与同屏 ag-grid 分页条格式不一致
- 实测：消费组位点表 lag/end offset 原样渲染（如 `1003`、组合大 lag），而分页条显示「共 5,000」；?big=1 下 lag 普遍 4,700+，裸数字可读性差且两种格式同屏并存。
- 建议：`numberColumn`（kafkaColumns.ts）统一 `valueFormatter` 千分位（en/es/it/pt-BR 与 zh 组内一致即可，跟随宿主 locale）。

### P2-24 错误横幅与成功通知无 aria-live，读屏不感知异步反馈
- 位置：App.vue `.error-banner` / `.notice`（源码无 role/aria-live）；对照 ProducePanel 成功条已带 `role="status"`。
- 影响：异步到达的错误（消费失败、策略拒绝）对读屏用户静默。
- 建议：两容器补 `aria-live="polite"`（error 可用 `role="alert"`），与既有 produce-success 的 role="status" 收敛为同一约定。

### 观察项（不计级）
- 消息时间戳以浏览器本地时区渲染且无时区标注（`formatTimestamp`），与 kcat/其它工具跨时区比对时易歧义。
- ACL `PATTERN_TYPES` 缺 `TYPE`（Kafka 协议 PatternType 枚举含 TYPE，极少用）；资源类型/操作/权限枚举其余与协议完全一致（含 `TRANSACTIONAL_ID`、`DELEGATION_TOKEN`、`IDEMPOTENT_WRITE`、`CLUSTER_ACTION`）。
- 生产面板不显示当前 topic 的分区数，合法 partition 范围需回树/Topic 页确认。
- ja 页签「プロデュース」为音译，与 ja Kafka 社区惯用语（送信/プロデューサー）有出入；页签级改动影响面大，仅记录。

### 7.3 已复核无问题维度（本轮实测内容与结论）

| 维度 | 实测内容 | 结论 |
| --- | --- | --- |
| 性能（?big=1） | 507 topic 树渲染、过滤响应、选中响应；5000 条消费（limit 手动设 5000）：首行 102ms、末页翻页 630ms；heap 31.7→34.7→43MB（消费→翻页→过滤后）；组表 200 行翻页滚动 | 全部无卡顿无报错；过滤 2ms / 选中 17ms 属即时响应；内存无异常增长 |
| Kafka 语义·生产 | key+value+headers+partition=1+count=2 组合发送（回显「分区 1，offset 2」且参数捕获逐字段正确）；空 key 不发 `key` 字段（符合后端二选一契约）；1MB 大消息 1671ms 发送成功、字数统计「1048576 字符」；count 填 5000 → 按钮「发送 ×1000」且实际发送 count=1000；双击/三连击发送均受 `sending`/`sendDisabled` 守卫 | 通过 |
| Kafka 语义·消费 | earliest/最新基线；分区过滤 `1` 只出分区 1；timestamp 策略 `offsetTime` 参数正确（datetime-local → UTC ISO）；per-partition 精确位点 `0=2,1=0` 生效（已扫描 1 · 命中 1）；limit=2 触发「已达条数上限」徽章；commit×过滤互斥、groupId×partitions 互斥（后者以禁用态双向强制）；详情抽屉标题「topic · 分区 N · Offset N」、headers 展示、Esc 关闭 | 通过（除 P1-6 offset 范围） |
| Kafka 语义·Topic/Schema/ACL/组 | 扩分区合法路径（3→4，`topics/partitions/update` 参数正确 + 通知 + 树刷新）；扩分区弹窗默认值为当前+1；schema 兼容检查：坏 JSON → 「不兼容」badge-danger + 原因消息，合法 JSON → 「兼容」badge-ok；ACL 查询→创建弹窗枚举矩阵（见观察项）；消费组状态 zh 本地化「稳定」、总 lag 计算正确、位点/成员/订阅三子表齐全 | 通过 |
| 健壮性·错误与竞态 | `?err=1` 全面板：树错误区本地化+title 原文（P2-18 修复确认仍生效）、流「开始」正确禁用、生产发送禁用、无崩溃；弹层连按 6 次开关（连接弹窗、消息抽屉）无残留 backdrop、焦点归还触发钮；删除确认 3 连点仅 1 次 delete 调用（busy 守卫）+「Topic 已删除」通知；流式 10s tick 稳定（heap 26MB）；ag-grid 5000 行结果快速翻页+滚动 0 错误 | 通过（除 P1-7 慢响应竞态） |
| UX·键盘 | 首屏 Tab 顺序：连接 → 刷新 → 9 页签 → 树刷新 → 折叠 → 过滤框；`/` 聚焦、Esc 清过滤并保留焦点 | 通过（树内选择死角见 P2-22） |
| UX·危险操作与空态 | 删 topic 双确认（输入完整名 + 不匹配禁删）；重置位点默认 earliest、partitionOffset 空值行内拦截且弹窗保持；删除组有后果说明（无输入确认，与删 topic 分级合理）；只读下生产禁用 + title/aria 原因「请填写消息内容」/「只读连接」 | 通过（空集群空态见 P2-21） |
| i18n | en/ja/zh-TW × 9 页签全量走查：0 原始键名、0 中英混排、en 无 CJK 泄漏；术语抽查（Group/State/Coordinator/Offset/Broker/Rack、rf 列 RF）符合惯例 | 通过 |
| a11y | 13 处弹层逐个实测：连接弹窗、Topics ×4、Groups ×2、Brokers 配置、ACL 创建、Schema 注册、消息抽屉——全部 `role="dialog"` + `aria-modal="true"` + 标题结构（P1-5 修复面确认无遗漏）；dark/light 关键前景/背景对比抽样正常 | 通过（aria-live 见 P2-24） |

### 7.4 第 4 轮统计

| 类别 | 数量 | 条目 |
| --- | --- | --- |
| 新发现 P0 | 0 | — |
| 新发现 P1 | 2 | P1-6（offset 范围输入内部异常、功能不可用）、P1-7（慢响应竞态跨 topic 串台） |
| 新发现 P2 | 5 | P2-20（扩分区报错文案错位）、P2-21（空集群空态误导）、P2-22（树过滤键盘死角 + 507 tab stop）、P2-23（数值无千分位且与分页条不一致）、P2-24（横幅/通知无 aria-live） |
| 观察项 | 4 | 时间戳时区标注、ACL PatternType 缺 TYPE、生产面板不显示分区数、ja「プロデュース」术语 |
| 已复核无问题 | 9 维度 | 性能、生产语义、消费语义、Topic/Schema/ACL/组语义、错误与竞态健壮性、键盘、危险操作、i18n、a11y（见 7.3） |
| 控制台健康度 | 7 场景 | 0 console error / 0 pageerror |

### 7.5 环境收尾

- dev server（:5294，含复用的既有进程）已 kill，端口释放；`/tmp/uiscan-kafka-r3` 下截图已删（脚本与 node_modules 为扫描工具产物，不入库）。
- 本轮未修改任何产品代码、未提交 git。

### 7.6 第 4 轮修复回填（实施侧，2026-09-06；只加状态标注，不改写上文）

本轮实施范围为 §7.2 全部 7 条（P1 × 2、P2 × 5）。全部改动限 `kafka/frontend/` 内。

| 项 | 状态 | 修复方式与复验结论 |
| --- | --- | --- |
| P1-6 | ✅ 关闭 | `MessagesPanel.vue` `optionalNumber` 入参 String 归一（`String(value ?? "").trim()`，与 P1-4B/GroupsPanel 同范式）。复验：消息页「起始 offset 0 / 结束 offset 1」→ 点消费，invoke 捕获实测 `kafka/messages/consume` 请求发出且带 `offsetFrom:0, offsetTo:1`，结果区「已扫描 3 · 命中 3」正常渲染，无 `value.trim is not a function` 横幅；单测 `MessagesPanel.spec` ×2（双端填值 / 仅填 offsetTo）守护 |
| P1-7 | ✅ 关闭 | `runConsume` 引入请求序号守卫 `consumeSeq`：响应（含失败）落地前比对序号，不一致即丢弃；topic 切换 watch 自增序号并复位 `consuming`（新 topic 可立即重发，旧请求 finally 不再抢先解锁）。复验：invoke 延迟 1.2s 注入下，order-events 消费在途 300ms 内切 payment-gateway 并重发——落地结果实测「已扫描 1 · 命中 1」+ 详情抽屉 `payment-gateway · 分区 0 · Offset 0`（串台的 order-events 3/3 未出现）；单测 `MessagesPanel.spec` 竞态用例（deferred promise 手控时序）守护 |
| P2-20 | ✅ 关闭 | `TopicsPanel.submitExpand` 改用专用文案键 `topics.expandCountInvalid`（「新分区数必须大于当前值（当前 {count}）」），不再错位复用 `err.partition`；七语同步补键。复验：order-events（2 分区）扩分区填 1 → 横幅实测「新分区数必须大于当前值（当前 2）」且 `topics/partitions/update` 零调用；单测 `TopicsPanel.spec` ×3（小值/相等值/合法值）守护 |
| P2-21 | ✅ 关闭 | `MessagesPanel` 空态分两态：未选 topic → 新键 `messages.uiNoTopicSelected`（「请先在左侧树中选择 topic 再消费」，七语）；已选 topic 无结果/无匹配 → 维持既有「请调整条件后重新消费」。复验：defineProperty 陷阱空集群夹具下，树空态「集群内暂无 topic」+ 消息主区实测「请先在左侧树中选择 topic 再消费」，不再出现误导文案；单测 `MessagesPanel.spec` 空态两态用例守护 |
| P2-22 | ✅ 关闭（收敛方案） | 改动小且可验证的方案：① 过滤框 Enter 选中首个（含唯一）匹配项并 emit select；② 清除钮 `tabindex="-1"` 移出 Tab 序（键盘清空走既有 Esc 路径），过滤激活时 Tab 从过滤框直达首个树行。roving tabindex / listbox 化（507 tab stop 全量收敛）留作后续增强。复验：过滤「payment」→ Enter 实测页签徽标与选中态均 payment-gateway；Tab 后 `document.activeElement` 为 `.tree-row`（非清除钮）；单测 `TopicTree.spec` ×3（Enter 选中/无匹配不误发/清除钮 tabindex + 鼠标路径仍可用）守护 |
| P2-23 | ✅ 关闭 | `kafkaColumns.numberColumn` 统一 `valueFormatter`：`Intl.NumberFormat(工作台 locale)` 千分位（实例按 locale 缓存），字符串数字（startOffset/endOffset 的 VM 为 string）同样格式化，非数值占位（「—」）与空值原样透传——与 ag-grid 分页条「共 5,000」同屏一致。复验：`?big=1` 消费组位点表（bulk-consumer-001）实测单元格 `1,250 / 1,149 / 1,199` 等千分位；单测 `kafkaColumns.spec` ×2（千分位 + 占位透传）守护 |
| P2-24 | ✅ 关闭 | `App.vue` 错误横幅补 `role="alert"`（隐含 assertive live），成功通知补 `role="status" aria-live="polite"`——与 ProducePanel 成功条既有 `role="status"` 收敛为同一约定。复验：`?audit=denied` 异步错误横幅实测 `role="alert"`；生产成功通知实测 `role="status"` + `aria-live="polite"`；单测 `App.spec` ×2（审计 denied 链触发横幅 + MessagesPanel notify 触发通知）守护 |

观察项 4 条（时间戳时区标注、ACL PatternType 缺 TYPE、生产面板分区数、ja「プロデュース」术语）本轮未动，维持不计级。

> Phase 3 追记（2026-09-07）：观察项中「生产面板分区数」「时间戳时区标注
> （Messages 面板）」已由 Phase 3 特性追赶收口（IMPL_PLAN §12.8 /
> PROGRESS-P §9，F6-4 分区数徽标 + 校验、F6-3 本地/UTC toggle）；其余 2
> 条维持不计级。

验证：`pnpm typecheck` 0 错；`pnpm test` 14 文件 132 用例全绿（基线 118 + MessagesPanel.spec ×4、TopicsPanel.spec ×3、App.spec ×2、TopicTree.spec ×3、kafkaColumns.spec ×2 新增防回归）；playwright（playwright-core 1.63.0 + 系统 Chrome headless `--disable-gpu`，独立实例 `/tmp/uiscan-kafka-r4`，vite :5294）8 项 PASS、0 console error / 0 pageerror；复验截图即删、dev server 已 kill、/tmp 夹具目录工具产物不入库。

## 八、第 5 轮（mock 夹具走查 + 修复，2026-09-27）

> 走查角色：基于 `frontend/mock.html` 夹具的 UI 美化 / 体验走查 + 即时修复（本轮扫描与实施同轮完成）。前 4 轮已收口项全部复核通过（P1-1～P1-7、P2-1～P2-9/P2-11～P2-13/P2-16～P2-20/P2-23/P2-24 实测在位），本轮只记新发现。

### 8.1 走查环境

| 项 | 值 |
| --- | --- |
| Dev server | `vite --port 5317 --strictPort`（kafka/frontend，IPv4 127.0.0.1，走查后已 kill，端口释放） |
| 自动化 | ZCode 内置浏览器（IAB）+ Playwright 表面，1280×900 与 720×900 双视口 |
| 夹具矩阵 | 默认 / `?theme=light&err=1` / `?audit=denied` / 开合记忆恢复路径 |
| 走查面 | 9 页签 + 树 + 连接弹窗 + 消息抽屉 + 条件抽屉 + 消费组详情；消费/流式/生产语义抽测 |

### 8.2 新发现与修复回填

统计：**P0 × 0，P1 × 0，P2 × 3，夹具失真 × 1（本轮全部关闭）**。

| 项 | 级别 | 现象与根因 | 修复方式与复验结论 |
| --- | --- | --- | --- |
| F5-1 | 夹具失真 | `mock.html` 自行 `createApp(App).mount`，绕过真实入口 `main.ts`——`installHostThemeBridge()` 从未执行，`--overlay` 未定义，`.modal-backdrop`/`.drawer-backdrop` 计算背景 `rgba(0,0,0,0)`，弹窗/抽屉遮罩在夹具中完全透明（真机不可复现，夹具失真误导走查） | `mock.html` 改为 `import mockDbxHost → import main.ts`（与 index.html 同一启动路径：主题桥 + pluginStore.ready + 挂载）。复验：桥样式节点在位、`--overlay` 解析为 `color-mix(background 70%)`，连接弹窗遮罩实测变暗 |
| F5-2 | P2 | `topicRequired` 键只存在于 `stream:` 命名空间，MessagesPanel 查 `messages.topicRequired` → 消费钮禁用悬停与 intent 拒绝原因透出原始键名（P2-12/P2-17 同类） | 按键位对齐范式改查 `stream.topicRequired`（七语齐备，零新增键）。复验：zh-CN 实测「请先选择 topic」；spec 守卫键存在性 + title 非原始键 |
| F5-3 | P2 | 消费条件抽屉（consume-drawer）无 Esc 处理（模板注释声称「遮罩/Esc 收起」但从未实现）——P1-5 弹层统一收口的漏网层 | 接入统一 `useModalBehavior`（容器补 `tabindex="-1"` + `role="dialog"` + `aria-modal`）：Esc 关闭、Tab 陷阱、打开聚焦首控件、关闭归还触发钮；`registerIfOpenOnMount` 覆盖开合记忆恢复态（初始即开时只补层栈不抢焦点，Esc 同样可关）。复验：真实浏览器实测开→焦点入抽屉→Esc 关→焦点归还四步全过；恢复态 Esc 亦过 |

新增防回归：`MessagesPanel.spec` ×2（Esc 关闭 + 焦点归还；title 非原始键）、`modalBehavior.spec` ×1（mount 即开层注册不抢焦点 + Esc 可关）。`useModalBehavior` 新增可选 `registerIfOpenOnMount`（向后兼容，缺省行为不变）。

走查确认无新增问题的维度：消费→结果→导出链路、流式启停、生产面板（分区数徽标/校验）、Topic/消费组（状态本地化、位点/成员子表、千分位）/Broker/ACL（空态引导）/Schema（级别前缀）/监控、`?err=1` 树错误本地化 + 横幅让位 70px、`?audit=denied` 审计链、浅色主题、720px 窄视口（侧栏内容自适应、页签横向滚动）。

另记（不计级，维持前轮观察）：页签切换瞬间截图曾见「旧页签高亮 + 新面板内容」组合，经 DOM 断言复核为懒加载 chunk 阻塞下的截图时序伪影（DOM is-active 恒正确），非产品缺陷。

### 8.3 补充走查（同日续轮，修复后回归 + 长尾夹具全覆盖）

上一轮修复落地后的补充走查，覆盖第 1–5 轮未深入的夹具模式；**新发现 P0/P1/P2 × 0**，全部通过：

| 模式 / 路径 | 实测内容 | 结论 |
| --- | --- | --- |
| `?theme=light&err=1` | 浅色全面板首屏 + 树错误本地化（红字可读、无拼色） | ✓ |
| `?ro=1` | 工具栏「只读」「禁删」双徽标；生产面板红色提示 + topic 输入禁用 + 发送禁用降饱和 | ✓ |
| `?glue=1` | Schema 面板「注册表: AWS Glue」徽标 + Glue subjects（BACKWARD_ALL/FULL_ALL） | ✓ |
| `?noconn=1` | 初始化错误态本地化文案「Kafka 连接上下文未就绪…」 | ✓ |
| `?nodelete=1` | 工具栏「禁删」徽标 | ✓ |
| `?big=1` | 508 topic 树渲染；过滤 1/508 即时；Enter 选首匹配（payment-gateway 入页签徽标）；Esc 清过滤且焦点留在输入框；big-throughput 消费「已扫描 400 · 命中 100 已达条数上限」+ 本地化分页「1 至 50 / 共 100」 | ✓ |
| 生产→消费端到端 | 生产 `{"round":6,...}` → 通知「分区 0，offset 2」→ 消费「已扫描 4 · 命中 4」新消息可见 | ✓ |
| `?msk=1` | 连接摘要 OAUTHBEARER / Token 来源 msk_iam / 区域 / Secret 已配置徽标链；弹窗遮罩按 F5-1 修复后正常变暗 | ✓ |
| `?nosave=1` | 无 saveFile 桥时导出走 Blob 兜底，反馈「已导出 JSON」 | ✓ |

环境备注：本节走查复用 :5317 dev server（用户浏览器标签正打开该页，**保留运行**；停止命令 `kill` 对应 `vite --port 5317` 进程即可）。走查中两次中间异常（树过滤「0 命中」、Esc「未清空」）均为自动化派发层级/猜测 topic 名所致的测试手法问题，复核后排除产品嫌疑。
