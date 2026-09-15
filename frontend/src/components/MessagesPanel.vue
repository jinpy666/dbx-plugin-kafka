<script setup lang="ts">
// 一次性消费面板：消费表单覆盖 §5.3 全参数（5 种 offset 策略、per-partition
// 精确 seek、isolation/commit 互斥、三通道过滤 + matchMode + fieldFilters、
// 时间/offset 范围、decode/decompression），消息表 + 详情抽屉（本地二次
// decode/format、valueBase64 完整查看/下载）+ JSON/CSV 导出 + 消费预设。
// commit×过滤互斥等校验在 lib/kafkaModel.validateConsumeForm（纯函数，有单测）。
// 布局压缩（R 路）：有结果后表单默认收起为一行摘要 chips 条（开合记忆
// dbx.kafka.ui.msgFormOpen），结果表格吃满剩余高度；大数据量防护见各标注。
import { computed, nextTick, onBeforeUnmount, onMounted, ref, shallowRef, triggerRef, watch } from "vue";
import { ChevronDown, ChevronsDown, Copy, Download, Play, Plus, Save, SlidersHorizontal, Trash2, X } from "@lucide/vue";
import type { ColDef } from "ag-grid-community";
import DbxAgGrid from "./DbxAgGrid.vue";
import CodeEditor from "./CodeEditor.vue";
import {
  kafkaApi,
  type ConsumeParams,
  type ConsumeResult,
  type DecodeMode,
  type Decompression,
  type FieldFilter,
  type IsolationLevel,
  type KafkaMessage,
  type KafkaTopic,
  type MatchMode,
  type OffsetStrategy,
  type SchemaAttach,
  type SchemaFormat,
  type SchemaSubject,
} from "../lib/api";
import {
  MINIMAL_MESSAGE_FIELDS,
  messageColumns,
  toggleWorkbenchTimestampTz,
  toMessageRows,
  workbenchTimestampTz,
  type MessageRow,
} from "../lib/kafkaColumns";
import {
  capRows,
  copyTextToClipboard,
  debounce,
  decideModalKeydown,
  fieldFilterIssue,
  focusableElements,
  formatMessageValue,
  formatTimestamp,
  isRangeReversed,
  looksLikeJson,
  looksLikeXml,
  messageFullValueText,
  nowDatetimeLocal,
  offsetTimeToParam,
  offsetTimeToUnixMs,
  parsePartitionList,
  parsePartitionOffsetsText,
  serializeMessagesToJson,
  serializeMessagesToTsv,
  switchTimeInputMode,
  timestampIso,
  validateConsumeForm,
  type DecodedValue,
  type ValueFormat,
} from "../lib/kafkaModel";
import { t } from "../lib/i18n";
import type { UiIntentOutcome, UiIntentSummary } from "../../../shared/frontend/uiIntent";

const props = defineProps<{
  topic: string;
  canWrite: boolean;
  /** 当前连接 topic 列表（consume-bar 下拉选择；与左侧树同一 App 状态源）。 */
  topics?: KafkaTopic[];
  /** 连接 SR provider（Phase P）：glue 时 schema 挂载区禁用并提示（管理面 only）。 */
  srProvider?: string;
}>();

const emit = defineEmits<{
  (e: "error", message: string): void;
  (e: "notify", message: string): void;
  (e: "selectTopic", topic: string): void;
}>();

/** 消费条上的 topic 下拉选择：上抛 App.selectTopic（与左侧树同源联动）。 */
function onTopicSelect(name: string) {
  if (name && name !== props.topic) emit("selectTopic", name);
}

// -- form state ---------------------------------------------------------------

const groupId = ref("");
const offsetStrategy = ref<OffsetStrategy>("latest");
const offsetTimeText = ref("");
const partitionsText = ref("");
const partitionOffsetsText = ref("");
const limit = ref("100");
const timeoutMs = ref("5000");
const maxScanRecords = ref("10000");
const isolationLevel = ref<IsolationLevel>("read_uncommitted");
const commit = ref(false);
const filterText = ref("");
const keyFilterText = ref("");
const valueFilterText = ref("");
const headerFilterText = ref("");
const matchMode = ref<MatchMode>("contains");
const fieldFilters = ref<FieldFilter[]>([]);
const timestampFrom = ref("");
const timestampTo = ref("");
const offsetFrom = ref("");
const offsetTo = ref("");
const decode = ref<DecodeMode>("none");
const decompression = ref<Decompression>("none");

const consuming = ref(false);
// 大数据量防护：result/行数组/详情 raw 均浅响应（shallowRef）——大数组不做深度
// 代理，整体替换引用驱动更新；行上限裁剪见 applyResult/capRows。
const result = shallowRef<ConsumeResult | null>(null);
const formIssues = ref<string[]>([]);
const presets = ref<Array<{ id: string; name: string }>>([]);
const presetName = ref("");

// -- 摘要条（收起态）：开合记忆 dbx.kafka.ui.msgFormOpen；无记忆时
// 「未选 topic 或尚无结果」默认展开、消费成功后自动收起为摘要条。-------------

const MSG_FORM_OPEN_KEY = "dbx.kafka.ui.msgFormOpen";

function loadStoredFormOpen(): boolean | null {
  try {
    const raw = localStorage.getItem(MSG_FORM_OPEN_KEY);
    if (raw === "1") return true;
    if (raw === "0") return false;
  } catch {
    /* 存储不可用：走默认 */
  }
  return null;
}

const storedFormOpen = loadStoredFormOpen();
// 条件抽屉（consume-drawer）默认收起：单行消费条 + 结果表格是常态布局。
const formOpen = ref(storedFormOpen ?? false);

watch(formOpen, (open) => {
  try {
    localStorage.setItem(MSG_FORM_OPEN_KEY, open ? "1" : "0");
  } catch {
    /* 内存态即可 */
  }
});

function toggleFormOpen() {
  formOpen.value = !formOpen.value;
}

// 摘要 chips：策略文案映射（无新增 i18n key，复用既有 strategy*/formatRaw）。
const STRATEGY_LABEL_KEYS: Record<OffsetStrategy, string> = {
  latest: "messages.strategyLatest",
  earliest: "messages.strategyEarliest",
  committed: "messages.strategyCommitted",
  timestamp: "messages.strategyTimestamp",
  offset: "messages.strategyOffset",
};
const strategyLabel = computed(() => t(STRATEGY_LABEL_KEYS[offsetStrategy.value]));
const decodeLabel = computed(() =>
  decompression.value !== "none" ? `${decode.value} · ${decompression.value}` : decode.value === "none" ? t("messages.formatRaw") : decode.value,
);
// 生效过滤条件数：三+1 通道文本非空 + 启用且有值的 fieldFilters 行。
const filterCount = computed(() => {
  let count = 0;
  if (filterText.value.trim()) count += 1;
  if (keyFilterText.value.trim()) count += 1;
  if (valueFilterText.value.trim()) count += 1;
  if (headerFilterText.value.trim()) count += 1;
  count += fieldFilters.value.filter((row) => row.enabled && row.value.trim().length > 0).length;
  return count;
});

// -- 筛选区分组（基础/定位/时间与范围/过滤/解码）：前两组（基础、定位）默认展开；
// 后三组可折叠，开态记忆在 dbx.kafka.ui.msgFilters（JSON 对象，高级项折叠）。

type ConsumeGroupKey = "basic" | "locate" | "timeRange" | "filter" | "decode";
const MSG_FILTERS_KEY = "dbx.kafka.ui.msgFilters";
const GROUP_DEFAULTS: Record<ConsumeGroupKey, boolean> = { basic: true, locate: true, timeRange: true, filter: true, decode: false };
// 可折叠记忆的组 = 除「基础」「定位」外的三组（前两组常驻展开，不落盘）。
const COLLAPSIBLE_GROUPS = ["timeRange", "filter", "decode"] as const;

function loadOpenGroups(): Record<ConsumeGroupKey, boolean> {
  const open = { ...GROUP_DEFAULTS };
  try {
    const raw = JSON.parse(localStorage.getItem(MSG_FILTERS_KEY) ?? "") as Partial<Record<ConsumeGroupKey, unknown>> | null;
    if (raw && typeof raw === "object") {
      for (const key of COLLAPSIBLE_GROUPS) {
        if (typeof raw[key] === "boolean") open[key] = raw[key] as boolean;
      }
    }
  } catch {
    /* 无记忆/损坏 → 默认 */
  }
  return open;
}

const openGroups = ref(loadOpenGroups());

watch(
  openGroups,
  (value) => {
    try {
      localStorage.setItem(
        MSG_FILTERS_KEY,
        JSON.stringify(Object.fromEntries(COLLAPSIBLE_GROUPS.map((key) => [key, value[key]]))),
      );
    } catch {
      /* 存储不可用：仅内存态 */
    }
  },
  { deep: true },
);

function toggleGroup(key: ConsumeGroupKey) {
  openGroups.value[key] = !openGroups.value[key];
}

// -- 时间与范围输入（timestampFrom/To：datetime-local ↔ unix ms 双模式）---------

const tsMode = ref<"datetime" | "unix">("datetime");

const tsFromMs = computed(() => offsetTimeToUnixMs(timestampFrom.value));
const tsToMs = computed(() => offsetTimeToUnixMs(timestampTo.value));
const tsRangeReversed = computed(() => isRangeReversed(tsFromMs.value, tsToMs.value));
const tsFromInvalid = computed(() => Boolean(timestampFrom.value.trim()) && tsFromMs.value === null);
const tsToInvalid = computed(() => Boolean(timestampTo.value.trim()) && tsToMs.value === null);

function toggleTsMode() {
  const next = tsMode.value === "datetime" ? "unix" : "datetime";
  timestampFrom.value = switchTimeInputMode(timestampFrom.value, next);
  timestampTo.value = switchTimeInputMode(timestampTo.value, next);
  tsMode.value = next;
}

function setNow(target: "from" | "to") {
  if (tsMode.value === "unix") {
    if (target === "from") timestampFrom.value = String(Date.now());
    else timestampTo.value = String(Date.now());
    return;
  }
  if (target === "from") timestampFrom.value = nowDatetimeLocal();
  else timestampTo.value = nowDatetimeLocal();
}

// -- fieldFilters 行校验（数值比较 operator 需要 value 可转数字）------------------

function fieldFilterIssueKey(index: number): string | null {
  const row = fieldFilters.value[index];
  if (!row) return null;
  // fieldFilterIssue 纯函数返回片段（fieldValueNumeric）；展示统一走 uiFilterValueRequired。
  return fieldFilterIssue(row) ? "messages.uiFilterValueRequired" : null;
}

function fieldFilterIssueText(index: number): string {
  const key = fieldFilterIssueKey(index);
  return key ? t(key) : "";
}

// -- schema mount（Phase 2：SR 解码挂载，version 空 = latest）---------------------

const schemaEnabled = ref(false);
const schemaSubjects = ref<SchemaSubject[]>([]);
const schemaSubject = ref("");
const schemaVersionText = ref("");
const schemaFormat = ref<SchemaFormat>("avro");
// Phase P：Glue 仅管理面（消息编解码仅 Confluent wire format，后端 -32000 拒绝），
// 前端同步禁用挂载区并提示（保留 discoverability，不隐藏）。
const glueSchemaDisabled = computed(() => props.srProvider === "glue");
watch(glueSchemaDisabled, (disabled) => {
  if (disabled) schemaEnabled.value = false;
});
const schemaVersions = computed(() => {
  const subject = schemaSubjects.value.find((row) => row.subject === schemaSubject.value);
  const latest = subject?.latestVersion ?? 0;
  return Array.from({ length: Math.max(latest, 0) }, (_unused, index) => latest - index);
});

async function loadSchemaSubjects() {
  try {
    // registry 参数省略 = 连接默认提供方（Glue 下挂载区已禁用，此列表仅供展示兜底）。
    const response = await kafkaApi.schemaSubjectsList();
    schemaSubjects.value = response.subjects ?? [];
  } catch {
    // SR 未启用/旧 sidecar：挂载区下拉为空且可关闭，不阻断消费主流程。
    schemaSubjects.value = [];
  }
}

watch(schemaEnabled, (enabled) => {
  if (enabled && schemaSubjects.value.length === 0) void loadSchemaSubjects();
});

watch(schemaSubject, () => {
  schemaVersionText.value = "";
  const found = schemaSubjects.value.find((row) => row.subject === schemaSubject.value);
  if (found?.formats?.length) schemaFormat.value = (found.formats[0] as SchemaFormat) ?? "avro";
});

function buildSchemaAttach(): SchemaAttach | undefined {
  if (glueSchemaDisabled.value) return undefined;
  if (!schemaEnabled.value || !schemaSubject.value) return undefined;
  const version = Number.parseInt(schemaVersionText.value, 10);
  return {
    subject: schemaSubject.value,
    ...(Number.isFinite(version) && version > 0 ? { version } : {}),
    format: schemaFormat.value,
  };
}

// commit 开启后过滤通道全部禁用（§5.3：commit 与过滤互斥，后端同规则）。
const filtersDisabled = computed(() => commit.value);
// 显式 partitions 与 groupId 互斥（§5.3）。
const groupDisabled = computed(() => parsePartitionList(partitionsText.value).length > 0);
const partitionsDisabled = computed(() => commit.value || Boolean(groupId.value.trim()));
const hasFilters = computed(() => filterCount.value > 0);
const detail = shallowRef<KafkaMessage | null>(null);
// 行数组浅响应 + 引用替换（不逐条改）；rowsTotal 为裁前行数（裁剪提示用）。
const messageRows = shallowRef<MessageRow[]>([]);
const rowsTotal = ref(0);
const rowsDropped = computed(() => Math.max(0, rowsTotal.value - messageRows.value.length));
const messageCols = computed(() =>
  messageColumns({ onCopyJson: (row) => void copyMessageJson(row) }) as ColDef<MessageRow>[],
);

// -- 即时搜索（F6-1）/时区切换（F6-3）/复制族（F6-2）---------------------------
// quickFilter：输入防抖 150ms 后喂给 DbxAgGrid.quickFilterText（只过滤已加载行）。
const quickFilterInput = ref("");
const quickFilter = ref("");
const applyQuickFilter = debounce((value: string) => {
  quickFilter.value = value;
}, 150);

onBeforeUnmount(() => applyQuickFilter.cancel());

const tzLabel = computed(() => (workbenchTimestampTz.value === "utc" ? "UTC" : t("messages.tzLocal")));

function toggleTz() {
  toggleWorkbenchTimestampTz();
}

async function copyWithNotify(text: string) {
  const ok = await copyTextToClipboard(text);
  if (ok) emit("notify", t("copied"));
  else emit("error", t("messages.copyFailed"));
}

function messageJsonText(message: KafkaMessage): string {
  return serializeMessagesToJson([message]);
}

function copyMessageJson(row: MessageRow) {
  void copyWithNotify(messageJsonText(row.raw));
}

function copyDetail(part: "key" | "value" | "headers" | "json") {
  const message = detail.value;
  if (!message) return;
  if (part === "key") void copyWithNotify(message.key ?? "");
  else if (part === "value") void copyWithNotify(messageFullValueText(message));
  else if (part === "headers") void copyWithNotify(message.headers ? JSON.stringify(message.headers) : "");
  else void copyWithNotify(messageJsonText(message));
}
// P1-1：结果区统计行锚点（消费后滚动目标）。
const resultMetaEl = ref<HTMLElement | null>(null);
// 消息表实例（跳到最新经 DbxAgGrid.goToLatest 走 gridApi：末页 + 滚入视口）。
const messagesGrid = ref<InstanceType<typeof DbxAgGrid> | null>(null);

/** 行数组重建（tz 切换/结果落地共用）：capRows 裁剪 → toMessageRows（按当前
 *  时区格式化）→ 引用替换一次性提交。 */
function rebuildRows() {
  const capped = capRows(result.value?.messages ?? []);
  messageRows.value = toMessageRows(capped.rows);
  triggerRef(messageRows);
  rowsTotal.value = capped.total;
}

/** 消费结果落地（唯一入口）：capRows 裁剪（保留最新 N 条）→ 批量构建行数组 →
 *  引用替换一次性提交（DbxAgGrid 以单次 setGridOption 批量应用，配合稳定
 *  getRowId，无逐条更新）。 */
function applyResult(next: ConsumeResult | null) {
  result.value = next;
  rebuildRows();
}

// F6-3：时区切换后行内已格式化文本需要重建（列 valueFormatter 是响应式的，
// 行文本不是——统一在这里重算）。
watch(workbenchTimestampTz, () => rebuildRows());

function openDetail(row: MessageRow) {
  // 详情 raw 单份存储：直接引用行内 raw（与 result.messages 同一对象，不拷贝）。
  detail.value = row.raw;
}

/** 跳到最新（R 路）：滚回结果区锚点 + 经 DbxAgGrid.goToLatest 跳分页末页并
 *  把最后一行滚入视口底部（最新数据行可见；分页模式由 gridApi 处理）。 */
function jumpToLatest() {
  resultMetaEl.value?.scrollIntoView({ block: "start" });
  messagesGrid.value?.goToLatest();
}

function positiveInt(value: unknown, fallback: number): number {
  const parsed = Number.parseInt(String(value ?? "").trim(), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function optionalNumber(value: unknown): number | undefined {
  // P1-6：Vue 3 对 <input type="number"> 的 v-model 可能给 number（科学计数/清空
  // 过程），直接 .trim() 抛 TypeError 且消费请求不发出——入参一律 String 归一
  // （与 ProducePanel/GroupsPanel 同范式修复）。
  const trimmed = String(value ?? "").trim();
  if (!trimmed) return undefined;
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : undefined;
}

// -- field filters editor -------------------------------------------------------

function addFieldFilter() {
  fieldFilters.value.push({ source: "value", path: "", operator: "contains", value: "", enabled: true });
}

function removeFieldFilter(index: number) {
  fieldFilters.value.splice(index, 1);
}

// -- params build / validation ---------------------------------------------------

function buildParams(topicOverride?: string): ConsumeParams {
  const offsetTime = offsetStrategy.value === "timestamp" ? offsetTimeToParam(offsetTimeText.value) : null;
  const partitions = parsePartitionList(partitionsText.value);
  const partitionOffsets = parsePartitionOffsetsText(partitionOffsetsText.value);
  const params: ConsumeParams = {
    topic: topicOverride ?? props.topic,
    offsetStrategy: offsetStrategy.value,
    isolationLevel: isolationLevel.value,
    limit: positiveInt(limit.value, 100),
    timeoutMs: positiveInt(timeoutMs.value, 5000),
    maxScanRecords: positiveInt(maxScanRecords.value, 10000),
    decode: decode.value,
    decompression: decompression.value,
  };
  const schema = buildSchemaAttach();
  if (schema) params.schema = schema;
  if (groupId.value.trim() && partitions.length === 0) params.groupId = groupId.value.trim();
  if (partitions.length > 0 && !params.groupId) params.partitions = partitions;
  if (offsetStrategy.value === "offset" && Object.keys(partitionOffsets).length > 0) {
    params.partitionOffsets = partitionOffsets;
  }
  if (offsetTime !== null) params.offsetTime = offsetTime;
  if (commit.value && params.groupId) {
    params.commit = true;
    return params; // commit 与过滤互斥，不再附带任何过滤字段
  }
  if (filterText.value.trim()) params.filter = filterText.value.trim();
  if (keyFilterText.value.trim()) params.keyFilter = keyFilterText.value.trim();
  if (valueFilterText.value.trim()) params.valueFilter = valueFilterText.value.trim();
  if (headerFilterText.value.trim()) params.headerFilter = headerFilterText.value.trim();
  if (hasFilters.value) params.matchMode = matchMode.value;
  const enabledFilters = fieldFilters.value.filter((row) => row.enabled && row.value.trim().length > 0);
  if (enabledFilters.length > 0) {
    params.fieldFilters = enabledFilters.map((row) => ({
      source: row.source,
      operator: row.operator,
      value: row.value,
      ...(row.path && row.path.trim() ? { path: row.path.trim() } : {}),
    }));
  }
  const tsFrom = offsetTimeToUnixMs(timestampFrom.value); // datetime-local/unix ms/RFC3339 → unix ms；空/非法 → null
  const tsTo = offsetTimeToUnixMs(timestampTo.value);
  const offFrom = optionalNumber(offsetFrom.value);
  const offTo = optionalNumber(offsetTo.value);
  if (tsFrom !== null) params.timestampFrom = tsFrom;
  if (tsTo !== null) params.timestampTo = tsTo;
  if (offFrom !== undefined) params.offsetFrom = offFrom;
  if (offTo !== undefined) params.offsetTo = offTo;
  return params;
}

// P1-7：消费请求序号守卫——topic 切换/重新消费都会自增；晚到的旧响应落地前
// 与当前序号比对，不一致即丢弃，避免旧 topic 消息串台到新选中 topic 名下。
let consumeSeq = 0;

async function runConsume(options: { topicOverride?: string } = {}) {
  if (consuming.value || !props.topic) return;
  const issues = validateConsumeForm({
    commit: commit.value,
    groupId: groupId.value,
    partitionsText: partitionsText.value,
    offsetStrategy: offsetStrategy.value,
    offsetTimeText: offsetTimeText.value,
    partitionOffsetsText: partitionOffsetsText.value,
    hasFilters: hasFilters.value,
  }).map((issue) => t(`messages.${issue.key}`));
  if (tsRangeReversed.value) issues.push(t("messages.uiTimeRangeInvalid"));
  formIssues.value = issues;
  if (formIssues.value.length > 0) return;
  consuming.value = true;
  const seq = ++consumeSeq;
  emit("error", "");
  try {
    const response = await kafkaApi.messagesConsume(buildParams(options.topicOverride));
    if (seq !== consumeSeq) return; // 竞态守卫：在途期间切了 topic / 重新消费 → 旧响应丢弃
    applyResult(response);
    // 消费成功后收起条件抽屉，结果表格立即可见（开合记忆仍由 watch(formOpen) 落盘）。
    formOpen.value = false;
    // P1-1：消费后自动滚到结果区顶部，数据行立即可见（空态/无滚动时为 no-op）。
    await nextTick();
    resultMetaEl.value?.scrollIntoView({ behavior: "smooth", block: "start" });
  } catch (cause) {
    if (seq !== consumeSeq) return; // 失败响应同样受守卫：旧请求的报错不打扰新 topic
    emit("error", cause instanceof Error ? cause.message : String(cause));
  } finally {
    // 仅当仍是最新请求时复位 consuming，避免旧请求 finally 抢先解锁新在途消费。
    if (seq === consumeSeq) consuming.value = false;
  }
}

// -- MCP UI intent（M3，shared/frontend/uiIntent 公共层调用方） ----------------
// App 的 useUiIntent("kafka") handlers 经组件 ref 调用：applyIntentConsume
// 填消费表单并触发查询（结果留 UI，回报摘要：count + 前 5 行 + partition/
// offset 锚点）；applyIntentSelect 按 partition+offset 在当前结果中定位并
// 打开详情抽屉。表单填充走既有 ref / preset 同款路径，UI 可视可撤销。

const INTENT_CELL_WIDTH = 120;

const INTENT_STRATEGIES: OffsetStrategy[] = ["latest", "earliest", "committed", "timestamp", "offset"];

function truncateIntentCell(value: string): string {
  return [...value].length > INTENT_CELL_WIDTH ? `${[...value].slice(0, INTENT_CELL_WIDTH).join("")}…` : value;
}

function summarizeIntentResult(response: ConsumeResult): UiIntentSummary {
  const messages = response.messages ?? [];
  const rows = messages.slice(0, 5).map((message) => ({
    topic: message.topic,
    partition: message.partition,
    offset: message.offset,
    key: truncateIntentCell(message.key ?? ""),
    value: truncateIntentCell(messageFullValueText(message)),
  }));
  const anchor = messages.length > 0 ? `${messages[0].topic}-p${messages[0].partition}-o${messages[0].offset}` : undefined;
  return {
    count: messages.length,
    truncated: response.hasMore === true,
    rows,
    ...(anchor ? { anchor } : {}),
  };
}

/** kafka_ui_search（action=search）落表：条件填入消费表单 → 触发消费 →
 *  返回 applied 摘要或 rejected 原因。topic 以 intent 参数为准（App 已先行
 *  selectTopic；此处再用 topicOverride 兜底时序竞态）。 */
async function applyIntentConsume(params: Record<string, unknown>): Promise<UiIntentOutcome> {
  if (consuming.value) {
    return { status: "rejected", reason: t("intent.consumeInProgress") };
  }
  const topic = String(params.topic ?? "").trim();
  if (!topic && !props.topic) {
    return { status: "rejected", reason: t("messages.topicRequired") };
  }
  const strategy = INTENT_STRATEGIES.find((candidate) => candidate === params.offsetStrategy);
  if (strategy) offsetStrategy.value = strategy;
  if (params.limit !== undefined && params.limit !== null) limit.value = String(params.limit);
  if (params.groupId !== undefined) groupId.value = String(params.groupId ?? "");
  if (params.offsetTime !== undefined) offsetTimeText.value = String(params.offsetTime ?? "");
  if (Array.isArray(params.partitions)) partitionsText.value = (params.partitions as unknown[]).join(",");
  if (params.filter !== undefined) filterText.value = String(params.filter ?? "");
  if (params.keyFilter !== undefined) keyFilterText.value = String(params.keyFilter ?? "");
  if (params.valueFilter !== undefined) valueFilterText.value = String(params.valueFilter ?? "");
  if (params.headerFilter !== undefined) headerFilterText.value = String(params.headerFilter ?? "");
  const matchModes: MatchMode[] = ["contains", "prefix", "exact", "regex"];
  const matchModeIntent = matchModes.find((candidate) => candidate === params.matchMode);
  if (matchModeIntent) matchMode.value = matchModeIntent;

  const issues = validateConsumeForm({
    commit: commit.value,
    groupId: groupId.value,
    partitionsText: partitionsText.value,
    offsetStrategy: offsetStrategy.value,
    offsetTimeText: offsetTimeText.value,
    partitionOffsetsText: partitionOffsetsText.value,
    hasFilters: hasFilters.value,
  }).map((issue) => t(`messages.${issue.key}`));
  formIssues.value = issues;
  if (issues.length > 0) {
    return { status: "rejected", reason: issues.join("; ") };
  }

  consuming.value = true;
  const seq = ++consumeSeq;
  emit("error", "");
  try {
    const response = await kafkaApi.messagesConsume(buildParams(topic || undefined));
    if (seq !== consumeSeq) {
      return { status: "rejected", reason: t("intent.consumeInProgress") };
    }
    applyResult(response);
    formOpen.value = false;
    await nextTick();
    resultMetaEl.value?.scrollIntoView({ behavior: "smooth", block: "start" });
    return { status: "applied", summary: summarizeIntentResult(response) };
  } catch (cause) {
    if (seq !== consumeSeq) {
      return { status: "rejected", reason: t("intent.consumeInProgress") };
    }
    const reason = cause instanceof Error ? cause.message : String(cause);
    emit("error", reason);
    return { status: "rejected", reason };
  } finally {
    if (seq === consumeSeq) consuming.value = false;
  }
}

/** kafka_ui_select（action=select）定位：按 partition+offset（可选 topic 校验）
 *  在当前结果行中查找，命中行打开详情抽屉。 */
async function applyIntentSelect(params: Record<string, unknown>): Promise<UiIntentOutcome> {
  const partition = Number.parseInt(String(params.partition ?? ""), 10);
  const offset = Number.parseInt(String(params.offset ?? ""), 10);
  if (!Number.isFinite(partition) || !Number.isFinite(offset) || partition < 0 || offset < 0) {
    return { status: "rejected", reason: "partition and offset are required (non-negative integers)" };
  }
  const topic = String(params.topic ?? "").trim();
  const row = messageRows.value.find(
    (candidate) => candidate.raw.partition === partition && candidate.raw.offset === offset && (!topic || candidate.raw.topic === topic),
  );
  if (!row) {
    return { status: "rejected", reason: t("intent.selectMissing") };
  }
  openDetail(row);
  return {
    status: "applied",
    summary: {
      count: 1,
      anchor: `${row.raw.topic}-p${row.raw.partition}-o${row.raw.offset}`,
      rows: [{ partition: row.raw.partition, offset: row.raw.offset, key: truncateIntentCell(row.raw.key ?? "") }],
    },
  };
}

defineExpose({ applyIntentConsume, applyIntentSelect });

// -- presets ---------------------------------------------------------------------

async function loadPresets() {
  try {
    const response = await kafkaApi.presetsList();
    // type=monitor 的预设归 MonitorPanel 管（同一 store，互不混显）。
    presets.value = (response.presets ?? [])
      .filter((preset) => preset.params?.type !== "monitor")
      .map((preset) => ({ id: preset.id, name: preset.name }));
  } catch {
    presets.value = [];
  }
}

function currentFormParams(): ConsumeParams {
  return { ...buildParams(), topic: "" };
}

async function savePreset() {
  const name = presetName.value.trim();
  if (!name) return;
  try {
    await kafkaApi.presetsSave({ id: `preset-${Date.now()}`, name, params: currentFormParams() });
    presetName.value = "";
    emit("notify", t("messages.presetSaved"));
    await loadPresets();
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  }
}

async function applyPreset(id: string) {
  try {
    const response = await kafkaApi.presetsList();
    const preset = (response.presets ?? []).find((row) => row.id === id);
    if (!preset) return;
    const params = preset.params ?? {};
    groupId.value = params.groupId ?? "";
    offsetStrategy.value = params.offsetStrategy ?? "latest";
    offsetTimeText.value = typeof params.offsetTime === "string" ? params.offsetTime : params.offsetTime ? String(params.offsetTime) : "";
    partitionsText.value = (params.partitions ?? []).join(",");
    partitionOffsetsText.value = Object.entries(params.partitionOffsets ?? {})
      .map(([partition, offset]) => `${partition}=${offset}`)
      .join(",");
    limit.value = String(params.limit ?? 100);
    timeoutMs.value = String(params.timeoutMs ?? 5000);
    maxScanRecords.value = String(params.maxScanRecords ?? 10000);
    isolationLevel.value = params.isolationLevel ?? "read_uncommitted";
    commit.value = params.commit === true;
    filterText.value = params.filter ?? "";
    keyFilterText.value = params.keyFilter ?? "";
    valueFilterText.value = params.valueFilter ?? "";
    headerFilterText.value = params.headerFilter ?? "";
    matchMode.value = params.matchMode ?? "contains";
    fieldFilters.value = (params.fieldFilters ?? []).map((row) => ({ ...row, enabled: true }));
    decode.value = params.decode ?? "none";
    decompression.value = params.decompression ?? "none";
    schemaEnabled.value = Boolean(params.schema);
    schemaSubject.value = params.schema?.subject ?? "";
    schemaVersionText.value = params.schema?.version !== undefined ? String(params.schema.version) : "";
    schemaFormat.value = params.schema?.format === "protobuf" || params.schema?.format === "json" ? params.schema.format : "avro";
    emit("notify", t("messages.presetApplied"));
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  }
}

async function removePreset(id: string) {
  try {
    await kafkaApi.presetsRemove(id);
    await loadPresets();
    emit("notify", t("messages.presetRemoved"));
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  }
}

// -- export ------------------------------------------------------------------------

async function exportMessages(format: "json" | "csv" | "tsv") {
  if (!result.value || result.value.messages.length === 0) return;
  // Lane4 打磨：后端 kafka/messages/export 仅接受 json/csv（其余 -32000），
  // TSV 走前端序列化——直接导出当前已加载结果行（复用 kafkaModel 的保真
  // value 文本与 TSV 转义；列序/行分隔与后端 CSV 一致），不发额外请求。
  if (format === "tsv") {
    downloadText("kafka-messages.tsv", "text/tab-separated-values", serializeMessagesToTsv(result.value.messages));
    emit("notify", t("messages.exportDone", { name: "TSV" }));
    return;
  }
  try {
    const response = await kafkaApi.messagesExport({
      ...buildParams(),
      format,
      limit: Math.max(result.value.messages.length, positiveInt(limit.value, 100), 1),
    });
    downloadText(response.filename || `kafka-messages.${format}`, response.contentType, response.content);
    emit("notify", t("messages.exportDone", { name: format.toUpperCase() }));
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  }
}

function downloadText(name: string, contentType: string, text: string) {
  // Host API 1.0 无 save-file 桥，Blob URL 下载为约定兜底。
  const blob = new Blob([text], { type: `${contentType};charset=utf-8` });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = name;
  anchor.click();
  window.setTimeout(() => URL.revokeObjectURL(url), 10_000);
}

// -- detail drawer（本地二次 decode/format，valueBase64 保真来源）-----------------
// 详情体验 v2：headers 表格 ⇄ JSON 切换 + 行级复制；value 走 CodeEditor 只读
//（JSON 高亮/换行开关/内部滚动），复制按钮收敛到各区块标题行（行内/头部 icon）。

const viewFormat = ref<ValueFormat>("raw");
const viewDecode = ref<DecodeMode>("none");
const viewDecompression = ref<Decompression>("none");
const viewResult = ref<DecodedValue>({ text: "" });
const viewBusy = ref(false);
const showFullBase64 = ref(false);
// headers 展示形态：表格（key|value+行复制，默认）/ 格式化 JSON。
const headersView = ref<"table" | "json">("table");
// Headers / Value 区块折叠态（抽屉内会话级；默认全展开）。
const sectionsOpen = ref({ headers: true, value: true });
const headersEntries = computed(() => Object.entries(detail.value?.headers ?? {}));
const headersJsonText = computed(() => JSON.stringify(detail.value?.headers ?? {}, null, 2));

watch(detail, (message) => {
  showFullBase64.value = false;
  headersView.value = "table";
  sectionsOpen.value = { headers: true, value: true };
  if (!message) return;
  const text = messageFullValueText(message).trim();
  viewFormat.value = looksLikeXml(text) ? "xml" : looksLikeJson(text) ? "json" : "raw";
  viewDecode.value = "none";
  viewDecompression.value = "none";
  void renderView();
});

function toggleSection(name: "headers" | "value") {
  sectionsOpen.value = { ...sectionsOpen.value, [name]: !sectionsOpen.value[name] };
}

async function renderView() {
  const message = detail.value;
  if (!message) return;
  viewBusy.value = true;
  try {
    viewResult.value = await formatMessageValue(message, {
      decode: viewDecode.value,
      decompression: viewDecompression.value,
      format: viewFormat.value,
    });
  } finally {
    viewBusy.value = false;
  }
}

// 编辑器直接承载全量解码/格式化文本（CodeMirror 虚拟渲染，16384 截断预览
// 退役）——「所见即所复制」：copy-value 复制当前解码/格式化结果文本。

function downloadValue() {
  const message = detail.value;
  if (!message) return;
  downloadText(`${message.topic}-p${message.partition}-o${message.offset}.txt`, "text/plain", messageFullValueText(message));
}

// -- 弹层交互（P1-2/P1-3）：抽屉 Esc 关闭 + Tab 焦点陷阱 + 关闭归还触发元素 --------
// 决策逻辑在 kafkaModel.decideModalKeydown（纯函数，有单测），这里只做 DOM 接线。

const drawerEl = ref<HTMLElement | null>(null);
let drawerTrigger: HTMLElement | null = null;

watch(detail, (message, previous) => {
  if (message && !previous) {
    // 打开：记住触发元素，下一帧焦点进抽屉（首个可交互控件，兜底抽屉容器）。
    drawerTrigger = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    void nextTick(() => {
      const drawer = drawerEl.value;
      if (!drawer) return;
      const first = focusableElements(drawer)[0];
      (first ?? drawer).focus({ preventScroll: true });
    });
  } else if (!message && previous) {
    // 关闭（Esc/✕/遮罩）：焦点归还触发元素，遮罩随 v-if 一并卸载、无残留。
    drawerTrigger?.focus({ preventScroll: true });
    drawerTrigger = null;
  }
});

function onWindowKeydown(event: KeyboardEvent) {
  if (!detail.value) return;
  // 更高层弹窗（连接弹窗 / teleport 助手弹窗）在场时让位，不抢 Esc/Tab。
  if (document.querySelector(".workbench .modal-backdrop, body > .modal-backdrop")) return;
  const drawer = drawerEl.value;
  if (!drawer) return;
  const focusables = focusableElements(drawer);
  const currentIndex = focusables.indexOf(document.activeElement as HTMLElement);
  const decision = decideModalKeydown(event.key, event.shiftKey, focusables.length, currentIndex);
  if (decision.kind === "close") {
    event.preventDefault();
    event.stopPropagation();
    detail.value = null;
  } else if (decision.kind === "focus") {
    event.preventDefault();
    event.stopPropagation();
    focusables[decision.index]?.focus();
  }
}

onMounted(() => window.addEventListener("keydown", onWindowKeydown));
onBeforeUnmount(() => window.removeEventListener("keydown", onWindowKeydown));

// topic 切换后清空旧结果（跨 topic 结果混排会误导）；无开合记忆时回到默认展开
// （「尚无结果默认展开」语义），有记忆则维持记忆。
// P1-7：切换即自增请求序号使在途旧响应全部作废，并复位 consuming——新 topic
// 可立即重新消费（旧请求的 finally 因序号不匹配不再抢先复位）。
watch(
  () => props.topic,
  () => {
    consumeSeq += 1;
    consuming.value = false;
    applyResult(null);
    detail.value = null;
  },
);

watch(() => props.topic, () => void loadPresets(), { immediate: true });
</script>

<template>
  <section class="section-block messages-panel" :class="{ 'has-result': Boolean(result) }">
    <!-- 单行消费条：topic 下拉 + 条件开关（活跃条件数 badge）+ 摘要 chips + 消费。
         原「收起态摘要条 / 展开态大表单」两种占位合并为常驻一行，条件收进抽屉。 -->
    <div class="consume-bar">
      <label class="consume-bar__topic">
        <span class="consume-bar__label">{{ t("messages.topic") }}</span>
        <select
          :value="topic"
          class="mono"
          data-testid="topic-select"
          :title="topic || t('messages.topicPlaceholder')"
          @change="onTopicSelect(($event.target as HTMLSelectElement).value)"
        >
          <option value="" disabled>{{ t("messages.topicPlaceholder") }}</option>
          <option v-for="topicItem in topics" :key="topicItem.name" :value="topicItem.name">{{ topicItem.name }}</option>
          <!-- topics 未下发时兜底展示当前选中，保证下拉不空挂 -->
          <option v-if="topic && !(topics ?? []).some((topicItem) => topicItem.name === topic)" :value="topic">{{ topic }}</option>
        </select>
      </label>
      <button
        class="consume-bar__filters"
        type="button"
        :class="{ 'is-active': formOpen }"
        :aria-expanded="formOpen"
        data-testid="filters-toggle"
        @click="toggleFormOpen"
      >
        <SlidersHorizontal aria-hidden="true" />
        <span>{{ t("messages.filtersToggle") }}</span>
        <span v-if="filterCount > 0" class="badge badge-warn">{{ filterCount }}</span>
      </button>
      <span class="consume-bar__summary">
        <span class="msg-chip" :title="t('messages.offsetStrategy')">{{ strategyLabel }}</span>
        <span class="msg-chip" :title="t('messages.limit')">{{ t("messages.limit") }} {{ limit }}</span>
        <span v-if="groupId.trim()" class="msg-chip mono" :title="t('messages.groupId')">{{ groupId }}</span>
        <span class="msg-chip" :title="t('messages.decode')">{{ decodeLabel }}</span>
      </span>
      <span class="consume-bar__spacer" />
      <button
        class="primary-button compact"
        type="button"
        :disabled="consuming || !topic || tsRangeReversed"
        data-testid="consume-run"
        @click="runConsume()"
      >
        <Play aria-hidden="true" />{{ consuming ? t("messages.running") : t("messages.run") }}
      </button>
    </div>

    <!-- 条件抽屉：浮层盖在结果区上方，不挤压表格高度；点外部遮罩/Esc 收起。 -->
    <div v-show="formOpen" class="consume-drawer">
      <div class="consume-drawer__backdrop" @click="formOpen = false" />
      <div class="consume-drawer__panel">
    <div class="consume-groups">
      <!-- 基础：常用项前置 -->
      <div class="filter-group">
        <button type="button" class="filter-group-head" :aria-expanded="openGroups.basic" @click="toggleGroup('basic')">
          <ChevronDown class="chev" :class="{ folded: !openGroups.basic }" aria-hidden="true" />
          <span>{{ t("messages.uiGroupBasic") }}</span>
        </button>
        <div v-if="openGroups.basic" class="filter-group-body">
          <div class="group-grid">
            <label class="field">
              <span>{{ t("messages.groupId") }}</span>
              <input v-model="groupId" type="text" :placeholder="t('messages.groupIdPlaceholder')" :disabled="groupDisabled" spellcheck="false" />
            </label>
            <label class="field">
              <span>{{ t("messages.limit") }}</span>
              <input v-model="limit" type="number" min="1" />
            </label>
            <label class="field">
              <span>{{ t("messages.timeoutMs") }}</span>
              <input v-model="timeoutMs" type="number" min="1" />
            </label>
            <label class="field">
              <span>{{ t("messages.maxScanRecords") }}</span>
              <input v-model="maxScanRecords" type="number" min="1" />
            </label>
          </div>
        </div>
      </div>

      <!-- 定位：offset 策略 / 分区 / 隔离 / commit -->
      <div class="filter-group">
        <button type="button" class="filter-group-head" :aria-expanded="openGroups.locate" @click="toggleGroup('locate')">
          <ChevronDown class="chev" :class="{ folded: !openGroups.locate }" aria-hidden="true" />
          <span>{{ t("messages.uiGroupPosition") }}</span>
        </button>
        <div v-if="openGroups.locate" class="filter-group-body">
          <div class="group-grid">
            <label class="field">
              <span>{{ t("messages.offsetStrategy") }}</span>
              <select v-model="offsetStrategy">
                <option value="latest">{{ t("messages.strategyLatest") }}</option>
                <option value="earliest">{{ t("messages.strategyEarliest") }}</option>
                <option value="committed">{{ t("messages.strategyCommitted") }}</option>
                <option value="timestamp">{{ t("messages.strategyTimestamp") }}</option>
                <option value="offset">{{ t("messages.strategyOffset") }}</option>
              </select>
            </label>
            <label v-if="offsetStrategy === 'timestamp'" class="field">
              <span>{{ t("messages.offsetTime") }} ({{ t("messages.offsetTimeHint") }})</span>
              <div class="time-input-row">
                <input v-model="offsetTimeText" type="text" placeholder="2026-09-05T08:30" spellcheck="false" />
                <button class="mini-button" type="button" :title="t('messages.uiTimeNow')" @click="offsetTimeText = nowDatetimeLocal()">
                  {{ t("messages.uiTimeNow") }}
                </button>
              </div>
            </label>
            <label class="field" :class="{ muted: partitionsDisabled }">
              <span>{{ t("messages.partitions") }} ({{ t("messages.partitionsHint") }})</span>
              <input v-model="partitionsText" type="text" placeholder="0,1,2" :disabled="partitionsDisabled" spellcheck="false" />
            </label>
            <label v-if="offsetStrategy === 'offset'" class="field">
              <span>{{ t("messages.partitionOffsets") }}</span>
              <input v-model="partitionOffsetsText" type="text" :placeholder="t('messages.partitionOffsetsPlaceholder')" spellcheck="false" />
            </label>
            <label class="field">
              <span>{{ t("messages.isolation") }}</span>
              <select v-model="isolationLevel">
                <option value="read_uncommitted">{{ t("messages.isolationReadUncommitted") }}</option>
                <option value="read_committed">{{ t("messages.isolationReadCommitted") }}</option>
              </select>
            </label>
            <label class="field--wide checkbox" :title="t('messages.commitHint')">
              <input v-model="commit" type="checkbox" :disabled="!canWrite" />
              <span>{{ t("messages.commit") }}</span>
            </label>
          </div>
        </div>
      </div>

      <!-- 时间与范围：时间选择器（datetime-local 秒级 / unix ms 切换）+ offset 范围 -->
      <div class="filter-group">
        <button type="button" class="filter-group-head" :aria-expanded="openGroups.timeRange" @click="toggleGroup('timeRange')">
          <ChevronDown class="chev" :class="{ folded: !openGroups.timeRange }" aria-hidden="true" />
          <span>{{ t("messages.uiGroupTime") }}</span>
          <span v-if="tsRangeReversed" class="group-summary form-error">{{ t("messages.uiTimeRangeInvalid") }}</span>
        </button>
        <div v-if="openGroups.timeRange" class="filter-group-body">
          <div class="group-grid">
            <label class="field" :class="{ 'is-invalid': tsFromInvalid }">
              <span>{{ t("messages.tsFrom") }} · {{ tsMode === "datetime" ? t("messages.timeModeDatetime") : t("messages.timeModeUnix") }}</span>
              <div class="time-input-row">
                <input
                  v-if="tsMode === 'datetime'"
                  v-model="timestampFrom"
                  type="datetime-local"
                  step="1"
                  :disabled="filtersDisabled"
                  spellcheck="false"
                />
                <input
                  v-else
                  v-model="timestampFrom"
                  type="text"
                  inputmode="numeric"
                  placeholder="1700000000000"
                  :disabled="filtersDisabled"
                  spellcheck="false"
                />
                <button class="mini-button" type="button" :title="t('messages.uiTimeNow')" :disabled="filtersDisabled" @click="setNow('from')">
                  {{ t("messages.uiTimeNow") }}
                </button>
                <button class="mini-button" type="button" :title="t('messages.uiTimeModeSwitch')" :disabled="filtersDisabled" @click="toggleTsMode">
                  {{ tsMode === "datetime" ? t("messages.timeModeUnix") : t("messages.timeModeDatetime") }}
                </button>
              </div>
            </label>
            <label class="field" :class="{ 'is-invalid': tsToInvalid }">
              <span>{{ t("messages.tsTo") }} · {{ tsMode === "datetime" ? t("messages.timeModeDatetime") : t("messages.timeModeUnix") }}</span>
              <div class="time-input-row">
                <input
                  v-if="tsMode === 'datetime'"
                  v-model="timestampTo"
                  type="datetime-local"
                  step="1"
                  :disabled="filtersDisabled"
                  spellcheck="false"
                />
                <input
                  v-else
                  v-model="timestampTo"
                  type="text"
                  inputmode="numeric"
                  placeholder="1700000000000"
                  :disabled="filtersDisabled"
                  spellcheck="false"
                />
                <button class="mini-button" type="button" :title="t('messages.uiTimeNow')" :disabled="filtersDisabled" @click="setNow('to')">
                  {{ t("messages.uiTimeNow") }}
                </button>
                <button class="mini-button" type="button" :title="t('messages.uiTimeModeSwitch')" :disabled="filtersDisabled" @click="toggleTsMode">
                  {{ tsMode === "datetime" ? t("messages.timeModeUnix") : t("messages.timeModeDatetime") }}
                </button>
              </div>
            </label>
            <label class="field">
              <span>{{ t("messages.offsetFrom") }}</span>
              <input v-model="offsetFrom" type="number" :disabled="filtersDisabled" spellcheck="false" />
            </label>
            <label class="field">
              <span>{{ t("messages.offsetTo") }}</span>
              <input v-model="offsetTo" type="number" :disabled="filtersDisabled" spellcheck="false" />
            </label>
          </div>
          <p v-if="tsRangeReversed" class="field-error">{{ t("messages.uiTimeRangeInvalid") }}</p>
        </div>
      </div>

      <!-- 过滤：三通道文本 + matchMode + fieldFilters 行式编辑器 -->
      <div class="filter-group">
        <button type="button" class="filter-group-head" :aria-expanded="openGroups.filter" @click="toggleGroup('filter')">
          <ChevronDown class="chev" :class="{ folded: !openGroups.filter }" aria-hidden="true" />
          <span>{{ t("messages.uiGroupFilter") }}</span>
        </button>
        <div v-if="openGroups.filter" class="filter-group-body">
          <div class="group-grid">
            <label class="field field--wide">
              <span>{{ t("messages.filter") }}</span>
              <input v-model="filterText" type="text" :placeholder="t('messages.filterPlaceholder')" :disabled="filtersDisabled" spellcheck="false" />
            </label>
            <label class="field">
              <span>{{ t("messages.keyFilter") }}</span>
              <input v-model="keyFilterText" type="text" :disabled="filtersDisabled" spellcheck="false" />
            </label>
            <label class="field">
              <span>{{ t("messages.valueFilter") }}</span>
              <input v-model="valueFilterText" type="text" :disabled="filtersDisabled" spellcheck="false" />
            </label>
            <label class="field">
              <span>{{ t("messages.headerFilter") }}</span>
              <input v-model="headerFilterText" type="text" :disabled="filtersDisabled" spellcheck="false" />
            </label>
            <label class="field">
              <span>{{ t("messages.matchMode") }}</span>
              <select v-model="matchMode" :disabled="filtersDisabled">
                <option value="contains">{{ t("messages.opContains") }}</option>
                <option value="prefix">{{ t("messages.opPrefix") }}</option>
                <option value="exact">{{ t("messages.opExact") }}</option>
                <option value="regex">{{ t("messages.opRegex") }}</option>
              </select>
            </label>
          </div>
          <div class="field-filters">
            <div class="filter-head">
              <span>{{ t("messages.fieldFilters") }}</span>
              <button class="qb-add" type="button" :disabled="filtersDisabled" @click="addFieldFilter">
                <Plus aria-hidden="true" />{{ t("messages.uiFilterAddCondition") }}
              </button>
            </div>
            <p v-if="fieldFilters.length === 0" class="hint">{{ t("messages.fieldFiltersEmpty") }}</p>
            <div
              v-for="(row, index) in fieldFilters"
              :key="index"
              class="field-filter-row"
              :class="{ 'is-invalid': Boolean(fieldFilterIssueKey(index)) }"
            >
              <select v-model="row.source" :disabled="filtersDisabled">
                <option value="value">{{ t("messages.sourceValue") }}</option>
                <option value="key">{{ t("messages.sourceKey") }}</option>
                <option value="header">{{ t("messages.sourceHeader") }}</option>
                <option value="topic">{{ t("messages.sourceTopic") }}</option>
                <option value="partition">{{ t("messages.sourcePartition") }}</option>
                <option value="offset">{{ t("messages.sourceOffset") }}</option>
                <option value="timestamp">{{ t("messages.sourceTimestamp") }}</option>
              </select>
              <input v-model="row.path" type="text" :placeholder="t('messages.fieldPath')" :disabled="filtersDisabled" spellcheck="false" />
              <select v-model="row.operator" :disabled="filtersDisabled">
                <option value="contains">{{ t("messages.opContains") }}</option>
                <option value="prefix">{{ t("messages.opPrefix") }}</option>
                <option value="exact">{{ t("messages.opExact") }}</option>
                <option value="regex">{{ t("messages.opRegex") }}</option>
                <option value="exists">{{ t("messages.opExists") }}</option>
                <option value="not_exists">{{ t("messages.opNotExists") }}</option>
                <option value="gt">{{ t("messages.opGt") }}</option>
                <option value="gte">{{ t("messages.opGte") }}</option>
                <option value="lt">{{ t("messages.opLt") }}</option>
                <option value="lte">{{ t("messages.opLte") }}</option>
              </select>
              <input v-model="row.value" type="text" :placeholder="t('messages.fieldValue')" :disabled="filtersDisabled" spellcheck="false" />
              <label class="checkbox" :title="t('messages.conditionEnabled')">
                <input v-model="row.enabled" type="checkbox" :disabled="filtersDisabled" />
              </label>
              <button class="row-remove" type="button" :disabled="filtersDisabled" @click="removeFieldFilter(index)">
                <Trash2 aria-hidden="true" />
              </button>
              <p v-if="fieldFilterIssueKey(index)" class="field-filter-error">{{ fieldFilterIssueText(index) }}</p>
            </div>
          </div>
        </div>
      </div>

      <!-- 解码：内层解码 / 解压 / SR 挂载（高级项，默认折叠） -->
      <div class="filter-group">
        <button type="button" class="filter-group-head" :aria-expanded="openGroups.decode" @click="toggleGroup('decode')">
          <ChevronDown class="chev" :class="{ folded: !openGroups.decode }" aria-hidden="true" />
          <span>{{ t("messages.uiGroupDecode") }}</span>
        </button>
        <div v-if="openGroups.decode" class="filter-group-body">
          <div class="group-grid">
            <label class="field">
              <span>{{ t("messages.decode") }}</span>
              <select v-model="decode">
                <option value="none">none</option>
                <option value="base64">base64</option>
              </select>
            </label>
            <label class="field">
              <span>{{ t("messages.decompression") }}</span>
              <select v-model="decompression">
                <option value="none">none</option>
                <option value="gzip">gzip</option>
                <option value="lz4">lz4</option>
                <option value="zstd">zstd</option>
                <option value="snappy">snappy</option>
              </select>
            </label>
            <label class="field--wide checkbox">
              <input v-model="schemaEnabled" type="checkbox" :disabled="glueSchemaDisabled" />
              <span>{{ t("messages.schemaMount") }}</span>
            </label>
            <template v-if="schemaEnabled">
              <label class="field">
                <span>{{ t("messages.schemaSubject") }}</span>
                <select v-model="schemaSubject" :disabled="glueSchemaDisabled">
                  <option value="">{{ t("acls.anyValue") }}</option>
                  <option v-for="subject in schemaSubjects" :key="subject.subject" :value="subject.subject">
                    {{ subject.subject }}
                  </option>
                </select>
              </label>
              <label class="field">
                <span>{{ t("messages.schemaVersion") }}</span>
                <select v-model="schemaVersionText" :disabled="glueSchemaDisabled">
                  <option value="">{{ t("messages.schemaLatest") }}</option>
                  <option v-for="version in schemaVersions" :key="version" :value="String(version)">{{ version }}</option>
                </select>
              </label>
              <label class="field">
                <span>{{ t("schemas.colFormat") }}</span>
                <select v-model="schemaFormat" :disabled="glueSchemaDisabled">
                  <option value="avro">avro</option>
                  <option value="json">json</option>
                  <option value="protobuf">protobuf</option>
              </select>
              </label>
            </template>
          </div>
          <p v-if="glueSchemaDisabled" class="hint">{{ t("messages.schemaGlueDisabled") }}</p>
        </div>
      </div>
    </div>

    <!-- 表单尾部固定 actions：预设左侧、收起条件 + 消费主按钮右侧 -->
    <div class="form-footer">
      <div class="inline-actions">
        <select :value="''" @change="applyPreset(($event.target as HTMLSelectElement).value)">
          <option value="">{{ t("messages.presets") }}</option>
          <option v-if="presets.length === 0" disabled value="">{{ t("messages.presetEmpty") }}</option>
          <option v-for="preset in presets" :key="preset.id" :value="preset.id">{{ preset.name }}</option>
        </select>
        <input v-model="presetName" type="text" class="preset-input" :placeholder="t('messages.presetName')" spellcheck="false" />
        <button class="qb-add" type="button" :title="t('messages.presetSave')" @click="savePreset">
          <Save aria-hidden="true" />
        </button>
        <button
          v-for="preset in presets"
          :key="`rm-${preset.id}`"
          class="qb-add"
          type="button"
          :title="`${t('messages.presetRemove')}: ${preset.name}`"
          @click="removePreset(preset.id)"
        >
          <X aria-hidden="true" />
        </button>
      </div>
      <span class="footer-spacer" />
      <button class="toolbar-button" type="button" @click="toggleFormOpen">{{ t("messages.uiHideFilters") }}</button>
      <button class="primary-button primary-button--lg" type="button" :disabled="consuming || !topic || tsRangeReversed" @click="runConsume()">
        <Play aria-hidden="true" />{{ consuming ? t("messages.running") : t("messages.run") }}
      </button>
    </div>
      </div><!-- /consume-drawer__panel -->
    </div><!-- /consume-drawer -->

    <div v-if="formIssues.length > 0" class="kafka-form-errors">
      <p v-for="issue in formIssues" :key="issue" class="form-error">{{ issue }}</p>
    </div>

    <div v-if="result" ref="resultMetaEl" class="result-meta">
      <span>
        {{ t("messages.scanned", { count: result.scanned }) }} · {{ t("messages.matched", { count: result.matched }) }}
        <span v-if="result.limited" class="badge badge-warn">{{ t("messages.limited") }}</span>
        <span v-if="result.hasMore" class="badge badge-warn">{{ t("messages.hasMore") }}</span>
        <span v-if="rowsDropped > 0" class="badge badge-warn" :title="t('messages.uiRowsCapped', { shown: messageRows.length, total: rowsTotal })">
          {{ t("messages.uiRowsCapped", { shown: messageRows.length, total: rowsTotal }) }}
        </span>
      </span>
      <span class="inline-actions">
        <input
          v-model="quickFilterInput"
          class="quick-filter-input"
          type="text"
          :placeholder="t('messages.quickFilterPlaceholder')"
          :title="t('messages.quickFilterTitle')"
          spellcheck="false"
          data-testid="quick-filter"
          @input="applyQuickFilter(quickFilterInput)"
        />
        <button
          class="tz-toggle"
          :class="{ 'is-utc': workbenchTimestampTz === 'utc' }"
          type="button"
          :title="t('messages.tzToggleTitle')"
          data-testid="tz-toggle"
          @click="toggleTz"
        >
          {{ tzLabel }}
        </button>
        <button class="toolbar-button" :title="t('messages.uiJumpLatest')" :disabled="messageRows.length === 0" @click="jumpToLatest">
          <ChevronsDown aria-hidden="true" /><span>{{ t("messages.uiJumpLatest") }}</span>
        </button>
        <button class="toolbar-button" :title="t('messages.exportJson')" :disabled="result.messages.length === 0" @click="exportMessages('json')">
          <Download aria-hidden="true" /><span>JSON</span>
        </button>
        <button class="toolbar-button" :title="t('messages.exportCsv')" :disabled="result.messages.length === 0" @click="exportMessages('csv')">
          <Download aria-hidden="true" /><span>CSV</span>
        </button>
        <button
          class="toolbar-button"
          :title="t('polish.exportTsv')"
          :disabled="result.messages.length === 0"
          data-testid="export-tsv"
          @click="exportMessages('tsv')"
        >
          <Download aria-hidden="true" /><span>TSV</span>
        </button>
      </span>
    </div>

    <div v-if="result" class="grid-box grid-box--fill">
      <p v-if="result.messages.length === 0" class="empty compact">{{ t("messages.noMessages") }}</p>
      <DbxAgGrid
        v-else
        ref="messagesGrid"
        table-key="messages"
        :row-data="messageRows"
        :column-defs="messageCols"
        :compact-fields="MINIMAL_MESSAGE_FIELDS"
        :quick-filter="quickFilter"
        row-selection="single"
        @row-click="(row: unknown) => openDetail(row as MessageRow)"
      />
    </div>
    <!-- P2-21：两态空态——未选 topic 引导先在左侧树选择；已选 topic 尚无结果
         或结果为空才是「调整条件重新消费」。空态包进 grid-box--fill 与结果区
         同一容器，占满剩余高度使文案垂直居中（裸 p 会贴在表单下方）。
         round4 面 1：消费在途（consuming）不再整块空白，给出进行中反馈
         （复用 messages.running，消费按钮同文案），请求落地/失败后由 result
         或错误横幅接管。 -->
    <div v-else-if="consuming" class="grid-box grid-box--fill">
      <p class="empty compact">{{ t("messages.running") }}</p>
    </div>
    <div v-else class="grid-box grid-box--fill">
      <p class="empty compact">{{ topic ? t("messages.noMessages") : t("messages.uiNoTopicSelected") }}</p>
    </div>

    <teleport to="body">
      <div v-if="detail" class="drawer-backdrop" @click="detail = null" />
      <div v-if="detail" class="drawer" ref="drawerEl" tabindex="-1" role="dialog" aria-modal="true">
        <header>
          <span class="mono">{{ detail.topic }} · {{ t("messages.colPartition") }} {{ detail.partition }} · {{ t("messages.colOffset") }} {{ detail.offset }}</span>
          <span class="drawer-head-actions">
            <button class="icon-button" :title="t('messages.copyJson')" data-testid="copy-json" @click="copyDetail('json')"><Copy /></button>
            <button class="icon-button" :title="t('close')" @click="detail = null"><X /></button>
          </span>
        </header>
        <div class="drawer-body">
          <dl class="kv-grid">
            <dt>{{ t("messages.colTimestamp") }}</dt>
            <dd :title="timestampIso(detail.timestamp)">{{ formatTimestamp(detail.timestamp, workbenchTimestampTz) }}</dd>
            <dt>{{ t("messages.colKey") }}</dt>
            <dd class="kv-dd-inline">
              <span class="kv-dd-text" :title="detail.key ?? undefined">{{ detail.key ?? "—" }}</span>
              <button v-if="detail.key" class="icon-button icon-button--inline" type="button" :title="t('messages.copyKey')" data-testid="copy-key" @click="copyDetail('key')"><Copy /></button>
            </dd>
            <dt v-if="detail.schemaSubject">{{ t("messages.colSchema") }}</dt>
            <dd v-if="detail.schemaSubject" class="mono">{{ detail.schemaSubject }} v{{ detail.schemaVersion ?? "?" }} (id {{ detail.schemaId ?? "—" }})</dd>
            <dt v-if="detail.decodeError">{{ t("messages.decodeError") }}</dt>
            <dd v-if="detail.decodeError" class="form-error">{{ detail.decodeError }}</dd>
          </dl>

          <!-- Headers：可折叠区块（标题行右侧直接挂切换/复制操作，省一行高度）；
               表格（key|value + 行复制，限高滚动）⇄ 格式化 JSON -->
          <section class="detail-block">
            <div class="detail-block__head">
              <button class="detail-block__toggle" type="button" :aria-expanded="sectionsOpen.headers" @click="toggleSection('headers')">
                <ChevronDown class="chev" :class="{ folded: !sectionsOpen.headers }" aria-hidden="true" />
                <span class="detail-block__title">{{ t("messages.colHeaders") }} · {{ headersEntries.length }}</span>
              </button>
              <span v-if="sectionsOpen.headers && headersEntries.length > 0" class="detail-block__actions">
                <button class="seg-toggle" type="button" :class="{ 'is-active': headersView === 'table' }" @click="headersView = 'table'">{{ t("messages.headersViewTable") }}</button>
                <button class="seg-toggle" type="button" :class="{ 'is-active': headersView === 'json' }" @click="headersView = 'json'">{{ t("messages.headersViewJson") }}</button>
                <button class="icon-button" type="button" :title="t('messages.copyHeaders')" data-testid="copy-headers" @click="copyDetail('headers')"><Copy /></button>
              </span>
            </div>
            <div v-show="sectionsOpen.headers" class="detail-block__body">
              <div v-if="headersView === 'table' && headersEntries.length > 0" class="kv-scroll">
                <table class="kv-table">
                  <thead>
                    <tr>
                      <th>{{ t("messages.colKey") }}</th>
                      <th>Value</th>
                      <th class="kv-table__action-col" aria-hidden="true"></th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr v-for="[headerKey, headerValue] in headersEntries" :key="headerKey">
                      <td class="mono">{{ headerKey }}</td>
                      <td class="kv-table__value" :title="headerValue">{{ headerValue }}</td>
                      <td class="kv-table__action-col">
                        <button class="icon-button icon-button--inline" type="button" :title="t('messages.copyHeaderValue')" @click="copyWithNotify(headerValue)"><Copy /></button>
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
              <pre v-else-if="headersView === 'json' && headersEntries.length > 0" class="value-view value-view--headers">{{ headersJsonText }}</pre>
              <p v-else class="empty compact detail-headers-empty">{{ t("messages.headersEmpty") }}</p>
            </div>
          </section>

          <!-- Value：可折叠区块（编辑器吃满抽屉剩余高度）；CodeEditor 只读高亮（json/xml）
               + 解码管线 + 标题行右侧操作 icon -->
          <section class="detail-block detail-block--value">
            <div class="detail-block__head">
              <button class="detail-block__toggle" type="button" :aria-expanded="sectionsOpen.value" @click="toggleSection('value')">
                <ChevronDown class="chev" :class="{ folded: !sectionsOpen.value }" aria-hidden="true" />
                <span class="detail-block__title">{{ t("messages.colValue") }}<span v-if="viewBusy" class="detail-block__busy">…</span></span>
              </button>
              <span v-if="sectionsOpen.value" class="detail-block__actions">
                <button class="seg-toggle" type="button" :class="{ 'is-active': showFullBase64 }" :title="t('messages.fullValue')" @click="showFullBase64 = !showFullBase64">
                  {{ showFullBase64 ? t("messages.formatRaw") : t("messages.fullValue") }}
                </button>
                <button class="icon-button" type="button" :title="t('messages.downloadValue')" @click="downloadValue"><Download /></button>
                <button class="icon-button" type="button" :title="t('messages.copyValue')" data-testid="copy-value" @click="copyDetail('value')"><Copy /></button>
              </span>
            </div>
            <div v-show="sectionsOpen.value" class="detail-block__body">
              <div class="kafka-form kafka-form--bare detail-view-form">
                <label class="field">
                  <span>{{ t("messages.decode") }}</span>
                  <select v-model="viewDecode" @change="renderView">
                    <option value="none">none</option>
                    <option value="base64">base64</option>
                  </select>
                </label>
                <label class="field">
                  <span>{{ t("messages.decompression") }}</span>
                  <select v-model="viewDecompression" @change="renderView">
                    <option value="none">none</option>
                    <option value="gzip">gzip</option>
                    <option value="lz4">lz4</option>
                    <option value="zstd">zstd</option>
                    <option value="snappy">snappy</option>
                  </select>
                </label>
                <label class="field">
                  <span>{{ t("messages.format") }}</span>
                  <select v-model="viewFormat" @change="renderView">
                    <option value="raw">{{ t("messages.formatRaw") }}</option>
                    <option value="json">{{ t("messages.formatJson") }}</option>
                    <option value="xml">XML</option>
                    <option value="hex">{{ t("messages.formatHex") }}</option>
                    <option value="bitset">{{ t("messages.formatBitset") }}</option>
                  </select>
                </label>
              </div>
              <pre v-if="viewResult.error" class="value-view error">{{ viewResult.error }}</pre>
              <pre v-else-if="showFullBase64" class="value-view">{{ detail.valueBase64 ?? detail.valueText ?? "" }}</pre>
              <CodeEditor
                v-else
                :model-value="viewResult.text"
                :language="viewFormat === 'json' || viewFormat === 'xml' ? viewFormat : 'text'"
                disabled
                min-height="140px"
              />
            </div>
          </section>
        </div>
      </div>
    </teleport>
  </section>
</template>
