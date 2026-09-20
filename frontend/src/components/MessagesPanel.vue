<script setup lang="ts">
// 一次性消费面板：消费表单覆盖 §5.3 全参数（5 种 offset 策略、per-partition
// 精确 seek、isolation/commit 互斥、三通道过滤 + matchMode + fieldFilters、
// 时间/offset 范围、decode/decompression），消息表 + 详情抽屉（本地二次
// decode/format、valueBase64 完整查看/下载）+ JSON/CSV 导出 + 消费预设。
// commit×过滤互斥等校验在 lib/consumeForm.validateConsumeForm（纯函数，有单测）。
// 布局压缩（R 路）：有结果后表单默认收起为一行摘要 chips 条（开合记忆
// dbx.kafka.ui.msgFormOpen），结果表格吃满剩余高度；大数据量防护见各标注。
import { nextTick, ref, watch } from "vue";
import { ChevronDown, ChevronsDown, Download, Play, Plus, Save, SlidersHorizontal, Trash2, X } from "@lucide/vue";
import { messageFullValueText } from "../lib/messageCodec";
import DbxAgGrid from "./DbxAgGrid.vue";
import MessageDetailDrawer from "./MessageDetailDrawer.vue";
import { kafkaApi, type ConsumeResult, type KafkaMessage, type KafkaTopic, type MatchMode, type OffsetStrategy } from "../lib/api";
import type { MessageRow } from "../lib/kafkaColumns";
import { messageCellCopyText, MINIMAL_MESSAGE_FIELDS } from "../lib/kafkaColumns";
import { serializeMessagesToJson, serializeMessagesToTsv } from "../lib/messageExport";
import { saveTextFile, type SaveFileOutcome } from "../lib/download";
import { copyTextToClipboard } from "../lib/uiHelpers";
import { nowDatetimeLocal, validateConsumeForm } from "../lib/consumeForm";
import { positiveInt, useConsumeForm } from "../composables/useConsumeForm";
import { useConsumeResults } from "../composables/useConsumeResults";
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

// -- form state（消费表单 + 预设：全部状态与参数构建在 useConsumeForm）--------

const form = useConsumeForm({
  topic: () => props.topic,
  srProvider: () => props.srProvider,
  notify: (message) => emit("notify", message),
  error: (message) => emit("error", message),
});
const {
  groupId,
  offsetStrategy,
  offsetTimeText,
  partitionsText,
  partitionOffsetsText,
  limit,
  timeoutMs,
  maxScanRecords,
  isolationLevel,
  commit,
  filterText,
  keyFilterText,
  valueFilterText,
  headerFilterText,
  matchMode,
  fieldFilters,
  timestampFrom,
  timestampTo,
  offsetFrom,
  offsetTo,
  decode,
  decompression,
  formOpen,
  toggleFormOpen,
  strategyLabel,
  decodeLabel,
  filterCount,
  openGroups,
  toggleGroup,
  tsMode,
  tsFromMs,
  tsToMs,
  tsRangeReversed,
  tsFromInvalid,
  tsToInvalid,
  toggleTsMode,
  setNow,
  fieldFilterIssueKey,
  fieldFilterIssueText,
  addFieldFilter,
  removeFieldFilter,
  schemaEnabled,
  schemaSubjects,
  schemaSubject,
  schemaVersionText,
  schemaFormat,
  glueSchemaDisabled,
  schemaVersions,
  filtersDisabled,
  groupDisabled,
  partitionsDisabled,
  hasFilters,
  buildParams,
  loadPresets,
  savePreset,
  applyPreset,
  removePreset,
  presets,
  presetName,
} = form;

const consuming = ref(false);
// 消费表单校验问题（i18n 文案；runConsume / applyIntentConsume 两处写入）。
const formIssues = ref<string[]>([]);

// 摘要 chips：策略文案映射（无新增 i18n key，复用既有 strategy*/formatRaw）。
// 生效过滤条件数 / 分组开合 / 时间双模式：见 useConsumeForm（composable）。

// -- 共享详情抽屉 + 结果表（useConsumeResults）------------

const detail = ref<KafkaMessage | null>(null);

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

// P1-1：结果区统计行锚点（消费后滚动目标）。
const resultMetaEl = ref<HTMLElement | null>(null);
// 消息表实例（跳到最新经 DbxAgGrid.goToLatest 走 gridApi：末页 + 滚入视口）。
const messagesGrid = ref<InstanceType<typeof DbxAgGrid> | null>(null);

const {
  result,
  messageRows,
  rowsTotal,
  rowsDropped,
  messageCols,
  quickFilterInput,
  quickFilter,
  applyQuickFilter,
  workbenchTimestampTz,
  tzLabel,
  toggleTz,
  applyResult,
  openDetail,
} = useConsumeResults({ detail, onCopyJson: (row) => void copyMessageJson(row) });

/** 跳到最新（R 路）：滚回结果区锚点 + 经 DbxAgGrid.goToLatest 跳分页末页并
 *  把最后一行滚入视口底部（最新数据行可见；分页模式由 gridApi 处理）。 */
function jumpToLatest() {
  resultMetaEl.value?.scrollIntoView({ block: "start" });
  messagesGrid.value?.goToLatest();
}

// -- field filters 编辑 / 参数构建 / 表单校验：见 useConsumeForm（composable）。

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

const INTENT_STRATEGIES: OffsetStrategy[] = ["recent", "latest", "earliest", "committed", "timestamp", "offset"];

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

// -- presets：load/save/apply/remove 见 useConsumeForm（composable）。------------

// -- export ------------------------------------------------------------------------

async function exportMessages(format: "json" | "csv" | "tsv") {
  if (!result.value || result.value.messages.length === 0) return;
  // Lane4 打磨：后端 kafka/messages/export 仅接受 json/csv（其余 -32000），
  // TSV 走前端序列化——直接导出当前已加载结果行（复用 messageExport 的保真
  // value 文本与 TSV 转义；列序/行分隔与后端 CSV 一致），不发额外请求。
  if (format === "tsv") {
    await saveExport("kafka-messages.tsv", "text/tab-separated-values", serializeMessagesToTsv(result.value.messages), "TSV");
    return;
  }
  try {
    const response = await kafkaApi.messagesExport({
      ...buildParams(),
      format,
      limit: Math.max(result.value.messages.length, positiveInt(limit.value, 100), 1),
    });
    await saveExport(response.filename || `kafka-messages.${format}`, response.contentType, response.content, format.toUpperCase());
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  }
}

/** 保存导出内容（宿主另存为优先/网页下载兜底，见 lib/download）；用户取消另存为不提示成功。 */
async function saveExport(name: string, contentType: string, content: string, label: string) {
  const outcome = await saveTextFile(name, contentType, content);
  if (outcome.mode !== "canceled") emit("notify", t("messages.exportDone", { name: label }));
}

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
        :title="!topic ? t('messages.topicRequired') : tsRangeReversed ? t('messages.uiTimeRangeInvalid') : undefined"
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
                <option value="recent">{{ t("messages.strategyRecent") }}</option>
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
        :cell-copy-text="messageCellCopyText"
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
      <!-- 未消费（初始/切 topic 清空）与 0 条命中分开提示：0 条才是「调整条件」，
           未消费引导点消费（否则切 topic 后看到的是误导性空态）。 -->
      <p class="empty compact">{{ topic ? t("messages.pendingConsume") : t("messages.uiNoTopicSelected") }}</p>
    </div>

    <MessageDetailDrawer v-model="detail" @notify="emit('notify', $event)" @error="emit('error', $event)" />
  </section>
</template>
