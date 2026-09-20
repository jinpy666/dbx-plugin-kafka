<script setup lang="ts">
// 生产面板：key/value/headers(JSON 校验)/partition/count≤1000/compression，
// 发送回显 partition/offset。read_only 下整体禁用 + 提示（后端同规则拒绝）。
// 布局（用户反馈重构）：单列竖排全宽——topic 只读行 → key(+base64) → value
// 大编辑器（CodeEditor json 模式，flex-grow 占面板剩余高度）→ headers 编辑器
// （实时 JSON 校验红框+错误提示）→ 发送选项栅格（partition/count/compression/
// schema 挂载区，Glue 禁用逻辑保留）→ 底部大号主色发送按钮 + 清空按钮；
// 发送结果渲染为醒目成功条。不写死 inline style，尺寸走 DBX 令牌与 class。
// Phase 3 F4：Flow 测试数据生成组（mode manual|flow）——零后端改动，复用
// produce + schema 挂载（version 缺省 = latest）；schema_random 按 subject
// 前缀发现取 latest schema 生成（生成器仅 AVRO——PROTOBUF/JSON Schema subject
// → 行内提示改用 template；手动挂载区 avro/json/protobuf 三格式均可用，后端
// encodeForProduce 对 PROTOBUF 补 Confluent message index 段）；template 走
// 占位符展开；自动停止：read_only / 校验失败 / 连续失败≥3 / 条数上限 / 时长
// 上限（Lane 2）。生成器与占位符纯函数在 lib/flowRandom（固定向量 spec）。
// Lane 2 投递参数：acks（all 默认 | 1）+ 幂等生产开关（默认开）——与后端
// franz-go 能力对齐（acks=0 不做：同步 ProduceSync 依赖 broker 响应）；
// 仅在偏离默认时随请求携带（acks!=="all" / enableIdempotence=false）。
// Phase 3 F6-4：头部显示所选 topic 分区数，partition 超界行内校验。
import { computed, nextTick, onBeforeUnmount, ref, watch } from "vue";
import { CircleCheck, Play, Send, Square } from "@lucide/vue";
import CodeEditor from "./CodeEditor.vue";
import { kafkaApi, type Compression, type ProduceAcks, type ProduceResult, type SchemaAttach, type SchemaFormat, type SchemaSubject } from "../lib/api";
import {
  clampFlowCount,
  clampFlowIntervalMs,
  expandTemplate,
  generateAvroRandom,
  matchingSchemaSubjects,
  mulberry32,
} from "../lib/flowRandom";
import { isInternalTopicName } from "../lib/topics";
import { parseHeadersJson } from "../lib/jsonText";
import { partitionInputIssue, previewText } from "../lib/uiHelpers";
import { formatTimestamp, timestampIso } from "../lib/timestamps";
import { t } from "../lib/i18n";

const props = defineProps<{
  topic: string;
  canWrite: boolean;
  /** 连接 SR provider（Phase P）：glue 时 schema 挂载区禁用并提示（管理面 only）。 */
  srProvider?: string;
  /** 所选 topic 分区数（F6-4，App 从 topics/list 行透传）：头部展示 + partition 上界校验。 */
  partitionCount?: number;
}>();

const emit = defineEmits<{
  (e: "error", message: string): void;
  (e: "notify", message: string): void;
}>();

const key = ref("");
const value = ref("");
const headersText = ref("");
const partitionText = ref("");
const count = ref("1");
const compression = ref<Compression>("none");
// Lane 2 投递参数：acks（all=默认）+ 幂等生产（默认开 = 后端 franz-go 默认，
// 仅在关闭时显式传 enableIdempotence=false；后端校验 acks=1 必须关幂等）。
const acks = ref<ProduceAcks>("all");
const idempotence = ref(true);
const sending = ref(false);
const lastResult = ref<ProduceResult | null>(null);
const localError = ref("");

// -- 会话发送历史（对齐 Confluent IDE 插件 Producer 的 Data 区）---------------------
// 手动发送与 Flow tick 的每一条 produce 都落一行（成功/失败均记录）：key/value
// 截断预览 + partition/offset + 时间 + 成败标记（失败附错误摘要）。只保留最近
// SENT_HISTORY_MAX 条（超出丢最旧，防长会话 OOM）；状态随组件卸载即弃，不持久化。
const SENT_HISTORY_MAX = 50;
const KEY_PREVIEW_MAX = 24;
const VALUE_PREVIEW_MAX = 60;
const ERROR_PREVIEW_MAX = 80;

interface SentHistoryEntry {
  seq: number;
  ok: boolean;
  keyPreview: string;
  valuePreview: string;
  partition?: number;
  offset?: number;
  at: number;
  error?: string;
}

const sentHistory = ref<SentHistoryEntry[]>([]);
let sentSeq = 0;
const historyEl = ref<HTMLElement | null>(null);

/** 追加一条发送记录（超上限丢最旧），并把历史区滚动到最新行。 */
function recordSent(entry: Omit<SentHistoryEntry, "seq" | "at">) {
  sentHistory.value.push({ ...entry, seq: (sentSeq += 1), at: Date.now() });
  if (sentHistory.value.length > SENT_HISTORY_MAX) sentHistory.value.shift();
  void nextTick(() => {
    const el = historyEl.value;
    if (el) el.scrollTop = el.scrollHeight;
  });
}

/** 由表单当前 key/value 生成历史行预览（Flow 行的 value 以实参为准）。 */
function historyPreviews(keyText: string, valueText: string) {
  return {
    keyPreview: previewText(keyText, KEY_PREVIEW_MAX),
    valuePreview: previewText(valueText, VALUE_PREVIEW_MAX),
  };
}

const topicInternal = computed(() => isInternalTopicName(props.topic));

// Phase 2：base64 直发切换（key/value 输入按 base64 解释，载荷走 keyBase64/valueBase64）
const keyIsBase64 = ref(false);
const valueIsBase64 = ref(false);
// Phase 2：schema 挂载（注册侧字段映射由 sidecar 完成）
const schemaEnabled = ref(false);
const schemaSubjects = ref<SchemaSubject[]>([]);
const schemaSubject = ref("");
const schemaVersionText = ref("");
const schemaFormat = ref<SchemaFormat>("avro");
// Phase P：Glue 仅管理面（消息编解码仅 Confluent wire format，后端 -32000 拒绝），
// 前端同步禁用挂载区并提示（保留 discoverability，不隐藏）。
const glueSchemaDisabled = computed(() => props.srProvider === "glue");
watch(glueSchemaDisabled, (glueDisabled) => {
  if (glueDisabled) schemaEnabled.value = false;
});

// headers 编辑器实时校验：错误既驱动 CodeEditor 红框，也保留发送前拦截。
const headersInvalid = computed(() => {
  const headers = parseHeadersJson(headersText.value);
  return "error" in headers ? headers.error : "";
});

const sendCount = computed(() => positiveInt(count.value, 1000, 1));

const schemaVersions = computed(() => {
  const subject = schemaSubjects.value.find((row) => row.subject === schemaSubject.value);
  const latest = subject?.latestVersion ?? 0;
  return Array.from({ length: Math.max(latest, 0) }, (_unused, index) => latest - index);
});

watch(schemaSubject, () => {
  schemaVersionText.value = "";
  const found = schemaSubjects.value.find((row) => row.subject === schemaSubject.value);
  if (found?.formats?.length) schemaFormat.value = (found.formats[0] as SchemaFormat) ?? "avro";
});

const disabled = computed(() => !props.canWrite || sending.value || !props.topic);

// 校验门禁：headers JSON 非法或 value 为空时禁用发送（原因透出到 title/aria-label）。
const sendDisabled = computed(() => disabled.value || !value.value || headersInvalid.value !== "");
const sendDisabledReason = computed(() => {
  if (!props.canWrite) return t("produce.readOnlyHint");
  if (!value.value) return t("produce.valueRequired");
  if (headersInvalid.value) return t("produce.headersInvalid", { error: headersInvalid.value });
  return "";
});

async function loadSchemaSubjects() {
  try {
    const response = await kafkaApi.schemaSubjectsList();
    schemaSubjects.value = response.subjects ?? [];
  } catch {
    schemaSubjects.value = [];
  }
}

function toggleSchema() {
  if (glueSchemaDisabled.value) return;
  schemaEnabled.value = !schemaEnabled.value;
  if (schemaEnabled.value && schemaSubjects.value.length === 0) void loadSchemaSubjects();
}

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

// 入参归一为 String（type=number 的 v-model 可能给出 number，直接 .trim 会运行时抛错）。
function positiveInt(value_: unknown, max: number, fallback: number): number {
  const parsed = Number.parseInt(String(value_ ?? "").trim(), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.min(parsed, max);
}

/** 清空消息草稿（key/value/headers/base64 标记/回显），发送选项保持不动。 */
function clearDraft() {
  key.value = "";
  value.value = "";
  headersText.value = "";
  keyIsBase64.value = false;
  valueIsBase64.value = false;
  lastResult.value = null;
  localError.value = "";
}

async function send() {
  if (disabled.value) return;
  localError.value = "";
  emit("error", "");
  if (!value.value) {
    localError.value = t("produce.valueRequired");
    return;
  }
  const headers = parseHeadersJson(headersText.value);
  if ("error" in headers) {
    localError.value = t("produce.headersInvalid", { error: headers.error });
    return;
  }
  // String 归一：type=number 的 v-model 运行时可能给 number，直接 .trim 会抛错。
  const partitionRaw = String(partitionText.value ?? "").trim();
  const partition = partitionRaw === "" ? undefined : Number.parseInt(partitionRaw, 10);
  // F6-4：非负整数 + 分区数上界统一走 partitionInputIssue（行内提示键）。
  const partitionIssue = partitionInputIssue(partitionRaw, props.partitionCount);
  if (partitionIssue) {
    localError.value = t(`produce.${partitionIssue}`);
    return;
  }
  if (partition !== undefined && !Number.isInteger(partition)) {
    localError.value = t("err.partition");
    return;
  }
  sending.value = true;
  try {
    const schema = buildSchemaAttach();
    const result = await kafkaApi.messagesProduce({
      topic: props.topic,
      ...(key.value
        ? keyIsBase64.value
          ? { keyBase64: key.value }
          : { key: key.value }
        : {}),
      ...(valueIsBase64.value ? { valueBase64: value.value } : { value: value.value }),
      ...(Object.keys(headers.headers).length > 0 ? { headers: headers.headers } : {}),
      ...(partition !== undefined ? { partition } : {}),
      count: positiveInt(count.value, 1000, 1),
      ...(compression.value !== "none" ? { compression: compression.value } : {}),
      ...(acks.value !== "all" ? { acks: acks.value } : {}),
      ...(!idempotence.value ? { enableIdempotence: false } : {}),
      ...(schema ? { schema } : {}),
    });
    lastResult.value = result;
    recordSent({ ok: true, ...historyPreviews(key.value, value.value), partition: result.partition, offset: result.offset });
    emit(
      "notify",
      `${t("produce.sent")} · ${t("produce.sentTo", { partition: lastResult.value.partition, offset: lastResult.value.offset })}`,
    );
  } catch (cause) {
    const message = cause instanceof Error ? cause.message : String(cause);
    recordSent({ ok: false, ...historyPreviews(key.value, value.value), error: message });
    emit("error", message);
  } finally {
    sending.value = false;
  }
}

// -- Flow 测试数据生成（F4）----------------------------------------------------------
// mode manual = 现状表单直发；flow = 定时循环生成 + 发送（独立于手动 send 状态）。
// 自动停止：read_only（canWrite 翻转）、表单校验失败（headers/partition/生成结果
// 非法）、发送连续失败 ≥3。全部计数与最近 1 条回显只在组内展示，不打扰全局通知。

type FlowSource = "schema_random" | "template";

// flowOn：测试数据生成组开关（勾选 = flow 模式，未勾选 = manual 现状直发）。
const flowOn = ref(false);
const flowSource = ref<FlowSource>("schema_random");
const flowCountText = ref("1");
const flowIntervalText = ref("1000");
const flowTemplateText = ref("");
const flowRunning = ref(false);
const flowSent = ref(0);
const flowLastValue = ref("");
const flowHint = ref("");
const flowStopping = ref(false);
// Lane 2 停止条件：总条数 / 总时长上限（0 = 不限），对标投递器 stop 条件；
// 纯前端 flow 循环语义（flow 逐条 produce），不进 produce 请求。
const flowMaxRecordsText = ref("0");
const flowMaxDurationText = ref("0");

const flowCount = computed(() => clampFlowCount(flowCountText.value));
const flowIntervalMs = computed(() => clampFlowIntervalMs(flowIntervalText.value));
const flowMaxRecords = computed(() => nonNegativeInt(flowMaxRecordsText.value, 100000));
const flowMaxDurationMs = computed(() => nonNegativeInt(flowMaxDurationText.value, 86_400_000));
let flowStartedAt = 0;

/** 非负整数归一（非法/负数 → 0；超出上限截断）——flow 停止条件 0=不限。 */
function nonNegativeInt(value_: unknown, max: number): number {
  const parsed = Number.parseInt(String(value_ ?? "").trim(), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return 0;
  return Math.min(parsed, max);
}
// F6-4：partition 行内校验文本（空输入 = 自动分区合法）。
const partitionIssueText = computed(() => {
  const issue = partitionInputIssue(String(partitionText.value ?? ""), props.partitionCount);
  return issue ? t(`produce.${issue}`) : "";
});
// mode=flow 且挂载相关时复用手动组的 schema 挂载选择？不——schema_random 自主
// 发现 subject（前缀匹配），无需用户先勾选挂载区；template 不挂 schema。
const flowDisabled = computed(() => !props.canWrite || !props.topic || sending.value);

let flowTimer = 0;
let flowFailures = 0;
let flowSeq = 0;

function partitionForFlow(): number | undefined {
  const raw = String(partitionText.value ?? "").trim();
  if (!raw) return undefined;
  const parsed = Number.parseInt(raw, 10);
  return Number.isInteger(parsed) && parsed >= 0 ? parsed : undefined;
}

/** 生成单条 value：schema_random 走 Avro 随机生成，template 走占位符展开。 */
function generateFlowValue(source: FlowSource, schemaText: string | null): string {
  if (source === "template") {
    return expandTemplate(flowTemplateText.value, mulberry32((flowSeq + 1) * 7919 + Date.now() % 100000));
  }
  const generated = generateAvroRandom(schemaText ?? "", mulberry32((flowSeq + 1) * 104729));
  if ("error" in generated) throw new Error(generated.error);
  return generated.value;
}

/** 启动前置校验：非法时给出行内提示且不启动（不发任何 produce）。 */
async function preflightFlow(): Promise<{ schemaText: string | null; attachSubject?: string } | null> {
  flowHint.value = "";
  if (!props.canWrite) {
    flowHint.value = t("produce.readOnlyHint");
    return null;
  }
  if (!props.topic) {
    flowHint.value = t("messages.uiNoTopicSelected");
    return null;
  }
  if (headersInvalid.value) {
    flowHint.value = t("produce.headersInvalid", { error: headersInvalid.value });
    return null;
  }
  if (partitionInputIssue(String(partitionText.value ?? ""), props.partitionCount)) {
    flowHint.value = t(`produce.${partitionInputIssue(String(partitionText.value ?? ""), props.partitionCount)}`);
    return null;
  }
  if (flowSource.value === "template") {
    if (!flowTemplateText.value.trim()) {
      flowHint.value = t("produce.flowTemplateRequired");
      return null;
    }
    return { schemaText: null };
  }
  // schema_random：Glue 挂载被后端拒绝（管理面 only），提前行内提示。
  if (glueSchemaDisabled.value) {
    flowHint.value = t("messages.schemaGlueDisabled");
    return null;
  }
  // subject 发现：subjects/list 行前缀匹配 <topic>-value（优先）/ <topic>-key。
  const response = await kafkaApi.schemaSubjectsList();
  const subjects = response.subjects ?? [];
  const matched = matchingSchemaSubjects(props.topic, subjects.map((row) => row.subject));
  const subjectName = matched.value ?? matched.key;
  if (!subjectName) {
    flowHint.value = t("produce.flowNoSubject", { topic: props.topic });
    return null;
  }
  const row = subjects.find((entry) => entry.subject === subjectName);
  const format = (row?.formats ?? ["avro"])[0] ?? "avro";
  // 命中 PROTOBUF / JSON Schema subject → 行内提示改用 template（前端只做 AVRO 生成）。
  if (format !== "avro") {
    flowHint.value = t("produce.flowSchemaNotAvro", { subject: subjectName, format });
    return null;
  }
  const detail = await kafkaApi.schemaGet(subjectName);
  const schemaText = detail.schema ?? "";
  // 生成器预演一条：schema 结构不支持时启动即失败，不如不启动。
  const probe = generateAvroRandom(schemaText, mulberry32(1));
  if ("error" in probe) {
    flowHint.value = t("produce.flowGenerateFailed", { error: probe.error });
    return null;
  }
  return { schemaText, attachSubject: subjectName };
}

function stopFlow(reason?: "manual" | "failures" | "readonly" | "records" | "duration") {
  if (flowTimer) {
    window.clearInterval(flowTimer);
    flowTimer = 0;
  }
  if (!flowRunning.value) return;
  flowRunning.value = false;
  if (reason === "failures") flowHint.value = t("produce.flowAutoStoppedFailures");
  else if (reason === "readonly") flowHint.value = t("produce.readOnlyHint");
  else if (reason === "records") flowHint.value = t("produceAdv.flowAutoStoppedRecords");
  else if (reason === "duration") flowHint.value = t("produceAdv.flowAutoStoppedDuration");
}

/** 单 tick：生成 countPerSend 条并逐条 produce（count=1，逐条独立生成数据）。 */
async function runFlowTick(schemaText: string | null, schemaAttach?: SchemaAttach) {
  const seq = ++flowSeq;
  const headers = parseHeadersJson(headersText.value);
  if ("error" in headers) {
    flowHint.value = t("produce.headersInvalid", { error: headers.error });
    stopFlow("failures");
    return;
  }
  for (let index = 0; index < flowCount.value; index += 1) {
    if (!flowRunning.value) return;
    // 时长停止条件在每条发送前兜底检查（tick 间隔下保持精度）。
    if (flowMaxDurationMs.value > 0 && Date.now() - flowStartedAt >= flowMaxDurationMs.value) {
      stopFlow("duration");
      return;
    }
    let value: string;
    try {
      value = generateFlowValue(flowSource.value, schemaText);
    } catch (cause) {
      flowHint.value = t("produce.flowGenerateFailed", { error: cause instanceof Error ? cause.message : String(cause) });
      stopFlow("failures");
      return;
    }
    try {
      const result = await kafkaApi.messagesProduce({
        topic: props.topic,
        value,
        ...(key.value ? { key: key.value } : {}),
        ...(Object.keys(headers.headers).length > 0 ? { headers: headers.headers } : {}),
        ...(partitionForFlow() !== undefined ? { partition: partitionForFlow() } : {}),
        ...(compression.value !== "none" ? { compression: compression.value } : {}),
        ...(acks.value !== "all" ? { acks: acks.value } : {}),
        ...(!idempotence.value ? { enableIdempotence: false } : {}),
        ...(schemaAttach ? { schema: schemaAttach } : {}),
      });
      flowSent.value += 1;
      flowFailures = 0;
      flowLastValue.value = value.length > 200 ? `${value.slice(0, 200)}…` : value;
      // 会话历史逐条落行（对齐 Confluent IDE Data 区）。
      recordSent({ ok: true, ...historyPreviews(key.value, value), partition: result.partition, offset: result.offset });
      // 停止条件：总条数 / 总时长（任一命中即停，条数优先）。
      if (flowMaxRecords.value > 0 && flowSent.value >= flowMaxRecords.value) {
        stopFlow("records");
        return;
      }
      if (flowMaxDurationMs.value > 0 && Date.now() - flowStartedAt >= flowMaxDurationMs.value) {
        stopFlow("duration");
        return;
      }
    } catch (cause) {
      flowFailures += 1;
      const message = cause instanceof Error ? cause.message : String(cause);
      recordSent({ ok: false, ...historyPreviews(key.value, value), error: message });
      emit("error", message);
      // 发送连续失败 ≥3 → 自动停止（read_only 由 canWrite watcher 兜底）。
      if (flowFailures >= 3) {
        stopFlow("failures");
        return;
      }
    }
    if (seq !== flowSeq) return;
  }
}

async function startFlow() {
  if (flowRunning.value || flowDisabled.value) return;
  emit("error", "");
  let preflight: Awaited<ReturnType<typeof preflightFlow>>;
  try {
    preflight = await preflightFlow();
  } catch (cause) {
    // SR 请求失败（subjects/list、schema/get）此前一路 reject 成未捕获异常：
    // 点击「开始」后无任何反馈。走统一错误横幅（与 runFlowTick 同一口径）。
    emit("error", cause instanceof Error ? cause.message : String(cause));
    return;
  }
  if (preflight === null) return;
  flowSent.value = 0;
  flowLastValue.value = "";
  flowFailures = 0;
  flowStartedAt = Date.now();
  flowRunning.value = true;
  const schemaAttach: SchemaAttach | undefined =
    flowSource.value === "schema_random" && preflight.attachSubject
      ? { subject: preflight.attachSubject, format: "avro" }
      : undefined;
  const runTick = () => void runFlowTick(preflight.schemaText, schemaAttach);
  runTick();
  flowTimer = window.setInterval(runTick, flowIntervalMs.value);
}

watch(
  () => props.canWrite,
  (writable) => {
    // read_only 翻转（宿主/策略层）→ 自动停止。
    if (!writable && flowRunning.value) stopFlow("readonly");
  },
);

watch(
  () => props.topic,
  () => {
    // topic 切换：生成上下文（subject 前缀/模板校验）失效，直接停。
    stopFlow();
    flowHint.value = "";
  },
);

onBeforeUnmount(() => stopFlow());
</script>

<template>
  <section class="section-block produce-panel panel-fill">
    <p v-if="!canWrite" class="form-error produce-readonly-hint">{{ t("produce.readOnlyHint") }}</p>
    <div class="kafka-form produce-form">
      <div class="field produce-field-full">
        <span>{{ t("messages.topic") }}</span>
        <div class="produce-topic-row">
          <input :value="topic" type="text" class="mono" readonly :placeholder="t('messages.topicUnselected')" :title="topic || t('messages.topicUnselected')" />
          <span v-if="partitionCount !== undefined && partitionCount > 0" class="badge" :title="t('produce.partitionCountTitle')">
            {{ t("produce.partitionCount", { count: partitionCount }) }}
          </span>
          <span v-if="topicInternal" class="badge badge-internal">{{ t("tree.internal") }}</span>
        </div>
      </div>

      <div class="produce-row">
        <label class="field produce-grow-field">
          <span>{{ t("produce.keyLabel") }}</span>
          <input v-model="key" type="text" :placeholder="t('produce.keyPlaceholder')" :disabled="disabled" spellcheck="false" />
        </label>
        <label class="checkbox">
          <input v-model="keyIsBase64" type="checkbox" :disabled="disabled" />
          <span>{{ t("produce.base64Key") }}</span>
        </label>
      </div>

      <div class="produce-editor-block produce-editor-block--value">
        <div class="produce-editor-head">
          <span>{{ t("produce.valueLabel") }}</span>
          <span v-if="value" class="produce-char-count">{{ t("produce.charCount", { count: value.length }) }}</span>
          <span class="produce-head-spacer"></span>
          <label class="checkbox">
            <input v-model="valueIsBase64" type="checkbox" :disabled="disabled" />
            <span>{{ t("produce.base64Value") }}</span>
          </label>
        </div>
        <CodeEditor
          v-model="value"
          language="json"
          class="produce-value-editor"
          :disabled="disabled"
          :placeholder="t('produce.valuePlaceholder')"
          min-height="240px"
        />
      </div>

      <div class="produce-editor-block">
        <div class="produce-editor-head">
          <span>{{ t("produce.headers") }}</span>
        </div>
        <CodeEditor
          v-model="headersText"
          language="json"
          :disabled="disabled"
          :invalid="headersInvalid !== ''"
          :placeholder="t('produce.headersPlaceholder')"
          min-height="96px"
          max-height="160px"
        />
        <p v-if="headersInvalid" class="form-error">{{ t("produce.headersInvalid", { error: headersInvalid }) }}</p>
      </div>

      <div class="produce-options">
        <label class="field" :class="{ 'is-invalid': Boolean(partitionIssueText) }">
          <span>{{ t("produce.partition") }}</span>
          <input v-model="partitionText" type="number" min="0" :placeholder="t('produce.partitionAny')" :disabled="disabled" spellcheck="false" />
          <span v-if="partitionIssueText" class="form-error" data-testid="partition-issue">{{ partitionIssueText }}</span>
        </label>
        <label class="field">
          <span>{{ t("produce.count") }} (≤1000)</span>
          <input v-model="count" type="number" min="1" max="1000" :disabled="disabled" />
        </label>
        <label class="field">
          <span>{{ t("produce.compression") }}</span>
          <select v-model="compression" :disabled="disabled">
            <option value="none">none</option>
            <option value="gzip">gzip</option>
            <option value="lz4">lz4</option>
            <option value="zstd">zstd</option>
            <option value="snappy">snappy</option>
          </select>
        </label>
        <label class="field">
          <span>{{ t("produceAdv.acks") }}</span>
          <select v-model="acks" :disabled="disabled" data-testid="produce-acks">
            <option value="all">all</option>
            <option value="1">1 (leader)</option>
          </select>
        </label>
        <div class="field produce-schema-field">
          <label class="checkbox">
            <input v-model="idempotence" type="checkbox" :disabled="disabled" data-testid="produce-idempotence" />
            <span>{{ t("produceAdv.idempotence") }}</span>
          </label>
          <p class="hint">{{ t("produceAdv.idempotenceHint") }}</p>
        </div>
        <div class="field produce-schema-field">
          <label class="checkbox">
            <input type="checkbox" :checked="schemaEnabled" :disabled="disabled || glueSchemaDisabled" @change="toggleSchema" />
            <span>{{ t("messages.schemaMount") }}</span>
          </label>
          <div v-if="schemaEnabled" class="produce-schema-grid">
            <label class="field">
              <span>{{ t("messages.schemaSubject") }}</span>
              <select v-model="schemaSubject" :disabled="disabled || glueSchemaDisabled">
                <option value="">{{ t("acls.anyValue") }}</option>
                <option v-for="subject in schemaSubjects" :key="subject.subject" :value="subject.subject">{{ subject.subject }}</option>
              </select>
            </label>
            <label class="field">
              <span>{{ t("messages.schemaVersion") }}</span>
              <select v-model="schemaVersionText" :disabled="disabled || glueSchemaDisabled">
                <option value="">{{ t("messages.schemaLatest") }}</option>
                <option v-for="version in schemaVersions || []" :key="version" :value="String(version)">{{ version }}</option>
              </select>
            </label>
            <label class="field">
              <span>{{ t("schemas.colFormat") }}</span>
              <select v-model="schemaFormat" :disabled="disabled || glueSchemaDisabled">
                <option value="avro">avro</option>
                <option value="json">json</option>
                <option value="protobuf">protobuf</option>
              </select>
            </label>
          </div>
          <p v-if="glueSchemaDisabled" class="hint">{{ t("messages.schemaGlueDisabled") }}</p>
        </div>
      </div>

      <!-- F4 Flow 测试数据生成：mode manual=现状直发；flow=定时生成+发送。
           启停按钮/运行徽标/累计计数与最近 1 条回显只在组内展示。 -->
      <div class="produce-flow" data-testid="flow-group">
        <div class="produce-flow-head">
          <label class="checkbox">
            <input v-model="flowOn" type="checkbox" :disabled="disabled" data-testid="flow-toggle" />
            <span>{{ t("produce.flowMode") }}</span>
          </label>
          <span v-if="flowRunning" class="badge badge-ok produce-flow-badge" data-testid="flow-badge">
            <span class="flow-dot" aria-hidden="true" />{{ t("produce.flowRunning") }}
          </span>
          <span v-if="flowRunning" class="produce-flow-counter">{{ t("produce.flowSent", { count: flowSent }) }}</span>
          <span class="produce-head-spacer"></span>
          <button v-if="!flowRunning" class="qb-add" type="button" :disabled="flowDisabled" :title="t('produce.flowStart')" data-testid="flow-start" @click="startFlow">
            <Play aria-hidden="true" />{{ t("produce.flowStart") }}
          </button>
          <button v-else class="qb-add" type="button" :title="t('produce.flowStop')" data-testid="flow-stop" @click="stopFlow('manual')">
            <Square aria-hidden="true" />{{ t("produce.flowStop") }}
          </button>
        </div>
        <div class="produce-flow-grid" :class="{ muted: !flowOn }">
          <label class="field">
            <span>{{ t("produce.flowSource") }}</span>
            <select v-model="flowSource" :disabled="disabled || flowRunning">
              <option value="schema_random">{{ t("produce.flowSourceSchemaRandom") }}</option>
              <option value="template">{{ t("produce.flowSourceTemplate") }}</option>
            </select>
          </label>
          <label class="field">
            <span>{{ t("produce.flowCountPerSend") }}</span>
            <input v-model="flowCountText" type="number" min="1" max="100" :disabled="disabled || flowRunning" />
          </label>
          <label class="field">
            <span>{{ t("produce.flowIntervalMs") }}</span>
            <input v-model="flowIntervalText" type="number" min="250" max="10000" step="50" :disabled="disabled || flowRunning" />
          </label>
          <label class="field">
            <span>{{ t("produceAdv.flowMaxRecords") }}</span>
            <input v-model="flowMaxRecordsText" type="number" min="0" max="100000" :disabled="disabled || flowRunning" data-testid="flow-max-records" />
          </label>
          <label class="field">
            <span>{{ t("produceAdv.flowMaxDurationMs") }}</span>
            <input v-model="flowMaxDurationText" type="number" min="0" step="1000" :disabled="disabled || flowRunning" data-testid="flow-max-duration" />
          </label>
        </div>
        <div v-if="flowOn && flowSource === 'template'" class="produce-editor-block">
          <div class="produce-editor-head">
            <span>{{ t("produce.flowTemplateLabel") }}</span>
            <span class="hint">{{ t("produce.flowTemplateHint") }}</span>
          </div>
          <CodeEditor
            v-model="flowTemplateText"
            language="json"
            :disabled="disabled"
            :placeholder="t('produce.flowTemplatePlaceholder')"
            min-height="96px"
            max-height="200px"
          />
        </div>
        <p v-if="flowHint" class="form-error" data-testid="flow-hint">{{ flowHint }}</p>
        <p v-if="flowRunning && flowLastValue" class="hint produce-flow-last mono-s" data-testid="flow-last">
          {{ t("produce.flowLastMessage") }}: {{ flowLastValue }}
        </p>
      </div>

      <div v-if="lastResult" class="produce-success" role="status">
        <CircleCheck aria-hidden="true" />
        <span>
          <strong>{{ t("produce.sent") }}</strong> ·
          {{ t("produce.sentTo", { partition: lastResult.partition, offset: lastResult.offset }) }}
        </span>
      </div>

      <div class="produce-actions">
        <p v-if="localError" class="form-error produce-error">{{ localError }}</p>
        <span class="produce-head-spacer"></span>
        <button class="produce-ghost-button" type="button" :disabled="disabled" @click="clearDraft">
          {{ t("produce.clear") }}
        </button>
        <button
          class="primary-button produce-send-button"
          type="button"
          :disabled="sendDisabled"
          :title="sendDisabledReason || undefined"
          :aria-label="sendDisabledReason || t('produce.send')"
          @click="send"
        >
          <Send aria-hidden="true" />
          {{ sendCount > 1 ? t("produce.sendCount", { count: sendCount }) : t("produce.send") }}
        </button>
      </div>

      <!-- 会话发送历史（对齐 Confluent IDE Producer 的 Data 区）：手动与 Flow 的
           每一条 produce 都落行，最新在底部并自动滚动；仅保留最近 50 条。 -->
      <div class="produce-history" data-testid="sent-history">
        <div class="produce-editor-head">
          <span>{{ t("produce.historyTitle") }}</span>
          <span v-if="sentHistory.length > 0" class="badge">{{ t("produce.historyRecent", { count: SENT_HISTORY_MAX }) }}</span>
        </div>
        <div v-if="sentHistory.length === 0" class="empty compact">{{ t("produce.historyEmpty") }}</div>
        <div v-else ref="historyEl" class="produce-history-rows">
          <div
            v-for="entry in sentHistory"
            :key="entry.seq"
            class="produce-history-row"
            data-testid="history-row"
            :title="entry.error ?? entry.valuePreview"
          >
            <span class="badge" :class="entry.ok ? 'badge-ok' : 'badge-danger'" data-testid="history-status">
              {{ entry.ok ? t("produce.historyOk") : t("produce.historyError") }}
            </span>
            <span class="mono-s produce-history-key" :title="entry.keyPreview">{{ entry.keyPreview || "—" }}</span>
            <span class="mono-s produce-history-value">{{ entry.valuePreview }}</span>
            <span v-if="!entry.ok" class="form-error produce-history-err" :title="entry.error">
              {{ previewText(entry.error, ERROR_PREVIEW_MAX) }}
            </span>
            <span class="mono-s produce-history-part" :title="t('produce.partition')">P{{ entry.partition ?? "—" }}</span>
            <span class="mono-s produce-history-offset">#{{ entry.offset ?? "—" }}</span>
            <span class="mono-s produce-history-time" :title="timestampIso(entry.at)">{{ formatTimestamp(entry.at) }}</span>
          </div>
        </div>
      </div>
    </div>
  </section>
</template>

<style scoped>
/* 面板撑满 main-pane 剩余高度，供 value 编辑器 flex-grow。 */
.produce-panel {
  flex: 1 1 auto;
  min-height: 0;
}
.produce-readonly-hint {
  padding: 6px 8px 0;
}
/* 单列竖排：覆盖 .kafka-form 的横排 wrap/border，其余（.field/.checkbox/输入
   尺寸 26px/字号 12px）沿用全局 DBX 令牌样式。 */
.produce-form {
  flex: 1 1 auto;
  min-height: 0;
  flex-direction: column;
  flex-wrap: nowrap;
  align-items: stretch;
  gap: 10px;
  overflow: auto;
  border-bottom: 0;
}
.produce-field-full {
  /* 竖排：topic 只读行不参与剩余高度分配（否则与 value 编辑器争抢 flex 空间） */
  flex: 0 0 auto;
}
.produce-topic-row {
  display: flex;
  min-width: 0;
  align-items: center;
  gap: 6px;
}
.produce-topic-row input {
  min-width: 0;
  flex: 1;
}
.produce-row {
  display: flex;
  min-width: 0;
  align-items: flex-end;
  gap: 8px;
}
.produce-grow-field {
  min-width: 0;
  flex: 1;
}
.produce-editor-block {
  display: flex;
  min-width: 0;
  min-height: 0;
  flex-direction: column;
  gap: 4px;
}
.produce-editor-block--value {
  /* value 编辑器占面板剩余高度（下限 240px） */
  flex: 1 1 auto;
  min-height: 240px;
}
.produce-editor-head {
  display: flex;
  align-items: center;
  gap: 8px;
  color: var(--muted-foreground);
  font-size: 10px;
}
.produce-head-spacer {
  flex: 1;
}
.produce-char-count {
  color: var(--primary);
}
.produce-value-editor {
  flex: 1 1 auto;
}
/* 发送选项一行栅格：窄容器自动换行（~700px 不破版）。 */
.produce-options {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: 8px;
  align-items: start;
  border-top: 1px solid var(--border);
  padding-top: 10px;
}
.produce-schema-field {
  gap: 4px;
}
.produce-schema-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(110px, 1fr));
  gap: 6px;
}
/* 发送成功条：醒目绿色横条（替代原小字 hint）。 */
.produce-success {
  display: flex;
  align-items: center;
  gap: 8px;
  border: 1px solid color-mix(in srgb, var(--success) 55%, var(--border));
  border-radius: 4px;
  padding: 8px 12px;
  background: color-mix(in srgb, var(--success) 10%, var(--background));
  color: color-mix(in srgb, var(--success) 75%, var(--foreground));
  font-size: 12px;
}
.produce-success svg {
  width: 15px;
  height: 15px;
  flex: 0 0 15px;
}
/* 底部操作行：右侧大号主色发送按钮 + 清空按钮。 */
.produce-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}
.produce-error {
  flex: 1 1 auto;
}
.produce-ghost-button {
  min-height: 32px;
  border: 1px solid var(--border);
  border-radius: 4px;
  padding: 4px 14px;
  background: var(--background);
  color: var(--foreground);
  cursor: pointer;
}
.produce-ghost-button:hover:not(:disabled) {
  background: var(--accent);
}
.produce-send-button {
  min-height: 32px;
  padding: 4px 20px;
  font-size: 12px;
  font-weight: 600;
}
.produce-send-button svg {
  width: 14px;
  height: 14px;
}
/* P2-7：只读/校验失败的禁用态视觉强化——主色按钮降饱和
   （title/aria 已由发送逻辑给出原因），dark/light 均成立。
   通用 cursor/复选框禁用规则已收敛至全局 style.css。 */
.produce-send-button:disabled {
  filter: grayscale(0.65) saturate(0.4);
  opacity: 0.55;
}
.produce-ghost-button:disabled {
  opacity: 0.45;
}
/* F4 Flow 组：头部（开关+徽标+计数+启停）一行，参数栅格复用 produce-options 基线。 */
.produce-flow {
  display: flex;
  min-width: 0;
  flex-direction: column;
  gap: 6px;
  border-top: 1px solid var(--border);
  padding-top: 10px;
}
.produce-flow-head {
  display: flex;
  align-items: center;
  gap: 8px;
}
.produce-flow-badge {
  gap: 5px;
}
.flow-dot {
  width: 6px;
  height: 6px;
  border-radius: 999px;
  background: var(--success);
  animation: flow-pulse 1.2s ease-in-out infinite;
}
@keyframes flow-pulse {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.35; }
}
.produce-flow-counter {
  color: var(--primary);
  font-size: 11px;
}
.produce-flow-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: 8px;
  align-items: start;
}
.produce-flow-last {
  overflow-wrap: anywhere;
}
/* 会话发送历史（对齐 Confluent IDE Data 区）：紧凑单行列表，最新在底部自动
   滚动；全局类只补 flex/截断等最小布局（不依赖 style.css 改动）。 */
.produce-history {
  display: flex;
  min-width: 0;
  flex-direction: column;
  gap: 4px;
  border-top: 1px solid var(--border);
  padding-top: 10px;
}
.produce-history-rows {
  display: flex;
  max-height: 168px;
  min-height: 0;
  flex-direction: column;
  gap: 2px;
  overflow: auto;
}
.produce-history-row {
  display: flex;
  min-width: 0;
  align-items: center;
  gap: 8px;
  font-size: 11px;
}
.produce-history-key,
.produce-history-value,
.produce-history-err {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.produce-history-key {
  flex: 0 0 110px;
}
.produce-history-value {
  flex: 1 1 auto;
}
.produce-history-err {
  flex: 0 1 240px;
  white-space: nowrap;
}
.produce-history-part,
.produce-history-offset,
.produce-history-time {
  flex: 0 0 auto;
  color: var(--muted-foreground);
  font-variant-numeric: tabular-nums;
}
</style>
