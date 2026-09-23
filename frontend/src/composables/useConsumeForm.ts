/**
 * 消费表单状态（自 MessagesPanel.vue 拆出）：§5.3 全参数表单 refs、摘要条
 * chips、筛选区分组开合记忆、时间双模式输入、fieldFilters 编辑、schema 挂载、
 * ConsumeParams 构建与消费预设（save/apply/remove）。组件保留消费执行、
 * MCP intent、导出等编排逻辑。
 *
 * 纯状态 + 纯函数拼装；副作用只有 pluginStore 开合记忆与 kafkaApi 预设调用。
 */
import { computed, ref, watch } from "vue";
import { t } from "../lib/i18n";
import { MSG_FILTERS_KEY, MSG_FORM_OPEN_KEY, pluginStore } from "../lib/pluginStore";
import { kafkaApi, type ConsumeParams, type DecodeMode, type Decompression, type FieldFilter, type IsolationLevel, type MatchMode, type OffsetStrategy, type SchemaAttach, type SchemaFormat, type SchemaSubject } from "../lib/api";
import {
  fieldFilterIssue,
  isRangeReversed,
  nowDatetimeLocal,
  offsetTimeToParam,
  offsetTimeToUnixMs,
  parsePartitionList,
  parsePartitionOffsetsText,
  switchTimeInputMode,
  validateConsumeForm,
} from "../lib/consumeForm";

/** 非空正整数解析（limit/timeout/maxScan 等）；非法回退 fallback。 */
export function positiveInt(value: unknown, fallback: number): number {
  const parsed = Number.parseInt(String(value ?? "").trim(), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

/** 可空数字解析（offsetFrom/To）：空串 → undefined，非数字 → undefined。 */
export function optionalNumber(value: unknown): number | undefined {
  // P1-6：Vue 3 对 <input type="number"> 的 v-model 可能给 number（科学计数/清空
  // 过程），直接 .trim() 抛 TypeError 且消费请求不发出——入参一律 String 归一
  // （与 ProducePanel/GroupsPanel 同范式修复）。
  const trimmed = String(value ?? "").trim();
  if (!trimmed) return undefined;
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : undefined;
}

export interface UseConsumeFormOptions {
  /** 当前选中 topic（App 单一来源；buildParams 缺省 topic 用）。 */
  topic: () => string;
  /** 连接 SR provider（Phase P）：glue 时 schema 挂载区禁用。 */
  srProvider: () => string | undefined;
  notify: (message: string) => void;
  error: (message: string) => void;
}

export function useConsumeForm(options: UseConsumeFormOptions) {
  // -- form state ---------------------------------------------------------------

  const groupId = ref("");
  // 默认 recent（每分区从日志末端回退扫描窗口起读）：latest 只等新消息，查历史
  // 永远「已扫描 0」（issue #16）；流式面板不受影响（订阅语义本就是 tail）。
  const offsetStrategy = ref<OffsetStrategy>("recent");
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

  const presets = ref<Array<{ id: string; name: string }>>([]);
  const presetName = ref("");

  // -- 摘要条（收起态）：开合记忆 dbx.kafka.ui.msgFormOpen；无记忆时
  // 「未选 topic 或尚无结果」默认展开、消费成功后自动收起为摘要条。-------------
  // （键常量与持久化通道统一在 lib/pluginStore：宿主 storage → localStorage 降级。）

  function loadStoredFormOpen(): boolean | null {
    try {
      const raw = pluginStore.getItem(MSG_FORM_OPEN_KEY);
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
      pluginStore.setItem(MSG_FORM_OPEN_KEY, open ? "1" : "0");
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
    recent: "messages.strategyRecent",
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
  const GROUP_DEFAULTS: Record<ConsumeGroupKey, boolean> = { basic: true, locate: true, timeRange: true, filter: true, decode: false };
  // 可折叠记忆的组 = 除「基础」「定位」外的三组（前两组常驻展开，不落盘）。
  const COLLAPSIBLE_GROUPS = ["timeRange", "filter", "decode"] as const;

  function loadOpenGroups(): Record<ConsumeGroupKey, boolean> {
    const open = { ...GROUP_DEFAULTS };
    try {
      const raw = JSON.parse(pluginStore.getItem(MSG_FILTERS_KEY) ?? "") as Partial<Record<ConsumeGroupKey, unknown>> | null;
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
        pluginStore.setItem(
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
  const glueSchemaDisabled = computed(() => options.srProvider() === "glue");
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
      topic: topicOverride ?? options.topic(),
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
      options.notify(t("messages.presetSaved"));
      await loadPresets();
    } catch (cause) {
      options.error(cause instanceof Error ? cause.message : String(cause));
    }
  }

  async function applyPreset(id: string) {
    try {
      const response = await kafkaApi.presetsList();
      const preset = (response.presets ?? []).find((row) => row.id === id);
      if (!preset) return;
      const params = preset.params ?? {};
      groupId.value = params.groupId ?? "";
      offsetStrategy.value = params.offsetStrategy ?? "recent";
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
      options.notify(t("messages.presetApplied"));
    } catch (cause) {
      options.error(cause instanceof Error ? cause.message : String(cause));
    }
  }

  async function removePreset(id: string) {
    try {
      await kafkaApi.presetsRemove(id);
      await loadPresets();
      options.notify(t("messages.presetRemoved"));
    } catch (cause) {
      options.error(cause instanceof Error ? cause.message : String(cause));
    }
  }

  return {
    // form state
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
    // 摘要条
    formOpen,
    toggleFormOpen,
    strategyLabel,
    decodeLabel,
    filterCount,
    // 分组开合
    openGroups,
    toggleGroup,
    // 时间双模式
    tsMode,
    tsFromMs,
    tsToMs,
    tsRangeReversed,
    tsFromInvalid,
    tsToInvalid,
    toggleTsMode,
    setNow,
    // fieldFilters
    fieldFilterIssueKey,
    fieldFilterIssueText,
    addFieldFilter,
    removeFieldFilter,
    // schema 挂载
    schemaEnabled,
    schemaSubjects,
    schemaSubject,
    schemaVersionText,
    schemaFormat,
    glueSchemaDisabled,
    schemaVersions,
    // 互斥/联动
    filtersDisabled,
    groupDisabled,
    partitionsDisabled,
    hasFilters,
    // params / presets
    buildParams,
    loadPresets,
    savePreset,
    applyPreset,
    removePreset,
    presets,
    presetName,
  };
}
