/**
 * ag-grid column definitions for every Kafka workbench table (Phase 2).
 *
 * Centralised here so sorting/filter types stay consistent across panels and
 * stay unit-testable: builders are pure functions over the locale-aware `t()`
 * (components rebuild them inside `computed` so a locale switch re-renders
 * headers). Row data is passed through small `to*Rows()` view-model mappers so
 * display text (previews, timestamps) is formatted once, not per-cell.
 *
 * Pagination page size persists per table key in localStorage (commercial
 * parity with tinyrdm KafkaGrid). `minimalColumns()` powers the narrow
 * container degradation to the minimal column set.
 */
import type { ColDef, ValueFormatterParams } from "ag-grid-community";
import { ref } from "vue";
import type {
  GroupMember,
  GroupOffsetRow,
  KafkaAcl,
  KafkaGroup,
  KafkaMessage,
  KafkaTopic,
  SchemaSubject,
  SchemaVersionRow,
  TopicOffsetRow,
  TopicPartitionInfo,
} from "./api";
import { formatTimestamp, timestampFilterTextComparator, timestampIso, headersPreview, previewText, type TimestampTz } from "./kafkaModel";
import { t, workbenchLocale } from "./i18n";

// -- timestamp timezone（F6-3：本地/UTC 切换，localStorage `kafka.ts.tz` 记忆）-----
// 行映射（to*Rows）与列 valueFormatter 共同消费该 ref：MessagesPanel 切换后
// computed 重建列与行，时间戳单元格即时换区；宿主 webview 禁存储时静默降级。

export const TIMESTAMP_TZ_STORAGE_KEY = "kafka.ts.tz";

function loadStoredTimestampTz(): TimestampTz {
  try {
    return localStorage.getItem(TIMESTAMP_TZ_STORAGE_KEY) === "utc" ? "utc" : "local";
  } catch {
    return "local";
  }
}

export const workbenchTimestampTz = ref<TimestampTz>(loadStoredTimestampTz());

export function setWorkbenchTimestampTz(tz: TimestampTz): void {
  workbenchTimestampTz.value = tz === "utc" ? "utc" : "local";
  try {
    localStorage.setItem(TIMESTAMP_TZ_STORAGE_KEY, workbenchTimestampTz.value);
  } catch {
    /* 存储不可用：仅内存态 */
  }
}

export function toggleWorkbenchTimestampTz(): TimestampTz {
  const next: TimestampTz = workbenchTimestampTz.value === "utc" ? "local" : "utc";
  setWorkbenchTimestampTz(next);
  return next;
}

/** 单元格 title：完整 ISO-8601（行内 raw.timestamp → ISO；缺失返回空串不悬停）。 */
export function timestampIsoTooltip(params: { data?: { raw?: { timestamp?: number } } | null }): string {
  return timestampIso(params.data?.raw?.timestamp);
}

// -- view models ----------------------------------------------------------------

export interface MessageRow {
  id: string;
  partition: number;
  offset: number;
  timestampText: string;
  keyText: string;
  valueText: string;
  headersText: string;
  schemaText: string;
  raw: KafkaMessage;
}

export interface GroupRow {
  group: string;
  state: string;
  protocolType: string;
  coordinator: string;
  raw: KafkaGroup;
}

export interface GroupOffsetVm {
  id: string;
  topic: string;
  partition: number;
  startOffset: string;
  endOffset: string;
  committedText: string;
  lag: number | null;
  raw: GroupOffsetRow;
}

export interface MemberVm {
  memberId: string;
  instanceId: string;
  clientId: string;
  clientHost: string;
  assignments: string;
  raw: GroupMember;
}

export interface AclVm {
  resourceType: string;
  resourceName: string;
  patternType: string;
  principal: string;
  host: string;
  operation: string;
  permission: string;
  raw: KafkaAcl;
}

export interface TopicVm {
  name: string;
  partitionCount: number;
  replicationFactor: number;
  internalText: string;
  raw: KafkaTopic;
}

export interface PartitionVm {
  partition: number;
  leader: number;
  replicas: string;
  isr: string;
  offline: string;
  healthyText: string;
  healthy: boolean;
  raw: TopicPartitionInfo;
}

export interface TopicOffsetVm {
  partition: number;
  offset: number;
  timestampText: string;
  leaderEpoch: string;
  raw: TopicOffsetRow;
}

export interface SubjectVm {
  subject: string;
  formats: string;
  latestVersion: number | null;
  compatibilityLevel: string;
  raw: SchemaSubject;
}

export interface SchemaVersionVm {
  version: number;
  id: number;
  format: string;
  raw: SchemaVersionRow;
}

export interface LagVm {
  id: string;
  topic: string;
  partition: number;
  committedText: string;
  endOffset: string;
  lag: number | null;
  raw: GroupOffsetRow;
}

// -- row mappers ------------------------------------------------------------------

export function toMessageRows(messages: KafkaMessage[]): MessageRow[] {
  return messages.map((message) => ({
    id: `${message.partition}:${message.offset}`,
    partition: message.partition,
    offset: message.offset,
    timestampText: formatTimestamp(message.timestamp, workbenchTimestampTz.value),
    keyText: previewText(message.key, 60),
    valueText: previewText(message.valueText, 160),
    headersText: headersPreview(message.headers),
    schemaText: message.schemaSubject ? `${message.schemaSubject} v${message.schemaVersion ?? "?"}` : "",
    raw: message,
  }));
}

export function toGroupRows(groups: KafkaGroup[]): GroupRow[] {
  return groups.map((group) => ({
    group: group.group,
    state: group.state ?? "—",
    protocolType: group.protocolType ?? "—",
    coordinator: group.coordinator === undefined || group.coordinator === null ? "—" : String(group.coordinator),
    raw: group,
  }));
}

export function toGroupOffsetRows(rows: GroupOffsetRow[]): GroupOffsetVm[] {
  return rows.map((row) => ({
    id: `${row.topic}:${row.partition}`,
    topic: row.topic,
    partition: row.partition,
    startOffset: row.startOffset === undefined || row.startOffset === null ? "—" : String(row.startOffset),
    endOffset: row.endOffset === undefined || row.endOffset === null ? "—" : String(row.endOffset),
    committedText:
      row.hasCommitted === false ? t("groups.hasCommittedFalse") : row.committedOffset === undefined || row.committedOffset === null ? "—" : String(row.committedOffset),
    lag: row.lag === undefined || row.lag === null ? null : Number(row.lag),
    raw: row,
  }));
}

export function toMemberRows(members: GroupMember[]): MemberVm[] {
  return members.map((member) => ({
    memberId: member.memberId,
    instanceId: member.instanceId ?? "—",
    clientId: member.clientId ?? "—",
    clientHost: member.clientHost ?? "—",
    assignments: Object.entries(member.assignments ?? {})
      .map(([topic, partitions]) => `${topic}[${partitions.join(",")}]`)
      .join("; "),
    raw: member,
  }));
}

export function toAclRows(acls: KafkaAcl[]): AclVm[] {
  return acls.map((acl) => ({
    resourceType: acl.resourceType,
    resourceName: acl.resourceName,
    patternType: acl.patternType || "LITERAL",
    principal: acl.principal,
    host: acl.host || "*",
    operation: acl.operation,
    permission: acl.permission,
    raw: acl,
  }));
}

export function toTopicRows(topics: KafkaTopic[]): TopicVm[] {
  return topics.map((topic) => ({
    name: topic.name,
    partitionCount: topic.partitionCount,
    replicationFactor: topic.replicationFactor,
    internalText: topic.isInternal || topic.name.startsWith("_") ? t("topics.colInternal") : "",
    raw: topic,
  }));
}

export function toPartitionRows(partitions: TopicPartitionInfo[]): PartitionVm[] {
  return partitions.map((partition) => ({
    partition: partition.partition,
    leader: partition.leader,
    replicas: partition.replicas.join(","),
    isr: partition.isr.join(","),
    offline: partition.offlineReplicas.length > 0 ? partition.offlineReplicas.join(",") : "—",
    healthyText: partition.isHealthy === false ? t("topics.unhealthy") : t("topics.healthy"),
    healthy: partition.isHealthy !== false,
    raw: partition,
  }));
}

export function toTopicOffsetRows(rows: TopicOffsetRow[]): TopicOffsetVm[] {
  return rows.map((row) => ({
    partition: row.partition,
    offset: row.offset,
    timestampText: formatTimestamp(row.timestamp, workbenchTimestampTz.value),
    leaderEpoch: row.leaderEpoch === undefined || row.leaderEpoch === null ? "—" : String(row.leaderEpoch),
    raw: row,
  }));
}

export function toSubjectRows(subjects: SchemaSubject[]): SubjectVm[] {
  return subjects.map((subject) => ({
    subject: subject.subject,
    formats: (subject.formats ?? []).join(", "),
    latestVersion: subject.latestVersion === undefined || subject.latestVersion === null ? null : Number(subject.latestVersion),
    compatibilityLevel: subject.compatibilityLevel ?? "",
    raw: subject,
  }));
}

export function toSchemaVersionRows(rows: SchemaVersionRow[]): SchemaVersionVm[] {
  return rows.map((row) => ({ version: row.version, id: row.id, format: row.format, raw: row }));
}

export function toLagRows(rows: GroupOffsetRow[]): LagVm[] {
  return rows.map((row) => ({
    id: `${row.topic}:${row.partition}`,
    topic: row.topic,
    partition: row.partition,
    committedText: row.hasCommitted === false ? t("groups.hasCommittedFalse") : row.committedOffset === undefined || row.committedOffset === null ? "—" : String(row.committedOffset),
    endOffset: row.endOffset === undefined || row.endOffset === null ? "—" : String(row.endOffset),
    lag: row.lag === undefined || row.lag === null ? null : Number(row.lag),
    raw: row,
  }));
}

// -- column builders ---------------------------------------------------------------

function textColumn(field: string, headerKey: string, extra: Partial<ColDef> = {}): ColDef {
  return { field, headerName: t(headerKey), sortable: true, resizable: true, filter: "agTextColumnFilter", ...extra };
}

/**
 * 时间戳列（F6-6）：agDateColumnFilter（ag-grid community 自带）+ 比较器按
 * 生成时区解析单元格文本；单元格文本 = formatTimestamp(tz)，title 悬停完整 ISO。
 */
function timestampColumn(headerKey: string, extra: Partial<ColDef> = {}): ColDef {
  return {
    field: "timestampText",
    headerName: t(headerKey),
    sortable: true,
    resizable: true,
    filter: "agDateColumnFilter",
    filterParams: {
      // 比较器捕获当前 tz（列在 computed 内构建，tz/locale 切换都会重建）。
      comparator: (filterLocalDateAtMidnight: Date, cellValue: unknown) =>
        timestampFilterTextComparator(filterLocalDateAtMidnight, cellValue, workbenchTimestampTz.value),
    },
    cellClass: "mono-s",
    tooltipValueGetter: timestampIsoTooltip,
    ...extra,
  };
}

/**
 * 行操作列（F6-2 复制 JSON / F5 克隆）：cellRenderer 返回原生 button DOM
 * （DbxAgGrid 是 vanilla createGrid，不走 Vue 组件渲染器）。不排序/过滤。
 */
function actionColumn<T>(
  label: string,
  titleKey: string,
  onAction: (data: T) => void,
  extra: Partial<ColDef<T>> = {},
): ColDef<T> {
  return {
    colId: `action-${label}`,
    headerName: "",
    sortable: false,
    resizable: false,
    filter: false,
    suppressMovable: true,
    width: 64,
    cellRenderer: (params: { data?: T | null }) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "grid-action-button";
      button.textContent = label;
      button.title = t(titleKey);
      button.setAttribute("aria-label", t(titleKey));
      button.addEventListener("click", (event) => {
        event.stopPropagation();
        if (params.data) onAction(params.data);
      });
      return button;
    },
    ...extra,
  };
}

function numberColumn(field: string, headerKey: string, extra: Partial<ColDef> = {}): ColDef {
  return {
    field,
    headerName: t(headerKey),
    sortable: true,
    resizable: true,
    filter: "agNumberColumnFilter",
    cellClass: "numeric",
    // P2-23：数值单元格统一千分位（跟随工作台 locale），与 ag-grid 分页条
    // 「共 5,000」同屏格式一致；非数值文本（如「—」占位）原样透传。
    valueFormatter: (params: ValueFormatterParams) => formatNumberCell(params.value),
    ...extra,
  };
}

// P2-23：千分位格式化（Intl.NumberFormat，按 locale 缓存实例）。入参兼容
// number 与字符串数字（startOffset/endOffset 的 VM 是 string）；非有限数值
// （"—" 占位等）原样返回，空值返回空串。
const numberFormatters = new Map<string, Intl.NumberFormat>();

function formatNumberCell(value: unknown): string {
  if (value === undefined || value === null || value === "") return "";
  const num = typeof value === "number" ? value : Number(String(value).trim());
  if (!Number.isFinite(num)) return String(value);
  const locale = workbenchLocale.value;
  let formatter = numberFormatters.get(locale);
  if (!formatter) {
    formatter = new Intl.NumberFormat(locale);
    numberFormatters.set(locale, formatter);
  }
  return formatter.format(num);
}

export function messageColumns(options: { onCopyJson?: (row: MessageRow) => void } = {}): ColDef<MessageRow>[] {
  return [
    numberColumn("partition", "messages.colPartition", { maxWidth: 90 }),
    numberColumn("offset", "messages.colOffset", { maxWidth: 120 }),
    timestampColumn("messages.colTimestamp", { minWidth: 150 }),
    textColumn("keyText", "messages.colKey", { cellClass: "mono-s" }),
    textColumn("valueText", "messages.colValue", { flex: 2, minWidth: 180, tooltipField: "valueText" }),
    textColumn("headersText", "messages.colHeaders", { cellClass: "mono-s", tooltipField: "headersText" }),
    textColumn("schemaText", "messages.colSchema", { cellClass: "mono-s", minWidth: 120 }),
    // F6-2：行操作「复制 JSON」（传入回调才出列，窄容器降级集不含它）。
    ...(options.onCopyJson ? [actionColumn<MessageRow>("{}", "messages.copyRowJson", options.onCopyJson)] : []),
  ];
}

export function groupColumns(): ColDef<GroupRow>[] {
  return [
    textColumn("group", "groups.colGroup", { flex: 1.4, cellClass: "mono-s" }),
    {
      ...textColumn("state", "groups.colState", { maxWidth: 150 }),
      valueFormatter: (params: ValueFormatterParams<GroupRow>) => {
        const raw = String(params.value ?? "").trim();
        if (!raw || raw === "—") return raw || "—";
        const pascal = raw.replace(/(^|[-_])([a-z])/g, (_m, _s, c) => c.toUpperCase());
        // P2-12 回归修复：状态文案键实际挂在 messages.state* 命名空间（七语皆有），
        // 以 i18n 实际存在的键为准查找；未命中枚举原文兜底。
        const key = `messages.state${pascal}`;
        const localized = t(key);
        return localized === key ? raw : localized;
      },
    } as ColDef<GroupRow>,
    textColumn("protocolType", "groups.colProtocol", { maxWidth: 120 }),
    textColumn("coordinator", "groups.colCoordinator", { maxWidth: 110, cellClass: "mono-s" }),
  ];
}

export function groupOffsetColumns(): ColDef<GroupOffsetVm>[] {
  return [
    textColumn("topic", "groups.colTopic", { flex: 1.4, cellClass: "mono-s" }),
    numberColumn("partition", "groups.colPartition", { maxWidth: 90 }),
    numberColumn("startOffset", "groups.colStart", { maxWidth: 110 }),
    numberColumn("endOffset", "groups.colEnd", { maxWidth: 110 }),
    textColumn("committedText", "groups.colCommitted", { maxWidth: 130, cellClass: "mono-s" }),
    numberColumn("lag", "groups.colLag", { maxWidth: 110 }),
  ];
}

export function memberColumns(): ColDef<MemberVm>[] {
  return [
    textColumn("memberId", "groups.memberId", { flex: 1.2, cellClass: "mono-s" }),
    textColumn("instanceId", "groups.instanceId", { cellClass: "mono-s" }),
    textColumn("clientId", "groups.clientId", { cellClass: "mono-s" }),
    textColumn("clientHost", "groups.clientHost", { cellClass: "mono-s" }),
    textColumn("assignments", "groups.assignments", { flex: 1.4, cellClass: "mono-s" }),
  ];
}

export function aclColumns(): ColDef<AclVm>[] {
  return [
    textColumn("resourceType", "acls.resourceType", { maxWidth: 130, cellClass: "mono-s" }),
    textColumn("resourceName", "acls.resourceName", { flex: 1.2, cellClass: "mono-s" }),
    textColumn("patternType", "acls.patternType", { maxWidth: 110, cellClass: "mono-s" }),
    textColumn("principal", "acls.principal", { flex: 1, cellClass: "mono-s" }),
    textColumn("host", "acls.host", { maxWidth: 110, cellClass: "mono-s" }),
    textColumn("operation", "acls.operation", { maxWidth: 130, cellClass: "mono-s" }),
    textColumn("permission", "acls.permission", { maxWidth: 110, cellClass: "mono-s" }),
  ];
}

/**
 * topic 表列（Lane4 打磨）：可选收藏星标列（传入 onToggleFavorite 才出现）。
 * 星标文案在 cellRenderer 内经 isFavorite 回调现读（模块级收藏态），配合
 * 调用方在收藏变化时重建 columnDefs 即可刷新；列不排序/过滤，不参与
 * 窄容器降级集（不入 MINIMAL_TOPIC_FIELDS）。
 */
export function topicColumns(
  options: { onToggleFavorite?: (row: TopicVm) => void; isFavorite?: (row: TopicVm) => boolean } = {},
): ColDef<TopicVm>[] {
  const favoriteColumn: ColDef<TopicVm>[] = options.onToggleFavorite
    ? [
        {
          colId: "action-favorite",
          headerName: "",
          sortable: false,
          resizable: false,
          filter: false,
          suppressMovable: true,
          width: 40,
          cellRenderer: (params: { data?: TopicVm | null }) => {
            const button = document.createElement("button");
            button.type = "button";
            button.className = "grid-action-button topic-star-button";
            // data 缺省（虚拟滚动竞态）按未收藏兜底渲染，不触发回调。
            const fav = params.data ? options.isFavorite?.(params.data) === true : false;
            button.textContent = fav ? "★" : "☆";
            button.title = t(fav ? "polish.favoriteRemove" : "polish.favoriteAdd");
            button.setAttribute("aria-label", button.title);
            button.addEventListener("click", (event) => {
              event.stopPropagation();
              if (params.data) options.onToggleFavorite?.(params.data);
            });
            return button;
          },
        } as ColDef<TopicVm>,
      ]
    : [];
  return [
    ...favoriteColumn,
    textColumn("name", "topics.colTopic", { flex: 2, cellClass: "mono-s" }),
    textColumn("internalText", "topics.colInternal", { maxWidth: 90, filter: false }),
    numberColumn("partitionCount", "topics.colPartitions", { maxWidth: 100 }),
    numberColumn("replicationFactor", "topics.colReplication", { maxWidth: 80 }),
  ];
}

export function partitionColumns(): ColDef<PartitionVm>[] {
  return [
    numberColumn("partition", "topics.colPartition", { maxWidth: 90 }),
    numberColumn("leader", "topics.colLeader", { maxWidth: 90 }),
    textColumn("replicas", "topics.colReplicas", { maxWidth: 120, cellClass: "mono-s" }),
    textColumn("isr", "topics.colIsr", { maxWidth: 120, cellClass: "mono-s" }),
    textColumn("offline", "topics.colOffline", { maxWidth: 110, cellClass: "mono-s" }),
    {
      field: "healthyText",
      headerName: t("topics.colHealthy"),
      sortable: true,
      resizable: true,
      filter: false,
      maxWidth: 110,
      cellClass: (params) => (params.data?.healthy ? "dbx-cell-ok" : "dbx-cell-bad"),
    },
  ];
}

export function topicOffsetColumns(): ColDef<TopicOffsetVm>[] {
  return [
    numberColumn("partition", "topics.colPartition", { maxWidth: 90 }),
    numberColumn("offset", "messages.colOffset", { maxWidth: 130 }),
    timestampColumn("messages.colTimestamp", { minWidth: 150 }),
    textColumn("leaderEpoch", "Epoch", { maxWidth: 90, cellClass: "mono-s", headerName: "Epoch" }),
  ];
}

export function subjectColumns(): ColDef<SubjectVm>[] {
  return [
    textColumn("subject", "schemas.colSubject", { flex: 2, cellClass: "mono-s" }),
    textColumn("formats", "schemas.colFormats", { maxWidth: 120, cellClass: "mono-s" }),
    numberColumn("latestVersion", "schemas.colLatestVersion", { maxWidth: 110 }),
    textColumn("compatibilityLevel", "schemas.colCompatibility", { maxWidth: 170, cellClass: "mono-s" }),
  ];
}

export function schemaVersionColumns(options: { onClone?: (row: SchemaVersionVm) => void } = {}): ColDef<SchemaVersionVm>[] {
  return [
    numberColumn("version", "schemas.colVersion", { maxWidth: 100 }),
    numberColumn("id", "schemas.colId", { maxWidth: 110 }),
    textColumn("format", "schemas.colFormat", { maxWidth: 100, cellClass: "mono-s" }),
    // F5：版本表行操作「克隆」（注册弹窗预填 schema 文本）。
    ...(options.onClone ? [actionColumn<SchemaVersionVm>("⧉", "schemas.clone", options.onClone)] : []),
  ];
}

export function lagColumns(): ColDef<LagVm>[] {
  return [
    textColumn("topic", "groups.colTopic", { flex: 1.4, cellClass: "mono-s" }),
    numberColumn("partition", "groups.colPartition", { maxWidth: 90 }),
    textColumn("committedText", "monitor.colCommitted", { maxWidth: 130, cellClass: "mono-s" }),
    numberColumn("endOffset", "monitor.colEnd", { maxWidth: 120 }),
    numberColumn("lag", "groups.colLag", { maxWidth: 110 }),
  ];
}

// -- narrow-container degradation ---------------------------------------------------

/**
 * Keep only the columns whose `field` is listed (used below the responsive
 * threshold). Pure so panels can unit-test their minimal sets.
 */
export function minimalColumns<T>(defs: ColDef<T>[], fields: string[]): ColDef<T>[] {
  const wanted = new Set(fields);
  return defs.filter((def) => typeof def.field === "string" && wanted.has(def.field));
}

/** 窄容器阈值（px）：低于此宽度表格降级到 minimal 列集。 */
export const GRID_COMPACT_WIDTH = 560;

export const MINIMAL_MESSAGE_FIELDS = ["partition", "offset", "valueText"];
export const MINIMAL_GROUP_FIELDS = ["group", "state"];
export const MINIMAL_GROUP_OFFSET_FIELDS = ["topic", "partition", "lag"];
export const MINIMAL_MEMBER_FIELDS = ["memberId", "assignments"];
export const MINIMAL_ACL_FIELDS = ["resourceType", "resourceName", "principal"];
export const MINIMAL_TOPIC_FIELDS = ["name", "partitionCount"];
export const MINIMAL_PARTITION_FIELDS = ["partition", "leader", "healthyText"];
export const MINIMAL_TOPIC_OFFSET_FIELDS = ["partition", "offset"];
export const MINIMAL_SUBJECT_FIELDS = ["subject", "latestVersion"];
export const MINIMAL_VERSION_FIELDS = ["version", "id"];
export const MINIMAL_LAG_FIELDS = ["topic", "partition", "lag"];

// -- page size persistence -----------------------------------------------------------

const PAGE_SIZE_STORAGE_PREFIX = "dbx-kafka-grid-pagesize-";
export const PAGE_SIZE_OPTIONS = [20, 50, 100, 200];
export const DEFAULT_PAGE_SIZE = 50;

function storage(): Storage | null {
  try {
    return typeof localStorage === "undefined" ? null : localStorage;
  } catch {
    return null; // 宿主 webview 禁用 localStorage 时的静默兜底
  }
}

export function loadPreferredPageSize(tableKey: string): number {
  const raw = storage()?.getItem(PAGE_SIZE_STORAGE_PREFIX + tableKey) ?? "";
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_PAGE_SIZE;
}

export function savePreferredPageSize(tableKey: string, size: number): void {
  const store = storage();
  if (!store) return;
  try {
    store.setItem(PAGE_SIZE_STORAGE_PREFIX + tableKey, String(size));
  } catch {
    // quota/private mode → 分页偏好放弃持久化即可
  }
}

// -- ag-grid built-in chrome locale（七语；键集对齐 AG_GRID_LOCALE_KEYS 守卫） --------
// P2-19：ag-grid v36 分页条（PageSummaryComp/RowSummaryComp/paginationComp）实际
// 消费的键：to / of / page / more / number（行摘要「1 to 50 of 100」与页摘要
// 「Page 1 of 2」）、firstPage/previousPage/nextPage/lastPage（翻页按钮 aria）、
// ariaPageSizeSelectorLabel + pageSizeSelectorLabel（页大小选择器）。
// Phase 3 F6-6（date/number filter 键，从 ag-grid 36.1.0 dist 包内核对的实际
// 消费点）：dateFormatOoo（DateFilter.setDateComponent 每输入框 placeholder）、
// before/after（DateFilter.translate 把 lessThan/greaterThan 归一为 before/after）、
// lessThanOrEqual/greaterThanOrEqual（数字 filter 选项）、inRangeStart/inRangeEnd
// （resetPlaceholder 的双输入 placeholder）、ariaDateFilterInput（只读日期输入
// aria）、invalidDate/invalidNumber（dataType 格式化校验文案）、blank/notBlank/
// empty（零输入选项名）。

export const AG_GRID_LOCALE_KEYS = [
  "searchOoo",
  "blanks",
  "noRowsToShow",
  "page",
  "to",
  "of",
  "more",
  "number",
  "firstPage",
  "previousPage",
  "nextPage",
  "lastPage",
  "pageSizeSelectorLabel",
  "ariaPageSizeSelectorLabel",
  "filterOoo",
  "equals",
  "notEqual",
  "contains",
  "notContains",
  "startsWith",
  "endsWith",
  "greaterThan",
  "lessThan",
  "inRange",
  "andCondition",
  "orCondition",
  "applyFilter",
  "resetFilter",
  "cancelFilter",
  "dateFormatOoo",
  "before",
  "after",
  "greaterThanOrEqual",
  "lessThanOrEqual",
  "inRangeStart",
  "inRangeEnd",
  "ariaDateFilterInput",
  "invalidDate",
  "invalidNumber",
  "blank",
  "notBlank",
  "empty",
] as const;

type AgLocaleText = Record<(typeof AG_GRID_LOCALE_KEYS)[number], string>;

const AG_LOCALE_TEXT: Record<string, AgLocaleText> = {
  en: {
    searchOoo: "Search…",
    blanks: "(Blanks)",
    noRowsToShow: "No rows",
    page: "Page",
    to: "to",
    of: "of",
    more: "more",
    number: "number",
    firstPage: "First Page",
    previousPage: "Previous Page",
    nextPage: "Next Page",
    lastPage: "Last Page",
    pageSizeSelectorLabel: "Page size:",
    ariaPageSizeSelectorLabel: "Page size",
    filterOoo: "Filter…",
    equals: "Equals",
    notEqual: "Not equal",
    contains: "Contains",
    notContains: "Not contains",
    startsWith: "Starts with",
    endsWith: "Ends with",
    greaterThan: "Greater than",
    lessThan: "Less than",
    inRange: "In range",
    andCondition: "AND",
    orCondition: "OR",
    applyFilter: "Apply",
    resetFilter: "Reset",
    cancelFilter: "Cancel",
    dateFormatOoo: "yyyy-mm-dd",
    before: "Before",
    after: "After",
    greaterThanOrEqual: "Greater or equal",
    lessThanOrEqual: "Less or equal",
    inRangeStart: "From",
    inRangeEnd: "To",
    ariaDateFilterInput: "Date Filter Input",
    invalidDate: "Invalid Date",
    invalidNumber: "Invalid Number",
    blank: "Blank",
    notBlank: "Not blank",
    empty: "Empty",
  },
  "zh-CN": {
    searchOoo: "搜索…",
    blanks: "（空）",
    noRowsToShow: "暂无数据",
    page: "第",
    to: "至",
    of: "/ 共",
    more: "更多",
    number: "页",
    firstPage: "第一页",
    previousPage: "上一页",
    nextPage: "下一页",
    lastPage: "最后一页",
    pageSizeSelectorLabel: "每页条数：",
    ariaPageSizeSelectorLabel: "每页条数",
    filterOoo: "过滤…",
    equals: "等于",
    notEqual: "不等于",
    contains: "包含",
    notContains: "不包含",
    startsWith: "开头为",
    endsWith: "结尾为",
    greaterThan: "大于",
    lessThan: "小于",
    inRange: "介于",
    andCondition: "且",
    orCondition: "或",
    applyFilter: "应用",
    resetFilter: "重置",
    cancelFilter: "取消",
    dateFormatOoo: "年-月-日",
    before: "早于",
    after: "晚于",
    greaterThanOrEqual: "大于等于",
    lessThanOrEqual: "小于等于",
    inRangeStart: "起",
    inRangeEnd: "止",
    ariaDateFilterInput: "日期过滤输入",
    invalidDate: "无效日期",
    invalidNumber: "无效数字",
    blank: "为空",
    notBlank: "不为空",
    empty: "空",
  },
  "zh-TW": {
    searchOoo: "搜尋…",
    blanks: "（空）",
    noRowsToShow: "尚無資料",
    page: "第",
    to: "至",
    of: "/ 共",
    more: "更多",
    number: "頁",
    firstPage: "第一頁",
    previousPage: "上一頁",
    nextPage: "下一頁",
    lastPage: "最後一頁",
    pageSizeSelectorLabel: "每頁筆數：",
    ariaPageSizeSelectorLabel: "每頁筆數",
    filterOoo: "過濾…",
    equals: "等於",
    notEqual: "不等於",
    contains: "包含",
    notContains: "不包含",
    startsWith: "開頭為",
    endsWith: "結尾為",
    greaterThan: "大於",
    lessThan: "小於",
    inRange: "介於",
    andCondition: "且",
    orCondition: "或",
    applyFilter: "套用",
    resetFilter: "重設",
    cancelFilter: "取消",
    dateFormatOoo: "年-月-日",
    before: "早於",
    after: "晚於",
    greaterThanOrEqual: "大於等於",
    lessThanOrEqual: "小於等於",
    inRangeStart: "起",
    inRangeEnd: "迄",
    ariaDateFilterInput: "日期過濾輸入",
    invalidDate: "無效日期",
    invalidNumber: "無效數字",
    blank: "空白",
    notBlank: "非空白",
    empty: "空",
  },
  es: {
    searchOoo: "Buscar…",
    blanks: "(Vacíos)",
    noRowsToShow: "Sin filas",
    page: "Página",
    to: "a",
    of: "de",
    more: "más",
    number: "número",
    firstPage: "Primera página",
    previousPage: "Página anterior",
    nextPage: "Página siguiente",
    lastPage: "Última página",
    pageSizeSelectorLabel: "Tamaño de página:",
    ariaPageSizeSelectorLabel: "Tamaño de página",
    filterOoo: "Filtrar…",
    equals: "Igual a",
    notEqual: "Distinto de",
    contains: "Contiene",
    notContains: "No contiene",
    startsWith: "Empieza por",
    endsWith: "Termina en",
    greaterThan: "Mayor que",
    lessThan: "Menor que",
    inRange: "Entre",
    andCondition: "Y",
    orCondition: "O",
    applyFilter: "Aplicar",
    resetFilter: "Restablecer",
    cancelFilter: "Cancelar",
    dateFormatOoo: "aaaa-mm-dd",
    before: "Antes de",
    after: "Después de",
    greaterThanOrEqual: "Mayor o igual",
    lessThanOrEqual: "Menor o igual",
    inRangeStart: "Desde",
    inRangeEnd: "Hasta",
    ariaDateFilterInput: "Entrada de filtro de fecha",
    invalidDate: "Fecha no válida",
    invalidNumber: "Número no válido",
    blank: "En blanco",
    notBlank: "No en blanco",
    empty: "Vacío",
  },
  it: {
    searchOoo: "Cerca…",
    blanks: "(Vuote)",
    noRowsToShow: "Nessuna riga",
    page: "Pagina",
    to: "a",
    of: "di",
    more: "altro",
    number: "numero",
    firstPage: "Prima pagina",
    previousPage: "Pagina precedente",
    nextPage: "Pagina successiva",
    lastPage: "Ultima pagina",
    pageSizeSelectorLabel: "Dimensione pagina:",
    ariaPageSizeSelectorLabel: "Dimensione pagina",
    filterOoo: "Filtra…",
    equals: "Uguale a",
    notEqual: "Diverso da",
    contains: "Contiene",
    notContains: "Non contiene",
    startsWith: "Inizia con",
    endsWith: "Termina con",
    greaterThan: "Maggiore di",
    lessThan: "Minore di",
    inRange: "Nell'intervallo",
    andCondition: "E",
    orCondition: "O",
    applyFilter: "Applica",
    resetFilter: "Reimposta",
    cancelFilter: "Annulla",
    dateFormatOoo: "aaaa-mm-gg",
    before: "Prima del",
    after: "Dopo il",
    greaterThanOrEqual: "Maggiore o uguale",
    lessThanOrEqual: "Minore o uguale",
    inRangeStart: "Da",
    inRangeEnd: "A",
    ariaDateFilterInput: "Input filtro data",
    invalidDate: "Data non valida",
    invalidNumber: "Numero non valido",
    blank: "Vuoto",
    notBlank: "Non vuoto",
    empty: "Nessun valore",
  },
  ja: {
    searchOoo: "検索…",
    blanks: "（空）",
    noRowsToShow: "データがありません",
    page: "ページ",
    to: "～",
    of: "/",
    more: "続き",
    number: "番号",
    firstPage: "最初のページ",
    previousPage: "前のページ",
    nextPage: "次のページ",
    lastPage: "最後のページ",
    pageSizeSelectorLabel: "ページサイズ：",
    ariaPageSizeSelectorLabel: "ページサイズ",
    filterOoo: "フィルター…",
    equals: "一致",
    notEqual: "不一致",
    contains: "含む",
    notContains: "含まない",
    startsWith: "前方一致",
    endsWith: "後方一致",
    greaterThan: "より大きい",
    lessThan: "より小さい",
    inRange: "範囲内",
    andCondition: "かつ",
    orCondition: "または",
    applyFilter: "適用",
    resetFilter: "リセット",
    cancelFilter: "キャンセル",
    dateFormatOoo: "yyyy-mm-dd",
    before: "以前",
    after: "以降",
    greaterThanOrEqual: "以上",
    lessThanOrEqual: "以下",
    inRangeStart: "開始",
    inRangeEnd: "終了",
    ariaDateFilterInput: "日付フィルター入力",
    invalidDate: "無効な日付",
    invalidNumber: "無効な数値",
    blank: "空欄",
    notBlank: "空欄以外",
    empty: "空",
  },
  "pt-BR": {
    searchOoo: "Pesquisar…",
    blanks: "(Vazios)",
    noRowsToShow: "Sem linhas",
    page: "Página",
    to: "a",
    of: "de",
    more: "mais",
    number: "número",
    firstPage: "Primeira página",
    previousPage: "Página anterior",
    nextPage: "Próxima página",
    lastPage: "Última página",
    pageSizeSelectorLabel: "Tamanho da página:",
    ariaPageSizeSelectorLabel: "Tamanho da página",
    filterOoo: "Filtrar…",
    equals: "Igual a",
    notEqual: "Diferente de",
    contains: "Contém",
    notContains: "Não contém",
    startsWith: "Começa com",
    endsWith: "Termina com",
    greaterThan: "Maior que",
    lessThan: "Menor que",
    inRange: "No intervalo",
    andCondition: "E",
    orCondition: "OU",
    applyFilter: "Aplicar",
    resetFilter: "Redefinir",
    cancelFilter: "Cancelar",
    dateFormatOoo: "aaaa-mm-dd",
    before: "Antes de",
    after: "Depois de",
    greaterThanOrEqual: "Maior ou igual",
    lessThanOrEqual: "Menor ou igual",
    inRangeStart: "De",
    inRangeEnd: "Até",
    ariaDateFilterInput: "Entrada do filtro de data",
    invalidDate: "Data inválida",
    invalidNumber: "Número inválido",
    blank: "Em branco",
    notBlank: "Não em branco",
    empty: "Vazio",
  },
};

/** 当前工作台 locale 对应的 ag-grid 内置文案（组件每次建表时读取，随 locale 切换重建）。 */
export function agGridLocaleText(): AgLocaleText {
  return AG_LOCALE_TEXT[workbenchLocale.value] ?? AG_LOCALE_TEXT["zh-CN"];
}
