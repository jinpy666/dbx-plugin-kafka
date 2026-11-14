/**
 * 消费结果表状态（自 MessagesPanel.vue 拆出）：result 行数组的浅响应管理、
 * capRows 裁剪重建、行点击打开详情、quickFilter 防抖、时区切换重建。
 */
import { computed, onBeforeUnmount, ref, shallowRef, triggerRef, watch, type Ref } from "vue";
import type { ColDef } from "ag-grid-community";
import type { ConsumeResult, KafkaMessage } from "../lib/api";
import type { MessageRow } from "../lib/kafkaColumns";
import { messageColumns, toMessageRows, toggleWorkbenchTimestampTz, workbenchTimestampTz } from "../lib/kafkaColumns";
import { t } from "../lib/i18n";
import { capRows, debounce } from "../lib/uiHelpers";

export interface UseConsumeResultsOptions {
  /** 详情抽屉 detail（openDetail 直接引用行内 raw，不拷贝）。 */
  detail: Ref<KafkaMessage | null>;
  /** 行内「复制 JSON」动作（复制成功提示由组件统一处理）。 */
  onCopyJson: (row: MessageRow) => void;
}

export function useConsumeResults(options: UseConsumeResultsOptions) {
  // 大数据量防护：result/行数组/详情 raw 均浅响应（shallowRef）——大数组不做深度
  // 代理，整体替换引用驱动更新；行上限裁剪见 applyResult/capRows。
  const result = shallowRef<ConsumeResult | null>(null);
  // 行数组浅响应 + 引用替换（不逐条改）；rowsTotal 为裁前行数（裁剪提示用）。
  const messageRows = shallowRef<MessageRow[]>([]);
  const rowsTotal = ref(0);
  const rowsDropped = computed(() => Math.max(0, rowsTotal.value - messageRows.value.length));
  const messageCols = computed(() =>
    messageColumns({ onCopyJson: options.onCopyJson }) as ColDef<MessageRow>[],
  );

  // -- 即时搜索（F6-1）/时区切换（F6-3）-------------------------------------------
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

  /** 行数组重建（tz 切换/结果落地共用）：capRows 裁剪 → 倒序（最新在前）→
   *  toMessageRows（按当前时区格式化）→ 引用替换一次性提交。
   *  后端按 offset 升序返回（= 时间升序），倒序后最新消息落在首行，与其它
   *  Kafka 工作台的时间倒序展示一致；capRows 必须先取尾部（最新 N 条）再
   *  倒序，否则会把最旧的一批翻到前面。 */
  function rebuildRows() {
    const capped = capRows(result.value?.messages ?? []);
    messageRows.value = toMessageRows([...capped.rows].reverse());
    triggerRef(messageRows);
    rowsTotal.value = capped.total;
  }

  /** 消费结果落地（唯一入口）：capRows 裁剪（保留最新 N 条）→ 倒序构建行数组 →
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
    options.detail.value = row.raw;
  }

  return {
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
    rebuildRows,
    applyResult,
    openDetail,
  };
}
