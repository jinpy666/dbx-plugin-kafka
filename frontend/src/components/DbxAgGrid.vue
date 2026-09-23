<script setup lang="ts">
// ag-grid-community 封装（Phase 2）：vanilla `createGrid` + 定向 API 更新，
// 避免再引 ag-grid-vue3 依赖。DBX 视觉经 style.css 的 `.dbx-grid` 用
// --ag-* CSS 变量对齐主题令牌（light/dark 随宿主 data-theme 切换，两套都成立）。
// 内建：排序/列内过滤/分页 + 页大小 pluginStore 持久化（kafkaColumns 存取）、
// 窄容器（< GRID_COMPACT_WIDTH）降级 minimal 列集、单行选择与行点击事件。
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";
import {
  AllCommunityModule,
  ModuleRegistry,
  createGrid,
  type ColDef,
  type GridApi,
  type GridOptions,
  type RowSelectionOptions,
} from "ag-grid-community";
import {
  DEFAULT_PAGE_SIZE,
  GRID_COMPACT_WIDTH,
  PAGE_SIZE_OPTIONS,
  agGridLocaleText,
  loadPreferredPageSize,
  minimalColumns,
  savePreferredPageSize,
} from "../lib/kafkaColumns";
import { copyTextToClipboard } from "../lib/uiHelpers";
import { t } from "../lib/i18n";

export interface GridContextMenuItem {
  id: string;
  label: string;
  action: (row: unknown) => void;
  disabled?: boolean | ((row: unknown) => boolean);
  danger?: boolean;
  separatorBefore?: boolean;
}

ModuleRegistry.registerModules([AllCommunityModule]);

const props = withDefaults(
  defineProps<{
    rowData: unknown[];
    columnDefs: ColDef[];
    /** 页大小持久化键（每表唯一，如 "messages"；落 pluginStore 单键 JSON map）。 */
    tableKey: string;
    /** 窄容器降级保留的列（field 名）。缺省 = 不降级。 */
    compactFields?: string[];
    rowSelection?: "single" | false;
    /** 行高亮规则（MonitorPanel 阈值告警用）。 */
    rowClassRules?: GridOptions["rowClassRules"];
    /** 详情抽屉行点击才打开、选择仅做高亮时置 false。 */
    emitRowClick?: boolean;
    /** Full cell text for copying when row fields contain display previews. */
    cellCopyText?: (row: unknown, field: string | undefined, value: unknown) => string;
    /** 即时搜索（F6-1）：quickFilterText 只过滤已加载行（防抖在调用方）。 */
    quickFilter?: string;
    /** 行右键管理项；复制值/复制行始终由网格提供。 */
    contextMenuItems?: GridContextMenuItem[] | ((row: unknown) => GridContextMenuItem[]);
  }>(),
  { compactFields: undefined, rowSelection: "single", rowClassRules: undefined, emitRowClick: true, quickFilter: "" },
);

const emit = defineEmits<{
  (e: "rowClick", data: unknown): void;
  (e: "selectionChanged", data: unknown | null): void;
  (e: "pageSizeChanged", size: number): void;
  (e: "copy", text: string): void;
}>();

const host = ref<HTMLElement>();
const pageSize = ref(loadPreferredPageSize(props.tableKey));
let gridApi: GridApi | null = null;
let resizeObserver: ResizeObserver | null = null;
let compactActive = false;
const contextMenu = ref<{ x: number; y: number; row: unknown; value: string } | null>(null);

const contextItems = computed(() => {
  const current = contextMenu.value;
  if (!current) return [] as Array<GridContextMenuItem & { resolvedDisabled: boolean }>;
  const custom = typeof props.contextMenuItems === "function" ? props.contextMenuItems(current.row) : props.contextMenuItems ?? [];
  const copyItems: GridContextMenuItem[] = [
    {
      id: "copy-value",
      label: t("grid.copyValue"),
      action: () => void copyToClipboard(current.value),
      disabled: current.value.length === 0,
    },
    {
      id: "copy-row",
      label: t("grid.copyRow"),
      action: () => void copyToClipboard(rowText(current.row)),
    },
  ];
  return [...copyItems, ...custom].map((item) => ({
    ...item,
    resolvedDisabled: typeof item.disabled === "function" ? item.disabled(current.row) : item.disabled === true,
  }));
});

function rowText(row: unknown): string {
  if (!row || typeof row !== "object") return String(row ?? "");
  const record = row as Record<string, unknown>;
  const defs = compactActive && props.compactFields ? minimalColumns(props.columnDefs, props.compactFields) : props.columnDefs;
  const fields = defs.map((def) => def.field).filter((field): field is string => typeof field === "string");
  if (fields.length === 0) return JSON.stringify(record, null, 2);
  return fields
    .map((field) => {
      const value = record[field];
      if (props.cellCopyText) return props.cellCopyText(row, field, value);
      if (value === undefined || value === null) return "";
      return typeof value === "object" ? JSON.stringify(value) : String(value);
    })
    .join("\t");
}

async function copyToClipboard(text: string) {
  if (!text) return;
  const copied = await copyTextToClipboard(text);
  if (copied) emit("copy", text);
}

function closeContextMenu() {
  contextMenu.value = null;
}

function onContextMenuKeydown(event: KeyboardEvent) {
  if (event.key === "Escape") closeContextMenu();
}

function openContextMenu(event: Parameters<NonNullable<GridOptions["onCellContextMenu"]>>[0]) {
  const nativeEvent = event.event as MouseEvent | undefined;
  if (!event.node?.data || !nativeEvent) return;
  nativeEvent.preventDefault();
  nativeEvent.stopPropagation();
  const value = props.cellCopyText
    ? props.cellCopyText(event.node.data, event.colDef.field, event.value)
    : event.value === undefined || event.value === null ? "" : String(event.value);
  const menuWidth = 220;
  const menuHeight = 38 + (typeof props.contextMenuItems === "function" ? props.contextMenuItems(event.node.data).length : props.contextMenuItems?.length ?? 0) * 30;
  contextMenu.value = {
    x: Math.max(6, Math.min(nativeEvent.clientX, window.innerWidth - menuWidth - 6)),
    y: Math.max(6, Math.min(nativeEvent.clientY, window.innerHeight - menuHeight - 6)),
    row: event.node.data,
    value,
  };
}

// 行 id：VM 带 id 字段（消息/位点类）直接用；其余按对象身份分配稳定自增 id
// （重复空串 id 会让 ag-grid 行覆盖合并——走查发现的 subject 表 bug）。
const autoRowIds = new WeakMap<object, number>();
let autoRowIdSeq = 0;

function resolveRowId(data: unknown): string {
  if (data && typeof data === "object") {
    const explicit = (data as { id?: unknown }).id;
    if (typeof explicit === "string" && explicit) return explicit;
    let id = autoRowIds.get(data);
    if (id === undefined) {
      id = autoRowIdSeq;
      autoRowIdSeq += 1;
      autoRowIds.set(data, id);
    }
    return `auto-${id}`;
  }
  return "row";
}

function buildOptions(): GridOptions {
  return {
    columnDefs: props.columnDefs,
    rowData: props.rowData,
    defaultColDef: {
      sortable: true,
      resizable: true,
      filter: "agTextColumnFilter",
      minWidth: 64,
      suppressHeaderMenuButton: false,
    },
    pagination: true,
    paginationPageSize: pageSize.value,
    paginationPageSizeSelector: PAGE_SIZE_OPTIONS,
    rowHeight: 26,
    headerHeight: 26,
    floatingFiltersHeight: 26,
    animateRows: false,
    suppressDragLeaveHidesColumns: true,
    suppressColumnVirtualisation: false,
    rowSelection: (props.rowSelection ? { mode: "singleRow", checkboxes: false, enableClickSelection: true } : undefined) as RowSelectionOptions | undefined,
    rowClassRules: props.rowClassRules,
    getRowId: (params) => resolveRowId(params.data),
    localeText: agGridLocaleText() as GridOptions["localeText"],
    onRowClicked: (event) => {
      if (!props.emitRowClick) return;
      if (event.data) emit("rowClick", event.data);
    },
    onSelectionChanged: (event) => {
      const rows = event.api.getSelectedRows();
      emit("selectionChanged", rows.length > 0 ? rows[0] : null);
    },
    onPaginationChanged: (event) => {
      const size = event.api.paginationGetPageSize();
      if (Number.isFinite(size) && size > 0 && size !== pageSize.value) {
        pageSize.value = size;
        savePreferredPageSize(props.tableKey, size);
        emit("pageSizeChanged", size);
      }
    },
    onCellContextMenu: openContextMenu,
  };
}

function applyColumns() {
  if (!gridApi) return;
  const defs = compactActive && props.compactFields ? minimalColumns(props.columnDefs, props.compactFields) : props.columnDefs;
  gridApi.setGridOption("columnDefs", defs);
}

onMounted(() => {
  if (!host.value) return;
  gridApi = createGrid(host.value, buildOptions());
  if (props.quickFilter) gridApi.setGridOption("quickFilterText", props.quickFilter);
  resizeObserver = new ResizeObserver((entries) => {
    const width = entries[0]?.contentRect.width ?? 0;
    const next = props.compactFields !== undefined && width > 0 && width < GRID_COMPACT_WIDTH;
    if (next !== compactActive) {
      compactActive = next;
      applyColumns();
    }
  });
  resizeObserver.observe(host.value);
  document.addEventListener("pointerdown", closeContextMenu);
  document.addEventListener("keydown", onContextMenuKeydown);
});

onBeforeUnmount(() => {
  resizeObserver?.disconnect();
  resizeObserver = null;
  document.removeEventListener("pointerdown", closeContextMenu);
  document.removeEventListener("keydown", onContextMenuKeydown);
  gridApi?.destroy();
  gridApi = null;
});

watch(
  () => props.rowData,
  (rows) => gridApi?.setGridOption("rowData", rows),
);
watch(
  () => props.quickFilter,
  (keyword) => gridApi?.setGridOption("quickFilterText", keyword ?? ""),
);
watch(
  () => props.columnDefs,
  () => applyColumns(),
);
watch(
  () => props.rowClassRules,
  (rules) => gridApi?.setGridOption("rowClassRules", rules),
);
watch(
  () => props.tableKey,
  () => {
    pageSize.value = loadPreferredPageSize(props.tableKey);
    gridApi?.setGridOption("paginationPageSize", pageSize.value);
  },
);

/** 跳到最新（对外契约，MessagesPanel「跳到最新」用）：分页表先切末页，再把
 *  最后一行滚入视口底部。之前由调用方直接改 viewport DOM 滚动，分页模式下
 *  viewport 不含未渲染页、且类名跨 ag-grid 版本不稳，统一收口到这里。 */
function goToLatest() {
  if (!gridApi) return;
  gridApi.paginationGoToLastPage();
  const last = gridApi.getDisplayedRowCount() - 1;
  if (last >= 0) gridApi.ensureIndexVisible(last, "bottom");
}
defineExpose({ goToLatest });
</script>

<template>
  <div ref="host" class="dbx-grid ag-theme-quartz" />
  <div
    v-if="contextMenu"
    class="context-menu dbx-grid-context-menu"
    :style="{ left: `${contextMenu.x}px`, top: `${contextMenu.y}px` }"
    role="menu"
    @pointerdown.stop
    @contextmenu.prevent.stop
  >
    <template v-for="item in contextItems" :key="item.id">
      <hr v-if="item.separatorBefore" />
      <button type="button" :class="{ danger: item.danger }" :disabled="item.resolvedDisabled" role="menuitem" @click="item.action(contextMenu!.row); closeContextMenu()">
        {{ item.label }}
      </button>
    </template>
  </div>
</template>

<style scoped>
/* P2-10：键盘导航可见焦点（ag-grid 默认 outline: none，Tab 进入网格后无法定位）。
   颜色走 --primary/--border 令牌，dark/light 两套随宿主主题成立；仅键盘聚焦
   （.ag-cell-focus）时描边，不干扰鼠标点选高亮。:deep 穿透 ag-grid 内部 DOM。 */
.dbx-grid :deep(.ag-cell.ag-cell-focus),
.dbx-grid :deep(.ag-cell:focus) {
  outline: 2px solid var(--primary);
  outline-offset: -2px;
}
.dbx-grid :deep(.ag-row.ag-row-focus) {
  outline: 1px solid color-mix(in srgb, var(--primary) 55%, var(--border));
  outline-offset: -1px;
}
</style>
