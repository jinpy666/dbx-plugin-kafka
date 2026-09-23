// @vitest-environment happy-dom
// DbxAgGrid 组件测试（连线行为锁定）：真实 ag-grid 在 happy-dom 下无法可靠
// 布局，故 mock ag-grid-community 的 createGrid 捕获 GridOptions + 假 GridApi：
// - quickFilter prop 初始与变更均下发 quickFilterText
// - 分页页大小变更按 tableKey 持久化 pluginStore（宿主 storage 适配）+ emit
//   pageSizeChanged；未变化/非法值不写（现有行为锁定）；tableKey 切换重载持久化页大小
// - 窄容器（< GRID_COMPACT_WIDTH）降级 minimal 列集、宽容器恢复
// - rowClick 仅在 emitRowClick 时转发；selectionChanged 有选中发行、无选发 null
// - goToLatest：切末页 + 末行滚入视口
// （页大小存取/minimalColumns 纯函数已在 kafkaColumns.spec，不重复。）
import { beforeEach, describe, expect, it, vi } from "vitest";
import { mount, type VueWrapper } from "@vue/test-utils";
import DbxAgGrid from "./DbxAgGrid.vue";
import type { GridContextMenuItem } from "./DbxAgGrid.vue";
import { GRID_PAGE_SIZES_KEY, pluginStore } from "../lib/pluginStore";
import { messageCellCopyText, toMessageRows, GRID_COMPACT_WIDTH } from "../lib/kafkaColumns";
import type { ColDef, GridOptions } from "ag-grid-community";

// -- ag-grid-community mock（捕获 options 与假 GridApi） ------------------------------
const gridMock = vi.hoisted(() => ({
  created: [] as Array<{ el: unknown; options: GridOptions }>,
  apis: [] as Array<{ setGridOption: ReturnType<typeof vi.fn>; paginationGetPageSize: ReturnType<typeof vi.fn> }>,
  pendingSize: 50,
}));

vi.mock("ag-grid-community", () => ({
  AllCommunityModule: {},
  ModuleRegistry: { registerModules: vi.fn() },
  createGrid: vi.fn((el: unknown, options: GridOptions) => {
    const api = {
      setGridOption: vi.fn(),
      paginationGetPageSize: vi.fn(() => gridMock.pendingSize),
      paginationGoToLastPage: vi.fn(),
      ensureIndexVisible: vi.fn(),
      getDisplayedRowCount: vi.fn(() => 5),
      destroy: vi.fn(),
    };
    gridMock.created.push({ el, options });
    gridMock.apis.push(api);
    return api;
  }),
}));

// -- ResizeObserver stub（手动触发宽度回调） -------------------------------------------
type ResizeEntry = { contentRect: { width: number } };
const resizeCallbacks: Array<(entries: ResizeEntry[]) => void> = [];
class ResizeObserverStub {
  constructor(callback: (entries: ResizeEntry[]) => void) {
    resizeCallbacks.push(callback);
  }
  observe(): void {}
  disconnect(): void {}
  unobserve(): void {}
}
vi.stubGlobal("ResizeObserver", ResizeObserverStub);

const columnDefs: ColDef[] = [
  { field: "partition", headerName: "P" },
  { field: "valueText", headerName: "V" },
  { field: "timestamp", headerName: "T" },
];

const row = { partition: 0, offset: 1 };

function mountGrid(props: Record<string, unknown> = {}) {
  return mount(DbxAgGrid, {
    props: { rowData: [row], columnDefs, tableKey: "spec-table", ...props },
  });
}

function lastApi(wrapper: VueWrapper<InstanceType<typeof DbxAgGrid>>) {
  void wrapper;
  return gridMock.apis[gridMock.apis.length - 1];
}

beforeEach(() => {
  // 持久化后端是 pluginStore（宿主 storage 适配），清理须走同一实例
  //（happy-dom 下 store 在模块导入时已水合，之后改 localStorage 读不到）。
  pluginStore.removeItem(GRID_PAGE_SIZES_KEY);
  gridMock.created.length = 0;
  gridMock.apis.length = 0;
  resizeCallbacks.length = 0;
  gridMock.pendingSize = 50;
});

describe("DbxAgGrid", () => {
  it("passes quickFilter into the grid and forwards prop updates", async () => {
    const wrapper = mountGrid({ quickFilter: "alpha" });
    expect(lastApi(wrapper).setGridOption).toHaveBeenCalledWith("quickFilterText", "alpha");
    lastApi(wrapper).setGridOption.mockClear();
    await wrapper.setProps({ quickFilter: "beta" });
    expect(lastApi(wrapper).setGridOption).toHaveBeenCalledWith("quickFilterText", "beta");
    // 清空也下发（watch 语义：keyword ?? ""）
    await wrapper.setProps({ quickFilter: "" });
    expect(lastApi(wrapper).setGridOption).toHaveBeenCalledWith("quickFilterText", "");
  });

  it("persists a pagination page size change per table key and emits it", () => {
    const wrapper = mountGrid();
    expect(gridMock.created[0].options.paginationPageSize).toBe(50);
    gridMock.pendingSize = 123;
    gridMock.created[0].options.onPaginationChanged?.({ api: lastApi(wrapper) } as never);
    // 页大小收敛为单键 JSON map（tableKey → size）
    expect(JSON.parse(pluginStore.getItem(GRID_PAGE_SIZES_KEY)!)).toEqual({ "spec-table": 123 });
    expect(wrapper.emitted("pageSizeChanged")).toEqual([[123]]);
  });

  it("skips persistence when the reported size is unchanged or invalid", () => {
    const wrapper = mountGrid();
    // 未变化（50 == 当前 pageSize）
    gridMock.created[0].options.onPaginationChanged?.({ api: lastApi(wrapper) } as never);
    // 非法值（NaN/0）
    gridMock.pendingSize = Number.NaN;
    gridMock.created[0].options.onPaginationChanged?.({ api: lastApi(wrapper) } as never);
    gridMock.pendingSize = 0;
    gridMock.created[0].options.onPaginationChanged?.({ api: lastApi(wrapper) } as never);
    expect(pluginStore.getItem(GRID_PAGE_SIZES_KEY)).toBeNull();
    expect(wrapper.emitted("pageSizeChanged")).toBeUndefined();
  });

  it("reloads the persisted page size when the table key changes", async () => {
    pluginStore.setItem(GRID_PAGE_SIZES_KEY, JSON.stringify({ "other-table": 25 }));
    const wrapper = mountGrid();
    lastApi(wrapper).setGridOption.mockClear();
    await wrapper.setProps({ tableKey: "other-table" });
    expect(lastApi(wrapper).setGridOption).toHaveBeenCalledWith("paginationPageSize", 25);
  });

  it("degrades to the minimal column set on a narrow container and restores on wide", () => {
    const wrapper = mountGrid({ compactFields: ["partition", "valueText"] });
    const api = lastApi(wrapper);
    // 窄容器（< GRID_COMPACT_WIDTH）→ minimal 列集
    resizeCallbacks.forEach((callback) => callback([{ contentRect: { width: GRID_COMPACT_WIDTH - 1 } }]));
    let call = api.setGridOption.mock.calls.find(([key]) => key === "columnDefs");
    expect((call?.[1] as ColDef[]).map((def) => def.field)).toEqual(["partition", "valueText"]);
    // 宽容器恢复全列
    api.setGridOption.mockClear();
    resizeCallbacks.forEach((callback) => callback([{ contentRect: { width: GRID_COMPACT_WIDTH } }]));
    call = api.setGridOption.mock.calls.find(([key]) => key === "columnDefs");
    expect((call?.[1] as ColDef[]).map((def) => def.field)).toEqual(["partition", "valueText", "timestamp"]);
    // 未配置 compactFields：宽度变化不触发改列
    const plainWrapper = mountGrid();
    lastApi(plainWrapper).setGridOption.mockClear();
    resizeCallbacks.forEach((callback) => callback([{ contentRect: { width: 10 } }]));
    expect(lastApi(plainWrapper).setGridOption).not.toHaveBeenCalled();
  });

  it("forwards row clicks only when emitRowClick is on, and selection changes", async () => {
    const wrapper = mountGrid();
    gridMock.created[0].options.onRowClicked?.({ data: row } as never);
    expect(wrapper.emitted("rowClick")).toEqual([[row]]);
    gridMock.created[0].options.onSelectionChanged?.({ api: { getSelectedRows: () => [row] } } as never);
    expect(wrapper.emitted("selectionChanged")).toEqual([[row]]);
    gridMock.created[0].options.onSelectionChanged?.({ api: { getSelectedRows: () => [] } } as never);
    expect(wrapper.emitted("selectionChanged")?.at(-1)).toEqual([null]);
    wrapper.unmount();

    const muted = mountGrid({ emitRowClick: false });
    gridMock.created[1].options.onRowClicked?.({ data: row } as never);
    expect(muted.emitted("rowClick")).toBeUndefined();
  });

  it("jumps to the last page and scrolls the final row into view via goToLatest", () => {
    const wrapper = mountGrid();
    (wrapper.vm as unknown as { goToLatest: () => void }).goToLatest();
    const api = lastApi(wrapper) as unknown as {
      paginationGoToLastPage: ReturnType<typeof vi.fn>;
      ensureIndexVisible: ReturnType<typeof vi.fn>;
    };
    expect(api.paginationGoToLastPage).toHaveBeenCalledTimes(1);
    expect(api.ensureIndexVisible).toHaveBeenCalledWith(4, "bottom");
  });

  it("opens the copy and custom management actions from a row context menu", async () => {
    const manage = vi.fn();
    const contextMenuItems: GridContextMenuItem[] = [{ id: "manage", label: "Manage row", action: manage }];
    const wrapper = mountGrid({ contextMenuItems });
    gridMock.created[0].options.onCellContextMenu?.({
      node: { data: row },
      value: "alpha",
      event: new MouseEvent("contextmenu", { clientX: 20, clientY: 20 }),
    } as never);
    await wrapper.vm.$nextTick();
    expect(wrapper.find(".context-menu").text()).toContain("复制值");
    expect(wrapper.find(".context-menu").text()).toContain("复制行");
    const manageButton = wrapper.findAll(".context-menu button").find((button) => button.text() === "Manage row");
    expect(manageButton).toBeDefined();
    await manageButton!.trigger("click");
    expect(manage).toHaveBeenCalledWith(row);
  });
});

it("copies full message cells and rows instead of truncated previews", async () => {
  const full = "VALUE".repeat(500) + "TAIL";
  const key = "KEY".repeat(100);
  const headers = { long: "HEADER".repeat(100) };
  const [messageRow] = toMessageRows([{ topic: "test", partition: 0, offset: 1, timestamp: 0, key, valueText: "backend preview…", valueBase64: btoa(full), headers }]);
  const writeText = vi.fn().mockResolvedValue(undefined);
  vi.stubGlobal("navigator", { clipboard: { writeText } });
  const wrapper = mountGrid({ rowData: [messageRow], columnDefs: [{ field: "keyText" }, { field: "valueText" }, { field: "headersText" }], cellCopyText: messageCellCopyText });
  try {
    for (const [field, expected] of [["valueText", full], ["keyText", key], ["headersText", JSON.stringify(headers)]]) {
      gridMock.created[0].options.onCellContextMenu?.({ node: { data: messageRow }, colDef: { field }, value: messageRow[field as keyof typeof messageRow], event: new MouseEvent("contextmenu") } as never);
      await wrapper.vm.$nextTick();
      await wrapper.findAll(".context-menu button")[0].trigger("click");
      expect(writeText).toHaveBeenLastCalledWith(expected);
    }
    gridMock.created[0].options.onCellContextMenu?.({ node: { data: messageRow }, colDef: { field: "valueText" }, value: messageRow.valueText, event: new MouseEvent("contextmenu") } as never);
    await wrapper.vm.$nextTick();
    await wrapper.findAll(".context-menu button")[1].trigger("click");
    expect(writeText).toHaveBeenLastCalledWith([key, full, JSON.stringify(headers)].join("\t"));
  } finally {
    wrapper.unmount();
    vi.unstubAllGlobals();
  }
});
