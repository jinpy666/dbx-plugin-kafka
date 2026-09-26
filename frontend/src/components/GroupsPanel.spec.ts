// @vitest-environment happy-dom
// GroupsPanel 组件测试：组列表（DbxAgGrid stub 行选择）/ 工具栏刷新 + reset/delete
// 门禁禁用态 / reset 弹窗（topic 输入、目标下拉、timestamp/partitionOffset 条件
// 字段与校验、提交成功/行级失败）/ delete 弹窗（二次执行 + notify）/ 空态。
import { beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import { defineComponent, h, type PropType } from "vue";
import GroupsPanel from "./GroupsPanel.vue";
import { setKafkaConnectionId, type KafkaGroup } from "../lib/api";
import { t } from "../lib/i18n";

// -- DbxAgGrid 轻量 stub（镜像真实桥形状：rowSelection 决定 selection-changed、
//    emitRowClick 决定 row-click；goToLatest 供调用方 ref 使用） -----------------
let goToLatestCalls = 0;
const DbxAgGridStub = defineComponent({
  name: "DbxAgGridStub",
  props: {
    rowData: { type: Array as PropType<unknown[]>, default: () => [] },
    tableKey: { type: String, default: "" },
    rowSelection: { type: [String, Boolean] as PropType<"single" | false>, default: "single" as const },
    emitRowClick: { type: Boolean, default: true },
  },
  emits: ["selection-changed", "row-click"],
  setup(props, { emit, expose }) {
    expose({
      goToLatest: () => {
        goToLatestCalls += 1;
      },
    });
    return () =>
      h(
        "div",
        { class: "grid-stub", "data-key": props.tableKey },
        (props.rowData ?? []).map((row, index) =>
          h(
            "button",
            {
              type: "button",
              class: "grid-stub-row",
              onClick: () => {
                if (props.rowSelection) emit("selection-changed", row);
                if (props.emitRowClick) emit("row-click", row);
              },
            },
            `${props.tableKey}-row-${index}`,
          ),
        ),
      );
  },
});

const invokeMock = vi.fn();

/** method → 响应 路由表；未命中抛错（对应未注册方法）；`{ error }` 信封按真实桥
 *  形态以异常拒绝（JSON-RPC error → invoke rejection → api 层转译为 Error）。 */
function installBridge(routes: Record<string, unknown>) {
  invokeMock.mockReset();
  invokeMock.mockImplementation(async (method: string) => {
    if (method in routes) {
      const result = routes[method];
      const envelope = result as { error?: { message?: string } } | null;
      if (envelope && typeof envelope === "object" && envelope.error) {
        throw new Error(envelope.error.message ?? "request failed");
      }
      return result;
    }
    throw new Error(`unhandled method: ${method}`);
  });
  (window as unknown as { dbxPlugin: unknown }).dbxPlugin = { invoke: invokeMock };
}

const groups: KafkaGroup[] = [
  { group: "orders-consumer", state: "Stable", coordinator: 1 },
  { group: "legacy-poller", state: "Empty", coordinator: 2 },
];

function mountPanel(props: Record<string, unknown> = {}) {
  return mount(GroupsPanel, {
    props: { canWrite: true, canDelete: true, ...props },
    // teleport stub 让弹窗留在 wrapper 内可查；DbxAgGrid stub 避开真实 ag-grid。
    global: { stubs: { DbxAgGrid: DbxAgGridStub, teleport: true } },
  });
}

async function selectFirstGroup(wrapper: ReturnType<typeof mountPanel>) {
  await wrapper.find(".grid-stub .grid-stub-row").trigger("click");
  await flushPromises();
}

beforeEach(() => {
  localStorage.clear();
  setKafkaConnectionId("conn-test");
  goToLatestCalls = 0;
});

describe("GroupsPanel", () => {
  it("loads groups on mount and shows title count", async () => {
    installBridge({ "kafka/groups/list": { groups } });
    const wrapper = mountPanel();
    await flushPromises();
    expect(wrapper.text()).toContain(t("groups.title"));
    expect(wrapper.text()).toContain("2");
    expect(wrapper.findAll(".grid-stub")).toHaveLength(1);
  });

  it("shows the empty state when no groups are returned", async () => {
    installBridge({ "kafka/groups/list": { groups: [] } });
    const wrapper = mountPanel();
    await flushPromises();
    expect(wrapper.find(".empty").text()).toBe(t("groups.empty"));
  });

  it("emits error when the group list call rejects", async () => {
    installBridge({});
    const wrapper = mountPanel();
    await flushPromises();
    expect(wrapper.emitted("error")?.at(-1)).toEqual(["unhandled method: kafka/groups/list"]);
  });

  it("refresh button reloads the list", async () => {
    installBridge({ "kafka/groups/list": { groups: [] } });
    const wrapper = mountPanel();
    await flushPromises();
    await wrapper.find(".icon-button").trigger("click");
    await flushPromises();
    expect(invokeMock).toHaveBeenCalledTimes(2);
  });

  it("reset and delete buttons are gated by canWrite/canDelete and selection", async () => {
    installBridge({ "kafka/groups/list": { groups } });
    const wrapper = mountPanel();
    await flushPromises();
    const buttons = wrapper.findAll(".result-meta .qb-add");
    const [resetButton, deleteButton] = buttons;
    // 未选中组：双按钮禁用
    expect(resetButton.attributes("disabled")).toBeDefined();
    expect(deleteButton.attributes("disabled")).toBeDefined();
    // 选中后可用；read_only 下均禁用
    await selectFirstGroup(wrapper);
    expect(resetButton.attributes("disabled")).toBeUndefined();
    expect(deleteButton.attributes("disabled")).toBeUndefined();
    const roWrapper = mountPanel({ canWrite: false, canDelete: false });
    await flushPromises();
    const roButtons = roWrapper.findAll(".result-meta .qb-add");
    expect(roButtons[0].attributes("disabled")).toBeDefined();
    expect(roButtons[0].attributes("title")).toBe(t("readOnly"));
    expect(roButtons[1].attributes("disabled")).toBeDefined();
    roWrapper.unmount();
  });

  it("selecting a group loads offsets and members, rendering lag badge", async () => {
    installBridge({
      "kafka/groups/list": { groups },
      "kafka/groups/offsets/list": { rows: [{ topic: "orders", partition: 0, endOffset: 10, committedOffset: 8, lag: 2 }], totalLag: 2 },
      "kafka/groups/describe": { members: [{ memberId: "m-1", clientId: "c-1", assignments: { orders: [0] } }] },
    });
    const wrapper = mountPanel();
    await flushPromises();
    await selectFirstGroup(wrapper);
    expect(wrapper.text()).toContain(t("groups.offsetsTitle", { group: "orders-consumer" }));
    // totalLag=2 → badge-warn
    const lagBadge = wrapper.find(".subpanel-title .badge");
    expect(lagBadge.classes()).toContain("badge-warn");
    expect(wrapper.text()).toContain(t("groups.totalLag", { lag: 2 }));
    // members 表渲染
    expect(wrapper.find('.grid-stub[data-key="group-members"]').exists()).toBe(true);
  });

  it("shows offsets empty and no-members empty states", async () => {
    installBridge({
      "kafka/groups/list": { groups },
      "kafka/groups/offsets/list": { rows: [] },
      "kafka/groups/describe": { members: [] },
    });
    const wrapper = mountPanel();
    await flushPromises();
    await selectFirstGroup(wrapper);
    const empties = wrapper.findAll(".empty");
    expect(empties.map((node) => node.text())).toContain(t("groups.offsetsEmpty"));
    expect(empties.map((node) => node.text())).toContain(t("groups.noMembers"));
  });

  it("marks hasCommitted=false rows with warn badges", async () => {
    installBridge({
      "kafka/groups/list": { groups },
      "kafka/groups/offsets/list": { rows: [{ topic: "orders", partition: 0, endOffset: 10, hasCommitted: false }] },
      "kafka/groups/describe": { members: [] },
    });
    const wrapper = mountPanel();
    await flushPromises();
    await selectFirstGroup(wrapper);
    expect(wrapper.text()).toContain(t("groups.hasCommittedFalse"));
    expect(wrapper.text()).toContain(t("groups.noCommitted"));
  });

  it("reset dialog: timestamp strategy validation and successful submit", async () => {
    installBridge({
      "kafka/groups/list": { groups },
      "kafka/groups/offsets/list": { rows: [] },
      "kafka/groups/describe": { members: [] },
      "kafka/groups/offsets/reset": { rows: [{ topic: "orders", partition: 0, ok: true }] },
    });
    const wrapper = mountPanel();
    await flushPromises();
    await selectFirstGroup(wrapper);
    await wrapper.findAll(".result-meta .qb-add")[0].trigger("click");
    // teleport stub 重渲染会替换弹窗元素：断言/交互前一律重查，不用过期 wrapper。
    const modal = () => wrapper.find(".modal-backdrop .modal");
    expect(modal().exists()).toBe(true);
    expect(modal().text()).toContain("orders-consumer");
    // topics 输入
    await modal().find('input[type="text"]').setValue("orders");
    // 切换到 timestamp 策略 → 出现时间戳输入
    await modal().find("select").setValue("timestamp");
    const tsInput = modal().find('input[type="number"]');
    expect(tsInput.exists()).toBe(true);
    // 非法时间戳 → 校验错误、不发起请求
    await tsInput.setValue("");
    await modal().find(".primary-button").trigger("click");
    await flushPromises();
    expect(wrapper.emitted("error")?.at(-1)).toEqual([t("messages.timestampRequired")]);
    expect(modal().exists()).toBe(true);
    // 合法时间戳 → 提交成功 + notify + 弹窗关闭
    await modal().find('input[type="number"]').setValue("1700000000000");
    await modal().find(".primary-button").trigger("click");
    await flushPromises();
    expect(wrapper.emitted("notify")?.at(-1)).toEqual([t("groups.resetDone")]);
    expect(wrapper.find(".modal-backdrop").exists()).toBe(false);
    const resetCall = invokeMock.mock.calls.find(([method]) => method === "kafka/groups/offsets/reset");
    expect(resetCall?.[1]).toMatchObject({ group: "orders-consumer", topics: ["orders"], resetTo: "timestamp", timestampMs: 1700000000000 });
  });

  it("reset dialog: partitionOffset strategy rejects invalid targets", async () => {
    installBridge({
      "kafka/groups/list": { groups },
      "kafka/groups/offsets/list": { rows: [] },
      "kafka/groups/describe": { members: [] },
    });
    const wrapper = mountPanel();
    await flushPromises();
    await selectFirstGroup(wrapper);
    await wrapper.findAll(".result-meta .qb-add")[0].trigger("click");
    // teleport stub 重渲染会替换弹窗元素：断言/交互前一律重查，不用过期 wrapper。
    const modal = () => wrapper.find(".modal-backdrop .modal");
    await modal().find('input[type="text"]').setValue("orders");
    await modal().find("select").setValue("partitionOffset");
    const offsetsInput = modal().find('input.mono[type="text"]');
    expect(offsetsInput.exists()).toBe(true);
    await offsetsInput.setValue("0=abc");
    await modal().find(".primary-button").trigger("click");
    await flushPromises();
    const errors = wrapper.emitted("error") ?? [];
    expect(errors.at(-1)?.[0]).toContain(t("groups.resetOffsetsInvalid"));
  });

  it("reset dialog cancel and header close buttons dismiss without side effects", async () => {
    installBridge({
      "kafka/groups/list": { groups },
      "kafka/groups/offsets/list": { rows: [] },
      "kafka/groups/describe": { members: [] },
    });
    const wrapper = mountPanel();
    await flushPromises();
    await selectFirstGroup(wrapper);
    await wrapper.findAll(".result-meta .qb-add")[0].trigger("click");
    // 头部 ✕ 关闭
    await wrapper.find(".modal-backdrop .modal .icon-button").trigger("click");
    expect(wrapper.find(".modal-backdrop").exists()).toBe(false);
    // 再开 → footer 取消关闭
    await wrapper.findAll(".result-meta .qb-add")[0].trigger("click");
    const footerButtons = wrapper.findAll(".modal-backdrop .modal footer button");
    await footerButtons[0].trigger("click");
    expect(wrapper.find(".modal-backdrop").exists()).toBe(false);
    expect(invokeMock.mock.calls.filter(([method]) => method === "kafka/groups/offsets/reset")).toHaveLength(0);
  });

  it("delete dialog requires typing the group name before submit", async () => {
    installBridge({
      "kafka/groups/list": { groups },
      "kafka/groups/offsets/list": { rows: [] },
      "kafka/groups/describe": { members: [] },
      "kafka/groups/delete": { success: true },
    });
    const wrapper = mountPanel();
    await flushPromises();
    await selectFirstGroup(wrapper);
    await wrapper.findAll(".result-meta .qb-add")[1].trigger("click");
    // 确认输入为空 / 不同名：按钮禁用，点击不触发删除。
    expect(wrapper.find(".modal-backdrop .modal .danger-button").attributes("disabled")).toBeDefined();
    await wrapper.find(".modal-backdrop .modal .danger-button").trigger("click");
    await flushPromises();
    expect(invokeMock.mock.calls.filter(([method]) => method === "kafka/groups/delete")).toHaveLength(0);
    // 输入同名后放行（confirmGroup 与后端 confirmTopic 同级门禁）。
    // teleport stub 重渲染会替换弹窗元素：断言/交互前一律重查，不用过期 wrapper。
    await wrapper.find(".modal-backdrop .modal input[type='text']").setValue("orders-consumer");
    await wrapper.find(".modal-backdrop .modal .danger-button").trigger("click");
    await flushPromises();
    expect(invokeMock.mock.calls.filter(([method]) => method === "kafka/groups/delete")).toHaveLength(1);
  });

  it("delete dialog submits deletion and reloads the list", async () => {
    installBridge({
      "kafka/groups/list": { groups },
      "kafka/groups/offsets/list": { rows: [] },
      "kafka/groups/describe": { members: [] },
      "kafka/groups/delete": { success: true },
    });
    const wrapper = mountPanel();
    await flushPromises();
    await selectFirstGroup(wrapper);
    await wrapper.findAll(".result-meta .qb-add")[1].trigger("click");
    const modal = wrapper.find(".modal-backdrop .modal");
    expect(modal.text()).toContain("orders-consumer");
    expect(modal.text()).toContain(t("groups.deleteMessage"));
    expect(modal.text()).toContain(t("groups.deleteConfirmLabel", { group: "orders-consumer" }));
    await wrapper.find(".modal-backdrop .modal input[type='text']").setValue("orders-consumer");
    await wrapper.find(".modal-backdrop .modal .danger-button").trigger("click");
    await flushPromises();
    expect(wrapper.emitted("notify")?.at(-1)).toEqual([t("groups.deleted")]);
    expect(wrapper.find(".modal-backdrop").exists()).toBe(false);
    // confirmGroup 与组同名随请求上送（后端 confirmTopic 同级门禁；
    // connectionId 由 api 层注入，objectContaining 只断言本面板关心的字段）。
    expect(invokeMock.mock.calls.find(([method]) => method === "kafka/groups/delete")?.[1]).toEqual(
      expect.objectContaining({ group: "orders-consumer", confirmGroup: "orders-consumer" }),
    );
    // 删除后重载列表
    expect(invokeMock.mock.calls.filter(([method]) => method === "kafka/groups/list").length).toBeGreaterThanOrEqual(2);
  });

  it("delete dialog emits error when backend rejects", async () => {
    installBridge({
      "kafka/groups/list": { groups },
      "kafka/groups/offsets/list": { rows: [] },
      "kafka/groups/describe": { members: [] },
      "kafka/groups/delete": { error: { code: -32000, message: "delete blocked" } },
    });
    const wrapper = mountPanel();
    await flushPromises();
    await selectFirstGroup(wrapper);
    await wrapper.findAll(".result-meta .qb-add")[1].trigger("click");
    await wrapper.find(".modal-backdrop .modal input[type='text']").setValue("orders-consumer");
    await wrapper.find(".modal-backdrop .modal .danger-button").trigger("click");
    await flushPromises();
    expect(wrapper.emitted("error")?.at(-1)).toEqual(["delete blocked"]);
    expect(wrapper.find(".modal-backdrop").exists()).toBe(true);
  });

  it("surfaces row-level failures from offsets reset", async () => {
    installBridge({
      "kafka/groups/list": { groups },
      "kafka/groups/offsets/list": { rows: [] },
      "kafka/groups/describe": { members: [] },
      "kafka/groups/offsets/reset": { rows: [{ topic: "orders", partition: 0, ok: false, error: "broker down" }] },
    });
    const wrapper = mountPanel();
    await flushPromises();
    await selectFirstGroup(wrapper);
    await wrapper.findAll(".result-meta .qb-add")[0].trigger("click");
    // teleport stub 重渲染会替换弹窗元素：断言/交互前一律重查，不用过期 wrapper。
    const modal = () => wrapper.find(".modal-backdrop .modal");
    await modal().find('input[type="text"]').setValue("orders");
    await modal().find(".primary-button").trigger("click");
    await flushPromises();
    expect(wrapper.emitted("error")?.at(-1)).toEqual(["orders-0: broker down"]);
    // 失败后弹窗仍会关闭并刷新详情（组件现行为）
    expect(wrapper.find(".modal-backdrop").exists()).toBe(false);
  });
});
