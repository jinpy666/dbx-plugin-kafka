// @vitest-environment happy-dom
// MonitorPanel 组件测试（IMPL_PLAN §11.1：lag 采样 + 阈值告警 + 方案 presets）：
// 选组前「开始采样」禁用 / 选组采样后 lag 表渲染 / 采样间隔 clamp 与停止 /
// 阈值告警每轮上穿只发一次（回落再上穿重复）/ 方案保存 / 方案加载回填 /
// 方案删除。
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import { defineComponent, h, type PropType } from "vue";
import MonitorPanel from "./MonitorPanel.vue";
import { setKafkaConnectionId, type GroupOffsetRow, type KafkaPreset } from "../lib/api";
import { t } from "../lib/i18n";

// -- DbxAgGrid 轻量 stub（镜像真实桥形状，见 GroupsPanel.spec 同款） -----------------
const DbxAgGridStub = defineComponent({
  name: "DbxAgGridStub",
  props: {
    rowData: { type: Array as PropType<unknown[]>, default: () => [] },
    tableKey: { type: String, default: "" },
    rowSelection: { type: [String, Boolean] as PropType<"single" | false>, default: "single" as const },
    emitRowClick: { type: Boolean, default: true },
  },
  emits: ["selection-changed", "row-click"],
  setup(props, { emit }) {
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

/** method → 响应 路由表；未命中抛错；`{ error }` 信封按真实桥形态以异常拒绝；
 *  响应可为函数（(params) => result，供序列采样响应）。 */
function installBridge(routes: Record<string, unknown>) {
  invokeMock.mockReset();
  invokeMock.mockImplementation(async (method: string, params?: unknown) => {
    if (method in routes) {
      const result = routes[method];
      const envelope = result as { error?: { message?: string } } | null;
      if (envelope && typeof envelope === "object" && envelope.error) {
        throw new Error(envelope.error.message ?? "request failed");
      }
      return typeof result === "function" ? result(params) : result;
    }
    throw new Error(`unhandled method: ${method}`);
  });
  (window as unknown as { dbxPlugin: unknown }).dbxPlugin = { invoke: invokeMock };
}

const groups = [{ group: "billing-consumer", state: "Stable", coordinator: 1 }];

// hasCommitted 是结果级字段（评审 M-2），行上不再携带。
const lagRows: GroupOffsetRow[] = [
  { topic: "order-events", partition: 0, endOffset: 400, committedOffset: 100, lag: 300 },
  { topic: "order-events", partition: 1, endOffset: 200, committedOffset: 0, lag: 200 },
];

function mountPanel() {
  return mount(MonitorPanel, {
    global: { stubs: { DbxAgGrid: DbxAgGridStub, teleport: true } },
  });
}

/** 选择消费者组（第一个 select 是 group）。 */
async function selectGroup(wrapper: ReturnType<typeof mountPanel>, group: string) {
  await wrapper.find("select").setValue(group);
}

beforeEach(() => {
  localStorage.clear();
  setKafkaConnectionId("conn-test");
});

afterEach(() => {
  vi.useRealTimers();
});

describe("MonitorPanel", () => {
  it("disables start sampling until a group is selected", async () => {
    installBridge({ "kafka/groups/list": { groups }, "kafka/presets/list": { presets: [] } });
    const wrapper = mountPanel();
    await flushPromises();
    const startButton = wrapper.find(".primary-button.compact");
    expect(startButton.attributes("disabled")).toBeDefined();
    expect(startButton.text()).toContain(t("monitor.start"));
    // 方案下拉空态
    expect(wrapper.text()).toContain(t("monitor.planEmpty"));
    await selectGroup(wrapper, "billing-consumer");
    expect(wrapper.find(".primary-button.compact").attributes("disabled")).toBeUndefined();
  });

  it("samples lag for the selected group and renders the lag table", async () => {
    installBridge({
      "kafka/groups/list": { groups },
      "kafka/presets/list": { presets: [] },
      "kafka/groups/offsets/list": { rows: lagRows, totalLag: 500, hasCommitted: true },
    });
    const wrapper = mountPanel();
    await flushPromises();
    await selectGroup(wrapper, "billing-consumer");
    await wrapper.find(".primary-button.compact").trigger("click");
    await flushPromises();
    expect(invokeMock.mock.calls.find(([method]) => method === "kafka/groups/offsets/list")?.[1]).toMatchObject({
      group: "billing-consumer",
    });
    // lag 表渲染 + 计数徽标 + 总 lag 徽标（500 > 0 → badge-warn）
    expect(wrapper.find('.grid-stub[data-key="monitor-lag"]').exists()).toBe(true);
    expect(wrapper.findAll(".grid-stub-row")).toHaveLength(2);
    expect(wrapper.text()).toContain(t("monitor.samples", { count: 1 }));
    expect(wrapper.text()).toContain(t("monitor.totalLag", { lag: 500 }));
    // 采样中：状态 badge-ok、按钮切为 stop
    expect(wrapper.find(".stream-meta .badge").classes()).toContain("badge-ok");
    expect(wrapper.find(".danger-button.compact").text()).toContain(t("monitor.stop"));
    // 清屏用的空串 error 事件（采样开始时清横幅）
    expect(wrapper.emitted("error")?.at(-1)).toEqual([""]);
  });

  it("passes the topic filter, clamps a small interval, and stops on demand", async () => {
    vi.useFakeTimers();
    installBridge({
      "kafka/groups/list": { groups },
      "kafka/presets/list": { presets: [] },
      "kafka/groups/offsets/list": { rows: [], totalLag: 0, hasCommitted: true },
    });
    const wrapper = mountPanel();
    await flushPromises();
    await selectGroup(wrapper, "billing-consumer");
    // 0 = topics，1 = interval，2 = threshold
    const numberInputs = wrapper.findAll('.kafka-form input[type="number"]');
    await wrapper.findAll('.kafka-form input[type="text"]')[0].setValue("order-events, payment-gateway");
    await numberInputs[0].setValue("3");
    await wrapper.find(".primary-button.compact").trigger("click");
    await flushPromises();
    const lagCalls = () => invokeMock.mock.calls.filter(([method]) => method === "kafka/groups/offsets/list");
    expect(lagCalls()[0]?.[1]).toMatchObject({ group: "billing-consumer", topics: ["order-events", "payment-gateway"] });
    // 3s 被 clamp 到 5s：5s 后第二轮采样
    await vi.advanceTimersByTimeAsync(5000);
    await flushPromises();
    expect(lagCalls()).toHaveLength(2);
    // 停止后不再采样
    await wrapper.find(".danger-button.compact").trigger("click");
    await flushPromises();
    await vi.advanceTimersByTimeAsync(20000);
    await flushPromises();
    expect(lagCalls()).toHaveLength(2);
  });

  it("emits the threshold alert once per breach and again only after recovery", async () => {
    vi.useFakeTimers();
    const lags = [
      { rows: lagRows, totalLag: 1500 }, // 上穿 → alert
      { rows: lagRows, totalLag: 1200 }, // 仍高于阈值 → 不重复
      { rows: lagRows, totalLag: 50 }, // 回落
      { rows: lagRows, totalLag: 2000 }, // 再次上穿 → 再 alert
    ];
    installBridge({
      "kafka/groups/list": { groups },
      "kafka/presets/list": { presets: [] },
      "kafka/groups/offsets/list": () => lags.shift() ?? { rows: [], totalLag: 0, hasCommitted: true },
    });
    const wrapper = mountPanel();
    await flushPromises();
    await selectGroup(wrapper, "billing-consumer");
    const numberInputs = wrapper.findAll('.kafka-form input[type="number"]');
    await numberInputs[0].setValue("5"); // 采样间隔 5s
    await numberInputs[1].setValue("1000");
    await wrapper.find(".primary-button.compact").trigger("click");
    await flushPromises();
    expect(wrapper.emitted("alert")).toHaveLength(1);
    expect(wrapper.emitted("alert")?.[0]).toEqual([t("monitor.thresholdBreached", { lag: 1500, threshold: 1000 })]);
    for (let round = 0; round < 3; round += 1) {
      await vi.advanceTimersByTimeAsync(5000);
      await flushPromises();
    }
    const alerts = wrapper.emitted("alert") ?? [];
    expect(alerts).toHaveLength(2);
    expect(alerts[1]).toEqual([t("monitor.thresholdBreached", { lag: 2000, threshold: 1000 })]);
  });

  it("saves the current form as a monitor plan", async () => {
    const savedPresets: KafkaPreset[] = [];
    installBridge({
      "kafka/groups/list": { groups },
      "kafka/presets/list": () => ({ presets: savedPresets }),
      "kafka/presets/save": (params?: unknown) => {
        const preset = (params as { preset: KafkaPreset }).preset;
        savedPresets.push(preset);
        return { presets: savedPresets };
      },
    });
    const wrapper = mountPanel();
    await flushPromises();
    await selectGroup(wrapper, "billing-consumer");
    await wrapper.findAll('.kafka-form input[type="text"]')[0].setValue("order-events"); // topics 输入（form1）
    await wrapper.findAll('.kafka-form input[type="number"]')[1].setValue("1000");
    // 方案名 + 保存按钮（title=monitor.planSave；form2 的 text input 是方案名）
    await wrapper.find('.kafka-form input[placeholder="' + t("monitor.planName") + '"]').setValue("nightly");
    await wrapper.find(`.qb-add[title="${t("monitor.planSave")}"]`).trigger("click");
    await flushPromises();
    const saveCall = invokeMock.mock.calls.find(([method]) => method === "kafka/presets/save");
    expect(saveCall?.[1]).toMatchObject({
      preset: {
        name: "nightly",
        params: {
          type: "monitor",
          topic: "",
          offsetStrategy: "latest",
          monitor: { group: "billing-consumer", topics: ["order-events"], intervalSec: 10, threshold: 1000 },
        },
      },
    });
    expect(wrapper.emitted("notify")?.at(-1)).toEqual([t("monitor.planSaved")]);
    // 保存后下拉出现新方案（loadPlans 重查）
    const options = wrapper.findAll("select")[1].findAll("option");
    expect(options.map((option) => option.text())).toContain("nightly");
  });

  it("applies a saved plan back into the form", async () => {
    const plan: KafkaPreset = {
      id: "monitor-1",
      name: "nightly",
      params: { topic: "", offsetStrategy: "latest", type: "monitor", monitor: { group: "billing-consumer", topics: ["order-events"], intervalSec: 30, threshold: 500 } },
    };
    installBridge({ "kafka/groups/list": { groups }, "kafka/presets/list": { presets: [plan] } });
    const wrapper = mountPanel();
    await flushPromises();
    await wrapper.findAll("select")[1].setValue("monitor-1");
    await flushPromises();
    // 表单回填
    expect((wrapper.find("select").element as HTMLSelectElement).value).toBe("billing-consumer");
    expect((wrapper.find('.kafka-form input[type="text"]').element as HTMLInputElement).value).toBe("order-events");
    const numberInputs = wrapper.findAll('.kafka-form input[type="number"]');
    expect((numberInputs[0].element as HTMLInputElement).value).toBe("30");
    expect((numberInputs[1].element as HTMLInputElement).value).toBe("500");
    expect(wrapper.emitted("notify")?.at(-1)).toEqual([t("monitor.planApplied")]);
  });

  it("removes a saved plan", async () => {
    const plan: KafkaPreset = {
      id: "monitor-1",
      name: "nightly",
      params: { topic: "", offsetStrategy: "latest", type: "monitor", monitor: { group: "billing-consumer", topics: [], intervalSec: 10, threshold: 100 } },
    };
    let presets = [plan];
    installBridge({
      "kafka/groups/list": { groups },
      "kafka/presets/list": () => ({ presets }),
      "kafka/presets/remove": () => {
        presets = [];
        return { presets };
      },
    });
    const wrapper = mountPanel();
    await flushPromises();
    await wrapper.find(`.qb-add[title="${t("monitor.planRemove")}: nightly"]`).trigger("click");
    await flushPromises();
    expect(invokeMock.mock.calls.find(([method]) => method === "kafka/presets/remove")?.[1]).toMatchObject({ id: "monitor-1" });
    expect(wrapper.emitted("notify")?.at(-1)).toEqual([t("monitor.planRemoved")]);
    // 方案列表清空 → planEmpty 空态回归
    expect(wrapper.text()).toContain(t("monitor.planEmpty"));
  });
});
