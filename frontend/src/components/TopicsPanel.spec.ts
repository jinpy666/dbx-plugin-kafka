// @vitest-environment happy-dom
// TopicsPanel 组件测试（UI 扫描第 4 轮防回归 + 覆盖完善轮 §10.3 补测）：
// - P2-20 扩分区填更小/相等值时使用专用文案（不再错位复用 err.partition），
//   且仍不发出 topics/partitions/update 请求。
// - 第 2 轮补测：describe/create/delete/config/offsets 行为流、只读门禁、
//   topics 刷新时选中保持/清空（全部测行为：请求参数 + 事件 + 可见面）。
import { beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import { defineComponent, h, type PropType } from "vue";
import TopicsPanel from "./TopicsPanel.vue";
import { setKafkaConnectionId, type KafkaTopic } from "../lib/api";
import { setTopicFavorites } from "../lib/topicFavorites";
import { t } from "../lib/i18n";

// -- DbxAgGrid 轻量 stub（镜像真实桥形状，见 GroupsPanel.spec 同款） -----------------
// Lane4：额外透出 columnDefs 的列数/首个 colId（收藏星标列断言用）与行 name 文本
// （收藏置顶排序断言用）。
const DbxAgGridStub = defineComponent({
  name: "DbxAgGridStub",
  props: {
    rowData: { type: Array as PropType<unknown[]>, default: () => [] },
    columnDefs: { type: Array as PropType<Array<{ colId?: string }>>, default: () => [] },
    tableKey: { type: String, default: "" },
    rowSelection: { type: [String, Boolean] as PropType<"single" | false>, default: "single" as const },
    emitRowClick: { type: Boolean, default: true },
  },
  emits: ["selection-changed", "row-click"],
  setup(props, { emit }) {
    return () =>
      h(
        "div",
        {
          class: "grid-stub",
          "data-key": props.tableKey,
          "data-cols": String(props.columnDefs.length),
          "data-first-col": String(props.columnDefs[0]?.colId ?? ""),
        },
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
            (row as { name?: string }).name ?? "",
          ),
        ),
      );
  },
});

const invokeMock = vi.fn();

function installBridge(routes: Record<string, unknown>) {
  invokeMock.mockReset();
  invokeMock.mockImplementation(async (method: string) => {
    if (method in routes) return routes[method];
    throw new Error(`unhandled method: ${method}`);
  });
  (window as unknown as { dbxPlugin: unknown }).dbxPlugin = { invoke: invokeMock };
}

const topics: KafkaTopic[] = [{ name: "order-events", partitionCount: 2, replicationFactor: 1 }];

const partitions = [
  { partition: 0, leader: 1, replicas: [1], isr: [1], offlineReplicas: [] },
  { partition: 1, leader: 1, replicas: [1], isr: [1], offlineReplicas: [] },
];

const configEntries = [
  { name: "cleanup.policy", value: "delete", isDefault: true },
  { name: "password", value: "secret", sensitive: true },
];

function mountPanel(props: Record<string, unknown> = {}) {
  return mount(TopicsPanel, {
    props: { topics, loading: false, canWrite: true, canDelete: true, ...props },
    global: { stubs: { DbxAgGrid: DbxAgGridStub, teleport: true } },
  });
}

/** 选中 topic 网格首行（selection-changed → selected）。 */
async function selectFirstTopic(wrapper: ReturnType<typeof mountPanel>) {
  await wrapper.find('.grid-stub[data-key="topics"] .grid-stub-row').trigger("click");
  await flushPromises();
}

/** 打开扩分区弹窗（工具栏第 5 个按钮：TrendingUp 扩分区）。 */
async function openExpandDialog(wrapper: ReturnType<typeof mountPanel>) {
  await wrapper.findAll(".result-meta .qb-add")[3].trigger("click");
  await flushPromises();
}

beforeEach(() => {
  localStorage.clear();
  setKafkaConnectionId("conn-test");
});

// round4 面 1：管理表空态/加载态/错误态三态收敛（与 TopicTree 同序：
// error → loading → empty），不再把「集群无 topic」与「加载失败」混为一谈。
describe("TopicsPanel grid states", () => {
  it("shows a friendly error instead of the empty hint when load failed", () => {
    const wrapper = mountPanel({ topics: [], error: "Not authorized to access topics: [Topic authorization failed.]" });
    const state = wrapper.find(".grid-box .empty");
    expect(state.exists()).toBe(true);
    expect(state.text()).not.toBe(t("topics.empty"));
    expect(state.text()).toBe(t("err.forbidden"));
    // 原始错误串留在 title 悬停里供排查
    expect(state.attributes("title")).toContain("Topic authorization failed");
    expect(wrapper.find(".grid-stub").exists()).toBe(false);
  });

  it("shows the loading state instead of the empty hint while loading", () => {
    const wrapper = mountPanel({ topics: [], loading: true });
    expect(wrapper.find(".grid-box .empty").text()).toBe(t("tree.loading"));
  });

  it("keeps the plain empty hint for a genuinely empty cluster", () => {
    const wrapper = mountPanel({ topics: [], loading: false });
    expect(wrapper.find(".grid-box .empty").text()).toBe(t("topics.empty"));
  });
});

describe("TopicsPanel expand partitions", () => {
  it("rejects a smaller new count with the dedicated message and no request (P2-20)", async () => {
    installBridge({});
    const wrapper = mountPanel();
    await selectFirstTopic(wrapper);
    await openExpandDialog(wrapper);
    expect(wrapper.find(".modal-backdrop .modal").exists()).toBe(true);
    // 默认值为当前 + 1；填更小值（1 ≤ 当前 2）确认
    await wrapper.find('.modal-backdrop input[type="number"]').setValue("1");
    await wrapper.find(".modal-backdrop .primary-button").trigger("click");
    await flushPromises();
    expect(wrapper.emitted("error")?.at(-1)).toEqual([t("topics.expandCountInvalid", { count: 2 })]);
    expect(invokeMock.mock.calls.filter(([method]) => method === "kafka/topics/partitions/update")).toHaveLength(0);
  });

  it("rejects an equal new count with the dedicated message as well", async () => {
    installBridge({});
    const wrapper = mountPanel();
    await selectFirstTopic(wrapper);
    await openExpandDialog(wrapper);
    await wrapper.find('.modal-backdrop input[type="number"]').setValue("2");
    await wrapper.find(".modal-backdrop .primary-button").trigger("click");
    await flushPromises();
    expect(wrapper.emitted("error")?.at(-1)).toEqual([t("topics.expandCountInvalid", { count: 2 })]);
  });

  it("still submits a valid larger count", async () => {
    installBridge({ "kafka/topics/partitions/update": { success: true } });
    const wrapper = mountPanel();
    await selectFirstTopic(wrapper);
    await openExpandDialog(wrapper);
    await wrapper.find('.modal-backdrop input[type="number"]').setValue("4");
    await wrapper.find(".modal-backdrop .primary-button").trigger("click");
    await flushPromises();
    expect(wrapper.emitted("notify")?.at(-1)).toEqual([t("topics.expanded")]);
    const call = invokeMock.mock.calls.find(([method]) => method === "kafka/topics/partitions/update");
    // api 层把 partitions 包在 params 里并注入 connectionId
    expect(call?.[1]).toMatchObject({ partitions: { "order-events": 4 }, connectionId: "conn-test" });
  });
});

describe("TopicsPanel describe + offsets", () => {
  it("describe loads partition rows and renders the subpanel title; failure emits error", async () => {
    installBridge({ "kafka/topics/describe": { partitions } });
    const wrapper = mountPanel();
    await selectFirstTopic(wrapper);
    // 工具栏按钮顺序：describe(0) offsets(1) config(2) expand(3) delete(4)
    await wrapper.findAll(".result-meta .qb-add")[0].trigger("click");
    await flushPromises();
    expect(wrapper.text()).toContain(t("topics.describeTitle", { topic: "order-events" }));
    const call = invokeMock.mock.calls.find(([method]) => method === "kafka/topics/describe");
    expect(call?.[1]).toMatchObject({ topic: "order-events", connectionId: "conn-test" });

    // 失败路径：错误经 emit 透出（at(-1)：成功清空 emit("error","") 之后的真实错误）
    installBridge({});
    await wrapper.findAll(".result-meta .qb-add")[0].trigger("click");
    await flushPromises();
    expect(wrapper.emitted("error")?.at(-1)).toEqual(["unhandled method: kafka/topics/describe"]);
  });

  it("offsets default mode queries latest; custom mode converts via offsetTimeToParam", async () => {
    installBridge({ "kafka/topics/offsets/list": { rows: [{ topic: "order-events", partition: 0, offset: 42 }] } });
    const wrapper = mountPanel();
    await selectFirstTopic(wrapper);
    await wrapper.findAll(".result-meta .qb-add")[1].trigger("click");
    await flushPromises();
    expect(wrapper.text()).toContain(t("topics.offsetsTitle", { topic: "order-events" }));
    const latestCall = invokeMock.mock.calls.filter(([m]) => m === "kafka/topics/offsets/list").at(-1);
    expect(latestCall?.[1]).toMatchObject({ topics: ["order-events"], offsetTime: "latest" });

    // 切 custom 模式 → 出现时间输入 → 转换为 RFC3339（UTC，以 Z 结尾）
    await wrapper.find(".kafka-form select").setValue("custom");
    await wrapper.find(".kafka-form input").setValue("2026-01-02T03:04");
    await wrapper.find(".kafka-form .primary-button").trigger("click");
    await flushPromises();
    const customCall = invokeMock.mock.calls.filter(([m]) => m === "kafka/topics/offsets/list").at(-1);
    expect(typeof customCall?.[1].offsetTime).toBe("string");
    expect(customCall?.[1].offsetTime).toMatch(/Z$/);
    expect(Number.isNaN(Date.parse(customCall?.[1].offsetTime))).toBe(false);
  });
});

describe("TopicsPanel create", () => {
  it("rejects an empty name before any request", async () => {
    installBridge({});
    const wrapper = mountPanel();
    await wrapper.find(".result-meta .toolbar-button").trigger("click");
    await flushPromises();
    expect(wrapper.find(".modal-backdrop .modal").exists()).toBe(true);
    await wrapper.find(".modal-backdrop .primary-button").trigger("click");
    await flushPromises();
    expect(wrapper.emitted("error")?.at(-1)).toEqual([t("topics.createInvalid")]);
    expect(invokeMock).not.toHaveBeenCalled();
  });

  it("rejects a malformed config JSON without requesting create", async () => {
    installBridge({});
    const wrapper = mountPanel();
    await wrapper.find(".result-meta .toolbar-button").trigger("click");
    await flushPromises();
    await wrapper.find('.modal-backdrop input[type="text"]').setValue("new-topic");
    await wrapper.find(".modal-backdrop textarea").setValue("{nope");
    await wrapper.find(".modal-backdrop .primary-button").trigger("click");
    await flushPromises();
    expect(wrapper.emitted("error")?.at(-1)?.[0]).toContain(t("topics.config"));
    expect(invokeMock.mock.calls.filter(([m]) => m === "kafka/topics/create")).toHaveLength(0);
  });

  it("creates with parsed config and notifies + refreshes", async () => {
    installBridge({ "kafka/topics/create": { results: [{ topic: "new-topic", ok: true }] } });
    const wrapper = mountPanel();
    await wrapper.find(".result-meta .toolbar-button").trigger("click");
    await flushPromises();
    await wrapper.find('.modal-backdrop input[type="text"]').setValue("new-topic");
    await wrapper.find(".modal-backdrop textarea").setValue('{"retention.ms":"86400000"}');
    await wrapper.find(".modal-backdrop .primary-button").trigger("click");
    await flushPromises();
    expect(wrapper.emitted("notify")?.at(-1)).toEqual([`${t("topics.created")}: new-topic`]);
    expect(wrapper.emitted("refresh")).toBeTruthy();
    const call = invokeMock.mock.calls.find(([m]) => m === "kafka/topics/create");
    expect(call?.[1]).toMatchObject({
      topics: ["new-topic"],
      partitions: 1,
      replicationFactor: 1,
      config: { "retention.ms": "86400000" },
      connectionId: "conn-test",
    });
  });
});

describe("TopicsPanel delete gate", () => {
  async function openDeleteDialog(wrapper: ReturnType<typeof mountPanel>) {
    await selectFirstTopic(wrapper);
    await wrapper.findAll(".result-meta .qb-add")[4].trigger("click");
    await flushPromises();
  }

  it("keeps the confirm button disabled until the typed name matches the topic", async () => {
    installBridge({});
    const wrapper = mountPanel();
    await openDeleteDialog(wrapper);
    // teleport stub 下重渲染会重建 modal 子树：每次都从顶层 wrapper 重新 find
    expect(wrapper.find(".danger-button").attributes("disabled")).toBeDefined();
    await wrapper.find('.modal-backdrop input[type="text"]').setValue("wrong-name");
    expect(wrapper.find(".danger-button").attributes("disabled")).toBeDefined();
    await wrapper.find('.modal-backdrop input[type="text"]').setValue("order-events");
    expect(wrapper.find(".danger-button").attributes("disabled")).toBeUndefined();
  });

  it("deletes with confirmTopic and clears the selection afterwards", async () => {
    installBridge({ "kafka/topics/delete": { results: [{ topic: "order-events", ok: true }] } });
    const wrapper = mountPanel();
    await openDeleteDialog(wrapper);
    await wrapper.find('.modal-backdrop input[type="text"]').setValue("order-events");
    await wrapper.find(".danger-button").trigger("click");
    await flushPromises();
    expect(wrapper.emitted("notify")?.at(-1)).toEqual([`${t("topics.deleted")}: order-events`]);
    expect(wrapper.emitted("refresh")).toBeTruthy();
    const call = invokeMock.mock.calls.find(([m]) => m === "kafka/topics/delete");
    expect(call?.[1]).toMatchObject({ topics: ["order-events"], confirmTopic: "order-events", connectionId: "conn-test" });
  });
});

describe("TopicsPanel config", () => {
  async function openConfigDialog(wrapper: ReturnType<typeof mountPanel>, extra: Record<string, unknown> = {}) {
    installBridge({ "kafka/topics/config/get": { entries: configEntries }, ...extra });
    await selectFirstTopic(wrapper);
    await wrapper.findAll(".result-meta .qb-add")[2].trigger("click");
    await flushPromises();
  }

  it("loads entries, masks sensitive values and badges defaults", async () => {
    const wrapper = mountPanel();
    await openConfigDialog(wrapper);
    expect(wrapper.find(".modal-backdrop .modal").exists()).toBe(true);
    const modalText = wrapper.find(".modal-backdrop .modal").text();
    expect(modalText).toContain("cleanup.policy");
    // sensitive 值掩码显示，不出明文
    expect(modalText).toContain(t("brokers.sensitiveMasked"));
    expect(modalText).not.toContain("secret");
    // 默认值徽标
    expect(modalText).toContain(t("brokers.colDefault"));
    const call = invokeMock.mock.calls.find(([m]) => m === "kafka/topics/config/get");
    expect(call?.[1]).toMatchObject({ topic: "order-events", connectionId: "conn-test" });
  });

  it("submits only non-empty edit rows and routes remove-flagged keys to deleteKeys", async () => {
    const wrapper = mountPanel();
    await openConfigDialog(wrapper, { "kafka/topics/config/alter": { entries: configEntries } });
    // 两行编辑：一行 set（key/value），一行 remove；第三行 key 为空 → 应被跳过
    for (let i = 0; i < 3; i += 1) {
      await wrapper.find('.modal-backdrop .panel-modal .qb-add').trigger("click");
    }
    // 重渲染重建子树：每次都重新抓行引用
    const rows = () => wrapper.findAll('.modal-backdrop .field-filter-row');
    expect(rows()).toHaveLength(3);
    await rows()[0].findAll('input[type="text"]')[0].setValue("retention.ms");
    await rows()[0].findAll('input[type="text"]')[1].setValue("3600000");
    await rows()[1].findAll('input[type="text"]')[0].setValue("stale.key");
    await rows()[1].find('input[type="checkbox"]').setValue(true);
    await wrapper.find(".modal-backdrop footer .primary-button").trigger("click");
    await flushPromises();
    expect(wrapper.emitted("notify")?.at(-1)).toEqual([t("topics.altered")]);
    const call = invokeMock.mock.calls.find(([m]) => m === "kafka/topics/config/alter");
    expect(call?.[1]).toMatchObject({
      topic: "order-events",
      config: { "retention.ms": "3600000" },
      deleteKeys: ["stale.key"],
      connectionId: "conn-test",
    });
  });

  it("disables save in read-only mode", async () => {
    const wrapper = mountPanel({ canWrite: false });
    await openConfigDialog(wrapper);
    expect(wrapper.find(".modal-backdrop footer .primary-button").attributes("disabled")).toBeDefined();
  });
});

describe("TopicsPanel gating + selection refresh", () => {
  it("read-only mode disables create and titles it with the readOnly hint", async () => {
    installBridge({});
    const wrapper = mountPanel({ canWrite: false });
    await flushPromises();
    const createButton = wrapper.find(".result-meta .toolbar-button");
    expect(createButton.attributes("disabled")).toBeDefined();
    expect(createButton.attributes("title")).toBe(t("readOnly"));
  });

  it("delete tool keeps the noDelete hint when delete is not allowed", async () => {
    installBridge({});
    const wrapper = mountPanel({ canDelete: false });
    await flushPromises();
    const deleteButton = wrapper.findAll(".result-meta .qb-add")[4];
    expect(deleteButton.attributes("disabled")).toBeDefined();
    expect(deleteButton.attributes("title")).toBe(t("noDelete"));
  });

  it("keeps the selection across a same-name refresh and drops it when the topic disappears", async () => {
    installBridge({ "kafka/topics/describe": { partitions } });
    const wrapper = mountPanel();
    await selectFirstTopic(wrapper);
    await wrapper.findAll(".result-meta .qb-add")[0].trigger("click");
    await flushPromises();
    expect(wrapper.text()).toContain(t("topics.describeTitle", { topic: "order-events" }));
    // 同名刷新：选中保持（分区子面板仍在）
    await wrapper.setProps({ topics: [{ ...topics[0], partitionCount: 3 }] });
    await flushPromises();
    expect(wrapper.text()).toContain(t("topics.describeTitle", { topic: "order-events" }));
    // topic 从列表消失：选中清空（子面板收起）
    await wrapper.setProps({ topics: [{ name: "other", partitionCount: 1, replicationFactor: 1 }] });
    await flushPromises();
    expect(wrapper.text()).not.toContain(t("topics.describeTitle", { topic: "order-events" }));
  });
});

// -- Lane4 前端打磨：收藏星标列 + 收藏置顶排序 --------------------------------

describe("TopicsPanel favorites (Lane4)", () => {
  const multi: KafkaTopic[] = [
    { name: "_schemas", partitionCount: 1, replicationFactor: 1, isInternal: true },
    { name: "users", partitionCount: 3, replicationFactor: 1 },
    { name: "order-events", partitionCount: 2, replicationFactor: 1 },
  ];

  beforeEach(() => {
    localStorage.clear();
    setTopicFavorites([]);
  });

  it("renders the star column first and pins favorite topics above the rest", async () => {
    setTopicFavorites(["users"]);
    const wrapper = mountPanel({ topics: multi });
    await flushPromises();
    const grid = () => wrapper.find('.grid-stub[data-key="topics"]');
    // 星标列在最前（5 列 = 星标 + name/internal/partitions/replication）
    expect(grid().attributes("data-first-col")).toBe("action-favorite");
    expect(grid().attributes("data-cols")).toBe("5");
    const names = () => wrapper.findAll('.grid-stub[data-key="topics"] .grid-stub-row').map((row) => row.text());
    // 收藏 users 置顶，internal 沉底保持
    expect(names()).toEqual(["users", "order-events", "_schemas"]);
    // 取消收藏 → 恢复业务评分序（order-events → users → _schemas）
    setTopicFavorites([]);
    await flushPromises();
    expect(names()).toEqual(["order-events", "users", "_schemas"]);
  });
});
