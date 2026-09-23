// @vitest-environment happy-dom
// TopicTree 组件测试：业务排序 + internal 沉底渲染、过滤框、选中态与 select 事件。
// Lane4 打磨：internal 显隐开关 + 收藏星标置顶（收藏态为模块级共享状态，
// 用例前经 setTopicFavorites([]) 复位，避免用例间串扰）。
import { beforeEach, describe, expect, it } from "vitest";
import { mount } from "@vue/test-utils";
import TopicTree from "./TopicTree.vue";
import type { KafkaTopic } from "../lib/api";
import { setTopicFavorites } from "../lib/topicFavorites";
import { SHOW_INTERNAL_KEY, pluginStore } from "../lib/pluginStore";
import { t } from "../lib/i18n";

const topics: KafkaTopic[] = [
  { name: "_schemas", partitionCount: 1, replicationFactor: 1, isInternal: true },
  { name: "users", partitionCount: 3, replicationFactor: 1 },
  { name: "order-events", partitionCount: 2, replicationFactor: 1 },
];

function mountTree(selectedTopic = "") {
  return mount(TopicTree, {
    props: { topics, loading: false, error: "", selectedTopic },
  });
}

describe("TopicTree", () => {
  it("renders business topics first, internal topics sunk to the bottom", () => {
    const wrapper = mountTree();
    const names = wrapper.findAll(".tree-name").map((node) => node.text());
    expect(names).toEqual(["order-events", "users", "_schemas"]);
    expect(wrapper.findAll(".badge-internal")).toHaveLength(1);
  });

  it("shows empty state for empty topic list", () => {
    const wrapper = mount(TopicTree, { props: { topics: [], loading: false, error: "", selectedTopic: "" } });
    expect(wrapper.text()).toContain("集群内暂无 topic");
  });

  it("shows the error text when the backend list fails", () => {
    const wrapper = mount(TopicTree, { props: { topics: [], loading: false, error: "boom", selectedTopic: "" } });
    expect(wrapper.find(".tree-error").text()).toBe("boom");
  });

  // P2-18：树错误区与错误横幅同源 friendlyKafkaError——夹具/网络类错误本地化，
  // 原始串留在 title 悬停；未覆盖错误原文透传且无 title。
  it("friendly-maps connection errors and keeps the raw string in the title (P2-18)", () => {
    const wrapper = mount(TopicTree, { props: { topics: [], loading: false, error: "connection lost (fixture error injection)", selectedTopic: "" } });
    const node = wrapper.find(".tree-error");
    expect(node.text()).toBe("无法连接 Kafka broker：请检查 bootstrap servers 与网络");
    expect(node.attributes("title")).toBe("connection lost (fixture error injection)");
  });

  it("passes unknown errors through unchanged without a hover title (P2-18)", () => {
    const wrapper = mount(TopicTree, { props: { topics: [], loading: false, error: "boom", selectedTopic: "" } });
    const node = wrapper.find(".tree-error");
    expect(node.text()).toBe("boom");
    expect(node.attributes("title")).toBeFalsy();
  });

  // F6-5：isHealthy===false 红点 + title「N 个分区不健康」（无额外请求）；
  // 健康行与旧 sidecar 缺省字段不出红点。
  it("marks unhealthy topics with a count-titled red dot (F6-5)", () => {
    const withUnhealthy: KafkaTopic[] = [
      { name: "degraded-topic", partitionCount: 2, replicationFactor: 1, isHealthy: false, unhealthyPartitions: 1 },
      { name: "order-events", partitionCount: 2, replicationFactor: 1, isHealthy: true },
      { name: "legacy-topic", partitionCount: 1, replicationFactor: 1 },
    ];
    const wrapper = mount(TopicTree, { props: { topics: withUnhealthy, loading: false, error: "", selectedTopic: "" } });
    const dots = wrapper.findAll(".tree-health-dot");
    expect(dots).toHaveLength(1);
    expect(dots[0].attributes("title")).toBe(t("tree.unhealthyTitle", { count: 1 }));
    expect(dots[0].attributes("aria-label")).toBe(t("tree.unhealthyTitle", { count: 1 }));
  });

  it("filters by keyword and emits select with the topic name", async () => {
    const wrapper = mountTree();
    await wrapper.find(".tree-filter input").setValue("user");
    expect(wrapper.findAll(".tree-row")).toHaveLength(1);
    await wrapper.find(".tree-row").trigger("click");
    expect(wrapper.emitted("select")?.[0]).toEqual(["users"]);
  });

  it("marks the selected topic row", () => {
    const wrapper = mountTree("users");
    expect(wrapper.find(".tree-row.selected").text()).toContain("users");
  });

  // P2-22：过滤框 Enter 选中首个匹配项（big 模式下逐 Tab 穿树可达数百个 tab stop）。
  it("selects the first matching topic on Enter in the filter box (P2-22)", async () => {
    const wrapper = mountTree();
    await wrapper.find(".tree-filter input").setValue("order");
    await wrapper.find(".tree-filter input").trigger("keydown", { key: "Enter" });
    expect(wrapper.emitted("select")?.[0]).toEqual(["order-events"]);
  });

  it("Enter with no match does not emit select", async () => {
    const wrapper = mountTree();
    await wrapper.find(".tree-filter input").setValue("no-such-topic");
    await wrapper.find(".tree-filter input").trigger("keydown", { key: "Enter" });
    expect(wrapper.emitted("select")).toBeUndefined();
  });

  // P2-22：清除钮移出 Tab 序——过滤激活时 Tab 从过滤框直达树行，不会先误停
  // 在清除钮上（键盘清空走既有 Esc 路径）。
  it("keeps the filter clear button out of the tab order (P2-22)", async () => {
    const wrapper = mountTree();
    expect(wrapper.find(".tree-filter .icon-button").exists()).toBe(false);
    await wrapper.find(".tree-filter input").setValue("user");
    const clearButton = wrapper.find(".tree-filter .icon-button");
    expect(clearButton.exists()).toBe(true);
    expect(clearButton.attributes("tabindex")).toBe("-1");
    // 清除钮仍可点击生效（鼠标路径不受影响）
    await clearButton.trigger("click");
    expect(wrapper.findAll(".tree-row")).toHaveLength(3);
  });
});

// round3：roving tabindex / listbox 键盘导航——树收敛为单 tab stop，
// 方向键 / Home / End 移动 active 并 emit select（选中态单一来源仍在父级），
// 鼠标点击路径与视觉不变。断言一律每次交互后从顶层 wrapper 重查（防 stale 引用）。
describe("TopicTree keyboard navigation (roving tabindex)", () => {
  const bigTopics: KafkaTopic[] = Array.from({ length: 507 }, (_, i) => ({
    name: `big-topic-${String(i).padStart(3, "0")}`,
    partitionCount: 1,
    replicationFactor: 1,
  }));

  // 507 节点在全套件并行时渲染可超过默认 5s，放宽超时避免假失败（CI 实测）。
  it("renders listbox semantics with exactly one tab stop (507-node big mode guard)", { timeout: 20_000 }, () => {
    const wrapper = mount(TopicTree, { props: { topics: bigTopics, loading: false, error: "", selectedTopic: "" } });
    const listbox = wrapper.find(".tree-node");
    expect(listbox.attributes("role")).toBe("listbox");
    expect(listbox.attributes("aria-label")).toBe(t("tree.title"));
    const rows = wrapper.findAll(".tree-row");
    expect(rows).toHaveLength(507);
    expect(rows[0].attributes("role")).toBe("option");
    // 全树只允许一个 tab stop（防回归：退回逐行停靠即 507 个 tab stop）
    expect(wrapper.findAll(".tree-row[tabindex='0']")).toHaveLength(1);
    expect(wrapper.findAll(".tree-row[tabindex='-1']")).toHaveLength(506);
    expect(rows[0].attributes("aria-selected")).toBe("false");
  });

  it("moves active and emits select with ArrowDown/ArrowUp", async () => {
    const wrapper = mountTree();
    // 初始 active = 排序后首行 order-events
    expect(wrapper.find(".tree-row[tabindex='0']").text()).toContain("order-events");
    await wrapper.find(".tree-row").trigger("keydown", { key: "ArrowDown" });
    expect(wrapper.findAll(".tree-row")[1].attributes("tabindex")).toBe("0");
    // 父级未回填 selectedTopic 时仅 active 移动，选中态仍由 prop 决定
    expect(wrapper.findAll(".tree-row")[1].classes()).not.toContain("selected");
    expect(wrapper.emitted("select")?.[0]).toEqual(["users"]);
    await wrapper.findAll(".tree-row")[1].trigger("keydown", { key: "ArrowUp" });
    expect(wrapper.find(".tree-row[tabindex='0']").text()).toContain("order-events");
    expect(wrapper.emitted("select")?.[1]).toEqual(["order-events"]);
  });

  it("stays put without emitting at the first/last boundary", async () => {
    const wrapper = mountTree();
    // active 在首行：ArrowUp 不移动、不 emit
    await wrapper.find(".tree-row").trigger("keydown", { key: "ArrowUp" });
    expect(wrapper.emitted("select")).toBeUndefined();
    // End 跳到末行后 ArrowDown 不再移动、不再 emit
    await wrapper.find(".tree-row").trigger("keydown", { key: "End" });
    expect(wrapper.emitted("select")?.[0]).toEqual(["_schemas"]);
    await wrapper.find(".tree-row[tabindex='0']").trigger("keydown", { key: "ArrowDown" });
    expect((wrapper.emitted("select") ?? []).map((args) => args[0])).toEqual(["_schemas"]);
  });

  it("jumps to first/last row with Home/End and emits select", async () => {
    const wrapper = mountTree();
    await wrapper.find(".tree-row").trigger("keydown", { key: "End" });
    expect(wrapper.find(".tree-row[tabindex='0']").text()).toContain("_schemas");
    expect(wrapper.emitted("select")?.[0]).toEqual(["_schemas"]);
    await wrapper.find(".tree-row[tabindex='0']").trigger("keydown", { key: "Home" });
    expect(wrapper.find(".tree-row[tabindex='0']").text()).toContain("order-events");
    expect(wrapper.emitted("select")?.[1]).toEqual(["order-events"]);
  });

  it("falls back to the first visible row when filtering hides the active row", async () => {
    const wrapper = mountTree("users");
    expect(wrapper.find(".tree-row[tabindex='0']").text()).toContain("users");
    await wrapper.find(".tree-filter input").setValue("order");
    expect(wrapper.find(".tree-row[tabindex='0']").text()).toContain("order-events");
    // 清过滤后 active 不回跳旧值（保持回落结果）
    await wrapper.find(".tree-filter input").setValue("");
    expect(wrapper.find(".tree-row[tabindex='0']").text()).toContain("order-events");
  });

  it("syncs active when the parent changes selectedTopic externally", async () => {
    const wrapper = mountTree();
    expect(wrapper.find(".tree-row[tabindex='0']").text()).toContain("order-events");
    await wrapper.setProps({ selectedTopic: "_schemas" });
    expect(wrapper.find(".tree-row[tabindex='0']").text()).toContain("_schemas");
  });
});

// Lane4 前端打磨：internal 显隐开关 + 收藏星标置顶。
describe("TopicTree internal toggle + favorites (Lane4)", () => {
  beforeEach(() => {
    // 持久化后端是 pluginStore（宿主 storage 适配），清理须走同一实例。
    pluginStore.removeItem(SHOW_INTERNAL_KEY);
    setTopicFavorites([]);
  });

  it("hides internal topics via the eye toggle and restores them (default = show)", async () => {
    const wrapper = mountTree();
    const names = () => wrapper.findAll(".tree-name").map((node) => node.text());
    expect(names()).toEqual(["order-events", "users", "_schemas"]);
    const toggle = () => wrapper.find('[data-testid="internal-toggle"]');
    // 默认显示：aria-pressed=false（未被隐藏）
    expect(toggle().attributes("aria-pressed")).toBe("false");
    await toggle().trigger("click");
    expect(names()).toEqual(["order-events", "users"]);
    expect(toggle().attributes("aria-pressed")).toBe("true");
    // 隐藏只过滤展示：总数徽章仍按 props.topics 计
    expect(wrapper.find(".tree-count").text()).toBe(t("messages.uiTreeFilterCount", { matched: 2, total: 3 }));
    // 隐藏态记忆落 pluginStore（与侧栏宽度同款模式）
    expect(pluginStore.getItem(SHOW_INTERNAL_KEY)).toBe("0");
    // 再点恢复显示，记忆回到「显示」
    await toggle().trigger("click");
    expect(names()).toEqual(["order-events", "users", "_schemas"]);
    expect(pluginStore.getItem(SHOW_INTERNAL_KEY)).toBe("1");
  });

  it("pins a starred topic to the top and toggles the star state without selecting the row", async () => {
    const wrapper = mountTree();
    const names = () => wrapper.findAll(".tree-name").map((node) => node.text());
    const star = () => wrapper.find('[data-testid="star-users"]');
    expect(star().classes()).not.toContain("is-fav");
    expect(star().attributes("aria-label")).toBe(t("polish.favoriteAdd"));
    await star().trigger("click");
    // 星标点击不触发行选中（@click.stop）
    expect(wrapper.emitted("select")).toBeUndefined();
    // users 置顶；internal 沉底保持
    expect(names()).toEqual(["users", "order-events", "_schemas"]);
    expect(wrapper.find('[data-testid="star-users"]').classes()).toContain("is-fav");
    expect(wrapper.find('[data-testid="star-users"]').attributes("aria-label")).toBe(t("polish.favoriteRemove"));
    // 再点取消收藏，恢复原排序
    await wrapper.find('[data-testid="star-users"]').trigger("click");
    expect(names()).toEqual(["order-events", "users", "_schemas"]);
  });

  it("pins internal topics too when explicitly starred (user intent wins over sinking)", async () => {
    setTopicFavorites(["_schemas"]);
    const wrapper = mountTree();
    const names = wrapper.findAll(".tree-name").map((node) => node.text());
    expect(names).toEqual(["_schemas", "order-events", "users"]);
    expect(wrapper.findAll(".badge-internal")).toHaveLength(1);
  });
});
