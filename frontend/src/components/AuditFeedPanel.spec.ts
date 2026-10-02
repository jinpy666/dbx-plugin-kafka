// @vitest-environment happy-dom
// AuditFeedPanel 组件测试（纯展示：数据面在 App.vue → lib/auditFeed）：
// 空事件不渲染 / 折叠计数（denied 徽标高亮）/ denied 事件到达自动展开 +
// denied 徽标 / ok 事件不自动展开 / 手动开合 / 清空 emit + 空态摘要回退。
// 事件条目照 mockDbxHost ?audit=denied 夹具直推（组件收 items props）。
import { beforeEach, describe, expect, it } from "vitest";
import { mount } from "@vue/test-utils";
import AuditFeedPanel from "./AuditFeedPanel.vue";
import type { AuditFeedItem } from "../lib/auditFeed";
import { t } from "../lib/i18n";

function item(partial: Partial<AuditFeedItem> & { id: number }): AuditFeedItem {
  return { action: "topics/delete", target: "order-events", result: "ok", at: 1_700_000_000_000, ...partial };
}

function mountFeed(items: AuditFeedItem[]) {
  return mount(AuditFeedPanel, { props: { items } });
}

beforeEach(() => {
  localStorage.clear();
});

describe("AuditFeedPanel", () => {
  it("renders nothing without events while collapsed", () => {
    const wrapper = mountFeed([]);
    expect(wrapper.find(".audit-feed").exists()).toBe(false);
  });

  it("shows collapsed header counts with the denied badge highlight", () => {
    const wrapper = mountFeed([
      item({ id: 1, action: "topics/delete", target: "order-events", result: "denied", detail: "read-only" }),
      item({ id: 2, action: "messages/produce", target: "order-events", result: "ok" }),
    ]);
    const feed = wrapper.find(".audit-feed");
    expect(feed.classes()).toContain("audit-feed-denied");
    expect(wrapper.find(".audit-summary").text()).toBe(
      `${t("audit.events", { total: 2 })} · ${t("audit.deniedCount", { denied: 1 })}`,
    );
    expect(wrapper.find(".audit-summary").classes()).toContain("audit-summary-denied");
    // 折叠态：列表不渲染，toggle 可用
    expect(wrapper.find(".audit-list").exists()).toBe(false);
    expect(wrapper.find(".audit-toggle").attributes("aria-expanded")).toBe("false");
  });

  it("auto-expands once a denied event arrives and renders the denied badge", async () => {
    const wrapper = mountFeed([item({ id: 1, action: "messages/produce", result: "ok" })]);
    expect(wrapper.find(".audit-list").exists()).toBe(false);
    // 照 ?audit=denied 夹具：denied 事件（新条目插头部）到达
    await wrapper.setProps({
      items: [
        item({ id: 3, action: "topics/delete", target: "order-events", result: "denied", detail: "connection is read-only" }),
        item({ id: 1, action: "messages/produce", result: "ok" }),
      ],
    });
    expect(wrapper.find(".audit-list").exists()).toBe(true);
    const deniedRow = wrapper.find(".audit-item-denied");
    expect(deniedRow.exists()).toBe(true);
    expect(deniedRow.find(".audit-badge").text()).toBe(t("audit.result.denied"));
    expect(deniedRow.find(".audit-action").text()).toBe("topics/delete");
    expect(deniedRow.find(".audit-target").text()).toBe("order-events");
    // ok 行对照
    expect(wrapper.find(".audit-item-ok .audit-badge").text()).toBe(t("audit.result.ok"));
  });

  it("stays collapsed when only ok events arrive", async () => {
    const wrapper = mountFeed([item({ id: 1, action: "topics/list", result: "ok" })]);
    await wrapper.setProps({
      items: [item({ id: 2, action: "groups/list", result: "ok" }), item({ id: 1, action: "topics/list", result: "ok" })],
    });
    expect(wrapper.find(".audit-list").exists()).toBe(false);
    expect(wrapper.find(".audit-summary").text()).toBe(t("audit.events", { total: 2 }));
  });

  it("expands and collapses via the toggle button", async () => {
    const wrapper = mountFeed([item({ id: 1, action: "messages/produce", result: "ok" })]);
    await wrapper.find(".audit-toggle").trigger("click");
    expect(wrapper.find(".audit-list").exists()).toBe(true);
    expect(wrapper.find(".audit-toggle").attributes("aria-expanded")).toBe("true");
    expect(wrapper.find(".audit-toggle").attributes("title")).toBe(t("audit.hide"));
    await wrapper.find(".audit-toggle").trigger("click");
    expect(wrapper.find(".audit-list").exists()).toBe(false);
    expect(wrapper.find(".audit-toggle").attributes("title")).toBe(t("audit.show"));
  });

  it("emits clear and collapses the empty feed (KAFKA-AF-L7)", async () => {
    const wrapper = mountFeed([item({ id: 1, action: "produce", result: "ok" })]);
    await wrapper.find(".audit-toggle").trigger("click");
    await wrapper.find(".audit-clear").trigger("click");
    expect(wrapper.emitted("clear")).toHaveLength(1);
    await wrapper.setProps({ items: [] });
    // 清空后 expanded 复位 → 空面板整体收起（此前 expanded 残留 true 且
    // toggle 禁用，空面板永久可见）。
    expect(wrapper.find(".audit-feed").exists()).toBe(false);
  });

  it("counts denied and error separately (KAFKA-AF-L6)", async () => {
    const wrapper = mountFeed([
      item({ id: 1, action: "produce", result: "denied" }),
      item({ id: 2, action: "topics-delete", result: "error" }),
    ]);
    const summary = wrapper.find(".audit-summary").text();
    expect(summary).toContain(t("audit.deniedCount", { denied: 1 }));
    expect(summary).toContain(t("audit.errorCount", { error: 1 }));
  });
});
