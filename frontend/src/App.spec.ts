// @vitest-environment happy-dom
// App 壳层 a11y 测试（UI 扫描第 4 轮 P2-24 防回归）：
// 错误横幅 role="alert"、成功通知 role="status" + aria-live="polite"——
// 异步到达的错误/成功反馈对读屏可感知（与 ProducePanel 成功条约定收敛）。
import { beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import App from "./App.vue";
import MessagesPanel from "./components/MessagesPanel.vue";
import { setKafkaConnectionId } from "./lib/api";

const invokeMock = vi.fn();
// 宿主桥事件监听器列表（App壳 handleEvent 与 useUiIntent 各订阅一份）。
const eventListeners: Array<(event: { method: string; params: Record<string, unknown> }) => void> = [];
function emitHostEvent(event: { method: string; params: Record<string, unknown> }) {
  for (const listener of eventListeners) listener(event);
}

function installHostBridge() {
  invokeMock.mockReset();
  invokeMock.mockImplementation(async (method: string) => {
    if (method === "kafka/topics/list") return { topics: [] };
    if (method === "kafka/connections/statuses") return { statuses: [] };
    if (method === "kafka/presets/list") return { presets: [] };
    throw new Error(`unhandled method: ${method}`);
  });
  eventListeners.length = 0;
  (window as unknown as { dbxPlugin: unknown }).dbxPlugin = {
    // ready 永不 resolve：走 Promise.any 兜底的 host.getContext（与真实宿主慢启动同型）
    ready: new Promise(() => {}),
    request: async (method: string) => {
      if (method === "host.getContext") return { connectionId: "conn-test" };
      return {};
    },
    invoke: invokeMock,
    locale: "zh-CN",
    onEvent: (listener: (event: { method: string; params: Record<string, unknown> }) => void) => {
      eventListeners.push(listener);
      return () => {
        const index = eventListeners.indexOf(listener);
        if (index >= 0) eventListeners.splice(index, 1);
      };
    },
  };
}

async function mountApp() {
  const wrapper = mount(App);
  await flushPromises();
  await flushPromises();
  return wrapper;
}

beforeEach(() => {
  localStorage.clear();
  setKafkaConnectionId("conn-test");
  installHostBridge();
});

describe("App a11y live regions (P2-24)", () => {
  it("announces async errors via role=alert on the error banner", async () => {
    const wrapper = await mountApp();
    expect(wrapper.find(".error-banner").exists()).toBe(false);
    // 异步到达的错误（审计 denied/error 事件走 showError）
    emitHostEvent({ method: "kafka/audit", params: { action: "kafka/test", result: "denied", detail: "boom" } });
    await flushPromises();
    const banner = wrapper.find(".error-banner");
    expect(banner.exists()).toBe(true);
    expect(banner.attributes("role")).toBe("alert");
    wrapper.unmount();
  });

  it("announces success notices via role=status with aria-live=polite", async () => {
    const wrapper = await mountApp();
    expect(wrapper.find(".notice").exists()).toBe(false);
    // 面板成功通知（@notify → showNotice），经 MessagesPanel 子组件实例触发
    wrapper.findComponent(MessagesPanel).vm.$emit("notify", "已保存");
    await flushPromises();
    const notice = wrapper.find(".notice");
    expect(notice.exists()).toBe(true);
    expect(notice.text()).toBe("已保存");
    expect(notice.attributes("role")).toBe("status");
    expect(notice.attributes("aria-live")).toBe("polite");
    wrapper.unmount();
  });
});

describe("App host font tokens (字体设置下发)", () => {
  it("applies colors inline but leaves fonts to the host theme bridge", async () => {
    const wrapper = await mountApp();
    document.dispatchEvent(new CustomEvent("dbx-plugin-env", { detail: { theme: { appearance: "dark", tokens: { "--color-background": "rgb(1 2 3)" } } } }));
    await flushPromises();
    // 颜色仍由 applyAppearance 内联回写。
    expect(document.documentElement.style.getPropertyValue("--background")).toBe("rgb(1 2 3)");
    // 字体交给主题桥（--ui-font-family:var(--font-sans,…)）跟随宿主字体设置，
    // 内联回写会压过桥接样式，把字体钉死在插件默认栈。
    expect(document.documentElement.style.getPropertyValue("--ui-font-family")).toBe("");
    wrapper.unmount();
  });
});

describe("App context menu behavior", () => {
  it("suppresses the native context menu across the workbench", async () => {
    const wrapper = await mountApp();
    const event = new MouseEvent("contextmenu", { bubbles: true, cancelable: true });

    document.dispatchEvent(event);

    expect(event.defaultPrevented).toBe(true);
    wrapper.unmount();
  });
});

describe("App stream backpressure visibility (§8.3 遗留收口)", () => {
  it("notifies dropped buffered stream events after returning to the stream panel", async () => {
    const wrapper = await mountApp();
    // 流面板未激活（App 层缓冲生效）：灌满 800 缓冲 + 1 条触发丢最旧。
    for (let i = 0; i < 801; i++) {
      emitHostEvent({
        method: "kafka/stream/messages",
        params: {
          sessionId: "s1",
          messages: [{ topic: "t", partition: 0, offset: i, timestamp: 1, valueText: "x", valueBase64: "", headers: {} }],
          totalScanned: i + 1,
          totalMatched: i + 1,
          paused: false,
          bufferSize: 0,
        },
      });
    }
    await flushPromises();
    expect(wrapper.find(".notice").exists()).toBe(false); // 面板隐藏期间静默缓冲
    // 切到流式面板 → 缓冲按序补发完成后，丢弃计数一次性可见。
    const streamTab = wrapper.findAll(".tab-bar button").find((b) => b.text().includes("流式"));
    expect(streamTab).toBeDefined();
    await streamTab!.trigger("click");
    await flushPromises();
    const notice = wrapper.find(".notice");
    expect(notice.exists()).toBe(true);
    expect(notice.text()).toContain("1");
    wrapper.unmount();
  });

  it("does not notify when no buffered events were dropped", async () => {
    const wrapper = await mountApp();
    for (let i = 0; i < 10; i++) {
      emitHostEvent({ method: "kafka/stream/messages", params: { sessionId: "s1", messages: [], totalScanned: i, totalMatched: i, paused: false, bufferSize: 0 } });
    }
    await flushPromises();
    const streamTab = wrapper.findAll(".tab-bar button").find((b) => b.text().includes("流式"));
    await streamTab!.trigger("click");
    await flushPromises();
    expect(wrapper.find(".notice").exists()).toBe(false);
    wrapper.unmount();
  });
});

describe("App MCP UI intent wiring (M3)", () => {
  it("applies a focus intent by switching panels and reports through kafka/ui/state/report", async () => {
    const wrapper = await mountApp();
    emitHostEvent({
      method: "kafka/ui/intent",
      params: { intentId: "i-focus", action: "focus", params: { panel: "topics" } },
    });
    await flushPromises();
    await flushPromises();
    // openPanel 先报快照（panel 切换），intent 处理器随后报 applied。
    const reports = invokeMock.mock.calls.filter(([method]) => method === "kafka/ui/state/report").map(([, body]) => body);
    expect(reports).toContainEqual(expect.objectContaining({ intentId: "i-focus", status: "applied", summary: { panel: "topics" } }));
    expect(reports).toContainEqual(expect.objectContaining({ status: "snapshot", summary: { panel: "topics" } }));
    // 面板确实切换（topics tab 激活态）。
    const tabs = wrapper.findAll(".tab-bar button");
    const topicsTab = tabs.find((tab) => tab.text().length > 0 && tab.classes().includes("is-active"));
    expect(topicsTab).toBeTruthy();
  });

  it("reports rejected for an unknown panel action", async () => {
    await mountApp();
    emitHostEvent({
      method: "kafka/ui/intent",
      params: { intentId: "i-bad", action: "focus", params: { panel: "nope" } },
    });
    await flushPromises();
    await flushPromises();
    const reports = invokeMock.mock.calls.filter(([method]) => method === "kafka/ui/state/report").map(([, body]) => body);
    expect(reports).toContainEqual(expect.objectContaining({ intentId: "i-bad", status: "rejected" }));
  });

  // MCP 检索必须可见地落到 UI：用户停在别的 tab 时，search intent 先把
  // 工作台带回消息面板再填表+消费（此前填表发生在 v-show 隐藏面板里，
  // 看起来像「没反应」）。
  it("search intent switches back to the messages panel before applying", async () => {
    const wrapper = await mountApp();
    emitHostEvent({
      method: "kafka/ui/intent",
      params: { intentId: "i-focus", action: "focus", params: { panel: "topics" } },
    });
    await flushPromises();
    await flushPromises();
    invokeMock.mockImplementation(async (method: string) => {
      if (method === "kafka/topics/list") return { topics: [] };
      if (method === "kafka/connections/statuses") return { statuses: [] };
      if (method === "kafka/presets/list") return { presets: [] };
      if (method === "kafka/messages/consume") return { messages: [], scanned: 0, matched: 0, limited: false, hasMore: false };
      throw new Error(`unhandled method: ${method}`);
    });
    emitHostEvent({
      method: "kafka/ui/intent",
      params: { intentId: "i-search", action: "search", params: { topic: "order-events" } },
    });
    await flushPromises();
    await flushPromises();
    const activeTab = wrapper.findAll(".tab-bar button").find((tab) => tab.classes().includes("is-active"));
    expect(activeTab?.text()).toContain("消息");
    const reports = invokeMock.mock.calls.filter(([method]) => method === "kafka/ui/state/report").map(([, body]) => body);
    expect(reports).toContainEqual(expect.objectContaining({ intentId: "i-search", status: "applied" }));
    wrapper.unmount();
  });
});
