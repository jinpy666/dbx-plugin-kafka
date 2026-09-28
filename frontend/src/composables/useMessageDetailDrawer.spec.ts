// @vitest-environment happy-dom
// useMessageDetailDrawer 组合式函数测试（测试覆盖续轮）：detail 打开时的
// 视图重置与格式探测（XML/JSON/raw）、headers 双视图、区块折叠、手动
// renderView、Esc 关闭焦点归还、Tab 焦点回绕与更高层弹窗让位。
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import { defineComponent, h, nextTick } from "vue";
import { useMessageDetailDrawer } from "./useMessageDetailDrawer";
import type { KafkaMessage } from "../lib/api";

type Drawer = ReturnType<typeof useMessageDetailDrawer>;

// 宿主组件：composable 的 onMounted/onBeforeUnmount 需要组件上下文；模板里
// 同时渲染触发按钮与抽屉容器（两个可聚焦按钮），驱动焦点归还/陷阱断言。
const Host = defineComponent({
  setup(_props, { expose }) {
    const drawer = useMessageDetailDrawer();
    expose({ getDrawer: () => drawer });
    return () =>
      h("div", [
        h("button", { class: "trigger", type: "button" }, "open"),
        h(
          "div",
          { ref: drawer.drawerEl, tabindex: "-1", class: "drawer" },
          [h("button", { class: "d1", type: "button" }), h("button", { class: "d2", type: "button" })],
        ),
      ]);
  },
});

function message(overrides: Partial<KafkaMessage> = {}): KafkaMessage {
  return { topic: "t", partition: 0, offset: 1, timestamp: 1_700_000_000_000, ...overrides };
}

function mountHost() {
  // attachTo: document.body —— document.activeElement 只对已挂载文档生效
  // （modalBehavior.spec 同款前提）。
  const wrapper = mount(Host, { attachTo: document.body });
  return { wrapper, drawer: (wrapper.vm as unknown as { getDrawer: () => Drawer }).getDrawer() };
}

beforeEach(() => {
  document.body.innerHTML = "";
});

afterEach(() => {
  document.body.innerHTML = "";
});

describe("useMessageDetailDrawer 视图状态", () => {
  it("打开 JSON 消息：探测为 json、视图重置、渲染 pretty JSON、headers 双视图", async () => {
    const { drawer } = mountHost();
    drawer.detail.value = message({
      valueBase64: btoa('{"a":1}'),
      headers: { h1: "v1" },
    });
    await flushPromises();
    expect(drawer.viewFormat.value).toBe("json");
    expect(drawer.viewDecode.value).toBe("none");
    expect(drawer.viewBusy.value).toBe(false);
    expect(drawer.viewResult.value.text).toContain("\n");
    expect(drawer.headersEntries.value).toEqual([["h1", "v1"]]);
    expect(drawer.headersJsonText.value).toContain('"h1"');
  });

  it("XML 探测为 xml；纯文本探测为 raw（无 base64 时渲染空文本属 messageCodec 契约）", async () => {
    const { drawer } = mountHost();
    drawer.detail.value = message({ valueBase64: btoa("<root><a>1</a></root>") });
    await flushPromises();
    expect(drawer.viewFormat.value).toBe("xml");
    expect(drawer.viewResult.value.text).toContain("<root>");

    drawer.detail.value = message({ valueText: "plain text" });
    await flushPromises();
    expect(drawer.viewFormat.value).toBe("raw");
  });

  it("关闭 detail：headers 清空、区块复位、JSON 文本回落空对象", async () => {
    const { drawer } = mountHost();
    drawer.detail.value = message({ valueBase64: btoa('{"a":1}'), headers: { h1: "v1" } });
    await flushPromises();
    drawer.toggleSection("headers");
    expect(drawer.sectionsOpen.value.headers).toBe(false);

    drawer.detail.value = null;
    await flushPromises();
    expect(drawer.sectionsOpen.value).toEqual({ headers: true, value: true });
    expect(drawer.headersEntries.value).toEqual([]);
    expect(drawer.headersJsonText.value).toBe("{}");
    expect(drawer.headersView.value).toBe("table");
  });

  it("手动 renderView：format 切换后按当前视图重渲染", async () => {
    const { drawer } = mountHost();
    drawer.detail.value = message({ valueBase64: btoa('{"a":1}') });
    await flushPromises();
    const pretty = drawer.viewResult.value.text;

    drawer.viewFormat.value = "raw";
    await drawer.renderView();
    expect(drawer.viewResult.value.text).toBe('{"a":1}');
    expect(drawer.viewResult.value.text).not.toBe(pretty);
  });
});

describe("useMessageDetailDrawer 弹层交互", () => {
  it("打开时焦点进抽屉首个控件；Esc 关闭并归还触发元素", async () => {
    const { wrapper, drawer } = mountHost();
    const trigger = wrapper.find(".trigger").element as HTMLElement;
    trigger.focus();
    expect(document.activeElement).toBe(trigger);

    drawer.detail.value = message({ valueBase64: btoa('{"a":1}') });
    await nextTick();
    await nextTick();
    expect(document.activeElement).toBe(wrapper.find(".d1").element);

    window.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" }));
    expect(drawer.detail.value).toBeNull();
    await nextTick();
    await nextTick();
    expect(document.activeElement).toBe(trigger);
  });

  it("Tab 焦点回绕：末尾 Tab 回到首个，反向 Tab 到末个", async () => {
    const { wrapper, drawer } = mountHost();
    drawer.detail.value = message({ valueBase64: btoa('{"a":1}') });
    await nextTick();
    await nextTick();

    const d1 = wrapper.find(".d1").element as HTMLElement;
    const d2 = wrapper.find(".d2").element as HTMLElement;
    d2.focus();
    window.dispatchEvent(new KeyboardEvent("keydown", { key: "Tab" }));
    expect(document.activeElement).toBe(d1);

    d1.focus();
    window.dispatchEvent(new KeyboardEvent("keydown", { key: "Tab", shiftKey: true }));
    expect(document.activeElement).toBe(d2);
  });

  it("更高层弹窗（modal-backdrop）在场时 Esc 让位", async () => {
    const { drawer } = mountHost();
    drawer.detail.value = message({ valueBase64: btoa('{"a":1}') });
    await nextTick();
    await nextTick();

    const backdrop = document.createElement("div");
    backdrop.className = "modal-backdrop";
    document.body.appendChild(backdrop);
    window.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" }));
    expect(drawer.detail.value).not.toBeNull();

    backdrop.remove();
    window.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" }));
    expect(drawer.detail.value).toBeNull();
  });

  it("抽屉未打开时 keydown 直接返回（不拦截全局 Esc）", () => {
    const { drawer } = mountHost();
    expect(() => window.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" }))).not.toThrow();
    expect(drawer.detail.value).toBeNull();
  });
});

// S-B64-PREVIEW-BOUNDED（评审 MED-5）：showFullBase64 的 <pre> 渲染必须有界
// ——此前全量 valueBase64 直接进 DOM，大 payload 可塞数十 MB 文本节点。
describe("useMessageDetailDrawer full base64 preview", () => {
  it("exposes a bounded preview for large payloads", async () => {
    const { wrapper, drawer } = mountHost();
    const bigBase64 = btoa("x".repeat(2_000_000));
    drawer.detail.value = { topic: "t", partition: 0, offset: 1, timestamp: 0, valueBase64: bigBase64 };
    await flushPromises();
    await nextTick();
    drawer.showFullBase64.value = true;
    await nextTick();
    expect(typeof drawer.fullBase64Preview.value.text).toBe("string");
    expect(drawer.fullBase64Preview.value.text.length).toBeLessThanOrEqual(17000);
    expect(drawer.fullBase64Preview.value.truncated).toBe(true);
    wrapper.unmount();
  });
});
