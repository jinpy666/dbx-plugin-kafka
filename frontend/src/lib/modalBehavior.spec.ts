// @vitest-environment happy-dom
// 弹层行为下沉单测（UI 扫描第 2 轮 P1-5）：Esc 关闭 + 焦点归还 + Tab 陷阱 +
// 层栈语义（后开的子弹层在上，Esc 逐层关闭不透传——导入助手参照语义）。
import { describe, expect, it } from "vitest";
import { defineComponent, h, nextTick, ref } from "vue";
import { mount } from "@vue/test-utils";
import { decideModalKeydown, useModalBehavior } from "./modalBehavior";

type HarnessExposed = {
  openA: ReturnType<typeof ref<boolean>>;
  openB: ReturnType<typeof ref<boolean>>;
  elA: ReturnType<typeof ref<HTMLElement | null>>;
  elB: ReturnType<typeof ref<HTMLElement | null>>;
};

/**
 * 双层弹层试验台：A（主弹层，触发钮 .trigger-a）+ B（子弹层，触发钮 .open-b）。
 * 与真实面板同构：open ref + container ref + close 回调。
 */
function createHarness() {
  return defineComponent({
    name: "ModalHarness",
    setup(): HarnessExposed & { _unused: true } {
      const openA = ref(false);
      const openB = ref(false);
      const elA = ref<HTMLElement | null>(null);
      const elB = ref<HTMLElement | null>(null);
      useModalBehavior({ open: openA, container: elA, close: () => (openA.value = false) });
      useModalBehavior({ open: openB, container: elB, close: () => (openB.value = false) });
      return { openA, openB, elA, elB, _unused: true } as HarnessExposed & { _unused: true };
    },
    render() {
      return h("div", [
        h("button", { class: "trigger-a", onClick: () => (this.openA = true) }, "open-a"),
        this.openA
          ? h(
              "div",
              { class: "modal-a", tabindex: "-1", ref: "elA" },
              [h("button", { class: "a1" }, "a1"), h("button", { class: "a2" }, "a2"), h("button", { class: "open-b", onClick: () => (this.openB = true) }, "open-b")],
            )
          : null,
        this.openB ? h("div", { class: "modal-b", tabindex: "-1", ref: "elB" }, [h("button", { class: "b1" }, "b1")]) : null,
      ]);
    },
  });
}

function press(key: string, init: KeyboardEventInit = {}) {
  const event = new KeyboardEvent("keydown", { key, bubbles: true, cancelable: true, ...init });
  window.dispatchEvent(event);
  return event;
}

async function settle() {
  await nextTick();
  await nextTick();
}

describe("useModalBehavior", () => {
  it("moves focus into the first focusable on open and restores the trigger on close", async () => {
    const wrapper = mount(createHarness(), { attachTo: document.body });
    const trigger = wrapper.find(".trigger-a").element as HTMLElement;
    trigger.focus();
    await trigger.click();
    await settle();
    // 打开：焦点进弹窗首个可交互控件。
    expect(wrapper.vm.openA).toBe(true);
    expect(document.activeElement).toBe(wrapper.find(".a1").element);
    // Esc 关闭 + 焦点归还触发按钮。
    press("Escape");
    await settle();
    expect(wrapper.vm.openA).toBe(false);
    expect(document.activeElement).toBe(trigger);
    wrapper.unmount();
  });

  it("traps Tab inside the modal with wrap-around in both directions", async () => {
    const wrapper = mount(createHarness(), { attachTo: document.body });
    await wrapper.find(".trigger-a").trigger("click");
    await settle();
    const a1 = wrapper.find(".a1").element as HTMLElement;
    const last = wrapper.find(".open-b").element as HTMLElement;
    // 尾部（.open-b）Tab 回绕到首部。
    last.focus();
    const forward = press("Tab");
    expect(forward.defaultPrevented).toBe(true);
    expect(document.activeElement).toBe(a1);
    // 首部 Shift+Tab 回绕到尾部。
    const backward = press("Tab", { shiftKey: true });
    expect(backward.defaultPrevented).toBe(true);
    expect(document.activeElement).toBe(last);
    wrapper.unmount();
  });

  it("ignores unrelated keys and stays closed without interference", async () => {
    const wrapper = mount(createHarness(), { attachTo: document.body });
    press("Enter");
    press("Escape");
    await settle();
    expect(wrapper.vm.openA).toBe(false);
    // 打开后普通按键不关闭、不抢焦点。
    await wrapper.find(".trigger-a").trigger("click");
    await settle();
    const event = press("Enter");
    expect(event.defaultPrevented).toBe(false);
    expect(wrapper.vm.openA).toBe(true);
    wrapper.unmount();
  });

  it("stacks layers: Esc closes only the topmost sublayer, then the base modal", async () => {
    const wrapper = mount(createHarness(), { attachTo: document.body });
    await wrapper.find(".trigger-a").trigger("click");
    await settle();
    // 从主弹层内打开子弹层（导入助手同构）。
    await wrapper.find(".open-b").trigger("click");
    await settle();
    expect(wrapper.vm.openA).toBe(true);
    expect(wrapper.vm.openB).toBe(true);
    expect(document.activeElement).toBe(wrapper.find(".b1").element);
    // 第一层 Esc：只关子弹层，主弹层保持。
    press("Escape");
    await settle();
    expect(wrapper.vm.openB).toBe(false);
    expect(wrapper.vm.openA).toBe(true);
    // 第二层 Esc：关主弹层。
    press("Escape");
    await settle();
    expect(wrapper.vm.openA).toBe(false);
    wrapper.unmount();
  });

  it("yields Esc/Tab handling to the topmost layer while a sublayer is open", async () => {
    const wrapper = mount(createHarness(), { attachTo: document.body });
    await wrapper.find(".trigger-a").trigger("click");
    await settle();
    await wrapper.find(".open-b").trigger("click");
    await settle();
    // Tab 陷阱圈定子弹层（B 只有一个控件，Shift+Tab 仍留在 b1）。
    press("Tab", { shiftKey: true });
    expect(document.activeElement).toBe(wrapper.find(".b1").element);
    // 主弹层控件不参与回绕（焦点未跳去 .a1）。
    expect(document.activeElement).not.toBe(wrapper.find(".a1").element);
    wrapper.unmount();
  });

  // 第 5 轮走查：恢复态初始即开（开合记忆）也要响应 Esc——补层栈但不抢焦点。
  it("registers an already-open layer on mount without stealing focus (round5)", async () => {
    const wrapper = mount(
      defineComponent({
        setup(): HarnessExposed & { _unused: true } {
          const openA = ref(true);
          const elA = ref<HTMLElement | null>(null);
          useModalBehavior({ open: openA, container: elA, close: () => (openA.value = false), registerIfOpenOnMount: true });
          return { openA, elA, _unused: true } as HarnessExposed & { _unused: true };
        },
        render() {
          return h("div", [
            h("button", { class: "outside" }, "outside"),
            this.openA ? h("div", { class: "modal-a", tabindex: "-1", ref: "elA" }, [h("button", { class: "a1" }, "a1")]) : null,
          ]);
        },
      }),
      { attachTo: document.body },
    );
    await settle();
    // 焦点保持原地（body），未被弹层抢走。
    expect(document.activeElement).not.toBe(wrapper.find(".a1").element);
    press("Escape");
    await settle();
    expect(wrapper.vm.openA).toBe(false);
    wrapper.unmount();
  });
});

// 纯决策函数单测（自 kafkaModel.spec 迁入）。
describe("modal keydown decision (Esc close + Tab focus trap)", () => {
  it("closes on Escape regardless of Tab state", () => {
    expect(decideModalKeydown("Escape", false, 0, -1)).toEqual({ kind: "close" });
    expect(decideModalKeydown("Escape", true, 5, 2)).toEqual({ kind: "close" });
  });

  it("ignores non-Esc/Tab keys and empty containers", () => {
    expect(decideModalKeydown("Enter", false, 5, 0)).toEqual({ kind: "none" });
    expect(decideModalKeydown("Tab", false, 0, -1)).toEqual({ kind: "none" });
    expect(decideModalKeydown("Tab", true, 0, 3)).toEqual({ kind: "none" });
  });

  it("cycles forward with wrap-around", () => {
    expect(decideModalKeydown("Tab", false, 3, 0)).toEqual({ kind: "focus", index: 1 });
    expect(decideModalKeydown("Tab", false, 3, 2)).toEqual({ kind: "focus", index: 0 });
  });

  it("cycles backward with wrap-around", () => {
    expect(decideModalKeydown("Tab", true, 3, 2)).toEqual({ kind: "focus", index: 1 });
    expect(decideModalKeydown("Tab", true, 3, 0)).toEqual({ kind: "focus", index: 2 });
  });

  it("enters at the start/end when focus is outside the container", () => {
    // 焦点尚未进容器（如打开瞬间）：Tab 进首个控件，Shift+Tab 进最后一个。
    expect(decideModalKeydown("Tab", false, 4, -1)).toEqual({ kind: "focus", index: 0 });
    expect(decideModalKeydown("Tab", true, 4, -1)).toEqual({ kind: "focus", index: 3 });
    // 越界（焦点被容器外逻辑移走）同样按方向兜底。
    expect(decideModalKeydown("Tab", false, 4, 99)).toEqual({ kind: "focus", index: 0 });
    expect(decideModalKeydown("Tab", true, 4, 99)).toEqual({ kind: "focus", index: 3 });
  });
});
