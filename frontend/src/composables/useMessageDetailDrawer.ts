/**
 * 消息详情抽屉状态（自 MessagesPanel.vue 拆出）：detail 打开态、value 本地
 * 二次 decode/format 视图、headers 表格/JSON 切换、区块折叠，以及抽屉
 * Esc 关闭 + Tab 焦点陷阱 + 开关焦点归还的 DOM 接线。
 */
import { computed, nextTick, onBeforeUnmount, onMounted, ref, shallowRef, watch, type Ref } from "vue";
import type { DecodeMode, Decompression } from "../lib/api";
import { formatMessageValue, looksLikeJson, looksLikeXml, messageValueSniffText, type DecodedValue, type ValueFormat } from "../lib/messageCodec";
import { truncatedValuePreview } from "../lib/uiHelpers";
import { decideModalKeydown, focusableElements } from "../lib/modalBehavior";
import type { KafkaMessage } from "../lib/api";

export function useMessageDetailDrawer(detail: Ref<KafkaMessage | null> = shallowRef(null)) {
  // 详情 raw 单份存储：直接引用行内 raw（与 result.messages 同一对象，不拷贝）。

  const viewFormat = ref<ValueFormat>("raw");
  const viewDecode = ref<DecodeMode>("none");
  const viewDecompression = ref<Decompression>("none");
  const viewResult = ref<DecodedValue>({ text: "" });
  const viewBusy = ref(false);
  const showFullBase64 = ref(false);
  // headers 展示形态：表格（key|value+行复制，默认）/ 格式化 JSON。
  const headersView = ref<"table" | "json">("table");
  // Headers / Value 区块折叠态（抽屉内会话级；默认全展开）。
  const sectionsOpen = ref({ headers: true, value: true });
  const headersEntries = computed(() => Object.entries(detail.value?.headers ?? {}));
  // showFullBase64 的渲染文本有界（评审 MED-5）：此前全量 valueBase64 直接
  // 进 <pre>，大 payload 可塞数十 MB 文本节点。
  const fullBase64Preview = computed(() => truncatedValuePreview(detail.value?.valueBase64 ?? detail.value?.valueText ?? ""));
  const headersJsonText = computed(() => JSON.stringify(detail.value?.headers ?? {}, null, 2));

  watch(detail, (message) => {
    showFullBase64.value = false;
    headersView.value = "table";
    sectionsOpen.value = { headers: true, value: true };
    if (!message) return;
    // 嗅探只解码前缀（评审 MED-5）：全量解码留给 renderView 一次完成。
    const text = messageValueSniffText(message).trim();
    viewFormat.value = looksLikeXml(text) ? "xml" : looksLikeJson(text) ? "json" : "raw";
    viewDecode.value = "none";
    viewDecompression.value = "none";
    void renderView();
  });

  function toggleSection(name: "headers" | "value") {
    sectionsOpen.value = { ...sectionsOpen.value, [name]: !sectionsOpen.value[name] };
  }

  // 渲染序号守卫（评审 L）：gzip 路径真实异步（DecompressionStream），快速
  // 切换两条消息时旧消息的慢完成解压结果不得覆盖新消息视图。
  let renderSeq = 0;

  async function renderView() {
    const message = detail.value;
    if (!message) return;
    const seq = ++renderSeq;
    viewBusy.value = true;
    try {
      const result = await formatMessageValue(message, {
        decode: viewDecode.value,
        decompression: viewDecompression.value,
        format: viewFormat.value,
      });
      if (seq !== renderSeq) return;
      viewResult.value = result;
    } finally {
      if (seq === renderSeq) viewBusy.value = false;
    }
  }

  // 编辑器直接承载全量解码/格式化文本（CodeMirror 虚拟渲染，16384 截断预览
  // 退役）——「所见即所复制」：copy-value 复制当前解码/格式化结果文本。

  // -- 弹层交互（P1-2/P1-3）：抽屉 Esc 关闭 + Tab 焦点陷阱 + 关闭归还触发元素 --------
  // 决策逻辑在 modalBehavior.decideModalKeydown（纯函数，有单测），这里只做 DOM 接线。

  const drawerEl = ref<HTMLElement | null>(null);
  let drawerTrigger: HTMLElement | null = null;

  watch(detail, (message, previous) => {
    if (message && !previous) {
      // 打开：记住触发元素，下一帧焦点进抽屉（首个可交互控件，兜底抽屉容器）。
      drawerTrigger = document.activeElement instanceof HTMLElement ? document.activeElement : null;
      void nextTick(() => {
        const drawer = drawerEl.value;
        if (!drawer) return;
        const first = focusableElements(drawer)[0];
        (first ?? drawer).focus({ preventScroll: true });
      });
    } else if (!message && previous) {
      // 关闭（Esc/✕/遮罩）：焦点归还触发元素，遮罩随 v-if 一并卸载、无残留。
      drawerTrigger?.focus({ preventScroll: true });
      drawerTrigger = null;
    }
  });

  function onWindowKeydown(event: KeyboardEvent) {
    if (!detail.value) return;
    // 更高层弹窗（连接弹窗 / teleport 助手弹窗）在场时让位，不抢 Esc/Tab。
    if (document.querySelector(".workbench .modal-backdrop, body > .modal-backdrop")) return;
    const drawer = drawerEl.value;
    if (!drawer) return;
    const focusables = focusableElements(drawer);
    const currentIndex = focusables.indexOf(document.activeElement as HTMLElement);
    const decision = decideModalKeydown(event.key, event.shiftKey, focusables.length, currentIndex);
    if (decision.kind === "close") {
      event.preventDefault();
      event.stopPropagation();
      detail.value = null;
    } else if (decision.kind === "focus") {
      event.preventDefault();
      event.stopPropagation();
      focusables[decision.index]?.focus();
    }
  }

  onMounted(() => window.addEventListener("keydown", onWindowKeydown));
  onBeforeUnmount(() => window.removeEventListener("keydown", onWindowKeydown));

  return {
    detail,
    viewFormat,
    viewDecode,
    viewDecompression,
    viewResult,
    viewBusy,
    showFullBase64,
    fullBase64Preview,
    headersView,
    sectionsOpen,
    headersEntries,
    headersJsonText,
    toggleSection,
    renderView,
    drawerEl,
  };
}
