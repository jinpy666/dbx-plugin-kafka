// @vitest-environment happy-dom
// MessagesPanel 组件测试（UI 扫描第 4 轮防回归）：
// P1-6 offset 范围输入（number v-model → optionalNumber String 归一）真正发请求；
// P1-7 慢响应竞态——在途切换 topic 后旧响应丢弃、新 topic 可立即重发且结果落地；
// P2-21 空态两态——未选 topic 与已选 topic 的文案区分。
import { beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import { defineComponent, h, type PropType } from "vue";
import MessagesPanel from "./MessagesPanel.vue";
import { setKafkaConnectionId, type ConsumeResult, type KafkaMessage } from "../lib/api";
import { formatTimestamp } from "../lib/timestamps";
import { setWorkbenchTimestampTz, TIMESTAMP_TZ_STORAGE_KEY } from "../lib/kafkaColumns";
import { pluginStore } from "../lib/pluginStore";
import { t } from "../lib/i18n";

// -- DbxAgGrid 轻量 stub（镜像真实桥形状，见 GroupsPanel.spec 同款） -----------------
let goToLatestCalls = 0;
const DbxAgGridStub = defineComponent({
  name: "DbxAgGridStub",
  props: {
    rowData: { type: Array as PropType<unknown[]>, default: () => [] },
    tableKey: { type: String, default: "" },
    rowSelection: { type: [String, Boolean] as PropType<"single" | false>, default: "single" as const },
    emitRowClick: { type: Boolean, default: true },
    quickFilter: { type: String, default: "" },
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
        { class: "grid-stub", "data-key": props.tableKey, "data-quick-filter": props.quickFilter },
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
            // 时间戳列文案进 stub 行文本（tz 切换断言用；VM 由 toMessageRows 产出）。
            `${props.tableKey}-row-${index}${(row as { timestampText?: string }).timestampText ? ` ts:${(row as { timestampText?: string }).timestampText}` : ""}`,
          ),
        ),
      );
  },
});

const invokeMock = vi.fn();

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

function mountPanel(props: Record<string, unknown> = {}) {
  return mount(MessagesPanel, {
    props: { topic: "order-events", canWrite: true, ...props },
    global: {
      stubs: {
        DbxAgGrid: DbxAgGridStub,
        teleport: true,
        // CodeEditor（CodeMirror）在 happy-dom 下无法布局，stub 渲染只读文本。
        CodeEditor: defineComponent({
          name: "CodeEditorStub",
          props: { modelValue: { type: String, default: "" }, language: { type: String, default: "text" } },
          setup(props) {
            return () => h("pre", { class: "code-editor-stub", "data-language": props.language }, props.modelValue);
          },
        }),
      },
    },
  });
}

function consumeResult(topic: string, offset: number): ConsumeResult {
  const message: KafkaMessage = { topic, partition: 0, offset, timestamp: 1_700_000_000_000, valueText: `{"v":"${topic}-${offset}"}` };
  return { messages: [message], scanned: 3, matched: 3, limited: false, hasMore: false };
}

beforeEach(() => {
  localStorage.clear();
  setKafkaConnectionId("conn-test");
  goToLatestCalls = 0;
});

describe("MessagesPanel", () => {
  // P1-6：<input type="number"> 的 v-model 赋 number，optionalNumber 直接 .trim()
  // 曾抛 TypeError（「value.trim is not a function」）导致消费请求根本不发出。
  it("consumes with an offset range and forwards offsetFrom/offsetTo as numbers (P1-6)", async () => {
    installBridge({
      "kafka/presets/list": { presets: [] },
      "kafka/messages/consume": { messages: [], scanned: 0, matched: 0, limited: false, hasMore: false },
    });
    const wrapper = mountPanel();
    await flushPromises();
    const numberInputs = wrapper.findAll('input[type="number"]');
    // 基础组 limit/timeout/maxScanRecords + 时间与范围组 offsetFrom/offsetTo
    expect(numberInputs).toHaveLength(5);
    await numberInputs[3].setValue(1);
    await numberInputs[4].setValue(2);
    await wrapper.find(".form-footer .primary-button").trigger("click");
    await flushPromises();
    const consumeCall = invokeMock.mock.calls.find(([method]) => method === "kafka/messages/consume");
    expect(consumeCall?.[1]).toMatchObject({ topic: "order-events", offsetFrom: 1, offsetTo: 2 });
    // 不再有内部异常透传到错误横幅（error 事件仅允许出现清屏用的空串）
    expect((wrapper.emitted("error") ?? []).every(([message]) => message === "")).toBe(true);
  });

  it("consumes with only offsetTo filled (partial range also sends the request)", async () => {
    installBridge({
      "kafka/presets/list": { presets: [] },
      "kafka/messages/consume": { messages: [], scanned: 0, matched: 0, limited: false, hasMore: false },
    });
    const wrapper = mountPanel();
    await flushPromises();
    const numberInputs = wrapper.findAll('input[type="number"]');
    await numberInputs[4].setValue(2);
    await wrapper.find(".form-footer .primary-button").trigger("click");
    await flushPromises();
    const consumeCall = invokeMock.mock.calls.find(([method]) => method === "kafka/messages/consume");
    expect(consumeCall?.[1]).toMatchObject({ offsetTo: 2 });
    expect(consumeCall?.[1]).not.toHaveProperty("offsetFrom");
    expect((wrapper.emitted("error") ?? []).every(([message]) => message === "")).toBe(true);
  });

  // P1-7：在途消费切换 topic 后，晚到的旧 topic 响应必须丢弃（不串台），且新
  // topic 立即可重新消费（consuming 复位），新响应正常落地。
  it("drops a stale consume response when the topic changes mid-flight (P1-7)", async () => {
    let resolveConsume!: (value: ConsumeResult) => void;
    invokeMock.mockReset();
    invokeMock.mockImplementation(async (method: string) => {
      if (method === "kafka/presets/list") return { presets: [] };
      if (method === "kafka/messages/consume") {
        return new Promise<ConsumeResult>((resolve) => {
          resolveConsume = resolve;
        });
      }
      throw new Error(`unhandled method: ${method}`);
    });
    (window as unknown as { dbxPlugin: unknown }).dbxPlugin = { invoke: invokeMock };

    const wrapper = mountPanel({ topic: "order-events" });
    await flushPromises();
    await wrapper.find(".form-footer .primary-button").trigger("click");
    // 在途切换 topic：旧结果清空、consuming 复位、请求序号自增
    await wrapper.setProps({ topic: "payment-gateway" });
    await flushPromises();
    // 晚到的 order-events 响应落地 → 必须被丢弃，不呈现在 payment-gateway 名下
    resolveConsume(consumeResult("order-events", 1));
    await flushPromises();
    expect(wrapper.find(".result-meta").exists()).toBe(false);
    expect(wrapper.find(".grid-stub").exists()).toBe(false);
    // 新 topic 立即可重新消费，且新响应正常落地
    await wrapper.find(".form-footer .primary-button").trigger("click");
    resolveConsume(consumeResult("payment-gateway", 7));
    await flushPromises();
    expect(wrapper.find(".result-meta").exists()).toBe(true);
    expect(wrapper.text()).toContain(t("messages.matched", { count: 3 }));
    const rows = wrapper.findAll(".grid-stub .grid-stub-row");
    expect(rows).toHaveLength(1);
    // 详情抽屉证实落地的是 payment-gateway 的消息（topic/offset 对得上，非串台）
    await rows[0]?.trigger("click");
    await flushPromises();
    expect(wrapper.find(".drawer").text()).toContain("payment-gateway");
    expect(wrapper.find(".drawer").text()).toContain("7");
    expect((wrapper.emitted("error") ?? []).every(([message]) => message === "")).toBe(true);
  });

  // P2-21：未选 topic 时引导先在左侧树选择；已选 topic 尚未消费引导点消费
  // （「调整条件重新消费」文案只属于 0 条命中，见 result 非空分支）。
  it("distinguishes the no-topic empty state from the not-consumed empty state (P2-21)", async () => {
    installBridge({ "kafka/presets/list": { presets: [] } });
    const wrapper = mountPanel({ topic: "" });
    await flushPromises();
    expect(wrapper.find(".empty").text()).toBe(t("messages.uiNoTopicSelected"));
    await wrapper.setProps({ topic: "order-events" });
    expect(wrapper.find(".empty").text()).toBe(t("messages.pendingConsume"));
  });

  // round4 面 1：消费在途给出进行中反馈（不再整块空白）——deferred consume 挂起
  // 期间面板显示 messages.running 文案，响应落地后由结果区接管。
  it("shows an in-flight consuming state instead of a blank pane (round4)", async () => {
    let resolveConsume!: (value: ConsumeResult) => void;
    invokeMock.mockReset();
    invokeMock.mockImplementation(async (method: string) => {
      if (method === "kafka/presets/list") return { presets: [] };
      if (method === "kafka/messages/consume") {
        return new Promise<ConsumeResult>((resolve) => {
          resolveConsume = resolve;
        });
      }
      throw new Error(`unhandled method: ${method}`);
    });
    (window as unknown as { dbxPlugin: unknown }).dbxPlugin = { invoke: invokeMock };
    const wrapper = mountPanel({ topic: "order-events" });
    await flushPromises();
    await wrapper.find(".form-footer .primary-button").trigger("click");
    expect(wrapper.find(".empty").text()).toBe(t("messages.running"));
    resolveConsume(consumeResult("order-events", 1));
    await flushPromises();
    expect(wrapper.find(".result-meta").exists()).toBe(true);
  });

  // F6-1：即时搜索输入 → 150ms 防抖后喂给 grid 的 quickFilterText。
  it("debounces the quick filter input into the grid quickFilter (F6-1)", async () => {
    vi.useFakeTimers();
    try {
      installBridge({
        "kafka/presets/list": { presets: [] },
        "kafka/messages/consume": consumeResult("order-events", 1),
      });
      const wrapper = mountPanel();
      await flushPromises();
      await wrapper.find(".form-footer .primary-button").trigger("click");
      await flushPromises();
      const input = wrapper.find('[data-testid="quick-filter"]');
      expect(input.exists()).toBe(true);
      expect(input.attributes("placeholder")).toBe(t("messages.quickFilterPlaceholder"));
      await input.setValue("order");
      await input.setValue("order-a");
      // 未到防抖窗口：尚未下传。
      vi.advanceTimersByTime(100);
      expect(wrapper.find(".grid-stub").attributes("data-quick-filter")).toBe("");
      await vi.advanceTimersByTimeAsync(60);
      expect(wrapper.find(".grid-stub").attributes("data-quick-filter")).toBe("order-a");
    } finally {
      vi.useRealTimers();
    }
  });

  // F6-3：本地/UTC toggle → pluginStore `kafka.ts.tz` 持久化 + 行时间戳按 UTC 渲染。
  it("persists the timestamp timezone toggle and renders UTC cells (F6-3)", async () => {
    installBridge({
      "kafka/presets/list": { presets: [] },
      "kafka/messages/consume": consumeResult("order-events", 1),
    });
    const wrapper = mountPanel();
    await flushPromises();
    await wrapper.find(".form-footer .primary-button").trigger("click");
    await flushPromises();
    const toggle = wrapper.find('[data-testid="tz-toggle"]');
    expect(toggle.text()).toBe(t("messages.tzLocal"));
    // 固定向量：1700000000000 → UTC 2023-11-14 22:13:20（本地时区逐分量断言）。
    const rowBefore = wrapper.find(".grid-stub-row").text();
    expect(rowBefore).toContain(`ts:${formatTimestamp(1_700_000_000_000, "local")}`);
    await toggle.trigger("click");
    await flushPromises();
    expect(pluginStore.getItem(TIMESTAMP_TZ_STORAGE_KEY)).toBe("utc");
    expect(wrapper.find('[data-testid="tz-toggle"]').text()).toBe("UTC");
    expect(wrapper.find(".grid-stub-row").text()).toContain("ts:2023-11-14 22:13:20");
    // 重挂载读取持久化偏好。
    const remounted = mountPanel();
    await flushPromises();
    // 首次消费后表单已收起为摘要条（开合记忆持久化）——重挂载走摘要条的重跑按钮。
    const rerun = remounted.find('[data-testid="consume-run"]');
    expect(rerun.exists()).toBe(true);
    await rerun.trigger("click");
    await flushPromises();
    expect(remounted.find('[data-testid="tz-toggle"]').text()).toBe("UTC");
    expect(remounted.find(".grid-stub-row").text()).toContain("ts:2023-11-14 22:13:20");
    setWorkbenchTimestampTz("local");
  });

  // F6-2：详情抽屉复制四按钮（key/value/headers/整条 JSON）+ 行操作「复制 JSON」。
  it("copies key/value/headers/message JSON from the drawer and rows (F6-2)", async () => {
    installBridge({
      "kafka/presets/list": { presets: [] },
      "kafka/messages/consume": {
        messages: [
          {
            topic: "order-events",
            partition: 0,
            offset: 1,
            timestamp: 1_700_000_000_000,
            key: "k-1",
            valueText: '{"v":"body"}',
            valueBase64: btoa('{"v":"body"}'),
            headers: { trace: "t-9" },
          },
        ],
        scanned: 1,
        matched: 1,
        limited: false,
        hasMore: false,
      },
    });
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", { value: { writeText }, configurable: true });
    const wrapper = mountPanel();
    await flushPromises();
    await wrapper.find(".form-footer .primary-button").trigger("click");
    await flushPromises();
    // 行点击打开详情抽屉（行操作「复制 JSON」列的渲染面由 kafkaColumns.spec 覆盖）。
    await wrapper.find(".grid-stub-row").trigger("click");
    await flushPromises();
    await wrapper.find('[data-testid="copy-key"]').trigger("click");
    await wrapper.find('[data-testid="copy-value"]').trigger("click");
    await wrapper.find('[data-testid="copy-headers"]').trigger("click");
    await wrapper.find('[data-testid="copy-json"]').trigger("click");
    await flushPromises();
    expect(writeText).toHaveBeenCalledTimes(4);
    const [keyArg, valueArg, headersArg, jsonArg] = writeText.mock.calls.map((call) => call[0] as string);
    expect(keyArg).toBe("k-1");
    expect(valueArg).toBe('{"v":"body"}');
    expect(JSON.parse(headersArg)).toEqual({ trace: "t-9" });
    // serializeMessagesToJson 输出 JSON 数组（单元素）。
    const parsed = (JSON.parse(jsonArg) as Array<Record<string, unknown>>)[0];
    expect(parsed.topic).toBe("order-events");
    expect(parsed.value).toBe('{"v":"body"}');
    // 通知走既有「已复制」文案。
    expect((wrapper.emitted("notify") ?? []).flat()).toContain(t("copied"));
  });

  // 详情体验 v2：headers 表格（行级复制）⇄ JSON 切换；value 走只读编辑器
  //（json 高亮态随 format 选择），完整 base64 视图保留。
  it("renders headers as a copyable table and switches to JSON view", async () => {
    installBridge({
      "kafka/presets/list": { presets: [] },
      "kafka/messages/consume": {
        messages: [
          {
            topic: "order-events",
            partition: 0,
            offset: 1,
            timestamp: 1_700_000_000_000,
            key: "k-1",
            valueText: '{"v":"body"}',
            headers: { trace: "t-9", env: "int" },
          },
        ],
        scanned: 1,
        matched: 1,
        hasMore: false,
      },
    });
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", { value: { writeText }, configurable: true });
    const wrapper = mountPanel();
    await flushPromises();
    await wrapper.find(".form-footer .primary-button").trigger("click");
    await flushPromises();
    await wrapper.find(".grid-stub-row").trigger("click");
    await flushPromises();

    // 表格视图（默认）：2 行 key|value + 行内复制按钮（复制该行值）。
    const table = wrapper.find(".kv-table");
    expect(table.exists()).toBe(true);
    expect(table.findAll("tbody tr")).toHaveLength(2);
    expect(table.text()).toContain("trace");
    expect(table.text()).toContain("t-9");
    const rowCopy = table.findAll("tbody tr .icon-button");
    await rowCopy[0].trigger("click");
    await flushPromises();
    expect(writeText).toHaveBeenCalledWith("t-9");

    // 切 JSON：格式化文本（键与值都在）。
    await wrapper.find('.detail-block__actions .seg-toggle:nth-child(2)').trigger("click");
    const jsonView = wrapper.find(".value-view--headers");
    expect(jsonView.exists()).toBe(true);
    expect(jsonView.text()).toContain('"trace": "t-9"');
    expect(jsonView.text()).toContain('"env": "int"');

    // 无 headers 消息：空态文案（有结果后表单收起为摘要条，用条内「重新消费」）。
    installBridge({
      "kafka/presets/list": { presets: [] },
      "kafka/messages/consume": {
        messages: [{ topic: "order-events", partition: 0, offset: 2, timestamp: 1, key: "k-2", valueText: "x" }],
        scanned: 1,
        matched: 1,
        hasMore: false,
      },
    });
    await wrapper.find('[data-testid="consume-run"]').trigger("click");
    await flushPromises();
    await wrapper.find(".grid-stub-row").trigger("click");
    await flushPromises();
    expect(wrapper.find(".detail-headers-empty").text()).toBe(t("messages.headersEmpty"));
  });

  it("shows the value in a read-only editor with JSON language and keeps raw base64 toggle", async () => {
    installBridge({
      "kafka/presets/list": { presets: [] },
      "kafka/messages/consume": {
        messages: [
          {
            topic: "order-events",
            partition: 0,
            offset: 1,
            timestamp: 1,
            key: "k-1",
            valueText: '{"v":"body"}',
            valueBase64: btoa('{"v":"body"}'),
          },
        ],
        scanned: 1,
        matched: 1,
        hasMore: false,
      },
    });
    const wrapper = mountPanel();
    await flushPromises();
    await wrapper.find(".form-footer .primary-button").trigger("click");
    await flushPromises();
    await wrapper.find(".grid-stub-row").trigger("click");
    await flushPromises();

    // looksJson → format=json：编辑器 language=json，内容是格式化后的文本。
    const editor = wrapper.find(".code-editor-stub");
    expect(editor.attributes("data-language")).toBe("json");
    expect(editor.text()).toContain('"v"');

    // fullValue（完整 base64）切换：编辑器让位给 base64 文本视图。
    await wrapper.find('.detail-block__actions .seg-toggle').trigger("click");
    const base64View = wrapper.find(".value-view:not(.value-view--headers)");
    expect(base64View.exists()).toBe(true);
    expect(base64View.text()).toBe(btoa('{"v":"body"}'));
  });
});

// -- MCP UI intent（M3）：applyIntentConsume / applyIntentSelect ----------------

describe("MessagesPanel MCP intent (M3)", () => {
  const intentResult: ConsumeResult = {
    messages: [
      { topic: "order-events", partition: 0, offset: 42, timestamp: 1_700_000_000_000, key: "k-42", valueText: '{"v":"a"}' },
      { topic: "order-events", partition: 1, offset: 7, timestamp: 1_700_000_000_001, key: "k-7", valueText: '{"v":"b"}' },
    ],
    scanned: 10,
    matched: 2,
    limited: false,
    hasMore: false,
  };

  it("applyIntentConsume fills the form, consumes and returns an applied summary with anchors", async () => {
    installBridge({
      "kafka/presets/list": { presets: [] },
      "kafka/messages/consume": intentResult,
    });
    const wrapper = mountPanel();
    await flushPromises();
    const panel = wrapper.vm as unknown as { applyIntentConsume(params: Record<string, unknown>): Promise<{ status: string; summary?: Record<string, unknown>; reason?: string }> };
    const outcome = await panel.applyIntentConsume({
      topic: "order-events",
      offsetStrategy: "earliest",
      limit: 50,
      valueFilter: "a",
      matchMode: "contains",
    });
    expect(outcome.status).toBe("applied");
    expect(outcome.summary?.count).toBe(2);
    expect(outcome.summary?.anchor).toBe("order-events-p0-o42");
    // rows ≤5、含 partition/offset 定位字段。
    expect((outcome.summary?.rows as unknown[]).length).toBe(2);
    const call = invokeMock.mock.calls.find(([method]) => method === "kafka/messages/consume");
    expect(call?.[1]).toMatchObject({ topic: "order-events", offsetStrategy: "earliest", limit: 50, valueFilter: "a", matchMode: "contains" });
  });

  it("applyIntentConsume reports rejected with the sidecar error message", async () => {
    installBridge({
      "kafka/presets/list": { presets: [] },
      "kafka/messages/consume": { error: { message: "kafka cluster not ready" } },
    });
    const wrapper = mountPanel();
    await flushPromises();
    const panel = wrapper.vm as unknown as { applyIntentConsume(params: Record<string, unknown>): Promise<{ status: string; reason?: string }> };
    const outcome = await panel.applyIntentConsume({ topic: "order-events" });
    expect(outcome.status).toBe("rejected");
    expect(outcome.reason).toBe("kafka cluster not ready");
  });

  it("applyIntentSelect locates partition+offset in current results and opens the detail drawer", async () => {
    installBridge({
      "kafka/presets/list": { presets: [] },
      "kafka/messages/consume": intentResult,
    });
    const wrapper = mountPanel();
    await flushPromises();
    const panel = wrapper.vm as unknown as {
      applyIntentConsume(params: Record<string, unknown>): Promise<{ status: string }>;
      applyIntentSelect(params: Record<string, unknown>): Promise<{ status: string; summary?: { anchor?: string }; reason?: string }>;
    };
    await panel.applyIntentConsume({ topic: "order-events" });
    await flushPromises();
    const hit = await panel.applyIntentSelect({ partition: 1, offset: 7 });
    expect(hit.status).toBe("applied");
    expect(hit.summary?.anchor).toBe("order-events-p1-o7");
    // 详情抽屉打开（detail drawer 渲染）。
    expect(wrapper.find(".detail-drawer, [role=dialog], .drawer").exists()).toBe(true);

    const miss = await panel.applyIntentSelect({ partition: 9, offset: 9 });
    expect(miss.status).toBe("rejected");
    expect(miss.reason).toContain("partition+offset");
  });
});

// -- Lane4 前端打磨：TSV 导出（纯前端序列化，不发 export 请求） ----------------

describe("MessagesPanel TSV export (Lane4)", () => {
  it("exports the loaded result as TSV client-side with tab/newline escaping and no export request", async () => {
    installBridge({
      "kafka/presets/list": { presets: [] },
      "kafka/messages/consume": {
        messages: [
          { topic: "order-events", partition: 0, offset: 7, timestamp: 1_700_000_000_000, key: "k\t1", valueText: "line1\nline2", headers: { trace: "abc" } },
        ],
        scanned: 3,
        matched: 1,
        limited: false,
        hasMore: false,
      },
    });
    const createObjectURL = vi.spyOn(URL, "createObjectURL").mockReturnValue("blob:mock");
    const revokeObjectURL = vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => {});
    const anchorClick = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => {});
    const wrapper = mountPanel();
    await flushPromises();
    await wrapper.find('[data-testid="consume-run"]').trigger("click");
    await flushPromises();
    const tsvButton = wrapper.find('[data-testid="export-tsv"]');
    expect(tsvButton.exists()).toBe(true);
    expect(tsvButton.attributes("disabled")).toBeUndefined();
    await tsvButton.trigger("click");
    await flushPromises();
    // 纯前端导出：不触发 kafka/messages/export 桥调用
    expect(invokeMock.mock.calls.filter(([method]) => method === "kafka/messages/export")).toHaveLength(0);
    expect(createObjectURL).toHaveBeenCalledTimes(1);
    const blob = createObjectURL.mock.calls[0]![0] as Blob;
    expect(blob.type).toBe("text/tab-separated-values;charset=utf-8");
    const [header, row] = (await blob.text()).split("\r\n");
    expect(header).toBe("topic\tpartition\toffset\ttimestamp\tkey\tvalue\theaders");
    expect(row).toBe("order-events\t0\t7\t1700000000000\tk\\t1\tline1\\nline2\ttrace=abc");
    expect(anchorClick).toHaveBeenCalledTimes(1);
    // 10s 延迟回收：flushPromises 不推进 timer，revoke 未发生
    expect(revokeObjectURL).not.toHaveBeenCalled();
    expect(wrapper.emitted("notify")?.at(-1)).toEqual([t("messages.exportDone", { name: "TSV" })]);
  });

  it("keeps the TSV button disabled without results", async () => {
    installBridge({
      "kafka/presets/list": { presets: [] },
      "kafka/messages/consume": { messages: [], scanned: 0, matched: 0, limited: false, hasMore: false },
    });
    const wrapper = mountPanel();
    await flushPromises();
    // 无结果时结果区未渲染，导出按钮不存在（与 JSON/CSV 同容器）
    expect(wrapper.find('[data-testid="export-tsv"]').exists()).toBe(false);
    await wrapper.find('[data-testid="consume-run"]').trigger("click");
    await flushPromises();
    expect(wrapper.find('[data-testid="export-tsv"]').exists()).toBe(true);
  });
});
