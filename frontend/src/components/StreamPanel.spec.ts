// @vitest-environment happy-dom
// StreamPanel 组件测试：start/stop 调桥 / 暂停-恢复徽标切换 / stream messages
// 事件按 sessionId 追加（他 session 丢弃）/ stream error 事件友好归一 /
// quickFilter 防抖透传 DbxAgGrid（不发请求）/ 环形缓冲 Older/Newer 分页按钮
// 禁用态与 offset clamp / 无 topic 时 Start 禁用。
// （流纯函数 appendStreamRows 已在 kafkaModel.spec，不重复。）
// DbxAgGrid mock 为桩组件：渲染行 valueText 便于断言落表内容，goToLatest 为
// 空实现（autoScroll 落表调用），quickFilter 以 data 属性透出供断言。
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount, type VueWrapper } from "@vue/test-utils";
import StreamPanel from "./StreamPanel.vue";
import { setKafkaConnectionId, type KafkaMessage, type KafkaStreamErrorEvent, type KafkaStreamMessagesEvent } from "../lib/api";
import { friendlyKafkaError } from "../lib/kafkaErrors";
import { t } from "../lib/i18n";

vi.mock("./DbxAgGrid.vue", () => ({
  default: {
    name: "DbxAgGridStub",
    props: ["rowData", "columnDefs", "tableKey", "compactFields", "quickFilter", "emitRowClick", "rowSelection"],
    emits: ["rowClick", "selectionChanged", "pageSizeChanged"],
    methods: {
      goToLatest() {},
    },
    template: `<div class="dbx-grid-stub" :data-filter="quickFilter ?? ''">
      <span v-for="row in rowData" :key="row.id" class="stub-row">{{ row.valueText }}</span>
    </div>`,
  },
}));

const invokeMock = vi.fn();

/** method → 响应 路由表；未命中抛错（对应未注册方法）；`{ error }` 信封按真实桥
 *  形态以异常拒绝（JSON-RPC error → invoke rejection → api 层转译为 Error）；
 *  响应可为函数（(params) => result，供动态分页响应）。 */
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

function message(offset: number, valueText: string): KafkaMessage {
  return { topic: "order-events", partition: 0, offset, timestamp: 1_700_000_000_000, key: `k-${offset}`, valueText };
}

function mountPanel(props: Record<string, unknown> = {}) {
  const wrapper = mount(StreamPanel, {
    props: { topic: "order-events", canWrite: true, ...props },
  });
  mounted.push(wrapper);
  return wrapper;
}

const mounted: Array<VueWrapper<InstanceType<typeof StreamPanel>>> = [];

/** defineExpose 的 pushEvent（App.vue handleEvent 转发入口）。 */
function pushEvent(wrapper: VueWrapper<InstanceType<typeof StreamPanel>>, event: KafkaStreamMessagesEvent | KafkaStreamErrorEvent) {
  return (wrapper.vm as unknown as { pushEvent: (event: KafkaStreamMessagesEvent | KafkaStreamErrorEvent) => void }).pushEvent(event);
}

async function startSession(wrapper: VueWrapper<InstanceType<typeof StreamPanel>>) {
  await wrapper.find(".primary-button.compact").trigger("click");
  await flushPromises();
}

const stubRows = (wrapper: VueWrapper<InstanceType<typeof StreamPanel>>) => wrapper.findAll(".dbx-grid-stub .stub-row");

beforeEach(() => {
  localStorage.clear();
  setKafkaConnectionId("conn-test");
  mounted.length = 0;
});

afterEach(() => {
  for (const wrapper of mounted.splice(0)) wrapper.unmount();
  vi.useRealTimers();
});

describe("StreamPanel", () => {
  it("starts a session through the bridge and flips to the running state", async () => {
    installBridge({ "kafka/stream/start": { sessionId: "stream-1" } });
    const wrapper = mountPanel();
    await flushPromises();
    // 无会话：分页按钮禁用
    for (const pager of wrapper.findAll(".stream-pager")) {
      expect(pager.attributes("disabled")).toBeDefined();
    }
    // 过滤条件 + limit
    const textInputs = wrapper.findAll('.kafka-form input[type="text"]');
    await textInputs[1].setValue("pay"); // 0 = topic(readonly)，1 = filter
    await wrapper.find(".kafka-form select").setValue("prefix");
    await wrapper.find('.kafka-form input[type="number"]').setValue("50");
    await startSession(wrapper);
    expect(invokeMock.mock.calls.find(([method]) => method === "kafka/stream/start")?.[1]).toMatchObject({
      topic: "order-events",
      offsetStrategy: "latest",
      limit: 50,
      filter: "pay",
      matchMode: "prefix",
    });
    expect(wrapper.emitted("notify")?.at(-1)).toEqual([`${t("stream.title")}: stream-1`]);
    // 运行态徽标 + sessionId + 会话按钮切换
    const badge = wrapper.find(".stream-meta .badge");
    expect(badge.text()).toBe(t("stream.stateRunning"));
    expect(badge.classes()).toContain("badge-ok");
    expect(wrapper.find(".stream-meta .mono-s").text()).toBe("stream-1");
    expect(wrapper.find(".toolbar-button").text()).toContain(t("stream.pause"));
    // 会话激活后分页按钮可用
    for (const pager of wrapper.findAll(".stream-pager")) {
      expect(pager.attributes("disabled")).toBeUndefined();
    }
  });

  it("pauses and resumes with the badge swapping states", async () => {
    installBridge({
      "kafka/stream/start": { sessionId: "stream-1" },
      "kafka/stream/pause": { status: { paused: true, totalScanned: 9, totalMatched: 9, bufferSize: 9 } },
      "kafka/stream/resume": { status: { paused: false, totalScanned: 12, totalMatched: 12, bufferSize: 12 } },
    });
    const wrapper = mountPanel();
    await flushPromises();
    await startSession(wrapper);
    const toggle = () => wrapper.find(".toolbar-button");
    await toggle().trigger("click");
    await flushPromises();
    expect(invokeMock.mock.calls.find(([method]) => method === "kafka/stream/pause")?.[1]).toMatchObject({ sessionId: "stream-1" });
    let badge = wrapper.find(".stream-meta .badge");
    expect(badge.text()).toBe(t("stream.statePaused"));
    expect(badge.classes()).toContain("badge-warn");
    expect(wrapper.text()).toContain(t("stream.scanned", { count: 9 }));
    expect(toggle().text()).toContain(t("stream.resume"));
    await toggle().trigger("click");
    await flushPromises();
    expect(invokeMock.mock.calls.find(([method]) => method === "kafka/stream/resume")).toBeTruthy();
    badge = wrapper.find(".stream-meta .badge");
    expect(badge.text()).toBe(t("stream.stateRunning"));
    expect(badge.classes()).toContain("badge-ok");
  });

  it("stops the session via the bridge and returns to idle", async () => {
    installBridge({ "kafka/stream/start": { sessionId: "stream-1" }, "kafka/stream/stop": { success: true } });
    const wrapper = mountPanel();
    await flushPromises();
    await startSession(wrapper);
    await wrapper.find(".danger-button.compact").trigger("click");
    await flushPromises();
    expect(invokeMock.mock.calls.find(([method]) => method === "kafka/stream/stop")?.[1]).toMatchObject({ sessionId: "stream-1" });
    expect(wrapper.find(".stream-meta .badge").text()).toBe(t("stream.stateIdle"));
    expect(wrapper.find(".stream-meta .mono-s").text()).toBe("—");
    // 按钮组回到 Start（toolbar-button 断言收敛到表单区：stream-meta 另有
    // Lane4 导出 JSON/CSV/TSV 按钮，不随会话结束消失）
    expect(wrapper.find(".primary-button.compact").text()).toContain(t("stream.start"));
    expect(wrapper.find(".kafka-form .toolbar-button").exists()).toBe(false);
  });

  it("appends stream message events for the active session and drops foreign ones", async () => {
    installBridge({ "kafka/stream/start": { sessionId: "stream-1" } });
    const wrapper = mountPanel();
    await flushPromises();
    await startSession(wrapper);
    pushEvent(wrapper, {
      sessionId: "stream-1",
      messages: [message(1, '{"tick":1}'), message(2, '{"tick":2}')],
      totalScanned: 2,
      totalMatched: 2,
      paused: false,
      bufferSize: 2,
    });
    await flushPromises();
    expect(stubRows(wrapper)).toHaveLength(2);
    expect(stubRows(wrapper)[0].text()).toContain('{"tick":1}');
    expect(wrapper.text()).toContain(t("stream.buffer", { count: 2 }));
    // 他 session 的事件丢弃，计数不增长
    pushEvent(wrapper, { sessionId: "stream-other", messages: [message(3, "x")], totalScanned: 3, totalMatched: 3, bufferSize: 3 });
    await flushPromises();
    expect(stubRows(wrapper)).toHaveLength(2);
    // drop 计数徽标（MAX_ROWS=1000 太大不易触发；直接锁定 dropped 徽标渲染条件不在此重复，
    // 由 kafkaModel.appendStreamRows 单测覆盖）
  });

  it("surfaces stream error events with friendly text for the active session", async () => {
    installBridge({ "kafka/stream/start": { sessionId: "stream-1" } });
    const wrapper = mountPanel();
    await flushPromises();
    await startSession(wrapper);
    pushEvent(wrapper, { sessionId: "stream-1", error: "connection refused: cannot reach broker" });
    await flushPromises();
    // 事件内嵌错误经 friendlyKafkaError 归一（与 App 错误横幅同源规则）
    expect(wrapper.emitted("error")?.at(-1)).toEqual([
      t("stream.statusError", { error: friendlyKafkaError("connection refused: cannot reach broker") }),
    ]);
    // 他 session 的错误不转发
    pushEvent(wrapper, { sessionId: "stream-other", error: "other failure" });
    await flushPromises();
    expect(wrapper.emitted("error")?.at(-1)).toEqual([
      t("stream.statusError", { error: friendlyKafkaError("connection refused: cannot reach broker") }),
    ]);
  });

  it("passes the debounced quick filter to the grid without new requests", async () => {
    vi.useFakeTimers();
    installBridge({ "kafka/stream/start": { sessionId: "stream-1" } });
    const wrapper = mountPanel();
    await flushPromises();
    await startSession(wrapper);
    pushEvent(wrapper, {
      sessionId: "stream-1",
      messages: [message(1, '{"tick":1}'), message(2, '{"order":"B-2"}')],
      totalScanned: 2,
      totalMatched: 2,
      bufferSize: 2,
    });
    await flushPromises();
    expect(stubRows(wrapper)).toHaveLength(2);
    const filter = wrapper.find('[data-testid="stream-quick-filter"]');
    expect(filter.attributes("placeholder")).toBe(t("messages.quickFilterPlaceholder"));
    const gridFilter = () => wrapper.find(".dbx-grid-stub").attributes("data-filter");
    await filter.setValue("k-2");
    // 防抖窗口未到：grid 尚未收到过滤词
    await vi.advanceTimersByTimeAsync(100);
    expect(gridFilter()).toBe("");
    await vi.advanceTimersByTimeAsync(60);
    expect(gridFilter()).toBe("k-2");
    // 清空恢复透传空词；quickFilter 只透传给 grid，不发任何请求
    await wrapper.find('[data-testid="stream-quick-filter"]').setValue("");
    await vi.advanceTimersByTimeAsync(200);
    expect(gridFilter()).toBe("");
    expect(invokeMock.mock.calls.filter(([method]) => method === "kafka/stream/start")).toHaveLength(1);
    expect(invokeMock.mock.calls.filter(([method]) => method.startsWith("kafka/stream/messages"))).toHaveLength(0);
  });

  it("pages the ring buffer with Older/Newer and clamps the offset", async () => {
    installBridge({
      "kafka/stream/start": { sessionId: "stream-1" },
      "kafka/stream/messages": (params?: unknown) => {
        const input = (params ?? {}) as { offset: number; limit: number };
        return { messages: [message(input.offset, `page@${input.offset}`)], total: 250 };
      },
    });
    const wrapper = mountPanel();
    await flushPromises();
    await startSession(wrapper);
    // 事件先同步 buffer 状态（bufferSize=250，limit 默认 100 → 窗口上限 150）
    pushEvent(wrapper, { sessionId: "stream-1", messages: [], totalScanned: 250, totalMatched: 250, bufferSize: 250, paused: false });
    await flushPromises();
    const pagers = () => wrapper.findAll(".stream-pager");
    // Older：已是最旧窗口 → offset 0
    await pagers()[0].trigger("click");
    await flushPromises();
    let pageCall = invokeMock.mock.calls.filter(([method]) => method === "kafka/stream/messages").at(-1);
    expect(pageCall?.[1]).toMatchObject({ sessionId: "stream-1", offset: 0, limit: 100 });
    expect(stubRows(wrapper)[0].text()).toContain("page@0");
    // Newer ×2：offset 100 → clamp 到 150（250 - pageSize 100）
    await pagers()[1].trigger("click");
    await flushPromises();
    pageCall = invokeMock.mock.calls.filter(([method]) => method === "kafka/stream/messages").at(-1);
    expect(pageCall?.[1]).toMatchObject({ offset: 100 });
    await pagers()[1].trigger("click");
    await flushPromises();
    pageCall = invokeMock.mock.calls.filter(([method]) => method === "kafka/stream/messages").at(-1);
    expect(pageCall?.[1]).toMatchObject({ offset: 150 });
  });

  it("keeps start disabled without a topic", async () => {
    installBridge({});
    const wrapper = mountPanel({ topic: "" });
    await flushPromises();
    expect(wrapper.find(".primary-button.compact").attributes("disabled")).toBeDefined();
  });
});

// -- Lane4 前端打磨：缓冲行导出 JSON/CSV/TSV（纯前端序列化，不发请求） --------

describe("StreamPanel export (Lane4)", () => {
  it("exports buffered rows client-side; buttons disabled without rows", async () => {
    installBridge({ "kafka/stream/start": { sessionId: "stream-1" } });
    const createObjectURL = vi.spyOn(URL, "createObjectURL").mockReturnValue("blob:mock");
    const anchorClick = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => {});
    const wrapper = mountPanel();
    await flushPromises();
    // 无行：三个导出按钮禁用
    for (const testid of ["stream-export-json", "stream-export-csv", "stream-export-tsv"]) {
      expect(wrapper.find(`[data-testid="${testid}"]`).attributes("disabled")).toBeDefined();
    }
    await startSession(wrapper);
    pushEvent(wrapper, {
      sessionId: "stream-1",
      messages: [message(1, "a\tb"), message(2, "c\nd")],
      totalScanned: 2,
      totalMatched: 2,
      paused: false,
      bufferSize: 2,
    });
    await flushPromises();
    for (const testid of ["stream-export-json", "stream-export-csv", "stream-export-tsv"]) {
      expect(wrapper.find(`[data-testid="${testid}"]`).attributes("disabled")).toBeUndefined();
    }
    // TSV：制表符/换行转义
    await wrapper.find('[data-testid="stream-export-tsv"]').trigger("click");
    const tsvBlob = createObjectURL.mock.calls.at(-1)![0] as Blob;
    expect(tsvBlob.type).toBe("text/tab-separated-values;charset=utf-8");
    const [header, row1, row2] = (await tsvBlob.text()).split("\r\n");
    expect(header).toBe("topic\tpartition\toffset\ttimestamp\tkey\tvalue\theaders");
    expect(row1).toContain("a\\tb");
    expect(row2).toContain("c\\nd");
    expect(wrapper.emitted("notify")?.at(-1)).toEqual([t("messages.exportDone", { name: "TSV" })]);
    // JSON：2 条消息的稳定数组
    await wrapper.find('[data-testid="stream-export-json"]').trigger("click");
    const jsonBlob = createObjectURL.mock.calls.at(-1)![0] as Blob;
    expect(JSON.parse(await jsonBlob.text())).toHaveLength(2);
    // 导出纯前端：start 仅一次，无任何 export/再次消费请求
    expect(anchorClick).toHaveBeenCalledTimes(2);
    expect(invokeMock.mock.calls.filter(([method]) => method === "kafka/stream/start")).toHaveLength(1);
    expect(invokeMock.mock.calls.filter(([method]) => String(method).includes("export"))).toHaveLength(0);
  });
});
