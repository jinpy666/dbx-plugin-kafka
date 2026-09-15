// @vitest-environment happy-dom
// ProducePanel 组件测试（Phase 3 F4/F6-4）：
// - Flow 测试数据生成：template 来源启停 + 每 tick 逐条 produce + 计数；
//   发送连续失败 ≥3 自动停止；schema_random 命中 PROTOBUF subject → 行内提示
//   改用 template 且不发出任何 produce；schema_random 正常路径带 schema 挂载。
// - 分区数：App 传入 partitionCount → 头部徽标 + partition 超界行内校验。
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import { defineComponent, h } from "vue";
import ProducePanel from "./ProducePanel.vue";
import { setKafkaConnectionId } from "../lib/api";
import { t } from "../lib/i18n";

// CodeEditor 用 stub（CodeMirror 挂载与 Flow 逻辑无关；v-model 走 input 事件）。
const CodeEditorStub = defineComponent({
  name: "CodeEditorStub",
  props: { modelValue: { type: String, default: "" }, disabled: { type: Boolean, default: false } },
  emits: ["update:modelValue"],
  setup(props, { emit }) {
    return () =>
      h("textarea", {
        class: "code-editor-stub",
        value: props.modelValue,
        disabled: props.disabled,
        onInput: (event: Event) => emit("update:modelValue", (event.target as HTMLTextAreaElement).value),
      });
  },
});

const invokeMock = vi.fn();

function installBridge(handler?: (method: string, params: Record<string, unknown>) => unknown) {
  invokeMock.mockReset();
  invokeMock.mockImplementation(async (method: string, params: Record<string, unknown> = {}) => {
    if (handler) return handler(method, params);
    throw new Error(`unhandled method: ${method}`);
  });
  (window as unknown as { dbxPlugin: unknown }).dbxPlugin = { invoke: invokeMock };
}

const AVRO_SCHEMA = JSON.stringify({ type: "record", name: "Order", fields: [{ name: "orderId", type: "string" }] });

function schemaBridge() {
  return (method: string, _params: Record<string, unknown> = {}) => {
    if (method === "kafka/schema/subjects/list") {
      return { subjects: [{ subject: "order-events-value", formats: ["avro"], latestVersion: 1 }] };
    }
    if (method === "kafka/schema/get") {
      return { subject: "order-events-value", version: 1, id: 42, schema: AVRO_SCHEMA, format: "avro", references: [] };
    }
    throw new Error(`unhandled method: ${method}`);
  };
}

function mountPanel(props: Record<string, unknown> = {}) {
  return mount(ProducePanel, {
    props: { topic: "order-events", canWrite: true, ...props },
    global: { stubs: { CodeEditor: CodeEditorStub, teleport: true } },
  });
}

beforeEach(() => {
  localStorage.clear();
  setKafkaConnectionId("conn-test");
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

async function startFlow(wrapper: ReturnType<typeof mountPanel>, testid = "flow-start") {
  await wrapper.find(`[data-testid="${testid}"]`).trigger("click");
  // 启动是 async（schema 发现/预演），首 tick 在微任务后同步执行。
  await vi.advanceTimersByTimeAsync(0);
  await flushPromises();
}

describe("ProducePanel Flow (F4)", () => {
  it("generates and sends per tick with the template source; stop halts the loop", async () => {
    const produced: Array<Record<string, unknown>> = [];
    installBridge((method, params) => {
      if (method !== "kafka/messages/produce") return {};
      produced.push(params);
      return { partition: 0, offset: produced.length - 1 };
    });
    const wrapper = mountPanel();
    await flushPromises();
    // 勾选 Flow 组 + 选 template 来源 + 填模板。
    await wrapper.find('[data-testid="flow-toggle"]').setValue(true);
    await wrapper.find('[data-testid="flow-group"] select').setValue("template");
    const template = '{"id":"{uuid}","n":{int:1,1}}';
    // CodeEditor stub 顺序：value 编辑器 → headers 编辑器 → flow 模板编辑器（末位）。
    await wrapper.findAll(".code-editor-stub")[2].setValue(template);
    await startFlow(wrapper);

    // 首 tick：countPerSend=1 → 1 条 produce；值已展开（无占位符残留）。
    expect(produced).toHaveLength(1);
    const first = produced[0] as { value: string };
    expect(first.value).not.toContain("{uuid}");
    expect(JSON.parse(first.value).n).toBe(1);
    // 运行徽标 + 计数可见。
    expect(wrapper.find('[data-testid="flow-badge"]').exists()).toBe(true);
    expect(wrapper.find('[data-testid="flow-badge"]').text()).toContain(t("produce.flowRunning"));

    // intervalMs 默认 1000：推进 2 个 tick → 累计 3 条。
    await vi.advanceTimersByTimeAsync(2000);
    await flushPromises();
    expect(produced).toHaveLength(3);
    expect(wrapper.text()).toContain(t("produce.flowSent", { count: 3 }));

    // 停止：不再发produce。
    await wrapper.find('[data-testid="flow-stop"]').trigger("click");
    await flushPromises();
    expect(wrapper.find('[data-testid="flow-badge"]').exists()).toBe(false);
    const afterStop = produced.length;
    await vi.advanceTimersByTimeAsync(5000);
    expect(produced.length).toBe(afterStop);
    expect((wrapper.emitted("error") ?? []).every(([message]) => message === "")).toBe(true);
  });

  it("auto-stops after 3 consecutive produce failures", async () => {
    let calls = 0;
    installBridge((method) => {
      if (method !== "kafka/messages/produce") return {};
      calls += 1;
      throw new Error("broker gone");
    });
    const wrapper = mountPanel();
    await flushPromises();
    await wrapper.find('[data-testid="flow-toggle"]').setValue(true);
    await wrapper.find('[data-testid="flow-group"] select').setValue("template");
    await wrapper.findAll(".code-editor-stub")[2].setValue('{"n":1}');
    await startFlow(wrapper);
    await vi.advanceTimersByTimeAsync(3000);
    await flushPromises();

    // 首 tick 1 次 + 3 个 tick = 4 次尝试，第 3 次连续失败即自动停止。
    expect(calls).toBeGreaterThanOrEqual(3);
    expect(wrapper.find('[data-testid="flow-badge"]').exists()).toBe(false);
    expect(wrapper.find('[data-testid="flow-hint"]').text()).toContain(t("produce.flowAutoStoppedFailures"));
    const afterStop = calls;
    await vi.advanceTimersByTimeAsync(10000);
    expect(calls).toBe(afterStop);
  });

  it("schema_random with a PROTOBUF subject shows the template hint and sends nothing (12.2.5)", async () => {
    installBridge((method) => {
      if (method === "kafka/schema/subjects/list") {
        return { subjects: [{ subject: "order-events-value", formats: ["protobuf"], latestVersion: 1 }] };
      }
      if (method === "kafka/schema/get") {
        return { subject: "order-events-value", version: 1, id: 7, schema: "syntax = \"proto3\";", format: "protobuf", references: [] };
      }
      throw new Error(`unhandled method: ${method}`);
    });
    const wrapper = mountPanel();
    await flushPromises();
    await wrapper.find('[data-testid="flow-toggle"]').setValue(true);
    // 默认来源即 schema_random。
    await startFlow(wrapper);
    expect(invokeMock.mock.calls.filter(([method]) => method === "kafka/messages/produce")).toHaveLength(0);
    const hint = wrapper.find('[data-testid="flow-hint"]');
    expect(hint.exists()).toBe(true);
    expect(hint.text()).toContain("protobuf");
    expect(hint.text()).toContain(t("produce.flowSourceTemplate"));
  });

  it("schema_random happy path attaches the matched subject with latest version (12.2.5)", async () => {
    const produced: Array<Record<string, unknown>> = [];
    installBridge((method, params) => {
      if (method === "kafka/messages/produce") {
        produced.push(params);
        return { partition: 0, offset: 0 };
      }
      return schemaBridge()(method, params);
    });
    const wrapper = mountPanel();
    await flushPromises();
    await wrapper.find('[data-testid="flow-toggle"]').setValue(true);
    await startFlow(wrapper);
    expect(produced).toHaveLength(1);
    // 零后端改动：复用 produce schema 挂载，version 缺省 = latest。
    expect(produced[0].schema).toEqual({ subject: "order-events-value", format: "avro" });
    const generated = JSON.parse(String(produced[0].value)) as { orderId: string };
    expect(typeof generated.orderId).toBe("string");
    expect(generated.orderId.length).toBeGreaterThan(0);
  });
});

describe("ProducePanel delivery params + flow stop conditions (Lane 2)", () => {
  it("sends acks/enableIdempotence only when they differ from backend defaults", async () => {
    const produced: Array<Record<string, unknown>> = [];
    installBridge((method, params) => {
      if (method !== "kafka/messages/produce") return {};
      produced.push(params);
      return { partition: 0, offset: produced.length - 1 };
    });
    const wrapper = mountPanel();
    await flushPromises();
    await wrapper.find(".produce-value-editor textarea, .code-editor-stub").setValue("hello");
    // 默认：acks=all + 幂等开 = 后端默认 → 请求不带这两字段。
    await wrapper.find(".produce-actions .produce-send-button").trigger("click");
    await flushPromises();
    expect(produced).toHaveLength(1);
    expect(produced[0]).not.toHaveProperty("acks");
    expect(produced[0]).not.toHaveProperty("enableIdempotence");

    // acks=1 + 关幂等：显式携带。
    await wrapper.find('[data-testid="produce-acks"]').setValue("1");
    await wrapper.find('[data-testid="produce-idempotence"]').setValue(false);
    await wrapper.find(".produce-actions .produce-send-button").trigger("click");
    await flushPromises();
    expect(produced).toHaveLength(2);
    expect(produced[1].acks).toBe("1");
    expect(produced[1].enableIdempotence).toBe(false);
  });

  it("stops the flow generator when the record limit is reached", async () => {
    const produced: Array<Record<string, unknown>> = [];
    installBridge((method, params) => {
      if (method !== "kafka/messages/produce") return {};
      produced.push(params);
      return { partition: 0, offset: produced.length - 1 };
    });
    const wrapper = mountPanel();
    await flushPromises();
    await wrapper.find('[data-testid="flow-toggle"]').setValue(true);
    await wrapper.find('[data-testid="flow-group"] select').setValue("template");
    await wrapper.findAll(".code-editor-stub")[2].setValue('{"n":1}');
    await wrapper.find('[data-testid="flow-max-records"]').setValue("2");
    await startFlow(wrapper);

    // 首 tick 1 条；上限 2 → 第 2 tick 达标后自动停止（不再发第 3 条）。
    expect(produced).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1000);
    await flushPromises();
    expect(produced).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(5000);
    await flushPromises();
    expect(produced).toHaveLength(2);
    expect(wrapper.find('[data-testid="flow-badge"]').exists()).toBe(false);
    expect(wrapper.find('[data-testid="flow-hint"]').text()).toContain(t("produceAdv.flowAutoStoppedRecords"));
  });

  it("stops the flow generator when the duration limit is reached", async () => {
    const produced: Array<Record<string, unknown>> = [];
    installBridge((method, params) => {
      if (method !== "kafka/messages/produce") return {};
      produced.push(params);
      return { partition: 0, offset: produced.length - 1 };
    });
    const wrapper = mountPanel();
    await flushPromises();
    await wrapper.find('[data-testid="flow-toggle"]').setValue(true);
    await wrapper.find('[data-testid="flow-group"] select').setValue("template");
    await wrapper.findAll(".code-editor-stub")[2].setValue('{"n":1}');
    await wrapper.find('[data-testid="flow-max-duration"]').setValue("1500");
    await startFlow(wrapper);

    // t=0 发 1 条（1500ms 内），t=1000 发第 2 条，t=2000 时长已超 → 停。
    expect(produced).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1000);
    await flushPromises();
    expect(produced).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(2000);
    await flushPromises();
    expect(produced).toHaveLength(2);
    expect(wrapper.find('[data-testid="flow-badge"]').exists()).toBe(false);
    expect(wrapper.find('[data-testid="flow-hint"]').text()).toContain(t("produceAdv.flowAutoStoppedDuration"));
  });
});

describe("ProducePanel partition guard (F6-4)", () => {
  it("shows the partition count badge from the selected topic", async () => {
    installBridge();
    const wrapper = mountPanel({ partitionCount: 6 });
    await flushPromises();
    const badge = wrapper.find(".produce-topic-row .badge");
    expect(badge.text()).toBe(t("produce.partitionCount", { count: 6 }));
  });

  it("blocks manual send when the partition is out of range", async () => {
    installBridge();
    const wrapper = mountPanel({ partitionCount: 2 });
    await flushPromises();
    await wrapper.find(".code-editor-stub").setValue("payload");
    await wrapper.findAll("input[type='number']")[0].setValue("5");
    await wrapper.find(".produce-actions .primary-button").trigger("click");
    await flushPromises();
    expect(invokeMock.mock.calls.filter(([method]) => method === "kafka/messages/produce")).toHaveLength(0);
    // 分区输入行内校验提示 + 底部错误条同键。
    expect(wrapper.find('[data-testid="partition-issue"]').text()).toBe(t("produce.partitionOutOfRange"));
    expect(wrapper.find(".produce-error").text()).toBe(t("produce.partitionOutOfRange"));
  });

  it("keeps auto-partition and in-range partitions sendable", async () => {
    installBridge();
    const wrapper = mountPanel({ partitionCount: 2 });
    await flushPromises();
    await wrapper.find(".produce-value-editor textarea, .code-editor-stub").setValue("payload");
    await wrapper.find(".produce-actions .primary-button").trigger("click");
    await flushPromises();
    expect(invokeMock.mock.calls.filter(([method]) => method === "kafka/messages/produce")).toHaveLength(1);
  });
});
