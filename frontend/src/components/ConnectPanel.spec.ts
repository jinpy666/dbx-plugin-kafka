// @vitest-environment happy-dom
import { beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import ConnectPanel from "./ConnectPanel.vue";
import { setKafkaConnectionId } from "../lib/api";
import { setWorkbenchLocale } from "../lib/i18n";

const invoke = vi.fn();
const row = { name: "orders-sink", type: "sink", state: "RUNNING", workerId: "worker:8083", tasks: [{ id: 0, state: "FAILED", workerId: "worker:8083", trace: "java.lang.RuntimeException" }], topics: ["orders"], consumerGroup: "connect-orders-sink" };
function mountPanel(canWrite = true, canDelete = true, topicFilter = "") {
  return mount(ConnectPanel, { props: { canWrite, canDelete, topicFilter }, global: { stubs: { teleport: true } } });
}
beforeEach(() => {
  setWorkbenchLocale("en"); setKafkaConnectionId("test");
  invoke.mockReset();
  invoke.mockImplementation(async (method: string) => {
    switch (method) {
      case "kafka/connect/clusters/list": return { clusters: [{ name: "local", url: "http://localhost:8083" }] };
      case "kafka/connect/connectors/list": return { connectors: [row, { name: "broken", tasks: [], topics: [], error: "Kafka Connect access denied (HTTP 403)" }] };
      case "kafka/connect/config/get": return { config: { "connector.class": "FileSink", password: "[REDACTED]", topics: "orders" }, sensitiveKeys: ["password"] };
      case "kafka/connect/plugins/list": return { plugins: [{ class: "FileSink", type: "sink", version: "1" }] };
      case "kafka/connect/plugins/config": return { definitions: [{ name: "topics", type: "STRING", required: true }] };
      case "kafka/connect/config/validate": return { errorCount: 0, fields: [] };
      default: return { success: true, supported: true };
    }
  });
  window.dbxPlugin = { invoke } as unknown as DbxPluginApi;
});

describe("Kafka Connect", () => {
  it("keeps partial failures visible and links tasks, topics and consumer groups", async () => {
    const panel = mountPanel(); await flushPromises();
    expect(panel.text()).toContain("access denied");
    await panel.findAll(".connector-name").find(button => button.text() === "orders-sink")!.trigger("click");
    expect(panel.text()).toContain("java.lang.RuntimeException");
    await panel.findAll("button").find(button => button.text() === "orders")!.trigger("click");
    expect(panel.emitted("open-topic")).toEqual([["orders"]]);
    await panel.findAll("button").find(button => button.text() === "connect-orders-sink")!.trigger("click");
    expect(panel.emitted("open-group")).toEqual([["connect-orders-sink"]]);
    panel.unmount();
  });
  it("disables every mutation when read-only", async () => {
    const panel = mountPanel(false, false); await flushPromises();
    await panel.findAll(".connector-name").find(button => button.text() === "orders-sink")!.trigger("click");
    for (const button of panel.findAll("button[data-action]:not([data-action=config])")) expect(button.attributes("disabled")).toBeDefined();
    const task = panel.findAll("button").find(button => button.text() === "Restart task")!;
    expect(task.attributes("disabled")).toBeDefined();
    panel.unmount();
  });
  it("requires exact connector confirmation before delete", async () => {
    const panel = mountPanel(); await flushPromises();
    await panel.findAll(".connector-name").find(button => button.text() === "orders-sink")!.trigger("click");
    await panel.find('[data-action="delete"]').trigger("click");
    expect(panel.find('[data-action="confirm"]').attributes("disabled")).toBeDefined();
    await panel.find('.modal input').setValue("orders-sink");
    await panel.find('[data-action="confirm"]').trigger("click"); await flushPromises();
    expect(invoke).toHaveBeenCalledWith("kafka/connect/connectors/delete", { connectionId: "test", cluster: "local", connector: "orders-sink", confirmConnector: "orders-sink" });
    panel.unmount();
  });
  it("keeps unsupported operations visible and disabled after a worker rejects them", async () => {
    const fallback = invoke.getMockImplementation()!;
    invoke.mockImplementation((method: string, ...args: unknown[]) => method === "kafka/connect/connectors/action" ? Promise.resolve({ success: false, supported: false, message: "Endpoint unavailable" }) : fallback(method, ...args));
    const panel = mountPanel(); await flushPromises();
    await panel.findAll(".connector-name").find(button => button.text() === "orders-sink")!.trigger("click");
    await panel.find('[data-action="stop"]').trigger("click"); await flushPromises();
    expect(panel.find('[data-action="stop"]').attributes("disabled")).toBeDefined();
    expect(panel.emitted("error")?.at(-1)).toEqual(["Endpoint unavailable"]);
    panel.unmount();
  });
  it("edits redacted configuration, validates and clears the editor on close", async () => {
    const panel = mountPanel(); await flushPromises();
    await panel.findAll(".connector-name").find(button => button.text() === "orders-sink")!.trigger("click");
    await panel.findAll("button").find(button => button.text() === "Configuration")!.trigger("click"); await flushPromises();
    expect((panel.find("textarea").element as HTMLTextAreaElement).value).toContain("[REDACTED]");
    await panel.findAll("button").find(button => button.text() === "Validate configuration")!.trigger("click"); await flushPromises();
    expect(panel.text()).toContain("Configuration is valid");
    await panel.find('[data-action="save"]').trigger("click"); await flushPromises();
    expect(invoke).toHaveBeenCalledWith("kafka/connect/config/set", expect.objectContaining({ config: expect.objectContaining({ password: "[REDACTED]" }) }));
    expect(panel.find("textarea").exists()).toBe(false);
    panel.unmount();
  });
  it("filters related connectors by topic", async () => {
    const panel = mountPanel(true, true, "orders"); await flushPromises();
    expect(panel.findAll(".connector-name")).toHaveLength(1);
    panel.unmount();
  });
});
