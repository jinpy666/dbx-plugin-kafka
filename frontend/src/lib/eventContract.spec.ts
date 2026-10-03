// 事件契约注册表（shared/contracts/events.json，sidecar 事件载荷单一真相）
// 的前端侧消费钉：与 backend/events_contract_test.go 对拍同一份文件。
// 本 spec 保证：注册表注册的事件面与前端消费函数的取值假设一致——
// audit result 曾漂移（后端发 success|blocked、前端只认 ok|denied，成功
// 操作在审计流全部显示为错误），bufferSize 漂移同模式；此钉保证两侧
// 从同一份文件对齐。
import { describe, expect, it } from "vitest";
import contract from "../../../shared/contracts/events.json";
import { normalizeAuditResult } from "./auditFeed";

const events = contract.events as Record<
  string,
  { keys: string[]; optionalKeys?: string[]; resultEnum?: string[] }
>;

describe("shared/contracts/events.json", () => {
  it("registers exactly the four sidecar events", () => {
    expect(Object.keys(events).sort()).toEqual([
      "kafka/audit",
      "kafka/stream/error",
      "kafka/stream/messages",
      "kafka/ui/intent",
    ]);
  });

  it("every event declares a non-empty key set", () => {
    for (const [name, shape] of Object.entries(events)) {
      expect(shape.keys.length, name).toBeGreaterThan(0);
      expect(new Set(shape.keys).size, name).toBe(shape.keys.length);
    }
  });

  it("audit resultEnum aligns with normalizeAuditResult", () => {
    const allowed = events["kafka/audit"].resultEnum ?? [];
    expect(allowed.sort()).toEqual(["denied", "error", "ok"]);
    for (const value of allowed) {
      expect(normalizeAuditResult(value), value).toBe(value);
    }
  });

  it("stream messages carries bufferSize (droppedRows estimation source)", () => {
    expect(events["kafka/stream/messages"].keys).toContain("bufferSize");
  });

  it("ui intent registry keys align with readUiIntentEvent consumption", async () => {
    // 后端构造点 mcp.UiIntentEventPayload 的键面（后端测试对拍同一份注册表）；
    // 前端消费假设在此钉住：三个键全部必填、无 optionalKeys。
    const { readUiIntentEvent } = await import("../../../shared/frontend/uiIntent");
    const shape = events["kafka/ui/intent"];
    expect([...shape.keys].sort()).toEqual(["action", "intentId", "params"]);
    expect(shape.optionalKeys ?? []).toEqual([]);
    const event = {
      type: "kafka",
      method: "kafka/ui/intent",
      params: { intentId: "i-1", action: "select", params: { partition: 0, offset: 1 } },
    };
    expect(readUiIntentEvent(event, "kafka")).toEqual({
      intentId: "i-1",
      action: "select",
      params: { partition: 0, offset: 1 },
    });
    // fail-closed：必需键缺失 → null（这正是漂移会让 intent 静默失效的路径）。
    expect(readUiIntentEvent({ method: "kafka/ui/intent", params: { intentId: "i-1" } }, "kafka")).toBeNull();
    expect(readUiIntentEvent({ method: "kafka/ui/intent", params: { action: "select" } }, "kafka")).toBeNull();
  });
});
