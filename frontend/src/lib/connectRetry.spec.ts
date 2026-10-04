import { describe, expect, it } from "vitest";
import {
  BOOT_RESTORE_RETRY_DELAY_MS,
  BOOT_RESTORE_RETRY_MAX,
  decideConnectRetry,
  MANUAL_RETRY_DELAY_MS,
  MANUAL_RETRY_MAX,
  isConnectionInactiveError,
} from "./connectRetry";

const INACTIVE_SIDECAR = `connection "conn-test" is not connected; call connection/connect first`;

describe("isConnectionInactiveError", () => {
  it("recognizes the sidecar registry miss and host inactive wording", () => {
    expect(isConnectionInactiveError(new Error(INACTIVE_SIDECAR))).toBe(true);
    expect(isConnectionInactiveError(new Error("Connection is not active"))).toBe(true);
    expect(isConnectionInactiveError(INACTIVE_SIDECAR)).toBe(true);
  });

  it("does not swallow real transport failures", () => {
    expect(isConnectionInactiveError(new Error("connection lost (fixture error injection)"))).toBe(false);
    expect(isConnectionInactiveError(new Error("client has been closed"))).toBe(false);
    expect(isConnectionInactiveError(new Error("dial tcp: connection refused"))).toBe(false);
    expect(isConnectionInactiveError(new Error("SASL authentication failed"))).toBe(false);
  });
});

describe("decideConnectRetry", () => {
  it("fails non-inactive errors immediately (no behavior change)", () => {
    expect(decideConnectRetry({ cause: new Error("dial tcp: connection refused"), attempt: 0, bootRestore: true })).toEqual({ kind: "fail" });
    expect(decideConnectRetry({ cause: new Error("SASL authentication failed"), attempt: 0, bootRestore: false })).toEqual({ kind: "fail" });
  });

  it("polls boot-restore inactive errors on the dedicated restore window", () => {
    // 整页刷新后恢复页首次 kafka/topics/list 跑赢宿主 connect 重放：固定 1s
    // 节奏吸收时序窗口，不再直接落错误终态（dbx-plugin-ssh#144 同类）。
    const first = decideConnectRetry({ cause: new Error(INACTIVE_SIDECAR), attempt: 0, bootRestore: true });
    expect(first).toEqual({ kind: "retry", attempt: 1, delayMs: BOOT_RESTORE_RETRY_DELAY_MS });
    const last = decideConnectRetry({ cause: new Error(INACTIVE_SIDECAR), attempt: BOOT_RESTORE_RETRY_MAX - 1, bootRestore: true });
    expect(last).toEqual({ kind: "retry", attempt: BOOT_RESTORE_RETRY_MAX, delayMs: BOOT_RESTORE_RETRY_DELAY_MS });
    const exhausted = decideConnectRetry({ cause: new Error(INACTIVE_SIDECAR), attempt: BOOT_RESTORE_RETRY_MAX, bootRestore: true });
    expect(exhausted).toEqual({ kind: "fail" });
  });

  it("polls manual-entry inactive errors on the manual window, then fails", () => {
    const first = decideConnectRetry({ cause: new Error(INACTIVE_SIDECAR), attempt: 0, bootRestore: false });
    expect(first).toEqual({ kind: "retry", attempt: 1, delayMs: MANUAL_RETRY_DELAY_MS });
    const last = decideConnectRetry({ cause: new Error(INACTIVE_SIDECAR), attempt: MANUAL_RETRY_MAX - 1, bootRestore: false });
    expect(last).toEqual({ kind: "retry", attempt: MANUAL_RETRY_MAX, delayMs: MANUAL_RETRY_DELAY_MS });
    const exhausted = decideConnectRetry({ cause: new Error(INACTIVE_SIDECAR), attempt: MANUAL_RETRY_MAX, bootRestore: false });
    expect(exhausted).toEqual({ kind: "fail" });
  });
});
