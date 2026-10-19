// @vitest-environment happy-dom
// 断链看守（移植 ssh useTerminalTransportWatch 的测试形态）：覆盖离线触发、
// 探测超时、恢复单飞与非 web 部署不启用四类行为。
import { defineComponent } from "vue";
import { mount } from "@vue/test-utils";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  HOST_REACHABILITY_TIMEOUT_MS,
  TRANSPORT_PROBE_INTERVAL_MS,
  TRANSPORT_PROBE_TIMEOUT_MS,
  useTransportWatch,
} from "./useTransportWatch";

describe("useTransportWatch", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("enters outage on the offline event and recovers only after reachability returns", async () => {
    vi.useFakeTimers();
    const probe = vi.fn().mockResolvedValue({ statuses: [] });
    const canReachHost = vi.fn().mockResolvedValue(false);
    const recover = vi.fn().mockResolvedValue(undefined);

    const wrapper = mount(defineComponent({
      setup() {
        useTransportWatch({ probe, canReachHost, recover, shouldProbe: () => true });
        return () => null;
      },
    }));

    await vi.advanceTimersByTimeAsync(TRANSPORT_PROBE_INTERVAL_MS);
    expect(probe).toHaveBeenCalledTimes(1);
    expect(recover).not.toHaveBeenCalled();

    // 断网事件即时置位；离线期间同源可达性失败，保持断链态不恢复。
    window.dispatchEvent(new Event("offline"));
    await flushAsync();
    expect(canReachHost).toHaveBeenCalledTimes(1);
    expect(recover).not.toHaveBeenCalled();

    canReachHost.mockResolvedValue(true);
    window.dispatchEvent(new Event("online"));
    await flushAsync();
    expect(recover).toHaveBeenCalledTimes(1);
    wrapper.unmount();
  });

  it("enters outage when the probe never settles and recovers once reachable", async () => {
    vi.useFakeTimers();
    const probe = vi.fn(() => new Promise<never>(() => {}));
    const canReachHost = vi.fn().mockResolvedValue(false);
    const recover = vi.fn().mockResolvedValue(undefined);

    const wrapper = mount(defineComponent({
      setup() {
        useTransportWatch({ probe, canReachHost, recover, shouldProbe: () => true });
        return () => null;
      },
    }));

    await vi.advanceTimersByTimeAsync(TRANSPORT_PROBE_INTERVAL_MS);
    expect(probe).toHaveBeenCalledTimes(1);

    // 8s 探测超时 → 断链；可达性失败期间后续探测全部转入恢复尝试，不再发探测。
    await vi.advanceTimersByTimeAsync(TRANSPORT_PROBE_TIMEOUT_MS);
    await flushAsync();
    expect(canReachHost).toHaveBeenCalledTimes(1);
    expect(recover).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(TRANSPORT_PROBE_INTERVAL_MS);
    expect(probe).toHaveBeenCalledTimes(1);

    canReachHost.mockResolvedValue(true);
    await vi.advanceTimersByTimeAsync(TRANSPORT_PROBE_INTERVAL_MS);
    await flushAsync();
    expect(recover).toHaveBeenCalledTimes(1);
    wrapper.unmount();
  });

  it("runs recovery single-flight while offline events and probes overlap", async () => {
    vi.useFakeTimers();
    let releaseReachability: ((value: boolean) => void) | undefined;
    const probe = vi.fn().mockRejectedValue(new Error("host bridge unavailable"));
    const recover = vi.fn().mockResolvedValue(undefined);

    const wrapper = mount(defineComponent({
      setup() {
        useTransportWatch({
          probe,
          canReachHost: () => new Promise<boolean>((resolve) => { releaseReachability = resolve; }),
          recover,
          shouldProbe: () => true,
        });
        return () => null;
      },
    }));

    // 首拍探测即失败 → 恢复流程挂起（可达性探测未决）。
    await vi.advanceTimersByTimeAsync(TRANSPORT_PROBE_INTERVAL_MS);
    await flushAsync();
    expect(recover).not.toHaveBeenCalled();

    // 并发的 online 事件与周期探测都被单飞挡住，可达性放行后 recover 只跑一次。
    window.dispatchEvent(new Event("online"));
    window.dispatchEvent(new Event("offline"));
    window.dispatchEvent(new Event("online"));
    await vi.advanceTimersByTimeAsync(TRANSPORT_PROBE_INTERVAL_MS);
    await flushAsync();
    expect(recover).not.toHaveBeenCalled();

    releaseReachability?.(true);
    await flushAsync();
    expect(recover).toHaveBeenCalledTimes(1);
    wrapper.unmount();
  });

  it("stays disabled entirely on non-web deployments", async () => {
    vi.useFakeTimers();
    const realLocation = window.location;
    Object.defineProperty(window, "location", {
      value: new URL("file:///workbench"),
      configurable: true,
      writable: true,
    });
    const probe = vi.fn().mockResolvedValue({ statuses: [] });
    const canReachHost = vi.fn().mockResolvedValue(true);
    const recover = vi.fn().mockResolvedValue(undefined);

    try {
      const wrapper = mount(defineComponent({
        setup() {
          useTransportWatch({ probe, canReachHost, recover, shouldProbe: () => true });
          return () => null;
        },
      }));

      window.dispatchEvent(new Event("offline"));
      window.dispatchEvent(new Event("online"));
      await vi.advanceTimersByTimeAsync(TRANSPORT_PROBE_INTERVAL_MS * 2 + TRANSPORT_PROBE_TIMEOUT_MS);
      await flushAsync();

      expect(probe).not.toHaveBeenCalled();
      expect(canReachHost).not.toHaveBeenCalled();
      expect(recover).not.toHaveBeenCalled();
      wrapper.unmount();
    } finally {
      Object.defineProperty(window, "location", {
        value: realLocation,
        configurable: true,
        writable: true,
      });
    }
  });
});

/** 让未决的微任务（恢复链/单飞守卫）落地。 */
async function flushAsync() {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}
