import { onBeforeUnmount } from "vue";

export const TRANSPORT_PROBE_INTERVAL_MS = 5_000;
export const TRANSPORT_PROBE_TIMEOUT_MS = 8_000;
export const HOST_REACHABILITY_TIMEOUT_MS = 2_000;

/**
 * Detects browser-to-host outages that produce no Kafka stream event and no
 * invoke rejection: the workbench iframe keeps running with every control-plane
 * call silently pending and the host event feed stopped. Browser
 * online/offline events give an immediate signal; a bounded probe over a
 * read-only RPC (injected by App.vue: kafkaApi.connectionStatuses()) covers
 * Docker/bridge paths where navigator.onLine stays true. Recovery (injected as
 * `recover`) runs once per outage after an abortable same-origin probe confirms
 * the DBX host is reachable again.移植自 ssh 插件的 useTerminalTransportWatch，
 * 常数与单飞/清理语义保持一致。
 */
export function useTransportWatch(options: {
  /** 工作台未就绪（context 未到/无 connectionId）时不探测也不恢复。 */
  shouldProbe?: () => boolean;
  /** 只读轻量探测 RPC；8s 内未定即视为断链（宿主无法替浏览器超时未收到的请求）。 */
  probe: () => Promise<unknown>;
  /** 同源可达性兜底；缺省用 2s AbortController HEAD 探测本页地址。 */
  canReachHost?: () => Promise<boolean>;
  /** 断链恢复动作（reopen → 策略/topics 重拉/面板补拉）；异常静默降级。 */
  recover: () => void | Promise<void>;
}) {
  let probeTimer = 0;
  let probeInFlight = false;
  let recoveryInFlight = false;
  let outage = false;
  let stopped = false;

  // DBX Web deployments run the plugin in an HTTP(S) browser context. Native
  // desktop webviews keep their existing host-pushed transport lifecycle.
  const browserDeployment = window.location.protocol === "http:" || window.location.protocol === "https:";

  async function probeHostReachability(): Promise<boolean> {
    if (options.canReachHost) {
      try {
        return await options.canReachHost();
      } catch {
        return false;
      }
    }

    // The invoke timeout is enforced in this page after the request reaches the
    // host. It cannot settle a request that is stuck between this browser and
    // the Docker host, so use an abortable same-origin request to decide when
    // it is safe to start recovering.
    const controller = new AbortController();
    const timer = window.setTimeout(() => controller.abort(), HOST_REACHABILITY_TIMEOUT_MS);
    try {
      await fetch(window.location.href, {
        method: "HEAD",
        cache: "no-store",
        credentials: "same-origin",
        signal: controller.signal,
      });
      return true;
    } catch {
      return false;
    } finally {
      window.clearTimeout(timer);
    }
  }

  function withTimeout<T>(operation: Promise<T>, timeoutMs: number): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      let settled = false;
      const timer = window.setTimeout(() => {
        settled = true;
        reject(new Error(`Kafka transport probe timed out after ${timeoutMs} ms`));
      }, timeoutMs);
      Promise.resolve(operation).then(
        (value) => {
          if (settled) return;
          settled = true;
          window.clearTimeout(timer);
          resolve(value);
        },
        (cause: unknown) => {
          if (settled) return;
          settled = true;
          window.clearTimeout(timer);
          reject(cause);
        },
      );
    });
  }

  function enterOutage() {
    if (stopped || !browserDeployment || outage) return;
    outage = true;
  }

  /** 单飞恢复：同源可达性放行后才执行注入的 recover，全程静默降级。 */
  async function attemptRecovery() {
    if (stopped || !browserDeployment || !outage || recoveryInFlight) return;
    if (!options.shouldProbe?.()) return;
    recoveryInFlight = true;
    try {
      if (await probeHostReachability()) {
        outage = false;
        await Promise.resolve(options.recover());
      }
    } catch {
      // 恢复动作失败不打断看守：下一轮探测若仍异常会再次进入恢复流程。
    } finally {
      recoveryInFlight = false;
    }
  }

  async function probe() {
    if (stopped || !browserDeployment || document.visibilityState === "hidden") return;
    if (!options.shouldProbe?.()) return;
    if (outage) {
      await attemptRecovery();
      return;
    }
    if (probeInFlight) return;
    probeInFlight = true;
    try {
      await withTimeout(Promise.resolve().then(options.probe), TRANSPORT_PROBE_TIMEOUT_MS);
    } catch {
      // A timeout must be enforced in this page: the host cannot time out an
      // HTTP request it never receives. Enter outage and retry only after an
      // abortable same-origin probe confirms the host is back.
      enterOutage();
      void attemptRecovery();
    } finally {
      probeInFlight = false;
    }
  }

  function onOffline() {
    enterOutage();
    void attemptRecovery();
  }

  function onOnline() {
    void attemptRecovery();
  }

  function onVisibilityChange() {
    if (document.visibilityState === "visible") void probe();
  }

  if (browserDeployment) {
    window.addEventListener("offline", onOffline);
    window.addEventListener("online", onOnline);
    document.addEventListener("visibilitychange", onVisibilityChange);
    probeTimer = window.setInterval(() => void probe(), TRANSPORT_PROBE_INTERVAL_MS);
  }

  onBeforeUnmount(() => {
    stopped = true;
    window.clearInterval(probeTimer);
    window.removeEventListener("offline", onOffline);
    window.removeEventListener("online", onOnline);
    document.removeEventListener("visibilitychange", onVisibilityChange);
  });
}
