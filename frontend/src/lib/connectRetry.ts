/**
 * Bounded retry decision for backend calls hitting a sidecar whose connection
 * registry has not been filled yet.
 *
 * 连接生命周期由宿主驱动（connection/connect|disconnect），工作台只持有
 * connectionId。web/docker 部署下整页刷新后宿主重建工作台 iframe，恢复页的
 * 首次 kafka/* 调用（boot 的 kafka/topics/list）可能跑赢宿主的 connect 重放
 * （SPA 启动顺序、凭据重推晚到数秒），sidecar 以
 * `connection "x" is not connected; call connection/connect first` 拒绝。
 * 此前这类错误直接落 topicsError 终态：原始串裸透传、无自愈路径，即使重放
 * 几秒后就到（ssh 插件同类问题 dbx-plugin-ssh#144；宿主侧修复见 t8y2/dbx
 * #11015 / #11016，插件侧需自己吸收这个时序窗口）。
 *
 * 决策是纯函数，窗位常量与失败语义可单测；调用方（App.loadTopics）负责计时
 * 与序号取消。仅识别「连接注册表未就绪」这一可自愈类：认证/网络/超时等真实
 * 拨号失败重试不会更好，保持既有直接报错行为。
 */

/**
 * 「连接未就绪」类错误的识别：kafka sidecar 的注册表未命中（connection …
 * is not connected; call connection/connect first）与宿主层的
 * Connection is not active 措辞。不能放宽到任意 "not connected"——
 * franz-go 的 client closed / connection lost 是真实断连，不是暂时态。
 */
export function isConnectionInactiveError(cause: unknown): boolean {
  const message = cause instanceof Error ? cause.message : String(cause ?? "");
  return /is not connected; call connection\/connect first|connection is not active/i.test(message);
}

/**
 * Boot 恢复窗口：恢复页首次加载撞上 connect 重放晚到。固定 1s 节奏 × 12 轮
 * ≈ 12s，覆盖 ssh 实证的重推晚到间隙（与 ssh 的 BOOT_RESTORE_RETRY 对齐）。
 */
export const BOOT_RESTORE_RETRY_MAX = 12;
export const BOOT_RESTORE_RETRY_DELAY_MS = 1000;

/**
 * 手动入口窗口：用户点刷新/重连时连接由宿主重开才恢复，sidecar 注册表只在
 * 宿主 connect 重放时回填。固定 3s × 10 轮，给用户「从 DBX 左侧连接列表重新
 * 打开」留出操作时间，重开后自动自愈（与 ssh 的 INACTIVE_RETRY 对齐）。
 */
export const MANUAL_RETRY_MAX = 10;
export const MANUAL_RETRY_DELAY_MS = 3000;

export type ConnectRetryDecision = { kind: "retry"; attempt: number; delayMs: number } | { kind: "fail" };

/**
 * 决定一次失败的后端调用接下来怎么办。`attempt` 是已消耗的重试次数
 * （0 = 首次尝试）。非「连接未就绪」类错误一律 fail（保持既有行为）。
 */
export function decideConnectRetry(options: { cause: unknown; attempt: number; bootRestore: boolean }): ConnectRetryDecision {
  if (!isConnectionInactiveError(options.cause)) return { kind: "fail" };
  const max = options.bootRestore ? BOOT_RESTORE_RETRY_MAX : MANUAL_RETRY_MAX;
  const delayMs = options.bootRestore ? BOOT_RESTORE_RETRY_DELAY_MS : MANUAL_RETRY_DELAY_MS;
  if (options.attempt >= max) return { kind: "fail" };
  return { kind: "retry", attempt: options.attempt + 1, delayMs };
}
