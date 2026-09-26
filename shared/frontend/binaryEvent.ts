// vendored: DBX 插件族 monorepo `shared/frontend` 的副本（只保留本插件引用的模块）。
// vendored-sync: 2026-09-27；上游公共层演进后同步回来时更新本行日期，
// 并按 docs/REPOSITORY_SPLIT.zh-CN.md 的同步约定在拆分基线上重新验证。
// scripts/validate_repo.py 检查本标记行的存在（副本身份，防「无从判断谁新谁旧」）。
/**
 * Plugin-host bridge binary event normalization (shared by all plugins).
 *
 * The host bridge changed the sandbox binary event shape: it now transfers
 * zero-copy bytes as `data: Uint8Array`, while Host API 1.0 bridges deliver
 * `dataBase64: string` (the base64 field is gone from current bridges). Every
 * onBinary consumer — terminal output frames, SFTP/files download chunks —
 * must render on either host, so they funnel through this helper instead of
 * decoding one fixed field.
 *
 * 宿主适配公共代码一律放 shared/frontend/，插件前端以相对路径引用并在各自
 * spec 里保底测试；禁止在插件内各抄一份（见 AGENTS.md 硬性规则 7）。
 */

export interface BridgeBinaryEvent {
  channel: string;
  /** Current host bridge: zero-copy bytes transferred with the message. */
  data?: Uint8Array;
  /** Legacy host bridge: base64-encoded payload. */
  dataBase64?: string;
}

export function bridgeBinaryBytes(
  event: BridgeBinaryEvent,
  decodeBase64: (value: string) => Uint8Array,
): Uint8Array {
  if (event.data instanceof Uint8Array) return event.data;
  return decodeBase64(event.dataBase64 ?? "");
}
