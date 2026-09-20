/**
 * 时间戳格式化纯函数（自 kafkaModel 拆分）：tz 感知的表格文本 / ISO /
 * ag-grid 日期过滤比较器（F6-3/F6-6）。
 */

// -- 时间戳时区（F6-3）：formatTimestamp 带 tz 参数（缺省 local，行为不变）--------

export type TimestampTz = "local" | "utc";

/**
 * unix ms → 「YYYY-MM-DD HH:mm:ss」文本。tz=utc 用 UTC 各分量（文本与单元格
 * title 的完整 ISO 同一时区语义），tz=local 保持既有本地时区行为。
 */
export function formatTimestamp(ms: number | undefined, tz: TimestampTz = "local"): string {
  if (!ms || !Number.isFinite(ms)) return "—";
  const date = new Date(ms);
  if (Number.isNaN(date.getTime())) return String(ms);
  const pad = (value: number) => String(value).padStart(2, "0");
  if (tz === "utc") {
    return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())} ${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())}:${pad(date.getUTCSeconds())}`;
  }
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

/** unix ms → 完整 ISO-8601（单元格 title 悬停用）；非法值原样字符串化。 */
export function timestampIso(ms: number | undefined): string {
  if (!ms || !Number.isFinite(ms)) return "";
  const date = new Date(ms);
  return Number.isNaN(date.getTime()) ? String(ms) : date.toISOString();
}

/**
 * unix ms → 相对时间文本（连接列表「最近使用」等场景）：一分钟内「刚刚/just now」，
 * 之后按分钟/小时/天聚合，超过 30 天回落绝对日期。locale 跟随工作台语言，
 * 由 Intl.RelativeTimeFormat 负责翻译，无需 i18n key。
 */
export function formatRelativeTime(ms: number | string | undefined, locale = "zh-CN", nowMs = Date.now()): string {
  if (ms === undefined || ms === null || ms === "") return "";
  const parsed = typeof ms === "number" ? ms : Date.parse(ms);
  if (!Number.isFinite(parsed)) return String(ms);
  const formatter = new Intl.RelativeTimeFormat(locale, { numeric: "auto" });
  const diffMinutes = Math.round((parsed - nowMs) / 60_000);
  if (Math.abs(diffMinutes) < 1) return formatter.format(0, "minute");
  if (Math.abs(diffMinutes) < 60) return formatter.format(diffMinutes, "minute");
  if (Math.abs(diffMinutes) < 60 * 24) return formatter.format(Math.round(diffMinutes / 60), "hour");
  if (Math.abs(diffMinutes) < 60 * 24 * 30) return formatter.format(Math.round(diffMinutes / (60 * 24)), "day");
  const date = new Date(parsed);
  const pad = (value: number) => String(value).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
}

/**
 * 时间戳列 date filter 比较器（F6-6）：把单元格文本「YYYY-MM-DD HH:mm:ss」
 * 按其生成时区解析回时间后按天比较（ag-grid 传入本地零点的过滤日期）。
 * 返回 -1/0/1；无法解析的单元格值排到过滤日期之后（不算命中）。
 */
export function timestampFilterTextComparator(filterLocalDateAtMidnight: Date, cellText: unknown, tz: TimestampTz = "local"): number {
  const text = String(cellText ?? "").trim();
  const match = text.match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})$/);
  if (!match) return 1;
  const [, year, month, day, hour, minute, second] = match;
  const iso =
    tz === "utc"
      ? `${year}-${month}-${day}T${hour}:${minute}:${second}Z`
      : `${year}-${month}-${day}T${hour}:${minute}:${second}`;
  const parsed = new Date(iso);
  if (Number.isNaN(parsed.getTime())) return 1;
  const cell = new Date(parsed.getFullYear(), parsed.getMonth(), parsed.getDate());
  if (cell.getTime() === filterLocalDateAtMidnight.getTime()) return 0;
  return cell.getTime() > filterLocalDateAtMidnight.getTime() ? 1 : -1;
}
