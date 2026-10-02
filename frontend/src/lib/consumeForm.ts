/**
 * consume 表单纯逻辑（自 kafkaModel 拆分）：分区/位点文本解析、时间输入互转、
 * 表单互斥校验与 fieldFilter 行级校验。
 */

// -- consume form helpers ---------------------------------------------------------

/** "0,1,2" / "0 2"（含全角逗号）→ 去重分区号列表；非法片段忽略。 */
export function parsePartitionList(text: string): number[] {
  const seen = new Set<number>();
  for (const part of text.split(/[,，\s]+/).filter(Boolean)) {
    const value = Number.parseInt(part, 10);
    if (Number.isInteger(value) && value >= 0) seen.add(value);
  }
  return [...seen].sort((left, right) => left - right);
}

/** 组级 reset 位点目标：`topic:0=100`（显式 topic）或 `0=100`（套用唯一 topic）→ {topic:{partition:offset}}。 */
export function parseGroupOffsetTargetsText(
  text: string,
  topics: string[],
): { targets: Record<string, Record<string, number>>; invalid: string[] } {
  const targets: Record<string, Record<string, number>> = {};
  const invalid: string[] = [];
  const fallback = topics.length === 1 ? topics[0] : null;
  for (const line of text.split(/[,，\n]+/).map((entry) => entry.trim()).filter(Boolean)) {
    const explicit = line.match(/^(.+?)\s*[:：](\d+)\s*[=:]\s*(\d+)$/);
    const plain = line.match(/^(\d+)\s*[=:]\s*(\d+)$/);
    if (explicit) {
      const topic = explicit[1].trim();
      (targets[topic] ??= {})[explicit[2]] = Number.parseInt(explicit[3], 10);
    } else if (plain && fallback) {
      (targets[fallback] ??= {})[plain[1]] = Number.parseInt(plain[2], 10);
    } else {
      invalid.push(line);
    }
  }
  return { targets, invalid };
}

/** "0=100,1:200"（= 或 :，逗号/换行分隔）→ {partition:offset}；非法片段忽略。 */
export function parsePartitionOffsetsText(text: string): Record<string, number> {
  const result: Record<string, number> = {};
  for (const line of text.split(/[,，\n]+/).map((entry) => entry.trim()).filter(Boolean)) {
    const match = line.match(/^(\d+)\s*[=:]\s*(\d+)$/);
    if (match) result[match[1]] = Number.parseInt(match[2], 10);
  }
  return result;
}

export function partitionOffsetsToText(offsets: Record<string, number>): string {
  return Object.keys(offsets)
    .sort((left, right) => Number(left) - Number(right))
    .map((partition) => `${partition}=${offsets[partition]}`)
    .join(",");
}

/** datetime-local（或留空）→ RFC3339（本地时区偏移）；unix ms 字符串透传
 *  （后端 OffsetTime 为 string，ParseInt/RFC3339 双解析，number 会被拒 -32602）。 */
export function offsetTimeToParam(text: string): string | null {
  const trimmed = text.trim();
  if (!trimmed) return null;
  if (/^\d{10,}$/.test(trimmed)) return trimmed;
  if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?$/.test(trimmed)) {
    const withSeconds = trimmed.length === 16 ? `${trimmed}:00` : trimmed;
    const parsed = new Date(withSeconds);
    if (Number.isNaN(parsed.getTime())) return null;
    return parsed.toISOString();
  }
  // 已是 RFC3339 / 其他可解析时间串 → 校验后透传。
  const parsed = new Date(trimmed);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

/**
 * 时间输入 → unix ms 数字（timestampFrom/To 提交用，ConsumeParams 为 number）：
 * 复用 offsetTimeToParam（datetime-local → RFC3339、unix ms 透传），再归一到 ms。
 * 留空或不可解析返回 null。
 */
export function offsetTimeToUnixMs(text: string): number | null {
  const trimmed = text.trim();
  if (!trimmed) return null;
  if (/^\d{10,}$/.test(trimmed)) {
    const ms = Number(trimmed);
    return Number.isSafeInteger(ms) ? ms : null;
  }
  const param = offsetTimeToParam(trimmed);
  if (param === null) return null;
  const parsed = Date.parse(param);
  return Number.isNaN(parsed) ? null : parsed;
}

/** unix ms → 本地时区 datetime-local 字符串（秒级精度，`YYYY-MM-DDTHH:mm:ss`）。 */
export function unixMsToDatetimeLocal(ms: number): string {
  if (!Number.isFinite(ms)) return "";
  const date = new Date(ms);
  if (Number.isNaN(date.getTime())) return "";
  const pad = (value: number) => String(value).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

/** 当前时间 → datetime-local 字符串（「现在」按钮用）。 */
export function nowDatetimeLocal(): string {
  return unixMsToDatetimeLocal(Date.now());
}

/**
 * 时间输入模式切换（datetime-local ↔ unix ms 文本）：能解析就转换，
 * 解析不了原样保留（不打断用户正在输入的草稿）。
 */
export function switchTimeInputMode(text: string, toMode: "datetime" | "unix"): string {
  const trimmed = text.trim();
  if (!trimmed) return "";
  if (toMode === "unix") {
    const ms = offsetTimeToUnixMs(trimmed);
    return ms === null ? text : String(ms);
  }
  const ms = offsetTimeToUnixMs(trimmed);
  return ms === null ? text : unixMsToDatetimeLocal(ms);
}

/** 起止范围校验：两侧都有值且 from > to 时非法（等于合法，后端语义 from ≤ to）。 */
export function isRangeReversed(fromMs: number | null, toMs: number | null): boolean {
  return fromMs !== null && toMs !== null && fromMs > toMs;
}

/**
 * fieldFilters 行级校验：数值比较 operator（gt/gte/lt/lte）要求 value 可转数字。
 * 返回哨兵片段 "fieldValueNumeric" 或 null（通过/不适用）。注意：该片段不再
 * 对应 i18n key——展示侧（useConsumeForm.fieldFilterIssueKey）统一映射为
 * messages.uiFilterValueRequired（原 messages.fieldValueNumeric 死键已清理）。
 */
export function fieldFilterIssue(row: { operator: string; value: string }): string | null {
  if (!["gt", "gte", "lt", "lte"].includes(row.operator)) return null;
  const trimmed = row.value.trim();
  if (!trimmed) return null; // 空值行不参与载荷，交给「启用 + 有值」过滤
  return Number.isFinite(Number(trimmed)) ? null : "fieldValueNumeric";
}

export interface ConsumeFormValidationIssue {
  field: "commit" | "strategy" | "partitions";
  key: string; // i18n key 后缀（messages.errXxx）
}

/** consume 表单互斥/必填校验（§5.3：commit×过滤互斥、partitions×groupId 互斥、
 *  strategy=offset 必填 partitionOffsets、strategy=timestamp 必填 offsetTime）。 */
export function validateConsumeForm(form: {
  commit: boolean;
  groupId: string;
  partitionsText: string;
  offsetStrategy: string;
  offsetTimeText: string;
  partitionOffsetsText: string;
  hasFilters: boolean;
}): ConsumeFormValidationIssue[] {
  const issues: ConsumeFormValidationIssue[] = [];
  if (form.commit && form.hasFilters) {
    issues.push({ field: "commit", key: "commitFilterConflict" });
  }
  if (form.commit && !form.groupId.trim()) {
    issues.push({ field: "commit", key: "commitNeedsGroup" });
  }
  const partitions = parsePartitionList(form.partitionsText);
  if (partitions.length > 0 && form.groupId.trim()) {
    issues.push({ field: "partitions", key: "partitionsGroupConflict" });
  }
  if (form.offsetStrategy === "timestamp" && !offsetTimeToParam(form.offsetTimeText)) {
    issues.push({ field: "strategy", key: "timestampRequired" });
  }
  if (form.offsetStrategy === "offset") {
    const offsets = parsePartitionOffsetsText(form.partitionOffsetsText);
    if (Object.keys(offsets).length === 0) issues.push({ field: "strategy", key: "offsetsRequired" });
  }
  return issues;
}
