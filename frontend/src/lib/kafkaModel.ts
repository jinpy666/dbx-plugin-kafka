/**
 * Kafka workbench pure functions: message value decode/format pipeline,
 * topic business ranking, group lag aggregation, JSON/CSV export
 * serialization and Confluent properties parsing. No sidecar calls, no
 * framework imports — everything here is unit-testable in isolation.
 * 对标 tinyrdm ConvertValue/kafkaNormalize，重写为纯函数管线。解压支持
 * gzip/lz4/zstd/snappy 全四种：gzip 用浏览器 DecompressionStream，
 * zstd= fzstd、snappy = snappyjs、lz4 = lz4js（轻量纯 JS、MIT/ISC，
 * Phase P 引入补齐 Phase 1 的降级标注）。
 */
import { decompress as fzstdDecompress } from "fzstd";
import { decompress as lz4Decompress } from "lz4js";
import { uncompress as snappyUncompress } from "snappyjs";
import type { KafkaMessage, MatchMode } from "./api";

// -- byte helpers -------------------------------------------------------------

export function base64ToBytes(value: string): Uint8Array {
  const binary = atob(value.replace(/-/g, "+").replace(/_/g, "/"));
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) bytes[index] = binary.charCodeAt(index);
  return bytes;
}

export function bytesToHex(bytes: Uint8Array): string {
  let text = "";
  for (const byte of bytes) text += byte.toString(16).padStart(2, "0");
  return text;
}

export function bytesToUtf8(bytes: Uint8Array): string {
  // fatal:false → 非法字节替换为 U+FFFD（valueText 预览语义一致）。
  return new TextDecoder("utf-8", { fatal: false }).decode(bytes);
}

// GZip 解压：浏览器 DecompressionStream（Node 18+ 同名全局，测试可用）。
// 缺失或非 gzip 算法时返回带 error 的降级结果，不抛异常。
export function isGzipSupported(): boolean {
  return typeof DecompressionStream !== "undefined";
}

export async function inflateGzip(bytes: Uint8Array): Promise<{ bytes: Uint8Array; error?: string }> {
  if (!isGzipSupported()) return { bytes, error: "gzip: DecompressionStream unsupported" };
  try {
    const stream = new Blob([bytes as BlobPart]).stream().pipeThrough(new DecompressionStream("gzip"));
    const buffer = await new Response(stream).arrayBuffer();
    return { bytes: new Uint8Array(buffer) };
  } catch (cause) {
    return { bytes, error: `gzip: ${cause instanceof Error ? cause.message : String(cause)}` };
  }
}

// -- value format pipeline ------------------------------------------------------

export type ValueFormat = "raw" | "json" | "xml" | "hex" | "bitset";
export type DecodeMode = "none" | "base64";
export type Decompression = "none" | "gzip" | "lz4" | "zstd" | "snappy";

export interface ValueFormatOptions {
  decode: DecodeMode;
  decompression: Decompression;
  format: ValueFormat;
}

export interface DecodedValue {
  text: string;
  error?: string;
}

// JSON pretty：可解析才格式化，否则原样返回（不视为错误，raw 兜底）。
export function prettyJson(text: string): string {
  try {
    const parsed = JSON.parse(text);
    return JSON.stringify(parsed, null, 2);
  } catch {
    return text;
  }
}

export function looksLikeJson(text: string): boolean {
  const trimmed = text.trim();
  if (!trimmed.startsWith("{") && !trimmed.startsWith("[")) return false;
  try {
    JSON.parse(trimmed);
    return true;
  } catch {
    return false;
  }
}

export function looksLikeXml(text: string): boolean {
  return /^\s*<\s*[A-Za-z_?![]/.test(text);
}

/**
 * XML 格式化（词法级缩进，不做 DOM 解析）。
 *
 * 安全红线（不可信载荷）：含 DOCTYPE/ENTITY 的文本一律原样返回——不解析、
 * 不展开、不重排，杜绝实体歧义；良构性只做词法校验（标签开闭栈匹配），
 * 不平衡即放弃重排。实体引用（&amp; 等）按原样保留在文本节点中。
 */
export function prettyXml(text: string): string {
  const trimmed = text.trim();
  if (!trimmed.startsWith("<")) return text;
  if (/<!DOCTYPE|<!ENTITY/i.test(trimmed)) return text;
  const tokens = trimmed.match(/<!--[\s\S]*?-->|<!\[CDATA\[[\s\S]*?\]\]>|<[^>]+>|[^<]+|</g);
  if (!tokens) return text;

  // 良构校验（词法级）：注释/CDATA/PI/声明跳过，开闭标签名栈匹配。
  const stack: string[] = [];
  const nameOf = (tag: string): string => tag.match(/^<\/?\s*([A-Za-z_][\w.:-]*)/)?.[1] ?? "";
  for (const token of tokens) {
    if (!token.startsWith("<")) continue;
    if (token.startsWith("<!--") || token.startsWith("<![") || token.startsWith("<?") || token.startsWith("<!")) continue;
    if (token.startsWith("</")) {
      if (stack.pop() !== nameOf(token)) return text;
      continue;
    }
    if (token.endsWith("/>")) continue;
    const name = nameOf(token);
    if (!name) return text;
    stack.push(name);
  }
  if (stack.length !== 0) return text;

  const lines: string[] = [];
  let depth = 0;
  let index = 0;
  while (index < tokens.length) {
    const token = tokens[index];
    // `<a>text</a>` 三连同行（最常见的紧凑叶子节点）。
    if (
      token.startsWith("<") &&
      !token.startsWith("</") &&
      !token.endsWith("/>") &&
      !token.startsWith("<!--") &&
      !token.startsWith("<![") &&
      !token.startsWith("<?") &&
      !token.startsWith("<!")
    ) {
      const textNode = tokens[index + 1];
      const close = tokens[index + 2];
      if (textNode !== undefined && !textNode.startsWith("<") && textNode.trim() && close !== undefined && close.startsWith("</")) {
        lines.push("  ".repeat(depth) + token.trim() + textNode.trim() + close.trim());
        index += 3;
        continue;
      }
    }
    if (token.startsWith("</")) {
      depth = Math.max(0, depth - 1);
      lines.push("  ".repeat(depth) + token.trim());
    } else if (!token.startsWith("<")) {
      if (token.trim()) lines.push("  ".repeat(depth) + token.trim());
    } else {
      lines.push("  ".repeat(depth) + token.trim());
      if (
        !token.endsWith("/>") &&
        !token.startsWith("<!--") &&
        !token.startsWith("<![") &&
        !token.startsWith("<?") &&
        !token.startsWith("<!")
      ) {
        depth += 1;
      }
    }
    index += 1;
  }
  return lines.join("\n");
}

// BitSet 展示：把整数字符串（十进制/0x 十六进制/二进制字面量）转成从
// 高位到低位的 0/1 串，按 8 位分组（tinyrdm BitSet 语义的纯函数版）。
export function formatBitSet(text: string): string | null {
  const trimmed = text.trim();
  if (!/^(0x[0-9a-f]+|0b[01]+|\d+)$/i.test(trimmed)) return null;
  let value = BigInt(trimmed);
  if (value === 0n) return "0";
  const bits: string[] = [];
  while (value > 0n) {
    bits.unshift(value & 1n ? "1" : "0");
    value >>= 1n;
  }
  return bits.join("").replace(/\B(?=(\d{4})+(?!\d))/g, " ");
}

const DECOMPRESSION_LABELS: Record<string, string> = { gzip: "gzip", lz4: "lz4", zstd: "zstd", snappy: "snappy" };

function decompressError(algorithm: string, cause: unknown): string {
  return `${DECOMPRESSION_LABELS[algorithm] ?? algorithm}: ${cause instanceof Error ? cause.message : String(cause)}`;
}

// zstd 解压：fzstd（纯 JS、MIT；仅解码，与生产端 zstd 帧格式兼容）。
export function inflateZstd(bytes: Uint8Array): { bytes: Uint8Array; error?: string } {
  try {
    return { bytes: fzstdDecompress(bytes) };
  } catch (cause) {
    return { bytes, error: decompressError("zstd", cause) };
  }
}

// snappy 解压：snappyjs（纯 JS、MIT，含 Hadoop 变体外的标准 framing）。
export function inflateSnappy(bytes: Uint8Array): { bytes: Uint8Array; error?: string } {
  try {
    return { bytes: snappyUncompress(bytes) };
  } catch (cause) {
    return { bytes, error: decompressError("snappy", cause) };
  }
}

// lz4 解压：lz4js（纯 JS、ISC，frame 格式，与 Kafka lz4 块兼容）。
export function inflateLz4(bytes: Uint8Array): { bytes: Uint8Array; error?: string } {
  try {
    return { bytes: new Uint8Array(lz4Decompress(bytes)) };
  } catch (cause) {
    return { bytes, error: decompressError("lz4", cause) };
  }
}

/** 单步解压分发（算法不存在时返回原字节 + error，不抛异常）。 */
export function inflateDecompression(algorithm: Decompression, bytes: Uint8Array): { bytes: Uint8Array; error?: string } {
  switch (algorithm) {
    case "zstd":
      return inflateZstd(bytes);
    case "snappy":
      return inflateSnappy(bytes);
    case "lz4":
      return inflateLz4(bytes);
    default:
      return { bytes, error: `decompression "${algorithm}" is not supported` };
  }
}

/**
 * 消息 value 二次解码/格式化（详情抽屉主流程）：
 * valueBase64（保真字节）→ [可选 base64 再解] → [可选 gzip/lz4/zstd/snappy 解压]
 * → 格式化。返回 { text, error? }；error 表示管线中不可恢复的一步（展示而非抛出）。
 */
export async function formatMessageValue(message: KafkaMessage, options: ValueFormatOptions): Promise<DecodedValue> {
  const encoded = message.valueBase64 ?? "";
  let bytes: Uint8Array;
  try {
    bytes = base64ToBytes(encoded);
  } catch {
    return { text: message.valueText ?? "", error: "invalid value base64" };
  }
  let error: string | undefined;
  if (options.decode === "base64") {
    const inner = bytesToUtf8(bytes).trim();
    try {
      bytes = base64ToBytes(inner);
    } catch {
      error = "invalid inner base64 (decode=base64)";
    }
  }
  if (options.decompression !== "none" && !error) {
    const algorithm = options.decompression;
    // gzip 走浏览器 DecompressionStream，其余走纯 JS 解压器（均为同步）。
    const inflated = algorithm === "gzip" ? await inflateGzip(bytes) : inflateDecompression(algorithm, bytes);
    bytes = inflated.bytes;
    error = inflated.error;
  }
  const text = bytesToUtf8(bytes);
  switch (options.format) {
    case "json":
      return { text: prettyJson(text), error };
    case "xml":
      return { text: prettyXml(text), error };
    case "hex":
      return { text: bytesToHex(bytes).replace(/(..)(?=.)/g, "$1 "), error };
    case "bitset": {
      const bitSet = formatBitSet(text);
      return bitSet === null ? { text, error: error ?? "value is not an integer (bitset)" } : { text: bitSet, error };
    }
    default:
      return { text, error };
  }
}

// 下载用完整 value（始终 base64 → bytes → UTF-8 保真文本，不受预览替换影响）。
export function messageFullValueText(message: KafkaMessage): string {
  if (!message.valueBase64) return message.valueText ?? "";
  try {
    return bytesToUtf8(base64ToBytes(message.valueBase64));
  } catch {
    return message.valueText ?? "";
  }
}

// -- topic ranking（对标 tinyrdm kafkaNormalize.scoreBusinessTopicName）---------

const BUSINESS_TOKENS = new Set([
  "account", "activity", "audit", "cart", "customer", "email", "event", "events", "invoice", "item",
  "message", "messages", "notification", "order", "orders", "payment", "product", "profile", "session",
  "transaction", "user", "users",
]);
const INFRA_TOKENS = new Set([
  "changelog", "command", "config", "connect", "dlq", "heartbeat", "offset", "offsets", "repartition", "retry",
]);

export function scoreTopicName(name: string): number {
  const normalized = String(name || "").toLowerCase();
  const tokens = normalized.split(/[._-]+/).filter(Boolean);
  const business = tokens.reduce((acc, token) => acc + (BUSINESS_TOKENS.has(token) ? 24 : 0), 0);
  const infra = tokens.reduce((acc, token) => acc + (INFRA_TOKENS.has(token) ? 18 : 0), 0);
  const structure = tokens.length > 1 ? 8 : 0;
  const readable = normalized.length >= 6 && /[a-z]/.test(normalized) ? 4 : 0;
  return business + structure + readable - infra;
}

export function isInternalTopicName(name: string): boolean {
  return String(name || "").startsWith("_");
}

export interface TopicSortItem {
  name: string;
  isInternal?: boolean;
}

/**
 * topic 排序：internal 沉底（_ 开头或后端标记），业务 topic 按业务评分
 * 降序、同分按名称字典序。返回新数组，不改入参。
 */
export function sortTopics<T extends TopicSortItem>(topics: T[]): T[] {
  return [...topics].sort((left, right) => {
    const leftInternal = left.isInternal === true || isInternalTopicName(left.name);
    const rightInternal = right.isInternal === true || isInternalTopicName(right.name);
    if (leftInternal !== rightInternal) return leftInternal ? 1 : -1;
    const scoreDelta = scoreTopicName(right.name) - scoreTopicName(left.name);
    if (scoreDelta !== 0) return scoreDelta;
    return left.name.localeCompare(right.name);
  });
}

export function filterTopics<T extends TopicSortItem>(topics: T[], keyword: string): T[] {
  const needle = keyword.trim().toLowerCase();
  if (!needle) return topics;
  return topics.filter((topic) => topic.name.toLowerCase().includes(needle));
}

/**
 * 收藏置顶排序（Lane4 前端打磨）：先按 sortTopics 排（业务评分 + internal
 * 沉底不变），再把收藏项稳定提前。收藏项内部保持同一相对顺序；internal topic
 * 被收藏时同样置顶（用户显式收藏优先于 internal 沉底）。返回新数组，不改入参。
 */
export function sortTopicsPinned<T extends TopicSortItem>(topics: T[], pinnedNames: ReadonlySet<string>): T[] {
  const sorted = sortTopics(topics);
  if (pinnedNames.size === 0) return sorted;
  return [...sorted.filter((topic) => pinnedNames.has(topic.name)), ...sorted.filter((topic) => !pinnedNames.has(topic.name))];
}

// -- lag aggregation ------------------------------------------------------------

export interface LagRow {
  lag?: number | null;
}

/** 分区 lag 求和；缺失/负值按 0 计（Option 语义的展示兜底在后端）。 */
export function sumLag(rows: LagRow[]): number {
  return rows.reduce((acc, row) => acc + Math.max(0, Number(row.lag ?? 0) || 0), 0);
}

// -- export serialization --------------------------------------------------------

function csvEscape(value: string): string {
  if (/[",\n\r]/.test(value)) return `"${value.replace(/"/g, '""')}"`;
  return value;
}

const CSV_COLUMNS = ["topic", "partition", "offset", "timestamp", "key", "value", "headers"] as const;

/** headers 列文本（CSV/TSV 共用）：k=v;… 连接，空 headers 返回空串。 */
function exportHeadersText(message: KafkaMessage): string {
  return message.headers && Object.keys(message.headers).length > 0
    ? Object.entries(message.headers)
        .map(([key, value]) => `${key}=${value}`)
        .join("; ")
    : "";
}

/** 消息数组 → CSV 文本（RFC 4180 转义；headers 序列化为 k=v;… JSON 兜底）。 */
export function serializeMessagesToCsv(messages: KafkaMessage[]): string {
  const lines = [CSV_COLUMNS.join(",")];
  for (const message of messages) {
    lines.push(
      [
        message.topic,
        String(message.partition),
        String(message.offset),
        String(message.timestamp),
        message.key ?? "",
        messageFullValueText(message),
        exportHeadersText(message),
      ]
        .map(csvEscape)
        .join(","),
    );
  }
  return lines.join("\r\n");
}

// TSV 转义与 CSV 不同：无引号包裹机制，字段内的分隔符（制表符）与换行必须
// 转义才能保列/行结构；反斜杠先转义避免歧义（对齐 Hive/MySQL LOAD DATA 约定）。
function tsvEscape(value: string): string {
  return value.replace(/\\/g, "\\\\").replace(/\t/g, "\\t").replace(/\n/g, "\\n").replace(/\r/g, "\\r");
}

/** 消息数组 → TSV 文本（列序与 CSV 一致；转义见 tsvEscape，行分隔同为 CRLF）。 */
export function serializeMessagesToTsv(messages: KafkaMessage[]): string {
  const lines = [CSV_COLUMNS.join("\t")];
  for (const message of messages) {
    lines.push(
      [
        message.topic,
        String(message.partition),
        String(message.offset),
        String(message.timestamp),
        message.key ?? "",
        messageFullValueText(message),
        exportHeadersText(message),
      ]
        .map(tsvEscape)
        .join("\t"),
    );
  }
  return lines.join("\r\n");
}

/** 消息数组 → JSON 文本（稳定键序，value 用保真文本）。 */
export function serializeMessagesToJson(messages: KafkaMessage[]): string {
  const payload = messages.map((message) => ({
    topic: message.topic,
    partition: message.partition,
    offset: message.offset,
    timestamp: message.timestamp,
    ...(message.leaderEpoch !== undefined ? { leaderEpoch: message.leaderEpoch } : {}),
    ...(message.key !== undefined ? { key: message.key } : {}),
    value: messageFullValueText(message),
    headers: message.headers ?? {},
    ...(message.committed !== undefined ? { committed: message.committed } : {}),
    ...(message.truncated ? { truncated: true } : {}),
    ...(message.decodeError ? { decodeError: message.decodeError } : {}),
  }));
  return JSON.stringify(payload, null, 2);
}

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

/** datetime-local（或留空）→ RFC3339（本地时区偏移）；unix ms 数字原样透传。 */
export function offsetTimeToParam(text: string): string | number | null {
  const trimmed = text.trim();
  if (!trimmed) return null;
  if (/^\d{10,}$/.test(trimmed)) return Number(trimmed);
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
  const param = offsetTimeToParam(text);
  if (param === null) return null;
  if (typeof param === "number") return param;
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
 * 返回 i18n key 片段（messages.fieldValueNumeric）或 null（通过/不适用）。
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

// -- matchMode 本地预览（后端为准，前端仅做详情过滤提示/导出前二次确认用）-----

export function matchText(candidate: string | undefined, pattern: string, mode: MatchMode): boolean {
  const haystack = candidate ?? "";
  switch (mode) {
    case "prefix":
      return haystack.startsWith(pattern);
    case "exact":
      return haystack === pattern;
    case "regex":
      try {
        return new RegExp(pattern).test(haystack);
      } catch {
        return false;
      }
    default:
      return haystack.includes(pattern);
  }
}

// -- headers JSON / Confluent properties ------------------------------------------

/** headers 编辑框 JSON 对象校验（值必须是 string；其余键值报错）。 */
export function parseHeadersJson(text: string): { headers: Record<string, string> } | { error: string } {
  const trimmed = text.trim();
  if (!trimmed) return { headers: {} };
  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch (cause) {
    return { error: cause instanceof Error ? cause.message : String(cause) };
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    return { error: "headers must be a JSON object" };
  }
  const headers: Record<string, string> = {};
  for (const [key, value] of Object.entries(parsed as Record<string, unknown>)) {
    if (typeof value !== "string") return { error: `header "${key}" must be a string` };
    headers[key] = value;
  }
  return { headers };
}

/**
 * 定位 JSON 首个语法错误所在行（1-based）。合法 JSON、空/纯空白文本返回 null
 * （空态不算错——编辑器 linter 语义）。不解析 JSON.parse 的报错文案：V8 新版
 * 对不少输入不再携带 position、WebKit/Firefox 格式各异，改用内置最小 JSON
 * 扫描器，跨引擎（Electron/Chromium、CI Node）行为一致。
 */
export function jsonErrorLine(text: string): number | null {
  const src = String(text ?? "");
  if (!src.trim()) return null;
  const length = src.length;
  let pos = 0;
  let line = 1;

  function skipWs(): void {
    while (pos < length) {
      const ch = src[pos];
      if (ch === "\n") {
        pos += 1;
        line += 1;
      } else if (ch === " " || ch === "\t" || ch === "\r") {
        pos += 1;
      } else {
        break;
      }
    }
  }

  function matchKeyword(word: string): boolean {
    if (src.startsWith(word, pos)) {
      pos += word.length;
      return true;
    }
    return false;
  }

  function scanString(): boolean {
    pos += 1; // 开引号
    while (pos < length) {
      const ch = src[pos];
      if (ch === '"') {
        pos += 1;
        return true;
      }
      if (ch === "\\") {
        const esc = src[pos + 1];
        if (esc === "u") {
          if (!/^[0-9a-fA-F]{4}$/.test(src.slice(pos + 2, pos + 6))) return false;
          pos += 6;
          continue;
        }
        if (esc === undefined || !"\"\\/bfnrt".includes(esc)) return false;
        pos += 2;
        continue;
      }
      // JSON 字符串内不允许字面控制字符（含裸换行），在此处报错。
      if (ch < " ") return false;
      pos += 1;
    }
    return false; // 未闭合
  }

  function scanNumber(): boolean {
    if (src[pos] === "-") pos += 1;
    if (src[pos] === "0") {
      pos += 1;
    } else if (src[pos]! >= "1" && src[pos]! <= "9") {
      while (pos < length && src[pos] >= "0" && src[pos] <= "9") pos += 1;
    } else {
      return false;
    }
    if (src[pos] === ".") {
      pos += 1;
      if (!(pos < length && src[pos] >= "0" && src[pos] <= "9")) return false;
      while (pos < length && src[pos] >= "0" && src[pos] <= "9") pos += 1;
    }
    if (src[pos] === "e" || src[pos] === "E") {
      pos += 1;
      if (src[pos] === "+" || src[pos] === "-") pos += 1;
      if (!(pos < length && src[pos] >= "0" && src[pos] <= "9")) return false;
      while (pos < length && src[pos] >= "0" && src[pos] <= "9") pos += 1;
    }
    return true;
  }

  function scanObject(): boolean {
    pos += 1; // {
    skipWs();
    if (src[pos] === "}") {
      pos += 1;
      return true;
    }
    for (;;) {
      skipWs();
      if (src[pos] !== '"') return false;
      if (!scanString()) return false;
      skipWs();
      if (src[pos] !== ":") return false;
      pos += 1;
      if (!scanValue()) return false;
      skipWs();
      if (src[pos] === ",") {
        pos += 1;
        continue;
      }
      if (src[pos] === "}") {
        pos += 1;
        return true;
      }
      return false;
    }
  }

  function scanArray(): boolean {
    pos += 1; // [
    skipWs();
    if (src[pos] === "]") {
      pos += 1;
      return true;
    }
    for (;;) {
      if (!scanValue()) return false;
      skipWs();
      if (src[pos] === ",") {
        pos += 1;
        continue;
      }
      if (src[pos] === "]") {
        pos += 1;
        return true;
      }
      return false;
    }
  }

  function scanValue(): boolean {
    skipWs();
    if (pos >= length) return false;
    const ch = src[pos];
    if (ch === "{") return scanObject();
    if (ch === "[") return scanArray();
    if (ch === '"') return scanString();
    if (ch === "-" || (ch >= "0" && ch <= "9")) return scanNumber();
    return matchKeyword("true") || matchKeyword("false") || matchKeyword("null");
  }

  if (!scanValue()) return line;
  skipWs();
  return pos < length ? line : null; // 尾部还有非空白内容 = 多余 token
}

export interface ConfluentProperties {
  [key: string]: string;
}

/**
 * Confluent properties 文本解析（`key=value` 行，# / ! 注释，`\` 续行，
 * 值内 `\:=` 等反斜杠转义）。纯解析，不做语义校验。
 */
export function parsePropertiesText(text: string): ConfluentProperties {
  const result: ConfluentProperties = {};
  // 续行：行尾奇数个反斜杠才生效（偶数个是转义的字面反斜杠）。
  const logical: string[] = [];
  let pending = "";
  for (const rawLine of text.split(/\r?\n/)) {
    const line = pending + rawLine;
    const trailing = /\\+$/.exec(line)?.[0].length ?? 0;
    if (trailing % 2 === 1) {
      pending = line.slice(0, -1);
      continue;
    }
    pending = "";
    logical.push(line);
  }
  if (pending) logical.push(pending);
  for (const line of logical) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#") || trimmed.startsWith("!")) continue;
    // 第一个未被转义的 = 或 : 为分隔符（\ 后跳过一个字符）。
    let separatorIndex = -1;
    for (let index = 0; index < trimmed.length; index += 1) {
      const character = trimmed[index];
      if (character === "\\") {
        index += 1;
        continue;
      }
      if (character === "=" || character === ":") {
        separatorIndex = index;
        break;
      }
    }
    if (separatorIndex <= 0) continue;
    const key = unescapeProperties(trimmed.slice(0, separatorIndex)).trim();
    const value = unescapeProperties(trimmed.slice(separatorIndex + 1).replace(/^[ \t]+/, "")).trimEnd();
    if (key) result[key] = value;
  }
  return result;
}

function unescapeProperties(input: string): string {
  let output = "";
  for (let index = 0; index < input.length; index += 1) {
    const character = input[index];
    if (character === "\\" && index + 1 < input.length) {
      output += input[index + 1];
      index += 1;
    } else {
      output += character;
    }
  }
  return output;
}

export interface ConnectionFormHints {
  bootstrapServers: string;
  securityProtocol: string;
  saslMechanism: string;
  saslUsername: string;
  saslPassword: string;
  tlsInsecureSkipVerify: boolean;
}

/**
 * Confluent properties → manifest 连接表单字段映射（§4）：
 * bootstrap.servers / security.protocol / sasl.mechanism /
 * sasl.jaas.config（提取 username/password）/ ssl.endpoint.identification.algorithm。
 * 跳过校验仅在 key 显式置空或 none 时为 true；key 缺省保持宿主默认校验。
 */
export function propertiesToConnectionForm(properties: ConfluentProperties): ConnectionFormHints {
  const bootstrap = properties["bootstrap.servers"] ?? "";
  const securityProtocol = (properties["security.protocol"] ?? "").toUpperCase();
  const saslMechanism = (properties["sasl.mechanism"] ?? "").toUpperCase();
  const jaas = properties["sasl.jaas.config"] ?? "";
  const username = jaas.match(/(?:^|\s)username\s*=\s*"([^"]*)"/)?.[1] ?? "";
  const password = jaas.match(/(?:^|\s)password\s*=\s*"([^"]*)"/)?.[1] ?? "";
  const endpointAlgorithm = properties["ssl.endpoint.identification.algorithm"];
  const skipVerify = endpointAlgorithm !== undefined && /^(|none)$/i.test(endpointAlgorithm.trim());
  return {
    bootstrapServers: bootstrap,
    securityProtocol,
    saslMechanism,
    saslUsername: username,
    saslPassword: password,
    tlsInsecureSkipVerify: skipVerify,
  };
}

// -- display helpers ---------------------------------------------------------------
// formatTimestamp / timestampIso 带 tz 语义的实现移到文件后段「时间戳时区」一节。

/** 消息表格 value 预览：单行化 + 截断（详情抽屉看全文）。 */
export function previewText(text: string | undefined, maxLength = 120): string {
  const singleLine = (text ?? "").replace(/\s+/g, " ").trim();
  return singleLine.length > maxLength ? `${singleLine.slice(0, maxLength)}…` : singleLine;
}

// -- message table cap（消息页大数据量防护）------------------------------------------

/**
 * 消息表内存行上限：一次性 consume 大批量结果只保留最新 N 条，超出裁掉头部
 * （较早的），防大表 ag-grid 全量行 + 深响应爆内存。StreamPanel 有独立的
 * STREAM_ROWS_MAX（流式环形缓冲），两者语义不同不共用。
 */
export const MESSAGE_ROWS_MAX = 10000;

/** 裁剪结果：rows 为保留的最新行、total 为裁剪前行数、dropped 为裁掉数量。 */
export interface CappedRows<T> {
  rows: T[];
  total: number;
  dropped: number;
}

/**
 * 行数据上限裁剪（纯函数）：rows 语义为时间正序（新消息在尾部），保留最新
 * max 条、裁掉头部。未超限返回原数组引用（零拷贝）；超限 slice 裁头部。
 */
export function capRows<T>(rows: T[], max: number = MESSAGE_ROWS_MAX): CappedRows<T> {
  if (rows.length <= max) return { rows, total: rows.length, dropped: 0 };
  const dropped = rows.length - max;
  return { rows: rows.slice(dropped), total: rows.length, dropped };
}

/** 详情抽屉 value 预览上限（字符）：大 value（如 512KB base64）不整段塞 DOM 文本节点。 */
export const DETAIL_VALUE_PREVIEW_MAX = 16384;

/**
 * 大 value 截断预览（纯函数）：保留头 80% + 尾 400 字符（头尾可判别格式/内容），
 * 中间以语言中性的省略标记连接（数字+chars）。未超限原样返回（零拷贝语义）。
 */
export function truncatedValuePreview(text: string, previewMax = DETAIL_VALUE_PREVIEW_MAX): { text: string; truncated: boolean } {
  if (text.length <= previewMax) return { text, truncated: false };
  const head = Math.max(previewMax - 400, 0);
  const hidden = text.length - head - 400;
  return { text: `${text.slice(0, head)}\n⋯ ${hidden} chars ⋯\n${text.slice(-400)}`, truncated: true };
}

// -- stream buffer ----------------------------------------------------------------

/** 流式面板展示上限（超出丢最旧并累计 dropped，防长会话 OOM）。 */
export const STREAM_ROWS_MAX = 1000;

/** 追加流式消息行并按上限裁剪；返回新数组与累计丢弃数（纯函数）。 */
export function appendStreamRows(
  current: KafkaMessage[],
  incoming: KafkaMessage[],
  max = STREAM_ROWS_MAX,
  droppedSoFar = 0,
): { rows: KafkaMessage[]; dropped: number } {
  const combined = [...current, ...incoming];
  if (combined.length <= max) return { rows: combined, dropped: droppedSoFar };
  const overflow = combined.length - max;
  return { rows: combined.slice(overflow), dropped: droppedSoFar + overflow };
}

/** 表格 headers 摘要：k=v, k2=v2（最多 2 个）。 */
export function headersPreview(headers: Record<string, string> | undefined): string {
  if (!headers) return "";
  const entries = Object.entries(headers);
  const head = entries.slice(0, 2).map(([key, value]) => `${key}=${value}`).join(", ");
  return entries.length > 2 ? `${head}, …` : head;
}

// -- Confluent properties 导入助手（Phase 2，只读映射展示，不回填不持久化）---------

export const PROPERTY_MASKED_PLACEHOLDER = "••••••";

export interface PropertyMappingRow {
  /** properties 原始键（或带提取标注的子键）。 */
  property: string;
  /** 解析出的值；敏感值以掩码占位，不携带明文。 */
  value: string;
  masked: boolean;
  /** 对应宿主连接表单字段（manifest 连接字段名）。 */
  formField: string;
}

/**
 * Confluent properties → 只读键值映射（Phase 2 导入助手展示用）：
 * bootstrap.servers / security.protocol / sasl.mechanism（含 GSSAPI 提示）/
 * sasl.jaas.config（username/password/principal/keyTab 提取）/
 * schema.registry.url / schema.registry.basic.auth.user.info。
 * 敏感值（密码/secret）一律以掩码占位；纯函数，不触碰 localStorage。
 */
export function buildPropertyMappings(properties: ConfluentProperties): PropertyMappingRow[] {
  const rows: PropertyMappingRow[] = [];
  const push = (property: string, value: string, formField: string, masked = false) => {
    if (value) rows.push({ property, value, masked, formField });
  };
  push("bootstrap.servers", properties["bootstrap.servers"] ?? "", "bootstrap_servers");
  push("security.protocol", (properties["security.protocol"] ?? "").toUpperCase(), "security_protocol");
  const mechanism = (properties["sasl.mechanism"] ?? "").toUpperCase();
  if (mechanism) {
    push("sasl.mechanism", mechanism, mechanism === "GSSAPI" ? "sasl_mechanism (GSSAPI) + kerberos_*" : "sasl_mechanism");
  }
  const jaas = properties["sasl.jaas.config"] ?? "";
  if (jaas) {
    const username = jaas.match(/(?:^|\s)username\s*=\s*"([^"]*)"/)?.[1] ?? "";
    const password = jaas.match(/(?:^|\s)password\s*=\s*"([^"]*)"/)?.[1] ?? "";
    const principal = jaas.match(/(?:^|\s)principal\s*=\s*"([^"]*)"/)?.[1] ?? "";
    const keytab = jaas.match(/(?:^|\s)keyTab\s*=\s*"([^"]*)"/)?.[1] ?? "";
    if (username) push("sasl.jaas.config → username", username, "sasl_username");
    if (password) push("sasl.jaas.config → password", PROPERTY_MASKED_PLACEHOLDER, "sasl_password", true);
    if (principal) push("sasl.jaas.config → principal", principal, "kerberos_principal");
    if (keytab) push("sasl.jaas.config → keyTab", keytab, "kerberos_keytab_path");
  }
  push("sasl.kerberos.service.name", properties["sasl.kerberos.service.name"] ?? "", "kerberos_service_name");
  push("schema.registry.url", properties["schema.registry.url"] ?? "", "sr_url");
  const srAuth = properties["schema.registry.basic.auth.user.info"] ?? "";
  if (srAuth) {
    const separatorIndex = srAuth.indexOf(":");
    if (separatorIndex > 0) {
      push("schema.registry.basic.auth.user.info → user", srAuth.slice(0, separatorIndex), "sr_username");
      push("schema.registry.basic.auth.user.info → secret", PROPERTY_MASKED_PLACEHOLDER, "sr_password", true);
    }
  }
  return rows;
}

// -- 弹层交互纯逻辑（P1-2/P1-3：Esc 关闭 + Tab 焦点陷阱）------------------------
// DOM 接线在各弹层组件（消息详情抽屉 / 连接弹窗），本节只放可单测的决策逻辑。

/** 容器内可聚焦元素选择器（disabled / hidden input / tabindex=-1 除外）。 */
export const FOCUSABLE_SELECTOR = [
  "a[href]",
  "button:not([disabled])",
  'input:not([disabled]):not([type="hidden"])',
  "select:not([disabled])",
  "textarea:not([disabled])",
  '[tabindex]:not([tabindex="-1"])',
].join(", ");

/** 容器内文档顺序的可聚焦元素列表。 */
export function focusableElements(root: ParentNode): HTMLElement[] {
  return Array.from(root.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR));
}

/** Tab 焦点陷阱回绕：无焦点/越界时按方向取首/尾，否则循环步进；空容器返回 -1。 */
export function nextFocusIndex(count: number, currentIndex: number, shift: boolean): number {
  if (count <= 0) return -1;
  if (currentIndex < 0 || currentIndex >= count) return shift ? count - 1 : 0;
  return (currentIndex + (shift ? -1 : 1) + count) % count;
}

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

// -- 即时搜索（F6-1）：通用防抖 + 流式面板已加载行过滤 ---------------------------

/** 防抖：延迟 ms 内重复调用只保留最后一次；返回的 cancel 丢弃未执行调用。 */
export function debounce<A extends unknown[]>(fn: (...args: A) => void, waitMs: number): ((...args: A) => void) & { cancel: () => void } {
  let timer: number | undefined;
  const wrapped = (...args: A) => {
    window.clearTimeout(timer);
    timer = window.setTimeout(() => fn(...args), waitMs);
  };
  wrapped.cancel = () => window.clearTimeout(timer);
  return wrapped;
}

/** 流式面板即时搜索：keyword 按 contains 匹配 partition/offset/key/value/headers 文本。 */
export function filterMessagesByKeyword<T extends { partition: number; offset: number; key?: string; valueText?: string; headers?: Record<string, string> }>(messages: T[], keyword: string): T[] {
  const needle = keyword.trim().toLowerCase();
  if (!needle) return messages;
  return messages.filter((message) => {
    const haystack = [
      String(message.partition),
      String(message.offset),
      message.key ?? "",
      message.valueText ?? "",
      Object.entries(message.headers ?? {}).map(([key, value]) => `${key}=${value}`).join(","),
    ].join("\n");
    return haystack.toLowerCase().includes(needle);
  });
}

// -- 复制族（F6-2）：navigator.clipboard 优先，execCommand 兜底 ----------------------

/** 写剪贴板：clipboard API 不可用或拒绝时用隐藏 textarea + execCommand 兜底。 */
export async function copyTextToClipboard(text: string): Promise<boolean> {
  const value = String(text ?? "");
  if (!value) return false;
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(value);
      return true;
    }
  } catch {
    /* 拒绝/不可用 → 落到 execCommand 兜底 */
  }
  try {
    const textarea = document.createElement("textarea");
    textarea.value = value;
    textarea.setAttribute("readonly", "true");
    textarea.style.position = "fixed";
    textarea.style.opacity = "0";
    document.body.appendChild(textarea);
    textarea.select();
    const ok = document.execCommand("copy");
    textarea.remove();
    return ok;
  } catch {
    return false;
  }
}

// -- 生产分区校验（F6-4）：partition 超出所选 topic 分区数的行内校验 ----------------

/**
 * partition 输入校验：空 = 自动分区（合法）；非负整数且 < partitionCount 才合法。
 * partitionCount 缺省（topic 未带健康度/列表未到）时不做上界校验。
 * 返回 i18n 键片段（produce.partitionInvalid / produce.partitionOutOfRange）或 null。
 */
export function partitionInputIssue(text: string, partitionCount?: number): string | null {
  const trimmed = String(text ?? "").trim();
  if (!trimmed) return null;
  if (!/^\d+$/.test(trimmed)) return "partitionInvalid";
  if (partitionCount !== undefined && partitionCount > 0 && Number(trimmed) >= partitionCount) return "partitionOutOfRange";
  return null;
}

// -- OAUTHBEARER 连接表单联动（F2 前端半件；冻结契约 §12.2.3，json camelCase）------

export type OauthTokenSource = "msk_iam" | "static_token";

/** OAUTHBEARER 各字段组可见性（镜像 manifest visible_when 联动链，供 spec/摘要共用）。 */
export interface OauthFormVisibility {
  /** oauth_token_source：security_protocol 含 SASL 且 mechanism=OAUTHBEARER。 */
  oauthTokenSource: boolean;
  /** msk_* 组：token source = msk_iam。 */
  mskFields: boolean;
  /** oauth_static_token：token source = static_token。 */
  staticToken: boolean;
}

const SASL_PROTOCOLS = new Set(["SASL_PLAINTEXT", "SASL_SSL"]);

function normalizeTokenSource(source: string): OauthTokenSource | "" {
  const normalized = String(source ?? "").trim().toLowerCase();
  return normalized === "msk_iam" || normalized === "static_token" ? normalized : "";
}

/** security_protocol × sasl_mechanism × oauth_token_source → 字段组可见性。 */
export function oauthFormVisibility(securityProtocol: string, saslMechanism: string, oauthTokenSource: string): OauthFormVisibility {
  const sasl = SASL_PROTOCOLS.has(String(securityProtocol ?? "").trim().toUpperCase());
  const oauth = String(saslMechanism ?? "").trim().toUpperCase() === "OAUTHBEARER";
  if (!sasl || !oauth) return { oauthTokenSource: false, mskFields: false, staticToken: false };
  const source = normalizeTokenSource(oauthTokenSource);
  return { oauthTokenSource: true, mskFields: source === "msk_iam", staticToken: source === "static_token" };
}

/** OAUTHBEARER 要求 SASL_SSL（否则后端 -32602）：表单 hint 用。 */
export function oauthRequiresSaslSsl(securityProtocol: string, saslMechanism: string): boolean {
  const protocol = String(securityProtocol ?? "").trim().toUpperCase();
  return String(saslMechanism ?? "").trim().toUpperCase() === "OAUTHBEARER" && protocol !== "SASL_SSL";
}

// -- Schema 三件套（F5）：模板常量（代码非 i18n）+ 详情树视图 -----------------------

/** 注册弹窗「插入模板」三段静态模板（代码常量，不进 i18n）。 */
export const SCHEMA_TEMPLATE_AVRO = `{
  "type": "record",
  "name": "DemoRecord",
  "fields": [
    { "name": "id", "type": "string" },
    { "name": "amount", "type": "double" },
    { "name": "quantity", "type": "int" },
    { "name": "status", "type": { "type": "enum", "name": "Status", "symbols": ["NEW", "PAID", "CANCELLED"] } }
  ]
}`;

export const SCHEMA_TEMPLATE_JSON = `{
  "type": "object",
  "properties": {
    "id": { "type": "string" },
    "amount": { "type": "number" },
    "quantity": { "type": "integer" },
    "status": { "type": "string", "enum": ["NEW", "PAID", "CANCELLED"] }
  },
  "required": ["id"]
}`;

export const SCHEMA_TEMPLATE_PROTOBUF = `syntax = "proto3";

package demo.v1;

message DemoMessage {
  string id = 1;
  double amount = 2;
  int32 quantity = 3;
  string status = 4;
}`;

export type SchemaTemplateFormat = "avro" | "json" | "protobuf";

export function schemaTemplateFor(format: SchemaTemplateFormat): string {
  if (format === "json") return SCHEMA_TEMPLATE_JSON;
  if (format === "protobuf") return SCHEMA_TEMPLATE_PROTOBUF;
  return SCHEMA_TEMPLATE_AVRO;
}

/** schema 树节点（详情区 树/文本 toggle 的递归渲染模型）。 */
export interface SchemaTreeNode {
  /** 字段名 / 分支标注（items、values 等）。 */
  name: string;
  /** 类型标注（record/string/union:…/logicalType 标尾等）。 */
  type: string;
  /** Avro default / JSON Schema default（有才显）。 */
  defaultValue?: string;
  children?: SchemaTreeNode[];
}

/**
 * AVRO / JSON Schema 文本 → 树模型（F5 树视图）。PROTOBUF 与解析失败返回 null
 * （调用方保持文本 + 行内提示）。递归覆盖 record/array/map/union/enum/fixed
 * 与 JSON Schema object/array/enum/default。
 */
export function buildSchemaTree(schemaText: string): SchemaTreeNode | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(schemaText);
  } catch {
    return null;
  }
  return buildSchemaTreeNode("root", parsed, 0);
}

const SCHEMA_TREE_DEPTH_MAX = 16;

function nodeTypeLabel(node: Record<string, unknown>, fallback: string): string {
  const logical = typeof node.logicalType === "string" ? ` (${node.logicalType})` : "";
  return `${fallback}${logical}`;
}

function buildSchemaTreeNode(name: string, raw: unknown, depth: number): SchemaTreeNode | null {
  if (depth > SCHEMA_TREE_DEPTH_MAX) return null;
  // union：取每个分支为子节点（union:第一非 null 分支标注）
  if (Array.isArray(raw)) {
    const children = raw
      .map((branch, index) => buildSchemaTreeNode(`[${index}]`, branch, depth + 1))
      .filter((branch): branch is SchemaTreeNode => branch !== null);
    const first = raw.find((branch) => branch !== "null" && !(typeof branch === "object" && branch !== null && (branch as Record<string, unknown>).type === "null"));
    return { name, type: `union:${avroTypeLabel(first)}`, children };
  }
  if (typeof raw === "string") return { name, type: raw };
  if (typeof raw !== "object" || raw === null) return { name, type: String(raw) };
  const node = raw as Record<string, unknown>;
  const type = node.type;
  const nodeDefault = "default" in node && node.default !== undefined ? stringifyTreeDefault(node.default) : undefined;

  if (typeof type === "string") {
    const label = nodeTypeLabel(node, type);
    if (type === "record" || type === "error") {
      const fields = Array.isArray(node.fields) ? node.fields : [];
      const children = fields
        .map((field) => {
          const entry = field as Record<string, unknown>;
          const child = buildSchemaTreeNode(String(entry.name ?? "?"), entry, depth + 1);
          return child;
        })
        .filter((child): child is SchemaTreeNode => child !== null);
      return { name, type: label, children, ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
    }
    if (type === "enum") {
      const symbols = Array.isArray(node.symbols) ? node.symbols.map(String) : [];
      return { name, type: symbols.length > 0 ? `enum[${symbols.join("|")}]` : "enum", ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
    }
    if (type === "array") {
      const child = buildSchemaTreeNode("items", node.items, depth + 1);
      return { name, type: label, children: child ? [child] : [], ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
    }
    if (type === "map") {
      const child = buildSchemaTreeNode("values", node.values, depth + 1);
      return { name, type: label, children: child ? [child] : [], ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
    }
    if (type === "fixed") {
      return { name, type: `${label}[${String(node.size ?? "?")}]`, ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
    }
    if (type === "object") {
      // JSON Schema object：properties → 子节点（required 加 * 标记）。
      const children = propertiesChildren(node, depth);
      if (children) {
        return { name, type: label, children, ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
      }
      return { name, type: label, ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
    }
    // 字段包装（{name, type, default}）与基础类型的内嵌逻辑类型。
    if (node.name !== undefined || node.logicalType !== undefined) {
      return { name: typeof node.name === "string" ? node.name : name, type: label, ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
    }
    return { name, type: label, ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
  }
  if (Array.isArray(type)) {
    const union = buildSchemaTreeNode(name, type, depth + 1);
    return union ? { ...union, name, ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) } : { name, type: "union" };
  }
  if (typeof type === "object" && type !== null) {
    const inner = buildSchemaTreeNode(name, type, depth + 1);
    if (inner) return { ...inner, name, ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
  }
  // JSON Schema 形状（type 在子级 / properties / items / enum）。
  const properties = typeof node.properties === "object" && node.properties !== null ? (node.properties as Record<string, unknown>) : null;
  if (properties) {
    const children = propertiesChildren(node, depth);
    if (children) {
      return { name, type: typeof node.type === "string" ? node.type : "object", children, ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
    }
  }
  if ("items" in node) {
    const child = buildSchemaTreeNode("items", node.items, depth + 1);
    return { name, type: typeof node.type === "string" ? node.type : "array", children: child ? [child] : [], ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
  }
  if (Array.isArray(node.enum)) {
    const symbols = node.enum.map(String);
    return { name, type: `enum[${symbols.join("|")}]`, ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
  }
  return { name, type: typeof node.type === "string" ? node.type : typeof node.type === "object" ? "object" : String(node.type ?? "?"), ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
}

function avroTypeLabel(raw: unknown): string {
  if (raw === undefined) return "null";
  if (typeof raw === "string") return raw;
  if (typeof raw === "object" && raw !== null) {
    const node = raw as Record<string, unknown>;
    const base = typeof node.type === "string" ? node.type : "record";
    return typeof node.logicalType === "string" ? `${base}(${node.logicalType})` : base;
  }
  return String(raw);
}

/** JSON Schema properties → 子节点（required 键名加 * 标记）；无 properties 返回 null。 */
function propertiesChildren(node: Record<string, unknown>, depth: number): SchemaTreeNode[] | null {
  const properties = typeof node.properties === "object" && node.properties !== null ? (node.properties as Record<string, unknown>) : null;
  if (!properties) return null;
  const required = new Set(Array.isArray(node.required) ? node.required.map(String) : []);
  return Object.entries(properties)
    .map(([key, value]) => {
      const child = buildSchemaTreeNode(key, value, depth + 1);
      if (child && required.has(key)) child.type = `${child.type} *`;
      return child;
    })
    .filter((child): child is SchemaTreeNode => child !== null);
}

function stringifyTreeDefault(value: unknown): string {
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value) ?? String(value);
  } catch {
    return String(value);
  }
}

// -- Flow 随机测试数据生成（F4；固定种子 RNG + Avro 随机 JSON + 模板占位符）---------

/** mulberry32 PRNG：固定种子 → 固定序列（spec 固定向量断言，照 zstd 向量范式）。 */
export type RandomSource = () => number;

export function mulberry32(seed: number): RandomSource {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) | 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Flow 参数夹持：countPerSend 1..100（默认 1）、intervalMs 250..10000（默认 1000）。 */
export function clampFlowCount(value: unknown, fallback = 1): number {
  return clampIntInclusive(value, 1, 100, fallback);
}

export function clampFlowIntervalMs(value: unknown, fallback = 1000): number {
  return clampIntInclusive(value, 250, 10000, fallback);
}

function clampIntInclusive(value: unknown, min: number, max: number, fallback: number): number {
  const parsed = Number.parseInt(String(value ?? "").trim(), 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(max, Math.max(min, parsed));
}

/** topic → 前缀匹配的 key/value subject（`<topic>-key` / `<topic>-value`）。 */
export function matchingSchemaSubjects(topic: string, subjects: string[]): { key?: string; value?: string } {
  const clean = String(topic ?? "").trim();
  const result: { key?: string; value?: string } = {};
  if (!clean) return result;
  for (const subject of subjects) {
    if (subject === `${clean}-key`) result.key = subject;
    else if (subject === `${clean}-value`) result.value = subject;
  }
  return result;
}

const AVRO_WORDS = ["alpha", "beta", "gamma", "delta", "omega"];

function randomInt(rng: RandomSource, min: number, max: number): number {
  return min + Math.floor(rng() * (max - min + 1));
}

function randomString(rng: RandomSource): string {
  return `${AVRO_WORDS[Math.floor(rng() * AVRO_WORDS.length)]}-${randomInt(rng, 100, 999)}`;
}

function randomUuid(rng: RandomSource): string {
  const hex = "0123456789abcdef";
  let out = "";
  for (let index = 0; index < 36; index += 1) {
    if (index === 8 || index === 13 || index === 18 || index === 23) out += "-";
    else if (index === 14) out += "4";
    else out += hex[Math.floor(rng() * 16)];
  }
  return out;
}

function daysSinceEpoch(nowMs: number): number {
  return Math.floor(nowMs / 86400000);
}

/**
 * AVRO schema 文本 → 随机 JSON 值（F4 schema_random）。递归覆盖
 * record/array/map/union（非 null 首支）/enum/fixed/int/long/float/double/
 * boolean/string/bytes + 逻辑类型 date（days 数）/timestamp-millis（unix ms
 * 数）/uuid/decimal（数值，最优尽力）。解析失败/空 schema 返回 { error }。
 */
export function generateAvroRandom(schemaText: string, rng: RandomSource, nowMs: number = Date.now()): { value: string } | { error: string } {
  let parsed: unknown;
  try {
    parsed = JSON.parse(schemaText);
  } catch {
    return { error: "schema is not valid JSON" };
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    return { error: "avro schema must be a JSON object" };
  }
  if ((parsed as Record<string, unknown>).type === undefined) {
    return { error: "avro schema is missing \"type\"" };
  }
  try {
    // 顶层直接传整个 schema 节点（{type:"record",…} / {type:"long",logicalType:…}）。
    return { value: JSON.stringify(generateAvroValue(parsed, rng, nowMs, 0)) };
  } catch (cause) {
    return { error: cause instanceof Error ? cause.message : String(cause) };
  }
}

const AVRO_GENERATION_DEPTH_MAX = 16;

function generateAvroValue(raw: unknown, rng: RandomSource, nowMs: number, depth: number): unknown {
  if (depth > AVRO_GENERATION_DEPTH_MAX) throw new Error("avro schema nesting too deep");
  // union：非 null 首支（契约：union（非 null 首支））。
  if (Array.isArray(raw)) {
    const branch = raw.find((entry) => entry !== "null" && !(typeof entry === "object" && entry !== null && (entry as Record<string, unknown>).type === "null"));
    if (branch === undefined) return null;
    return generateAvroValue(branch, rng, nowMs, depth + 1);
  }
  if (typeof raw === "string") {
    return generateAvroPrimitive(raw, rng, nowMs);
  }
  if (typeof raw !== "object" || raw === null) {
    throw new Error(`unsupported avro type: ${String(raw)}`);
  }
  const node = raw as Record<string, unknown>;
  const logical = typeof node.logicalType === "string" ? node.logicalType : "";
  const type = node.type;
  if (typeof type === "string") {
    if (logical) return generateAvroLogical(logical, rng, nowMs, node);
    switch (type) {
      case "record":
      case "error": {
        const fields = Array.isArray(node.fields) ? node.fields : [];
        const record: Record<string, unknown> = {};
        for (const field of fields) {
          const entry = field as Record<string, unknown>;
          record[String(entry.name ?? "?")] = generateAvroValue(entry.type, rng, nowMs, depth + 1);
        }
        return record;
      }
      case "enum": {
        const symbols = Array.isArray(node.symbols) ? node.symbols.map(String) : [];
        if (symbols.length === 0) throw new Error("avro enum has no symbols");
        return symbols[Math.floor(rng() * symbols.length)];
      }
      case "array": {
        return Array.from({ length: randomInt(rng, 1, 3) }, () => generateAvroValue(node.items, rng, nowMs, depth + 1));
      }
      case "map": {
        const map: Record<string, unknown> = {};
        const count = randomInt(rng, 1, 3);
        for (let index = 0; index < count; index += 1) {
          map[`k${index}`] = generateAvroValue(node.values, rng, nowMs, depth + 1);
        }
        return map;
      }
      case "fixed": {
        const size = Number(node.size ?? 0);
        let out = "";
        for (let index = 0; index < Math.max(1, Math.min(size, 64)); index += 1) out += String(randomInt(rng, 0, 9));
        return out;
      }
      default:
        return generateAvroPrimitive(type, rng, nowMs);
    }
  }
  if (Array.isArray(type) || typeof type === "object") {
    return generateAvroValue(type, rng, nowMs, depth + 1);
  }
  throw new Error(`unsupported avro type: ${String(type)}`);
}

function generateAvroPrimitive(type: string, rng: RandomSource, nowMs: number): unknown {
  switch (type) {
    case "null":
      return null;
    case "boolean":
      return rng() < 0.5;
    case "int":
      return randomInt(rng, 0, 999);
    case "long":
      return randomInt(rng, 0, 99999);
    case "float":
    case "double":
      return Math.round(rng() * 10000) / 100;
    case "bytes":
      return String(randomInt(rng, 1000, 9999));
    case "string":
      return randomString(rng);
    default:
      throw new Error(`unsupported avro type: ${type}`);
  }
}

function generateAvroLogical(logical: string, rng: RandomSource, nowMs: number, node: Record<string, unknown>): unknown {
  switch (logical) {
    case "date":
      return daysSinceEpoch(nowMs);
    case "timestamp-millis":
      return nowMs;
    case "timestamp-micros":
      return nowMs * 1000;
    case "time-millis":
      return nowMs % 86400000;
    case "time-micros":
      return (nowMs % 86400000) * 1000;
    case "uuid":
      return randomUuid(rng);
    case "decimal": {
      const scale = Number(node.scale ?? 0);
      const base = randomInt(rng, 1, 99999);
      return scale > 0 ? Math.round(base / Math.pow(10, scale) * Math.pow(10, scale)) / Math.pow(10, scale) : base;
    }
    default:
      // 未知逻辑类型按底层基础类型生成。
      return generateAvroValue(node.type, rng, nowMs, 1);
  }
}

// -- Flow 模板占位符展开（F4 template）----------------------------------------------

const TEMPLATE_INT_PATTERN = /\{int:(-?\d+),(-?\d+)\}/g;
const TEMPLATE_FLOAT_PATTERN = /\{float:(-?\d+(?:\.\d+)?),(-?\d+(?:\.\d+)?)\}/g;
const TEMPLATE_PICK_PATTERN = /\{pick:([^}]*)\}/g;

/**
 * JSON 模板占位符展开：{uuid} {now} {int:min,max} {float:min,max} {pick:a|b|c}。
 * nowMs 可注入（spec 固定向量）；未知占位符原样保留。
 */
export function expandTemplate(text: string, rng: RandomSource, nowMs: number = Date.now()): string {
  let out = String(text ?? "");
  out = out.replace(/\{uuid\}/g, () => randomUuid(rng));
  out = out.replace(/\{now\}/g, () => new Date(nowMs).toISOString());
  out = out.replace(TEMPLATE_INT_PATTERN, (_match, min: string, max: string) => String(randomInt(rng, Number(min), Number(max))));
  out = out.replace(TEMPLATE_FLOAT_PATTERN, (_match, min: string, max: string) => {
    const low = Number(min);
    const high = Number(max);
    const value = low + rng() * (high - low);
    return String(Math.round(value * 100) / 100);
  });
  out = out.replace(TEMPLATE_PICK_PATTERN, (_match, choices: string) => {
    const parts = choices.split("|").filter((part) => part.length > 0);
    return parts.length === 0 ? _match : parts[Math.floor(rng() * parts.length)];
  });
  return out;
}

// -- 弹层 keydown 决策（P1-2/P1-3：Esc 关闭 + Tab 焦点陷阱；FOCUSABLE_SELECTOR 等见上）--

/** 弹层 keydown 决策：Esc → close；Tab → focus 回绕目标下标；其余 → none。 */
export type ModalKeydownDecision = { kind: "none" } | { kind: "close" } | { kind: "focus"; index: number };

export function decideModalKeydown(
  key: string,
  shiftKey: boolean,
  focusableCount: number,
  currentIndex: number,
): ModalKeydownDecision {
  if (key === "Escape") return { kind: "close" };
  if (key !== "Tab" || focusableCount <= 0) return { kind: "none" };
  return { kind: "focus", index: nextFocusIndex(focusableCount, currentIndex, shiftKey) };
}
