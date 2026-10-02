/**
 * UI 通用纯助手（自 kafkaModel 拆分）：预览/行上限/流式缓冲、防抖、
 * 剪贴板写入（execCommand 兜底）、生产分区输入校验、即时搜索过滤。
 */
import type { KafkaMessage } from "./api";

// -- display helpers ---------------------------------------------------------------

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

/**
 * L-3（2026-10-02）：base64 直发的客户端预校验（标准或 URL-safe 字母表，
 * 允许无 padding）。非法输入此前要等后端报错才可见；atob 试解一次即可
 * 在行内提示（RFC 4648 §4/§5 字符集 + 长度对 4 取模非 1）。
 */
export function isValidBase64(value: string): boolean {
  const trimmed = value.replace(/\s+/g, "");
  if (trimmed === "" || trimmed.length % 4 === 1) return false;
  if (!/^[A-Za-z0-9+\-/]*={0,2}$/.test(trimmed)) return false;
  try {
    atob(trimmed.replace(/-/g, "+").replace(/_/g, "/"));
    return true;
  } catch {
    return false;
  }
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
