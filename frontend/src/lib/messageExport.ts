/**
 * 消息导出序列化（自 kafkaModel 拆分）：CSV（RFC 4180）/TSV/JSON，
 * value 列一律用 base64 解码后的保真文本。
 */
import type { KafkaMessage } from "./api";
import { messageFullValueText } from "./messageCodec";

// 公式注入中和（评审 LOW-5）：以 = + - @ 制表/回车开头的单元格在 Excel/
// Sheets 中会被当公式执行——前缀 ' 中和（业界通行约定）。
function neutralizeFormula(value: string): string {
  return /^[=+\-@\t\r]/.test(value) ? `'${value}` : value;
}

function csvEscape(value: string): string {
  const neutral = neutralizeFormula(value);
  if (/[",\n\r]/.test(neutral)) return `"${neutral.replace(/"/g, '""')}"`;
  return neutral;
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
  return neutralizeFormula(
    value.replace(/\\/g, "\\\\").replace(/\t/g, "\\t").replace(/\n/g, "\\n").replace(/\r/g, "\\r"),
  );
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
