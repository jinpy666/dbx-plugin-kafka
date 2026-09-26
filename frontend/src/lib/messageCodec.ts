/**
 * 消息 value 编解码/格式化管线（自 kafkaModel 拆分）：base64/hex/utf8 字节
 * 助手、gzip/lz4/zstd/snappy 解压、JSON/XML/BitSet 格式化与 value 解码主管线。
 * 纯函数、无框架依赖；解压库选型与降级语义见各函数注释。
 */
import { decompress as fzstdDecompress } from "fzstd";
import { decompress as lz4Decompress } from "lz4js";
import { uncompress as snappyUncompress } from "snappyjs";
import type { KafkaMessage } from "./api";

// 解压输出上限（评审 M：对齐后端 kafkaconn maxDecodedBytes——后端解压有
// 16MiB 防护，前端详情抽屉的手动解压是镜像缺口：恶意 topic 消息在抽屉里
// 解压可把渲染进程打到 GB 级）。超限返回带 error 的降级结果（原值透传），
// 语义与解压失败一致。
export const MAX_DECODED_BYTES = 16 * 1024 * 1024;

function bombGuardError(algorithm: string): string {
  return `${DECOMPRESSION_LABELS[algorithm] ?? algorithm}: decompressed payload exceeds ${MAX_DECODED_BYTES} bytes (decompression bomb guard)`;
}

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
    // 计数中间层：解压输出累计超上限即 error 断流，不物化超出部分。
    let total = 0;
    const capped = new TransformStream<Uint8Array, Uint8Array>({
      transform(chunk, controller) {
        total += chunk.byteLength;
        if (total > MAX_DECODED_BYTES) {
          controller.error(new Error(bombGuardError("gzip")));
          return;
        }
        controller.enqueue(chunk);
      },
    });
    const stream = new Blob([bytes as BlobPart])
      .stream()
      .pipeThrough(new DecompressionStream("gzip"))
      .pipeThrough(capped);
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

/**
 * zstd 帧头声明的 Frame_Content_Size（魔数不匹配/字段缺省/截断返回 null，
 * 调用方退回解压后检查）。fzstd 按声明尺寸预分配输出——炸弹帧正是借这条
 * 路径放大内存，因此解压前先看声明值。
 */
export function zstdFrameContentSize(bytes: Uint8Array): number | null {
  if (bytes.length < 5) return null;
  // zstd magic 0xFD2FB528（小端字节序 28 B5 2F FD）。
  if (bytes[0] !== 0x28 || bytes[1] !== 0xb5 || bytes[2] !== 0x2f || bytes[3] !== 0xfd) return null;
  const fhd = bytes[4];
  const singleSegment = (fhd & 0x20) !== 0;
  const fcsFlag = (fhd >> 6) & 0x03;
  let offset = 5;
  if (!singleSegment) offset += 1; // Window_Descriptor
  offset += [0, 1, 2, 4][fhd & 0x03]; // Dictionary_ID
  const fcsSize = fcsFlag === 0 ? (singleSegment ? 1 : 0) : [0, 2, 4, 8][fcsFlag];
  if (fcsSize === 0 || offset + fcsSize > bytes.length) return null;
  let size = 0;
  for (let index = 0; index < fcsSize; index += 1) size += bytes[offset + index] * 2 ** (8 * index);
  return size;
}

// zstd 解压：fzstd（纯 JS、MIT；仅解码，与生产端 zstd 帧格式兼容）。
export function inflateZstd(bytes: Uint8Array): { bytes: Uint8Array; error?: string } {
  const declared = zstdFrameContentSize(bytes);
  if (declared !== null && declared > MAX_DECODED_BYTES) {
    return { bytes, error: bombGuardError("zstd") };
  }
  try {
    const output = fzstdDecompress(bytes);
    if (output.length > MAX_DECODED_BYTES) return { bytes, error: bombGuardError("zstd") };
    return { bytes: output };
  } catch (cause) {
    return { bytes, error: decompressError("zstd", cause) };
  }
}

/**
 * snappy block 前导 varint 声明的未压缩长度（截断/超长 varint 返回 null）。
 * snappyjs 按声明值分配输出——解压前先看声明值（与后端 DecodedLen 预检同型）。
 */
export function snappyDeclaredLength(bytes: Uint8Array): number | null {
  const limit = Math.min(bytes.length, 10);
  let length = 0;
  for (let index = 0; index < limit; index += 1) {
    length += (bytes[index] & 0x7f) * 2 ** (7 * index);
    if ((bytes[index] & 0x80) === 0) return length;
  }
  return null;
}

// snappy 解压：snappyjs（纯 JS、MIT，含 Hadoop 变体外的标准 framing）。
export function inflateSnappy(bytes: Uint8Array): { bytes: Uint8Array; error?: string } {
  const declared = snappyDeclaredLength(bytes);
  if (declared !== null && declared > MAX_DECODED_BYTES) {
    return { bytes, error: bombGuardError("snappy") };
  }
  try {
    const output = snappyUncompress(bytes);
    if (output.length > MAX_DECODED_BYTES) return { bytes, error: bombGuardError("snappy") };
    return { bytes: output };
  } catch (cause) {
    return { bytes, error: decompressError("snappy", cause) };
  }
}

// lz4 解压：lz4js（纯 JS、ISC，frame 格式，与 Kafka lz4 块兼容）。frame 的
// content size 字段可选且 lz4js 不透出，只有解压后检查（zstd/snappy 的
// 声明预检路径在此不可用）。
export function inflateLz4(bytes: Uint8Array): { bytes: Uint8Array; error?: string } {
  try {
    const output = new Uint8Array(lz4Decompress(bytes));
    if (output.length > MAX_DECODED_BYTES) return { bytes, error: bombGuardError("lz4") };
    return { bytes: output };
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
