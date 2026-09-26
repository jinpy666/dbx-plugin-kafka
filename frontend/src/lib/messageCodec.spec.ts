// messageCodec 纯函数单测：字节助手 + 解码/格式化管线（gzip/zstd 固定向量，
// snappy/lz4 用各库自身 compress API 构造 roundtrip 样本）。
import { describe, expect, it } from "vitest";
import { compress as lz4Compress } from "lz4js";
import { compress as snappyCompress } from "snappyjs";
import {
  base64ToBytes,
  bytesToHex,
  bytesToUtf8,
  formatBitSet,
  formatMessageValue,
  inflateGzip,
  inflateLz4,
  inflateSnappy,
  inflateZstd,
  looksLikeJson,
  looksLikeXml,
  MAX_DECODED_BYTES,
  messageFullValueText,
  prettyJson,
  prettyXml,
} from "./messageCodec";
import { headersPreview, previewText } from "./uiHelpers";
import type { KafkaMessage } from "./api";

// gzipSync('{"n":7}') 的 base64 常量（生成命令见仓库 PROGRESS 文档）。
const GZIP_JSON_N7_BASE64 = "H4sIAAAAAAAAE6tWylOyMq8FAPicEYIHAAAA";
// `zstd -q`('{"orderId":"A-1001","amount":42,"currency":"USD"}') 的 base64 常量
// （fzstd 仅提供解码器，无法库内 roundtrip，故用 CLI 预生成固定向量）。
const ZSTD_ORDER_JSON_BASE64 =
  "KLUv/SQxiQEAeyJvcmRlcklkIjoiQS0xMDAxIiwiYW1vdW50Ijo0MiwiY3VycmVuY3kiOiJVU0QifbQxJQg=";

function b64(text: string): string {
  return utf8ToBase64(text);
}

function bytesToBase64(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
}

function utf8ToBase64(text: string): string {
  const bytes = new TextEncoder().encode(text);
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
}

function msg(overrides: Partial<KafkaMessage>): KafkaMessage {
  return { topic: "orders", partition: 0, offset: 1, timestamp: 1_700_000_000_000, ...overrides };
}

describe("byte helpers", () => {
  it("round-trips base64/utf8/hex", () => {
    expect(bytesToUtf8(base64ToBytes(b64("hello")))).toBe("hello");
    expect(b64("hello")).toBe("aGVsbG8=");
    expect(bytesToHex(base64ToBytes(b64("\n\x1f")))).toBe("0a1f");
  });

  it("replaces invalid utf-8 bytes instead of throwing", () => {
    const text = bytesToUtf8(new Uint8Array([0x61, 0xff, 0x62]));
    expect(text).toContain("a");
    expect(text).toContain("b");
    expect(text.length).toBe(3);
  });
});

describe("format pipeline", () => {
  it("pretty-prints JSON only when parseable", () => {
    expect(prettyJson('{"a":1}')).toBe('{\n  "a": 1\n}');
    expect(prettyJson("plain text")).toBe("plain text");
  });

  it("pretty-prints well-formed XML with leaf nodes inline", () => {
    expect(prettyXml("<a><b>1</b><c/><d x=\"2\">y</d></a>")).toBe(
      ["<a>", "  <b>1</b>", "  <c/>", '  <d x="2">y</d>', "</a>"].join("\n"),
    );
  });

  it("leaves malformed XML, text, and DOCTYPE/ENTITY payloads untouched (no parsing)", () => {
    // 不良构：标签不平衡 → 原样。
    expect(prettyXml("<a><b></a>")).toBe("<a><b></a>");
    // 纯文本 → 原样。
    expect(prettyXml("hello world")).toBe("hello world");
    // DOCTYPE/ENTITY：安全红线——不解析不重排，原样返回。
    expect(prettyXml('<!DOCTYPE a [<!ENTITY x "y">]><a>&x;</a>')).toBe(
      '<!DOCTYPE a [<!ENTITY x "y">]><a>&x;</a>',
    );
    // 实体引用按原样保留在文本节点中。
    expect(prettyXml("<a>r&amp;D</a>")).toBe("<a>r&amp;D</a>");
  });

  it("detects xml/json shapes for auto format selection", () => {
    expect(looksLikeXml("<order id=\"1\"/>")).toBe(true);
    expect(looksLikeXml("<?xml version=\"1.0\"?><a/>")).toBe(true);
    expect(looksLikeXml('{"a":1}')).toBe(false);
    expect(looksLikeJson('{"a":1}')).toBe(true);
    expect(looksLikeJson("<a/>")).toBe(false);
  });

  it("formats xml values through the pipeline", async () => {
    const result = await formatMessageValue(msg({ valueBase64: b64('<r><a>1</a></r>') }), {
      decode: "none",
      decompression: "none",
      format: "xml",
    });
    expect(result.text).toBe("<r>\n  <a>1</a>\n</r>");
    expect(result.error).toBeUndefined();
  });

  it("formats bitsets from decimal/hex/binary literals", () => {
    expect(formatBitSet("5")).toBe("101");
    expect(formatBitSet("0xff")).toBe("1111 1111");
    expect(formatBitSet("0b1010")).toBe("1010");
    expect(formatBitSet("nope")).toBeNull();
  });

  it("passes raw value through by default", async () => {
    const result = await formatMessageValue(msg({ valueBase64: b64("hello") }), {
      decode: "none",
      decompression: "none",
      format: "raw",
    });
    expect(result).toEqual({ text: "hello" });
  });

  it("applies inner base64 decode then format", async () => {
    const result = await formatMessageValue(msg({ valueBase64: b64(b64("payload")) }), {
      decode: "base64",
      decompression: "none",
      format: "json",
    });
    expect(result.text).toBe("payload");
  });

  it("inflates gzip payloads via DecompressionStream", async () => {
    const result = await formatMessageValue(msg({ valueBase64: GZIP_JSON_N7_BASE64 }), {
      decode: "none",
      decompression: "gzip",
      format: "json",
    });
    expect(result.error).toBeUndefined();
    expect(result.text).toBe('{\n  "n": 7\n}');
  });

  it("inflates zstd payloads (fzstd, fixed CLI vector)", async () => {
    const result = await formatMessageValue(msg({ valueBase64: ZSTD_ORDER_JSON_BASE64 }), {
      decode: "none",
      decompression: "zstd",
      format: "json",
    });
    expect(result.error).toBeUndefined();
    expect(result.text).toBe('{\n  "orderId": "A-1001",\n  "amount": 42,\n  "currency": "USD"\n}');
  });

  it("inflates snappy payloads (roundtrip with snappyjs compress)", async () => {
    const payload = new TextEncoder().encode('{"n":7,"ok":true}');
    const result = await formatMessageValue(msg({ valueBase64: bytesToBase64(snappyCompress(payload)) }), {
      decode: "none",
      decompression: "snappy",
      format: "raw",
    });
    expect(result.error).toBeUndefined();
    expect(result.text).toBe('{"n":7,"ok":true}');
  });

  it("inflates lz4 payloads (roundtrip with lz4js compress)", async () => {
    const payload = new TextEncoder().encode('{"n":7,"ok":true}');
    const result = await formatMessageValue(msg({ valueBase64: bytesToBase64(lz4Compress(payload)) }), {
      decode: "none",
      decompression: "lz4",
      format: "raw",
    });
    expect(result.error).toBeUndefined();
    expect(result.text).toBe('{"n":7,"ok":true}');
  });

  it("reports decompression failures per algorithm instead of crashing", async () => {
    // 非 zstd 字节流喂给 zstd：fzstd 抛错 → 管线转 error 展示（原值透传）。
    const result = await formatMessageValue(msg({ valueBase64: b64("data") }), {
      decode: "none",
      decompression: "zstd",
      format: "raw",
    });
    expect(result.error).toContain("zstd");
    expect(result.text).toBe("data");
    const snappy = await formatMessageValue(msg({ valueBase64: b64("data") }), {
      decode: "none",
      decompression: "snappy",
      format: "raw",
    });
    expect(snappy.error).toContain("snappy");
    const lz4 = await formatMessageValue(msg({ valueBase64: b64("data") }), {
      decode: "none",
      decompression: "lz4",
      format: "raw",
    });
    expect(lz4.error).toContain("lz4");
  });

  it("hex-formats and reports invalid inner base64", async () => {
    const hex = await formatMessageValue(msg({ valueBase64: b64("A") }), {
      decode: "none",
      decompression: "none",
      format: "hex",
    });
    expect(hex.text).toBe("41");
    const bad = await formatMessageValue(msg({ valueBase64: b64("!!!") }), {
      decode: "base64",
      decompression: "none",
      format: "raw",
    });
    expect(bad.error).toContain("inner base64");
  });

  it("falls back to full value text for preview/download", () => {
    const message = msg({ valueText: "safe", valueBase64: b64("line1\nline2") });
    expect(messageFullValueText(message)).toBe("line1\nline2");
    expect(previewText("a  b\nc", 2)).toBe("a …");
    expect(headersPreview({ h1: "v1", h2: "v2", h3: "v3" })).toBe("h1=v1, h2=v2, …");
  });
});

// -- 解压炸弹防护（评审 M：后端已有 16MiB 上限，前端手动解压路径是镜像缺口）--

describe("decompression bomb guard", () => {
  function snappyVarint(value: number): Uint8Array {
    const out: number[] = [];
    let rest = value;
    while (rest >= 0x80) {
      out.push((rest & 0x7f) | 0x80);
      rest = Math.floor(rest / 128);
    }
    out.push(rest);
    return new Uint8Array(out);
  }

  it("caps gzip output at MAX_DECODED_BYTES (17MiB bomb errors instead of inflating)", async () => {
    // CompressionStream("gzip")（压缩方向）现场构造炸弹，无 Node 依赖。
    const stream = new Blob([new Uint8Array(MAX_DECODED_BYTES + 1024)])
      .stream()
      .pipeThrough(new CompressionStream("gzip"));
    const bomb = new Uint8Array(await new Response(stream).arrayBuffer());
    const result = await inflateGzip(bomb);
    expect(result.error).toContain("decompression bomb guard");
    expect(result.bytes.length).toBe(bomb.length);
  });

  it("rejects zstd frames declaring oversized content size before decompressing", () => {
    // magic + FHD(0xE0: single_segment + FCS 8 字节) + 声明 17MiB+1。
    const declared = MAX_DECODED_BYTES + 1;
    const header = new Uint8Array([
      0x28, 0xb5, 0x2f, 0xfd, 0xe0,
      ...new Uint8Array(new DataView(new ArrayBuffer(8)).buffer).map((_, i) => Math.floor(declared / 2 ** (8 * i)) & 0xff),
    ]);
    const result = inflateZstd(header);
    expect(result.error).toContain("decompression bomb guard");
  });

  it("rejects snappy streams declaring oversized length from the preamble", () => {
    const preamble = snappyVarint(MAX_DECODED_BYTES + 1);
    const result = inflateSnappy(preamble);
    expect(result.error).toContain("decompression bomb guard");
  });

  it("caps lz4 output (17MiB roundtrip bomb errors instead of inflating)", () => {
    const bomb = lz4Compress(new Uint8Array(MAX_DECODED_BYTES + 1024));
    const result = inflateLz4(bomb);
    expect(result.error).toContain("decompression bomb guard");
  });

  it("keeps legit payloads under the cap working", () => {
    const ok = inflateZstd(new Uint8Array(base64ToBytes(ZSTD_ORDER_JSON_BASE64)));
    expect(ok.error).toBeUndefined();
  });
});
