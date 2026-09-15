// kafkaModel 纯函数单测：解码/格式化管线、topic 排序、lag 聚合、CSV/JSON
// 导出、properties 解析与 consume 表单校验。不连网、不依赖组件；
// 不引 node 专有模块（zlib/Buffer）：gzip/zstd 用预生成 base64 常量，
// snappy/lz4 用各库自身 compress API 构造 roundtrip 样本。
import { describe, expect, it } from "vitest";
import { compress as lz4Compress } from "lz4js";
import { compress as snappyCompress } from "snappyjs";
import {
  appendStreamRows,
  base64ToBytes,
  bytesToHex,
  bytesToUtf8,
  capRows,
  decideModalKeydown,
  filterTopics,
  formatBitSet,
  formatMessageValue,
  headersPreview,
  isInternalTopicName,
  fieldFilterIssue,
  isRangeReversed,
  jsonErrorLine,
  matchText,
  messageFullValueText,
  nowDatetimeLocal,
  offsetTimeToParam,
  offsetTimeToUnixMs,
  parseHeadersJson,
  parsePartitionList,
  parseGroupOffsetTargetsText,
  parsePartitionOffsetsText,
  partitionOffsetsToText,
  parsePropertiesText,
  prettyJson,
  looksLikeJson,
  looksLikeXml,
  prettyXml,
  previewText,
  propertiesToConnectionForm,
  serializeMessagesToCsv,
  serializeMessagesToJson,
  serializeMessagesToTsv,
  sortTopics,
  sortTopicsPinned,
  scoreTopicName,
  sumLag,
  switchTimeInputMode,
  unixMsToDatetimeLocal,
  validateConsumeForm,
} from "./kafkaModel";
import type { KafkaMessage } from "./api";
import { MESSAGE_ROWS_MAX, truncatedValuePreview } from "./kafkaModel";

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

describe("topic ranking", () => {
  it("scores business-like names above infra names", () => {
    expect(scoreTopicName("order-events")).toBeGreaterThan(scoreTopicName("connect-offsets"));
  });

  it("detects internal topics and sinks them to the bottom", () => {
    expect(isInternalTopicName("_schemas")).toBe(true);
    const sorted = sortTopics([
      { name: "_internal.state" },
      { name: "zzz-raw" },
      { name: "order-events", isInternal: false },
      { name: "users" },
    ]);
    expect(sorted.map((topic) => topic.name)).toEqual(["order-events", "users", "zzz-raw", "_internal.state"]);
  });

  it("filters topics by keyword", () => {
    const topics = [{ name: "orders" }, { name: "users" }];
    expect(filterTopics(topics, "ORD")).toHaveLength(1);
    expect(filterTopics(topics, "  ")).toHaveLength(2);
  });

  // Lane4 打磨：收藏置顶——先按 sortTopics 排，收藏项稳定提前；收藏项内部
  // 保持同一相对顺序；internal 收藏同样置顶（用户显式收藏优先于沉底）。
  it("pins favorites to the top while keeping the base order (Lane4)", () => {
    const topics = [{ name: "_internal.state" }, { name: "zzz-raw" }, { name: "order-events" }, { name: "users" }];
    expect(sortTopicsPinned(topics, new Set())).toEqual(sortTopics(topics));
    const pinned = sortTopicsPinned(topics, new Set(["users", "_internal.state"]));
    expect(pinned.map((topic) => topic.name)).toEqual(["users", "_internal.state", "order-events", "zzz-raw"]);
    // 不改入参
    expect(topics.map((topic) => topic.name)).toEqual(["_internal.state", "zzz-raw", "order-events", "users"]);
  });
});

describe("lag aggregation", () => {
  it("sums non-negative lags and treats missing as zero", () => {
    expect(sumLag([{ lag: 5 }, { lag: 0 }, {}, { lag: -3 }, { lag: null }])).toBe(5);
  });
});

describe("stream buffer", () => {
  it("appends rows, caps at max and counts dropped oldest rows", () => {
    const seed = [{ topic: "t", partition: 0, offset: 0, timestamp: 1 }, { topic: "t", partition: 0, offset: 1, timestamp: 2 }];
    const keep = appendStreamRows(seed, [], 10);
    expect(keep).toEqual({ rows: seed, dropped: 0 });
    const grown = appendStreamRows(seed, [{ topic: "t", partition: 0, offset: 2, timestamp: 3 }], 3);
    expect(grown.rows.map((row) => row.offset)).toEqual([0, 1, 2]);
    expect(grown.dropped).toBe(0);
    const overflow = appendStreamRows(grown.rows, [{ topic: "t", partition: 0, offset: 3, timestamp: 4 }], 3, 0);
    expect(overflow.rows.map((row) => row.offset)).toEqual([1, 2, 3]);
    expect(overflow.dropped).toBe(1);
    const stacked = appendStreamRows(overflow.rows, overflow.rows.slice(), 3, 1);
    expect(stacked.dropped).toBe(1 + 3);
    expect(stacked.rows).toHaveLength(3);
  });
});

describe("message table cap", () => {
  it("returns the same reference and zero dropped when under the cap", () => {
    const rows = [msg({ partition: 0, offset: 0 }), msg({ partition: 0, offset: 1 })];
    const capped = capRows(rows);
    expect(capped.rows).toBe(rows); // 零拷贝语义
    expect(capped).toEqual({ rows, total: 2, dropped: 0 });
  });

  it("trims the head and keeps the newest tail when over the cap", () => {
    const rows = Array.from({ length: 5 }, (_unused, index) => msg({ partition: 0, offset: index }));
    const capped = capRows(rows, 3);
    expect(capped.rows.map((row) => row.offset)).toEqual([2, 3, 4]); // 最新在尾部
    expect(capped.total).toBe(5);
    expect(capped.dropped).toBe(2);
    expect(capped.rows).not.toBe(rows); // 裁剪产出新数组
  });

  it("caps to a single row and handles an empty list", () => {
    const rows = [msg({ partition: 0, offset: 7 }), msg({ partition: 0, offset: 8 })];
    expect(capRows(rows, 1).rows.map((row) => row.offset)).toEqual([8]);
    expect(capRows([], 10)).toEqual({ rows: [], total: 0, dropped: 0 });
    expect(MESSAGE_ROWS_MAX).toBeGreaterThan(0);
  });

  it("keeps short value previews untouched", () => {
    const short = "hello world";
    expect(truncatedValuePreview(short)).toEqual({ text: short, truncated: false });
  });

  it("truncates long values keeping head and tail with a hidden-char marker", () => {
    const text = "A".repeat(20000) + "B".repeat(100) + "C".repeat(20000);
    const preview = truncatedValuePreview(text);
    expect(preview.truncated).toBe(true);
    expect(preview.text.length).toBeLessThan(text.length);
    expect(preview.text.startsWith("A".repeat(100))).toBe(true); // 头部保留
    expect(preview.text.endsWith("C".repeat(100))).toBe(true); // 尾部保留
    expect(preview.text).toMatch(/⋯ \d+ chars ⋯/); // 中段省略标记
  });
});

describe("export serialization", () => {
  it("serializes CSV with RFC 4180 escaping", () => {
    const csv = serializeMessagesToCsv([
      msg({ key: "k,1", valueText: 'say "hi"', headers: { trace: "abc" } }),
    ]);
    expect(csv.split("\r\n")[0]).toBe("topic,partition,offset,timestamp,key,value,headers");
    expect(csv).toContain('"k,1"');
    expect(csv).toContain('"say ""hi"""');
    expect(csv).toContain("trace=abc");
  });

  it("serializes stable JSON with full value text", () => {
    const json = JSON.parse(serializeMessagesToJson([msg({ key: "k", valueBase64: b64("body") })]));
    expect(json).toHaveLength(1);
    expect(json[0]).toMatchObject({ topic: "orders", partition: 0, offset: 1, key: "k", value: "body" });
  });

  // Lane4 打磨：TSV 导出——转义规则与 CSV 不同（无引号包裹，制表符/换行/回车
  // 反斜杠转义，反斜杠自身先转义），列序与 CSV 一致。
  it("serializes TSV with tab/newline escaping and CSV column order", () => {
    const tsv = serializeMessagesToTsv([
      msg({ key: "k\t1", valueText: "line1\nline2\rback\\slash", headers: { trace: "abc" } }),
      msg({ key: "plain", valueText: "no escapes" }),
    ]);
    const [header, first, second] = tsv.split("\r\n");
    expect(header).toBe("topic\tpartition\toffset\ttimestamp\tkey\tvalue\theaders");
    expect(first).toBe("orders\t0\t1\t1700000000000\tk\\t1\tline1\\nline2\\rback\\\\slash\ttrace=abc");
    expect(second).toBe("orders\t0\t1\t1700000000000\tplain\tno escapes\t");
  });
});

describe("consume form helpers", () => {
  it("parses partition lists and offset maps", () => {
    expect(parsePartitionList("2, 0，0 1")).toEqual([0, 1, 2]);
    expect(parsePartitionOffsetsText("0=100, 1:200\n2=300")).toEqual({ "0": 100, "1": 200, "2": 300 });
    expect(partitionOffsetsToText({ "1": 200, "0": 100 })).toBe("0=100,1=200");
  });

  it("parses group reset offset targets (explicit topic and single-topic fallback)", () => {
    const multi = parseGroupOffsetTargetsText("t1:0=100, t1:1=200, t2:0=5", ["t1", "t2"]);
    expect(multi.invalid).toEqual([]);
    expect(multi.targets).toEqual({ t1: { "0": 100, "1": 200 }, t2: { "0": 5 } });
    const single = parseGroupOffsetTargetsText("0=100, 1:200", ["only"]);
    expect(single.targets).toEqual({ only: { "0": 100, "1": 200 } });
    const ambiguous = parseGroupOffsetTargetsText("0=100", ["a", "b"]);
    expect(ambiguous.invalid).toEqual(["0=100"]);
  });

  it("converts offset time inputs (unix ms, datetime-local, RFC3339)", () => {
    expect(offsetTimeToParam("1700000000000")).toBe(1700000000000);
    expect(offsetTimeToParam("2026-09-05T08:30")).toBe(new Date("2026-09-05T08:30:00").toISOString());
    expect(offsetTimeToParam("2026-09-05T08:30:00Z")).toBe("2026-09-05T08:30:00.000Z");
    expect(offsetTimeToParam("junk")).toBeNull();
    expect(offsetTimeToParam("")).toBeNull();
  });

  it("validates consume form mutex rules", () => {
    const base = {
      commit: false,
      groupId: "",
      partitionsText: "",
      offsetStrategy: "latest",
      offsetTimeText: "",
      partitionOffsetsText: "",
      hasFilters: false,
    };
    expect(validateConsumeForm(base)).toEqual([]);
    expect(validateConsumeForm({ ...base, commit: true, hasFilters: true })).toEqual([
      { field: "commit", key: "commitFilterConflict" },
      { field: "commit", key: "commitNeedsGroup" },
    ]);
    expect(validateConsumeForm({ ...base, commit: true })).toEqual([{ field: "commit", key: "commitNeedsGroup" }]);
    expect(validateConsumeForm({ ...base, partitionsText: "0", groupId: "g1" })).toEqual([
      { field: "partitions", key: "partitionsGroupConflict" },
    ]);
    expect(validateConsumeForm({ ...base, offsetStrategy: "timestamp" })).toEqual([
      { field: "strategy", key: "timestampRequired" },
    ]);
    expect(validateConsumeForm({ ...base, offsetStrategy: "offset" })).toEqual([
      { field: "strategy", key: "offsetsRequired" },
    ]);
  });

  it("matches text in all four modes", () => {
    expect(matchText("orders-eu", "orders", "prefix")).toBe(true);
    expect(matchText("orders", "orders", "exact")).toBe(true);
    expect(matchText("payload", "OA", "contains")).toBe(false);
    expect(matchText("error-42", "error-\\d+", "regex")).toBe(true);
    expect(matchText(undefined, "x", "contains")).toBe(false);
  });
});

describe("time range inputs", () => {
  it("normalizes datetime-local / unix ms / RFC3339 inputs to unix ms", () => {
    expect(offsetTimeToUnixMs("1700000000000")).toBe(1700000000000);
    expect(offsetTimeToUnixMs("2026-09-05T08:30:00")).toBe(new Date("2026-09-05T08:30:00").getTime());
    expect(offsetTimeToUnixMs("2026-09-05T08:30:00Z")).toBe(Date.parse("2026-09-05T08:30:00.000Z"));
    expect(offsetTimeToUnixMs("junk")).toBeNull();
    expect(offsetTimeToUnixMs("  ")).toBeNull();
  });

  it("round-trips unix ms through datetime-local with second precision", () => {
    const ms = Date.UTC(2026, 8, 5, 0, 30, 15);
    const text = unixMsToDatetimeLocal(ms);
    expect(text).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$/);
    expect(offsetTimeToUnixMs(text)).toBe(ms);
    expect(unixMsToDatetimeLocal(Number.NaN)).toBe("");
  });

  it("builds now() as a parseable datetime-local string", () => {
    const now = nowDatetimeLocal();
    expect(now).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$/);
    expect(offsetTimeToUnixMs(now)).not.toBeNull();
  });

  it("switches input modes without losing unparseable drafts", () => {
    expect(switchTimeInputMode("2026-09-05T08:30:00", "unix")).toBe(String(new Date("2026-09-05T08:30:00").getTime()));
    const ms = 1_700_000_000_000;
    const back = switchTimeInputMode(String(ms), "datetime");
    expect(offsetTimeToUnixMs(back)).toBe(ms);
    expect(switchTimeInputMode("junk", "unix")).toBe("junk");
    expect(switchTimeInputMode("junk", "datetime")).toBe("junk");
    expect(switchTimeInputMode("", "datetime")).toBe("");
  });

  it("flags only concrete from>to ranges as reversed", () => {
    expect(isRangeReversed(200, 100)).toBe(true);
    expect(isRangeReversed(100, 100)).toBe(false);
    expect(isRangeReversed(null, 100)).toBe(false);
    expect(isRangeReversed(100, null)).toBe(false);
  });

  it("requires numeric values only for numeric-comparison operators", () => {
    expect(fieldFilterIssue({ operator: "gt", value: "42" })).toBeNull();
    expect(fieldFilterIssue({ operator: "lte", value: "abc" })).toBe("fieldValueNumeric");
    expect(fieldFilterIssue({ operator: "gt", value: "" })).toBeNull();
    expect(fieldFilterIssue({ operator: "contains", value: "abc" })).toBeNull();
  });
});

describe("headers json", () => {
  it("accepts string-valued objects only", () => {
    expect(parseHeadersJson('{"trace":"abc"}')).toEqual({ headers: { trace: "abc" } });
    expect(parseHeadersJson("")).toEqual({ headers: {} });
    expect(parseHeadersJson("[1]")).toHaveProperty("error");
    expect(parseHeadersJson('{"n":1}')).toHaveProperty("error");
    expect(parseHeadersJson("{bad")).toHaveProperty("error");
  });
});

describe("json error line (editor linter)", () => {
  it("returns null for valid, empty and whitespace-only text", () => {
    expect(jsonErrorLine('{"a": [1, 2, {"b": null}], "c": "x"}')).toBeNull();
    expect(jsonErrorLine("")).toBeNull();
    expect(jsonErrorLine("   \n\t ")).toBeNull();
  });

  it("locates the first syntax error line across value kinds", () => {
    expect(jsonErrorLine("{bad}")).toBe(1);
    expect(jsonErrorLine('{\n  "a": 1,\n  "b": tru\n}')).toBe(3); // 非法字面量
    expect(jsonErrorLine('{\n  "a": 1\n  "b": 2\n}')).toBe(3); // 缺逗号
    expect(jsonErrorLine("[1, 2,\n3,\n]")).toBe(3); // 数组尾逗号
    expect(jsonErrorLine('{"k": "v"')).toBe(1); // 未闭合
  });

  it("tracks multi-line strings/objects and trailing junk", () => {
    expect(jsonErrorLine('{\n  "a": "line1\nbroken"}')).toBe(2); // 字符串内裸换行
    expect(jsonErrorLine('{"a": 1} extra')).toBe(1); // 尾部多余 token
    expect(jsonErrorLine("123")).toBeNull(); // 顶层标量合法
    expect(jsonErrorLine("1.5e-3")).toBeNull();
    expect(jsonErrorLine("01")).toBe(1); // 前导零 → 多余 token
    expect(jsonErrorLine('{"\\u00zz": 1}')).toBe(1); // 非法 \\u 转义
  });
});

describe("confluent properties", () => {
  it("parses key=value lines with comments, continuations and escapes", () => {
    const properties = parsePropertiesText(`
# comment line
! another comment
bootstrap.servers=dbx-kafka-test\\
:9092
security.protocol=SASL_SSL
sasl.mechanism=SCRAM\\-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="kafka" password="pass=1";
`);
    expect(properties["bootstrap.servers"]).toBe("dbx-kafka-test:9092");
    expect(properties["security.protocol"]).toBe("SASL_SSL");
    expect(properties["sasl.mechanism"]).toBe("SCRAM-SHA-256");
    expect(properties["sasl.jaas.config"]).toContain('password="pass=1"');
  });

  it("maps properties onto the connection form fields", () => {
    const form = propertiesToConnectionForm(
      parsePropertiesText(`
bootstrap.servers=broker1:9092,broker2:9092
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=ScramLoginModule required username="app" password="secret";
`),
    );
    expect(form).toEqual({
      bootstrapServers: "broker1:9092,broker2:9092",
      securityProtocol: "SASL_SSL",
      saslMechanism: "SCRAM-SHA-256",
      saslUsername: "app",
      saslPassword: "secret",
      tlsInsecureSkipVerify: false,
    });
    const open = propertiesToConnectionForm(parsePropertiesText("bootstrap.servers=b:9092"));
    // key 缺省 = 保持默认校验（不跳过）
    expect(open.tlsInsecureSkipVerify).toBe(false);
    expect(open.saslUsername).toBe("");
    const skipped = propertiesToConnectionForm(parsePropertiesText("ssl.endpoint.identification.algorithm="));
    expect(skipped.tlsInsecureSkipVerify).toBe(true);
    const skippedNone = propertiesToConnectionForm(parsePropertiesText("ssl.endpoint.identification.algorithm=NONE"));
    expect(skippedNone.tlsInsecureSkipVerify).toBe(true);
  });
});

describe("modal keydown decision (Esc close + Tab focus trap)", () => {
  it("closes on Escape regardless of Tab state", () => {
    expect(decideModalKeydown("Escape", false, 0, -1)).toEqual({ kind: "close" });
    expect(decideModalKeydown("Escape", true, 5, 2)).toEqual({ kind: "close" });
  });

  it("ignores non-Esc/Tab keys and empty containers", () => {
    expect(decideModalKeydown("Enter", false, 5, 0)).toEqual({ kind: "none" });
    expect(decideModalKeydown("Tab", false, 0, -1)).toEqual({ kind: "none" });
    expect(decideModalKeydown("Tab", true, 0, 3)).toEqual({ kind: "none" });
  });

  it("cycles forward with wrap-around", () => {
    expect(decideModalKeydown("Tab", false, 3, 0)).toEqual({ kind: "focus", index: 1 });
    expect(decideModalKeydown("Tab", false, 3, 2)).toEqual({ kind: "focus", index: 0 });
  });

  it("cycles backward with wrap-around", () => {
    expect(decideModalKeydown("Tab", true, 3, 2)).toEqual({ kind: "focus", index: 1 });
    expect(decideModalKeydown("Tab", true, 3, 0)).toEqual({ kind: "focus", index: 2 });
  });

  it("enters at the start/end when focus is outside the container", () => {
    // 焦点尚未进容器（如打开瞬间）：Tab 进首个控件，Shift+Tab 进最后一个。
    expect(decideModalKeydown("Tab", false, 4, -1)).toEqual({ kind: "focus", index: 0 });
    expect(decideModalKeydown("Tab", true, 4, -1)).toEqual({ kind: "focus", index: 3 });
    // 越界（焦点被容器外逻辑移走）同样按方向兜底。
    expect(decideModalKeydown("Tab", false, 4, 99)).toEqual({ kind: "focus", index: 0 });
    expect(decideModalKeydown("Tab", true, 4, 99)).toEqual({ kind: "focus", index: 3 });
  });
});

// == Phase 3 H 路：F4 Flow 生成器固定向量 / 占位符展开 / F5 树模型 /
//    F2 表单联动 / F6-4 分区校验（新增，均纯函数） ==============================

import {
  buildSchemaTree,
  clampFlowCount,
  clampFlowIntervalMs,
  expandTemplate,
  filterMessagesByKeyword,
  formatTimestamp,
  generateAvroRandom,
  matchingSchemaSubjects,
  mulberry32,
  oauthFormVisibility,
  oauthRequiresSaslSsl,
  partitionInputIssue,
  timestampFilterTextComparator,
  timestampIso,
} from "./kafkaModel";

describe("mulberry32 (F4 fixed-seed RNG)", () => {
  // 固定向量（照 zstd 预生成向量范式：实现 + 种子 42 的序列锁定，防算法漂移）。
  it("produces the locked sequence for seed 42", () => {
    const rng = mulberry32(42);
    const sequence = [rng(), rng(), rng(), rng(), rng()];
    expect(sequence).toEqual([
      0.6011037519201636,
      0.44829055899754167,
      0.8524657934904099,
      0.6697340414393693,
      0.17481389874592423,
    ]);
  });

  it("is deterministic per seed and stays in [0,1)", () => {
    const left = mulberry32(7);
    const right = mulberry32(7);
    for (let index = 0; index < 32; index += 1) {
      const a = left();
      const b = right();
      expect(a).toBe(b);
      expect(a).toBeGreaterThanOrEqual(0);
      expect(a).toBeLessThan(1);
    }
    expect(mulberry32(8)()).not.toBe(mulberry32(7)());
  });
});

const ORDER_AVRO_SCHEMA = {
  type: "record",
  name: "Order",
  fields: [
    { name: "orderId", type: "string" },
    { name: "amount", type: "double" },
    { name: "quantity", type: "int" },
    { name: "paid", type: "boolean" },
    { name: "status", type: { type: "enum", name: "Status", symbols: ["NEW", "PAID", "CANCELLED"] } },
    { name: "tags", type: { type: "array", items: "string" } },
    { name: "meta", type: { type: "map", values: "string" } },
    { name: "ref", type: ["null", "string"], default: null },
    { name: "day", type: { type: "int", logicalType: "date" } },
    { name: "ts", type: { type: "long", logicalType: "timestamp-millis" } },
    { name: "uid", type: { type: "string", logicalType: "uuid" } },
  ],
};

describe("generateAvroRandom (F4 schema_random)", () => {
  // 固定向量：seed=7 + now=1700000000000 的完整生成值锁定。
  it("matches the locked vector for the Order schema (seed 7)", () => {
    const out = generateAvroRandom(JSON.stringify(ORDER_AVRO_SCHEMA), mulberry32(7), 1_700_000_000_000);
    expect(out).toEqual({
      value:
        '{"orderId":"alpha-155","amount":97.69,"quantity":699,"paid":false,"status":"PAID","tags":["beta-597","delta-332"],"meta":{"k0":"delta-566"},"ref":"alpha-431","day":19675,"ts":1700000000000,"uid":"484f3e32-248c-41e8-9af9-ed025601c567"}',
    });
  });

  it("covers record/array/map/union/enum/fixed and logical types", () => {
    const schema = {
      type: "record",
      name: "All",
      fields: [
        { name: "unionPicksFirstNonNull", type: ["null", "string", "int"] },
        { name: "allNullUnion", type: ["null"] },
        { name: "fixed16", type: { type: "fixed", size: 16, name: "H16" } },
        { name: "dateDays", type: { type: "int", logicalType: "date" } },
        { name: "tsMillis", type: { type: "long", logicalType: "timestamp-millis" } },
        { name: "uid", type: { type: "string", logicalType: "uuid" } },
        { name: "amount", type: { type: "bytes", logicalType: "decimal", precision: 8, scale: 2 } },
      ],
    };
    const out = generateAvroRandom(JSON.stringify(schema), mulberry32(3), 1_700_000_000_000);
    expect("error" in out).toBe(false);
    const value = JSON.parse((out as { value: string }).value) as Record<string, unknown>;
    // union 取非 null 首支（string）；全 null union 只能是 null。
    expect(typeof value.unionPicksFirstNonNull).toBe("string");
    expect(value.allNullUnion).toBeNull();
    // fixed(size 16) → 16 位字符串；date → days 数；timestamp-millis → nowMs。
    expect(String(value.fixed16)).toHaveLength(16);
    expect(value.dateDays).toBe(19675);
    expect(value.tsMillis).toBe(1_700_000_000_000);
    // uuid 形状；decimal 为数值（goavro 侧编码支持度由后端契约管辖）。
    expect(String(value.uid)).toMatch(/^[0-9a-f-]{36}$/);
    expect(typeof value.amount).toBe("number");
  });

  it("reports deterministic errors for invalid schema text", () => {
    expect("error" in generateAvroRandom("not json", mulberry32(1))).toBe(true);
    expect("error" in generateAvroRandom("[]", mulberry32(1))).toBe(true);
    expect(generateAvroRandom("{}", mulberry32(1))).toEqual({ error: 'avro schema is missing "type"' });
    // 未知类型在首次生成时抛错（不产出畸形数据）。
    const out = generateAvroRandom('{"type":"warp"}', mulberry32(1));
    expect("error" in out && out.error).toContain("warp");
  });
});

describe("expandTemplate (F4 template placeholders)", () => {
  // 固定向量：seed=11 + now=1700000000000。
  it("matches the locked vector for all placeholder kinds", () => {
    const out = expandTemplate(
      '{"id":"{uuid}","n":{int:1,9},"f":{float:0,100},"k":"{pick:gold|silver}","now":"{now}"}',
      mulberry32(11),
      1_700_000_000_000,
    );
    expect(out).toBe('{"id":"8899d818-977d-46aa-2246-692996dcc594","n":1,"f":74.09,"k":"silver","now":"2023-11-14T22:13:20.000Z"}');
  });

  it("leaves unknown placeholders and edge cases untouched", () => {
    const rng = mulberry32(1);
    expect(expandTemplate('{"keep":"{nope}","multi":"{int:5,5}","empty":"{pick:}","now":"{now}"}', rng, 0)).toBe(
      '{"keep":"{nope}","multi":"5","empty":"{pick:}","now":"1970-01-01T00:00:00.000Z"}',
    );
    expect(expandTemplate("plain", rng, 0)).toBe("plain");
    // int 边界：min=max 时恒等。
    for (let index = 0; index < 8; index += 1) {
      expect(expandTemplate("{int:3,3}", rng, 0)).toBe("3");
    }
  });
});

describe("matchingSchemaSubjects (F4 subject discovery)", () => {
  it("prefix-matches <topic>-value first and <topic>-key second", () => {
    const subjects = ["order-events-value", "order-events-key", "order-events-v2-value", "other-value"];
    expect(matchingSchemaSubjects("order-events", subjects)).toEqual({
      key: "order-events-key",
      value: "order-events-value",
    });
    expect(matchingSchemaSubjects("order-events", ["order-events-key"])).toEqual({ key: "order-events-key" });
    expect(matchingSchemaSubjects("missing", subjects)).toEqual({});
    expect(matchingSchemaSubjects("", subjects)).toEqual({});
  });
});

describe("flow parameter clamps (F4)", () => {
  it("clamps countPerSend into 1..100 with fallback 1", () => {
    expect(clampFlowCount("1")).toBe(1);
    expect(clampFlowCount(100)).toBe(100);
    expect(clampFlowCount(0)).toBe(1);
    expect(clampFlowCount(101)).toBe(100);
    expect(clampFlowCount("abc")).toBe(1);
    expect(clampFlowCount(undefined)).toBe(1);
    expect(clampFlowCount(-5)).toBe(1);
  });

  it("clamps intervalMs into 250..10000 with fallback 1000", () => {
    expect(clampFlowIntervalMs("1000")).toBe(1000);
    expect(clampFlowIntervalMs(250)).toBe(250);
    expect(clampFlowIntervalMs(249)).toBe(250);
    expect(clampFlowIntervalMs(10001)).toBe(10000);
    expect(clampFlowIntervalMs("")).toBe(1000);
  });
});

describe("oauth form visibility (F2 frozen field chain)", () => {
  it("reveals oauth_token_source only for SASL protocol + OAUTHBEARER", () => {
    expect(oauthFormVisibility("SASL_SSL", "OAUTHBEARER", "")).toEqual({ oauthTokenSource: true, mskFields: false, staticToken: false });
    expect(oauthFormVisibility("PLAINTEXT", "OAUTHBEARER", "msk_iam")).toEqual({ oauthTokenSource: false, mskFields: false, staticToken: false });
    expect(oauthFormVisibility("SASL_SSL", "SCRAM-SHA-512", "msk_iam")).toEqual({ oauthTokenSource: false, mskFields: false, staticToken: false });
  });

  it("branches msk_* group vs oauth_static_token by token source", () => {
    expect(oauthFormVisibility("SASL_SSL", "OAUTHBEARER", "msk_iam")).toEqual({ oauthTokenSource: true, mskFields: true, staticToken: false });
    expect(oauthFormVisibility("SASL_PLAINTEXT", "OAUTHBEARER", "static_token")).toEqual({ oauthTokenSource: true, mskFields: false, staticToken: true });
    // 未选/非法来源：两组都不可见，仅 token source 选择器可见。
    expect(oauthFormVisibility("SASL_SSL", "OAUTHBEARER", "bogus")).toEqual({ oauthTokenSource: true, mskFields: false, staticToken: false });
  });

  it("requires SASL_SSL for OAUTHBEARER (backend -32602 guard)", () => {
    expect(oauthRequiresSaslSsl("SASL_SSL", "OAUTHBEARER")).toBe(false);
    expect(oauthRequiresSaslSsl("SASL_PLAINTEXT", "OAUTHBEARER")).toBe(true);
    expect(oauthRequiresSaslSsl("SASL_SSL", "PLAIN")).toBe(false);
  });
});

describe("partitionInputIssue (F6-4 produce partition guard)", () => {
  it("accepts empty (auto) and in-range partitions", () => {
    expect(partitionInputIssue("", 3)).toBeNull();
    expect(partitionInputIssue("  ", 3)).toBeNull();
    expect(partitionInputIssue("0", 3)).toBeNull();
    expect(partitionInputIssue("2", 3)).toBeNull();
  });

  it("rejects non-integers and out-of-range partitions with i18n key fragments", () => {
    expect(partitionInputIssue("-1", 3)).toBe("partitionInvalid");
    expect(partitionInputIssue("1.5", 3)).toBe("partitionInvalid");
    expect(partitionInputIssue("abc", 3)).toBe("partitionInvalid");
    expect(partitionInputIssue("3", 3)).toBe("partitionOutOfRange");
    expect(partitionInputIssue("9", 3)).toBe("partitionOutOfRange");
    // partitionCount 缺省（未拿到 topics/list）不做上界校验。
    expect(partitionInputIssue("9", undefined)).toBeNull();
  });
});

describe("schema tree model (F5)", () => {
  it("builds a collapsible tree from an AVRO record", () => {
    const root = buildSchemaTree(JSON.stringify(ORDER_AVRO_SCHEMA))!;
    expect(root.type).toBe("record");
    const names = root.children!.map((child) => child.name);
    expect(names).toEqual(["orderId", "amount", "quantity", "paid", "status", "tags", "meta", "ref", "day", "ts", "uid"]);
    const status = root.children!.find((child) => child.name === "status")!;
    expect(status.type).toContain("NEW");
    // union 节点带分支子节点；逻辑类型在类型标注里。
    const ref = root.children!.find((child) => child.name === "ref")!;
    expect(ref.type).toBe("union:string");
    expect(ref.children!.map((child) => child.name)).toEqual(["[0]", "[1]"]);
    const day = root.children!.find((child) => child.name === "day")!;
    expect(day.type).toContain("date");
  });

  it("renders Avro field defaults and JSON Schema required markers", () => {
    const avro = buildSchemaTree('{"type":"record","name":"R","fields":[{"name":"c","type":"string","default":"usd"}]}')!;
    expect(avro.children![0].defaultValue).toBe("usd");

    const json = buildSchemaTree('{"type":"object","properties":{"id":{"type":"string"},"n":{"type":"integer","default":7}},"required":["id"]}')!;
    expect(json.children!.map((child) => child.name)).toEqual(["id", "n"]);
    expect(json.children![0].type).toContain("*");
    expect(json.children![1].defaultValue).toBe("7");
  });

  it("returns null for PROTOBUF text / broken JSON (text + hint path)", () => {
    expect(buildSchemaTree('syntax = "proto3";\\nmessage Order {}')).toBeNull();
    expect(buildSchemaTree("{broken")).toBeNull();
  });
});

describe("timestamp tz helpers (F6-3)", () => {
  it("formats local vs utc and exposes full ISO for cell titles", () => {
    const ms = Date.UTC(2023, 10, 14, 22, 13, 20);
    expect(formatTimestamp(ms, "utc")).toBe("2023-11-14 22:13:20");
    expect(timestampIso(ms)).toBe("2023-11-14T22:13:20.000Z");
    // local 与 utc 的日期文本按定义逐分量断言（与机器时区无关）。
    const local = formatTimestamp(ms, "local");
    const date = new Date(ms);
    const pad = (value: number) => String(value).padStart(2, "0");
    expect(local).toBe(`${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`);
    // 缺省参数 = local（既有调用面行为不变）；非法值兜底。
    expect(formatTimestamp(ms)).toBe(local);
    expect(formatTimestamp(undefined)).toBe("—");
    expect(timestampIso(undefined)).toBe("");
  });

  it("compares date-filter days after parsing cell text in its tz", () => {
    const filterDay = new Date(2023, 10, 14);
    expect(timestampFilterTextComparator(filterDay, "2023-11-14 08:00:00", "local")).toBe(0);
    expect(timestampFilterTextComparator(filterDay, "2023-11-13 23:59:59", "local")).toBe(-1);
    expect(timestampFilterTextComparator(filterDay, "2023-11-15 00:00:00", "local")).toBe(1);
    expect(timestampFilterTextComparator(filterDay, "garbage", "local")).toBe(1);
    // UTC 文本按 UTC 解析为时刻再取本地日比较。
    const instant = new Date(Date.UTC(2023, 10, 13, 23, 0, 0));
    const cellLocalDay = new Date(instant.getFullYear(), instant.getMonth(), instant.getDate());
    expect(timestampFilterTextComparator(cellLocalDay, "2023-11-13 23:00:00", "utc")).toBe(0);
  });
});

describe("filterMessagesByKeyword (F6-1 stream quick filter)", () => {
  const rows: Array<{ partition: number; offset: number; key: string; valueText: string; headers: Record<string, string> }> = [
    { partition: 0, offset: 1, key: "alpha", valueText: '{"v":1}', headers: { trace: "t1" } },
    { partition: 1, offset: 2, key: "beta", valueText: "plain", headers: {} },
    { partition: 2, offset: 3, key: "gamma", valueText: "gold", headers: {} },
  ];

  it("filters loaded rows across partition/offset/key/value/headers", () => {
    expect(filterMessagesByKeyword(rows, "")).toEqual(rows);
    expect(filterMessagesByKeyword(rows, "  ")).toEqual(rows);
    expect(filterMessagesByKeyword(rows, "alpha")).toEqual([rows[0]]);
    // "2" 同时命中 offset=2 与 partition=2（contains 语义，只看已加载行）。
    expect(filterMessagesByKeyword(rows, "2")).toEqual([rows[1], rows[2]]);
    expect(filterMessagesByKeyword(rows, "beta")).toEqual([rows[1]]);
    expect(filterMessagesByKeyword(rows, "TRACE")).toEqual([rows[0]]);
    expect(filterMessagesByKeyword(rows, "nope")).toEqual([]);
  });
});
