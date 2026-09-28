// messageExport 纯函数单测：CSV（RFC 4180）/TSV/JSON 导出序列化。
import { describe, expect, it } from "vitest";
import { serializeMessagesToCsv, serializeMessagesToJson, serializeMessagesToTsv } from "./messageExport";
import type { KafkaMessage } from "./api";

function b64(text: string): string {
  const bytes = new TextEncoder().encode(text);
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
}

function msg(overrides: Partial<KafkaMessage>): KafkaMessage {
  return { topic: "orders", partition: 0, offset: 1, timestamp: 1_700_000_000_000, ...overrides };
}

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

// S-EXPORT-FORMULA（评审 LOW-5）：CSV/TSV 公式注入中和——以 = + - @ TAB CR
// 开头的单元格在 Excel/Sheets 中会被当公式执行，前缀 ' 中和。
describe("export formula neutralization", () => {
  it("neutralizes formula-leading cells in CSV", () => {
    const csv = serializeMessagesToCsv([
      msg({ key: "-1", valueText: "=cmd|' /C calc'!A0" }),
      msg({ key: "safe", valueText: "plain" }),
    ]);
    expect(csv).toContain("'-1");
    expect(csv).toContain("'=cmd");
    expect(csv).toContain("plain");
    // 安全值不加前缀。
    expect(csv).not.toContain("'safe");
  });

  it("neutralizes formula-leading cells in TSV", () => {
    const tsv = serializeMessagesToTsv([msg({ key: "+1", valueText: "@x" })]);
    expect(tsv).toContain("'+1");
    expect(tsv).toContain("'@x");
  });
});
