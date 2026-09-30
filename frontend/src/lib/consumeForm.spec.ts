// consumeForm 纯函数单测：分区/位点解析、表单互斥校验、时间输入互转、fieldFilter 校验。
import { describe, expect, it } from "vitest";
import {
  fieldFilterIssue,
  isRangeReversed,
  nowDatetimeLocal,
  offsetTimeToParam,
  offsetTimeToUnixMs,
  parseGroupOffsetTargetsText,
  parsePartitionList,
  parsePartitionOffsetsText,
  partitionOffsetsToText,
  switchTimeInputMode,
  unixMsToDatetimeLocal,
  validateConsumeForm,
} from "./consumeForm";

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
    expect(offsetTimeToParam("1700000000000")).toBe("1700000000000");
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
