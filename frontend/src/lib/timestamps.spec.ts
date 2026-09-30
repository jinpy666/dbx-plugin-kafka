// timestamps 纯函数单测（F6-3/F6-6）：tz 感知格式化 + 日期过滤比较器。
import { describe, expect, it } from "vitest";
import { formatTimestamp, timestampFilterTextComparator, timestampIso } from "./timestamps";

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
    // 缺省参数 = local（既有调用面行为不变）；非法值兜底；epoch 0 是合法时间戳。
    expect(formatTimestamp(ms)).toBe(local);
    expect(formatTimestamp(undefined)).toBe("—");
    expect(timestampIso(undefined)).toBe("");
    expect(formatTimestamp(0, "utc")).toBe("1970-01-01 00:00:00");
    expect(timestampIso(0)).toBe("1970-01-01T00:00:00.000Z");
  });

  it("compares date-filter days after parsing cell text in its tz", () => {
    const filterDay = new Date(2023, 10, 14);
    expect(timestampFilterTextComparator(filterDay, "2023-11-14 08:00:00", "local")).toBe(0);
    expect(timestampFilterTextComparator(filterDay, "2023-11-13 23:59:59", "local")).toBe(-1);
    expect(timestampFilterTextComparator(filterDay, "2023-11-15 00:00:00", "local")).toBe(1);
    expect(timestampFilterTextComparator(filterDay, "garbage", "local")).toBe(1);
    // UTC 模式按「日序数」比较（评审 L：旧实现取本地分量，跨日时区整体错位
    // 一天）——单元格文本取 UTC 分量，过滤日期的本地 Y/M/D 当日序数。
    expect(timestampFilterTextComparator(new Date(2023, 10, 13), "2023-11-13 23:00:00", "utc")).toBe(0);
    expect(timestampFilterTextComparator(new Date(2023, 10, 13), "2023-11-14 00:30:00", "utc")).toBe(1);
    expect(timestampFilterTextComparator(new Date(2023, 10, 13), "2023-11-12 23:59:59", "utc")).toBe(-1);
  });
});
