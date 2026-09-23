// lib/kafkaColumns 单测：列定义形状（字段/过滤类型/排序）、VM 映射、
// minimalColumns 窄容器降级、页大小持久化（pluginStore/宿主 storage）、ag 内置文案七语键齐。
// @vitest-environment happy-dom
import { describe, expect, it } from "vitest";
import {
  AG_GRID_LOCALE_KEYS,
  DEFAULT_PAGE_SIZE,
  agGridLocaleText,
  aclColumns,
  groupColumns,
  groupOffsetColumns,
  lagColumns,
  loadPreferredPageSize,
  memberColumns,
  messageColumns,
  minimalColumns,
  partitionColumns,
  savePreferredPageSize,
  schemaVersionColumns,
  setWorkbenchTimestampTz,
  subjectColumns,
  toAclRows,
  toGroupOffsetRows,
  toLagRows,
  toMemberRows,
  toMessageRows,
  toPartitionRows,
  toSchemaVersionRows,
  toSubjectRows,
  toTopicOffsetRows,
  toTopicRows,
  topicColumns,
  topicOffsetColumns,
} from "./kafkaColumns";
import { setWorkbenchLocale, messages, t } from "./i18n";
import type { MessageRow, SchemaVersionVm, TopicVm } from "./kafkaColumns";
import type { KafkaMessage } from "./api";

describe("column builders", () => {
  it("message columns expose sortable/filterable fields", () => {
    setWorkbenchLocale("en");
    const cols = messageColumns();
    expect(cols.map((col) => col.field)).toEqual([
      "partition",
      "offset",
      "timestampText",
      "keyText",
      "valueText",
      "headersText",
      "schemaText",
    ]);
    for (const col of cols) {
      expect(col.sortable).toBe(true);
      // F6-6：时间戳列为 agDateColumnFilter（社区版自带 date filter）。
      expect(["agTextColumnFilter", "agNumberColumnFilter", "agDateColumnFilter"]).toContain(col.filter);
    }
    expect(cols[0].filter).toBe("agNumberColumnFilter");
  });

  // F6-6：timestamp 列 date filter 比较器按当前 tz 解析单元格文本按天比较。
  it("timestamp column uses the date filter with a tz-aware day comparator (F6-6)", () => {
    setWorkbenchLocale("en");
    const col = messageColumns().find((def) => def.field === "timestampText")!;
    expect(col.filter).toBe("agDateColumnFilter");
    const comparator = (col.filterParams as { comparator: (filterDate: Date, value: unknown) => number }).comparator;
    const filterDay = new Date(2023, 10, 14); // 本地 2023-11-14 零点
    // local tz（模块缺省）：14 日当天 → 0；13 日 → 负；15 日 → 正；非时间文本排后。
    setWorkbenchTimestampTz("local");
    expect(comparator(filterDay, "2023-11-14 08:00:00")).toBe(0);
    expect(comparator(filterDay, "2023-11-13 23:59:59")).toBeLessThan(0);
    expect(comparator(filterDay, "2023-11-15 00:00:01")).toBeGreaterThan(0);
    expect(comparator(filterDay, "not-a-date")).toBe(1);
    // utc tz：UTC 文本按 UTC 解析为时刻后取「本地日」与过滤日期的本地零点按天
    // 比较（与机器时区无关的确定性断言：先求出该时刻的本地零点）。
    setWorkbenchTimestampTz("utc");
    const instant = new Date(Date.UTC(2023, 10, 13, 23, 0, 0));
    const cellLocalDay = new Date(instant.getFullYear(), instant.getMonth(), instant.getDate());
    expect(comparator(cellLocalDay, "2023-11-13 23:00:00")).toBe(0);
    expect(comparator(new Date(cellLocalDay.getTime() + 86400000), "2023-11-13 23:00:00")).toBeLessThan(0);
    setWorkbenchTimestampTz("local");
  });

  it("every builder produces header names for the current locale", () => {
    setWorkbenchLocale("zh-CN");
    const builders = [messageColumns, groupColumns, groupOffsetColumns, memberColumns, aclColumns, topicColumns, partitionColumns, topicOffsetColumns, subjectColumns, schemaVersionColumns, lagColumns];
    for (const build of builders) {
      for (const col of build()) {
        expect(String(col.headerName).length).toBeGreaterThan(0);
      }
    }
  });

  it("subject/version/lag columns match the Phase 2 contract shape", () => {
    setWorkbenchLocale("en");
    expect(subjectColumns().map((col) => col.field)).toEqual(["subject", "formats", "latestVersion", "compatibilityLevel"]);
    expect(schemaVersionColumns().map((col) => col.field)).toEqual(["version", "id", "format"]);
    expect(lagColumns().map((col) => col.field)).toEqual(["topic", "partition", "committedText", "endOffset", "lag"]);
  });
});

describe("row mappers", () => {
  it("maps messages with previews and schema badge text", () => {
    setWorkbenchLocale("en");
    const message: KafkaMessage = {
      topic: "t",
      partition: 1,
      offset: 2,
      timestamp: 0,
      key: "k",
      valueText: "v",
      headers: { a: "1" },
      schemaId: 3,
      schemaSubject: "s-value",
      schemaVersion: 5,
    };
    const rows = toMessageRows([message]);
    expect(rows[0].id).toBe("1:2");
    expect(rows[0].schemaText).toBe("s-value v5");
    expect(rows[0].raw).toBe(message);
    expect(rows[0].keyText).toBe("k");
  });

  it("marks uncommitted offset rows and null lag", () => {
    setWorkbenchLocale("en");
    const rows = toGroupOffsetRows([
      { topic: "t", partition: 0, hasCommitted: false, lag: undefined },
      { topic: "t", partition: 1, committedOffset: 4, lag: 2 },
    ]);
    expect(rows[0].committedText).toContain("no committed data");
    expect(rows[0].lag).toBeNull();
    expect(rows[1].lag).toBe(2);
    expect(toLagRows(rows.map((row) => row.raw))[0].id).toBe("t:0");
  });

  it("maps partitions health flag and internal topics", () => {
    setWorkbenchLocale("en");
    const partitions = toPartitionRows([{ partition: 0, leader: 1, replicas: [1], isr: [1], offlineReplicas: [], isHealthy: false }]);
    expect(partitions[0].healthy).toBe(false);
    expect(partitions[0].healthyText).toBe("degraded");
    const topics = toTopicRows([{ name: "_internal", partitionCount: 1, replicationFactor: 1, isInternal: true }]);
    expect(topics[0].internalText).toBe("internal");
  });

  it("maps acl / subject / version / topic offset rows", () => {
    setWorkbenchLocale("en");
    const acls = toAclRows([{ resourceType: "TOPIC", resourceName: "r", principal: "User:a", operation: "READ", permission: "ALLOW" }]);
    expect(acls[0].patternType).toBe("LITERAL");
    expect(acls[0].host).toBe("*");
    const subjects = toSubjectRows([{ subject: "s", formats: ["avro"], latestVersion: 2 }]);
    expect(subjects[0].formats).toBe("avro");
    expect(subjects[0].latestVersion).toBe(2);
    const versions = toSchemaVersionRows([{ version: 1, id: 11, format: "avro" }]);
    expect(versions[0].raw.version).toBe(1);
    const offsets = toTopicOffsetRows([{ topic: "t", partition: 0, offset: 9 }]);
    expect(offsets[0].leaderEpoch).toBe("—");
    expect(toMemberRows([{ memberId: "m", assignments: { t: [0, 1] } }])[0].assignments).toBe("t[0,1]");
  });
});

describe("minimalColumns", () => {
  it("keeps only the requested fields in order", () => {
    setWorkbenchLocale("en");
    const defs = messageColumns();
    const compact = minimalColumns(defs, ["partition", "valueText"]);
    expect(compact.map((col) => col.field)).toEqual(["partition", "valueText"]);
    expect(compact).toHaveLength(2);
  });
});

describe("page size persistence", () => {
  it("round-trips a preferred page size with safe fallbacks", () => {
    savePreferredPageSize("spec-table", 123);
    expect(loadPreferredPageSize("spec-table")).toBe(123);
    savePreferredPageSize("spec-table", 50);
    expect(loadPreferredPageSize("spec-table")).toBe(50);
    expect(loadPreferredPageSize("spec-table-never-set")).toBe(DEFAULT_PAGE_SIZE);
  });
});

describe("ag-grid built-in locale text", () => {
  it("covers the same key set across the seven workbench locales", () => {
    const locales = ["en", "zh-CN", "zh-TW", "es", "it", "ja", "pt-BR"];
    for (const locale of locales) {
      setWorkbenchLocale(locale);
      const text = agGridLocaleText();
      for (const key of AG_GRID_LOCALE_KEYS) {
        expect(typeof text[key], `${locale}:${key}`).toBe("string");
        expect(text[key].length, `${locale}:${key}`).toBeGreaterThan(0);
      }
    }
  });

  // P2-19 防回归：ag-grid v36 分页条用 to/of/page 组合出「1 to 50 of 100」与
  // 「Page 1 of 2」，此前这些键缺失导致中英混排。七语键齐由上面的键集测试守护，
  // 这里冒烟各语族的组合值。
  it("localizes the pagination summary composition keys (P2-19)", () => {
    setWorkbenchLocale("en");
    expect(agGridLocaleText()).toMatchObject({ page: "Page", to: "to", of: "of", firstPage: "First Page" });
    setWorkbenchLocale("zh-CN");
    expect(agGridLocaleText()).toMatchObject({ page: "第", to: "至", of: "/ 共", lastPage: "最后一页" });
    setWorkbenchLocale("zh-TW");
    expect(agGridLocaleText()).toMatchObject({ page: "第", to: "至", of: "/ 共" });
    setWorkbenchLocale("ja");
    expect(agGridLocaleText()).toMatchObject({ page: "ページ", to: "～", of: "/" });
    for (const locale of ["es", "it", "pt-BR"] as const) {
      setWorkbenchLocale(locale);
      expect(agGridLocaleText().to).toBe("a");
    }
    setWorkbenchLocale("en");
  });
});

// -- P2-12 防回归：列定义引用的 i18n 键必须真实存在 -----------------------------------

const LOCALES = ["en", "zh-CN", "zh-TW", "es", "it", "ja", "pt-BR"] as const;
const BUILDERS = [
  messageColumns,
  groupColumns,
  groupOffsetColumns,
  memberColumns,
  aclColumns,
  topicColumns,
  partitionColumns,
  topicOffsetColumns,
  subjectColumns,
  schemaVersionColumns,
  lagColumns,
];

/** t() 未命中时原样返回键名（形如 `ns.camelKey` 的点分键样式）。 */
const UNRESOLVED_KEY_PATTERN = /^[a-z][a-zA-Z0-9]*(\.[a-zA-Z0-9]+)+$/;

function i18nLookup(locale: (typeof LOCALES)[number], key: string): unknown {
  return key.split(".").reduce<unknown>(
    (value, part) => (value && typeof value === "object" ? (value as Record<string, unknown>)[part] : undefined),
    messages[locale],
  );
}

describe("column i18n key existence (P2-12 regression guard)", () => {
  it("resolves consumer-group state enums via existing messages.state* keys in every locale", () => {
    const states = ["Stable", "Empty", "Preparing", "PreparingRebalance", "CompletingRebalance", "Dead"];
    for (const locale of LOCALES) {
      for (const state of states) {
        // 键位回归：曾错挂 groups.state* 导致 t() 未命中、整列回退英文原文。
        expect(typeof i18nLookup(locale, `messages.state${state}`), `${locale}:messages.state${state}`).toBe("string");
      }
    }
    // zh-CN 链路冒烟：Stable 列不再显示原始键/英文枚举。
    setWorkbenchLocale("zh-CN");
    const formatter = groupColumns().find((col) => col.field === "state")!.valueFormatter as (params: { value: string }) => string;
    expect(formatter({ value: "Stable" })).toBe("稳定");
    expect(formatter({ value: "Dead" })).toBe("已失效");
    // 未知枚举原文兜底。
    expect(formatter({ value: "SomeNewState" })).toBe("SomeNewState");
    expect(formatter({ value: "—" })).toBe("—");
    setWorkbenchLocale("en");
  });

  it("never renders a raw dotted i18n key as a column header in any locale", () => {
    for (const locale of LOCALES) {
      setWorkbenchLocale(locale);
      for (const build of BUILDERS) {
        for (const col of build()) {
          expect(String(col.headerName), `${locale}:${build.name}:${col.field}`).not.toMatch(UNRESOLVED_KEY_PATTERN);
        }
      }
    }
  });
});

// F6-2/F5：行操作列（cellRenderer 原生 button DOM）渲染产物与回调接线。
describe("action columns (F6-2 copy JSON / F5 clone)", () => {
  it("messageColumns adds a copy-JSON action column only when a callback is given", () => {
    setWorkbenchLocale("en");
    expect(messageColumns().some((col) => String(col.colId).startsWith("action-"))).toBe(false);
    const seen: MessageRow[] = [];
    const cols = messageColumns({ onCopyJson: (row) => seen.push(row) });
    const action = cols.find((col) => col.field === undefined) as unknown as {
      cellRenderer: (params: { data?: MessageRow }) => HTMLElement;
    };
    expect(action).toBeDefined();
    const button = action.cellRenderer({ data: toMessageRows([{ topic: "t", partition: 0, offset: 1, timestamp: 1 }])[0] });
    expect(button.textContent).toBe("{}");
    expect(button.getAttribute("aria-label")).toBe(t("messages.copyRowJson"));
    document.body.appendChild(button);
    button.click();
    expect(seen).toHaveLength(1);
    button.remove();
    // data 缺省时不触发回调。
    expect(() => action.cellRenderer({ data: undefined })).not.toThrow();
    expect(seen).toHaveLength(1);
  });

  it("schemaVersionColumns adds a clone action column wired to the callback", () => {
    setWorkbenchLocale("en");
    const seen: number[] = [];
    const cols = schemaVersionColumns({ onClone: (row) => seen.push(row.version) });
    const action = cols.find((col) => col.field === undefined) as unknown as {
      cellRenderer: (params: { data?: SchemaVersionVm }) => HTMLElement;
    };
    expect(action).toBeDefined();
    const button = action.cellRenderer({ data: { version: 2, id: 11, format: "avro", raw: { version: 2, id: 11, format: "avro" } } });
    expect(button.textContent).toBe("⧉");
    expect(button.getAttribute("aria-label")).toBe(t("schemas.clone"));
    document.body.appendChild(button);
    button.click();
    expect(seen).toEqual([2]);
    button.remove();
  });
});

// P2-23 回归守卫：数值单元格统一千分位（跟随工作台 locale），与 ag-grid 分页条
// 「共 5,000」同屏一致；非数值占位（如「—」）原样透传、空值返回空串。
describe("number cell thousands formatting (P2-23 regression guard)", () => {
  it("formats lag/endOffset cells with thousands separators per locale", () => {
    setWorkbenchLocale("en");
    const lagFormatter = lagColumns().find((col) => col.field === "lag")!.valueFormatter as (params: { value: unknown }) => string;
    const endFormatter = groupOffsetColumns().find((col) => col.field === "endOffset")!.valueFormatter as (params: { value: unknown }) => string;
    expect(lagFormatter({ value: 4701 })).toBe("4,701");
    expect(lagFormatter({ value: 1003 })).toBe("1,003");
    // startOffset/endOffset 的 VM 值是字符串数字，同样走千分位
    expect(endFormatter({ value: "102400" })).toBe("102,400");
  });

  it("keeps non-numeric placeholders and empty values untouched", () => {
    setWorkbenchLocale("zh-CN");
    const lagFormatter = lagColumns().find((col) => col.field === "lag")!.valueFormatter as (params: { value: unknown }) => string;
    expect(lagFormatter({ value: "—" })).toBe("—");
    expect(lagFormatter({ value: "abc" })).toBe("abc");
    expect(lagFormatter({ value: null })).toBe("");
    expect(lagFormatter({ value: undefined })).toBe("");
    expect(lagFormatter({ value: 0 })).toBe("0");
    // zh-CN 千分位与 en 同为 3 位分组（跟随宿主 locale 即可）
    expect(lagFormatter({ value: 5000 })).toBe("5,000");
  });
});

// Lane4 前端打磨：topic 表收藏星标列（回调可选；星形文案经 isFavorite 现读）。
describe("topic favorite star column (Lane4)", () => {
  const row = (name: string): TopicVm => ({ name, partitionCount: 1, replicationFactor: 1, internalText: "", raw: { name, partitionCount: 1, replicationFactor: 1 } });

  it("adds a favorite star column only when a callback is given", () => {
    setWorkbenchLocale("en");
    expect(topicColumns().some((col) => col.colId === "action-favorite")).toBe(false);
    const seen: string[] = [];
    const cols = topicColumns({
      onToggleFavorite: (target) => seen.push(target.name),
      isFavorite: (target) => target.name === "users",
    });
    expect(cols[0]!.colId).toBe("action-favorite");
    expect(cols).toHaveLength(5);
    const action = cols[0] as unknown as { cellRenderer: (params: { data?: TopicVm }) => HTMLElement };
    // 已收藏：实心星 + 取消收藏文案；未收藏：空心星 + 收藏文案
    const favButton = action.cellRenderer({ data: row("users") });
    expect(favButton.textContent).toBe("★");
    expect(favButton.getAttribute("aria-label")).toBe(t("polish.favoriteRemove"));
    document.body.appendChild(favButton);
    favButton.click();
    expect(seen).toEqual(["users"]);
    favButton.remove();
    const plainButton = action.cellRenderer({ data: row("orders") });
    expect(plainButton.textContent).toBe("☆");
    expect(plainButton.getAttribute("aria-label")).toBe(t("polish.favoriteAdd"));
    document.body.appendChild(plainButton);
    plainButton.click();
    expect(seen).toEqual(["users", "orders"]);
    plainButton.remove();
    // data 缺省时不触发回调。
    expect(() => action.cellRenderer({ data: undefined })).not.toThrow();
    expect(seen).toEqual(["users", "orders"]);
  });
});
