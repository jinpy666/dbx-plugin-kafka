// @vitest-environment happy-dom
// useConsumeForm 组合式函数测试（测试覆盖续轮）：参数构建全分支（互斥短路、
// 过滤通道、时间/offset 范围、schema 挂载）、摘要 chips、开合记忆、时间双
// 模式、fieldFilters 行校验与预设四操作（load/save/apply/remove）。
import { beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises } from "@vue/test-utils";
import { nextTick, ref } from "vue";
import { useConsumeForm, optionalNumber, positiveInt, type UseConsumeFormOptions } from "./useConsumeForm";
import { MSG_FILTERS_KEY, MSG_FORM_OPEN_KEY, pluginStore } from "../lib/pluginStore";
import { setKafkaConnectionId } from "../lib/api";
import { t } from "../lib/i18n";

const invokeMock = vi.fn();

function installBridge(routes: Record<string, unknown>) {
  invokeMock.mockReset();
  invokeMock.mockImplementation(async (method: string) => {
    if (method in routes) {
      const result = routes[method];
      const envelope = result as { error?: { message?: string } } | null;
      if (envelope && typeof envelope === "object" && envelope.error) {
        throw new Error(envelope.error.message ?? "request failed");
      }
      return result;
    }
    throw new Error(`unhandled method: ${method}`);
  });
  (window as unknown as { dbxPlugin: unknown }).dbxPlugin = { invoke: invokeMock };
}

type FormOptions = Partial<UseConsumeFormOptions>;

function makeForm(overrides: FormOptions = {}) {
  return useConsumeForm({
    topic: () => "order-events",
    srProvider: overrides.srProvider ?? (() => undefined),
    notify: overrides.notify ?? vi.fn(),
    error: overrides.error ?? vi.fn(),
  });
}

beforeEach(() => {
  // 持久化后端是 pluginStore（宿主 storage 适配），清理须走同一实例
  //（happy-dom 下 store 在模块导入时已水合，之后改 localStorage 读不到）。
  pluginStore.removeItem(MSG_FORM_OPEN_KEY);
  pluginStore.removeItem(MSG_FILTERS_KEY);
  setKafkaConnectionId("conn-test");
});

describe("useConsumeForm 纯解析函数", () => {
  it("positiveInt：非正/非法回退 fallback", () => {
    expect(positiveInt("42", 7)).toBe(42);
    expect(positiveInt(" 42 ", 7)).toBe(42);
    expect(positiveInt("abc", 7)).toBe(7);
    expect(positiveInt("0", 7)).toBe(7);
    expect(positiveInt("-3", 7)).toBe(7);
    expect(positiveInt(undefined, 7)).toBe(7);
  });

  it("optionalNumber：空串/非法 → undefined（P1-6 String 归一）", () => {
    expect(optionalNumber("5")).toBe(5);
    expect(optionalNumber(5)).toBe(5);
    expect(optionalNumber("")).toBeUndefined();
    expect(optionalNumber("x")).toBeUndefined();
    expect(optionalNumber(null)).toBeUndefined();
  });
});

describe("buildParams", () => {
  it("默认参数：recent / limit 100 / timeout 5000 / maxScan 10000", () => {
    const form = makeForm();
    expect(form.buildParams()).toEqual({
      topic: "order-events",
      offsetStrategy: "recent",
      isolationLevel: "read_uncommitted",
      limit: 100,
      timeoutMs: 5000,
      maxScanRecords: 10000,
      decode: "none",
      decompression: "none",
    });
  });

  it("topicOverride 覆盖 options.topic", () => {
    const form = makeForm();
    expect(form.buildParams("other-topic").topic).toBe("other-topic");
  });

  it("groupId 与显式 partitions 双填时 partitions 优先（groupId 落选）", () => {
    const form = makeForm();
    form.groupId.value = "g1";
    form.partitionsText.value = "0,1";
    const params = form.buildParams();
    expect(params.groupId).toBeUndefined();
    expect(params.partitions).toEqual([0, 1]);
  });

  it("仅 groupId → groupId；仅 partitions → partitions", () => {
    const withGroup = makeForm();
    withGroup.groupId.value = " g1 ";
    expect(withGroup.buildParams().groupId).toBe("g1");

    const withPartitions = makeForm();
    withPartitions.partitionsText.value = "0, 2";
    expect(withPartitions.buildParams().partitions).toEqual([0, 2]);
  });

  it("timestamp 策略：offsetTime 走 offsetTimeToParam（datetime-local → ISO）", () => {
    const form = makeForm();
    form.offsetStrategy.value = "timestamp";
    form.offsetTimeText.value = "2026-01-02T03:04";
    expect(form.buildParams().offsetTime).toBe(new Date("2026-01-02T03:04:00").toISOString());
  });

  it("offset 策略：partitionOffsets 文本解析，非法条目丢弃", () => {
    const form = makeForm();
    form.offsetStrategy.value = "offset";
    form.partitionOffsetsText.value = "0=5,1=abc";
    expect(form.buildParams().partitionOffsets).toEqual({ 0: 5 });
  });

  it("commit + groupId：互斥短路，过滤通道一律不上送", () => {
    const form = makeForm();
    form.groupId.value = "g1";
    form.commit.value = true;
    form.filterText.value = "kw";
    const params = form.buildParams();
    expect(params.commit).toBe(true);
    expect(params.filter).toBeUndefined();
  });

  it("过滤通道 + matchMode + fieldFilters（仅启用且有值行上送，path 修剪）", () => {
    const form = makeForm();
    form.filterText.value = " kw ";
    form.keyFilterText.value = "kk";
    form.matchMode.value = "regex";
    form.addFieldFilter();
    form.fieldFilters.value[0].path = " user.id ";
    form.fieldFilters.value[0].value = "42";
    form.addFieldFilter(); // 未填值 → 不上送
    form.addFieldFilter();
    form.fieldFilters.value[2].operator = "gt";
    form.fieldFilters.value[2].value = "abc";
    const params = form.buildParams();
    expect(params.filter).toBe("kw");
    expect(params.keyFilter).toBe("kk");
    expect(params.matchMode).toBe("regex");
    // 行过滤只看「启用 + 有值」：gt + 非法数值行仍上送（issue 仅作 UI 标红）。
    expect(params.fieldFilters).toEqual([
      { source: "value", operator: "contains", value: "42", path: "user.id" },
      { source: "value", operator: "gt", value: "abc" },
    ]);
  });

  it("时间范围与 offset 范围：有值才带，offsetFrom 空缺省", () => {
    const form = makeForm();
    form.timestampFrom.value = "2026-01-02T03:04:05";
    form.offsetTo.value = "10";
    const params = form.buildParams();
    expect(params.timestampFrom).toBe(new Date("2026-01-02T03:04:05").getTime());
    expect(params.offsetTo).toBe(10);
    expect(params.offsetFrom).toBeUndefined();
  });

  it("schema 挂载：启用 + subject + 合法 version 才进 params", async () => {
    installBridge({
      "kafka/schema/subjects/list": { subjects: [{ subject: "order-value", formats: ["json", "avro"], latestVersion: 3 }] },
    });
    const form = makeForm();
    form.schemaEnabled.value = true;
    await flushPromises();
    form.schemaSubject.value = "order-value";
    await flushPromises();
    form.schemaVersionText.value = "2";
    expect(form.buildParams().schema).toEqual({ subject: "order-value", version: 2, format: "json" });

    form.schemaVersionText.value = "x";
    expect(form.buildParams().schema).toEqual({ subject: "order-value", format: "json" });
  });
});

describe("摘要条与 chips", () => {
  it("formOpen 记忆：读旧值、toggle 回写", async () => {
    pluginStore.setItem(MSG_FORM_OPEN_KEY, "1");
    const form = makeForm();
    expect(form.formOpen.value).toBe(true);
    form.toggleFormOpen();
    await nextTick();
    expect(form.formOpen.value).toBe(false);
    expect(pluginStore.getItem(MSG_FORM_OPEN_KEY)).toBe("0");
  });

  it("开合组记忆：损坏 JSON 回默认；可折叠组落盘；已存值恢复", async () => {
    pluginStore.setItem(MSG_FILTERS_KEY, "{bad");
    const fallback = makeForm();
    expect(fallback.openGroups.value.filter).toBe(true);
    expect(fallback.openGroups.value.decode).toBe(false);

    fallback.toggleGroup("filter");
    fallback.toggleGroup("timeRange");
    await nextTick();
    expect(JSON.parse(pluginStore.getItem(MSG_FILTERS_KEY) ?? "{}")).toEqual({
      timeRange: false,
      filter: false,
      decode: false,
    });

    pluginStore.setItem(MSG_FILTERS_KEY, JSON.stringify({ filter: false }));
    const restored = makeForm();
    expect(restored.openGroups.value.filter).toBe(false);
  });

  it("chips：策略/decode/filterCount（启用且有值的 fieldFilters 才计数）", () => {
    const form = makeForm();
    expect(form.strategyLabel.value).toBe(t("messages.strategyRecent"));
    expect(form.decodeLabel.value).toBe(t("messages.formatRaw"));
    form.decompression.value = "gzip";
    expect(form.decodeLabel.value).toBe("none · gzip");

    form.filterText.value = "a";
    expect(form.filterCount.value).toBe(1);
    form.addFieldFilter();
    form.fieldFilters.value[0].value = "42";
    expect(form.filterCount.value).toBe(2);
    form.fieldFilters.value[0].enabled = false;
    expect(form.filterCount.value).toBe(1);
  });

  it("互斥 computed：commit 禁过滤、partitions 禁 groupId", () => {
    const form = makeForm();
    expect(form.filtersDisabled.value).toBe(false);
    expect(form.partitionsDisabled.value).toBe(false);
    form.commit.value = true;
    expect(form.filtersDisabled.value).toBe(true);
    expect(form.partitionsDisabled.value).toBe(true);

    const grouped = makeForm();
    grouped.partitionsText.value = "0";
    expect(grouped.groupDisabled.value).toBe(true);
  });
});

describe("时间双模式", () => {
  it("computed 校验态：非法时间标红、倒序范围告警", () => {
    const form = makeForm();
    form.timestampFrom.value = "2026-01-02T03:04";
    form.timestampTo.value = "not-a-time";
    expect(form.tsFromMs.value).toBe(new Date("2026-01-02T03:04:00").getTime());
    expect(form.tsToInvalid.value).toBe(true);
    form.timestampTo.value = "2026-01-01T00:00";
    expect(form.tsRangeReversed.value).toBe(true);
  });

  it("toggleTsMode：datetime→unix 换算，解析不了的原样保留", () => {
    const form = makeForm();
    form.timestampFrom.value = "2026-01-02T03:04";
    form.timestampTo.value = "draft";
    form.toggleTsMode();
    expect(form.tsMode.value).toBe("unix");
    expect(form.timestampFrom.value).toBe(String(new Date("2026-01-02T03:04:00").getTime()));
    expect(form.timestampTo.value).toBe("draft");
    form.toggleTsMode();
    expect(form.tsMode.value).toBe("datetime");
  });

  it("setNow：datetime 模式填本地串，unix 模式填毫秒数字串", () => {
    const form = makeForm();
    form.setNow("from");
    expect(form.timestampFrom.value).not.toBe("");
    form.toggleTsMode();
    form.setNow("to");
    expect(/^\d{13}$/.test(form.timestampTo.value)).toBe(true);
  });
});

describe("fieldFilters 行编辑", () => {
  it("issue 文案、增删行", () => {
    const form = makeForm();
    form.addFieldFilter();
    form.addFieldFilter();
    form.fieldFilters.value[0].operator = "gt";
    form.fieldFilters.value[0].value = "abc";
    expect(form.fieldFilterIssueKey(0)).toBe("messages.uiFilterValueRequired");
    expect(form.fieldFilterIssueText(0)).toBe(t("messages.uiFilterValueRequired"));
    expect(form.fieldFilterIssueKey(1)).toBeNull();
    expect(form.fieldFilterIssueText(1)).toBe("");

    form.removeFieldFilter(0);
    expect(form.fieldFilters.value).toHaveLength(1);
    expect(form.fieldFilterIssueKey(9)).toBeNull();
  });
});

describe("schema 挂载联动", () => {
  it("glue provider：挂载区禁用、开关被 watch 复位、attach 恒 undefined", async () => {
    // computed 依赖必须响应式：provider 用 ref 承载，闭包变量驱动不了重算。
    const provider = ref<string | undefined>("confluent");
    const form = makeForm({ srProvider: () => provider.value });
    form.schemaEnabled.value = true;
    await nextTick();
    provider.value = "glue";
    await nextTick();
    expect(form.glueSchemaDisabled.value).toBe(true);
    expect(form.schemaEnabled.value).toBe(false);
    expect(form.buildParams().schema).toBeUndefined();
  });

  it("启用即拉 subjects；版本倒序；选 subject 重置 version 并取首个 format", async () => {
    installBridge({
      "kafka/schema/subjects/list": { subjects: [{ subject: "order-value", formats: ["json", "avro"], latestVersion: 3 }] },
    });
    const form = makeForm();
    expect(form.schemaVersions.value).toEqual([]);
    form.schemaEnabled.value = true;
    await flushPromises();
    expect(invokeMock.mock.calls.some(([method]) => method === "kafka/schema/subjects/list")).toBe(true);

    form.schemaSubject.value = "order-value";
    await flushPromises();
    expect(form.schemaVersionText.value).toBe("");
    expect(form.schemaFormat.value).toBe("json");
    expect(form.schemaVersions.value).toEqual([3, 2, 1]);
  });

  it("SR 失败 → 空列表不阻断；无 subjects 时不重复拉取", async () => {
    installBridge({ "kafka/schema/subjects/list": { error: { message: "sr down" } } });
    const form = makeForm();
    form.schemaEnabled.value = true;
    await flushPromises();
    expect(form.schemaSubjects.value).toEqual([]);

    form.schemaSubject.value = "any";
    form.schemaVersionText.value = "3";
    await flushPromises();
    expect(form.buildParams().schema).toEqual({ subject: "any", format: "avro" });
  });
});

describe("预设", () => {
  it("loadPresets 过滤 monitor 型预设", async () => {
    installBridge({
      "kafka/presets/list": {
        presets: [
          { id: "p1", name: "hourly", params: { topic: "t" } },
          { id: "m1", name: "lag-watch", params: { type: "monitor" } },
        ],
      },
    });
    const form = makeForm();
    await form.loadPresets();
    expect(form.presets.value).toEqual([{ id: "p1", name: "hourly" }]);
  });

  it("savePreset：空名不发请求；成功修剪名、带表单参数并通知刷新", async () => {
    const routes: Record<string, unknown> = { "kafka/presets/list": { presets: [] }, "kafka/presets/save": { success: true } };
    installBridge(routes);
    const notify = vi.fn();
    const form = makeForm({ notify });
    form.filterText.value = "kw";
    await form.savePreset();
    expect(invokeMock).not.toHaveBeenCalled();

    form.presetName.value = " my ";
    await form.savePreset();
    const save = invokeMock.mock.calls.find(([method]) => method === "kafka/presets/save");
    expect(save).toBeTruthy();
    expect((save?.[1] as { preset: { name: string; params: { filter?: string; topic: string } } }).preset).toMatchObject({
      name: "my",
      params: { filter: "kw", topic: "" },
    });
    expect(notify).toHaveBeenCalledWith(t("messages.presetSaved"));
    expect(form.presetName.value).toBe("");
    expect(form.presets.value).toEqual([]);
  });

  it("savePreset 失败走 error 回调", async () => {
    installBridge({
      "kafka/presets/list": { presets: [] },
      "kafka/presets/save": { error: { message: "denied" } },
    });
    const error = vi.fn();
    const form = makeForm({ error });
    form.presetName.value = "x";
    await form.savePreset();
    expect(error).toHaveBeenCalledWith("denied");
  });

  it("applyPreset 恢复全字段；未知 id 不动表单", async () => {
    installBridge({
      "kafka/presets/list": {
        presets: [
          {
            id: "p1",
            name: "x",
            params: {
              groupId: "g1",
              offsetStrategy: "earliest",
              partitions: [0, 3],
              partitionOffsets: { 0: 7 },
              limit: 250,
              timeoutMs: 9000,
              maxScanRecords: 20000,
              isolationLevel: "read_committed",
              commit: true,
              filter: "f",
              keyFilter: "k",
              valueFilter: "v",
              headerFilter: "h",
              matchMode: "prefix",
              decode: "base64",
              decompression: "zstd",
              fieldFilters: [{ source: "key", path: "p", operator: "gt", value: "1" }],
              schema: { subject: "s1", version: 2, format: "protobuf" },
            },
          },
          { id: "p2", name: "y", params: {} },
        ],
      },
    });
    const notify = vi.fn();
    const form = makeForm({ notify });
    await form.applyPreset("p1");

    expect(form.groupId.value).toBe("g1");
    expect(form.offsetStrategy.value).toBe("earliest");
    expect(form.partitionsText.value).toBe("0,3");
    expect(form.partitionOffsetsText.value).toBe("0=7");
    expect(form.limit.value).toBe("250");
    expect(form.timeoutMs.value).toBe("9000");
    expect(form.maxScanRecords.value).toBe("20000");
    expect(form.isolationLevel.value).toBe("read_committed");
    expect(form.commit.value).toBe(true);
    expect(form.filterText.value).toBe("f");
    expect(form.keyFilterText.value).toBe("k");
    expect(form.valueFilterText.value).toBe("v");
    expect(form.headerFilterText.value).toBe("h");
    expect(form.matchMode.value).toBe("prefix");
    expect(form.decode.value).toBe("base64");
    expect(form.decompression.value).toBe("zstd");
    expect(form.fieldFilters.value).toEqual([{ source: "key", path: "p", operator: "gt", value: "1", enabled: true }]);
    expect(form.schemaEnabled.value).toBe(true);
    expect(form.schemaSubject.value).toBe("s1");
    // 行为锁定：applyPreset 先设 version 再设 subject，subject watch 会把
    // versionText 复位（subjects 未加载时预设的版本号被清，格式保留）。
    expect(form.schemaVersionText.value).toBe("");
    expect(form.schemaFormat.value).toBe("protobuf");
    expect(notify).toHaveBeenCalledWith(t("messages.presetApplied"));

    await form.applyPreset("nope");
    expect(form.groupId.value).toBe("g1");
  });

  it("applyPreset 桥失败走 error 回调", async () => {
    installBridge({ "kafka/presets/list": { error: { message: "boom" } } });
    const error = vi.fn();
    const form = makeForm({ error });
    await form.applyPreset("p1");
    expect(error).toHaveBeenCalledWith("boom");
  });

  it("removePreset：成功刷新并通知；失败走 error 回调", async () => {
    const routes: Record<string, unknown> = {
      "kafka/presets/list": { presets: [{ id: "p1", name: "n", params: {} }] },
      "kafka/presets/remove": { presets: [] },
    };
    installBridge(routes);
    const notify = vi.fn();
    const error = vi.fn();
    const form = makeForm({ notify, error });
    await form.loadPresets();
    expect(form.presets.value).toHaveLength(1);

    routes["kafka/presets/list"] = { presets: [] };
    await form.removePreset("p1");
    expect(notify).toHaveBeenCalledWith(t("messages.presetRemoved"));
    expect(form.presets.value).toEqual([]);

    routes["kafka/presets/remove"] = { error: { message: "nope" } };
    await form.removePreset("p1");
    expect(error).toHaveBeenCalledWith("nope");
  });
});
