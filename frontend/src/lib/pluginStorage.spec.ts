// 薄 spec：验证 kafka pluginStore 接线——键集合完整、node 环境默认通道为
// memory、import 解析成立（适配器实现与文档只在 shared/frontend 维护）。
import { describe, expect, it } from "vitest";
import {
  GRID_PAGE_SIZES_KEY,
  MSG_FILTERS_KEY,
  MSG_FORM_OPEN_KEY,
  PLUGIN_STORE_KEYS,
  SHOW_INTERNAL_KEY,
  TIMESTAMP_TZ_STORAGE_KEY,
  TOPIC_FAVORITES_STORAGE_KEY,
  TREE_COLLAPSED_KEY,
  TREE_WIDTH_KEY,
  pluginStore,
} from "./pluginStore";

describe("kafka pluginStore wiring", () => {
  it("declares every persisted UI key and resolves to memory in node tests", async () => {
    expect(new Set(PLUGIN_STORE_KEYS)).toEqual(
      new Set([
        "dbx.kafka.ui.msgFormOpen",
        "dbx.kafka.ui.msgFilters",
        "dbx.kafka.ui.treeWidth",
        "dbx.kafka.ui.treeCollapsed",
        "dbx.kafka.ui.showInternal",
        "dbx.kafka.ui.topicFavorites",
        "kafka.ts.tz",
        "dbx.kafka.ui.gridPageSizes",
      ]),
    );
    // 导出常量与键面一致（防拼写漂移）。
    expect(MSG_FORM_OPEN_KEY).toBe("dbx.kafka.ui.msgFormOpen");
    expect(MSG_FILTERS_KEY).toBe("dbx.kafka.ui.msgFilters");
    expect(TREE_WIDTH_KEY).toBe("dbx.kafka.ui.treeWidth");
    expect(TREE_COLLAPSED_KEY).toBe("dbx.kafka.ui.treeCollapsed");
    expect(SHOW_INTERNAL_KEY).toBe("dbx.kafka.ui.showInternal");
    expect(TOPIC_FAVORITES_STORAGE_KEY).toBe("dbx.kafka.ui.topicFavorites");
    expect(TIMESTAMP_TZ_STORAGE_KEY).toBe("kafka.ts.tz");
    expect(GRID_PAGE_SIZES_KEY).toBe("dbx.kafka.ui.gridPageSizes");
    // node 环境无 window：默认解析应落到内存档而不是抛错。
    expect(pluginStore.channel).toBe("memory");
    await pluginStore.ready;
  });

  it("keeps Web Storage semantics in the memory channel", () => {
    expect(pluginStore.getItem("dbx.kafka.ui.msgFormOpen")).toBeNull();
    pluginStore.setItem(MSG_FORM_OPEN_KEY, "1");
    expect(pluginStore.getItem(MSG_FORM_OPEN_KEY)).toBe("1");
    pluginStore.removeItem(MSG_FORM_OPEN_KEY);
    expect(pluginStore.getItem(MSG_FORM_OPEN_KEY)).toBeNull();
  });
});
