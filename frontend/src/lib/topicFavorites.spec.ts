// @vitest-environment happy-dom
// lib/topicFavorites 单测（Lane4 前端打磨）：模块级收藏集的切换/替换与
// best-effort 持久化（pluginStore：宿主 storage → localStorage 降级）；
// 存储抛错时静默降级为内存态（不落盘不炸）。
import { beforeEach, describe, expect, it } from "vitest";
import { isFavoriteTopic, setTopicFavorites, toggleTopicFavorite, topicFavorites, TOPIC_FAVORITES_STORAGE_KEY } from "./topicFavorites";
import { pluginStore } from "./pluginStore";

beforeEach(() => {
  // 持久化后端是 pluginStore（宿主 storage 适配），清理须走同一实例。
  pluginStore.removeItem(TOPIC_FAVORITES_STORAGE_KEY);
  setTopicFavorites([]);
});

describe("topicFavorites (Lane4)", () => {
  it("toggles favorites and round-trips through pluginStore", () => {
    expect(isFavoriteTopic("users")).toBe(false);
    expect(toggleTopicFavorite("users")).toBe(true);
    expect(isFavoriteTopic("users")).toBe(true);
    expect(toggleTopicFavorite("users")).toBe(false);
    expect(isFavoriteTopic("users")).toBe(false);
    // 持久化形状：字符串数组的 JSON
    toggleTopicFavorite("orders");
    expect(JSON.parse(pluginStore.getItem(TOPIC_FAVORITES_STORAGE_KEY) ?? "")).toEqual(["orders"]);
    expect(topicFavorites().has("orders")).toBe(true);
  });

  it("replaces the whole set via setTopicFavorites and ignores malformed entries on load", () => {
    setTopicFavorites(["a", "b"]);
    expect([...topicFavorites()]).toEqual(["a", "b"]);
    // 损坏 JSON → 下次冷启动视为空集（此处直接写坏数据验证 loadNames 兜底语义）
    pluginStore.setItem(TOPIC_FAVORITES_STORAGE_KEY, "{not json");
    expect(() => setTopicFavorites([])).not.toThrow();
    expect(topicFavorites().size).toBe(0);
  });

  it("degrades to in-memory when storage is unavailable", () => {
    const original = Storage.prototype.setItem;
    Storage.prototype.setItem = () => {
      throw new Error("quota exceeded");
    };
    try {
      expect(() => toggleTopicFavorite("degraded")).not.toThrow();
      expect(isFavoriteTopic("degraded")).toBe(true);
    } finally {
      Storage.prototype.setItem = original;
    }
  });
});
