/**
 * topic 收藏（Lane4 前端打磨）：TopicTree 星标 + 收藏置顶排序共享的模块级状态。
 *
 * 持久化调研结论（不落插件 store）：backend/internal/store 只有两条协议通道——
 * kafka/presets/*（消费/过滤预设专用，MessagesPanel/MonitorPanel 共享同一
 * presets.json 下拉列表，混入收藏会污染预设语义）与 kafka/connections/statuses
 * （连接态只读摘要）——都不适合低成本复用；按任务约定不新增后端协议方法。
 * 因此收藏仅走前端 best-effort 持久化（pluginStore：宿主 storage → localStorage
 * 降级，与侧栏宽度/时区开关同款模式，无桥且存储禁用时静默降级为会话内存态，
 * 重启丢失）；服务端不持久化。
 */
import { ref } from "vue";
import { TOPIC_FAVORITES_STORAGE_KEY, pluginStore } from "./pluginStore";

export { TOPIC_FAVORITES_STORAGE_KEY };

function loadNames(): string[] {
  try {
    const raw = JSON.parse(pluginStore.getItem(TOPIC_FAVORITES_STORAGE_KEY) ?? "");
    if (Array.isArray(raw)) return raw.filter((row): row is string => typeof row === "string");
  } catch {
    /* 无记忆/损坏 → 空集 */
  }
  return [];
}

const favorites = ref<ReadonlySet<string>>(new Set(loadNames()));

/** 当前收藏集（只读视图；组件 computed 内读取即获得响应式）。 */
export function topicFavorites(): ReadonlySet<string> {
  return favorites.value;
}

export function isFavoriteTopic(name: string): boolean {
  return favorites.value.has(name);
}

function persist(names: ReadonlySet<string>) {
  try {
    pluginStore.setItem(TOPIC_FAVORITES_STORAGE_KEY, JSON.stringify([...names]));
  } catch {
    /* 存储不可用（隐私模式等）：仅内存态 */
  }
}

/** 切换收藏；返回切换后是否处于收藏态（提示文案用）。 */
export function toggleTopicFavorite(name: string): boolean {
  const next = new Set(favorites.value);
  if (next.has(name)) next.delete(name);
  else next.add(name);
  favorites.value = next;
  persist(next);
  return next.has(name);
}

/** 整集替换（恢复/测试/未来设置入口用；同样 best-effort 落盘）。 */
export function setTopicFavorites(names: Iterable<string>): void {
  const next = new Set(names);
  favorites.value = next;
  persist(next);
}
