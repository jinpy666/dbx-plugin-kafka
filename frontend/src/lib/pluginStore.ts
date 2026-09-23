// 插件工作台 UI 持久化单点（shared/frontend/pluginStorage.ts 适配器接线）。
//
// 工作台 iframe 是 sandbox opaque origin，直接读 localStorage 抛 SecurityError，
// 原各调用点的 best-effort localStorage 在真机上全部静默失效；统一经 pluginStore
// （宿主桥 → guarded localStorage → 内存）读写，调用点保持 getItem/setItem/
// removeItem 的 Web Storage 同名语义。宿主 storage 无列键方法，本插件全部键在
// 此显式声明，水合阶段逐键拉入缓存；main.ts 挂载前 await pluginStore.ready，
// 保证组件 setup 内的同步首读命中持久化值（files 插件同款模式）。
//
// 分页页大小原为 dbx-kafka-grid-pagesize-<tableKey> 动态拼键，收敛为单一
// gridPageSizes 固定键 + JSON map（tableKey → size）；旧动态键不做迁移——
// 工作台 opaque origin 下 localStorage 本就从未写成功过，无存量可搬。

import { createPluginKvStore } from "../../../shared/frontend/pluginStorage";

/** 消费表单开合记忆（useConsumeForm 摘要条；"1"/"0"）。 */
export const MSG_FORM_OPEN_KEY = "dbx.kafka.ui.msgFormOpen";
/** 消费表单筛选区分组开合（JSON：timeRange/filter/decode 布尔）。 */
export const MSG_FILTERS_KEY = "dbx.kafka.ui.msgFilters";
/** 左栏 topic 树宽度（px 数字串）。 */
export const TREE_WIDTH_KEY = "dbx.kafka.ui.treeWidth";
/** 左栏 topic 树折叠态（"1"/"0"）。 */
export const TREE_COLLAPSED_KEY = "dbx.kafka.ui.treeCollapsed";
/** internal topic 显隐（"1"/"0"，缺省显示）。 */
export const SHOW_INTERNAL_KEY = "dbx.kafka.ui.showInternal";
/** topic 收藏集（JSON 字符串数组）。 */
export const TOPIC_FAVORITES_STORAGE_KEY = "dbx.kafka.ui.topicFavorites";
/** 时间戳时区偏好（"local"/"utc"；沿用历史键名不动）。 */
export const TIMESTAMP_TZ_STORAGE_KEY = "kafka.ts.tz";
/** 各表格分页页大小（JSON map：tableKey → size；取代旧动态拼键）。 */
export const GRID_PAGE_SIZES_KEY = "dbx.kafka.ui.gridPageSizes";

/** 本插件全部 UI 状态键（宿主 storage 无列键方法，水合需显式声明）。 */
export const PLUGIN_STORE_KEYS = [
  MSG_FORM_OPEN_KEY,
  MSG_FILTERS_KEY,
  TREE_WIDTH_KEY,
  TREE_COLLAPSED_KEY,
  SHOW_INTERNAL_KEY,
  TOPIC_FAVORITES_STORAGE_KEY,
  TIMESTAMP_TZ_STORAGE_KEY,
  GRID_PAGE_SIZES_KEY,
];

export const pluginStore = createPluginKvStore(PLUGIN_STORE_KEYS);
