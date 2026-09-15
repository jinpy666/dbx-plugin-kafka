<script setup lang="ts">
// 左栏 topic 树：业务 topic 按评分排序、internal（_ 前缀 / 后端标记）沉底，
// 过滤框本地过滤，选中态由父级持有（selectedTopic 单一来源）。
// Lane4 打磨：internal 显隐开关（默认显示；隐藏仅过滤展示，不动数据请求）+
// topic 收藏星标（置顶排序，收藏态见 lib/topicFavorites：仅前端 localStorage
// best-effort，不落插件 store）。侧栏收空间：右缘 resizer 拖拽调宽
// （180–480px，localStorage 记忆，双击重置）、折叠成 40px 竖条（折叠时不渲染
// 树内容），过滤框 / 快捷键聚焦 + 匹配/总数徽章。
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { ChevronsLeft, ChevronsRight, Eye, EyeOff, HardDrive, RefreshCw, Search, Star, X } from "@lucide/vue";
import type { KafkaTopic } from "../lib/api";
import { filterTopics, sortTopicsPinned } from "../lib/kafkaModel";
import { isFavoriteTopic, toggleTopicFavorite, topicFavorites } from "../lib/topicFavorites";
import { friendlyKafkaError } from "../lib/kafkaErrors";
import { t } from "../lib/i18n";

const props = defineProps<{
  topics: KafkaTopic[];
  loading: boolean;
  error: string;
  selectedTopic: string;
}>();

const emit = defineEmits<{
  (e: "refresh"): void;
  (e: "select", topic: string): void;
}>();

// -- 侧栏宽度 / 折叠（localStorage 记忆，宿主 webview 禁存储时静默降级）---------

const TREE_WIDTH_KEY = "dbx.kafka.ui.treeWidth";
const TREE_COLLAPSED_KEY = "dbx.kafka.ui.treeCollapsed";
const TREE_WIDTH_DEFAULT = 240;
const TREE_WIDTH_MIN = 180;
const TREE_WIDTH_MAX = 480;

function readStoredWidth(): number {
  try {
    const parsed = Number.parseInt(localStorage.getItem(TREE_WIDTH_KEY) ?? "", 10);
    return Number.isFinite(parsed) ? Math.min(TREE_WIDTH_MAX, Math.max(TREE_WIDTH_MIN, parsed)) : TREE_WIDTH_DEFAULT;
  } catch {
    return TREE_WIDTH_DEFAULT;
  }
}

function persist(key: string, value: string) {
  try {
    localStorage.setItem(key, value);
  } catch {
    /* 存储不可用（隐私模式等）：仅内存态 */
  }
}

const width = ref(readStoredWidth());
const collapsed = ref((() => {
  try {
    return localStorage.getItem(TREE_COLLAPSED_KEY) === "1";
  } catch {
    return false;
  }
})());

function clampWidth(value: number): number {
  return Math.min(TREE_WIDTH_MAX, Math.max(TREE_WIDTH_MIN, Math.round(value)));
}

function setCollapsed(next: boolean) {
  collapsed.value = next;
  persist(TREE_COLLAPSED_KEY, next ? "1" : "0");
}

// resizer：pointer capture 拖拽，双击重置默认宽。
const resizing = ref(false);
let dragStartX = 0;
let dragStartWidth = 0;

function onResizeStart(event: PointerEvent) {
  event.preventDefault();
  dragStartX = event.clientX;
  dragStartWidth = width.value;
  resizing.value = true;
  (event.currentTarget as HTMLElement).setPointerCapture(event.pointerId);
}

function onResizeMove(event: PointerEvent) {
  if (!resizing.value) return;
  width.value = clampWidth(dragStartWidth + event.clientX - dragStartX);
}

function onResizeEnd(event: PointerEvent) {
  if (!resizing.value) return;
  resizing.value = false;
  (event.currentTarget as HTMLElement).releasePointerCapture(event.pointerId);
  persist(TREE_WIDTH_KEY, String(width.value));
}

function onResizeReset() {
  width.value = TREE_WIDTH_DEFAULT;
  persist(TREE_WIDTH_KEY, String(TREE_WIDTH_DEFAULT));
}

// -- 树内容 ---------------------------------------------------------------------

const keyword = ref("");
const filterInput = ref<HTMLInputElement | null>(null);

// Lane4：internal 显隐开关（默认显示，localStorage 记忆与侧栏宽度同款降级）。
// 隐藏只过滤展示层（visible computed），不动 topics 数据请求与总数徽章。
const SHOW_INTERNAL_KEY = "dbx.kafka.ui.showInternal";

function readStoredShowInternal(): boolean {
  try {
    return localStorage.getItem(SHOW_INTERNAL_KEY) !== "0";
  } catch {
    return true;
  }
}

const showInternal = ref(readStoredShowInternal());

function toggleInternal() {
  showInternal.value = !showInternal.value;
  persist(SHOW_INTERNAL_KEY, showInternal.value ? "1" : "0");
}

// 收藏置顶（排序见 kafkaModel.sortTopicsPinned；收藏态为模块级共享状态）。
const favorites = computed(() => topicFavorites());

function isFav(name: string): boolean {
  return isFavoriteTopic(name);
}

function toggleStar(name: string) {
  toggleTopicFavorite(name);
}

const visible = computed(() =>
  filterTopics(sortTopicsPinned(props.topics, favorites.value), keyword.value).filter((topic) => showInternal.value || !isInternal(topic)),
);

// P2-18：树错误区与 App 错误横幅同源——friendlyKafkaError 友好化正文，
// 未覆盖/与原文不同时把原始串留在 title 悬停里供排查。
const friendlyError = computed(() => (props.error ? friendlyKafkaError(props.error) : ""));
const errorDetail = computed(() => (props.error && friendlyError.value !== props.error ? props.error : ""));

function isInternal(topic: KafkaTopic): boolean {
  return topic.isInternal === true || topic.name.startsWith("_");
}

// F6-5：topic 级健康徽标（isHealthy===false 红点 + title「N 个分区不健康」）。
// 字段由 topics/list 直接下发（12.2.4），无额外请求；旧 sidecar 缺省 = 视为健康。
function unhealthyCount(topic: KafkaTopic): number {
  return topic.isHealthy === false ? Math.max(1, Number(topic.unhealthyPartitions ?? 0) || 0) : 0;
}

function clearFilter() {
  keyword.value = "";
}

function onFilterKeydown(event: KeyboardEvent) {
  // Escape 清空并还原列表，避免「过滤后找不到原项」的死角。
  if (event.key === "Escape" && keyword.value) {
    event.stopPropagation();
    clearFilter();
    return;
  }
  // P2-22：Enter 选中首个（或唯一）匹配项——过滤后逐 Tab 穿树在 big 模式下
  // 有数百个 tab stop，Enter 直达首个匹配是键盘主路径。
  if (event.key === "Enter" && visible.value.length > 0) {
    event.preventDefault();
    emit("select", visible.value[0]!.name);
  }
}

// 全局 "/" 聚焦过滤框（输入控件内不劫持）。
function onGlobalKeydown(event: KeyboardEvent) {
  if (event.key !== "/" || event.ctrlKey || event.metaKey || event.altKey) return;
  const target = event.target as HTMLElement | null;
  if (target && (target.tagName === "INPUT" || target.tagName === "TEXTAREA" || target.tagName === "SELECT" || target.isContentEditable)) return;
  if (collapsed.value) setCollapsed(false);
  event.preventDefault();
  // 折叠态先展开再聚焦（v-if 重建输入框，等一帧）。
  requestAnimationFrame(() => filterInput.value?.focus());
}

// -- 键盘导航（roving tabindex / listbox，round3 收口 P2-22）---------------------
// 树收敛为单 tab stop：仅 active 行 tabindex=0，其余行 -1（big 模式数百行不再
// 逐行停靠）；方向键 / Home / End 移动 active 并 emit select（选中态单一来源
// 仍在父级，经 props.selectedTopic 回流同步）；鼠标点击路径与视觉不变。

const activeKey = ref("");
const rowsEl = ref<HTMLElement | null>(null);

// active 取首选名（仍可见时），否则回落首行——覆盖初始、过滤、刷新三类场景。
function syncActive(preferred: string) {
  activeKey.value = visible.value.some((topic) => topic.name === preferred)
    ? preferred
    : visible.value[0]?.name ?? "";
}

syncActive(props.selectedTopic);

watch(
  () => props.selectedTopic,
  (selected) => syncActive(selected),
);

// 过滤 / 刷新后 active 失联回落首行；仍可见则保持原地。
watch(visible, () => {
  if (!visible.value.some((topic) => topic.name === activeKey.value)) {
    activeKey.value = visible.value[0]?.name ?? "";
  }
});

function focusActiveRow() {
  const index = visible.value.findIndex((topic) => topic.name === activeKey.value);
  const row = rowsEl.value?.querySelectorAll<HTMLButtonElement>(".tree-row")[index];
  row?.focus();
  row?.scrollIntoView?.({ block: "nearest" });
}

function moveActive(step: number | "first" | "last") {
  const names = visible.value.map((topic) => topic.name);
  if (names.length === 0) return;
  const current = names.indexOf(activeKey.value);
  const next = step === "first"
    ? names[0]!
    : step === "last"
      ? names[names.length - 1]!
      : names[Math.min(names.length - 1, Math.max(0, (current < 0 ? 0 : current) + step))]!;
  if (next === activeKey.value) return; // 边界不再移动，避免重复 emit
  activeKey.value = next;
  emit("select", next);
  void nextTick(focusActiveRow);
}

function onRowKeydown(event: KeyboardEvent) {
  if (event.key === "ArrowDown") {
    event.preventDefault();
    moveActive(1);
  } else if (event.key === "ArrowUp") {
    event.preventDefault();
    moveActive(-1);
  } else if (event.key === "Home") {
    event.preventDefault();
    moveActive("first");
  } else if (event.key === "End") {
    event.preventDefault();
    moveActive("last");
  }
}

onMounted(() => window.addEventListener("keydown", onGlobalKeydown));
onBeforeUnmount(() => window.removeEventListener("keydown", onGlobalKeydown));
</script>

<template>
  <aside
    class="tree-pane"
    :class="{ 'is-collapsed': collapsed, 'is-resizing': resizing }"
    :style="{ '--tree-pane-width': `${width}px` }"
  >
    <div v-if="collapsed" class="tree-rail">
      <button class="tree-rail-toggle" :title="t('messages.uiSidebarExpand')" :aria-label="t('messages.uiSidebarExpand')" @click="setCollapsed(false)">
        <ChevronsRight aria-hidden="true" />
      </button>
      <span class="tree-rail-label">{{ t("tree.title") }}</span>
    </div>
    <template v-else>
      <div class="panel-header">
        <span class="panel-title">
          <HardDrive aria-hidden="true" /> {{ t("tree.title") }}
          <span class="badge tree-count">{{ t("messages.uiTreeFilterCount", { matched: visible.length, total: props.topics.length }) }}</span>
        </span>
        <span class="actions">
          <!-- Lane4：internal 显隐开关（Eye=显示中 / EyeOff=已隐藏；aria-pressed=隐藏态） -->
          <button
            class="icon-button"
            :title="t('polish.internalToggle')"
            :aria-label="t('polish.internalToggle')"
            :aria-pressed="!showInternal"
            data-testid="internal-toggle"
            @click="toggleInternal"
          >
            <Eye v-if="showInternal" />
            <EyeOff v-else />
          </button>
          <button class="icon-button" :disabled="loading" :title="t('refresh')" @click="emit('refresh')">
            <RefreshCw :class="{ spinning: loading }" />
          </button>
          <button class="icon-button" :title="t('messages.uiSidebarCollapse')" :aria-label="t('messages.uiSidebarCollapse')" @click="setCollapsed(true)">
            <ChevronsLeft aria-hidden="true" />
          </button>
        </span>
      </div>
      <div class="tree-filter">
        <Search aria-hidden="true" class="icon-neutral icon-13" />
        <input
          ref="filterInput"
          v-model="keyword"
          :placeholder="t('tree.filterPlaceholder')"
          :title="t('tree.filterFocusHint')"
          type="text"
          spellcheck="false"
          @keydown="onFilterKeydown"
        />
        <!-- P2-22：清除钮移出 Tab 序（tabindex="-1"）——过滤激活时 Tab 从过滤框
             直达树行，不会先误停在清除钮上；键盘清空走既有 Esc 路径。 -->
        <button v-if="keyword" class="icon-button" tabindex="-1" :title="t('close')" :aria-label="t('close')" @click="clearFilter"><X /></button>
      </div>
      <div v-if="error" class="tree-error" :title="errorDetail">{{ friendlyError }}</div>
      <div v-else-if="loading && topics.length === 0" class="tree-state">{{ t("tree.loading") }}</div>
      <div v-else-if="visible.length === 0" class="tree-state">
        {{ keyword ? t("tree.noMatch", { keyword }) : t("tree.empty") }}
      </div>
      <div v-else class="tree-rows">
        <!-- round3：listbox 语义 + roving tabindex——容器单 tab stop，方向键导航 -->
        <div ref="rowsEl" class="tree-node" role="listbox" :aria-label="t('tree.title')">
          <button
            v-for="topic in visible"
            :key="topic.name"
            type="button"
            role="option"
            class="tree-row"
            :class="{ selected: topic.name === selectedTopic }"
            :aria-selected="topic.name === selectedTopic"
            :tabindex="topic.name === activeKey ? 0 : -1"
            :title="topic.error ? `${topic.name}: ${topic.error}` : topic.name"
            @click="emit('select', topic.name)"
            @keydown="onRowKeydown"
          >
            <span class="tree-label">
              <HardDrive aria-hidden="true" class="icon-13" :class="isInternal(topic) ? 'icon-neutral' : 'icon-violet'" />
              <span class="tree-name mono">{{ topic.name }}</span>
            </span>
            <!-- Lane4：收藏星标（span role=button + tabindex=-1：树保持单 tab stop
                 键盘模型；@click.stop 防止触发行选中；实心=已收藏） -->
            <span
              class="tree-star"
              :class="{ 'is-fav': isFav(topic.name) }"
              role="button"
              tabindex="-1"
              :aria-label="t(isFav(topic.name) ? 'polish.favoriteRemove' : 'polish.favoriteAdd')"
              :title="t(isFav(topic.name) ? 'polish.favoriteRemove' : 'polish.favoriteAdd')"
              :data-testid="`star-${topic.name}`"
              @click.stop="toggleStar(topic.name)"
            >
              <Star aria-hidden="true" />
            </span>
            <!-- F6-5：不健康红点（title = N 个分区不健康），无额外请求 -->
            <span
              v-if="unhealthyCount(topic) > 0"
              class="tree-health-dot"
              :title="t('tree.unhealthyTitle', { count: unhealthyCount(topic) })"
              :aria-label="t('tree.unhealthyTitle', { count: unhealthyCount(topic) })"
            />
            <span v-if="isInternal(topic)" class="badge badge-internal">{{ t("tree.internal") }}</span>
            <span v-else class="tree-badge">{{ t("tree.partitions", { count: topic.partitionCount }) }}</span>
          </button>
        </div>
      </div>
      <div
        class="tree-resizer"
        :class="{ active: resizing }"
        role="separator"
        aria-orientation="vertical"
        @pointerdown="onResizeStart"
        @pointermove="onResizeMove"
        @pointerup="onResizeEnd"
        @pointercancel="onResizeEnd"
        @dblclick="onResizeReset"
      />
    </template>
  </aside>
</template>
