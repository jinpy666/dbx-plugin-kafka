// 审计事件面板：最近 `kafka/audit` 事件的工作台可视化（照 ldap 改）。
// 数据面在 App.vue（handleEvent → lib/auditFeed 纯函数），本组件纯展示：
// - 头部常驻：事件总数 + denied/error 计数徽标（denied 高亮为 destructive）
// - 列表折叠可展开；denied/error 事件到达时自动展开一次保证可见
// - 清空按钮由父级处理（App.vue 持有列表状态）
<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { ChevronDown, ChevronRight, ScrollText, Trash2 } from "@lucide/vue";
import { formatAuditTime, type AuditFeedItem } from "../lib/auditFeed";
import { t } from "../lib/i18n";

const props = defineProps<{ items: AuditFeedItem[] }>();
const emit = defineEmits<{ (e: "clear"): void }>();

const expanded = ref(false);
const hasEvents = computed(() => props.items.length > 0);
// KAFKA-AF-L6：denied（策略拒绝）与 error（操作失败）分开计数——此前
// `result !== "ok"` 把 error 事件也算进「被拒绝」，语义误导。
const deniedCount = computed(() => props.items.filter((item) => item.result === "denied").length);
const errorCount = computed(() => props.items.filter((item) => item.result === "error").length);
const attentionCount = computed(() => deniedCount.value + errorCount.value);
// KAFKA-AF-L7：清空后收起头部（此前 expanded 残留 true 且 toggle 禁用，
// 空面板永久可见）。
watch(hasEvents, (events) => {
  if (!events) expanded.value = false;
});

// denied/error 到达时自动展开（ok 事件不打扰用户当前视图）。
watch(
  () => props.items[0],
  (item, previous) => {
    if (item && item !== previous && item.result !== "ok") expanded.value = true; // denied/error 自动展开（语义不变）
  },
);

const summary = computed(() => {
  if (!hasEvents.value) return t("audit.empty");
  const total = t("audit.events", { total: props.items.length });
  const parts = [total];
  if (deniedCount.value > 0) parts.push(t("audit.deniedCount", { denied: deniedCount.value }));
  if (errorCount.value > 0) parts.push(t("audit.errorCount", { error: errorCount.value }));
  return parts.join(" · ");
});

function resultLabel(result: AuditFeedItem["result"]): string {
  return t(`audit.result.${result}`);
}
</script>

<template>
  <section v-if="hasEvents || expanded" class="audit-feed" :class="{ 'audit-feed-denied': attentionCount > 0 }">
    <div class="audit-header">
      <button
        type="button"
        class="audit-toggle"
        :disabled="!hasEvents"
        :aria-expanded="expanded"
        :title="expanded ? t('audit.hide') : t('audit.show')"
        @click="expanded = !expanded"
      >
        <ChevronDown v-if="expanded" aria-hidden="true" />
        <ChevronRight v-else aria-hidden="true" />
        <ScrollText aria-hidden="true" />
        <span class="audit-title">{{ t("audit.title") }}</span>
        <span class="audit-summary" :class="{ 'audit-summary-denied': attentionCount > 0 }">{{ summary }}</span>
      </button>
      <button v-if="hasEvents" type="button" class="audit-clear" :title="t('audit.clear')" @click="emit('clear')">
        <Trash2 aria-hidden="true" />
      </button>
    </div>
    <ol v-if="expanded && hasEvents" class="audit-list">
      <li
        v-for="item in items"
        :key="item.id"
        class="audit-item"
        :class="`audit-item-${item.result}`"
        :title="item.detail || item.target"
      >
        <span class="audit-time">{{ formatAuditTime(item.at) }}</span>
        <span class="audit-badge" :class="`audit-badge-${item.result}`">{{ resultLabel(item.result) }}</span>
        <span class="audit-action">{{ item.action }}</span>
        <span class="audit-target">{{ item.target || "—" }}</span>
      </li>
    </ol>
  </section>
</template>
