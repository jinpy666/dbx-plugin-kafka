<script setup lang="ts">
// Topic 管理面板：list（业务排序照 TopicTree 同源函数）/create/delete/
// 扩分区/config get+alter/offsets 查询。高危操作分级：
// - create/alter/扩分区：read_only 禁用
// - delete：read_only 或 allowDelete=false 禁用，且需输入与 topic 同名的
//   confirmTopic 确认文本（§6 门禁，后端同样校验）
import { computed, ref, watch } from "vue";
import { Plus, RefreshCw, Trash2, TrendingUp, Wrench } from "@lucide/vue";
import type { ColDef } from "ag-grid-community";
import DbxAgGrid, { type GridContextMenuItem } from "./DbxAgGrid.vue";
import { kafkaApi, type ConfigEntry, type KafkaTopic, type TopicOffsetRow, type TopicPartitionInfo } from "../lib/api";
import {
  MINIMAL_PARTITION_FIELDS,
  MINIMAL_TOPIC_FIELDS,
  MINIMAL_TOPIC_OFFSET_FIELDS,
  partitionColumns,
  toPartitionRows,
  toTopicOffsetRows,
  toTopicRows,
  topicColumns,
  topicOffsetColumns,
  type PartitionVm,
  type TopicOffsetVm,
  type TopicVm,
} from "../lib/kafkaColumns";
import { offsetTimeToParam } from "../lib/consumeForm";
import { parseHeadersJson } from "../lib/jsonText";
import { sortTopicsPinned } from "../lib/topics";
import { isFavoriteTopic, toggleTopicFavorite, topicFavorites } from "../lib/topicFavorites";
import { friendlyKafkaError } from "../lib/kafkaErrors";
import { useModalBehavior } from "../lib/modalBehavior";
import { t } from "../lib/i18n";

const props = defineProps<{
  topics: KafkaTopic[];
  loading: boolean;
  /** round4 面 1：与 TopicTree 同源的加载错误（App topicsError）——空态不再把
   *  「集群无 topic」与「列表加载失败」混为一谈。 */
  error?: string;
  canWrite: boolean;
  canDelete: boolean;
}>();

const emit = defineEmits<{
  (e: "error", message: string): void;
  (e: "notify", message: string): void;
  (e: "refresh"): void;
}>();

const selected = ref<KafkaTopic | null>(null);
const partitions = ref<TopicPartitionInfo[]>([]);
const offsetRows = ref<TopicOffsetRow[]>([]);
const configEntries = ref<ConfigEntry[]>([]);
const busy = ref(false);

// create dialog
const createOpen = ref(false);
const createName = ref("");
const createPartitions = ref("1");
const createReplication = ref("1");
const createConfigText = ref("{}");

// delete dialog
const deleteOpen = ref(false);
const deleteTarget = ref("");
const deleteConfirmText = ref("");

// expand dialog
const expandOpen = ref(false);
const expandCount = ref("");

// offsets form（Phase 2：策略全量 earliest/latest/max-timestamp/log-start/custom）
type OffsetTimeMode = "earliest" | "latest" | "max-timestamp" | "log-start" | "custom";
const offsetTimeMode = ref<OffsetTimeMode>("latest");
const offsetCustomTime = ref("");

// P2-3：与侧栏 TopicTree 同源排序（sortTopicsPinned：internal 沉底 + 业务评分
// + Lane4 收藏置顶），修复管理表默认顺序与侧栏树不一致。
// perf(topics)：只有行序 computed 读 topicFavorites()（模块级响应式收藏集）。
// 行带稳定 id（TopicVm.id = topic name），收藏切换走 DbxAgGrid 的 immutable
// 增量更新：行节点按 id 复用、数据变化的行重渲染单元格，星标列在 cellRenderer
// 内现读收藏态即时换星；大表下不再整表换行。列定义 computed 不读收藏集——
// 避免每次点星都重建 columnDefs（setGridOption 全列刷新）。
const topicGridRows = computed(() => toTopicRows(sortTopicsPinned(props.topics, topicFavorites())));
const topicGridCols = computed(() =>
  topicColumns({ onToggleFavorite: (row) => toggleTopicFavorite(row.name), isFavorite: (row) => isFavoriteTopic(row.name) }) as ColDef<TopicVm>[],
);
const partitionGridRows = computed(() => toPartitionRows(partitions.value));
const partitionGridCols = computed(() => partitionColumns() as ColDef<PartitionVm>[]);
const offsetGridRows = computed(() => toTopicOffsetRows(offsetRows.value));
const offsetGridCols = computed(() => topicOffsetColumns() as ColDef<TopicOffsetVm>[]);

// config editor
const configOpen = ref(false);
const configEdits = ref<Array<{ key: string; value: string; remove?: boolean }>>([]);

// 弹层行为统一接入（UI 扫描第 2 轮 P1-5）：创建/删除/扩分区/配置四弹窗支持
// Esc 关闭 + Tab 焦点陷阱 + 关闭归还触发元素（决策逻辑 lib/modalBehavior）。
const createModalEl = ref<HTMLElement | null>(null);
const deleteModalEl = ref<HTMLElement | null>(null);
const expandModalEl = ref<HTMLElement | null>(null);
const configModalEl = ref<HTMLElement | null>(null);
useModalBehavior({ open: createOpen, container: createModalEl, close: () => (createOpen.value = false) });
useModalBehavior({ open: deleteOpen, container: deleteModalEl, close: () => (deleteOpen.value = false) });
useModalBehavior({ open: expandOpen, container: expandModalEl, close: () => (expandOpen.value = false) });
useModalBehavior({ open: configOpen, container: configModalEl, close: () => (configOpen.value = false) });

const canManage = computed(() => props.canWrite);
const canDeleteTopic = computed(() => props.canWrite && props.canDelete);

// round4 面 1：错误正文走 friendlyKafkaError（与 TopicTree 同源），原文留 title。
const friendlyError = computed(() => (props.error ? friendlyKafkaError(props.error) : ""));
const errorDetail = computed(() => (props.error && friendlyError.value !== props.error ? props.error : ""));

function topicContextMenuItems(row: unknown): GridContextMenuItem[] {
  const topic = (row as TopicVm | undefined)?.raw;
  if (!topic) return [];
  return [
    { id: "topic-describe", label: t("topics.describe"), action: () => { selected.value = topic; void describeSelected(); } },
    { id: "topic-offsets", label: t("topics.offsets"), action: () => { selected.value = topic; void queryOffsets(); } },
    { id: "topic-config", label: t("topics.configGet"), action: () => void openConfig(topic) },
    { id: "topic-expand", label: t("topics.expand"), action: () => openExpand(topic), disabled: !canManage.value },
    { id: "topic-delete", label: t("topics.delete"), action: () => askDelete(topic), disabled: !canDeleteTopic.value, danger: true, separatorBefore: true },
  ];
}

function selectTopic(topic: KafkaTopic | null) {
  selected.value = topic;
  partitions.value = [];
  offsetRows.value = [];
  configEntries.value = [];
}

async function describeSelected() {
  if (!selected.value) return;
  busy.value = true;
  emit("error", "");
  try {
    const response = await kafkaApi.topicsDescribe(selected.value.name);
    partitions.value = response.partitions ?? [];
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  } finally {
    busy.value = false;
  }
}

function openCreate() {
  createName.value = "";
  createPartitions.value = "1";
  createReplication.value = "1";
  createConfigText.value = "{}";
  createOpen.value = true;
}

async function submitCreate() {
  const name = createName.value.trim();
  const partitionsCount = Number.parseInt(createPartitions.value, 10);
  const replication = Number.parseInt(createReplication.value, 10);
  if (!name || !Number.isInteger(partitionsCount) || partitionsCount <= 0 || !Number.isInteger(replication) || replication <= 0) {
    emit("error", t("topics.createInvalid"));
    return;
  }
  const config = parseHeadersJson(createConfigText.value);
  if ("error" in config) {
    emit("error", `${t("topics.config")}: ${config.error}`);
    return;
  }
  busy.value = true;
  try {
    await kafkaApi.topicsCreate([name], partitionsCount, replication, config.headers);
    createOpen.value = false;
    emit("notify", `${t("topics.created")}: ${name}`);
    emit("refresh");
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  } finally {
    busy.value = false;
  }
}

function askDelete(topic: KafkaTopic | null) {
  if (!topic) return;
  deleteTarget.value = topic.name;
  deleteConfirmText.value = "";
  deleteOpen.value = true;
}

async function submitDelete() {
  // confirmTopic 必须与 topic 同名（防误删，§6）。
  if (deleteConfirmText.value.trim() !== deleteTarget.value) return;
  busy.value = true;
  try {
    await kafkaApi.topicsDelete([deleteTarget.value], deleteConfirmText.value.trim());
    deleteOpen.value = false;
    emit("notify", `${t("topics.deleted")}: ${deleteTarget.value}`);
    if (selected.value?.name === deleteTarget.value) selected.value = null;
    emit("refresh");
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  } finally {
    busy.value = false;
  }
}

function openExpand(topic: KafkaTopic | null) {
  if (!topic) return;
  selected.value = topic;
  expandCount.value = String(topic.partitionCount + 1);
  expandOpen.value = true;
}

async function submitExpand() {
  if (!selected.value) return;
  const next = Number.parseInt(expandCount.value, 10);
  // P2-20：扩分区校验用专用文案（语义=新分区数必须大于当前值），不再复用
  // err.partition（分区无效或 offset 超出范围）造成语义错位。
  if (!Number.isInteger(next) || next <= selected.value.partitionCount) {
    emit("error", t("topics.expandCountInvalid", { count: selected.value.partitionCount }));
    return;
  }
  busy.value = true;
  try {
    await kafkaApi.topicsPartitionsUpdate({ [selected.value.name]: next });
    expandOpen.value = false;
    emit("notify", t("topics.expanded"));
    emit("refresh");
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  } finally {
    busy.value = false;
  }
}

async function queryOffsets() {
  if (!selected.value) return;
  busy.value = true;
  emit("error", "");
  try {
    const offsetTime =
      offsetTimeMode.value === "custom" ? offsetTimeToParam(offsetCustomTime.value) : (offsetTimeMode.value as string);
    const response = await kafkaApi.topicsOffsetsList(
      [selected.value.name],
      offsetTime === undefined ? undefined : (offsetTime as string | number),
    );
    offsetRows.value = response.rows ?? [];
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  } finally {
    busy.value = false;
  }
}

async function openConfig(topic: KafkaTopic | null) {
  if (!topic) return;
  selected.value = topic;
  busy.value = true;
  try {
    const response = await kafkaApi.topicsConfigGet(topic.name);
    configEntries.value = response.entries ?? [];
    configEdits.value = [];
    configOpen.value = true;
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  } finally {
    busy.value = false;
  }
}

function addConfigEdit() {
  configEdits.value.push({ key: "", value: "" });
}

async function submitConfig() {
  if (!selected.value) return;
  const config: Record<string, string> = {};
  const deleteKeys: string[] = [];
  for (const edit of configEdits.value) {
    const key = edit.key.trim();
    if (!key) continue;
    if (edit.remove) deleteKeys.push(key);
    else config[key] = edit.value;
  }
  busy.value = true;
  try {
    const response = await kafkaApi.topicsConfigAlter(selected.value.name, config, deleteKeys);
    configEntries.value = response.entries ?? [];
    configEdits.value = [];
    emit("notify", t("topics.altered"));
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  } finally {
    busy.value = false;
  }
}

watch(
  () => props.topics,
  (next) => {
    if (selected.value) {
      const refreshed = next.find((topic) => topic.name === selected.value?.name);
      selected.value = refreshed ?? null;
    }
  },
);
</script>

<template>
  <section class="section-block panel-fill">
    <div class="result-meta">
      <span>{{ t("topics.title") }} · {{ topics.length }}</span>
      <span class="inline-actions">
        <button class="icon-button" :title="t('topics.refresh')" @click="emit('refresh')">
          <RefreshCw :class="{ spinning: loading }" />
        </button>
        <button class="qb-add" type="button" :disabled="!selected" :title="t('topics.describe')" @click="describeSelected">
          {{ t("topics.describe") }}
        </button>
        <button class="qb-add" type="button" :disabled="!selected" :title="t('topics.offsets')" @click="queryOffsets">
          {{ t("topics.offsets") }}
        </button>
        <button class="qb-add" type="button" :disabled="!selected" :title="t('topics.configGet')" @click="selected && openConfig(selected)">
          <Wrench aria-hidden="true" />
        </button>
        <button class="qb-add" type="button" :disabled="!canManage || !selected" :title="canManage ? t('topics.expand') : t('readOnly')" @click="selected && openExpand(selected)">
          <TrendingUp aria-hidden="true" />
        </button>
        <button class="toolbar-button" :disabled="!canManage" :title="canManage ? t('topics.create') : t('readOnly')" @click="openCreate">
          <Plus aria-hidden="true" /><span>{{ t("topics.create") }}</span>
        </button>
        <button
          class="qb-add"
          type="button"
          :disabled="!canDeleteTopic || !selected"
          :title="canDeleteTopic ? t('topics.delete') : canDelete ? t('readOnly') : t('noDelete')"
          @click="selected && askDelete(selected)"
        >
          <Trash2 aria-hidden="true" />
        </button>
      </span>
    </div>

    <div class="grid-box grid-box--fill">
      <!-- round4 面 1：三态收敛（与 TopicTree 同序：error → loading → empty），
           加载中/失败不再误显「空集群」。 -->
      <p v-if="error" class="empty compact" :title="errorDetail">{{ friendlyError }}</p>
      <p v-else-if="loading && topics.length === 0" class="empty compact">{{ t("tree.loading") }}</p>
      <p v-else-if="topics.length === 0" class="empty compact">{{ t("topics.empty") }}</p>
      <DbxAgGrid
        v-else
        table-key="topics"
        :row-data="topicGridRows"
        :column-defs="topicGridCols"
        :compact-fields="MINIMAL_TOPIC_FIELDS"
        :context-menu-items="topicContextMenuItems"
        row-selection="single"
        @selection-changed="(row: unknown) => selectTopic((row as TopicVm | null)?.raw ?? null)"
      />
    </div>

    <div v-if="selected && partitions.length > 0">
      <p class="subpanel-title">{{ t("topics.describeTitle", { topic: selected.name }) }}</p>
      <div class="grid-box" style="height: 180px">
        <DbxAgGrid
          table-key="topic-partitions"
          :row-data="partitionGridRows"
          :column-defs="partitionGridCols"
          :compact-fields="MINIMAL_PARTITION_FIELDS"
          :row-selection="false"
          :emit-row-click="false"
        />
      </div>
    </div>

    <div v-if="selected && offsetRows.length > 0">
      <p class="subpanel-title">{{ t("topics.offsetsTitle", { topic: selected.name }) }}</p>
      <div class="kafka-form" style="border: 0; padding: 0 0 4px">
        <label class="field">
          <span>{{ t("topics.offsetTime") }}</span>
          <select v-model="offsetTimeMode">
            <option value="earliest">{{ t("topics.timeEarliest") }}</option>
            <option value="latest">{{ t("topics.timeLatest") }}</option>
            <option value="max-timestamp">{{ t("topics.timeMaxTimestamp") }}</option>
            <option value="log-start">{{ t("topics.timeLogStart") }}</option>
            <option value="custom">{{ t("topics.timeCustom") }}</option>
          </select>
        </label>
        <label v-if="offsetTimeMode === 'custom'" class="field">
          <span>{{ t("messages.offsetTime") }} ({{ t("messages.offsetTimeHint") }})</span>
          <input v-model="offsetCustomTime" type="text" spellcheck="false" />
        </label>
        <button class="primary-button compact" type="button" :disabled="busy" @click="queryOffsets">{{ t("acls.filterRun") }}</button>
      </div>
      <div class="grid-box" style="height: 180px">
        <DbxAgGrid
          table-key="topic-offsets"
          :row-data="offsetGridRows"
          :column-defs="offsetGridCols"
          :compact-fields="MINIMAL_TOPIC_OFFSET_FIELDS"
          :row-selection="false"
          :emit-row-click="false"
        />
      </div>
    </div>

    <teleport to="body">
      <div v-if="createOpen" class="modal-backdrop" @click.self="createOpen = false">
        <div class="modal" ref="createModalEl" tabindex="-1" role="dialog" aria-modal="true">
          <header>
            <h2>{{ t("topics.createTitle") }}</h2>
            <button class="icon-button" :title="t('close')" @click="createOpen = false">✕</button>
          </header>
          <div class="settings-body">
            <label class="settings-field">
              <span>{{ t("topics.name") }}</span>
              <input v-model="createName" type="text" :placeholder="t('topics.namePlaceholder')" spellcheck="false" />
            </label>
            <label class="settings-field">
              <span>{{ t("topics.partitions") }}</span>
              <input v-model="createPartitions" type="number" min="1" />
            </label>
            <label class="settings-field">
              <span>{{ t("topics.replicationFactor") }}</span>
              <input v-model="createReplication" type="number" min="1" />
            </label>
            <label class="settings-field">
              <span>{{ t("topics.config") }}</span>
              <textarea v-model="createConfigText" rows="3" :placeholder="t('topics.configPlaceholder')" class="mono" spellcheck="false" />
            </label>
          </div>
          <footer>
            <button type="button" @click="createOpen = false">{{ t("cancel") }}</button>
            <button class="primary-button" type="button" :disabled="busy || !createName.trim()" :title="createName.trim() ? undefined : t('topics.namePlaceholder')" @click="submitCreate">{{ t("topics.create") }}</button>
          </footer>
        </div>
      </div>

      <div v-if="deleteOpen" class="modal-backdrop" @click.self="deleteOpen = false">
        <div class="modal" ref="deleteModalEl" tabindex="-1" role="dialog" aria-modal="true">
          <header>
            <h2>{{ t("topics.deleteTitle") }}</h2>
            <button class="icon-button" :title="t('close')" @click="deleteOpen = false">✕</button>
          </header>
          <p>{{ t("topics.deleteMessage") }}</p>
          <div class="destructive-copy">
            <div class="destructive-icon"><Trash2 aria-hidden="true" /></div>
            <div>
              <strong class="mono">{{ deleteTarget }}</strong>
              <p>
                <label class="settings-field">
                  <span>{{ t("topics.deleteConfirmLabel", { topic: deleteTarget }) }}</span>
                  <input v-model="deleteConfirmText" type="text" class="mono" spellcheck="false" @keyup.enter="submitDelete" />
                </label>
              </p>
            </div>
          </div>
          <footer>
            <button type="button" @click="deleteOpen = false">{{ t("cancel") }}</button>
            <button class="danger-button" type="button" :disabled="busy || deleteConfirmText.trim() !== deleteTarget" @click="submitDelete">
              {{ t("delete") }}
            </button>
          </footer>
        </div>
      </div>

      <div v-if="expandOpen" class="modal-backdrop" @click.self="expandOpen = false">
        <div class="modal small-modal" ref="expandModalEl" tabindex="-1" role="dialog" aria-modal="true">
          <header>
            <h2>{{ t("topics.expandTitle", { topic: selected?.name ?? "" }) }}</h2>
            <button class="icon-button" :title="t('close')" @click="expandOpen = false">✕</button>
          </header>
          <label class="settings-field">
            <span>{{ t("topics.expandNewCount") }}</span>
            <input v-model="expandCount" type="number" min="1" />
          </label>
          <footer>
            <button type="button" @click="expandOpen = false">{{ t("cancel") }}</button>
            <button class="primary-button" type="button" :disabled="busy" @click="submitExpand">{{ t("confirm") }}</button>
          </footer>
        </div>
      </div>

      <div v-if="configOpen" class="modal-backdrop" @click.self="configOpen = false">
        <div class="modal panel-modal" ref="configModalEl" tabindex="-1" role="dialog" aria-modal="true">
          <header>
            <h2>{{ t("topics.configTitle", { topic: selected?.name ?? "" }) }}</h2>
            <button class="icon-button" :title="t('close')" @click="configOpen = false">✕</button>
          </header>
          <div class="settings-body">
            <table class="config-table">
              <thead>
                <tr>
                  <th>{{ t("topics.configKey") }}</th>
                  <th>{{ t("topics.configValue") }}</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="entry in configEntries" :key="entry.name">
                  <td class="mono-s">{{ entry.name }}</td>
                  <td class="mono-s">
                    {{ entry.sensitive ? t("brokers.sensitiveMasked") : entry.value }}
                    <span v-if="entry.isDefault" class="badge">{{ t("brokers.colDefault") }}</span>
                  </td>
                </tr>
              </tbody>
            </table>
            <p class="hint">{{ t("topics.configDeleteKeys") }} → {{ t("topics.configRemove") }}</p>
            <div v-for="(edit, index) in configEdits" :key="index" class="field-filter-row" style="grid-template-columns: minmax(120px, 1fr) minmax(120px, 1fr) 26px 26px">
              <input v-model="edit.key" type="text" :placeholder="t('topics.configKey')" spellcheck="false" />
              <input v-model="edit.value" type="text" :placeholder="t('topics.configValue')" spellcheck="false" />
              <label class="checkbox"><input v-model="edit.remove" type="checkbox" :title="t('topics.configRemove')" /></label>
              <button class="row-remove" type="button" @click="configEdits.splice(index, 1)">✕</button>
            </div>
            <button class="qb-add" style="align-self: flex-start" type="button" @click="addConfigEdit">+ {{ t("topics.configAdd") }}</button>
          </div>
          <footer>
            <button type="button" @click="configOpen = false">{{ t("close") }}</button>
            <button class="primary-button" type="button" :disabled="busy || !canManage" :title="canManage ? undefined : t('readOnly')" @click="submitConfig">
              {{ t("save") }}
            </button>
          </footer>
        </div>
      </div>
    </teleport>
  </section>
</template>

<style scoped>
.config-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 11px;
}
.config-table th,
.config-table td {
  border-bottom: 1px solid color-mix(in srgb, var(--border) 45%, transparent);
  padding: 3px 6px;
  text-align: left;
  overflow-wrap: anywhere;
}
.config-table th {
  color: var(--muted-foreground);
  font-weight: 600;
}
/* P2 统一禁用态：只读下创建/扩分区/删除等按钮弱对比补强（cursor + 去饱和）。 */
button:disabled,
input:disabled,
select:disabled {
  cursor: not-allowed;
}
button:disabled {
  filter: grayscale(0.4);
}
</style>
