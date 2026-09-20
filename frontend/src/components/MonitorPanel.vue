<script setup lang="ts">
// Lag 监控面板（Phase 2）：选组 + topics → per-partition lag 快照表（ag-grid，
// 超阈值行高亮）→ 开始/停止采样（interval 5–60s 可配）→ 总 lag 迷你趋势 SVG。
// 阈值告警：总 lag 上穿阈值时发一次 alert 横幅（回落再上穿才重复）。
// 监控方案保存/加载/删除走既有 kafka/presets/*（params.type="monitor"）。
import { computed, onBeforeUnmount, onMounted, ref } from "vue";
import { Activity, Play, Save, Square, X } from "@lucide/vue";
import type { ColDef, GridOptions } from "ag-grid-community";
import DbxAgGrid from "./DbxAgGrid.vue";
import { kafkaApi, type GroupOffsetRow, type KafkaGroup, type MonitorPresetParams } from "../lib/api";
import {
  MINIMAL_LAG_FIELDS,
  lagColumns,
  toLagRows,
  type LagVm,
} from "../lib/kafkaColumns";
import { sumLag } from "../lib/topics";
import { t } from "../lib/i18n";

const emit = defineEmits<{
  (e: "error", message: string): void;
  (e: "notify", message: string): void;
  (e: "alert", message: string): void;
}>();

const HISTORY_MAX = 120;

const groups = ref<KafkaGroup[]>([]);
const group = ref("");
const topicsText = ref("");
const intervalSec = ref("10");
const threshold = ref("1000");
const sampling = ref(false);
const samplingBusy = ref(false);
const sampleCount = ref(0);
const totalLag = ref<number | null>(null);
const hasCommitted = ref(true);
const lagRows = ref<GroupOffsetRow[]>([]);
const history = ref<Array<{ at: number; lag: number }>>([]);
const breached = ref(false);

const plans = ref<Array<{ id: string; name: string }>>([]);
const planName = ref("");

const lagGridRows = computed(() => toLagRows(lagRows.value));
const lagGridCols = computed(() => lagColumns() as ColDef<LagVm>[]);

const lagRowClassRules = computed<GridOptions["rowClassRules"]>(() => ({
  "dbx-row-alert": (params) => {
    const value = Number(params.data?.lag ?? 0);
    return Number(threshold.value) > 0 && value > Number(threshold.value);
  },
}));

const trendPoints = computed(() => {
  const samples = history.value;
  if (samples.length < 2) return "";
  const maxLag = Math.max(...samples.map((sample) => sample.lag), 1);
  return samples
    .map((sample, index) => {
      const x = (index / (samples.length - 1)) * 100;
      const y = 40 - (sample.lag / maxLag) * 36 - 2;
      return `${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(" ");
});

const trendThresholdY = computed(() => {
  const samples = history.value;
  const limit = Number(threshold.value);
  if (samples.length < 2 || limit <= 0) return null;
  const maxLag = Math.max(...samples.map((sample) => sample.lag), limit, 1);
  return Math.max(2, 40 - (limit / maxLag) * 36 - 2);
});

function topicsArg(): string[] {
  return topicsText.value
    .split(/[,，\s]+/)
    .map((entry) => entry.trim())
    .filter(Boolean);
}

async function loadGroups() {
  try {
    const response = await kafkaApi.groupsList();
    groups.value = response.groups ?? [];
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  }
}

async function sample() {
  // 重入守卫：上一轮请求未返回时跳过本 tick，防止慢响应乱序覆盖
  // lagRows/history（趋势曲线乱序）。
  if (samplingBusy.value) return;
  if (!group.value) {
    emit("notify", t("monitor.needGroup"));
    return;
  }
  samplingBusy.value = true;
  // 不在此处 emit("error","")：阈值告警横幅由 App 持久展示，逐轮清空会吞掉告警。
  try {
    const response = await kafkaApi.groupsOffsetsList(group.value, topicsArg());
    const rows = response.rows ?? [];
    lagRows.value = rows;
    totalLag.value = typeof response.totalLag === "number" ? response.totalLag : sumLag(rows);
    hasCommitted.value = rows.some((row) => row.hasCommitted !== false);
    sampleCount.value += 1;
    history.value = [...history.value, { at: Date.now(), lag: totalLag.value }].slice(-HISTORY_MAX);
    const limit = Number(threshold.value);
    if (limit > 0 && totalLag.value > limit && !breached.value) {
      breached.value = true;
      emit("alert", t("monitor.thresholdBreached", { lag: totalLag.value, threshold: limit }));
    } else if (totalLag.value <= limit && breached.value) {
      breached.value = false;
    }
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  } finally {
    samplingBusy.value = false;
  }
}

let sampleTimer = 0;

function clampInterval(): number {
  const parsed = Number.parseInt(intervalSec.value, 10);
  return Number.isFinite(parsed) ? Math.min(60, Math.max(5, parsed)) : 10;
}

function startSampling() {
  if (sampling.value) return;
  if (!group.value) {
    emit("notify", t("monitor.needGroup"));
    return;
  }
  sampling.value = true;
  emit("error", "");
  void sample();
  const intervalMs = clampInterval() * 1000;
  sampleTimer = window.setInterval(() => void sample(), intervalMs);
}

function stopSampling() {
  sampling.value = false;
  window.clearInterval(sampleTimer);
  sampleTimer = 0;
}

// -- monitor plans（kafka/presets/*，params.type="monitor"）---------------------------

async function loadPlans() {
  try {
    const response = await kafkaApi.presetsList();
    plans.value = (response.presets ?? [])
      .filter((preset) => preset.params?.type === "monitor")
      .map((preset) => ({ id: preset.id, name: preset.name }));
  } catch {
    plans.value = [];
  }
}

async function savePlan() {
  const name = planName.value.trim();
  if (!name) return;
  const monitor: MonitorPresetParams = {
    group: group.value,
    topics: topicsArg(),
    intervalSec: clampInterval(),
    threshold: Math.max(0, Number.parseInt(threshold.value, 10) || 0),
  };
  try {
    await kafkaApi.presetsSave({
      id: `monitor-${Date.now()}`,
      name,
      params: { topic: "", offsetStrategy: "latest", type: "monitor", monitor },
    });
    planName.value = "";
    emit("notify", t("monitor.planSaved"));
    await loadPlans();
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  }
}

async function applyPlan(id: string) {
  if (!id) return;
  try {
    const response = await kafkaApi.presetsList();
    const preset = (response.presets ?? []).find((row) => row.id === id);
    const monitor = preset?.params?.monitor;
    if (!monitor) return;
    group.value = monitor.group ?? "";
    topicsText.value = (monitor.topics ?? []).join(",");
    intervalSec.value = String(monitor.intervalSec ?? 10);
    threshold.value = String(monitor.threshold ?? 0);
    emit("notify", t("monitor.planApplied"));
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  }
}

async function removePlan(id: string) {
  try {
    await kafkaApi.presetsRemove(id);
    emit("notify", t("monitor.planRemoved"));
    await loadPlans();
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  }
}

onMounted(() => {
  void loadGroups();
  void loadPlans();
});

onBeforeUnmount(() => {
  stopSampling();
});
</script>

<template>
  <section class="section-block panel-fill">
    <div class="kafka-form">
      <label class="field" style="flex: 1 1 180px">
        <span>{{ t("monitor.group") }}</span>
        <select v-model="group">
          <option value="">{{ t("acls.anyValue") }}</option>
          <option v-for="item in groups" :key="item.group" :value="item.group">{{ item.group }}</option>
        </select>
      </label>
      <label class="field" style="flex: 1 1 200px">
        <span>{{ t("monitor.topics") }}</span>
        <input v-model="topicsText" type="text" placeholder="order-events, payment-gateway" spellcheck="false" />
      </label>
      <label class="field">
        <span>{{ t("monitor.interval") }} ({{ t("monitor.intervalHint") }})</span>
        <input v-model="intervalSec" type="number" min="5" max="60" />
      </label>
      <label class="field">
        <span>{{ t("monitor.threshold") }}</span>
        <input v-model="threshold" type="number" min="0" :title="t('monitor.thresholdHint')" />
      </label>
      <button v-if="!sampling" class="primary-button compact" type="button" :disabled="!group" @click="startSampling">
        <Play aria-hidden="true" />{{ t("monitor.start") }}
      </button>
      <button v-else class="danger-button compact" type="button" @click="stopSampling">
        <Square aria-hidden="true" />{{ t("monitor.stop") }}
      </button>
    </div>

    <div class="kafka-form" style="border-bottom: 1px solid var(--border)">
      <select :value="''" @change="applyPlan(($event.target as HTMLSelectElement).value)">
        <option value="">{{ t("monitor.plans") }}</option>
        <option v-if="plans.length === 0" disabled value="">{{ t("monitor.planEmpty") }}</option>
        <option v-for="plan in plans" :key="plan.id" :value="plan.id">{{ plan.name }}</option>
      </select>
      <input v-model="planName" type="text" :placeholder="t('monitor.planName')" style="width: 140px" spellcheck="false" />
      <button class="qb-add" type="button" :title="t('monitor.planSave')" @click="savePlan"><Save aria-hidden="true" /></button>
      <button
        v-for="plan in plans"
        :key="`rm-${plan.id}`"
        class="qb-add"
        type="button"
        :title="`${t('monitor.planRemove')}: ${plan.name}`"
        @click="removePlan(plan.id)"
      >
        <X aria-hidden="true" />
      </button>
    </div>

    <div class="stream-meta">
      <span class="badge" :class="{ 'badge-ok': sampling, 'badge-warn': !sampling }">
        <Activity aria-hidden="true" style="width: 11px; height: 11px" />
        {{ t("monitor.title") }}
      </span>
      <span>{{ t("monitor.samples", { count: sampleCount }) }}</span>
      <span v-if="totalLag !== null" class="badge" :class="breached ? 'badge-danger' : totalLag > 0 ? 'badge-warn' : 'badge-ok'">
        {{ t("monitor.totalLag", { lag: totalLag }) }}
      </span>
      <span v-if="!hasCommitted && lagRows.length > 0" class="badge badge-warn">{{ t("groups.hasCommittedFalse") }}</span>
    </div>

    <div class="monitor-trend">
      <p class="subpanel-title">{{ t("monitor.trend") }}</p>
      <svg v-if="trendPoints" class="trend-svg" viewBox="0 0 100 40" preserveAspectRatio="none" role="img" :aria-label="t('monitor.trend')">
        <line v-if="trendThresholdY !== null" x1="0" :y1="trendThresholdY" x2="100" :y2="trendThresholdY" class="trend-threshold" />
        <polyline :points="trendPoints" class="trend-line" />
      </svg>
      <p v-else class="empty compact">{{ t("monitor.trendEmpty") }}</p>
    </div>

    <p class="subpanel-title">{{ t("monitor.lagTitle") }}</p>
    <div class="grid-box grid-box--fill">
      <p v-if="lagRows.length === 0" class="empty compact">{{ t("monitor.lagEmpty") }}</p>
      <DbxAgGrid
        v-else
        table-key="monitor-lag"
        :row-data="lagGridRows"
        :column-defs="lagGridCols"
        :compact-fields="MINIMAL_LAG_FIELDS"
        :row-selection="false"
        :emit-row-click="false"
        :row-class-rules="lagRowClassRules"
      />
    </div>
  </section>
</template>

<style scoped>
.monitor-trend { display: flex; min-height: 0; flex-direction: column; }
.trend-svg {
  width: calc(100% - 16px);
  height: 96px;
  margin: 0 8px;
  border: 1px solid var(--border);
  border-radius: 6px;
  background: color-mix(in srgb, var(--muted) 24%, var(--background));
}
.trend-line {
  fill: none;
  stroke: var(--primary);
  stroke-width: 1.4;
  vector-effect: non-scaling-stroke;
}
.trend-threshold {
  stroke: var(--destructive);
  stroke-width: 1;
  stroke-dasharray: 3 2;
  vector-effect: non-scaling-stroke;
}
/* P2 统一禁用态：未选组时开始采样按钮补强。 */
button:disabled,
input:disabled,
select:disabled {
  cursor: not-allowed;
}
button:disabled {
  filter: grayscale(0.4);
}
</style>
