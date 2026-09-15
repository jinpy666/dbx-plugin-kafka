<script setup lang="ts">
// Schema Registry 面板（Phase 2）：subject 列表（ag-grid）→ 版本列表（ag-grid）→
// schema 查看（JSON pretty）→ 版本对比（hunks add/remove/modify + summary）、
// 兼容性 get/set、compatibility check（粘贴候选 schema）、register（avro/json +
// JSON 校验）、delete subject/version（critical：read_only + allowDelete +
// 输入名称 + 二次确认 双门禁）。
// Phase P：registry 徽章/切换（Confluent/AWS Glue）——schemaTest 逐提供方探测，
// 两方均配置时展示下拉切换；Glue 兼容性枚举为 NONE/DISABLED/BACKWARD_ALL/…
// （api.ts SchemaCompatibilityLevel 联合类型已扩展），全部调用透传 registry 参数。
import { computed, onMounted, ref, watch } from "vue";
import { GitCompare, Plus, RefreshCw, ShieldQuestion, Trash2, X } from "@lucide/vue";
import type { ColDef } from "ag-grid-community";
import DbxAgGrid from "./DbxAgGrid.vue";
import CodeEditor from "./CodeEditor.vue";
import SchemaTree from "./SchemaTree.vue";
import {
  kafkaApi,
  type SchemaCompatibilityCheckResult,
  type SchemaCompatibilityLevel,
  type SchemaDetail,
  type SchemaDiff,
  type SchemaFormat,
  type SchemaRegistryProvider,
  type SchemaSubject,
  type SchemaVersionRow,
} from "../lib/api";
import {
  MINIMAL_SUBJECT_FIELDS,
  MINIMAL_VERSION_FIELDS,
  schemaVersionColumns,
  subjectColumns,
  toSchemaVersionRows,
  toSubjectRows,
  type SchemaVersionVm,
  type SubjectVm,
} from "../lib/kafkaColumns";
import { buildSchemaTree, prettyJson, schemaTemplateFor, type SchemaTreeNode } from "../lib/kafkaModel";
import { useModalBehavior } from "../lib/modalBehavior";
import { t } from "../lib/i18n";

const props = defineProps<{
  canWrite: boolean;
  canDelete: boolean;
  /** App.vue 经 kafka/connections/statuses 读到的 SR provider（旧 sidecar 缺省 = ""）。 */
  srProvider?: string;
}>();

const emit = defineEmits<{
  (e: "error", message: string): void;
  (e: "notify", message: string): void;
}>();

// Confluent 兼容枚举（老五档 + TRANSITIVE）。
const CONFLUENT_COMPATIBILITY_LEVELS: SchemaCompatibilityLevel[] = [
  "NONE",
  "BACKWARD",
  "BACKWARD_TRANSITIVE",
  "FORWARD",
  "FORWARD_TRANSITIVE",
  "FULL",
  "FULL_TRANSITIVE",
];
// AWS Glue 枚举（与 Confluent NONE/DISABLED 语义不同，原样透传展示）。
const GLUE_COMPATIBILITY_LEVELS: SchemaCompatibilityLevel[] = [
  "NONE",
  "DISABLED",
  "BACKWARD",
  "BACKWARD_ALL",
  "FORWARD",
  "FORWARD_ALL",
  "FULL",
  "FULL_ALL",
];

// -- registry（Phase P：provider 徽章 + 双后端切换）--------------------------------

const registry = ref<SchemaRegistryProvider>("confluent");
const registryAvailable = ref<{ confluent: boolean; glue: boolean }>({ confluent: false, glue: false });
const registryTouched = ref(false);
const bothRegistriesAvailable = computed(() => registryAvailable.value.confluent && registryAvailable.value.glue);
const compatibilityLevels = computed(() =>
  registry.value === "glue" ? GLUE_COMPATIBILITY_LEVELS : CONFLUENT_COMPATIBILITY_LEVELS,
);

function registryLabel(provider: SchemaRegistryProvider): string {
  return t(provider === "glue" ? "schemas.registryGlue" : "schemas.registryConfluent");
}

function onRegistryChange() {
  registryTouched.value = true;
  // 切换 provider = 切换命名空间：清空选中与派生态后整表重载。
  selectedSubject.value = "";
  versions.value = [];
  selectedVersion.value = null;
  detail.value = null;
  diff.value = null;
  compatLevel.value = "";
  compatScope.value = "";
  void loadSubjects();
}

// 逐提供方 schemaTest 探测可用性（旧 sidecar 忽略 registry 参数时 confluent 仍可用）。
async function probeRegistries() {
  const results = await Promise.all(
    (["confluent", "glue"] as const).map(async (candidate) => {
      try {
        const response = await kafkaApi.schemaTest(candidate);
        return [candidate, response.success !== false] as const;
      } catch {
        return [candidate, false] as const;
      }
    }),
  );
  registryAvailable.value = {
    confluent: results.some(([name, ok]) => name === "confluent" && ok),
    glue: results.some(([name, ok]) => name === "glue" && ok),
  };
  const preferred = props.srProvider === "glue" || props.srProvider === "confluent" ? props.srProvider : registry.value;
  registry.value = registryAvailable.value[preferred] ? preferred : registryAvailable.value.confluent ? "confluent" : "glue";
}

// statuses.provider 异步到达时，若用户尚未手动切换则跟随连接默认 provider。
watch(
  () => props.srProvider,
  (provider) => {
    if (registryTouched.value) return;
    if ((provider === "confluent" || provider === "glue") && registryAvailable.value[provider]) {
      if (registry.value !== provider) {
        registry.value = provider;
        void loadSubjects();
      }
    }
  },
);

const srAvailable = ref(true);
const subjects = ref<SchemaSubject[]>([]);
const loading = ref(false);
const busy = ref(false);

const selectedSubject = ref("");
const versions = ref<SchemaVersionRow[]>([]);
const selectedVersion = ref<number | null>(null);
const detail = ref<SchemaDetail | null>(null);

const compatLevel = ref("");
const compatScope = ref("");
const compatChoice = ref<SchemaCompatibilityLevel>("BACKWARD");

const diffFrom = ref("");
const diffTo = ref("");
const diff = ref<SchemaDiff | null>(null);
const diffBusy = ref(false);

const registerOpen = ref(false);
const registerSubject = ref("");
const registerFormat = ref<SchemaFormat>("avro");
const registerSchemaText = ref("");
// normalize：Confluent SR 可选查询参数（SR 侧归一化存储文本；glue 后端明确拒绝）。
const registerNormalize = ref(false);
// F5 克隆：来源版本号（弹窗标题/提示用；普通注册为 null）。
const registerClonedFrom = ref<number | null>(null);
// PROTOBUF schema 是 proto3 文本（非 JSON），不做 JSON 校验（SR 侧校验）。
const registerJsonError = computed(() => {
  if (registerFormat.value === "protobuf") return "";
  if (!registerSchemaText.value.trim()) return "";
  const parsed = parseJsonCandidate(registerSchemaText.value);
  return parsed.ok ? "" : parsed.error;
});
// 注册编辑器语言：AVRO/JSON 走 json 高亮 + 错误行标红；PROTOBUF 是 proto3 纯文本。
const registerEditorLanguage = computed(() => (registerFormat.value === "protobuf" ? "text" : "json"));

const checkOpen = ref(false);
const checkSubject = ref("");
const checkFormat = ref<"avro" | "json">("avro");
const checkVersionText = ref("");
const checkSchemaText = ref("");
const checkResult = ref<SchemaCompatibilityCheckResult | null>(null);
const checkBusy = ref(false);

// delete 双确认（critical 双门禁：canDelete + 输入名称 + 二次确认）
const deletePlan = ref<{ kind: "subject" | "version"; subject: string; version?: number } | null>(null);
const deletePhase = ref<1 | 2>(1);
const deleteConfirmText = ref("");
const deleteBusy = ref(false);

// 弹层行为统一接入（UI 扫描第 2 轮 P1-5）：注册 / 兼容检查 / 删除弹窗支持
// Esc 关闭 + Tab 焦点陷阱 + 关闭归还触发元素（决策逻辑 lib/modalBehavior）。
const registerModalEl = ref<HTMLElement | null>(null);
const checkModalEl = ref<HTMLElement | null>(null);
const deleteModalEl = ref<HTMLElement | null>(null);
useModalBehavior({ open: registerOpen, container: registerModalEl, close: () => (registerOpen.value = false) });
useModalBehavior({ open: checkOpen, container: checkModalEl, close: () => (checkOpen.value = false) });
useModalBehavior({ open: computed(() => deletePlan.value !== null), container: deleteModalEl, close: () => (deletePlan.value = null) });

const subjectGridRows = computed(() => toSubjectRows(subjects.value));
const subjectGridCols = computed(() => subjectColumns() as ColDef<SubjectVm>[]);
// F5：版本表带行操作「克隆」列（kafkaColumns.actionColumn 原生 button 渲染）。
const versionGridRows = computed(() => toSchemaVersionRows(versions.value));
const versionGridCols = computed(() => schemaVersionColumns({ onClone: (row) => void cloneVersion(row) }) as ColDef<SchemaVersionVm>[]);
const versionOptions = computed(() => versions.value.map((row) => row.version));
const schemaText = computed(() => (detail.value ? prettyJson(detail.value.schema) : ""));

// F5 树视图：详情区 树/文本 toggle（缺省文本 = 既有行为；PROTOBUF 树化后续）。
const viewMode = ref<"text" | "tree">("text");
const schemaTreeRoot = computed<SchemaTreeNode | null>(() => {
  if (!detail.value || detail.value.format === "protobuf") return null;
  return buildSchemaTree(detail.value.schema);
});
const treeAvailable = computed(() => schemaTreeRoot.value !== null);
// PROTOBUF：保持文本 + 行内提示（后端 FDSet 树化登记后续，§12.2.7）。
const treeUnsupported = computed(() => Boolean(detail.value && detail.value.format === "protobuf"));

async function loadSubjects() {
  loading.value = true;
  emit("error", "");
  try {
    const probe = await kafkaApi.schemaTest(registry.value);
    srAvailable.value = probe.success !== false;
    if (!srAvailable.value) {
      subjects.value = [];
      return;
    }
    const response = await kafkaApi.schemaSubjectsList(registry.value);
    subjects.value = response.subjects ?? [];
  } catch (cause) {
    srAvailable.value = false;
    subjects.value = [];
    emit("error", cause instanceof Error ? cause.message : String(cause));
  } finally {
    loading.value = false;
  }
}

async function loadCompat(subject: string | undefined) {
  try {
    const response = await kafkaApi.schemaCompatibilityGet(subject, registry.value);
    compatLevel.value = response.level ?? "";
    compatScope.value = response.scope ?? "";
    compatChoice.value = (response.level as SchemaCompatibilityLevel) || "BACKWARD";
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  }
}

async function selectSubject(row: SubjectVm | null) {
  selectedSubject.value = row?.subject ?? "";
  versions.value = [];
  selectedVersion.value = null;
  detail.value = null;
  diff.value = null;
  if (!selectedSubject.value) return;
  busy.value = true;
  emit("error", "");
  try {
    const response = await kafkaApi.schemaVersionsList(selectedSubject.value, registry.value);
    versions.value = response.versions ?? [];
    if (versions.value.length > 0) {
      const latest = versions.value[versions.value.length - 1].version;
      diffFrom.value = String(latest > 1 ? latest - 1 : latest);
      diffTo.value = String(latest);
      await viewVersion(latest);
    }
    await loadCompat(selectedSubject.value);
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  } finally {
    busy.value = false;
  }
}

async function viewVersion(version: number) {
  if (!selectedSubject.value) return;
  busy.value = true;
  emit("error", "");
  try {
    detail.value = await kafkaApi.schemaGet(selectedSubject.value, version, registry.value);
    selectedVersion.value = version;
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  } finally {
    busy.value = false;
  }
}

async function runDiff() {
  if (!selectedSubject.value || !diffFrom.value || !diffTo.value) return;
  diffBusy.value = true;
  emit("error", "");
  try {
    diff.value = await kafkaApi.schemaVersionsCompare(selectedSubject.value, Number(diffFrom.value), Number(diffTo.value), registry.value);
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  } finally {
    diffBusy.value = false;
  }
}

async function applyCompat() {
  if (!props.canWrite) return;
  busy.value = true;
  emit("error", "");
  try {
    const response = await kafkaApi.schemaCompatibilitySet(selectedSubject.value || undefined, compatChoice.value, registry.value);
    compatLevel.value = response.level ?? compatChoice.value;
    emit("notify", t("schemas.compatSetDone"));
    await loadSubjects();
    if (selectedSubject.value) await selectSubject({ subject: selectedSubject.value } as SubjectVm);
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  } finally {
    busy.value = false;
  }
}

// -- register ---------------------------------------------------------------------

function openRegister() {
  registerSubject.value = selectedSubject.value ? `${selectedSubject.value}` : "";
  registerFormat.value = "avro";
  registerSchemaText.value = "";
  registerNormalize.value = false;
  registerClonedFrom.value = null;
  registerOpen.value = true;
}

// F5 克隆：版本表行操作「克隆」→ 取该版本 schema 文本（schemaGet 已有数据形状）
// 预填注册弹窗，subject 默认原值可改。
async function cloneVersion(row: SchemaVersionVm) {
  if (!selectedSubject.value) return;
  emit("error", "");
  try {
    const detailRow = await kafkaApi.schemaGet(selectedSubject.value, row.version, registry.value);
    registerSubject.value = selectedSubject.value;
    registerFormat.value = (["avro", "json", "protobuf"].includes(detailRow.format) ? detailRow.format : "avro") as SchemaFormat;
    registerSchemaText.value = detailRow.schema ?? "";
    registerNormalize.value = false;
    registerClonedFrom.value = row.version;
    registerOpen.value = true;
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  }
}

/** F5 模板：format 选定后插入对应静态模板（覆盖当前文本）。 */
function insertTemplate() {
  registerSchemaText.value = schemaTemplateFor(registerFormat.value);
}

function parseJsonCandidate(text: string): { ok: true } | { ok: false; error: string } {
  try {
    JSON.parse(text);
    return { ok: true };
  } catch (cause) {
    return { ok: false, error: cause instanceof Error ? cause.message : String(cause) };
  }
}

async function submitRegister() {
  const subject = registerSubject.value.trim();
  if (!subject || !registerSchemaText.value.trim()) {
    emit("error", !subject ? t("schemas.needSubject") : t("schemas.needSchema"));
    return;
  }
  // PROTOBUF 是 proto3 文本，SR 侧校验；AVRO/JSON 才做前端 JSON 语法校验。
  if (registerFormat.value !== "protobuf") {
    const parsed = parseJsonCandidate(registerSchemaText.value);
    if (!parsed.ok) {
      emit("error", `${t("schemas.registerInvalidJson")}: ${parsed.error}`);
      return;
    }
  }
  busy.value = true;
  emit("error", "");
  try {
    const response = await kafkaApi.schemaRegister(subject, registerFormat.value, registerSchemaText.value, registry.value, registerNormalize.value || undefined);
    registerOpen.value = false;
    emit("notify", t("schemas.registered", { version: response.version, id: response.id }));
    await loadSubjects();
    // selectSubject 只读 row.subject：传 { raw: ... } 形状会把选中清空。
    await selectSubject({ subject } as SubjectVm);
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  } finally {
    busy.value = false;
  }
}

// -- compatibility check ------------------------------------------------------------

function openCheck() {
  checkSubject.value = selectedSubject.value;
  checkFormat.value = "avro";
  checkVersionText.value = "";
  checkSchemaText.value = "";
  checkResult.value = null;
  checkOpen.value = true;
}

async function submitCheck() {
  const subject = checkSubject.value.trim();
  if (!subject || !checkSchemaText.value.trim()) {
    emit("error", !subject ? t("schemas.needSubject") : t("schemas.needSchema"));
    return;
  }
  const version = Number.parseInt(checkVersionText.value, 10);
  checkBusy.value = true;
  emit("error", "");
  try {
    checkResult.value = await kafkaApi.schemaCompatibilityCheck(
      subject,
      checkFormat.value,
      checkSchemaText.value,
      Number.isFinite(version) && version > 0 ? version : undefined,
      registry.value,
    );
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  } finally {
    checkBusy.value = false;
  }
}

// -- delete（双门禁 + 双确认）----------------------------------------------------------

function askDeleteSubject() {
  if (!selectedSubject.value) return;
  deletePlan.value = { kind: "subject", subject: selectedSubject.value };
  deletePhase.value = 1;
  deleteConfirmText.value = "";
}

function askDeleteVersion() {
  if (!selectedSubject.value || selectedVersion.value === null) return;
  deletePlan.value = { kind: "version", subject: selectedSubject.value, version: selectedVersion.value };
  deletePhase.value = 1;
  deleteConfirmText.value = "";
}

function deleteTargetName(): string {
  const plan = deletePlan.value;
  if (!plan) return "";
  return plan.kind === "subject" ? plan.subject : `${plan.subject} v${plan.version}`;
}

function proceedDeletePhase() {
  if (deleteConfirmText.value.trim() !== deleteTargetName()) return;
  deletePhase.value = 2;
}

async function submitDelete() {
  const plan = deletePlan.value;
  if (!plan) return;
  deleteBusy.value = true;
  emit("error", "");
  try {
    if (plan.kind === "subject") {
      await kafkaApi.schemaDelete(plan.subject, registry.value);
      emit("notify", t("schemas.deletedSubject"));
      selectedSubject.value = "";
      versions.value = [];
      detail.value = null;
      diff.value = null;
    } else {
      await kafkaApi.schemaDeleteVersion(plan.subject, plan.version ?? 0, registry.value);
      emit("notify", t("schemas.deletedVersion"));
    }
    deletePlan.value = null;
    await loadSubjects();
    if (plan.kind === "version" && selectedSubject.value) await selectSubject({ subject: selectedSubject.value } as SubjectVm);
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  } finally {
    deleteBusy.value = false;
  }
}

function hunkClass(op: string): string {
  if (op === "add") return "badge-ok";
  if (op === "remove") return "badge-danger";
  return "badge-warn";
}

function hunkLabel(op: string): string {
  if (op === "add") return t("schemas.hunkAdd");
  if (op === "remove") return t("schemas.hunkRemove");
  return t("schemas.hunkModify");
}

onMounted(async () => {
  await probeRegistries();
  await loadSubjects();
});
</script>

<template>
  <section class="section-block">
    <div class="result-meta">
      <span>{{ t("schemas.title") }} · {{ subjects.length }}</span>
      <span class="inline-actions">
        <span
          class="badge"
          :class="srAvailable ? '' : 'badge-warn'"
          :title="t('schemas.registryLabel')"
        >{{ t("schemas.registryLabel") }}: {{ registryLabel(registry) }}</span>
        <select
          v-if="bothRegistriesAvailable"
          v-model="registry"
          :title="t('schemas.registryLabel')"
          style="max-width: 130px"
          @change="onRegistryChange"
        >
          <option value="confluent">{{ t("schemas.registryConfluent") }}</option>
          <option value="glue">{{ t("schemas.registryGlue") }}</option>
        </select>
        <button class="icon-button" :title="t('refresh')" @click="loadSubjects"><RefreshCw :class="{ spinning: loading }" /></button>
        <button class="qb-add" type="button" :disabled="!canWrite" :title="canWrite ? t('schemas.register') : t('schemas.readOnlyHint')" @click="openRegister">
          <Plus aria-hidden="true" />{{ t("schemas.register") }}
        </button>
        <button class="qb-add" type="button" :title="t('schemas.compatCheckTitle')" @click="openCheck">
          <ShieldQuestion aria-hidden="true" />{{ t("schemas.compatCheck") }}
        </button>
        <button
          class="qb-add"
          type="button"
          :disabled="!canDelete || !selectedSubject"
          :title="canDelete ? t('schemas.deleteSubject') : canWrite ? t('noDelete') : t('readOnly')"
          @click="askDeleteSubject"
        >
          <Trash2 aria-hidden="true" />{{ t("schemas.deleteSubject") }}
        </button>
      </span>
    </div>
    <p v-if="!srAvailable" class="form-error" style="padding: 0 8px 4px">{{ t("schemas.srRequired") }}</p>
    <p v-if="!canWrite" class="hint" style="padding: 0 8px 4px">{{ t("schemas.readOnlyHint") }}</p>

    <div class="kafka-form" style="border-bottom: 1px solid var(--border)">
      <label class="field">
        <span>{{ t("schemas.compatibility") }} ({{ compatScope || "GLOBAL" }})</span>
        <select v-model="compatChoice" :disabled="!canWrite || busy">
          <option v-for="level in compatibilityLevels" :key="level" :value="level">{{ level }}</option>
        </select>
      </label>
      <!-- P2-6：当前兼容级别徽标加语义前缀（复用 compatLevel key），不再只是孤零零的「—」 -->
      <span class="badge" :class="compatLevel ? '' : 'badge-warn'" :title="t('schemas.compatScope')">
        {{ t("schemas.compatLevel") }}: {{ compatLevel || "—" }}
      </span>
      <button class="primary-button compact" type="button" :disabled="!canWrite || busy" :title="canWrite ? t('schemas.compatSet') : t('schemas.readOnlyHint')" @click="applyCompat">
        {{ t("schemas.compatSet") }}
      </button>
    </div>

    <div class="grid-box" style="height: 200px">
      <p v-if="subjects.length === 0 && !loading" class="empty compact">{{ t("acls.empty") }}</p>
      <DbxAgGrid
        v-else
        table-key="schema-subjects"
        :row-data="subjectGridRows"
        :column-defs="subjectGridCols"
        :compact-fields="MINIMAL_SUBJECT_FIELDS"
        row-selection="single"
        @selection-changed="(row: unknown) => selectSubject(row as SubjectVm | null)"
      />
    </div>

    <template v-if="selectedSubject">
      <p class="subpanel-title">{{ t("schemas.versionsTitle", { subject: selectedSubject }) }}</p>
      <div class="kafka-form" style="border: 0; padding: 0 8px 4px">
        <label class="field">
          <span>{{ t("schemas.diffFrom") }}</span>
          <select v-model="diffFrom">
            <option v-for="version in versionOptions" :key="`from-${version}`" :value="String(version)">{{ version }}</option>
          </select>
        </label>
        <label class="field">
          <span>{{ t("schemas.diffTo") }}</span>
          <select v-model="diffTo">
            <option v-for="version in versionOptions" :key="`to-${version}`" :value="String(version)">{{ version }}</option>
          </select>
        </label>
        <button class="primary-button compact" type="button" :disabled="diffBusy || diffFrom === diffTo" :title="t('schemas.diffRun')" @click="runDiff">
          <GitCompare aria-hidden="true" />{{ t("schemas.diffRun") }}
        </button>
        <button
          class="qb-add"
          type="button"
          :disabled="!canDelete || selectedVersion === null"
          :title="canDelete ? t('schemas.deleteVersion') : canWrite ? t('noDelete') : t('readOnly')"
          @click="askDeleteVersion"
        >
          <Trash2 aria-hidden="true" />{{ t("schemas.deleteVersion") }}
        </button>
      </div>
      <div class="grid-box" style="height: 150px">
        <p v-if="versions.length === 0" class="empty compact">{{ t("topics.offsetsEmpty") }}</p>
        <DbxAgGrid
          v-else
          table-key="schema-versions"
          :row-data="versionGridRows"
          :column-defs="versionGridCols"
          :compact-fields="MINIMAL_VERSION_FIELDS"
          row-selection="single"
          @selection-changed="(row: unknown) => row && viewVersion((row as SchemaVersionVm).version)"
        />
      </div>

      <div v-if="detail" class="schema-detail">
        <p class="subpanel-title">
          {{ t("schemas.viewSchema") }} · v{{ detail.version }} · id {{ detail.id }} · {{ detail.format }}
          <span v-if="detail.references && detail.references.length > 0" class="badge">{{ t("schemas.references") }}: {{ detail.references.length }}</span>
        </p>
        <!-- F5 树/文本 toggle：AVRO/JSON 递归树；PROTOBUF 保持文本 + 行内提示。 -->
        <div class="inline-actions schema-view-toggle">
          <button
            class="tz-toggle"
            :class="{ 'is-utc': viewMode === 'text' }"
            type="button"
            :aria-pressed="viewMode === 'text'"
            data-testid="schema-view-text"
            @click="viewMode = 'text'"
          >
            {{ t("schemas.viewText") }}
          </button>
          <button
            class="tz-toggle"
            :class="{ 'is-utc': viewMode === 'tree' }"
            type="button"
            :disabled="!treeAvailable"
            :title="treeAvailable ? t('schemas.viewTree') : t('schemas.treeUnsupportedHint')"
            :aria-pressed="viewMode === 'tree'"
            data-testid="schema-view-tree"
            @click="viewMode = 'tree'"
          >
            {{ t("schemas.viewTree") }}
          </button>
        </div>
        <p v-if="treeUnsupported" class="hint" data-testid="schema-tree-hint">{{ t("schemas.treeUnsupportedHint") }}</p>
        <div v-if="viewMode === 'tree' && treeAvailable" class="value-view schema-view schema-tree-box" data-testid="schema-tree">
          <SchemaTree :node="schemaTreeRoot!" :default-expanded="true" />
        </div>
        <pre v-else class="value-view schema-view">{{ schemaText }}</pre>
      </div>

      <div v-if="diff" class="schema-diff">
        <p class="subpanel-title">
          {{ t("schemas.diffTitle") }} · v{{ diffFrom }} → v{{ diffTo }}
          <span class="badge">{{ t("schemas.diffSummary") }}: {{ diff.summary }}</span>
        </p>
        <p v-if="!diff.hunks || diff.hunks.length === 0" class="hint" style="padding: 0 8px">{{ t("schemas.diffNoChange") }}</p>
        <div v-for="(hunk, index) in diff.hunks" :key="index" class="diff-hunk">
          <span class="badge" :class="hunkClass(hunk.op)">{{ hunkLabel(hunk.op) }}</span>
          <span class="mono-s diff-path">{{ hunk.path }}</span>
          <div v-if="hunk.before !== undefined" class="diff-block">
            <span class="hint">{{ t("schemas.before") }}</span>
            <pre class="diff-lines diff-lines--before">{{ hunk.before }}</pre>
          </div>
          <div v-if="hunk.after !== undefined" class="diff-block">
            <span class="hint">{{ t("schemas.after") }}</span>
            <pre class="diff-lines diff-lines--after">{{ hunk.after }}</pre>
          </div>
        </div>
      </div>
    </template>

    <teleport to="body">
      <div v-if="registerOpen" class="modal-backdrop" @click.self="registerOpen = false">
        <div class="modal panel-modal" ref="registerModalEl" tabindex="-1" role="dialog" aria-modal="true">
          <header>
            <h2>
              {{ registerClonedFrom !== null ? t("schemas.cloneTitle", { version: registerClonedFrom }) : t("schemas.registerTitle") }}
            </h2>
            <button class="icon-button" :title="t('close')" @click="registerOpen = false"><X /></button>
          </header>
          <div class="settings-body">
            <label class="settings-field">
              <span>{{ t("schemas.registerSubject") }}</span>
              <input v-model="registerSubject" type="text" :placeholder="t('schemas.registerSubjectPlaceholder')" class="mono" spellcheck="false" />
            </label>
            <label class="settings-field">
              <span>{{ t("schemas.registerFormat") }}</span>
              <select v-model="registerFormat">
                <option value="avro">avro</option>
                <option value="json">json</option>
                <option value="protobuf">protobuf</option>
              </select>
            </label>
            <label class="settings-field">
              <span>{{ t("schemas.registerSchemaJson") }}</span>
              <CodeEditor
                v-model="registerSchemaText"
                :language="registerEditorLanguage"
                :invalid="registerJsonError !== ''"
                :placeholder="t('schemaWrite.editorPlaceholder')"
                min-height="180px"
                max-height="320px"
              />
            </label>
            <div class="inline-actions">
              <button class="toolbar-button" type="button" data-testid="insert-template" @click="insertTemplate">
                {{ t("schemas.insertTemplate") }}
              </button>
              <label class="checkbox" :title="t('schemaWrite.normalizeHint')">
                <input v-model="registerNormalize" type="checkbox" :disabled="!canWrite" data-testid="register-normalize" />
                <span>{{ t("schemaWrite.normalize") }}</span>
              </label>
            </div>
            <p v-if="registerJsonError" class="form-error">{{ t("schemas.registerInvalidJson") }}: {{ registerJsonError }}</p>
          </div>
          <footer>
            <button type="button" @click="registerOpen = false">{{ t("cancel") }}</button>
            <button class="primary-button" type="button" :disabled="busy || !canWrite" :title="canWrite ? undefined : t('schemas.readOnlyHint')" @click="submitRegister">
              {{ t("schemas.register") }}
            </button>
          </footer>
        </div>
      </div>

      <div v-if="checkOpen" class="modal-backdrop" @click.self="checkOpen = false">
        <div class="modal panel-modal" ref="checkModalEl" tabindex="-1" role="dialog" aria-modal="true">
          <header>
            <h2>{{ t("schemas.compatCheckTitle") }}</h2>
            <button class="icon-button" :title="t('close')" @click="checkOpen = false"><X /></button>
          </header>
          <div class="settings-body">
            <label class="settings-field">
              <span>{{ t("schemas.compatCheckSubject") }}</span>
              <select v-model="checkSubject">
                <option value="">{{ t("acls.anyValue") }}</option>
                <option v-for="subject in subjects" :key="subject.subject" :value="subject.subject">{{ subject.subject }}</option>
              </select>
            </label>
            <div class="schema-cols">
              <label class="settings-field">
                <span>{{ t("schemas.compatCheckFormat") }}</span>
                <select v-model="checkFormat">
                  <option value="avro">avro</option>
                  <option value="json">json</option>
                </select>
              </label>
              <label class="settings-field">
                <span>{{ t("schemas.compatCheckVersion") }}</span>
                <input v-model="checkVersionText" type="number" min="1" />
              </label>
            </div>
            <label class="settings-field">
              <span>{{ t("schemas.compatCheckPaste") }}</span>
              <textarea v-model="checkSchemaText" rows="10" class="mono" spellcheck="false" />
            </label>
            <div v-if="checkResult" class="check-result">
              <span class="badge" :class="checkResult.isCompatible ? 'badge-ok' : 'badge-danger'">
                {{ checkResult.isCompatible ? t("schemas.compatCheckOk") : t("schemas.compatCheckFail") }}
              </span>
              <p v-if="checkResult.messages && checkResult.messages.length > 0" class="hint">
                {{ t("schemas.compatCheckMessages") }}: {{ checkResult.messages.join("; ") }}
              </p>
            </div>
          </div>
          <footer>
            <button type="button" @click="checkOpen = false">{{ t("close") }}</button>
            <button class="primary-button" type="button" :disabled="checkBusy" @click="submitCheck">{{ t("schemas.compatCheckRun") }}</button>
          </footer>
        </div>
      </div>

      <div v-if="deletePlan" class="modal-backdrop" @click.self="deletePlan = null">
        <div class="modal" ref="deleteModalEl" tabindex="-1" role="dialog" aria-modal="true">
          <header>
            <h2>{{ deletePlan.kind === "subject" ? t("schemas.deleteSubjectTitle", { subject: deletePlan.subject }) : t("schemas.deleteVersionTitle", { subject: deletePlan.subject, version: deletePlan.version ?? 0 }) }}</h2>
            <button class="icon-button" :title="t('close')" @click="deletePlan = null"><X /></button>
          </header>
          <div class="destructive-copy">
            <div class="destructive-icon"><Trash2 aria-hidden="true" /></div>
            <div>
              <strong class="mono">{{ deleteTargetName() }}</strong>
              <p>{{ deletePlan.kind === "subject" ? t("schemas.deleteSubjectMessage") : t("schemas.deleteVersionMessage") }}</p>
              <label v-if="deletePhase === 1" class="settings-field">
                <span>{{ t("schemas.deleteConfirmLabel", { name: deleteTargetName() }) }}</span>
                <input v-model="deleteConfirmText" type="text" class="mono" spellcheck="false" @keyup.enter="proceedDeletePhase" />
              </label>
              <p v-else class="form-error">{{ t("schemas.deleteConfirmLabel", { name: deleteTargetName() }) }}</p>
            </div>
          </div>
          <footer>
            <button type="button" @click="deletePlan = null">{{ t("cancel") }}</button>
            <button v-if="deletePhase === 1" type="button" :disabled="deleteConfirmText.trim() !== deleteTargetName()" @click="proceedDeletePhase">
              {{ t("confirm") }}
            </button>
            <button v-else class="danger-button" type="button" :disabled="deleteBusy || !canDelete" @click="submitDelete">{{ t("delete") }}</button>
          </footer>
        </div>
      </div>
    </teleport>
  </section>
</template>

<style scoped>
.schema-view { min-height: 120px; max-height: 240px; }
.schema-view-toggle { padding: 0 8px 4px; }
.schema-tree-box { overflow: auto; }
.schema-detail, .schema-diff { display: flex; min-height: 0; flex-direction: column; }
.diff-hunk {
  display: grid;
  grid-template-columns: 74px minmax(90px, 220px) minmax(0, 1fr) minmax(0, 1fr);
  gap: 6px;
  align-items: start;
  border-bottom: 1px solid color-mix(in srgb, var(--border) 45%, transparent);
  padding: 4px 8px;
}
.diff-path { overflow-wrap: anywhere; }
.diff-block { display: flex; min-width: 0; flex-direction: column; gap: 2px; }
.diff-lines {
  margin: 0;
  border-radius: 4px;
  border: 1px solid color-mix(in srgb, var(--border) 60%, transparent);
  padding: 4px 6px;
  overflow-wrap: anywhere;
  white-space: pre-wrap;
  font-family: var(--mono-font-family);
  font-size: 10.5px;
}
.diff-lines--before { background: color-mix(in srgb, var(--destructive) 10%, var(--background)); }
.diff-lines--after { background: color-mix(in srgb, var(--success) 10%, var(--background)); }
.check-result { display: flex; flex-direction: column; gap: 4px; }
@media (max-width: 900px) {
  .diff-hunk { grid-template-columns: 74px 1fr; }
}
/* P2 统一禁用态：只读下注册/应用按钮、兼容级别下拉补强。 */
button:disabled,
input:disabled,
select:disabled {
  cursor: not-allowed;
}
button:disabled {
  filter: grayscale(0.4);
}
</style>
