<script setup lang="ts">
import { computed, onMounted, onBeforeUnmount, ref, watch } from "vue";
import { RefreshCw } from "@lucide/vue";
import { kafkaApi, type ConnectAction, type ConnectCluster, type ConnectConnector, type ConnectDefinition, type ConnectOperationResult, type ConnectPlugin, type ConnectValidation } from "../lib/api";
import { useModalBehavior } from "../lib/modalBehavior";
import { t } from "../lib/i18n";

const props = defineProps<{ canWrite: boolean; canDelete: boolean; topicFilter?: string }>();
const emit = defineEmits<{
  (e: "error", message: string): void; (e: "notify", message: string): void;
  (e: "open-topic", topic: string): void; (e: "open-group", group: string): void; (e: "clear-filter"): void;
}>();
const clusters = ref<ConnectCluster[]>([]);
const cluster = ref("");
const connectors = ref<ConnectConnector[]>([]);
const selectedName = ref("");
const search = ref("");
const loading = ref(false);
const busy = ref(false);
const plugins = ref<ConnectPlugin[]>([]);
const unavailable = ref<string[]>([]);
const selected = computed(() => connectors.value.find(row => row.name === selectedName.value));
const visible = computed(() => connectors.value.filter(row => (!props.topicFilter || row.topics.includes(props.topicFilter)) && row.name.toLowerCase().includes(search.value.toLowerCase())));
const actions: ConnectAction[] = ["pause", "resume", "stop", "restart", "restart-all", "restart-failed"];
let loadSequence = 0;
let editorSequence = 0;
let disposed = false;
const editorOpen = ref(false);
const editorMode = ref<"create" | "edit">("create");
const editorName = ref("");
const configText = ref("{}");
const pluginClass = ref("");
const definitions = ref<ConnectDefinition[]>([]);
const validation = ref<ConnectValidation | null>(null);
const confirmation = ref<"delete" | "reset" | null>(null);
const confirmName = ref("");
const editorElement = ref<HTMLElement | null>(null);
const confirmElement = ref<HTMLElement | null>(null);
const confirmOpen = computed(() => confirmation.value !== null);
useModalBehavior({ open: editorOpen, container: editorElement, close: () => { if (!busy.value) editorOpen.value = false; } });
useModalBehavior({ open: confirmOpen, container: confirmElement, close: () => { if (!busy.value) confirmation.value = null; } });

function fail(cause: unknown) { if (!disposed) emit("error", cause instanceof Error ? cause.message : String(cause)); }
function parseConfig(): Record<string, string> {
  let value: unknown;
  try { value = JSON.parse(configText.value); } catch { throw new Error(t("connect.configObject")); }
  if (!value || typeof value !== "object" || Array.isArray(value) || Object.values(value).some(v => typeof v !== "string")) throw new Error(t("connect.configObject"));
  return value as Record<string, string>;
}
async function load() {
  if (!cluster.value) return;
  const sequence = ++loadSequence;
  const target = cluster.value;
  loading.value = true;
  emit("error", "");
  try {
    const result = await kafkaApi.connectList(target);
    if (sequence !== loadSequence || disposed) return;
    connectors.value = result.connectors ?? [];
    if (!connectors.value.some(row => row.name === selectedName.value)) selectedName.value = "";
  } catch (cause) { if (sequence === loadSequence) fail(cause); }
  finally { if (sequence === loadSequence) loading.value = false; }
}
watch(cluster, () => {
  connectors.value = []; selectedName.value = ""; plugins.value = []; unavailable.value = [];
  editorOpen.value = false; confirmation.value = null; editorSequence++;
  void load();
});
watch(editorOpen, open => { if (!open) { editorSequence++; configText.value = "{}"; validation.value = null; definitions.value = []; } });
watch(configText, () => { validation.value = null; });
async function testConnection() {
  busy.value = true;
  try { const result = await kafkaApi.connectTest(cluster.value); emit("notify", t("connect.connected", { version: result.version })); }
  catch (cause) { fail(cause); } finally { busy.value = false; }
}
async function operation(key: string, execute: () => Promise<ConnectOperationResult>) {
  busy.value = true;
  try {
    const result = await execute();
    if (!result.supported) { unavailable.value = [...unavailable.value, key]; emit("error", result.message ?? t("connect.unsupported")); return; }
    if (!result.success) throw new Error(result.message ?? t("connect.unsupported"));
    editorOpen.value = false; confirmation.value = null;
    emit("notify", t("connect.done"));
    await load();
  } catch (cause) { fail(cause); } finally { busy.value = false; }
}
async function runAction(action: ConnectAction) {
  const row = selected.value;
  if (!row || !props.canWrite || busy.value || unavailable.value.includes(action)) return;
  await operation(action, () => kafkaApi.connectAction(cluster.value, row.name, action));
}
async function restartTask(taskId: number) {
  const row = selected.value;
  if (row && props.canWrite && !busy.value) await operation("task", () => kafkaApi.connectTaskRestart(cluster.value, row.name, taskId));
}
async function loadDefinitions() {
  if (!pluginClass.value) return;
  const sequence = ++editorSequence;
  try {
    const result = await kafkaApi.connectDefinitions(cluster.value, pluginClass.value);
    if (editorOpen.value && sequence === editorSequence) definitions.value = result.definitions ?? [];
  } catch (cause) { if (sequence === editorSequence) fail(cause); }
}
async function openEditor(mode: "create" | "edit") {
  editorMode.value = mode;
  editorName.value = mode === "edit" ? selectedName.value : "";
  configText.value = "{}"; pluginClass.value = ""; validation.value = null;
  editorOpen.value = true;
  const sequence = ++editorSequence;
  busy.value = true;
  try {
    if (mode === "edit") {
      const result = await kafkaApi.connectConfig(cluster.value, editorName.value);
      if (sequence !== editorSequence || disposed) return;
      configText.value = JSON.stringify(result.config, null, 2);
      pluginClass.value = result.config["connector.class"] ?? "";
    }
    const result = await kafkaApi.connectPlugins(cluster.value);
    if (sequence !== editorSequence || disposed) return;
    plugins.value = result.plugins ?? [];
  } catch (cause) { if (sequence === editorSequence) fail(cause); }
  finally { busy.value = false; }
  if (sequence === editorSequence) await loadDefinitions();
}
function selectPlugin() {
  try { const config = parseConfig(); config["connector.class"] = pluginClass.value; configText.value = JSON.stringify(config, null, 2); void loadDefinitions(); }
  catch (cause) { fail(cause); }
}
async function validate() {
  busy.value = true;
  try { const config = parseConfig(); config.name = editorName.value; validation.value = await kafkaApi.connectValidate(cluster.value, config["connector.class"] ?? pluginClass.value, config, editorMode.value === "edit" ? editorName.value : undefined); }
  catch (cause) { fail(cause); } finally { busy.value = false; }
}
async function save() {
  if (!props.canWrite || busy.value || !editorName.value.trim()) return;
  try {
    const config = parseConfig();
    if (editorMode.value === "create") await operation("create", () => kafkaApi.connectCreate(cluster.value, editorName.value, config));
    else await operation("config", () => kafkaApi.connectSave(cluster.value, editorName.value, config));
  } catch (cause) { fail(cause); }
}
function ask(kind: "delete" | "reset") { confirmName.value = ""; confirmation.value = kind; }
async function confirm() {
  const row = selected.value;
  if (!row || !props.canWrite || !props.canDelete || confirmName.value !== row.name || busy.value) return;
  if (confirmation.value === "delete") await operation("delete", () => kafkaApi.connectDelete(cluster.value, row.name, confirmName.value));
  else await operation("reset", () => kafkaApi.connectResetOffsets(cluster.value, row.name, confirmName.value));
}
onMounted(async () => {
  try { const result = await kafkaApi.connectClusters(); if (!disposed) { clusters.value = result.clusters ?? []; cluster.value = clusters.value[0]?.name ?? ""; } }
  catch (cause) { fail(cause); }
});
onBeforeUnmount(() => { disposed = true; loadSequence++; editorSequence++; configText.value = "{}"; });
</script>

<template>
  <section class="section-block panel-fill connect-panel">
    <div class="result-meta">
      <label>{{ t('connect.cluster') }} <select v-model="cluster" :disabled="busy" :aria-label="t('connect.cluster')"><option v-for="item in clusters" :key="item.name" :value="item.name">{{ item.name }}</option></select></label>
      <span class="inline-actions">
        <button type="button" :disabled="!cluster || busy" @click="testConnection">{{ t('connect.test') }}</button>
        <button type="button" data-action="create" :disabled="!cluster || !canWrite || busy" @click="openEditor('create')">{{ t('connect.create') }}</button>
        <button type="button" class="icon-button" :title="t('refresh')" :disabled="!cluster || busy || loading" @click="load"><RefreshCw :class="{ spinning: loading }" /></button>
      </span>
    </div>
    <p v-if="clusters.length === 0" class="empty">{{ t('connect.setup') }}</p>
    <template v-else>
      <div class="result-meta">
        <input v-model="search" :placeholder="t('connect.search')" :aria-label="t('connect.search')" />
        <span v-if="topicFilter">{{ t('connect.topicFilter', { topic: topicFilter }) }} <button type="button" @click="emit('clear-filter')">{{ t('connect.allTopics') }}</button></span>
        <span role="status">{{ loading ? t('tree.loading') : visible.length }}</span>
      </div>
      <div class="connect-scroll">
        <table class="connect-table">
          <thead><tr><th>{{ t('connect.name') }}</th><th>{{ t('connect.type') }}</th><th>{{ t('connect.state') }}</th><th>{{ t('connect.worker') }}</th><th>{{ t('connect.tasks') }}</th></tr></thead>
          <tbody>
            <tr v-for="row in visible" :key="row.name" :class="{ 'is-selected': selectedName === row.name }">
              <td><button type="button" class="connector-name" @click="selectedName = row.name">{{ row.name }}</button><p v-if="row.error" role="status">{{ row.error }}</p></td>
              <td>{{ row.type || '—' }}</td><td><span class="badge" :class="{ 'badge-warn': row.state === 'FAILED' }">{{ row.state || '—' }}</span></td><td>{{ row.workerId || '—' }}</td><td>{{ row.tasks.length }}</td>
            </tr>
            <tr v-if="!loading && !visible.length"><td colspan="5" class="empty">{{ t('connect.empty') }}</td></tr>
          </tbody>
        </table>
      </div>
      <div v-if="selected" class="connect-details">
        <h3>{{ selected.name }}</h3>
        <div class="inline-actions connect-actions">
          <button type="button" data-action="config" :disabled="busy" @click="openEditor('edit')">{{ t('connect.config') }}</button>
          <button v-for="action in actions" :key="action" type="button" :data-action="action" :disabled="!canWrite || busy || unavailable.includes(action)" :title="unavailable.includes(action) ? t('connect.unsupported') : !canWrite ? t('readOnly') : ''" @click="runAction(action)">{{ t(`connect.${action}`) }}</button>
          <button type="button" data-action="reset" :disabled="!canWrite || !canDelete || busy || selected.state !== 'STOPPED' || unavailable.includes('reset')" @click="ask('reset')">{{ t('connect.reset') }}</button>
          <button type="button" data-action="delete" :disabled="!canWrite || !canDelete || busy" @click="ask('delete')">{{ t('connect.delete') }}</button>
        </div>
        <p v-if="selected.trace">{{ t('connect.errorClass') }}: <code>{{ selected.trace }}</code></p>
        <p>{{ t('connect.topic') }}: <button v-for="topic in selected.topics" :key="topic" type="button" @click="emit('open-topic', topic)">{{ topic }}</button></p>
        <p v-if="selected.consumerGroup">{{ t('connect.group') }}: <button type="button" @click="emit('open-group', selected.consumerGroup)">{{ selected.consumerGroup }}</button></p>
        <table class="connect-table"><thead><tr><th>ID</th><th>{{ t('connect.state') }}</th><th>{{ t('connect.worker') }}</th><th>{{ t('connect.errorClass') }}</th><th /></tr></thead><tbody>
          <tr v-for="task in selected.tasks" :key="task.id"><td>{{ task.id }}</td><td>{{ task.state }}</td><td>{{ task.workerId }}</td><td>{{ task.trace || '—' }}</td><td><button type="button" :disabled="!canWrite || busy || unavailable.includes('task')" @click="restartTask(task.id)">{{ t('connect.restartTask') }}</button></td></tr>
        </tbody></table>
      </div>
    </template>
    <teleport to="body">
      <div v-if="editorOpen" class="modal-backdrop" @click.self="!busy && (editorOpen = false)">
        <div ref="editorElement" class="modal panel-modal connect-editor" role="dialog" aria-modal="true" :aria-label="t('connect.config')" tabindex="-1">
          <header><h2>{{ editorMode === 'create' ? t('connect.create') : editorName }}</h2><button type="button" :disabled="busy" @click="editorOpen = false">{{ t('close') }}</button></header>
          <div class="settings-body">
            <label>{{ t('connect.name') }}<input v-model="editorName" :disabled="editorMode === 'edit' || busy" /></label>
            <label>{{ t('connect.plugin') }}<select v-model="pluginClass" :disabled="busy || !canWrite" @change="selectPlugin"><option value="">—</option><option v-for="plugin in plugins" :key="plugin.class" :value="plugin.class">{{ plugin.class }} ({{ plugin.type }} {{ plugin.version }})</option></select></label>
            <p>{{ t('connect.secretHint') }}</p>
            <textarea v-model="configText" :readonly="!canWrite" :disabled="busy" spellcheck="false" :aria-label="t('connect.config')" rows="14" />
            <p v-if="validation" role="status">{{ validation.errorCount ? t('connect.errors', { count: validation.errorCount }) : t('connect.valid') }}</p>
            <template v-if="validation?.errorCount"><p>{{ t('connect.validationNote') }}</p><ul><li v-for="field in validation.fields.filter(field => field.errorCount)" :key="field.name">{{ field.name }}: {{ field.errorCount }}</li></ul></template>
            <details v-if="definitions.length"><summary>{{ t('connect.plugin') }} · {{ definitions.length }}</summary><table class="connect-table"><tbody><tr v-for="definition in definitions" :key="definition.name"><th>{{ definition.name }}</th><td>{{ definition.type }} {{ definition.required ? t('connect.required') : '' }}</td><td>{{ definition.documentation }}</td></tr></tbody></table></details>
          </div>
          <footer><button type="button" :disabled="busy" @click="validate">{{ t('connect.validate') }}</button><button type="button" data-action="save" :disabled="!canWrite || busy || !editorName.trim() || !!validation?.errorCount" @click="save">{{ t('save') }}</button><button type="button" :disabled="busy" @click="editorOpen = false">{{ t('cancel') }}</button></footer>
        </div>
      </div>
      <div v-if="confirmation && selected" class="modal-backdrop" @click.self="!busy && (confirmation = null)">
        <div ref="confirmElement" class="modal panel-modal" role="dialog" aria-modal="true" :aria-label="t('connect.confirmAction')" tabindex="-1">
          <header><h2>{{ t(`connect.${confirmation}`) }} · {{ selected.name }}</h2></header>
          <div class="settings-body"><p v-if="confirmation === 'reset'">{{ t('connect.resetHint') }}</p><label>{{ t('connect.confirmName') }}<input v-model="confirmName" :disabled="busy" autocomplete="off" /></label></div>
          <footer><button type="button" :disabled="busy" @click="confirmation = null">{{ t('cancel') }}</button><button type="button" data-action="confirm" :disabled="!canWrite || !canDelete || busy || confirmName !== selected.name" @click="confirm">{{ t('confirm') }}</button></footer>
        </div>
      </div>
    </teleport>
  </section>
</template>

<style scoped>
.connect-panel { gap: 10px; overflow: auto; }
.connect-scroll { min-height: 100px; max-height: 40vh; overflow: auto; flex-shrink: 0; }
.connect-table { width: 100%; border-collapse: collapse; font-size: 12px; }
.connect-table th, .connect-table td { padding: 8px; text-align: left; border-bottom: 1px solid var(--border); overflow-wrap: anywhere; }
.connect-table th { color: var(--muted-foreground); font-weight: 600; }
.is-selected { background: color-mix(in srgb, var(--primary) 12%, transparent); }
.connector-name { text-align: left; font-family: monospace; }
.connect-actions { flex-wrap: wrap; gap: 6px; }
.connect-details { border-top: 1px solid var(--border); padding-top: 10px; }
.connect-details p button { margin: 2px 4px; }
.connect-editor { width: min(900px, 95vw); }
.connect-editor .settings-body { display: flex; flex-direction: column; gap: 10px; }
.connect-editor textarea { width: 100%; resize: vertical; min-height: 200px; font-family: monospace; }
label { display: flex; align-items: center; gap: 8px; }
select { max-width: 100%; }
button:not(.icon-button) { border: 1px solid var(--border); border-radius: 4px; background: var(--background); color: var(--foreground); padding: 5px 8px; font: inherit; }
button:not(:disabled):hover { background: color-mix(in srgb, var(--primary) 12%, var(--background)); }
button:disabled { cursor: not-allowed; opacity: .45; }
input, select, textarea { border: 1px solid var(--border); border-radius: 4px; background: var(--background); color: var(--foreground); padding: 5px 8px; font: inherit; }
.connector-name { border: 0 !important; background: transparent !important; padding: 0 !important; }
button:focus-visible, input:focus-visible, select:focus-visible, textarea:focus-visible { outline: 2px solid var(--primary); outline-offset: 2px; }
</style>
