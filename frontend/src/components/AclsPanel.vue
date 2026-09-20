<script setup lang="ts">
// ACL 面板：list（过滤条件至少一项具体值，过宽被后端拒绝，前端预检）/
// create / delete（按过滤条件删除，回显 matched 数）。
// 枚举值（资源类型/操作/许可）为 Kafka 协议术语，保持原文不翻译。
import { computed, onMounted, ref } from "vue";
import { Plus, RefreshCw, Trash2, X } from "@lucide/vue";
import type { ColDef } from "ag-grid-community";
import DbxAgGrid from "./DbxAgGrid.vue";
import { kafkaApi, type AclFilter, type KafkaAcl } from "../lib/api";
import { MINIMAL_ACL_FIELDS, aclColumns, toAclRows, type AclVm } from "../lib/kafkaColumns";
import { useModalBehavior } from "../lib/modalBehavior";
import { t } from "../lib/i18n";

const props = defineProps<{
  canWrite: boolean;
  canDelete: boolean;
}>();

const emit = defineEmits<{
  (e: "error", message: string): void;
  (e: "notify", message: string): void;
}>();

const RESOURCE_TYPES = ["ANY", "TOPIC", "GROUP", "CLUSTER", "TRANSACTIONAL_ID", "DELEGATION_TOKEN"];
const PATTERN_TYPES = ["ANY", "MATCH", "LITERAL", "PREFIXED"];
const OPERATIONS = ["ANY", "ALL", "READ", "WRITE", "CREATE", "DELETE", "ALTER", "DESCRIBE", "CLUSTER_ACTION", "DESCRIBE_CONFIGS", "ALTER_CONFIGS", "IDEMPOTENT_WRITE"];
const PERMISSIONS = ["ANY", "ALLOW", "DENY"];

const acls = ref<KafkaAcl[]>([]);
const loading = ref(false);
const busy = ref(false);
const detail = ref<AclVm | null>(null);

const aclGridRows = computed(() => toAclRows(acls.value));
const aclGridCols = computed(() => aclColumns() as ColDef<AclVm>[]);

const filter = ref<AclFilter>({});
const filterLocalError = ref("");

const createOpen = ref(false);
const createForm = ref<KafkaAcl>({ resourceType: "TOPIC", resourceName: "", principal: "", host: "*", operation: "READ", permission: "ALLOW", patternType: "LITERAL" });

const deleteOpen = ref(false);

// 弹层行为统一接入（UI 扫描第 2 轮 P1-5）：详情抽屉 / 创建 / 删除弹窗均支持
// Esc 关闭 + Tab 焦点陷阱 + 关闭归还触发元素（决策逻辑 lib/modalBehavior）。
const detailEl = ref<HTMLElement | null>(null);
const createModalEl = ref<HTMLElement | null>(null);
const deleteModalEl = ref<HTMLElement | null>(null);
useModalBehavior({ open: computed(() => detail.value !== null), container: detailEl, close: () => (detail.value = null) });
useModalBehavior({ open: createOpen, container: createModalEl, close: () => (createOpen.value = false) });
useModalBehavior({ open: deleteOpen, container: deleteModalEl, close: () => (deleteOpen.value = false) });

const hasConcreteFilter = computed(() => {
  const candidate = filter.value;
  return Boolean(candidate.resourceName?.trim() || candidate.principal?.trim());
});

async function load() {
  filterLocalError.value = "";
  if (!hasConcreteFilter.value) {
    filterLocalError.value = t("acls.filterTooBroad");
    acls.value = [];
    return;
  }
  loading.value = true;
  emit("error", "");
  try {
    const response = await kafkaApi.aclsList(cleanFilter(filter.value));
    acls.value = response.acls ?? [];
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  } finally {
    loading.value = false;
  }
}

function cleanFilter(input: AclFilter): AclFilter {
  const cleaned: AclFilter = {};
  for (const [key, value] of Object.entries(input)) {
    const text = typeof value === "string" ? value.trim() : "";
    if (text) (cleaned as Record<string, string>)[key] = text;
  }
  return cleaned;
}

async function submitCreate() {
  const acl = { ...createForm.value, resourceName: createForm.value.resourceName.trim(), principal: createForm.value.principal.trim() };
  if (!acl.resourceName || !acl.principal) {
    // P2-16：ACL 校验不再复用 topic 专属文案（分区数/副本因子）。
    emit("error", t("acls.createInvalid"));
    return;
  }
  busy.value = true;
  try {
    await kafkaApi.aclsCreate(acl);
    createOpen.value = false;
    emit("notify", t("acls.created"));
    filter.value = { resourceType: acl.resourceType, resourceName: acl.resourceName, patternType: acl.patternType, principal: acl.principal };
    await load();
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  } finally {
    busy.value = false;
  }
}

async function submitDelete() {
  busy.value = true;
  try {
    const response = await kafkaApi.aclsDelete(cleanFilter(filter.value));
    emit("notify", t("acls.deleted", { count: response.matched ?? 0 }));
    deleteOpen.value = false;
    await load();
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  } finally {
    busy.value = false;
  }
}

onMounted(() => {
  filter.value = { resourceType: "ANY", resourceName: "", principal: "" };
});
</script>

<template>
  <section class="section-block panel-fill">
    <p class="subpanel-title">{{ t("acls.filterTitle") }}</p>
    <div class="kafka-form">
      <label class="field">
        <span>{{ t("acls.resourceType") }}</span>
        <select v-model="filter.resourceType">
          <option v-for="value in RESOURCE_TYPES" :key="value" :value="value">{{ value }}</option>
        </select>
      </label>
      <label class="field">
        <span>{{ t("acls.resourceName") }}</span>
        <input v-model="filter.resourceName" type="text" spellcheck="false" />
      </label>
      <label class="field">
        <span>{{ t("acls.patternType") }}</span>
        <select v-model="filter.patternType">
          <option :value="undefined">{{ t("acls.anyValue") }}</option>
          <option v-for="value in PATTERN_TYPES" :key="value" :value="value">{{ value }}</option>
        </select>
      </label>
      <label class="field">
        <span>{{ t("acls.principal") }}</span>
        <input v-model="filter.principal" type="text" spellcheck="false" />
      </label>
      <label class="field">
        <span>{{ t("acls.operation") }}</span>
        <select v-model="filter.operation">
          <option :value="undefined">{{ t("acls.anyValue") }}</option>
          <option v-for="value in OPERATIONS" :key="value" :value="value">{{ value }}</option>
        </select>
      </label>
      <label class="field">
        <span>{{ t("acls.permission") }}</span>
        <select v-model="filter.permission">
          <option :value="undefined">{{ t("acls.anyValue") }}</option>
          <option v-for="value in PERMISSIONS" :key="value" :value="value">{{ value }}</option>
        </select>
      </label>
      <button class="primary-button compact" type="button" :disabled="loading" @click="load">
        <RefreshCw :class="{ spinning: loading }" aria-hidden="true" />{{ t("acls.filterRun") }}
      </button>
      <button class="toolbar-button" type="button" :disabled="!canWrite" :title="canWrite ? t('acls.create') : t('readOnly')" @click="createOpen = true">
        <Plus aria-hidden="true" /><span>{{ t("acls.create") }}</span>
      </button>
      <button class="danger-button compact" type="button" :disabled="!canDelete || acls.length === 0" :title="canDelete ? t('acls.delete') : t('noDelete')" @click="deleteOpen = true">
        <Trash2 aria-hidden="true" /><span>{{ t("acls.delete") }}</span>
      </button>
    </div>
    <p v-if="filterLocalError" class="form-error" style="padding: 0 8px 4px">{{ filterLocalError }}</p>

    <div class="grid-box grid-box--fill">
      <!-- P2-9：空态不再裸列一行灰字，附过滤引导（复用现有 key，不新增 i18n）。 -->
      <!-- 过宽被拒（filterLocalError）时只留拒绝提示，不再叠加「没有匹配」空态。 -->
      <div v-if="acls.length === 0 && !loading && !filterLocalError" class="acls-empty">
        <p class="empty compact">{{ t("acls.empty") }}</p>
        <p class="hint">{{ t("acls.resourceName") }} / {{ t("acls.principal") }} → {{ t("acls.filterRun") }}</p>
      </div>
      <DbxAgGrid
        v-else
        table-key="acls"
        :row-data="aclGridRows"
        :column-defs="aclGridCols"
        :compact-fields="MINIMAL_ACL_FIELDS"
        row-selection="single"
        @row-click="(row: unknown) => (detail = row as AclVm)"
      />
    </div>

    <teleport to="body">
      <div v-if="detail" class="drawer-backdrop" @click="detail = null" />
      <div v-if="detail" class="drawer" ref="detailEl" tabindex="-1" role="dialog" aria-modal="true">
        <header>
          <span class="mono">{{ detail.resourceType }} · {{ detail.resourceName }}</span>
          <button class="icon-button" :title="t('close')" @click="detail = null"><X /></button>
        </header>
        <div class="drawer-body">
          <dl class="kv-grid">
            <dt>{{ t("acls.resourceType") }}</dt>
            <dd>{{ detail.resourceType }}</dd>
            <dt>{{ t("acls.resourceName") }}</dt>
            <dd>{{ detail.resourceName }}</dd>
            <dt>{{ t("acls.patternType") }}</dt>
            <dd>{{ detail.patternType }}</dd>
            <dt>{{ t("acls.principal") }}</dt>
            <dd>{{ detail.principal }}</dd>
            <dt>{{ t("acls.host") }}</dt>
            <dd>{{ detail.host }}</dd>
            <dt>{{ t("acls.operation") }}</dt>
            <dd>{{ detail.operation }}</dd>
            <dt>{{ t("acls.permission") }}</dt>
            <dd>{{ detail.permission }}</dd>
          </dl>
        </div>
      </div>
    </teleport>

    <teleport to="body">
      <div v-if="createOpen" class="modal-backdrop" @click.self="createOpen = false">
        <div class="modal" ref="createModalEl" tabindex="-1" role="dialog" aria-modal="true">
          <header>
            <h2>{{ t("acls.createTitle") }}</h2>
            <button class="icon-button" :title="t('close')" @click="createOpen = false">✕</button>
          </header>
          <div class="settings-body">
            <label class="settings-field">
              <span>{{ t("acls.resourceType") }}</span>
              <select v-model="createForm.resourceType">
                <option v-for="value in RESOURCE_TYPES.filter((v) => v !== 'ANY')" :key="value" :value="value">{{ value }}</option>
              </select>
            </label>
            <label class="settings-field">
              <span>{{ t("acls.resourceName") }}</span>
              <input v-model="createForm.resourceName" type="text" spellcheck="false" />
            </label>
            <label class="settings-field">
              <span>{{ t("acls.patternType") }}</span>
              <select v-model="createForm.patternType">
                <option v-for="value in PATTERN_TYPES.filter((v) => v !== 'ANY')" :key="value" :value="value">{{ value }}</option>
              </select>
            </label>
            <label class="settings-field">
              <span>{{ t("acls.principal") }}</span>
              <input v-model="createForm.principal" type="text" placeholder="User:app" spellcheck="false" />
            </label>
            <label class="settings-field">
              <span>{{ t("acls.host") }}</span>
              <input v-model="createForm.host" type="text" placeholder="*" spellcheck="false" />
            </label>
            <label class="settings-field">
              <span>{{ t("acls.operation") }}</span>
              <select v-model="createForm.operation">
                <option v-for="value in OPERATIONS.filter((v) => v !== 'ANY')" :key="value" :value="value">{{ value }}</option>
              </select>
            </label>
            <label class="settings-field">
              <span>{{ t("acls.permission") }}</span>
              <select v-model="createForm.permission">
                <option v-for="value in PERMISSIONS.filter((v) => v !== 'ANY')" :key="value" :value="value">{{ value }}</option>
              </select>
            </label>
          </div>
          <footer>
            <button type="button" @click="createOpen = false">{{ t("cancel") }}</button>
            <button class="primary-button" type="button" :disabled="busy" @click="submitCreate">{{ t("save") }}</button>
          </footer>
        </div>
      </div>

      <div v-if="deleteOpen" class="modal-backdrop" @click.self="deleteOpen = false">
        <div class="modal" ref="deleteModalEl" tabindex="-1" role="dialog" aria-modal="true">
          <header>
            <h2>{{ t("acls.deleteTitle") }}</h2>
            <button class="icon-button" :title="t('close')" @click="deleteOpen = false">✕</button>
          </header>
          <div class="destructive-copy">
            <div class="destructive-icon"><Trash2 aria-hidden="true" /></div>
            <div>
              <strong>{{ t("acls.deleteTitle") }}</strong>
              <p class="mono-s">{{ JSON.stringify(cleanFilter(filter)) }}</p>
            </div>
          </div>
          <footer>
            <button type="button" @click="deleteOpen = false">{{ t("cancel") }}</button>
            <button class="danger-button" type="button" :disabled="busy" @click="submitDelete">{{ t("delete") }}</button>
          </footer>
        </div>
      </div>
    </teleport>
  </section>
</template>

<style scoped>
/* P2-9：空态引导行居中、弱化。 */
.acls-empty {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 4px;
  height: 100%;
}
/* P2 统一禁用态：只读/禁删下创建、删除按钮补强。 */
button:disabled,
input:disabled,
select:disabled {
  cursor: not-allowed;
}
button:disabled {
  filter: grayscale(0.4);
}
</style>
