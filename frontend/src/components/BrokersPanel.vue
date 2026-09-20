<script setup lang="ts">
// Broker 面板：列表（nodeId/host/port/rack）+ 单 broker config 查看
// （sensitive 条目掩码展示，与后端"凭据不回显"策略一致）。
import { onMounted, ref } from "vue";
import { RefreshCw, Wrench } from "@lucide/vue";
import { kafkaApi, type ConfigEntry, type KafkaBroker } from "../lib/api";
import { useModalBehavior } from "../lib/modalBehavior";
import { t } from "../lib/i18n";

const emit = defineEmits<{
  (e: "error", message: string): void;
}>();

const brokers = ref<KafkaBroker[]>([]);
const loading = ref(false);
const configOpen = ref(false);
const configBroker = ref<KafkaBroker | null>(null);
const entries = ref<ConfigEntry[]>([]);

// 弹层行为统一接入（UI 扫描第 2 轮 P1-5）：broker 配置弹窗支持 Esc 关闭 +
// Tab 焦点陷阱 + 关闭归还触发元素（决策逻辑 lib/modalBehavior）。
const configModalEl = ref<HTMLElement | null>(null);
useModalBehavior({ open: configOpen, container: configModalEl, close: () => (configOpen.value = false) });

async function load() {
  loading.value = true;
  emit("error", "");
  try {
    const response = await kafkaApi.brokersList();
    brokers.value = response.brokers ?? [];
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  } finally {
    loading.value = false;
  }
}

async function openConfig(broker: KafkaBroker) {
  configBroker.value = broker;
  entries.value = [];
  configOpen.value = true;
  try {
    const response = await kafkaApi.brokersConfig(broker.nodeId);
    entries.value = response.entries ?? [];
  } catch (cause) {
    emit("error", cause instanceof Error ? cause.message : String(cause));
  }
}

onMounted(() => {
  void load();
});
</script>

<template>
  <section class="section-block panel-fill">
    <div class="result-meta">
      <span>{{ t("brokers.title") }} · {{ brokers.length }}</span>
      <span class="inline-actions">
        <button class="icon-button" :title="t('brokers.refresh')" @click="load"><RefreshCw :class="{ spinning: loading }" /></button>
      </span>
    </div>
    <div class="kafka-table">
      <div class="kafka-table-header broker-cols">
        <span>{{ t("brokers.colNode") }}</span>
        <span>{{ t("brokers.colHost") }}</span>
        <span>{{ t("brokers.colPort") }}</span>
        <span>{{ t("brokers.colRack") }}</span>
        <span />
      </div>
      <div class="kafka-table-rows">
        <p v-if="brokers.length === 0 && !loading" class="empty compact">{{ t("brokers.empty") }}</p>
        <div v-for="broker in brokers" :key="broker.nodeId" class="kafka-table-row broker-cols" style="cursor: default">
          <span class="mono-s">{{ broker.nodeId }}</span>
          <span class="mono-s">{{ broker.host }}</span>
          <span class="mono-s">{{ broker.port }}</span>
          <span class="mono-s">{{ broker.rack || "—" }}</span>
          <span class="inline-actions">
            <button class="qb-add" type="button" @click="openConfig(broker)"><Wrench aria-hidden="true" /> {{ t("brokers.config") }}</button>
          </span>
        </div>
      </div>
    </div>

    <teleport to="body">
      <div v-if="configOpen" class="modal-backdrop" @click.self="configOpen = false">
        <div class="modal panel-modal" ref="configModalEl" tabindex="-1" role="dialog" aria-modal="true">
          <header>
            <h2>{{ t("brokers.configTitle", { id: configBroker?.nodeId ?? "" }) }}</h2>
            <button class="icon-button" :title="t('close')" @click="configOpen = false">✕</button>
          </header>
          <div class="settings-body">
            <table class="config-table">
              <thead>
                <tr>
                  <th>{{ t("brokers.colName") }}</th>
                  <th>{{ t("brokers.colValue") }}</th>
                  <th>{{ t("brokers.colSource") }}</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="entry in entries" :key="entry.name">
                  <td class="mono-s">{{ entry.name }}</td>
                  <td class="mono-s">{{ entry.sensitive ? t("brokers.sensitiveMasked") : entry.value }}</td>
                  <td>
                    <span class="inline-actions">
                      <span>{{ entry.source || "—" }}</span>
                      <span v-if="entry.sensitive" class="badge badge-warn">{{ t("brokers.colSensitive") }}</span>
                      <span v-if="entry.isDefault" class="badge">{{ t("brokers.colDefault") }}</span>
                    </span>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
          <footer>
            <button type="button" @click="configOpen = false">{{ t("close") }}</button>
          </footer>
        </div>
      </div>
    </teleport>
  </section>
</template>

<style scoped>
.broker-cols {
  grid-template-columns: 60px minmax(140px, 2fr) 80px minmax(80px, 1fr) auto;
}
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
/* P2 统一禁用态（该面板按钮均可用，规则兜底保持一致）。 */
button:disabled,
input:disabled,
select:disabled {
  cursor: not-allowed;
}
</style>
