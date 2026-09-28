<script setup lang="ts">
import { ChevronDown, Copy, Download, X } from "@lucide/vue";
import CodeEditor from "./CodeEditor.vue";
import type { KafkaMessage } from "../lib/api";
import { workbenchTimestampTz } from "../lib/kafkaColumns";
import { messageFullValueText } from "../lib/messageCodec";
import { serializeMessagesToJson } from "../lib/messageExport";
import { saveTextFile } from "../lib/download";
import { formatTimestamp, timestampIso } from "../lib/timestamps";
import { copyTextToClipboard } from "../lib/uiHelpers";
import { useMessageDetailDrawer } from "../composables/useMessageDetailDrawer";
import { t } from "../lib/i18n";
const model = defineModel<KafkaMessage | null>({ required: true });
const emit = defineEmits<{ (e: "error", message: string): void; (e: "notify", message: string): void }>();

const {
  detail,
  viewFormat,
  viewDecode,
  viewDecompression,
  viewResult,
  viewBusy,
  showFullBase64,
  fullBase64Preview,
  headersView,
  sectionsOpen,
  headersEntries,
  headersJsonText,
  toggleSection,
  renderView,
  drawerEl,
} = useMessageDetailDrawer(model);
async function copyWithNotify(text: string) {
  const ok = await copyTextToClipboard(text);
  if (ok) emit("notify", t("copied"));
  else emit("error", t("messages.copyFailed"));
}

function messageJsonText(message: KafkaMessage): string {
  return serializeMessagesToJson([message]);
}

function copyDetail(part: "key" | "value" | "headers" | "json") {
  const message = detail.value;
  if (!message) return;
  if (part === "key") void copyWithNotify(message.key ?? "");
  else if (part === "value") void copyWithNotify(messageFullValueText(message));
  else if (part === "headers") void copyWithNotify(message.headers ? JSON.stringify(message.headers) : "");
  else void copyWithNotify(messageJsonText(message));
}

async function downloadValue() {
  const message = detail.value;
  if (!message) return;
  const outcome = await saveTextFile(`${message.topic}-p${message.partition}-o${message.offset}.txt`, "text/plain", messageFullValueText(message));
  if (outcome.mode !== "canceled") emit("notify", t("messages.exportDone", { name: "TXT" }));
}


</script>
<template>
    <teleport to="body">
      <div v-if="detail" class="drawer-backdrop" @click="detail = null" />
      <div v-if="detail" class="drawer" ref="drawerEl" tabindex="-1" role="dialog" aria-modal="true">
        <header>
          <span class="mono">{{ detail.topic }} · {{ t("messages.colPartition") }} {{ detail.partition }} · {{ t("messages.colOffset") }} {{ detail.offset }}</span>
          <span class="drawer-head-actions">
            <button class="icon-button" :title="t('messages.copyJson')" data-testid="copy-json" @click="copyDetail('json')"><Copy /></button>
            <button class="icon-button" :title="t('close')" @click="detail = null"><X /></button>
          </span>
        </header>
        <div class="drawer-body">
          <p v-if="detail.truncated" class="form-error" role="status">{{ t("messages.truncated") }}</p>
          <dl class="kv-grid">
            <dt>{{ t("messages.colTimestamp") }}</dt>
            <dd :title="timestampIso(detail.timestamp)">{{ formatTimestamp(detail.timestamp, workbenchTimestampTz) }}</dd>
            <dt>{{ t("messages.colKey") }}</dt>
            <dd class="kv-dd-inline">
              <span class="kv-dd-text" :title="detail.key ?? undefined">{{ detail.key ?? "—" }}</span>
              <button v-if="detail.key" class="icon-button icon-button--inline" type="button" :title="t('messages.copyKey')" data-testid="copy-key" @click="copyDetail('key')"><Copy /></button>
            </dd>
            <dt v-if="detail.schemaSubject">{{ t("messages.colSchema") }}</dt>
            <dd v-if="detail.schemaSubject" class="mono">{{ detail.schemaSubject }} v{{ detail.schemaVersion ?? "?" }} (id {{ detail.schemaId ?? "—" }})</dd>
            <dt v-if="detail.decodeError">{{ t("messages.decodeError") }}</dt>
            <dd v-if="detail.decodeError" class="form-error">{{ detail.decodeError }}</dd>
          </dl>

          <!-- Headers：可折叠区块（标题行右侧直接挂切换/复制操作，省一行高度）；
               表格（key|value + 行复制，限高滚动）⇄ 格式化 JSON -->
          <section class="detail-block">
            <div class="detail-block__head">
              <button class="detail-block__toggle" type="button" :aria-expanded="sectionsOpen.headers" @click="toggleSection('headers')">
                <ChevronDown class="chev" :class="{ folded: !sectionsOpen.headers }" aria-hidden="true" />
                <span class="detail-block__title">{{ t("messages.colHeaders") }} · {{ headersEntries.length }}</span>
              </button>
              <span v-if="sectionsOpen.headers && headersEntries.length > 0" class="detail-block__actions">
                <button class="seg-toggle" type="button" :class="{ 'is-active': headersView === 'table' }" @click="headersView = 'table'">{{ t("messages.headersViewTable") }}</button>
                <button class="seg-toggle" type="button" :class="{ 'is-active': headersView === 'json' }" @click="headersView = 'json'">{{ t("messages.headersViewJson") }}</button>
                <button class="icon-button" type="button" :title="t('messages.copyHeaders')" data-testid="copy-headers" @click="copyDetail('headers')"><Copy /></button>
              </span>
            </div>
            <div v-show="sectionsOpen.headers" class="detail-block__body">
              <div v-if="headersView === 'table' && headersEntries.length > 0" class="kv-scroll">
                <table class="kv-table">
                  <thead>
                    <tr>
                      <th>{{ t("messages.colKey") }}</th>
                      <th>Value</th>
                      <th class="kv-table__action-col" aria-hidden="true"></th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr v-for="[headerKey, headerValue] in headersEntries" :key="headerKey">
                      <td class="mono">{{ headerKey }}</td>
                      <td class="kv-table__value" :title="headerValue">{{ headerValue }}</td>
                      <td class="kv-table__action-col">
                        <button class="icon-button icon-button--inline" type="button" :title="t('messages.copyHeaderValue')" @click="copyWithNotify(headerValue)"><Copy /></button>
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
              <pre v-else-if="headersView === 'json' && headersEntries.length > 0" class="value-view value-view--headers">{{ headersJsonText }}</pre>
              <p v-else class="empty compact detail-headers-empty">{{ t("messages.headersEmpty") }}</p>
            </div>
          </section>

          <!-- Value：可折叠区块（编辑器吃满抽屉剩余高度）；CodeEditor 只读高亮（json/xml）
               + 解码管线 + 标题行右侧操作 icon -->
          <section class="detail-block detail-block--value">
            <div class="detail-block__head">
              <button class="detail-block__toggle" type="button" :aria-expanded="sectionsOpen.value" @click="toggleSection('value')">
                <ChevronDown class="chev" :class="{ folded: !sectionsOpen.value }" aria-hidden="true" />
                <span class="detail-block__title">{{ t("messages.colValue") }}<span v-if="viewBusy" class="detail-block__busy">…</span></span>
              </button>
              <span v-if="sectionsOpen.value" class="detail-block__actions">
                <button class="seg-toggle" type="button" :class="{ 'is-active': showFullBase64 }" :title="t('messages.fullValue')" @click="showFullBase64 = !showFullBase64">
                  {{ showFullBase64 ? t("messages.formatRaw") : t("messages.fullValue") }}
                </button>
                <button class="icon-button" type="button" :title="t('messages.downloadValue')" @click="downloadValue"><Download /></button>
                <button class="icon-button" type="button" :title="t('messages.copyValue')" data-testid="copy-value" @click="copyDetail('value')"><Copy /></button>
              </span>
            </div>
            <div v-show="sectionsOpen.value" class="detail-block__body">
              <div class="kafka-form kafka-form--bare detail-view-form">
                <label class="field">
                  <span>{{ t("messages.decode") }}</span>
                  <select v-model="viewDecode" @change="renderView">
                    <option value="none">none</option>
                    <option value="base64">base64</option>
                  </select>
                </label>
                <label class="field">
                  <span>{{ t("messages.decompression") }}</span>
                  <select v-model="viewDecompression" @change="renderView">
                    <option value="none">none</option>
                    <option value="gzip">gzip</option>
                    <option value="lz4">lz4</option>
                    <option value="zstd">zstd</option>
                    <option value="snappy">snappy</option>
                  </select>
                </label>
                <label class="field">
                  <span>{{ t("messages.format") }}</span>
                  <select v-model="viewFormat" @change="renderView">
                    <option value="raw">{{ t("messages.formatRaw") }}</option>
                    <option value="json">{{ t("messages.formatJson") }}</option>
                    <option value="xml">XML</option>
                    <option value="hex">{{ t("messages.formatHex") }}</option>
                    <option value="bitset">{{ t("messages.formatBitset") }}</option>
                  </select>
                </label>
              </div>
              <pre v-if="viewResult.error" class="value-view error">{{ viewResult.error }}</pre>
              <pre v-else-if="showFullBase64" class="value-view">{{ fullBase64Preview.text }}</pre>
              <CodeEditor
                v-else
                :model-value="viewResult.text"
                :language="viewFormat === 'json' || viewFormat === 'xml' ? viewFormat : 'text'"
                disabled
                min-height="140px"
              />
            </div>
          </section>
        </div>
      </div>
    </teleport>
</template>
