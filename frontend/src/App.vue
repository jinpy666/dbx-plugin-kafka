<script setup lang="ts">
// Kafka 工作台外壳：布局 + 连接上下文（照 ldap App.vue 的宿主桥用法）。
// 连接生命周期由宿主驱动（connection/test|connect|disconnect），工作台只持有
// connectionId；所有 kafka/* 调用经 lib/api.ts 注入 connectionId。
// 面板：messages / stream / produce / topics / groups / brokers / acls。
import { computed, defineAsyncComponent, nextTick, ref, onBeforeUnmount, onMounted, watch } from "vue";
import { Network, RefreshCw } from "@lucide/vue";
import { DBX_POPOVER, resolveAppearance, type DbxPluginAppearanceInput } from "./lib/appearance";
import { isDbxPluginTheme, onHostThemeChange, themeToAppearance } from "./lib/hostTheme";
import { setWorkbenchLocale, t } from "./lib/i18n";
import { kafkaApi, setKafkaConnectionId, type KafkaStreamErrorEvent, type KafkaStreamMessagesEvent, type KafkaTopic } from "./lib/api";
import { friendlyKafkaError } from "./lib/kafkaErrors";
import { decideModalKeydown, focusableElements } from "./lib/modalBehavior";
import { parseAuditEvent, pushAuditItem, type AuditFeedItem } from "./lib/auditFeed";
import { useUiIntent, type UiIntentOutcome } from "../../shared/frontend/uiIntent";
import { applyAppearanceColorVars, subscribeHostEnvironment } from "../../shared/frontend/hostThemeRuntime";
import TopicTree from "./components/TopicTree.vue";
import MessagesPanel from "./components/MessagesPanel.vue";
import StreamPanel from "./components/StreamPanel.vue";
import ConnectionsPanel from "./components/ConnectionsPanel.vue";
import AuditFeedPanel from "./components/AuditFeedPanel.vue";

// 启动性能（大数据量专项）：非默认 tab 的重面板（ag-grid / CodeMirror / 图表
// 所在）走 defineAsyncComponent + 首次访问才挂载（visited set + v-if）——模块
// 求值与实例化都推迟到首次进入；已访问面板用 v-show 常驻保状态。MessagesPanel
// （默认 tab）与 StreamPanel（App 转发流事件的 ref 目标，模块体轻）保持静态。
const ProducePanel = defineAsyncComponent(() => import("./components/ProducePanel.vue"));
const TopicsPanel = defineAsyncComponent(() => import("./components/TopicsPanel.vue"));
const GroupsPanel = defineAsyncComponent(() => import("./components/GroupsPanel.vue"));
const BrokersPanel = defineAsyncComponent(() => import("./components/BrokersPanel.vue"));
const AclsPanel = defineAsyncComponent(() => import("./components/AclsPanel.vue"));
const SchemasPanel = defineAsyncComponent(() => import("./components/SchemasPanel.vue"));
const MonitorPanel = defineAsyncComponent(() => import("./components/MonitorPanel.vue"));

interface ConnectionSummary {
  name?: string;
  host?: string;
  port?: number;
  username?: string;
  color?: string;
  readOnly?: boolean;
  external_config?: Record<string, unknown>;
}

type PanelKey = "messages" | "stream" | "produce" | "topics" | "groups" | "brokers" | "acls" | "schemas" | "monitor";

const hostContext = ref<Record<string, unknown>>({});
const appearance = ref(resolveAppearance());
const ready = ref(false);
const initError = ref("");
const kafkaError = ref("");
const kafkaErrorDetail = ref("");
const notice = ref("");
const activePanel = ref<PanelKey>("messages");

// 懒挂载：首次访问才实例化面板（v-if），已访问面板 v-show 常驻保状态
// （Monitor 切走不停止采样是既有遗留语义，保持不变）。
const visitedPanels = ref<Set<PanelKey>>(new Set<PanelKey>(["messages"]));

function hasVisited(key: PanelKey): boolean {
  return visitedPanels.value.has(key);
}

function openPanel(key: PanelKey) {
  visitedPanels.value.add(key);
  activePanel.value = key;
  // MCP UI 快照（M3）：面板切换后主动上报（无 intentId，sidecar 覆盖最新快照）。
  uiIntent.reportSnapshot({ panel: key, topic: selectedTopic.value || undefined });
}

const topics = ref<KafkaTopic[]>([]);
const topicsLoading = ref(false);
const topicsError = ref("");
const selectedTopic = ref("");

// F6-4：所选 topic 的分区数（topics/list 行已有该字段），供生产面板头部展示
// 与 partition 上界行内校验；未选中或列表未到时缺省 undefined（不做上界校验）。
const selectedTopicPartitionCount = computed(() =>
  topics.value.find((topic) => topic.name === selectedTopic.value)?.partitionCount,
);

const streamRef = ref<InstanceType<typeof StreamPanel>>();
const connectionsOpen = ref(false);

// MessagesPanel ref（MCP intent 落表面：applyIntentConsume/applyIntentSelect
// 经 defineExpose 暴露；面板常驻挂载（v-show），ref 恒可用）。
const messagesRef = ref<InstanceType<typeof MessagesPanel>>();

// -- MCP UI intent 通道（M3，shared/frontend/uiIntent 公共层） ----------------

const INTENT_PANELS: Record<string, PanelKey> = { messages: "messages", topics: "topics", groups: "groups", schemas: "schemas" };

const uiIntentHandlers = {
  focus: async (params: Record<string, unknown>): Promise<UiIntentOutcome> => {
    const panel = INTENT_PANELS[String(params.panel ?? "")];
    if (!panel) {
      return { status: "rejected", reason: t("intent.unknownPanel") };
    }
    openPanel(panel);
    return { status: "applied", summary: { panel } };
  },
  search: async (params: Record<string, unknown>): Promise<UiIntentOutcome> => {
    // 立即切到消息面板：MCP 检索必须可见地落到 UI（用户停在别的 tab 时，
    // 不切面板的填表+消费发生在 v-show 隐藏面板里，看起来像「没反应」）。
    openPanel("messages");
    const topic = String(params.topic ?? "").trim();
    if (topic && topic !== selectedTopic.value) selectTopic(topic);
    await nextTick(); // 等 props.topic 传播（watch 会清空旧结果，避免串台）
    if (!messagesRef.value) {
      return { status: "rejected", reason: t("intent.noMessagesPanel") };
    }
    const outcome = await messagesRef.value.applyIntentConsume(params);
    if (outcome.status === "applied") showNotice(t("intent.applied"));
    else if (outcome.reason) showNotice(t("intent.rejected", { reason: outcome.reason }));
    return outcome;
  },
  select: async (params: Record<string, unknown>): Promise<UiIntentOutcome> => {
    if (!messagesRef.value) {
      return { status: "rejected", reason: t("intent.noMessagesPanel") };
    }
    return messagesRef.value.applyIntentSelect(params);
  },
};

// intent 处理器引用 openPanel/selectTopic（上方函数声明提升），声明后装配；
// 快照型 report 的上报点：面板切换（openPanel）、topic 选中（selectTopic）。
const uiIntent = useUiIntent("kafka", uiIntentHandlers);

// -- 连接弹窗交互（P1-2/P1-3）：Esc 关闭 + Tab 焦点陷阱 + 关闭归还触发元素 --------
// ConnectionsPanel 不可内改，keydown 在 App 壳层监听；决策逻辑走
// modalBehavior.decideModalKeydown（纯函数，有单测）。

const connectionsTrigger = ref<HTMLElement | null>(null);

function modalContainerForTrap(): HTMLElement | null {
  // 导入助手弹窗（teleport 到 body）在场时置顶，陷阱圈定它；否则圈定主弹窗。
  return (
    document.querySelector<HTMLElement>("body > .modal-backdrop .modal") ??
    document.querySelector<HTMLElement>(".workbench .modal-backdrop .modal")
  );
}

function onConnectionsKeydown(event: KeyboardEvent) {
  if (!connectionsOpen.value || (event.key !== "Escape" && event.key !== "Tab")) return;
  const container = modalContainerForTrap();
  if (!container && event.key !== "Escape") return;
  const focusables = container ? focusableElements(container) : [];
  const currentIndex = focusables.indexOf(document.activeElement as HTMLElement);
  const decision = decideModalKeydown(event.key, event.shiftKey, focusables.length, currentIndex);
  if (decision.kind === "close") {
    event.preventDefault();
    event.stopPropagation();
    connectionsOpen.value = false;
  } else if (decision.kind === "focus") {
    event.preventDefault();
    event.stopPropagation();
    focusables[decision.index]?.focus();
  }
}

watch(connectionsOpen, (open) => {
  if (open) {
    connectionsTrigger.value = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    window.addEventListener("keydown", onConnectionsKeydown);
    void nextTick(() => {
      // 打开：焦点进弹窗首个可交互控件（header 操作按钮）。
      const container = document.querySelector<HTMLElement>(".workbench .modal-backdrop .modal");
      const first = container ? focusableElements(container)[0] : null;
      first?.focus({ preventScroll: true });
    });
  } else {
    // 关闭（Esc/✕/footer/遮罩）：焦点归还工具栏触发按钮。
    window.removeEventListener("keydown", onConnectionsKeydown);
    connectionsTrigger.value?.focus({ preventScroll: true });
    connectionsTrigger.value = null;
  }
});

const auditItems = ref<AuditFeedItem[]>([]);
let auditSeq = 0;

let noticeTimer = 0;
// X-P2/P3/P4 收敛：宿主环境订阅聚合句柄（shared/frontend/hostThemeRuntime）。
const unsubscribeEnvironment: Array<() => void> = [];
const unsubscribeEvent: Array<() => void> = [];

const connectionId = computed(() => String(hostContext.value.connectionId || ""));
const connection = computed<ConnectionSummary>(() => {
  const value = hostContext.value.connection;
  return value && typeof value === "object" ? (value as ConnectionSummary) : {};
});
// 写权限 = 宿主 context 未标记只读 且 后端策略层未开启只读门禁；
// 删除门禁 = 策略层 allow_delete（read_only 下后端强制无效，两门同时生效）。
const backendReadOnly = ref(false);
const backendAllowDelete = ref(false);
const canWrite = computed(() => !connection.value.readOnly && !backendReadOnly.value);
const canDelete = computed(() => canWrite.value && backendAllowDelete.value);
// SR provider（Phase P）：statuses.schemaRegistry.provider（旧 sidecar 缺省 = ""，
// 各面板按 confluent 处理；Glue 下消息面板的 schema 挂载区禁用 + 提示）。
const srProvider = ref<"confluent" | "glue" | "">("");

async function refreshBackendPolicy() {
  try {
    const result = await kafkaApi.connectionStatuses();
    const mine = (result.statuses || []).find((row) => row.connectionId === connectionId.value);
    backendReadOnly.value = mine?.readOnly === true;
    // 旧 sidecar 缺 allowDelete 字段时不主动禁用删除按钮（后端仍会拒绝）。
    backendAllowDelete.value = mine?.allowDelete !== false;
    const provider = mine?.schemaRegistry?.provider;
    srProvider.value = provider === "confluent" || provider === "glue" ? provider : "";
  } catch {
    backendReadOnly.value = false;
    backendAllowDelete.value = true;
    srProvider.value = "";
  }
}

const connectionIdentity = computed(() => {
  const host = connection.value.host || connection.value.name || connectionId.value;
  const identity = connection.value.username ? `${connection.value.username}@${host}` : host;
  const port = connection.value.port ? `:${connection.value.port}` : "";
  return `${identity}${port}`;
});
const toolbarStyle = computed(() => {
  const color = connection.value.color;
  if (!color) return undefined;
  // 连接色只做轻染色（P2-11）：浅色主题下 10% 红系会把整条工具栏染成错误态
  // 观感，降到 5%；颜色主线索由 identity 前的 4px 色条承担。
  const light = appearance.value.colorScheme === "light";
  return { backgroundColor: colorWithAlpha(color, light ? 0.05 : 0.1), boxShadow: `inset 0 1px 0 ${colorWithAlpha(color, light ? 0.12 : 0.18)}` };
});

// 颜色变量 → 宿主令牌名。宿主在插件根节点维护了令牌的字段交给 themeSync 桥
// 颜色变量 → 宿主令牌名探测/回退循环收敛到 shared 单点（X-P4，本策略以
// kafka 最初实现为准）；inline 写入会以更高优先级永久冻结 themeSync 桥的
// var() 引用（issue #25：暗色宿主下左栏/表格文字停留在错误快照）。
function applyAppearance(next?: DbxPluginAppearanceInput | null) {
  // 宿主可能缺字段（1.0 部分下发、1.1 theme 通道只带颜色令牌），按 DBX 规范色板补齐。
  const resolved = resolveAppearance(next);
  appearance.value = resolved;
  const root = document.documentElement;
  root.dataset.theme = resolved.colorScheme;
  root.style.colorScheme = resolved.colorScheme;
  applyAppearanceColorVars(root, resolved.colors);
  root.style.setProperty("--popover", DBX_POPOVER[resolved.colorScheme]);
  // 字体不在此内联回写：main.ts 安装的宿主令牌桥已把 --ui-font-family /
  // --mono-font-family 声明为宿主 --font-sans / --font-mono 的 var() 引用，
  // 内联样式会压过桥接样式，导致字体永远停留在插件默认栈。令牌缺失时
  // （Host API 1.0 / mock）桥自身的回退值与原默认一致，行为不变。
}

function showNotice(message: string) {
  notice.value = message;
  window.clearTimeout(noticeTimer);
  noticeTimer = window.setTimeout(() => (notice.value = ""), 3500);
}

function showError(messageOrCause: unknown) {
  const message = messageOrCause instanceof Error ? messageOrCause.message : String(messageOrCause ?? "");
  // 横幅展示本地化的可行动文案；原始错误串挂在 title 悬停里供排查。
  kafkaError.value = friendlyKafkaError(message);
  kafkaErrorDetail.value = kafkaError.value === message ? "" : message;
}

function dismissError() {
  kafkaError.value = "";
}

function colorWithAlpha(color: string, alpha: number) {
  const match = color.trim().match(/^#([0-9a-f]{3}|[0-9a-f]{6})$/i);
  if (!match) return `color-mix(in srgb, ${color} ${Math.round(alpha * 100)}%, transparent)`;
  const hex = match[1].length === 3 ? [...match[1]].map((part) => `${part}${part}`).join("") : match[1];
  const red = Number.parseInt(hex.slice(0, 2), 16);
  const green = Number.parseInt(hex.slice(2, 4), 16);
  const blue = Number.parseInt(hex.slice(4, 6), 16);
  return `rgb(${red} ${green} ${blue} / ${alpha})`;
}

// -- topics ---------------------------------------------------------------------

async function loadTopics() {
  if (topicsLoading.value) return;
  topicsLoading.value = true;
  topicsError.value = "";
  try {
    const response = await kafkaApi.topicsList(true);
    topics.value = Array.isArray(response.topics) ? response.topics : [];
  } catch (cause) {
    topicsError.value = cause instanceof Error ? cause.message : String(cause);
  } finally {
    topicsLoading.value = false;
  }
}

function selectTopic(topic: string) {
  selectedTopic.value = topic;
  // MCP UI 快照（M3）：topic 选中后上报（consume-bar 下拉/树选中同源）。
  uiIntent.reportSnapshot({ panel: activePanel.value, topic: topic || undefined });
}

// 树快捷动作（对齐 Confluent IDE 右键打开 Producer/Consumer）：先保证 topic
// 选中（selectTopic 内含快照上报），再切到对应面板（produce=生产 / messages=消费）。
function openTopicPanel(topic: string, panel: PanelKey) {
  if (selectedTopic.value !== topic) selectTopic(topic);
  openPanel(panel);
}

// -- host bridge ------------------------------------------------------------------

// -- stream 事件背压（大数据量专项）--------------------------------------------
// 流面板不可见（未激活或 document.hidden）时暂停向 StreamPanel 转发流事件，
// 改入有界队列（丢最旧，镜像面板 MAX_ROWS 丢弃语义），可见后按序补发——
// 隐藏期间避免高频响应式追加与 display:none 下的无效 DOM 更新。
const STREAM_BUFFER_LIMIT = 800;
const pendingStreamEvents: Array<KafkaStreamMessagesEvent | KafkaStreamErrorEvent> = [];
let droppedStreamEvents = 0;
const documentHidden = ref(document.hidden);

const streamPanelHidden = computed(() => activePanel.value !== "stream" || documentHidden.value);

function onVisibilityChange() {
  documentHidden.value = document.hidden;
  if (!documentHidden.value) void nextTick(flushStreamEvents);
}

function forwardStreamEvent(params: KafkaStreamMessagesEvent | KafkaStreamErrorEvent) {
  if (streamPanelHidden.value) {
    if (pendingStreamEvents.length >= STREAM_BUFFER_LIMIT) {
      pendingStreamEvents.shift();
      droppedStreamEvents += 1;
    }
    pendingStreamEvents.push(params);
    return;
  }
  streamRef.value?.pushEvent(params);
}

function flushStreamEvents() {
  while (pendingStreamEvents.length > 0 && !streamPanelHidden.value) {
    streamRef.value?.pushEvent(pendingStreamEvents.shift()!);
  }
  // 缓冲丢弃可见化（§8.3 遗留收口）：切走面板期间超出后台缓冲上限的事件此前
  // 只静默计数，用户回来后无从知晓丢了消息；按序补发完成后一次性提示。
  if (droppedStreamEvents > 0) {
    showNotice(t("stream.bufferDropped", { count: droppedStreamEvents }));
    droppedStreamEvents = 0;
  }
}

watch(activePanel, (next) => {
  // 切入 stream：刚挂载（首次访问）或 v-show 显示完成后补发缓冲事件。
  if (next === "stream" && !documentHidden.value) void nextTick(flushStreamEvents);
});

function handleEvent(event: DbxPluginEvent) {
  // env（locale/theme）由 shared/frontend/hostThemeRuntime 的订阅分发；
  // 此处只做窄化排除，后端事件走下方 method 分派。
  if (event.type === "env") return;
  if (event.method === "kafka/audit") {
    // 数据面：进入最近操作面板（denied/error 高亮）；即时反馈走横幅/通知。
    auditItems.value = pushAuditItem(auditItems.value, parseAuditEvent(event.params, auditSeq++, Date.now()));
    const result = String(event.params.result ?? "");
    if (result === "denied" || result === "error") {
      showError(`${event.params.action ?? "kafka"}: ${event.params.detail ?? result}`);
    }
    return;
  }
  if (event.method === "kafka/stream/messages" || event.method === "kafka/stream/error") {
    forwardStreamEvent(event.params as unknown as KafkaStreamMessagesEvent | KafkaStreamErrorEvent);
  }
}

function clearAuditFeed() {
  auditItems.value = [];
}

async function waitForHostApi(timeoutMs = 8000) {
  const deadline = Date.now() + timeoutMs;
  while (!window.dbxPlugin && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 50));
  if (!window.dbxPlugin) throw new Error(t("hostApiUnavailable"));
  return window.dbxPlugin;
}

async function initialize() {
  const api = await waitForHostApi();
  // 竞速超时兜底（评审 LOW-2）：宿主 request 挂起时界面此前永久 Loading
  // 且无错误出口——8s（与 waitForHostApi 同额）后取 ready/超时错误。
  hostContext.value = await Promise.any([
    api.ready,
    api.request<Record<string, unknown>>("host.getContext"),
    new Promise<Record<string, unknown>>((_, reject) =>
      setTimeout(() => reject(new Error(t("hostApiUnavailable"))), 8000),
    ),
  ]);
  setWorkbenchLocale(api.locale || "zh-CN");
  if (api.appearance) applyAppearance(api.appearance);
  else if (isDbxPluginTheme(api.theme)) applyAppearance(themeToAppearance(api.theme));
  // X-P2/P3/P4 收敛：env（locale/theme）+ context + appearance 订阅统一走
  // shared/frontend/hostThemeRuntime 单点（真桥无 onLocaleChange/onContextChange
  // 幽灵 API；onContext 旧桥回退 onContextChange；appearance 契约缺失时经
  // theme 通道兜底，两套不同时挂）。
  unsubscribeEnvironment.push(subscribeHostEnvironment<DbxPluginAppearance, DbxPluginTheme>(api, {
    onLocale: (next) => setWorkbenchLocale(next || "zh-CN"),
    onContext: (context) => {
      hostContext.value = context;
      syncConnectionContext();
    },
    onAppearance: applyAppearance,
    onTheme: (theme) => applyAppearance(themeToAppearance(theme)),
  }, { themeChannel: onHostThemeChange }));
  if (api.onEvent) unsubscribeEvent.push(api.onEvent(handleEvent));
  if (!connectionId.value) throw new Error(t("connectionMissing"));
  syncConnectionContext();
  ready.value = true;
  void loadTopics();
  void refreshBackendPolicy();
}

function syncConnectionContext() {
  setKafkaConnectionId(connectionId.value);
  void refreshBackendPolicy();
}

// 工作台统一使用自己的交互菜单；拦截原生 context menu，避免宿主/浏览器菜单
// 覆盖插件内容。使用捕获阶段是为了覆盖 teleport 到 body 的弹窗区域。
function preventNativeContextMenu(event: MouseEvent) {
  event.preventDefault();
}

onMounted(() => {
  document.addEventListener("visibilitychange", onVisibilityChange);
  document.addEventListener("contextmenu", preventNativeContextMenu, true);
  void initialize().catch((cause) => {
    initError.value = cause instanceof Error ? cause.message : String(cause);
  });
});

onBeforeUnmount(() => {
  document.removeEventListener("visibilitychange", onVisibilityChange);
  document.removeEventListener("contextmenu", preventNativeContextMenu, true);
  // 兜底移除（评审 LOW-1）：connectionsOpen=true 时卸载，keydown 监听不随
  // 关闭路径摘除——HMR/测试场景会泄漏。
  window.removeEventListener("keydown", onConnectionsKeydown);
  window.clearTimeout(noticeTimer);
  uiIntent.stop();
  for (const dispose of [...unsubscribeEnvironment, ...unsubscribeEvent]) dispose();
});
</script>

<template>
  <div class="workbench">
    <header class="toolbar" :style="toolbarStyle">
      <div class="identity">
        <span class="connection-color" :style="connection.color ? { background: connection.color } : undefined" />
        <strong :title="connectionIdentity">{{ connectionIdentity }}</strong>
        <span v-if="!canWrite" class="read-only-badge">{{ t("readOnly") }}</span>
        <span v-if="!canDelete" class="read-only-badge">{{ t("noDelete") }}</span>
      </div>
      <div class="toolbar-actions">
        <button class="toolbar-button" :disabled="!ready" :title="t('connections.title')" @click="connectionsOpen = true">
          <Network class="icon-cyan" aria-hidden="true" /><span>{{ t("connections.title") }}</span>
        </button>
        <span class="toolbar-separator" />
        <button class="icon-button" :disabled="!ready" :title="t('refresh')" @click="loadTopics">
          <RefreshCw aria-hidden="true" />
        </button>
      </div>
    </header>

    <div v-if="initError" class="tree-state">{{ initError }}</div>
    <div v-else-if="!ready" class="tree-state">{{ t("tree.loading") }}</div>

    <template v-else>
      <nav class="tab-bar">
        <button type="button" :class="{ 'is-active': activePanel === 'messages' }" @click="openPanel('messages')">{{ t("tabs.messages") }}</button>
        <button type="button" :class="{ 'is-active': activePanel === 'stream' }" @click="openPanel('stream')">{{ t("tabs.stream") }}</button>
        <button type="button" :class="{ 'is-active': activePanel === 'produce' }" @click="openPanel('produce')">{{ t("tabs.produce") }}</button>
        <button type="button" :class="{ 'is-active': activePanel === 'topics' }" @click="openPanel('topics')">{{ t("tabs.topics") }}</button>
        <button type="button" :class="{ 'is-active': activePanel === 'groups' }" @click="openPanel('groups')">{{ t("tabs.groups") }}</button>
        <button type="button" :class="{ 'is-active': activePanel === 'brokers' }" @click="openPanel('brokers')">{{ t("tabs.brokers") }}</button>
        <button type="button" :class="{ 'is-active': activePanel === 'acls' }" @click="openPanel('acls')">{{ t("tabs.acls") }}</button>
        <button type="button" :class="{ 'is-active': activePanel === 'schemas' }" @click="openPanel('schemas')">{{ t("tabs.schemas") }}</button>
        <button type="button" :class="{ 'is-active': activePanel === 'monitor' }" @click="openPanel('monitor')">{{ t("tabs.monitor") }}</button>
        <span class="tab-spacer" />
        <span v-if="selectedTopic" class="badge" :title="selectedTopic">{{ selectedTopic }}</span>
      </nav>

      <div class="panes">
        <TopicTree
          :topics="topics"
          :loading="topicsLoading"
          :error="topicsError"
          :selected-topic="selectedTopic"
          @refresh="loadTopics"
          @select="selectTopic"
          @open-produce="(topic) => openTopicPanel(topic, 'produce')"
          @open-consume="(topic) => openTopicPanel(topic, 'messages')"
        />
        <div class="divider" />
        <main class="main-pane">
          <MessagesPanel
            ref="messagesRef"
            v-show="activePanel === 'messages'"
            :topic="selectedTopic"
            :topics="topics"
            :can-write="canWrite"
            :sr-provider="srProvider"
            @select-topic="selectTopic"
            @error="showError"
            @notify="showNotice"
          />
          <StreamPanel
            v-if="hasVisited('stream')"
            v-show="activePanel === 'stream'"
            ref="streamRef"
            :topic="selectedTopic"
            :can-write="canWrite"
            :sr-provider="srProvider"
            @error="showError"
            @notify="showNotice"
          />
          <ProducePanel
            v-if="hasVisited('produce')"
            v-show="activePanel === 'produce'"
            :topic="selectedTopic"
            :can-write="canWrite"
            :sr-provider="srProvider"
            :partition-count="selectedTopicPartitionCount"
            @error="showError"
            @notify="showNotice"
          />
          <TopicsPanel
            v-if="hasVisited('topics')"
            v-show="activePanel === 'topics'"
            :topics="topics"
            :loading="topicsLoading"
            :error="topicsError"
            :can-write="canWrite"
            :can-delete="canDelete"
            @error="showError"
            @notify="showNotice"
            @refresh="loadTopics"
          />
          <GroupsPanel v-if="hasVisited('groups')" v-show="activePanel === 'groups'" :can-write="canWrite" :can-delete="canDelete" @error="showError" @notify="showNotice" />
          <BrokersPanel v-if="hasVisited('brokers')" v-show="activePanel === 'brokers'" @error="showError" />
          <AclsPanel v-if="hasVisited('acls')" v-show="activePanel === 'acls'" :can-write="canWrite" :can-delete="canDelete" @error="showError" @notify="showNotice" />
          <SchemasPanel
            v-if="hasVisited('schemas')"
            v-show="activePanel === 'schemas'"
            :can-write="canWrite"
            :can-delete="canDelete"
            :sr-provider="srProvider"
            @error="showError"
            @notify="showNotice"
          />
          <MonitorPanel v-if="hasVisited('monitor')" v-show="activePanel === 'monitor'" @error="showError" @notify="showNotice" @alert="showError" />
          <AuditFeedPanel :items="auditItems" @clear="clearAuditFeed" />
        </main>
      </div>
    </template>

    <!-- P2-24：异步到达的错误/成功通知对读屏可感知——error 用 role="alert"
         （隐含 assertive live），notice 用 role="status"（polite），与
         ProducePanel 成功条既有约定收敛一致。 -->
    <div v-if="kafkaError" class="error-banner" role="alert">
      <span :title="kafkaErrorDetail || kafkaError">{{ kafkaError }}</span>
      <button type="button" @click="dismissError">✕</button>
    </div>
    <div v-if="notice" class="notice" role="status" aria-live="polite">{{ notice }}</div>

    <ConnectionsPanel :open="connectionsOpen" :disabled="!ready" :connection="connection as unknown as Record<string, unknown>" @close="connectionsOpen = false" @error="showError" />
  </div>
</template>
