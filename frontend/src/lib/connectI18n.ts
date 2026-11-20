// New Connect strings share an English fallback; existing locale keys remain complete.
const en = {
  title: "Kafka Connect", cluster: "Connect cluster", test: "Test connection", create: "Create connector",
  empty: "No connectors", setup: "Add connect_clusters and connect_credentials in the DBX Kafka connection settings.",
  search: "Filter connectors", name: "Name", type: "Type", state: "State", worker: "Worker", tasks: "Tasks",
  config: "Configuration", plugins: "Connector plugins", plugin: "Plugin class", required: "Required",
  validate: "Validate configuration", valid: "Configuration is valid", errors: "Validation errors: {count}",
  validationNote: "Correct the listed fields. Worker validation messages are withheld because they can contain secrets.",
  secretHint: "[REDACTED] preserves an existing value. New secret values are sent only to the backend and are cleared when this editor closes.",
  pause: "Pause", resume: "Resume", stop: "Stop", restart: "Restart connector", "restart-all": "Restart all tasks", "restart-failed": "Restart failed tasks",
  restartTask: "Restart task", reset: "Reset offsets", delete: "Delete connector", confirmName: "Type the connector name to confirm",
  resetHint: "Stop the connector before resetting offsets. This removes stored connector offsets; subsequent reads may replay data.",
  confirmAction: "Confirm operation", unsupported: "This endpoint is unavailable on this worker/version.",
  topic: "Topics", group: "Consumer group", topicFilter: "Connectors for topic: {topic}", allTopics: "Show all topics",
  errorClass: "Error class (inspect restricted worker logs for full details)", done: "Kafka Connect operation completed",
  connected: "Kafka Connect {version}", configObject: "Configuration must be a JSON object with string values.",
};
export const connectMessages = {
  en,
  es: { ...en, cluster: "Clúster Connect", create: "Crear conector", empty: "Sin conectores", config: "Configuración", name: "Nombre", state: "Estado", pause: "Pausar", resume: "Reanudar", stop: "Detener" },
  it: { ...en, cluster: "Cluster Connect", create: "Crea connettore", empty: "Nessun connettore", config: "Configurazione", name: "Nome", state: "Stato", pause: "Pausa", resume: "Riprendi", stop: "Arresta" },
  ja: { ...en, cluster: "Connect クラスター", create: "コネクターを作成", empty: "コネクターなし", config: "設定", name: "名前", state: "状態", pause: "一時停止", resume: "再開", stop: "停止" },
  "pt-BR": { ...en, cluster: "Cluster Connect", create: "Criar conector", empty: "Nenhum conector", config: "Configuração", name: "Nome", state: "Estado", pause: "Pausar", resume: "Retomar", stop: "Parar" },
  "zh-CN": { ...en, cluster: "Connect 集群", create: "创建连接器", empty: "暂无连接器", config: "配置", name: "名称", state: "状态", pause: "暂停", resume: "恢复", stop: "停止" },
  "zh-TW": { ...en, cluster: "Connect 叢集", create: "建立連接器", empty: "暫無連接器", config: "設定", name: "名稱", state: "狀態", pause: "暫停", resume: "恢復", stop: "停止" },
};
