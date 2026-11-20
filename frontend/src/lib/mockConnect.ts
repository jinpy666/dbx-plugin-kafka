import type { ConnectConnector } from "./api";

const sinkClass = "org.apache.kafka.connect.file.FileStreamSinkConnector";
const initial: ConnectConnector = {
  name: "orders-sink", type: "sink", state: "RUNNING", workerId: "worker:8083",
  tasks: [{ id: 0, state: "RUNNING", workerId: "worker:8083" }],
  topics: ["orders"], consumerGroup: "connect-orders-sink",
};
const connectors = new Map<string, ConnectConnector>([[initial.name, initial]]);
const configurations = new Map<string, Record<string, string>>([
  [initial.name, { name: initial.name, "connector.class": sinkClass, topics: "orders", "tasks.max": "1" }],
]);

export function mockConnect(method: string, input: Record<string, unknown>): unknown {
  const name = String(input.connector ?? "");
  const config = (input.config ?? {}) as Record<string, string>;
  const requireConnector = () => {
    const row = connectors.get(name);
    if (!row) throw new Error("Connector not found (fixture)");
    return row;
  };
  switch (method) {
    case "kafka/connect/clusters/list": return { clusters: [{ name: "local", url: "http://localhost:8083", auth: "none" }] };
    case "kafka/connect/test": return { version: "4.3.1", commit: "fixture", kafkaClusterId: "fixture" };
    case "kafka/connect/connectors/list": return { connectors: structuredClone([...connectors.values()]) };
    case "kafka/connect/connectors/get": return { connector: structuredClone(requireConnector()) };
    case "kafka/connect/config/get": requireConnector(); return { config: { ...configurations.get(name) }, sensitiveKeys: [] };
    case "kafka/connect/plugins/list": return { plugins: [{ class: sinkClass, type: "sink", version: "4.3.1" }] };
    case "kafka/connect/plugins/config": return { definitions: [{ name: "topics", type: "STRING", required: true }] };
    case "kafka/connect/config/validate": {
      const missing = ["name", "connector.class"].filter(key => !config[key]);
      return { errorCount: missing.length, fields: missing.map(name => ({ name, type: "STRING", required: true, errorCount: 1 })) };
    }
    case "kafka/connect/connectors/create":
      if (!name || connectors.has(name)) throw new Error("Connector name is missing or already exists (fixture)");
      connectors.set(name, { ...structuredClone(initial), name, topics: (config.topics ?? config.topic ?? "").split(",").filter(Boolean), consumerGroup: "connect-" + name });
      configurations.set(name, { ...config });
      break;
    case "kafka/connect/config/set":
      requireConnector().topics = (config.topics ?? config.topic ?? "").split(",").filter(Boolean);
      configurations.set(name, { ...config });
      break;
    case "kafka/connect/connectors/delete":
      requireConnector(); connectors.delete(name); configurations.delete(name);
      break;
    case "kafka/connect/connectors/action": {
      const row = requireConnector();
      row.state = input.action === "pause" ? "PAUSED" : input.action === "stop" ? "STOPPED" : "RUNNING";
      break;
    }
    case "kafka/connect/tasks/restart":
      requireConnector().tasks.forEach(task => { if (task.id === input.taskId) { task.state = "RUNNING"; task.trace = undefined; } });
      break;
    case "kafka/connect/offsets/reset": requireConnector(); break;
    default: throw new Error("Unknown Kafka Connect method (fixture)");
  }
  return { success: true, supported: true };
}
