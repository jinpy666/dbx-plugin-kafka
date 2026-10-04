// @vitest-environment happy-dom
// 方法契约守护（frontend 侧）：backend/contract_methods_test.go 已用 AST 守护
// main.go 方法面并实调 verifiable 方法对比响应键；本文件以同一份
// shared/contracts/methods.json（评审架构 M-1 迁入 shared/contracts，与
// events.json 同侧，归 contract lane）守护 frontend——
//  1. mockDbxHost 的方法面 ⊆ 契约（mock 不能虚构方法/漂移出未登记方法，
//     presets 响应形状漂移被掩盖的根因正是 mock 与前端类型同源造假）；
//  2. 契约 ∩ mock 面的 verifiable 方法实调（走真实 window.dbxPlugin.invoke
//     分发），返回顶层键 ⊆ 契约 keys——mock 返回形状漂移即红灯；
//  3. api.ts 的方法面 ⊆ 契约（2026-09 评审：config/alter、schema/test、
//     schema/delete 三处漂移正是在此前的守护盲区合入——契约文件自述
//     "frontend 侧由本 spec 守护"，但正则只扫得到 mock）。
import { describe, expect, it } from "vitest";
import contract from "../../../shared/contracts/methods.json";
import mockSource from "../mockDbxHost.ts?raw";
import apiSource from "./api.ts?raw";
import "../mockDbxHost";

interface ContractEntry {
  keys: string[];
  verifiable?: boolean;
  phase?: number;
  params?: string;
}

const methods = contract.methods as unknown as Record<string, ContractEntry>;

// 注释剥离后再提取（评审 M-4）：注释里提到的方法名（漂移复盘等）不得计入
// 方法面，否则守护面被注释措辞污染。
const stripComments = (source: string) => source.replace(/\/\*[\s\S]*?\*\//g, "").replace(/\/\/.*$/gm, "");

// mock 源码里的方法字面量（`method === "kafka/..."` / `method === "connection/..."`）。
const mockMethods = new Set(
  [...stripComments(mockSource).matchAll(/method === "([^"]+)"/g)]
    .map((match) => match[1])
    .filter((method) => method.startsWith("kafka/") || method.startsWith("connection/")),
);

// api.ts 全文提取 wire 方法字面量（评审 M-4）：只匹配 callKafka( 调用位的话，
// 把方法串抽成常量或包装函数的无害重构会静默缩水守护面（size>0 挡不住
// 「缩到剩一个」）；字面量无论内联还是常量都留在本文件里，全文提取对两类
// 写法同样生效。
const apiMethods = new Set(
  [...stripComments(apiSource).matchAll(/"((?:kafka|connection)\/[a-z0-9/]+)"/g)].map((match) => match[1]),
);

describe("method contract (frontend side)", () => {
  it("mock methods must all be declared in the contract", () => {
    expect(mockMethods.size).toBeGreaterThan(0);
    const undeclared = [...mockMethods].filter((method) => !(method in methods));
    expect(undeclared, `mock methods missing from methodContract.json: ${undeclared.join(", ")}`).toEqual([]);
  });

  it("api.ts callKafka methods must all be declared in the contract", () => {
    expect(apiMethods.size).toBeGreaterThan(0);
    const undeclared = [...apiMethods].filter((method) => !(method in methods));
    expect(undeclared, `api.ts methods missing from methodContract.json: ${undeclared.join(", ")}`).toEqual([]);
  });

  it("contract ∩ mock verifiable methods return keys within the declared set", async () => {
    const invokable = Object.entries(methods).filter(
      ([method, entry]) => entry.verifiable && mockMethods.has(method),
    );
    // mock 面至少覆盖 presets 族与连接状态（工作台走查面的契约子集）。
    expect(invokable.map(([method]) => method)).toEqual(
      expect.arrayContaining(["kafka/presets/list", "kafka/presets/save", "kafka/presets/remove", "kafka/connections/statuses"]),
    );

    const api = window as unknown as {
      dbxPlugin: { invoke: (method: string, params?: unknown) => Promise<unknown> };
    };

    // phase 1（缺省）先建立状态，phase 2（presets/remove）再验证。
    const byPhase = (phase: number) =>
      invokable.filter(([, entry]) => (entry.phase ?? 1) === phase);
    for (const phase of [1, 2]) {
      for (const [method, entry] of byPhase(phase)) {
        const rawParams = entry.params ? JSON.parse(entry.params) : undefined;
        const result = (await api.dbxPlugin.invoke(method, rawParams)) as Record<string, unknown>;
        expect(result, `${method} must return an object`).toBeTypeOf("object");
        const unexpected = Object.keys(result ?? {}).filter((key) => !entry.keys.includes(key));
        expect(
          unexpected,
          `${method}: response keys ${JSON.stringify(unexpected)} missing from contract keys ${JSON.stringify(entry.keys)}`,
        ).toEqual([]);
      }
    }
  });

  it("drift pins: previously drifted methods return contract keys against mock", async () => {
    // 2026-09 评审实锤的三处漂移（config/alter、schema/test、schema/delete
    // 族）：需要真实连接、进不了 backend verifiable 实调，此处以 mock 实调
    // 钉死「必需键存在 + 全部键 ⊆ 契约 keys」，防止同类漂移再次合入。
    // 2026-09-28 评审追加三处形状漂移（M-2/M-3/M-4）：契约 keys 只查顶层
    // 键存在，字段类型/层级漂移在其盲区——本批 pin 补「类型 + 嵌套形状」
    // 断言，mock 与后端对齐后由本守护锁住。
    const api = window as unknown as {
      dbxPlugin: { invoke: (method: string, params?: unknown) => Promise<unknown> };
    };
    const pinned: Array<{
      method: string;
      params: Record<string, unknown>;
      required: string[];
      assert?: (result: Record<string, unknown>) => void;
    }> = [
      // 只读 pin 在前：schema/delete 族与 acls/delete 会改夹具状态
      //（删版本/删 ACL），后跑的 pin 不能依赖它们删掉的实体。
      {
        // M-2：hasCommitted 是结果级布尔（组从未提交时 false），不在行上。
        method: "kafka/groups/offsets/list",
        params: { group: "billing-consumer" },
        required: ["rows", "totalLag", "hasCommitted"],
        assert: (result) => {
          expect(result.hasCommitted, "offsets/list hasCommitted must be a result-level boolean").toBeTypeOf("boolean");
          const rows = result.rows as Array<Record<string, unknown>>;
          expect(rows.length, "fixture group must yield offset rows").toBeGreaterThan(0);
          expect("hasCommitted" in rows[0], "offset rows must not carry row-level hasCommitted").toBe(false);
        },
      },
      {
        // M-3：summary 是结构化 SchemaDiffSummary 对象，非字符串。
        method: "kafka/schema/versions/compare",
        params: { subject: "order-events-value", fromVersion: 1, toVersion: 2 },
        required: ["hunks", "summary"],
        assert: (result) => {
          const summary = result.summary as Record<string, unknown> | null;
          expect(summary, "schema compare summary must be a structured object").toBeTypeOf("object");
          for (const key of ["added", "removed", "unchanged", "beforeLines", "afterLines"]) {
            expect(summary?.[key], `summary.${key} must be numeric`).toBeTypeOf("number");
          }
        },
      },
      { method: "kafka/topics/config/alter", params: { topic: "orders", config: {}, deleteKeys: [] }, required: ["results"] },
      { method: "kafka/schema/test", params: { registry: "confluent" }, required: ["ok", "provider"] },
      {
        // M-4：matched 是逐条删除结果数组，非计数。
        method: "kafka/acls/delete",
        params: { filter: {} },
        required: ["matched"],
        assert: (result) => {
          expect(Array.isArray(result.matched), "acls/delete matched must be an array of deleted bindings").toBe(true);
          for (const binding of result.matched as Array<Record<string, unknown>>) {
            expect(binding.resourceType).toBeTypeOf("string");
            expect(binding.principal).toBeTypeOf("string");
          }
        },
      },
      { method: "kafka/schema/delete", params: { subject: "orders-proto-value", registry: "confluent" }, required: ["deletedVersions"] },
      { method: "kafka/schema/delete/version", params: { subject: "order-events-value", version: 1, registry: "confluent" }, required: ["deletedVersions"] },
    ];
    for (const { method, params, required, assert: verify } of pinned) {
      const result = (await api.dbxPlugin.invoke(method, params)) as Record<string, unknown>;
      const keys = Object.keys(result ?? {});
      const unexpected = keys.filter((key) => !methods[method].keys.includes(key));
      expect(unexpected, `${method}: unexpected keys ${JSON.stringify(unexpected)}`).toEqual([]);
      const missing = required.filter((key) => !keys.includes(key));
      expect(missing, `${method}: missing required keys ${JSON.stringify(missing)}`).toEqual([]);
      verify?.(result);
    }
  });
});
