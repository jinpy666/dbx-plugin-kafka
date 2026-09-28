// @vitest-environment happy-dom
// 方法契约守护（frontend 侧）：backend/contract_methods_test.go 已用 AST 守护
// main.go 方法面并实调 verifiable 方法对比响应键；本文件以同一份
// methodContract.json 守护 frontend——
//  1. mockDbxHost 的方法面 ⊆ 契约（mock 不能虚构方法/漂移出未登记方法，
//     presets 响应形状漂移被掩盖的根因正是 mock 与前端类型同源造假）；
//  2. 契约 ∩ mock 面的 verifiable 方法实调（走真实 window.dbxPlugin.invoke
//     分发），返回顶层键 ⊆ 契约 keys——mock 返回形状漂移即红灯；
//  3. api.ts 的 callKafka 方法面 ⊆ 契约（2026-09 评审：config/alter、
//     schema/test、schema/delete 三处漂移正是在此前的守护盲区合入——
//     契约文件自述"frontend 侧由本 spec 守护"，但正则只扫得到 mock）。
import { describe, expect, it } from "vitest";
import contract from "./methodContract.json";
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

// mock 源码里的方法字面量（`method === "kafka/..."` / `method === "connection/..."`）。
const mockMethods = new Set(
  [...mockSource.matchAll(/method === "([^"]+)"/g)]
    .map((match) => match[1])
    .filter((method) => method.startsWith("kafka/") || method.startsWith("connection/")),
);

// api.ts 源码里的方法字面量（`callKafka<...>("kafka/..."`）。
const apiMethods = new Set(
  [...apiSource.matchAll(/callKafka[^("]*\(\s*"([^"]+)"/g)]
    .map((match) => match[1])
    .filter((method) => method.startsWith("kafka/") || method.startsWith("connection/")),
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
    const api = window as unknown as {
      dbxPlugin: { invoke: (method: string, params?: unknown) => Promise<unknown> };
    };
    const pinned: Array<{ method: string; params: Record<string, unknown>; required: string[] }> = [
      { method: "kafka/topics/config/alter", params: { topic: "orders", config: {}, deleteKeys: [] }, required: ["results"] },
      { method: "kafka/schema/test", params: { registry: "confluent" }, required: ["ok", "provider"] },
      { method: "kafka/schema/delete", params: { subject: "orders-proto-value", registry: "confluent" }, required: ["deletedVersions"] },
      { method: "kafka/schema/delete/version", params: { subject: "order-events-value", version: 1, registry: "confluent" }, required: ["deletedVersions"] },
    ];
    for (const { method, params, required } of pinned) {
      const result = (await api.dbxPlugin.invoke(method, params)) as Record<string, unknown>;
      const keys = Object.keys(result ?? {});
      const unexpected = keys.filter((key) => !methods[method].keys.includes(key));
      expect(unexpected, `${method}: unexpected keys ${JSON.stringify(unexpected)}`).toEqual([]);
      const missing = required.filter((key) => !keys.includes(key));
      expect(missing, `${method}: missing required keys ${JSON.stringify(missing)}`).toEqual([]);
    }
  });
});
