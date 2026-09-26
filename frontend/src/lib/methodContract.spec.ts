// @vitest-environment happy-dom
// 方法契约守护（frontend 侧）：backend/contract_methods_test.go 已用 AST 守护
// main.go 方法面并实调 verifiable 方法对比响应键；本文件以同一份
// methodContract.json 守护 frontend——
//  1. mockDbxHost 的方法面 ⊆ 契约（mock 不能虚构方法/漂移出未登记方法，
//     presets 响应形状漂移被掩盖的根因正是 mock 与前端类型同源造假）；
//  2. 契约 ∩ mock 面的 verifiable 方法实调（走真实 window.dbxPlugin.invoke
//     分发），返回顶层键 ⊆ 契约 keys——mock 返回形状漂移即红灯。
import { describe, expect, it } from "vitest";
import contract from "./methodContract.json";
import mockSource from "../mockDbxHost.ts?raw";
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

describe("method contract (frontend side)", () => {
  it("mock methods must all be declared in the contract", () => {
    expect(mockMethods.size).toBeGreaterThan(0);
    const undeclared = [...mockMethods].filter((method) => !(method in methods));
    expect(undeclared, `mock methods missing from methodContract.json: ${undeclared.join(", ")}`).toEqual([]);
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
});
