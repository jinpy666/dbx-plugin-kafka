package main

// contract_methods_test.go：工作台方法契约守护（评审 2026-09-26 WATCH：方法
// 名↔响应形状此前靠人工四方同步——backend Handle switch / frontend api.ts /
// mockDbxHost / 协议文档——且 presets 响应形状已实际漂移过，mock 与前端类型
// 同源造假让测试全绿）。单一事实源是 ../frontend/src/lib/methodContract.json。
//
// 本文件守护 backend 侧：
//  1. main.go Handle switch 的方法面（AST 提取 case 字符串字面量）与
//     fixtures 完全一致——新增/删除方法必须先改契约文件；
//  2. verifiable 方法（无需 Kafka 连接）经 callHandle 实调，成功返回的
//     顶层键集合与 fixtures 声明全等——响应形状漂移即红灯。
// frontend 侧（api.ts 方法面 + mockDbxHost 返回键）由
// frontend/src/lib/methodContract.spec.ts 以同一 fixtures 守护。

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"slices"
	"strconv"
	"testing"
)

// contractEntry 契约清单的一行。
type contractEntry struct {
	// Keys 成功响应的顶层键集合（json 序列化后的实际键名）。
	Keys []string `json:"keys"`
	// Verifiable 无需 Kafka 连接即可离线实调（presets/settings/statuses/
	// mcp/tools 族）；需要拨号的域方法为 false，仅登记方法面。
	Verifiable bool `json:"verifiable"`
	// Params verifiable 实调用的请求参数（JSON 字符串；空 = "{}"）。
	Params string `json:"params,omitempty"`
	// Phase 实调阶段：2 = 依赖 phase 1 先建立状态（presets/remove 依赖
	// save 落库），phase 1 全部调完后再调。缺省 1。
	Phase int `json:"phase,omitempty"`
}

type methodContract struct {
	Methods map[string]contractEntry `json:"methods"`
}

func loadMethodContract(t *testing.T) methodContract {
	t.Helper()
	data, err := os.ReadFile("../frontend/src/lib/methodContract.json")
	if err != nil {
		t.Fatalf("read method contract: %v — frontend/src/lib/methodContract.json is the single source of truth", err)
	}
	var contract methodContract
	if err := json.Unmarshal(data, &contract); err != nil {
		t.Fatalf("parse method contract: %v", err)
	}
	if len(contract.Methods) == 0 {
		t.Fatal("method contract must not be empty")
	}
	return contract
}

// handleCaseMethods AST 解析 main.go：Handle 函数体内**直接子级**的 tagless
// switch（`switch { case "kafka/...": }`）的 case 字符串字面量集合。不递归——
// case body 里的嵌套 switch（mcp/call 工具分发等）不是方法面。
func handleCaseMethods(t *testing.T) []string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "main.go", nil, 0)
	if err != nil {
		t.Fatalf("parse main.go: %v", err)
	}
	seen := map[string]bool{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "Handle" || fn.Recv == nil || fn.Body == nil {
			continue
		}
		for _, stmt := range fn.Body.List {
			// 只取 Handle 的顶层 method switch（含 `switch method {` 的带
			// Tag 形式）；case body 里的嵌套 switch 不在 Body.List 直接
			// 子级，不会被误收。
			sw, ok := stmt.(*ast.SwitchStmt)
			if !ok {
				continue
			}
			for _, clause := range sw.Body.List {
				cc, ok := clause.(*ast.CaseClause)
				if !ok {
					continue
				}
				for _, expr := range cc.List {
					lit, ok := expr.(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						continue
					}
					name, unquoteErr := strconv.Unquote(lit.Value)
					if unquoteErr == nil {
						seen[name] = true
					}
				}
			}
		}
	}
	if len(seen) == 0 {
		t.Fatal("no case methods extracted from (*pluginHandler).Handle")
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// TestMethodContractSurface 方法面对齐：main.go Handle 派发的每个方法都在
// 契约清单里，契约清单也没有幽灵条目（双向全等）。
func TestMethodContractSurface(t *testing.T) {
	contract := loadMethodContract(t)
	handleMethods := handleCaseMethods(t)
	for _, name := range handleMethods {
		if _, ok := contract.Methods[name]; !ok {
			t.Errorf("main.go Handle dispatches %q but methodContract.json has no entry — add it (keys = success-response top-level keys, verifiable only if callable without a Kafka connection)", name)
		}
	}
	for name := range contract.Methods {
		if !slices.Contains(handleMethods, name) {
			t.Errorf("methodContract.json lists %q but main.go Handle never dispatches it (stale contract entry)", name)
		}
	}
}

// TestMethodContractVerifiableResponses verifiable 方法实调：成功响应的顶层
// 键必须都已登记在契约 keys 里（新增/改名未登记即漂移红灯）。keys 是「键
// 并集」语义——omitempty 条件键不要求每次响应都出现，故只做单向包含。
// phase 2 条目（如 presets/remove 依赖 save 落库）在 phase 1 全部调用之后
// 再实调。
func TestMethodContractVerifiableResponses(t *testing.T) {
	contract := loadMethodContract(t)
	h := newTestHandler(t)
	for _, phase := range []int{1, 2} {
		for name, entry := range contract.Methods {
			if !entry.Verifiable || entry.Phase != phase-1 {
				continue
			}
			params := entry.Params
			if params == "" {
				params = "{}"
			}
			result, perr := callHandle(h, name, params, nil)
			if perr != nil {
				t.Errorf("%s: Handle failed: %s (params=%s)", name, perr.Message, params)
				continue
			}
			body, marshalErr := json.Marshal(result)
			if marshalErr != nil {
				t.Errorf("%s: result not marshalable: %v", name, marshalErr)
				continue
			}
			var got map[string]json.RawMessage
			if err := json.Unmarshal(body, &got); err != nil {
				t.Errorf("%s: success result must be an object (got %s)", name, truncateForTest(body))
				continue
			}
			declared := map[string]bool{}
			for _, key := range entry.Keys {
				declared[key] = true
			}
			for key := range got {
				if !declared[key] {
					t.Errorf("%s: response key %q missing from contract (declared keys=%v) — update methodContract.json", name, key, entry.Keys)
				}
			}
		}
	}
}

func gotKeys(m map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func truncateForTest(body []byte) string {
	if len(body) > 200 {
		return fmt.Sprintf("%s…", body[:200])
	}
	return string(body)
}
