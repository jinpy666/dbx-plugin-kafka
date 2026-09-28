package mcp

// stdio_panic_test.go：请求 goroutine 的 panic 防护（评审 H-2）——单个
// 畸形请求引发的 panic 必须转为 -32603 响应，不得带崩插件进程（宿主侧
// 只会看到进程退出，会话全断）。

import (
	"encoding/json"
	"strings"
	"testing"
)

// S-PANIC-1 助手契约：panic → -32603 失败响应（id 未知置 null）；正常
// 路径原样透传。
func TestRequestRecoverSafePanics(t *testing.T) {
	response := requestRecoverSafe(func() map[string]any { panic("boom") })
	if response == nil {
		t.Fatal("panic must yield a failure response")
	}
	data, _ := json.Marshal(response)
	got := string(data)
	if !strings.Contains(got, "-32603") || !strings.Contains(got, "boom") {
		t.Fatalf("response = %s, want -32603 carrying panic value", got)
	}
}

func TestRequestRecoverSafePassthrough(t *testing.T) {
	want := map[string]any{"jsonrpc": "2.0", "id": json.RawMessage("1"), "result": "ok"}
	got := requestRecoverSafe(func() map[string]any { return want })
	if got == nil || got["result"] != "ok" {
		t.Fatalf("passthrough broken: %v", got)
	}
}
