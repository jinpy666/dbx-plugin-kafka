package mcp

// stdio_notifications_test.go：通知分派的 JSON-RPC id 语义（评审 M-4）。

import (
	"encoding/json"
	"strings"
	"testing"
)

// S-NOTIFY-ID：通知 = 无 id（JSON-RPC 规约）。无 id 通知不回包；带 id 的
// notifications/* 是非法请求，必须回 -32600——此前被无条件吞掉，同步等
// 响应的客户端会永久挂起一个请求槽。
func TestHandleLineNotificationWithIDReplied(t *testing.T) {
	srv := NewStdioServer("0.0.0-test", nil, nil)
	if got := srv.handleLine([]byte(`{"jsonrpc":"2.0","method":"notifications/initialized"}`)); got != nil {
		t.Fatalf("id-less notification must return nil, got %v", got)
	}
	resp := srv.handleLine([]byte(`{"jsonrpc":"2.0","id":1,"method":"notifications/initialized"}`))
	if resp == nil {
		t.Fatal("notification with id must be answered")
	}
	data, _ := json.Marshal(resp)
	if !strings.Contains(string(data), "-32600") {
		t.Fatalf("response = %s, want -32600", data)
	}
}
