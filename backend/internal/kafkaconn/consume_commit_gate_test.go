package kafkaconn

// consume_commit_gate_test.go：commit 型消费的两道门（2026-09 全量评审）——
//
//	S-GATE-1（SEC-001）read_only 连接禁止 commit（§5.5，与 stream/start
//	同门禁；未连接连接按只读兜底），拒绝留审计；非 commit 消费不受门禁。
//	S-GATE-2（H-1）commit 用独立短预算：不复用扫描窗口 ctx——窗口耗尽
//	（timeoutMs 到点）是 commit 型消费最常见的退出方式，复用已超时的
//	consumeCtx 会让提交必然失败并丢弃全部已扫结果。

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestConsumeReadOnlyCommitBlocked(t *testing.T) {
	svc := NewService()
	var audit AuditRecord
	svc.Audit = func(rec AuditRecord) { audit = rec }

	_, err := svc.Consume(context.Background(), ConsumeParams{
		ConnectionID: "ghost",
		Topic:        "t",
		GroupID:      "g",
		Commit:       true,
	})
	if err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("err = %v, want read-only commit block", err)
	}
	if audit.Action != "messages-consume-commit" || audit.Result != "blocked" {
		t.Fatalf("audit = %+v, want blocked messages-consume-commit", audit)
	}

	// 非 commit 消费是读操作，不受门禁：应放行到连接层（连接不存在报错，
	// 而非 read-only）。
	_, err = svc.Consume(context.Background(), ConsumeParams{
		ConnectionID: "ghost",
		Topic:        "t",
	})
	if err == nil || strings.Contains(err.Error(), "read-only") {
		t.Fatalf("non-commit consume err = %v, want non-readonly connection failure", err)
	}
}

func TestCommitConsumeOffsetsFreshBudget(t *testing.T) {
	// 父 ctx 存活（评审场景：5s 扫描窗口已超时退出，请求本身未取消），
	// commit 必须拿到带新预算的 ctx，且错误原样上抛。
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()

	var gotDeadline time.Duration
	err := commitConsumeOffsets(parent, func(ctx context.Context) error {
		dl, ok := ctx.Deadline()
		if !ok {
			t.Fatal("commit ctx has no deadline")
		}
		gotDeadline = time.Until(dl)
		return nil
	})
	if err != nil {
		t.Fatalf("commit error = %v", err)
	}
	if gotDeadline <= 0 || gotDeadline > consumeCommitBudget {
		t.Fatalf("commit deadline = %v, want fresh budget within %v", gotDeadline, consumeCommitBudget)
	}

	wantErr := errors.New("broker said no")
	if err := commitConsumeOffsets(parent, func(context.Context) error { return wantErr }); !errors.Is(err, wantErr) {
		t.Fatalf("commit error = %v, want %v", err, wantErr)
	}
}
