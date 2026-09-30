package kafkaconn

// consume_cancel_test.go：一次性消费取消句柄（工作台停止按钮）——全离线。
// 覆盖：注册表登记/命中/注销、未知 id 幂等 no-op、同 id 复用防串扰
// （先结束的旧请求不得清掉新请求的登记）、退出标记推导（cancel 命中
// =cancelled，窗口到点/父请求取消=timedOut 既有语义）。

import (
	"context"
	"testing"
	"time"
)

func TestConsumeCancelRegistry(t *testing.T) {
	t.Run("cancel hits the registered handle", func(t *testing.T) {
		var registry consumeCancelRegistry
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		registry.register("c-1", cancel)
		if !registry.cancel("c-1") {
			t.Fatal("cancel must report a hit for a registered id")
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
			t.Fatal("registered cancel must fire")
		}
	})

	t.Run("unknown id is an idempotent no-op", func(t *testing.T) {
		var registry consumeCancelRegistry
		if registry.cancel("missing") {
			t.Fatal("cancel of an unknown id must return false")
		}
	})

	t.Run("deregister removes the handle", func(t *testing.T) {
		var registry consumeCancelRegistry
		_, cancel := context.WithCancel(context.Background())
		defer cancel()
		handle := registry.register("c-1", cancel)
		registry.deregister("c-1", handle)
		if registry.cancel("c-1") {
			t.Fatal("cancel after deregister must return false")
		}
	})

	t.Run("stale deregister must not drop a newer registration", func(t *testing.T) {
		var registry consumeCancelRegistry
		_, oldCancel := context.WithCancel(context.Background())
		defer oldCancel()
		ctx2, newCancel := context.WithCancel(context.Background())
		defer newCancel()
		registry.register("c-1", oldCancel)
		registry.register("c-1", newCancel) // 同 id 复用：后登记者覆盖
		registry.deregister("c-1", &consumeCancelHandle{cancel: oldCancel})
		if !registry.cancel("c-1") {
			t.Fatal("stale deregister must not remove the newer registration")
		}
		select {
		case <-ctx2.Done():
		case <-time.After(time.Second):
			t.Fatal("newest registered cancel must fire")
		}
	})

	t.Run("empty id and nil handle are ignored", func(t *testing.T) {
		var registry consumeCancelRegistry
		if registry.register("", context.CancelFunc(func() {})) != nil {
			t.Fatal("empty id must not register")
		}
		if registry.register("c-1", nil) != nil {
			t.Fatal("nil handle must not register")
		}
		if registry.cancel("c-1") {
			t.Fatal("nil handle must not be registered")
		}
	})
}

func TestServiceCancelConsume(t *testing.T) {
	svc := NewService()
	if svc.CancelConsume("never-registered") {
		t.Fatal("unknown consumeId must return false")
	}

	// 经公共入口登记后可命中（句柄通路走 Service 字段）。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc.consumeCancels.register("c-1", cancel)
	if !svc.CancelConsume("c-1") {
		t.Fatal("CancelConsume must hit a registered handle")
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("CancelConsume must cancel the scan window")
	}
	if svc.CancelConsume("  ") {
		t.Fatal("blank consumeId must return false")
	}
}

func TestConsumeExitFlags(t *testing.T) {
	cancelled, timedOut := consumeExitFlags(true)
	if !cancelled || timedOut {
		t.Fatalf("user cancel: cancelled=%v timedOut=%v, want true/false", cancelled, timedOut)
	}
	cancelled, timedOut = consumeExitFlags(false)
	if cancelled || !timedOut {
		t.Fatalf("window elapsed: cancelled=%v timedOut=%v, want false/true", cancelled, timedOut)
	}
}
