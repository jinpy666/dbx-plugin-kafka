package kafkaconn

// consume_cancel.go：一次性消费的取消句柄（工作台停止按钮）。
// 一次性消费是同步 RPC，宿主 invoke 无请求中断信号——扫描窗口内长时间无
// 数据时前端只能干等。前端为每次消费生成 consumeId（uuid）随请求携带；
// Consume 把扫描窗口 ctx 的 cancel 登记进注册表，kafka/messages/consume/
// cancel 按 id 提前中断（PollRecords 立即返回，循环按 cancelled 退出并
// 返回停止前的部分结果）。请求结束（成功/失败）即注销；未知 consumeId
// 的 cancel 是 no-op（success:false，调用方按已停止处理）。

import (
	"context"
	"sync"
)

// consumeCancelHandle 取消句柄：独立结构体提供指针身份——同 id 被并发复用
// 时 deregister 靠它识别「自己的登记」（Go 的 func 值不可比较）。
type consumeCancelHandle struct {
	cancel context.CancelFunc
}

// consumeCancelRegistry 按请求 id 管理在途一次性消费的 cancel 句柄。
// 零值可用；map 惰性初始化。
type consumeCancelRegistry struct {
	mu      sync.Mutex
	cancels map[string]*consumeCancelHandle
}

// register 登记取消句柄并返回句柄（同 id 复用时后登记者覆盖——最后一个
// 在途请求的句柄胜出）。
func (r *consumeCancelRegistry) register(consumeID string, cancel context.CancelFunc) *consumeCancelHandle {
	if consumeID == "" || cancel == nil {
		return nil
	}
	handle := &consumeCancelHandle{cancel: cancel}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancels == nil {
		r.cancels = make(map[string]*consumeCancelHandle)
	}
	r.cancels[consumeID] = handle
	return handle
}

// deregister 注销：仅当现存句柄仍是自己登记的实例才删（同 id 被并发复用
// 时，先结束的旧请求不得清掉新请求的登记）。
func (r *consumeCancelRegistry) deregister(consumeID string, handle *consumeCancelHandle) {
	if consumeID == "" || handle == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancels[consumeID] == handle {
		delete(r.cancels, consumeID)
	}
}

// cancel 中断在途消费；返回是否命中（未知/已结束的 id 返回 false）。
func (r *consumeCancelRegistry) cancel(consumeID string) bool {
	r.mu.Lock()
	handle, ok := r.cancels[consumeID]
	r.mu.Unlock()
	if !ok {
		return false
	}
	handle.cancel()
	return true
}

// CancelConsume 提前中断进行中的一次性消费（kafka/messages/consume/cancel）。
// 未知/已结束的 consumeId 返回 false（幂等：调用方可按「已停止」处理）。
func (s *Service) CancelConsume(consumeID string) bool {
	return s.consumeCancels.cancel(trimSpace(consumeID))
}

// consumeExitFlags 由「取消请求命中」推导退出标记：cancelled 只在显式
// cancel 命中时为真；父请求取消与扫描窗口到点都保持既有 timedOut 语义。
func consumeExitFlags(userCancelled bool) (cancelled, timedOut bool) {
	if userCancelled {
		return true, false
	}
	return false, true
}
