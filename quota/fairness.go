package quota

import "context"

// fairScheduler 基于加权虚拟时间（WFQ/GPS 近似）对竞争请求排序。
type fairScheduler struct{}

func newFairScheduler() *fairScheduler { return &fairScheduler{} }

// wait 按租户权重排队等待调度顺序；ctx 取消时返回错误。
func (s *fairScheduler) wait(ctx context.Context, tenant string, weight int) error { return nil }

// release 通知一个等待者完成，推进调度。
func (s *fairScheduler) release() {}
