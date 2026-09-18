package quota

import (
	"container/heap"
	"context"
	"errors"
	"math"
	"sync"
)

// admitOutcome 是队首准入回调的结果。
type admitOutcome int

const (
	outcomeAdmitted  admitOutcome = iota // 已获得容量
	outcomeContended                     // 共享池暂满：继续等待并重试
	outcomeRejected                      // 命中速率/租户硬上限：终止排队
)

// admitFunc 在调用方成为队首时被调用，执行一次原子配额判定。
type admitFunc func() (admitOutcome, error)

// fairScheduler 是基于加权虚拟完成时间（WFQ/GPS 近似）的准入调度器。
//
// 语义：
//   - 请求 F = max(flow.finish, V) + 1/weight；等待请求严格按 (F, seq) 升序
//     成为队首，权重越大的租户单位时间被放行越多 → 按 weight 比例分享容量。
//   - 队首通过 admitFunc 原子判定：拿到容量即准入；共享池占满则在队列内
//     睡眠，由 Release 广播唤醒后重试，因此队列一定可以被排空，不会死锁，
//     也不会因“只试一次”而错误拒绝可被服务的请求。
//   - flow 完全闲置后基线对齐当前 V：闲置容量可被他人借用，原租户恢复时
//     既不被陈旧高 finish 压制，也不凭旧低 finish 插队。
type fairScheduler struct {
	mu    sync.Mutex
	cond  sync.Cond
	flows map[string]*flowState
	queue waiterHeap
	seq   int64
}

type flowState struct {
	finish   float64
	inflight int
	waiting  int
}

type waiter struct {
	tenant   string
	finish   float64
	seq      int64
	canceled bool
}

func newFairScheduler() *fairScheduler {
	s := &fairScheduler{flows: make(map[string]*flowState)}
	s.cond.L = &s.mu
	return s
}

var errWaiterCanceled = errors.New("quota: fair waiter canceled")

// acquire 按 (tenant, weight) 排队，并在成为队首时反复调用 fn 直到：
// 准入成功（nil）、fn 终止拒绝（返回其 error）、ctx 取消/超时（返回 ctx.Err）。
func (s *fairScheduler) acquire(ctx context.Context, tenant string, weight int, fn admitFunc) error {
	if weight < 1 {
		weight = DefaultWeight
	}

	s.mu.Lock()
	fs := s.flows[tenant]
	if fs == nil {
		fs = &flowState{}
		s.flows[tenant] = fs
	}
	myFinish := math.Max(fs.finish, s.virtualTimeLocked()) + 1.0/float64(weight)
	s.seq++
	w := &waiter{tenant: tenant, finish: myFinish, seq: s.seq}
	heap.Push(&s.queue, w)
	fs.waiting++
	s.mu.Unlock()

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			s.cond.Broadcast()
		case <-stop:
		}
	}()

	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			s.removeWaiter(w, fs)
			s.cond.Broadcast()
			return err
		}
		s.compactLocked()
		if s.queue.Len() == 0 || s.queue[0] != w {
			s.cond.Wait()
			continue
		}

		// 成为队首：先出堆（既非纯等待，也尚未准入），在锁外执行原子判定。
		heap.Pop(&s.queue)
		s.mu.Unlock()
		outcome, err := fn()
		s.mu.Lock()
		if ctxErr := ctx.Err(); ctxErr != nil {
			// 判定期间被取消：该请求不再占位，也不回队。
			if !w.canceled {
				w.canceled = true
				fs.waiting--
			}
			s.cond.Broadcast()
			return ctxErr
		}
		switch outcome {
		case outcomeAdmitted:
			fs.waiting--
			fs.finish = myFinish
			fs.inflight++
			s.cond.Broadcast()
			return nil
		case outcomeRejected:
			fs.waiting--
			s.cond.Broadcast()
			return err
		default: // outcomeContended
			if err != nil {
				fs.waiting--
				s.cond.Broadcast()
				return err
			}
			// 未抢到容量：重新入队（F 不变），等待 Release 广播后重试。
			heap.Push(&s.queue, w)
			s.cond.Wait()
		}
	}
}

// removeWaiter 惰性/主动移除一个等待者。
func (s *fairScheduler) removeWaiter(w *waiter, fs *flowState) {
	if !w.canceled {
		w.canceled = true
		fs.waiting--
	}
	s.compactLocked()
}

// release 表示一个已准入请求完成，更新在途计数并广播唤醒队首。
func (s *fairScheduler) release(tenant string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fs := s.flows[tenant]
	if fs == nil {
		return
	}
	if fs.inflight > 0 {
		fs.inflight--
	}
	if fs.inflight == 0 && fs.waiting == 0 {
		fs.finish = s.virtualTimeLocked()
	}
	s.cond.Broadcast()
}

// compactLocked 移除堆顶已取消的等待者。
func (s *fairScheduler) compactLocked() {
	for s.queue.Len() > 0 && s.queue[0].canceled {
		heap.Pop(&s.queue)
	}
}

// virtualTimeLocked 返回 GPS 虚拟时间近似：忙碌 flow 中最小 finish。
func (s *fairScheduler) virtualTimeLocked() float64 {
	v := 0.0
	first := true
	for _, fs := range s.flows {
		if fs.inflight <= 0 && fs.waiting <= 0 {
			continue
		}
		if first || fs.finish < v {
			v = fs.finish
			first = false
		}
	}
	return v
}

// waiterHeap 实现 container/heap.Interface。
type waiterHeap []*waiter

func (h waiterHeap) Len() int { return len(h) }

func (h waiterHeap) Less(i, j int) bool {
	if h[i].finish != h[j].finish {
		return h[i].finish < h[j].finish
	}
	return h[i].seq < h[j].seq
}

func (h waiterHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *waiterHeap) Push(x any) { *h = append(*h, x.(*waiter)) }

func (h *waiterHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return it
}
