package quota

import (
	"container/heap"
	"context"
	"math"
	"sync"
)

// fairScheduler 是基于加权虚拟完成时间（WFQ/GPS 近似）的本地准入排序器。
//
// 语义：
//   - 每个请求被视为长度 L=1 的分组，其虚拟完成时间
//     F = max(flow.finish, V) + L/weight；V 为当前所有忙碌 flow 中
//     最小的 finish（GPS 虚拟时间的离散近似）。
//   - 等待中的请求严格按 F 升序放行（F 相同按到达顺序），权重越大的
//     租户在单位时间内被放行越多，从而按 weight 比例分享可用容量。
//   - flow 完全闲置（在途与排队均为 0）后再次到达时，基线对齐当前
//     虚拟时间：闲置容量可被他人借用，原租户恢复请求时既不会被陈旧
//     的高 finish 长期压制，也不会凭旧的低 finish 插队。
//
// 调度器仅决定“准入顺序”，临界区极小，与配额存储实现解耦。
type fairScheduler struct {
	mu    sync.Mutex
	cond  sync.Cond
	flows map[string]*flowState
	queue waiterHeap // 所有排队请求，按 (finish, seq) 升序
	seq   int64
}

type flowState struct {
	finish   float64 // 该 flow 最近一次准入请求的虚拟完成时间
	inflight int     // 已准入但未 release 的请求数
	waiting  int     // 排队中的请求数
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

// wait 按 (tenant, weight) 参与公平排队，轮到自己时返回 nil。
// ctx 取消或超时后请求退出队列（惰性删除），不影响其他请求的排序。
func (s *fairScheduler) wait(ctx context.Context, tenant string, weight int) error {
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

	// ctx 取消时唤醒全体等待者重新评估，从而完成惰性删除。
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
			if !w.canceled {
				w.canceled = true
				fs.waiting--
			}
			s.compactLocked()
			s.cond.Broadcast()
			return err
		}
		s.compactLocked()
		if s.queue.Len() > 0 && s.queue[0] == w {
			heap.Pop(&s.queue)
			fs.waiting--
			fs.finish = myFinish
			fs.inflight++
			s.cond.Broadcast()
			return nil
		}
		s.cond.Wait()
	}
}

// release 表示一个已准入请求完成，更新在途计数并唤醒等待者。
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
		// 完全闲置：对齐虚拟时间，允许容量被借用，后续请求以当前进度为基线。
		fs.finish = s.virtualTimeLocked()
	}
	s.cond.Broadcast()
}

// compactLocked 惰性移除堆顶已取消的等待者。
func (s *fairScheduler) compactLocked() {
	for s.queue.Len() > 0 && s.queue[0].canceled {
		heap.Pop(&s.queue)
	}
}

// virtualTimeLocked 返回当前 GPS 虚拟时间近似：忙碌 flow 中最小 finish。
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
