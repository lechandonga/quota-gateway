// Package quota 的 localGate 在配额后端不可用（降级放行）以及后端恢复
// 过渡期提供“本实例、本租户、本接口”维度的并发硬上限：
//
//   - 降级放行（fail-open）时，后端无法计数，localGate 保证同一实例上
//     同一 (租户,接口) 的在途请求绝不超过规则配置的 maxConcurrent，
//     避免故障期本地堆积拖垮实例；
//   - 后端恢复后，降级期间仍在运行的请求继续占用本地名额直到结束，
//     新请求由后端统一计账；本地名额与后端名额合起来不超过
//     maxConcurrent（每实例视角），既不放大额度，也不会把同一请求
//     在两处重复计数而误拒正常请求。
//
// 闸门容量随热更新即时变化（与后端判定读取同一配置）：调大立即生效，
// 调小不抢占已在途请求，仅阻止新请求，与 TTL/Release 语义一致。
package quota

import "sync"

type localGate struct {
	mu    sync.Mutex
	slots map[string]*gateState
}

type gateState struct {
	capacity int64 // <=0 表示不限
	inflight int64
}

func newLocalGate() *localGate {
	return &localGate{slots: make(map[string]*gateState)}
}

// tryAcquire 尝试占用一个本地名额。capacity<=0 表示不限；
// 返回 false 表示已达本地硬上限。
func (g *localGate) tryAcquire(key string, capacity int64) bool {
	if capacity <= 0 {
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	s := g.slots[key]
	if s == nil {
		s = &gateState{capacity: capacity}
		g.slots[key] = s
	}
	// 热更新调小：以最新容量为准，但不抢占已在途请求。
	s.capacity = capacity
	if s.inflight >= s.capacity {
		return false
	}
	s.inflight++
	return true
}

// release 归还一个本地名额，幂等：超出在途数的释放（不应发生）被钳制到 0。
func (g *localGate) release(key string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	s := g.slots[key]
	if s == nil || s.inflight == 0 {
		return
	}
	s.inflight--
	if s.inflight == 0 && s.capacity <= 0 {
		delete(g.slots, key)
	}
}

// inflight 返回某键的本地在途数（测试/观测用）。
func (g *localGate) inflight(key string) int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	if s := g.slots[key]; s != nil {
		return s.inflight
	}
	return 0
}
