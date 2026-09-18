// Package metrics 提供配额可观测性：进程内聚合并通过 expvar 暴露，
// 可被 /debug/vars 抓取，或由 Prometheus 等系统做桥接导出。
package metrics

import (
	"expvar"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Recorder 记录配额运行指标。零值不可用，请使用 New。
type Recorder struct {
	startTime time.Time

	mu       sync.RWMutex
	requests map[string]*atomic.Int64 // key: tenant|api
	allowed  map[string]*atomic.Int64
	rejected map[string]*atomic.Int64 // key: tenant|api|reason
	degraded map[string]*atomic.Int64 // key: tenant|api|action
	latency  map[string]*latencyStats // key: tenant|api
	inflight map[string]*atomic.Int64

	storeHealthy atomic.Bool
	configVer    atomic.Value // string

	requestsTotal atomic.Int64
	allowedTotal  atomic.Int64
	rejectTotal   atomic.Int64
	degradeTotal  atomic.Int64
}

type latencyStats struct {
	count atomic.Int64
	sumUS atomic.Int64
	maxUS atomic.Int64
}

var (
	defaultMu       sync.Mutex
	defaultRecorder *Recorder
)

// New 创建指标记录器，并以 expvar.Map 形式注册到 "quota" 命名空间。
// 多次调用会复用同一默认实例（测试友好）。
func New() *Recorder {
	defaultMu.Lock()
	defer defaultMu.Unlock()
	if defaultRecorder != nil {
		return defaultRecorder
	}
	r := &Recorder{
		startTime: time.Now(),
		requests:  make(map[string]*atomic.Int64),
		allowed:   make(map[string]*atomic.Int64),
		rejected:  make(map[string]*atomic.Int64),
		degraded:  make(map[string]*atomic.Int64),
		latency:   make(map[string]*latencyStats),
		inflight:  make(map[string]*atomic.Int64),
	}
	r.storeHealthy.Store(true)
	r.configVer.Store("")
	defaultRecorder = r
	return r
}

func mkKey(parts ...string) string { return strings.Join(parts, "|") }

func counter(m map[string]*atomic.Int64, mu *sync.RWMutex, key string) *atomic.Int64 {
	mu.RLock()
	c := m[key]
	mu.RUnlock()
	if c != nil {
		return c
	}
	mu.Lock()
	defer mu.Unlock()
	if c = m[key]; c == nil {
		c = &atomic.Int64{}
		m[key] = c
	}
	return c
}

func (r *Recorder) IncRequests(tenant, api string) {
	counter(r.requests, &r.mu, mkKey(tenant, api)).Add(1)
	r.requestsTotal.Add(1)
}

func (r *Recorder) IncAllowed(tenant, api string) {
	counter(r.allowed, &r.mu, mkKey(tenant, api)).Add(1)
	r.allowedTotal.Add(1)
}

func (r *Recorder) IncRejected(tenant, api, reason string) {
	counter(r.rejected, &r.mu, mkKey(tenant, api, reason)).Add(1)
	r.rejectTotal.Add(1)
}

func (r *Recorder) IncDegraded(tenant, api, action string) {
	counter(r.degraded, &r.mu, mkKey(tenant, api, action)).Add(1)
	r.degradeTotal.Add(1)
}

func (r *Recorder) IncInflight(tenant, api string) {
	counter(r.inflight, &r.mu, mkKey(tenant, api)).Add(1)
}

func (r *Recorder) DecInflight(tenant, api string) {
	c := counter(r.inflight, &r.mu, mkKey(tenant, api))
	c.Add(-1)
}

func (r *Recorder) ObserveLatency(tenant, api string, d time.Duration) {
	r.mu.RLock()
	s := r.latency[mkKey(tenant, api)]
	r.mu.RUnlock()
	if s == nil {
		r.mu.Lock()
		if s = r.latency[mkKey(tenant, api)]; s == nil {
			s = &latencyStats{}
			r.latency[mkKey(tenant, api)] = s
		}
		r.mu.Unlock()
	}
	us := d.Microseconds()
	s.count.Add(1)
	s.sumUS.Add(us)
	// 简单 CAS 更新最大值。
	for {
		old := s.maxUS.Load()
		if us <= old || s.maxUS.CompareAndSwap(old, us) {
			break
		}
	}
}

func (r *Recorder) SetInflight(tenant, api string, n int64) {
	counter(r.inflight, &r.mu, mkKey(tenant, api)).Store(n)
}

func (r *Recorder) SetStoreHealthy(healthy bool) { r.storeHealthy.Store(healthy) }

func (r *Recorder) SetConfigVersion(version string) { r.configVer.Store(version) }

func (r *Recorder) IncConfigRejected(version string) {
	counter(r.rejected, &r.mu, mkKey("_config", version, "invalid")).Add(1)
}

// Snapshot 返回指标快照，用于日志、测试或自定义导出。
func (r *Recorder) Snapshot() map[string]any {
	snap := map[string]any{
		"uptime_seconds": int64(time.Since(r.startTime).Seconds()),
		"requests_total": r.requestsTotal.Load(),
		"allowed_total":  r.allowedTotal.Load(),
		"rejected_total": r.rejectTotal.Load(),
		"degraded_total": r.degradeTotal.Load(),
		"store_healthy":  r.storeHealthy.Load(),
		"config_version": r.configVer.Load(),
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	add := func(prefix string, m map[string]*atomic.Int64) map[string]int64 {
		out := make(map[string]int64, len(m))
		for k, v := range m {
			out[k] = v.Load()
		}
		return out
	}
	snap["requests_by_key"] = add("requests", r.requests)
	snap["allowed_by_key"] = add("allowed", r.allowed)
	snap["rejected_by_key"] = add("rejected", r.rejected)
	snap["degraded_by_key"] = add("degraded", r.degraded)
	snap["inflight_by_key"] = add("inflight", r.inflight)
	lat := make(map[string]map[string]int64, len(r.latency))
	keys := make([]string, 0, len(r.latency))
	for k := range r.latency {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		s := r.latency[k]
		lat[k] = map[string]int64{"count": s.count.Load(), "sum_us": s.sumUS.Load(), "max_us": s.maxUS.Load()}
	}
	snap["latency_by_key_us"] = lat
	return snap
}

// PublishExpvar 将快照注册为 expvar 变量，可通过 /debug/vars 观测。
func (r *Recorder) PublishExpvar() {
	expvar.Publish("quota", expvar.Func(func() any { return r.Snapshot() }))
}
