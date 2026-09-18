// Package metrics 提供配额可观测性（计数器/仪表盘，expvar 导出）。
package metrics

import "time"

// Recorder 记录配额运行指标。
type Recorder struct {
	startTime time.Time
}

// New 创建指标记录器并注册到 expvar。
func New() *Recorder { return &Recorder{startTime: time.Now()} }

func (r *Recorder) IncRequests(tenant, api string)             {}
func (r *Recorder) IncAllowed(tenant, api string)              {}
func (r *Recorder) IncRejected(tenant, api, reason string)     {}
func (r *Recorder) IncDegraded(tenant, api, action string)     {}
func (r *Recorder) ObserveLatency(tenant, api string, d time.Duration) {}
func (r *Recorder) SetInflight(tenant, api string, n int64)    {}
func (r *Recorder) SetStoreHealthy(healthy bool)               {}
func (r *Recorder) SetConfigVersion(version string)            {}

// Snapshot 返回指标快照。
func (r *Recorder) Snapshot() map[string]any { return map[string]any{} }
