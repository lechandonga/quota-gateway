package quota

import (
	"log/slog"
	"strconv"
)

// Metrics 是配额系统的可观测性接口，可桥接到 Prometheus 等系统。
type Metrics interface {
	Allowed(tenant, iface string)
	Rejected(tenant, iface string, reason Reason)
	RateTaken(tenant, iface string, remaining int64)
	Concurrency(tenant, iface string, active int64)
	Degraded(tenant, iface string, mode DegradePolicy)
	ConfigReloaded(version int64, ok bool)
}

// LogMetrics 是基于 slog 的最小实现：结构化记录拒绝、降级与配置事件。
// 高频的 Allowed/RateTaken/Concurrency 默认降级为 debug 级别，
// 避免正常流量刷爆日志；异常事件始终 warn。
type LogMetrics struct {
	logger *slog.Logger
}

// NewLogMetrics 创建日志指标实现。
func NewLogMetrics() *LogMetrics {
	return &LogMetrics{logger: slog.Default()}
}

// NewLogMetricsWithLogger 使用指定 logger 创建。
func NewLogMetricsWithLogger(logger *slog.Logger) *LogMetrics {
	return &LogMetrics{logger: logger}
}

func (m *LogMetrics) Allowed(tenant, iface string) {
	m.logger.Debug("quota allowed", "tenant", tenant, "iface", iface)
}

func (m *LogMetrics) Rejected(tenant, iface string, reason Reason) {
	m.logger.Warn("quota rejected",
		"tenant", tenant, "iface", iface, "reason", string(reason))
}

func (m *LogMetrics) RateTaken(tenant, iface string, remaining int64) {
	m.logger.Debug("rate tokens taken",
		"tenant", tenant, "iface", iface, "remaining", strconv.FormatInt(remaining, 10))
}

func (m *LogMetrics) Concurrency(tenant, iface string, active int64) {
	m.logger.Debug("concurrency active",
		"tenant", tenant, "iface", iface, "active", active)
}

func (m *LogMetrics) Degraded(tenant, iface string, mode DegradePolicy) {
	m.logger.Warn("quota dependency degraded",
		"tenant", tenant, "iface", iface, "policy", string(mode))
}

func (m *LogMetrics) ConfigReloaded(version int64, ok bool) {
	if ok {
		m.logger.Info("quota config reloaded", "version", version)
	} else {
		m.logger.Error("quota config reload rejected", "version", version)
	}
}

// releaseFailed 记录释放调用失败（租约会由会话 TTL 兜底回收，不泄漏）。
// 这是一条运维排障日志，不属于“请求被拒绝”。
func (m *LogMetrics) releaseFailed(tenant, iface string) {
	m.logger.Warn("quota lease release failed; relying on session TTL reaping",
		"tenant", tenant, "iface", iface)
}
