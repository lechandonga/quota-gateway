package quota

import (
	"context"
	"sync"
	"time"
)

// HealthMonitor 周期性探测配额后端可用性，并把状态同步给 Manager 指标。
type HealthMonitor struct {
	m  *Manager
	st interface {
		Ping(ctx context.Context) error
	}
	interval time.Duration
	logger   Logger

	stop chan struct{}
	wg   sync.WaitGroup

	mu      sync.Mutex
	healthy bool
}

// StartHealthMonitor 启动后台探测并在 Stop 前持续运行。
func StartHealthMonitor(m *Manager, pinger interface {
	Ping(ctx context.Context) error
}, interval time.Duration, logger Logger) *HealthMonitor {
	if interval <= 0 {
		interval = time.Second
	}
	if logger == nil {
		logger = discardLogger{}
	}
	h := &HealthMonitor{m: m, st: pinger, interval: interval, logger: logger, stop: make(chan struct{}), healthy: true}
	h.wg.Add(1)
	go h.loop()
	return h
}

func (h *HealthMonitor) loop() {
	defer h.wg.Done()
	t := time.NewTicker(h.interval)
	defer t.Stop()
	for {
		select {
		case <-h.stop:
			return
		case <-t.C:
			ctx, cancel := context.WithTimeout(context.Background(), h.interval)
			err := h.st.Ping(ctx)
			cancel()
			h.setState(err == nil, err)
		}
	}
}

func (h *HealthMonitor) setState(healthy bool, cause error) {
	h.mu.Lock()
	changed := h.healthy != healthy
	h.healthy = healthy
	h.mu.Unlock()
	h.m.SetStoreHealthy(healthy)
	if changed {
		if healthy {
			h.logger.Printf("quota: backend recovered")
		} else {
			h.logger.Printf("quota: backend unhealthy: %v", cause)
		}
	}
}

// Healthy 返回最近一次探测结果。
func (h *HealthMonitor) Healthy() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.healthy
}

// Stop 停止探测并等待循环退出。
func (h *HealthMonitor) Stop() {
	close(h.stop)
	h.wg.Wait()
}
