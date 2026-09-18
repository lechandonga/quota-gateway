package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

// newTestRecorder 构造不依赖全局默认实例的独立 Recorder。
func newTestRecorder() *Recorder {
	r := &Recorder{
		startTime: time.Now(),
		requests:  map[string]*atomic.Int64{},
		allowed:   map[string]*atomic.Int64{},
		rejected:  map[string]*atomic.Int64{},
		degraded:  map[string]*atomic.Int64{},
		latency:   map[string]*latencyStats{},
		inflight:  map[string]*atomic.Int64{},
	}
	r.storeHealthy.Store(true)
	r.configVer.Store("")
	_ = sync.RWMutex{}
	return r
}
