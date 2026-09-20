// Package middleware 提供配额相关的 HTTP 中间件。
package middleware

import (
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/lechandonga/quota-gateway/metrics"
	"github.com/lechandonga/quota-gateway/quota"
)

// ReasonHeader 携带机器可区分的拒绝原因（quota.Error.Code）。
const ReasonHeader = "X-Quota-Reason"

// IdentityResolver 从请求解析租户与接口标识。
type IdentityResolver func(*http.Request) (tenant, api string)

// Options 是中间件配置。
type Options struct {
	// Resolve 解析 (租户, 接口)；返回空 tenant 时该请求不施加配额。
	Resolve IdentityResolver
	// Metrics 可选，用于观测被中间件服务的请求延迟。
	Metrics *metrics.Recorder
}

// DefaultResolver 默认：租户取 X-Tenant-ID 头，接口取 URL 路径。
func DefaultResolver(r *http.Request) (string, string) {
	return r.Header.Get("X-Tenant-ID"), r.URL.Path
}

type errorBody struct {
	Error struct {
		Code             string `json:"code"`
		Message          string `json:"message"`
		RetryAfterSecond int    `json:"retry_after_seconds,omitempty"`
	} `json:"error"`
}

// QuotaHandler 返回配额中间件。
//
// 拒绝语义：
//   - rate_limited / concurrency_limited / 降级期本地并发上限 → 429 Too Many
//     Requests，带 Retry-After（秒）与 X-Quota-Reason；
//   - 后端不可用且降级策略为 deny → 503 Service Unavailable；
//   - 请求方 ctx 截止/取消（含客户端断连）不产生占位。
//
// 并发占用在以下任一路径都会被幂等释放：handler 正常返回、handler panic、
// 请求超时、客户端中断（r.Context 被取消）；存储异常路径另有租约 TTL 兜底。
// 释放只执行一次：即使“客户端中断”与“handler 结束”同时发生（高并发压测下
// 的常见交错），也绝不会重复释放或 panic。
func QuotaHandler(m *quota.Manager, opts Options) func(http.Handler) http.Handler {
	resolve := opts.Resolve
	if resolve == nil {
		resolve = DefaultResolver
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tenant, api := resolve(r)
			if tenant == "" {
				next.ServeHTTP(w, r)
				return
			}

			start := time.Now()
			release, err := m.Acquire(r.Context(), tenant, api)
			if err != nil {
				writeQuotaError(w, err)
				return
			}

			// 任一路径先发生即释放（ReleaseFunc 自身也幂等）：
			// handler 结束 / panic / 超时 / 客户端中断。
			// 必须用 sync.Once：客户端断连的监听 goroutine 与 handler
			// 返回会并发调用 doRelease，select+close 模式在该交错下会
			// double-close channel 并使进程 panic。
			var once sync.Once
			doRelease := func() { once.Do(release) }
			defer doRelease()
			contextDone := r.Context().Done()
			if contextDone != nil {
				stop := make(chan struct{})
				go func() {
					select {
					case <-contextDone:
						doRelease()
					case <-stop:
					}
				}()
				// handler 返回后停止监听；defer 保证 panic 路径也会停止，
				// goroutine 数量与“进行中的长连接数”同阶，连接结束即退出。
				defer close(stop)
			}

			// panic 安全兜底：业务 handler 出现运行时异常时，名额由上方
			// defer 正常归还，客户端得到 500 而不是连接被重置/进程退出。
			rw := &panicGuardResponseWriter{ResponseWriter: w}
			defer func() {
				if rec := recover(); rec != nil {
					if !rw.wroteHeader {
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(http.StatusInternalServerError)
						_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
							"code":    string(quota.ErrCodeInternal),
							"message": "upstream handler panic",
						}})
					}
				}
			}()

			next.ServeHTTP(rw, r)
			if opts.Metrics != nil {
				opts.Metrics.ObserveLatency(tenant, api, time.Since(start))
			}
		})
	}
}

// panicGuardResponseWriter 记录是否已写响应头，避免 panic 兜底路径重复
// WriteHeader 产生噪声告警。
type panicGuardResponseWriter struct {
	http.ResponseWriter
	wroteHeader bool
}

func (p *panicGuardResponseWriter) WriteHeader(code int) {
	if !p.wroteHeader {
		p.wroteHeader = true
	}
	p.ResponseWriter.WriteHeader(code)
}

func (p *panicGuardResponseWriter) Write(b []byte) (int, error) {
	if !p.wroteHeader {
		p.wroteHeader = true
	}
	return p.ResponseWriter.Write(b)
}

// writeQuotaError 将 quota.Error 映射为稳定可区分的 HTTP 响应。
func writeQuotaError(w http.ResponseWriter, err error) {
	qe, ok := quota.AsError(err)
	if !ok {
		// ctx 取消等非配额错误：交由上游/网关处理，返回 499 风格不适用，
		// 这里以 500 + internal 兜底（正常不会发生）。
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{
			"code":    string(quota.ErrCodeInternal),
			"message": err.Error(),
		}})
		return
	}

	status := http.StatusTooManyRequests
	if qe.Code == quota.ErrCodeUnavailable || qe.Code == quota.ErrCodeDegradedDeny {
		status = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(ReasonHeader, string(qe.Code))
	if secs := qe.RetryAfterSeconds(); secs > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(secs))
	}
	w.WriteHeader(status)
	body := errorBody{}
	body.Error.Code = string(qe.Code)
	body.Error.Message = qe.Message
	body.Error.RetryAfterSecond = qe.RetryAfterSeconds()
	_ = json.NewEncoder(w).Encode(body)
}
