// Package middleware 提供配额相关的 HTTP 中间件。
package middleware

import (
	"encoding/json"
	"net/http"
	"strconv"
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
//   - rate_limited / concurrency_limited → 429 Too Many Requests，
//     带 Retry-After（秒）与 X-Quota-Reason；
//   - 后端不可用且降级策略为 deny → 503 Service Unavailable；
//   - 请求方 ctx 截止/取消（含客户端断连）不产生占位。
//
// 并发占用的释放不依赖任何每请求 goroutine：Manager 返回的 ReleaseFunc
// 自身幂等（正常返回 / handler panic / 超时路径的 defer 触发都安全），
// 且 Manager 后台循环会回收“ctx 已取消但尚未释放”的租约（客户端中断
// 而 handler 仍未返回的场景），因此任意并发交错下都不会重复归还、
// 提前归还或泄漏，也不会因释放路径 panic 导致进程退出。
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

			// 幂等释放：handler 正常结束 / panic / 超时由 defer 触发；
			// 客户端中断但 handler 未返回的场景由 Manager 租约回收兜底。
			defer release()

			next.ServeHTTP(w, r)
			if opts.Metrics != nil {
				opts.Metrics.ObserveLatency(tenant, api, time.Since(start))
			}
		})
	}
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
