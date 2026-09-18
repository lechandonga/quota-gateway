// Package middleware 提供配额相关的 HTTP 中间件。
package middleware

import (
	"net/http"

	"github.com/lechandonga/quota-gateway/quota"
)

// IdentityResolver 从请求解析租户与接口标识。
type IdentityResolver func(*http.Request) (tenant, api string)

// Options 是中间件配置。
type Options struct {
	Resolve IdentityResolver
}

// QuotaHandler 返回配额中间件。
func QuotaHandler(m *quota.Manager, opts Options) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler { return next }
}
