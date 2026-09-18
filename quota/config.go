package quota

// DegradePolicy 定义配额后端不可用时的降级策略。
type DegradePolicy int

const (
	// DegradeDeny 后端不可用时拒绝请求（fail-closed）。
	DegradeDeny DegradePolicy = iota
	// DegradeAllow 后端不可用时放行请求（fail-open）。
	DegradeAllow
)

// Rule 是单个 (租户, 接口) 维度的配额规则。
type Rule struct {
	Tenant        string  // 租户标识
	API           string  // 接口标识，空字符串表示该租户的默认规则
	Rate          float64 // 平均速率（令牌/秒），0 表示不限速
	Burst         int64   // 突发容量（令牌桶大小）
	MaxConcurrent int64   // 并发上限，0 表示不限制
	Weight        int     // 公平调度权重，<=0 时取 DefaultWeight
}

// Config 是一份版本化的配额配置。
type Config struct {
	Version       string
	Rules         []Rule
	DefaultWeight int
	Degrade       DegradePolicy
}

// Validate 校验配置；非法或不兼容的配置返回错误。
func Validate(c *Config) error { return nil }

// RuleFor 返回 (tenant, api) 命中的规则。
func (c *Config) RuleFor(tenant, api string) (Rule, bool) { return Rule{}, false }

// Clone 返回配置的深拷贝。
func (c *Config) Clone() *Config { return nil }
