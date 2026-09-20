package quota

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// DegradePolicy 定义配额后端不可用时的降级策略。
type DegradePolicy int

const (
	// DegradeDeny 后端不可用时拒绝请求（fail-closed，默认，更安全）。
	DegradeDeny DegradePolicy = iota
	// DegradeAllow 后端不可用时放行请求（fail-open，保证可用性）。
	DegradeAllow
)

// String 返回降级策略的可读名称。
func (p DegradePolicy) String() string {
	switch p {
	case DegradeAllow:
		return "allow"
	default:
		return "deny"
	}
}

// ParseDegradePolicy 解析降级策略字符串。
func ParseDegradePolicy(s string) (DegradePolicy, error) {
	switch s {
	case "", "deny":
		return DegradeDeny, nil
	case "allow":
		return DegradeAllow, nil
	default:
		return DegradeDeny, fmt.Errorf("unknown degrade policy %q", s)
	}
}

const (
	// DefaultWeight 是未显式配置时的调度权重。
	DefaultWeight = 1
	// MinSupportedConfigVersion 是本实例能够兼容的最早配置版本。
	// 语义化版本：同一主版本号内向前/向后兼容。
	MinSupportedConfigVersion = "1.0"
	// maxConcurrencyTTLSeconds 限制租约 TTL 上限（1 小时），防止误配置成
	// 超大值导致崩溃占位长时间无法回收。
	maxConcurrencyTTLSeconds = 3600
)

// Rule 是单个 (租户, 接口) 维度的配额规则。
//
// API 为空字符串表示该租户的默认规则；找不到接口专属规则时回落到默认规则。
// Rate 单位为“令牌/秒”，Burst 为令牌桶容量，MaxConcurrent 为在途请求上限。
// Weight 用于容量竞争时的加权公平调度，<=0 时取 Config.DefaultWeight。
type Rule struct {
	Tenant        string  `json:"tenant"`
	API           string  `json:"api,omitempty"`
	Rate          float64 `json:"rate"`
	Burst         int64   `json:"burst"`
	MaxConcurrent int64   `json:"maxConcurrent"`
	Weight        int     `json:"weight,omitempty"`
}

// Key 返回规则的唯一键。
func (r Rule) Key() string { return r.Tenant + "\x00" + r.API }

// Config 是一份版本化的配额配置。
type Config struct {
	// Version 为配置版本（语义化版本，如 "1.3"）。
	// 新版本实例只接受主版本号不高于自身、且 >= MinSupportedConfigVersion 的配置。
	Version       string        `json:"version"`
	Rules         []Rule        `json:"rules"`
	DefaultWeight int           `json:"defaultWeight,omitempty"`
	Degrade       DegradePolicy `json:"degrade"`
	// GlobalMaxConcurrent 是所有租户竞争的共享在途容量（0 表示不设全局池）。
	// 容量竞争时，各租户按 Weight 比例分享该池；闲置容量可被临时借用。
	GlobalMaxConcurrent int64 `json:"globalMaxConcurrent,omitempty"`
	// ConcurrencyTTLSeconds 是并发占位租约 TTL（秒，可选）：持有者存活时
	// 由本实例自动续租，因此长请求不会被提前回收；实例崩溃后遗留占位在
	// 该时长内有界回收。<=0 表示使用实例默认值（30s）。
	ConcurrencyTTLSeconds int64 `json:"concurrencyTTLSeconds,omitempty"`
}

// Validate 校验配置；非法或不兼容的配置返回错误，且不产生任何副作用。
func Validate(c *Config) error {
	if c == nil {
		return newConfigError("", "config is nil")
	}
	if c.Version == "" {
		return newConfigError("version", "must not be empty")
	}
	if err := validateVersion(c.Version); err != nil {
		return newConfigError("version", err.Error())
	}
	if c.DefaultWeight < 0 {
		return newConfigError("defaultWeight", "must be >= 0")
	}
	if c.GlobalMaxConcurrent < 0 {
		return newConfigError("globalMaxConcurrent", "must be >= 0")
	}
	if c.ConcurrencyTTLSeconds < 0 {
		return newConfigError("concurrencyTTLSeconds", "must be >= 0")
	}
	if c.ConcurrencyTTLSeconds > maxConcurrencyTTLSeconds {
		return newConfigError("concurrencyTTLSeconds", "must be <= "+itoa(maxConcurrencyTTLSeconds))
	}
	seen := make(map[string]struct{}, len(c.Rules))
	for i := range c.Rules {
		r := &c.Rules[i]
		if r.Tenant == "" {
			return newConfigError("rules["+itoa(i)+"].tenant", "must not be empty")
		}
		if r.Rate < 0 {
			return newConfigError("rules["+itoa(i)+"].rate", "must be >= 0")
		}
		if r.Burst < 0 {
			return newConfigError("rules["+itoa(i)+"].burst", "must be >= 0")
		}
		if r.Rate > 0 && r.Burst < 1 {
			return newConfigError("rules["+itoa(i)+"].burst", "must be >= 1 when rate > 0")
		}
		if r.MaxConcurrent < 0 {
			return newConfigError("rules["+itoa(i)+"].maxConcurrent", "must be >= 0")
		}
		if r.Weight < 0 {
			return newConfigError("rules["+itoa(i)+"].weight", "must be >= 0")
		}
		if _, dup := seen[r.Key()]; dup {
			return newConfigErrorf("rules", "duplicate rule for tenant=%q api=%q", r.Tenant, r.API)
		}
		seen[r.Key()] = struct{}{}
	}
	return nil
}

// normalize 返回归一化副本：填充默认权重，并按稳定顺序排序。
func (c *Config) normalize() *Config {
	cp := c.Clone()
	dw := cp.DefaultWeight
	if dw <= 0 {
		dw = DefaultWeight
	}
	for i := range cp.Rules {
		if cp.Rules[i].Weight <= 0 {
			cp.Rules[i].Weight = dw
		}
	}
	sort.SliceStable(cp.Rules, func(i, j int) bool {
		if cp.Rules[i].Tenant != cp.Rules[j].Tenant {
			return cp.Rules[i].Tenant < cp.Rules[j].Tenant
		}
		return cp.Rules[i].API < cp.Rules[j].API
	})
	return cp
}

// RuleFor 返回 (tenant, api) 命中的规则：优先精确匹配，其次租户默认规则。
func (c *Config) RuleFor(tenant, api string) (Rule, bool) {
	var fallback Rule
	var hasFallback bool
	for _, r := range c.Rules {
		if r.Tenant != tenant {
			continue
		}
		if r.API == api {
			return r, true
		}
		if r.API == "" {
			fallback, hasFallback = r, true
		}
	}
	return fallback, hasFallback
}

// Clone 返回配置的深拷贝。
func (c *Config) Clone() *Config {
	if c == nil {
		return nil
	}
	cp := &Config{
		Version:               c.Version,
		DefaultWeight:         c.DefaultWeight,
		Degrade:               c.Degrade,
		GlobalMaxConcurrent:   c.GlobalMaxConcurrent,
		ConcurrencyTTLSeconds: c.ConcurrencyTTLSeconds,
		Rules:                 make([]Rule, len(c.Rules)),
	}
	copy(cp.Rules, c.Rules)
	return cp
}

// validateVersion 校验语义化版本兼容性（主版本号必须为 1）。
func validateVersion(v string) error {
	major, minor, ok := parseSemVer(v)
	if !ok {
		return fmt.Errorf("malformed semantic version %q, want MAJOR.MINOR", v)
	}
	supMajor, supMinor, _ := parseSemVer(MinSupportedConfigVersion)
	if major != supMajor {
		return fmt.Errorf("incompatible config version %q: major version must be %d", v, supMajor)
	}
	if minor < supMinor {
		return fmt.Errorf("incompatible config version %q: lower than minimum supported %s", v, MinSupportedConfigVersion)
	}
	return nil
}

// parseSemVer 解析严格的 "MAJOR.MINOR" 或 "MAJOR.MINOR.PATCH"，拒绝尾随内容。
func parseSemVer(v string) (major, minor int, ok bool) {
	parts := strings.Split(v, ".")
	if len(parts) < 2 || len(parts) > 3 {
		return 0, 0, false
	}
	for i, p := range parts {
		if p == "" {
			return 0, 0, false
		}
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return 0, 0, false
		}
		switch i {
		case 0:
			major = n
		case 1:
			minor = n
		}
	}
	return major, minor, true
}
