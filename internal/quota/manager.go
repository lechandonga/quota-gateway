package quota

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
)

// Manager 负责配置校验、热更新与版本演进。
type Manager struct {
	current atomic.Pointer[Config]
	clock   Clock
	metrics Metrics
}

// NewManager 用初始已校验配置创建管理器。
func NewManager(initial *Config, clock Clock, metrics Metrics) (*Manager, error) {
	if err := ValidateConfig(initial); err != nil {
		return nil, err
	}
	m := &Manager{clock: clock, metrics: metrics}
	m.current.Store(initial)
	return m, nil
}

// Current 返回当前生效配置。
func (m *Manager) Current() *Config {
	c := m.current.Load()
	if c == nil {
		return &Config{}
	}
	return c
}

// Replace 校验并原子安装新配置：
//   - 校验失败或 API 主版本不兼容：拒绝，当前配置完全不受影响；
//   - 版本号回退：拒绝（防止乱序下发覆盖新配置）；
//   - 相同版本：幂等接受（实例重启/重复下发不报错）。
func (m *Manager) Replace(ctx context.Context, cfg *Config) error {
	if err := ValidateConfig(cfg); err != nil {
		if m.metrics != nil {
			m.metrics.ConfigReloaded(cfgVersion(cfg), false)
		}
		return err
	}
	cur := m.current.Load()
	if cur != nil && cfg.Version < cur.Version {
		err := fmt.Errorf("%w: config version regression: new=%d current=%d",
			errBadConfig, cfg.Version, cur.Version)
		if m.metrics != nil {
			m.metrics.ConfigReloaded(cfg.Version, false)
		}
		return err
	}
	m.current.Store(cfg)
	if m.metrics != nil {
		m.metrics.ConfigReloaded(cfg.Version, true)
	}
	return nil
}

func cfgVersion(c *Config) int64 {
	if c == nil {
		return 0
	}
	return c.Version
}

// majorVersion 解析 "v12" 形式的 API 版本号；无法解析返回错误。
func majorVersion(api string) (int, error) {
	if !strings.HasPrefix(api, "v") {
		return 0, fmt.Errorf("apiVersion %q must look like vN", api)
	}
	n, err := strconv.Atoi(api[1:])
	if err != nil || n < 1 {
		return 0, fmt.Errorf("apiVersion %q has invalid version number", api)
	}
	return n, nil
}

// ValidateConfig 静态校验配置：必填字段、取值范围、引用完整性、API 兼容性。
// 纯函数，不触碰任何运行态。
func ValidateConfig(cfg *Config) error {
	if cfg == nil {
		return fmt.Errorf("%w: nil config", errBadConfig)
	}
	maj, err := majorVersion(cfg.APIVersion)
	if err != nil {
		return err
	}
	curMaj, _ := majorVersion(APIVersion)
	if maj != curMaj {
		// 主版本不同：语义可能不兼容（字段含义/协议变化），拒绝共存风险。
		return fmt.Errorf("%w: incompatible apiVersion %s (running %s): major version mismatch",
			errBadConfig, cfg.APIVersion, APIVersion)
	}
	switch cfg.Mode {
	case ModeLocal, ModeDistributed:
	default:
		return fmt.Errorf("%w: unknown mode %q", errBadConfig, cfg.Mode)
	}
	switch cfg.Degrade {
	case DegradeFailOpen, DegradeFailClosed:
	default:
		return fmt.Errorf("%w: degrade policy must be fail-open or fail-closed, got %q",
			errBadConfig, cfg.Degrade)
	}
	for tenant, ifaces := range cfg.Tenants {
		if tenant == "" {
			return fmt.Errorf("%w: empty tenant id", errBadConfig)
		}
		for iface, lim := range ifaces {
			if iface == "" {
				return fmt.Errorf("%w: tenant %q has empty interface name", errBadConfig, tenant)
			}
			if lim.RatePerSec <= 0 {
				return fmt.Errorf("%w: tenant=%q iface=%q ratePerSec must be > 0",
					errBadConfig, tenant, iface)
			}
			if lim.Burst < 1 {
				return fmt.Errorf("%w: tenant=%q iface=%q burst must be >= 1",
					errBadConfig, tenant, iface)
			}
			if lim.Weight < 0 {
				return fmt.Errorf("%w: tenant=%q iface=%q weight must be >= 0",
					errBadConfig, tenant, iface)
			}
			if lim.MaxConcurrent < 0 {
				return fmt.Errorf("%w: tenant=%q iface=%q maxConcurrent must be >= 0",
					errBadConfig, tenant, iface)
			}
		}
	}
	for name, g := range cfg.Groups {
		if name == "" {
			return fmt.Errorf("%w: empty group name", errBadConfig)
		}
		if g.Name != "" && g.Name != name {
			return fmt.Errorf("%w: group map key %q != name %q", errBadConfig, name, g.Name)
		}
		if g.Capacity <= 0 {
			return fmt.Errorf("%w: group %q capacity must be > 0", errBadConfig, name)
		}
		if g.DefaultWeight < 0 {
			return fmt.Errorf("%w: group %q defaultWeight must be >= 0", errBadConfig, name)
		}
	}
	for tenant, ifaces := range cfg.TenantGroups {
		for iface, group := range ifaces {
			if _, ok := cfg.Groups[group]; !ok {
				return fmt.Errorf("%w: tenant=%q iface=%q references undefined group %q",
					errBadConfig, tenant, iface, group)
			}
		}
	}
	if cfg.Mode == ModeDistributed {
		if cfg.SessionTTLMillis < 0 || cfg.HeartbeatMillis < 0 || cfg.RequestTimeoutMillis < 0 {
			return fmt.Errorf("%w: distributed timing fields must be >= 0", errBadConfig)
		}
		// 二者同时显式配置时，心跳必须显著小于会话 TTL，否则实例会在两次心跳
		// 之间被判死，造成在途租约被过早回收。
		if cfg.HeartbeatMillis > 0 && cfg.SessionTTLMillis > 0 &&
			cfg.HeartbeatMillis*2 > cfg.SessionTTLMillis {
			return fmt.Errorf("%w: heartbeatMillis (%d) must be at most half of sessionTtlMillis (%d)",
				errBadConfig, cfg.HeartbeatMillis, cfg.SessionTTLMillis)
		}
	}
	return nil
}
