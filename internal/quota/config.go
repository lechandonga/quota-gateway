package quota

// APIVersion 是配置与协议的语义版本。主版本相同表示语义兼容。
const APIVersion = "v1"

// Mode 表示额度执行模式。
type Mode string

const (
	ModeLocal       Mode = "local"       // 单实例：使用进程内状态
	ModeDistributed Mode = "distributed" // 多实例：通过共享协调存储执行
)

// DegradePolicy 定义配额依赖不可用时的降级行为。
type DegradePolicy string

const (
	DegradeFailOpen   DegradePolicy = "fail-open"   // 放行：记录降级指标，仅保留本地并发兜底
	DegradeFailClosed DegradePolicy = "fail-closed" // 拒绝：返回依赖不可用错误
)

// TenantLimit 描述单个租户在某个接口上的限额。
type TenantLimit struct {
	RatePerSec    float64 `json:"ratePerSec"`    // 平均填充速率（令牌/秒）
	Burst         int64   `json:"burst"`         // 突发容量（桶大小）
	MaxConcurrent int64   `json:"maxConcurrent"` // 并发上限，0 表示不限制
	Weight        float64 `json:"weight"`        // 公平调度权重，<=0 时按 1 处理
}

// Group 定义一个共享并发容量池。组内租户按权重分享，
// 闲置容量可被借用；未达自身保证份额的租户需要时可收回。
type Group struct {
	Name          string  `json:"name"`          // 组名
	Capacity      int64   `json:"capacity"`      // 组总并发容量
	DefaultWeight float64 `json:"defaultWeight"` // 默认权重，<=0 时按 1
}

// Config 是配额系统的完整配置（带版本，支持热更新）。
type Config struct {
	APIVersion           string                            `json:"apiVersion"`
	Version              int64                             `json:"version"`              // 单调递增的配置版本
	Mode                 Mode                              `json:"mode"`                 // local | distributed
	Tenants              map[string]map[string]TenantLimit `json:"tenants"`              // tenant -> interface -> limit
	Groups               map[string]Group                  `json:"groups"`               // groupName -> group
	TenantGroups         map[string]map[string]string      `json:"tenantGroups"`         // tenant -> interface -> groupName
	Degrade              DegradePolicy                     `json:"degrade"`              // fail-open | fail-closed
	SessionTTLMillis     int64                             `json:"sessionTtlMillis"`     // 会话租约时长
	HeartbeatMillis      int64                             `json:"heartbeatMillis"`      // 心跳间隔
	RequestTimeoutMillis int64                             `json:"requestTimeoutMillis"` // 远程存储调用超时
}

// MatchLimit 返回某租户/接口的限额及是否命中。
func (c *Config) MatchLimit(tenant, iface string) (TenantLimit, bool) {
	if c == nil {
		return TenantLimit{}, false
	}
	ifaces, ok := c.Tenants[tenant]
	if !ok {
		return TenantLimit{}, false
	}
	l, ok := ifaces[iface]
	return l, ok
}

// MatchGroup 返回某租户/接口所属组及是否命中。
func (c *Config) MatchGroup(tenant, iface string) (Group, bool) {
	if c == nil {
		return Group{}, false
	}
	ifaces, ok := c.TenantGroups[tenant]
	if !ok {
		return Group{}, false
	}
	name, ok := ifaces[iface]
	if !ok {
		return Group{}, false
	}
	g, ok := c.Groups[name]
	return g, ok
}
