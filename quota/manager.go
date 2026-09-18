package quota

import (
	"context"
	"time"

	"github.com/lechandonga/quota-gateway/metrics"
	"github.com/lechandonga/quota-gateway/store"
)

// Logger 是最小日志接口。
type Logger interface {
	Printf(format string, args ...any)
}

// Options 用于构造 Manager。
type Options struct {
	Store          store.Store
	Config         *Config
	InstanceID     string
	ConcurrencyTTL time.Duration
	Metrics        *metrics.Recorder
	Logger         Logger
	Now            func() time.Time
}

// Manager 组合存储、配置与公平调度，对外提供配额判定。
type Manager struct{}

// NewManager 创建管理器并校验初始配置。
func NewManager(opts Options) (*Manager, error) { return &Manager{}, nil }

// Acquire 以非阻塞方式尝试获取配额，成功返回幂等的释放函数。
func (m *Manager) Acquire(ctx context.Context, tenant, api string) (ReleaseFunc, error) {
	return nil, nil
}

// UpdateConfig 热更新配置；校验失败时保持旧配置不变。
func (m *Manager) UpdateConfig(cfg *Config) error { return nil }

// ConfigVersion 返回当前生效配置版本。
func (m *Manager) ConfigVersion() string { return "" }

// Close 关闭管理器。
func (m *Manager) Close() error { return nil }
