package quota

import "time"

// AcquireStatus 是一次配额获取的结果状态。
type AcquireStatus int

const (
	// StatusAcquired 表示获取成功。
	StatusAcquired AcquireStatus = iota
	// StatusRateLimited 表示超过平均速率或突发容量。
	StatusRateLimited
	// StatusConcurrencyLimited 表示超过并发上限。
	StatusConcurrencyLimited
)

// Decision 描述配额判定结果。
type Decision struct {
	Status          AcquireStatus
	RetryAfter      time.Duration
	Tenant          string
	API             string
	Degraded        bool
	BackendInvolved bool
}

// ReleaseFunc 用于释放一次并发占用；必须幂等。
type ReleaseFunc func()
