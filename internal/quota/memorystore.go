package quota

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strconv"
	"sync"
	"time"
)

// errStoreNotReady 表示存储侧尚未安装任何配置，调用方应按依赖不可用处理。
var errStoreNotReady = errors.New("quota: store not ready (no config installed)")

// demandTTL 是排队标记的存活时间：超过该时间没有新的准入申请即认为该租户
// 已不再竞争（其标记被回收，避免永久占用“需求”身份）。
const demandTTL = 2 * time.Second

type leaseRecord struct {
	key        string
	groupName  string
	reqID      RequestID
	sessionID  SessionID
	acquiredAt int64 // unixmilli
}

type sessionRecord struct {
	lastBeat int64 // unixmilli
	ttl      time.Duration
}

type keyMeta struct {
	groupName string
	weight    float64
}

// MemoryStore 是进程内存储，单实例模式直接使用，多实例部署中作为协调服务器后端。
// 所有状态变更在单一互斥锁下完成，从而使公平裁决天然串行化，
// 杜绝多请求并发看到同一快照而超额准入。
type MemoryStore struct {
	mu     sync.Mutex
	clock  Clock
	cfg    *Config
	closed bool
	stopCh chan struct{}
	wg     sync.WaitGroup
	tokenN uint64

	// 速率桶：tenant\x00iface
	buckets map[RateKey]*TokenBucket

	// 速率扣减一次性去重：dedupKey -> 首次结果（带 TTL）。
	rateDedup map[string]cachedRate

	// 并发占用
	leases        map[LeaseToken]*leaseRecord
	active        map[string]int64 // key -> 占用数
	activeByGroup map[string]int64 // group -> 占用数
	keyInfo       map[string]keyMeta
	demand        map[string]int64 // key -> 首次排队 unixmilli

	// 幂等表：键为 (RequestID, Attempt)。
	// 网络重试（同 attempt）返回原裁决，防重复计数；
	// 公平排队新一轮（新 attempt）是独立裁决，允许状态变化后准入。
	// 条目带 TTL（idempotencyTTL），过期回收，避免无界增长。
	admitResults   map[attemptKey]cachedAdmit
	releaseResults map[RequestID]cachedRelease

	// 会话
	sessions map[SessionID]*sessionRecord
}

// idempotencyTTL 是幂等记录的保留时长：显著大于任何合理的网络重试窗口
// （秒级），同时避免长期堆积。已准入选中持有的名额不受此影响——名额由
// 租约表与会话 TTL 管理，幂等记录过期只影响“能否用旧 ID 重放”。
const idempotencyTTL = 10 * time.Minute

type cachedAdmit struct {
	res     AdmitResult
	atMilli int64
}

type cachedRelease struct {
	res     ReleaseResult
	atMilli int64
}

type cachedRate struct {
	res     RateResult
	atMilli int64
}

// attemptKey 是准幂等表的复合键。
type attemptKey struct {
	reqID   RequestID
	attempt int64
}

// NewMemoryStore 创建内存存储并启动租约/需求过期回收协程。
func NewMemoryStore(clock Clock) *MemoryStore {
	if clock == nil {
		clock = SystemClock()
	}
	s := &MemoryStore{
		clock:          clock,
		stopCh:         make(chan struct{}),
		buckets:        make(map[RateKey]*TokenBucket),
		rateDedup:      make(map[string]cachedRate),
		leases:         make(map[LeaseToken]*leaseRecord),
		active:         make(map[string]int64),
		activeByGroup:  make(map[string]int64),
		keyInfo:        make(map[string]keyMeta),
		demand:         make(map[string]int64),
		admitResults:   make(map[attemptKey]cachedAdmit),
		releaseResults: make(map[RequestID]cachedRelease),
		sessions:       make(map[SessionID]*sessionRecord),
	}
	s.wg.Add(1)
	go s.reapLoop()
	return s
}

func compositeKey(tenant, iface string) string { return tenant + "\x00" + iface }

// SyncConfig 在存储侧校验并安装新配置；非法配置返回错误且旧配置保持不变。
func (s *MemoryStore) SyncConfig(ctx context.Context, cfg *Config) error {
	if err := ValidateConfig(cfg); err != nil {
		return err
	}
	now := s.clock.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	// 桶增删改：已有桶热更新参数；新增桶建立；被移除配置的桶删除。
	want := make(map[RateKey]TenantLimit, 256)
	for tenant, ifaces := range cfg.Tenants {
		for iface, lim := range ifaces {
			want[RateKey{tenant, iface}] = lim
		}
	}
	for k, b := range s.buckets {
		lim, ok := want[k]
		if !ok {
			delete(s.buckets, k)
			continue
		}
		b.Update(lim.RatePerSec, lim.Burst, now)
	}
	for k, lim := range want {
		if _, ok := s.buckets[k]; !ok {
			s.buckets[k] = NewTokenBucket(lim.RatePerSec, lim.Burst, now)
		}
	}
	// 重算每个 key 的组成员信息；清除不再属于任何组的陈旧元数据。
	meta := make(map[string]keyMeta, len(want))
	for tenant, ifaces := range cfg.TenantGroups {
		for iface, groupName := range ifaces {
			g, ok := cfg.Groups[groupName]
			if !ok {
				continue
			}
			key := compositeKey(tenant, iface)
			w := 1.0
			if lim, ok := want[RateKey{tenant, iface}]; ok && lim.Weight > 0 {
				w = lim.Weight
			} else if g.DefaultWeight > 0 {
				w = g.DefaultWeight
			}
			meta[key] = keyMeta{groupName: groupName, weight: w}
		}
	}
	// 保留仍有在途占用的 key 的旧组信息（租约释放时需要据此扣减组计数）；
	// 其余陈旧 keyInfo 丢弃。
	for k, info := range s.keyInfo {
		if _, fresh := meta[k]; fresh {
			continue
		}
		if s.active[k] > 0 {
			meta[k] = info
		}
	}
	s.keyInfo = meta
	s.cfg = cfg
	return nil
}

func (s *MemoryStore) configVersionLocked() int64 {
	if s.cfg == nil {
		return 0
	}
	return s.cfg.Version
}

// TryTakeRate 从速率桶尝试取走令牌。相同 DedupKey 的重放返回首次结果，
// 从而“请求已执行、响应丢失、客户端重试”只扣一次令牌。
func (s *MemoryStore) TryTakeRate(ctx context.Context, ded RateDeduction) (RateResult, error) {
	now := s.clock.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	res := RateResult{ConfigVersion: s.configVersionLocked()}
	if ded.DedupKey != "" {
		if cached, ok := s.rateDedup[ded.DedupKey]; ok {
			return cached.res, nil
		}
	}
	key := ded.Key
	want := ded.Want
	if want <= 0 {
		want = 1
	}
	b, ok := s.buckets[key]
	if !ok {
		if s.cfg == nil {
			return res, errStoreNotReady
		}
		lim, ok := s.cfg.MatchLimit(key.Tenant, key.Iface)
		if !ok {
			// 未配置限额的租户/接口不限流。
			res.Allowed = true
			return res, nil
		}
		b = NewTokenBucket(lim.RatePerSec, lim.Burst, now)
		s.buckets[key] = b
	}
	allowed, retryAfter, remaining := b.Take(want, now)
	res.Allowed = allowed
	res.RetryIn = retryAfter
	res.Remaining = remaining
	if ded.DedupKey != "" {
		s.rateDedup[ded.DedupKey] = cachedRate{res: res, atMilli: now.UnixMilli()}
	}
	return res, nil
}

// Admit 申请一个并发名额。同一 RequestID 的重试返回首次裁决，杜绝重复计数。
func (s *MemoryStore) Admit(ctx context.Context, req AdmitRequest) (AdmitResult, error) {
	now := s.clock.Now()
	key := compositeKey(req.Tenant, req.Iface)

	s.mu.Lock()
	defer s.mu.Unlock()

	idem := attemptKey{reqID: req.RequestID, attempt: req.Attempt}
	if req.RequestID != "" {
		if cached, ok := s.admitResults[idem]; ok {
			cr := cached.res
			if cr.Admitted && cr.Token != "" {
				if _, stillHeld := s.leases[cr.Token]; !stillHeld {
					// 该幂等键对应的名额已释放（正常 Release 或 TTL 回收）。
					// 绝不返回一个指向已失效凭证的“成功”结果，否则调用方后续
					// Release 是 no-op，会误以为占用仍在。返回明确的复用拒绝。
					return AdmitResult{
						Admitted:      false,
						Reason:        ReasonRequestReused,
						ConfigVersion: cr.ConfigVersion,
						RetryIn:       10 * time.Millisecond,
					}, nil
				}
			}
			return cr, nil
		}
	}

	res := AdmitResult{ConfigVersion: s.configVersionLocked(), Active: s.active[key]}

	// 刷新/记录排队身份（含时间戳，用于 TTL 回收与观测）。
	if _, exists := s.demand[key]; !exists {
		s.demand[key] = now.UnixMilli()
	}
	s.keyInfo[key] = keyMeta{groupName: req.GroupName, weight: req.Weight}

	verdict := s.decideLocked(req, key)
	res.Reason = verdict.reason
	res.Active = s.active[key]
	if verdict.retryMillis > 0 {
		res.RetryIn = time.Duration(verdict.retryMillis) * time.Millisecond
	}
	if !verdict.admitted {
		if req.RequestID != "" {
			s.admitResults[idem] = cachedAdmit{res: res, atMilli: now.UnixMilli()}
		}
		return res, nil
	}

	tok := s.newTokenLocked()
	rec := &leaseRecord{
		key:        key,
		groupName:  req.GroupName,
		reqID:      req.RequestID,
		sessionID:  "",
		acquiredAt: now.UnixMilli(),
	}
	s.leases[tok] = rec
	s.active[key]++
	if req.GroupName != "" {
		s.activeByGroup[req.GroupName]++
	}
	delete(s.demand, key) // 已获得名额，退出排队集合
	res.Admitted = true
	res.Token = tok
	res.Active = s.active[key]
	if req.RequestID != "" {
		s.admitResults[idem] = cachedAdmit{res: res, atMilli: now.UnixMilli()}
	}
	return res, nil
}

// decideLocked 在持锁状态下构造公平裁决输入并调用纯函数。
func (s *MemoryStore) decideLocked(req AdmitRequest, key string) admitVerdict {
	in := admitInput{
		key:           key,
		hardCap:       req.HardCap,
		groupName:     req.GroupName,
		groupCapacity: req.GroupCapacity,
		active:        map[string]int64{},
		waiters:       map[string]int64{},
		weights:       map[string]float64{},
	}
	if req.GroupName == "" {
		in.active[key] = s.active[key]
		return decideAdmit(in)
	}
	now := s.clock.Now().UnixMilli()
	for k, meta := range s.keyInfo {
		if meta.groupName != req.GroupName {
			continue
		}
		if n := s.active[k]; n > 0 {
			in.active[k] = n
		}
		if ts, ok := s.demand[k]; ok && now-ts <= demandTTL.Milliseconds() {
			in.waiters[k] = ts
		}
		w := meta.weight
		if w <= 0 {
			w = 1
		}
		in.weights[k] = w
	}
	// 申请者自身：确保权重、占用、排队身份都在输入中。
	w := req.Weight
	if w <= 0 {
		w = 1
	}
	in.weights[key] = w
	in.active[key] = s.active[key]
	if ts, ok := s.demand[key]; ok && now-ts <= demandTTL.Milliseconds() {
		in.waiters[key] = ts
	}
	return decideAdmit(in)
}

// Release 归还并发名额。未知/重复/乱序凭证均安全：幂等且绝不 panic。
func (s *MemoryStore) Release(ctx context.Context, token LeaseToken, reqID RequestID) (ReleaseResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if reqID != "" {
		if cached, ok := s.releaseResults[reqID]; ok {
			return cached.res, nil
		}
	}
	res := ReleaseResult{ConfigVersion: s.configVersionLocked()}
	rec, ok := s.leases[token]
	if !ok {
		// 未知凭证（重复释放、过期已回收、伪造）：no-op。
		if reqID != "" {
			s.releaseResults[reqID] = cachedRelease{res: res, atMilli: s.clock.Now().UnixMilli()}
		}
		return res, nil
	}
	delete(s.leases, token)
	s.active[rec.key]--
	if s.active[rec.key] <= 0 {
		delete(s.active, rec.key)
	}
	if rec.groupName != "" {
		s.activeByGroup[rec.groupName]--
		if s.activeByGroup[rec.groupName] <= 0 {
			delete(s.activeByGroup, rec.groupName)
		}
	}
	res.Released = true
	if reqID != "" {
		s.releaseResults[reqID] = cachedRelease{res: res, atMilli: s.clock.Now().UnixMilli()}
	}
	return res, nil
}

func (s *MemoryStore) newTokenLocked() LeaseToken {
	s.tokenN++
	var b [8]byte
	_, _ = rand.Read(b[:])
	return LeaseToken(hex.EncodeToString(b[:]) + "-" + strconv.FormatUint(s.tokenN, 10))
}

// ---- 会话管理（协调服务器后端使用；本地模式不注册、不过期） ----

// RegisterSession 注册实例会话。
func (s *MemoryStore) RegisterSession(id SessionID, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[id] = &sessionRecord{lastBeat: s.clock.Now().UnixMilli(), ttl: ttl}
}

// Heartbeat 续期会话；未知会话返回 false（调用方需重新注册）。
func (s *MemoryStore) Heartbeat(id SessionID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[id]
	if !ok {
		return false
	}
	sess.lastBeat = s.clock.Now().UnixMilli()
	return true
}

// BindLease 将会话绑定到既有租约（远程准入路径使用）。
func (s *MemoryStore) BindLease(token LeaseToken, sessionID SessionID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.leases[token]
	if !ok {
		return false
	}
	rec.sessionID = sessionID
	return true
}

func (s *MemoryStore) reapLoop() {
	defer s.wg.Done()
	interval := 200 * time.Millisecond
	ch, cancel := s.clock.NewTimer(interval)
	defer cancel()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ch:
			s.reap()
			ch, cancel = s.clock.NewTimer(interval)
		}
	}
}

// reap 回收：1) 过期会话持有的全部租约（实例崩溃/网络分区后的唯一释放路径）；
// 2) 超时未刷新的排队标记。幂等，可被并发安全调用。
func (s *MemoryStore) reap() {
	now := s.clock.Now().UnixMilli()
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, sess := range s.sessions {
		ttl := sess.ttl
		if ttl <= 0 {
			ttl = 30 * time.Second
		}
		if now-sess.lastBeat <= ttl.Milliseconds() {
			continue
		}
		// 会话死亡：释放其名下全部租约，且不写入 releaseResults
		// （原客户端的 Release 到达后按未知凭证 no-op 处理，天然幂等）。
		for tok, rec := range s.leases {
			if rec.sessionID != id {
				continue
			}
			delete(s.leases, tok)
			s.active[rec.key]--
			if s.active[rec.key] <= 0 {
				delete(s.active, rec.key)
			}
			if rec.groupName != "" {
				s.activeByGroup[rec.groupName]--
				if s.activeByGroup[rec.groupName] <= 0 {
					delete(s.activeByGroup, rec.groupName)
				}
			}
		}
		delete(s.sessions, id)
	}
	for k, ts := range s.demand {
		if now-ts > demandTTL.Milliseconds() {
			delete(s.demand, k)
		}
	}
	// 幂等表 TTL 回收。注意：凭证仍持有的准入记录不得清理，否则同一逻辑请求
	// 用旧 RequestID 重放可能再次占名额（重复计数）。
	for k, c := range s.admitResults {
		if now-c.atMilli <= idempotencyTTL.Milliseconds() {
			continue
		}
		if c.res.Admitted && c.res.Token != "" {
			if _, held := s.leases[c.res.Token]; held {
				continue
			}
		}
		delete(s.admitResults, k)
	}
	for k, c := range s.releaseResults {
		if now-c.atMilli > idempotencyTTL.Milliseconds() {
			delete(s.releaseResults, k)
		}
	}
	for k, c := range s.rateDedup {
		if now-c.atMilli > idempotencyTTL.Milliseconds() {
			delete(s.rateDedup, k)
		}
	}
}

// Close 停止后台回收。
func (s *MemoryStore) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.stopCh)
	s.mu.Unlock()
	s.wg.Wait()
	return nil
}
