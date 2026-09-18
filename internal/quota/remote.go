package quota

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// RemoteStore 是 CoordinatorServer 的 HTTP 客户端，实现 Store。
//
// 多实例正确性依赖以下三条：
//  1. 准入/释放携带调用方生成的 RequestID，协调端幂等表保证网络重试
//     （请求已执行但响应丢失）不会重复占名额或重复归还；
//  2. 每次取令牌只在“确认未到达服务端”的传输失败上重试，避免一次请求扣两次；
//  3. 实例会话有 TTL，心跳续约；实例崩溃/网络分区后其全部占用由
//     协调端按 TTL 回收，额度不会被放大也不会永久泄漏。
type RemoteStore struct {
	baseURL     string
	instanceID  string
	sessionKey  SessionID // 本次进程运行的唯一会话标识（实例重启即换新）
	clock       Clock
	httpClient  *http.Client
	ttlMillis   int64
	hbMillis    int64
	callTimeout time.Duration

	mu         sync.Mutex
	closed     bool
	stopCh     chan struct{}
	wg         sync.WaitGroup
	registered atomic.Bool
}

// RemoteOptions 为远程存储选项。
type RemoteOptions struct {
	BaseURL     string
	InstanceID  string
	Clock       Clock
	SessionTTL  int64 // millis，0 使用默认
	Heartbeat   int64 // millis，0 使用默认
	CallTimeout int64 // millis，0 使用默认
}

// NewRemoteStore 创建客户端并向协调服务器注册会话。
func NewRemoteStore(ctx context.Context, opts RemoteOptions) (*RemoteStore, error) {
	clock := opts.Clock
	if clock == nil {
		clock = SystemClock()
	}
	ttl := time.Duration(opts.SessionTTL) * time.Millisecond
	if ttl <= 0 {
		ttl = defaultSessionTTL
	}
	hb := time.Duration(opts.Heartbeat) * time.Millisecond
	if hb <= 0 {
		hb = defaultHeartbeat
	}
	callTO := time.Duration(opts.CallTimeout) * time.Millisecond
	if callTO <= 0 {
		callTO = defaultCallTimeout
	}
	inst := opts.InstanceID
	if inst == "" {
		inst = "instance"
	}
	var rb [8]byte
	_, _ = rand.Read(rb[:])
	s := &RemoteStore{
		baseURL:     opts.BaseURL,
		instanceID:  inst,
		sessionKey:  SessionID(inst + "-" + strconv.FormatInt(clock.Now().UnixNano(), 36) + "-" + fmt.Sprintf("%x", rb[:4])),
		clock:       clock,
		httpClient:  &http.Client{Timeout: callTO},
		ttlMillis:   ttl.Milliseconds(),
		hbMillis:    hb.Milliseconds(),
		callTimeout: callTO,
		stopCh:      make(chan struct{}),
	}
	if err := s.registerWithRetry(ctx); err != nil {
		return nil, err
	}
	s.wg.Add(1)
	go s.heartbeatLoop()
	return s, nil
}

func (s *RemoteStore) registerWithRetry(ctx context.Context) error {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		body := wireSession{InstanceID: string(s.sessionKey), TTLMillis: s.ttlMillis}
		code, _, err := s.doPost(ctx, pathSession, body, nil, false)
		if err == nil && code == http.StatusOK {
			s.registered.Store(true)
			return nil
		}
		if err == nil {
			lastErr = fmt.Errorf("register session failed: http %d", code)
		} else {
			lastErr = err
		}
		if err := s.sleep(ctx, s.backoff(attempt)); err != nil {
			return err
		}
	}
	return fmt.Errorf("quota: remote store register failed after retries: %w", lastErr)
}

func (s *RemoteStore) heartbeatLoop() {
	defer s.wg.Done()
	interval := time.Duration(s.hbMillis) * time.Millisecond
	ch, cancel := s.clock.NewTimer(interval)
	defer cancel()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ch:
			ctx, cfn := context.WithTimeout(context.Background(), s.callTimeout)
			if !s.heartbeatOnce(ctx) {
				// 会话过期（协调端重启、TTL 超时、网络分区恢复）：
				// 重新注册。旧会话名下的占用若已被回收，重新注册是全新身份，
				// 不会重复计数；新请求从干净状态开始。
				_ = s.registerWithRetry(ctx)
			}
			cfn()
			ch, cancel = s.clock.NewTimer(interval)
		}
	}
}

func (s *RemoteStore) heartbeatOnce(ctx context.Context) bool {
	body := wireHeartbeat{InstanceID: string(s.sessionKey)}
	code, _, err := s.doPost(ctx, pathHeartbeat, body, nil, false)
	return err == nil && code == http.StatusOK
}

func (s *RemoteStore) backoff(attempt int) time.Duration {
	base := time.Duration(20*(1<<attempt)) * time.Millisecond
	jitter := time.Duration(rand.Int63n(int64(20 * time.Millisecond)))
	return base + jitter
}

func (s *RemoteStore) sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// SyncConfig 推送配置；协调端校验失败（4xx）原样返回，不做重试（重试无意义）。
func (s *RemoteStore) SyncConfig(ctx context.Context, cfg *Config) error {
	code, raw, err := s.doPost(ctx, pathConfig, cfg, nil, false)
	if err != nil {
		return fmt.Errorf("quota: sync config transport error: %w", err)
	}
	if code == http.StatusOK {
		return nil
	}
	var we wireError
	_ = json.Unmarshal(raw, &we)
	return newQuotaError(ReasonBadConfig, "coordinator rejected config: "+we.Error, 0)
}

// TryTakeRate 的重试规则：仅在“请求确定未到达服务端”（DNS/连接错误、
// context deadline 由客户端侧超时且无法确认）时谨慎重试。
// 由于取令牌失败本身不改变状态，而成功取令牌会扣减，我们对 5xx 重试，
// 对传输错误重试一次（保守，避免放大）；4xx 直接返回。
func (s *RemoteStore) TryTakeRate(ctx context.Context, ded RateDeduction) (RateResult, error) {
	body := wireRateReq{Tenant: ded.Key.Tenant, Iface: ded.Key.Iface, Want: ded.Want, DedupKey: ded.DedupKey}
	var out wireRateResp
	code, raw, err := s.callWithRetry(ctx, pathRateTake, body, &out)
	if err != nil {
		return RateResult{}, err
	}
	if code != http.StatusOK {
		var we wireError
		_ = json.Unmarshal(raw, &we)
		return RateResult{}, fmt.Errorf("rate take failed: %s", we.Error)
	}
	return RateResult{
		Allowed:       out.Allowed,
		Remaining:     out.Remaining,
		RetryIn:       time.Duration(out.RetryMillis) * time.Millisecond,
		ConfigVersion: out.ConfigVersion,
	}, nil
}

func (s *RemoteStore) Admit(ctx context.Context, req AdmitRequest) (AdmitResult, error) {
	body := wireAdmitReq{
		Tenant:        req.Tenant,
		Iface:         req.Iface,
		RequestID:     string(req.RequestID),
		Attempt:       req.Attempt,
		HardCap:       req.HardCap,
		GroupName:     req.GroupName,
		GroupCapacity: req.GroupCapacity,
		Weight:        req.Weight,
		SessionID:     string(s.sessionKey),
	}
	var out wireAdmitResp
	code, raw, err := s.callWithRetry(ctx, pathAdmit, body, &out)
	if err != nil {
		return AdmitResult{}, err
	}
	if code != http.StatusOK {
		var we wireError
		_ = json.Unmarshal(raw, &we)
		return AdmitResult{}, fmt.Errorf("admit failed: %s", we.Error)
	}
	return AdmitResult{
		Admitted:      out.Admitted,
		Token:         LeaseToken(out.Token),
		Reason:        Reason(out.Reason),
		RetryIn:       time.Duration(out.RetryMillis) * time.Millisecond,
		Active:        out.Active,
		ConfigVersion: out.ConfigVersion,
	}, nil
}

func (s *RemoteStore) Release(ctx context.Context, token LeaseToken, reqID RequestID) (ReleaseResult, error) {
	body := wireReleaseReq{Token: string(token), RequestID: string(reqID)}
	var out wireReleaseResp
	code, raw, err := s.callWithRetry(ctx, pathRelease, body, &out)
	if err != nil {
		return ReleaseResult{}, err
	}
	if code != http.StatusOK {
		var we wireError
		_ = json.Unmarshal(raw, &we)
		return ReleaseResult{}, fmt.Errorf("release failed: %s", we.Error)
	}
	return ReleaseResult{Released: out.Released, ConfigVersion: out.ConfigVersion}, nil
}

// callWithRetry 在幂等键保护下处理瞬时故障：
// 传输错误与 502/503/504 指数退避重试（最多 3 次）；
// 4xx 立即返回（业务裁决或配置问题，重试无益）。
// Admit/Release 因携带 RequestID/Token，服务端保证重试安全。
func (s *RemoteStore) callWithRetry(ctx context.Context, path string, body, out any) (int, []byte, error) {
	var lastCode int
	var lastRaw []byte
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if ctx.Err() != nil {
			return 0, nil, ctx.Err()
		}
		code, raw, err := s.doPost(ctx, path, body, out, true)
		if err == nil {
			if code == http.StatusOK || code < 500 {
				return code, raw, nil
			}
			lastCode, lastRaw = code, raw
		} else {
			lastErr = err
		}
		if err := s.sleep(ctx, s.backoff(attempt)); err != nil {
			return 0, nil, err
		}
	}
	if lastErr != nil {
		return 0, nil, fmt.Errorf("quota: remote call %s failed after retries: %w", path, lastErr)
	}
	return lastCode, lastRaw, nil
}

// doPost 执行一次 HTTP POST。decodeOut 非空时把成功响应解码进去。
// 返回 (httpStatus, rawBody, transportErr)。
func (s *RemoteStore) doPost(ctx context.Context, path string, body any, decodeOut any, withSession bool) (int, []byte, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return 0, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.baseURL+path, bytes.NewReader(raw))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Quota-Api", APIVersion)
	if withSession {
		req.Header.Set("X-Instance-Id", string(s.sessionKey))
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if decodeOut != nil && resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(data, decodeOut); err != nil {
			return resp.StatusCode, data, fmt.Errorf("decode response: %w", err)
		}
	}
	return resp.StatusCode, data, nil
}

// Close 停止心跳。已占有的租约由协调端按会话 TTL 兜底回收，
// 因此即使 Close 时协调端不可达也不会泄漏。
func (s *RemoteStore) Close() error {
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
