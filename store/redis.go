package store

import (
	"context"
	"strconv"
	"sync"
	"time"
)

// RedisOptions 是 Redis 存储配置。
type RedisOptions struct {
	Addr        string
	Password    string
	DB          int
	KeyPrefix   string
	DialTimeout time.Duration
	PoolSize    int
}

// tryAcquireScript 原子完成：惰性补充/扣减速率令牌、清理过期并发占位、
// 判定并发上限并写入新占位。
//
// KEYS[1]=速率键(Hash: t=令牌数, l=上次补充毫秒时间戳)
// KEYS[2]=并发键(Hash: token -> 过期毫秒时间戳，0 表示不过期)
// ARGV: now_ms rate burst maxConcurrent token ttl_ms
// 返回 {status, retry_after_ms}，status: 0 成功 / 1 速率受限 / 2 并发受限。
const tryAcquireScript = `
local rk, ck = KEYS[1], KEYS[2]
local now = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])
local maxc = tonumber(ARGV[4])
local token = ARGV[5]
local ttl = tonumber(ARGV[6])

local holders = redis.call('HGETALL', ck)
local count = 0
local earliest = -1
for i = 1, #holders, 2 do
  local exp = tonumber(holders[i+1])
  if exp > 0 and exp <= now then
    redis.call('HDEL', ck, holders[i])
  else
    count = count + 1
    if exp > 0 and (earliest < 0 or exp < earliest) then earliest = exp end
  end
end

local function persist_rate(tokens, last)
  redis.call('HSET', rk, 't', tostring(tokens), 'l', tostring(last))
  redis.call('PEXPIRE', rk, 86400000)
end

if rate > 0 then
  local tokens = tonumber(redis.call('HGET', rk, 't'))
  local last = tonumber(redis.call('HGET', rk, 'l'))
  if tokens == nil then
    tokens = burst
    last = now
  end
  local elapsed = (now - last) / 1000.0
  if elapsed > 0 then
    tokens = math.min(burst, tokens + elapsed * rate)
    last = now
  end
  if tokens < 1 then
    persist_rate(tokens, last)
    local retry = math.ceil((1 - tokens) / rate * 1000)
    return {1, retry}
  end
  if maxc > 0 and count >= maxc then
    persist_rate(tokens, last)
    local retry = earliest - now
    if retry < 10 then retry = 10 end
    return {2, retry}
  end
  persist_rate(tokens - 1, last)
else
  if maxc > 0 and count >= maxc then
    local retry = earliest - now
    if retry < 10 then retry = 10 end
    return {2, retry}
  end
end

if maxc > 0 then
  local exp = 0
  if ttl > 0 then exp = now + ttl end
  redis.call('HSET', ck, token, tostring(exp))
end
return {0, 0}
`

// releaseScript 幂等释放一个并发占位。
//
// 仅当 token 仍是该键内的有效占位时才删除；重复释放、乱序释放（占位已
// 因 TTL 过期被清理或已被新逻辑替代）均为无副作用的空操作。
const releaseScript = `
local ck = KEYS[1]
local token = ARGV[1]
local removed = redis.call('HDEL', ck, token)
if redis.call('HLEN', ck) == 0 then redis.call('DEL', ck) end
return removed
`

// RedisStore 通过 Redis Lua 脚本实现跨实例原子配额。
//
// 多实例语义：所有配额判定都在 Redis 单线程内以单个 Lua 脚本原子执行，
// 因此实例增减、重启或短暂不可达都不会放大额度；并发占位带 TTL，
// 崩溃实例遗留的占位会被自动回收，不会误伤正常请求。
type RedisStore struct {
	cl          *redisClient
	shaTry      string
	shaRelease  string
	scriptMu    sync.Mutex
	loaded      bool
	dialTimeout time.Duration
}

// NewRedisStore 创建 Redis 存储，连接失败时返回错误（由上层决定降级策略）。
func NewRedisStore(opts RedisOptions) (*RedisStore, error) {
	dialTO := opts.DialTimeout
	if dialTO <= 0 {
		dialTO = 500 * time.Millisecond
	}
	cl := newRedisClient(opts.Addr, opts.Password, opts.DB, opts.KeyPrefix, dialTO, opts.PoolSize)
	ctx, cancel := context.WithTimeout(context.Background(), dialTO*2)
	defer cancel()
	if err := cl.ping(ctx); err != nil {
		_ = cl.close()
		return nil, err
	}
	return &RedisStore{cl: cl, dialTimeout: dialTO}, nil
}

func (s *RedisStore) ensureScripts(ctx context.Context) error {
	s.scriptMu.Lock()
	defer s.scriptMu.Unlock()
	if s.loaded {
		return nil
	}
	sha1, err := func() (string, error) {
		v, err := s.cl.do(ctx, "SCRIPT", "LOAD", tryAcquireScript)
		if err != nil {
			return "", err
		}
		return strReply(v)
	}()
	if err != nil {
		return err
	}
	sha2, err := func() (string, error) {
		v, err := s.cl.do(ctx, "SCRIPT", "LOAD", releaseScript)
		if err != nil {
			return "", err
		}
		return strReply(v)
	}()
	if err != nil {
		return err
	}
	s.shaTry, s.shaRelease, s.loaded = sha1, sha2, true
	return nil
}

func (s *RedisStore) TryAcquire(ctx context.Context, rateKey, concKey string, rate float64, burst, maxConcurrent int64, token string, ttl time.Duration) (Result, error) {
	if err := s.ensureScripts(ctx); err != nil {
		return Result{}, err
	}
	args := []string{
		strconv.FormatInt(time.Now().UnixMilli(), 10),
		strconv.FormatFloat(rate, 'f', -1, 64),
		strconv.FormatInt(burst, 10),
		strconv.FormatInt(maxConcurrent, 10),
		token,
		strconv.FormatInt(ttl.Milliseconds(), 10),
	}
	v, err := s.cl.eval(ctx, s.shaTry, tryAcquireScript,
		[]string{s.cl.key(rateKey), s.cl.key(concKey)}, args)
	if err != nil {
		// 主从切换/重启可能清空脚本缓存：下次自动重新 LOAD。
		if isNoScript(err) {
			s.scriptMu.Lock()
			s.loaded = false
			s.scriptMu.Unlock()
		}
		return Result{}, err
	}
	arr, ok := v.([]any)
	if !ok || len(arr) != 2 {
		return Result{}, errRedisProto
	}
	status, err := atoiReply(arr[0])
	if err != nil {
		return Result{}, err
	}
	retryMS, err := atoiReply(arr[1])
	if err != nil {
		return Result{}, err
	}
	res := Result{RetryAfter: time.Duration(retryMS) * time.Millisecond}
	switch status {
	case 0:
		res.Status = StatusAcquired
		res.Token = token
	case 1:
		res.Status = StatusRateLimited
	default:
		res.Status = StatusConcurrencyLimited
	}
	return res, nil
}

func (s *RedisStore) Release(ctx context.Context, concKey, token string) error {
	if err := s.ensureScripts(ctx); err != nil {
		return err
	}
	_, err := s.cl.eval(ctx, s.shaRelease, releaseScript,
		[]string{s.cl.key(concKey)}, []string{token})
	if err != nil && isNoScript(err) {
		s.scriptMu.Lock()
		s.loaded = false
		s.scriptMu.Unlock()
	}
	return err
}

func (s *RedisStore) Ping(ctx context.Context) error { return s.cl.ping(ctx) }

func (s *RedisStore) Close() error { return s.cl.close() }
