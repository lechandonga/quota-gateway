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

// tryAcquireScript 原子完成：惰性补充/扣减速率令牌、清理过期占位、
// 判定租户并发硬上限、判定共享容量池并写入占位。
//
// KEYS[1]=速率键(Hash: t=令牌数, l=上次补充毫秒时间戳)
// KEYS[2]=租户并发键(Hash: token -> 过期毫秒, 0=不过期)
// KEYS[3]=共享容量键(同结构；空串占位 "_" 表示不启用)
// ARGV: now_ms rate burst max_tenant max_shared token ttl_ms
// 返回 {status, retry_after_ms}：
//
//	0 成功 / 1 速率受限 / 2 租户并发硬上限 / 3 共享池占满(可排队)
const tryAcquireScript = `
local rk, ck, sk = KEYS[1], KEYS[2], KEYS[3]
local now = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])
local maxt = tonumber(ARGV[4])
local maxs = tonumber(ARGV[5])
local token = ARGV[6]
local ttl = tonumber(ARGV[7])

local function count_alive(key)
  local holders = redis.call('HGETALL', key)
  local count, earliest = 0, -1
  for i = 1, #holders, 2 do
    local exp = tonumber(holders[i+1])
    if exp > 0 and exp <= now then
      redis.call('HDEL', key, holders[i])
    else
      count = count + 1
      if exp > 0 and (earliest < 0 or exp < earliest) then earliest = exp end
    end
  end
  return count, earliest
end

local function retry_from(earliest)
  local retry = earliest - now
  if retry < 10 then retry = 10 end
  return retry
end

local function persist_rate(tokens, last)
  redis.call('HSET', rk, 't', tostring(tokens), 'l', tostring(last))
  redis.call('PEXPIRE', rk, 86400000)
end

local function commit_hold(key, exp)
  redis.call('HSET', key, token, tostring(exp))
end

-- 1) 速率判定
local rate_tokens, have_rate = nil, false
if rate > 0 then
  rate_tokens = tonumber(redis.call('HGET', rk, 't'))
  local last = tonumber(redis.call('HGET', rk, 'l'))
  if rate_tokens == nil then
    rate_tokens = burst
    last = now
  end
  local elapsed = (now - last) / 1000.0
  if elapsed > 0 then
    rate_tokens = math.min(burst, rate_tokens + elapsed * rate)
    last = now
  end
  if rate_tokens < 1 then
    persist_rate(rate_tokens, last)
    return {1, math.ceil((1 - rate_tokens) / rate * 1000)}
  end
  have_rate = true
end

-- 2) 租户并发硬上限：立即拒绝
local tcount, tearliest = count_alive(ck)
if maxt > 0 and tcount >= maxt then
  if have_rate then persist_rate(rate_tokens, now) end
  return {2, retry_from(tearliest)}
end

-- 3) 共享容量池：可公平排队
if sk ~= '_' and maxs > 0 then
  local scount, searliest = count_alive(sk)
  if scount >= maxs then
    if have_rate then persist_rate(rate_tokens, now) end
    return {3, retry_from(searliest)}
  end
end

-- 4) 提交：扣令牌 + 占位（同一脚本内原子完成）
if have_rate then persist_rate(rate_tokens - 1, now) end
local exp = 0
if ttl > 0 then exp = now + ttl end
if maxt > 0 then commit_hold(ck, exp) end
if sk ~= '_' and maxs > 0 then commit_hold(sk, exp) end
return {0, 0}
`

// releaseScript 幂等释放一个 token 在一个键上的占位。
// 仅当 token 仍存在时删除；重复/乱序释放为无副作用空操作。
const releaseScript = `
local removed = redis.call('HDEL', KEYS[1], ARGV[1])
if redis.call('HLEN', KEYS[1]) == 0 then redis.call('DEL', KEYS[1]) end
return removed
`

// renewScript 批量续租：仅刷新仍存在的占位（HEXISTS 守卫），
// 已过期/已释放的占位不复活，防止名额被重复计入。
// KEYS=占位键列表；ARGV: now_ms ttl_ms token1..tokenN；
// 返回数组：{刷新的字段数, 缺失token1, 缺失token2, ...}。
// 缺失判定：token 在任一 KEYS 上不存在即为缺失（可能是正常释放与续租
// 并发交错，也可能是后端记录中途丢失；由调用方按本地租约状态区分）。
const renewScript = `
local now = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local renewed = 0
local missing = {}
if ttl > 0 then
  local exp = tostring(now + ttl)
  for i = 3, #ARGV do
    local tok = ARGV[i]
    local lost = false
    for k = 1, #KEYS do
      if redis.call('HEXISTS', KEYS[k], tok) == 1 then
        redis.call('HSET', KEYS[k], tok, exp)
        renewed = renewed + 1
      else
        lost = true
      end
    end
    if lost then table.insert(missing, tok) end
  end
end
return {renewed, unpack(missing)}
`

// reclaimScript 在“持有方存活但后端记录丢失”时按当前上限原子补占：
// 不触碰速率令牌；先检查所有启用键的有效占位数（惰性清理过期占位），
// 任一键达到上限则整体不写入并返回受限状态；全部有空位时写入同一个
// token，保证补占后总数严格不超过上限（不放大额度）。
// KEYS[1]=租户并发键（"_" 表示该规则不设租户上限，仅受共享池约束）；
// KEYS[2]=共享容量键（"_" 表示不启用）。
// ARGV: now_ms max_tenant max_shared token ttl_ms
// 返回 0=已补占（或 token 本就在）、2=租户上限、3=共享池上限。
const reclaimScript = `
local ck, sk = KEYS[1], KEYS[2]
local now = tonumber(ARGV[1])
local maxt = tonumber(ARGV[2])
local maxs = tonumber(ARGV[3])
local token = ARGV[4]
local ttl = tonumber(ARGV[5])

local function count_alive(key)
  local holders = redis.call('HGETALL', key)
  local count = 0
  for i = 1, #holders, 2 do
    local exp = tonumber(holders[i+1])
    if exp > 0 and exp <= now then
      redis.call('HDEL', key, holders[i])
    else
      count = count + 1
    end
  end
  return count
end

if ck ~= '_' and maxt > 0 and count_alive(ck) >= maxt then return 2 end
if sk ~= '_' and maxs > 0 and count_alive(sk) >= maxs then return 3 end
local exp = 0
if ttl > 0 then exp = now + ttl end
if ck ~= '_' then redis.call('HSET', ck, token, tostring(exp)) end
if sk ~= '_' and maxs > 0 then
  redis.call('HSET', sk, token, tostring(exp))
end
return 0
`

// RedisStore 通过 Redis Lua 脚本实现跨实例原子配额。
//
// 多实例语义：所有判定在 Redis 单线程内以单个 Lua 脚本原子执行，
// 实例增减、重启或短暂不可达都不会放大额度；占位带 TTL，崩溃实例的
// 遗留占位自动回收，不会误伤正常请求。
type RedisStore struct {
	cl          *redisClient
	shaTry      string
	shaRelease  string
	shaRenew    string
	shaReclaim  string
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
	v, err := s.cl.do(ctx, "SCRIPT", "LOAD", tryAcquireScript)
	if err != nil {
		return err
	}
	sha1, err := strReply(v)
	if err != nil {
		return err
	}
	v, err = s.cl.do(ctx, "SCRIPT", "LOAD", releaseScript)
	if err != nil {
		return err
	}
	sha2, err := strReply(v)
	if err != nil {
		return err
	}
	v, err = s.cl.do(ctx, "SCRIPT", "LOAD", renewScript)
	if err != nil {
		return err
	}
	sha3, err := strReply(v)
	if err != nil {
		return err
	}
	v, err = s.cl.do(ctx, "SCRIPT", "LOAD", reclaimScript)
	if err != nil {
		return err
	}
	sha4, err := strReply(v)
	if err != nil {
		return err
	}
	s.shaTry, s.shaRelease, s.shaRenew, s.shaReclaim, s.loaded = sha1, sha2, sha3, sha4, true
	return nil
}

func (s *RedisStore) TryAcquire(ctx context.Context, req AcquireRequest) (Result, error) {
	if err := s.ensureScripts(ctx); err != nil {
		return Result{}, err
	}
	sharedKey := req.SharedKey
	redisSharedKey := "_"
	if sharedKey != "" && req.MaxShared > 0 {
		redisSharedKey = s.cl.key(sharedKey)
	}
	args := []string{
		strconv.FormatInt(time.Now().UnixMilli(), 10),
		strconv.FormatFloat(req.Rate, 'f', -1, 64),
		strconv.FormatInt(req.Burst, 10),
		strconv.FormatInt(req.MaxConcurrent, 10),
		strconv.FormatInt(req.MaxShared, 10),
		req.Token,
		strconv.FormatInt(req.TTL.Milliseconds(), 10),
	}
	v, err := s.cl.eval(ctx, s.shaTry, tryAcquireScript,
		[]string{s.cl.key(req.RateKey), s.cl.key(req.ConcKey), redisSharedKey}, args)
	if err != nil {
		if isNoScript(err) { // 主从切换/重启清空脚本缓存：下次自动重新 LOAD
			s.markStale()
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
	res := Result{RetryAfter: time.Duration(retryMS) * time.Millisecond, Token: req.Token}
	switch status {
	case 0:
		res.Status = StatusAcquired
	case 1:
		res.Status = StatusRateLimited
	case 2:
		res.Status = StatusConcurrencyLimited
	default:
		res.Status = StatusSharedLimited
	}
	return res, nil
}

func (s *RedisStore) Release(ctx context.Context, keys []string, token string) error {
	if err := s.ensureScripts(ctx); err != nil {
		return err
	}
	for _, k := range keys {
		if k == "" {
			continue
		}
		_, err := s.cl.eval(ctx, s.shaRelease, releaseScript,
			[]string{s.cl.key(k)}, []string{token})
		if err != nil {
			if isNoScript(err) {
				s.markStale()
			}
			return err
		}
	}
	return nil
}

// Renew 批量续租：单次 EVAL 完成一组键上多个 token 的过期时间刷新，
// 并返回在任一占位键上已不存在的 token 列表（供丢失感知与补占）。
func (s *RedisStore) Renew(ctx context.Context, keys []string, tokens []string, ttl time.Duration) ([]string, error) {
	if len(keys) == 0 || len(tokens) == 0 || ttl <= 0 {
		return nil, nil
	}
	if err := s.ensureScripts(ctx); err != nil {
		return nil, err
	}
	redisKeys := make([]string, 0, len(keys))
	for _, k := range keys {
		if k == "" {
			continue
		}
		redisKeys = append(redisKeys, s.cl.key(k))
	}
	if len(redisKeys) == 0 {
		return nil, nil
	}
	args := make([]string, 0, len(tokens)+2)
	args = append(args,
		strconv.FormatInt(time.Now().UnixMilli(), 10),
		strconv.FormatInt(ttl.Milliseconds(), 10),
	)
	args = append(args, tokens...)
	v, err := s.cl.eval(ctx, s.shaRenew, renewScript, redisKeys, args)
	if err != nil {
		if isNoScript(err) {
			s.markStale()
		}
		return nil, err
	}
	arr, ok := v.([]any)
	if !ok || len(arr) == 0 {
		return nil, errRedisProto
	}
	missing := make([]string, 0, len(arr)-1)
	for _, item := range arr[1:] {
		tok, err := strReply(item)
		if err != nil {
			return nil, err
		}
		missing = append(missing, tok)
	}
	return missing, nil
}

// Reclaim 以一段原子 Lua 脚本按当前上限补占丢失的 token（不扣速率令牌）。
func (s *RedisStore) Reclaim(ctx context.Context, req ReclaimRequest) (Status, error) {
	if err := s.ensureScripts(ctx); err != nil {
		return 0, err
	}
	redisConcKey := "_"
	if req.ConcKey != "" && req.MaxConcurrent > 0 {
		redisConcKey = s.cl.key(req.ConcKey)
	}
	redisSharedKey := "_"
	if req.SharedKey != "" && req.MaxShared > 0 {
		redisSharedKey = s.cl.key(req.SharedKey)
	}
	args := []string{
		strconv.FormatInt(time.Now().UnixMilli(), 10),
		strconv.FormatInt(req.MaxConcurrent, 10),
		strconv.FormatInt(req.MaxShared, 10),
		req.Token,
		strconv.FormatInt(req.TTL.Milliseconds(), 10),
	}
	v, err := s.cl.eval(ctx, s.shaReclaim, reclaimScript,
		[]string{redisConcKey, redisSharedKey}, args)
	if err != nil {
		if isNoScript(err) {
			s.markStale()
		}
		return 0, err
	}
	code, err := atoiReply(v)
	if err != nil {
		return 0, err
	}
	switch code {
	case 0:
		return StatusAcquired, nil
	case 2:
		return StatusConcurrencyLimited, nil
	default:
		return StatusSharedLimited, nil
	}
}

func (s *RedisStore) markStale() {
	s.scriptMu.Lock()
	s.loaded = false
	s.scriptMu.Unlock()
}

func (s *RedisStore) Ping(ctx context.Context) error { return s.cl.ping(ctx) }

func (s *RedisStore) Close() error { return s.cl.close() }
