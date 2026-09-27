# 本地验证指南

仅依赖 Go 标准库与 Go 工具链（Go 1.25+），无需第三方服务即可运行单元测试；
端到端的多实例验证可选使用 Docker 版 Redis。

## 1. 编译与静态检查

```bash
go build ./...
go vet ./...
gofmt -l .   # 无输出表示格式正确
```

## 2. 自动化测试

```bash
# 普通测试
go test -count=1 ./...

# 竞态检测（推荐，需要 cgo）
CGO_ENABLED=1 go test -race -count=1 ./...

# 仅跑某一场景
go test -count=1 ./quota/   -run TestAcquire
go test -count=1 ./quota/   -run TestLease_
go test -count=1 ./quota/   -run TestDegradeAllow_
go test -count=1 ./quota/   -run TestReclaim_
go test -count=1 ./store/   -run 'TestMemoryStore_Reclaim|TestRedisStore_RenewReclaim'
go test -count=1 ./store/   -run TestRedisStore_MultiInstanceConsistency
go test -count=1 ./middleware/ -run TestMiddleware_LongConnection

# 释放路径并发交错（建议在竞态检测下反复运行）
CGO_ENABLED=1 go test -race -count=10 ./quota/       -run 'TestRelease_|TestReclaim_Concurrent'
CGO_ENABLED=1 go test -race -count=10 ./middleware/  -run TestMiddleware_ConcurrentDisconnectStorm

# 名额丢失补占 + 降级/抖动交错（竞态检测下重复跑）
CGO_ENABLED=1 go test -race -count=5 ./quota/ -run 'TestLease_BackendRecordLoss|TestLease_ReclaimWaits|TestReclaim_DegradeFlapping'
```

测试覆盖的关键场景：

| 场景 | 测试 |
| --- | --- |
| 突发耗尽、按速率精确恢复、不误伤 | `store.TestMemoryStore_RateBurstAndRecovery`、`quota.TestAcquire_AllowThenRateLimitWithRetryHint`、`..._RateRecoveryServesAgain` |
| 高并发下额度不放大 | `store.TestMemoryStore_ConcurrentAcquireNoAmplification`、`store.TestRedisStore_MultiInstanceConsistency`（5 实例×60 请求） |
| 共享池加权公平、可排空、不超发 | `quota.TestFairScheduler_*`、`quota.TestAcquire_SharedPoolDrainsFairly`、`..._WeightsDetermineOrder` |
| 长连接名额不被提前回收（续租保活，远超 TTL） | `quota.TestLease_LongConnectionKeepsSlotBeyondTTL`、`store.TestMemoryStore_RenewKeepsLongConnectionSlot`、`store.TestRedisStore_RenewSemantics` |
| 实例崩溃后名额有界回收（含多实例） | `quota.TestLease_CrashRecoversSlotWithinBoundedTime`、`store.TestRedisStore_RenewMultiInstanceCrashRecovery`、`store.TestMemoryStore_TTLReclaimsDeadHolders` |
| 已释放名额不被续租复活 | `quota.TestLease_ReleasedSlotIsNotResurrected`、`store.TestMemoryStore_RenewDoesNotResurrect` |
| 断连租约后台有界回收 | `quota.TestLease_CanceledContextReapedInBackground`、`quota.TestDegradeAllow_DisconnectReclaimed` |
| 释放路径任意并发交错不崩溃、不泄漏（竞态检测） | `quota.TestRelease_ConcurrentInterleavingsNoCrashNoLeak`、`quota.TestRelease_BackendFlakyNoCrashTTLBackstop`、`middleware.TestMiddleware_ConcurrentDisconnectStorm` |
| 长连接正常/超时/客户端中断释放、重复与乱序释放 | `quota.TestAcquire_LongConnectionReleasePaths`、`middleware.TestMiddleware_LongConnectionReleasesOnClientInterrupt` |
| 降级放行的本地硬上限、恢复后不放大不重复计数 | `quota.TestDegradeAllow_LocalHardCapEnforced`、`..._RecoveryNoAmplificationNoDoubleCount` |
| 故障前已在途遇上放行仍不超上限、放行在途回收与恢复计账 | `quota.TestDegradeAllow_PreExistingInFlightPlusFailOpenNeverExceedsCap`、`..._DefaultCapIncludesPreExisting` |
| 较长请求在途时后端记录中途丢失：自动补占、正常释放、上限满等待恢复 | `quota.TestLease_BackendRecordLossIsReclaimed`、`..._ReclaimedLeaseReleasesNormally`、`..._ReclaimWaitsWhenCapFullThenRecovers` |
| 存储层丢失上报与按上限补占（内存 + 真实 Lua 路径） | `store.TestMemoryStore_RenewReportsMissingToken`、`..._ReclaimRefillsLostSlot`、`..._ReclaimRespectsCaps`、`store.TestRedisStore_RenewReclaimAfterBackendRecordLoss` |
| 高并发释放/断连/超时/补占/降级抖动交错不崩不泄漏（竞态） | `quota.TestReclaim_ConcurrentInterleavings`、`..._DegradeFlappingNoExceedNoLeak` |
| 长连接期间的热更新（不误回收/不重复占用、非法配置被拒） | `quota.TestHotUpdate_DuringLongConnection`、`..._RemoveRuleKeepsInFlightSafe` |
| 排队客户端取消不留幽灵占位 | `quota.TestAcquire_ClientCancelWhileQueuedDoesNotLeak` |
| 配置非法/不兼容被拒绝且保留旧配置 | `quota.TestUpdateConfig_InvalidKeepsOldConfig`、`quota.TestParseConfig_Invalid` |
| 后端不可用 fail-open / fail-closed | `quota.TestAcquire_DegradeDenyAndAllow`、`middleware.TestMiddleware_Degrade*` |
| 脚本缓存丢失自动回退 | `store.TestRedisStore_NoScriptFallback` |
| 健康状态翻转 | `quota.TestHealthMonitor_Transitions` |

> Redis 相关测试使用**进程内的假 Redis**（`store/fakeredis_test.go`，
> 用 Go 复现 Lua 脚本语义），因此无需安装 Redis 也能验证多实例一致性、
> RESP 协议、连接池与 NOSCRIPT 回退。

## 3. 启动示例服务

```bash
# 单实例（内存存储，仅用于本地）
go run ./cmd/quota-gateway -addr :8080 -config ./docs/config-example.json

# 多实例（共享 Redis；任选不同端口启动多个进程）
go run ./cmd/quota-gateway -addr :8080 -config ./docs/config-example.json -redis 127.0.0.1:6379
go run ./cmd/quota-gateway -addr :8081 -config ./docs/config-example.json -redis 127.0.0.1:6379
```

可选参数：`-degrade` 由配置文件控制；`-reload-interval`（配置轮询，
`<=0` 表示仅响应 SIGHUP）、`-health-interval`、`-key-prefix`、
`-concurrency-ttl`（占位 TTL，崩溃后有界回收）、`-renew-interval`
（在途续租周期，默认 TTL/3）、`-reap-interval`（断连回收周期，默认
100ms）、`-queue-timeout`、`-degrade-max-concurrent`（降级放行本地
硬上限兜底，默认 1024）。均为可选，缺省即有安全默认值。

接口：`/api/echo`、`/api/slow`（~150ms）、`/api/heavy`（~800ms）、
`/healthz`、`/debug/vars`。租户通过请求头 `X-Tenant-ID` 指定。

## 4. 手工冒烟（curl）

```bash
# 正常请求
curl -i -H 'X-Tenant-ID: team-alpha' http://127.0.0.1:8080/api/echo

# 连发超过 burst 个请求，观察 429 + Retry-After + X-Quota-Reason
for i in $(seq 1 50); do
  curl -s -o /dev/null -w '%{http_code}\n' -H 'X-Tenant-ID: team-beta' \
    http://127.0.0.1:8080/api/echo
done | sort | uniq -c

# 指标与健康
curl -s http://127.0.0.1:8080/debug/vars | python3 -m json.tool | less
curl -s http://127.0.0.1:8080/healthz

# 热更新：编辑配置后发送 SIGHUP，或等待轮询；非法配置会被拒绝且保留旧版本
kill -HUP <pid>
```

## 5. 用真实 Redis 做多实例验证（可选）

```bash
docker run --rm -p 6379:6379 redis:7
# 启动两个实例后，用压测工具以同一租户打两个端口，
# 观察在途总数恒等、429 行为一致，且重启其中一个实例不放大额度。
```
