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

# 竞态检测（推荐，需要 cgo）；并发交错类测试建议多跑几遍
CGO_ENABLED=1 go test -race -count=1 ./...
CGO_ENABLED=1 go test -race -count=3 ./quota/ ./middleware/

# 仅跑某一场景
go test -count=1 ./quota/   -run TestAcquire
go test -count=1 ./store/   -run TestRedisStore_MultiInstanceConsistency
go test -count=1 ./store/   -run TestMemoryStore_TTLReclaimsDeadHolders
go test -count=1 ./quota/   -run TestAcquire_LongRequestLeaseRenewedNoEarlyReclaim
go test -count=1 ./quota/   -run TestRelease_HighConcurrencyInterleavingsNoLeak
go test -count=1 ./quota/   -run TestDegradeAllow
go test -count=1 ./quota/   -run TestHotReload_DuringLongConnections
go test -count=1 ./middleware/ -run TestMiddleware_LongConnection
```

测试覆盖的关键场景：

| 场景 | 测试 |
| --- | --- |
| 突发耗尽、按速率精确恢复、不误伤 | `store.TestMemoryStore_RateBurstAndRecovery`、`quota.TestAcquire_AllowThenRateLimitWithRetryHint`、`..._RateRecoveryServesAgain` |
| 高并发下额度不放大 | `store.TestMemoryStore_ConcurrentAcquireNoAmplification`、`store.TestRedisStore_MultiInstanceConsistency`（5 实例×60 请求） |
| 共享池加权公平、可排空、不超发 | `quota.TestFairScheduler_*`、`quota.TestAcquire_SharedPoolDrainsFairly`、`..._WeightsDetermineOrder` |
| 长连接正常/超时/客户端中断释放、重复与乱序释放 | `quota.TestAcquire_LongConnectionReleasePaths`、`middleware.TestMiddleware_LongConnectionReleasesOnClientInterrupt` |
| **长连接租约持续续租、名额不被提前回收（远超 TTL）** | `store.TestMemoryStore_RenewKeepsLongHolderAlive`、`store.TestRedisStore_Renew*`、`quota.TestAcquire_LongRequestLeaseRenewedNoEarlyReclaim` |
| **多实例共享后端：续租期间他实例不超额** | `store.TestRedisStore_RenewMultiInstanceNoOverAdmit` |
| **实例崩溃/强杀后名额在一个 TTL 内有界回收** | `store.TestMemoryStore_TTLReclaimsDeadHolders`、`store.TestMemoryStore_RenewStopBoundedReclaim`、`quota.TestAcquire_DeadInstanceBoundedReclaim` |
| **凭证在后端丢失后的保守记账（不放大、自愈合）** | `quota.TestLeaseLost_LocalSlotHeldUntilRequestEnds` |
| **短暂后端抖动（< TTL）续租自动恢复** | `quota.TestAcquire_RenewSurvivesBackendOutage` |
| **释放路径高循环×高并发交错不崩溃、不泄漏（竞态检测）** | `quota.TestRelease_HighConcurrencyInterleavingsNoLeak`、`middleware.TestMiddleware_DisconnectFinishInterleaveNoPanic`、`..._HandlerPanicReleasesSlot` |
| 排队客户端取消不留幽灵占位 | `quota.TestAcquire_ClientCancelWhileQueuedDoesNotLeak` |
| 配置非法/不兼容被拒绝且保留旧配置 | `quota.TestUpdateConfig_InvalidKeepsOldConfig`、`quota.TestParseConfig_Invalid` |
| **长连接在途期间热更新：不误回收/不重复占用、TTL 热更生效** | `quota.TestHotReload_DuringLongConnections` |
| 后端不可用 fail-open / fail-closed | `quota.TestAcquire_DegradeDenyAndAllow`、`middleware.TestMiddleware_Degrade*` |
| **降级放行的本地并发硬上限、拒绝原因、回收** | `quota.TestDegradeAllow_LocalCapEnforcedAndReclaimed`、`middleware.TestMiddleware_DegradeAllowLocalCap429` |
| **降级恢复过渡期不放大、不重复计数** | `quota.TestDegradeAllow_RecoveryNoAmplificationNoDoubleCount` |
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
`-lease-ttl`（并发占位租约 TTL，即实例崩溃后名额的有界回收时间；
默认 30s，可被配置文件 `concurrencyTTLSeconds` 覆盖，范围 1–3600s）。
续租周期自动取 `TTL/3`（100ms–10s），无需也不建议手工调整。

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
