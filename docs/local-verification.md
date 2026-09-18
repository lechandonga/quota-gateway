# 本地验证指南

本文档说明如何在本机完整验证多租户配额与公平调度能力（单实例 + 多实例）。

## 1. 环境要求

- Go 1.25+（开发于 1.26）
- 带竞态检测需启用 cgo（需要 gcc）：`export CGO_ENABLED=1`

## 2. 测试命令

```bash
# 全量自动化测试
CGO_ENABLED=1 go test ./... -race -count=1

# 普通模式
go test ./... -count=1

# 单测某个场景
go test ./internal/quota/ -run 'TestDistributed' -v
go test ./internal/quota/ -run 'TestGroupWeightedSteadyState' -v
go vet ./...
```

## 3. 测试场景对照表

| 场景                         | 测试/文件                            | 验证点                                    |
| ---------------------------- | ------------------------------------ | ----------------------------------------- |
| 突发打满后精确恢复           | `TestRateRecoveryOverTime`           | 用假时钟验证令牌按速率回填，RetryIn 可执行 |
| 速率超限可区分错误           | `TestRateLimitDistinguishableError`  | burst=5 恰放 5 个，第 6 个带 reason+retry |
| 并发硬上限及释放恢复         | `TestConcurrencyHardCap`             | 到顶立即拒绝，释放后立即恢复              |
| 64 goroutine 竞争            | `TestStoreHardCapUnderContention`    | 同一时刻占用绝不超 3，race 检测通过       |
| 网络重试不双占               | `TestStoreIdempotentAdmitRetries`    | 同 (RequestID,Attempt) 返回相同凭证       |
| 重复/乱序/未知释放           | `TestStoreReleaseOutOfOrderAndUnknown` | 全部 no-op 安全，最终占用归零           |
| 加权稳态 1:3 + 闲置借用/归还 | `TestGroupWeightedSteadyState`       | B 借满→A 加入→稳态 A=1,B=3               |
| 长连接四类结束路径           | `TestLongConnectionReleasedOnAllExitPaths` | 正常/超时/中断/panic 均正确释放      |
| 混乱释放不泄漏               | `TestNoLeakUnderMixedChaos`          | 100 并发占用 + 重复/乱序释放，残余 0      |
| 配置热更新失败不影响现状     | `TestConfigHotReloadChangesLimits`   | 非法配置被拒，当前版本/限额不变           |
| 多实例硬上限全局一致         | `TestDistributedHardCapAcrossInstances` | 两实例并发申请，集群在途绝不超 4      |
| 跨实例释放恢复               | `TestDistributedSlotsRecoveredAfterRelease` | 两实例各持，协调端计数=2，释放后归零 |
| 实例崩溃 TTL 回收            | `TestDistributedInstanceCrashReapsLeases` | 不 Release 直接 Close，400ms 后占用清零 |
| 协调端拒绝坏配置             | `TestDistributedCoordinatorRejectConfig` | 422 返回，线上保持 v1                  |
| 跨实例加权分享               | `TestDistributedWeightedSharing`     | B 借满 4，A 跨实例加入后收回份额 1        |
| fail-open / fail-closed      | `degrade_test.go`                    | 依赖全坏时拒绝或按本地硬上限放行          |
| 客户端中断公平等待           | `TestContextCancelWhileFairWaiting`  | 取消后立即返回，不留下悬挂占用            |

> 注：多实例测试通过 `httptest` 在进程内拉起真实 HTTP 协调服务，
> 因此无需启动额外进程或安装 Redis 等中间件即可验证分布式路径（含真实
> JSON 编解码、HTTP 重试、会话注册/心跳、TTL 回收）。

## 4. 手动验证协调服务

```bash
# 构建并启动
go build ./cmd/coordserver
./coordserver -addr :18000

# 健康检查
curl -s localhost:18000/quota/v1/healthz
# {"status":"ok","api":"v1"}

# 下发一份配置
curl -s -XPOST localhost:18000/quota/v1/config \
  -H 'Content-Type: application/json' -d @configs/example.json

# 非法配置应被拒绝（422），线上配置不变
curl -i -XPOST localhost:18000/quota/v1/config \
  -H 'Content-Type: application/json' \
  -d '{"apiVersion":"v1","version":99,"mode":"local","degrade":"bad"}'
```

## 5. 配置文件示例

见 [`../configs/example.json`](../configs/example.json)。关键字段：

```json
{
  "apiVersion": "v1",
  "version": 1,
  "mode": "distributed",
  "degrade": "fail-open",
  "sessionTtlMillis": 30000,
  "heartbeatMillis": 5000,
  "requestTimeoutMillis": 2000,
  "tenants": {
    "team-alpha": {
      "/v1/chat": { "ratePerSec": 20, "burst": 40, "maxConcurrent": 10, "weight": 1 }
    },
    "team-beta": {
      "/v1/chat": { "ratePerSec": 60, "burst": 120, "maxConcurrent": 30, "weight": 3 }
    }
  },
  "groups": {
    "llm-pool": { "name": "llm-pool", "capacity": 40, "defaultWeight": 1 }
  },
  "tenantGroups": {
    "team-alpha": { "/v1/chat": "llm-pool" },
    "team-beta":  { "/v1/chat": "llm-pool" }
  }
}
```

## 6. 故障演练建议

1. **实例崩溃**：启动两个 distributed 网关占满名额后 `kill -9` 其中一个，
   观察协调端在 `sessionTtlMillis` 后回收其占用，另一实例可申请到名额。
2. **协调端重启**：请求过程中重启 coordserver，观察：
   - fail-closed 实例收到 `dependency_down`；
   - fail-open 实例继续按本地硬上限服务；
   - 恢复后自动重新注册并回到正常路径。
3. **错误热更新**：下发一份非法或旧版本号配置，确认被拒绝且业务限额不变。
