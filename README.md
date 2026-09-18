# quota-gateway

为现有服务增加的一层**多租户配额与公平调度**能力。支持：

- 按 **租户 × 接口** 维度配置平均速率（令牌/秒）、突发容量（burst）与并发上限；
- **多实例共享**同一套额度与并发占用（通过无外部依赖的协调服务）；
- 容量竞争时按**权重比例**分享，闲置容量可临时借用并在原租户需要时归还；
- 配额依赖不可用时在 **fail-open / fail-closed** 间做可解释降级；
- 配置**热更新**与版本校验，非法/不兼容配置不影响线上；
- 长连接在正常结束、超时、客户端中断、panic 等所有路径下安全释放。

## 架构

```
                 ┌─────────────────────────────┐
   请求 ──► 业务服务内嵌 quota.Gateway
                 │  1) 速率桶 (rate/burst)     │
                 │  2) 并发准入 + 加权公平调度  │
                 └──────────────┬──────────────┘
                单实例: MemoryStore (进程内)
                多实例: RemoteStore ──HTTP──► coordserver (MemoryStore)
                                            会话 TTL / 幂等表 / 租约回收
```

- **单实例（`mode: local`）**：直接用进程内 `MemoryStore`，零外部依赖。
- **多实例（`mode: distributed`）**：所有网关节点通过 HTTP 连到 `coordserver`，
  由其单点串行化全部准入裁决，保证集群范围内速率、并发、公平份额严格一致。

详细语义见 [`docs/quota-semantics.md`](docs/quota-semantics.md)，
降级策略见 [`docs/degradation.md`](docs/degradation.md)，
本地验证见 [`docs/local-verification.md`](docs/local-verification.md)。

## 快速开始

### 1. 单实例（进程内）

```go
cfg := &quota.Config{
    APIVersion: quota.APIVersion, // "v1"
    Version:    1,
    Mode:       quota.ModeLocal,
    Degrade:    quota.DegradeFailClosed,
    Tenants: map[string]map[string]quota.TenantLimit{
        "tenant-a": {
            "/v1/chat": {RatePerSec: 20, Burst: 40, MaxConcurrent: 10, Weight: 1},
        },
    },
}
gw, _ := quota.NewGateway(cfg, quota.Options{})
defer gw.Close()

lease, err := gw.Acquire(ctx, "tenant-a", "/v1/chat")
if err != nil {
    var qe *quota.QuotaError
    if errors.As(err, &qe) {
        // qe.Reason: rate_limited | concurrent_limit | fairness_wait |
        //            dependency_down | bad_config | request_reused
        // qe.RetryIn: 建议重试等待
    }
    return err
}
defer lease.Release() // 必须在所有结束路径（含超时/中断/panic）调用
```

> 未在配置中出现的租户/接口**完全放行、不计数**，不会被误伤。

### 2. 多实例

```bash
# 启动协调服务（可带初始配置）
go run ./cmd/coordserver -addr :18000 -config configs/example.json
```

各业务实例以 distributed 模式接入同一地址：

```go
gw, _ := quota.NewGateway(cfg, quota.Options{
    Mode:      quota.ModeDistributed,
    BaseURL:   "http://coord-host:18000",
    InstanceID: "gateway-pod-7", // 可选，默认自动生成
})
```

热更新配置（校验失败保持现状）：

```go
// 直接在任一实例上调用，或 POST 到协调服务
err := gw.ReplaceConfig(ctx, newCfg)

// 或：
// curl -XPOST coord-host:18000/quota/v1/config -d @new-config.json
```

## 拒绝错误与重试提示

所有拒绝都返回 `*quota.QuotaError`，包含稳定可机读的 `Reason` 与 `RetryIn`：

| Reason               | 含义                       | 客户端建议            |
| -------------------- | -------------------------- | --------------------- |
| `rate_limited`       | 超过平均速率或突发容量     | 等待 `RetryIn` 后重试 |
| `concurrent_limit`   | 超过租户/接口并发硬上限    | 稍后重试，降并发      |
| `fairness_wait`      | 容量竞争，公平排队预算耗尽 | 带退避重试            |
| `dependency_down`    | 配额依赖不可用且 fail-closed | 稍后重试            |
| `bad_config`         | 配置非法/不兼容（管控面）  | 修正配置，不要重试    |
| `request_reused`     | 幂等键被复用（通常是 bug） | 生成新请求 ID         |

## 本地验证

```bash
go test ./...                      # 全部单元/集成测试
CGO_ENABLED=1 go test ./... -race  # 带竞态检测（推荐）
go vet ./...
go run ./cmd/coordserver -addr :18000
```

测试覆盖：并发竞争硬上限、幂等重试不双计数、加权份额与闲置借用/归还、
长连接四类结束路径释放、重复/乱序释放、会话 TTL 崩溃回收、
多实例一致性、配置热更新成功/失败、fail-open/fail-closed 降级。
