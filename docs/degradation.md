# 降级策略说明

当配额依赖（多实例模式下的协调服务，或其网络链路）不可用时，网关需要在
“拒绝”与“放行”之间做出明确、可解释、可配置的选择，并且避免级联故障。

## 配置

```go
cfg.Degrade = quota.DegradeFailOpen   // 或 quota.DegradeFailClosed
```

| 策略          | 依赖故障时的行为                                        | 适用场景                     |
| ------------- | ------------------------------------------------------- ---------------------------- |
| `fail-closed` | 一律拒绝，返回 `dependency_down` + 重试提示             | 绝不能超额、额度优先于可用性 |
| `fail-open`   | 放行，但在**本进程内**仍按租户硬上限兜底计数            | 可用性优先、宁可短暂宽松     |

## fail-closed

- 速率或并发任一存储调用失败 → 返回 `*QuotaError{Reason: dependency_down}`，
  `RetryIn` 给出退避建议（默认 500ms）。
- 好处：状态不明时绝不多放一个请求，额度不会被放大。
- 代价：协调端故障期间该租户请求全部失败——因此要配合协调端高可用部署，
  或在可接受短暂不可用时选用。

## fail-open

- 存储调用失败时放行请求，同时在网关进程内维护一张**本地兜底并发表**，
  继续按 `MaxConcurrent` 限制单实例在途请求数，避免依赖宕机期间被打爆。
- 兜底名额通过返回的 fallback `Lease` 正常 `Release()` 归还，重复安全。
- 依赖恢复后无需特殊处理：后续请求自动回到正常分布式执行路径。
- **注意**：fail-open 期间速率桶与跨实例公平暂时失效（各实例独立兜底），
  这是“可用性优先”的有意取舍；硬上限仍是防线，不会无限放行。

## 未配置租户不受影响

没有配置限额的租户/接口根本不访问存储，因此无论 fail-open / fail-closed、
无论依赖是否健康，它们始终放行。降级只影响实际受配额管控的流量。

## 避免级联故障

- 远程调用有独立超时（`RequestTimeoutMillis` / 默认 2s），不会无限挂住
  业务请求。
- 瞬时故障（连接错误、502/503/504）做**有界指数退避 + 抖动**重试
  （最多 3 次）；4xx 业务错误不重试。
- 心跳失败不会拖垮请求路径：心跳在独立协程，失败后后台静默重新注册。
- 会话 TTL 通常显著大于心跳间隔（默认 30s vs 5s），容忍多次连续心跳丢失，
  避免短暂网络抖动导致租约被误回收（误回收会让在途请求的 Release 变成
  no-op，虽然安全，但会轻微改变占用计数）。

## 可观测性

所有降级事件都通过 `Metrics` 接口输出，内置 `slog` 实现：

| 事件                | 方法/标志                                   |
| ------------------- | ------------------------------------------- |
| 进入降级            | `Metrics.Degraded(tenant, iface, policy)`（warn 日志） |
| 是否处于降级态      | `Gateway.IsDegraded()`（可接健康检查）      |
| 拒绝原因分布        | `Metrics.Rejected(..., reason)`             |
| 速率余量/在途并发   | `RateTaken` / `Concurrency`（debug 日志）   |
| 配置热更新成败      | `ConfigReloaded(version, ok)`               |

可实现 `quota.Metrics` 接口桥接到 Prometheus（建议计数器：
`quota_requests_total{result,reason}`、`quota_concurrency_active`、
`quota_dependency_degraded`、`quota_config_version`）。
排障时优先看：拒绝原因是否为 `dependency_down`、`IsDegraded()` 是否为真、
协调端 `/quota/v1/healthz` 与实例心跳状态。
