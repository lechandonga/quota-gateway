# quota-gateway

为现有服务增加一层**多租户配额与公平调度**能力，在单实例与多实例
（共享 Redis）并发处理请求时保证：

- **租户隔离**：按 `(租户, 接口)` 维度配置平均速率（令牌桶）、突发容量
  与并发上限；超限请求被稳定拒绝，返回可区分错误码与 `Retry-After`，
  未超限请求不被误伤。
- **多实例一致**：配额判定是 Redis 上的单条原子 Lua 脚本；实例增减、
  重启、短暂不可达都不放大额度、不重复计数。并发占位是带 TTL 的**租约**：
  实例存活时由唯一的后台续租器持续续期，数分钟级的长请求也不会被提前
  回收；实例崩溃/被强杀后遗留占位在一个 TTL 内有界回收。
- **加权公平**：容量竞争时按租户 `weight` 比例分享全局共享容量
  （WFQ/GPS 近似），闲置容量可临时借用并在原租户需要时归还。
- **可靠释放**：长连接在正常结束、超时、客户端中断（含与 handler 结束
  同时发生的竞态）、panic、重复/乱序释放下都恰好释放一次，不崩溃、不
  泄漏、不提前归还。
- **降级兜底**：配额后端不可用时按配置在拒绝（fail-closed，默认）与
  放行（fail-open）间选择；fail-open 仍然遵守每个租户的**本地并发硬
  上限**并可观测，后端恢复后平滑收敛回统一计账，不放大、不重复计数。
- **热更新与版本演进**：新配置先校验后生效，非法/不兼容配置被拒绝且
  不影响在用配置；长连接在途期间热更新不误回收名额；同主版本内新旧
  实例可共存。
- **资源有界**：续租只有单个 goroutine 与单个定时器，不随长连接数增长；
  数千条长连接集中释放也不阻塞请求主路径。
- **可解释降级**：配额后端不可用时按配置在拒绝（fail-closed，默认）
  与放行（fail-open）间选择，超时隔离 + 健康监测避免级联故障；
  拒绝原因与状态通过日志和 expvar 指标可观测。

仅依赖 **Go 标准库**（含一个最小 RESP/Redis 客户端），无第三方依赖。

## 包结构

```
quota/      错误类型、配置校验/版本、令牌桶、WFQ 公平调度、Manager、
            租约续租（lease）、降级本地闸门（localgate）、热更新、健康监测
store/      存储抽象；内存实现（单实例/测试）与 Redis Lua 实现（多实例，
            含原子 TryAcquire / Release / Renew 续租脚本）
middleware/ HTTP 中间件：429/503 映射、Retry-After、并发安全的长连接释放
metrics/    expvar 指标（计数、在途、降级本地在途、续租失败/丢约、延迟、
            健康、配置版本）
cmd/quota-gateway/  可运行的示例服务（SIGHUP + 轮询热更新）
docs/       配额语义与本地验证文档
```

## 快速开始

```bash
go run ./cmd/quota-gateway -config ./docs/config-example.json            # 内存（单实例）
go run ./cmd/quota-gateway -config ./docs/config-example.json -redis :6379 # Redis（多实例）
```

## 配置示例

```json
{
  "version": "1.0",
  "defaultWeight": 1,
  "degrade": "deny",
  "globalMaxConcurrent": 100,
  "concurrencyTTLSeconds": 30,
  "rules": [
    {"tenant": "team-alpha", "api": "/v1/chat", "rate": 20, "burst": 40, "maxConcurrent": 30, "weight": 4},
    {"tenant": "team-alpha", "rate": 50, "burst": 100, "maxConcurrent": 60, "weight": 4},
    {"tenant": "team-beta",  "rate": 10, "burst": 20, "maxConcurrent": 20, "weight": 1}
  ]
}
```

`api` 为空表示该租户的默认规则；接口专属规则优先。未配置规则的租户不
施加配额。`concurrencyTTLSeconds` 可选（默认 30，范围 1–3600）：它是
并发占位租约的 TTL——实例存活时长请求会被自动续租不受其限制，仅用于
实例崩溃后的有界回收；也可通过启动参数 `-lease-ttl` 调优（配置文件优先）。

## 文档

- [配额语义、降级策略与热更新](docs/quota-semantics.md)
- [本地验证方式](docs/local-verification.md)

## 测试

```bash
go test -count=1 ./...
CGO_ENABLED=1 go test -race -count=1 ./...
```
