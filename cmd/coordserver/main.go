// Command coordserver 运行多实例共享的配额协调服务。
//
// 它把统一的配额状态（速率桶、并发租约、会话、公平裁决）以 HTTP API
// 暴露给所有网关节点，自身无外部中间件依赖，便于本地与 CI 验证。
//
// 用法:
//
//	coordserver -addr :18000 [-config /path/to/config.json]
//
// 配置可在运行期通过 POST /quota/v1/config 热更新（校验失败保持现状）。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lechandonga/quota-gateway/internal/quota"
)

func main() {
	addr := flag.String("addr", ":18000", "listen address")
	configPath := flag.String("config", "", "optional initial config JSON file")
	flag.Parse()

	store := quota.NewMemoryStore(quota.SystemClock())
	defer store.Close()

	if *configPath != "" {
		cfg, err := loadConfig(*configPath)
		if err != nil {
			log.Fatalf("load initial config: %v", err)
		}
		if err := store.SyncConfig(context.Background(), cfg); err != nil {
			log.Fatalf("install initial config: %v", err)
		}
		log.Printf("initial quota config installed: version=%d mode=%s", cfg.Version, cfg.Mode)
	}

	srv := quota.NewCoordinatorServer(store, quota.SystemClock())
	if err := srv.Start(*addr); err != nil {
		log.Fatalf("start coordinator: %v", err)
	}
	log.Printf("quota coordinator listening on %s (api %s)", *addr, quota.APIVersion)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Printf("shutting down coordinator")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
}

func loadConfig(path string) (*quota.Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var cfg quota.Config
	dec := json.NewDecoder(f)
	if err := dec.Decode(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
