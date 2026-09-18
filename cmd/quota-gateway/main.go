// quota-gateway 是多租户配额网关的示例服务。
//
// 用法示例：
//
//	# 单实例（内存存储）
//	go run ./cmd/quota-gateway -config ./docs/config-example.json
//
//	# 多实例（共享 Redis，配额与并发全局一致）
//	go run ./cmd/quota-gateway -redis 127.0.0.1:6379 -config ./docs/config-example.json
package main

import (
	"context"
	"expvar"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lechandonga/quota-gateway/metrics"
	"github.com/lechandonga/quota-gateway/middleware"
	"github.com/lechandonga/quota-gateway/quota"
	"github.com/lechandonga/quota-gateway/store"
)

func main() {
	var (
		addr           = flag.String("addr", ":8080", "监听地址")
		configPath     = flag.String("config", "", "配额配置文件路径(JSON)")
		redisAddr      = flag.String("redis", "", "Redis 地址；为空时使用进程内内存存储（仅单实例）")
		keyPrefix      = flag.String("key-prefix", "quota-gateway", "Redis 键前缀")
		reloadInterval = flag.Duration("reload-interval", 2*time.Second, "配置文件轮询热更新间隔；<=0 表示仅响应 SIGHUP")
		healthInterval = flag.Duration("health-interval", time.Second, "后端健康探测间隔")
	)
	flag.Parse()

	logger := log.New(os.Stdout, "quota-gateway ", log.LstdFlags|log.Lmicroseconds)
	rec := metrics.New()
	rec.PublishExpvar()

	var st store.Store
	if *redisAddr != "" {
		rs, err := store.NewRedisStore(store.RedisOptions{
			Addr:      *redisAddr,
			KeyPrefix: *keyPrefix,
			PoolSize:  16,
		})
		if err != nil {
			log.Fatalf("连接 Redis 失败: %v（可修改配置 degrade=allow 做 fail-open 前需先有后端）", err)
		}
		st = rs
		logger.Printf("配额后端: Redis %s", *redisAddr)
	} else {
		st = store.NewMemoryStore()
		logger.Printf("配额后端: 进程内内存（仅适合单实例/本地验证）")
	}

	var cfg *quota.Config
	if *configPath != "" {
		c, err := quota.LoadConfigFile(*configPath)
		if err != nil {
			log.Fatalf("初始配置非法: %v", err)
		}
		cfg = c
	}
	m, err := quota.NewManager(quota.Options{
		Store:   st,
		Config:  cfg,
		Metrics: rec,
		Logger:  logger,
	})
	if err != nil {
		log.Fatalf("初始化配额管理器失败: %v", err)
	}
	defer m.Close()

	hm := quota.StartHealthMonitor(m, st, *healthInterval, logger)
	defer hm.Stop()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if *configPath != "" {
		startConfigReloader(ctx, m, *configPath, *reloadInterval, logger)
	}

	mux := http.NewServeMux()
	qmw := middleware.QuotaHandler(m, middleware.Options{Metrics: rec})

	mux.Handle("/api/echo", qmw(http.HandlerFunc(echoHandler)))
	mux.Handle("/api/slow", qmw(http.HandlerFunc(slowHandler(150*time.Millisecond))))
	mux.Handle("/api/heavy", qmw(http.HandlerFunc(slowHandler(800*time.Millisecond))))

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if hm.Healthy() {
			fmt.Fprintf(w, `{"status":"ok","config_version":%q}`, m.ConfigVersion())
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, `{"status":"degraded","config_version":%q}`, m.ConfigVersion())
	})
	mux.Handle("/debug/vars", expvar.Handler())

	srv := &http.Server{Addr: *addr, Handler: logRequests(mux, logger)}
	go func() {
		logger.Printf("listening on %s", *addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("server error: %v", err)
		}
	}()
	<-ctx.Done()
	logger.Printf("shutting down ...")
	shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shCtx)
}

// ---- 演示上游 ----

func echoHandler(w http.ResponseWriter, r *http.Request) {
	tenant, api := r.Header.Get("X-Tenant-ID"), r.URL.Path
	fmt.Fprintf(w, `{"tenant":%q,"api":%q,"status":"served"}`+"\n", tenant, api)
}

func slowHandler(d time.Duration) http.HandlerFunc {
	var n atomic.Int64
	return func(w http.ResponseWriter, r *http.Request) {
		id := n.Add(1)
		select {
		case <-time.After(d):
			fmt.Fprintf(w, `{"id":%d,"status":"done"}`+"\n", id)
		case <-r.Context().Done():
			// 客户端中断：中间件仍会释放并发占用。
			return
		}
	}
}

// ---- 配置热更新：SIGHUP + 文件 mtime 轮询（非法配置保留旧版本） ----

func startConfigReloader(ctx context.Context, m *quota.Manager, path string, interval time.Duration, logger *log.Logger) {
	var lastMod atomic.Int64
	reload := func() {
		fi, err := os.Stat(path)
		if err != nil {
			logger.Printf("热更新: 读取配置失败: %v（保留版本 %s）", err, m.ConfigVersion())
			return
		}
		mt := fi.ModTime().UnixNano()
		if mt == lastMod.Load() {
			return
		}
		cfg, err := quota.LoadConfigFile(path)
		if err != nil {
			logger.Printf("热更新: 非法配置被拒绝: %v（保留版本 %s）", err, m.ConfigVersion())
			return
		}
		if err := m.UpdateConfig(cfg); err != nil {
			logger.Printf("热更新: 配置未生效: %v（保留版本 %s）", err, m.ConfigVersion())
			return
		}
		lastMod.Store(mt)
		logger.Printf("热更新: 配置已生效 version=%s", cfg.Version)
	}
	reload()
	go func() {
		if interval <= 0 {
			return
		}
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				reload()
			}
		}
	}()
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGHUP)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ch:
				lastMod.Store(0)
				reload()
			}
		}
	}()
}

// logRequests 是极简访问日志。
func logRequests(h http.Handler, logger *log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rw := &statusWriter{ResponseWriter: w, status: 200}
		h.ServeHTTP(rw, r)
		logger.Printf("%s %s %d tenant=%q", r.Method, r.URL.Path, rw.status, r.Header.Get("X-Tenant-ID"))
	})
}

type statusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (s *statusWriter) WriteHeader(code int) {
	if !s.wroteHeader {
		s.status = code
		s.wroteHeader = true
	}
	s.ResponseWriter.WriteHeader(code)
}
