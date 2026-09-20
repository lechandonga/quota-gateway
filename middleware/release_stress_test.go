package middleware

import (
	"context"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/quota-gateway/quota"
	"github.com/lechandonga/quota-gateway/store"
)

// 高并发 + 高频客户端断连下，释放路径不得让进程崩溃（历史上
// doRelease 的 close(channel) 竞态会 panic），结束后名额零泄漏。
// 建议在 -race 下反复运行：go test -race -count=N ./middleware/
func TestMiddleware_ConcurrentDisconnectStorm(t *testing.T) {
	cfg := &quota.Config{
		Version: "1.0",
		Degrade: quota.DegradeDeny,
		Rules: []quota.Rule{
			{Tenant: "t1", API: "/w", Rate: 0, Burst: 1, MaxConcurrent: 64, Weight: 1},
		},
	}
	ms := store.NewMemoryStore()
	srv := newServer(t, ms, cfg, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ping" { // 活性探测路径：立即返回
			_, _ = w.Write([]byte("pong"))
			return
		}
		switch rand.Intn(3) {
		case 0: // 立即返回
		case 1: // 短暂处理后返回
			time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
		default: // 等断连后返回
			<-r.Context().Done()
		}
		_, _ = w.Write([]byte("ok"))
	}))

	client := &http.Client{Timeout: 5 * time.Second}
	const workers = 24
	const rounds = 60
	var okCount atomic.Int64
	var canceled atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < rounds; i++ {
				ctx, cancel := context.WithCancel(context.Background())
				req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/w", nil)
				req.Header.Set("X-Tenant-ID", "t1")
				// 所有请求都会在随机短延时后断连：case 0/1 的 handler 通常
				// 在断连前正常返回（正常路径），case 2 则靠断连驱动退出
				// （断连路径），两种路径以任意交错并发触发。
				canceled.Add(1)
				time.AfterFunc(time.Duration(rng.Intn(6))*time.Millisecond, cancel)
				resp, err := client.Do(req)
				if err == nil {
					resp.Body.Close()
					okCount.Add(1)
				}
				cancel()
			}
		}(int64(w))
	}
	wg.Wait()

	// 服务仍存活且能正常响应。
	resp, err := client.Get(srv.URL + "/ping")
	if err != nil {
		t.Fatalf("server not responding after disconnect storm: %v", err)
	}
	resp.Body.Close()

	// 全部连接结束后：名额零泄漏。
	deadline := time.Now().Add(5 * time.Second)
	for ms.Inflight("cc:t1:/w") != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("inflight leak after storm: %d (ok=%d canceled=%d)",
				ms.Inflight("cc:t1:/w"), okCount.Load(), canceled.Load())
		}
		time.Sleep(20 * time.Millisecond)
	}
}
