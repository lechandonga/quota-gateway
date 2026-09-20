package middleware

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/quota-gateway/quota"
	"github.com/lechandonga/quota-gateway/store"
)

// 高循环 × 高并发制造“客户端断连”与“handler 恰好结束”的交错：
// 旧实现在该交错下会 double-close channel 直接 panic 退出。
// 这里反复跑多轮，验证进程稳定、名额不泄漏。
func TestMiddleware_DisconnectFinishInterleaveNoPanic(t *testing.T) {
	const capN = 50
	// finish 只关闭一次、永不复用：所有轮次的 handler 都监听它，
	// 避免“重建 channel 时仍有 handler 读旧值”的数据竞争。
	finish := make(chan struct{})
	var finishOnce sync.Once
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fl, _ := w.(http.Flusher)
		w.WriteHeader(http.StatusOK)
		if fl != nil {
			fl.Flush()
		}
		// 在“客户端可能随时断开”和“服务端可能随时结束”之间制造竞态窗口。
		select {
		case <-r.Context().Done():
		case <-finish:
		}
	})
	cfg := &quota.Config{Version: "1.0", Degrade: quota.DegradeDeny, Rules: []quota.Rule{
		{Tenant: "t1", API: "/c", Rate: 0, Burst: 1, MaxConcurrent: capN, Weight: 1},
	}}
	ms := store.NewMemoryStore()
	m, err := quota.NewManager(quota.Options{Store: ms, Config: cfg, ConcurrencyTTL: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	srv := httptest.NewServer(QuotaHandler(m, Options{})(handler))
	t.Cleanup(srv.Close)

	const rounds, perRound = 6, capN * 3
	for round := 0; round < rounds; round++ {
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < perRound; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				// 建立原始 TCP 连接并手写请求，随后在任意时刻粗暴关闭，
				// 最大化“读到响应 / 服务端结束 / 客户端断连”的交错面。
				conn, err := net.Dial("tcp", srv.Listener.Addr().String())
				if err != nil {
					return
				}
				fmt.Fprintf(conn, "GET /c HTTP/1.1\r\nHost: x\r\nX-Tenant-ID: t1\r\nConnection: close\r\n\r\n")
				go func() {
					time.Sleep(time.Duration(i%2) * time.Millisecond)
					_ = conn.Close() // 模拟客户端中途断开
				}()
				buf := make([]byte, 16)
				_, _ = conn.Read(buf) // 读到响应头或连接断开
				time.Sleep(time.Duration(i%2) * time.Millisecond)
				_ = conn.Close()
			}(i)
		}
		close(start)
		// 在请求飞行中广播“服务端正常结束”，与断连风暴竞争。
		go func() { time.Sleep(time.Millisecond); finishOnce.Do(func() { close(finish) }) }()
		wg.Wait()
	}
	// 所有连接都应排空，后端名额归零（无泄漏、无超额残留）。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if msInflight(ms, "cc:t1:/c") == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("inflight=%d want 0 after disconnect/finish interleavings (leak)", msInflight(ms, "cc:t1:/c"))
}

// handler panic：进程不退出、名额被归还、客户端得到 500。
func TestMiddleware_HandlerPanicReleasesSlot(t *testing.T) {
	var once int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.CompareAndSwapInt32(&once, 0, 1) {
			panic("boom in upstream")
		}
		_, _ = w.Write([]byte("ok"))
	})
	ms := store.NewMemoryStore()
	srv := newServer(t, ms, testConfig(quota.DegradeDeny), handler)

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/c", nil)
	req.Header.Set("X-Tenant-ID", "t1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	// 中间件兜底 recover 并返回 500；关键是第二次请求仍能获取名额，
	// 说明 panic 没有泄漏名额，进程也没有退出。
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("panic response status=%d want 500", resp.StatusCode)
	}
	resp2 := do(t, srv.URL+"/api/c", "t1")
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("after panic recovery status=%d want 200", resp2.StatusCode)
	}
}

// fail-open 期间撞本地硬上限：返回 429 + 可区分 reason + Retry-After。
func TestMiddleware_DegradeAllowLocalCap429(t *testing.T) {
	cfg := testConfig(quota.DegradeAllow) // /api/c maxConcurrent=1
	st := &boomStore{err: errors.New("redis down")}
	inHandler := make(chan struct{})
	releaseHandler := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inHandler <- struct{}{}
		<-releaseHandler
		_, _ = w.Write([]byte("done"))
	})
	srv := newServer(t, st, cfg, handler)

	firstDone := make(chan struct{})
	go func() {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/c", nil)
		req.Header.Set("X-Tenant-ID", "t1")
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
		}
		close(firstDone)
	}()
	<-inHandler

	resp := do(t, srv.URL+"/api/c", "t1")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status=%d want 429", resp.StatusCode)
	}
	if got := resp.Header.Get(ReasonHeader); got != string(quota.ErrCodeDegradedLocalLimited) {
		t.Fatalf("reason=%q want %s", got, quota.ErrCodeDegradedLocalLimited)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Fatal("expected Retry-After")
	}
	close(releaseHandler)
	<-firstDone
}
