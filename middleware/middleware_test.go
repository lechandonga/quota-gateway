package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/quota-gateway/quota"
	"github.com/lechandonga/quota-gateway/store"
)

func testConfig(degrade quota.DegradePolicy) *quota.Config {
	return &quota.Config{
		Version:             "1.0",
		Degrade:             degrade,
		GlobalMaxConcurrent: 1,
		Rules: []quota.Rule{
			{Tenant: "t1", API: "/api/x", Rate: 1, Burst: 1, MaxConcurrent: 1, Weight: 1},
			// 仅限制并发、不限速，用于确定性验证长连接释放。
			{Tenant: "t1", API: "/api/c", Rate: 0, Burst: 1, MaxConcurrent: 1, Weight: 1},
		},
	}
}

func newServer(t *testing.T, st store.Store, cfg *quota.Config, handler http.Handler) *httptest.Server {
	t.Helper()
	m, err := quota.NewManager(quota.Options{Store: st, Config: cfg, QueueTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	srv := httptest.NewServer(QuotaHandler(m, Options{})(handler))
	t.Cleanup(srv.Close)
	return srv
}

func do(t *testing.T, url, tenant string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("X-Tenant-ID", tenant)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("http: %v", err)
	}
	return resp
}

func TestMiddleware_AllowThen429WithRetryAfter(t *testing.T) {
	srv := newServer(t, store.NewMemoryStore(), testConfig(quota.DegradeDeny),
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("ok"))
		}))

	resp := do(t, srv.URL+"/api/x", "t1")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first: status=%d want 200", resp.StatusCode)
	}
	resp.Body.Close()

	// burst=1 且 rate=1/s：紧接着的第二个请求应 429。
	resp = do(t, srv.URL+"/api/x", "t1")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status=%d want 429", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Fatal("expected Retry-After header")
	}
	if got := resp.Header.Get(ReasonHeader); got != string(quota.ErrCodeRateLimited) {
		t.Fatalf("reason=%q want rate_limited", got)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	errObj, _ := body["error"].(map[string]any)
	if errObj["code"] != string(quota.ErrCodeRateLimited) {
		t.Fatalf("body=%v", body)
	}
}

func TestMiddleware_UnknownTenantPasses(t *testing.T) {
	srv := newServer(t, store.NewMemoryStore(), testConfig(quota.DegradeDeny),
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("ok"))
		}))
	resp := do(t, srv.URL+"/api/x", "no-such-tenant")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d want 200", resp.StatusCode)
	}
}

func TestMiddleware_LongConnectionReleasesOnClientInterrupt(t *testing.T) {
	var inHandler atomic.Int64
	finish := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inHandler.Add(1)
		defer inHandler.Add(-1)
		select {
		case <-r.Context().Done(): // 客户端中断
		case <-finish: // 正常结束
		}
		w.WriteHeader(http.StatusOK)
	})
	ms := store.NewMemoryStore()
	srv := newServer(t, ms, testConfig(quota.DegradeDeny), handler)

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/c", nil)
	req.Header.Set("X-Tenant-ID", "t1")
	rtDone := make(chan error, 1)
	go func() {
		resp, err := http.DefaultTransport.RoundTrip(req)
		if err == nil {
			resp.Body.Close()
		}
		rtDone <- err
	}()

	// 等待长连接进入处理。
	deadlineWait(t, func() bool { return inHandler.Load() == 1 })

	cancel() // 客户端中断
	select {
	case <-rtDone:
	case <-time.After(2 * time.Second):
		t.Fatal("RoundTrip did not return after cancel")
	}

	// handler 必须退出，且中间件已释放并发占位（在途归零）。
	deadlineWait(t, func() bool { return inHandler.Load() == 0 })
	if got := msInflight(ms, "cc:t1:/api/c"); got != 0 {
		t.Fatalf("inflight=%d want 0 after client interrupt (leak)", got)
	}

	// 同一连接可再次进入：证明没有被上一次中断的连接误伤。
	re2Done := make(chan *http.Response, 1)
	go func() { re2Done <- do(t, srv.URL+"/api/c", "t1") }()
	close(finish) // 让重入的长连接正常结束
	r2 := <-re2Done
	if r2.StatusCode != http.StatusOK {
		t.Fatalf("re-entry status=%d want 200", r2.StatusCode)
	}
	r2.Body.Close()
}

func deadlineWait(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before deadline")
}

// msInflight 读取内存存储在途数。
func msInflight(st store.Store, key string) int {
	return st.(*store.MemoryStore).Inflight(key)
}

type boomStore struct{ err error }

func (b *boomStore) TryAcquire(context.Context, store.AcquireRequest) (store.Result, error) {
	return store.Result{}, b.err
}
func (b *boomStore) Release(context.Context, []string, string) error { return b.err }
func (b *boomStore) Ping(context.Context) error                      { return b.err }
func (b *boomStore) Close() error                                    { return nil }

func TestMiddleware_DegradeDenyReturns503(t *testing.T) {
	srv := newServer(t, &boomStore{err: errors.New("redis down")}, testConfig(quota.DegradeDeny),
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Fatal("upstream must not be called when degraded deny")
		}))
	resp := do(t, srv.URL+"/api/x", "t1")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503", resp.StatusCode)
	}
	if got := resp.Header.Get(ReasonHeader); got != string(quota.ErrCodeDegradedDeny) {
		t.Fatalf("reason=%q", got)
	}
}

func TestMiddleware_DegradeAllowProxies(t *testing.T) {
	srv := newServer(t, &boomStore{err: errors.New("redis down")}, testConfig(quota.DegradeAllow),
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("served-while-degraded"))
		}))
	resp := do(t, srv.URL+"/api/x", "t1")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d want 200 under fail-open", resp.StatusCode)
	}
}
