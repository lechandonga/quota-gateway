package quota

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/quota-gateway/store"
)

// 数千条并发长连接：
//   - 续租只有一个后台 goroutine（不随连接数增长），所有长连接都被持续续租；
//   - 集中释放时不 panic、不泄漏、不阻塞过久。
func TestThousandsOfLongConnections_SingleRenewerAndBulkRelease(t *testing.T) {
	const n = 3000
	ms := store.NewMemoryStore()
	cfg := &Config{Version: "1.0", Rules: []Rule{
		{Tenant: "t1", API: "/c", Rate: 0, Burst: 1, MaxConcurrent: n + 100, Weight: 1}, // 上限足够大但仍记账
	}}
	// 固定较短 TTL + 固定续租周期，强制续租器在窗口内工作多轮。
	m, err := NewManager(Options{
		Store: ms, Config: cfg,
		ConcurrencyTTL: time.Second,
		RenewInterval:  150 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })

	before := runtime.NumGoroutine()

	rels := make([]ReleaseFunc, n)
	for i := 0; i < n; i++ {
		rel, err := m.Acquire(context.Background(), "t1", "/c")
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		rels[i] = rel
	}
	if got := m.leases.len(); got != n {
		t.Fatalf("registered leases=%d want %d", got, n)
	}

	// 让续租器跑两个完整周期（覆盖全部 3000 凭证的批量续租）；
	// TTL=1s 远大于建连耗时，任何凭证都不会在首次续租前过期。
	time.Sleep(400 * time.Millisecond)
	if got := ms.Inflight("cc:t1:/c"); got != n {
		t.Fatalf("after renew cycles backend inflight=%d want %d (renewer must keep all)", got, n)
	}

	// 长连接存续期间 goroutine 数不应随 3000 连接线性增长（只多一个续租器）。
	// 给予宽裕上界：中间件的监听 goroutine 不在本层；这里只有续租器。
	if got := runtime.NumGoroutine() - before; got > 4 {
		t.Fatalf("goroutine growth=%d want <= ~1 (single renewer regardless of connections)", got)
	}

	// 集中释放：并发、快速、允许重复调用。
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			rels[i]()
		}(i)
	}
	close(start)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("bulk release blocked too long")
	}
	if got := m.leases.len(); got != 0 {
		t.Fatalf("leases left=%d want 0", got)
	}
	if got := ms.Inflight("cc:t1:/c"); got != 0 {
		t.Fatalf("backend inflight=%d want 0 after bulk release", got)
	}
}
