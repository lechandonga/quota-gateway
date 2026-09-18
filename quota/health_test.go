package quota

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/quota-gateway/store"
)

type togglePinger struct{ fail atomic.Bool }

func (p *togglePinger) Ping(context.Context) error {
	if p.fail.Load() {
		return errUnavailableForTest
	}
	return nil
}

var errUnavailableForTest = &simpleErr{"backend down"}

type simpleErr struct{ msg string }

func (e *simpleErr) Error() string { return e.msg }

func TestHealthMonitor_Transitions(t *testing.T) {
	ms := store.NewMemoryStore()
	m, err := NewManager(Options{Store: ms, Config: &Config{Version: "1.0"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })

	p := &togglePinger{}
	hm := StartHealthMonitor(m, p, 20*time.Millisecond, discardLogger{})
	defer hm.Stop()
	if !hm.Healthy() {
		t.Fatal("initial should be healthy")
	}

	p.fail.Store(true)
	waitTrue(t, func() bool { return !hm.Healthy() })

	p.fail.Store(false)
	waitTrue(t, hm.Healthy)
}

func waitTrue(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met")
}
