package quota

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func alwaysAdmit() (admitOutcome, error) { return outcomeAdmitted, nil }

func TestFairScheduler_SerializesByWeightOrder(t *testing.T) {
	s := newFairScheduler()
	ctx := context.Background()

	// holder：直接成为“已准入、在途”的占用（不进入等待队列），
	// 让它的 finish 成为系统虚拟时间基线。
	holdFnStarted := make(chan struct{})
	holderFinish := make(chan struct{})
	go func() {
		_ = s.acquire(ctx, "holder", 1, func() (admitOutcome, error) {
			close(holdFnStarted)
			<-holderFinish
			return outcomeAdmitted, nil
		})
	}()
	<-holdFnStarted

	tenants := []struct {
		name   string
		weight int
	}{{"low", 1}, {"mid", 4}, {"high", 16}, {"low2", 1}}
	// 每个等待者：成为队首且 holder 仍在途时先 contended；
	// holder 释放后，每收到一次 step 就放行一个，严格观测 F 顺序。
	// 注意：调度器允许“前一个队首判定在飞时，下一个等待者成为新队首”，
	// 因此用 judging 单飞标志让判定串行化，否则两个判定并发阻塞在
	// step 上时，谁先收到是不确定的（测试本身的竞态，非调度器错误）。
	step := make(chan struct{})
	got := make(chan string, 8)
	var judging atomic.Bool
	var wg sync.WaitGroup
	for _, tn := range tenants {
		wg.Add(1)
		go func(name string, w int) {
			defer wg.Done()
			err := s.acquire(ctx, name, w, func() (admitOutcome, error) {
				if !judging.CompareAndSwap(false, true) {
					return outcomeContended, nil // 另一判定在飞：重新排队
				}
				defer judging.Store(false)
				select {
				case <-holderFinish:
					// holder 已释放：等待主测试逐个放行
				default:
					return outcomeContended, nil
				}
				<-step
				return outcomeAdmitted, nil
			})
			if err == nil {
				got <- name
				s.release(name)
			}
		}(tn.name, tn.weight)
	}
	// 等待 4 个租户全部进入等待队列（contended 重试期间 waiting 不减）。
	waitUntil(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		n := 0
		for _, tn := range tenants {
			if fs := s.flows[tn.name]; fs != nil && fs.waiting == 1 {
				n++
			}
		}
		return n == len(tenants)
	})

	// holder 释放，广播唤醒当前队首；随后逐次 step 观测放行顺序。
	close(holderFinish)
	s.release("holder")
	var order []string
	for i := 0; i < len(tenants); i++ {
		step <- struct{}{}
		select {
		case name := <-got:
			order = append(order, name)
		case <-time.After(2 * time.Second):
			t.Fatalf("drain stalled at %d, order=%v", i, order)
		}
	}
	wg.Wait()

	// 按 F 升序：high(1/16) → mid(1/4) → low/low2(1.0，按到达序)。
	if order[0] != "high" || order[1] != "mid" {
		t.Fatalf("order=%v want high,mid first", order)
	}
	if (order[2] != "low" || order[3] != "low2") && (order[2] != "low2" || order[3] != "low") {
		t.Fatalf("order=%v low-weight tenants last", order)
	}
}

func TestFairScheduler_ContendedThenAdmitOnRelease(t *testing.T) {
	s := newFairScheduler()
	var canAdmit atomic.Bool
	fn := func() (admitOutcome, error) {
		if canAdmit.Load() {
			return outcomeAdmitted, nil
		}
		return outcomeContended, nil
	}
	done := make(chan struct{})
	go func() {
		err := s.acquire(context.Background(), "t", 1, fn)
		if err == nil {
			close(done)
		}
	}()
	time.Sleep(50 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("should still be waiting (contended)")
	default:
	}
	canAdmit.Store(true)
	s.release("t") // 持票租户释放广播唤醒队首（这里模拟外部归还）
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("waiter not admitted after release broadcast")
	}
}

func TestFairScheduler_RejectedStopsWaiting(t *testing.T) {
	s := newFairScheduler()
	sentinel := errors.New("hard reject")
	err := s.acquire(context.Background(), "t", 1, func() (admitOutcome, error) {
		return outcomeRejected, sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err=%v want sentinel", err)
	}
}

func TestFairScheduler_CanceledWaiterDoesNotBlockOthers(t *testing.T) {
	s := newFairScheduler()
	ctxA, cancelA := context.WithCancel(context.Background())
	aInQueue := make(chan struct{})

	// A 以队首身份一直 contended。
	holdA := make(chan struct{})
	go func() {
		_ = s.acquire(ctxA, "a", 1, func() (admitOutcome, error) {
			select {
			case <-holdA:
				return outcomeRejected, errors.New("a done")
			default:
			}
			close(aInQueue)
			return outcomeContended, nil
		})
	}()
	<-aInQueue

	// B 排在 A 后面，永远成为不了队首，直到 A 取消。
	bDone := make(chan error, 1)
	go func() { bDone <- s.acquire(context.Background(), "b", 1, alwaysAdmit) }()
	time.Sleep(50 * time.Millisecond)
	select {
	case <-bDone:
		t.Fatal("B must not be admitted while head A is waiting")
	default:
	}

	cancelA() // A 退出队列
	select {
	case err := <-bDone:
		if err != nil {
			t.Fatalf("B should admit after A cancel, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("B stuck behind canceled A")
	}
}

func TestFairScheduler_IdleFlowRealignsVirtualTime(t *testing.T) {
	s := newFairScheduler()
	// idle 流先准入再释放，finish 对齐 V；重新排队时不应被陈旧高 finish 压制。
	if err := s.acquire(context.Background(), "idle", 1, alwaysAdmit); err != nil {
		t.Fatal(err)
	}
	s.release("idle")
	if err := s.acquire(context.Background(), "idle", 1, alwaysAdmit); err != nil {
		t.Fatalf("idle tenant re-acquire: %v", err)
	}
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met")
}
