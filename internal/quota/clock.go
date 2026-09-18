package quota

import (
	"sync"
	"time"
)

// Clock 抽象时间来源，便于测试中注入假时钟。
type Clock interface {
	Now() time.Time
	Sleep(d time.Duration)
	// NewTimer 创建一次性定时器，返回触发通道与取消函数。
	NewTimer(d time.Duration) (<-chan time.Time, func())
}

type systemClock struct{}

// SystemClock 返回真实时钟。
func SystemClock() Clock { return systemClock{} }

func (systemClock) Now() time.Time { return time.Now() }

func (systemClock) Sleep(d time.Duration) { time.Sleep(d) }

func (systemClock) NewTimer(d time.Duration) (<-chan time.Time, func()) {
	t := time.NewTimer(d)
	return t.C, func() {
		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
	}
}

// fakeTimer 是假时钟内部的一个定时器。
type fakeTimer struct {
	deadline time.Time
	ch       chan time.Time
	fired    bool
	stopped  bool
}

// FakeClock 是测试用可控时钟：Advance 推进时间并同步触发到期定时器。
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

// NewFakeClock 构造假时钟。
func NewFakeClock(now time.Time) *FakeClock {
	return &FakeClock{now: now}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Sleep 阻塞直到假时钟被推进至少 d（测试中通常配合 goroutine 使用）。
func (c *FakeClock) Sleep(d time.Duration) {
	ch, cancel := c.NewTimer(d)
	defer cancel()
	<-ch
}

func (c *FakeClock) NewTimer(d time.Duration) (<-chan time.Time, func()) {
	c.mu.Lock()
	t := &fakeTimer{deadline: c.now.Add(d), ch: make(chan time.Time, 1)}
	c.timers = append(c.timers, t)
	c.mu.Unlock()
	return t.ch, func() {
		c.mu.Lock()
		t.stopped = true
		c.mu.Unlock()
	}
}

// Advance 推进时钟，并触发所有到期定时器（按截止时间顺序）。
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	var due []*fakeTimer
	remaining := c.timers[:0]
	for _, t := range c.timers {
		if !t.stopped && !t.fired && !t.deadline.After(c.now) {
			t.fired = true
			due = append(due, t)
		} else if !t.fired && !t.stopped {
			remaining = append(remaining, t)
		}
	}
	c.timers = remaining
	c.mu.Unlock()
	for _, t := range due {
		select {
		case t.ch <- t.deadline:
		default:
		}
	}
}
