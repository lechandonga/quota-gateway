package quota

import (
	"testing"
	"time"
)

func TestTokenBucketBurstThenRate(t *testing.T) {
	now := time.Unix(1000, 0)
	b := NewTokenBucket(10, 3, now) // 10 tokens/sec, burst 3
	for i := 0; i < 3; i++ {
		if ok, _, _ := b.Take(1, now); !ok {
			t.Fatalf("burst token %d should be allowed", i)
		}
	}
	if ok, retry, _ := b.Take(1, now); ok || retry <= 0 {
		t.Fatalf("4th token must be rejected with retry hint, got ok=%v retry=%v", ok, retry)
	}
	// 100ms 后应恰好补充 1 个令牌
	later := now.Add(100 * time.Millisecond)
	if ok, _, _ := b.Take(1, later); !ok {
		t.Fatal("refilled token should be allowed after 100ms")
	}
	if ok, _, _ := b.Take(1, later); ok {
		t.Fatal("only one token should have accrued")
	}
}

func TestTokenBucketUpdateShrink(t *testing.T) {
	now := time.Unix(1000, 0)
	b := NewTokenBucket(100, 100, now)
	if ok, _, _ := b.Take(50, now); !ok {
		t.Fatal("take 50")
	}
	// 缩小突发：剩余 50 > 新上限 10，应被收敛到 10
	b.Update(1, 10, now)
	if ok, _, _ := b.Take(11, now); ok {
		t.Fatal("tokens must be clamped to new burst")
	}
	if ok, _, _ := b.Take(10, now); !ok {
		t.Fatal("clamped tokens should be consumable")
	}
}
