package quota

import (
	"context"
	"testing"
	"time"
)

func TestIdempotencyEntriesExpire(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	clock := NewFakeClock(start)
	s := NewMemoryStore(clock)
	defer s.Close()
	if err := s.SyncConfig(context.Background(), baseConfig()); err != nil {
		t.Fatal(err)
	}

	// 一次被拒绝的准入（硬上限打满前的公平/竞争拒绝这里用不同 request）
	// 先占满 3 个名额
	var tokens []LeaseToken
	for i := 0; i < 3; i++ {
		r, err := s.Admit(context.Background(), AdmitRequest{
			Tenant: "t1", Iface: "api", RequestID: RequestID("hold-" + itoaSafe(i)), HardCap: 3,
		})
		if err != nil || !r.Admitted {
			t.Fatalf("hold %d: %+v %v", i, r, err)
		}
		tokens = append(tokens, r.Token)
	}
	rej, err := s.Admit(context.Background(), AdmitRequest{
		Tenant: "t1", Iface: "api", RequestID: "rejected-1", HardCap: 3,
	})
	if err != nil || rej.Admitted {
		t.Fatalf("expected rejection, got %+v", rej)
	}
	if len(s.admitResults) != 4 {
		t.Fatalf("expected 4 idempotency records, got %d", len(s.admitResults))
	}

	// 推进超过 TTL 并触发回收：被拒绝记录应消失；持有中的 3 条必须保留
	clock.Advance(idempotencyTTL + time.Second)
	s.reap()
	if _, ok := s.admitResults[attemptKey{reqID: "rejected-1"}]; ok {
		t.Fatal("rejected idempotency record should expire")
	}
	for i, tok := range tokens {
		var found bool
		for _, c := range s.admitResults {
			if c.res.Token == tok {
				found = true
			}
		}
		if !found {
			t.Fatalf("held lease record %d must be retained past TTL", i)
		}
	}

	// 释放后再推进，持有记录也应可被回收
	for i, tok := range tokens {
		if _, err := s.Release(context.Background(), tok, RequestID("rel-"+itoaSafe(i))); err != nil {
			t.Fatal(err)
		}
	}
	clock.Advance(idempotencyTTL + time.Second)
	s.reap()
	if len(s.admitResults) != 0 {
		t.Fatalf("all admit idempotency entries should expire after release+TTL, got %d", len(s.admitResults))
	}
}
