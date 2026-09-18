package quota

import "testing"

func TestFairnessPure(t *testing.T) {
	weights := map[string]float64{"A": 1, "B": 3}
	// 容量 4：A 保证份额 1，B 保证份额 3（仅当二者同时竞争）。

	// A 占用 0，组内空闲 -> 直接准入
	v := decideAdmit(admitInput{key: "A", groupName: "g", groupCapacity: 4,
		active: map[string]int64{}, waiters: map[string]int64{"A": 1}, weights: weights})
	if !v.admitted {
		t.Fatal("A should be admitted with free capacity")
	}

	// B 已借用到 4，A 回来排队：容量满 -> 等待
	v = decideAdmit(admitInput{key: "A", groupName: "g", groupCapacity: 4,
		active: map[string]int64{"B": 4}, waiters: map[string]int64{"A": 1}, weights: weights})
	if v.admitted || v.reason != ReasonFairnessWait {
		t.Fatalf("A must wait when capacity full, got %+v", v)
	}

	// B 释放一个（B=3），A 仍排队：A(0) < 份额(1) -> A 准入收回份额
	v = decideAdmit(admitInput{key: "A", groupName: "g", groupCapacity: 4,
		active: map[string]int64{"B": 3}, waiters: map[string]int64{"A": 1}, weights: weights})
	if !v.admitted {
		t.Fatalf("A must reclaim its share once borrower releases, got %+v", v)
	}

	// A 不在等待（无 waiters），B 持有 3 申请：闲置容量可借用
	v = decideAdmit(admitInput{key: "B", groupName: "g", groupCapacity: 4,
		active: map[string]int64{"B": 3}, waiters: map[string]int64{}, weights: weights})
	if !v.admitted {
		t.Fatalf("B should borrow idle capacity, got %+v", v)
	}

	// B 持有 3 想继续借用，而 A 排队且低于份额(A=0<1)：必须拒绝 B
	v = decideAdmit(admitInput{key: "B", groupName: "g", groupCapacity: 4,
		active: map[string]int64{"B": 3}, waiters: map[string]int64{"A": 1}, weights: weights})
	if v.admitted {
		t.Fatal("B must not borrow while under-share waiter A exists")
	}
}

func TestFairnessHardCap(t *testing.T) {
	v := decideAdmit(admitInput{key: "A", hardCap: 2, groupName: "",
		active: map[string]int64{"A": 2}, waiters: map[string]int64{"A": 1}, weights: map[string]float64{"A": 1}})
	if v.admitted || v.reason != ReasonConcurrentLimit {
		t.Fatalf("hard cap must reject, got %+v", v)
	}
}
