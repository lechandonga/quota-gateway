package quota

// 默认公平排队重试提示（毫秒）。实际等待由网关侧的退避循环控制，
// 这里给出可机读的建议值，避免客户端忙等。
const defaultFairnessRetryMillis = 50

// admitInput 是公平裁决的纯函数输入（快照，已剔除过期占用与排队标记）。
//
// 两个关键集合必须区分：
//   - contenders: 正在参与组内容量分配的成员 = 当前持有占用(active)的成员
//     ∪ 正在排队等待(waiters)的成员。用于计算各自的加权保证份额。
//   - waiters:   仅排队中、尚未拿满的成员。只有 waiters 能阻止借用方
//     使用闲置容量（否则一个已经在跑的租户会被永久当成“竞争者”，
//     让真正闲置的容量无法被任何人借用）。
type admitInput struct {
	key           string
	hardCap       int64
	groupName     string
	groupCapacity int64
	active        map[string]int64 // 各成员当前占用
	waiters       map[string]int64 // 排队成员 -> 首次排队时间(unixmilli)
	weights       map[string]float64
}

func (in admitInput) contenders() map[string]int64 {
	out := make(map[string]int64, len(in.active)+len(in.waiters))
	for k, n := range in.active {
		if n > 0 {
			out[k] = 0
		}
	}
	for k, ts := range in.waiters {
		out[k] = ts
	}
	return out
}

// admitVerdict 是纯函数裁决。
type admitVerdict struct {
	admitted    bool
	reason      Reason
	retryMillis int64
}

// decideAdmit 执行“加权最大最小公平 + 闲置借用 + 原租户可收回”裁决。
//
//  1. 超过租户自身硬上限：拒绝(concurrent_limit)。
//  2. 无组约束：有物理名额即可获取。
//  3. 组内已满：拒绝(fairness_wait)，调用方退避重试/排队唤醒。
//  4. 有物理空闲名额：
//     a. 申请者当前占用 < 其加权保证份额 -> 准入（这是它应得的容量，
//     即使该名额此前被借用方占用，也会随借用方释放自然回流）。
//     b. 申请者已达份额 -> 仅当没有任何“排队中且低于份额”的其他成员时，
//     才允许借用真正闲置的容量；否则必须等待（防长期占用/饿死他人）。
//
// 确定性纯函数：同样快照必然得到同样结果，便于串行化与测试。
func decideAdmit(in admitInput) admitVerdict {
	if in.hardCap > 0 && in.active[in.key] >= in.hardCap {
		return admitVerdict{admitted: false, reason: ReasonConcurrentLimit}
	}
	if in.groupName == "" || in.groupCapacity <= 0 {
		return admitVerdict{admitted: true}
	}

	var totalActive int64
	for _, n := range in.active {
		totalActive += n
	}
	if totalActive >= in.groupCapacity {
		return admitVerdict{admitted: false, reason: ReasonFairnessWait, retryMillis: defaultFairnessRetryMillis}
	}

	// 被争夺的容量 = 总容量 - 完全没人占用的闲置容量。
	// 保证份额只在“已投入使用/争夺”的容量上按权重分配：
	// 完全闲置的容量不属于任何人，只能被借用，从而保证：
	//   - 先到租户不会因为后来者出现而被追溯削减保证份额；
	//   - 闲置容量可被临时借用；
	//   - 原租户回来竞争时，借用方超份额占用逐步回流。
	contended := float64(totalActive)
	if contended > float64(in.groupCapacity) {
		contended = float64(in.groupCapacity)
	}
	if contended < 1 {
		contended = 1
	}

	contenders := in.contenders()
	shares := fairShares(in.weights, contenders, contended)
	myShare := shares[in.key]

	if float64(in.active[in.key])+1e-9 < myShare {
		return admitAllowed
	}
	// 申请者已达份额：若存在排队中且低于份额的其他成员，不可借用。
	for other := range in.waiters {
		if other == in.key {
			continue
		}
		if float64(in.active[other])+1e-9 < shares[other] {
			return admitVerdict{admitted: false, reason: ReasonFairnessWait, retryMillis: defaultFairnessRetryMillis}
		}
	}
	return admitAllowed
}

// admitAllowed 是准入通过的裁决常量。
var admitAllowed = admitVerdict{admitted: true}

// fairShares 在 contenders 之间按权重分配 capacity 的比例保证份额。
// 无每成员额外上限时加权比例一次成（硬上限在入口单独判定）。
func fairShares(weights map[string]float64, contenders map[string]int64, capacity float64) map[string]float64 {
	out := make(map[string]float64, len(contenders))
	var sumW float64
	for k := range contenders {
		w := weights[k]
		if w <= 0 {
			w = 1
		}
		sumW += w
	}
	if sumW <= 0 {
		return out
	}
	for k := range contenders {
		w := weights[k]
		if w <= 0 {
			w = 1
		}
		if s := w / sumW * capacity; s > 0 {
			out[k] = s
		}
	}
	return out
}
