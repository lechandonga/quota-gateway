package store

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// fakeRedis 是进程内最小 Redis：支持 PING/AUTH/SELECT 与 H* 原语，
// 并用 Go 复现两条配额 Lua 脚本的等价语义（脚本内容匹配后路由）。
type fakeRedis struct {
	ln net.Listener

	mu      sync.Mutex
	scripts map[string]string // sha -> script
	data    map[string]map[string]string
	// 若 >0，则下一个 EVALSHA 返回 NOSCRIPT（测试缓存丢失回退）。
	failNextEvalSHA int
}

func startFakeRedis(tb interface {
	Helper()
	Fatalf(string, ...any)
}) *fakeRedis {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen: %v", err)
	}
	f := &fakeRedis{ln: ln, scripts: map[string]string{}, data: map[string]map[string]string{}}
	go f.serve()
	return f
}

func (f *fakeRedis) addr() string { return f.ln.Addr().String() }

func (f *fakeRedis) close() { _ = f.ln.Close() }

func (f *fakeRedis) serve() {
	for {
		c, err := f.ln.Accept()
		if err != nil {
			return
		}
		go f.handle(c)
	}
}

func (f *fakeRedis) handle(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	w := bufio.NewWriter(c)
	for {
		args, err := readCommand(r)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}
		reply := f.dispatch(args)
		_, _ = w.WriteString(reply)
		_ = w.Flush()
	}
}

func readCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "*") {
		return nil, io.ErrUnexpectedEOF
	}
	n, _ := strconv.Atoi(line[1:])
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		hdr, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		hdr = strings.TrimSpace(hdr)
		if !strings.HasPrefix(hdr, "$") {
			return nil, io.ErrUnexpectedEOF
		}
		l, _ := strconv.Atoi(hdr[1:])
		buf := make([]byte, l+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:l]))
	}
	return args, nil
}

func encInt(n int64) string  { return fmt.Sprintf(":%d\r\n", n) }
func encOK() string          { return "+OK\r\n" }
func encPong() string        { return "+PONG\r\n" }
func encStr(s string) string { return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s) }
func encErr(e string) string { return "-" + e + "\r\n" }
func encArr(vals []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(vals))
	for _, v := range vals {
		b.WriteString(encStr(v))
	}
	return b.String()
}

func (f *fakeRedis) dispatch(args []string) string {
	cmd := strings.ToUpper(args[0])
	switch cmd {
	case "PING":
		return encPong()
	case "AUTH", "SELECT":
		return encOK()
	case "SCRIPT":
		if len(args) >= 3 && strings.ToUpper(args[1]) == "LOAD" {
			sha := fakeSHA(args[2])
			f.mu.Lock()
			f.scripts[sha] = args[2]
			f.mu.Unlock()
			return encStr(sha)
		}
		return encErr("ERR unknown script subcommand")
	case "EVALSHA", "EVAL":
		return f.eval(args)
	case "HGET":
		f.mu.Lock()
		defer f.mu.Unlock()
		v, ok := f.data[args[1]][args[2]]
		if !ok {
			return "$-1\r\n"
		}
		return encStr(v)
	case "HSET":
		f.mu.Lock()
		defer f.mu.Unlock()
		h := f.data[args[1]]
		if h == nil {
			h = map[string]string{}
			f.data[args[1]] = h
		}
		h[args[2]] = args[3]
		return encInt(0)
	case "HGETALL":
		f.mu.Lock()
		defer f.mu.Unlock()
		var vals []string
		for k, v := range f.data[args[1]] {
			vals = append(vals, k, v)
		}
		return encArr(vals)
	case "HDEL":
		f.mu.Lock()
		defer f.mu.Unlock()
		h := f.data[args[1]]
		removed := 0
		for _, k := range args[2:] {
			if _, ok := h[k]; ok {
				delete(h, k)
				removed++
			}
		}
		return encInt(int64(removed))
	case "HLEN":
		f.mu.Lock()
		defer f.mu.Unlock()
		return encInt(int64(len(f.data[args[1]])))
	case "DEL":
		f.mu.Lock()
		defer f.mu.Unlock()
		delete(f.data, args[1])
		return encInt(1)
	case "PEXPIRE":
		return encInt(1)
	default:
		return encErr("ERR unsupported command " + cmd)
	}
}

func (f *fakeRedis) eval(args []string) string {
	var script string
	if strings.ToUpper(args[0]) == "EVAL" {
		script = args[1]
	} else {
		f.mu.Lock()
		if f.failNextEvalSHA > 0 {
			f.failNextEvalSHA--
			f.mu.Unlock()
			return encErr("NOSCRIPT No matching script. Please use EVAL.")
		}
		script = f.scripts[args[1]]
		f.mu.Unlock()
		if script == "" {
			return encErr("NOSCRIPT No matching script. Please use EVAL.")
		}
	}
	// args: cmd script/sha numkeys k1 k2 k3 a1..
	numKeys, _ := strconv.Atoi(args[2])
	keys := args[3 : 3+numKeys]
	argv := args[3+numKeys:]
	switch script {
	case tryAcquireScript:
		return f.runTryAcquire(keys, argv)
	case releaseScript:
		return f.runRelease(keys, argv)
	case renewScript:
		return f.runRenew(keys, argv)
	default:
		return encErr("ERR unknown script")
	}
}

func encIntArr(vals []int64) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(vals))
	for _, v := range vals {
		b.WriteString(encInt(v))
	}
	return b.String()
}

func fakeSHA(s string) string {
	// 非加密用途的稳定伪 sha。
	h := uint64(1469598103934665603)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return fmt.Sprintf("%016x%016x", h, h^0xdeadbeef)
}

// runTryAcquire 与 tryAcquireScript 等价的 Go 实现。
func (f *fakeRedis) runTryAcquire(keys, argv []string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	rk, ck, sk := keys[0], keys[1], keys[2]
	now := time.Now().UnixMilli()
	rate, _ := strconv.ParseFloat(argv[1], 64)
	burst, _ := strconv.ParseInt(argv[2], 10, 64)
	maxT, _ := strconv.ParseInt(argv[3], 10, 64)
	maxS, _ := strconv.ParseInt(argv[4], 10, 64)
	token := argv[5]
	ttl, _ := strconv.ParseInt(argv[6], 10, 64)

	count := func(key string) (int64, int64) {
		h := f.data[key]
		var c, earliest int64
		for k, v := range h {
			exp, _ := strconv.ParseInt(v, 10, 64)
			if exp > 0 && exp <= now {
				delete(h, k)
				continue
			}
			c++
			if exp > 0 && (earliest == 0 || exp < earliest) {
				earliest = exp
			}
		}
		return c, earliest
	}
	retry := func(earliest int64) string {
		d := earliest - now
		if d < 10 {
			d = 10
		}
		return encIntArr([]int64{3, d})
	}

	var tokens float64
	haveRate := false
	if rate > 0 {
		rh := f.data[rk]
		if rh == nil {
			rh = map[string]string{}
			f.data[rk] = rh
		}
		tStr, ok1 := rh["t"]
		lStr, ok2 := rh["l"]
		if !ok1 || !ok2 {
			tokens = float64(burst)
		} else {
			tokens, _ = strconv.ParseFloat(tStr, 64)
			last, _ := strconv.ParseInt(lStr, 10, 64)
			tokens += float64(now-last) / 1000.0 * rate
			if tokens > float64(burst) {
				tokens = float64(burst)
			}
		}
		rh["l"] = strconv.FormatInt(now, 10)
		if tokens < 1 {
			rh["t"] = strconv.FormatFloat(tokens, 'f', 4, 64)
			d := int64((1 - tokens) / rate * 1000)
			return encIntArr([]int64{1, d})
		}
		haveRate = true
	}

	tc, te := count(ck)
	if maxT > 0 && tc >= maxT {
		return retryStatus2("", haveRate, f.data[rk], tokens, te)
	}
	if sk != "_" && maxS > 0 {
		sc, se := count(sk)
		if sc >= maxS {
			return retry(se)
		}
	}

	if haveRate {
		f.data[rk]["t"] = strconv.FormatFloat(tokens-1, 'f', 4, 64)
	}
	exp := int64(0)
	if ttl > 0 {
		exp = now + ttl
	}
	if maxT > 0 {
		put(f.data, ck, token, exp)
	}
	if sk != "_" && maxS > 0 {
		put(f.data, sk, token, exp)
	}
	return encIntArr([]int64{0, 0})
}

// retryStatus2 把租户硬上限映射为状态 2（与共享池状态 3 区分）。
func retryStatus2(_ string, haveRate bool, rh map[string]string, tokens float64, earliest int64) string {
	if haveRate {
		rh["t"] = strconv.FormatFloat(tokens, 'f', 4, 64)
	}
	d := earliest
	if d < 10 {
		d = 10
	}
	return encIntArr([]int64{2, d})
}

func put(data map[string]map[string]string, key, token string, exp int64) {
	h := data[key]
	if h == nil {
		h = map[string]string{}
		data[key] = h
	}
	h[token] = strconv.FormatInt(exp, 10)
}

func (f *fakeRedis) runRelease(keys, argv []string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	h := f.data[keys[0]]
	removed := 0
	if _, ok := h[argv[0]]; ok {
		delete(h, argv[0])
		removed = 1
	}
	return encInt(int64(removed))
}

// runRenew 与 renewScript 等价的 Go 实现：仅刷新仍存在的占位。
func (f *fakeRedis) runRenew(keys, argv []string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	ttl, _ := strconv.ParseInt(argv[1], 10, 64)
	if ttl <= 0 {
		return encInt(0)
	}
	exp := strconv.FormatInt(time.Now().UnixMilli()+ttl, 10)
	renewed := 0
	for _, tok := range argv[2:] {
		for _, key := range keys {
			h := f.data[key]
			if h == nil {
				continue
			}
			if _, ok := h[tok]; ok {
				h[tok] = exp
				renewed++
			}
		}
	}
	return encInt(int64(renewed))
}
