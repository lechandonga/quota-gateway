package store

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// errRedisProto 表示 RESP 协议解析异常。
var errRedisProto = errors.New("store: malformed redis response")

// redisError 是 Redis 错误回复（如 NOSCRIPT）。
type redisError string

func (e redisError) Error() string { return "store: redis: " + string(e) }

// isNoScript 报告错误是否为 NOSCRIPT（脚本缓存丢失，需重新 EVAL）。
func isNoScript(err error) bool {
	var re redisError
	return errors.As(err, &re) && strings.HasPrefix(string(re), "NOSCRIPT")
}

// redisConn 是一条最小 RESP 连接。
type redisConn struct {
	c  net.Conn
	br *bufio.Reader
	bw *bufio.Writer
}

func (rc *redisConn) close() { _ = rc.c.Close() }

// redisClient 是带池的最小 Redis 客户端（仅依赖标准库）。
type redisClient struct {
	addr     string
	password string
	db       int
	prefix   string
	dialTO   time.Duration
	pool     chan *redisConn
	mu       sync.Mutex
	size     int
	maxSize  int
	closed   atomic.Bool
}

func newRedisClient(addr, password string, db int, prefix string, dialTO time.Duration, poolSize int) *redisClient {
	if poolSize <= 0 {
		poolSize = 8
	}
	return &redisClient{
		addr:     addr,
		password: password,
		db:       db,
		prefix:   prefix,
		dialTO:   dialTO,
		pool:     make(chan *redisConn, poolSize),
		maxSize:  poolSize,
	}
}

// key 加上全局前缀。
func (c *redisClient) key(k string) string {
	if c.prefix == "" {
		return k
	}
	return c.prefix + ":" + k
}

// connLease 表示一次借出的连接，Close 时归还或在坏连接时关闭。
type connLease struct {
	client *redisClient
	conn   *redisConn
	bad    bool
	once   sync.Once
}

func (l *connLease) close() {
	l.once.Do(func() {
		if l.bad {
			l.client.dropConn(l.conn)
			return
		}
		l.client.putConn(l.conn)
	})
}

func (c *redisClient) getConn(ctx context.Context) (*connLease, error) {
	if c.closed.Load() {
		return nil, errors.New("store: redis client closed")
	}
	select {
	case rc := <-c.pool:
		return &connLease{client: c, conn: rc}, nil
	default:
	}

	c.mu.Lock()
	if c.size < c.maxSize {
		c.size++
		c.mu.Unlock()
		rc, err := c.dial(ctx)
		if err != nil {
			c.mu.Lock()
			c.size--
			c.mu.Unlock()
			// 新建失败时，仍尝试复用池中可能已归还的连接。
			select {
			case rc2 := <-c.pool:
				return &connLease{client: c, conn: rc2}, nil
			default:
				return nil, err
			}
		}
		return &connLease{client: c, conn: rc}, nil
	}
	c.mu.Unlock()

	select {
	case rc := <-c.pool:
		return &connLease{client: c, conn: rc}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *redisClient) putConn(rc *redisConn) {
	if c.closed.Load() {
		rc.close()
		return
	}
	select {
	case c.pool <- rc:
	default:
		rc.close()
	}
}

func (c *redisClient) dropConn(rc *redisConn) {
	rc.close()
	c.mu.Lock()
	if c.size > 0 {
		c.size--
	}
	c.mu.Unlock()
}

func (c *redisClient) dial(ctx context.Context) (*redisConn, error) {
	d := net.Dialer{Timeout: c.dialTO}
	nc, err := d.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return nil, err
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = nc.SetDeadline(dl)
	}
	rc := &redisConn{c: nc, br: bufio.NewReader(nc), bw: bufio.NewWriter(nc)}
	// 认证与选库。
	if c.password != "" {
		if _, err := rc.command(ctx, "AUTH", c.password); err != nil {
			rc.close()
			return nil, err
		}
	}
	if c.db != 0 {
		if _, err := rc.command(ctx, "SELECT", strconv.Itoa(c.db)); err != nil {
			rc.close()
			return nil, err
		}
	}
	_ = nc.SetDeadline(time.Time{})
	return rc, nil
}

// do 执行一条命令并读取回复。
func (c *redisClient) do(ctx context.Context, args ...string) (any, error) {
	lease, err := c.getConn(ctx)
	if err != nil {
		return nil, err
	}
	defer lease.close()
	if dl, ok := ctx.Deadline(); ok {
		_ = lease.conn.c.SetDeadline(dl)
		defer lease.conn.c.SetDeadline(time.Time{})
	}
	v, err := lease.conn.command(ctx, args...)
	if err != nil {
		lease.bad = true
	}
	return v, err
}

// eval 执行 Lua 脚本；优先 EVALSHA，NOSCRIPT 时回退 EVAL。
func (c *redisClient) eval(ctx context.Context, sha, script string, keys []string, args []string) (any, error) {
	lease, err := c.getConn(ctx)
	if err != nil {
		return nil, err
	}
	defer lease.close()
	if dl, ok := ctx.Deadline(); ok {
		_ = lease.conn.c.SetDeadline(dl)
		defer lease.conn.c.SetDeadline(time.Time{})
	}
	call := func(cmd string, first string) (any, error) {
		full := append([]string{cmd, first}, strconv.Itoa(len(keys)))
		full = append(full, keys...)
		full = append(full, args...)
		return lease.conn.command(ctx, full...)
	}
	v, err := call("EVALSHA", sha)
	if err != nil && isNoScript(err) {
		v, err = call("EVAL", script)
	}
	if err != nil {
		lease.bad = true
	}
	return v, err
}

func (c *redisClient) ping(ctx context.Context) error {
	v, err := c.do(ctx, "PING")
	if err != nil {
		return err
	}
	if s, ok := v.(string); !ok || s != "PONG" {
		return fmt.Errorf("%w: unexpected PING reply %v", errRedisProto, v)
	}
	return nil
}

func (c *redisClient) close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	for {
		select {
		case rc := <-c.pool:
			rc.close()
		default:
			return nil
		}
	}
}

// command 在连接上发送命令并读取一个回复。
func (rc *redisConn) command(ctx context.Context, args ...string) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := writeCommand(rc.bw, args); err != nil {
		return nil, err
	}
	if err := rc.bw.Flush(); err != nil {
		return nil, err
	}
	return readReply(rc.br)
}

// ---- RESP 编解码 ----

func writeCommand(w io.Writer, args []string) error {
	if _, err := fmt.Fprintf(w, "*%d\r\n", len(args)); err != nil {
		return err
	}
	for _, a := range args {
		if _, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(a), a); err != nil {
			return err
		}
	}
	return nil
}

// readReply 读取一个 RESP 回复（支持简单串/错误/整数/批量串/数组）。
func readReply(r *bufio.Reader) (any, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, io.ErrUnexpectedEOF
	}
	switch line[0] {
	case '+':
		return line[1:], nil
	case '-':
		return nil, redisError(line[1:])
	case ':':
		n, err := strconv.ParseInt(line[1:], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: integer %q", errRedisProto, line)
		}
		return n, nil
	case '$':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, fmt.Errorf("%w: bulk len %q", errRedisProto, line)
		}
		if n < 0 {
			return nil, nil // nil bulk
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, fmt.Errorf("%w: array len %q", errRedisProto, line)
		}
		if n < 0 {
			return nil, nil
		}
		arr := make([]any, n)
		for i := 0; i < n; i++ {
			if arr[i], err = readReply(r); err != nil {
				return nil, err
			}
		}
		return arr, nil
	default:
		return nil, fmt.Errorf("%w: unexpected prefix %q", errRedisProto, line[0])
	}
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	return line, nil
}

func atoiReply(v any) (int64, error) {
	n, ok := v.(int64)
	if !ok {
		return 0, fmt.Errorf("%w: want integer, got %T", errRedisProto, v)
	}
	return n, nil
}

func strReply(v any) (string, error) {
	if v == nil {
		return "", nil
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("%w: want string, got %T", errRedisProto, v)
	}
	return s, nil
}
