package quota

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// HTTP API（与客户端共存于同一主版本 APIVersion；未知字段一律忽略，
// 从而保证新旧实例混部时的前向/后向兼容）：
//
//	POST /quota/v1/sessions              注册实例会话
//	POST /quota/v1/sessions/heartbeat    会话心跳
//	POST /quota/v1/config                下发/校验配置（热更新）
//	POST /quota/v1/rate/take             速率取令牌
//	POST /quota/v1/admit                 并发准入
//	POST /quota/v1/release               并发释放
//	GET  /quota/v1/healthz               健康检查
const (
	pathSession   = "/quota/v1/sessions"
	pathHeartbeat = "/quota/v1/sessions/heartbeat"
	pathConfig    = "/quota/v1/config"
	pathRateTake  = "/quota/v1/rate/take"
	pathAdmit     = "/quota/v1/admit"
	pathRelease   = "/quota/v1/release"
	pathHealth    = "/quota/v1/healthz"
)

// ---- 线上协议结构（独立于内部结构，字段名稳定） ----

type wireSession struct {
	InstanceID string `json:"instanceId"`
	TTLMillis  int64  `json:"ttlMillis"`
}

type wireHeartbeat struct {
	InstanceID string `json:"instanceId"`
}

type wireRateReq struct {
	Tenant   string `json:"tenant"`
	Iface    string `json:"iface"`
	Want     int64  `json:"want"`
	DedupKey string `json:"dedupKey"`
}

type wireRateResp struct {
	Allowed       bool  `json:"allowed"`
	Remaining     int64 `json:"remaining"`
	RetryMillis   int64 `json:"retryMillis"`
	ConfigVersion int64 `json:"configVersion"`
}

type wireAdmitReq struct {
	Tenant        string  `json:"tenant"`
	Iface         string  `json:"iface"`
	RequestID     string  `json:"requestId"`
	Attempt       int64   `json:"attempt"`
	HardCap       int64   `json:"hardCap"`
	GroupName     string  `json:"groupName"`
	GroupCapacity int64   `json:"groupCapacity"`
	Weight        float64 `json:"weight"`
	SessionID     string  `json:"sessionId"`
	ConfigVersion int64   `json:"configVersion"`
}

type wireAdmitResp struct {
	Admitted      bool   `json:"admitted"`
	Token         string `json:"token,omitempty"`
	Reason        string `json:"reason,omitempty"`
	RetryMillis   int64  `json:"retryMillis"`
	Active        int64  `json:"active"`
	ConfigVersion int64  `json:"configVersion"`
}

type wireReleaseReq struct {
	Token     string `json:"token"`
	RequestID string `json:"requestId"`
}

type wireReleaseResp struct {
	Released      bool  `json:"released"`
	ConfigVersion int64 `json:"configVersion"`
}

type wireError struct {
	Error string `json:"error"`
}

// CoordinatorServer 把 MemoryStore 暴露为 HTTP API，供多实例共享。
type CoordinatorServer struct {
	store   *MemoryStore
	clock   Clock
	httpSrv *http.Server
}

// NewCoordinatorServer 基于内存存储创建协调服务器。
func NewCoordinatorServer(store *MemoryStore, clock Clock) *CoordinatorServer {
	if clock == nil {
		clock = SystemClock()
	}
	return &CoordinatorServer{store: store, clock: clock}
}

// Handler 返回可挂载的 http.Handler（路径前缀 /quota/v1/）。
func (s *CoordinatorServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(pathSession, s.handleSession)
	mux.HandleFunc(pathHeartbeat, s.handleHeartbeat)
	mux.HandleFunc(pathConfig, s.handleConfig)
	mux.HandleFunc(pathRateTake, s.handleRateTake)
	mux.HandleFunc(pathAdmit, s.handleAdmit)
	mux.HandleFunc(pathRelease, s.handleRelease)
	mux.HandleFunc(pathHealth, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "api": APIVersion})
	})
	return mux
}

func (s *CoordinatorServer) handleSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req wireSession
	if err := decodeJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.InstanceID == "" {
		writeErr(w, http.StatusBadRequest, "instanceId required")
		return
	}
	ttl := time.Duration(req.TTLMillis) * time.Millisecond
	if ttl <= 0 {
		ttl = defaultSessionTTL
	}
	s.store.RegisterSession(SessionID(req.InstanceID), ttl)
	writeJSON(w, http.StatusOK, map[string]string{"status": "registered"})
}

func (s *CoordinatorServer) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req wireHeartbeat
	if err := decodeJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if !s.store.Heartbeat(SessionID(req.InstanceID)) {
		writeErr(w, http.StatusConflict, "session unknown or expired; re-register")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *CoordinatorServer) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var cfg Config
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid config payload: "+err.Error())
		return
	}
	// 校验 + 安装在存储层原子完成：非法/不兼容配置被拒绝，线上配置不受影响。
	if err := s.store.SyncConfig(r.Context(), &cfg); err != nil {
		code := http.StatusBadRequest
		if qe, ok := AsQuotaError(err); ok && qe.Reason == ReasonBadConfig {
			code = http.StatusUnprocessableEntity
		}
		writeErr(w, code, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "installed", "version": cfg.Version})
}

func (s *CoordinatorServer) handleRateTake(w http.ResponseWriter, r *http.Request) {
	var req wireRateReq
	if err := decodeJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	res, err := s.store.TryTakeRate(r.Context(), RateDeduction{
		Key:      RateKey{Tenant: req.Tenant, Iface: req.Iface},
		Want:     req.Want,
		DedupKey: req.DedupKey,
	})
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, wireRateResp{
		Allowed:       res.Allowed,
		Remaining:     res.Remaining,
		RetryMillis:   res.RetryIn.Milliseconds(),
		ConfigVersion: res.ConfigVersion,
	})
}

func (s *CoordinatorServer) handleAdmit(w http.ResponseWriter, r *http.Request) {
	var req wireAdmitReq
	if err := decodeJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	ar, err := s.store.Admit(r.Context(), AdmitRequest{
		Tenant:        req.Tenant,
		Iface:         req.Iface,
		RequestID:     RequestID(req.RequestID),
		Attempt:       req.Attempt,
		HardCap:       req.HardCap,
		GroupName:     req.GroupName,
		GroupCapacity: req.GroupCapacity,
		Weight:        req.Weight,
	})
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	if ar.Admitted && req.SessionID != "" {
		s.store.BindLease(ar.Token, SessionID(req.SessionID))
	}
	writeJSON(w, http.StatusOK, wireAdmitResp{
		Admitted:      ar.Admitted,
		Token:         string(ar.Token),
		Reason:        string(ar.Reason),
		RetryMillis:   ar.RetryIn.Milliseconds(),
		Active:        ar.Active,
		ConfigVersion: ar.ConfigVersion,
	})
}

func (s *CoordinatorServer) handleRelease(w http.ResponseWriter, r *http.Request) {
	var req wireReleaseReq
	if err := decodeJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	res, err := s.store.Release(r.Context(), LeaseToken(req.Token), RequestID(req.RequestID))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, wireReleaseResp{Released: res.Released, ConfigVersion: res.ConfigVersion})
}

// Start 在 addr 上启动 HTTP 服务（非阻塞）。
func (s *CoordinatorServer) Start(addr string) error {
	mux := s.Handler()
	s.httpSrv = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	ln, err := netListen(addr)
	if err != nil {
		return err
	}
	go func() { _ = s.httpSrv.Serve(ln) }()
	return nil
}

// Shutdown 优雅关闭。
func (s *CoordinatorServer) Shutdown(ctx context.Context) error {
	if s.httpSrv == nil {
		return nil
	}
	return s.httpSrv.Shutdown(ctx)
}

func decodeJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	return dec.Decode(v)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, wireError{Error: msg})
}
