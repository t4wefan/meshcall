package meshcall

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	DefaultRouterURL       = "ws://127.0.0.1:8765"
	routerRegistryService  = "meshcall.router.v1.Registry"
	routerAuthService      = "meshcall.router.v1.AuthService"
)

// BearerToken formats an Authorization header value for a bearer token ("Bearer …").
func BearerToken(token string) string {
	if token == "" || hasControlChars(token) {
		panic("meshcall: bearer token must be non-empty and contain no control characters")
	}
	return "Bearer " + token
}

// BasicAuth formats an Authorization header value for HTTP Basic credentials ("Basic …").
func BasicAuth(username, password string) string {
	if username == "" || strings.Contains(username, ":") || hasControlChars(username) || hasControlChars(password) {
		panic("meshcall: invalid BasicAuth username or password")
	}
	encoded := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
	return "Basic " + encoded
}

func hasControlChars(s string) bool {
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return true
		}
	}
	return false
}

// --- Built-in Router service types -----------------------------------------

// RegistryInstance is one live server instance reported by the Registry service.
type RegistryInstance struct {
	ID          string `json:"id"`
	Inflight    int    `json:"inflight"`
	MaxInflight int    `json:"max_inflight,omitempty"`
	Breaker     string `json:"breaker"`
	Draining    bool   `json:"draining"`
}

// RegistryMethod is one method entry in RegistryService.
type RegistryMethod struct {
	Name         string          `json:"name"`
	Stream       string          `json:"stream"`
	Balance      BalancePolicy   `json:"balance"`
	Description  *string         `json:"description,omitempty"`
	Annotations  json.RawMessage `json:"annotations,omitempty"`
	Deprecated   *string         `json:"deprecated,omitempty"`
	Parameters   []string        `json:"parameters,omitempty"`
	DuplexReason *string         `json:"duplex_reason,omitempty"`
	Subscription json.RawMessage `json:"subscription,omitempty"`
	Instances    []string        `json:"instances"`
}

// RegistryService is one entry returned by Router.ListServices (meshcall.router.v1.Registry/list).
type RegistryService struct {
	Name        string             `json:"name"`
	Version     string             `json:"version,omitempty"`
	Description *string            `json:"description,omitempty"`
	Targeted    bool               `json:"targeted"`
	Instances   []RegistryInstance `json:"instances"`
	Methods     []RegistryMethod   `json:"methods"`
}

// RegistryServiceDetail is returned by Router.DescribeService (meshcall.router.v1.Registry/describe).
type RegistryServiceDetail struct {
	RegistryService
	Contract json.RawMessage `json:"contract,omitempty"`
}

// RouterScope grants access to a service and a list of methods (or "*").
type RouterScope struct {
	Service  string   `json:"service"`
	Methods  []string `json:"methods,omitempty"`
	Register bool     `json:"register,omitempty"`
}

// WhoamiResponse is returned by Router.Whoami (meshcall.router.v1.AuthService/whoami).
type WhoamiResponse struct {
	Username        string   `json:"username"`
	Roles           []string `json:"roles"`
	Register        []string `json:"register"`
	Call            []string `json:"call"`
	ExpiresAtUnixMS *int64   `json:"expires_at_unix_ms,omitempty"`
}

// IssuedToken is returned by Router.IssueToken (meshcall.router.v1.AuthService/issue_token).
type IssuedToken struct {
	Token           string        `json:"token"`
	TokenID         string        `json:"token_id"`
	ExpiresAtUnixMS int64         `json:"expires_at_unix_ms"`
	Scopes          []RouterScope `json:"scopes"`
}

// RevokeTokenResponse is returned by Router.RevokeToken (meshcall.router.v1.AuthService/revoke_token).
type RevokeTokenResponse struct {
	Revoked bool `json:"revoked"`
}

// --- Router configuration & options ----------------------------------------

type routerConfig struct {
	urls               []string
	unixPath           string
	pipe               string
	auth               string
	peerID             string
	pingInterval       time.Duration
	pingTimeout        time.Duration
	reconnect          *ReconnectPolicy
	retry              *RetryPolicy
	deadlineGrace      time.Duration
	handshakeTimeout   time.Duration
	maxFrameSize       int
	clientInterceptors []ClientInterceptor
	logger             *slog.Logger

	// Embedded Router subprocess options
	spawnIfLocal       bool
	httpDefaultTimeout time.Duration
	httpCorsOrigins    []string
	metricsListen      string
	authFile           string
	outboxSize         int
	maxConnections     int
	binaryPath         string
}

func defaultRouterConfig() routerConfig {
	return routerConfig{
		spawnIfLocal: true,
	}
}

// RouterOption configures a Router (and can also be passed directly to Serve / Run).
type RouterOption interface {
	applyRouter(*routerConfig)
}

type routerOptFunc func(*routerConfig)

func (f routerOptFunc) applyRouter(c *routerConfig) { f(c) }
func (f routerOptFunc) applyServe(s *serveConfig)   { f(&s.routerCfg) }

// WithRouterURLs sets one or more Router WebSocket URLs ("ws://...", "wss://...").
func WithRouterURLs(urls ...string) RouterOption {
	return routerOptFunc(func(c *routerConfig) { c.urls = append([]string{}, urls...) })
}

// WithUnixSocket configures the Router to use a Unix domain socket ("unix:///path" or "/path").
func WithUnixSocket(path string) RouterOption {
	return routerOptFunc(func(c *routerConfig) { c.unixPath = path })
}

// WithPipe configures the Router to use a Windows named pipe ("pipe://NAME" or "NAME").
func WithPipe(name string) RouterOption {
	return routerOptFunc(func(c *routerConfig) { c.pipe = name })
}

// WithAuth sets the Authorization header value (e.g. BearerToken(...) or BasicAuth(...)).
func WithAuth(authorization string) RouterOption {
	return routerOptFunc(func(c *routerConfig) { c.auth = authorization })
}

// WithPeerID sets the client peer ID sent during handshake.
func WithPeerID(peerID string) RouterOption {
	return routerOptFunc(func(c *routerConfig) { c.peerID = peerID })
}

// WithPing sets the keepalive ping interval and pong timeout.
func WithPing(interval, timeout time.Duration) RouterOption {
	return routerOptFunc(func(c *routerConfig) {
		c.pingInterval = interval
		c.pingTimeout = timeout
	})
}

// WithReconnect sets the exponential backoff policy for reconnecting after a lost connection.
func WithReconnect(policy ReconnectPolicy) RouterOption {
	return routerOptFunc(func(c *routerConfig) { c.reconnect = &policy })
}

// WithRetry sets the client-side unary retry policy.
func WithRetry(policy RetryPolicy) RouterOption {
	return routerOptFunc(func(c *routerConfig) { c.retry = &policy })
}

// WithDeadlineGrace sets how long past a call's timeout the client waits for the peer's deadline error.
func WithDeadlineGrace(grace time.Duration) RouterOption {
	return routerOptFunc(func(c *routerConfig) { c.deadlineGrace = grace })
}

// WithHandshakeTimeout bounds the WebSocket + hello handshake.
func WithHandshakeTimeout(timeout time.Duration) RouterOption {
	return routerOptFunc(func(c *routerConfig) { c.handshakeTimeout = timeout })
}

// WithMaxFrameSize caps incoming and outgoing frame size in bytes.
func WithMaxFrameSize(bytes int) RouterOption {
	return routerOptFunc(func(c *routerConfig) { c.maxFrameSize = bytes })
}

// WithClientInterceptors attaches client interceptors to the Router's underlying client.
func WithClientInterceptors(interceptors ...ClientInterceptor) RouterOption {
	return routerOptFunc(func(c *routerConfig) {
		c.clientInterceptors = append(c.clientInterceptors, interceptors...)
	})
}

// WithSpawnIfLocal controls whether a local Router subprocess is automatically spawned when the target local port/socket is not listening (default true).
func WithSpawnIfLocal(spawn bool) RouterOption {
	return routerOptFunc(func(c *routerConfig) { c.spawnIfLocal = spawn })
}

// WithHTTPConfig configures the embedded Router's HTTP v1 gateway timeout and CORS origins (HTTP shares the same port as WebSocket).
func WithHTTPConfig(defaultTimeout time.Duration, corsOrigins ...string) RouterOption {
	return routerOptFunc(func(c *routerConfig) {
		c.httpDefaultTimeout = defaultTimeout
		c.httpCorsOrigins = append([]string{}, corsOrigins...)
	})
}

// WithMetricsListen configures the Prometheus metrics listener address ("host:port") when spawning a local Router.
func WithMetricsListen(addr string) RouterOption {
	return routerOptFunc(func(c *routerConfig) { c.metricsListen = addr })
}

// WithAuthFile sets the path to the Router auth configuration file when spawning a local Router.
func WithAuthFile(path string) RouterOption {
	return routerOptFunc(func(c *routerConfig) { c.authFile = path })
}

// WithRouterLimits sets the per-connection outbox size (bytes) and max concurrent connections when spawning a local Router.
func WithRouterLimits(outboxSize, maxConnections int) RouterOption {
	return routerOptFunc(func(c *routerConfig) {
		c.outboxSize = outboxSize
		c.maxConnections = maxConnections
	})
}

// WithRouterBinary overrides the path to the meshcall-router executable.
func WithRouterBinary(binaryPath string) RouterOption {
	return routerOptFunc(func(c *routerConfig) { c.binaryPath = binaryPath })
}

// WithRouterLogger sets the structured logger used by the Router client.
func WithRouterLogger(logger *slog.Logger) RouterOption {
	return routerOptFunc(func(c *routerConfig) { c.logger = logger })
}

// --- Unified Router object -------------------------------------------------

// Router represents a MeshCall Router endpoint, its multiplexed client connection,
// and its built-in Registry and AuthService RPC methods.
type Router struct {
	cfg       routerConfig
	endpoints []string
	client    *Client
	spawned   *spawnedRouter
}

// NewRouter constructs a Router configuration and lazy multiplexed client.
// It can be passed to Serve / Run (via WithRouter) or used directly by generated clients.
func NewRouter(opts ...RouterOption) *Router {
	cfg := defaultRouterConfig()
	for _, opt := range opts {
		if opt != nil {
			opt.applyRouter(&cfg)
		}
	}
	endpoints := resolveRouterEndpoints(cfg)
	r := &Router{cfg: cfg, endpoints: endpoints}
	if c, err := r.buildClient(endpoints); err == nil {
		r.client = c
	}
	return r
}

// ConnectRouter connects to a Router (auto-spawning a local meshcall-router if
// SpawnIfLocal is true and the local address is not listening yet) and returns
// a ready *Router that multiplexes generated clients and exposes built-in services.
func ConnectRouter(ctx context.Context, opts ...RouterOption) (*Router, error) {
	r := NewRouter(opts...)
	if r.cfg.spawnIfLocal && len(r.endpoints) == 1 {
		if spawned, updatedURL, err := maybeSpawnLocalRouter(ctx, r.cfg, r.endpoints[0]); err != nil {
			return nil, err
		} else if spawned != nil {
			r.spawned = spawned
			r.endpoints = []string{updatedURL}
		}
	}
	c, err := r.buildClient(r.endpoints)
	if err != nil {
		if r.spawned != nil {
			r.spawned.close()
		}
		return nil, err
	}
	if r.spawned != nil {
		spawned := r.spawned
		c.onClose = func() { spawned.close() }
	}
	if _, err := c.connection(ctx); err != nil {
		_ = c.Close()
		return nil, err
	}
	r.client = c
	return r, nil
}

func (r *Router) buildClient(endpoints []string) (*Client, error) {
	clientOpts := ClientOptions{
		Authorization:    r.cfg.auth,
		PeerID:           r.cfg.peerID,
		MaxFrameSize:     r.cfg.maxFrameSize,
		PingInterval:     r.cfg.pingInterval,
		PingTimeout:      r.cfg.pingTimeout,
		DeadlineGrace:    r.cfg.deadlineGrace,
		HandshakeTimeout: r.cfg.handshakeTimeout,
		Retry:            r.cfg.retry,
		Interceptors:     r.cfg.clientInterceptors,
		Logger:           r.cfg.logger,
	}
	if r.cfg.reconnect != nil {
		clientOpts.Reconnect = *r.cfg.reconnect
	}
	return NewClient(clientOpts, endpoints...)
}

// Client returns the underlying multiplexed *Client (satisfying Caller).
func (r *Router) Client() *Client {
	if r.client == nil {
		c, err := r.buildClient(r.endpoints)
		if err != nil {
			panic(err)
		}
		r.client = c
	}
	return r.client
}

// Endpoints returns the resolved Router endpoint URLs.
func (r *Router) Endpoints() []string {
	return append([]string{}, r.endpoints...)
}

// URL returns the primary Router endpoint URL.
func (r *Router) URL() string {
	if len(r.endpoints) == 0 {
		return DefaultRouterURL
	}
	return r.endpoints[0]
}

// WithOptions returns a lightweight shallow copy of Router that attaches default
// CallOptions to calls made through it while sharing the underlying connection.
func (r *Router) WithOptions(opts CallOptions) *Router {
	clone := *r
	clone.client = r.Client().WithOptions(opts)
	return &clone
}

// Close closes the underlying client connection and stops any locally spawned Router subprocess.
func (r *Router) Close() error {
	var err error
	if r.client != nil {
		err = r.client.Close()
	}
	if r.spawned != nil {
		r.spawned.close()
	}
	return err
}

// --- Built-in Router service methods ---------------------------------------

// ListServices calls meshcall.router.v1.Registry/list to list all registered services.
func (r *Router) ListServices(ctx context.Context, opts ...CallOption) ([]RegistryService, error) {
	var req struct{}
	var resp struct {
		Services []RegistryService `json:"services"`
	}
	callOpts := append([]CallOption{IdempotentCall()}, opts...)
	resp, err := Unary[struct{}, struct {
		Services []RegistryService `json:"services"`
	}](ctx, r.Client(), routerRegistryService, "list", req, callOpts...)
	if err != nil {
		return nil, err
	}
	if resp.Services == nil {
		return []RegistryService{}, nil
	}
	return resp.Services, nil
}

// DescribeService calls meshcall.router.v1.Registry/describe for one service.
func (r *Router) DescribeService(ctx context.Context, service string, opts ...CallOption) (RegistryServiceDetail, error) {
	type describeReq struct {
		Service string `json:"service"`
	}
	callOpts := append([]CallOption{IdempotentCall()}, opts...)
	return Unary[describeReq, RegistryServiceDetail](
		ctx,
		r.Client(),
		routerRegistryService,
		"describe",
		describeReq{Service: service},
		callOpts...,
	)
}

// Whoami calls meshcall.router.v1.AuthService/whoami to inspect the caller's identity and scopes.
func (r *Router) Whoami(ctx context.Context, opts ...CallOption) (WhoamiResponse, error) {
	var req struct{}
	callOpts := append([]CallOption{IdempotentCall()}, opts...)
	return Unary[struct{}, WhoamiResponse](ctx, r.Client(), routerAuthService, "whoami", req, callOpts...)
}

// IssueToken calls meshcall.router.v1.AuthService/issue_token to mint a scoped bearer token.
func (r *Router) IssueToken(ctx context.Context, scopes []RouterScope, ttlSeconds int64, opts ...CallOption) (IssuedToken, error) {
	if ttlSeconds <= 0 {
		ttlSeconds = 900
	}
	type issueReq struct {
		Scopes     []RouterScope `json:"scopes"`
		TTLSeconds int64         `json:"ttl_seconds"`
	}
	return Unary[issueReq, IssuedToken](
		ctx,
		r.Client(),
		routerAuthService,
		"issue_token",
		issueReq{Scopes: scopes, TTLSeconds: ttlSeconds},
		opts...,
	)
}

// RevokeToken calls meshcall.router.v1.AuthService/revoke_token to revoke a token by its ID.
func (r *Router) RevokeToken(ctx context.Context, tokenID string, opts ...CallOption) (RevokeTokenResponse, error) {
	type revokeReq struct {
		TokenID string `json:"token_id"`
	}
	return Unary[revokeReq, RevokeTokenResponse](
		ctx,
		r.Client(),
		routerAuthService,
		"revoke_token",
		revokeReq{TokenID: tokenID},
		opts...,
	)
}

// --- Serve / Run & local Router launcher -----------------------------------

type serveConfig struct {
	routerCfg    routerConfig
	router       *Router
	instanceID   string
	maxInflight  int
	interceptors []ServerInterceptor
	logger       *slog.Logger
	accessLog    bool
	idleTimeout  time.Duration
	strict       bool
	drainTimeout time.Duration
	listenAddr   string
	dependencies []DependencyProvider
}

// ServeOption configures Serve and Run.
type ServeOption interface {
	applyServe(*serveConfig)
}

type serveOptFunc func(*serveConfig)

func (f serveOptFunc) applyServe(s *serveConfig) { f(s) }

// WithRouter attaches an explicit *Router configuration to Serve or Run.
func WithRouter(router *Router) ServeOption {
	return serveOptFunc(func(s *serveConfig) {
		s.router = router
		if router != nil {
			s.routerCfg = router.cfg
		}
	})
}

// WithInstanceID sets the server's instance ID.
func WithInstanceID(id string) ServeOption {
	return serveOptFunc(func(s *serveConfig) { s.instanceID = id })
}

// WithMaxInflight caps concurrent in-flight calls across all services on this server.
func WithMaxInflight(n int) ServeOption {
	return serveOptFunc(func(s *serveConfig) { s.maxInflight = n })
}

// WithDrainTimeout sets the maximum duration to wait for in-flight calls during graceful shutdown (default 10s).
func WithDrainTimeout(d time.Duration) ServeOption {
	return serveOptFunc(func(s *serveConfig) { s.drainTimeout = d })
}

// WithStrict fails server startup if any contract lint warnings are found.
func WithStrict(strict bool) ServeOption {
	return serveOptFunc(func(s *serveConfig) { s.strict = strict })
}

// WithAccessLog enables per-call access logging on the server.
func WithAccessLog(enabled bool) ServeOption {
	return serveOptFunc(func(s *serveConfig) { s.accessLog = enabled })
}

// WithServerInterceptors attaches server interceptors.
func WithServerInterceptors(interceptors ...ServerInterceptor) ServeOption {
	return serveOptFunc(func(s *serveConfig) {
		s.interceptors = append(s.interceptors, interceptors...)
	})
}

// WithServerLogger sets the structured logger used by the server.
func WithServerLogger(logger *slog.Logger) ServeOption {
	return serveOptFunc(func(s *serveConfig) { s.logger = logger })
}

// WithDirectListen starts the server in direct listener mode (without a Router) on addr.
func WithDirectListen(addr string) ServeOption {
	return serveOptFunc(func(s *serveConfig) { s.listenAddr = addr })
}

// RunningServer is an active server started by Serve.
type RunningServer struct {
	Server          *Server
	RouterURL       string
	RouterEndpoints []string
	drainTimeout    time.Duration
	spawned         *spawnedRouter
	closeOnce       sync.Once
	closeErr        error
}

// Close gracefully drains in-flight calls and shuts down the server and any embedded Router.
func (r *RunningServer) Close() error {
	r.closeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), r.drainTimeout)
		defer cancel()
		_ = r.Server.Drain(ctx)
		r.closeErr = r.Server.Close()
		if r.spawned != nil {
			r.spawned.close()
		}
	})
	return r.closeErr
}

// Serve starts one or more services, auto-spawning a local Router when targeting
// a local address that is not yet listening, and returns a *RunningServer handle.
// Arguments may be *Service, []*Service, a struct (or pointer) embedding
// ServiceMeta, *Router, ServeOption, or RouterOption.
func Serve(ctx context.Context, args ...any) (*RunningServer, error) {
	cfg := serveConfig{
		routerCfg:    defaultRouterConfig(),
		drainTimeout: 10 * time.Second,
	}
	// Pass 1: apply options first so WithDependencies is available when reflecting structs.
	for _, arg := range args {
		switch v := arg.(type) {
		case nil, *Service, []*Service:
			continue
		case *Router:
			WithRouter(v).applyServe(&cfg)
		case ServeOption:
			v.applyServe(&cfg)
		case RouterOption:
			v.applyRouter(&cfg.routerCfg)
		default:
			if hasServiceMeta(arg) {
				continue
			}
			return nil, fmt.Errorf("meshcall: unsupported Serve argument of type %T", arg)
		}
	}
	// Pass 2: collect and reflect services.
	var services []*Service
	for _, arg := range args {
		switch v := arg.(type) {
		case nil:
			continue
		case *Service:
			services = append(services, v)
		case []*Service:
			services = append(services, v...)
		case *Router, ServeOption, RouterOption:
			continue
		default:
			if hasServiceMeta(arg) {
				svc, err := ReflectService(arg, WithDependencies(cfg.dependencies...))
				if err != nil {
					return nil, err
				}
				services = append(services, svc)
			}
		}
	}
	serverOpts := ServerOptions{
		Services:      services,
		InstanceID:    cfg.instanceID,
		MaxInflight:   cfg.maxInflight,
		Interceptors:  cfg.interceptors,
		Logger:        cfg.logger,
		AccessLog:     cfg.accessLog,
		PingInterval:  cfg.routerCfg.pingInterval,
		PingTimeout:   cfg.routerCfg.pingTimeout,
		Authorization: cfg.routerCfg.auth,
		IdleTimeout:   cfg.idleTimeout,
		MaxFrameSize:  cfg.routerCfg.maxFrameSize,
		Strict:        cfg.strict,
	}
	if cfg.routerCfg.reconnect != nil {
		serverOpts.Reconnect = *cfg.routerCfg.reconnect
	}
	srv, err := NewServer(serverOpts)
	if err != nil {
		return nil, err
	}
	if cfg.listenAddr != "" {
		if err := srv.Listen(cfg.listenAddr); err != nil {
			_ = srv.Close()
			return nil, err
		}
		var urlStr string
		if addr := srv.Addr(); addr != nil {
			urlStr = "ws://" + addr.String()
		}
		return &RunningServer{
			Server:          srv,
			RouterURL:       urlStr,
			RouterEndpoints: []string{urlStr},
			drainTimeout:    cfg.drainTimeout,
		}, nil
	}

	endpoints := resolveRouterEndpoints(cfg.routerCfg)
	var spawned *spawnedRouter
	if cfg.routerCfg.spawnIfLocal && len(endpoints) == 1 {
		s, updatedURL, err := maybeSpawnLocalRouter(ctx, cfg.routerCfg, endpoints[0])
		if err != nil {
			_ = srv.Close()
			return nil, err
		}
		if s != nil {
			spawned = s
			endpoints = []string{updatedURL}
			if cfg.router != nil {
				cfg.router.endpoints = []string{updatedURL}
				if c, err := cfg.router.buildClient(endpoints); err == nil {
					cfg.router.client = c
				}
			}
		}
	}
	if err := srv.ConnectRouter(ctx, endpoints...); err != nil {
		_ = srv.Close()
		if spawned != nil {
			spawned.close()
		}
		return nil, err
	}
	return &RunningServer{
		Server:          srv,
		RouterURL:       endpoints[0],
		RouterEndpoints: endpoints,
		drainTimeout:    cfg.drainTimeout,
		spawned:         spawned,
	}, nil
}

// Run starts one or more services and blocks until SIGINT or SIGTERM is received,
// then gracefully drains in-flight calls and closes the server.
func Run(args ...any) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	running, err := Serve(ctx, args...)
	if err != nil {
		return err
	}
	<-ctx.Done()
	return running.Close()
}

// --- Endpoint resolution & local subprocess management ---------------------

func resolveRouterEndpoints(cfg routerConfig) []string {
	if cfg.unixPath != "" {
		p := cfg.unixPath
		if !strings.HasPrefix(p, "unix://") {
			p = "unix://" + p
		}
		return []string{p}
	}
	if cfg.pipe != "" {
		p := cfg.pipe
		if !strings.HasPrefix(p, "pipe://") {
			p = "pipe://" + p
		}
		return []string{p}
	}
	if len(cfg.urls) > 0 {
		return append([]string{}, cfg.urls...)
	}
	if env := strings.TrimSpace(os.Getenv("MESHCALL_ROUTER_URL")); env != "" {
		var out []string
		for _, part := range strings.Split(env, ",") {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				out = append(out, trimmed)
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return []string{DefaultRouterURL}
}

type spawnedRouter struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	once   sync.Once
}

func (s *spawnedRouter) close() {
	if s == nil {
		return
	}
	s.once.Do(func() {
		if s.stdin != nil {
			_ = s.stdin.Close()
		}
		if s.cmd != nil && s.cmd.Process != nil {
			done := make(chan struct{})
			go func() {
				_ = s.cmd.Wait()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				_ = s.cmd.Process.Kill()
				<-done
			}
		}
	})
}

func findRouterBinary(explicit string) (string, error) {
	if explicit != "" {
		if info, err := os.Stat(explicit); err == nil && !info.IsDir() {
			return explicit, nil
		}
		return "", fmt.Errorf("meshcall: router binary not found at %q", explicit)
	}
	if env := os.Getenv("MESHCALL_ROUTER_BINARY"); env != "" {
		if info, err := os.Stat(env); err == nil && !info.IsDir() {
			return env, nil
		}
		return "", fmt.Errorf("meshcall: MESHCALL_ROUTER_BINARY %q does not exist", env)
	}
	exeName := "meshcall-router"
	if runtime.GOOS == "windows" {
		exeName += ".exe"
	}
	if _, file, _, ok := runtime.Caller(0); ok {
		candidate := filepath.Join(filepath.Dir(file), "..", "meshcall-router", "bin", exeName)
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate, nil
		}
	}
	if path, err := exec.LookPath(exeName); err == nil {
		return path, nil
	}
	return "", errors.New("meshcall: could not find meshcall-router binary (set MESHCALL_ROUTER_BINARY or install meshcall-router)")
}

func isLocalHost(host string) bool {
	return host == "127.0.0.1" || host == "localhost" || host == "::1"
}

func tcpReachable(host string, port int) bool {
	if port <= 0 {
		return false
	}
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(port)), 150*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func unixReachable(path string) bool {
	conn, err := net.DialTimeout("unix", path, 150*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func maybeSpawnLocalRouter(ctx context.Context, cfg routerConfig, rawEndpoint string) (*spawnedRouter, string, error) {
	var args []string
	var checkReachable func() bool
	var isZeroPort bool

	switch {
	case strings.HasPrefix(rawEndpoint, "unix://"):
		sockPath := strings.TrimPrefix(rawEndpoint, "unix://")
		if unixReachable(sockPath) {
			return nil, rawEndpoint, nil
		}
		args = append(args, "--unix-socket", sockPath)
		checkReachable = func() bool { return unixReachable(sockPath) }
	case strings.HasPrefix(rawEndpoint, "pipe://"):
		name := strings.TrimPrefix(rawEndpoint, "pipe://")
		args = append(args, "--pipe", name)
	case strings.HasPrefix(rawEndpoint, "ws://"):
		u, err := url.Parse(rawEndpoint)
		if err != nil {
			return nil, rawEndpoint, err
		}
		host := u.Hostname()
		if !isLocalHost(host) {
			return nil, rawEndpoint, nil
		}
		port := 8765
		if p := u.Port(); p != "" {
			parsed, err := strconv.Atoi(p)
			if err != nil {
				return nil, rawEndpoint, err
			}
			port = parsed
		}
		if port > 0 && tcpReachable(host, port) {
			return nil, rawEndpoint, nil
		}
		isZeroPort = port == 0
		args = append(args, "--host", host, "--port", strconv.Itoa(port))
		checkReachable = func() bool { return port > 0 && tcpReachable(host, port) }
	default:
		return nil, rawEndpoint, nil
	}

	args = append(args, "--shutdown-on-stdin-close")
	if cfg.httpDefaultTimeout > 0 {
		args = append(args, "--http-default-timeout", cfg.httpDefaultTimeout.String())
	}
	for _, origin := range cfg.httpCorsOrigins {
		args = append(args, "--http-cors-origin", origin)
	}
	if cfg.metricsListen != "" {
		args = append(args, "--metrics-listen", cfg.metricsListen)
	}
	if cfg.authFile != "" {
		args = append(args, "--auth-file", cfg.authFile)
	}
	if cfg.outboxSize > 0 {
		args = append(args, "--outbox-size", strconv.Itoa(cfg.outboxSize))
	}
	if cfg.maxConnections > 0 {
		args = append(args, "--max-connections", strconv.Itoa(cfg.maxConnections))
	}

	binary, err := findRouterBinary(cfg.binaryPath)
	if err != nil {
		return nil, rawEndpoint, err
	}

	cmd := exec.Command(binary, args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, rawEndpoint, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, rawEndpoint, err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return nil, rawEndpoint, err
	}

	type readyResult struct {
		url string
		err error
	}
	readyCh := make(chan readyResult, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			var payload struct {
				Kind       string `json:"kind"`
				Status     string `json:"status"`
				URL        string `json:"url"`
				Host       string `json:"host"`
				Port       int    `json:"port"`
				UnixPath   string `json:"unix_path"`
				UnixSocket string `json:"unix_socket"`
				Pipe       string `json:"pipe"`
			}
			if err := json.Unmarshal(scanner.Bytes(), &payload); err == nil && (payload.Kind == "router.ready" || payload.Kind == "ready" || payload.Status == "ready") {
				resolved := payload.URL
				if resolved == "" {
					if payload.UnixPath != "" {
						resolved = "unix://" + payload.UnixPath
					} else if payload.UnixSocket != "" {
						resolved = "unix://" + payload.UnixSocket
					} else if payload.Pipe != "" {
						resolved = "pipe://" + payload.Pipe
					} else if payload.Port > 0 {
						h := payload.Host
						if h == "" {
							h = "127.0.0.1"
						}
						resolved = fmt.Sprintf("ws://%s:%d", h, payload.Port)
					} else {
						resolved = rawEndpoint
					}
				}
				readyCh <- readyResult{url: resolved}
				// Drain remaining stdout so the subprocess never blocks.
				_, _ = io.Copy(io.Discard, stdout)
				return
			}
		}
		_ = cmd.Wait()
		readyCh <- readyResult{err: fmt.Errorf("meshcall-router exited before ready: %s", strings.TrimSpace(stderr.String()))}
	}()

	select {
	case res := <-readyCh:
		if res.err != nil {
			_ = stdin.Close()
			if !isZeroPort && checkReachable != nil && checkReachable() {
				return nil, rawEndpoint, nil
			}
			return nil, rawEndpoint, res.err
		}
		return &spawnedRouter{cmd: cmd, stdin: stdin}, res.url, nil
	case <-ctx.Done():
		_ = stdin.Close()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		return nil, rawEndpoint, ctx.Err()
	}
}
