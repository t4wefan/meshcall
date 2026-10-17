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
	"net/http"
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
	DefaultRouterURL      = "ws://127.0.0.1:8765"
	routerRegistryService = "meshcall.router.v1.Registry"
	routerAuthService     = "meshcall.router.v1.AuthService"
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
	Instances    []string        `json:"instances"`
}

// RegistryService is one entry returned by Mesh.Registry().List() (meshcall.router.v1.Registry/list).
type RegistryService struct {
	Name        string             `json:"name"`
	Version     string             `json:"version,omitempty"`
	Description *string            `json:"description,omitempty"`
	Targeted    bool               `json:"targeted"`
	Instances   []RegistryInstance `json:"instances"`
	Methods     []RegistryMethod   `json:"methods"`
}

// RegistryServiceDetail is returned by Mesh.Registry().Describe() (meshcall.router.v1.Registry/describe).
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

// WhoamiResponse is returned by Mesh.Auth().Whoami() (meshcall.router.v1.AuthService/whoami).
type WhoamiResponse struct {
	Username        string   `json:"username"`
	Roles           []string `json:"roles"`
	Register        []string `json:"register"`
	Call            []string `json:"call"`
	ExpiresAtUnixMS *int64   `json:"expires_at_unix_ms,omitempty"`
}

// IssuedToken is returned by Mesh.Auth().IssueToken() (meshcall.router.v1.AuthService/issue_token).
type IssuedToken struct {
	Token           string        `json:"token"`
	TokenID         string        `json:"token_id"`
	ExpiresAtUnixMS int64         `json:"expires_at_unix_ms"`
	Scopes          []RouterScope `json:"scopes"`
}

// RevokeTokenResponse is returned by Mesh.Auth().RevokeToken() (meshcall.router.v1.AuthService/revoke_token).
type RevokeTokenResponse struct {
	Revoked bool `json:"revoked"`
}

// --- Layer 2: Router & LocalRouter -----------------------------------------

// RouterEndpoint is implemented by *Router (remote endpoint descriptor) and
// *LocalRouter (managed local meshcall-router subprocess).
type RouterEndpoint interface {
	URL() string
	Endpoints() []string
	IsHTTP() bool
	isRouterEndpoint()
}

type routerEndpointConfig struct {
	urls     []string
	unixPath string
	pipe     string
}

// RouterOption configures a remote Router endpoint descriptor.
type RouterOption interface {
	applyRouterEndpoint(*routerEndpointConfig)
}

type routerEndpointOptFunc func(*routerEndpointConfig)

func (f routerEndpointOptFunc) applyRouterEndpoint(c *routerEndpointConfig) { f(c) }

// WithRouterURLs sets one or more Router URLs ("ws://...", "wss://...", "http://...", "https://...").
func WithRouterURLs(urls ...string) RouterOption {
	return routerEndpointOptFunc(func(c *routerEndpointConfig) {
		c.urls = append(c.urls, urls...)
	})
}

type unixSocketOpt string

func (u unixSocketOpt) applyRouterEndpoint(c *routerEndpointConfig) { c.unixPath = string(u) }
func (u unixSocketOpt) applyLocalRouter(c *localRouterConfig)       { c.unixPath = string(u) }

// WithUnixSocket configures a Router or LocalRouter to use a Unix domain socket ("unix:///path" or "/path").
func WithUnixSocket(path string) interface {
	RouterOption
	LocalRouterOption
} {
	return unixSocketOpt(path)
}

type pipeOpt string

func (p pipeOpt) applyRouterEndpoint(c *routerEndpointConfig) { c.pipe = string(p) }
func (p pipeOpt) applyLocalRouter(c *localRouterConfig)       { c.pipe = string(p) }

// WithPipe configures a Router or LocalRouter to use a Windows named pipe ("pipe://NAME" or "NAME").
func WithPipe(name string) interface {
	RouterOption
	LocalRouterOption
} {
	return pipeOpt(name)
}

// Router is a Layer 2 endpoint descriptor for a remote/external meshcall-router.
// It performs no network I/O and spawns no subprocesses. Pass it to NewMesh or ConnectMesh.
type Router struct {
	endpoints []string
	isHTTP    bool
}

// NewRouter creates a Layer 2 Router endpoint descriptor.
// Arguments may be URL strings ("ws://...", "http://..."), []string, or RouterOption
// (WithRouterURLs, WithUnixSocket, WithPipe). When called with no arguments, it
// reads MESHCALL_ROUTER_URL or defaults to DefaultRouterURL ("ws://127.0.0.1:8765").
func NewRouter(args ...any) *Router {
	var cfg routerEndpointConfig
	for _, arg := range args {
		switch v := arg.(type) {
		case nil:
			continue
		case string:
			if trimmed := strings.TrimSpace(v); trimmed != "" {
				cfg.urls = append(cfg.urls, trimmed)
			}
		case []string:
			for _, u := range v {
				if trimmed := strings.TrimSpace(u); trimmed != "" {
					cfg.urls = append(cfg.urls, trimmed)
				}
			}
		case RouterOption:
			v.applyRouterEndpoint(&cfg)
		default:
			panic(fmt.Sprintf("meshcall: unsupported NewRouter argument of type %T (pass a URL string or RouterOption)", arg))
		}
	}
	endpoints, isHTTP := resolveAndValidateEndpoints(cfg)
	return &Router{
		endpoints: endpoints,
		isHTTP:    isHTTP,
	}
}

func (*Router) isRouterEndpoint() {}

// URL returns the primary Router endpoint URL.
func (r *Router) URL() string {
	if len(r.endpoints) == 0 {
		return DefaultRouterURL
	}
	return r.endpoints[0]
}

// Endpoints returns a copy of all configured Router endpoint URLs.
func (r *Router) Endpoints() []string {
	return append([]string{}, r.endpoints...)
}

// IsHTTP reports whether the Router uses an HTTP(S) endpoint.
func (r *Router) IsHTTP() bool {
	return r.isHTTP
}

func isValidEndpointScheme(u string) bool {
	return strings.HasPrefix(u, "ws://") ||
		strings.HasPrefix(u, "wss://") ||
		strings.HasPrefix(u, "unix://") ||
		strings.HasPrefix(u, "pipe://") ||
		strings.HasPrefix(u, "http://") ||
		strings.HasPrefix(u, "https://")
}

func isHTTPScheme(u string) bool {
	return strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://")
}

func resolveAndValidateEndpoints(cfg routerEndpointConfig) ([]string, bool) {
	choices := 0
	if len(cfg.urls) > 0 {
		choices++
	}
	if cfg.unixPath != "" {
		choices++
	}
	if cfg.pipe != "" {
		choices++
	}
	if choices > 1 {
		panic("meshcall: specify at most one of URLs, WithUnixSocket, or WithPipe on Router")
	}

	var resolved []string
	switch {
	case cfg.unixPath != "":
		p := cfg.unixPath
		if !strings.HasPrefix(p, "unix://") {
			p = "unix://" + p
		}
		resolved = []string{p}
	case cfg.pipe != "":
		p := cfg.pipe
		if !strings.HasPrefix(p, "pipe://") {
			p = "pipe://" + p
		}
		resolved = []string{p}
	case len(cfg.urls) > 0:
		resolved = append([]string{}, cfg.urls...)
	default:
		if env := strings.TrimSpace(os.Getenv("MESHCALL_ROUTER_URL")); env != "" {
			for _, part := range strings.Split(env, ",") {
				if trimmed := strings.TrimSpace(part); trimmed != "" {
					resolved = append(resolved, trimmed)
				}
			}
		}
		if len(resolved) == 0 {
			resolved = []string{DefaultRouterURL}
		}
	}

	for _, ep := range resolved {
		if !isValidEndpointScheme(ep) {
			panic(fmt.Sprintf("meshcall: invalid Router endpoint %q: must start with ws://, wss://, unix://, pipe://, http://, or https://", ep))
		}
	}
	hasHTTP := false
	hasNonHTTP := false
	for _, ep := range resolved {
		if isHTTPScheme(ep) {
			hasHTTP = true
		} else {
			hasNonHTTP = true
		}
	}
	if hasHTTP && hasNonHTTP {
		panic("meshcall: Router endpoints cannot mix HTTP(S) and WebSocket/Unix/Pipe schemes")
	}
	return resolved, hasHTTP
}

// --- LocalRouter (Managed OS Subprocess) -----------------------------------

type localRouterConfig struct {
	host               string
	port               int
	unixPath           string
	pipe               string
	httpDefaultTimeout time.Duration
	httpCorsOrigins    []string
	metricsListen      string
	authFile           string
	outboxSize         int
	maxConnections     int
	binaryPath         string
}

// LocalRouterOption configures a LocalRouter subprocess.
type LocalRouterOption interface {
	applyLocalRouter(*localRouterConfig)
}

type localRouterOptFunc func(*localRouterConfig)

func (f localRouterOptFunc) applyLocalRouter(c *localRouterConfig) { f(c) }

// WithHost sets the TCP bind host for LocalRouter (default "127.0.0.1").
func WithHost(host string) LocalRouterOption {
	return localRouterOptFunc(func(c *localRouterConfig) { c.host = host })
}

// WithPort sets the TCP bind port for LocalRouter (0 for an OS-assigned ephemeral port).
func WithPort(port int) LocalRouterOption {
	return localRouterOptFunc(func(c *localRouterConfig) { c.port = port })
}

// WithHTTPConfig configures the LocalRouter's HTTP v1 gateway timeout and CORS origins.
func WithHTTPConfig(defaultTimeout time.Duration, corsOrigins ...string) LocalRouterOption {
	return localRouterOptFunc(func(c *localRouterConfig) {
		c.httpDefaultTimeout = defaultTimeout
		c.httpCorsOrigins = append([]string{}, corsOrigins...)
	})
}

// WithMetricsListen configures the Prometheus metrics listener address ("host:port") on LocalRouter.
func WithMetricsListen(addr string) LocalRouterOption {
	return localRouterOptFunc(func(c *localRouterConfig) { c.metricsListen = addr })
}

// WithAuthFile sets the path to the Router auth configuration file on LocalRouter.
func WithAuthFile(path string) LocalRouterOption {
	return localRouterOptFunc(func(c *localRouterConfig) { c.authFile = path })
}

// WithRouterLimits sets the per-connection outbox size and max concurrent connections on LocalRouter.
func WithRouterLimits(outboxSize, maxConnections int) LocalRouterOption {
	return localRouterOptFunc(func(c *localRouterConfig) {
		c.outboxSize = outboxSize
		c.maxConnections = maxConnections
	})
}

// WithRouterBinary overrides the path to the meshcall-router executable for LocalRouter.
func WithRouterBinary(binaryPath string) LocalRouterOption {
	return localRouterOptFunc(func(c *localRouterConfig) { c.binaryPath = binaryPath })
}

// LocalRouter manages a local meshcall-router OS subprocess (Layer 2).
type LocalRouter struct {
	cfg       localRouterConfig
	mu        sync.Mutex
	spawned   *spawnedRouter
	url       string
	boundPort int
	pid       int
	running   bool
}

// NewLocalRouter creates a new LocalRouter subprocess manager (not started yet).
// Call Start(ctx) or use StartLocalRouter(ctx, opts...) to launch the subprocess.
func NewLocalRouter(opts ...LocalRouterOption) *LocalRouter {
	cfg := localRouterConfig{
		host: "127.0.0.1",
		port: 8765,
	}
	for _, opt := range opts {
		if opt != nil {
			opt.applyLocalRouter(&cfg)
		}
	}
	var initialURL string
	switch {
	case cfg.unixPath != "":
		p := cfg.unixPath
		if !strings.HasPrefix(p, "unix://") {
			p = "unix://" + p
		}
		initialURL = p
	case cfg.pipe != "":
		p := cfg.pipe
		if !strings.HasPrefix(p, "pipe://") {
			p = "pipe://" + p
		}
		initialURL = p
	default:
		initialURL = fmt.Sprintf("ws://%s:%d", cfg.host, cfg.port)
	}
	return &LocalRouter{
		cfg:       cfg,
		url:       initialURL,
		boundPort: cfg.port,
	}
}

// StartLocalRouter creates and starts a LocalRouter subprocess, waiting until it is ready.
func StartLocalRouter(ctx context.Context, opts ...LocalRouterOption) (*LocalRouter, error) {
	r := NewLocalRouter(opts...)
	if err := r.Start(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

func (*LocalRouter) isRouterEndpoint() {}

// Start spawns the meshcall-router subprocess and waits until it signals readiness.
func (r *LocalRouter) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.running {
		return nil
	}

	var args []string
	switch {
	case r.cfg.unixPath != "":
		sockPath := strings.TrimPrefix(r.cfg.unixPath, "unix://")
		args = append(args, "--unix-socket", sockPath)
	case r.cfg.pipe != "":
		name := strings.TrimPrefix(r.cfg.pipe, "pipe://")
		args = append(args, "--pipe", name)
	default:
		args = append(args, "--host", r.cfg.host, "--port", strconv.Itoa(r.cfg.port))
	}

	args = append(args, "--shutdown-on-stdin-close")
	if r.cfg.httpDefaultTimeout > 0 {
		args = append(args, "--http-default-timeout", r.cfg.httpDefaultTimeout.String())
	}
	for _, origin := range r.cfg.httpCorsOrigins {
		args = append(args, "--http-cors-origin", origin)
	}
	if r.cfg.metricsListen != "" {
		args = append(args, "--metrics-listen", r.cfg.metricsListen)
	}
	if r.cfg.authFile != "" {
		args = append(args, "--auth-file", r.cfg.authFile)
	}
	if r.cfg.outboxSize > 0 {
		args = append(args, "--outbox-size", strconv.Itoa(r.cfg.outboxSize))
	}
	if r.cfg.maxConnections > 0 {
		args = append(args, "--max-connections", strconv.Itoa(r.cfg.maxConnections))
	}

	binary, err := findRouterBinary(r.cfg.binaryPath)
	if err != nil {
		return err
	}

	cmd := exec.Command(binary, args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return err
	}

	type readyResult struct {
		url  string
		port int
		err  error
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
							h = r.cfg.host
						}
						resolved = fmt.Sprintf("ws://%s:%d", h, payload.Port)
					} else {
						resolved = r.url
					}
				}
				readyCh <- readyResult{url: resolved, port: payload.Port}
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
			return res.err
		}
		r.spawned = &spawnedRouter{cmd: cmd, stdin: stdin}
		r.url = res.url
		if res.port > 0 {
			r.boundPort = res.port
		} else if strings.HasPrefix(res.url, "ws://") {
			if u, err := url.Parse(res.url); err == nil {
				if p, err := strconv.Atoi(u.Port()); err == nil {
					r.boundPort = p
				}
			}
		}
		if cmd.Process != nil {
			r.pid = cmd.Process.Pid
		}
		r.running = true
		return nil
	case <-ctx.Done():
		_ = stdin.Close()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		return ctx.Err()
	}
}

// Stop terminates the LocalRouter subprocess and waits for it to exit.
func (r *LocalRouter) Stop() error {
	r.mu.Lock()
	spawned := r.spawned
	r.spawned = nil
	r.running = false
	r.pid = 0
	r.mu.Unlock()
	if spawned != nil {
		spawned.close()
	}
	return nil
}

// Close is an alias for Stop.
func (r *LocalRouter) Close() error {
	return r.Stop()
}

// URL returns the WebSocket/Unix/Pipe endpoint URL of the LocalRouter.
func (r *LocalRouter) URL() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.url
}

// HTTPURL returns the HTTP gateway URL ("http://host:port") if listening on TCP, or "".
func (r *LocalRouter) HTTPURL() string {
	u := r.URL()
	if strings.HasPrefix(u, "ws://") {
		return "http://" + strings.TrimPrefix(u, "ws://")
	}
	return ""
}

// Endpoints returns the single-element slice containing URL().
func (r *LocalRouter) Endpoints() []string {
	return []string{r.URL()}
}

// IsHTTP returns false (LocalRouter always exposes its primary WebSocket/Unix/Pipe endpoint).
func (r *LocalRouter) IsHTTP() bool {
	return false
}

// BoundPort returns the TCP port the LocalRouter is bound to (or 0 for Unix/Pipe).
func (r *LocalRouter) BoundPort() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.boundPort
}

// PID returns the OS process ID of the running meshcall-router subprocess, or 0 if not running.
func (r *LocalRouter) PID() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pid
}

// IsRunning reports whether the LocalRouter subprocess is currently running.
func (r *LocalRouter) IsRunning() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.running
}

// --- Built-in Sub-clients: RegistryClient & AuthClient ---------------------

// RegistryClient provides access to the Router's built-in Registry service (mesh.Registry()).
type RegistryClient struct {
	caller Caller
}

// List calls meshcall.router.v1.Registry/list to list all registered services visible to this caller.
func (rc *RegistryClient) List(ctx context.Context, opts ...CallOption) ([]RegistryService, error) {
	return listServices(ctx, rc.caller, opts...)
}

// Describe calls meshcall.router.v1.Registry/describe for one service.
func (rc *RegistryClient) Describe(ctx context.Context, service string, opts ...CallOption) (RegistryServiceDetail, error) {
	return describeService(ctx, rc.caller, service, opts...)
}

// AuthClient provides access to the Router's built-in AuthService (mesh.Auth()).
type AuthClient struct {
	caller Caller
}

// Whoami calls meshcall.router.v1.AuthService/whoami to inspect the caller's identity and scopes.
func (ac *AuthClient) Whoami(ctx context.Context, opts ...CallOption) (WhoamiResponse, error) {
	return whoami(ctx, ac.caller, opts...)
}

// IssueToken calls meshcall.router.v1.AuthService/issue_token to mint a scoped bearer token.
func (ac *AuthClient) IssueToken(ctx context.Context, scopes []RouterScope, ttlSeconds int64, opts ...CallOption) (IssuedToken, error) {
	return issueToken(ctx, ac.caller, scopes, ttlSeconds, opts...)
}

// RevokeToken calls meshcall.router.v1.AuthService/revoke_token to revoke a token by its ID.
func (ac *AuthClient) RevokeToken(ctx context.Context, tokenID string, opts ...CallOption) (RevokeTokenResponse, error) {
	return revokeToken(ctx, ac.caller, tokenID, opts...)
}

func listServices(ctx context.Context, c Caller, opts ...CallOption) ([]RegistryService, error) {
	callOpts := append([]CallOption{IdempotentCall()}, opts...)
	resp, err := Unary[struct{}, struct {
		Services []RegistryService `json:"services"`
	}](ctx, c, routerRegistryService, "list", struct{}{}, callOpts...)
	if err != nil {
		return nil, err
	}
	if resp.Services == nil {
		return []RegistryService{}, nil
	}
	return resp.Services, nil
}

func describeService(ctx context.Context, c Caller, service string, opts ...CallOption) (RegistryServiceDetail, error) {
	type describeReq struct {
		Service string `json:"service"`
	}
	callOpts := append([]CallOption{IdempotentCall()}, opts...)
	return Unary[describeReq, RegistryServiceDetail](ctx, c, routerRegistryService, "describe", describeReq{Service: service}, callOpts...)
}

func whoami(ctx context.Context, c Caller, opts ...CallOption) (WhoamiResponse, error) {
	callOpts := append([]CallOption{IdempotentCall()}, opts...)
	return Unary[struct{}, WhoamiResponse](ctx, c, routerAuthService, "whoami", struct{}{}, callOpts...)
}

func issueToken(ctx context.Context, c Caller, scopes []RouterScope, ttlSeconds int64, opts ...CallOption) (IssuedToken, error) {
	if ttlSeconds <= 0 {
		ttlSeconds = 900
	}
	type issueReq struct {
		Scopes     []RouterScope `json:"scopes"`
		TTLSeconds int64         `json:"ttl_seconds"`
	}
	return Unary[issueReq, IssuedToken](ctx, c, routerAuthService, "issue_token", issueReq{Scopes: scopes, TTLSeconds: ttlSeconds}, opts...)
}

func revokeToken(ctx context.Context, c Caller, tokenID string, opts ...CallOption) (RevokeTokenResponse, error) {
	type revokeReq struct {
		TokenID string `json:"token_id"`
	}
	return Unary[revokeReq, RevokeTokenResponse](ctx, c, routerAuthService, "revoke_token", revokeReq{TokenID: tokenID}, opts...)
}

// --- Layer 3: Mesh ---------------------------------------------------------

type meshConfig struct {
	token              string
	auth               string
	headers            map[string]string
	httpTimeout        time.Duration
	httpClient         *http.Client
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
}

// MeshOption configures a Mesh (Layer 3).
type MeshOption interface {
	applyMesh(*meshConfig)
}

type meshOptFunc func(*meshConfig)

func (f meshOptFunc) applyMesh(c *meshConfig) { f(c) }

// WithToken authenticates the Mesh with a Bearer token ("mk_…" API key or issued token).
func WithToken(token string) MeshOption {
	return meshOptFunc(func(c *meshConfig) { c.token = token })
}

// WithAuth sets the Authorization header value (e.g. BearerToken(...) or BasicAuth(...)).
func WithAuth(authorization string) MeshOption {
	return meshOptFunc(func(c *meshConfig) { c.auth = authorization })
}

// WithHeaders adds custom HTTP headers when connecting to an HTTP(S) Router gateway.
func WithHeaders(headers map[string]string) MeshOption {
	return meshOptFunc(func(c *meshConfig) { c.headers = headers })
}

// WithHTTPTimeout bounds HTTP requests that set no per-call timeout.
func WithHTTPTimeout(timeout time.Duration) MeshOption {
	return meshOptFunc(func(c *meshConfig) { c.httpTimeout = timeout })
}

// WithHTTPClient overrides the underlying *http.Client used for HTTP(S) Router gateways.
func WithHTTPClient(client *http.Client) MeshOption {
	return meshOptFunc(func(c *meshConfig) { c.httpClient = client })
}

// WithPeerID sets the client peer ID sent during handshake.
func WithPeerID(peerID string) MeshOption {
	return meshOptFunc(func(c *meshConfig) { c.peerID = peerID })
}

// WithPing sets the keepalive ping interval and pong timeout.
func WithPing(interval, timeout time.Duration) MeshOption {
	return meshOptFunc(func(c *meshConfig) {
		c.pingInterval = interval
		c.pingTimeout = timeout
	})
}

// WithReconnect sets the exponential backoff policy for reconnecting after a lost connection.
func WithReconnect(policy ReconnectPolicy) MeshOption {
	return meshOptFunc(func(c *meshConfig) { c.reconnect = &policy })
}

// WithRetry sets the client-side unary retry policy.
func WithRetry(policy RetryPolicy) MeshOption {
	return meshOptFunc(func(c *meshConfig) { c.retry = &policy })
}

// WithDeadlineGrace sets how long past a call's timeout the client waits for the peer's deadline error.
func WithDeadlineGrace(grace time.Duration) MeshOption {
	return meshOptFunc(func(c *meshConfig) { c.deadlineGrace = grace })
}

// WithHandshakeTimeout bounds the WebSocket + hello handshake.
func WithHandshakeTimeout(timeout time.Duration) MeshOption {
	return meshOptFunc(func(c *meshConfig) { c.handshakeTimeout = timeout })
}

// WithMaxFrameSize caps incoming and outgoing frame size in bytes.
func WithMaxFrameSize(bytes int) MeshOption {
	return meshOptFunc(func(c *meshConfig) { c.maxFrameSize = bytes })
}

// WithClientInterceptors attaches client interceptors to the Mesh's underlying client.
func WithClientInterceptors(interceptors ...ClientInterceptor) MeshOption {
	return meshOptFunc(func(c *meshConfig) {
		c.clientInterceptors = append(c.clientInterceptors, interceptors...)
	})
}

// WithMeshLogger sets the structured logger used by the Mesh.
func WithMeshLogger(logger *slog.Logger) MeshOption {
	return meshOptFunc(func(c *meshConfig) { c.logger = logger })
}

// WithRouterLogger is an alias for WithMeshLogger.
func WithRouterLogger(logger *slog.Logger) MeshOption {
	return WithMeshLogger(logger)
}

// Mesh is the Layer 3 RPC runtime for calling and serving services over a Router or LocalRouter.
type Mesh struct {
	router          RouterEndpoint
	cfg             meshConfig
	authHeader      string
	wsClient        *Client
	httpCaller      *httpTransport
	registry        *RegistryClient
	auth            *AuthClient
	mountedRaw      []any
	mountCfg        serveConfig
	activeSrvMu     sync.Mutex
	activeSrv       *RunningServer
	ownsLocalRouter bool
}

func (m *Mesh) ensureRouterStarted(ctx context.Context) error {
	if lr, ok := m.router.(*LocalRouter); ok && !lr.IsRunning() {
		if err := lr.Start(ctx); err != nil {
			return err
		}
		m.ownsLocalRouter = true
	}
	return nil
}

// NewMesh constructs a Layer 3 Mesh bound to a Router or LocalRouter.
func NewMesh(router RouterEndpoint, opts ...MeshOption) *Mesh {
	if router == nil {
		panic("meshcall: NewMesh requires a non-nil RouterEndpoint (*Router or *LocalRouter)")
	}
	var cfg meshConfig
	for _, opt := range opts {
		if opt != nil {
			opt.applyMesh(&cfg)
		}
	}
	if cfg.token != "" && cfg.auth != "" {
		panic("meshcall: pass either WithToken or WithAuth to NewMesh, not both")
	}
	authHeader := cfg.auth
	if cfg.token != "" {
		authHeader = BearerToken(cfg.token)
	}
	m := &Mesh{
		router:     router,
		cfg:        cfg,
		authHeader: authHeader,
		mountCfg: serveConfig{
			drainTimeout: 10 * time.Second,
		},
	}
	m.registry = &RegistryClient{caller: m}
	m.auth = &AuthClient{caller: m}
	return m
}

// ConnectMesh creates a Mesh and eagerly establishes its client connection to the Router.
func ConnectMesh(ctx context.Context, router RouterEndpoint, opts ...MeshOption) (*Mesh, error) {
	m := NewMesh(router, opts...)
	if err := m.Connect(ctx); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Mesh) ensureCaller() (Caller, error) {
	if m.router.IsHTTP() {
		if m.httpCaller == nil {
			reconnect := DefaultReconnect
			if m.cfg.reconnect != nil {
				reconnect = *m.cfg.reconnect
			}
			m.httpCaller = newHTTPTransport(m.router.URL(), httpTransportOptions{
				Authorization: m.authHeader,
				Headers:       m.cfg.headers,
				Retry:         m.cfg.retry,
				Reconnect:     reconnect,
				Timeout:       m.cfg.httpTimeout,
				Logger:        m.cfg.logger,
				Client:        m.cfg.httpClient,
			})
		}
		return m.httpCaller, nil
	}
	if m.wsClient == nil {
		clientOpts := ClientOptions{
			Authorization:    m.authHeader,
			PeerID:           m.cfg.peerID,
			MaxFrameSize:     m.cfg.maxFrameSize,
			PingInterval:     m.cfg.pingInterval,
			PingTimeout:      m.cfg.pingTimeout,
			DeadlineGrace:    m.cfg.deadlineGrace,
			HandshakeTimeout: m.cfg.handshakeTimeout,
			Retry:            m.cfg.retry,
			Interceptors:     m.cfg.clientInterceptors,
			Logger:           m.cfg.logger,
		}
		if m.cfg.reconnect != nil {
			clientOpts.Reconnect = *m.cfg.reconnect
		}
		c, err := NewClient(clientOpts, m.router.Endpoints()...)
		if err != nil {
			return nil, err
		}
		m.wsClient = c
	}
	return m.wsClient, nil
}

// Client returns the underlying Caller (*Client or *httpTransport).
func (m *Mesh) Client() Caller {
	c, err := m.ensureCaller()
	if err != nil {
		panic(err)
	}
	return c
}

// Connect eagerly connects the Mesh client to the Router.
func (m *Mesh) Connect(ctx context.Context) error {
	if err := m.ensureRouterStarted(ctx); err != nil {
		return err
	}
	if m.router.IsHTTP() {
		_, err := m.ensureCaller()
		return err
	}
	// Reset wsClient if endpoints changed (e.g., LocalRouter was started after NewMesh).
	if m.wsClient != nil {
		_ = m.wsClient.Close()
		m.wsClient = nil
	}
	if _, err := m.ensureCaller(); err != nil {
		return err
	}
	if _, err := m.wsClient.connection(ctx); err != nil {
		_ = m.wsClient.Close()
		m.wsClient = nil
		return err
	}
	return nil
}

// Router returns the underlying RouterEndpoint (*Router or *LocalRouter).
func (m *Mesh) Router() RouterEndpoint {
	return m.router
}

// URL returns the primary Router endpoint URL.
func (m *Mesh) URL() string {
	return m.router.URL()
}

// Endpoints returns the resolved Router endpoint URLs.
func (m *Mesh) Endpoints() []string {
	return m.router.Endpoints()
}

// Registry returns the built-in RegistryClient for listing and describing services on the Mesh.
func (m *Mesh) Registry() *RegistryClient {
	return m.registry
}

// Auth returns the built-in AuthClient for inspecting identity and managing scoped tokens on the Mesh.
func (m *Mesh) Auth() *AuthClient {
	return m.auth
}

// WithOptions returns a shallow copy of Mesh that attaches default CallOptions to calls made through it.
func (m *Mesh) WithOptions(opts CallOptions) *Mesh {
	clone := &Mesh{
		router:     m.router,
		cfg:        m.cfg,
		authHeader: m.authHeader,
		mountedRaw: m.mountedRaw,
		mountCfg:   m.mountCfg,
	}
	if m.router.IsHTTP() {
		_, _ = m.ensureCaller()
		clone.httpCaller = m.httpCaller.WithOptions(opts)
	} else {
		_, _ = m.ensureCaller()
		clone.wsClient = m.wsClient.WithOptions(opts)
	}
	clone.registry = &RegistryClient{caller: clone}
	clone.auth = &AuthClient{caller: clone}
	return clone
}

// WithCallOptions implements Caller.
func (m *Mesh) WithCallOptions(opts CallOptions) Caller {
	return m.WithOptions(opts)
}

func (m *Mesh) unaryRaw(ctx context.Context, service, method string, request json.RawMessage, opts []CallOption) (json.RawMessage, map[string]string, error) {
	c, err := m.ensureCaller()
	if err != nil {
		return nil, nil, err
	}
	return c.unaryRaw(ctx, service, method, request, opts)
}

func (m *Mesh) serverStreamRaw(ctx context.Context, service, method string, request json.RawMessage, opts []CallOption) (streamSource, error) {
	c, err := m.ensureCaller()
	if err != nil {
		return nil, err
	}
	return c.serverStreamRaw(ctx, service, method, request, opts)
}

func (m *Mesh) clientStreamRaw(ctx context.Context, service, method string, request json.RawMessage, items func(yield func(json.RawMessage) bool), opts []CallOption) (json.RawMessage, map[string]string, error) {
	c, err := m.ensureCaller()
	if err != nil {
		return nil, nil, err
	}
	return c.clientStreamRaw(ctx, service, method, request, items, opts)
}

func (m *Mesh) duplexRaw(ctx context.Context, service, method string, request json.RawMessage, opts []CallOption) (rawDuplex, error) {
	c, err := m.ensureCaller()
	if err != nil {
		return nil, err
	}
	return c.duplexRaw(ctx, service, method, request, opts)
}

// Drain gracefully drains in-flight calls on the Mesh's active server, if serving.
func (m *Mesh) Drain(ctx context.Context) error {
	m.activeSrvMu.Lock()
	srv := m.activeSrv
	m.activeSrvMu.Unlock()
	if srv != nil && srv.Server != nil {
		return srv.Server.Drain(ctx)
	}
	return nil
}

// Close closes the Mesh's active server (if serving), its client connection,
// and any LocalRouter subprocess that was auto-started by this Mesh.
func (m *Mesh) Close() error {
	m.activeSrvMu.Lock()
	srv := m.activeSrv
	m.activeSrv = nil
	m.activeSrvMu.Unlock()

	var firstErr error
	if srv != nil {
		if err := srv.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if m.wsClient != nil {
		if err := m.wsClient.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if m.httpCaller != nil {
		if err := m.httpCaller.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if m.ownsLocalRouter {
		m.ownsLocalRouter = false
		if lr, ok := m.router.(*LocalRouter); ok {
			if err := lr.Stop(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// --- Server Mounting & Lifecycle on Mesh -----------------------------------

type serveConfig struct {
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

// ServeOption configures service mounting and serving on a Mesh.
type ServeOption interface {
	applyServe(*serveConfig)
}

type serveOptFunc func(*serveConfig)

func (f serveOptFunc) applyServe(s *serveConfig) { f(s) }

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

// RunningServer is an active server started by Mesh.StartServing.
type RunningServer struct {
	Server          *Server
	RouterURL       string
	RouterEndpoints []string
	drainTimeout    time.Duration
	onClose         func()
	closeOnce       sync.Once
	closeErr        error
}

// Close gracefully drains in-flight calls and shuts down the server.
func (r *RunningServer) Close() error {
	r.closeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), r.drainTimeout)
		defer cancel()
		_ = r.Server.Drain(ctx)
		r.closeErr = r.Server.Close()
		if r.onClose != nil {
			r.onClose()
		}
	})
	return r.closeErr
}

// Mount registers one or more services (and optional ServeOptions such as WithDependencies)
// on the Mesh without performing any network I/O.
// Arguments may be *Service, []*Service, a struct (or pointer) embedding ServiceMeta, or ServeOption.
func (m *Mesh) Mount(args ...any) *Mesh {
	if m.router.IsHTTP() {
		panic("meshcall: cannot mount services on a Mesh configured with an HTTP(S) Router; serving requires ws://, wss://, unix://, or pipe://")
	}
	m.activeSrvMu.Lock()
	serving := m.activeSrv != nil
	m.activeSrvMu.Unlock()
	if serving {
		panic("meshcall: cannot mount services while Mesh is already serving")
	}

	for _, arg := range args {
		switch v := arg.(type) {
		case nil:
			continue
		case *Service, []*Service:
			m.mountedRaw = append(m.mountedRaw, v)
		case ServeOption:
			v.applyServe(&m.mountCfg)
		default:
			if hasServiceMeta(arg) {
				m.mountedRaw = append(m.mountedRaw, arg)
				continue
			}
			panic(fmt.Sprintf("meshcall: unsupported Mount argument of type %T", arg))
		}
	}
	return m
}

// StartServing connects the Mesh's mounted services to the Router in the background
// and returns a *RunningServer handle.
func (m *Mesh) StartServing(ctx context.Context, opts ...ServeOption) (*RunningServer, error) {
	if m.router.IsHTTP() {
		return nil, errors.New("meshcall: cannot serve on a Mesh configured with an HTTP(S) Router")
	}
	m.activeSrvMu.Lock()
	if m.activeSrv != nil {
		m.activeSrvMu.Unlock()
		return nil, errors.New("meshcall: Mesh is already serving")
	}
	m.activeSrvMu.Unlock()

	cfg := m.mountCfg
	cfg.interceptors = append([]ServerInterceptor{}, m.mountCfg.interceptors...)
	cfg.dependencies = append([]DependencyProvider{}, m.mountCfg.dependencies...)
	for _, opt := range opts {
		if opt != nil {
			opt.applyServe(&cfg)
		}
	}

	var services []*Service
	for _, item := range m.mountedRaw {
		switch v := item.(type) {
		case *Service:
			services = append(services, v)
		case []*Service:
			services = append(services, v...)
		default:
			if hasServiceMeta(item) {
				svc, err := ReflectService(item, WithDependencies(cfg.dependencies...))
				if err != nil {
					return nil, err
				}
				services = append(services, svc)
			}
		}
	}
	if len(services) == 0 {
		return nil, errors.New("meshcall: no services mounted on Mesh (call mesh.Mount(...) before serving)")
	}

	logger := cfg.logger
	if logger == nil {
		logger = m.cfg.logger
	}
	serverOpts := ServerOptions{
		Services:      services,
		InstanceID:    cfg.instanceID,
		MaxInflight:   cfg.maxInflight,
		Interceptors:  cfg.interceptors,
		Logger:        logger,
		AccessLog:     cfg.accessLog,
		PingInterval:  m.cfg.pingInterval,
		PingTimeout:   m.cfg.pingTimeout,
		Authorization: m.authHeader,
		IdleTimeout:   cfg.idleTimeout,
		MaxFrameSize:  m.cfg.maxFrameSize,
		Strict:        cfg.strict,
	}
	if m.cfg.reconnect != nil {
		serverOpts.Reconnect = *m.cfg.reconnect
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
		running := &RunningServer{
			Server:          srv,
			RouterURL:       urlStr,
			RouterEndpoints: []string{urlStr},
			drainTimeout:    cfg.drainTimeout,
		}
		running.onClose = func() {
			m.activeSrvMu.Lock()
			if m.activeSrv == running {
				m.activeSrv = nil
			}
			m.activeSrvMu.Unlock()
		}
		m.activeSrvMu.Lock()
		m.activeSrv = running
		m.activeSrvMu.Unlock()
		return running, nil
	}

	if err := m.ensureRouterStarted(ctx); err != nil {
		_ = srv.Close()
		return nil, err
	}
	endpoints := m.router.Endpoints()
	if err := srv.ConnectRouter(ctx, endpoints...); err != nil {
		_ = srv.Close()
		if m.ownsLocalRouter && m.wsClient == nil {
			_ = m.Close()
		}
		return nil, err
	}
	running := &RunningServer{
		Server:          srv,
		RouterURL:       endpoints[0],
		RouterEndpoints: endpoints,
		drainTimeout:    cfg.drainTimeout,
	}
	running.onClose = func() {
		m.activeSrvMu.Lock()
		if m.activeSrv == running {
			m.activeSrv = nil
		}
		m.activeSrvMu.Unlock()
	}
	m.activeSrvMu.Lock()
	m.activeSrv = running
	m.activeSrvMu.Unlock()
	return running, nil
}

// Serve connects the Mesh's mounted services to the Router, blocks until ctx is done,
// and then gracefully drains in-flight calls and closes the server.
func (m *Mesh) Serve(ctx context.Context, opts ...ServeOption) error {
	running, err := m.StartServing(ctx, opts...)
	if err != nil {
		return err
	}
	<-ctx.Done()
	closeErr := running.Close()
	if m.ownsLocalRouter {
		if err := m.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}

// Run connects the Mesh's mounted services to the Router and blocks until SIGINT
// or SIGTERM is received, then gracefully drains in-flight calls and closes the server.
func (m *Mesh) Run(opts ...ServeOption) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return m.Serve(ctx, opts...)
}

// --- Subprocess management helpers -----------------------------------------

type spawnedRouter struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser
	once  sync.Once
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
