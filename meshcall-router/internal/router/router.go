package router

import (
	"container/list"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/t4wefan/meshcall/meshcall-router/internal/localsock"
)

type Config struct {
	Host     string
	Port     int
	UnixPath string
	// Pipe is a named pipe name (\\.\pipe\<Pipe>); Windows only, where it is
	// the local transport instead of a Unix socket.
	Pipe         string
	MaxFrameSize int
	Auth         *Auth
	// DeadlineGrace is added to every call's timeout_ms before the router
	// enforces it, so the service's own deadline_exceeded normally arrives first.
	DeadlineGrace time.Duration
	// IdleTimeout closes a connection that has sent no frame for this long.
	// Connecting peers must ping more often than this.
	IdleTimeout time.Duration
	// BreakerThreshold consecutive failures open an instance's circuit for
	// BreakerCooldown; then one probe call is admitted. See breaker.go.
	BreakerThreshold int
	BreakerCooldown  time.Duration
	// MaxAnonymousConnections bounds WebSocket connections made without
	// credentials, inside MaxConnections, so anonymous load cannot lock out
	// credentialed clients and services. Zero means the default.
	MaxAnonymousConnections int
	// HTTP calls (httpapi.go). HTTPDefaultTimeout applies to unary and
	// client-stream calls that send no MeshCall-Timeout-Ms; HTTPMaxTimeout,
	// when set, refuses longer requested or default timeouts; HTTPMaxInflight
	// caps concurrent HTTP calls (429 beyond); HTTPCORSOrigins lists browser
	// origins allowed to call (exact match, or "*").
	HTTPDefaultTimeout time.Duration
	HTTPMaxTimeout     time.Duration
	HTTPMaxInflight    int
	HTTPCORSOrigins    []string
	// MaxConnections bounds open WebSocket connections; past it new upgrades
	// are refused with 503. HTTP calls are not counted: their admission is a
	// separate design (#8). Zero means the default.
	MaxConnections int
}

const (
	DefaultDeadlineGrace           = 500 * time.Millisecond
	DefaultIdleTimeout             = 45 * time.Second
	DefaultMaxConnections          = 10000
	DefaultMaxAnonymousConnections = 1000
	DefaultHTTPDefaultTimeout      = 30 * time.Second
	DefaultHTTPMaxInflight         = 10000
)

type Ready struct {
	Kind     string `json:"kind"`
	Protocol string `json:"protocol"`
	PID      int    `json:"pid"`
	Host     string `json:"host,omitempty"`
	Port     int    `json:"port,omitempty"`
	UnixPath string `json:"unix_path,omitempty"`
	Pipe     string `json:"pipe,omitempty"`
}

type peer struct {
	conn        *websocket.Conn
	ctx         context.Context
	cancel      context.CancelFunc
	out         *outbox
	user        *User
	id          string
	role        string
	instanceID  string
	services    []service
	inflight    int
	maxInflight int // 0 = unlimited
	health      health
	draining    bool
}

func (p *peer) send(f frame) {
	if !p.out.push(f) {
		p.cancel() // Disconnect a slow peer instead of growing memory indefinitely.
	}
}

func (p *peer) writeLoop(maxFrameSize int) {
	for {
		f, ok := p.out.pop()
		if !ok {
			select {
			case <-p.ctx.Done():
				return
			case <-p.out.ready:
				continue
			}
		}
		if len(f.raw) > maxFrameSize {
			p.cancel()
			return
		}
		ctx, cancel := context.WithTimeout(p.ctx, 10*time.Second)
		err := p.conn.Write(ctx, websocket.MessageText, f.raw)
		cancel()
		if err != nil {
			p.cancel()
			return
		}
	}
}

type route struct {
	id       string
	client   *peer
	instance *peer
	deadline time.Time
	index    int         // position in Router.deadlines, or -1
	dedup    *dedupEntry // set when the call carries an idempotency key
	// Item credit outstanding in each direction: granted by the consumer's
	// stream.window frames, spent by the producer's stream.item frames.
	clientCredit, serverCredit int64
}

// credit applies a stream.window or stream.item to the route's credit and
// reports whether the frame may be forwarded: false for an item beyond it.
// allowedFrame has already matched each direction to the role that sends it.
func (rt *route) credit(f frame) bool {
	counter := &rt.clientCredit
	if f.Direction == "server" {
		counter = &rt.serverCredit
	}
	switch f.Kind {
	case "stream.window":
		if *counter > math.MaxInt64-*f.Credit {
			*counter = math.MaxInt64
		} else {
			*counter += *f.Credit
		}
	case "stream.item":
		if *counter == 0 {
			return false
		}
		*counter--
	}
	return true
}

type Router struct {
	config       Config
	listener     net.Listener
	socketInfo   os.FileInfo
	http         *http.Server
	ctx          context.Context
	cancel       context.CancelFunc
	mu           sync.Mutex
	closed       bool
	peers        map[*peer]struct{}
	instances    map[string]*peer
	services     map[string]*pool
	routes       map[string]*route
	deadlines    deadlineHeap
	deadlineWake chan struct{}
	dedup        map[string]*dedupEntry
	// connections counts admitted WebSocket connections; anonymous counts
	// those among them made without credentials.
	connections  atomic.Int64
	anonymous    atomic.Int64
	httpInflight atomic.Int64
	dedupOrder   *list.List
	wg           sync.WaitGroup
	closeOnce    sync.Once
	done         chan struct{}
	ready        Ready
}

func Start(config Config) (*Router, error) {
	if config.MaxFrameSize == 0 {
		config.MaxFrameSize = 1024 * 1024
	}
	if config.MaxFrameSize < 1 || config.MaxFrameSize > 64*1024*1024 {
		return nil, errors.New("max-frame-size must be between 1 and 67108864")
	}
	if config.DeadlineGrace == 0 {
		config.DeadlineGrace = DefaultDeadlineGrace
	}
	if config.IdleTimeout == 0 {
		config.IdleTimeout = DefaultIdleTimeout
	}
	if config.DeadlineGrace < 0 || config.IdleTimeout < time.Second {
		return nil, errors.New("deadline-grace must be non-negative and idle-timeout at least 1s")
	}
	if config.MaxConnections == 0 {
		config.MaxConnections = DefaultMaxConnections
	}
	if config.MaxConnections < 0 {
		return nil, errors.New("max connections must be positive")
	}
	if config.HTTPDefaultTimeout == 0 {
		config.HTTPDefaultTimeout = DefaultHTTPDefaultTimeout
	}
	if config.HTTPMaxInflight == 0 {
		config.HTTPMaxInflight = DefaultHTTPMaxInflight
	}
	if config.HTTPDefaultTimeout < 0 || config.HTTPMaxTimeout < 0 || config.HTTPMaxInflight < 0 {
		return nil, errors.New("HTTP timeouts and limits must be positive")
	}
	if config.HTTPMaxTimeout > 0 && config.HTTPDefaultTimeout > config.HTTPMaxTimeout {
		config.HTTPDefaultTimeout = config.HTTPMaxTimeout
	}
	if config.MaxAnonymousConnections == 0 {
		config.MaxAnonymousConnections = DefaultMaxAnonymousConnections
	}
	if config.MaxAnonymousConnections < 0 {
		return nil, errors.New("max anonymous connections must be positive")
	}
	if config.BreakerThreshold == 0 {
		config.BreakerThreshold = DefaultBreakerThreshold
	}
	if config.BreakerCooldown == 0 {
		config.BreakerCooldown = DefaultBreakerCooldown
	}
	if config.BreakerThreshold < 1 || config.BreakerCooldown <= 0 {
		return nil, errors.New("breaker-threshold must be positive and breaker-cooldown greater than zero")
	}
	if config.Port < 0 || config.Port > 65535 {
		return nil, errors.New("port must be between 0 and 65535")
	}
	var listener net.Listener
	var err error
	if config.Pipe != "" {
		if config.Host != "" || config.Port != 0 || config.UnixPath != "" {
			return nil, errors.New("a named pipe cannot be combined with host, port or a Unix socket")
		}
		if config.Pipe, err = localsock.PipeName(config.Pipe); err != nil {
			return nil, err
		}
		listener, err = localsock.ListenPipe(config.Pipe)
	} else if config.UnixPath != "" {
		if !localsock.UnixSupported() {
			return nil, localsock.ErrUnixOnWindows
		}
		if config.Host != "" || config.Port != 0 {
			return nil, errors.New("Unix socket cannot be combined with host or port")
		}
		config.UnixPath, err = filepath.Abs(config.UnixPath)
		if err != nil {
			return nil, err
		}
		if len(config.UnixPath) > 103 {
			return nil, errors.New("Unix socket path exceeds the portable 103-byte limit")
		}
		if _, err := os.Lstat(config.UnixPath); !errors.Is(err, os.ErrNotExist) {
			return nil, errors.New("Unix socket path already exists or is inaccessible")
		}
		if err := os.MkdirAll(filepath.Dir(config.UnixPath), 0750); err != nil {
			return nil, err
		}
		listener, err = net.Listen("unix", config.UnixPath)
	} else {
		if config.Host == "" {
			config.Host = "127.0.0.1"
		}
		listener, err = net.Listen("tcp", net.JoinHostPort(config.Host, fmt.Sprint(config.Port)))
	}
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}
	var socketInfo os.FileInfo
	if unix, ok := listener.(*net.UnixListener); ok {
		unix.SetUnlinkOnClose(false)
		socketInfo, err = os.Lstat(config.UnixPath)
		if err != nil {
			_ = listener.Close()
			return nil, err
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &Router{
		config: config, listener: listener, socketInfo: socketInfo, ctx: ctx, cancel: cancel,
		peers: make(map[*peer]struct{}), instances: make(map[string]*peer),
		services: make(map[string]*pool), routes: make(map[string]*route), done: make(chan struct{}),
		deadlineWake: make(chan struct{}, 1),
		dedup:        make(map[string]*dedupEntry),
		dedupOrder:   list.New(),
	}
	r.wg.Add(1)
	go r.expireLoop()
	r.ready = Ready{Kind: "router.ready", Protocol: Protocol, PID: os.Getpid(), UnixPath: config.UnixPath, Pipe: config.Pipe}
	if tcp, ok := listener.Addr().(*net.TCPAddr); ok {
		r.ready.Host, r.ready.Port = config.Host, tcp.Port
	}
	r.http = &http.Server{
		Handler: http.HandlerFunc(r.handle), ReadHeaderTimeout: 5 * time.Second,
		MaxHeaderBytes: 16 * 1024,
	}
	go func() {
		if err := r.http.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) && ctx.Err() == nil {
			slog.Error("router listener stopped", "error", err)
			go r.Close()
		}
	}()
	return r, nil
}

// Snapshot is a read-only view of registration state for tooling and tests.
type Snapshot struct {
	// Instances maps a service name to the ids of instances currently in its
	// pool (draining instances are excluded).
	Instances map[string][]string
	// Methods maps a service name to its registered methods and stream kinds.
	Methods map[string]map[string]string
	// Contracts holds the portable contract entry registered for each
	// service, when the instances provided one.
	Contracts map[string]json.RawMessage
	// Targeted lists services whose calls must name an instance.
	Targeted map[string]bool
	Routes   int
	Peers    int
	// Health describes every registered instance.
	Health map[string]InstanceHealth
}

type InstanceHealth struct {
	Inflight    int
	MaxInflight int
	Breaker     string // closed | open | half_open
	Failures    int
	Draining    bool
}

func (r *Router) Snapshot() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	snapshot := Snapshot{
		Instances: make(map[string][]string, len(r.services)),
		Methods:   make(map[string]map[string]string, len(r.services)),
		Contracts: make(map[string]json.RawMessage, len(r.services)),
		Targeted:  make(map[string]bool, len(r.services)),
		Routes:    len(r.routes),
		Peers:     len(r.peers),
		Health:    make(map[string]InstanceHealth, len(r.instances)),
	}
	now := time.Now()
	for id, p := range r.instances {
		snapshot.Health[id] = InstanceHealth{
			Inflight: p.inflight, MaxInflight: p.maxInflight,
			Breaker: string(p.health.state(now)), Failures: p.health.failures, Draining: p.draining,
		}
	}
	for name, pool := range r.services {
		ids := make([]string, 0, len(pool.instances))
		for _, p := range pool.instances {
			ids = append(ids, p.instanceID)
		}
		snapshot.Instances[name] = ids
		methods := make(map[string]string, len(pool.methods))
		for _, m := range pool.methods {
			methods[m.Name] = m.Stream
		}
		snapshot.Methods[name] = methods
		if pool.targeted {
			snapshot.Targeted[name] = true
		}
		if pool.contract != nil {
			snapshot.Contracts[name] = pool.contract
		}
	}
	return snapshot
}

func (r *Router) Ready() Ready          { return r.ready }
func (r *Router) Done() <-chan struct{} { return r.done }

func (r *Router) Close() {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.cancel()
		r.mu.Unlock()
		_ = r.http.Close()
		_ = r.listener.Close() // Also covers Close racing with the Serve goroutine.
		r.wg.Wait()
		if r.socketInfo != nil {
			if current, err := os.Lstat(r.config.UnixPath); err == nil && os.SameFile(r.socketInfo, current) {
				_ = os.Remove(r.config.UnixPath)
			}
		}
		close(r.done)
	})
}

func (r *Router) handle(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path == "/healthz" || req.URL.Path == "/readyz" {
		r.serveHealth(w, req)
		return
	}
	if strings.HasPrefix(req.URL.Path, httpPrefix) && !strings.EqualFold(req.Header.Get("Upgrade"), "websocket") {
		r.serveHTTP(w, req)
		return
	}
	// Admission before authentication, so a flood cannot even reach the
	// password check. The slot is held for the connection's life.
	if r.connections.Add(1) > int64(r.config.MaxConnections) {
		r.connections.Add(-1)
		w.Header().Set("Retry-After", "1")
		http.Error(w, "connection limit reached", http.StatusServiceUnavailable)
		return
	}
	defer r.connections.Add(-1)
	user, ok := r.config.Auth.authenticate(w, req)
	if !ok {
		return
	}
	if user != nil && user.kind == identityAnonymous {
		if r.anonymous.Add(1) > int64(r.config.MaxAnonymousConnections) {
			r.anonymous.Add(-1)
			w.Header().Set("Retry-After", "1")
			http.Error(w, "anonymous connection limit reached", http.StatusServiceUnavailable)
			return
		}
		defer r.anonymous.Add(-1)
	}
	// Default origin checks protect browser-originated connections. Node/Python
	// clients do not send Origin. Cross-origin browser auth is a separate concern.
	conn, err := websocket.Accept(w, req, nil)
	if err != nil {
		return
	}
	conn.SetReadLimit(int64(r.config.MaxFrameSize))
	var ctx context.Context
	var cancel context.CancelFunc
	if user != nil && !user.expires.IsZero() {
		ctx, cancel = context.WithDeadline(r.ctx, user.expires)
	} else {
		ctx, cancel = context.WithCancel(r.ctx)
	}
	p := &peer{conn: conn, ctx: ctx, cancel: cancel, user: user, out: newOutbox(max(8*1024*1024, 2*r.config.MaxFrameSize))}
	r.mu.Lock()
	if r.closed || !r.config.Auth.valid(user) {
		r.mu.Unlock()
		cancel()
		_ = conn.CloseNow()
		return
	}
	r.peers[p] = struct{}{}
	r.wg.Add(1)
	r.mu.Unlock()
	writerDone := make(chan struct{})
	go func() { defer close(writerDone); p.writeLoop(r.config.MaxFrameSize) }()
	defer func() {
		cancel()
		p.out.close()
		_ = conn.CloseNow()
		<-writerDone
		r.remove(p)
		r.wg.Done()
	}()
	handshake, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	f, err := readFrame(handshake, conn)
	if err != nil || f.Kind != "hello" {
		return
	}
	if !user.allowsRole(f.Role) {
		_ = conn.Close(websocket.StatusPolicyViolation, "permission denied for role")
		return
	}
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return
	}
	p.id, p.role = hex.EncodeToString(id), f.Role
	p.send(frame{Kind: "hello.ack", Protocol: Protocol, Connection: p.id})
	if p.role == "server" {
		registration, err := readFrame(handshake, conn)
		if err != nil || registration.Kind != "server.register" || f.InstanceID == "" || f.InstanceID != registration.InstanceID {
			return
		}
		if err := r.register(p, registration); err != nil {
			slog.Warn("registration rejected", "reason", err)
			_ = conn.Close(websocket.StatusPolicyViolation, "registration rejected")
			return
		}
	}
	stop()
	for {
		// A peer that sends nothing for IdleTimeout is treated as gone; connecting
		// peers keep the connection alive with protocol pings.
		read, idle := context.WithTimeout(ctx, r.config.IdleTimeout)
		f, err := readFrame(read, conn)
		idle()
		if err != nil {
			// A malformed call frame that still names its call is answered for
			// that call alone; anything else makes the connection unsafe.
			if f.CallID != "" && p.role == "client" && f.Kind != "" {
				p.send(callError(f.CallID, "protocol_error", "Invalid "+f.Kind+" frame", false))
				continue
			}
			return
		}
		if f.Kind == "ping" {
			p.send(frame{Kind: "pong", Nonce: f.Nonce})
			continue
		}
		if f.Kind == "pong" {
			continue
		}
		if f.Kind == "server.drain" {
			// Graceful exit: leave every pool now, keep serving routed calls.
			if p.role != "server" {
				_ = conn.Close(websocket.StatusPolicyViolation, "frame not allowed for role")
				return
			}
			r.drain(p)
			p.send(frame{Kind: "server.drain.ack", InstanceID: p.instanceID})
			continue
		}
		if !allowedFrame(p.role, f) {
			_ = conn.Close(websocket.StatusPolicyViolation, "frame not allowed for role")
			return
		}
		r.forward(p, f)
	}
}

func readFrame(ctx context.Context, conn *websocket.Conn) (frame, error) {
	_, data, err := conn.Read(ctx)
	if err != nil {
		return frame{}, err
	}
	f, err := decode(data)
	if err != nil {
		// Keep whatever decoded so the caller can attribute the failure.
		return frame{Kind: f.Kind, CallID: f.CallID}, err
	}
	return f, nil
}

var (
	nameSegment    = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`)
	majorSegment   = regexp.MustCompile(`^v(0|[1-9][0-9]*)$`)
	numericSegment = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)
	patchSegment   = regexp.MustCompile(`^(0|[1-9][0-9]*)(-[0-9A-Za-z-]+)?$`)
)

// validServiceName accepts the normalised form the SDKs register: dotted
// segments with exactly one version group (`v1` or `v1.2.0[-pre]`).
func validServiceName(name string) bool {
	segments := strings.Split(name, ".")
	versions, names := 0, 0
	for i := 0; i < len(segments); {
		s := segments[i]
		switch {
		case majorSegment.MatchString(s):
			versions++
			if i+2 < len(segments) && numericSegment.MatchString(segments[i+1]) && patchSegment.MatchString(segments[i+2]) {
				i += 3
			} else {
				i++
			}
		case nameSegment.MatchString(s):
			names++
			i++
		default:
			return false
		}
	}
	return versions == 1 && names > 0
}

func allowedFrame(role string, f frame) bool {
	if role == "client" {
		return f.Kind == "call.open" || f.Kind == "call.cancel" ||
			((f.Kind == "stream.item" || f.Kind == "stream.end") && f.Direction == "client") ||
			(f.Kind == "stream.window" && f.Direction == "server")
	}
	return f.Kind == "call.result" || f.Kind == "call.error" ||
		((f.Kind == "stream.item" || f.Kind == "stream.end") && f.Direction == "server") ||
		(f.Kind == "stream.window" && f.Direction == "client")
}

func (r *Router) register(p *peer, f frame) error {
	seen := make(map[string]bool)
	for _, s := range f.Services {
		if !validServiceName(s.Name) || strings.HasPrefix(s.Name, reservedPrefix) || seen[s.Name] || !p.user.allowsRegistration(s.Name) {
			return errors.New("duplicate, invalid, or unauthorized service")
		}
		seen[s.Name] = true
		methods := make(map[string]bool)
		for _, m := range s.Methods {
			if m.Name == "" || methods[m.Name] || !slices.Contains([]string{"unary", "server_stream", "client_stream", "duplex"}, m.Stream) ||
				!slices.Contains([]string{"round_robin", "least_inflight", "random", "sticky", "disabled"}, m.Balance.Kind) {
				return errors.New("invalid method registration")
			}
			if m.Balance.Kind == "sticky" && (m.Balance.Key == nil || !strings.HasPrefix(*m.Balance.Key, "request.") || strings.HasSuffix(*m.Balance.Key, ".")) {
				return errors.New("sticky key must address a request field")
			}
			methods[m.Name] = true
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.instances[f.InstanceID] != nil {
		return errors.New("duplicate instance_id")
	}
	for _, s := range f.Services {
		if s.Contract != nil && !json.Valid(s.Contract) {
			return errors.New("invalid service contract")
		}
		// Supersets are fine (rolling upgrades); the same method must mean
		// the same thing, and targeted is a property of the whole service.
		if pool := r.services[s.Name]; pool != nil {
			if pool.targeted != s.Targeted {
				return errors.New("incompatible service registration: targeted differs")
			}
			for _, m := range s.Methods {
				if existing, ok := pool.methods[m.Name]; ok && (existing.Stream != m.Stream || !reflect.DeepEqual(existing.Balance, m.Balance)) {
					return errors.New("incompatible service registration: method " + m.Name + " differs")
				}
			}
		}
	}
	p.instanceID, p.services, p.maxInflight = f.InstanceID, f.Services, f.MaxInflight
	r.instances[p.instanceID] = p
	// Queue the ack before exposing the instance to calls.
	p.send(frame{Kind: "server.register.ack", InstanceID: p.instanceID})
	for _, s := range f.Services {
		if r.services[s.Name] == nil {
			r.services[s.Name] = &pool{targeted: s.Targeted, methods: make(map[string]method), cursors: make(map[string]int)}
		}
		pool := r.services[s.Name]
		for _, m := range s.Methods {
			pool.methods[m.Name] = m
		}
		// The Registry serves the widest contract it has seen.
		if s.Contract != nil && (pool.contract == nil || len(s.Methods) > pool.contractMethods) {
			pool.contract, pool.contractMethods = s.Contract, len(s.Methods)
		}
		pool.instances = append(pool.instances, p)
		slices.SortFunc(pool.instances, func(a, b *peer) int { return strings.Compare(a.instanceID, b.instanceID) })
	}
	return nil
}

func (r *Router) forward(p *peer, f frame) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p.ctx.Err() != nil || !r.config.Auth.valid(p.user) {
		p.cancel()
		return
	}
	if f.Kind == "call.open" {
		if !(f.Service == AuthService && f.Method == "whoami") && !p.user.allowsCall(f.Service, f.Method) {
			p.send(callError(f.CallID, "permission_denied", "Call is not permitted", false))
			return
		}
		if r.routes[f.CallID] != nil {
			p.send(callError(f.CallID, "protocol_error", "Duplicate call_id", false))
			return
		}
		if strings.HasPrefix(f.Service, reservedPrefix) {
			r.management(p, f)
			return
		}
		if len(r.routes) >= 65536 {
			p.send(routerError(f.CallID, "resource_exhausted", "Router call capacity reached", true, "overloaded", ""))
			return
		}
		stream := r.methodStream(f.Service, f.Method)
		if f.Idempotency != "" && stream == "duplex" {
			p.send(routerError(f.CallID, "invalid_argument", "Duplex calls are never replayed and take no idempotency key", false, "idempotency_key_on_duplex", ""))
			return
		}
		var entry *dedupEntry
		var avoid *peer
		if f.Idempotency != "" && stream == "unary" {
			key := dedupKey(f)
			if entry = r.dedupLookup(key); entry != nil {
				if entry.result != nil {
					p.send(withCallID(*entry.result, f.CallID))
					return
				}
				if entry.route != nil {
					entry.waiters = append(entry.waiters, dedupWaiter{client: p, callID: f.CallID})
					return
				}
				avoid = entry.lastInstance
				r.dedupRemove(entry)
				entry = nil
			}
		}
		instance, errorFrame := r.selectInstance(f, avoid)
		if errorFrame != nil {
			p.send(*errorFrame)
			return
		}
		rt := &route{id: f.CallID, client: p, instance: instance, index: -1}
		r.routes[f.CallID] = rt
		instance.inflight++
		if f.TimeoutMS != nil {
			r.track(rt, time.Duration(*f.TimeoutMS)*time.Millisecond)
		}
		if f.Idempotency != "" && stream == "unary" {
			r.dedupInsert(dedupKey(f), rt)
		}
		instance.send(f)
		return
	}
	rt := r.routes[f.CallID]
	if rt == nil {
		return
	}
	if p.role == "client" && rt.client == p {
		if !rt.credit(f) {
			r.flowViolation(rt, false)
			return
		}
		rt.instance.send(f)
		if f.Kind == "call.cancel" {
			// The client has given up; forget the route now so a service that
			// ignores cancellation cannot pin router capacity.
			r.endRoute(f.CallID, rt)
		}
	} else if p.role == "server" && rt.instance == p {
		if !rt.credit(f) {
			r.flowViolation(rt, true)
			return
		}
		rt.client.send(f)
		if f.Kind == "call.result" || f.Kind == "call.error" {
			r.observe(p, !instanceFailed(f))
			if rt.dedup != nil {
				r.dedupSettle(rt.dedup, f)
			}
			r.endRoute(f.CallID, rt)
		}
	}
}

// flowViolation fails a call whose producer sent a stream item beyond the
// consumer's credit. The item is not forwarded: the producer is at fault, so
// the receiver's buffer never holds what it did not ask for, and only this
// call ends; both connections stay up. Called under r.mu.
func (r *Router) flowViolation(rt *route, byInstance bool) {
	message, instance := "Client sent a stream item beyond granted credit", ""
	if byInstance {
		message, instance = "Service instance sent a stream item beyond granted credit", rt.instance.instanceID
		r.observe(rt.instance, false)
	}
	violation := routerError(rt.id, "protocol_error", message, false, "flow_control_violation", instance)
	if rt.dedup != nil && rt.dedup.route == rt {
		r.dedupFail(rt.dedup, violation, nil)
	}
	r.endRoute(rt.id, rt)
	rt.client.send(violation)
	rt.instance.send(frame{Kind: "call.cancel", CallID: rt.id, Reason: "flow_control_violation"})
}

// methodStream returns the registered stream kind of a method, or "".
func (r *Router) methodStream(service, method string) string {
	pool := r.services[service]
	if pool == nil {
		return ""
	}
	return pool.methods[method].Stream
}

// drain removes an instance from every pool so no new call is routed to it
// while its in-flight calls finish. The instance id stays reserved until the
// connection closes. Idempotent.
func (r *Router) drain(p *peer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p.draining || r.instances[p.instanceID] != p {
		return
	}
	p.draining = true
	r.leavePools(p)
}

func (r *Router) leavePools(p *peer) {
	for _, s := range p.services {
		pool := r.services[s.Name]
		if pool == nil {
			continue
		}
		pool.instances = slices.DeleteFunc(pool.instances, func(candidate *peer) bool { return candidate == p })
		if len(pool.instances) == 0 {
			delete(r.services, s.Name)
			continue
		}
		pool.rebuildMethods()
	}
}

func (r *Router) remove(p *peer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.peers, p)
	if r.instances[p.instanceID] == p {
		delete(r.instances, p.instanceID)
		r.leavePools(p)
	}
	for id, rt := range r.routes {
		switch {
		case rt.client == p:
			r.endRoute(id, rt)
			rt.instance.send(frame{Kind: "call.cancel", CallID: id, Reason: "client_disconnected"})
		case rt.instance == p:
			lost := routerError(id, "unavailable", "Service instance disconnected", true, "instance_lost", p.instanceID)
			if rt.dedup != nil && rt.dedup.route == rt {
				r.dedupFail(rt.dedup, lost, p)
				rt.dedup = nil
			}
			r.endRoute(id, rt)
			rt.client.send(lost)
		}
	}
}
