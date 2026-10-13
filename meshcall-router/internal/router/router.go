package router

import (
	"bytes"
	"container/list"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

type Config struct {
	Host         string
	Port         int
	UnixPath     string
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
	// Peers are WebSocket URLs (ws:// or wss://) of other Routers to peer with.
	Peers []string
	// PeerID uniquely identifies this Router in hello and router.sync frames.
	PeerID string
}

const (
	DefaultDeadlineGrace = 500 * time.Millisecond
	DefaultIdleTimeout   = 45 * time.Second
)

type Ready struct {
	Kind     string `json:"kind"`
	Protocol string `json:"protocol"`
	PID      int    `json:"pid"`
	Host     string `json:"host,omitempty"`
	Port     int    `json:"port,omitempty"`
	UnixPath string `json:"unix_path,omitempty"`
}

type peer struct {
	conn         *websocket.Conn
	ctx          context.Context
	cancel       context.CancelFunc
	out          *outbox
	user         *User
	id           string
	peerID       string
	role         string
	instanceID   string
	services     []service
	inflight     int
	maxInflight  int // 0 = unlimited
	health       health
	draining     bool
	gateway      *peer               // non-nil for virtual instances learned via router.sync
	hops         int                 // 0 for local instances, >= 1 for peer-routed instances
	via          []string            // router peer_ids on the path to this virtual instance
	advertised   []remoteInstance    // last router.sync received from this router peer
	lastSentSync []byte              // last router.sync payload sent to this router peer
	rtt          time.Duration       // EWMA ping/pong RTT for a router peer
	ewma         time.Duration       // EWMA call duration for an instance
	routes       map[*route]struct{} // active routes currently targeting this instance
}

func (p *peer) oldestInflightAge(now time.Time) time.Duration {
	var oldest time.Duration
	for rt := range p.routes {
		if age := now.Sub(rt.started); age > oldest {
			oldest = age
		}
	}
	return oldest
}

func (p *peer) observeLatency(d time.Duration) {
	if d <= 0 {
		return
	}
	if p.ewma == 0 {
		p.ewma = d
	} else {
		p.ewma = (p.ewma*4 + d) / 5
	}
}

func (p *peer) observeRTT(d time.Duration) {
	if d <= 0 {
		return
	}
	if p.rtt == 0 {
		p.rtt = d
	} else {
		p.rtt = (p.rtt*4 + d) / 5
	}
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
	started  time.Time
	deadline time.Time
	index    int         // position in Router.deadlines, or -1
	dedup    *dedupEntry // set when the call carries an idempotency key
}

type Router struct {
	config       Config
	peerID       string
	listener     net.Listener
	socketInfo   os.FileInfo
	http         *http.Server
	ctx          context.Context
	cancel       context.CancelFunc
	mu           sync.Mutex
	closed       bool
	peers        map[*peer]struct{}
	routerPeers  map[*peer]struct{}
	instances    map[string]*peer
	services     map[string]*pool
	routes       map[string]*route
	deadlines    deadlineHeap
	deadlineWake chan struct{}
	dedup        map[string]*dedupEntry
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
	for _, peerURL := range config.Peers {
		u, err := url.Parse(peerURL)
		if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") || u.Host == "" {
			return nil, errors.New("peer must be a valid ws:// or wss:// URL")
		}
	}
	peerID := config.PeerID
	if peerID == "" {
		rawID := make([]byte, 16)
		if _, err := rand.Read(rawID); err != nil {
			return nil, err
		}
		peerID = "router-" + hex.EncodeToString(rawID)
	}
	var listener net.Listener
	var err error
	if config.UnixPath != "" {
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
		config: config, peerID: peerID, listener: listener, socketInfo: socketInfo, ctx: ctx, cancel: cancel,
		peers: make(map[*peer]struct{}), routerPeers: make(map[*peer]struct{}), instances: make(map[string]*peer),
		services: make(map[string]*pool), routes: make(map[string]*route), done: make(chan struct{}),
		deadlineWake: make(chan struct{}, 1),
		dedup:        make(map[string]*dedupEntry),
		dedupOrder:   list.New(),
	}
	r.wg.Add(1)
	go r.expireLoop()
	r.ready = Ready{Kind: "router.ready", Protocol: Protocol, PID: os.Getpid(), UnixPath: config.UnixPath}
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
	for _, peerURL := range config.Peers {
		r.wg.Add(1)
		go r.dialPeerLoop(peerURL)
	}
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
	Hops        int
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
			Breaker: string(p.health.state(now)), Failures: p.health.failures, Draining: p.draining, Hops: p.hops,
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
	user, ok := r.config.Auth.authenticate(w, req)
	if !ok {
		return
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
	p := &peer{
		conn: conn, ctx: ctx, cancel: cancel, user: user,
		out:    newOutbox(max(8*1024*1024, 2*r.config.MaxFrameSize)),
		routes: make(map[*route]struct{}),
	}
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
	p.id, p.role, p.peerID = hex.EncodeToString(id), f.Role, f.PeerID
	ack := frame{Kind: "hello.ack", Protocol: Protocol, Connection: p.id}
	if p.role == "router" {
		if p.peerID == r.peerID {
			return
		}
		ack.PeerID = r.peerID
	}
	p.send(ack)
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
	} else if p.role == "router" {
		r.mu.Lock()
		r.routerPeers[p] = struct{}{}
		r.broadcastRouterSyncLocked()
		r.mu.Unlock()
	}
	stop()
	r.readLoop(p)
}

func (r *Router) dialPeerLoop(peerURL string) {
	defer r.wg.Done()
	for {
		if r.ctx.Err() != nil {
			return
		}
		r.dialAndServePeer(peerURL)
		select {
		case <-r.ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (r *Router) dialAndServePeer(peerURL string) {
	u, err := url.Parse(peerURL)
	if err != nil {
		return
	}
	var opts *websocket.DialOptions
	if u.User != nil {
		header := make(http.Header)
		pw, _ := u.User.Password()
		cred := base64.StdEncoding.EncodeToString([]byte(u.User.Username() + ":" + pw))
		header.Set("Authorization", "Basic "+cred)
		opts = &websocket.DialOptions{HTTPHeader: header}
		clean := *u
		clean.User = nil
		peerURL = clean.String()
	}
	dialCtx, dialCancel := context.WithTimeout(r.ctx, 5*time.Second)
	conn, _, err := websocket.Dial(dialCtx, peerURL, opts)
	dialCancel()
	if err != nil {
		return
	}
	conn.SetReadLimit(int64(r.config.MaxFrameSize))
	ctx, cancel := context.WithCancel(r.ctx)
	p := &peer{
		conn: conn, ctx: ctx, cancel: cancel, role: "router",
		out:    newOutbox(max(8*1024*1024, 2*r.config.MaxFrameSize)),
		routes: make(map[*route]struct{}),
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		cancel()
		_ = conn.CloseNow()
		return
	}
	r.peers[p] = struct{}{}
	r.mu.Unlock()
	writerDone := make(chan struct{})
	go func() { defer close(writerDone); p.writeLoop(r.config.MaxFrameSize) }()
	defer func() {
		cancel()
		p.out.close()
		_ = conn.CloseNow()
		<-writerDone
		r.remove(p)
	}()
	handshake, stop := context.WithTimeout(ctx, 5*time.Second)
	p.send(frame{Kind: "hello", Protocol: Protocol, Role: "router", PeerID: r.peerID})
	ack, err := readFrame(handshake, conn)
	stop()
	if err != nil || ack.Kind != "hello.ack" || ack.PeerID == r.peerID {
		return
	}
	p.id, p.peerID = ack.Connection, ack.PeerID
	r.mu.Lock()
	r.routerPeers[p] = struct{}{}
	r.broadcastRouterSyncLocked()
	r.mu.Unlock()
	r.readLoop(p)
}

func (r *Router) readLoop(p *peer) {
	var pingDone chan struct{}
	if p.role == "router" {
		pingDone = make(chan struct{})
		interval := min(r.config.IdleTimeout/3, 5*time.Second)
		go func() {
			defer close(pingDone)
			p.send(frame{Kind: "ping", Nonce: fmt.Sprintf("t:%d", time.Now().UnixNano())})
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-p.ctx.Done():
					return
				case <-ticker.C:
					p.send(frame{Kind: "ping", Nonce: fmt.Sprintf("t:%d", time.Now().UnixNano())})
				}
			}
		}()
	}
	defer func() {
		if pingDone != nil {
			p.cancel()
			<-pingDone
		}
	}()
	for {
		// A peer that sends nothing for IdleTimeout is treated as gone; connecting
		// peers keep the connection alive with protocol pings.
		read, idle := context.WithTimeout(p.ctx, r.config.IdleTimeout)
		f, err := readFrame(read, p.conn)
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
			if p.role == "router" && strings.HasPrefix(f.Nonce, "t:") {
				if ns, err := strconv.ParseInt(f.Nonce[2:], 10, 64); err == nil {
					r.mu.Lock()
					p.observeRTT(time.Since(time.Unix(0, ns)))
					r.mu.Unlock()
				}
			}
			continue
		}
		if f.Kind == "server.drain" {
			// Graceful exit: leave every pool now, keep serving routed calls.
			if p.role != "server" {
				_ = p.conn.Close(websocket.StatusPolicyViolation, "frame not allowed for role")
				return
			}
			r.drain(p)
			p.send(frame{Kind: "server.drain.ack", InstanceID: p.instanceID})
			continue
		}
		if !allowedFrame(p.role, f) {
			_ = p.conn.Close(websocket.StatusPolicyViolation, "frame not allowed for role")
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

func allowedCallerFrame(f frame) bool {
	return f.Kind == "call.open" || f.Kind == "call.cancel" ||
		((f.Kind == "stream.item" || f.Kind == "stream.end") && f.Direction == "client") ||
		(f.Kind == "stream.window" && f.Direction == "server")
}

func allowedCalleeFrame(f frame) bool {
	return f.Kind == "call.result" || f.Kind == "call.error" ||
		((f.Kind == "stream.item" || f.Kind == "stream.end") && f.Direction == "server") ||
		(f.Kind == "stream.window" && f.Direction == "client")
}

func allowedFrame(role string, f frame) bool {
	switch role {
	case "client":
		return allowedCallerFrame(f)
	case "server":
		return allowedCalleeFrame(f)
	case "router":
		return f.Kind == "router.sync" || allowedCallerFrame(f) || allowedCalleeFrame(f)
	default:
		return false
	}
}

func validateServices(services []service, user *User, checkAuth bool) error {
	seen := make(map[string]bool)
	for _, s := range services {
		if !validServiceName(s.Name) || strings.HasPrefix(s.Name, reservedPrefix) || seen[s.Name] || (checkAuth && !user.allowsRegistration(s.Name)) {
			return errors.New("duplicate, invalid, or unauthorized service")
		}
		if s.Contract != nil && !json.Valid(s.Contract) {
			return errors.New("invalid service contract")
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
	return nil
}

func (r *Router) compatibleWithPoolsLocked(services []service) error {
	for _, s := range services {
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
	return nil
}

func (r *Router) joinPoolsLocked(p *peer) {
	for _, s := range p.services {
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
}

func (r *Router) register(p *peer, f frame) error {
	if err := validateServices(f.Services, p.user, true); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing := r.instances[f.InstanceID]; existing != nil {
		if existing.gateway == nil {
			return errors.New("duplicate instance_id")
		}
		// A direct local registration supersedes a virtual remote advertisement
		// for the same instance_id.
		r.leavePools(existing)
		delete(r.instances, f.InstanceID)
	}
	if err := r.compatibleWithPoolsLocked(f.Services); err != nil {
		return err
	}
	p.instanceID, p.services, p.maxInflight = f.InstanceID, f.Services, f.MaxInflight
	r.instances[p.instanceID] = p
	// Queue the ack before exposing the instance to calls.
	p.send(frame{Kind: "server.register.ack", InstanceID: p.instanceID})
	r.joinPoolsLocked(p)
	r.broadcastRouterSyncLocked()
	return nil
}

func (r *Router) reconcileRemoteInstancesLocked() {
	type winner struct {
		gateway *peer
		adv     remoteInstance
	}
	rps := make([]*peer, 0, len(r.routerPeers))
	for rp := range r.routerPeers {
		rps = append(rps, rp)
	}
	slices.SortFunc(rps, func(a, b *peer) int {
		if c := strings.Compare(a.peerID, b.peerID); c != 0 {
			return c
		}
		return strings.Compare(a.id, b.id)
	})
	winners := make(map[string]winner)
	for _, rp := range rps {
		for _, adv := range rp.advertised {
			if slices.Contains(adv.Via, r.peerID) || len(adv.Via) > 4 {
				continue
			}
			if existing := r.instances[adv.InstanceID]; existing != nil && existing.gateway == nil {
				continue
			}
			if validateServices(adv.Services, rp.user, false) != nil {
				continue
			}
			cur, ok := winners[adv.InstanceID]
			if !ok || len(adv.Via) < len(cur.adv.Via) {
				winners[adv.InstanceID] = winner{gateway: rp, adv: adv}
			} else if len(adv.Via) == len(cur.adv.Via) {
				if existing := r.instances[adv.InstanceID]; existing != nil && existing.gateway == rp {
					winners[adv.InstanceID] = winner{gateway: rp, adv: adv}
				}
			}
		}
	}
	for id, inst := range r.instances {
		if inst.gateway == nil {
			continue
		}
		w, ok := winners[id]
		if !ok || w.gateway != inst.gateway {
			delete(r.instances, id)
			r.leavePools(inst)
			for callID, rt := range r.routes {
				if rt.instance == inst {
					lost := routerError(callID, "unavailable", "Service instance disconnected", true, "instance_lost", inst.instanceID)
					if rt.dedup != nil && rt.dedup.route == rt {
						r.dedupFail(rt.dedup, lost, inst)
						rt.dedup = nil
					}
					r.endRoute(callID, rt)
					rt.client.send(lost)
				}
			}
		}
	}
	winIDs := make([]string, 0, len(winners))
	for id := range winners {
		winIDs = append(winIDs, id)
	}
	slices.Sort(winIDs)
	for _, id := range winIDs {
		w := winners[id]
		if existing := r.instances[id]; existing != nil {
			r.leavePools(existing)
			if r.compatibleWithPoolsLocked(w.adv.Services) != nil {
				delete(r.instances, id)
				continue
			}
			existing.services = w.adv.Services
			existing.maxInflight = w.adv.MaxInflight
			existing.hops = len(w.adv.Via)
			existing.via = slices.Clone(w.adv.Via)
			r.joinPoolsLocked(existing)
			continue
		}
		if r.compatibleWithPoolsLocked(w.adv.Services) != nil {
			continue
		}
		inst := &peer{
			conn:        w.gateway.conn,
			ctx:         w.gateway.ctx,
			cancel:      w.gateway.cancel,
			out:         w.gateway.out,
			user:        w.gateway.user,
			id:          w.gateway.id,
			role:        "server",
			instanceID:  id,
			services:    w.adv.Services,
			maxInflight: w.adv.MaxInflight,
			gateway:     w.gateway,
			hops:        len(w.adv.Via),
			via:         slices.Clone(w.adv.Via),
			routes:      make(map[*route]struct{}),
		}
		r.instances[id] = inst
		r.joinPoolsLocked(inst)
	}
}

func (r *Router) broadcastRouterSyncLocked() {
	if len(r.routerPeers) == 0 {
		return
	}
	ids := make([]string, 0, len(r.instances))
	for id := range r.instances {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for target := range r.routerPeers {
		list := make([]remoteInstance, 0, len(ids))
		for _, id := range ids {
			inst := r.instances[id]
			if inst.draining || inst.gateway == target || len(inst.via) >= 4 {
				continue
			}
			if target.peerID != "" && slices.Contains(inst.via, target.peerID) {
				continue
			}
			via := append(slices.Clone(inst.via), r.peerID)
			list = append(list, remoteInstance{
				InstanceID:  inst.instanceID,
				Services:    inst.services,
				MaxInflight: inst.maxInflight,
				Via:         via,
			})
		}
		syncFrame := frame{Kind: "router.sync", Instances: list}
		raw := syncFrame.bytes()
		if !bytes.Equal(target.lastSentSync, raw) {
			target.lastSentSync = raw
			target.send(frame{Kind: "router.sync", raw: raw})
		}
	}
}

func (r *Router) forward(p *peer, f frame) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p.ctx.Err() != nil || !r.config.Auth.valid(p.user) {
		p.cancel()
		return
	}
	if f.Kind == "router.sync" {
		p.advertised = f.Instances
		r.reconcileRemoteInstancesLocked()
		r.broadcastRouterSyncLocked()
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
		var entry *dedupEntry
		var avoid *peer
		if f.Idempotency != "" && r.methodStream(f.Service, f.Method) == "unary" {
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
		instance, errorFrame := r.selectInstance(p, f, avoid)
		if errorFrame != nil {
			p.send(*errorFrame)
			return
		}
		now := time.Now()
		rt := &route{id: f.CallID, client: p, instance: instance, started: now, index: -1}
		r.routes[f.CallID] = rt
		instance.inflight++
		if instance.routes == nil {
			instance.routes = make(map[*route]struct{})
		}
		instance.routes[rt] = struct{}{}
		if f.TimeoutMS != nil {
			r.track(rt, time.Duration(*f.TimeoutMS)*time.Millisecond)
		}
		if f.Idempotency != "" && r.methodStream(f.Service, f.Method) == "unary" {
			r.dedupInsert(dedupKey(f), rt)
		}
		instance.send(f)
		return
	}
	rt := r.routes[f.CallID]
	if rt == nil {
		return
	}
	if rt.client == p && allowedCallerFrame(f) {
		rt.instance.send(f)
		if f.Kind == "call.cancel" {
			// The client has given up; forget the route now so a service that
			// ignores cancellation cannot pin router capacity.
			r.endRoute(f.CallID, rt)
		}
	} else if (rt.instance == p || rt.instance.gateway == p) && allowedCalleeFrame(f) {
		rt.client.send(f)
		if f.Kind == "call.result" || f.Kind == "call.error" {
			if f.Kind == "call.result" {
				rt.instance.observeLatency(time.Since(rt.started))
			}
			r.observe(rt.instance, !instanceFailed(f))
			if rt.dedup != nil {
				r.dedupSettle(rt.dedup, f)
			}
			r.endRoute(f.CallID, rt)
		}
	}
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
	r.broadcastRouterSyncLocked()
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
	if p.role == "router" {
		delete(r.routerPeers, p)
		r.reconcileRemoteInstancesLocked()
		r.broadcastRouterSyncLocked()
	}
	if r.instances[p.instanceID] == p {
		delete(r.instances, p.instanceID)
		r.leavePools(p)
		r.reconcileRemoteInstancesLocked()
		r.broadcastRouterSyncLocked()
	}
	for id, rt := range r.routes {
		switch {
		case rt.client == p:
			r.endRoute(id, rt)
			rt.instance.send(frame{Kind: "call.cancel", CallID: id, Reason: "client_disconnected"})
		case rt.instance == p || rt.instance.gateway == p:
			lost := routerError(id, "unavailable", "Service instance disconnected", true, "instance_lost", rt.instance.instanceID)
			if rt.dedup != nil && rt.dedup.route == rt {
				r.dedupFail(rt.dedup, lost, rt.instance)
				rt.dedup = nil
			}
			r.endRoute(id, rt)
			rt.client.send(lost)
		}
	}
}
