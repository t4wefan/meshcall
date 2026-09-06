package router

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
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
}

type Ready struct {
	Kind     string `json:"kind"`
	Protocol string `json:"protocol"`
	PID      int    `json:"pid"`
	Host     string `json:"host,omitempty"`
	Port     int    `json:"port,omitempty"`
	UnixPath string `json:"unix_path,omitempty"`
}

type peer struct {
	conn       *websocket.Conn
	ctx        context.Context
	cancel     context.CancelFunc
	out        *outbox
	user       *User
	id         string
	role       string
	instanceID string
	services   []service
	inflight   int
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
	client   *peer
	instance *peer
}

type Router struct {
	config     Config
	listener   net.Listener
	socketInfo os.FileInfo
	http       *http.Server
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	closed     bool
	peers      map[*peer]struct{}
	instances  map[string]*peer
	services   map[string]*pool
	routes     map[string]*route
	wg         sync.WaitGroup
	closeOnce  sync.Once
	done       chan struct{}
	ready      Ready
}

func Start(config Config) (*Router, error) {
	if config.MaxFrameSize == 0 {
		config.MaxFrameSize = 1024 * 1024
	}
	if config.MaxFrameSize < 1 || config.MaxFrameSize > 64*1024*1024 {
		return nil, errors.New("max-frame-size must be between 1 and 67108864")
	}
	if config.Port < 0 || config.Port > 65535 {
		return nil, errors.New("port must be between 0 and 65535")
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
		config: config, listener: listener, socketInfo: socketInfo, ctx: ctx, cancel: cancel,
		peers: make(map[*peer]struct{}), instances: make(map[string]*peer),
		services: make(map[string]*pool), routes: make(map[string]*route), done: make(chan struct{}),
	}
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
	return r, nil
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
		f, err := readFrame(ctx, conn)
		if err != nil {
			return
		}
		if f.Kind == "ping" {
			p.send(frame{Kind: "pong", Nonce: f.Nonce})
			continue
		}
		if f.Kind == "pong" {
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
	return decode(data)
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
		if s.Name == "" || strings.HasPrefix(s.Name, reservedPrefix) || seen[s.Name] || !p.user.allowsRegistration(s.Name) {
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
		if pool := r.services[s.Name]; pool != nil && !reflect.DeepEqual(pool.signature, s) {
			return errors.New("incompatible service registration")
		}
	}
	p.instanceID, p.services = f.InstanceID, f.Services
	r.instances[p.instanceID] = p
	// Queue the ack before exposing the instance to calls.
	p.send(frame{Kind: "server.register.ack", InstanceID: p.instanceID})
	for _, s := range f.Services {
		if r.services[s.Name] == nil {
			r.services[s.Name] = &pool{signature: s, cursors: make(map[string]int)}
		}
		pool := r.services[s.Name]
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
		if f.Deadline != nil && time.Now().UnixMilli() >= *f.Deadline {
			p.send(callError(f.CallID, "deadline_exceeded", "Call deadline exceeded", false))
			return
		}
		if strings.HasPrefix(f.Service, reservedPrefix) {
			r.management(p, f)
			return
		}
		if len(r.routes) >= 65536 {
			p.send(callError(f.CallID, "resource_exhausted", "Router call capacity reached", true))
			return
		}
		instance, errorFrame := r.selectInstance(f)
		if errorFrame != nil {
			p.send(*errorFrame)
			return
		}
		r.routes[f.CallID] = &route{client: p, instance: instance}
		instance.inflight++
		instance.send(f)
		return
	}
	route := r.routes[f.CallID]
	if route == nil {
		return
	}
	if p.role == "client" && route.client == p {
		route.instance.send(f)
	} else if p.role == "server" && route.instance == p {
		route.client.send(f)
		if f.Kind == "call.result" || f.Kind == "call.error" {
			delete(r.routes, f.CallID)
			route.instance.inflight--
		}
	}
}

func (r *Router) remove(p *peer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.peers, p)
	if r.instances[p.instanceID] == p {
		delete(r.instances, p.instanceID)
		for _, s := range p.services {
			pool := r.services[s.Name]
			pool.instances = slices.DeleteFunc(pool.instances, func(candidate *peer) bool { return candidate == p })
			if len(pool.instances) == 0 {
				delete(r.services, s.Name)
			}
		}
	}
	for id, route := range r.routes {
		switch {
		case route.client == p:
			delete(r.routes, id)
			route.instance.inflight--
			route.instance.send(frame{Kind: "call.cancel", CallID: id, Reason: "client_disconnected"})
		case route.instance == p:
			delete(r.routes, id)
			p.inflight--
			route.client.send(callError(id, "unavailable", "Service instance disconnected", true))
		}
	}
}
