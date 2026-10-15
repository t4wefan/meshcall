package meshcall

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// ServerOptions configures a Server.
type ServerOptions struct {
	Services []*Service
	// InstanceID is visible to the Router, `router inspect` and targeted
	// callers; generated when empty.
	InstanceID string
	// MaxInflight caps concurrent calls across all services (0: unlimited).
	MaxInflight  int
	Interceptors []ServerInterceptor
	Logger       *slog.Logger
	// AccessLog logs one line per call at Info (errors at Warn / Error).
	AccessLog bool
	// Router connections: keepalive and re-registration after a loss.
	PingInterval time.Duration
	PingTimeout  time.Duration
	Reconnect    ReconnectPolicy
	// Authorization is sent to the Router ("Basic …" or "Bearer mk_…").
	Authorization string
	// IdleTimeout closes a direct client connection silent for this long.
	IdleTimeout  time.Duration
	MaxFrameSize int
	// Strict turns contract warnings into an error from NewServer (also set
	// by MESHCALL_STRICT=1).
	Strict bool
}

// Server runs services in Router mode (ConnectRouter) or direct mode (Listen).
type Server struct {
	opts     ServerOptions
	services map[string]*Service
	targeted map[string]bool
	logger   *slog.Logger
	inflight atomic.Int64
	ctx      context.Context
	stop     context.CancelFunc

	mu       sync.Mutex
	sessions map[*session]struct{}
	links    []*routerLink
	listener net.Listener
	http     *http.Server
	unixPath string
	unixInfo os.FileInfo
	draining bool
	closed   bool
}

// NewServer validates the services and lints their contracts.
func NewServer(opts ServerOptions) (*Server, error) {
	if len(opts.Services) == 0 {
		return nil, errors.New("meshcall: a server needs at least one service")
	}
	if opts.InstanceID == "" {
		opts.InstanceID = randomID()
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.PingInterval == 0 {
		opts.PingInterval = DefaultPingInterval
	}
	if opts.PingTimeout == 0 {
		opts.PingTimeout = DefaultPingTimeout
	}
	if opts.IdleTimeout == 0 {
		opts.IdleTimeout = DefaultIdleTimeout
	}
	if opts.MaxFrameSize == 0 {
		opts.MaxFrameSize = DefaultMaxFrameSize
	}
	s := &Server{opts: opts, services: map[string]*Service{}, targeted: map[string]bool{}, logger: opts.Logger, sessions: map[*session]struct{}{}}
	s.ctx, s.stop = context.WithCancel(context.Background())
	var findings []ContractWarning
	for _, svc := range opts.Services {
		if s.services[svc.name] != nil {
			return nil, fmt.Errorf("meshcall: duplicate service %s", svc.name)
		}
		if len(svc.methods) == 0 {
			return nil, fmt.Errorf("meshcall: service %s declares no methods", svc.name)
		}
		s.services[svc.name] = svc
		s.targeted[svc.name] = svc.targeted
		findings = append(findings, LintService(svc)...)
	}
	findings = withoutIgnored(findings)
	if len(findings) > 0 {
		if opts.Strict || strictMode() {
			lines := make([]string, len(findings))
			for i, f := range findings {
				lines[i] = f.String()
			}
			return nil, fmt.Errorf("meshcall: strict mode: %d contract warning(s)\n%s", len(findings), strings.Join(lines, "\n"))
		}
		s.logger.Warn(fmt.Sprintf("%d contract warning(s); see LintService for details and hints, or set MESHCALL_STRICT=1 to fail on them", len(findings)))
	}
	return s, nil
}

// InstanceID is this server's instance id.
func (s *Server) InstanceID() string { return s.opts.InstanceID }

func (s *Server) registrations() []registration {
	out := make([]registration, 0, len(s.opts.Services))
	for _, svc := range s.opts.Services {
		out = append(out, svc.registration())
	}
	return out
}

// --- Router mode -----------------------------------------------------------

type routerLink struct {
	endpoint endpoint
	conn     *conn // set only while registered
	session  *session
}

// ConnectRouter registers with one or more Routers (ws://, unix://, pipe://)
// and returns once at least one accepted the registration. Each link then
// re-registers on its own after a loss, with the reconnect policy.
func (s *Server) ConnectRouter(ctx context.Context, urls ...string) error {
	if len(urls) == 0 {
		return errors.New("meshcall: ConnectRouter needs a Router URL")
	}
	var links []*routerLink
	for _, raw := range urls {
		e, err := parseEndpoint(raw)
		if err != nil {
			return err
		}
		links = append(links, &routerLink{endpoint: e})
	}
	s.mu.Lock()
	s.links = append(s.links, links...)
	s.mu.Unlock()
	errs := make([]error, len(links))
	var wg sync.WaitGroup
	for i, link := range links {
		wg.Add(1)
		go func() { defer wg.Done(); errs[i] = s.register(ctx, link) }()
	}
	wg.Wait()
	registered := 0
	for i, link := range links {
		if errs[i] == nil {
			registered++
			go s.supervise(link, false)
		} else {
			s.logger.Warn("Router refused the instance at start; retrying in the background", "router", link.endpoint.text, "instance", s.opts.InstanceID, "error", errs[i])
			go s.supervise(link, true)
		}
	}
	if registered == 0 {
		return errs[0]
	}
	return nil
}

func (s *Server) register(ctx context.Context, link *routerLink) error {
	header := http.Header{}
	if s.opts.Authorization != "" {
		header.Set("Authorization", s.opts.Authorization)
	}
	c, err := link.endpoint.connect(ctx, header, s.opts.MaxFrameSize)
	if err != nil {
		return err
	}
	fail := func(err error) error { c.close(); return err }
	if err := c.send(frame{Kind: "hello", Protocol: Protocol, Role: "server", PeerID: s.opts.InstanceID, InstanceID: s.opts.InstanceID}); err != nil {
		return fail(err)
	}
	ack, err := c.receive()
	if err != nil || ack.Kind != "hello.ack" || ack.Protocol != Protocol {
		return fail(fmt.Errorf("meshcall: expected a compatible hello.ack from the Router: %v", err))
	}
	if err := c.send(frame{Kind: "server.register", InstanceID: s.opts.InstanceID, Services: s.registrations(), MaxInflight: s.opts.MaxInflight}); err != nil {
		return fail(err)
	}
	reg, err := c.receive()
	if err != nil || reg.Kind != "server.register.ack" || reg.InstanceID != s.opts.InstanceID {
		if err == nil {
			err = errors.New("meshcall: the Router refused the registration")
		}
		return fail(err)
	}
	sess := s.newSession(c)
	s.mu.Lock()
	if s.closed || s.draining {
		s.mu.Unlock()
		return fail(errors.New("meshcall: server is stopping"))
	}
	link.conn, link.session = c, sess
	s.mu.Unlock()
	go c.keepAlive(s.opts.PingInterval, s.opts.PingTimeout)
	return nil
}

// supervise serves a link's connection and re-registers after it drops.
func (s *Server) supervise(link *routerLink, startDown bool) {
	for {
		if !startDown {
			s.mu.Lock()
			sess := link.session
			s.mu.Unlock()
			if sess != nil {
				sess.serve()
			}
			s.mu.Lock()
			link.conn, link.session = nil, nil
			stopping := s.closed || s.draining
			s.mu.Unlock()
			if stopping {
				return
			}
			s.logger.Warn("Router connection lost; reconnecting", "router", link.endpoint.text, "instance", s.opts.InstanceID)
		}
		startDown = false
		next := s.opts.Reconnect.delays()
		for {
			delay, ok := next()
			if !ok {
				s.logger.Error("Giving up on Router reconnection", "router", link.endpoint.text)
				return
			}
			select {
			case <-time.After(delay):
			case <-s.ctx.Done():
				return
			}
			ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
			err := s.register(ctx, link)
			cancel()
			if err == nil {
				s.logger.Info("Router connection restored", "router", link.endpoint.text)
				break
			}
			s.mu.Lock()
			stopping := s.closed || s.draining
			s.mu.Unlock()
			if stopping {
				return
			}
			s.logger.Warn("Router re-registration failed", "router", link.endpoint.text, "error", err)
		}
	}
}

// --- direct mode -----------------------------------------------------------

// Listen serves clients directly on addr: "host:port" (port 0 picks one),
// "unix:///path" (not on Windows) or "pipe://NAME" (Windows only).
func (s *Server) Listen(addr string) error {
	var l net.Listener
	var err error
	switch {
	case strings.HasPrefix(addr, "pipe://"):
		if !pipesSupported {
			return errPipeOffWindows
		}
		name, perr := pipeName(strings.TrimPrefix(addr, "pipe://"))
		if perr != nil {
			return perr
		}
		l, err = listenPipe(name)
	case strings.HasPrefix(addr, "unix://"):
		if pipesSupported {
			return errUnixOnWindows
		}
		path, aerr := filepath.Abs(strings.TrimPrefix(addr, "unix://"))
		if aerr != nil {
			return aerr
		}
		if len(path) > 103 {
			return errors.New("meshcall: Unix socket path exceeds the portable 103-byte limit")
		}
		if _, statErr := os.Lstat(path); !errors.Is(statErr, os.ErrNotExist) {
			return errors.New("meshcall: Unix socket path already exists or is inaccessible: " + path)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			return err
		}
		l, err = net.Listen("unix", path)
		if err == nil {
			l.(*net.UnixListener).SetUnlinkOnClose(false)
			info, _ := os.Lstat(path)
			s.mu.Lock()
			s.unixPath, s.unixInfo = path, info
			s.mu.Unlock()
		}
	default:
		l, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: http.HandlerFunc(s.accept), ReadHeaderTimeout: 10 * time.Second}
	s.mu.Lock()
	s.listener, s.http = l, srv
	s.mu.Unlock()
	go func() { _ = srv.Serve(l) }()
	return nil
}

// Addr is the bound address in direct mode ("127.0.0.1:43210", a socket path…).
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

func (s *Server) accept(w http.ResponseWriter, r *http.Request) {
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	c := newConn(ws, s.opts.MaxFrameSize)
	hello, err := receiveWithin(c, 10*time.Second)
	if err != nil || hello.Kind != "hello" || hello.Role != "client" || hello.Protocol != Protocol {
		_ = ws.Close(websocket.StatusProtocolError, "expected a compatible client hello")
		c.close()
		return
	}
	if err := c.send(frame{Kind: "hello.ack", Protocol: Protocol, Connection: randomID()}); err != nil {
		c.close()
		return
	}
	go c.watchIdle(s.opts.IdleTimeout)
	s.newSession(c).serve()
}

func receiveWithin(c *conn, timeout time.Duration) (frame, error) {
	type result struct {
		f   frame
		err error
	}
	done := make(chan result, 1)
	go func() { f, err := c.receive(); done <- result{f, err} }()
	select {
	case r := <-done:
		return r.f, r.err
	case <-time.After(timeout):
		c.close()
		return frame{}, errors.New("handshake timed out")
	}
}

// --- lifecycle -------------------------------------------------------------

// Drain stops taking new calls — Router mode leaves every pool (server.drain),
// direct mode closes the listener — and waits for in-flight calls to finish
// or ctx to end. Close the server afterwards.
func (s *Server) Drain(ctx context.Context) error {
	s.mu.Lock()
	s.draining = true
	links := append([]*routerLink{}, s.links...)
	listener := s.listener
	s.mu.Unlock()
	if listener != nil {
		_ = listener.Close()
	}
	var wg sync.WaitGroup
	for _, link := range links {
		s.mu.Lock()
		sess := link.session
		s.mu.Unlock()
		if sess == nil {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			ack := sess.expectDrainAck()
			if sess.conn.send(frame{Kind: "server.drain"}) != nil {
				return
			}
			select {
			case <-ack:
			case <-ctx.Done():
			case <-sess.conn.closed:
			}
		}()
	}
	wg.Wait()
	for s.inflight.Load() > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	return nil
}

// Close stops the server: open calls fail, connections close.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	sessions := make([]*session, 0, len(s.sessions))
	for sess := range s.sessions {
		sessions = append(sessions, sess)
	}
	srv, unixPath, unixInfo := s.http, s.unixPath, s.unixInfo
	s.mu.Unlock()
	s.stop()
	if srv != nil {
		_ = srv.Close()
	}
	for _, sess := range sessions {
		sess.conn.close()
	}
	if unixPath != "" && unixInfo != nil {
		if current, err := os.Lstat(unixPath); err == nil && os.SameFile(unixInfo, current) {
			_ = os.Remove(unixPath)
		}
	}
	return nil
}

// --- sessions and calls ----------------------------------------------------

type session struct {
	server    *Server
	conn      *conn
	mu        sync.Mutex
	calls     map[string]*serverCall
	drainAcks []chan struct{}
}

func (s *Server) newSession(c *conn) *session {
	sess := &session{server: s, conn: c, calls: map[string]*serverCall{}}
	s.mu.Lock()
	s.sessions[sess] = struct{}{}
	s.mu.Unlock()
	return sess
}

func (sess *session) expectDrainAck() <-chan struct{} {
	ch := make(chan struct{})
	sess.mu.Lock()
	sess.drainAcks = append(sess.drainAcks, ch)
	sess.mu.Unlock()
	return ch
}

// serve reads frames until the connection ends, then fails its calls.
func (sess *session) serve() {
	defer func() {
		sess.server.mu.Lock()
		delete(sess.server.sessions, sess)
		sess.server.mu.Unlock()
		sess.mu.Lock()
		calls := make([]*serverCall, 0, len(sess.calls))
		for _, call := range sess.calls {
			calls = append(calls, call)
		}
		sess.mu.Unlock()
		for _, call := range calls {
			call.abort(&Error{Code: CodeUnavailable, Message: "Connection was lost", Retryable: true})
		}
		sess.conn.close()
	}()
	for {
		f, err := sess.conn.receive()
		if err != nil {
			if sess.conn.ctx.Err() != nil {
				return
			}
			continue // a malformed frame; the connection itself is fine
		}
		sess.handle(f)
	}
}

func (sess *session) handle(f frame) {
	switch f.Kind {
	case "server.drain.ack":
		sess.mu.Lock()
		for _, ch := range sess.drainAcks {
			close(ch)
		}
		sess.drainAcks = nil
		sess.mu.Unlock()
	case "call.open":
		sess.open(f)
	case "call.cancel", "stream.item", "stream.end", "stream.window":
		sess.mu.Lock()
		call := sess.calls[f.CallID]
		sess.mu.Unlock()
		if call != nil {
			call.receive(f)
		}
	}
}

func (sess *session) reject(f frame, e *Error) {
	_ = sess.conn.send(frame{Kind: "call.error", CallID: f.CallID, Error: e.payload()})
	sess.server.access(f, e.Code, time.Now())
}

func (sess *session) open(f frame) {
	s := sess.server
	sess.mu.Lock()
	duplicate := sess.calls[f.CallID] != nil
	sess.mu.Unlock()
	if duplicate {
		sess.reject(f, &Error{Code: CodeProtocolError, Message: "Duplicate call_id"})
		return
	}
	if s.opts.MaxInflight > 0 && s.inflight.Load() >= int64(s.opts.MaxInflight) {
		sess.reject(f, detailed(CodeResourceExhausted, "Instance is at its max_inflight", "server", "overloaded", true))
		return
	}
	svc := s.services[f.Service]
	var m *method
	if svc != nil {
		m = svc.byName[f.Method]
	}
	if m == nil {
		sess.reject(f, &Error{Code: CodeMethodNotFound, Message: "Unknown method " + f.Service + "." + f.Method})
		return
	}
	// Targeting, applied the way the Router does (docs/protocol.md → Targeted calls).
	switch {
	case f.InstanceID != "" && f.InstanceID != s.opts.InstanceID:
		e := detailed(CodeUnavailable, "Instance "+f.InstanceID+" is not this server ("+s.opts.InstanceID+")", "server", "instance_unknown", false)
		e.Details["instance_id"] = f.InstanceID
		sess.reject(f, e)
		return
	case f.InstanceID == "" && svc.targeted:
		sess.reject(f, detailed(CodeInvalidArgument, f.Service+" is a targeted service: call.open needs instance_id", "server", "instance_required", false))
		return
	case m.stream == duplexShape && f.Idempotency != "":
		sess.reject(f, detailed(CodeInvalidArgument, "Duplex calls are never replayed and take no idempotency key", "server", "idempotency_key_on_duplex", false))
		return
	}
	call := newServerCall(sess, f, m)
	sess.mu.Lock()
	sess.calls[f.CallID] = call
	sess.mu.Unlock()
	s.inflight.Add(1)
	go call.run()
}

func (s *Server) access(f frame, status string, started time.Time) {
	if !s.opts.AccessLog {
		return
	}
	level := slog.LevelInfo
	if status == CodeInternal || status == CodeProtocolError {
		level = slog.LevelError
	} else if status != "ok" {
		level = slog.LevelWarn
	}
	s.logger.Log(context.Background(), level, "RPC call "+f.Service+"."+f.Method, "status", status,
		"duration_ms", float64(time.Since(started).Microseconds())/1000, "call_id", f.CallID)
}

type serverCall struct {
	sess      *session
	open      frame
	method    *method
	ctx       *Context
	cancel    context.CancelCauseFunc
	started   time.Time
	credit    *creditWindow // output credit granted by the caller
	trailerMu sync.Mutex
	trailers  map[string]string

	inputMu        sync.Mutex
	input          []inputEvent
	inputWake      chan struct{}
	inputAllowance int64
	inputSequence  int64
	inputClosed    bool
	idle           *time.Timer

	outputMu       sync.Mutex
	outputSequence int64
	outputClosed   bool
	terminal       atomic.Bool
}

func newServerCall(sess *session, f frame, m *method) *serverCall {
	call := &serverCall{sess: sess, open: f, method: m, started: time.Now(), credit: newCreditWindow(),
		trailers: map[string]string{}, inputWake: make(chan struct{})}
	base, cancel := context.WithCancelCause(sess.server.ctx)
	if f.TimeoutMS != nil {
		// The budget is relative, measured from when this peer saw call.open.
		var stop context.CancelFunc
		base, stop = context.WithDeadlineCause(base, time.Now().Add(time.Duration(*f.TimeoutMS)*time.Millisecond),
			detailed(CodeDeadlineExceeded, "Call deadline exceeded", "server", "", false))
		go func() { <-base.Done(); stop() }()
	}
	call.cancel = cancel
	metadata := f.Metadata
	if metadata == nil {
		metadata = map[string]string{}
	}
	call.ctx = &Context{
		Context: base, CallID: f.CallID, Service: f.Service, Method: f.Method, Metadata: metadata,
		IdempotencyKey: f.Idempotency, InstanceID: sess.server.opts.InstanceID, call: call,
		Logger: sess.server.logger.With("service", f.Service, "method", f.Method, "call_id", f.CallID),
	}
	return call
}

func (call *serverCall) intercept(ctx *Context, request any, handler ServerCallNext) (any, error) {
	return chain(call.sess.server.opts.Interceptors, ctx, request, handler)
}

func (call *serverCall) send(f frame) error {
	f.CallID = call.open.CallID
	return call.sess.conn.send(f)
}

func (call *serverCall) run() {
	m := call.method
	hasInput := m.stream == clientStreamShape || m.stream == duplexShape
	if hasInput {
		call.inputMu.Lock()
		call.inputAllowance = initialStreamCredit
		call.inputMu.Unlock()
		_ = call.send(frame{Kind: "stream.window", Direction: "client", Credit: int64p(initialStreamCredit)})
		call.armIdle()
	}
	result, err := call.invoke()
	if err == nil && (m.stream == serverStreamShape || m.stream == duplexShape) {
		err = call.closeOutput()
	}
	var payload json.RawMessage
	if err == nil && m.response != nil {
		payload, err = encodeOutput(m.schemas.response, result, "invalid_response", call.logf)
	}
	if err != nil {
		call.finish(frame{Kind: "call.error", Error: call.errorOf(err).payload()}, call.errorOf(err).Code)
		return
	}
	call.finish(frame{Kind: "call.result", Payload: payload}, "ok")
}

// invoke runs the handler; a panic is an internal error, logged with its stack.
func (call *serverCall) invoke() (result any, err error) {
	defer func() {
		if r := recover(); r != nil {
			call.ctx.Logger.Error("Handler panicked", "panic", r, "stack", string(debug.Stack()))
			err = &Error{Code: CodeInternal, Message: "Internal service error"}
		}
	}()
	result, err = call.method.invoke(call.ctx, call, call.open.Payload)
	if err == nil && call.ctx.Err() != nil {
		err = context.Cause(call.ctx)
	}
	return result, err
}

func (call *serverCall) logf(format string, args ...any) {
	call.ctx.Logger.Error(fmt.Sprintf(format, args...))
}

// errorOf turns any handler or runtime error into the wire error.
func (call *serverCall) errorOf(err error) *Error {
	if cause := context.Cause(call.ctx); cause != nil && call.ctx.Err() != nil {
		if e, ok := AsError(cause); ok {
			return e
		}
	}
	if e, ok := AsError(err); ok {
		return e
	}
	if errors.Is(err, context.Canceled) {
		return &Error{Code: CodeCancelled, Message: "Call was cancelled"}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return detailed(CodeDeadlineExceeded, "Call deadline exceeded", "server", "", false)
	}
	call.ctx.Logger.Error("Unhandled service error", "error", err)
	return &Error{Code: CodeInternal, Message: "Internal service error"}
}

func (call *serverCall) finish(f frame, status string) {
	if !call.terminal.CompareAndSwap(false, true) {
		return
	}
	call.trailerMu.Lock()
	if len(call.trailers) > 0 {
		f.Metadata = call.trailers
	}
	call.trailerMu.Unlock()
	_ = call.send(f)
	call.cleanup()
	call.sess.server.access(call.open, status, call.started)
}

// abort ends the call from the outside (cancel, deadline, lost connection).
func (call *serverCall) abort(e *Error) {
	call.cancel(e)
	call.credit.close(e)
	call.pushInput(nil, e)
}

func (call *serverCall) cleanup() {
	call.cancel(&Error{Code: CodeCancelled, Message: "Call is complete"})
	call.credit.close(&Error{Code: CodeCancelled, Message: "Call is closed"})
	call.inputMu.Lock()
	if call.idle != nil {
		call.idle.Stop()
	}
	call.inputMu.Unlock()
	call.sess.mu.Lock()
	if call.sess.calls[call.open.CallID] == call {
		delete(call.sess.calls, call.open.CallID)
	}
	call.sess.mu.Unlock()
	call.sess.server.inflight.Add(-1)
}

func (call *serverCall) receive(f frame) {
	switch f.Kind {
	case "call.cancel":
		e := &Error{Code: CodeCancelled, Message: "Call was cancelled"}
		call.abort(e)
		call.finish(frame{Kind: "call.error", Error: e.payload()}, CodeCancelled)
	case "stream.window":
		if f.Direction != "server" || call.method.output == nil || f.Credit == nil || *f.Credit <= 0 {
			call.protocolError("Invalid output stream window")
			return
		}
		call.credit.grant(*f.Credit)
	case "stream.item":
		call.inputMu.Lock()
		ok := call.method.input != nil && f.Direction == "client" && !call.inputClosed && call.inputAllowance > 0 &&
			f.Sequence != nil && *f.Sequence == call.inputSequence
		if ok {
			call.inputAllowance--
			call.inputSequence++
		}
		call.inputMu.Unlock()
		if !ok {
			call.protocolError("Invalid input stream item, sequence, or credit")
			return
		}
		call.armIdle()
		payload := []byte(f.Payload)
		if payload == nil {
			payload = []byte("null")
		}
		call.pushInput(payload, nil)
	case "stream.end":
		call.inputMu.Lock()
		ok := call.method.input != nil && f.Direction == "client" && !call.inputClosed
		call.inputClosed = true
		if call.idle != nil {
			call.idle.Stop()
		}
		call.inputMu.Unlock()
		if !ok {
			call.protocolError("Invalid input stream end")
			return
		}
		call.pushInput(nil, nil)
	}
}

func (call *serverCall) protocolError(message string) {
	e := &Error{Code: CodeProtocolError, Message: message}
	call.abort(e)
	call.finish(frame{Kind: "call.error", Error: e.payload()}, CodeProtocolError)
}

// armIdle (re)starts the input idle timer (docs/protocol.md → Timeouts).
func (call *serverCall) armIdle() {
	if call.open.IdleTimeout == nil {
		return
	}
	call.inputMu.Lock()
	defer call.inputMu.Unlock()
	if call.idle != nil {
		call.idle.Stop()
	}
	if call.inputClosed {
		return
	}
	call.idle = time.AfterFunc(time.Duration(*call.open.IdleTimeout)*time.Millisecond, func() {
		e := detailed(CodeDeadlineExceeded, "Input stream idle timeout", "server", "idle", false)
		call.abort(e)
		call.finish(frame{Kind: "call.error", Error: e.payload()}, CodeDeadlineExceeded)
	})
}

// inputEvent is one input item, or the end of input (data nil): err nil for
// a normal end, else what ended the call.
type inputEvent struct {
	data []byte
	err  error
}

// pushInput queues an item (data), the end of input (nil, nil) or a failure (nil, err).
func (call *serverCall) pushInput(data []byte, err error) {
	call.inputMu.Lock()
	if data == nil {
		call.inputClosed = true
		if err == nil {
			err = errEndOfInput
		}
	}
	call.input = append(call.input, inputEvent{data: data, err: err})
	close(call.inputWake)
	call.inputWake = make(chan struct{})
	call.inputMu.Unlock()
}

// nextInput returns the next raw input item, io.EOF at the end of input, or
// the error that ended the call. Consuming an item grants one more credit.
func (call *serverCall) nextInput() ([]byte, error) {
	for {
		call.inputMu.Lock()
		if len(call.input) > 0 {
			event := call.input[0]
			if event.data == nil {
				call.inputMu.Unlock() // the end stays queued for later reads
				return nil, event.err
			}
			call.input = call.input[1:]
			closed := call.inputClosed
			call.inputMu.Unlock()
			if !closed {
				call.inputMu.Lock()
				call.inputAllowance++
				call.inputMu.Unlock()
				_ = call.send(frame{Kind: "stream.window", Direction: "client", Credit: int64p(1)})
			}
			return event.data, nil
		}
		wake := call.inputWake
		call.inputMu.Unlock()
		select {
		case <-wake:
		case <-call.ctx.Done():
			return nil, context.Cause(call.ctx)
		}
	}
}

func (call *serverCall) sendOutput(item any) error {
	payload, err := encodeOutput(call.method.schemas.output, item, "invalid_output_item", call.logf)
	if err != nil {
		call.cancel(err)
		return err
	}
	if err := call.credit.acquire(call.ctx); err != nil {
		return err
	}
	call.outputMu.Lock()
	defer call.outputMu.Unlock()
	if call.outputClosed {
		return &Error{Code: CodeProtocolError, Message: "Call output stream is closed"}
	}
	if call.terminal.Load() {
		return &Error{Code: CodeCancelled, Message: "Call is closed"}
	}
	seq := call.outputSequence
	call.outputSequence++
	return call.send(frame{Kind: "stream.item", Direction: "server", Sequence: &seq, Payload: payload})
}

func (call *serverCall) closeOutput() error {
	call.outputMu.Lock()
	defer call.outputMu.Unlock()
	if call.outputClosed || call.terminal.Load() {
		return nil
	}
	call.outputClosed = true
	return call.send(frame{Kind: "stream.end", Direction: "server"})
}
