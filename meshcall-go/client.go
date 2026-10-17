package meshcall

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"reflect"
	"slices"
	"sync"
	"time"
)

// ClientOptions configures a Client.
type ClientOptions struct {
	// Authorization is sent with the WebSocket upgrade ("Basic …", "Bearer mk_…").
	Authorization string
	PeerID        string
	MaxFrameSize  int
	// Keepalive: ping when idle this long (negative disables); close when a
	// pong takes longer than PingTimeout.
	PingInterval time.Duration
	PingTimeout  time.Duration
	// DeadlineGrace is how long past a call's timeout to wait for the peer's
	// own deadline error before failing locally (default max(1s, 10%)).
	DeadlineGrace    time.Duration
	HandshakeTimeout time.Duration
	// Reconnect paces reconnecting after a lost connection and resubscribing.
	Reconnect ReconnectPolicy
	// Retry re-sends unary calls that failed with a retryable error, when the
	// method is idempotent or the call has an idempotency key.
	Retry        *RetryPolicy
	Interceptors []ClientInterceptor
	Logger       *slog.Logger
}

// RetryPolicy is the built-in unary retry (docs/protocol.md → Idempotency keys).
type RetryPolicy struct {
	Disabled     bool
	MaxAttempts  int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
	Jitter       float64
	RetryCodes   []string
}

// DefaultRetry matches the other runtimes.
var DefaultRetry = RetryPolicy{MaxAttempts: 3, InitialDelay: 100 * time.Millisecond, MaxDelay: 2 * time.Second,
	Multiplier: 2, Jitter: 0.2, RetryCodes: []string{CodeUnavailable, CodeResourceExhausted}}

func (p RetryPolicy) delay(attempt int) time.Duration {
	base := min(float64(p.MaxDelay), float64(p.InitialDelay)*pow(p.Multiplier, attempt-1))
	spread := base * p.Jitter
	return time.Duration(max(0, base+(rand.Float64()*2-1)*spread))
}

func pow(x float64, n int) float64 {
	out := 1.0
	for range n {
		out *= x
	}
	return out
}

// CallOptions are one call's options; interceptors may change them.
type CallOptions struct {
	Timeout        time.Duration
	IdleTimeout    time.Duration
	Metadata       map[string]string
	IdempotencyKey string
	Instance       string
	// Idempotent states the method's own semantics; generated clients set it.
	Idempotent bool
}

// CallOption sets a call option.
type CallOption func(*CallOptions)

// WithTimeout bounds the whole call (also enforced by the server and Router).
// The context's deadline applies too; the earlier one wins.
func WithTimeout(d time.Duration) CallOption { return func(o *CallOptions) { o.Timeout = d } }

// WithIdleTimeout bounds the gap between stream items.
func WithIdleTimeout(d time.Duration) CallOption { return func(o *CallOptions) { o.IdleTimeout = d } }

// WithMetadata attaches request metadata (trace context, tenant ids…).
func WithMetadata(md map[string]string) CallOption {
	return func(o *CallOptions) {
		if o.Metadata == nil {
			o.Metadata = map[string]string{}
		}
		for k, v := range md {
			o.Metadata[k] = v
		}
	}
}

// WithIdempotencyKey names the call's intent so retries are safe (unary
// calls are deduplicated by the Router). Not for duplex calls.
func WithIdempotencyKey(key string) CallOption {
	return func(o *CallOptions) { o.IdempotencyKey = key }
}

// WithInstance addresses one service instance; required for targeted services.
func WithInstance(id string) CallOption { return func(o *CallOptions) { o.Instance = id } }

// IdempotentCall marks the method as safe to retry; generated clients pass it.
func IdempotentCall() CallOption { return func(o *CallOptions) { o.Idempotent = true } }

// CallInfo is what a client interceptor sees for one call.
type CallInfo struct {
	Service string
	Method  string
	Stream  string // unary | server_stream | client_stream | duplex
	Options *CallOptions
	// Trailers is set for unary and client-stream calls once next returns.
	Trailers map[string]string
}

// ClientCallNext runs the rest of the chain: the whole call for unary and
// client-stream calls, opening it for streams. It may be called again to retry.
type ClientCallNext func(ctx context.Context, info *CallInfo) error

// ClientInterceptor wraps every outgoing call; the first one is outermost.
type ClientInterceptor func(ctx context.Context, info *CallInfo, next ClientCallNext) error

// Caller is satisfied by both *Router and *Client so generated clients can be
// constructed from either a shared Router or a standalone Client.
// Caller is the transport a generated client speaks through: *Client
// (WebSocket), *Router (its shared WebSocket client) or *HTTPRouter.
//
// The methods are raw JSON so the generic helpers below stay type-safe and
// every transport only implements the wire mapping it supports. Duplex is
// WebSocket-only; HTTPRouter returns a method_not_found error for it.
type Caller interface {
	unaryRaw(ctx context.Context, service, method string, request json.RawMessage, opts []CallOption) (json.RawMessage, map[string]string, error)
	serverStreamRaw(ctx context.Context, service, method string, request json.RawMessage, opts []CallOption) (streamSource, error)
	clientStreamRaw(ctx context.Context, service, method string, request json.RawMessage, items func(yield func(json.RawMessage) bool), opts []CallOption) (json.RawMessage, map[string]string, error)
	duplexRaw(ctx context.Context, service, method string, request json.RawMessage, opts []CallOption) (rawDuplex, error)
	// reconnectDelays drives subscription resume backoff; log receives the
	// resubscribe warning.
	reconnectDelays() func() (time.Duration, bool)
	log() *slog.Logger
	// withOptions backs the generated clients' WithOptions, which must keep
	// the concrete transport type (so the next call still has it).
	WithCallOptions(CallOptions) Caller
}

func (c *Client) reconnectDelays() func() (time.Duration, bool) { return c.opts.Reconnect.delays() }
func (c *Client) log() *slog.Logger                             { return c.logger }
func (c *Client) WithCallOptions(opts CallOptions) Caller       { return c.WithOptions(opts) }

type clientState struct {
	mu            sync.Mutex
	cursor        int
	conn          *conn
	connecting    chan struct{}
	everConnected bool
	closed        bool
	calls         map[string]*clientCall
	onClose       func()
}

// Client connects lazily to a Router or direct server and makes calls.
type Client struct {
	opts        ClientOptions
	endpoints   []endpoint
	logger      *slog.Logger
	defaultOpts []CallOption
	*clientState
}

// NewClient creates a client for one endpoint, or several tried in order
// starting from the last one that worked (failover without Router-side
// coordination). Endpoints are ws://, wss://, unix:///path or pipe://NAME.
func NewClient(opts ClientOptions, endpoints ...string) (*Client, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("meshcall: NewClient needs an endpoint")
	}
	c := &Client{opts: opts, clientState: &clientState{calls: map[string]*clientCall{}}}
	for _, raw := range endpoints {
		e, err := parseEndpoint(raw)
		if err != nil {
			return nil, err
		}
		c.endpoints = append(c.endpoints, e)
	}
	if c.opts.PeerID == "" {
		c.opts.PeerID = randomID()
	}
	if c.opts.MaxFrameSize == 0 {
		c.opts.MaxFrameSize = DefaultMaxFrameSize
	}
	if c.opts.PingInterval == 0 {
		c.opts.PingInterval = DefaultPingInterval
	}
	if c.opts.PingTimeout == 0 {
		c.opts.PingTimeout = DefaultPingTimeout
	}
	if c.opts.HandshakeTimeout == 0 {
		c.opts.HandshakeTimeout = 10 * time.Second
	}
	if c.opts.Retry == nil {
		retry := DefaultRetry
		c.opts.Retry = &retry
	}
	c.logger = opts.Logger
	if c.logger == nil {
		c.logger = slog.Default()
	}
	return c, nil
}

// Client implements Caller.
func (c *Client) Client() *Client { return c }

// WithOptions returns a lightweight shallow copy of Client that shares the
// underlying connection and merges opts into every call made through it.
func (c *Client) WithOptions(opts CallOptions) *Client {
	clone := *c
	merged := make([]CallOption, 0, len(c.defaultOpts)+5)
	merged = append(merged, c.defaultOpts...)
	if opts.Timeout > 0 {
		merged = append(merged, WithTimeout(opts.Timeout))
	}
	if opts.IdleTimeout > 0 {
		merged = append(merged, WithIdleTimeout(opts.IdleTimeout))
	}
	if len(opts.Metadata) > 0 {
		md := make(map[string]string, len(opts.Metadata))
		for k, v := range opts.Metadata {
			md[k] = v
		}
		merged = append(merged, WithMetadata(md))
	}
	if opts.IdempotencyKey != "" {
		merged = append(merged, WithIdempotencyKey(opts.IdempotencyKey))
	}
	if opts.Instance != "" {
		merged = append(merged, WithInstance(opts.Instance))
	}
	if opts.Idempotent {
		merged = append(merged, IdempotentCall())
	}
	clone.defaultOpts = merged
	return &clone
}

// Close closes the connection; calls in flight fail with unavailable.
func (c *Client) Close() error {
	c.mu.Lock()
	c.closed = true
	conn := c.conn
	c.conn = nil
	onClose := c.onClose
	c.onClose = nil
	c.mu.Unlock()
	if conn != nil {
		conn.close()
	}
	c.failAll(&Error{Code: CodeUnavailable, Message: "Client is closed"})
	if onClose != nil {
		onClose()
	}
	return nil
}

// connection returns the live connection, connecting if needed. After a
// loss it retries with backoff; ctx bounds the wait.
func (c *Client) connection(ctx context.Context) (*conn, error) {
	for {
		c.mu.Lock()
		if c.conn != nil {
			conn := c.conn
			c.mu.Unlock()
			return conn, nil
		}
		if c.connecting != nil {
			wait := c.connecting
			c.mu.Unlock()
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return nil, contextError(ctx)
			}
		}
		c.closed = false
		c.connecting = make(chan struct{})
		retry := c.everConnected
		c.mu.Unlock()

		conn, err := c.connectWithBackoff(ctx, retry)
		c.mu.Lock()
		close(c.connecting)
		c.connecting = nil
		if err == nil {
			c.conn = conn
			c.everConnected = true
		}
		c.mu.Unlock()
		if err != nil {
			return nil, err
		}
		go c.readLoop(conn)
		go conn.keepAlive(c.opts.PingInterval, c.opts.PingTimeout)
		return conn, nil
	}
}

func (c *Client) connectWithBackoff(ctx context.Context, retry bool) (*conn, error) {
	next := c.opts.Reconnect.delays()
	for {
		conn, err := c.connectAny(ctx)
		if err == nil {
			return conn, nil
		}
		delay, ok := next()
		if !retry || !ok {
			if e, isMesh := AsError(err); isMesh {
				return nil, e
			}
			return nil, &Error{Code: CodeUnavailable, Message: "Could not connect: " + err.Error(), Retryable: true}
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, contextError(ctx)
		}
	}
}

// connectAny makes one pass over the endpoints, starting from the last good one.
func (c *Client) connectAny(ctx context.Context) (*conn, error) {
	var last error
	for offset := range c.endpoints {
		c.mu.Lock()
		index := (c.cursor + offset) % len(c.endpoints)
		c.mu.Unlock()
		conn, err := c.handshake(ctx, c.endpoints[index])
		if err == nil {
			c.mu.Lock()
			c.cursor = index
			c.mu.Unlock()
			return conn, nil
		}
		last = err
	}
	return nil, last
}

func (c *Client) handshake(ctx context.Context, e endpoint) (*conn, error) {
	ctx, cancel := context.WithTimeout(ctx, c.opts.HandshakeTimeout)
	defer cancel()
	header := http.Header{}
	if c.opts.Authorization != "" {
		header.Set("Authorization", c.opts.Authorization)
	}
	conn, err := e.connect(ctx, header, c.opts.MaxFrameSize)
	if err != nil {
		return nil, err
	}
	if err := conn.send(frame{Kind: "hello", Protocol: Protocol, Role: "client", PeerID: c.opts.PeerID}); err != nil {
		conn.close()
		return nil, err
	}
	ack, err := receiveWithin(conn, c.opts.HandshakeTimeout)
	if err != nil || ack.Kind != "hello.ack" || ack.Protocol != Protocol {
		conn.close()
		return nil, &Error{Code: CodeProtocolError, Message: "Expected a compatible hello.ack"}
	}
	return conn, nil
}

func (c *Client) readLoop(conn *conn) {
	for {
		f, err := conn.receive()
		if err != nil {
			if conn.ctx.Err() == nil {
				continue // a malformed frame
			}
			break
		}
		c.mu.Lock()
		call := c.calls[f.CallID]
		c.mu.Unlock()
		if call != nil {
			call.deliver(f)
		}
	}
	c.mu.Lock()
	if c.conn == conn {
		c.conn = nil
		// The next connect starts from the following endpoint: the one that
		// just died is the least likely to come back first.
		if len(c.endpoints) > 1 {
			c.cursor = (c.cursor + 1) % len(c.endpoints)
		}
	}
	c.mu.Unlock()
	c.failAll(&Error{Code: CodeUnavailable, Message: "Connection was lost", Retryable: true}, conn)
}

func (c *Client) failAll(e *Error, only ...*conn) {
	c.mu.Lock()
	var calls []*clientCall
	for _, call := range c.calls {
		if len(only) == 0 || call.conn == only[0] {
			calls = append(calls, call)
		}
	}
	c.mu.Unlock()
	for _, call := range calls {
		call.fail(e)
	}
}

func contextError(ctx context.Context) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return detailed(CodeDeadlineExceeded, "Call deadline exceeded", "client", "", false)
	}
	return &Error{Code: CodeCancelled, Message: "Call was cancelled"}
}

// --- one call --------------------------------------------------------------

type clientCall struct {
	client *Client
	conn   *conn
	id     string
	stream string

	mu       sync.Mutex
	queue    []frame
	wake     chan struct{}
	failure  error
	terminal bool
	trailers map[string]string

	inputCredit *creditWindow
	inputSeq    int64
	inputClosed bool

	outputAllowance int64
	expectedSeq     int64
	outputEnded     bool
	itemSchema      *orderedObject

	timer     *time.Timer
	idle      time.Duration
	idleTimer *time.Timer
	stop      func() bool
}

func (c *Client) open(ctx context.Context, info *CallInfo, payload json.RawMessage) (*clientCall, error) {
	o := info.Options
	timeout := o.Timeout
	if deadline, ok := ctx.Deadline(); ok {
		if until := time.Until(deadline); timeout == 0 || until < timeout {
			timeout = max(time.Millisecond, until)
		}
	}
	started := time.Now()
	connectCtx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		connectCtx, cancel = context.WithTimeout(ctx, timeout+c.grace(timeout))
		defer cancel()
	}
	conn, err := c.connection(connectCtx)
	if err != nil {
		return nil, err
	}
	call := &clientCall{client: c, conn: conn, id: randomID(), stream: info.Stream, wake: make(chan struct{}), idle: o.IdleTimeout}
	if info.Stream == "client_stream" || info.Stream == "duplex" {
		call.inputCredit = newCreditWindow()
	}
	open := frame{Kind: "call.open", CallID: call.id, Service: info.Service, Method: info.Method, Payload: payload,
		Idempotency: o.IdempotencyKey, InstanceID: o.Instance}
	if len(o.Metadata) > 0 {
		open.Metadata = o.Metadata
	}
	if timeout > 0 {
		open.TimeoutMS = int64p(max(1, timeout.Milliseconds()))
		budget := timeout + c.grace(timeout) - time.Since(started)
		call.timer = time.AfterFunc(max(0, budget), func() {
			call.abort(detailed(CodeDeadlineExceeded, "Call deadline exceeded", "client", "", false), "deadline_exceeded")
		})
	}
	if o.IdleTimeout > 0 {
		open.IdleTimeout = int64p(max(1, o.IdleTimeout.Milliseconds()))
	}
	c.mu.Lock()
	c.calls[call.id] = call
	c.mu.Unlock()
	call.stop = context.AfterFunc(ctx, func() {
		reason := "caller_cancelled"
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			reason = "deadline_exceeded"
		}
		call.abort(contextError(ctx), reason)
	})
	if err := conn.send(open); err != nil {
		if e := frameTooLarge("client", err); e != nil {
			call.fail(e)
		} else {
			call.fail(&Error{Code: CodeUnavailable, Message: "Connection was lost", Retryable: true})
		}
		return nil, call.failure
	}
	if info.Stream == "server_stream" || info.Stream == "duplex" {
		call.outputAllowance = initialStreamCredit
		_ = conn.send(frame{Kind: "stream.window", CallID: call.id, Direction: "server", Credit: int64p(initialStreamCredit)})
		call.armIdle()
	}
	return call, nil
}

func (c *Client) grace(timeout time.Duration) time.Duration {
	if c.opts.DeadlineGrace > 0 {
		return c.opts.DeadlineGrace
	}
	return max(time.Second, timeout/10)
}

func (call *clientCall) deliver(f frame) {
	switch f.Kind {
	case "stream.window":
		if f.Direction != "client" || call.inputCredit == nil || f.Credit == nil || *f.Credit <= 0 {
			call.fail(&Error{Code: CodeProtocolError, Message: "Invalid input stream window"})
			return
		}
		call.inputCredit.grant(*f.Credit)
		return
	case "stream.item":
		call.mu.Lock()
		ok := f.Direction == "server" && !call.outputEnded && call.outputAllowance > 0 && f.Sequence != nil && *f.Sequence == call.expectedSeq
		if ok {
			call.outputAllowance--
			call.expectedSeq++
		}
		call.mu.Unlock()
		if !ok {
			call.fail(&Error{Code: CodeProtocolError, Message: "Invalid output stream item, sequence, or credit"})
			return
		}
		call.armIdle()
	case "stream.end":
		call.mu.Lock()
		call.outputEnded = true
		if call.idleTimer != nil {
			call.idleTimer.Stop()
		}
		call.mu.Unlock()
		return // the consumer sees the end with the terminal frame
	case "call.result", "call.error":
		call.mu.Lock()
		call.terminal = true
		call.trailers = f.Metadata
		call.mu.Unlock()
		call.done()
	}
	call.mu.Lock()
	call.queue = append(call.queue, f)
	close(call.wake)
	call.wake = make(chan struct{})
	call.mu.Unlock()
}

// next returns the next item or terminal frame for this call.
func (call *clientCall) next() (frame, error) {
	for {
		call.mu.Lock()
		if len(call.queue) > 0 {
			f := call.queue[0]
			call.queue = call.queue[1:]
			call.mu.Unlock()
			return f, nil
		}
		if call.failure != nil {
			err := call.failure
			call.mu.Unlock()
			return frame{}, err
		}
		wake := call.wake
		call.mu.Unlock()
		<-wake
	}
}

func (call *clientCall) fail(err error) {
	call.mu.Lock()
	if call.failure != nil || (call.terminal && len(call.queue) > 0) {
		call.mu.Unlock()
		return
	}
	call.failure = err
	call.terminal = true
	close(call.wake)
	call.wake = make(chan struct{})
	call.mu.Unlock()
	call.done()
}

// abort fails the call locally and tells the peer.
func (call *clientCall) abort(err error, reason string) {
	call.mu.Lock()
	finished := call.terminal
	call.mu.Unlock()
	if finished {
		return
	}
	call.fail(err)
	_ = call.conn.send(frame{Kind: "call.cancel", CallID: call.id, Reason: reason})
}

func (call *clientCall) done() {
	if call.timer != nil {
		call.timer.Stop()
	}
	call.mu.Lock()
	if call.idleTimer != nil {
		call.idleTimer.Stop()
	}
	call.mu.Unlock()
	if call.stop != nil {
		call.stop()
	}
	if call.inputCredit != nil {
		call.inputCredit.close(&Error{Code: CodeCancelled, Message: "Call is closed"})
	}
	call.client.mu.Lock()
	if call.client.calls[call.id] == call {
		delete(call.client.calls, call.id)
	}
	call.client.mu.Unlock()
}

func (call *clientCall) armIdle() {
	if call.idle <= 0 {
		return
	}
	call.mu.Lock()
	defer call.mu.Unlock()
	if call.idleTimer != nil {
		call.idleTimer.Stop()
	}
	if call.outputEnded || call.terminal {
		return
	}
	call.idleTimer = time.AfterFunc(call.idle, func() {
		call.abort(detailed(CodeDeadlineExceeded, "Output stream idle timeout", "client", "idle", false), "deadline_exceeded")
	})
}

// consumed grants the server one more output credit.
func (call *clientCall) consumed() {
	call.mu.Lock()
	open := !call.outputEnded && !call.terminal
	if open {
		call.outputAllowance++
	}
	call.mu.Unlock()
	if open {
		_ = call.conn.send(frame{Kind: "stream.window", CallID: call.id, Direction: "server", Credit: int64p(1)})
	}
}

func (call *clientCall) sendInput(ctx context.Context, payload json.RawMessage) error {
	call.mu.Lock()
	closed := call.inputClosed
	call.mu.Unlock()
	if closed {
		return &Error{Code: CodeProtocolError, Message: "Input stream is closed"}
	}
	if err := call.inputCredit.acquire(ctx); err != nil {
		call.mu.Lock()
		failure := call.failure
		call.mu.Unlock()
		if failure != nil {
			return failure
		}
		return err
	}
	call.mu.Lock()
	seq := call.inputSeq
	call.inputSeq++
	call.mu.Unlock()
	err := call.conn.send(frame{Kind: "stream.item", CallID: call.id, Direction: "client", Sequence: &seq, Payload: payload})
	if e := frameTooLarge("client", err); e != nil {
		call.abort(e, "frame_too_large") // the call is open at the peer
		return e
	}
	return err
}

func (call *clientCall) closeInput() error {
	call.mu.Lock()
	closed := call.inputClosed
	call.inputClosed = true
	call.mu.Unlock()
	if closed {
		return nil
	}
	return call.conn.send(frame{Kind: "stream.end", CallID: call.id, Direction: "client"})
}

// result waits for the terminal frame of a unary or client-stream call.
func (call *clientCall) result() (json.RawMessage, map[string]string, error) {
	for {
		f, err := call.next()
		if err != nil {
			return nil, nil, err
		}
		switch f.Kind {
		case "call.result":
			return f.Payload, f.Metadata, nil
		case "call.error":
			return nil, f.Metadata, errorFromPayload(f.Error)
		}
	}
}

// --- typed calls -----------------------------------------------------------

func (c *Client) buildInfo(service, method, stream string, opts []CallOption) *CallInfo {
	o := &CallOptions{}
	for _, opt := range c.defaultOpts {
		opt(o)
	}
	for _, opt := range opts {
		opt(o)
	}
	return &CallInfo{Service: service, Method: method, Stream: stream, Options: o}
}

func (c *Client) runChain(ctx context.Context, info *CallInfo, innermost ClientCallNext) error {
	next := innermost
	for i := len(c.opts.Interceptors) - 1; i >= 0; i-- {
		interceptor, inner := c.opts.Interceptors[i], next
		next = func(ctx context.Context, info *CallInfo) error { return interceptor(ctx, info, inner) }
	}
	if info.Stream == "unary" && !c.opts.Retry.Disabled {
		inner := next
		next = func(ctx context.Context, info *CallInfo) error { return c.retry(ctx, info, inner) }
	}
	return next(ctx, info)
}

func (c *Client) retry(ctx context.Context, info *CallInfo, next ClientCallNext) error {
	policy := *c.opts.Retry
	if policy.MaxAttempts == 0 {
		policy.MaxAttempts = DefaultRetry.MaxAttempts
	}
	if policy.InitialDelay == 0 {
		policy.InitialDelay, policy.MaxDelay, policy.Multiplier, policy.Jitter = DefaultRetry.InitialDelay, DefaultRetry.MaxDelay, DefaultRetry.Multiplier, DefaultRetry.Jitter
	}
	if policy.RetryCodes == nil {
		policy.RetryCodes = DefaultRetry.RetryCodes
	}
	mayRetry := info.Options.Idempotent || info.Options.IdempotencyKey != ""
	var deadline time.Time
	if info.Options.Timeout > 0 {
		deadline = time.Now().Add(info.Options.Timeout)
	}
	for attempt := 1; ; attempt++ {
		err := next(ctx, info)
		e, ok := AsError(err)
		if err == nil || !ok || !mayRetry || !e.Retryable || !slices.Contains(policy.RetryCodes, e.Code) || attempt >= policy.MaxAttempts {
			if ok && attempt > 1 {
				details := map[string]any{}
				for k, v := range e.Details {
					details[k] = v
				}
				details["attempts"] = attempt
				return &Error{Code: e.Code, Message: e.Message, Retryable: e.Retryable, Details: details}
			}
			return err
		}
		delay := policy.delay(attempt)
		if !deadline.IsZero() {
			remaining := time.Until(deadline)
			if remaining <= delay {
				return err
			}
			info.Options.Timeout = remaining - delay
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return contextError(ctx)
		}
	}
}

// decodeReceived checks what the server sent against this client's types:
// a mismatch is version skew between the two (protocol_error from the client).
func decodeReceived[T any](raw json.RawMessage, reason string) (T, error) {
	var value T
	if len(raw) == 0 {
		raw = json.RawMessage("null")
	}
	if vs := validate(schemaOf(reflect.TypeFor[T]()), raw); len(vs) > 0 {
		message := "Invalid response"
		if reason == "invalid_output_item" {
			message = "Invalid output stream item"
		}
		e := detailed(CodeProtocolError, message, "client", reason, false)
		e.Details["violations"] = violationList(vs)
		return value, e
	}
	if err := json.Unmarshal(raw, &value); err != nil {
		return value, detailed(CodeProtocolError, "Invalid response", "client", reason, false)
	}
	return value, nil
}

// Unary makes a request–response call.
func Unary[Req, Resp any](ctx context.Context, c Caller, service, method string, req Req, opts ...CallOption) (Resp, error) {
	var resp Resp
	payload, err := json.Marshal(req)
	if err != nil {
		return resp, err
	}
	raw, _, err := c.unaryRaw(ctx, service, method, payload, opts)
	if err != nil {
		return resp, err
	}
	resp, err = decodeReceived[Resp](raw, "invalid_response")
	return resp, err
}

// unaryRaw is *Client's WebSocket implementation of Caller.
func (c *Client) unaryRaw(ctx context.Context, service, method string, payload json.RawMessage, opts []CallOption) (json.RawMessage, map[string]string, error) {
	var result json.RawMessage
	var trailers map[string]string
	info := c.buildInfo(service, method, "unary", opts)
	err := c.runChain(ctx, info, func(ctx context.Context, info *CallInfo) error {
		call, err := c.open(ctx, info, payload)
		if err != nil {
			return err
		}
		raw, terminal, err := call.result()
		info.Trailers = terminal
		trailers = terminal
		result = raw
		return err
	})
	return result, trailers, err
}

// ClientStream sends items and returns the server's single answer.
func ClientStream[Req, Item, Resp any](ctx context.Context, c Caller, service, method string, req Req, items func(yield func(Item) bool), opts ...CallOption) (Resp, error) {
	var resp Resp
	payload, err := json.Marshal(req)
	if err != nil {
		return resp, err
	}
	raw, _, err := c.clientStreamRaw(ctx, service, method, payload, func(yield func(json.RawMessage) bool) {
		for item := range items {
			encoded, err := json.Marshal(item)
			if err != nil || !yield(encoded) {
				return
			}
		}
	}, opts)
	if err != nil {
		return resp, err
	}
	resp, err = decodeReceived[Resp](raw, "invalid_response")
	return resp, err
}

// clientStreamRaw is *Client's WebSocket implementation of Caller.
func (c *Client) clientStreamRaw(ctx context.Context, service, method string, payload json.RawMessage, items func(yield func(json.RawMessage) bool), opts []CallOption) (json.RawMessage, map[string]string, error) {
	var result json.RawMessage
	var trailers map[string]string
	info := c.buildInfo(service, method, "client_stream", opts)
	err := c.runChain(ctx, info, func(ctx context.Context, info *CallInfo) error {
		call, err := c.open(ctx, info, payload)
		if err != nil {
			return err
		}
		pumpCtx, stopPump := context.WithCancel(ctx)
		defer stopPump()
		go func() {
			for raw := range items {
				if call.sendInput(pumpCtx, raw) != nil {
					return
				}
			}
			_ = call.closeInput()
		}()
		raw, terminal, err := call.result()
		info.Trailers = terminal
		trailers = terminal
		result = raw
		return err
	})
	return result, trailers, err
}

// ClientStreamSeq sends items from an iter.Seq2[Item, error] and returns the server's single answer.
func ClientStreamSeq[Req, Item, Resp any](ctx context.Context, c Caller, service, method string, req Req, items iter.Seq2[Item, error], opts ...CallOption) (Resp, error) {
	var resp Resp
	payload, err := json.Marshal(req)
	if err != nil {
		return resp, err
	}
	var producerErr error
	raw, _, err := c.clientStreamRaw(ctx, service, method, payload, func(yield func(json.RawMessage) bool) {
		for item, itemErr := range items {
			if itemErr != nil {
				producerErr = itemErr
				return
			}
			encoded, err := json.Marshal(item)
			if err != nil {
				producerErr = err
				return
			}
			if !yield(encoded) {
				return
			}
		}
	}, opts)
	if err != nil {
		return resp, err
	}
	if producerErr != nil {
		return resp, producerErr
	}
	resp, err = decodeReceived[Resp](raw, "invalid_response")
	return resp, err
}

// streamSource is what Stream needs from an open stream: the WebSocket
// clientCall and the HTTP NDJSON reader both implement it.
type streamSource interface {
	next() (frame, error)
	consumed()
	abort(err error, reason string)
}

// rawDuplex is an open duplex call; WebSocket only.
type rawDuplex interface {
	streamSource
	send(item json.RawMessage) error
	closeSend() error
}

// Stream is a stream of items from the server.
type Stream[Item any] struct {
	source   streamSource
	done     bool
	trailers map[string]string
}

// Recv returns the next item, io.EOF when the stream ends normally, or the
// call's error.
func (s *Stream[Item]) Recv() (Item, error) {
	var zero Item
	if s.done {
		return zero, io.EOF
	}
	for {
		f, err := s.source.next()
		if err != nil {
			s.done = true
			return zero, err
		}
		switch f.Kind {
		case "stream.item":
			item, err := decodeReceived[Item](f.Payload, "invalid_output_item")
			if err != nil {
				s.source.abort(err, "invalid_output_item")
				s.done = true
				return zero, err
			}
			s.source.consumed()
			return item, nil
		case "call.result":
			s.done, s.trailers = true, f.Metadata
			return zero, io.EOF
		case "call.error":
			s.done, s.trailers = true, f.Metadata
			return zero, errorFromPayload(f.Error)
		}
	}
}

// All ranges over the stream: for item, err := range stream.All() { … }.
func (s *Stream[Item]) All() func(yield func(Item, error) bool) {
	return func(yield func(Item, error) bool) {
		for {
			item, err := s.Recv()
			if err == io.EOF {
				return
			}
			if !yield(item, err) || err != nil {
				if err == nil {
					s.Close()
				}
				return
			}
		}
	}
}

// Trailers are the server's trailers once the stream has ended.
func (s *Stream[Item]) Trailers() map[string]string { return s.trailers }

// Close cancels the stream if it is still running.
func (s *Stream[Item]) Close() {
	if !s.done {
		s.done = true
		s.source.abort(&Error{Code: CodeCancelled, Message: "Call was cancelled"}, "client_closed")
	}
}

func openStream[Item any](ctx context.Context, c *Client, service, method, shape string, payload json.RawMessage, opts []CallOption) (*clientCall, error) {
	info := c.buildInfo(service, method, shape, opts)
	var call *clientCall
	err := c.runChain(ctx, info, func(ctx context.Context, info *CallInfo) error {
		var err error
		call, err = c.open(ctx, info, payload)
		return err
	})
	return call, err
}

// ServerStream opens a call that streams items back.
func ServerStream[Req, Item any](ctx context.Context, c Caller, service, method string, req Req, opts ...CallOption) (*Stream[Item], error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	source, err := c.serverStreamRaw(ctx, service, method, payload, opts)
	if err != nil {
		return nil, err
	}
	return &Stream[Item]{source: source}, nil
}

// serverStreamRaw is *Client's WebSocket implementation of Caller.
func (c *Client) serverStreamRaw(ctx context.Context, service, method string, payload json.RawMessage, opts []CallOption) (streamSource, error) {
	info := c.buildInfo(service, method, "server_stream", opts)
	var call *clientCall
	err := c.runChain(ctx, info, func(ctx context.Context, info *CallInfo) error {
		var err error
		call, err = c.open(ctx, info, payload)
		return err
	})
	if err != nil {
		return nil, err
	}
	return call, nil
}

// ServerStreamSeq returns an iter.Seq2[Item, error] that opens a server-stream call
// on first iteration and yields items until io.EOF or error.
func ServerStreamSeq[Req, Item any](ctx context.Context, c Caller, service, method string, req Req, opts ...CallOption) iter.Seq2[Item, error] {
	return func(yield func(Item, error) bool) {
		var zero Item
		stream, err := ServerStream[Req, Item](ctx, c, service, method, req, opts...)
		if err != nil {
			yield(zero, err)
			return
		}
		defer stream.Close()
		for {
			item, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return
			}
			if !yield(item, err) || err != nil {
				return
			}
		}
	}
}

// DuplexCall is an open duplex call: Send input items while Recv-ing output.
// Read output concurrently with sending: a caller that only sends stalls
// once the server's output credit runs out.
type DuplexCall[In, Out any] struct {
	Stream[Out]
	raw rawDuplex
}

// Send sends one input item, waiting for the server's credit.
func (d *DuplexCall[In, Out]) Send(item In) error {
	raw, err := json.Marshal(item)
	if err != nil {
		return err
	}
	return d.raw.send(raw)
}

// CloseSend half-closes the input.
func (d *DuplexCall[In, Out]) CloseSend() error { return d.raw.closeSend() }

// clientRawDuplex adapts an open WebSocket call to rawDuplex.
type clientRawDuplex struct {
	call *clientCall
	ctx  context.Context
}

func (d *clientRawDuplex) next() (frame, error)            { return d.call.next() }
func (d *clientRawDuplex) consumed()                       { d.call.consumed() }
func (d *clientRawDuplex) abort(err error, reason string)  { d.call.abort(err, reason) }
func (d *clientRawDuplex) send(item json.RawMessage) error { return d.call.sendInput(d.ctx, item) }
func (d *clientRawDuplex) closeSend() error                { return d.call.closeInput() }

// duplexRaw is *Client's WebSocket implementation of Caller.
func (c *Client) duplexRaw(ctx context.Context, service, method string, payload json.RawMessage, opts []CallOption) (rawDuplex, error) {
	info := c.buildInfo(service, method, "duplex", opts)
	var call *clientCall
	err := c.runChain(ctx, info, func(ctx context.Context, info *CallInfo) error {
		var err error
		call, err = c.open(ctx, info, payload)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &clientRawDuplex{call: call, ctx: ctx}, nil
}

// Duplex opens a duplex call (docs/protocol.md → Duplex calls). It is never
// retried and takes no idempotency key; HTTP transports refuse it.
func Duplex[Req, In, Out any](ctx context.Context, c Caller, service, method string, req Req, opts ...CallOption) (*DuplexCall[In, Out], error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	raw, err := c.duplexRaw(ctx, service, method, payload, opts)
	if err != nil {
		return nil, err
	}
	return &DuplexCall[In, Out]{Stream: Stream[Out]{source: raw}, raw: raw}, nil
}

// Subscription is a server stream that is reopened after retryable failures,
// resuming after the last item's cursor (docs/protocol.md → Subscriptions).
type Subscription[Item any] struct {
	ctx     context.Context
	caller  Caller
	service string
	method  string
	request map[string]any
	spec    SubscriptionSpec
	opts    []CallOption
	stream  *Stream[Item]
	last    any
	seen    bool
	next    func() (time.Duration, bool)
	closed  bool
}

// Subscribe opens a subscription. Pass the method's cursor and resume fields
// (generated clients do); an empty SubscriptionSpec resumes from "now".
func Subscribe[Req, Item any](ctx context.Context, c Caller, service, method string, req Req, spec SubscriptionSpec, opts ...CallOption) (*Subscription[Item], error) {
	raw, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	request := map[string]any{}
	if err := json.Unmarshal(raw, &request); err != nil {
		return nil, fmt.Errorf("meshcall: a subscription request must encode as a JSON object: %w", err)
	}
	return &Subscription[Item]{ctx: ctx, caller: c, service: service, method: method, request: request, spec: spec, opts: opts}, nil
}

// Recv returns the next item, io.EOF once the server ends the stream, or the
// error that ended the subscription (non-retryable, cancelled, or reconnect
// attempts exhausted).
func (s *Subscription[Item]) Recv() (Item, error) {
	var zero Item
	for {
		if s.closed {
			return zero, io.EOF
		}
		if s.stream == nil {
			request := s.request
			if s.spec.Resume != "" && s.seen {
				request = map[string]any{}
				for k, v := range s.request {
					request[k] = v
				}
				request[s.spec.Resume] = s.last
			}
			payload, _ := json.Marshal(request)
			source, err := s.caller.serverStreamRaw(s.ctx, s.service, s.method, payload, s.opts)
			if err != nil {
				if !s.resumable(err) {
					return zero, err
				}
				if err := s.backoff(err); err != nil {
					return zero, err
				}
				continue
			}
			s.stream = &Stream[Item]{source: source}
		}
		item, err := s.stream.Recv()
		if err == io.EOF {
			s.closed = true
			return zero, io.EOF
		}
		if err != nil {
			s.stream = nil
			if !s.resumable(err) {
				s.closed = true
				return zero, err
			}
			if err := s.backoff(err); err != nil {
				return zero, err
			}
			continue
		}
		s.next = nil // an item arrived: the next failure starts the backoff over
		if s.spec.Cursor != "" {
			raw, _ := json.Marshal(item)
			var fields map[string]any
			if json.Unmarshal(raw, &fields) == nil {
				s.last, s.seen = fields[s.spec.Cursor], true
			}
		}
		return item, nil
	}
}

func (s *Subscription[Item]) resumable(err error) bool {
	e, ok := AsError(err)
	return ok && e.Retryable && (e.Code == CodeUnavailable || e.Code == CodeResourceExhausted) && s.ctx.Err() == nil
}

func (s *Subscription[Item]) backoff(err error) error {
	if s.next == nil {
		s.next = s.caller.reconnectDelays()
	}
	delay, ok := s.next()
	if !ok {
		s.closed = true
		return err
	}
	s.caller.log().Warn("Subscription interrupted; resubscribing", "service", s.service, "method", s.method, "error", err, "delay", delay)
	select {
	case <-time.After(delay):
		return nil
	case <-s.ctx.Done():
		s.closed = true
		return contextError(s.ctx)
	}
}

// All ranges over the subscription.
func (s *Subscription[Item]) All() func(yield func(Item, error) bool) {
	return func(yield func(Item, error) bool) {
		for {
			item, err := s.Recv()
			if err == io.EOF {
				return
			}
			if !yield(item, err) || err != nil {
				if err == nil {
					s.Close()
				}
				return
			}
		}
	}
}

// Close ends the subscription.
func (s *Subscription[Item]) Close() {
	s.closed = true
	if s.stream != nil {
		s.stream.Close()
	}
}
