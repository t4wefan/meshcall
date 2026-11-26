package meshcall

// A Mesh over several independent Routers (docs/protocol.md → Several
// Routers): one connection per Router, opened when first needed; each call
// goes to the Router where its instance was last reached, or to the Routers
// in turn, moving on only when a Router did not route the call.

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
	"slices"
	"strings"
	"sync"
	"time"
)

// notRouted is what a Router answers for a call it did not route, the answer
// worth reporting first when none routed it: a draining Router may take the
// call again soon, and one that serves the service but not the instance says
// more than one that lacks the service.
var notRouted = []string{"router_draining", "instance_unknown", "no_instances"}

// routerDidNotRoute reports whether the Router answered without routing the
// call, so it reached no handler and another Router may take it.
func routerDidNotRoute(err error) bool {
	e, ok := AsError(err)
	return ok && e.Details["source"] == "router" && slices.Contains(notRouted, e.Reason())
}

func notRoutedRank(err error) int {
	e, _ := AsError(err)
	return slices.Index(notRouted, e.Reason())
}

type routerSetState struct {
	mu      sync.Mutex
	clients []*Client
	// located maps an instance id to the Router a call to it last succeeded on.
	located map[string]int
	// cursor is the Router that served the last call; tried first.
	cursor        int
	everConnected bool
	closed        bool
}

// routerSet is a Caller over several Routers that know nothing of each other.
// Each open(router) client must try only that Router, must not wait between
// connection attempts and must not retry calls: the set moves on to the next
// Router instead, backs off (reconnect) only once none could be reached, and
// retries unary calls (retry) across all of them.
type routerSet struct {
	routers   []string
	open      func(router string) (*Client, error)
	reconnect ReconnectPolicy
	retry     RetryPolicy
	defaults  []CallOption
	*routerSetState
}

func newRouterSet(routers []string, open func(string) (*Client, error), reconnect ReconnectPolicy, retry RetryPolicy) *routerSet {
	return &routerSet{
		routers: routers, open: open, reconnect: reconnect, retry: retry,
		routerSetState: &routerSetState{clients: make([]*Client, len(routers)), located: map[string]int{}},
	}
}

// WithCallOptions implements Caller.
func (s *routerSet) WithCallOptions(opts CallOptions) Caller { return s.withOptions(opts) }

func (s *routerSet) withOptions(opts CallOptions) *routerSet {
	clone := *s
	clone.defaults = append(append([]CallOption{}, s.defaults...), opts.asOptions()...)
	return &clone
}

// at returns the client of one of the Routers, opened when first needed.
func (s *routerSet) at(router string) (*Client, error) {
	index := slices.Index(s.routers, router)
	if index < 0 {
		return nil, errors.New("meshcall: " + router + " is not one of this Mesh's Routers: " + strings.Join(s.routers, ", "))
	}
	return s.client(index)
}

func (s *routerSet) client(index int) (*Client, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, detailed(CodeUnavailable, "Mesh is closed", "client", "", false)
	}
	if s.clients[index] == nil {
		c, err := s.open(s.routers[index])
		if err != nil {
			return nil, err
		}
		s.clients[index] = c
	}
	return s.clients[index], nil
}

// connect connects to one Router, the first that answers.
func (s *routerSet) connect(ctx context.Context) error {
	return s.route(ctx, s.resolve(nil), func(context.Context, *Client, []CallOption, string) error { return nil })
}

// Close closes every Router's connection.
func (s *routerSet) Close() error {
	s.mu.Lock()
	s.closed = true
	clients := s.clients
	s.clients = make([]*Client, len(s.routers))
	clear(s.located)
	s.mu.Unlock()
	var first error
	for _, c := range clients {
		if c != nil {
			if err := c.Close(); err != nil && first == nil {
				first = err
			}
		}
	}
	return first
}

func (s *routerSet) resolve(opts []CallOption) CallOptions {
	var o CallOptions
	for _, opt := range s.defaults {
		opt(&o)
	}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

func (s *routerSet) prepare(service string, opts []CallOption) (CallOptions, error) {
	if strings.HasPrefix(service, routerServicePrefix) {
		return CallOptions{}, errors.New("meshcall: " + service + " belongs to one Router and this Mesh has several: pick it with mesh.At(router) (" + strings.Join(s.routers, ", ") + ")")
	}
	return s.resolve(opts), nil
}

// order is where the instance was last reached, then the Routers from the
// last good one on.
func (s *routerSet) order(instance string) []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := len(s.routers)
	order := make([]int, 0, count)
	located, ok := s.located[instance]
	if instance != "" && ok {
		order = append(order, located)
	}
	for i := range count {
		index := (s.cursor + i) % count
		if !ok || index != located {
			order = append(order, index)
		}
	}
	return order
}

// route runs attempt on each Router in turn until one routes the call. A
// Router that cannot be reached, or answers that it did not route the call,
// is skipped; any other outcome is the call's. When no Router routed it and
// one could not be reached, it waits per the reconnect policy and goes round
// again within the call's timeout.
func (s *routerSet) route(ctx context.Context, o CallOptions, attempt func(ctx context.Context, c *Client, opts []CallOption, router string) error) error {
	var deadline time.Time
	if o.Timeout > 0 {
		deadline = time.Now().Add(o.Timeout)
	}
	s.mu.Lock()
	retry := s.everConnected
	s.mu.Unlock()
	delays := s.reconnect.delays()
	for {
		var unreachable, refused error
		for _, index := range s.order(o.Instance) {
			call := o
			connectCtx, cancel := ctx, context.CancelFunc(func() {})
			if !deadline.IsZero() {
				left := time.Until(deadline)
				if left <= 0 {
					return detailed(CodeDeadlineExceeded, "Call deadline exceeded", "client", "", false)
				}
				call.Timeout = left
				connectCtx, cancel = context.WithDeadline(ctx, deadline)
			}
			c, err := s.client(index)
			if err == nil {
				_, err = c.connection(connectCtx)
			}
			cancel()
			if err != nil {
				var kind *CredentialSourceError
				if errors.As(err, &kind) || ctx.Err() != nil {
					return err
				}
				if !deadline.IsZero() && time.Until(deadline) <= 0 {
					return detailed(CodeDeadlineExceeded, "Call deadline exceeded", "client", "", false)
				}
				if _, ok := AsError(err); !ok {
					return err
				}
				if unreachable == nil {
					unreachable = err
				}
				continue
			}
			s.mu.Lock()
			s.everConnected = true
			s.mu.Unlock()
			err = attempt(ctx, c, call.asOptions(), s.routers[index])
			if err == nil {
				s.mu.Lock()
				s.cursor = index
				if o.Instance != "" {
					s.located[o.Instance] = index
				}
				s.mu.Unlock()
				return nil
			}
			// Lost, draining or gone from there: find it again next time.
			if e, ok := AsError(err); ok && o.Instance != "" && e.Code == CodeUnavailable {
				s.mu.Lock()
				if s.located[o.Instance] == index {
					delete(s.located, o.Instance)
				}
				s.mu.Unlock()
			}
			if !routerDidNotRoute(err) {
				return err
			}
			if refused == nil || notRoutedRank(err) < notRoutedRank(refused) {
				refused = err
			}
		}
		// Every Router answered that it did not route the call: that is the answer.
		if unreachable == nil {
			return refused
		}
		delay, ok := delays()
		s.mu.Lock()
		closed := s.closed
		retry = retry || s.everConnected
		s.mu.Unlock()
		if !retry || !ok || closed || (!deadline.IsZero() && delay >= time.Until(deadline)) {
			return unreachable
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return contextError(ctx)
		}
	}
}

func (s *routerSet) unaryRaw(ctx context.Context, service, method string, request json.RawMessage, opts []CallOption) (json.RawMessage, map[string]string, error) {
	o, err := s.prepare(service, opts)
	if err != nil {
		return nil, nil, err
	}
	var result json.RawMessage
	var trailers map[string]string
	routed := func(ctx context.Context, info *CallInfo) error {
		return s.route(ctx, *info.Options, func(ctx context.Context, c *Client, opts []CallOption, _ string) error {
			var err error
			result, trailers, err = c.unaryRaw(ctx, service, method, request, opts)
			return err
		})
	}
	info := &CallInfo{Service: service, Method: method, Stream: "unary", Options: &o}
	if s.retry.Disabled {
		err = routed(ctx, info)
	} else {
		err = retryCall(ctx, s.retry, info, routed)
	}
	return result, trailers, err
}

func (s *routerSet) serverStreamRaw(ctx context.Context, service, method string, request json.RawMessage, opts []CallOption) (streamSource, error) {
	o, err := s.prepare(service, opts)
	if err != nil {
		return nil, err
	}
	stream := newRoutedStream()
	go func() {
		var first chan frameResult
		var source streamSource
		err := s.route(ctx, o, func(ctx context.Context, c *Client, opts []CallOption, _ string) error {
			opened, err := c.serverStreamRaw(ctx, service, method, request, opts)
			if err != nil {
				return err
			}
			result := nextResult(opened)
			if result.err != nil {
				return result.err
			}
			first, source = make(chan frameResult, 1), opened
			first <- result
			return nil
		})
		stream.finish(source, first, err)
	}()
	return stream, nil
}

func (s *routerSet) clientStreamRaw(ctx context.Context, service, method string, request json.RawMessage, items func(yield func(json.RawMessage) bool), opts []CallOption) (json.RawMessage, map[string]string, error) {
	o, err := s.prepare(service, opts)
	if err != nil {
		return nil, nil, err
	}
	input := newReplayableInput(items)
	defer input.close()
	var result json.RawMessage
	var trailers map[string]string
	generation := 0
	err = s.route(ctx, o, func(ctx context.Context, c *Client, opts []CallOption, _ string) error {
		generation++
		var err error
		result, trailers, err = c.clientStreamRaw(ctx, service, method, request, input.attempt(generation), opts)
		return err
	})
	return result, trailers, err
}

func (s *routerSet) duplexRaw(ctx context.Context, service, method string, request json.RawMessage, opts []CallOption) (rawDuplex, error) {
	o, err := s.prepare(service, opts)
	if err != nil {
		return nil, err
	}
	d := &routedDuplex{routedStream: newRoutedStream()}
	go func() {
		var first chan frameResult
		var routed rawDuplex
		err := s.route(ctx, o, func(ctx context.Context, c *Client, opts []CallOption, _ string) error {
			call, err := c.duplexRaw(ctx, service, method, request, opts)
			if err != nil {
				return err
			}
			ch, err := d.probe(ctx, call)
			if err != nil {
				return err
			}
			first, routed = ch, call
			return nil
		})
		d.settleAll(routed, err)
		d.finish(routed, first, err)
	}()
	return d, nil
}

type frameResult struct {
	f   frame
	err error
}

// nextResult is a stream's next frame, a call.error as its error.
func nextResult(source streamSource) frameResult {
	f, err := source.next()
	if err == nil && f.Kind == "call.error" {
		return frameResult{f: f, err: errorFromPayload(f.Error)}
	}
	return frameResult{f: f, err: err}
}

// routedStream is a stream opened on the first Router that routes it, in the
// background. Whether it was routed is known from its first frame (or, for a
// duplex call, a completed send), which is held for the caller.
type routedStream struct {
	ready   chan struct{}
	mu      sync.Mutex
	source  streamSource
	first   chan frameResult
	err     error
	aborted *abortRequest // requested before the stream was opened
}

type abortRequest struct {
	err    error
	reason string
}

func newRoutedStream() *routedStream { return &routedStream{ready: make(chan struct{})} }

func (r *routedStream) finish(source streamSource, first chan frameResult, err error) {
	r.mu.Lock()
	r.source, r.first, r.err = source, first, err
	aborted := r.aborted
	r.mu.Unlock()
	close(r.ready)
	if aborted != nil && source != nil {
		source.abort(aborted.err, aborted.reason)
	}
}

func (r *routedStream) next() (frame, error) {
	<-r.ready
	r.mu.Lock()
	first, source, err := r.first, r.source, r.err
	r.first = nil
	r.mu.Unlock()
	if err != nil {
		return frame{}, err
	}
	if first != nil {
		result := <-first
		return result.f, result.err
	}
	return source.next()
}

func (r *routedStream) consumed() {
	r.mu.Lock()
	source := r.source
	r.mu.Unlock()
	if source != nil {
		source.consumed()
	}
}

func (r *routedStream) abort(err error, reason string) {
	r.mu.Lock()
	source := r.source
	if source == nil {
		r.aborted = &abortRequest{err: err, reason: reason}
	}
	r.mu.Unlock()
	if source != nil {
		source.abort(err, reason)
	}
}

// replayableInput holds the caller's items for a client stream that may be
// opened on more than one Router. A runtime pulls the next item only after
// sending the previous one, and sends nothing before the server grants
// credit; a Router that did not route the call granted none, so only the
// item last pulled can be unsent, and it is offered again to the next
// Router. Pulls are serialized: an attempt that failed may still be pulling.
type replayableInput struct {
	mu      sync.Mutex
	pull    func() (json.RawMessage, bool)
	release func()
	held    json.RawMessage
	has     bool
	heldBy  int
	done    bool
}

func newReplayableInput(items func(yield func(json.RawMessage) bool)) *replayableInput {
	pull, release := iter.Pull(iter.Seq[json.RawMessage](items))
	return &replayableInput{pull: pull, release: release}
}

// close ends the caller's items once the call is over (after any pull an
// attempt that failed may still be making).
func (r *replayableInput) close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.release()
}

func (r *replayableInput) attempt(generation int) func(yield func(json.RawMessage) bool) {
	return func(yield func(json.RawMessage) bool) {
		for {
			r.mu.Lock()
			if r.has && r.heldBy != generation {
				// Pulled by an attempt that was not routed: never sent.
				r.heldBy = generation
				item := r.held
				r.mu.Unlock()
				if !yield(item) {
					return
				}
				continue
			}
			if r.done {
				r.mu.Unlock()
				return
			}
			item, ok := r.pull()
			r.held, r.has, r.heldBy, r.done = item, ok, generation, !ok
			r.mu.Unlock()
			if !ok || !yield(item) {
				return
			}
		}
	}
}

// pendingSend is an item the caller sent on a duplex call not yet routed.
type pendingSend struct {
	item    json.RawMessage
	done    chan error    // the caller's answer
	settled chan struct{} // closed once answered
	failed  error         // sending it on the current attempt failed
}

// routedDuplex is a duplex call opened on the first Router that routes it.
// The call is routed once a send completes (the server granted input
// credit) or it produces a frame; until then the caller's items are kept
// and, when a Router did not route the call, sent again to the next one. A
// send returns once its item went out on the call that was routed.
type routedDuplex struct {
	*routedStream
	dmu         sync.Mutex
	current     rawDuplex
	routed      rawDuplex
	failure     error
	unsent      []*pendingSend
	sentOn      chan struct{}
	halfClosing bool
	closeDone   chan struct{}
	closeErr    error
	closedOn    rawDuplex
}

// probe opens the call on one Router: it sends what is kept and waits until
// the call is routed or fails.
func (d *routedDuplex) probe(ctx context.Context, call rawDuplex) (chan frameResult, error) {
	d.dmu.Lock()
	d.current = call
	sent := make(chan struct{})
	d.sentOn = sent
	pending := slices.Clone(d.unsent)
	halfClose := d.halfClosing && len(d.unsent) == 0
	d.dmu.Unlock()
	for _, p := range pending {
		d.forward(call, p)
	}
	if halfClose {
		go func() { _ = d.closeOn(call) }()
	}
	first := make(chan frameResult, 1)
	go func() { first <- nextResult(call) }()
	select {
	case result := <-first:
		if result.err != nil {
			return nil, result.err
		}
		held := make(chan frameResult, 1)
		held <- result
		return held, nil
	case <-sent:
		return first, nil
	case <-ctx.Done():
		call.abort(contextError(ctx), "caller_cancelled")
		return nil, contextError(ctx)
	}
}

func (d *routedDuplex) forward(call rawDuplex, p *pendingSend) {
	d.dmu.Lock()
	p.failed = nil
	d.dmu.Unlock()
	go func() {
		err := call.send(p.item)
		d.dmu.Lock()
		defer d.dmu.Unlock()
		if d.current != call {
			return
		}
		switch {
		case err == nil:
			d.settle(p, nil)
			// Credit was granted: the call is routed.
			if d.sentOn != nil {
				close(d.sentOn)
				d.sentOn = nil
			}
		case d.routed == call:
			d.settle(p, err)
		default:
			p.failed = err // decided once the attempt is
		}
	}()
}

// settle answers a pending send; the caller holds dmu.
func (d *routedDuplex) settle(p *pendingSend, err error) {
	index := slices.Index(d.unsent, p)
	if index < 0 {
		return
	}
	d.unsent = slices.Delete(d.unsent, index, index+1)
	p.done <- err
	close(p.settled)
}

// settleAll answers the sends kept while routing: those that failed on the
// routed call, or all of them when no Router routed it.
func (d *routedDuplex) settleAll(routed rawDuplex, err error) {
	d.dmu.Lock()
	defer d.dmu.Unlock()
	d.routed = routed
	if err != nil {
		d.failure = err
	}
	for _, p := range slices.Clone(d.unsent) {
		switch {
		case err != nil:
			d.settle(p, err)
		case p.failed != nil:
			d.settle(p, p.failed)
		}
	}
}

func (d *routedDuplex) send(item json.RawMessage) error {
	d.dmu.Lock()
	if d.halfClosing {
		d.dmu.Unlock()
		return &Error{Code: CodeProtocolError, Message: "Input stream is closed"}
	}
	if routed := d.routed; routed != nil {
		d.dmu.Unlock()
		return routed.send(item)
	}
	if d.failure != nil {
		err := d.failure
		d.dmu.Unlock()
		return err
	}
	p := &pendingSend{item: item, done: make(chan error, 1), settled: make(chan struct{})}
	d.unsent = append(d.unsent, p)
	current := d.current
	d.dmu.Unlock()
	if current != nil {
		d.forward(current, p)
	}
	return <-p.done
}

// closeSend half-closes after the items sent before it: they settle once
// the call is routed.
func (d *routedDuplex) closeSend() error {
	d.dmu.Lock()
	if d.halfClosing {
		done := d.closeDone
		d.dmu.Unlock()
		<-done
		return d.closeErr
	}
	d.halfClosing = true
	d.closeDone = make(chan struct{})
	pending := slices.Clone(d.unsent)
	d.dmu.Unlock()
	for _, p := range pending {
		<-p.settled
	}
	d.dmu.Lock()
	routed, current := d.routed, d.current
	d.dmu.Unlock()
	// Not routed yet: the server may be waiting for the end of the input.
	if routed == nil && current != nil {
		go func() { _ = d.closeOn(current) }()
	}
	<-d.ready
	d.dmu.Lock()
	routed = d.routed
	d.dmu.Unlock()
	if routed != nil {
		d.closeErr = d.closeOn(routed)
	}
	close(d.closeDone)
	return d.closeErr
}

func (d *routedDuplex) closeOn(call rawDuplex) error {
	d.dmu.Lock()
	if d.closedOn == call {
		d.dmu.Unlock()
		return nil
	}
	d.closedOn = call
	d.dmu.Unlock()
	return call.closeSend()
}
