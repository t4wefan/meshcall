package meshcall

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"time"
)

// Context is what a handler learns about, and attaches to, the current call.
// It is a context.Context that ends when the call is cancelled, its
// timeout_ms passes, or the connection is lost.
type Context struct {
	context.Context
	CallID         string
	Service        string
	Method         string
	Metadata       map[string]string
	IdempotencyKey string
	// InstanceID is this server's instance id; what a targeted caller addressed.
	InstanceID string
	Logger     *slog.Logger
	call       *serverCall
}

// SetTrailer attaches a trailer to the call's terminal frame.
func (c *Context) SetTrailer(key, value string) {
	if key == "" || len(key) >= 9 && key[:9] == "meshcall-" {
		panic(fmt.Sprintf("meshcall: invalid trailer key %q", key))
	}
	c.call.trailerMu.Lock()
	c.call.trailers[key] = value
	c.call.trailerMu.Unlock()
}

// Remaining is the time left in the call's budget; ok is false when the
// caller set no timeout.
func (c *Context) Remaining() (time.Duration, bool) {
	deadline, ok := c.Deadline()
	if !ok {
		return 0, false
	}
	return max(0, time.Until(deadline)), true
}

// ReconnectPolicy is exponential backoff with jitter for re-establishing a
// lost connection. MaxAttempts 0 keeps trying.
type ReconnectPolicy struct {
	Disabled     bool
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
	Jitter       float64
	MaxAttempts  int
}

// DefaultReconnect matches the other runtimes: 100 ms doubling to 30 s, ±20 %.
var DefaultReconnect = ReconnectPolicy{InitialDelay: 100 * time.Millisecond, MaxDelay: 30 * time.Second, Multiplier: 2, Jitter: 0.2}

func (p ReconnectPolicy) withDefaults() ReconnectPolicy {
	if p.InitialDelay <= 0 {
		p.InitialDelay = DefaultReconnect.InitialDelay
	}
	if p.MaxDelay < p.InitialDelay {
		p.MaxDelay = max(p.InitialDelay, DefaultReconnect.MaxDelay)
	}
	if p.Multiplier < 1 {
		p.Multiplier = DefaultReconnect.Multiplier
	}
	if p.Jitter <= 0 || p.Jitter > 1 {
		p.Jitter = DefaultReconnect.Jitter
	}
	return p
}

// delays yields successive sleeps; it returns false once MaxAttempts are used.
func (p ReconnectPolicy) delays() func() (time.Duration, bool) {
	p = p.withDefaults()
	delay, attempt := p.InitialDelay, 0
	return func() (time.Duration, bool) {
		if p.Disabled || (p.MaxAttempts > 0 && attempt >= p.MaxAttempts) {
			return 0, false
		}
		attempt++
		spread := float64(delay) * p.Jitter
		next := time.Duration(max(0, float64(delay)+(rand.Float64()*2-1)*spread))
		delay = min(p.MaxDelay, time.Duration(float64(delay)*p.Multiplier))
		return next, true
	}
}

const initialStreamCredit = 16

var errEndOfInput = io.EOF

// creditWindow counts items a producer may still send.
type creditWindow struct {
	mu     chan struct{} // 1-slot mutex usable with select
	n      int64
	wake   chan struct{}
	closed error
}

func newCreditWindow() *creditWindow {
	w := &creditWindow{mu: make(chan struct{}, 1), wake: make(chan struct{})}
	return w
}

func (w *creditWindow) lock()   { w.mu <- struct{}{} }
func (w *creditWindow) unlock() { <-w.mu }

func (w *creditWindow) grant(n int64) {
	w.lock()
	w.n += n
	close(w.wake)
	w.wake = make(chan struct{})
	w.unlock()
}

func (w *creditWindow) close(err error) {
	w.lock()
	if w.closed == nil {
		w.closed = err
		close(w.wake)
		w.wake = make(chan struct{})
	}
	w.unlock()
}

func (w *creditWindow) acquire(ctx context.Context) error {
	for {
		w.lock()
		if w.closed != nil {
			err := w.closed
			w.unlock()
			return err
		}
		if w.n > 0 {
			w.n--
			w.unlock()
			return nil
		}
		wake := w.wake
		w.unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
}

// Interceptor chains compose here so every handler shape shares one path.
func chain(interceptors []ServerInterceptor, ctx *Context, request any, handler ServerCallNext) (any, error) {
	next := handler
	for i := len(interceptors) - 1; i >= 0; i-- {
		interceptor, inner := interceptors[i], next
		next = func() (any, error) { return interceptor(ctx, request, inner) }
	}
	return next()
}

func errorString(err error) string {
	if err == nil {
		return "ok"
	}
	if e, ok := AsError(err); ok {
		return e.Code
	}
	return CodeInternal
}

var _ = errors.New
var _ = json.Marshal
