// Package conformance drives any MeshCall runtime through the protocol.
//
// The suite starts a real Router in-process, launches a fixture program that
// must register the service `conformance.v1.Fixture` (see FIXTURE.md), and
// then plays scripted frame exchanges against it as a raw client. A runtime
// is conformant when every scenario passes for its fixture.
package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/t4wefan/meshcall/meshcall-router/internal/router"
)

const (
	Service    = "conformance.v1.Fixture"
	InstanceID = "fixture-1"
	protocol   = "meshcall/1"
)

// Options configures one run.
type Options struct {
	// Fixture is the command that starts the runtime under test; it receives
	// MESHCALL_ROUTER_URL, MESHCALL_CONFORMANCE_INSTANCE and
	// MESHCALL_PING_INTERVAL_MS in its environment.
	Fixture []string
	// Only runs scenarios whose name contains this substring.
	Only string
	// Verbose prints fixture output even on success.
	Verbose bool
	// Timeout bounds any single wait for a frame.
	Timeout time.Duration
}

// Result is the outcome of one scenario.
type Result struct {
	Name     string        `json:"name"`
	Passed   bool          `json:"passed"`
	Error    string        `json:"error,omitempty"`
	Duration time.Duration `json:"duration_ns"`
}

type scenario struct {
	name string
	run  func(h *harness) error
}

// Run executes every scenario and writes a human-readable report to out.
// The returned error is non-nil when the harness itself failed; individual
// scenario failures are reported through Results.
func Run(ctx context.Context, opts Options, out io.Writer) ([]Result, error) {
	if opts.Timeout == 0 {
		opts.Timeout = 5 * time.Second
	}
	h, err := newHarness(ctx, opts)
	if err != nil {
		return nil, err
	}
	defer h.close()

	var results []Result
	for _, s := range scenarios {
		if opts.Only != "" && !strings.Contains(s.name, opts.Only) {
			continue
		}
		started := time.Now()
		err := h.guard(s.run)
		result := Result{Name: s.name, Passed: err == nil, Duration: time.Since(started)}
		if err != nil {
			result.Error = err.Error()
		}
		results = append(results, result)
		status := "ok  "
		if err != nil {
			status = "FAIL"
		}
		fmt.Fprintf(out, "%s %-48s %6dms\n", status, s.name, result.Duration.Milliseconds())
		if err != nil {
			fmt.Fprintf(out, "     %s\n", strings.ReplaceAll(err.Error(), "\n", "\n     "))
		}
	}
	failed := 0
	for _, r := range results {
		if !r.Passed {
			failed++
		}
	}
	if failed > 0 || opts.Verbose {
		fmt.Fprintf(out, "\n--- fixture output ---\n%s\n", h.fixtureOutput())
	}
	fmt.Fprintf(out, "\n%d scenarios, %d failed\n", len(results), failed)
	return results, nil
}

// --- harness ---------------------------------------------------------------

type harness struct {
	ctx     context.Context
	opts    Options
	router  *router.Router
	port    int
	fixture *exec.Cmd
	output  bytes.Buffer
	outMu   sync.Mutex
	exited  chan struct{}
}

func newHarness(ctx context.Context, opts Options) (*harness, error) {
	if len(opts.Fixture) == 0 {
		return nil, errors.New("a fixture command is required")
	}
	h := &harness{ctx: ctx, opts: opts}
	// Short Router idle timeout so the keepalive scenario is observable.
	if err := h.startRouter(0); err != nil {
		return nil, err
	}
	if err := h.startFixture(); err != nil {
		h.close()
		return nil, err
	}
	if err := h.waitRegistered(15 * time.Second); err != nil {
		h.close()
		return nil, fmt.Errorf("fixture did not register %s as %s: %w\n%s", Service, InstanceID, err, h.fixtureOutput())
	}
	return h, nil
}

func (h *harness) startRouter(port int) error {
	r, err := router.Start(router.Config{Port: port, IdleTimeout: 1500 * time.Millisecond, DeadlineGrace: 300 * time.Millisecond})
	if err != nil {
		return err
	}
	h.router, h.port = r, r.Ready().Port
	return nil
}

func (h *harness) startFixture() error {
	cmd := exec.CommandContext(h.ctx, h.opts.Fixture[0], h.opts.Fixture[1:]...)
	cmd.Env = append(os.Environ(),
		"MESHCALL_ROUTER_URL=ws://127.0.0.1:"+fmt.Sprint(h.port),
		"MESHCALL_CONFORMANCE_INSTANCE="+InstanceID,
		"MESHCALL_PING_INTERVAL_MS=500",
		"MESHCALL_STRICT=1",
	)
	cmd.Stdout = &lockedWriter{h: h}
	cmd.Stderr = &lockedWriter{h: h}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start fixture: %w", err)
	}
	h.fixture = cmd
	h.exited = make(chan struct{})
	go func() { _ = cmd.Wait(); close(h.exited) }()
	return nil
}

type lockedWriter struct{ h *harness }

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.h.outMu.Lock()
	defer w.h.outMu.Unlock()
	return w.h.output.Write(p)
}

func (h *harness) fixtureOutput() string {
	h.outMu.Lock()
	defer h.outMu.Unlock()
	return h.output.String()
}

func (h *harness) close() {
	if h.fixture != nil && h.fixture.Process != nil {
		_ = h.fixture.Process.Kill()
		<-h.exited
	}
	if h.router != nil {
		h.router.Close()
	}
}

func (h *harness) guard(run func(h *harness) error) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("panic: %v", rec)
		}
	}()
	return run(h)
}

func (h *harness) registered() bool {
	ids := h.router.Snapshot().Instances[Service]
	for _, id := range ids {
		if id == InstanceID {
			return true
		}
	}
	return false
}

func (h *harness) waitRegistered(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if h.registered() {
			return nil
		}
		select {
		case <-h.exited:
			return errors.New("fixture exited")
		case <-time.After(20 * time.Millisecond):
		}
	}
	return errors.New("timeout")
}

func (h *harness) waitUnregistered(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !h.registered() {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return errors.New("instance still registered")
}

// --- raw client ------------------------------------------------------------

type frame = map[string]any

type client struct {
	h      *harness
	conn   *websocket.Conn
	seq    int
	frames chan frame
	errors chan error
}

func (h *harness) client() (*client, error) {
	return h.clientAt(h.port)
}

func (h *harness) clientAt(port int) (*client, error) {
	ctx, cancel := context.WithTimeout(h.ctx, h.opts.Timeout)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, fmt.Sprintf("ws://127.0.0.1:%d", port), nil)
	if err != nil {
		return nil, fmt.Errorf("dial router: %w", err)
	}
	c := &client{h: h, conn: conn, frames: make(chan frame, 1024), errors: make(chan error, 1)}
	// A single reader goroutine: cancelling a websocket.Read closes the
	// connection, so waits are implemented on the channel instead.
	go func() {
		for {
			_, data, err := conn.Read(h.ctx)
			if err != nil {
				c.errors <- fmt.Errorf("receive: %w", err)
				close(c.frames)
				return
			}
			var f frame
			if err := json.Unmarshal(data, &f); err != nil {
				c.errors <- fmt.Errorf("decode %q: %w", data, err)
				close(c.frames)
				return
			}
			if f["kind"] == "pong" {
				continue
			}
			c.frames <- f
		}
	}()
	// The Router drops silent peers; keep this raw client alive like a real one.
	go func() {
		ticker := time.NewTicker(400 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-h.ctx.Done():
				return
			case <-ticker.C:
				if c.send(frame{"kind": "ping", "nonce": fmt.Sprint(time.Now().UnixNano())}) != nil {
					return
				}
			}
		}
	}()
	if err := c.send(frame{"kind": "hello", "protocol": protocol, "role": "client", "peer_id": "conformance"}); err != nil {
		return nil, err
	}
	ack, err := c.receive()
	if err != nil {
		return nil, err
	}
	if ack["kind"] != "hello.ack" {
		return nil, fmt.Errorf("expected hello.ack, got %v", ack)
	}
	return c, nil
}

func (c *client) close() { _ = c.conn.CloseNow() }

func (c *client) send(f frame) error {
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(c.h.ctx, c.h.opts.Timeout)
	defer cancel()
	return c.conn.Write(ctx, websocket.MessageText, data)
}

var errNoFrame = errors.New("no frame within the wait")

func (c *client) receive() (frame, error) { return c.receiveWithin(c.h.opts.Timeout) }

func (c *client) receiveWithin(timeout time.Duration) (frame, error) {
	select {
	case f, ok := <-c.frames:
		if !ok {
			return nil, <-c.errors
		}
		return f, nil
	case <-time.After(timeout):
		return nil, errNoFrame
	case <-c.h.ctx.Done():
		return nil, c.h.ctx.Err()
	}
}

// expectNothing fails if any frame arrives within d.
func (c *client) expectNothing(d time.Duration) error {
	f, err := c.receiveWithin(d)
	if err == nil {
		return fmt.Errorf("unexpected frame %v", f)
	}
	if errors.Is(err, errNoFrame) {
		return nil
	}
	return err
}

func (c *client) callID() string {
	c.seq++
	return fmt.Sprintf("call-%d-%d", time.Now().UnixNano()%1_000_000, c.seq)
}

func (c *client) open(method string, payload any, extra frame) (string, error) {
	id := c.callID()
	f := frame{"kind": "call.open", "call_id": id, "service": Service, "method": method, "payload": payload}
	for k, v := range extra {
		f[k] = v
	}
	return id, c.send(f)
}

// unary opens a call and returns its terminal frame.
func (c *client) unary(method string, payload any, extra frame) (frame, error) {
	id, err := c.open(method, payload, extra)
	if err != nil {
		return nil, err
	}
	for {
		f, err := c.receive()
		if err != nil {
			return nil, err
		}
		if f["call_id"] != id {
			continue
		}
		switch f["kind"] {
		case "call.result", "call.error":
			return f, nil
		}
	}
}

func errorOf(f frame) (code, message string, retryable bool, details map[string]any) {
	e, _ := f["error"].(map[string]any)
	code, _ = e["code"].(string)
	message, _ = e["message"].(string)
	retryable, _ = e["retryable"].(bool)
	details, _ = e["details"].(map[string]any)
	return
}

func number(v any) float64 {
	n, _ := v.(float64)
	return n
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
