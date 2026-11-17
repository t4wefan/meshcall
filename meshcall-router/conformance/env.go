package conformance

// The client environment: a Router and a fixture service for testing a
// *client* runtime (CLIENTS.md). Unlike Run, which plays scripted frames
// against a fixture, the runtime under test drives the calls here; this
// side only provides the mesh and the disruptions a client must survive.

import (
	"bufio"
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/t4wefan/meshcall/meshcall-router/internal/router"
)

// EnvOptions configures a client environment.
type EnvOptions struct {
	// Fixture starts the service the client calls (FIXTURE.md); any
	// runtime's fixture will do.
	Fixture []string
	// Log receives the fixture's output (stderr when nil).
	Log io.Writer
}

// EnvReady is the first line Serve writes.
type EnvReady struct {
	Kind string `json:"kind"` // "env.ready"
	// RouterURL is where clients connect: a proxy in front of the Router,
	// so pause_router can stall their connections.
	RouterURL  string `json:"router_url"`
	InstanceID string `json:"instance_id"`
	// E2EKey is the fixture's public key (base64url), for end-to-end tunnels.
	E2EKey string `json:"e2e_key"`
}

// Env is a running client environment.
type Env struct {
	opts    EnvOptions
	key     *ecdh.PrivateKey
	port    int // the Router's own port (the fixture connects to it directly)
	router  *router.Router
	proxy   *pausingProxy
	fixture *exec.Cmd
	exited  chan struct{}
	ctx     context.Context
}

// StartEnv starts the Router, the proxy and the fixture, and waits until the
// fixture has registered.
func StartEnv(ctx context.Context, opts EnvOptions) (*Env, error) {
	if len(opts.Fixture) == 0 {
		return nil, errors.New("a fixture command is required")
	}
	if opts.Log == nil {
		opts.Log = os.Stderr
	}
	key, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	e := &Env{opts: opts, key: key, ctx: ctx}
	if err := e.startRouter(); err != nil {
		return nil, err
	}
	if e.proxy, err = newPausingProxy(func() string { return fmt.Sprintf("127.0.0.1:%d", e.port) }); err != nil {
		e.Close()
		return nil, err
	}
	if err := e.startFixture(); err != nil {
		e.Close()
		return nil, err
	}
	return e, nil
}

func (e *Env) Ready() EnvReady {
	return EnvReady{
		Kind: "env.ready", RouterURL: "ws://" + e.proxy.addr(), InstanceID: InstanceID,
		E2EKey: b64.EncodeToString(e.key.PublicKey().Bytes()),
	}
}

func (e *Env) startRouter() error {
	r, err := router.Start(router.Config{Port: e.port})
	if err != nil {
		return err
	}
	e.router, e.port = r, r.Ready().Port
	return nil
}

func (e *Env) startFixture() error {
	cmd := exec.CommandContext(e.ctx, e.opts.Fixture[0], e.opts.Fixture[1:]...)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("MESHCALL_ROUTER_URL=ws://127.0.0.1:%d", e.port),
		"MESHCALL_CONFORMANCE_INSTANCE="+InstanceID,
		"MESHCALL_PING_INTERVAL_MS=1000",
		"MESHCALL_STRICT=1",
		"MESHCALL_E2E_PRIVATE_KEY="+b64.EncodeToString(e.key.Bytes()),
	)
	cmd.Stdout, cmd.Stderr = e.opts.Log, e.opts.Log
	// A launcher (uv run, npx) may leave a child holding the output pipes.
	cmd.WaitDelay = 2 * time.Second
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start fixture: %w", err)
	}
	e.fixture, e.exited = cmd, make(chan struct{})
	go func() { _ = cmd.Wait(); close(e.exited) }()
	return e.waitRegistered()
}

func (e *Env) waitRegistered() error {
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range e.router.Snapshot().Instances[Service] {
			if id == InstanceID {
				return nil
			}
		}
		select {
		case <-e.exited:
			return errors.New("the fixture exited before registering")
		case <-time.After(20 * time.Millisecond):
		}
	}
	return errors.New("the fixture did not register")
}

// Do runs one command (CLIENTS.md → Commands).
func (e *Env) Do(command string) error {
	switch command {
	case "restart_router":
		// No drain: in-flight calls end as on a crash. Tunnels and tokens are
		// lost with it; the fixture re-registers on the same port.
		e.router.Close()
		e.proxy.dropAll()
		if err := e.startRouter(); err != nil {
			return err
		}
		return e.waitRegistered()
	case "pause_router":
		e.proxy.setPaused(true)
		return nil
	case "resume_router":
		e.proxy.setPaused(false)
		return nil
	case "restart_fixture":
		// Stopped, not drained: it re-registers and has lost its E2E sessions.
		e.stopFixture()
		return e.startFixture()
	}
	return fmt.Errorf("unknown command %q", command)
}

// stopFixture interrupts the fixture, which a launcher such as uv passes on
// to the program it runs, and kills it if it is still there after a moment.
func (e *Env) stopFixture() {
	if e.fixture == nil || e.fixture.Process == nil {
		return
	}
	if err := e.fixture.Process.Signal(os.Interrupt); err == nil {
		select {
		case <-e.exited:
			return
		case <-time.After(3 * time.Second):
		}
	}
	_ = e.fixture.Process.Kill()
	<-e.exited
}

func (e *Env) Close() {
	e.stopFixture()
	if e.proxy != nil {
		e.proxy.close()
	}
	if e.router != nil {
		e.router.Close()
	}
}

// Serve runs a client environment over a line protocol: the ready line on
// out, then one command object per line on in, each answered with
// {"ok":true} or {"ok":false,"error":…}. It returns when in ends or says stop.
func Serve(ctx context.Context, opts EnvOptions, in io.Reader, out io.Writer) error {
	env, err := StartEnv(ctx, opts)
	if err != nil {
		return err
	}
	defer env.Close()
	encoder := json.NewEncoder(out)
	if err := encoder.Encode(env.Ready()); err != nil {
		return err
	}
	scanner := bufio.NewScanner(in)
	for scanner.Scan() {
		var request struct {
			Command string `json:"command"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
			_ = encoder.Encode(map[string]any{"ok": false, "error": "not a command object"})
			continue
		}
		if request.Command == "stop" {
			_ = encoder.Encode(map[string]any{"ok": true})
			return nil
		}
		if err := env.Do(request.Command); err != nil {
			_ = encoder.Encode(map[string]any{"ok": false, "error": err.Error()})
		} else {
			_ = encoder.Encode(map[string]any{"ok": true})
		}
	}
	return scanner.Err()
}

// pausingProxy forwards TCP connections to the Router. Paused, it stops
// moving bytes in both directions without closing anything: to a client
// the Router looks alive but never answers, as after an app was suspended.
type pausingProxy struct {
	listener net.Listener
	target   func() string
	mu       sync.Mutex
	paused   bool
	resumed  chan struct{}
	conns    map[net.Conn]bool
}

func newPausingProxy(target func() string) (*pausingProxy, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &pausingProxy{listener: listener, target: target, resumed: make(chan struct{}), conns: map[net.Conn]bool{}}
	close(p.resumed)
	go p.accept()
	return p, nil
}

func (p *pausingProxy) addr() string { return p.listener.Addr().String() }

func (p *pausingProxy) accept() {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		go p.forward(client)
	}
}

func (p *pausingProxy) forward(client net.Conn) {
	upstream, err := net.Dial("tcp", p.target())
	if err != nil {
		_ = client.Close()
		return
	}
	p.track(client, upstream)
	done := make(chan struct{}, 2)
	pipe := func(dst, src net.Conn) {
		buf := make([]byte, 32*1024)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				p.waitResumed()
				if _, werr := dst.Write(buf[:n]); werr != nil {
					break
				}
			}
			if err != nil {
				break
			}
		}
		done <- struct{}{}
	}
	go pipe(upstream, client)
	go pipe(client, upstream)
	<-done
	_ = client.Close()
	_ = upstream.Close()
	<-done
	p.untrack(client, upstream)
}

func (p *pausingProxy) track(conns ...net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range conns {
		p.conns[c] = true
	}
}

func (p *pausingProxy) untrack(conns ...net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range conns {
		delete(p.conns, c)
	}
}

func (p *pausingProxy) waitResumed() {
	p.mu.Lock()
	resumed := p.resumed
	p.mu.Unlock()
	<-resumed
}

func (p *pausingProxy) setPaused(paused bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if paused == p.paused {
		return
	}
	p.paused = paused
	if paused {
		p.resumed = make(chan struct{})
	} else {
		close(p.resumed)
	}
}

// dropAll closes every proxied connection, as a Router that went away would.
func (p *pausingProxy) dropAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.conns {
		_ = c.Close()
	}
}

func (p *pausingProxy) close() {
	_ = p.listener.Close()
	p.setPaused(false)
	p.dropAll()
}
