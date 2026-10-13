// Package wire is a minimal raw MeshCall client: dial, hello, send and
// receive JSON frames. The conformance suite and the meshcall CLI use it; it
// deliberately knows nothing about contracts or generated code.
package wire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

const Protocol = "meshcall/1"

// Frame is a decoded JSON frame; callers index fields directly.
type Frame = map[string]any

// ErrNoFrame is returned by Recv when nothing arrived within the wait.
var ErrNoFrame = errors.New("no frame within the wait")

// Conn is a client-role connection with a background reader and keepalive.
type Conn struct {
	conn    *websocket.Conn
	ctx     context.Context
	cancel  context.CancelFunc
	frames  chan Frame
	readErr chan error
	seq     atomic.Int64
	Timeout time.Duration
}

// Options for Dial.
type Options struct {
	// Authorization header value (e.g. "Basic …" or "Bearer …"), optional.
	Authorization string
	PeerID        string
	// Timeout bounds Send and the default Recv; zero means 5s.
	Timeout time.Duration
	// PingInterval keeps the connection alive; zero means 10s, negative disables.
	PingInterval time.Duration
}

// Dial connects, completes the client hello and starts the reader.
func Dial(ctx context.Context, url string, opts Options) (*Conn, error) {
	if opts.Timeout == 0 {
		opts.Timeout = 5 * time.Second
	}
	if opts.PingInterval == 0 {
		opts.PingInterval = 10 * time.Second
	}
	if opts.PeerID == "" {
		opts.PeerID = "wire"
	}
	header := http.Header{}
	if opts.Authorization != "" {
		header.Set("Authorization", opts.Authorization)
	}
	dialCtx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()
	ws, _, err := websocket.Dial(dialCtx, url, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", url, err)
	}
	ws.SetReadLimit(64 * 1024 * 1024)
	connCtx, connCancel := context.WithCancel(ctx)
	c := &Conn{conn: ws, ctx: connCtx, cancel: connCancel, frames: make(chan Frame, 1024), readErr: make(chan error, 1), Timeout: opts.Timeout}
	go c.read()
	if opts.PingInterval > 0 {
		go c.ping(opts.PingInterval)
	}
	if err := c.Send(Frame{"kind": "hello", "protocol": Protocol, "role": "client", "peer_id": opts.PeerID}); err != nil {
		c.Close()
		return nil, err
	}
	ack, err := c.Recv()
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("handshake: %w", err)
	}
	if ack["kind"] != "hello.ack" || ack["protocol"] != Protocol {
		c.Close()
		return nil, fmt.Errorf("handshake: expected hello.ack for %s, got %v", Protocol, ack)
	}
	return c, nil
}

func (c *Conn) read() {
	for {
		_, data, err := c.conn.Read(c.ctx)
		if err != nil {
			c.readErr <- err
			close(c.frames)
			return
		}
		var f Frame
		if err := json.Unmarshal(data, &f); err != nil {
			c.readErr <- fmt.Errorf("decode %q: %w", data, err)
			close(c.frames)
			return
		}
		if f["kind"] == "pong" {
			continue
		}
		c.frames <- f
	}
}

func (c *Conn) ping(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if c.Send(Frame{"kind": "ping", "nonce": fmt.Sprint(time.Now().UnixNano())}) != nil {
				return
			}
		}
	}
}

// Close tears the connection down; pending Recv calls fail.
func (c *Conn) Close() {
	c.cancel()
	_ = c.conn.CloseNow()
}

// Send writes one frame.
func (c *Conn) Send(f Frame) error {
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(c.ctx, c.Timeout)
	defer cancel()
	return c.conn.Write(ctx, websocket.MessageText, data)
}

// Recv returns the next non-keepalive frame within the default timeout.
func (c *Conn) Recv() (Frame, error) { return c.RecvWithin(c.Timeout) }

// RecvWithin waits up to d for a frame; ErrNoFrame when none arrives.
func (c *Conn) RecvWithin(d time.Duration) (Frame, error) {
	select {
	case f, ok := <-c.frames:
		if !ok {
			return nil, fmt.Errorf("connection closed: %w", <-c.readErr)
		}
		return f, nil
	case <-time.After(d):
		return nil, ErrNoFrame
	case <-c.ctx.Done():
		return nil, c.ctx.Err()
	}
}

// NewCallID returns a unique call id for this connection.
func (c *Conn) NewCallID() string {
	return fmt.Sprintf("wire-%d-%d", time.Now().UnixNano()%1_000_000_000, c.seq.Add(1))
}

// Unary opens a call and returns its terminal frame, ignoring frames for
// other calls. extra fields (timeout_ms, metadata, …) are merged into call.open.
func (c *Conn) Unary(service, method string, payload any, extra Frame) (Frame, error) {
	id := c.NewCallID()
	open := Frame{"kind": "call.open", "call_id": id, "service": service, "method": method, "payload": payload}
	for k, v := range extra {
		open[k] = v
	}
	if err := c.Send(open); err != nil {
		return nil, err
	}
	for {
		f, err := c.RecvWithin(c.Timeout)
		if err != nil {
			return nil, err
		}
		if f["call_id"] != id {
			continue
		}
		if kind := f["kind"]; kind == "call.result" || kind == "call.error" {
			return f, nil
		}
	}
}

// Error extracts the error envelope of a call.error frame.
func Error(f Frame) (code, message string, retryable bool, details map[string]any) {
	e, _ := f["error"].(map[string]any)
	code, _ = e["code"].(string)
	message, _ = e["message"].(string)
	retryable, _ = e["retryable"].(bool)
	details, _ = e["details"].(map[string]any)
	return
}
