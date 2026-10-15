package meshcall

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// Liveness defaults (docs/protocol.md → Liveness).
const (
	DefaultPingInterval = 15 * time.Second
	DefaultPingTimeout  = 10 * time.Second
	DefaultIdleTimeout  = 45 * time.Second
	DefaultMaxFrameSize = 64 << 20
)

// conn is one WebSocket carrying meshcall/1 frames.
type conn struct {
	ws       *websocket.Conn
	ctx      context.Context
	cancel   context.CancelFunc
	writeMu  sync.Mutex
	lastSeen atomic.Int64 // unix nanos of the last frame received
	pongs    sync.Map     // nonce → chan struct{}
	closed   chan struct{}
	once     sync.Once
}

func newConn(ws *websocket.Conn, maxFrame int) *conn {
	ws.SetReadLimit(int64(maxFrame))
	ctx, cancel := context.WithCancel(context.Background())
	c := &conn{ws: ws, ctx: ctx, cancel: cancel, closed: make(chan struct{})}
	c.lastSeen.Store(time.Now().UnixNano())
	return c
}

func (c *conn) send(f frame) error {
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()
	return c.ws.Write(ctx, websocket.MessageText, data)
}

// receive returns the next frame; pings are answered and pongs consumed here.
func (c *conn) receive() (frame, error) {
	for {
		_, data, err := c.ws.Read(c.ctx)
		if err != nil {
			c.close()
			return frame{}, err
		}
		c.lastSeen.Store(time.Now().UnixNano())
		f, err := decodeFrame(data)
		if err != nil {
			return frame{}, err
		}
		switch f.Kind {
		case "ping":
			go func() { _ = c.send(frame{Kind: "pong", Nonce: f.Nonce}) }()
			continue
		case "pong":
			if ch, ok := c.pongs.LoadAndDelete(f.Nonce); ok {
				close(ch.(chan struct{}))
			}
			continue
		}
		return f, nil
	}
}

// keepAlive is the connecting peer's side: ping when idle for interval,
// close when the pong does not come back within timeout.
func (c *conn) keepAlive(interval, timeout time.Duration) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
		}
		if time.Since(time.Unix(0, c.lastSeen.Load())) < interval {
			continue
		}
		nonce := randomID()
		pong := make(chan struct{})
		c.pongs.Store(nonce, pong)
		if c.send(frame{Kind: "ping", Nonce: nonce}) != nil {
			c.close()
			return
		}
		select {
		case <-pong:
		case <-time.After(timeout):
			c.pongs.Delete(nonce)
			c.close()
			return
		case <-c.ctx.Done():
			return
		}
	}
}

// watchIdle is the accepting peer's side: close a connection silent for timeout.
func (c *conn) watchIdle(timeout time.Duration) {
	if timeout <= 0 {
		return
	}
	ticker := time.NewTicker(min(timeout/4, time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if time.Since(time.Unix(0, c.lastSeen.Load())) > timeout {
				c.close()
				return
			}
		}
	}
}

func (c *conn) close() {
	c.once.Do(func() {
		c.cancel()
		_ = c.ws.CloseNow()
		close(c.closed)
	})
}

func randomID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// --- endpoints -------------------------------------------------------------

// endpoint is a parsed Router or server address: ws(s)://host:port,
// unix:///path (not on Windows) or pipe://NAME (Windows only).
type endpoint struct {
	url  string
	dial func(ctx context.Context) (net.Conn, error)
	text string
}

func parseEndpoint(raw string) (endpoint, error) {
	switch {
	case strings.HasPrefix(raw, "pipe://"):
		if !pipesSupported {
			return endpoint{}, errPipeOffWindows
		}
		name, err := pipeName(strings.TrimPrefix(raw, "pipe://"))
		if err != nil {
			return endpoint{}, err
		}
		return endpoint{url: "ws://localhost/", text: raw, dial: func(ctx context.Context) (net.Conn, error) { return dialPipe(ctx, name) }}, nil
	case strings.HasPrefix(raw, "unix://"):
		if pipesSupported {
			return endpoint{}, errUnixOnWindows
		}
		path := strings.TrimPrefix(raw, "unix://")
		if path == "" {
			return endpoint{}, errors.New("unix:// needs a socket path, e.g. unix:///run/meshcall.sock")
		}
		return endpoint{url: "ws://localhost/", text: raw, dial: func(ctx context.Context) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", path)
		}}, nil
	case strings.HasPrefix(raw, "ws://"), strings.HasPrefix(raw, "wss://"):
		return endpoint{url: raw, text: raw}, nil
	default:
		return endpoint{}, errors.New("endpoint must be ws://, wss://, unix:///path or pipe://NAME, got " + raw)
	}
}

func (e endpoint) httpClient() *http.Client {
	if e.dial == nil {
		return http.DefaultClient
	}
	dial := e.dial
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) { return dial(ctx) },
	}}
}

func (e endpoint) connect(ctx context.Context, header http.Header, maxFrame int) (*conn, error) {
	ws, _, err := websocket.Dial(ctx, e.url, &websocket.DialOptions{HTTPHeader: header, HTTPClient: e.httpClient()})
	if err != nil {
		return nil, err
	}
	return newConn(ws, maxFrame), nil
}

var (
	errUnixOnWindows  = errors.New("Unix sockets are not supported on Windows; use a named pipe (pipe://NAME)")
	errPipeOffWindows = errors.New("named pipes are Windows-only; use a Unix socket (unix:///path)")
)

const pipePrefix = `\\.\pipe\`

func pipeName(name string) (string, error) {
	name = strings.TrimPrefix(name, pipePrefix)
	if name == "" || len(name) > 256 || strings.Contains(name, `\`) {
		return "", errors.New("invalid pipe name " + name + ": 1-256 characters, no backslash")
	}
	return name, nil
}
