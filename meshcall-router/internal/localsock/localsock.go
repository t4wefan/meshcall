// Package localsock is MeshCall's local transport: Unix sockets on Linux,
// macOS and FreeBSD, named pipes on Windows (and only there). It also parses
// the Router URLs tools accept: ws(s)://host:port, unix:///path, pipe://NAME.
package localsock

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
)

// PipePrefix is the namespace every local named pipe lives in.
const PipePrefix = `\\.\pipe\`

// PipeName accepts NAME or \\.\pipe\NAME and returns NAME. Windows allows
// any character but a backslash, up to 256 characters.
func PipeName(name string) (string, error) {
	name = strings.TrimPrefix(name, PipePrefix)
	if name == "" || len(name) > 256 || strings.Contains(name, `\`) {
		return "", fmt.Errorf("invalid pipe name %q: 1-256 characters, no backslash", name)
	}
	return name, nil
}

// UnixSupported reports whether this platform's local transport is a Unix
// socket. On Windows it is a named pipe instead.
func UnixSupported() bool { return unixSupported }

// ErrUnixOnWindows and ErrPipeOffWindows explain a transport used on the
// wrong platform, with the alternative to use.
var (
	ErrUnixOnWindows  = errors.New("Unix sockets are not supported on Windows; use a named pipe (--pipe NAME, pipe://NAME)")
	ErrPipeOffWindows = errors.New("named pipes are Windows-only; use a Unix socket (--unix-socket PATH, unix:///path)")
)

// Endpoint is a parsed Router URL.
type Endpoint struct {
	// URL is the WebSocket URL to dial: the original for ws(s)://, and a
	// placeholder ws://localhost/ for local transports.
	URL string
	// Dial connects to a local transport; nil for TCP.
	Dial func(ctx context.Context) (net.Conn, error)
	// Display names the endpoint in messages.
	Display string
}

// Parse reads ws://, wss://, http(s)://, unix:///path and pipe://NAME.
func Parse(raw string) (Endpoint, error) {
	switch {
	case strings.HasPrefix(raw, "pipe://"):
		if !pipesSupported {
			return Endpoint{}, ErrPipeOffWindows
		}
		name, err := PipeName(strings.TrimPrefix(raw, "pipe://"))
		if err != nil {
			return Endpoint{}, err
		}
		return Endpoint{URL: "ws://localhost/", Display: raw,
			Dial: func(ctx context.Context) (net.Conn, error) { return DialPipe(ctx, name) }}, nil
	case strings.HasPrefix(raw, "unix://"):
		if !unixSupported {
			return Endpoint{}, ErrUnixOnWindows
		}
		path := strings.TrimPrefix(raw, "unix://")
		if path == "" {
			return Endpoint{}, errors.New("unix:// needs a socket path, e.g. unix:///run/meshcall.sock")
		}
		return Endpoint{URL: "ws://localhost/", Display: raw,
			Dial: func(ctx context.Context) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", path)
			}}, nil
	default:
		return Endpoint{URL: raw, Display: raw}, nil
	}
}

// HTTPClient returns a client whose connections reach the endpoint: the
// default client for TCP, one bound to the socket or pipe otherwise.
func (e Endpoint) HTTPClient() *http.Client {
	if e.Dial == nil {
		return http.DefaultClient
	}
	dial := e.Dial
	return &http.Client{Transport: &http.Transport{
		DialContext:       func(ctx context.Context, _, _ string) (net.Conn, error) { return dial(ctx) },
		DisableKeepAlives: true,
	}}
}
