package router

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

// HTTP calls (docs/protocol.md → HTTP calls). Each request becomes a virtual
// client connection: a peer whose outbox is drained by the request handler
// instead of a WebSocket writer. It goes through the same forward() path as
// any client, so permissions, balancing, deadlines, breakers, dedup and
// targeting need no HTTP-specific code, and the Router keeps no extra state:
// the peer exists exactly as long as the request.

const (
	httpPrefix      = "/v1/"
	ndjson          = "application/x-ndjson"
	streamCredit    = 128 // server-stream items granted per window
	metaHeader      = "Meshcall-Meta-"
	trailerHeader   = "MeshCall-Trailer-"
	httpCallPath    = "/v1/call/"
	httpServicePath = "/v1/services"
)

// httpStatus maps an error code to the HTTP status in the protocol's table.
func httpStatus(e *errorPayload) int {
	switch e.Code {
	case "invalid_argument":
		return http.StatusBadRequest
	case "unauthenticated":
		return http.StatusUnauthorized
	case "permission_denied":
		return http.StatusForbidden
	case "method_not_found":
		var d map[string]any
		if json.Unmarshal(e.Details, &d) == nil && d["reason"] == "duplex_over_http" {
			return http.StatusMethodNotAllowed
		}
		return http.StatusNotFound
	case "resource_exhausted":
		return http.StatusTooManyRequests
	case "cancelled":
		return 499 // client closed request, as nginx reports it
	case "unavailable":
		return http.StatusServiceUnavailable
	case "deadline_exceeded":
		return http.StatusGatewayTimeout
	default:
		return http.StatusInternalServerError
	}
}

func writeHTTPError(w http.ResponseWriter, e *errorPayload) {
	writeHTTPErrorStatus(w, httpStatus(e), e)
}

func writeHTTPErrorStatus(w http.ResponseWriter, status int, e *errorPayload) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]*errorPayload{"error": e})
}

// routerHTTPError is an error the Router itself raises for a malformed request.
func routerHTTPError(code, message, reason string) *errorPayload {
	details, _ := json.Marshal(map[string]string{"source": "router", "reason": reason})
	// Capacity and availability are transient; a malformed request is not.
	retryable := code == "unavailable" || code == "resource_exhausted"
	return &errorPayload{Code: code, Message: message, Retryable: retryable, Details: details}
}

// serveHealth answers /healthz (the process is up) and /readyz (it accepts
// calls). Neither needs credentials: they reveal no services.
func (r *Router) serveHealth(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if req.URL.Path == "/readyz" {
		if r.stopping.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"stopping"}` + "\n"))
			return
		}
		_, _ = w.Write([]byte(`{"status":"ready"}` + "\n"))
		return
	}
	_, _ = w.Write([]byte(`{"status":"ok"}` + "\n"))
}

// cors applies the origin allowlist. It reports false when the request was
// a preflight that has been answered (or refused) and needs nothing more.
func (r *Router) cors(w http.ResponseWriter, req *http.Request) bool {
	origin := req.Header.Get("Origin")
	allowed := origin != "" && slices.ContainsFunc(r.config.HTTPCORSOrigins, func(o string) bool { return o == "*" || o == origin })
	if allowed {
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", origin)
		h.Add("Vary", "Origin")
		// No cookies are ever honoured, so "*" is safe for exposed headers.
		h.Set("Access-Control-Expose-Headers", "*")
	}
	if req.Method != http.MethodOptions {
		return true
	}
	if !allowed {
		w.WriteHeader(http.StatusForbidden)
		return false
	}
	h := w.Header()
	h.Set("Access-Control-Allow-Methods", "GET, POST")
	// The wildcard does not cover Authorization; it must be listed.
	h.Set("Access-Control-Allow-Headers", "Authorization, *")
	h.Set("Access-Control-Max-Age", "600")
	w.WriteHeader(http.StatusNoContent)
	return false
}

// serveHTTP answers every request under /v1/.
func (r *Router) serveHTTP(w http.ResponseWriter, req *http.Request) {
	if !r.cors(w, req) {
		return
	}
	if r.httpInflight.Add(1) > int64(r.config.HTTPMaxInflight) {
		r.httpInflight.Add(-1)
		w.Header().Set("Retry-After", "1")
		writeHTTPErrorStatus(w, http.StatusTooManyRequests, routerHTTPError("resource_exhausted", "Router HTTP call limit reached", "overloaded"))
		return
	}
	defer r.httpInflight.Add(-1)
	var service, method string
	var payload []byte
	openapi := false
	switch path := req.URL.Path; {
	case path == "/v1/openapi.json" && req.Method == http.MethodGet:
		openapi = true
	case path == httpServicePath && req.Method == http.MethodGet:
		service, method, payload = RegistryService, "list", []byte(`{}`)
	case strings.HasPrefix(path, httpServicePath+"/") && req.Method == http.MethodGet:
		name, _ := json.Marshal(strings.TrimPrefix(path, httpServicePath+"/"))
		service, method, payload = RegistryService, "describe", []byte(`{"service":`+string(name)+`}`)
	case strings.HasPrefix(path, httpCallPath):
		if req.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeHTTPError(w, routerHTTPError("invalid_argument", "calls are POST requests", "http_method"))
			return
		}
		rest := strings.TrimPrefix(path, httpCallPath)
		slash := strings.LastIndexByte(rest, '/')
		if slash <= 0 || slash == len(rest)-1 {
			writeHTTPError(w, routerHTTPError("invalid_argument", "expected /v1/call/{service}/{method}", "http_path"))
			return
		}
		service, method = rest[:slash], rest[slash+1:]
	default:
		writeHTTPError(w, routerHTTPError("method_not_found", "unknown path "+path, "http_path"))
		return
	}

	user, failure := r.config.Auth.check(req)
	if failure != nil {
		if failure.challenge {
			w.Header().Set("WWW-Authenticate", `Basic realm="meshcall", charset="UTF-8"`)
		}
		code := "unauthenticated"
		if failure.status == http.StatusServiceUnavailable {
			code = "unavailable"
		}
		writeHTTPError(w, &errorPayload{Code: code, Message: failure.message, Retryable: code == "unavailable"})
		return
	}
	if !user.allowsRole("client") {
		writeHTTPError(w, &errorPayload{Code: "permission_denied", Message: "the identity may not act as a client"})
		return
	}
	if openapi {
		r.serveOpenAPI(w, user)
		return
	}

	r.mu.Lock()
	stream := r.methodStream(service, method)
	r.mu.Unlock()
	if stream == "duplex" {
		writeHTTPError(w, routerHTTPError("method_not_found", "duplex methods are WebSocket-only", "duplex_over_http"))
		return
	}

	var body *bufio.Reader
	contentType := strings.TrimSpace(strings.Split(req.Header.Get("Content-Type"), ";")[0])
	if stream == "client_stream" {
		if contentType != ndjson {
			writeHTTPErrorStatus(w, http.StatusUnsupportedMediaType, routerHTTPError("invalid_argument", "a client stream takes "+ndjson, "http_content_type"))
			return
		}
		body = bufio.NewReaderSize(req.Body, 64*1024)
		r.readDeadline(w)
		line, err := readLine(body, r.config.MaxFrameSize)
		if err != nil && !errors.Is(err, io.EOF) {
			writeHTTPError(w, routerHTTPError("invalid_argument", "the first line must be the request payload: "+err.Error(), "http_body"))
			return
		}
		payload = line
	} else if payload == nil {
		// The content type is not checked: the body must be JSON either way,
		// and `curl -d` labels JSON as a form.
		r.readDeadline(w)
		data, err := io.ReadAll(io.LimitReader(req.Body, int64(r.config.MaxFrameSize)+1))
		if err != nil || len(data) > r.config.MaxFrameSize {
			writeHTTPError(w, routerHTTPError("invalid_argument", "the request body is unreadable or too large", "http_body"))
			return
		}
		payload = data
	}
	if len(bytes.TrimSpace(payload)) == 0 {
		payload = []byte(`{}`)
	}
	if !json.Valid(payload) {
		writeHTTPError(w, routerHTTPError("invalid_argument", "the request payload is not JSON", "http_body"))
		return
	}

	open, err := r.httpOpenFrame(service, method, stream, payload, req.Header)
	if err != nil {
		writeHTTPError(w, routerHTTPError("invalid_argument", err.Error(), "http_header"))
		return
	}

	p, release, ok := r.virtualClient(req.Context(), user)
	if !ok {
		writeHTTPError(w, &errorPayload{Code: "unavailable", Message: "Router is shutting down", Retryable: true})
		return
	}
	defer release()
	r.forward(p, open)

	_ = http.NewResponseController(w).SetReadDeadline(time.Time{}) // waiting for the instance is not the client's delay
	c := &httpCall{r: r, p: p, id: open.CallID, w: w, req: req}
	switch stream {
	case "server_stream":
		c.serverStream()
	case "client_stream":
		c.clientStream(body)
	default: // unary, management services, and unknown methods (the Router answers)
		c.unary()
	}
}

// httpOpenFrame builds and validates the call.open for a request, applying
// the Router's HTTP default and maximum timeouts.
func (r *Router) httpOpenFrame(service, method, stream string, payload []byte, h http.Header) (frame, error) {
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return frame{}, err
	}
	open := map[string]any{
		"kind": "call.open", "call_id": "http-" + hex.EncodeToString(id),
		"service": service, "method": method, "payload": json.RawMessage(payload),
	}
	for header, field := range map[string]string{"MeshCall-Timeout-Ms": "timeout_ms", "MeshCall-Idle-Timeout-Ms": "idle_timeout_ms"} {
		if v := h.Get(header); v != "" {
			ms, err := strconv.ParseInt(v, 10, 64)
			if err != nil || ms <= 0 {
				return frame{}, errors.New(header + " must be a positive integer")
			}
			open[field] = ms
		}
	}
	if _, given := open["timeout_ms"]; !given && stream != "server_stream" {
		// HTTP callers often send no timeout; a stuck instance must not hold
		// them forever. Streams of updates may legitimately run indefinitely.
		open["timeout_ms"] = r.config.HTTPDefaultTimeout.Milliseconds()
	}
	if ms, ok := open["timeout_ms"].(int64); ok && r.config.HTTPMaxTimeout > 0 && ms > r.config.HTTPMaxTimeout.Milliseconds() {
		return frame{}, errors.New("MeshCall-Timeout-Ms exceeds the Router's maximum of " + strconv.FormatInt(r.config.HTTPMaxTimeout.Milliseconds(), 10))
	}
	if v := h.Get("Idempotency-Key"); v != "" {
		if len(v) > 128 {
			return frame{}, errors.New("Idempotency-Key must be at most 128 bytes")
		}
		open["idempotency_key"] = v
	}
	if v := h.Get("MeshCall-Instance"); v != "" {
		open["instance_id"] = v
	}
	metadata := map[string]string{}
	for name, values := range h {
		if strings.HasPrefix(name, metaHeader) && len(name) > len(metaHeader) && len(values) > 0 {
			metadata[strings.ToLower(name[len(metaHeader):])] = values[0]
		}
	}
	if len(metadata) > 0 {
		open["metadata"] = metadata
	}
	data, err := json.Marshal(open)
	if err != nil {
		return frame{}, err
	}
	f, err := decode(data)
	if err != nil {
		return frame{}, err
	}
	return f, nil
}

// virtualClient registers a client peer for one HTTP request. release
// forgets it; any route it still owns is cancelled towards the instance,
// exactly as when a WebSocket client disconnects.
func (r *Router) virtualClient(parent context.Context, user *User) (*peer, func(), bool) {
	var ctx context.Context
	var cancel context.CancelFunc
	if user != nil && !user.expires.IsZero() {
		ctx, cancel = context.WithDeadline(parent, user.expires)
	} else {
		ctx, cancel = context.WithCancel(parent)
	}
	id := make([]byte, 16)
	_, _ = rand.Read(id)
	p := &peer{ctx: ctx, cancel: cancel, user: user, role: "client", id: hex.EncodeToString(id),
		out: newOutbox(r.config.OutboxSize), drops: &r.metrics.outboxDrops}
	r.mu.Lock()
	if r.closed || !r.config.Auth.valid(user) {
		r.mu.Unlock()
		cancel()
		return nil, nil, false
	}
	r.peers[p] = struct{}{}
	r.wg.Add(1)
	r.mu.Unlock()
	go func() { // the Router shutting down ends the request too
		select {
		case <-r.ctx.Done():
			cancel()
		case <-ctx.Done():
		}
	}()
	return p, func() {
		cancel()
		p.out.close()
		r.remove(p)
		r.wg.Done()
	}, true
}

type httpCall struct {
	r   *Router
	p   *peer
	id  string
	w   http.ResponseWriter
	req *http.Request
}

var errHTTPGone = errors.New("request ended")

// next returns the next frame the Router routed to this request.
func (c *httpCall) next() (frame, error) {
	for {
		if f, ok := c.p.out.pop(); ok {
			return f, nil
		}
		select {
		case <-c.p.out.ready:
		case <-c.p.ctx.Done():
			return frame{}, errHTTPGone
		}
	}
}

func (c *httpCall) send(f frame) {
	f.CallID = c.id
	c.r.forward(c.p, f)
}

func (c *httpCall) setTrailerHeaders(metadata json.RawMessage, prefix string) {
	var trailers map[string]string
	if json.Unmarshal(metadata, &trailers) != nil {
		return
	}
	for k, v := range trailers {
		c.w.Header().Set(prefix+trailerHeader+k, v)
	}
}

func (c *httpCall) unary() {
	for {
		f, err := c.next()
		if err != nil {
			return
		}
		switch f.Kind {
		case "call.result":
			c.setTrailerHeaders(f.Metadata, "")
			c.w.Header().Set("Content-Type", "application/json")
			c.w.WriteHeader(http.StatusOK)
			_, _ = c.w.Write(compactJSON(f.Payload))
			_, _ = c.w.Write([]byte("\n"))
			return
		case "call.error":
			c.setTrailerHeaders(f.Metadata, "")
			writeHTTPError(c.w, f.Error)
			return
		}
	}
}

// serverStream writes one typed envelope per line: {"item": …} for each
// item, then exactly one {"end": {"trailers": …}} or {"error": …}. Typed
// lines leave no ambiguity between an item and the stream's end, and need no
// HTTP trailers (unreadable by browser fetch, dropped by many proxies).
//
// Credit is granted only for items already written, so a slow reader slows
// the instance down (Flush blocks once TCP's window is full) instead of
// filling Router memory. An error before the first item is an ordinary
// error response with its status code.
func (c *httpCall) serverStream() {
	flusher, _ := c.w.(http.Flusher)
	credit := int64(streamCredit)
	c.send(frame{Kind: "stream.window", Direction: "server", Credit: &credit})
	started, written := false, 0
	line := func(parts ...[]byte) {
		if !started {
			started = true
			c.w.Header().Set("Content-Type", ndjson)
			c.w.Header().Set("X-Accel-Buffering", "no")
			c.w.WriteHeader(http.StatusOK)
		}
		for _, part := range parts {
			_, _ = c.w.Write(part)
		}
		_, _ = c.w.Write([]byte("\n"))
		if flusher != nil {
			flusher.Flush()
		}
	}
	trailers := func(metadata json.RawMessage) []byte {
		if len(metadata) == 0 || string(metadata) == "null" {
			return []byte("{}")
		}
		return compactJSON(metadata)
	}
	for {
		f, err := c.next()
		if err != nil {
			return
		}
		switch f.Kind {
		case "stream.item":
			line([]byte(`{"item":`), compactJSON(f.Payload), []byte("}"))
			if written++; written%(streamCredit/2) == 0 {
				more := int64(streamCredit / 2)
				c.send(frame{Kind: "stream.window", Direction: "server", Credit: &more})
			}
		case "stream.end":
			// The terminal frame follows with the trailers.
		case "call.result":
			line([]byte(`{"end":{"trailers":`), trailers(f.Metadata), []byte("}}"))
			return
		case "call.error":
			if !started {
				c.setTrailerHeaders(f.Metadata, "")
				writeHTTPError(c.w, f.Error)
				return
			}
			e, _ := json.Marshal(f.Error)
			line([]byte(`{"error":`), e, []byte(`,"trailers":`), trailers(f.Metadata), []byte("}"))
			return
		}
	}
}

// clientStream reads one body line per credit the instance grants: while it
// has no credit it does not read, the unread bytes stay in the kernel
// receive buffer, TCP's window closes and the sender's writes block.
func (c *httpCall) clientStream(body *bufio.Reader) {
	type line struct {
		data []byte
		err  error
	}
	want := make(chan struct{})
	lines := make(chan line)
	go func() {
		defer close(lines)
		for range want {
			// The idle clock runs only while the Router wants a line: time
			// spent waiting for credit is the instance's, not the sender's.
			c.r.readDeadline(c.w)
			data, err := readLine(body, c.r.config.MaxFrameSize)
			for err == nil && len(bytes.TrimSpace(data)) == 0 {
				data, err = readLine(body, c.r.config.MaxFrameSize) // blank lines carry nothing
			}
			_ = http.NewResponseController(c.w).SetReadDeadline(time.Time{})
			select {
			case lines <- line{data, err}:
			case <-c.p.ctx.Done():
				return
			}
		}
	}()
	defer close(want)
	credit, sequence, ended, reading := int64(0), int64(0), false, false
	fail := func(message string) {
		c.send(frame{Kind: "call.cancel", Reason: "invalid_input"})
		writeHTTPError(c.w, routerHTTPError("invalid_argument", message, "http_body"))
	}
	for {
		if credit > 0 && !ended && !reading {
			reading = true
			select {
			case want <- struct{}{}:
			case <-c.p.ctx.Done():
				return
			}
		}
		var incoming <-chan line
		if reading {
			incoming = lines
		}
		select {
		case l, ok := <-incoming:
			reading = false
			if !ok {
				return
			}
			if errors.Is(l.err, io.EOF) && len(bytes.TrimSpace(l.data)) == 0 {
				ended = true
				c.send(frame{Kind: "stream.end", Direction: "client"})
				continue
			}
			if l.err != nil && !errors.Is(l.err, io.EOF) {
				fail("input stream: " + l.err.Error())
				return
			}
			if !json.Valid(l.data) {
				fail("input item " + strconv.FormatInt(sequence, 10) + " is not JSON")
				return
			}
			seq := sequence
			c.send(frame{Kind: "stream.item", Direction: "client", Sequence: &seq, Payload: compactJSON(l.data), raw: nil})
			sequence++
			credit--
			if errors.Is(l.err, io.EOF) { // last line without a trailing newline
				ended = true
				c.send(frame{Kind: "stream.end", Direction: "client"})
			}
		case <-c.p.out.ready:
			for {
				f, ok := c.p.out.pop()
				if !ok {
					break
				}
				switch f.Kind {
				case "stream.window":
					if f.Credit != nil {
						credit += *f.Credit
					}
				case "call.result":
					c.setTrailerHeaders(f.Metadata, "")
					c.w.Header().Set("Content-Type", "application/json")
					c.w.WriteHeader(http.StatusOK)
					_, _ = c.w.Write(compactJSON(f.Payload))
					_, _ = c.w.Write([]byte("\n"))
					return
				case "call.error":
					c.setTrailerHeaders(f.Metadata, "")
					writeHTTPError(c.w, f.Error)
					return
				}
			}
		case <-c.p.ctx.Done():
			return
		}
	}
}

// readDeadline bounds the next body read by the idle timeout, so a sender
// that stops mid-body cannot hold a connection slot forever.
func (r *Router) readDeadline(w http.ResponseWriter) {
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(r.config.IdleTimeout))
}

// readLine reads one newline-terminated line of at most limit bytes.
func readLine(r *bufio.Reader, limit int) ([]byte, error) {
	var out []byte
	for {
		chunk, err := r.ReadSlice('\n')
		out = append(out, chunk...)
		if len(out) > limit {
			return nil, errors.New("line exceeds the maximum frame size")
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return bytes.TrimRight(out, "\r\n"), err
	}
}

func compactJSON(raw []byte) []byte {
	if len(raw) == 0 {
		return []byte("null")
	}
	var b bytes.Buffer
	if json.Compact(&b, raw) != nil {
		return raw
	}
	return b.Bytes()
}
