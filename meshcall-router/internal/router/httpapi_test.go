package router

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func httpURL(r *Router, path string) string {
	return fmt.Sprintf("http://127.0.0.1:%d%s", r.Ready().Port, path)
}

// fakeInstance registers testService with one method of each shape.
func fakeInstance(t *testing.T, r *Router) *websocket.Conn {
	t.Helper()
	conn := dial(t, r, "")
	hello(t, conn, "server", "fake")
	send(t, conn, frame{Kind: "server.register", InstanceID: "fake", Services: []service{{Name: testService, Methods: []method{
		{Name: "echo", Stream: "unary", Balance: balance{Kind: "round_robin"}},
		{Name: "download", Stream: "server_stream", Balance: balance{Kind: "round_robin"}},
		{Name: "upload", Stream: "client_stream", Balance: balance{Kind: "round_robin"}},
		{Name: "chat", Stream: "duplex", Balance: balance{Kind: "round_robin"}},
	}}}})
	if got := receive(t, conn); got.Kind != "server.register.ack" {
		t.Fatalf("registration: %+v", got)
	}
	return conn
}

// pump reads frames from conn in the background; a timed Read would close
// the connection when it expires, so "nothing arrived" is asserted on the
// channel instead.
func pump(t *testing.T, conn *websocket.Conn) <-chan frame {
	t.Helper()
	out := make(chan frame, 64)
	go func() {
		defer close(out)
		for {
			_, data, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			f, err := decode(data)
			if err != nil {
				return
			}
			out <- f
		}
	}()
	return out
}

func next(t *testing.T, frames <-chan frame) frame {
	t.Helper()
	select {
	case f, ok := <-frames:
		if !ok {
			t.Fatal("instance connection closed")
		}
		return f
	case <-time.After(3 * time.Second):
		t.Fatal("no frame within 3s")
	}
	return frame{}
}

func quiet(t *testing.T, frames <-chan frame, d time.Duration, why string) {
	t.Helper()
	select {
	case f := <-frames:
		t.Fatalf("%s: got %+v", why, f)
	case <-time.After(d):
	}
}

func httpError(t *testing.T, resp *http.Response) (int, errorPayload) {
	t.Helper()
	defer resp.Body.Close()
	var body struct {
		Error errorPayload `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("error body: %v", err)
	}
	return resp.StatusCode, body.Error
}

func TestHTTPUnaryCallsAndErrors(t *testing.T) {
	r := runningRouter(t, nil)
	inst := fakeInstance(t, r)
	post := func(body string, headers map[string]string) *http.Response {
		req, _ := http.NewRequest(http.MethodPost, httpURL(r, "/v1/call/"+testService+"/echo"), strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	done := make(chan *http.Response)
	go func() {
		done <- post(`{"n": 1}`, map[string]string{
			"MeshCall-Timeout-Ms": "5000", "Idempotency-Key": "k1", "MeshCall-Meta-Trace": "t-9",
		})
	}()
	open := receive(t, inst)
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(open.raw, &fields)
	if open.Kind != "call.open" || string(fields["payload"]) != `{"n":1}` || string(fields["timeout_ms"]) != "5000" ||
		string(fields["idempotency_key"]) != `"k1"` || string(open.Metadata) != `{"trace":"t-9"}` {
		t.Fatalf("headers did not become call.open fields: %s", open.raw)
	}
	send(t, inst, frame{Kind: "call.result", CallID: open.CallID, Payload: json.RawMessage(`{"ok": true}`), Metadata: json.RawMessage(`{"served-by":"fake"}`)})
	resp := <-done
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || strings.TrimSpace(string(body)) != `{"ok":true}` || resp.Header.Get("MeshCall-Trailer-served-by") != "fake" {
		t.Fatalf("unary response: %d %q %v", resp.StatusCode, body, resp.Header)
	}

	// An instance error keeps its envelope and maps to a status.
	go func() { done <- post(`{}`, nil) }()
	open = receive(t, inst)
	send(t, inst, frame{Kind: "call.error", CallID: open.CallID, Error: &errorPayload{Code: "invalid_argument", Message: "bad",
		Details: json.RawMessage(`{"source":"server","reason":"request"}`)}})
	if status, e := httpError(t, <-done); status != 400 || e.Code != "invalid_argument" || !strings.Contains(string(e.Details), `"request"`) {
		t.Fatalf("error: %d %+v", status, e)
	}

	// Router-side failures: unknown method, malformed body, duplex.
	req, _ := http.NewRequest(http.MethodPost, httpURL(r, "/v1/call/"+testService+"/nope"), strings.NewReader(`{}`))
	resp, _ = http.DefaultClient.Do(req)
	if status, e := httpError(t, resp); status != 404 || e.Code != "method_not_found" {
		t.Fatalf("unknown method: %d %+v", status, e)
	}
	if status, e := httpError(t, post(`{not json`, nil)); status != 400 || e.Code != "invalid_argument" {
		t.Fatalf("bad body: %d %+v", status, e)
	}
	req, _ = http.NewRequest(http.MethodPost, httpURL(r, "/v1/call/"+testService+"/chat"), strings.NewReader(`{}`))
	resp, _ = http.DefaultClient.Do(req)
	if status, e := httpError(t, resp); status != 405 || !strings.Contains(string(e.Details), "duplex_over_http") {
		t.Fatalf("duplex: %d %+v", status, e)
	}

	// Discovery mirrors the Registry.
	resp, _ = http.Get(httpURL(r, "/v1/services"))
	var listed struct {
		Services []struct {
			Name string `json:"name"`
		} `json:"services"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&listed)
	resp.Body.Close()
	if resp.StatusCode != 200 || len(listed.Services) != 1 || listed.Services[0].Name != testService {
		t.Fatalf("services: %d %+v", resp.StatusCode, listed)
	}

	// Closing the request cancels the call at the instance.
	ctx, cancel := context.WithCancel(context.Background())
	req, _ = http.NewRequestWithContext(ctx, http.MethodPost, httpURL(r, "/v1/call/"+testService+"/echo"), strings.NewReader(`{}`))
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
		}
	}()
	open = receive(t, inst)
	cancel()
	if got := receive(t, inst); got.Kind != "call.cancel" || got.CallID != open.CallID {
		t.Fatalf("expected call.cancel after the HTTP client left, got %+v", got)
	}
}

func TestHTTPServerStreamWritesTypedEnvelopes(t *testing.T) {
	r := runningRouter(t, nil)
	inst := fakeInstance(t, r)
	open := func() (frame, chan *http.Response) {
		req, _ := http.NewRequest(http.MethodPost, httpURL(r, "/v1/call/"+testService+"/download"), strings.NewReader(`{"n":2}`))
		done := make(chan *http.Response)
		go func() {
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Error(err)
			}
			done <- resp
		}()
		f := receive(t, inst)
		if w := receive(t, inst); w.Kind != "stream.window" || *w.Credit != streamCredit {
			t.Fatalf("expected an initial window of %d, got %+v", streamCredit, w)
		}
		return f, done
	}
	body := func(resp *http.Response) string {
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return string(data)
	}

	call, done := open()
	for i := range 2 {
		seq := int64(i)
		// An item whose own top-level key is "error" stays unambiguous.
		send(t, inst, frame{Kind: "stream.item", CallID: call.CallID, Direction: "server", Sequence: &seq,
			Payload: json.RawMessage(fmt.Sprintf(`{ "error": %d }`, i))})
	}
	send(t, inst, frame{Kind: "stream.end", CallID: call.CallID, Direction: "server"})
	send(t, inst, frame{Kind: "call.result", CallID: call.CallID, Metadata: json.RawMessage(`{"count":"2"}`)})
	resp := <-done
	want := "{\"item\":{\"error\":0}}\n{\"item\":{\"error\":1}}\n{\"end\":{\"trailers\":{\"count\":\"2\"}}}\n"
	if got := body(resp); resp.Header.Get("Content-Type") != ndjson || got != want {
		t.Fatalf("stream: %q %q", resp.Header.Get("Content-Type"), got)
	}

	// An error after items is the final line; the status is already 200.
	call, done = open()
	seq := int64(0)
	send(t, inst, frame{Kind: "stream.item", CallID: call.CallID, Direction: "server", Sequence: &seq, Payload: json.RawMessage(`1`)})
	send(t, inst, frame{Kind: "call.error", CallID: call.CallID, Error: &errorPayload{Code: "internal", Message: "boom"}})
	resp = <-done
	want = "{\"item\":1}\n{\"error\":{\"code\":\"internal\",\"message\":\"boom\",\"retryable\":false},\"trailers\":{}}\n"
	if got := body(resp); resp.StatusCode != 200 || got != want {
		t.Fatalf("mid-stream error: %d %q", resp.StatusCode, got)
	}

	// An error before any item is an ordinary error response.
	call, done = open()
	send(t, inst, frame{Kind: "call.error", CallID: call.CallID, Error: &errorPayload{Code: "invalid_argument", Message: "bad"}})
	if status, e := httpError(t, <-done); status != 400 || e.Code != "invalid_argument" {
		t.Fatalf("early error: %d %+v", status, e)
	}
}

func TestHTTPClientStreamReadsOnlyWhatTheInstanceGranted(t *testing.T) {
	r := runningRouter(t, nil)
	inst := fakeInstance(t, r)
	bodyReader, bodyWriter := io.Pipe()
	req, _ := http.NewRequest(http.MethodPost, httpURL(r, "/v1/call/"+testService+"/upload"), bodyReader)
	req.Header.Set("Content-Type", ndjson)
	done := make(chan *http.Response)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Error(err)
		}
		done <- resp
	}()
	lines := bufio.NewWriter(bodyWriter)
	write := func(s string) {
		_, _ = lines.WriteString(s + "\n")
		_ = lines.Flush()
	}
	write(`{"offset": 10}`)
	write(`{"v":1}`)
	write(`{"v":2}`)
	frames := pump(t, inst)
	open := next(t, frames)
	if string(open.Payload) != `{"offset":10}` {
		t.Fatalf("first line must be the request: %s", open.Payload)
	}
	quiet(t, frames, 200*time.Millisecond, "an item was forwarded before the instance granted credit")
	one := int64(1)
	send(t, inst, frame{Kind: "stream.window", CallID: open.CallID, Direction: "client", Credit: &one})
	if got := next(t, frames); got.Kind != "stream.item" || *got.Sequence != 0 || string(got.Payload) != `{"v":1}` {
		t.Fatalf("first item: %+v", got)
	}
	quiet(t, frames, 200*time.Millisecond, "a second item was forwarded on one credit")
	send(t, inst, frame{Kind: "stream.window", CallID: open.CallID, Direction: "client", Credit: &one})
	if got := next(t, frames); got.Kind != "stream.item" || *got.Sequence != 1 {
		t.Fatalf("second item: %+v", got)
	}
	_ = bodyWriter.Close()
	send(t, inst, frame{Kind: "stream.window", CallID: open.CallID, Direction: "client", Credit: &one})
	if got := next(t, frames); got.Kind != "stream.end" {
		t.Fatalf("expected stream.end at the end of the body, got %+v", got)
	}
	send(t, inst, frame{Kind: "call.result", CallID: open.CallID, Payload: json.RawMessage(`{"total":13}`)})
	resp := <-done
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || strings.TrimSpace(string(body)) != `{"total":13}` {
		t.Fatalf("client stream result: %d %q", resp.StatusCode, body)
	}

	// Wrong content type for a client stream.
	req, _ = http.NewRequest(http.MethodPost, httpURL(r, "/v1/call/"+testService+"/upload"), strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	resp, _ = http.DefaultClient.Do(req)
	if status, e := httpError(t, resp); status != 415 || e.Code != "invalid_argument" {
		t.Fatalf("content type: %d %+v", status, e)
	}
}

func TestHTTPAuthUsesTheSameAccounts(t *testing.T) {
	r := runningRouter(t, testAuth(t))
	post := func(authorization string) *http.Response {
		req, _ := http.NewRequest(http.MethodPost, httpURL(r, "/v1/call/"+testService+"/echo"), strings.NewReader(`{}`))
		if authorization != "" {
			req.Header.Set("Authorization", authorization)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	// No credentials: the anonymous user, which testAuth grants nothing, so
	// the call is refused exactly like any other forbidden call.
	if status, e := httpError(t, post("")); status != 403 || e.Code != "permission_denied" {
		t.Fatalf("anonymous: %d %+v", status, e)
	}
	// Wrong credentials are refused with a challenge, never made anonymous.
	resp := post(basicHeader("reader", "wrong-password"))
	if resp.Header.Get("WWW-Authenticate") == "" {
		t.Fatal("missing Basic challenge")
	}
	if status, e := httpError(t, resp); status != 401 || e.Code != "unauthenticated" {
		t.Fatalf("wrong password: %d %+v", status, e)
	}
	// reader may call echo; with nothing registered the Router says so.
	if status, e := httpError(t, post(basicHeader("reader", "test-only-password"))); status != 503 || e.Code != "unavailable" {
		t.Fatalf("authorised, no instance: %d %+v", status, e)
	}
	// reader may not call download.
	req, _ := http.NewRequest(http.MethodPost, httpURL(r, "/v1/call/"+testService+"/download"), strings.NewReader(`{}`))
	req.Header.Set("Authorization", basicHeader("reader", "test-only-password"))
	resp, _ = http.DefaultClient.Do(req)
	if status, e := httpError(t, resp); status != 403 || e.Code != "permission_denied" {
		t.Fatalf("not permitted: %d %+v", status, e)
	}
}

func TestConnectionLimitCountsWebSocketsOnly(t *testing.T) {
	r, err := Start(Config{MaxConnections: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	a, b := dial(t, r, ""), dial(t, r, "")
	hello(t, a, "client", "a")
	hello(t, b, "client", "b")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, resp, err := websocket.Dial(ctx, fmt.Sprintf("ws://127.0.0.1:%d", r.Ready().Port), nil)
	if err == nil || resp == nil || resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("third WebSocket: err=%v resp=%v", err, resp)
	}
	// HTTP calls are not counted: with both slots taken, the call still
	// reaches routing (and finds no instance).
	httpResp, err := http.Post(httpURL(r, "/v1/call/"+testService+"/echo"), "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if status, e := httpError(t, httpResp); status != 503 || !strings.Contains(string(e.Details), "no_instances") {
		t.Fatalf("HTTP should bypass the WebSocket limit: %d %+v", status, e)
	}
	// Closing a WebSocket frees its slot.
	_ = a.CloseNow()
	eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		conn, _, err := websocket.Dial(ctx, fmt.Sprintf("ws://127.0.0.1:%d", r.Ready().Port), nil)
		if err != nil {
			return false
		}
		_ = conn.CloseNow()
		return true
	})
}

func TestHTTPBodyThatStopsIsCutOffByTheIdleTimeout(t *testing.T) {
	r, err := Start(Config{IdleTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	bodyReader, bodyWriter := io.Pipe()
	t.Cleanup(func() { _ = bodyWriter.Close() })
	req, _ := http.NewRequest(http.MethodPost, httpURL(r, "/v1/call/"+testService+"/echo"), bodyReader)
	go func() { _, _ = bodyWriter.Write([]byte(`{"partial":`)) }() // and then nothing
	started := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		status, e := httpError(t, resp)
		if status != 400 || e.Code != "invalid_argument" {
			t.Fatalf("stalled body: %d %+v", status, e)
		}
	}
	if elapsed := time.Since(started); elapsed > 4*time.Second {
		t.Fatalf("a stalled body held the request for %v", elapsed)
	}
}

func TestHTTPTimeoutsLimitsCORSAndHealth(t *testing.T) {
	r, err := Start(Config{HTTPDefaultTimeout: 2 * time.Second, HTTPMaxTimeout: 5 * time.Second, HTTPMaxInflight: 1,
		HTTPCORSOrigins: []string{"https://app.example"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	inst := fakeInstance(t, r)
	frames := pump(t, inst)

	// The default timeout is applied to unary calls, not to server streams.
	done := make(chan *http.Response, 2)
	go func() {
		resp, _ := http.Post(httpURL(r, "/v1/call/"+testService+"/echo"), "application/json", strings.NewReader(`{}`))
		done <- resp
	}()
	open := next(t, frames)
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(open.raw, &fields)
	if string(fields["timeout_ms"]) != "2000" {
		t.Fatalf("default timeout not applied: %s", open.raw)
	}
	// While that call holds the only HTTP slot, the next one is refused.
	resp, _ := http.Post(httpURL(r, "/v1/call/"+testService+"/echo"), "application/json", strings.NewReader(`{}`))
	if status, e := httpError(t, resp); status != 429 || e.Code != "resource_exhausted" || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("over the HTTP limit: %d %+v", status, e)
	}
	send(t, inst, frame{Kind: "call.result", CallID: open.CallID, Payload: json.RawMessage(`{}`)})
	(<-done).Body.Close()

	go func() {
		resp, _ := http.Post(httpURL(r, "/v1/call/"+testService+"/download"), "application/json", strings.NewReader(`{}`))
		done <- resp
	}()
	open = next(t, frames)
	fields = nil
	_ = json.Unmarshal(open.raw, &fields)
	if _, has := fields["timeout_ms"]; has {
		t.Fatalf("server streams get no default timeout: %s", open.raw)
	}
	next(t, frames) // window
	send(t, inst, frame{Kind: "call.result", CallID: open.CallID})
	(<-done).Body.Close()

	// Asking for more than the maximum is refused, not silently shortened.
	req, _ := http.NewRequest(http.MethodPost, httpURL(r, "/v1/call/"+testService+"/echo"), strings.NewReader(`{}`))
	req.Header.Set("MeshCall-Timeout-Ms", "60000")
	resp, _ = http.DefaultClient.Do(req)
	if status, e := httpError(t, resp); status != 400 || !strings.Contains(e.Message, "maximum") {
		t.Fatalf("over the maximum timeout: %d %+v", status, e)
	}

	// CORS: the listed origin gets a preflight and headers; others do not.
	preflight := func(origin string) *http.Response {
		req, _ := http.NewRequest(http.MethodOptions, httpURL(r, "/v1/call/"+testService+"/echo"), nil)
		req.Header.Set("Origin", origin)
		req.Header.Set("Access-Control-Request-Method", "POST")
		req.Header.Set("Access-Control-Request-Headers", "authorization, content-type")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp
	}
	ok := preflight("https://app.example")
	if ok.StatusCode != 204 || ok.Header.Get("Access-Control-Allow-Origin") != "https://app.example" ||
		!strings.Contains(ok.Header.Get("Access-Control-Allow-Headers"), "Authorization") {
		t.Fatalf("allowed preflight: %d %v", ok.StatusCode, ok.Header)
	}
	if bad := preflight("https://evil.example"); bad.StatusCode != 403 || bad.Header.Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("refused preflight: %d %v", bad.StatusCode, bad.Header)
	}

	// Health needs no credentials and reveals nothing.
	for path, want := range map[string]string{"/healthz": `{"status":"ok"}`, "/readyz": `{"status":"ready"}`} {
		resp, err := http.Get(httpURL(r, path))
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 || strings.TrimSpace(string(body)) != want {
			t.Fatalf("%s: %d %q", path, resp.StatusCode, body)
		}
	}
}

func TestOpenAPIIsGeneratedFromVisibleContracts(t *testing.T) {
	doc, err := os.ReadFile("../codegen/testdata/showcase/contract.json")
	if err != nil {
		t.Fatal(err)
	}
	var parsed struct {
		Services []json.RawMessage `json:"services"`
	}
	_ = json.Unmarshal(doc, &parsed)
	entry := parsed.Services[0]
	var meta struct {
		Name    string `json:"name"`
		Methods []struct {
			Name   string `json:"name"`
			Stream string `json:"stream"`
		} `json:"methods"`
	}
	_ = json.Unmarshal(entry, &meta)

	r := runningRouter(t, nil)
	conn := dial(t, r, "")
	hello(t, conn, "server", "showcase")
	methods := make([]method, 0, len(meta.Methods))
	for _, m := range meta.Methods {
		methods = append(methods, method{Name: m.Name, Stream: m.Stream, Balance: balance{Kind: "round_robin"}})
	}
	send(t, conn, frame{Kind: "server.register", InstanceID: "showcase", Services: []service{{Name: meta.Name, Methods: methods, Contract: entry}}})
	receive(t, conn)

	resp, err := http.Get(httpURL(r, "/v1/openapi.json"))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var api map[string]any
	if err := json.Unmarshal(body, &api); err != nil || api["openapi"] != "3.1.0" {
		t.Fatalf("not an OpenAPI document: %v %.200s", err, body)
	}
	paths, _ := api["paths"].(map[string]any)
	if len(paths) != len(meta.Methods) {
		t.Fatalf("%d paths for %d methods", len(paths), len(meta.Methods))
	}
	// Every $ref resolves inside the document; none still points at $defs.
	schemas, _ := api["components"].(map[string]any)["schemas"].(map[string]any)
	refs := 0
	var walk func(any)
	walk = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			if ref, ok := x["$ref"].(string); ok {
				refs++
				name, found := strings.CutPrefix(ref, "#/components/schemas/")
				if !found || schemas[name] == nil {
					t.Fatalf("dangling $ref %q", ref)
				}
			}
			if _, ok := x["$defs"]; ok {
				t.Fatalf("$defs left in place: %v", x)
			}
			for _, child := range x {
				walk(child)
			}
		case []any:
			for _, child := range x {
				walk(child)
			}
		}
	}
	walk(api)
	if refs == 0 {
		t.Fatal("expected the showcase's nested types to produce $refs")
	}
	if !bytes.Contains(body, []byte(`"x-meshcall-stream":"server_stream"`)) || !bytes.Contains(body, []byte(`application/x-ndjson`)) {
		t.Fatalf("streams are not described: %.300s", body)
	}
}
