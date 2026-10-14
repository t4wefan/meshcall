package router

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

func TestHTTPServerStreamAsNDJSONAndSSE(t *testing.T) {
	r := runningRouter(t, nil)
	inst := fakeInstance(t, r)
	run := func(accept string) (*http.Response, string) {
		req, _ := http.NewRequest(http.MethodPost, httpURL(r, "/v1/call/"+testService+"/download"), strings.NewReader(`{"n":2}`))
		req.Header.Set("Accept", accept)
		done := make(chan *http.Response)
		go func() {
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Error(err)
			}
			done <- resp
		}()
		open := receive(t, inst)
		if w := receive(t, inst); w.Kind != "stream.window" || *w.Credit != streamCredit {
			t.Fatalf("expected an initial window of %d, got %+v", streamCredit, w)
		}
		for i := range 2 {
			seq := int64(i)
			send(t, inst, frame{Kind: "stream.item", CallID: open.CallID, Direction: "server", Sequence: &seq,
				Payload: json.RawMessage(fmt.Sprintf(`{ "i": %d }`, i))})
		}
		send(t, inst, frame{Kind: "stream.end", CallID: open.CallID, Direction: "server"})
		send(t, inst, frame{Kind: "call.result", CallID: open.CallID, Metadata: json.RawMessage(`{"count":"2"}`)})
		resp := <-done
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp, string(body)
	}
	resp, body := run("")
	if resp.Header.Get("Content-Type") != ndjson || body != "{\"i\":0}\n{\"i\":1}\n" || resp.Trailer.Get("MeshCall-Trailer-count") != "2" ||
		resp.Trailer.Get("MeshCall-Status") != "ok" {
		t.Fatalf("ndjson: %q %q trailer=%v", resp.Header.Get("Content-Type"), body, resp.Trailer)
	}
	resp, body = run(eventStream)
	want := "event: item\ndata: {\"i\":0}\n\nevent: item\ndata: {\"i\":1}\n\nevent: end\ndata: {}\n\n"
	if resp.Header.Get("Content-Type") != eventStream || body != want {
		t.Fatalf("sse: %q %q", resp.Header.Get("Content-Type"), body)
	}

	// An error after items arrives as a final line.
	req, _ := http.NewRequest(http.MethodPost, httpURL(r, "/v1/call/"+testService+"/download"), strings.NewReader(`{}`))
	done := make(chan *http.Response)
	go func() { resp, _ := http.DefaultClient.Do(req); done <- resp }()
	open := receive(t, inst)
	receive(t, inst) // window
	seq := int64(0)
	send(t, inst, frame{Kind: "stream.item", CallID: open.CallID, Direction: "server", Sequence: &seq, Payload: json.RawMessage(`1`)})
	send(t, inst, frame{Kind: "call.error", CallID: open.CallID, Error: &errorPayload{Code: "internal", Message: "boom"}})
	resp = <-done
	lines, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.HasSuffix(string(lines), "\"code\":\"internal\",\"message\":\"boom\",\"retryable\":false}}\n") ||
		resp.Trailer.Get("MeshCall-Status") != "internal" {
		t.Fatalf("mid-stream error: %d %q", resp.StatusCode, lines)
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
	resp := post("")
	if resp.Header.Get("WWW-Authenticate") == "" {
		t.Fatal("missing Basic challenge")
	}
	if status, e := httpError(t, resp); status != 401 || e.Code != "unauthenticated" {
		t.Fatalf("anonymous: %d %+v", status, e)
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
