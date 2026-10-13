package router

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

const testService = "test.v1.Echo"

func basicHeader(username, password string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
}

var passwordHash = sync.OnceValue(func() string {
	hash, err := HashPassword("test-only-password")
	if err != nil {
		panic(err)
	}
	return hash
})

func testAuth(t *testing.T) *Auth {
	t.Helper()
	config := map[string]any{"users": []map[string]any{
		{"username": "issuer", "password_hash": passwordHash(), "roles": []string{"client", "server"},
			"register": []string{testService}, "call": []string{testService + "/*", AuthService + "/*"}},
		{"username": "reader", "password_hash": passwordHash(), "roles": []string{"client"},
			"call": []string{testService + "/echo", AuthService + "/*"}},
	}}
	data, _ := json.Marshal(config)
	path := filepath.Join(t.TempDir(), "auth.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	auth, err := LoadAuth(path)
	if err != nil {
		t.Fatal(err)
	}
	return auth
}

func runningRouter(t *testing.T, auth *Auth) *Router {
	t.Helper()
	r, err := Start(Config{Auth: auth})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	return r
}

func dial(t *testing.T, r *Router, authorization string) *websocket.Conn {
	t.Helper()
	header := http.Header{}
	if authorization != "" {
		header.Set("Authorization", authorization)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, fmt.Sprintf("ws://127.0.0.1:%d", r.Ready().Port), &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	return conn
}

func send(t *testing.T, conn *websocket.Conn, f frame) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, f.bytes()); err != nil {
		t.Fatal(err)
	}
}

func receive(t *testing.T, conn *websocket.Conn) frame {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var f frame
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatal(err)
	}
	f.raw = data
	return f
}

func hello(t *testing.T, conn *websocket.Conn, role, id string) {
	t.Helper()
	send(t, conn, frame{Kind: "hello", Protocol: Protocol, Role: role, PeerID: id, InstanceID: id})
	if got := receive(t, conn); got.Kind != "hello.ack" {
		t.Fatalf("hello: %+v", got)
	}
}

func registerPeer(t *testing.T, conn *websocket.Conn, id string) {
	t.Helper()
	hello(t, conn, "server", id)
	send(t, conn, frame{Kind: "server.register", InstanceID: id, Services: []service{
		{Name: testService, Methods: []method{
			{Name: "echo", Stream: "unary", Balance: balance{Kind: "round_robin"}},
			{Name: "download", Stream: "server_stream", Balance: balance{Kind: "round_robin"}},
		}},
	}})
	if got := receive(t, conn); got.Kind != "server.register.ack" {
		t.Fatalf("registration: %+v", got)
	}
}

func eventually(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not become true")
}

func TestRoutingOwnershipAndDisconnectCleanup(t *testing.T) {
	r := runningRouter(t, nil)
	a, b := dial(t, r, ""), dial(t, r, "")
	registerPeer(t, a, "a")
	registerPeer(t, b, "b")
	client, intruder := dial(t, r, ""), dial(t, r, "")
	hello(t, client, "client", "client")
	hello(t, intruder, "client", "intruder")
	open := frame{Kind: "call.open", CallID: "call-1", Service: testService, Method: "echo",
		Payload: json.RawMessage(`{"integer":12345678901234567890,"text":"中文"}`)}
	send(t, client, open)
	if got := receive(t, a); !bytes.Equal(got.raw, open.bytes()) {
		t.Fatal("payload was re-encoded")
	}
	send(t, intruder, frame{Kind: "call.cancel", CallID: open.CallID})
	send(t, intruder, frame{Kind: "ping", Nonce: "barrier"})
	if receive(t, intruder).Kind != "pong" {
		t.Fatal("ping")
	}
	credit := int64(16)
	send(t, client, frame{Kind: "stream.window", CallID: open.CallID, Direction: "server", Credit: &credit})
	if got := receive(t, a); got.Kind != "stream.window" {
		t.Fatal("another client hijacked the route")
	}
	send(t, b, frame{Kind: "call.result", CallID: open.CallID, Payload: json.RawMessage(`"forged"`)})
	send(t, b, frame{Kind: "ping", Nonce: "barrier"})
	receive(t, b)
	send(t, a, frame{Kind: "call.result", CallID: open.CallID, Payload: json.RawMessage(`"real"`)})
	if got := receive(t, client); string(got.Payload) != `"real"` {
		t.Fatal("another server hijacked the route")
	}
	eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return len(r.routes) == 0 && r.instances["a"].inflight == 0
	})
	open.CallID = "call-2"
	send(t, client, open)
	if got := receive(t, b); got.CallID != open.CallID {
		t.Fatal("round robin")
	}
	_ = b.CloseNow()
	if got := receive(t, client); got.Error == nil || got.Error.Code != "unavailable" || !got.Error.Retryable {
		t.Fatal("missing disconnect error")
	}
	open.CallID = "call-3"
	send(t, client, open)
	receive(t, a)
	_ = client.CloseNow()
	if got := receive(t, a); got.Kind != "call.cancel" || got.Reason != "client_disconnected" {
		t.Fatal("missing disconnect cancellation")
	}
	eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return len(r.routes) == 0 && r.instances["a"].inflight == 0
	})
}

func TestAccountAndTokenScopesExpiryRevocation(t *testing.T) {
	auth := testAuth(t)
	r := runningRouter(t, auth)
	for _, header := range []string{"", basicHeader("issuer", "wrong-password"), "Bearer invalid"} {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		conn, response, err := websocket.Dial(ctx, fmt.Sprintf("ws://127.0.0.1:%d", r.Ready().Port),
			&websocket.DialOptions{HTTPHeader: http.Header{"Authorization": []string{header}}})
		cancel()
		if conn != nil {
			_ = conn.CloseNow()
		}
		if err == nil || response == nil || response.StatusCode != 401 {
			t.Fatal("unauthenticated connection accepted")
		}
	}
	issuer := dial(t, r, basicHeader("issuer", "test-only-password"))
	hello(t, issuer, "client", "issuer")
	rpc := func(conn *websocket.Conn, method string, payload any) frame {
		data, _ := json.Marshal(payload)
		send(t, conn, frame{Kind: "call.open", CallID: "management", Service: AuthService, Method: method, Payload: data})
		return receive(t, conn)
	}
	issue := func(ttl int, scopes []Scope) IssueTokenResult {
		t.Helper()
		response := rpc(issuer, "issue_token", IssueTokenRequest{TTLSeconds: ttl, Scopes: scopes})
		if response.Error != nil {
			t.Fatalf("issue token: %+v", response.Error)
		}
		var token IssueTokenResult
		if err := json.Unmarshal(response.Payload, &token); err != nil {
			t.Fatal(err)
		}
		return token
	}
	scope := []Scope{{Service: testService, Methods: []string{"echo"}}}
	token := issue(60, scope)
	c := dial(t, r, "Bearer "+token.Token)
	hello(t, c, "client", "scoped")
	if got := rpc(c, "whoami", map[string]any{}); got.Kind != "call.result" {
		t.Fatal("identity")
	}
	for _, target := range []struct{ service, method string }{
		{testService, "download"}, {"other.v1.Service", "echo"}, {AuthService, "issue_token"},
	} {
		send(t, c, frame{Kind: "call.open", CallID: "denied", Service: target.service, Method: target.method, Payload: json.RawMessage("{}")})
		if got := receive(t, c); got.Error == nil || got.Error.Code != "permission_denied" {
			t.Fatalf("scope escaped: %+v", got)
		}
	}
	worker := dial(t, r, basicHeader("issuer", "test-only-password"))
	registerPeer(t, worker, "worker")
	send(t, c, frame{Kind: "call.open", CallID: "allowed", Service: testService, Method: "echo", Payload: json.RawMessage("{}")})
	if got := receive(t, worker); got.CallID != "allowed" {
		t.Fatal("scoped call not routed")
	}
	send(t, worker, frame{Kind: "call.result", CallID: "allowed", Payload: json.RawMessage("{}")})
	receive(t, c)
	reader := dial(t, r, basicHeader("reader", "test-only-password"))
	hello(t, reader, "client", "reader")
	if got := rpc(reader, "issue_token", IssueTokenRequest{TTLSeconds: 60, Scopes: []Scope{{Service: testService, Methods: []string{"*"}}}}); got.Error == nil || got.Error.Code != "permission_denied" {
		t.Fatal("issuer widened permissions")
	}
	if got := rpc(reader, "revoke_token", map[string]any{"token_id": token.TokenID}); string(got.Payload) != `{"revoked":false}` {
		t.Fatal("another account revoked token")
	}
	send(t, c, frame{Kind: "call.open", CallID: "active", Service: testService, Method: "echo", Payload: json.RawMessage("{}")})
	receive(t, worker)
	if got := rpc(issuer, "revoke_token", map[string]any{"token_id": token.TokenID}); string(got.Payload) != `{"revoked":true}` {
		t.Fatal("revocation")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, _, err := c.Read(ctx); err == nil {
		t.Fatal("revoked connection survived")
	}
	if got := receive(t, worker); got.Kind != "call.cancel" || got.CallID != "active" {
		t.Fatal("revoked call leaked")
	}
	short := issue(1, scope)
	shortConn := dial(t, r, "Bearer "+short.Token)
	hello(t, shortConn, "client", "short")
	if _, _, err := shortConn.Read(ctx); err == nil {
		t.Fatal("expired connection survived")
	}
	if auth.valid(auth.tokens[short.TokenID]) {
		t.Fatal("expired token still valid")
	}
}

func TestRegistrationPermissionsAndReservedServices(t *testing.T) {
	r := runningRouter(t, testAuth(t))
	reader := dial(t, r, basicHeader("reader", "test-only-password"))
	send(t, reader, frame{Kind: "hello", Protocol: Protocol, Role: "server", PeerID: "reader"})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, _, err := reader.Read(ctx); websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
		t.Fatal("client account became server")
	}
	worker := dial(t, r, basicHeader("issuer", "test-only-password"))
	hello(t, worker, "server", "worker")
	send(t, worker, frame{Kind: "server.register", InstanceID: "worker", Services: []service{{Name: AuthService}}})
	if _, _, err := worker.Read(ctx); websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
		t.Fatal("reserved service registered")
	}
}

func TestQueuePreservesOpenBarrierAndBoundsMemory(t *testing.T) {
	q := newOutbox(4096)
	credit := int64(1)
	if !q.push(frame{Kind: "call.open", CallID: "a"}) || !q.push(frame{Kind: "stream.window", CallID: "a", Credit: &credit}) ||
		!q.push(frame{Kind: "call.open", CallID: "b"}) {
		t.Fatal("enqueue")
	}
	for _, kind := range []string{"call.open", "stream.window", "call.open"} {
		f, ok := q.pop()
		if !ok || f.Kind != kind {
			t.Fatal("control overtook open")
		}
	}
	if q.push(frame{raw: make([]byte, 4097)}) {
		t.Fatal("unbounded queue")
	}
	q.close()
	if q.push(frame{Kind: "ping"}) {
		t.Fatal("closed queue accepted frame")
	}
}

func TestDeadlineCancelAndIdleLifecycle(t *testing.T) {
	r, err := Start(Config{DeadlineGrace: 50 * time.Millisecond, IdleTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	worker := dial(t, r, "")
	registerPeer(t, worker, "worker")
	client := dial(t, r, "")
	hello(t, client, "client", "client")
	timeout := int64(100)
	open := frame{Kind: "call.open", CallID: "slow", Service: testService, Method: "echo",
		Payload: json.RawMessage(`{}`), TimeoutMS: &timeout, Metadata: json.RawMessage(`{"traceparent":"00-abc"}`)}
	send(t, client, open)
	if got := receive(t, worker); !bytes.Equal(got.raw, open.bytes()) {
		t.Fatal("metadata or timeout was not forwarded verbatim")
	}
	// The worker never answers: the router fires at timeout + grace.
	started := time.Now()
	if got := receive(t, client); got.Error == nil || got.Error.Code != "deadline_exceeded" ||
		string(got.Error.Details) != `{"source":"router"}` || got.Error.Retryable {
		t.Fatalf("router deadline: %+v", got)
	}
	if elapsed := time.Since(started); elapsed < 100*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("deadline fired after %v", elapsed)
	}
	if got := receive(t, worker); got.Kind != "call.cancel" || got.Reason != "deadline_exceeded" {
		t.Fatalf("worker was not cancelled: %+v", got)
	}
	r.mu.Lock()
	if len(r.routes) != 0 || len(r.deadlines) != 0 || r.instances["worker"].inflight != 0 {
		t.Fatal("expired route leaked")
	}
	r.mu.Unlock()
	// A late result from the worker is dropped, not delivered to the client.
	send(t, worker, frame{Kind: "call.result", CallID: "slow", Payload: json.RawMessage(`{}`)})

	// Client cancel frees the route immediately even though the worker never answers.
	open.CallID = "cancelled"
	open.TimeoutMS = nil
	send(t, client, open)
	receive(t, worker)
	send(t, client, frame{Kind: "call.cancel", CallID: "cancelled"})
	if got := receive(t, worker); got.Kind != "call.cancel" {
		t.Fatal("cancel not forwarded")
	}
	eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return len(r.routes) == 0 && r.instances["worker"].inflight == 0
	})
	send(t, worker, frame{Kind: "call.result", CallID: "cancelled", Payload: json.RawMessage(`{}`)})

	// A normally completed call leaves the deadline heap.
	timeout = 60_000
	open.CallID = "fast"
	open.TimeoutMS = &timeout
	send(t, client, open)
	receive(t, worker)
	send(t, worker, frame{Kind: "call.result", CallID: "fast", Payload: json.RawMessage(`{}`)})
	if got := receive(t, client); got.Kind != "call.result" || got.CallID != "fast" {
		t.Fatalf("late frames leaked to client: %+v", got)
	}
	r.mu.Lock()
	if len(r.deadlines) != 0 {
		t.Fatal("completed route stayed in deadline heap")
	}
	r.mu.Unlock()

	// An idle peer that never pings is closed.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, _, err := client.Read(ctx); err == nil {
		t.Fatal("idle connection survived")
	}
}

func TestDecodeCompatibilityRules(t *testing.T) {
	r := runningRouter(t, nil)
	worker := dial(t, r, "")
	registerPeer(t, worker, "worker")
	client := dial(t, r, "")
	hello(t, client, "client", "client")
	// A malformed frame that names a call is answered for that call only.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Write(ctx, websocket.MessageText, []byte(`{"kind":"stream.window","call_id":"bad","direction":"server","credit":0}`)); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, client); got.Error == nil || got.Error.Code != "protocol_error" || got.CallID != "bad" {
		t.Fatalf("malformed frame: %+v", got)
	}
	send(t, client, frame{Kind: "call.open", CallID: "ok", Service: testService, Method: "echo", Payload: json.RawMessage(`{}`)})
	if got := receive(t, worker); got.CallID != "ok" {
		t.Fatal("connection did not survive a malformed call frame")
	}
	// A frame that cannot be attributed closes the connection.
	if err := client.Write(ctx, websocket.MessageText, []byte(`not json`)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.Read(ctx); err == nil {
		t.Fatal("unattributable garbage kept the connection")
	}
	if _, err := decode([]byte(`{"kind":"call.cancel","call_id":"x","future_field":1}`)); err != nil {
		t.Fatal("unknown field rejected:", err)
	}
	if _, err := decode([]byte(`{"kind":"call.future","call_id":"x"}`)); err == nil {
		t.Fatal("unknown frame kind accepted")
	}
	for _, bad := range []string{
		`{"kind":"call.open","call_id":"x","service":"s","method":"m","payload":{},"timeout_ms":0}`,
		`{"kind":"call.open","call_id":"x","service":"s","method":"m","payload":{},"idle_timeout_ms":-1}`,
		`{"kind":"call.open","call_id":"x","service":"s","method":"m","payload":{},"metadata":{"k":1}}`,
		`{"kind":"call.open","call_id":"x","service":"s","method":"m","payload":{},"metadata":[]}`,
		`{"kind":"call.result","call_id":"x","metadata":{"":"v"}}`,
	} {
		if _, err := decode([]byte(bad)); err == nil {
			t.Fatal("accepted:", bad)
		}
	}
	if _, err := decode([]byte(`{"kind":"call.error","call_id":"x","error":{"code":"internal","message":"m"},"metadata":{"k":"v"}}`)); err != nil {
		t.Fatal("trailers rejected:", err)
	}
}

func TestDrainLeavesPoolsButFinishesRoutedCalls(t *testing.T) {
	r := runningRouter(t, nil)
	a, b := dial(t, r, ""), dial(t, r, "")
	registerPeer(t, a, "a")
	registerPeer(t, b, "b")
	client := dial(t, r, "")
	hello(t, client, "client", "client")
	open := frame{Kind: "call.open", CallID: "on-a", Service: testService, Method: "download", Payload: json.RawMessage(`{}`)}
	send(t, client, open)
	if got := receive(t, a); got.CallID != "on-a" {
		t.Fatal("round robin should start at a")
	}
	send(t, a, frame{Kind: "server.drain"})
	if got := receive(t, a); got.Kind != "server.drain.ack" || got.InstanceID != "a" {
		t.Fatalf("drain ack: %+v", got)
	}
	// New calls avoid a; the routed call keeps flowing both ways.
	for _, id := range []string{"n1", "n2"} {
		open.CallID = id
		send(t, client, open)
		if got := receive(t, b); got.CallID != id {
			t.Fatal("drained instance still selected")
		}
		send(t, b, frame{Kind: "call.result", CallID: id, Payload: json.RawMessage(`{}`)})
		receive(t, client)
	}
	credit := int64(1)
	send(t, client, frame{Kind: "stream.window", CallID: "on-a", Direction: "server", Credit: &credit})
	if got := receive(t, a); got.Kind != "stream.window" {
		t.Fatal("client frames stopped reaching the draining instance")
	}
	send(t, a, frame{Kind: "call.result", CallID: "on-a", Payload: json.RawMessage(`"done"`)})
	if got := receive(t, client); got.CallID != "on-a" || string(got.Payload) != `"done"` {
		t.Fatal("draining instance's result did not reach the client")
	}
	send(t, a, frame{Kind: "server.drain"}) // idempotent
	receive(t, a)
	_ = a.CloseNow()
	open.CallID = "after"
	send(t, client, open)
	if got := receive(t, b); got.CallID != "after" {
		t.Fatal("pool broken after drained instance left")
	}
	eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		_, present := r.instances["a"]
		return !present && len(r.services[testService].instances) == 1
	})
	// A client may not drain.
	send(t, client, frame{Kind: "server.drain"})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, _, err := client.Read(ctx); websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
		t.Fatal("client drain accepted")
	}
}

func TestIdempotencyJoinsReplaysAndRedelivers(t *testing.T) {
	r := runningRouter(t, nil)
	a, b := dial(t, r, ""), dial(t, r, "")
	registerPeer(t, a, "a")
	registerPeer(t, b, "b")
	c1, c2 := dial(t, r, ""), dial(t, r, "")
	hello(t, c1, "client", "c1")
	hello(t, c2, "client", "c2")
	open := func(conn *websocket.Conn, id, key string) {
		send(t, conn, frame{Kind: "call.open", CallID: id, Service: testService, Method: "echo",
			Payload: json.RawMessage(`{"n":1}`), Idempotency: key})
	}
	// Two callers, same key: one execution, both answered.
	open(c1, "x1", "k1")
	open(c2, "x2", "k1")
	first := receive(t, a)
	if first.CallID != "x1" || first.Idempotency != "k1" {
		t.Fatalf("first open: %+v", first)
	}
	send(t, b, frame{Kind: "ping", Nonce: "barrier"})
	receive(t, b)
	r.mu.Lock()
	joined := len(r.dedup) == 1 && len(r.routes) == 1
	r.mu.Unlock()
	if !joined {
		t.Fatal("duplicate key was not joined")
	}
	send(t, a, frame{Kind: "call.result", CallID: "x1", Payload: json.RawMessage(`{"ok":true}`)})
	if got := receive(t, c1); got.Kind != "call.result" || got.CallID != "x1" {
		t.Fatalf("c1: %+v", got)
	}
	if got := receive(t, c2); got.Kind != "call.result" || got.CallID != "x2" || string(got.Payload) != `{"ok":true}` {
		t.Fatalf("c2 did not get the joined result: %+v", got)
	}
	// Replay from cache: no instance sees a third execution.
	open(c1, "x3", "k1")
	if got := receive(t, c1); got.Kind != "call.result" || got.CallID != "x3" {
		t.Fatalf("replay: %+v", got)
	}
	send(t, a, frame{Kind: "ping", Nonce: "barrier"})
	receive(t, a)
	send(t, b, frame{Kind: "ping", Nonce: "barrier"})
	receive(t, b)
	// Instance lost mid-call: both callers fail with the reason, and the retry
	// with the same key is routed to the other instance.
	open(c1, "y1", "k2")
	open(c2, "y2", "k2")
	target := receive(t, b) // round robin moved on to b
	if target.CallID != "y1" {
		t.Fatalf("expected y1 on b, got %+v", target)
	}
	_ = b.CloseNow()
	for _, conn := range []*websocket.Conn{c1, c2} {
		got := receive(t, conn)
		if got.Error == nil || got.Error.Code != "unavailable" || !got.Error.Retryable ||
			!bytes.Contains(got.Error.Details, []byte(`"reason":"instance_lost"`)) ||
			!bytes.Contains(got.Error.Details, []byte(`"instance_id":"b"`)) {
			t.Fatalf("instance_lost: %+v", got)
		}
	}
	open(c1, "y3", "k2")
	if got := receive(t, a); got.CallID != "y3" {
		t.Fatalf("retry not delivered to the surviving instance: %+v", got)
	}
	send(t, a, frame{Kind: "call.result", CallID: "y3", Payload: json.RawMessage(`{}`)})
	receive(t, c1)
	// No instances at all: the reason says so.
	_ = a.CloseNow()
	eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return len(r.instances) == 0
	})
	open(c1, "z1", "")
	if got := receive(t, c1); got.Error == nil || !bytes.Contains(got.Error.Details, []byte(`"reason":"no_instances"`)) {
		t.Fatalf("no_instances: %+v", got)
	}
	if _, err := decode([]byte(`{"kind":"call.open","call_id":"x","service":"s","method":"m","payload":{},"idempotency_key":"` + strings.Repeat("k", 129) + `"}`)); err == nil {
		t.Fatal("oversize idempotency key accepted")
	}
}

func TestMaxInflightAndCircuitBreaker(t *testing.T) {
	r, err := Start(Config{BreakerThreshold: 2, BreakerCooldown: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	register := func(conn *websocket.Conn, id string, max int) {
		hello(t, conn, "server", id)
		send(t, conn, frame{Kind: "server.register", InstanceID: id, MaxInflight: max, Services: []service{
			{Name: testService, Methods: []method{{Name: "echo", Stream: "unary", Balance: balance{Kind: "round_robin"}}}},
		}})
		if got := receive(t, conn); got.Kind != "server.register.ack" {
			t.Fatalf("registration: %+v", got)
		}
	}
	a, b := dial(t, r, ""), dial(t, r, "")
	register(a, "a", 1)
	register(b, "b", 1)
	client := dial(t, r, "")
	hello(t, client, "client", "client")
	open := func(id string) {
		send(t, client, frame{Kind: "call.open", CallID: id, Service: testService, Method: "echo", Payload: json.RawMessage(`{}`)})
	}
	// Caps: a takes one, b takes one, the third finds everyone full.
	open("c1")
	if got := receive(t, a); got.CallID != "c1" {
		t.Fatal("c1 not on a")
	}
	open("c2")
	if got := receive(t, b); got.CallID != "c2" {
		t.Fatal("c2 not on b")
	}
	open("c3")
	if got := receive(t, client); got.Error == nil || got.Error.Code != "resource_exhausted" || !got.Error.Retryable ||
		!bytes.Contains(got.Error.Details, []byte(`"reason":"overloaded"`)) {
		t.Fatalf("expected overloaded, got %+v", got)
	}
	if got := r.Snapshot().Health["a"]; got.Inflight != 1 || got.MaxInflight != 1 || got.Breaker != "closed" {
		t.Fatalf("health: %+v", got)
	}
	send(t, a, frame{Kind: "call.result", CallID: "c1", Payload: json.RawMessage(`{}`)})
	receive(t, client)
	send(t, b, frame{Kind: "call.result", CallID: "c2", Payload: json.RawMessage(`{}`)})
	receive(t, client)
	// Breaker: a answers `internal` twice → open; calls go to b only.
	for _, id := range []string{"f1", "f2"} {
		open(id)
		got := receive(t, a)
		if got.CallID != id { // round robin alternates; drain b's turn when needed
			t.Fatalf("expected %s on a, got %+v", id, got)
		}
		send(t, a, frame{Kind: "call.error", CallID: id, Error: &errorPayload{Code: "internal", Message: "boom"}})
		receive(t, client)
		// b's turn
		open(id + "-b")
		if got := receive(t, b); got.CallID != id+"-b" {
			t.Fatal("b turn")
		}
		send(t, b, frame{Kind: "call.result", CallID: id + "-b", Payload: json.RawMessage(`{}`)})
		receive(t, client)
	}
	if got := r.Snapshot().Health["a"]; got.Breaker != "open" || got.Failures != 2 {
		t.Fatalf("breaker should be open: %+v", got)
	}
	for _, id := range []string{"o1", "o2"} {
		open(id)
		if got := receive(t, b); got.CallID != id {
			t.Fatalf("open circuit still routed to a: %+v", got)
		}
		send(t, b, frame{Kind: "call.result", CallID: id, Payload: json.RawMessage(`{}`)})
		receive(t, client)
	}
	// Half-open after the cooldown: exactly one probe reaches a; its success closes the circuit.
	time.Sleep(350 * time.Millisecond)
	if got := r.Snapshot().Health["a"]; got.Breaker != "half_open" {
		t.Fatalf("expected half_open: %+v", got)
	}
	open("p1")
	open("p2")
	probe := receive(t, a)
	if probe.CallID != "p1" && probe.CallID != "p2" {
		t.Fatalf("probe: %+v", probe)
	}
	other := receive(t, b)
	if other.CallID == probe.CallID {
		t.Fatal("second call should not probe a")
	}
	send(t, a, frame{Kind: "call.result", CallID: probe.CallID, Payload: json.RawMessage(`{}`)})
	send(t, b, frame{Kind: "call.result", CallID: other.CallID, Payload: json.RawMessage(`{}`)})
	receive(t, client)
	receive(t, client)
	if got := r.Snapshot().Health["a"]; got.Breaker != "closed" || got.Failures != 0 {
		t.Fatalf("probe success should close: %+v", got)
	}
	// When every instance is tripped the reason says so. Read through
	// channels: a timed-out websocket.Read would close the losing socket.
	reader := func(conn *websocket.Conn) <-chan frame {
		ch := make(chan frame, 8)
		go func() {
			for {
				_, data, err := conn.Read(context.Background())
				if err != nil {
					close(ch)
					return
				}
				var f frame
				_ = json.Unmarshal(data, &f)
				ch <- f
			}
		}()
		return ch
	}
	fromA, fromB := reader(a), reader(b)
	for _, id := range []string{"t1", "t2", "t3", "t4"} {
		open(id)
		var got frame
		var from *websocket.Conn
		select {
		case got = <-fromA:
			from = a
		case got = <-fromB:
			from = b
		case <-time.After(3 * time.Second):
			t.Fatalf("%s was not delivered", id)
		}
		send(t, from, frame{Kind: "call.error", CallID: got.CallID, Error: &errorPayload{Code: "internal", Message: "boom"}})
		receive(t, client)
	}
	open("none")
	if got := receive(t, client); got.Error == nil || !bytes.Contains(got.Error.Details, []byte(`"reason":"circuit_open"`)) {
		t.Fatalf("expected circuit_open, got %+v", got)
	}
}

func TestServiceNameValidation(t *testing.T) {
	for _, name := range []string{"chat.v1", "demo.v1.Greeting", "chat.v1.2.0", "chat.v2.0.0-beta1", "a_b.v0", AuthService} {
		if !validServiceName(name) {
			t.Fatal("rejected valid name", name)
		}
	}
	for _, name := range []string{"", "chat", "v1", "chat.v1.v2", "chat.1", "1chat.v1", "chat..v1", "chat.v1.2"} {
		if validServiceName(name) {
			t.Fatal("accepted invalid name", name)
		}
	}
}

func TestEmptyPingNonceRemainsARequiredField(t *testing.T) {
	r := runningRouter(t, nil)
	conn := dial(t, r, "")
	hello(t, conn, "client", "ping-client")
	send(t, conn, frame{Kind: "ping", Nonce: ""})
	if got := receive(t, conn); !bytes.Contains(got.raw, []byte(`"nonce":""`)) {
		t.Fatal("empty nonce was omitted")
	}
	if _, err := decode([]byte(`{"kind":"ping"}`)); err == nil {
		t.Fatal("missing nonce accepted")
	}
}

func TestCanonicalStickyJSON(t *testing.T) {
	for _, pair := range [][2]string{
		{`{"z":12345678901234567890,"a":"中文😀<>&\u007f"}`, `{"a":"\u4e2d\u6587\ud83d\ude00<>&\u007f","z":12345678901234567890}`},
		{`[-0,-0.0,1e0,1e6,1e16,1e-4,1e-5]`, `[0,-0.0,1.0,1000000.0,1e+16,0.0001,1e-05]`},
	} {
		var value any
		d := json.NewDecoder(strings.NewReader(pair[0]))
		d.UseNumber()
		if err := d.Decode(&value); err != nil {
			t.Fatal(err)
		}
		var out strings.Builder
		if err := canonicalJSON(&out, value); err != nil {
			t.Fatal(err)
		}
		if out.String() != pair[1] {
			t.Fatalf("got %s want %s", out.String(), pair[1])
		}
	}
}

func TestUnixSocketOwnershipAndConfigValidation(t *testing.T) {
	path := filepath.Join(os.TempDir(), fmt.Sprintf("meshcall-go-%d.sock", time.Now().UnixNano()))
	if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(path) })
	if r, err := Start(Config{UnixPath: path}); err == nil {
		r.Close()
		t.Fatal("existing file replaced")
	}
	if data, _ := os.ReadFile(path); string(data) != "keep" {
		t.Fatal("existing file changed")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	r, err := Start(Config{UnixPath: path})
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	r.Close()
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatal("owned socket leaked")
	}
	r, err = Start(Config{UnixPath: path})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	r.Close()
	if data, _ := os.ReadFile(path); string(data) != "replacement" {
		t.Fatal("unowned replacement removed")
	}
	if _, err := Start(Config{Port: -1}); err == nil {
		t.Fatal("invalid port accepted")
	}
	if _, err := Start(Config{MaxFrameSize: -1}); err == nil {
		t.Fatal("invalid limit accepted")
	}
}

func TestTargetedCallsAndTargetedServices(t *testing.T) {
	r := runningRouter(t, nil)
	register := func(conn *websocket.Conn, id string, targeted bool) {
		hello(t, conn, "server", id)
		send(t, conn, frame{Kind: "server.register", InstanceID: id, MaxInflight: 1, Services: []service{
			{Name: testService, Targeted: targeted, Methods: []method{{Name: "echo", Stream: "unary", Balance: balance{Kind: "round_robin"}}}},
		}})
		if got := receive(t, conn); got.Kind != "server.register.ack" {
			t.Fatalf("registration: %+v", got)
		}
	}
	a, b := dial(t, r, ""), dial(t, r, "")
	register(a, "a", true)
	register(b, "b", true)
	// All instances of one service must agree on targeted.
	c := dial(t, r, "")
	hello(t, c, "server", "c")
	send(t, c, frame{Kind: "server.register", InstanceID: "c", Services: []service{
		{Name: testService, Methods: []method{{Name: "echo", Stream: "unary", Balance: balance{Kind: "round_robin"}}}},
	}})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, _, err := c.Read(ctx); err == nil {
		t.Fatal("a non-targeted registration joined a targeted pool")
	}
	if !r.Snapshot().Targeted[testService] {
		t.Fatal("snapshot does not report the service as targeted")
	}

	client := dial(t, r, "")
	hello(t, client, "client", "client")
	open := func(id, instance string) {
		send(t, client, frame{Kind: "call.open", CallID: id, Service: testService, Method: "echo", InstanceID: instance, Payload: json.RawMessage(`{}`)})
	}
	expectError := func(code, reason string) {
		t.Helper()
		got := receive(t, client)
		if got.Error == nil || got.Error.Code != code || !bytes.Contains(got.Error.Details, []byte(`"reason":"`+reason+`"`)) {
			t.Fatalf("expected %s/%s, got %+v", code, reason, got)
		}
	}
	// Untargeted call to a targeted service is refused before routing.
	open("u1", "")
	expectError("invalid_argument", "instance_required")
	// Named instance gets the call; the Router keeps the instance_id on the frame.
	open("t1", "b")
	if got := receive(t, b); got.CallID != "t1" || got.InstanceID != "b" {
		t.Fatalf("t1 not on b: %+v", got)
	}
	// Capacity still applies; unknown and draining instances are refused.
	open("t2", "b")
	expectError("resource_exhausted", "overloaded")
	open("t3", "nope")
	expectError("unavailable", "instance_unknown")
	send(t, a, frame{Kind: "server.drain"})
	receive(t, a) // drain.ack
	open("t4", "a")
	expectError("unavailable", "instance_draining")
	// Dedup is per instance: the same key on b and on a new instance are distinct calls.
	send(t, b, frame{Kind: "call.result", CallID: "t1", Payload: json.RawMessage(`{}`)})
	receive(t, client)
	send(t, client, frame{Kind: "call.open", CallID: "k1", Service: testService, Method: "echo", InstanceID: "b", Idempotency: "same", Payload: json.RawMessage(`{}`)})
	if got := receive(t, b); got.CallID != "k1" {
		t.Fatalf("k1 not on b: %+v", got)
	}
	send(t, b, frame{Kind: "call.result", CallID: "k1", Payload: json.RawMessage(`{"from":"b"}`)})
	receive(t, client)
	d := dial(t, r, "")
	register(d, "d", true)
	send(t, client, frame{Kind: "call.open", CallID: "k2", Service: testService, Method: "echo", InstanceID: "d", Idempotency: "same", Payload: json.RawMessage(`{}`)})
	if got := receive(t, d); got.CallID != "k2" {
		t.Fatalf("same key on another instance must not replay b's result: %+v", got)
	}
}
