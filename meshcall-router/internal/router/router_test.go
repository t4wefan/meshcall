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
