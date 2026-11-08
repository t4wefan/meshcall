package router

import (
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

const otherService = "test.v1.Other"

// tunnelHost registers id with testService (echo, download) and otherService
// (ping), announcing the tunnels capability unless refuse is set.
func tunnelHost(t *testing.T, r *Router, id, authorization string, capable bool) (*websocket.Conn, <-chan frame) {
	t.Helper()
	conn := dial(t, r, authorization)
	var caps []string
	if capable {
		caps = []string{TunnelCapability}
	}
	send(t, conn, frame{Kind: "hello", Protocol: Protocol, Role: "server", PeerID: id, InstanceID: id, Capabilities: caps})
	if got := receive(t, conn); got.Kind != "hello.ack" {
		t.Fatalf("hello: %+v", got)
	}
	send(t, conn, frame{Kind: "server.register", InstanceID: id, Services: []service{
		{Name: testService, Targeted: true, Methods: []method{
			{Name: "echo", Stream: "unary", Balance: balance{Kind: "round_robin"}},
			{Name: "download", Stream: "server_stream", Balance: balance{Kind: "round_robin"}},
		}},
		{Name: otherService, Methods: []method{{Name: "ping", Stream: "unary", Balance: balance{Kind: "round_robin"}}}},
	}})
	if got := receive(t, conn); got.Kind != "server.register.ack" {
		t.Fatalf("registration: %+v", got)
	}
	return conn, pump(t, conn)
}

func clientConn(t *testing.T, r *Router, authorization string) *websocket.Conn {
	t.Helper()
	conn := dial(t, r, authorization)
	hello(t, conn, "client", "client")
	return conn
}

func call(t *testing.T, conn *websocket.Conn, id, service, method, payload string, extra map[string]any) frame {
	t.Helper()
	openCall(t, conn, id, service, method, payload, extra)
	return receive(t, conn)
}

// openCall sends a call.open without waiting for its answer.
func openCall(t *testing.T, conn *websocket.Conn, id, service, method, payload string, extra map[string]any) {
	t.Helper()
	open := map[string]any{"kind": "call.open", "call_id": id, "service": service, "method": method, "payload": json.RawMessage(payload)}
	for k, v := range extra {
		open[k] = v
	}
	data, _ := json.Marshal(open)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
		t.Fatal(err)
	}
}

func errorReason(f frame) string {
	if f.Error == nil {
		return ""
	}
	var d map[string]any
	_ = json.Unmarshal(f.Error.Details, &d)
	reason, _ := d["reason"].(string)
	return reason
}

type openedTunnel struct {
	Credential string `json:"credential"`
	TunnelInfo
}

// openTunnelVia opens a tunnel on issuer for instance and answers the offer
// on the instance's frames with accept.
func openTunnelVia(t *testing.T, issuer *websocket.Conn, host *websocket.Conn, offers <-chan frame, request string, accept bool) (frame, frame) {
	t.Helper()
	data, _ := json.Marshal(map[string]any{"kind": "call.open", "call_id": "open-" + fmt.Sprint(time.Now().UnixNano()),
		"service": TunnelService, "method": "open", "payload": json.RawMessage(request)})
	if err := issuer.Write(context.Background(), websocket.MessageText, data); err != nil {
		t.Fatal(err)
	}
	offer := next(t, offers)
	if offer.Kind != "call.open" || offer.Service != TunnelHostService || offer.Method != "offer" {
		t.Fatalf("offer: %+v", offer)
	}
	send(t, host, frame{Kind: "call.result", CallID: offer.CallID, Payload: json.RawMessage(fmt.Sprintf(`{"accept":%v}`, accept))})
	return offer, receive(t, issuer)
}

func mustOpen(t *testing.T, issuer, host *websocket.Conn, offers <-chan frame, request string) openedTunnel {
	t.Helper()
	_, result := openTunnelVia(t, issuer, host, offers, request, true)
	if result.Kind != "call.result" {
		t.Fatalf("open: %+v", result.Error)
	}
	var opened openedTunnel
	if err := json.Unmarshal(result.Payload, &opened); err != nil || !strings.HasPrefix(opened.Credential, tunnelPrefix) {
		t.Fatalf("open result: %s", result.Payload)
	}
	return opened
}

func TestTunnelScopesTheConnectionToOneInstance(t *testing.T) {
	r := runningRouter(t, nil)
	host, frames := tunnelHost(t, r, "box-a", "", true)
	_, _ = tunnelHost(t, r, "box-b", "", true)
	issuer := clientConn(t, r, "")

	offer, _ := openTunnelVia(t, issuer, host, frames, `{"instance_id":"box-a","principal":"user:alice","ttl_seconds":60,
		"scopes":[{"service":"test.v1.Echo","methods":["echo"]}]}`, false)
	var offered map[string]any
	_ = json.Unmarshal(offer.Payload, &offered)
	if offered["principal"] != "user:alice" || offered["instance_id"] != "box-a" || offered["resumed"] != false {
		t.Fatalf("offer payload: %s", offer.Payload)
	}
	opened := mustOpen(t, issuer, host, frames, `{"instance_id":"box-a","principal":"user:alice","ttl_seconds":60,
		"scopes":[{"service":"test.v1.Echo","methods":["echo"]}]}`)
	if opened.InstanceID != "box-a" || opened.Principal != "user:alice" || !opened.Online {
		t.Fatalf("opened: %+v", opened)
	}

	client := clientConn(t, r, "Bearer "+opened.Credential)
	// No instance named; spoofed tunnel metadata is replaced.
	openCall(t, client, "c1", testService, "echo", `{"x":1}`, map[string]any{
		"metadata": map[string]string{tunnelIDKey: "forged", tunnelPrincipalKey: "user:mallory", "trace": "t1"}})
	routed := next(t, frames)
	var metadata map[string]string
	_ = json.Unmarshal(routed.Metadata, &metadata)
	if routed.InstanceID != "box-a" || metadata[tunnelIDKey] != opened.TunnelID || metadata[tunnelPrincipalKey] != "user:alice" || metadata["trace"] != "t1" {
		t.Fatalf("routed call: %s", routed.raw)
	}
	send(t, host, frame{Kind: "call.result", CallID: "c1", Payload: json.RawMessage(`{"ok":true}`)})
	if got := receive(t, client); got.Kind != "call.result" {
		t.Fatalf("tunnel call result: %+v", got)
	}

	for _, c := range []struct {
		name, service, method string
		extra                 map[string]any
	}{
		{"other instance", testService, "echo", map[string]any{"instance_id": "box-b"}},
		{"method outside the scope", testService, "download", nil},
		{"service outside the scope", otherService, "ping", nil},
		{"nonexistent service", "nope.v1.Nope", "x", nil},
		{"tunnel issuance", TunnelService, "list", nil},
		{"token issuance", AuthService, "issue_token", nil},
	} {
		if got := call(t, client, "deny-"+c.name, c.service, c.method, `{}`, c.extra); got.Error == nil || got.Error.Code != "permission_denied" {
			t.Fatalf("%s: %+v", c.name, got)
		}
	}

	list := call(t, client, "reg", RegistryService, "list", `{}`, nil)
	var listed struct {
		Services []registryService `json:"services"`
	}
	_ = json.Unmarshal(list.Payload, &listed)
	if len(listed.Services) != 1 || listed.Services[0].Name != testService || len(listed.Services[0].Instances) != 1 ||
		listed.Services[0].Instances[0].ID != "box-a" || len(listed.Services[0].Methods) != 1 ||
		listed.Services[0].Methods[0].Name != "echo" || len(listed.Services[0].Methods[0].Instances) != 1 {
		t.Fatalf("registry view: %s", list.Payload)
	}
	if got := call(t, client, "desc", RegistryService, "describe", `{"service":"test.v1.Other"}`, nil); got.Error == nil || got.Error.Code != "method_not_found" {
		t.Fatalf("describe outside the view: %+v", got)
	}
	who := call(t, client, "who", AuthService, "whoami", `{}`, nil)
	if !strings.Contains(string(who.Payload), opened.TunnelID) {
		t.Fatalf("whoami: %s", who.Payload)
	}

	// A tunnel credential never registers.
	server := dial(t, r, "Bearer "+opened.Credential)
	send(t, server, frame{Kind: "hello", Protocol: Protocol, Role: "server", PeerID: "x", InstanceID: "box-a"})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, _, err := server.Read(ctx); websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
		t.Fatalf("tunnel registration: %v", err)
	}

	// Spoofed tunnel metadata from an ordinary connection is stripped too.
	plain := clientConn(t, r, "")
	openCall(t, plain, "p1", testService, "echo", `{}`, map[string]any{"instance_id": "box-a",
		"metadata": map[string]string{tunnelIDKey: opened.TunnelID, "meshcall-tunnel-extra": "x", "keep": "1"}})
	routed = next(t, frames)
	metadata = nil
	_ = json.Unmarshal(routed.Metadata, &metadata)
	if len(metadata) != 1 || metadata["keep"] != "1" {
		t.Fatalf("plain call metadata: %s", routed.raw)
	}
}

func TestTunnelRevokeClosesAndExpiryDoesNot(t *testing.T) {
	r := runningRouter(t, nil)
	host, frames := tunnelHost(t, r, "box-a", "", true)
	issuer := clientConn(t, r, "")
	opened := mustOpen(t, issuer, host, frames, `{"instance_id":"box-a","principal":"p","ttl_seconds":1,"scopes":[{"service":"test.v1.Echo","methods":["*"]}]}`)
	live := clientConn(t, r, "Bearer "+opened.Credential)
	time.Sleep(1200 * time.Millisecond)

	// Expired: new connections are refused, the live one keeps working.
	header := http.Header{"Authorization": {"Bearer " + opened.Credential}}
	if _, resp, err := websocket.Dial(context.Background(), fmt.Sprintf("ws://127.0.0.1:%d", r.Ready().Port), &websocket.DialOptions{HTTPHeader: header}); err == nil || resp == nil || resp.StatusCode != 401 {
		t.Fatalf("expired credential connected: %v", err)
	}
	openCall(t, live, "c1", testService, "echo", `{}`, nil)
	if routed := next(t, frames); routed.CallID != "c1" {
		t.Fatalf("live connection after expiry: %+v", routed)
	}
	send(t, host, frame{Kind: "call.result", CallID: "c1", Payload: json.RawMessage(`{}`)})
	_ = receive(t, live)

	listed := call(t, issuer, "l", TunnelService, "list", `{}`, nil)
	if !strings.Contains(string(listed.Payload), opened.TunnelID) || strings.Contains(string(listed.Payload), opened.Credential) {
		t.Fatalf("list: %s", listed.Payload)
	}
	revoked := call(t, issuer, "rv", TunnelService, "revoke", `{"tunnel_id":"`+opened.TunnelID+`"}`, nil)
	if string(revoked.Payload) != `{"revoked":true}` {
		t.Fatalf("revoke: %s", revoked.Payload)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		if _, _, err := live.Read(ctx); err != nil {
			if ctx.Err() != nil {
				t.Fatal("revoked connection stayed open")
			}
			break
		}
	}
	closed := next(t, frames)
	if closed.Kind != "call.open" || closed.Method != "closed" || !strings.Contains(string(closed.Payload), `"revoked"`) {
		t.Fatalf("close notice: %+v", closed)
	}
	if len(r.Snapshot().Tunnels) != 0 {
		t.Fatal("revoked tunnel still listed")
	}
}

func TestExpiredTunnelClosesWithItsLastConnection(t *testing.T) {
	r := runningRouter(t, nil)
	host, frames := tunnelHost(t, r, "box-a", "", true)
	issuer := clientConn(t, r, "")
	opened := mustOpen(t, issuer, host, frames, `{"instance_id":"box-a","principal":"p","ttl_seconds":1,"scopes":[{"service":"test.v1.Echo","methods":["*"]}]}`)
	unused := mustOpen(t, issuer, host, frames, `{"instance_id":"box-a","principal":"q","ttl_seconds":1,"scopes":[{"service":"test.v1.Echo","methods":["*"]}]}`)
	live := clientConn(t, r, "Bearer "+opened.Credential)
	// The unused one closes at expiry; the used one when its connection ends.
	if closed := next(t, frames); !strings.Contains(string(closed.Payload), unused.TunnelID) || !strings.Contains(string(closed.Payload), "expired") {
		t.Fatalf("expiry notice: %s", closed.Payload)
	}
	_ = live.Close(websocket.StatusNormalClosure, "")
	if closed := next(t, frames); !strings.Contains(string(closed.Payload), opened.TunnelID) {
		t.Fatalf("last-connection notice: %s", closed.Payload)
	}
}

func TestTunnelRefusalAndInstanceReconnect(t *testing.T) {
	r := runningRouter(t, nil)
	_, _ = tunnelHost(t, r, "plain", "", false)
	issuer := clientConn(t, r, "")
	request := `{"instance_id":"%s","principal":"p","ttl_seconds":60,"scopes":[{"service":"test.v1.Echo","methods":["echo"]}]}`
	if got := call(t, issuer, "o1", TunnelService, "open", fmt.Sprintf(request, "plain"), nil); errorReason(got) != "tunnel_rejected" || got.Error.Code != "permission_denied" {
		t.Fatalf("instance without the capability: %+v", got.Error)
	}
	if got := call(t, issuer, "o2", TunnelService, "open", fmt.Sprintf(request, "ghost"), nil); errorReason(got) != "instance_unknown" || got.Error.Code != "unavailable" {
		t.Fatalf("unknown instance: %+v", got.Error)
	}
	if got := call(t, issuer, "o3", TunnelService, "open", `{"instance_id":"plain","principal":"p","ttl_seconds":60,"scopes":[{"service":"test.v1.Echo","methods":["nope"]}]}`, nil); got.Error == nil || got.Error.Code != "invalid_argument" {
		t.Fatalf("method the instance does not serve: %+v", got)
	}
	host, frames := tunnelHost(t, r, "box-a", "", true)
	if _, refused := openTunnelVia(t, issuer, host, frames, fmt.Sprintf(request, "box-a"), false); errorReason(refused) != "tunnel_rejected" {
		t.Fatalf("refused offer: %+v", refused.Error)
	}
	opened := mustOpen(t, issuer, host, frames, fmt.Sprintf(request, "box-a"))
	client := clientConn(t, r, "Bearer "+opened.Credential)

	_ = host.Close(websocket.StatusNormalClosure, "")
	eventually(t, func() bool { return !r.Snapshot().Tunnels[0].Online })
	if got := call(t, client, "c1", testService, "echo", `{}`, nil); got.Error == nil || got.Error.Code != "unavailable" || !got.Error.Retryable || errorReason(got) != "instance_offline" {
		t.Fatalf("offline target: %+v", got.Error)
	}
	host, frames = tunnelHost(t, r, "box-a", "", true)
	resumed := next(t, frames)
	if resumed.Method != "offer" || !strings.Contains(string(resumed.Payload), `"resumed":true`) {
		t.Fatalf("re-offer: %+v", resumed)
	}
	send(t, host, frame{Kind: "call.result", CallID: resumed.CallID, Payload: json.RawMessage(`{"accept":true}`)})
	eventually(t, func() bool { return r.Snapshot().Tunnels[0].Online })
	openCall(t, client, "c2", testService, "echo", `{}`, nil)
	if routed := next(t, frames); routed.CallID != "c2" {
		t.Fatalf("resumed call: %+v", routed)
	}

	// A re-offer the instance refuses closes the tunnel and its connections.
	_ = host.Close(websocket.StatusNormalClosure, "")
	eventually(t, func() bool { return !r.Snapshot().Tunnels[0].Online })
	host, frames = tunnelHost(t, r, "box-a", "", true)
	resumed = next(t, frames)
	send(t, host, frame{Kind: "call.result", CallID: resumed.CallID, Payload: json.RawMessage(`{"accept":false}`)})
	eventually(t, func() bool { return len(r.Snapshot().Tunnels) == 0 })
}

func TestIssuerPermissionsAndInstanceBoundRegistration(t *testing.T) {
	auth := authFrom(t, map[string]any{
		"users": []map[string]any{
			{"username": "control", "password_hash": passwordHash(), "roles": []string{"client"},
				"call": []string{TunnelService + "/*", testService + "/echo", AuthService + "/*"}},
			{"username": "daemon", "password_hash": passwordHash(), "roles": []string{"server", "client"},
				"register": []string{testService, otherService}, "instance_ids": []string{"box-a", "box-b"}, "call": []string{AuthService + "/*", testService + "/echo"}},
			{"username": "viewer", "password_hash": passwordHash(), "roles": []string{"client"}, "call": []string{testService + "/*"}},
		},
		"api_keys": []map[string]any{{"id": "plane", "key_hash": strings.Repeat("0", 64), "roles": []string{"client"}, "call": []string{TunnelService + "/open"}}},
	})
	r := runningRouter(t, auth)
	daemon := basicHeader("daemon", "test-only-password")

	// Registration under an id outside instance_ids is refused.
	conn := dial(t, r, daemon)
	send(t, conn, frame{Kind: "hello", Protocol: Protocol, Role: "server", PeerID: "x", InstanceID: "box-z"})
	_ = receive(t, conn)
	send(t, conn, frame{Kind: "server.register", InstanceID: "box-z", Services: []service{{Name: testService, Methods: []method{{Name: "echo", Stream: "unary", Balance: balance{Kind: "round_robin"}}}}}})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, _, err := conn.Read(ctx); websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
		t.Fatalf("foreign instance id: %v", err)
	}
	host, frames := tunnelHost(t, r, "box-a", daemon, true)

	// Token scopes narrow instance_ids within the issuer's.
	issuerOfTokens := clientConn(t, r, daemon)
	for _, c := range []struct{ scope, code string }{
		{`{"service":"test.v1.Echo","register":true,"instance_ids":["box-z"]}`, "permission_denied"},
		{`{"service":"test.v1.Echo","methods":["echo"],"instance_ids":["box-a"]}`, "invalid_argument"},
	} {
		if got := call(t, issuerOfTokens, "t-"+c.code, AuthService, "issue_token", `{"ttl_seconds":60,"scopes":[`+c.scope+`]}`, nil); got.Error == nil || got.Error.Code != c.code {
			t.Fatalf("token scope %s: %+v", c.scope, got)
		}
	}
	issued := call(t, issuerOfTokens, "t-ok", AuthService, "issue_token", `{"ttl_seconds":60,"scopes":[{"service":"test.v1.Echo","register":true,"instance_ids":["box-b"]}]}`, nil)
	var token IssueTokenResult
	_ = json.Unmarshal(issued.Payload, &token)
	tokenConn := dial(t, r, "Bearer "+token.Token)
	send(t, tokenConn, frame{Kind: "hello", Protocol: Protocol, Role: "server", PeerID: "x", InstanceID: "box-a"})
	_ = receive(t, tokenConn)
	send(t, tokenConn, frame{Kind: "server.register", InstanceID: "box-a", Services: []service{{Name: testService, Methods: []method{{Name: "echo", Stream: "unary", Balance: balance{Kind: "round_robin"}}}}}})
	if _, _, err := tokenConn.Read(ctx); websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
		t.Fatalf("token registering a foreign id: %v", err)
	}

	control := clientConn(t, r, basicHeader("control", "test-only-password"))
	if got := call(t, control, "wide", TunnelService, "open", `{"instance_id":"box-a","principal":"p","ttl_seconds":60,"scopes":[{"service":"test.v1.Echo","methods":["*"]}]}`, nil); got.Error == nil || got.Error.Code != "permission_denied" {
		t.Fatalf("scope beyond the issuer: %+v", got)
	}
	viewer := clientConn(t, r, basicHeader("viewer", "test-only-password"))
	if got := call(t, viewer, "no-acl", TunnelService, "open", `{}`, nil); got.Error == nil || got.Error.Code != "permission_denied" {
		t.Fatalf("issuer without the ACL: %+v", got)
	}
	opened := mustOpen(t, control, host, frames, `{"instance_id":"box-a","principal":"p","ttl_seconds":60,"scopes":[{"service":"test.v1.Echo","methods":["echo"]}],"max_connections":1}`)
	if got := call(t, viewer, "foreign-revoke", TunnelService, "revoke", `{"tunnel_id":"`+opened.TunnelID+`"}`, nil); got.Error == nil {
		t.Fatalf("revoke without the ACL: %+v", got)
	}
	_ = clientConn(t, r, "Bearer "+opened.Credential)
	header := http.Header{"Authorization": {"Bearer " + opened.Credential}}
	if _, resp, err := websocket.Dial(context.Background(), fmt.Sprintf("ws://127.0.0.1:%d", r.Ready().Port), &websocket.DialOptions{HTTPHeader: header}); err == nil || resp == nil || resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("max_connections exceeded: %v", err)
	}
}

func TestTunnelOverHTTPAndHelloCredentials(t *testing.T) {
	r := runningRouter(t, nil)
	host, frames := tunnelHost(t, r, "box-a", "", true)
	_, _ = tunnelHost(t, r, "box-b", "", true)
	issuer := clientConn(t, r, "")
	opened := mustOpen(t, issuer, host, frames, `{"instance_id":"box-a","principal":"p","ttl_seconds":60,"scopes":[{"service":"test.v1.Echo","methods":["echo"]}]}`)

	request, _ := http.NewRequest(http.MethodPost, httpURL(r, "/v1/call/"+testService+"/echo"), strings.NewReader(`{}`))
	request.Header.Set("Authorization", "Bearer "+opened.Credential)
	request.Header.Set("MeshCall-Meta-Meshcall-Tunnel-Principal", "forged")
	done := make(chan *http.Response, 1)
	go func() {
		resp, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Error(err)
		}
		done <- resp
	}()
	routed := next(t, frames)
	if !strings.Contains(string(routed.Metadata), `"meshcall-tunnel-principal":"p"`) || routed.InstanceID != "box-a" {
		t.Fatalf("HTTP tunnel call: %s", routed.raw)
	}
	send(t, host, frame{Kind: "call.result", CallID: routed.CallID, Payload: json.RawMessage(`{"ok":1}`)})
	if resp := <-done; resp == nil || resp.StatusCode != 200 {
		t.Fatalf("HTTP status: %v", resp)
	}

	get, _ := http.NewRequest(http.MethodGet, httpURL(r, "/v1/openapi.json"), nil)
	get.Header.Set("Authorization", "Bearer "+opened.Credential)
	resp, err := http.DefaultClient.Do(get)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), "/v1/call/test.v1.Echo/echo") || strings.Contains(string(body), "download") || strings.Contains(string(body), otherService) {
		t.Fatalf("OpenAPI view: %s", body)
	}

	// Credentials in hello for peers that cannot set the header.
	conn := dial(t, r, "")
	send(t, conn, frame{Kind: "hello", Protocol: Protocol, Role: "client", PeerID: "browser", Authorization: "Bearer " + opened.Credential})
	if got := receive(t, conn); got.Kind != "hello.ack" {
		t.Fatalf("hello credentials: %+v", got)
	}
	who := call(t, conn, "who", AuthService, "whoami", `{}`, nil)
	if !strings.Contains(string(who.Payload), opened.TunnelID) {
		t.Fatalf("whoami via hello credentials: %s", who.Payload)
	}
	bad := dial(t, r, "")
	send(t, bad, frame{Kind: "hello", Protocol: Protocol, Role: "client", PeerID: "browser", Authorization: "Bearer mtun_wrong"})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, _, err := bad.Read(ctx); websocket.CloseStatus(err) != StatusUnauthenticated {
		t.Fatalf("bad hello credentials: %v", err)
	}
	twice := dial(t, r, "Bearer "+opened.Credential)
	send(t, twice, frame{Kind: "hello", Protocol: Protocol, Role: "client", PeerID: "browser", Authorization: "Bearer " + opened.Credential})
	if _, _, err := twice.Read(ctx); websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
		t.Fatalf("credentials twice: %v", err)
	}
}

func TestIssuerGivingUpLeavesNoTunnel(t *testing.T) {
	r := runningRouter(t, nil)
	host, frames := tunnelHost(t, r, "box-a", "", true)
	issuer := clientConn(t, r, "")
	data, _ := json.Marshal(map[string]any{"kind": "call.open", "call_id": "o1", "service": TunnelService, "method": "open",
		"payload": json.RawMessage(`{"instance_id":"box-a","principal":"p","ttl_seconds":60,"scopes":[{"service":"test.v1.Echo","methods":["echo"]}]}`)})
	_ = issuer.Write(context.Background(), websocket.MessageText, data)
	offer := next(t, frames)
	send(t, issuer, frame{Kind: "call.cancel", CallID: "o1"})
	time.Sleep(50 * time.Millisecond)
	send(t, host, frame{Kind: "call.result", CallID: offer.CallID, Payload: json.RawMessage(`{"accept":true}`)})
	if closed := next(t, frames); closed.Method != "closed" || !strings.Contains(string(closed.Payload), "abandoned") {
		t.Fatalf("abandoned offer: %+v", closed)
	}
	if len(r.Snapshot().Tunnels) != 0 {
		t.Fatal("abandoned tunnel kept")
	}
}
