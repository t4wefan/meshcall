package router

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func peeredRouters(t *testing.T, hubAuth *Auth, credentials string) (hub, edge *Router) {
	t.Helper()
	hub = runningRouter(t, hubAuth)
	hubURL := fmt.Sprintf("ws://%s127.0.0.1:%d", credentials, hub.Ready().Port)
	var err error
	edge, err = Start(Config{Peers: []string{hubURL}, PeerID: "edge"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(edge.Close)
	return hub, edge
}

func registerOn(t *testing.T, r *Router, id string, methods ...method) *websocket.Conn {
	return registerOnAs(t, r, "", id, methods...)
}

func registerOnAs(t *testing.T, r *Router, authorization, id string, methods ...method) *websocket.Conn {
	t.Helper()
	conn := dial(t, r, authorization)
	hello(t, conn, "server", id)
	send(t, conn, frame{Kind: "server.register", InstanceID: id, MaxInflight: 2, Services: []service{{Name: testService, Methods: methods}}})
	if got := receive(t, conn); got.Kind != "server.register.ack" {
		t.Fatalf("register %s: %+v", id, got)
	}
	return conn
}

func serves(r *Router, id string) bool {
	for _, inst := range r.Snapshot().Instances[testService] {
		if inst == id {
			return true
		}
	}
	return false
}

var echoMethod = method{Name: "echo", Stream: "unary", Balance: balance{Kind: "round_robin"}}
var downloadMethod = method{Name: "download", Stream: "server_stream", Balance: balance{Kind: "round_robin"}}

func TestPeerLinkIsAnOrdinaryInstanceOnBothSides(t *testing.T) {
	hub, edge := peeredRouters(t, nil, "")
	// Each side sees the other as an instance named by its peer id, even with
	// nothing registered yet (an empty service list).
	eventually(t, func() bool {
		_, ok := hub.Snapshot().Health["edge"]
		_, ok2 := edge.Snapshot().Health[hub.PeerID()]
		return ok && ok2
	})
	if h := hub.Snapshot().Health["edge"]; !h.Peer {
		t.Fatalf("edge link not marked as a peer: %+v", h)
	}

	// A service behind the edge becomes callable from the hub: the link
	// advertises the edge's local instances with their summed capacity.
	srv := registerOn(t, edge, "srv", echoMethod, downloadMethod)
	eventually(t, func() bool {
		s := hub.Snapshot()
		return len(s.Instances[testService]) == 1 && s.Health["edge"].MaxInflight == 2
	})
	client := dial(t, hub, "")
	hello(t, client, "client", "c")
	send(t, client, frame{Kind: "call.open", CallID: "u1", Service: testService, Method: "echo", Payload: json.RawMessage(`{"n":1}`), Metadata: json.RawMessage(`{"trace":"t1"}`)})
	open := receive(t, srv)
	if open.CallID != "u1" || string(open.Metadata) != `{"trace":"t1"}` {
		t.Fatalf("call did not cross the link intact: %+v", open)
	}
	send(t, srv, frame{Kind: "call.result", CallID: "u1", Payload: json.RawMessage(`{"ok":true}`)})
	if got := receive(t, client); got.Kind != "call.result" || string(got.Payload) != `{"ok":true}` {
		t.Fatalf("result: %+v", got)
	}
	// Streams and credit cross too, in both directions of the call.
	send(t, client, frame{Kind: "call.open", CallID: "s1", Service: testService, Method: "download", Payload: json.RawMessage(`{}`)})
	if got := receive(t, srv); got.CallID != "s1" {
		t.Fatalf("stream open: %+v", got)
	}
	credit := int64(4)
	send(t, client, frame{Kind: "stream.window", CallID: "s1", Direction: "server", Credit: &credit})
	if got := receive(t, srv); got.Kind != "stream.window" || *got.Credit != 4 {
		t.Fatalf("window did not reach the instance: %+v", got)
	}
	seq := int64(0)
	send(t, srv, frame{Kind: "stream.item", CallID: "s1", Direction: "server", Sequence: &seq, Payload: json.RawMessage(`1`)})
	send(t, srv, frame{Kind: "stream.end", CallID: "s1", Direction: "server"})
	send(t, srv, frame{Kind: "call.result", CallID: "s1"})
	kinds := []string{receive(t, client).Kind, receive(t, client).Kind, receive(t, client).Kind}
	if fmt.Sprint(kinds) != "[stream.item stream.end call.result]" {
		t.Fatalf("stream frames: %v", kinds)
	}

	// The other direction: a service on the hub is callable from the edge.
	hubSrv := registerOn(t, hub, "hub-srv", echoMethod)
	eventually(t, func() bool { return serves(edge, hub.PeerID()) })
	edgeClient := dial(t, edge, "")
	hello(t, edgeClient, "client", "ec")
	send(t, edgeClient, frame{Kind: "call.open", CallID: "r1", Service: testService, Method: "echo", Payload: json.RawMessage(`{}`)})
	if got := receive(t, hubSrv); got.CallID != "r1" {
		t.Fatalf("reverse call: %+v", got)
	}
	send(t, hubSrv, frame{Kind: "call.result", CallID: "r1", Payload: json.RawMessage(`{}`)})
	receive(t, edgeClient)

	// Path-qualified targeting: the hub client names edge/srv; the edge strips
	// its prefix so the instance sees its own id.
	send(t, client, frame{Kind: "call.open", CallID: "t1", Service: testService, Method: "echo", InstanceID: "edge/srv", Payload: json.RawMessage(`{"x":1}`)})
	got := receive(t, srv)
	if got.CallID != "t1" || got.InstanceID != "srv" || !bytes.Contains(got.raw, []byte(`"x":1`)) {
		t.Fatalf("targeted call across the link: %+v %s", got, got.raw)
	}
	send(t, srv, frame{Kind: "call.result", CallID: "t1", Payload: json.RawMessage(`{}`)})
	receive(t, client)
	// Naming an unknown instance behind the edge fails at the edge, from the router.
	send(t, client, frame{Kind: "call.open", CallID: "t2", Service: testService, Method: "echo", InstanceID: "edge/nope", Payload: json.RawMessage(`{}`)})
	if got := receive(t, client); got.Error == nil || !bytes.Contains(got.Error.Details, []byte(`"reason":"instance_unknown"`)) {
		t.Fatalf("expected instance_unknown, got %+v", got)
	}

	// A call that arrived over the link never bounces back over it: with the
	// edge's only instance gone, the edge answers no_instances itself.
	_ = srv.CloseNow()
	eventually(t, func() bool { return len(hub.Snapshot().Instances[testService]) == 1 }) // hub-srv only
	send(t, client, frame{Kind: "call.open", CallID: "u2", Service: testService, Method: "echo", Payload: json.RawMessage(`{}`)})
	if got := receive(t, hubSrv); got.CallID != "u2" {
		t.Fatalf("expected u2 on hub-srv after the edge lost its instance, got %+v", got)
	}
	send(t, hubSrv, frame{Kind: "call.result", CallID: "u2", Payload: json.RawMessage(`{}`)})
	receive(t, client)

	// Losing the link fails in-flight calls with instance_lost and removes
	// the peer instance.
	edgeSrv2 := registerOn(t, edge, "srv2", echoMethod)
	eventually(t, func() bool {
		return hub.Snapshot().Health["edge"].MaxInflight == 2 && len(hub.Snapshot().Instances[testService]) == 2
	})
	_ = hubSrv.CloseNow()
	eventually(t, func() bool { return len(hub.Snapshot().Instances[testService]) == 1 })
	send(t, client, frame{Kind: "call.open", CallID: "l1", Service: testService, Method: "echo", Payload: json.RawMessage(`{}`)})
	if got := receive(t, edgeSrv2); got.CallID != "l1" {
		t.Fatalf("l1 not on srv2: %+v", got)
	}
	edge.Close()
	if got := receive(t, client); got.Error == nil || !bytes.Contains(got.Error.Details, []byte(`"reason":"instance_lost"`)) {
		t.Fatalf("expected instance_lost when the link dropped, got %+v", got)
	}
	eventually(t, func() bool { _, ok := hub.Snapshot().Health["edge"]; return !ok })
}

func TestPeerLinkIsBoundByItsAccount(t *testing.T) {
	config := map[string]any{"users": []map[string]any{
		{"username": "edge", "password_hash": passwordHash(), "roles": []string{"router"},
			"register": []string{testService}, "call": []string{testService + "/echo"}},
		{"username": "client-only", "password_hash": passwordHash(), "roles": []string{"client"}, "call": []string{"*"}},
		{"username": "hub-srv", "password_hash": passwordHash(), "roles": []string{"server"}, "register": []string{testService}},
	}}
	data, _ := json.Marshal(config)
	path := filepath.Join(t.TempDir(), "auth.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	auth, err := LoadAuth(path)
	if err != nil {
		t.Fatal(err)
	}
	// An account without the router role cannot peer.
	hub := runningRouter(t, auth)
	denied, err := Start(Config{Peers: []string{fmt.Sprintf("ws://client-only:test-only-password@127.0.0.1:%d", hub.Ready().Port)}, PeerID: "denied"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(denied.Close)
	time.Sleep(300 * time.Millisecond)
	if _, ok := hub.Snapshot().Health["denied"]; ok {
		t.Fatal("a client-only account was accepted as a router peer")
	}
	// The router account may advertise only testService and forward only echo.
	edge, err := Start(Config{Peers: []string{fmt.Sprintf("ws://edge:test-only-password@127.0.0.1:%d", hub.Ready().Port)}, PeerID: "edge"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(edge.Close)
	eventually(t, func() bool { _, ok := hub.Snapshot().Health["edge"]; return ok })
	srv := registerOn(t, edge, "srv", echoMethod, downloadMethod)
	eventually(t, func() bool { return len(hub.Snapshot().Instances[testService]) == 1 })
	client := dial(t, hub, basicHeader("client-only", "test-only-password"))
	hello(t, client, "client", "c")
	// Calls from the hub to the edge are not bound by the edge's account on
	// the hub (that account bounds what the edge may *send* to the hub)...
	send(t, client, frame{Kind: "call.open", CallID: "d1", Service: testService, Method: "download", Payload: json.RawMessage(`{}`)})
	if got := receive(t, srv); got.CallID != "d1" {
		t.Fatalf("download should reach the edge: %+v", got)
	}
	// ...while a call the edge forwards to the hub for a method outside its
	// `call` list is refused by the hub.
	hubSrv := registerOnAs(t, hub, basicHeader("hub-srv", "test-only-password"), "hub-srv", echoMethod, downloadMethod)
	_ = hubSrv
	eventually(t, func() bool { return serves(edge, hub.PeerID()) })
	edgeClient := dial(t, edge, "")
	hello(t, edgeClient, "client", "ec")
	send(t, edgeClient, frame{Kind: "call.open", CallID: "p1", Service: testService, Method: "download", Payload: json.RawMessage(`{}`)})
	if got := receive(t, edgeClient); got.Error == nil || got.Error.Code != "permission_denied" {
		t.Fatalf("expected permission_denied through the link, got %+v", got)
	}
	// An unregistered service is dropped from the advertisement, not fatal.
	other := dial(t, edge, "")
	hello(t, other, "server", "other")
	send(t, other, frame{Kind: "server.register", InstanceID: "other", Services: []service{{Name: "other.v1.Svc", Methods: []method{echoMethod}}}})
	receive(t, other)
	time.Sleep(200 * time.Millisecond)
	if _, ok := hub.Snapshot().Health["edge"]; !ok {
		t.Fatal("the link should survive an advertisement it may not fully register")
	}
}
