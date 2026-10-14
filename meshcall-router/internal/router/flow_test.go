package router

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// frames is the number of frames waiting in the queue.
func (q *outbox) frames() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := 0
	for _, queued := range q.calls {
		n += len(queued)
	}
	return n
}

// dialSlowReader connects with a small receive buffer, so frames the client
// does not read back up into the Router's outbox instead of the kernel's.
func dialSlowReader(t *testing.T, r *Router) *websocket.Conn {
	t.Helper()
	transport := &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		if err == nil {
			_ = conn.(*net.TCPConn).SetReadBuffer(4096)
		}
		return conn, err
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, fmt.Sprintf("ws://127.0.0.1:%d", r.Ready().Port),
		&websocket.DialOptions{HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	conn.SetReadLimit(1 << 20)
	t.Cleanup(func() { _ = conn.CloseNow() })
	return conn
}

func clientPeer(r *Router) *peer {
	r.mu.Lock()
	defer r.mu.Unlock()
	for p := range r.peers {
		if p.role == "client" {
			return p
		}
	}
	return nil
}

// Many concurrent streams, each within the credit the client granted, may
// back up more frames than any fixed count while the client is not reading.
// The client asked for every one of them, so it must not be disconnected.
func TestConcurrentStreamsToASlowReaderStayConnected(t *testing.T) {
	r := runningRouter(t, nil)
	instance := dial(t, r, "")
	registerPeer(t, instance, "a")
	client := dialSlowReader(t, r)
	hello(t, client, "client", "client")
	const streams, credit = 128, 16
	window := int64(credit)
	for i := range streams {
		id := fmt.Sprint("s", i)
		send(t, client, frame{Kind: "call.open", CallID: id, Service: testService, Method: "download", Payload: json.RawMessage(`{}`)})
		send(t, client, frame{Kind: "stream.window", CallID: id, Direction: "server", Credit: &window})
	}
	for range 2 * streams {
		receive(t, instance)
	}
	payload := json.RawMessage(`"` + strings.Repeat("x", 2048) + `"`)
	for i := range streams {
		id := fmt.Sprint("s", i)
		for seq := range int64(credit) {
			send(t, instance, frame{Kind: "stream.item", CallID: id, Direction: "server", Sequence: &seq, Payload: payload})
		}
		send(t, instance, frame{Kind: "stream.end", CallID: id, Direction: "server"})
		send(t, instance, frame{Kind: "call.result", CallID: id})
	}
	eventually(t, func() bool { return r.Snapshot().Routes == 0 })
	p := clientPeer(r)
	if p == nil || p.ctx.Err() != nil {
		t.Fatal("the Router disconnected a client that read slowly but stayed within its credit")
	}
	if backlog := p.out.frames(); backlog <= 256 {
		t.Fatalf("only %d frames backed up; the test did not reproduce a slow reader", backlog)
	}
	items := make(map[string]int64, streams)
	for done := 0; done < streams; {
		f := receive(t, client)
		switch f.Kind {
		case "stream.item":
			if *f.Sequence != items[f.CallID] {
				t.Fatalf("%s: item %d out of order", f.CallID, *f.Sequence)
			}
			items[f.CallID]++
		case "stream.end":
		case "call.result":
			if items[f.CallID] != credit {
				t.Fatalf("%s ended after %d items", f.CallID, items[f.CallID])
			}
			done++
		default:
			t.Fatalf("unexpected frame %+v", f)
		}
	}
}

// An item beyond the consumer's credit is the producer's violation: only that
// call fails, and neither connection is closed.
func TestItemsBeyondCreditFailOnlyThatCall(t *testing.T) {
	r := runningRouter(t, nil)
	instance := dial(t, r, "")
	registerPeer(t, instance, "a")
	client := dial(t, r, "")
	hello(t, client, "client", "client")
	violation := func(f frame) bool {
		return f.Kind == "call.error" && f.Error.Code == "protocol_error" && !f.Error.Retryable &&
			bytes.Contains(f.Error.Details, []byte(`"reason":"flow_control_violation"`)) &&
			bytes.Contains(f.Error.Details, []byte(`"source":"router"`))
	}

	// The instance overruns the client's credit of two.
	two := int64(2)
	send(t, client, frame{Kind: "call.open", CallID: "server-overrun", Service: testService, Method: "download", Payload: json.RawMessage(`{}`)})
	send(t, client, frame{Kind: "stream.window", CallID: "server-overrun", Direction: "server", Credit: &two})
	receive(t, instance)
	receive(t, instance)
	for seq := range int64(3) {
		send(t, instance, frame{Kind: "stream.item", CallID: "server-overrun", Direction: "server", Sequence: &seq, Payload: json.RawMessage(`1`)})
	}
	for seq := range int64(2) {
		if got := receive(t, client); got.Kind != "stream.item" || *got.Sequence != seq {
			t.Fatalf("credited item %d: %+v", seq, got)
		}
	}
	if got := receive(t, client); !violation(got) || !bytes.Contains(got.Error.Details, []byte(`"instance_id":"a"`)) {
		t.Fatalf("expected flow_control_violation naming the instance, got %+v", got)
	}
	if got := receive(t, instance); got.Kind != "call.cancel" || got.Reason != "flow_control_violation" {
		t.Fatalf("instance was not told: %+v", got)
	}
	if got := r.Snapshot().Health["a"]; got.Failures != 1 {
		t.Fatalf("an overrunning instance counts as a failure, got %+v", got)
	}

	// The client sends input before the instance granted any credit.
	send(t, client, frame{Kind: "call.open", CallID: "client-overrun", Service: testService, Method: "download", Payload: json.RawMessage(`{}`)})
	receive(t, instance)
	zero := int64(0)
	send(t, client, frame{Kind: "stream.item", CallID: "client-overrun", Direction: "client", Sequence: &zero, Payload: json.RawMessage(`1`)})
	if got := receive(t, instance); got.Kind != "call.cancel" || got.Reason != "flow_control_violation" {
		t.Fatalf("expected the uncredited item to be dropped and the call cancelled, got %+v", got)
	}
	if got := receive(t, client); !violation(got) {
		t.Fatalf("expected flow_control_violation, got %+v", got)
	}

	// Both connections and the instance's pool membership survive.
	for _, conn := range []*websocket.Conn{client, instance} {
		send(t, conn, frame{Kind: "ping", Nonce: "alive"})
		if got := receive(t, conn); got.Kind != "pong" {
			t.Fatalf("connection closed after a flow-control violation: %+v", got)
		}
	}
	eventually(t, func() bool {
		s := r.Snapshot()
		return s.Routes == 0 && s.Health["a"].Inflight == 0
	})
	send(t, client, frame{Kind: "call.open", CallID: "after", Service: testService, Method: "echo", Payload: json.RawMessage(`{}`)})
	if got := receive(t, instance); got.CallID != "after" {
		t.Fatalf("instance no longer routed: %+v", got)
	}
}
