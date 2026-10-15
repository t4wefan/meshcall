package router

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// watch reads conn in the background: frames go to the first channel, and the
// close status the connection ended with to the second.
func watch(conn *websocket.Conn) (<-chan frame, <-chan websocket.StatusCode) {
	frames, status := make(chan frame, 64), make(chan websocket.StatusCode, 1)
	go func() {
		defer close(frames)
		for {
			_, data, err := conn.Read(context.Background())
			if err != nil {
				status <- websocket.CloseStatus(err)
				return
			}
			if f, err := decode(data); err == nil {
				frames <- f
			}
		}
	}()
	return frames, status
}

func closedWith(t *testing.T, status <-chan websocket.StatusCode, want websocket.StatusCode) {
	t.Helper()
	select {
	case got := <-status:
		if got != want {
			t.Fatalf("closed with %d, want %d", got, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("connection not closed within 3s")
	}
}

func hasReason(f frame, code, reason string) bool {
	return f.Kind == "call.error" && f.Error.Code == code && f.Error.Retryable &&
		bytes.Contains(f.Error.Details, []byte(fmt.Sprintf(`"reason":%q`, reason)))
}

func readyStatus(t *testing.T, r *Router) int {
	t.Helper()
	resp, err := http.Get(httpURL(r, "/readyz"))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

// Draining refuses new work, closes idle connections at once and the others
// as their last call ends, and stops without waiting out the timeout.
func TestShutdownDrainsCallsInFlight(t *testing.T) {
	r, err := Start(Config{ShutdownTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	instance := fakeInstance(t, r)
	busy := dial(t, r, "")
	hello(t, busy, "client", "busy")
	idle := dial(t, r, "")
	hello(t, idle, "client", "idle")
	send(t, busy, frame{Kind: "call.open", CallID: "c1", Service: testService, Method: "echo", Payload: json.RawMessage(`{}`)})
	if got := receive(t, instance); got.CallID != "c1" {
		t.Fatalf("open: %+v", got)
	}
	_, instanceClosed := watch(instance)
	busyFrames, busyClosed := watch(busy)
	_, idleClosed := watch(idle)

	started := time.Now()
	stopped := make(chan struct{})
	go func() { r.Shutdown(); close(stopped) }()
	closedWith(t, idleClosed, websocket.StatusGoingAway)
	if status := readyStatus(t, r); status != http.StatusServiceUnavailable {
		t.Fatalf("/readyz while draining: %d", status)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, resp, err := websocket.Dial(ctx, fmt.Sprintf("ws://127.0.0.1:%d", r.Ready().Port), nil); err == nil || resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("a new connection while draining: %v", err)
	}
	resp, err := http.Post(httpURL(r, "/v1/call/"+testService+"/echo"), "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if status, e := httpError(t, resp); status != http.StatusServiceUnavailable || e.Code != "unavailable" || !e.Retryable {
		t.Fatalf("an HTTP call while draining: %d %+v", status, e)
	}

	send(t, busy, frame{Kind: "call.open", CallID: "c2", Service: testService, Method: "echo", Payload: json.RawMessage(`{}`)})
	if got := next(t, busyFrames); got.CallID != "c2" || !hasReason(got, "unavailable", "router_draining") {
		t.Fatalf("a new call while draining: %+v", got)
	}
	select {
	case <-stopped:
		t.Fatal("stopped with a call in flight")
	case <-time.After(100 * time.Millisecond):
	}
	send(t, instance, frame{Kind: "call.result", CallID: "c1", Payload: json.RawMessage(`{"ok":true}`)})
	if got := next(t, busyFrames); got.Kind != "call.result" || got.CallID != "c1" {
		t.Fatalf("the call in flight: %+v", got)
	}
	closedWith(t, busyClosed, websocket.StatusGoingAway)
	closedWith(t, instanceClosed, websocket.StatusGoingAway)
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown did not return once drained")
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("Shutdown waited %v", elapsed)
	}
}

// After the timeout, what is still in flight ends with router_shutdown on
// both sides; joined idempotent calls end with it too.
func TestShutdownTimeoutEndsRemainingCalls(t *testing.T) {
	r, err := Start(Config{ShutdownTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	instance := fakeInstance(t, r)
	client := dial(t, r, "")
	hello(t, client, "client", "client")
	window := int64(4)
	send(t, client, frame{Kind: "call.open", CallID: "s", Service: testService, Method: "download", Payload: json.RawMessage(`{}`)})
	send(t, client, frame{Kind: "stream.window", CallID: "s", Direction: "server", Credit: &window})
	send(t, client, frame{Kind: "call.open", CallID: "u1", Service: testService, Method: "echo", Payload: json.RawMessage(`{}`), Idempotency: "k"})
	send(t, client, frame{Kind: "call.open", CallID: "u2", Service: testService, Method: "echo", Payload: json.RawMessage(`{}`), Idempotency: "k"})
	for range 3 {
		receive(t, instance)
	}
	eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.routes["u1"] != nil && len(r.routes["u1"].dedup.waiters) == 1
	})
	instanceFrames, instanceClosed := watch(instance)
	clientFrames, clientClosed := watch(client)

	started := time.Now()
	r.Shutdown()
	if elapsed := time.Since(started); elapsed < 200*time.Millisecond {
		t.Fatalf("Shutdown returned after %v, before its timeout", elapsed)
	}
	ended := map[string]bool{}
	for range 3 {
		f := next(t, clientFrames)
		if !hasReason(f, "unavailable", "router_shutdown") {
			t.Fatalf("client: %+v", f)
		}
		ended[f.CallID] = true
	}
	if len(ended) != 3 {
		t.Fatalf("ended: %v", ended)
	}
	cancelled := map[string]bool{}
	for range 2 {
		f := next(t, instanceFrames)
		if f.Kind != "call.cancel" || f.Reason != "router_shutdown" {
			t.Fatalf("instance: %+v", f)
		}
		cancelled[f.CallID] = true
	}
	if !cancelled["s"] || !cancelled["u1"] {
		t.Fatalf("cancelled: %v", cancelled)
	}
	closedWith(t, clientClosed, websocket.StatusGoingAway)
	closedWith(t, instanceClosed, websocket.StatusGoingAway)
}

func TestMaxCallsPerConnection(t *testing.T) {
	r, err := Start(Config{MaxCallsPerConnection: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	instance := fakeInstance(t, r)
	client := dial(t, r, "")
	hello(t, client, "client", "client")
	other := dial(t, r, "")
	hello(t, other, "client", "other")
	open := func(conn *websocket.Conn, id, key string) {
		send(t, conn, frame{Kind: "call.open", CallID: id, Service: testService, Method: "echo", Payload: json.RawMessage(`{}`), Idempotency: key})
	}
	open(client, "a", "")
	open(client, "b", "k") // a call that others join still counts once per caller
	receive(t, instance)
	receive(t, instance)
	open(client, "c", "")
	if got := receive(t, client); got.CallID != "c" || !hasReason(got, "resource_exhausted", "overloaded") {
		t.Fatalf("beyond the limit: %+v", got)
	}
	open(other, "d", "k") // joins b: one call for other
	open(other, "e", "")
	if got := receive(t, instance); got.CallID != "e" {
		t.Fatalf("another connection has its own limit: %+v", got)
	}
	open(other, "f", "")
	if got := receive(t, other); got.CallID != "f" || !hasReason(got, "resource_exhausted", "overloaded") {
		t.Fatalf("a joined call counts: %+v", got)
	}
	send(t, instance, frame{Kind: "call.result", CallID: "a", Payload: json.RawMessage(`{}`)})
	if got := receive(t, client); got.CallID != "a" || got.Kind != "call.result" {
		t.Fatalf("result: %+v", got)
	}
	open(client, "g", "")
	if got := receive(t, instance); got.CallID != "g" {
		t.Fatalf("a finished call frees its slot: %+v", got)
	}
	unlimited, err := Start(Config{MaxCallsPerConnection: -1})
	if err != nil || unlimited.config.MaxCallsPerConnection != -1 {
		t.Fatalf("no limit: %v", err)
	}
	unlimited.Close()
}

// Close without Shutdown performs the last step at once.
func TestCloseEndsCallsAtOnce(t *testing.T) {
	r, err := Start(Config{})
	if err != nil {
		t.Fatal(err)
	}
	instance := fakeInstance(t, r)
	client := dial(t, r, "")
	hello(t, client, "client", "client")
	send(t, client, frame{Kind: "call.open", CallID: "c", Service: testService, Method: "echo", Payload: json.RawMessage(`{}`)})
	receive(t, instance)
	instanceFrames, _ := watch(instance)
	clientFrames, clientClosed := watch(client)
	started := time.Now()
	r.Close()
	if elapsed := time.Since(started); elapsed > closeGrace {
		t.Fatalf("Close took %v", elapsed)
	}
	if got := next(t, clientFrames); !hasReason(got, "unavailable", "router_shutdown") {
		t.Fatalf("client: %+v", got)
	}
	if got := next(t, instanceFrames); got.Kind != "call.cancel" || got.Reason != "router_shutdown" {
		t.Fatalf("instance: %+v", got)
	}
	closedWith(t, clientClosed, websocket.StatusGoingAway)
	select {
	case <-r.Done():
	default:
		t.Fatal(errors.New("Done not closed"))
	}
}
