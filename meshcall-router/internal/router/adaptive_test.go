package router

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestAdaptiveScoringPrefersFasterAndIdlerInstances(t *testing.T) {
	fast, slow, cold := &peer{instanceID: "fast"}, &peer{instanceID: "slow"}, &peer{instanceID: "cold"}
	for range 4 {
		fast.observeDuration("echo", 10*time.Millisecond)
		slow.observeDuration("echo", 100*time.Millisecond)
	}
	all := []*peer{fast, slow, cold}
	// The cold instance starts from the pool average, so it is neither
	// starved (scored 0 would always win) nor excluded.
	if got := serviceTime(cold, "echo", all); got != 55*time.Millisecond {
		t.Fatalf("cold-start estimate = %v, want pool average 55ms", got)
	}
	// Queueing counts: a fast instance with four calls in flight loses to an
	// idle slow one only once its expected completion is actually longer.
	fast.inflight = 4
	if expectedCompletion(fast, "echo", all) != 50*time.Millisecond || expectedCompletion(slow, "echo", all) != 100*time.Millisecond {
		t.Fatalf("expected 50ms vs 100ms, got %v vs %v", expectedCompletion(fast, "echo", all), expectedCompletion(slow, "echo", all))
	}
	fast.inflight = 12
	if expectedCompletion(fast, "echo", all) <= expectedCompletion(slow, "echo", all) {
		t.Fatal("a deep queue on the fast instance should cost more than an idle slow one")
	}
	// RTT is part of the estimate: a remote twin of fast loses to fast.
	remote := &peer{instanceID: "remote", rtt: 30 * time.Millisecond, stats: fast.stats}
	fast.inflight = 0
	if expectedCompletion(remote, "echo", all) <= expectedCompletion(fast, "echo", all) {
		t.Fatal("rtt must penalise the remote instance")
	}
	// Power of two: with a scripted draw the better of the two drawn wins.
	draws := []int{1, 0} // picks slow (index 1), then fast (index 0 after skip adjustment)
	pick := func(n int) int { v := draws[0]; draws = draws[1:]; return v }
	if got := pickAdaptive([]*peer{fast, slow, cold}, "echo", pick); got != fast {
		t.Fatalf("power of two picked %s, want fast", got.instanceID)
	}
}

func TestAdaptiveRoutingLearnsFromResults(t *testing.T) {
	r := runningRouter(t, nil)
	// Deterministic draws: always compare instance 0 and 1 (a and b).
	r.mu.Lock()
	r.pick = func(n int) int { return 0 }
	r.mu.Unlock()
	register := func(id string) *websocket.Conn {
		conn := dial(t, r, "")
		hello(t, conn, "server", id)
		send(t, conn, frame{Kind: "server.register", InstanceID: id, Services: []service{
			{Name: testService, Methods: []method{{Name: "echo", Stream: "unary", Balance: balance{Kind: "adaptive"}}}},
		}})
		if got := receive(t, conn); got.Kind != "server.register.ack" {
			t.Fatalf("register %s: %+v", id, got)
		}
		return conn
	}
	a, b := register("a"), register("b")
	client := dial(t, r, "")
	hello(t, client, "client", "c")
	open := func(id string) {
		send(t, client, frame{Kind: "call.open", CallID: id, Service: testService, Method: "echo", Payload: json.RawMessage(`{}`)})
	}
	// Nobody knows anything: both score 0, the first drawn (a) wins, and its
	// answer becomes the first service-time sample.
	open("c1")
	if got := receive(t, a); got.CallID != "c1" {
		t.Fatalf("c1 not on a: %+v", got)
	}
	send(t, a, frame{Kind: "call.result", CallID: "c1", Payload: json.RawMessage(`{}`)})
	receive(t, client)
	r.mu.Lock()
	learned := r.instances["a"].stats["echo"]
	// Fix the estimates so the rest of the test is independent of timing:
	// a is slow (121 ms), b is fast (5 ms).
	r.instances["a"].stats["echo"] = &methodStats{ewma: 121 * time.Millisecond, samples: 4}
	r.instances["b"].stats = map[string]*methodStats{"echo": {ewma: 5 * time.Millisecond, samples: 4}}
	r.mu.Unlock()
	if learned == nil || learned.samples != 1 || learned.ewma <= 0 {
		t.Fatalf("result did not teach the Router a's service time: %+v", learned)
	}
	// Idle: the faster instance wins. Park calls on it without answering;
	// expected completion grows with its queue (5 ms × (inflight + 1)) and
	// crosses a's idle 121 ms after 24 parked calls.
	for i := range 24 {
		id := "park" + string(rune('a'+i))
		open(id)
		if got := receive(t, b); got.CallID != id {
			t.Fatalf("%s not on the faster b: %+v", id, got)
		}
	}
	open("flip")
	if got := receive(t, a); got.CallID != "flip" {
		t.Fatalf("expected the deep queue on b to send flip to a, got %+v", got)
	}
}
