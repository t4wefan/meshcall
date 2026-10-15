package conformance

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// Scenarios run in order; the last two restart the Router and drain the
// fixture, so they must stay last.
var scenarios = []scenario{
	{"registration: service, methods and shapes", registration},
	{"unary: echo round trip, unknown fields ignored", unaryEcho},
	{"unary: invalid request fails only that call", invalidArgument},
	{"unary: invalid response is the server's fault", invalidResponse},
	{"unary: application errors pass through", applicationError},
	{"targeted: instance_id routes, targeted services require it", targeted},
	{"router: unknown method is method_not_found", unknownMethod},
	{"server stream: item credit is respected", serverStreamCredit},
	{"server stream: concurrent streams keep per-call credit", concurrentStreamCredit},
	{"client stream: window, items, end, result", clientStream},
	{"timeout: server enforces timeout_ms", serverTimeout},
	{"idle: server enforces idle_timeout_ms on input", serverIdle},
	{"cancel: handler observes call.cancel", cancelObserved},
	{"metadata: request metadata and trailers", metadataAndTrailers},
	{"duplex: interleaved, per-direction credit, input half-close", duplexInterleaved},
	{"duplex: output half-closes while input stays open", duplexServerHalfClose},
	{"duplex: server enforces idle_timeout_ms on input", duplexIdle},
	{"duplex: an idempotency key is refused", duplexIdempotencyKey},
	{"http: unary calls, metadata, errors and deadlines", httpUnary},
	{"http: server stream as typed NDJSON envelopes", httpServerStream},
	{"http: client stream from an NDJSON body", httpClientStream},
	{"http: closing the request cancels the call", httpCancelOnDisconnect},
	{"idempotency: duplicate keys join once and replay", idempotency},
	{"keepalive: instance pings within the router idle window", keepalive},
	{"reconnect: instance re-registers after router restart", routerRestart},
	{"drain: leaves the pool, finishes streams, disconnects", drain},
}

func registration(h *harness) error {
	snapshot := h.router.Snapshot()
	methods := snapshot.Methods[Service]
	want := map[string]string{
		"echo": "unary", "bad_response": "unary", "fail": "unary", "slow": "unary", "cancelled_tags": "unary", "invoked_tags": "unary",
		"count": "server_stream", "sum": "client_stream", "metadata_echo": "unary", "drain_self": "unary",
		"chat": "duplex", "announce": "duplex",
	}
	if !reflect.DeepEqual(methods, want) {
		return fmt.Errorf("registered methods %v, want %v", methods, want)
	}
	var entry struct {
		Version     string  `json:"version"`
		Description *string `json:"description"`
		Methods     []struct {
			Name        string  `json:"name"`
			Stream      string  `json:"stream"`
			Description *string `json:"description"`
			Annotations struct {
				ReadOnly bool `json:"read_only"`
			} `json:"annotations"`
			Response     any     `json:"response"`
			DuplexReason *string `json:"duplex_reason"`
		} `json:"methods"`
	}
	raw := snapshot.Contracts[Service]
	if raw == nil || json.Unmarshal(raw, &entry) != nil {
		return fmt.Errorf("registration did not carry a contract entry")
	}
	if entry.Version != "1" || entry.Description == nil || *entry.Description == "" {
		return fmt.Errorf("contract entry lacks version/description: %s", raw)
	}
	for _, m := range entry.Methods {
		if m.Description == nil || *m.Description == "" {
			return fmt.Errorf("method %s has no description in the registered contract", m.Name)
		}
		if m.Name == "echo" && !m.Annotations.ReadOnly {
			return fmt.Errorf("echo should be registered read_only")
		}
		if duplex := m.Stream == "duplex"; duplex != (m.DuplexReason != nil && *m.DuplexReason != "") || duplex && m.Response != nil {
			return fmt.Errorf("method %s: a duplex method, and only one, has a duplex_reason and no response: %s", m.Name, raw)
		}
	}
	return nil
}

func unaryEcho(h *harness) error {
	c, err := h.client()
	if err != nil {
		return err
	}
	defer c.close()
	result, err := c.unary("echo", frame{"value": 7, "text": "héllo 😀"}, frame{"future_field": frame{"x": 1}})
	if err != nil {
		return err
	}
	if result["kind"] != "call.result" {
		return fmt.Errorf("expected call.result, got %v", result)
	}
	payload, _ := result["payload"].(map[string]any)
	if number(payload["value"]) != 7 || payload["text"] != "héllo 😀" {
		return fmt.Errorf("echo payload %v", payload)
	}
	return nil
}

func invalidArgument(h *harness) error {
	c, err := h.client()
	if err != nil {
		return err
	}
	defer c.close()
	result, err := c.unary("echo", frame{"value": "not-an-int", "text": "x"}, nil)
	if err != nil {
		return err
	}
	code, message, _, details := errorOf(result)
	if code != "invalid_argument" {
		return fmt.Errorf("expected invalid_argument, got %v", result)
	}
	// Caller's fault, said structurally and without echoing the input.
	if details["source"] != "server" || details["reason"] != "request" {
		return fmt.Errorf("expected details.source=server reason=request, got %v", details)
	}
	violations, _ := details["violations"].([]any)
	if len(violations) == 0 {
		return fmt.Errorf("expected details.violations, got %v", details)
	}
	first, _ := violations[0].(map[string]any)
	if first["path"] != "value" || first["code"] == "" || first["message"] == "" {
		return fmt.Errorf("violation must name path=value with code and message, got %v", first)
	}
	if serialised := fmt.Sprint(message, details); strings.Contains(serialised, "not-an-int") {
		return fmt.Errorf("validation error leaks the input value: %s", serialised)
	}
	// The connection and the service survive.
	ok, err := c.unary("echo", frame{"value": 1, "text": "still here"}, nil)
	if err != nil {
		return err
	}
	if ok["kind"] != "call.result" {
		return fmt.Errorf("connection poisoned: %v", ok)
	}
	return nil
}

func invalidResponse(h *harness) error {
	c, err := h.client()
	if err != nil {
		return err
	}
	defer c.close()
	result, err := c.unary("bad_response", frame{}, nil)
	if err != nil {
		return err
	}
	code, message, _, details := errorOf(result)
	if code != "internal" || message != "Internal service error" {
		return fmt.Errorf("a response that breaks the contract is the server's fault (internal), got %v", result)
	}
	if details["source"] != "server" || details["reason"] != "invalid_response" {
		return fmt.Errorf("expected details.source=server reason=invalid_response, got %v", details)
	}
	if strings.Contains(fmt.Sprint(details), "not-a-bool") {
		return fmt.Errorf("internal error leaks the invalid response: %v", details)
	}
	return nil
}

func targeted(h *harness) error {
	c, err := h.client()
	if err != nil {
		return err
	}
	defer c.close()
	expect := func(f frame, code, reason string) error {
		got, _, _, details := errorOf(f)
		if got != code || details["source"] != "router" || details["reason"] != reason {
			return fmt.Errorf("expected %s/%s from the router, got %v", code, reason, f)
		}
		return nil
	}
	const svc = "conformance.v1.Targeted"
	// A targeted service refuses calls that do not name an instance.
	result, err := c.unary("whoami", frame{}, frame{"service": svc})
	if err != nil {
		return err
	}
	if err := expect(result, "invalid_argument", "instance_required"); err != nil {
		return err
	}
	// Naming the fixture's instance routes there, and the handler sees its own id.
	result, err = c.unary("whoami", frame{}, frame{"service": svc, "instance_id": InstanceID})
	if err != nil {
		return err
	}
	payload, _ := result["payload"].(map[string]any)
	if result["kind"] != "call.result" || payload["instance"] != InstanceID {
		return fmt.Errorf("targeted whoami: %v", result)
	}
	// Unknown instances are refused without retry; ordinary services accept targeting too.
	result, err = c.unary("whoami", frame{}, frame{"service": svc, "instance_id": "no-such-instance"})
	if err != nil {
		return err
	}
	if err := expect(result, "unavailable", "instance_unknown"); err != nil {
		return err
	}
	if _, _, retryable, _ := errorOf(result); retryable {
		return fmt.Errorf("instance_unknown must not be retryable: %v", result)
	}
	result, err = c.unary("echo", frame{"value": 1, "text": "pinned"}, frame{"instance_id": InstanceID})
	if err != nil {
		return err
	}
	if result["kind"] != "call.result" {
		return fmt.Errorf("targeted echo: %v", result)
	}
	// Registration carries the flag.
	if snapshot := h.router.Snapshot(); !snapshot.Targeted[svc] || snapshot.Targeted[Service] {
		return fmt.Errorf("targeted flag not registered as expected: %v", snapshot.Targeted)
	}
	return nil
}

func applicationError(h *harness) error {
	c, err := h.client()
	if err != nil {
		return err
	}
	defer c.close()
	result, err := c.unary("fail", frame{"code": "app.v1.quota_exceeded", "message": "no more", "retryable": true}, nil)
	if err != nil {
		return err
	}
	code, message, retryable, details := errorOf(result)
	if code != "app.v1.quota_exceeded" || message != "no more" || !retryable || details["from"] != "fixture" {
		return fmt.Errorf("error did not pass through: %v", result)
	}
	return nil
}

func unknownMethod(h *harness) error {
	c, err := h.client()
	if err != nil {
		return err
	}
	defer c.close()
	result, err := c.unary("no_such_method", frame{}, nil)
	if err != nil {
		return err
	}
	if code, _, _, _ := errorOf(result); code != "method_not_found" {
		return fmt.Errorf("expected method_not_found, got %v", result)
	}
	return nil
}

func serverStreamCredit(h *harness) error {
	c, err := h.client()
	if err != nil {
		return err
	}
	defer c.close()
	id, err := c.open("count", frame{"n": 40, "gap_ms": 0}, nil)
	if err != nil {
		return err
	}
	if err := c.send(frame{"kind": "stream.window", "call_id": id, "direction": "server", "credit": 16}); err != nil {
		return err
	}
	next := 0
	receiveItems := func(n int) error {
		for i := 0; i < n; i++ {
			f, err := c.receive()
			if err != nil {
				return fmt.Errorf("item %d: %w", next, err)
			}
			if f["kind"] != "stream.item" || f["direction"] != "server" || int(number(f["sequence"])) != next {
				return fmt.Errorf("expected item %d, got %v", next, f)
			}
			if int(number(f["payload"].(map[string]any)["index"])) != next {
				return fmt.Errorf("item %d payload %v", next, f["payload"])
			}
			next++
		}
		return nil
	}
	if err := receiveItems(16); err != nil {
		return err
	}
	// Credit exhausted: the server must wait.
	if err := c.expectNothing(300 * time.Millisecond); err != nil {
		return fmt.Errorf("server sent beyond its credit: %w", err)
	}
	if err := c.send(frame{"kind": "stream.window", "call_id": id, "direction": "server", "credit": 24}); err != nil {
		return err
	}
	if err := receiveItems(24); err != nil {
		return err
	}
	end, err := c.receive()
	if err != nil {
		return err
	}
	if end["kind"] != "stream.end" || end["direction"] != "server" {
		return fmt.Errorf("expected stream.end, got %v", end)
	}
	result, err := c.receive()
	if err != nil {
		return err
	}
	if result["kind"] != "call.result" {
		return fmt.Errorf("expected call.result after stream.end, got %v", result)
	}
	return nil
}

// concurrentStreamCredit runs many streams on one connection, granting a
// little credit up front and one more per item consumed, as the runtimes do.
// The Router enforces each call's credit (docs/protocol.md → Flow control), so
// a producer that overran it would fail with flow_control_violation here.
func concurrentStreamCredit(h *harness) error {
	c, err := h.client()
	if err != nil {
		return err
	}
	defer c.close()
	const streams, items, initial = 24, 20, 4
	next, granted := map[string]int{}, map[string]int{}
	for range streams {
		id, err := c.open("count", frame{"n": items, "gap_ms": 0}, nil)
		if err != nil {
			return err
		}
		if err := c.send(frame{"kind": "stream.window", "call_id": id, "direction": "server", "credit": initial}); err != nil {
			return err
		}
		next[id], granted[id] = 0, initial
	}
	// Before any replenishment each stream stops at its initial credit.
	for received := 0; received < streams*initial; received++ {
		f, err := c.receive()
		if err != nil {
			return err
		}
		id, _ := f["call_id"].(string)
		if f["kind"] != "stream.item" || int(number(f["sequence"])) != next[id] {
			return fmt.Errorf("expected item %d of %s, got %v", next[id], id, f)
		}
		if next[id]++; next[id] > granted[id] {
			return fmt.Errorf("%s sent item %d with %d credit", id, next[id]-1, granted[id])
		}
	}
	if err := c.expectNothing(200 * time.Millisecond); err != nil {
		return fmt.Errorf("a stream sent beyond its initial credit: %w", err)
	}
	for id := range next {
		if err := c.send(frame{"kind": "stream.window", "call_id": id, "direction": "server", "credit": initial}); err != nil {
			return err
		}
		granted[id] += initial
	}
	for done := 0; done < streams; {
		f, err := c.receive()
		if err != nil {
			return err
		}
		id, _ := f["call_id"].(string)
		switch f["kind"] {
		case "stream.item":
			if int(number(f["sequence"])) != next[id] {
				return fmt.Errorf("expected item %d of %s, got %v", next[id], id, f)
			}
			next[id]++
			if granted[id] < items {
				if err := c.send(frame{"kind": "stream.window", "call_id": id, "direction": "server", "credit": 1}); err != nil {
					return err
				}
				granted[id]++
			}
		case "stream.end":
		case "call.result":
			if next[id] != items {
				return fmt.Errorf("%s finished after %d of %d items", id, next[id], items)
			}
			done++
		default:
			return fmt.Errorf("unexpected frame %v", f)
		}
	}
	return nil
}

func clientStream(h *harness) error {
	c, err := h.client()
	if err != nil {
		return err
	}
	defer c.close()
	id, err := c.open("sum", frame{"offset": 10}, nil)
	if err != nil {
		return err
	}
	window, err := c.receive()
	if err != nil {
		return err
	}
	if window["kind"] != "stream.window" || window["direction"] != "client" || number(window["credit"]) != 16 {
		return fmt.Errorf("expected initial client window of 16, got %v", window)
	}
	for i := 1; i <= 5; i++ {
		if err := c.send(frame{"kind": "stream.item", "call_id": id, "direction": "client", "sequence": i - 1, "payload": frame{"value": i}}); err != nil {
			return err
		}
	}
	// Credit is replenished as items are consumed while the stream is open.
	// Wait for the first grant before half-closing: a server that sees the
	// end before it consumed anything has nothing left to grant for, so
	// sending the end immediately would make this check timing-dependent.
	credits := 0
	for credits == 0 {
		f, err := c.receive()
		if err != nil {
			return err
		}
		if f["kind"] != "stream.window" || f["direction"] != "client" || number(f["credit"]) < 1 {
			return fmt.Errorf("expected the server to replenish input credit, got %v", f)
		}
		credits += int(number(f["credit"]))
	}
	if err := c.send(frame{"kind": "stream.end", "call_id": id, "direction": "client"}); err != nil {
		return err
	}
	for {
		f, err := c.receive()
		if err != nil {
			return err
		}
		switch f["kind"] {
		case "stream.window":
			credits += int(number(f["credit"]))
		case "call.result":
			total := number(f["payload"].(map[string]any)["total"])
			if total != 25 {
				return fmt.Errorf("sum total %v, want 25", total)
			}
			return nil
		case "call.error":
			return fmt.Errorf("sum failed: %v", f)
		}
	}
}

func serverTimeout(h *harness) error {
	c, err := h.client()
	if err != nil {
		return err
	}
	defer c.close()
	started := time.Now()
	result, err := c.unary("slow", frame{"ms": 2000, "tag": "timeout"}, frame{"timeout_ms": 100})
	if err != nil {
		return err
	}
	elapsed := time.Since(started)
	code, _, retryable, details := errorOf(result)
	if code != "deadline_exceeded" || retryable {
		return fmt.Errorf("expected non-retryable deadline_exceeded, got %v", result)
	}
	if details["source"] != "server" {
		return fmt.Errorf("deadline must come from the server (router grace is 300ms): %v", result)
	}
	// The lower bound allows one tick of a coarse timer: asyncio fires a
	// timer due within the clock's resolution (15.6 ms on Windows) early.
	if elapsed < 80*time.Millisecond || elapsed > 700*time.Millisecond {
		return fmt.Errorf("deadline fired after %v", elapsed)
	}
	return nil
}

func serverIdle(h *harness) error {
	c, err := h.client()
	if err != nil {
		return err
	}
	defer c.close()
	id, err := c.open("sum", frame{"offset": 0}, frame{"idle_timeout_ms": 150})
	if err != nil {
		return err
	}
	if _, err := c.receive(); err != nil { // initial window
		return err
	}
	if err := c.send(frame{"kind": "stream.item", "call_id": id, "direction": "client", "sequence": 0, "payload": frame{"value": 1}}); err != nil {
		return err
	}
	for {
		f, err := c.receive()
		if err != nil {
			return err
		}
		if f["kind"] == "stream.window" {
			continue
		}
		code, _, _, details := errorOf(f)
		if f["kind"] != "call.error" || code != "deadline_exceeded" || details["reason"] != "idle" || details["source"] != "server" {
			return fmt.Errorf("expected idle deadline_exceeded from server, got %v", f)
		}
		return nil
	}
}

func cancelObserved(h *harness) error {
	c, err := h.client()
	if err != nil {
		return err
	}
	defer c.close()
	id, err := c.open("slow", frame{"ms": 5000, "tag": "cancel-me"}, nil)
	if err != nil {
		return err
	}
	time.Sleep(100 * time.Millisecond)
	if err := c.send(frame{"kind": "call.cancel", "call_id": id, "reason": "conformance"}); err != nil {
		return err
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		result, err := c.unary("cancelled_tags", frame{}, nil)
		if err != nil {
			return err
		}
		tags, _ := result["payload"].(map[string]any)["tags"].([]any)
		for _, t := range tags {
			if t == "cancel-me" {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("handler never observed cancellation")
}

func metadataAndTrailers(h *harness) error {
	c, err := h.client()
	if err != nil {
		return err
	}
	defer c.close()
	result, err := c.unary("metadata_echo", frame{}, frame{"metadata": frame{"a": "1", "traceparent": "00-abc"}})
	if err != nil {
		return err
	}
	if result["kind"] != "call.result" {
		return fmt.Errorf("metadata_echo failed: %v", result)
	}
	echoed, _ := result["payload"].(map[string]any)["metadata"].(map[string]any)
	if echoed["a"] != "1" || echoed["traceparent"] != "00-abc" {
		return fmt.Errorf("handler did not see metadata: %v", result["payload"])
	}
	trailers, _ := result["metadata"].(map[string]any)
	if trailers["echoed"] != "yes" || trailers["count"] != "2" {
		return fmt.Errorf("trailers missing or wrong: %v", result["metadata"])
	}
	return nil
}

func idempotency(h *harness) error {
	c1, err := h.client()
	if err != nil {
		return err
	}
	defer c1.close()
	c2, err := h.client()
	if err != nil {
		return err
	}
	defer c2.close()
	key := frame{"idempotency_key": "conf-" + fmt.Sprint(time.Now().UnixNano())}
	id1, err := c1.open("slow", frame{"ms": 300, "tag": "idem"}, key)
	if err != nil {
		return err
	}
	time.Sleep(50 * time.Millisecond)
	id2, err := c2.open("slow", frame{"ms": 300, "tag": "idem"}, key)
	if err != nil {
		return err
	}
	for _, pair := range []struct {
		c  *client
		id string
	}{{c1, id1}, {c2, id2}} {
		f, err := pair.c.receive()
		if err != nil {
			return err
		}
		if f["kind"] != "call.result" || f["call_id"] != pair.id {
			return fmt.Errorf("joined caller got %v", f)
		}
	}
	// A third call after completion is served from the Router's cache.
	started := time.Now()
	replay, err := c1.unary("slow", frame{"ms": 300, "tag": "idem"}, key)
	if err != nil {
		return err
	}
	if replay["kind"] != "call.result" || time.Since(started) > 150*time.Millisecond {
		return fmt.Errorf("replay was not served from cache: %v after %v", replay, time.Since(started))
	}
	// The handler ran exactly once for the key; the Router (not the service)
	// owns the guarantee, so the service's own view is the authority here.
	tags, err := c1.unary("invoked_tags", frame{}, nil)
	if err != nil {
		return err
	}
	count := 0
	for _, t := range tags["payload"].(map[string]any)["tags"].([]any) {
		if t == "idem" {
			count++
		}
	}
	if count != 1 {
		return fmt.Errorf("handler ran %d times for one idempotency key", count)
	}
	// Service handlers can read the key.
	return nil
}

func keepalive(h *harness) error {
	// Router idle timeout is 1.5s; the fixture was asked to ping every 500ms.
	time.Sleep(3500 * time.Millisecond)
	if !h.registered() {
		return fmt.Errorf("instance was dropped: it does not send protocol pings often enough")
	}
	return nil
}

func routerRestart(h *harness) error {
	h.router.Close()
	time.Sleep(200 * time.Millisecond)
	if err := h.startRouter(); err != nil {
		return fmt.Errorf("restart router at %s: %w", h.routerURL(), err)
	}
	if err := h.waitRegistered(10 * time.Second); err != nil {
		return fmt.Errorf("instance did not re-register after router restart: %w", err)
	}
	c, err := h.client()
	if err != nil {
		return err
	}
	defer c.close()
	result, err := c.unary("echo", frame{"value": 1, "text": "back"}, nil)
	if err != nil {
		return err
	}
	if result["kind"] != "call.result" {
		return fmt.Errorf("call after re-registration failed: %v", result)
	}
	return nil
}

func drain(h *harness) error {
	c, err := h.client()
	if err != nil {
		return err
	}
	defer c.close()
	id, err := c.open("count", frame{"n": 30, "gap_ms": 40}, nil)
	if err != nil {
		return err
	}
	if err := c.send(frame{"kind": "stream.window", "call_id": id, "direction": "server", "credit": 64}); err != nil {
		return err
	}
	if _, err := c.receive(); err != nil { // first item: the stream is live
		return err
	}
	talk, err := h.client()
	if err != nil {
		return err
	}
	defer talk.close()
	chat, err := talk.openChat("chat", frame{"prefix": "~"}, nil)
	if err != nil {
		return err
	}
	if err := talk.send(frame{"kind": "stream.window", "call_id": chat, "direction": "server", "credit": 8}); err != nil {
		return err
	}
	control, err := h.client()
	if err != nil {
		return err
	}
	defer control.close()
	result, err := control.unary("drain_self", frame{"delay_ms": 0}, nil)
	if err != nil {
		return err
	}
	if result["kind"] != "call.result" {
		return fmt.Errorf("drain_self failed: %v", result)
	}
	if err := h.waitUnregistered(3 * time.Second); err != nil {
		return fmt.Errorf("drain did not remove the instance from the pool: %w", err)
	}
	// New calls find no instance; the live stream completes.
	gone, err := control.unary("echo", frame{"value": 1, "text": "x"}, nil)
	if err != nil {
		return err
	}
	if code, _, _, _ := errorOf(gone); code != "unavailable" {
		return fmt.Errorf("expected unavailable after drain, got %v", gone)
	}
	// A duplex call open before the drain goes on in both directions.
	if err := talk.input(chat, 0, "still"); err != nil {
		return err
	}
	if err := talk.expectOutput(chat, 0, "~still"); err != nil {
		return fmt.Errorf("duplex during drain: %w", err)
	}
	if err := talk.send(frame{"kind": "stream.end", "call_id": chat, "direction": "client"}); err != nil {
		return err
	}
	if err := talk.expectOutput(chat, 1, "done:1"); err != nil {
		return err
	}
	if err := talk.expectEnd(chat); err != nil {
		return fmt.Errorf("duplex during drain: %w", err)
	}
	items := 1
	for {
		f, err := c.receive()
		if err != nil {
			return fmt.Errorf("stream broke during drain after %d items: %w", items, err)
		}
		switch f["kind"] {
		case "stream.item":
			items++
		case "stream.end":
		case "call.result":
			if items != 30 {
				return fmt.Errorf("stream ended early: %d items", items)
			}
			select {
			case <-h.exited:
				return nil
			case <-time.After(5 * time.Second):
				return fmt.Errorf("fixture process did not exit after draining")
			}
		case "call.error":
			return fmt.Errorf("stream failed during drain: %v", f)
		}
	}
}
