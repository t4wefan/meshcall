package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/t4wefan/meshcall/meshcall-router/internal/localsock"
	"github.com/t4wefan/meshcall/meshcall-router/internal/router"
)

// fakeInstance registers a service with a contract and answers calls like a
// runtime would, so the CLI can be tested without Python or TypeScript.
func fakeInstance(t *testing.T, routerURL string) {
	t.Helper()
	ctx := context.Background()
	endpoint, err := localsock.Parse(routerURL)
	if err != nil {
		t.Fatal(err)
	}
	conn, _, err := websocket.Dial(ctx, endpoint.URL, &websocket.DialOptions{HTTPClient: endpoint.HTTPClient()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	send := func(v any) {
		data, _ := json.Marshal(v)
		if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
			t.Error(err)
		}
	}
	recv := func() map[string]any {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return nil
		}
		var f map[string]any
		_ = json.Unmarshal(data, &f)
		return f
	}
	send(map[string]any{"kind": "hello", "protocol": "meshcall/1", "role": "server", "peer_id": "fake", "instance_id": "fake"})
	if recv()["kind"] != "hello.ack" {
		t.Fatal("hello")
	}
	contract := map[string]any{
		"name": "demo.v1.Greeter", "version": "1", "description": "Says hello.\nSecond line.",
		"methods": []map[string]any{
			{"name": "greet", "stream": "unary", "description": "Greet someone.", "parameters": []string{"name", "times"},
				"annotations": map[string]bool{"read_only": true, "idempotent": true},
				"request": map[string]any{"schema": map[string]any{"type": "object", "properties": map[string]any{
					"name": map[string]any{"type": "string"}, "times": map[string]any{"type": "integer", "default": 1}}, "required": []string{"name"}}}},
			{"name": "count", "stream": "server_stream", "description": "Count up.", "parameters": []string{"n"}},
			{"name": "sum", "stream": "client_stream", "description": "Add items.", "parameters": []string{}},
			{"name": "echo", "stream": "duplex", "description": "Echo items.", "parameters": []string{},
				"duplex_reason": "answers while the caller types"},
		},
	}
	send(map[string]any{"kind": "server.register", "instance_id": "fake", "max_inflight": 4, "services": []map[string]any{{
		"name": "demo.v1.Greeter", "contract": contract,
		"methods": []map[string]any{
			{"name": "greet", "stream": "unary", "balance": map[string]any{"kind": "round_robin"}},
			{"name": "count", "stream": "server_stream", "balance": map[string]any{"kind": "round_robin"}},
			{"name": "sum", "stream": "client_stream", "balance": map[string]any{"kind": "round_robin"}},
			{"name": "echo", "stream": "duplex", "balance": map[string]any{"kind": "round_robin"}},
		},
	}}})
	if recv()["kind"] != "server.register.ack" {
		t.Fatal("register")
	}
	go func() {
		totals := map[string]float64{}
		echoes := map[string]int{} // duplex calls: output sequence so far
		// count calls send only as much as the caller's credit allows, like a
		// runtime: the Router drops a client whose outbox overflows.
		type countCall struct{ n, next, credit int }
		counts := map[string]*countCall{}
		flush := func(id string) {
			c := counts[id]
			for ; c.credit > 0 && c.next < c.n; c.next, c.credit = c.next+1, c.credit-1 {
				send(map[string]any{"kind": "stream.item", "call_id": id, "direction": "server", "sequence": c.next, "payload": map[string]any{"i": c.next}})
			}
			if c.next == c.n {
				send(map[string]any{"kind": "stream.end", "call_id": id, "direction": "server"})
				send(map[string]any{"kind": "call.result", "call_id": id, "payload": nil})
				delete(counts, id)
			}
		}
		for {
			f := recv()
			if f == nil {
				return
			}
			id, _ := f["call_id"].(string)
			switch f["kind"] {
			case "call.open":
				payload, _ := f["payload"].(map[string]any)
				switch f["method"] {
				case "greet":
					name, ok := payload["name"].(string)
					if !ok {
						send(map[string]any{"kind": "call.error", "call_id": id, "error": map[string]any{
							"code": "invalid_argument", "message": "Invalid request",
							"details": map[string]any{"source": "server", "reason": "request", "violations": []any{
								map[string]any{"path": "name", "code": "missing", "message": "Field required"},
							}},
						}})
						continue
					}
					meta, _ := f["metadata"].(map[string]any)
					send(map[string]any{"kind": "call.result", "call_id": id, "payload": map[string]any{"greeting": "hello " + name, "saw": meta, "key": f["idempotency_key"]},
						"metadata": map[string]string{"served_by": "fake"}})
				case "count":
					counts[id] = &countCall{n: int(payload["n"].(float64))}
					flush(id)
				case "sum":
					send(map[string]any{"kind": "stream.window", "call_id": id, "direction": "client", "credit": 16})
				case "echo":
					// Little credit, so input must wait for each echo to go out.
					echoes[id] = 0
					send(map[string]any{"kind": "stream.window", "call_id": id, "direction": "client", "credit": 2})
				}
			case "stream.window":
				if c := counts[id]; c != nil && f["direction"] == "server" {
					c.credit += int(f["credit"].(float64))
					flush(id)
				}
			case "stream.item":
				payload, _ := f["payload"].(map[string]any)
				if seq, ok := echoes[id]; ok {
					send(map[string]any{"kind": "stream.item", "call_id": id, "direction": "server", "sequence": seq, "payload": payload})
					send(map[string]any{"kind": "stream.window", "call_id": id, "direction": "client", "credit": 1})
					echoes[id] = seq + 1
					continue
				}
				totals[id] += payload["v"].(float64)
			case "stream.end":
				if _, ok := echoes[id]; ok {
					send(map[string]any{"kind": "stream.end", "call_id": id, "direction": "server"})
					send(map[string]any{"kind": "call.result", "call_id": id, "metadata": map[string]string{"echoed": fmt.Sprint(echoes[id])}})
					continue
				}
				send(map[string]any{"kind": "call.result", "call_id": id, "payload": map[string]any{"total": totals[id]}})
			}
		}
	}()
}

func TestInspectAndCall(t *testing.T) {
	r, err := router.Start(router.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	fakeInstance(t, fmt.Sprintf("ws://127.0.0.1:%d", r.Ready().Port))
	opts := Options{Router: fmt.Sprintf("ws://127.0.0.1:%d", r.Ready().Port), Timeout: 3 * time.Second}
	ctx := context.Background()

	result, err := Inspect(ctx, opts, "")
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	FormatInspect(result, &out)
	text := out.String()
	for _, want := range []string{"demo.v1.Greeter  — Says hello.", "instance fake", "inflight=0/4", "unary          greet(name, times)  [read_only, idempotent]  — Greet someone.", "server_stream  count(n)"} {
		if !strings.Contains(text, want) {
			t.Fatalf("inspect output missing %q:\n%s", want, text)
		}
	}
	described, err := Inspect(ctx, opts, "demo.v1.Greeter")
	if err != nil {
		t.Fatal(err)
	}
	if described["contract"] == nil || described["version"] != "1" {
		t.Fatalf("describe lacks contract: %v", described)
	}
	if _, err := Inspect(ctx, opts, "nope.v1"); err == nil || !strings.Contains(err.Error(), "router inspect") {
		t.Fatalf("expected a hint for an unknown service, got %v", err)
	}

	out.Reset()
	err = Call(ctx, opts, CallOptions{Service: "demo.v1.Greeter", Method: "greet", Payload: map[string]any{"name": "cli"},
		Metadata: map[string]string{"trace": "t1"}, IdempotencyKey: "k1", Timeout: 2 * time.Second}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"greeting": "hello cli"`) || !strings.Contains(out.String(), `"trace": "t1"`) ||
		!strings.Contains(out.String(), `"key": "k1"`) || !strings.Contains(out.String(), `trailers: {"served_by":"fake"}`) {
		t.Fatalf("unary output:\n%s", out.String())
	}
	out.Reset()
	err = Call(ctx, opts, CallOptions{Service: "demo.v1.Greeter", Method: "greet", Payload: map[string]any{"times": 2}, JSON: true}, &out)
	var failure *Failure
	if !errors.As(err, &failure) || failure.Code != "invalid_argument" ||
		!strings.Contains(failure.Hint, "name: Field required (missing)") ||
		!strings.Contains(failure.Hint, "request is {name: string, times?: integer}") {
		t.Fatalf("expected invalid_argument with violation and schema hints, got %v", err)
	}
	if err := Call(ctx, opts, CallOptions{Service: "demo.v1.Greeter", Method: "nope"}, &out); !errors.As(err, &failure) ||
		failure.Code != "method_not_found" || !strings.Contains(failure.Hint, "count, echo, greet, sum") {
		t.Fatalf("expected method_not_found listing methods, got %v", err)
	}
	out.Reset()
	if err := Call(ctx, opts, CallOptions{Service: "demo.v1.Greeter", Method: "count", Payload: map[string]any{"n": 300}, JSON: true}, &out); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 300 || lines[0] != `{"i":0}` || lines[299] != `{"i":299}` {
		t.Fatalf("server stream printed %d lines; first %q last %q", len(lines), lines[0], lines[len(lines)-1])
	}
	out.Reset()
	items := []any{map[string]any{"v": 1}, map[string]any{"v": 2}, map[string]any{"v": 3.5}}
	if err := Call(ctx, opts, CallOptions{Service: "demo.v1.Greeter", Method: "sum", Items: items, JSON: true}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"total":6.5`) {
		t.Fatalf("client stream result: %s", out.String())
	}
	parsed, err := ReadItems(strings.NewReader("{\"v\":1}\n\n{\"v\":2}\n"))
	if err != nil || len(parsed) != 2 {
		t.Fatalf("ReadItems: %v %v", parsed, err)
	}
}

func TestDuplexCall(t *testing.T) {
	r, err := router.Start(router.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	fakeInstance(t, fmt.Sprintf("ws://127.0.0.1:%d", r.Ready().Port))
	opts := Options{Router: fmt.Sprintf("ws://127.0.0.1:%d", r.Ready().Port), Timeout: 3 * time.Second}
	ctx := context.Background()

	var input strings.Builder
	for i := 0; i < 10; i++ {
		fmt.Fprintf(&input, "{\"i\":%d}\n\n", i)
	}
	var out bytes.Buffer
	if err := Call(ctx, opts, CallOptions{Service: "demo.v1.Greeter", Method: "echo", Input: strings.NewReader(input.String()), JSON: true}, &out); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 11 || lines[0] != `{"i":0}` || lines[9] != `{"i":9}` || !strings.Contains(lines[10], `"echoed":"10"`) {
		t.Fatalf("duplex output:\n%s", out.String())
	}
	var failure *Failure
	err = Call(ctx, opts, CallOptions{Service: "demo.v1.Greeter", Method: "echo", IdempotencyKey: "k"}, &out)
	if !errors.As(err, &failure) || failure.Code != "invalid_argument" || failure.Details["reason"] != "idempotency_key_on_duplex" {
		t.Fatalf("expected a local refusal of the idempotency key, got %v", err)
	}
	err = Call(ctx, opts, CallOptions{Service: "demo.v1.Greeter", Method: "echo", Input: strings.NewReader("{\"i\":0}\nnot json\n")}, &out)
	if err == nil || !strings.Contains(err.Error(), "input item 2 is not JSON") {
		t.Fatalf("expected a bad input line to fail the call, got %v", err)
	}
	result, err := Inspect(ctx, opts, "")
	if err != nil {
		t.Fatal(err)
	}
	out.Reset()
	FormatInspect(result, &out)
	if !strings.Contains(out.String(), "duplex         echo()  — Echo items.  (duplex: answers while the caller types)") {
		t.Fatalf("inspect does not show the duplex reason:\n%s", out.String())
	}
}

// The local transport: a Unix socket, or a named pipe on Windows. Both the
// WebSocket and the HTTP paths of the CLI must reach the Router through it.
func TestLocalTransport(t *testing.T) {
	var config router.Config
	var url string
	if localsock.UnixSupported() {
		dir, err := os.MkdirTemp("/tmp", "meshcall-cli-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(dir) })
		config.UnixPath = filepath.Join(dir, "router.sock")
		url = "unix://" + config.UnixPath
	} else {
		config.Pipe = fmt.Sprintf("meshcall-cli-test-%d", time.Now().UnixNano())
		url = "pipe://" + config.Pipe
	}
	r, err := router.Start(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	if localsock.UnixSupported() != (r.Ready().UnixPath != "") || localsock.UnixSupported() == (r.Ready().Pipe != "") {
		t.Fatalf("ready does not report the local transport: %+v", r.Ready())
	}
	fakeInstance(t, url)
	opts := Options{Router: url, Timeout: 3 * time.Second}
	ctx := context.Background()
	for _, overHTTP := range []bool{false, true} {
		var out bytes.Buffer
		err := Call(ctx, opts, CallOptions{Service: "demo.v1.Greeter", Method: "greet", Payload: map[string]any{"name": "local"}, JSON: true, HTTP: overHTTP}, &out)
		if err != nil || !strings.Contains(out.String(), "hello local") {
			t.Fatalf("call (http=%v): %v\n%s", overHTTP, err, out.String())
		}
	}
}
