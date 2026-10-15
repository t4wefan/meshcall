package meshcall_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	meshcall "github.com/t4wefan/meshcall/meshcall-go"
)

type Num struct {
	Value int `json:"value" doc:"a number"`
}

type Text struct {
	Text string `json:"text"`
}

type Since struct {
	Since *int `json:"since"`
}

type Tick struct {
	Seq int `json:"seq"`
}

type Empty struct{}

type Who struct {
	Instance string `json:"instance"`
}

// fixture is a service exercising every shape.
func fixture(flaps *atomic.Int32) *meshcall.Service {
	svc := meshcall.NewService("test.v1.Go", meshcall.Describe("Go runtime tests."))
	meshcall.HandleUnary(svc, "double", func(ctx *meshcall.Context, req Num) (Num, error) {
		ctx.SetTrailer("seen", fmt.Sprint(req.Value))
		return Num{Value: req.Value * 2}, nil
	}, meshcall.Doc("Double a number."), meshcall.ReadOnly())
	meshcall.HandleUnary(svc, "fail", func(ctx *meshcall.Context, req Num) (Num, error) {
		return Num{}, &meshcall.Error{Code: "app.nope", Message: "no", Details: map[string]any{"value": req.Value}}
	}, meshcall.Doc("Fail with an application error."), meshcall.ReadOnly())
	meshcall.HandleUnary(svc, "flaky", func(ctx *meshcall.Context, req Num) (Num, error) {
		if flaps.Add(1) <= int32(req.Value) {
			return Num{}, &meshcall.Error{Code: meshcall.CodeUnavailable, Message: "try again", Retryable: true}
		}
		return Num{Value: int(flaps.Load())}, nil
	}, meshcall.Doc("Fail value times, then succeed with the attempt count."), meshcall.Idempotent())
	meshcall.HandleUnary(svc, "sleep", func(ctx *meshcall.Context, req Num) (Num, error) {
		select {
		case <-time.After(time.Duration(req.Value) * time.Millisecond):
			return req, nil
		case <-ctx.Done():
			return Num{}, ctx.Err()
		}
	}, meshcall.Doc("Sleep value milliseconds."), meshcall.ReadOnly())
	meshcall.HandleUnary(svc, "metadata", func(ctx *meshcall.Context, req Empty) (Text, error) {
		return Text{Text: ctx.Metadata["trace"]}, nil
	}, meshcall.Doc("Return the trace metadata."), meshcall.ReadOnly())
	meshcall.HandleServerStream(svc, "count", func(ctx *meshcall.Context, req Num, send func(Num) error) error {
		for i := range req.Value {
			if err := send(Num{Value: i}); err != nil {
				return err
			}
		}
		return nil
	}, meshcall.Doc("Yield 0..value-1."), meshcall.ReadOnly())
	meshcall.HandleClientStream(svc, "sum", func(ctx *meshcall.Context, req Num, items *meshcall.Input[Num]) (Num, error) {
		total := req.Value
		for item, err := range items.All() {
			if err != nil {
				return Num{}, err
			}
			total += item.Value
		}
		return Num{Value: total}, nil
	}, meshcall.Doc("Add the items to value."), meshcall.Idempotent())
	meshcall.HandleDuplex(svc, "echo", "replies interleave with input", func(ctx *meshcall.Context, req Text, ch *meshcall.Channel[Text, Text]) error {
		for {
			item, err := ch.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			if err := ch.Send(Text{Text: req.Text + item.Text}); err != nil {
				return err
			}
		}
	}, meshcall.Doc("Answer each input as it arrives."))
	meshcall.HandleSubscription(svc, "ticks", "seq", "since", func(ctx *meshcall.Context, req Since, send func(Tick) error) error {
		start := 0
		if req.Since != nil {
			start = *req.Since + 1
		}
		for seq := start; seq < 6; seq++ {
			if err := send(Tick{Seq: seq}); err != nil {
				return err
			}
			if seq == 2 && flaps.Add(1) == 1 {
				return &meshcall.Error{Code: meshcall.CodeUnavailable, Message: "flap", Retryable: true}
			}
		}
		return nil
	}, meshcall.Doc("Ticks after since."), meshcall.ReadOnly())
	return svc
}

func direct(t *testing.T, addr string, opts meshcall.ServerOptions) (*meshcall.Server, string) {
	t.Helper()
	server, err := meshcall.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Listen(addr); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	if strings.HasPrefix(addr, "unix://") {
		return server, addr
	}
	return server, "ws://" + server.Addr().String()
}

func client(t *testing.T, url string, opts meshcall.ClientOptions) *meshcall.Client {
	t.Helper()
	opts.Reconnect = meshcall.ReconnectPolicy{InitialDelay: 10 * time.Millisecond, MaxDelay: 50 * time.Millisecond}
	c, err := meshcall.NewClient(opts, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestEveryShapeDirect(t *testing.T) {
	var flaps atomic.Int32
	_, url := direct(t, "127.0.0.1:0", meshcall.ServerOptions{Services: []*meshcall.Service{fixture(&flaps)}})
	exercise(t, client(t, url, meshcall.ClientOptions{}), &flaps)
}

func TestEveryShapeThroughARouter(t *testing.T) {
	binary := os.Getenv("MESHCALL_ROUTER_BINARY")
	if binary == "" {
		t.Skip("set MESHCALL_ROUTER_BINARY to run against the Go Router")
	}
	cmd := exec.Command(binary, "--port", "0", "--shutdown-on-stdin-close")
	stdin, _ := cmd.StdinPipe()
	stdout, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stdin.Close(); _ = cmd.Wait() })
	var ready struct {
		Port int `json:"port"`
	}
	if err := json.NewDecoder(stdout).Decode(&ready); err != nil {
		t.Fatal(err)
	}
	url := fmt.Sprintf("ws://127.0.0.1:%d", ready.Port)
	var flaps atomic.Int32
	server, err := meshcall.NewServer(meshcall.ServerOptions{Services: []*meshcall.Service{fixture(&flaps)}, InstanceID: "go-1"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.ConnectRouter(ctx, url); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	c := client(t, url, meshcall.ClientOptions{})
	exercise(t, c, &flaps)
	who, err := meshcall.Unary[Empty, Text](context.Background(), c, "test.v1.Go", "metadata", Empty{}, meshcall.WithInstance("go-1"))
	if err != nil || who.Text != "" {
		t.Fatalf("targeting by instance id: %v %v", who, err)
	}
	if err := server.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = meshcall.Unary[Num, Num](context.Background(), c, "test.v1.Go", "double", Num{Value: 1})
	if !meshcall.IsCode(err, meshcall.CodeUnavailable) {
		t.Fatalf("after drain: %v", err)
	}
}

func exercise(t *testing.T, c *meshcall.Client, flaps *atomic.Int32) {
	t.Helper()
	ctx := context.Background()
	const svc = "test.v1.Go"

	var info *meshcall.CallInfo
	got, err := meshcall.Unary[Num, Num](ctx, c, svc, "double", Num{Value: 21})
	if err != nil || got.Value != 42 {
		t.Fatalf("double: %v %v", got, err)
	}
	_ = info

	_, err = meshcall.Unary[Num, Num](ctx, c, svc, "fail", Num{Value: 3})
	if e, ok := meshcall.AsError(err); !ok || e.Code != "app.nope" || e.Details["value"] != float64(3) {
		t.Fatalf("application error: %v", err)
	}
	_, err = meshcall.Unary[map[string]any, Num](ctx, c, svc, "double", map[string]any{"value": "x"})
	if e, ok := meshcall.AsError(err); !ok || e.Code != meshcall.CodeInvalidArgument || e.Reason() != "request" {
		t.Fatalf("invalid request: %v", err)
	} else if v := e.Details["violations"].([]any)[0].(map[string]any); v["path"] != "value" || v["code"] != "type" {
		t.Fatalf("violation: %v", v)
	}
	_, err = meshcall.Unary[Num, Num](ctx, c, svc, "nope", Num{})
	if !meshcall.IsCode(err, meshcall.CodeMethodNotFound) {
		t.Fatalf("unknown method: %v", err)
	}

	md, err := meshcall.Unary[Empty, Text](ctx, c, svc, "metadata", Empty{}, meshcall.WithMetadata(map[string]string{"trace": "t1"}))
	if err != nil || md.Text != "t1" {
		t.Fatalf("metadata: %v %v", md, err)
	}

	// Retries only what is safe: declared idempotent, retryable code.
	flaps.Store(0)
	got, err = meshcall.Unary[Num, Num](ctx, c, svc, "flaky", Num{Value: 2}, meshcall.IdempotentCall())
	if err != nil || got.Value != 3 {
		t.Fatalf("retried call: %v %v", got, err)
	}
	flaps.Store(0)
	_, err = meshcall.Unary[Num, Num](ctx, c, svc, "flaky", Num{Value: 1})
	if !meshcall.IsCode(err, meshcall.CodeUnavailable) {
		t.Fatalf("a call not marked idempotent must not be retried: %v", err)
	}

	// Deadlines: the server's own error wins; the context bounds the call too.
	_, err = meshcall.Unary[Num, Num](ctx, c, svc, "sleep", Num{Value: 2000}, meshcall.WithTimeout(100*time.Millisecond))
	if e, ok := meshcall.AsError(err); !ok || e.Code != meshcall.CodeDeadlineExceeded || e.Details["source"] == "client" {
		t.Fatalf("server deadline: %v", err)
	}
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	_, err = meshcall.Unary[Num, Num](short, c, svc, "sleep", Num{Value: 2000})
	cancel()
	if !meshcall.IsCode(err, meshcall.CodeDeadlineExceeded) {
		t.Fatalf("context deadline: %v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	time.AfterFunc(50*time.Millisecond, cancel)
	_, err = meshcall.Unary[Num, Num](cancelled, c, svc, "sleep", Num{Value: 2000})
	if !meshcall.IsCode(err, meshcall.CodeCancelled) {
		t.Fatalf("cancelled: %v", err)
	}

	// Server stream beyond the initial credit, then early close.
	stream, err := meshcall.ServerStream[Num, Num](ctx, c, svc, "count", Num{Value: 300})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for item, err := range stream.All() {
		if err != nil || item.Value != n {
			t.Fatalf("item %d: %v %v", n, item, err)
		}
		n++
	}
	if n != 300 {
		t.Fatalf("got %d items", n)
	}
	early, _ := meshcall.ServerStream[Num, Num](ctx, c, svc, "count", Num{Value: 1000})
	for item := range early.All() {
		if item.Value == 3 {
			break
		}
	}

	total, err := meshcall.ClientStream[Num, Num, Num](ctx, c, svc, "sum", Num{Value: 1}, func(yield func(Num) bool) {
		for i := 1; i <= 100; i++ {
			if !yield(Num{Value: i}) {
				return
			}
		}
	})
	if err != nil || total.Value != 5051 {
		t.Fatalf("client stream: %v %v", total, err)
	}

	duplex, err := meshcall.Duplex[Text, Text, Text](ctx, c, svc, "echo", Text{Text: "> "})
	if err != nil {
		t.Fatal(err)
	}
	for _, word := range []string{"a", "b", "c"} {
		if err := duplex.Send(Text{Text: word}); err != nil {
			t.Fatal(err)
		}
		if out, err := duplex.Recv(); err != nil || out.Text != "> "+word {
			t.Fatalf("duplex echo: %v %v", out, err)
		}
	}
	_ = duplex.CloseSend()
	if _, err := duplex.Recv(); err != io.EOF {
		t.Fatalf("duplex end: %v", err)
	}
	_, err = meshcall.Duplex[Text, Text, Text](ctx, c, svc, "echo", Text{}, meshcall.WithIdempotencyKey("k"))
	if err == nil {
		d, _ := meshcall.Duplex[Text, Text, Text](ctx, c, svc, "echo", Text{}, meshcall.WithIdempotencyKey("k"))
		_, err = d.Recv()
	}
	if e, ok := meshcall.AsError(err); !ok || e.Reason() != "idempotency_key_on_duplex" {
		t.Fatalf("duplex with a key: %v", err)
	}

	// A subscription resumes after the retryable failure from its cursor.
	flaps.Store(0)
	sub, err := meshcall.Subscribe[Since, Tick](ctx, c, svc, "ticks", Since{}, meshcall.SubscriptionSpec{Cursor: "seq", Resume: "since"})
	if err != nil {
		t.Fatal(err)
	}
	var ticks []int
	for tick, err := range sub.All() {
		if err != nil {
			t.Fatal(err)
		}
		ticks = append(ticks, tick.Seq)
	}
	if fmt.Sprint(ticks) != "[0 1 2 3 4 5]" {
		t.Fatalf("ticks: %v", ticks)
	}
}

func TestLocalTransport(t *testing.T) {
	var flaps atomic.Int32
	var addr string
	if os.PathSeparator == '\\' {
		addr = fmt.Sprintf("pipe://meshcall-go-test-%d", time.Now().UnixNano())
	} else {
		dir, err := os.MkdirTemp("/tmp", "meshcall-go-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(dir) })
		addr = "unix://" + filepath.Join(dir, "s.sock")
	}
	_, url := direct(t, addr, meshcall.ServerOptions{Services: []*meshcall.Service{fixture(&flaps)}})
	got, err := meshcall.Unary[Num, Num](context.Background(), client(t, url, meshcall.ClientOptions{}), "test.v1.Go", "double", Num{Value: 4})
	if err != nil || got.Value != 8 {
		t.Fatalf("local transport: %v %v", got, err)
	}
}

func TestMaxInflightAndTargeting(t *testing.T) {
	var flaps atomic.Int32
	_, url := direct(t, "127.0.0.1:0", meshcall.ServerOptions{Services: []*meshcall.Service{fixture(&flaps)}, MaxInflight: 1, InstanceID: "me"})
	c := client(t, url, meshcall.ClientOptions{})
	ctx := context.Background()
	go func() { _, _ = meshcall.Unary[Num, Num](ctx, c, "test.v1.Go", "sleep", Num{Value: 500}) }()
	time.Sleep(100 * time.Millisecond)
	_, err := meshcall.Unary[Num, Num](ctx, c, "test.v1.Go", "double", Num{Value: 1})
	if e, ok := meshcall.AsError(err); !ok || e.Code != meshcall.CodeResourceExhausted || e.Reason() != "overloaded" {
		t.Fatalf("max_inflight: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	_, err = meshcall.Unary[Num, Num](ctx, c, "test.v1.Go", "double", Num{Value: 1}, meshcall.WithInstance("other"))
	if e, ok := meshcall.AsError(err); !ok || e.Reason() != "instance_unknown" {
		t.Fatalf("foreign instance: %v", err)
	}
}

func TestContractAndLint(t *testing.T) {
	var flaps atomic.Int32
	svc := fixture(&flaps)
	var contract map[string]any
	if err := json.Unmarshal(svc.Contract(), &contract); err != nil {
		t.Fatal(err)
	}
	methods := contract["methods"].([]any)
	double := methods[0].(map[string]any)
	if double["stream"] != "unary" || fmt.Sprint(double["parameters"]) != "[value]" {
		t.Fatalf("double: %v", double)
	}
	schema := double["request"].(map[string]any)["schema"].(map[string]any)
	prop := schema["properties"].(map[string]any)["value"].(map[string]any)
	if prop["type"] != "integer" || prop["description"] != "a number" || fmt.Sprint(schema["required"]) != "[value]" {
		t.Fatalf("request schema: %v", schema)
	}
	for _, m := range methods {
		m := m.(map[string]any)
		switch m["name"] {
		case "echo":
			if m["duplex_reason"] != "replies interleave with input" {
				t.Fatalf("echo: %v", m)
			}
		case "ticks":
			if fmt.Sprint(m["subscription"]) != "map[cursor:seq resume:since]" {
				t.Fatalf("ticks: %v", m)
			}
		}
	}
	bare := meshcall.NewService("test.v1.Bare")
	meshcall.HandleUnary(bare, "create_thing", func(ctx *meshcall.Context, req Empty) (Empty, error) { return req, nil })
	var codes []string
	for _, w := range meshcall.LintService(bare) {
		codes = append(codes, w.Code)
	}
	if fmt.Sprint(codes) != "[W001 W001 W002]" {
		t.Fatalf("lint: %v", codes)
	}
	if _, err := meshcall.NewServer(meshcall.ServerOptions{Services: []*meshcall.Service{bare}, Strict: true}); err == nil || !strings.Contains(err.Error(), "W002") {
		t.Fatalf("strict mode: %v", err)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("a duplex method without a reason was accepted")
		}
	}()
	meshcall.HandleDuplex(bare, "talk", " ", func(ctx *meshcall.Context, req Empty, ch *meshcall.Channel[Text, Text]) error { return nil })
}

var _ = errors.New
