// Command fixture is the Go conformance fixture
// (meshcall-router/conformance/FIXTURE.md).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	meshcall "github.com/t4wefan/meshcall/meshcall-go"
)

type Echo struct {
	Value int    `json:"value"`
	Text  string `json:"text"`
}

type Fail struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable,omitempty"`
}

// Ok is {"ok": true}; Broken makes it break its own contract.
type Ok struct {
	OK     bool `json:"ok"`
	Broken bool `json:"-"`
}

func (o Ok) MarshalJSON() ([]byte, error) {
	if o.Broken {
		return []byte(`{"ok":"not-a-bool"}`), nil
	}
	return json.Marshal(struct {
		OK bool `json:"ok"`
	}{o.OK})
}

type Empty struct{}

type Slow struct {
	Ms  int    `json:"ms"`
	Tag string `json:"tag,omitempty"`
}

type Tag struct {
	Tag string `json:"tag"`
}

type Tags struct {
	Tags []string `json:"tags"`
}

type Count struct {
	N     int `json:"n"`
	GapMs int `json:"gap_ms,omitempty"`
}

type Index struct {
	Index int `json:"index"`
}

type Sum struct {
	Offset int `json:"offset,omitempty"`
}

type Value struct {
	Value int `json:"value"`
}

type Total struct {
	Total int `json:"total"`
}

type Metadata struct {
	Metadata map[string]string `json:"metadata"`
}

type Drain struct {
	DelayMs int `json:"delay_ms,omitempty"`
}

type Chat struct {
	Prefix string `json:"prefix,omitempty"`
}

type Text struct {
	Text string `json:"text"`
}

type Size struct {
	Bytes int `json:"bytes"`
}

type Instance struct {
	Instance string `json:"instance"`
}

func main() {
	var mu sync.Mutex
	var invoked, cancelled []string
	var server *meshcall.Server

	fixture := meshcall.NewService("conformance.v1.Fixture",
		meshcall.Describe("Deterministic behaviours the conformance suite drives through the Router."))
	meshcall.HandleUnary(fixture, "echo", func(ctx *meshcall.Context, req Echo) (Echo, error) { return req, nil },
		meshcall.Doc("Return the input unchanged."), meshcall.ReadOnly())
	meshcall.HandleUnary(fixture, "fail", func(ctx *meshcall.Context, req Fail) (Ok, error) {
		return Ok{}, &meshcall.Error{Code: req.Code, Message: req.Message, Retryable: req.Retryable, Details: map[string]any{"from": "fixture"}}
	}, meshcall.Doc("Raise an application error with the given code."), meshcall.Idempotent())
	meshcall.HandleUnary(fixture, "bad_response", func(ctx *meshcall.Context, req Empty) (Ok, error) {
		return Ok{Broken: true}, nil
	}, meshcall.Doc("Return a response that breaks this method's own contract."), meshcall.ReadOnly())
	meshcall.HandleUnary(fixture, "slow", func(ctx *meshcall.Context, req Slow) (Tag, error) {
		mu.Lock()
		invoked = append(invoked, req.Tag)
		mu.Unlock()
		select {
		case <-time.After(time.Duration(req.Ms) * time.Millisecond):
			return Tag{Tag: req.Tag}, nil
		case <-ctx.Done():
			mu.Lock()
			cancelled = append(cancelled, req.Tag)
			mu.Unlock()
			return Tag{}, ctx.Err()
		}
	}, meshcall.Doc("Sleep ms milliseconds; record tag if cancelled meanwhile."), meshcall.Idempotent())
	meshcall.HandleUnary(fixture, "invoked_tags", func(ctx *meshcall.Context, req Empty) (Tags, error) {
		mu.Lock()
		defer mu.Unlock()
		return Tags{Tags: append([]string{}, invoked...)}, nil
	}, meshcall.Doc("Tags of every slow call that started, in order."), meshcall.ReadOnly())
	meshcall.HandleUnary(fixture, "cancelled_tags", func(ctx *meshcall.Context, req Empty) (Tags, error) {
		mu.Lock()
		defer mu.Unlock()
		return Tags{Tags: append([]string{}, cancelled...)}, nil
	}, meshcall.Doc("Tags of slow calls that observed cancellation."), meshcall.ReadOnly())
	meshcall.HandleServerStream(fixture, "count", func(ctx *meshcall.Context, req Count, send func(Index) error) error {
		for i := 0; i < req.N; i++ {
			if req.GapMs > 0 {
				select {
				case <-time.After(time.Duration(req.GapMs) * time.Millisecond):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			if err := send(Index{Index: i}); err != nil {
				return err
			}
		}
		return nil
	}, meshcall.Doc("Yield 0..n-1 with gap_ms between items."), meshcall.ReadOnly())
	meshcall.HandleClientStream(fixture, "sum", func(ctx *meshcall.Context, req Sum, items *meshcall.Input[Value]) (Total, error) {
		total := req.Offset
		for item, err := range items.All() {
			if err != nil {
				return Total{}, err
			}
			total += item.Value
		}
		return Total{Total: total}, nil
	}, meshcall.Doc("Add every streamed value to offset."), meshcall.Idempotent())
	meshcall.HandleUnary(fixture, "metadata_echo", func(ctx *meshcall.Context, req Empty) (Metadata, error) {
		ctx.SetTrailer("echoed", "yes")
		ctx.SetTrailer("count", strconv.Itoa(len(ctx.Metadata)))
		return Metadata{Metadata: ctx.Metadata}, nil
	}, meshcall.Doc("Return the call metadata and set trailers describing it."), meshcall.ReadOnly())
	meshcall.HandleUnary(fixture, "big", func(ctx *meshcall.Context, req Size) (Text, error) {
		return Text{Text: strings.Repeat("x", req.Bytes)}, nil
	}, meshcall.Doc("Return a text of bytes bytes."), meshcall.ReadOnly())
	meshcall.HandleServerStream(fixture, "big_items", func(ctx *meshcall.Context, req Size, send func(Text) error) error {
		if err := send(Text{Text: "small"}); err != nil {
			return err
		}
		return send(Text{Text: strings.Repeat("x", req.Bytes)})
	}, meshcall.Doc("Yield one small item, then one of bytes bytes."), meshcall.ReadOnly())
	meshcall.HandleDuplex(fixture, "chat", "conformance: answers interleave with input",
		func(ctx *meshcall.Context, req Chat, ch *meshcall.Channel[Text, Text]) error {
			count := 0
			for {
				item, err := ch.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				count++
				if err := ch.Send(Text{Text: req.Prefix + item.Text}); err != nil {
					return err
				}
			}
			return ch.Send(Text{Text: fmt.Sprintf("done:%d", count)})
		}, meshcall.Doc("Echo each input with prefix; send done:<inputs> when input ends."))
	meshcall.HandleDuplex(fixture, "announce", "conformance: output ends before input",
		func(ctx *meshcall.Context, req Empty, ch *meshcall.Channel[Text, Text]) error {
			if err := ch.Send(Text{Text: "hello"}); err != nil {
				return err
			}
			if err := ch.CloseSend(); err != nil {
				return err
			}
			for {
				if _, err := ch.Recv(); err == io.EOF {
					return nil
				} else if err != nil {
					return err
				}
			}
		}, meshcall.Doc("Send hello, close the output, then read the input to its end."))
	meshcall.HandleUnary(fixture, "drain_self", func(ctx *meshcall.Context, req Drain) (Ok, error) {
		go func() {
			time.Sleep(time.Duration(req.DelayMs) * time.Millisecond)
			drainCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_ = server.Drain(drainCtx)
			_ = server.Close()
			os.Exit(0)
		}()
		return Ok{OK: true}, nil
	}, meshcall.Doc("Drain and stop this instance after delay_ms."), meshcall.Destructive())

	targeted := meshcall.NewService("conformance.v1.Targeted", meshcall.Targeted(),
		meshcall.Describe("A targeted service: every call must name the instance it is for."))
	meshcall.HandleUnary(targeted, "whoami", func(ctx *meshcall.Context, req Empty) (Instance, error) {
		return Instance{Instance: ctx.InstanceID}, nil
	}, meshcall.Doc("Return the id of the instance that served the call."), meshcall.ReadOnly())

	ping := 15000
	if v, err := strconv.Atoi(os.Getenv("MESHCALL_PING_INTERVAL_MS")); err == nil {
		ping = v
	}
	instance := os.Getenv("MESHCALL_CONFORMANCE_INSTANCE")
	if instance == "" {
		instance = "fixture-1"
	}
	maxFrame, _ := strconv.Atoi(os.Getenv("MESHCALL_MAX_FRAME_SIZE")) // 0: the default
	var err error
	server, err = meshcall.NewServer(meshcall.ServerOptions{
		Services: []*meshcall.Service{fixture, targeted}, InstanceID: instance, MaxFrameSize: maxFrame,
		PingInterval: time.Duration(ping) * time.Millisecond, PingTimeout: max(time.Second, 2*time.Duration(ping)*time.Millisecond),
		Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.ConnectRouter(ctx, os.Getenv("MESHCALL_ROUTER_URL")); err != nil {
		fmt.Fprintln(os.Stderr, "register:", err)
		os.Exit(1)
	}
	fmt.Fprintln(os.Stderr, "fixture registered")
	select {}
}
