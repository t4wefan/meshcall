// Command server is the Go side of the cross-language interop tests
// (meshcall-py/tests/test_multilang_interop.py): test.v1.GoInteropService,
// shaped like the TypeScript interop service, registered with a Router.
//
//	go run ./internal/interop/server <router-url> <contract.json>
//
// It prints {"ready": true} once registered and stops on a line (or EOF) on stdin.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	meshcall "github.com/t4wefan/meshcall/meshcall-go"
)

type Request struct {
	Value  int `json:"value"`
	Factor int `json:"factor"`
}

type Result struct {
	Product   int    `json:"product"`
	HandledBy string `json:"handled_by"`
}

type Item struct {
	Value     int    `json:"value"`
	HandledBy string `json:"handled_by"`
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: server <router-url> <contract.json>")
		os.Exit(2)
	}
	svc := meshcall.NewService("test.v1.GoInteropService", meshcall.Describe("The Go side of the interop tests."))
	meshcall.HandleUnary(svc, "multiply", func(ctx *meshcall.Context, req Request) (Result, error) {
		return Result{Product: req.Value * req.Factor, HandledBy: "go"}, nil
	}, meshcall.Doc("Multiply value by factor."), meshcall.ReadOnly())
	meshcall.HandleServerStream(svc, "download", func(ctx *meshcall.Context, req Request, send func(Item) error) error {
		for v := range req.Value {
			if err := send(Item{Value: v * req.Factor, HandledBy: "go"}); err != nil {
				return err
			}
		}
		return nil
	}, meshcall.Doc("Yield 0..value-1 times factor."), meshcall.ReadOnly())
	meshcall.HandleClientStream(svc, "upload", func(ctx *meshcall.Context, req Request, items *meshcall.Input[Item]) (Result, error) {
		product := req.Value
		for item, err := range items.All() {
			if err != nil {
				return Result{}, err
			}
			product += item.Value * req.Factor
		}
		return Result{Product: product, HandledBy: "go"}, nil
	}, meshcall.Doc("Add every item times factor to value."), meshcall.Idempotent())
	meshcall.HandleDuplex(svc, "exchange", "interop: answers each item as it arrives",
		func(ctx *meshcall.Context, req Request, ch *meshcall.Channel[Item, Item]) error {
			for {
				item, err := ch.Recv()
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
				if err := ch.Send(Item{Value: item.Value * req.Factor, HandledBy: "go"}); err != nil {
					return err
				}
			}
		}, meshcall.Doc("Answer each item times factor."))

	if err := meshcall.WriteContract(os.Args[2], svc); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	server, err := meshcall.NewServer(meshcall.ServerOptions{Services: []*meshcall.Service{svc}, Strict: true})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.ConnectRouter(ctx, os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	_ = json.NewEncoder(os.Stdout).Encode(map[string]bool{"ready": true})
	_, _ = bufio.NewReader(os.Stdin).ReadString('\n') // a line or EOF stops it
	_ = server.Close()
}
