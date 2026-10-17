package meshcall_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	meshcall "github.com/t4wefan/meshcall/meshcall-go"
)

// startRouterProcess runs the Go Router on an ephemeral port for one test.
func startRouterProcess(t *testing.T) string {
	t.Helper()
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
	return fmt.Sprintf("ws://127.0.0.1:%d", ready.Port)
}

func httpURL(wsURL string) string {
	return strings.Replace(wsURL, "ws://", "http://", 1)
}

func TestHTTPMeshEveryShape(t *testing.T) {
	routerURL := startRouterProcess(t)
	var flaps atomic.Int32
	server, err := meshcall.NewServer(meshcall.ServerOptions{
		Services: []*meshcall.Service{fixture(&flaps)}, InstanceID: "go-http",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := server.ConnectRouter(ctx, routerURL); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })

	httpMesh := meshcall.NewMesh(meshcall.NewRouter(httpURL(routerURL)))
	t.Cleanup(func() { _ = httpMesh.Close() })
	const svc = "test.v1.Go"

	got, err := meshcall.Unary[Num, Num](ctx, httpMesh, svc, "double", Num{Value: 21})
	if err != nil || got.Value != 42 {
		t.Fatalf("double over HTTP: %v %v", got, err)
	}
	_, err = meshcall.Unary[Num, Num](ctx, httpMesh, svc, "fail", Num{Value: 3})
	if e, ok := meshcall.AsError(err); !ok || e.Code != "app.nope" || e.Details["value"] != float64(3) {
		t.Fatalf("application error over HTTP: %v", err)
	}

	stream, err := meshcall.ServerStream[Num, Num](ctx, httpMesh, svc, "count", Num{Value: 300})
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for item, err := range stream.All() {
		if err != nil || item.Value != seen {
			t.Fatalf("stream item %d: %v %v", seen, item, err)
		}
		seen++
	}
	if seen != 300 {
		t.Fatalf("received %d items", seen)
	}

	total, err := meshcall.ClientStream[Num, Num, Num](ctx, httpMesh, svc, "sum", Num{Value: 1}, func(yield func(Num) bool) {
		for i := 1; i <= 100; i++ {
			if !yield(Num{Value: i}) {
				return
			}
		}
	})
	if err != nil || total.Value != 5051 {
		t.Fatalf("client stream over HTTP: %v %v", total, err)
	}

	_, err = meshcall.Duplex[Text, Text, Text](ctx, httpMesh, svc, "echo", Text{})
	if e, ok := meshcall.AsError(err); !ok || e.Reason() != "duplex_over_http" {
		t.Fatalf("duplex over HTTP: %v", err)
	}

	services, err := httpMesh.Registry().List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, entry := range services {
		if entry.Name == svc {
			found = true
		}
	}
	if !found {
		t.Fatalf("registry over HTTP: %v", services)
	}

	scoped := httpMesh.WithCallOptions(meshcall.CallOptions{Metadata: map[string]string{"trace": "t1"}})
	metadata, err := meshcall.Unary[Empty, Text](ctx, scoped, svc, "metadata", Empty{})
	if err != nil || metadata.Text != "t1" {
		t.Fatalf("metadata over HTTP: %v %v", metadata, err)
	}
}

func TestHTTPMeshRefusesOversizedResult(t *testing.T) {
	routerURL := startRouterProcess(t)
	const limit = 64 << 10
	svc := meshcall.NewService("test.v1.HTTPBig", meshcall.Describe("Oversized frames."))
	meshcall.HandleUnary(svc, "result", func(ctx *meshcall.Context, req Num) (Text, error) {
		return Text{Text: strings.Repeat("x", req.Value)}, nil
	}, meshcall.Doc("A result of value bytes."), meshcall.ReadOnly())
	server, err := meshcall.NewServer(meshcall.ServerOptions{
		Services: []*meshcall.Service{svc}, InstanceID: "go-http-big", MaxFrameSize: limit,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := server.ConnectRouter(ctx, routerURL); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })

	httpMesh := meshcall.NewMesh(meshcall.NewRouter(httpURL(routerURL)))
	t.Cleanup(func() { _ = httpMesh.Close() })
	_, err = meshcall.Unary[Num, Text](ctx, httpMesh, "test.v1.HTTPBig", "result", Num{Value: 2 * limit})
	if !meshcall.IsCode(err, meshcall.CodeResourceExhausted) {
		t.Fatalf("oversized result over HTTP: %v", err)
	}

	if got, err := meshcall.Unary[Num, Text](ctx, httpMesh, "test.v1.HTTPBig", "result", Num{Value: 1}); err != nil || got.Text != "x" {
		t.Fatalf("small result after the oversized one: %v %v", got, err)
	}
}
