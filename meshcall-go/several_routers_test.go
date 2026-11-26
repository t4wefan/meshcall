package meshcall_test

// docs/protocol.md → Several Routers.
// conformance-scenario: routers.several

import (
	"context"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	meshcall "github.com/t4wefan/meshcall/meshcall-go"
)

type severalEmpty struct{}

type severalCount struct {
	N int `json:"n"`
}

type severalWhere struct {
	Instance string `json:"instance"`
}

func severalBox() *meshcall.Service {
	svc := meshcall.NewService("test.v1.Box", meshcall.Describe("A box on one of several Routers."), meshcall.Targeted())
	meshcall.HandleUnary(svc, "whoami", func(ctx *meshcall.Context, _ severalEmpty) (severalWhere, error) {
		return severalWhere{Instance: ctx.InstanceID}, nil
	}, meshcall.Doc("Name this instance."), meshcall.ReadOnly())
	meshcall.HandleServerStream(svc, "count", func(ctx *meshcall.Context, req severalCount, send func(severalCount) error) error {
		for i := range req.N {
			if err := send(severalCount{N: i}); err != nil {
				return err
			}
		}
		return nil
	}, meshcall.Doc("Count up to n."), meshcall.ReadOnly())
	meshcall.HandleClientStream(svc, "sum", func(ctx *meshcall.Context, req severalCount, items *meshcall.Input[severalCount]) (severalCount, error) {
		total := req.N
		for item, err := range items.All() {
			if err != nil {
				return severalCount{}, err
			}
			total += item.N
		}
		return severalCount{N: total}, nil
	}, meshcall.Doc("Add the items to n."))
	meshcall.HandleDuplex(svc, "double", "answers as items arrive", func(ctx *meshcall.Context, _ severalEmpty, ch *meshcall.Channel[severalCount, severalCount]) error {
		for {
			item, err := ch.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			if err := ch.Send(severalCount{N: item.N * 2}); err != nil {
				return err
			}
		}
	}, meshcall.Doc("Double each item."))
	return svc
}

func severalPlain() *meshcall.Service {
	svc := meshcall.NewService("test.v1.Plain", meshcall.Describe("A service registered on one Router only."))
	meshcall.HandleUnary(svc, "hello", func(ctx *meshcall.Context, _ severalEmpty) (severalWhere, error) {
		return severalWhere{Instance: ctx.InstanceID}, nil
	}, meshcall.Doc("Answer."), meshcall.ReadOnly())
	return svc
}

func severalWhoami(ctx context.Context, c meshcall.Caller, instance string) (severalWhere, error) {
	return meshcall.Unary[severalEmpty, severalWhere](ctx, c, "test.v1.Box", "whoami", severalEmpty{}, meshcall.WithInstance(instance))
}

func startSeveralRouters(t *testing.T, ctx context.Context) (*meshcall.LocalRouter, *meshcall.LocalRouter) {
	t.Helper()
	if os.Getenv("MESHCALL_ROUTER_BINARY") == "" {
		t.Skip("set MESHCALL_ROUTER_BINARY to run Router tests")
	}
	a, err := meshcall.StartLocalRouter(ctx, meshcall.WithPort(0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Stop() })
	b, err := meshcall.StartLocalRouter(ctx, meshcall.WithPort(0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	return a, b
}

func TestCallsReachTheRouterTheirInstanceOrServiceIsOn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a, b := startSeveralRouters(t, ctx)
	// Registered on b only: a answers instance_unknown / no_instances.
	running, err := meshcall.NewMesh(meshcall.NewRouter(b.URL())).
		Mount(severalBox(), severalPlain(), meshcall.WithAcceptTunnels(nil)).
		StartServing(ctx, meshcall.WithInstanceID("box-b"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = running.Close() }()
	routers := []string{a.URL(), b.URL()}
	fresh := func() *meshcall.Mesh {
		mesh := meshcall.NewMesh(meshcall.NewRouter(routers[0], routers[1]))
		t.Cleanup(func() { _ = mesh.Close() })
		return mesh
	}
	box := meshcall.WithInstance("box-b")

	mesh := fresh()
	if got := mesh.Endpoints(); !slices.Equal(got, routers) {
		t.Fatalf("Endpoints() = %v", got)
	}
	for range 2 { // the second from memory
		if got, err := severalWhoami(ctx, mesh, "box-b"); err != nil || got.Instance != "box-b" {
			t.Fatalf("whoami: %+v %v", got, err)
		}
	}
	if got, err := meshcall.Unary[severalEmpty, severalWhere](ctx, mesh, "test.v1.Plain", "hello", severalEmpty{}); err != nil || got.Instance != "box-b" {
		t.Fatalf("hello: %+v %v", got, err)
	}
	if _, err := severalWhoami(ctx, mesh, "nowhere"); !meshcall.IsCode(err, meshcall.CodeUnavailable) || !strings.Contains(reasonOf(err), "instance_unknown") {
		t.Fatalf("an unknown instance: %v", err)
	}

	// Every call shape moves on from a Router that did not route it, with
	// nothing lost from what the caller already handed over.
	stream, err := meshcall.ServerStream[severalCount, severalCount](ctx, fresh(), "test.v1.Box", "count", severalCount{N: 3}, box)
	if err != nil {
		t.Fatal(err)
	}
	var counted []int
	for item, err := range stream.All() {
		if err != nil {
			t.Fatal(err)
		}
		counted = append(counted, item.N)
	}
	if !slices.Equal(counted, []int{0, 1, 2}) {
		t.Fatalf("count: %v", counted)
	}
	sum, err := meshcall.ClientStream[severalCount, severalCount, severalCount](ctx, fresh(), "test.v1.Box", "sum", severalCount{N: 1},
		func(yield func(severalCount) bool) {
			for _, n := range []int{2, 3, 4} {
				if !yield(severalCount{N: n}) {
					return
				}
			}
		}, box)
	if err != nil || sum.N != 10 {
		t.Fatalf("sum: %+v %v", sum, err)
	}
	call, err := meshcall.Duplex[severalEmpty, severalCount, severalCount](ctx, fresh(), "test.v1.Box", "double", severalEmpty{}, box)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{1, 2} {
		if err := call.Send(severalCount{N: n}); err != nil {
			t.Fatal(err)
		}
	}
	if err := call.CloseSend(); err != nil {
		t.Fatal(err)
	}
	var doubled []int
	for item, err := range call.All() {
		if err != nil {
			t.Fatal(err)
		}
		doubled = append(doubled, item.N)
	}
	if !slices.Equal(doubled, []int{2, 4}) {
		t.Fatalf("double: %v", doubled)
	}

	// What belongs to one Router is asked of that Router.
	issuer := fresh()
	if _, err := issuer.Registry().List(ctx); err == nil || !strings.Contains(err.Error(), "mesh.At(router)") {
		t.Fatalf("a Router service on several Routers: %v", err)
	}
	atA, err := issuer.At(a.URL())
	if err != nil {
		t.Fatal(err)
	}
	if services, err := atA.Registry().List(ctx); err != nil || len(services) != 0 {
		t.Fatalf("a's registry: %+v %v", services, err)
	}
	atB, err := atA.At(b.URL())
	if err != nil {
		t.Fatal(err)
	}
	if services, err := atB.Registry().List(ctx); err != nil || len(services) == 0 {
		t.Fatalf("b's registry: %+v %v", services, err)
	}
	// A tunnel is opened where the instance is, and revoked everywhere.
	opened, err := issuer.Tunnels().Open(ctx, meshcall.OpenTunnelRequest{
		Instance: "box-b", Scopes: []meshcall.TunnelScope{{Service: "test.v1.Box"}}, Principal: "user:alice", TTLSeconds: 60,
	})
	if err != nil {
		t.Fatal(err)
	}
	if opened.Router != b.URL() {
		t.Fatalf("opened on %q, want %q", opened.Router, b.URL())
	}
	client := meshcall.NewMesh(meshcall.NewRouter(opened.Router), meshcall.WithTunnel(opened.Credential))
	defer func() { _ = client.Close() }()
	if got, err := meshcall.Unary[severalEmpty, severalWhere](ctx, client, "test.v1.Box", "whoami", severalEmpty{}); err != nil || got.Instance != "box-b" {
		t.Fatalf("through the tunnel: %+v %v", got, err)
	}
	if revoked, err := issuer.Tunnels().Revoke(ctx, opened.TunnelID); err != nil || !revoked {
		t.Fatalf("revoke: %v %v", revoked, err)
	}
	if revoked, err := issuer.Tunnels().Revoke(ctx, opened.TunnelID); err != nil || revoked {
		t.Fatalf("revoke again: %v %v", revoked, err)
	}

	// A Router that cannot be reached is skipped too.
	if err := a.Stop(); err != nil {
		t.Fatal(err)
	}
	if got, err := severalWhoami(ctx, fresh(), "box-b"); err != nil || got.Instance != "box-b" {
		t.Fatalf("with a stopped: %+v %v", got, err)
	}
}

func TestAnInstanceRegistersWithEveryRouterAskingItsSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	a, b := startSeveralRouters(t, ctx)
	var mu sync.Mutex
	asked := map[string]bool{}
	box := meshcall.NewMesh(meshcall.NewRouter(a.URL(), b.URL()), meshcall.WithTokenSource(func(_ context.Context, router string) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		asked[router] = true
		return "mt_any", nil
	}))
	running, err := box.Mount(severalBox()).StartServing(ctx, meshcall.WithInstanceID("box-ab"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = running.Close() }()
	mu.Lock()
	if !asked[a.URL()] || !asked[b.URL()] {
		t.Fatalf("asked for %v", asked)
	}
	mu.Unlock()
	for _, router := range []*meshcall.LocalRouter{a, b} {
		caller := meshcall.NewMesh(router)
		got, err := severalWhoami(ctx, caller, "box-ab")
		_ = caller.Close()
		if err != nil || got.Instance != "box-ab" {
			t.Fatalf("via %s: %+v %v", router.URL(), got, err)
		}
	}
}

func reasonOf(err error) string {
	if e, ok := meshcall.AsError(err); ok {
		return e.Reason()
	}
	return ""
}
