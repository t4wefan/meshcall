package meshcall_test

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	meshcall "github.com/t4wefan/meshcall/meshcall-go"
)

func TestEndToEndSealedTunnel(t *testing.T) {
	if os.Getenv("MESHCALL_ROUTER_BINARY") == "" {
		t.Skip("set MESHCALL_ROUTER_BINARY to run Router tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	router, err := meshcall.StartLocalRouter(ctx, meshcall.WithPort(0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = router.Stop() }()
	private, public, err := meshcall.GenerateE2EKey()
	if err != nil {
		t.Fatal(err)
	}
	if derived, _ := meshcall.E2EPublicKey(private); derived != public {
		t.Fatalf("E2EPublicKey: %s != %s", derived, public)
	}
	serve := func() *meshcall.RunningServer {
		box := meshcall.NewMesh(router).Mount(fixture(new(atomic.Int32)), meshcall.WithAcceptTunnels(nil))
		running, err := box.StartServing(ctx, meshcall.WithInstanceID("box-a"), meshcall.WithE2EPrivateKey(private))
		if err != nil {
			t.Fatal(err)
		}
		return running
	}
	running := serve()
	issuer := meshcall.NewMesh(router)
	defer func() { _ = issuer.Close() }()
	scopes := []meshcall.TunnelScope{{Service: "test.v1.Go"}}
	opened, err := issuer.Tunnels().Open(ctx, meshcall.OpenTunnelRequest{Instance: "box-a", Scopes: scopes, Principal: "user:alice", E2EKey: public})
	if err != nil || opened.E2EKey != public {
		t.Fatalf("open: %v %+v", err, opened)
	}
	// An offer pinning a key the instance does not hold is refused.
	_, otherKey, _ := meshcall.GenerateE2EKey()
	_, err = issuer.Tunnels().Open(ctx, meshcall.OpenTunnelRequest{Instance: "box-a", Scopes: scopes, Principal: "p", E2EKey: otherKey})
	if e, _ := meshcall.AsError(err); e == nil || e.Reason() != "tunnel_rejected" {
		t.Fatalf("an offer pinning another key: %v", err)
	}

	client := meshcall.NewMesh(meshcall.NewRouter(router.URL()), meshcall.WithTunnel(opened.Credential), meshcall.WithE2EKey(public))
	defer func() { _ = client.Close() }()
	doubled, err := meshcall.Unary[Num, Num](ctx, client, "test.v1.Go", "double", Num{Value: 21})
	if err != nil || doubled.Value != 42 {
		t.Fatalf("sealed unary: %v %+v", err, doubled)
	}
	stream, err := meshcall.ServerStream[Num, Num](ctx, client, "test.v1.Go", "count", Num{Value: 5})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for item, err := range stream.All() {
		if err != nil || item.Value != n {
			t.Fatalf("sealed server stream item %d: %v %+v", n, err, item)
		}
		n++
	}
	if n != 5 {
		t.Fatalf("sealed server stream: %d items", n)
	}
	sum, err := meshcall.ClientStream[Num, Num, Num](ctx, client, "test.v1.Go", "sum", Num{Value: 1}, func(yield func(Num) bool) {
		for i := 1; i <= 3 && yield(Num{Value: i}); i++ {
		}
	})
	if err != nil || sum.Value != 7 {
		t.Fatalf("sealed client stream: %v %+v", err, sum)
	}
	chat, err := meshcall.Duplex[Text, Text, Text](ctx, client, "test.v1.Go", "echo", Text{Text: "> "})
	if err != nil {
		t.Fatal(err)
	}
	if err := chat.Send(Text{Text: "ls"}); err != nil {
		t.Fatal(err)
	}
	if got, err := chat.Recv(); err != nil || got.Text != "> ls" {
		t.Fatalf("sealed duplex: %v %+v", err, got)
	}
	_ = chat.CloseSend()
	// Application errors keep their code; the message travels sealed.
	_, err = meshcall.Unary[Num, Num](ctx, client, "test.v1.Go", "fail", Num{Value: 3})
	if e, _ := meshcall.AsError(err); e == nil || e.Code != "app.nope" || e.Message != "no" || e.Details["value"] != float64(3) {
		t.Fatalf("sealed error: %v", err)
	}

	// The same tunnel without sealing is refused by the Router.
	plain := meshcall.NewMesh(meshcall.NewRouter(router.URL()), meshcall.WithTunnel(opened.Credential))
	defer func() { _ = plain.Close() }()
	_, err = meshcall.Unary[Num, Num](ctx, plain, "test.v1.Go", "double", Num{Value: 1})
	if e, _ := meshcall.AsError(err); e == nil || e.Reason() != "tunnel_e2e_required" {
		t.Fatalf("a plain call on an E2E tunnel: %v", err)
	}
	// A client pinning the wrong key cannot complete the handshake.
	wrong := meshcall.NewMesh(meshcall.NewRouter(router.URL()), meshcall.WithTunnel(opened.Credential), meshcall.WithE2EKey(otherKey))
	defer func() { _ = wrong.Close() }()
	_, err = meshcall.Unary[Num, Num](ctx, wrong, "test.v1.Go", "double", Num{Value: 1})
	if e, _ := meshcall.AsError(err); e == nil || e.Code != meshcall.CodeUnauthenticated || e.Reason() != "e2e_handshake_failed" {
		t.Fatalf("a wrong pinned key: %v", err)
	}

	// The instance restarts with the same key: it loses its sessions, the
	// Router re-offers the tunnel, and the client handshakes again.
	_ = running.Close()
	running = serve()
	defer func() { _ = running.Close() }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		doubled, err = meshcall.Unary[Num, Num](ctx, client, "test.v1.Go", "double", Num{Value: 2})
		if err == nil {
			break
		}
		if e, _ := meshcall.AsError(err); time.Now().After(deadline) || (e != nil && e.Reason() == "e2e_session_unknown") {
			t.Fatalf("after the instance restarted: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if doubled.Value != 4 {
		t.Fatalf("after the restart: %+v", doubled)
	}
}
