package meshcall_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	meshcall "github.com/t4wefan/meshcall/meshcall-go"
)

func TestTunnelsThroughALocalRouter(t *testing.T) {
	if os.Getenv("MESHCALL_ROUTER_BINARY") == "" {
		t.Skip("set MESHCALL_ROUTER_BINARY to run Router tests")
	}
	type Empty struct{}
	type Caller struct {
		Instance  string `json:"instance"`
		TunnelID  string `json:"tunnel_id"`
		Principal string `json:"principal"`
	}
	workspace := func() *meshcall.Service {
		svc := meshcall.NewService("test.v1.Workspace", meshcall.Describe("One developer box."), meshcall.Targeted())
		meshcall.HandleUnary(svc, "whoami", func(ctx *meshcall.Context, _ Empty) (Caller, error) {
			c := Caller{Instance: ctx.InstanceID}
			if ctx.Tunnel != nil {
				c.TunnelID, c.Principal = ctx.Tunnel.ID, ctx.Tunnel.Principal
			}
			return c, nil
		}, meshcall.Doc("Report the instance and the tunnel."), meshcall.ReadOnly())
		return svc
	}
	admin := meshcall.NewService("test.v1.Admin", meshcall.Describe("Never reachable through a tunnel."))
	meshcall.HandleUnary(admin, "whoami", func(ctx *meshcall.Context, _ Empty) (Caller, error) {
		return Caller{Instance: ctx.InstanceID}, nil
	}, meshcall.Doc("Report the instance."), meshcall.ReadOnly())

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	router, err := meshcall.StartLocalRouter(ctx, meshcall.WithPort(0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = router.Stop() }()

	var mu sync.Mutex
	var offers []meshcall.TunnelOffer
	closed := make(chan meshcall.TunnelClosed, 4)
	box := meshcall.NewMesh(router)
	box.Mount(workspace(),
		meshcall.WithAcceptTunnels(func(_ context.Context, offer meshcall.TunnelOffer) bool {
			mu.Lock()
			offers = append(offers, offer)
			mu.Unlock()
			return offer.Principal != "user:nobody"
		}),
		meshcall.WithTunnelClosed(func(_ context.Context, c meshcall.TunnelClosed) { closed <- c }))
	box.Mount(admin)
	running, err := box.StartServing(ctx, meshcall.WithInstanceID("box-a"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = running.Close() }()
	other := meshcall.NewMesh(router).Mount(workspace(), meshcall.WithAcceptTunnels(nil))
	otherRunning, err := other.StartServing(ctx, meshcall.WithInstanceID("box-b"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = otherRunning.Close() }()

	issuer := meshcall.NewMesh(router)
	defer func() { _ = issuer.Close() }()
	opened, err := issuer.Tunnels().Open(ctx, meshcall.OpenTunnelRequest{
		Instance: "box-a", Scopes: []meshcall.TunnelScope{{Service: "test.v1.Workspace"}}, Principal: "user:alice", TTLSeconds: 60,
	})
	if err != nil || !strings.HasPrefix(opened.Credential, "mtun_") {
		t.Fatalf("open: %v %+v", err, opened)
	}
	_, err = issuer.Tunnels().Open(ctx, meshcall.OpenTunnelRequest{
		Instance: "box-a", Scopes: []meshcall.TunnelScope{{Service: "test.v1.Workspace"}}, Principal: "user:nobody",
	})
	var refused *meshcall.Error
	if !errors.As(err, &refused) || refused.Code != meshcall.CodePermissionDenied || refused.Reason() != "tunnel_rejected" {
		t.Fatalf("refusal: %v", err)
	}

	client := meshcall.NewMesh(meshcall.NewRouter(router.URL()), meshcall.WithTunnel(opened.Credential))
	defer func() { _ = client.Close() }()
	seen, err := meshcall.Unary[Empty, Caller](ctx, client, "test.v1.Workspace", "whoami", Empty{}, meshcall.WithInstance("box-a"))
	if err != nil || seen != (Caller{Instance: "box-a", TunnelID: opened.TunnelID, Principal: "user:alice"}) {
		t.Fatalf("tunnel call: %v %+v", err, seen)
	}
	for _, c := range []struct {
		service string
		opts    []meshcall.CallOption
	}{
		{"test.v1.Workspace", []meshcall.CallOption{meshcall.WithInstance("box-b")}},
		{"test.v1.Admin", nil},
	} {
		_, err := meshcall.Unary[Empty, Caller](ctx, client, c.service, "whoami", Empty{}, c.opts...)
		var denied *meshcall.Error
		if !errors.As(err, &denied) || denied.Code != meshcall.CodePermissionDenied || denied.Reason() != "tunnel_scope" {
			t.Fatalf("%s outside the scope: %v", c.service, err)
		}
	}
	services, err := client.Registry().List(ctx)
	if err != nil || len(services) != 1 || services[0].Name != "test.v1.Workspace" || len(services[0].Instances) != 1 {
		t.Fatalf("registry view: %v %+v", err, services)
	}
	who, err := client.Auth().Whoami(ctx)
	if err != nil || who.Tunnel == nil || who.Tunnel.InstanceID != "box-a" {
		t.Fatalf("whoami: %v %+v", err, who)
	}

	listed, err := issuer.Tunnels().List(ctx)
	if err != nil || len(listed) != 1 || listed[0].TunnelID != opened.TunnelID {
		t.Fatalf("list: %v %+v", err, listed)
	}
	if revoked, err := issuer.Tunnels().Revoke(ctx, opened.TunnelID); err != nil || !revoked {
		t.Fatalf("revoke: %v %v", revoked, err)
	}
	select {
	case c := <-closed:
		if c != (meshcall.TunnelClosed{TunnelID: opened.TunnelID, Principal: "user:alice", Reason: "revoked"}) {
			t.Fatalf("close notice: %+v", c)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no close notice")
	}
}

func TestTunnelCredentialsHaveOneWayIn(t *testing.T) {
	router := meshcall.NewRouter("ws://127.0.0.1:1")
	for name, build := range map[string]func(){
		"token":     func() { meshcall.NewMesh(router, meshcall.WithToken("mtun_abc")) },
		"prefix":    func() { meshcall.NewMesh(router, meshcall.WithTunnel("mt_abc")) },
		"two":       func() { meshcall.NewMesh(router, meshcall.WithToken("mt_abc"), meshcall.WithTunnel("mtun_abc")) },
		"mount":     func() { meshcall.NewMesh(router, meshcall.WithTunnel("mtun_abc")).Mount(meshcall.NewService("x.v1.X")) },
		"no accept": func() { meshcall.NewMesh(router).Mount(meshcall.NewService("x.v1.X"), meshcall.WithTunnelClosed(nil)) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: expected a panic", name)
				}
			}()
			build()
		}()
	}
}
