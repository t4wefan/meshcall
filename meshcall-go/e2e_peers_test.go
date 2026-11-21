package meshcall_test

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	meshcall "github.com/t4wefan/meshcall/meshcall-go"
)

// Clients that authenticate themselves on end-to-end tunnels
// (docs/protocol.md → Client authentication): IK for known clients, IKpsk2
// to pair a new one.

type Seen struct {
	Protocol  string `json:"protocol"`
	PeerKey   string `json:"peer_key"`
	PairingID string `json:"pairing_id"`
}

func whoamiService(name string) *meshcall.Service {
	svc := meshcall.NewService(name, meshcall.Describe("Reports the end-to-end session of each call."))
	meshcall.HandleUnary(svc, "whoami", func(ctx *meshcall.Context, _ Empty) (Seen, error) {
		if ctx.E2E == nil {
			return Seen{}, nil
		}
		return Seen{Protocol: ctx.E2E.Protocol, PeerKey: ctx.E2E.PeerKey, PairingID: ctx.E2E.PairingID}, nil
	}, meshcall.Doc("The end-to-end session of this call."), meshcall.ReadOnly())
	return svc
}

// owner is what the device keeps: paired client keys and one-time pairings.
type owner struct {
	mu       sync.Mutex
	paired   map[string]bool
	pairings map[string]string
	asked    []meshcall.E2EPeer
}

func (o *owner) psk(_ context.Context, id string) (string, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	psk, ok := o.pairings[id]
	return psk, ok
}

func (o *owner) authorize(_ context.Context, peer meshcall.E2EPeer) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.asked = append(o.asked, peer)
	if peer.PeerKey == "" {
		return false
	}
	if peer.PairingID != "" {
		// Spent here, once the client proved it holds the PSK.
		if _, ok := o.pairings[peer.PairingID]; !ok {
			return false
		}
		delete(o.pairings, peer.PairingID)
		o.paired[peer.PeerKey] = true
		return true
	}
	return o.paired[peer.PeerKey]
}

func (o *owner) lastAsked() meshcall.E2EPeer {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.asked) == 0 {
		return meshcall.E2EPeer{}
	}
	return o.asked[len(o.asked)-1]
}

func (o *owner) has(pairing, key string) (bool, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	_, pending := o.pairings[pairing]
	return pending, o.paired[key]
}

func TestE2EPeersPairThenReconnectWithIK(t *testing.T) {
	if os.Getenv("MESHCALL_ROUTER_BINARY") == "" {
		t.Skip("set MESHCALL_ROUTER_BINARY to run Router tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	router, err := meshcall.StartLocalRouter(ctx, meshcall.WithPort(0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = router.Stop() }()
	private, public, _ := meshcall.GenerateE2EKey()
	alicePriv, alicePub, _ := meshcall.GenerateE2EKey()
	malloryPriv, malloryPub, _ := meshcall.GenerateE2EKey()
	o := &owner{paired: map[string]bool{}, pairings: map[string]string{}}
	serve := func() *meshcall.RunningServer {
		device := meshcall.NewMesh(router).
			Mount(whoamiService("test.v1.Device"), meshcall.WithAcceptTunnels(nil), meshcall.WithRequireE2EPeer()).
			Mount(whoamiService("test.v1.Open"), meshcall.WithAcceptTunnels(nil))
		running, err := device.StartServing(ctx, meshcall.WithInstanceID("dev-a"), meshcall.WithE2EPrivateKey(private),
			meshcall.WithAuthorizeE2EPeer(o.authorize), meshcall.WithE2EPairingPSK(o.psk))
		if err != nil {
			t.Fatal(err)
		}
		return running
	}
	running := serve()
	issuer := meshcall.NewMesh(router)
	defer func() { _ = issuer.Close() }()
	tunnel := func() string {
		opened, err := issuer.Tunnels().Open(ctx, meshcall.OpenTunnelRequest{Instance: "dev-a",
			Scopes: []meshcall.TunnelScope{{Service: "test.v1.Device"}, {Service: "test.v1.Open"}}, Principal: "user:alice", E2EKey: public})
		if err != nil {
			t.Fatal(err)
		}
		return opened.Credential
	}
	whoami := func(m *meshcall.Mesh, service string, opts ...meshcall.CallOption) (Seen, error) {
		return meshcall.Unary[Empty, Seen](ctx, m, service, "whoami", Empty{}, opts...)
	}
	reason := func(err error) string {
		if e, ok := meshcall.AsError(err); ok {
			return e.Code + "/" + e.Reason()
		}
		return "?"
	}
	client := func(credential string, identity *meshcall.E2EIdentity) *meshcall.Mesh {
		opts := []meshcall.MeshOption{meshcall.WithTunnel(credential), meshcall.WithE2EKey(public)}
		if identity != nil {
			opts = append(opts, meshcall.WithE2EIdentity(*identity))
		}
		m := meshcall.NewMesh(meshcall.NewRouter(router.URL()), opts...)
		t.Cleanup(func() { _ = m.Close() })
		return m
	}

	// WithRequireE2EPeer implies WithRequireE2E: a plain tunnel is refused.
	_, err = issuer.Tunnels().Open(ctx, meshcall.OpenTunnelRequest{Instance: "dev-a", Scopes: []meshcall.TunnelScope{{Service: "test.v1.Device"}}, Principal: "p"})
	if e, _ := meshcall.AsError(err); e == nil || e.Reason() != "tunnel_rejected" {
		t.Fatalf("a plain tunnel to a peer-only service: %v", err)
	}
	// Not through a tunnel at all: refused by the instance itself.
	if _, err := whoami(issuer, "test.v1.Device", meshcall.WithInstance("dev-a")); reason(err) != "permission_denied/e2e_peer_required" {
		t.Fatalf("a call without a tunnel: %v", err)
	}

	// An unknown client key: the handshake fails, without saying why.
	if _, err := whoami(client(tunnel(), &meshcall.E2EIdentity{PrivateKey: malloryPriv, InstanceID: "dev-a"}), "test.v1.Device"); reason(err) != "unauthenticated/e2e_handshake_failed" {
		t.Fatalf("an unknown client key: %v", err)
	}
	if o.lastAsked().PeerKey != malloryPub || o.lastAsked().Protocol != meshcall.E2EProtocolIK {
		t.Fatalf("the hook was asked about %+v", o.lastAsked())
	}

	// An NK client reaches services that do not require a peer, not others.
	nk := client(tunnel(), nil)
	if seen, err := whoami(nk, "test.v1.Open"); err != nil || seen != (Seen{Protocol: meshcall.E2EProtocolNK}) {
		t.Fatalf("NK to an open service: %v %+v", err, seen)
	}
	if _, err := whoami(nk, "test.v1.Device"); reason(err) != "permission_denied/e2e_peer_required" {
		t.Fatalf("NK to a peer-only service: %v", err)
	}

	// Pairing: the owner shows a pairing id and PSK on the device.
	psk, _ := meshcall.GenerateE2EPSK()
	o.mu.Lock()
	o.pairings["pair-1"] = psk
	o.mu.Unlock()
	// A wrong PSK fails on the client and does not spend the pairing.
	wrongPSK, _ := meshcall.GenerateE2EPSK()
	wrong := client(tunnel(), &meshcall.E2EIdentity{PrivateKey: malloryPriv, InstanceID: "dev-a", Pairing: &meshcall.E2EPairing{ID: "pair-1", PSK: wrongPSK}})
	if _, err := whoami(wrong, "test.v1.Device"); reason(err) != "unauthenticated/e2e_handshake_failed" {
		t.Fatalf("a wrong PSK: %v", err)
	}
	if pending, paired := o.has("pair-1", malloryPub); !pending || paired {
		t.Fatal("a wrong PSK spent the pairing")
	}
	// The wrong instance id is bound in the prologue: no session.
	pairing := &meshcall.E2EPairing{ID: "pair-1", PSK: psk}
	elsewhere := client(tunnel(), &meshcall.E2EIdentity{PrivateKey: alicePriv, InstanceID: "dev-b", Pairing: pairing})
	if _, err := whoami(elsewhere, "test.v1.Device"); reason(err) != "unauthenticated/e2e_handshake_failed" {
		t.Fatalf("another instance id: %v", err)
	}
	if pending, _ := o.has("pair-1", alicePub); !pending {
		t.Fatal("a handshake for another instance spent the pairing")
	}

	alice := client(tunnel(), &meshcall.E2EIdentity{PrivateKey: alicePriv, InstanceID: "dev-a", Pairing: pairing})
	seen, err := whoami(alice, "test.v1.Device")
	if err != nil || seen != (Seen{Protocol: meshcall.E2EProtocolIKpsk2, PeerKey: alicePub, PairingID: "pair-1"}) {
		t.Fatalf("paired: %v %+v", err, seen)
	}
	if pending, paired := o.has("pair-1", alicePub); pending || !paired {
		t.Fatal("the confirmed pairing was not spent")
	}

	// The device restarts and loses its sessions: the same client handshakes
	// again with IK (the pairing is spent) and goes on.
	_ = running.Close()
	running = serve()
	defer func() { _ = running.Close() }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		seen, err = whoami(alice, "test.v1.Device")
		if err == nil {
			break
		}
		if strings.HasSuffix(reason(err), "e2e_handshake_failed") || time.Now().After(deadline) {
			t.Fatalf("after the device restarted: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if seen != (Seen{Protocol: meshcall.E2EProtocolIK, PeerKey: alicePub}) {
		t.Fatalf("after the restart: %+v", seen)
	}

	// Another client cannot reuse the spent pairing.
	again := client(tunnel(), &meshcall.E2EIdentity{PrivateKey: malloryPriv, InstanceID: "dev-a", Pairing: pairing})
	if _, err := whoami(again, "test.v1.Device"); reason(err) != "unauthenticated/e2e_handshake_failed" {
		t.Fatalf("a spent pairing: %v", err)
	}
	if _, paired := o.has("pair-1", malloryPub); paired {
		t.Fatal("a spent pairing paired another client")
	}
}

func TestE2EIdentityNeedsAPinnedKeyAndAHook(t *testing.T) {
	_, public, _ := meshcall.GenerateE2EKey()
	me, _, _ := meshcall.GenerateE2EKey()
	router := meshcall.NewRouter("ws://127.0.0.1:1")
	for name, build := range map[string]func(){
		"an identity without a pinned key": func() {
			meshcall.NewMesh(router, meshcall.WithTunnel("mtun_e2e.x"), meshcall.WithE2EIdentity(meshcall.E2EIdentity{PrivateKey: me, InstanceID: "dev-a"}))
		},
		"an identity without an instance id": func() {
			meshcall.NewMesh(router, meshcall.WithTunnel("mtun_e2e.x"), meshcall.WithE2EKey(public), meshcall.WithE2EIdentity(meshcall.E2EIdentity{PrivateKey: me}))
		},
		"a short PSK": func() {
			meshcall.WithE2EIdentity(meshcall.E2EIdentity{PrivateKey: me, InstanceID: "a", Pairing: &meshcall.E2EPairing{ID: "p", PSK: "short"}})
		},
		"WithRequireE2EPeer without WithAcceptTunnels": func() {
			meshcall.NewMesh(router).Mount(whoamiService("test.v1.Device"), meshcall.WithRequireE2EPeer())
		},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s was accepted", name)
				}
			}()
			build()
		}()
	}
	if _, err := meshcall.NewClient(meshcall.ClientOptions{Authorization: "Bearer mtun_e2e.x",
		E2EIdentity: &meshcall.E2EIdentity{PrivateKey: me, InstanceID: "a"}}, "ws://127.0.0.1:1"); err == nil {
		t.Error("NewClient took an identity without E2EKey")
	}

	private, _, _ := meshcall.GenerateE2EKey()
	policy := &meshcall.TunnelPolicy{RequireE2EPeer: true}
	for name, opts := range map[string]meshcall.ServerOptions{
		"WithRequireE2EPeer without WithAuthorizeE2EPeer": {E2EPrivateKey: private},
		"WithRequireE2EPeer without WithE2EPrivateKey": {
			AuthorizeE2EPeer: func(context.Context, meshcall.E2EPeer) bool { return true }},
	} {
		opts.Services = []*meshcall.Service{whoamiService("test.v1.Device")}
		opts.TunnelPolicies = map[string]*meshcall.TunnelPolicy{"test.v1.Device": policy}
		if _, err := meshcall.NewServer(opts); err == nil || !strings.Contains(err.Error(), "WithRequireE2EPeer") {
			t.Errorf("%s: %v", name, err)
		}
	}
}
