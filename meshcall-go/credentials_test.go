package meshcall_test

// docs/protocol.md → Credential sources.
// conformance-scenario: auth.credential_source

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	meshcall "github.com/t4wefan/meshcall/meshcall-go"
)

type sourceEmpty struct{}

type sourceSeen struct {
	Tunnel string `json:"tunnel"`
	Sealed bool   `json:"sealed"`
}

func sourceBox() *meshcall.Service {
	svc := meshcall.NewService("test.v1.Box", meshcall.Describe("A box reached through tunnels."), meshcall.Targeted())
	meshcall.HandleUnary(svc, "whoami", func(ctx *meshcall.Context, _ sourceEmpty) (sourceSeen, error) {
		if ctx.Tunnel == nil {
			return sourceSeen{}, nil
		}
		return sourceSeen{Tunnel: ctx.Tunnel.ID, Sealed: ctx.Tunnel.Sealed}, nil
	}, meshcall.Doc("Report the tunnel of this call."), meshcall.ReadOnly())
	return svc
}

func sourceWhoami(ctx context.Context, caller meshcall.Caller) (sourceSeen, error) {
	return meshcall.Unary[sourceEmpty, sourceSeen](ctx, caller, "test.v1.Box", "whoami", sourceEmpty{}, meshcall.WithInstance("box-a"))
}

// box holds a source's current credential and counts how often it is asked.
type sourceBoxState struct {
	mu      sync.Mutex
	current string
	asked   int
}

func (s *sourceBoxState) set(credential string) { s.mu.Lock(); s.current = credential; s.mu.Unlock() }

func (s *sourceBoxState) count() int { s.mu.Lock(); defer s.mu.Unlock(); return s.asked }

func (s *sourceBoxState) reset() { s.mu.Lock(); s.asked = 0; s.mu.Unlock() }

func (s *sourceBoxState) source(context.Context, string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.asked++
	return s.current, nil
}

func TestTunnelSourceReplacesARevokedTunnel(t *testing.T) {
	if os.Getenv("MESHCALL_ROUTER_BINARY") == "" {
		t.Skip("set MESHCALL_ROUTER_BINARY to run Router tests")
	}
	for _, sealed := range []bool{false, true} {
		t.Run(fmt.Sprintf("sealed=%v", sealed), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
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
			mountOpts := []any{sourceBox(), meshcall.WithAcceptTunnels(nil)}
			serveOpts := []meshcall.ServeOption{meshcall.WithInstanceID("box-a")}
			if sealed {
				mountOpts = append(mountOpts, meshcall.WithRequireE2E())
				serveOpts = append(serveOpts, meshcall.WithE2EPrivateKey(private))
			}
			running, err := meshcall.NewMesh(router).Mount(mountOpts...).StartServing(ctx, serveOpts...)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = running.Close() }()
			issuer := meshcall.NewMesh(router)
			defer func() { _ = issuer.Close() }()
			open := func() meshcall.OpenedTunnel {
				request := meshcall.OpenTunnelRequest{
					Instance: "box-a", Scopes: []meshcall.TunnelScope{{Service: "test.v1.Box"}}, Principal: "user:alice", TTLSeconds: 60,
				}
				if sealed {
					request.E2EKey = public
				}
				opened, err := issuer.Tunnels().Open(ctx, request)
				if err != nil {
					t.Fatal(err)
				}
				return opened
			}
			settle := func() { time.Sleep(200 * time.Millisecond) }

			current := open()
			state := &sourceBoxState{current: current.Credential}
			options := []meshcall.MeshOption{meshcall.WithTunnelSource(state.source)}
			if sealed {
				options = append(options, meshcall.WithE2EKey(public))
			}
			client := meshcall.NewMesh(meshcall.NewRouter(router.URL()), options...)
			defer func() { _ = client.Close() }()
			want := func(opened meshcall.OpenedTunnel) {
				t.Helper()
				seen, err := sourceWhoami(ctx, client)
				if err != nil || seen != (sourceSeen{Tunnel: opened.TunnelID, Sealed: sealed}) {
					t.Fatalf("whoami: %+v %v (want tunnel %s)", seen, err, opened.TunnelID)
				}
			}
			want(current)
			if state.count() != 1 {
				t.Fatalf("asked %d times", state.count())
			}

			// Revoked: the next call connects with what the source returns now.
			first := current
			current = open()
			state.set(current.Credential)
			if _, err := issuer.Tunnels().Revoke(ctx, first.TunnelID); err != nil {
				t.Fatal(err)
			}
			settle()
			want(current)

			// A source whose new credential is refused too: the call fails…
			if _, err := issuer.Tunnels().Revoke(ctx, current.TunnelID); err != nil {
				t.Fatal(err)
			}
			settle()
			state.reset()
			if _, err := sourceWhoami(ctx, client); !meshcall.IsCode(err, meshcall.CodeUnauthenticated) {
				t.Fatalf("a refused credential: %v", err)
			}
			if state.count() != 2 {
				t.Fatalf("asked %d times, want once and once more after the refusal", state.count())
			}

			// …and the refusal is not kept: the next call asks again.
			current = open()
			state.set(current.Credential)
			want(current)
		})
	}
}

func TestFailingOrWrongSourceFailsTheAttempt(t *testing.T) {
	if os.Getenv("MESHCALL_ROUTER_BINARY") == "" {
		t.Skip("set MESHCALL_ROUTER_BINARY to run Router tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	router, err := meshcall.StartLocalRouter(ctx, meshcall.WithPort(0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = router.Stop() }()
	var mu sync.Mutex
	fail := true
	mesh := meshcall.NewMesh(meshcall.NewRouter(router.URL()), meshcall.WithTokenSource(func(context.Context, string) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		if fail {
			return "", errors.New("vault unreachable")
		}
		return "mt_any", nil
	}))
	defer func() { _ = mesh.Close() }()
	_, err = mesh.Registry().List(ctx)
	var failed *meshcall.Error
	if !errors.As(err, &failed) || failed.Code != meshcall.CodeUnavailable || !failed.Retryable || failed.Reason() != "credential_source" {
		t.Fatalf("a failing source: %v", err)
	}
	// Not kept: once the source answers, the next call connects (a
	// development Router takes any token).
	mu.Lock()
	fail = false
	mu.Unlock()
	if _, err := mesh.Registry().List(ctx); err != nil {
		t.Fatalf("after the source recovered: %v", err)
	}

	wrong := meshcall.NewMesh(meshcall.NewRouter(router.URL()), meshcall.WithTokenSource(func(context.Context, string) (string, error) {
		return "mtun_abc", nil
	}))
	defer func() { _ = wrong.Close() }()
	var kind *meshcall.CredentialSourceError
	if _, err := wrong.Registry().List(ctx); !errors.As(err, &kind) {
		t.Fatalf("a token source returning a tunnel credential: %v", err)
	}
}

func TestInstanceWithTokenSourceRegistersAgain(t *testing.T) {
	bin := os.Getenv("MESHCALL_ROUTER_BINARY")
	if bin == "" {
		t.Skip("set MESHCALL_ROUTER_BINARY to run Router tests")
	}
	hashCmd := exec.Command(bin, "hash-password")
	hashCmd.Stdin = strings.NewReader("secret\n")
	hashOut, err := hashCmd.Output()
	if err != nil {
		t.Fatalf("hash-password: %v", err)
	}
	authFile := filepath.Join(t.TempDir(), "auth.json")
	doc := fmt.Sprintf(`{"users":[{"username":"admin","password_hash":%q,"roles":["client","server"],"register":["*"],"call":["*"]}]}`,
		strings.TrimSpace(string(hashOut)))
	if err := os.WriteFile(authFile, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	router, err := meshcall.StartLocalRouter(ctx, meshcall.WithPort(0), meshcall.WithAuthFile(authFile))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = router.Stop() }()
	issuer, err := meshcall.ConnectMesh(ctx, meshcall.NewRouter(router.URL()), meshcall.WithAuth(meshcall.BasicAuth("admin", "secret")))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = issuer.Close() }()
	issue := func(scope meshcall.RouterScope) meshcall.IssuedToken {
		issued, err := issuer.Auth().IssueToken(ctx, meshcall.IssueTokenRequest{Scopes: []meshcall.RouterScope{scope}, TTLSeconds: 60})
		if err != nil {
			t.Fatal(err)
		}
		return issued
	}
	register := meshcall.RouterScope{Service: "test.v1.Box", Register: true, InstanceIDs: []string{"box-a"}}

	current := issue(register)
	state := &sourceBoxState{current: current.Token}
	box := meshcall.NewMesh(meshcall.NewRouter(router.URL()), meshcall.WithTokenSource(state.source),
		meshcall.WithReconnect(meshcall.ReconnectPolicy{InitialDelay: 10 * time.Millisecond, MaxDelay: 50 * time.Millisecond}))
	running, err := box.Mount(sourceBox()).StartServing(ctx, meshcall.WithInstanceID("box-a"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = running.Close() }()
	if _, err := sourceWhoami(ctx, issuer); err != nil {
		t.Fatal(err)
	}

	first := current
	current = issue(register)
	state.set(current.Token)
	before := state.count()
	if _, err := issuer.Auth().RevokeToken(ctx, first.TokenID); err != nil {
		t.Fatal(err)
	}
	// The instance reconnects with backoff, asking the source again.
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		if _, err = sourceWhoami(ctx, issuer); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the instance did not register again: %v", err)
		}
	}
	if state.count() <= before {
		t.Fatal("the source was not asked again")
	}

	// Over HTTP: a request answered 401 is sent once more with a new token.
	callers := &sourceBoxState{}
	call := meshcall.RouterScope{Service: "test.v1.Box", Methods: []string{"*"}}
	caller := issue(call)
	callers.set(caller.Token)
	http := meshcall.NewMesh(meshcall.NewRouter(strings.Replace(router.URL(), "ws://", "http://", 1)),
		meshcall.WithTokenSource(callers.source))
	defer func() { _ = http.Close() }()
	if _, err := sourceWhoami(ctx, http); err != nil {
		t.Fatal(err)
	}
	old := caller
	caller = issue(call)
	callers.set(caller.Token)
	if _, err := issuer.Auth().RevokeToken(ctx, old.TokenID); err != nil {
		t.Fatal(err)
	}
	if _, err := sourceWhoami(ctx, http); err != nil {
		t.Fatalf("after revoking the HTTP caller's token: %v", err)
	}
}
