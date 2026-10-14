package router

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func authFrom(t *testing.T, doc map[string]any) *Auth {
	t.Helper()
	data, _ := json.Marshal(doc)
	path := filepath.Join(t.TempDir(), "auth.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	auth, err := LoadAuth(path)
	if err != nil {
		t.Fatal(err)
	}
	return auth
}

func TestAnonymousUserIsAnOrdinaryUserWithAReservedName(t *testing.T) {
	key, keyHash, err := NewAPIKey("ci")
	if err != nil {
		t.Fatal(err)
	}
	auth := authFrom(t, map[string]any{
		"users": []map[string]any{
			{"username": "anonymous", "roles": []string{"client"}, "call": []string{testService + "/echo", RegistryService + "/*"}},
			{"username": "worker", "password_hash": passwordHash(), "roles": []string{"server"}, "register": []string{"*"}},
		},
		"api_keys": []map[string]any{
			{"id": "ci", "key_hash": keyHash, "roles": []string{"client"}, "call": []string{testService + "/*"}},
		},
	})
	r := runningRouter(t, auth)
	inst := dial(t, r, basicHeader("worker", "test-only-password"))
	hello(t, inst, "server", "inst")
	send(t, inst, frame{Kind: "server.register", InstanceID: "inst", Services: []service{{Name: testService, Methods: []method{
		{Name: "echo", Stream: "unary", Balance: balance{Kind: "round_robin"}},
		{Name: "secret", Stream: "unary", Balance: balance{Kind: "round_robin"}},
	}}}})
	receive(t, inst)

	// No credentials: the connection is accepted as anonymous.
	anon := dial(t, r, "")
	hello(t, anon, "client", "anon")
	send(t, anon, frame{Kind: "call.open", CallID: "a1", Service: testService, Method: "echo", Payload: json.RawMessage(`{}`)})
	if got := receive(t, inst); got.CallID != "a1" {
		t.Fatalf("anonymous echo did not reach the instance: %+v", got)
	}
	send(t, inst, frame{Kind: "call.result", CallID: "a1", Payload: json.RawMessage(`{}`)})
	receive(t, anon)
	// A forbidden method and a missing one look the same.
	send(t, anon, frame{Kind: "call.open", CallID: "a2", Service: testService, Method: "secret", Payload: json.RawMessage(`{}`)})
	forbidden := receive(t, anon)
	send(t, anon, frame{Kind: "call.open", CallID: "a3", Service: "nothing.v1.Here", Method: "x", Payload: json.RawMessage(`{}`)})
	missing := receive(t, anon)
	if forbidden.Error == nil || forbidden.Error.Code != "permission_denied" || missing.Error == nil ||
		forbidden.Error.Code != missing.Error.Code || forbidden.Error.Message != missing.Error.Message {
		t.Fatalf("asymmetric denial: %+v / %+v", forbidden.Error, missing.Error)
	}
	// Anonymous can neither register nor issue tokens.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	srv := dial(t, r, "")
	send(t, srv, frame{Kind: "hello", Protocol: Protocol, Role: "server", PeerID: "x", InstanceID: "x"})
	if _, _, err := srv.Read(ctx); err == nil {
		t.Fatal("anonymous was allowed the server role")
	}
	send(t, anon, frame{Kind: "call.open", CallID: "a4", Service: AuthService, Method: "issue_token", Payload: json.RawMessage(`{"ttl_seconds":60,"scopes":[]}`)})
	if got := receive(t, anon); got.Error == nil || got.Error.Code != "permission_denied" {
		t.Fatalf("anonymous issue_token: %+v", got)
	}

	// An API key authenticates with one hash lookup and survives as config.
	keyed := dial(t, r, "Bearer "+key)
	hello(t, keyed, "client", "keyed")
	send(t, keyed, frame{Kind: "call.open", CallID: "k1", Service: testService, Method: "secret", Payload: json.RawMessage(`{}`)})
	if got := receive(t, inst); got.CallID != "k1" {
		t.Fatalf("API key call: %+v", got)
	}
	send(t, inst, frame{Kind: "call.result", CallID: "k1", Payload: json.RawMessage(`{}`)})
	receive(t, keyed)
	// A wrong key is refused at connect, never downgraded to anonymous.
	_, resp, err := websocket.Dial(ctx, fmt.Sprintf("ws://127.0.0.1:%d", r.Ready().Port),
		&websocket.DialOptions{HTTPHeader: http.Header{"Authorization": []string{"Bearer " + key[:len(key)-1] + "0"}}})
	if err == nil || resp == nil || resp.StatusCode != 401 {
		t.Fatalf("a wrong API key was accepted: %v %v", err, resp)
	}
	// Over HTTP the same rules apply.
	req, _ := http.NewRequest(http.MethodGet, httpURL(r, "/v1/services"), nil)
	httpResp, _ := http.DefaultClient.Do(req)
	var listed struct {
		Services []struct {
			Methods []struct {
				Name string `json:"name"`
			} `json:"methods"`
		} `json:"services"`
	}
	_ = json.NewDecoder(httpResp.Body).Decode(&listed)
	httpResp.Body.Close()
	if len(listed.Services) != 1 || len(listed.Services[0].Methods) != 1 || listed.Services[0].Methods[0].Name != "echo" {
		t.Fatalf("anonymous discovery over HTTP: %+v", listed)
	}
}

func TestReservedAnonymousEntryIsValidated(t *testing.T) {
	for _, bad := range []map[string]any{
		{"username": "anonymous", "password_hash": passwordHash(), "roles": []string{"client"}},
		{"username": "anonymous", "roles": []string{"server"}, "register": []string{"x.v1.Y"}},
		{"username": "anonymous", "roles": []string{"client", "server"}},
	} {
		data, _ := json.Marshal(map[string]any{"users": []map[string]any{bad}})
		if _, err := ParseAuth(data); err == nil || !strings.Contains(err.Error(), "anonymous") {
			t.Fatalf("accepted %v: %v", bad, err)
		}
	}
	// An anonymous-only configuration is valid: a public Router with rules.
	data, _ := json.Marshal(map[string]any{"users": []map[string]any{{"username": "anonymous", "roles": []string{"client"}, "call": []string{"*"}}}})
	if _, err := ParseAuth(data); err != nil {
		t.Fatal(err)
	}
	var _ = bytes.Equal
}

func TestBasicVerificationIsCachedOnSuccessOnly(t *testing.T) {
	auth := testAuth(t)
	req, _ := http.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", basicHeader("issuer", "test-only-password"))
	started := time.Now()
	if _, failure := auth.check(req); failure != nil {
		t.Fatal(failure.message)
	}
	first := time.Since(started)
	started = time.Now()
	if _, failure := auth.check(req); failure != nil {
		t.Fatal(failure.message)
	}
	if again := time.Since(started); again*10 > first {
		t.Fatalf("second check took %v after a first of %v: not cached", again, first)
	}
	wrong, _ := http.NewRequest(http.MethodGet, "/", nil)
	wrong.Header.Set("Authorization", basicHeader("issuer", "wrong-password"))
	for range 2 {
		if _, failure := auth.check(wrong); failure == nil {
			t.Fatal("wrong password accepted")
		}
	}
	auth.mu.Lock()
	entries := len(auth.verified)
	auth.mu.Unlock()
	if entries != 1 {
		t.Fatalf("failures must not be cached: %d entries", entries)
	}
}

func TestAnonymousConnectionsHaveTheirOwnLimit(t *testing.T) {
	auth := authFrom(t, map[string]any{"users": []map[string]any{
		{"username": "app", "password_hash": passwordHash(), "roles": []string{"client"}, "call": []string{"*"}},
	}})
	r, err := Start(Config{Auth: auth, MaxAnonymousConnections: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	first := dial(t, r, "")
	hello(t, first, "client", "anon-1")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, resp, err := websocket.Dial(ctx, fmt.Sprintf("ws://127.0.0.1:%d", r.Ready().Port), nil)
	if err == nil || resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("second anonymous connection: %v %v", err, resp)
	}
	// Credentialed clients are not affected.
	app := dial(t, r, basicHeader("app", "test-only-password"))
	hello(t, app, "client", "app")
}
