package router

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/coder/websocket"
)

func TestRegistryShowsOnlyWhatTheCallerMayCall(t *testing.T) {
	config := map[string]any{"users": []map[string]any{
		{"username": "worker", "password_hash": passwordHash(), "roles": []string{"server"}, "register": []string{"*"}},
		{"username": "viewer", "password_hash": passwordHash(), "roles": []string{"client"},
			"call": []string{RegistryService + "/*", "public.v1.Docs/read"}},
	}}
	data, _ := json.Marshal(config)
	path := filepath.Join(t.TempDir(), "auth.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	auth, err := LoadAuth(path)
	if err != nil {
		t.Fatal(err)
	}
	r := runningRouter(t, auth)
	contractOf := func(name string, methods ...string) json.RawMessage {
		entries := make([]map[string]any, 0, len(methods))
		for _, m := range methods {
			entries = append(entries, map[string]any{"name": m, "description": m + " does things"})
		}
		raw, _ := json.Marshal(map[string]any{"name": name, "version": "1", "methods": entries})
		return raw
	}
	register := func(id, name string, methods ...string) {
		conn := dial(t, r, basicHeader("worker", "test-only-password"))
		hello(t, conn, "server", id)
		ms := make([]method, 0, len(methods))
		for _, m := range methods {
			ms = append(ms, method{Name: m, Stream: "unary", Balance: balance{Kind: "round_robin"}})
		}
		send(t, conn, frame{Kind: "server.register", InstanceID: id, Services: []service{{Name: name, Methods: ms, Contract: contractOf(name, methods...)}}})
		if got := receive(t, conn); got.Kind != "server.register.ack" {
			t.Fatalf("register %s: %+v", name, got)
		}
	}
	register("docs", "public.v1.Docs", "read", "write")
	register("billing", "billing.v1.Invoices", "list")

	viewer := dial(t, r, basicHeader("viewer", "test-only-password"))
	hello(t, viewer, "client", "viewer")
	call := func(id, method, payload string) frame {
		send(t, viewer, frame{Kind: "call.open", CallID: id, Service: RegistryService, Method: method, Payload: json.RawMessage(payload)})
		return receive(t, viewer)
	}

	var listed struct {
		Services []struct {
			Name    string `json:"name"`
			Methods []struct {
				Name string `json:"name"`
			} `json:"methods"`
		} `json:"services"`
	}
	if err := json.Unmarshal(call("l", "list", `{}`).Payload, &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed.Services) != 1 || listed.Services[0].Name != "public.v1.Docs" ||
		len(listed.Services[0].Methods) != 1 || listed.Services[0].Methods[0].Name != "read" {
		t.Fatalf("list leaked services or methods: %+v", listed)
	}

	described := call("d", "describe", `{"service":"public.v1.Docs"}`)
	var withContract struct {
		Contract struct {
			Methods []struct {
				Name string `json:"name"`
			} `json:"methods"`
		} `json:"contract"`
	}
	_ = json.Unmarshal(described.Payload, &withContract)
	if len(withContract.Contract.Methods) != 1 || withContract.Contract.Methods[0].Name != "read" {
		t.Fatalf("describe leaked contract entries: %s", described.Payload)
	}

	// A forbidden service and a missing one are indistinguishable.
	forbidden := call("f", "describe", `{"service":"billing.v1.Invoices"}`)
	missing := call("m", "describe", `{"service":"nothing.v1.Here"}`)
	normalise := func(f frame, name string) []byte {
		f.CallID, f.raw = "", nil
		return bytes.ReplaceAll(f.bytes(), []byte(name), []byte("SERVICE"))
	}
	if forbidden.Error == nil || !bytes.Equal(normalise(forbidden, "billing.v1.Invoices"), normalise(missing, "nothing.v1.Here")) {
		t.Fatalf("forbidden and missing differ:\n%s\n%s", forbidden.bytes(), missing.bytes())
	}

	// Calls stay symmetric too: permission is checked before existence.
	send(t, viewer, frame{Kind: "call.open", CallID: "c1", Service: "billing.v1.Invoices", Method: "list", Payload: json.RawMessage(`{}`)})
	a := receive(t, viewer)
	send(t, viewer, frame{Kind: "call.open", CallID: "c2", Service: "nothing.v1.Here", Method: "list", Payload: json.RawMessage(`{}`)})
	b := receive(t, viewer)
	if a.Error == nil || b.Error == nil || a.Error.Code != "permission_denied" || a.Error.Code != b.Error.Code || a.Error.Message != b.Error.Message {
		t.Fatalf("calls differ: %+v / %+v", a.Error, b.Error)
	}
	var _ *websocket.Conn
}
