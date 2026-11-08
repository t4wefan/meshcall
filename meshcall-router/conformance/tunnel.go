package conformance

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"

	"github.com/t4wefan/meshcall/meshcall-router/internal/router"
)

// Tunnel scenarios (docs/protocol.md → Tunnels), one per acceptance
// criterion of the tunnel feature. The fixture opts both its services in to
// tunnels, refuses offers whose principal is "refuse:me", reports ctx.tunnel
// through `tunnel_info` and the close notices it got through
// `closed_tunnels`.

const (
	tunnelService   = "meshcall.router.v1.TunnelService"
	targetedName    = "conformance.v1.Targeted"
	registryName    = "meshcall.router.v1.Registry"
	tunnelIDKey     = "meshcall-tunnel-id"
	tunnelPrinKey   = "meshcall-tunnel-principal"
	refusePrincipal = "refuse:me"
)

type tunnelCredential struct {
	Credential string `json:"credential"`
	TunnelID   string `json:"tunnel_id"`
}

// callService is open for any service, returning the terminal frame.
func (c *client) callService(service, method string, payload any, extra frame) (frame, error) {
	merged := frame{"service": service}
	for k, v := range extra {
		merged[k] = v
	}
	return c.unary(method, payload, merged)
}

// openTunnel opens a tunnel to the fixture from an ordinary (issuer) client.
func (h *harness) openTunnel(issuer *client, principal string, ttl int, scopes []frame) (tunnelCredential, frame, error) {
	f, err := issuer.callService(tunnelService, "open", frame{"instance_id": InstanceID, "principal": principal, "ttl_seconds": ttl, "scopes": scopes}, nil)
	if err != nil {
		return tunnelCredential{}, nil, err
	}
	var opened tunnelCredential
	if f["kind"] == "call.result" {
		data, _ := json.Marshal(f["payload"])
		_ = json.Unmarshal(data, &opened)
		if !strings.HasPrefix(opened.Credential, "mtun_") || opened.TunnelID == "" {
			return opened, f, fmt.Errorf("open returned no credential: %v", f)
		}
	}
	return opened, f, nil
}

func expectReason(f frame, code, reason string) error {
	got, _, _, details := errorOf(f)
	if f["kind"] != "call.error" || got != code || details["reason"] != reason {
		return fmt.Errorf("expected %s / %s, got %v", code, reason, f)
	}
	return nil
}

func tunnelConsent(h *harness) error {
	issuer, err := h.client()
	if err != nil {
		return err
	}
	defer issuer.close()
	scopes := []frame{{"service": Service, "methods": []string{"echo"}}}
	if _, refused, err := h.openTunnel(issuer, refusePrincipal, 60, scopes); err != nil {
		return err
	} else if err := expectReason(refused, "permission_denied", "tunnel_rejected"); err != nil {
		return fmt.Errorf("the instance's refusal: %w", err)
	}
	opened, f, err := h.openTunnel(issuer, "user:alice", 60, scopes)
	if err != nil {
		return err
	}
	if f["kind"] != "call.result" {
		return fmt.Errorf("an accepted offer: %v", f)
	}
	_, err = issuer.callService(tunnelService, "revoke", frame{"tunnel_id": opened.TunnelID}, nil)
	return err
}

// tunnelScope: with only the credential, every allowed shape reaches the
// target without naming it — from a browser-style hello too — and nothing
// else is reachable, the Registry and OpenAPI included.
func tunnelScope(h *harness) error {
	issuer, err := h.client()
	if err != nil {
		return err
	}
	defer issuer.close()
	opened, _, err := h.openTunnel(issuer, "user:alice", 60, []frame{
		{"service": Service, "methods": []string{"echo", "count", "sum", "chat", "tunnel_info"}},
		{"service": targetedName, "methods": []string{"whoami"}},
	})
	if err != nil {
		return err
	}
	defer issuer.callService(tunnelService, "revoke", frame{"tunnel_id": opened.TunnelID}, nil) //nolint:errcheck
	c, err := h.clientWith("", "Bearer "+opened.Credential)                                     // credentials in hello, as a browser sends them
	if err != nil {
		return err
	}
	defer c.close()

	if f, err := c.unary("echo", frame{"value": 1, "text": "t"}, nil); err != nil || f["kind"] != "call.result" {
		return fmt.Errorf("unary without an instance: %v %v", err, f)
	}
	// A targeted service through a tunnel needs no instance_id: it is implied.
	if f, err := c.callService(targetedName, "whoami", frame{}, nil); err != nil || asMap(f["payload"])["instance"] != InstanceID {
		return fmt.Errorf("targeted call with an implied instance: %v %v", err, f)
	}
	// Server stream.
	id, err := c.open("count", frame{"n": 3}, nil)
	if err != nil {
		return err
	}
	if err := c.send(frame{"kind": "stream.window", "call_id": id, "direction": "server", "credit": 8}); err != nil {
		return err
	}
	items := 0
	for done := false; !done; {
		f, err := c.receive()
		if err != nil {
			return fmt.Errorf("server stream: %w", err)
		}
		switch f["kind"] {
		case "stream.item":
			items++
		case "call.result":
			done = true
		case "call.error":
			return fmt.Errorf("server stream: %v", f)
		}
	}
	if items != 3 {
		return fmt.Errorf("server stream items: %d", items)
	}
	// Client stream.
	if id, err = c.open("sum", frame{"offset": 1}, nil); err != nil {
		return err
	}
	if _, err := c.receive(); err != nil { // the initial input window
		return err
	}
	for i := 0; i < 3; i++ {
		if err := c.send(frame{"kind": "stream.item", "call_id": id, "direction": "client", "sequence": i, "payload": frame{"value": 2}}); err != nil {
			return err
		}
	}
	if err := c.send(frame{"kind": "stream.end", "call_id": id, "direction": "client"}); err != nil {
		return err
	}
	for {
		f, err := c.nextData(id, h.opts.Timeout)
		if err != nil {
			return fmt.Errorf("client stream: %w", err)
		}
		if f["kind"] == "call.result" {
			if number(asMap(f["payload"])["total"]) != 7 {
				return fmt.Errorf("client stream total: %v", f)
			}
			break
		}
	}
	// Duplex.
	if id, err = c.openChat("chat", frame{"prefix": "> "}, nil); err != nil {
		return err
	}
	if err := c.send(frame{"kind": "stream.window", "call_id": id, "direction": "server", "credit": 8}); err != nil {
		return err
	}
	if err := c.input(id, 0, "ls"); err != nil {
		return err
	}
	if err := c.expectOutput(id, 0, "> ls"); err != nil {
		return fmt.Errorf("duplex through a tunnel: %w", err)
	}
	if err := c.send(frame{"kind": "stream.end", "call_id": id, "direction": "client"}); err != nil {
		return err
	}
	if err := c.expectOutput(id, 1, "done:1"); err != nil {
		return err
	}
	if err := c.expectEnd(id); err != nil {
		return err
	}

	for _, probe := range []struct {
		what            string
		service, method string
		extra           frame
	}{
		{"another instance", Service, "echo", frame{"instance_id": "fixture-2"}},
		{"a method outside the scopes", Service, "bad_response", nil},
		{"a service that does not exist", "nope.v1.Nope", "x", nil},
		{"the tunnel API", tunnelService, "list", nil},
	} {
		f, err := c.callService(probe.service, probe.method, frame{"value": 1, "text": "t"}, probe.extra)
		if err != nil {
			return err
		}
		if err := expectReason(f, "permission_denied", "tunnel_scope"); err != nil {
			return fmt.Errorf("%s: %w", probe.what, err)
		}
	}
	// Naming the target itself is fine.
	if f, err := c.unary("echo", frame{"value": 1, "text": "t"}, frame{"instance_id": InstanceID}); err != nil || f["kind"] != "call.result" {
		return fmt.Errorf("naming the target: %v %v", err, f)
	}

	f, err := c.callService(registryName, "list", frame{}, nil)
	if err != nil {
		return err
	}
	var listed struct {
		Services []struct {
			Name      string `json:"name"`
			Instances []struct {
				ID string `json:"id"`
			} `json:"instances"`
			Methods []struct {
				Name string `json:"name"`
			} `json:"methods"`
		} `json:"services"`
	}
	data, _ := json.Marshal(f["payload"])
	_ = json.Unmarshal(data, &listed)
	if len(listed.Services) != 2 {
		return fmt.Errorf("registry view: %s", data)
	}
	for _, s := range listed.Services {
		if len(s.Instances) != 1 || s.Instances[0].ID != InstanceID {
			return fmt.Errorf("registry view instances: %s", data)
		}
		for _, m := range s.Methods {
			if m.Name == "bad_response" {
				return fmt.Errorf("registry view shows a method outside the scopes: %s", data)
			}
		}
	}

	// HTTP with the tunnel as Bearer: same scoping, OpenAPI included.
	resp, err := h.httpCall(h.ctx, "echo", `{"value": 2, "text": "h"}`, map[string]string{"Authorization": "Bearer " + opened.Credential})
	if err != nil {
		return err
	}
	if body, err := readJSON(resp); err != nil || resp.StatusCode != 200 || number(body["value"]) != 2 {
		return fmt.Errorf("HTTP through a tunnel: %d %v %v", resp.StatusCode, body, err)
	}
	if resp, err = h.httpCall(h.ctx, "bad_response", `{}`, map[string]string{"Authorization": "Bearer " + opened.Credential}); err != nil {
		return err
	} else if resp.Body.Close(); resp.StatusCode != 403 {
		return fmt.Errorf("HTTP outside the scopes: %d", resp.StatusCode)
	}
	doc, err := h.httpGet("/v1/openapi.json", "Bearer "+opened.Credential)
	if err != nil {
		return err
	}
	if !strings.Contains(doc, "/v1/call/"+Service+"/echo") || strings.Contains(doc, "bad_response") {
		return fmt.Errorf("OpenAPI view: %.400s", doc)
	}

	// A tunnel credential never registers.
	server, err := h.dialRaw("Bearer " + opened.Credential)
	if err != nil {
		return err
	}
	defer server.CloseNow()
	_ = server.Write(h.ctx, websocket.MessageText, []byte(`{"kind":"hello","protocol":"meshcall/1","role":"server","peer_id":"x","instance_id":"`+InstanceID+`"}`))
	ctx, cancel := context.WithTimeout(h.ctx, h.opts.Timeout)
	defer cancel()
	if _, _, err := server.Read(ctx); websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
		return fmt.Errorf("registering with a tunnel credential: %v", err)
	}
	return nil
}

// tunnelIdentity: the instance sees the id and principal the issuer set; no
// client can forge them, on the tunnel or off it.
func tunnelIdentity(h *harness) error {
	issuer, err := h.client()
	if err != nil {
		return err
	}
	defer issuer.close()
	opened, _, err := h.openTunnel(issuer, "user:alice", 60, []frame{{"service": Service, "methods": []string{"tunnel_info", "metadata_echo"}}})
	if err != nil {
		return err
	}
	defer issuer.callService(tunnelService, "revoke", frame{"tunnel_id": opened.TunnelID}, nil) //nolint:errcheck
	tunnel, err := h.clientWith("Bearer "+opened.Credential, "")
	if err != nil {
		return err
	}
	defer tunnel.close()
	forged := frame{"metadata": map[string]string{tunnelIDKey: "tun_forged", tunnelPrinKey: "user:mallory", "meshcall-tunnel-extra": "x"}}
	f, err := tunnel.unary("tunnel_info", frame{}, forged)
	if err != nil {
		return err
	}
	if seen := asMap(f["payload"]); seen["tunnel_id"] != opened.TunnelID || seen["principal"] != "user:alice" {
		return fmt.Errorf("ctx.tunnel on the tunnel: %v", f)
	}
	f, err = tunnel.unary("metadata_echo", frame{}, forged)
	if err != nil {
		return err
	}
	if md := asMap(asMap(f["payload"])["metadata"]); md[tunnelPrinKey] != "user:alice" || md["meshcall-tunnel-extra"] != nil {
		return fmt.Errorf("metadata on the tunnel: %v", f)
	}
	// Off the tunnel: the same keys, even with the real id, are stripped.
	plain, err := h.client()
	if err != nil {
		return err
	}
	defer plain.close()
	spoof := frame{"metadata": map[string]string{tunnelIDKey: opened.TunnelID, tunnelPrinKey: "user:alice"}}
	if f, err = plain.unary("tunnel_info", frame{}, spoof); err != nil {
		return err
	}
	if seen := asMap(f["payload"]); seen["tunnel_id"] != nil || seen["principal"] != nil {
		return fmt.Errorf("a forged ctx.tunnel from an ordinary client: %v", f)
	}
	if f, err = plain.unary("metadata_echo", frame{}, spoof); err != nil {
		return err
	}
	if md := asMap(asMap(f["payload"])["metadata"]); len(md) != 0 {
		return fmt.Errorf("forged tunnel metadata reached the instance: %v", f)
	}
	return nil
}

// tunnelLifetime: expiry refuses new connections but keeps live ones;
// revoking ends them at once and the instance is told.
func tunnelLifetime(h *harness) error {
	issuer, err := h.client()
	if err != nil {
		return err
	}
	defer issuer.close()
	opened, _, err := h.openTunnel(issuer, "user:alice", 1, []frame{{"service": Service, "methods": []string{"echo"}}})
	if err != nil {
		return err
	}
	live, err := h.clientWith("Bearer "+opened.Credential, "")
	if err != nil {
		return err
	}
	defer live.close()
	time.Sleep(1200 * time.Millisecond)
	if late, err := h.clientWith("Bearer "+opened.Credential, ""); err == nil {
		late.close()
		return fmt.Errorf("an expired credential opened a new connection")
	}
	if f, err := live.unary("echo", frame{"value": 1, "text": "still"}, nil); err != nil || f["kind"] != "call.result" {
		return fmt.Errorf("a live connection after expiry: %v %v", err, f)
	}
	if f, err := issuer.callService(tunnelService, "revoke", frame{"tunnel_id": opened.TunnelID}, nil); err != nil || asMap(f["payload"])["revoked"] != true {
		return fmt.Errorf("revoke: %v %v", err, f)
	}
	for {
		if _, err := live.receiveWithin(h.opts.Timeout); err != nil {
			if err == errNoFrame {
				return fmt.Errorf("a revoked tunnel's connection stayed open")
			}
			break
		}
	}
	deadline := time.Now().Add(h.opts.Timeout)
	for {
		f, err := issuer.unary("closed_tunnels", frame{}, nil)
		if err != nil {
			return err
		}
		for _, entry := range asList(asMap(f["payload"])["closed"]) {
			if e := asMap(entry); e["tunnel_id"] == opened.TunnelID && e["reason"] == "revoked" {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the instance was not told the tunnel closed: %v", f)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// tunnelReconnect: while the instance is away calls fail with a retryable
// error; once it re-registers and accepts the re-offer they resume.
func tunnelReconnect(h *harness) error {
	issuer, err := h.client()
	if err != nil {
		return err
	}
	defer issuer.close()
	opened, _, err := h.openTunnel(issuer, "user:alice", 60, []frame{{"service": Service, "methods": []string{"echo", "tunnel_info"}}})
	if err != nil {
		return err
	}
	defer issuer.callService(tunnelService, "revoke", frame{"tunnel_id": opened.TunnelID}, nil) //nolint:errcheck
	c, err := h.clientWith("Bearer "+opened.Credential, "")
	if err != nil {
		return err
	}
	defer c.close()
	if !h.router.DropInstance(InstanceID) {
		return fmt.Errorf("the fixture is not registered")
	}
	if err := h.waitUnregistered(h.opts.Timeout); err != nil {
		// It may already be back; the offline window is then too short to see.
		_ = err
	} else if f, err := c.unary("echo", frame{"value": 1, "text": "away"}, nil); err != nil {
		return err
	} else if f["kind"] == "call.error" {
		if _, _, retryable, _ := errorOf(f); !retryable {
			return fmt.Errorf("a call while the instance is away must be retryable: %v", f)
		}
	}
	if err := h.waitRegistered(15 * time.Second); err != nil {
		return fmt.Errorf("the fixture did not reconnect: %w", err)
	}
	deadline := time.Now().Add(h.opts.Timeout)
	for {
		f, err := c.unary("tunnel_info", frame{}, nil)
		if err != nil {
			return err
		}
		if f["kind"] == "call.result" && asMap(f["payload"])["principal"] == "user:alice" {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("calls did not resume after the re-offer: %v", f)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// tunnelBoundRegistration: a credential bound to instance A cannot register
// as instance B. Router-only; it runs its own Router with accounts.
func tunnelBoundRegistration(h *harness) error {
	hash, err := router.HashPassword("conformance")
	if err != nil {
		return err
	}
	doc, _ := json.Marshal(map[string]any{"users": []map[string]any{{
		"username": "box", "password_hash": hash, "roles": []string{"server"},
		"register": []string{Service}, "instance_ids": []string{"box-a"},
	}}})
	auth, err := router.ParseAuth(doc)
	if err != nil {
		return err
	}
	r, err := router.Start(router.Config{Auth: auth})
	if err != nil {
		return err
	}
	defer r.Close()
	url := fmt.Sprintf("ws://127.0.0.1:%d", r.Ready().Port)
	header := http.Header{"Authorization": {"Basic " + base64.StdEncoding.EncodeToString([]byte("box:conformance"))}}
	register := func(id string) error {
		ctx, cancel := context.WithTimeout(h.ctx, h.opts.Timeout)
		defer cancel()
		conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: header})
		if err != nil {
			return err
		}
		defer conn.CloseNow()
		for _, f := range []string{
			`{"kind":"hello","protocol":"meshcall/1","role":"server","peer_id":"x","instance_id":"` + id + `"}`,
			`{"kind":"server.register","instance_id":"` + id + `","services":[{"name":"` + Service + `","methods":[{"name":"echo","stream":"unary","balance":{"kind":"round_robin"}}]}]}`,
		} {
			if err := conn.Write(ctx, websocket.MessageText, []byte(f)); err != nil {
				return err
			}
		}
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return err
			}
			if strings.Contains(string(data), "server.register.ack") {
				return nil
			}
		}
	}
	if err := register("box-b"); websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
		return fmt.Errorf("registering under a foreign instance id: %v", err)
	}
	if err := register("box-a"); err != nil {
		return fmt.Errorf("registering under its own instance id: %w", err)
	}
	return nil
}

func (h *harness) dialRaw(authorization string) (*websocket.Conn, error) {
	ctx, cancel := context.WithTimeout(h.ctx, h.opts.Timeout)
	defer cancel()
	endpoint := h.endpoint()
	conn, resp, err := websocket.Dial(ctx, endpoint.URL, &websocket.DialOptions{
		HTTPClient: endpoint.HTTPClient(), HTTPHeader: http.Header{"Authorization": {authorization}},
	})
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	return conn, err
}

func (h *harness) httpGet(path, authorization string) (string, error) {
	base := fmt.Sprintf("http://127.0.0.1:%d", h.port)
	endpoint := h.endpoint()
	if endpoint.Dial != nil {
		base = "http://localhost"
	}
	req, err := http.NewRequestWithContext(h.ctx, http.MethodGet, base+path, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", authorization)
	resp, err := endpoint.HTTPClient().Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return string(body), err
}

func asMap(v any) map[string]any { m, _ := v.(map[string]any); return m }
func asList(v any) []any         { l, _ := v.([]any); return l }
