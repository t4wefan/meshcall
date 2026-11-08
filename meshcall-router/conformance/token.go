package conformance

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"

	"github.com/t4wefan/meshcall/meshcall-router/internal/router"
)

const authService = "meshcall.router.v1.AuthService"

// tokenLifetime covers issue #67's acceptance criteria: long TTLs up to the
// Router's maximum; expiry bounds new connections only (calls and
// registrations on live ones keep working); renew_token extends a token
// without reconnecting, for its issuer only; revoke_token still ends every
// connection, past expiry too; list_tokens shows expiry and live connections
// without secrets. Router-only: it runs its own Router with accounts, since
// only an account may issue tokens.
func tokenLifetime(h *harness) error {
	hash, err := router.HashPassword("conformance")
	if err != nil {
		return err
	}
	account := func(name string) map[string]any {
		return map[string]any{"username": name, "password_hash": hash, "roles": []string{"client", "server"},
			"register": []string{"*"}, "call": []string{"*"}}
	}
	doc, _ := json.Marshal(map[string]any{"users": []map[string]any{account("ctl"), account("other")}})
	auth, err := router.ParseAuth(doc)
	if err != nil {
		return err
	}
	r, err := router.Start(router.Config{Auth: auth})
	if err != nil {
		return err
	}
	defer r.Close()
	opts := h.opts
	opts.Local = false
	sub := &harness{ctx: h.ctx, opts: opts, port: r.Ready().Port}
	basic := func(name string) string {
		return "Basic " + base64.StdEncoding.EncodeToString([]byte(name+":conformance"))
	}
	ctl, err := sub.clientWith(basic("ctl"), "")
	if err != nil {
		return err
	}
	defer ctl.close()
	other, err := sub.clientWith(basic("other"), "")
	if err != nil {
		return err
	}
	defer other.close()

	const service = "conformance.token.v1.Probe"
	issue := func(ttl int) (token, id string, err error) {
		f, err := ctl.callService(authService, "issue_token", frame{"ttl_seconds": ttl,
			"scopes": []frame{{"service": service, "methods": []string{"*"}, "register": true}}}, nil)
		if err != nil {
			return "", "", err
		}
		if f["kind"] != "call.result" {
			return "", "", fmt.Errorf("issue_token(%d): %v", ttl, f)
		}
		payload := asMap(f["payload"])
		token, _ = payload["token"].(string)
		id, _ = payload["token_id"].(string)
		return token, id, nil
	}
	refusal := func(f frame) (string, any) {
		code, _, _, details := errorOf(f)
		return code, details["reason"]
	}

	// 1. Above an hour is accepted; above the maximum is invalid_argument.
	if _, _, err := issue(7 * 24 * 3600); err != nil {
		return err
	}
	f, err := ctl.callService(authService, "issue_token", frame{"ttl_seconds": 31 * 24 * 3600,
		"scopes": []frame{{"service": service, "methods": []string{"*"}}}}, nil)
	if err != nil {
		return err
	}
	if code, reason := refusal(f); code != "invalid_argument" || reason != "ttl_too_long" || asMap(asMap(f["error"])["details"])["max_ttl_seconds"] != float64(30*24*3600) {
		return fmt.Errorf("a TTL above the maximum: %v", f)
	}

	for method, payload := range map[string]frame{
		"issue_token": {"ttl_seconds": 0, "scopes": []frame{{"service": service, "methods": []string{"*"}}}},
		"renew_token": {"token_id": "x", "ttl_seconds": 0},
	} {
		f, err := ctl.callService(authService, method, payload, nil)
		if err != nil {
			return err
		}
		if code, reason := refusal(f); code != "invalid_argument" || reason != "ttl_invalid" {
			return fmt.Errorf("%s with ttl_seconds 0: %v", method, f)
		}
	}

	// Two one-second tokens: one runs past its expiry, the other is renewed.
	expiring, expiringID, err := issue(1)
	if err != nil {
		return err
	}
	renewed, renewedID, err := issue(1)
	if err != nil {
		return err
	}
	if f, err := other.callService(authService, "renew_token", frame{"token_id": renewedID, "ttl_seconds": 60}, nil); err != nil {
		return err
	} else if code, reason := refusal(f); code != "invalid_argument" || reason != "token_expired" {
		return fmt.Errorf("another issuer renewed the token: %v", f)
	}
	if f, err := ctl.callService(authService, "renew_token", frame{"token_id": renewedID, "ttl_seconds": 60}, nil); err != nil {
		return err
	} else if f["kind"] != "call.result" || asMap(f["payload"])["token_id"] != renewedID {
		return fmt.Errorf("renew_token: %v", f)
	}

	server, serverClosed, err := sub.rawServer("Bearer "+expiring, service, "probe-a")
	if err != nil {
		return err
	}
	defer server.CloseNow()
	caller, err := sub.clientWith("Bearer "+expiring, "")
	if err != nil {
		return err
	}
	defer caller.close()
	time.Sleep(1200 * time.Millisecond)

	// 2. Past expiry: no new connection, but calls and the registration work.
	if late, err := sub.clientWith("Bearer "+expiring, ""); err == nil {
		late.close()
		return fmt.Errorf("an expired token opened a new connection")
	}
	if f, err := caller.callService(service, "ping", frame{}, nil); err != nil || f["kind"] != "call.result" {
		return fmt.Errorf("a call on a live connection past expiry, to an instance registered with it: %v %v", err, f)
	}

	// 3. The renewed token connects past its old expiry.
	fresh, err := sub.clientWith("Bearer "+renewed, "")
	if err != nil {
		return fmt.Errorf("a renewed token past its old expiry: %w", err)
	}
	fresh.close()

	// 5. list_tokens: expiry and live connections, the expired token included, no secrets.
	f, err = ctl.callService(authService, "list_tokens", frame{}, nil)
	if err != nil {
		return err
	}
	if raw, _ := json.Marshal(f); strings.Contains(string(raw), expiring) || strings.Contains(string(raw), renewed) {
		return fmt.Errorf("list_tokens returned a secret")
	}
	found := false
	for _, entry := range asList(asMap(f["payload"])["tokens"]) {
		if e := asMap(entry); e["token_id"] == expiringID {
			found = true
			if e["expired"] != true || e["connections"] != float64(2) || e["issuer"] != "ctl" {
				return fmt.Errorf("list_tokens entry for the expired token: %v", e)
			}
		}
	}
	if !found {
		return fmt.Errorf("list_tokens omits an expired token with live connections: %v", f)
	}
	if f, err := ctl.callService(authService, "renew_token", frame{"token_id": expiringID, "ttl_seconds": 60}, nil); err != nil {
		return err
	} else if code, reason := refusal(f); code != "invalid_argument" || reason != "token_expired" {
		return fmt.Errorf("renewing an expired token: %v", f)
	}

	// 4. Revoking ends both connections at once, past expiry.
	if f, err := ctl.callService(authService, "revoke_token", frame{"token_id": expiringID}, nil); err != nil || asMap(f["payload"])["revoked"] != true {
		return fmt.Errorf("revoke_token: %v %v", err, f)
	}
	for {
		if _, err := caller.receiveWithin(h.opts.Timeout); err != nil {
			if err == errNoFrame {
				return fmt.Errorf("a revoked token's client connection stayed open")
			}
			// 4401 token_revoked tells revocation apart from a network loss.
			var closed websocket.CloseError
			if !errors.As(err, &closed) || closed.Code != router.StatusUnauthenticated || closed.Reason != "token_revoked" {
				return fmt.Errorf("a revoked token's connection closed with %v, want 4401 token_revoked", err)
			}
			break
		}
	}
	select {
	case <-serverClosed:
		return nil
	case <-time.After(h.opts.Timeout):
		return fmt.Errorf("a revoked token's server connection stayed open")
	}
}

// rawServer registers one unary method, ping, and answers every call to it
// until the connection closes, which closes the returned channel.
func (h *harness) rawServer(authorization, service, instanceID string) (*websocket.Conn, <-chan struct{}, error) {
	ctx, cancel := context.WithTimeout(h.ctx, h.opts.Timeout)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, fmt.Sprintf("ws://127.0.0.1:%d", h.port),
		&websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {authorization}}})
	if err != nil {
		return nil, nil, err
	}
	for _, f := range []string{
		`{"kind":"hello","protocol":"` + protocol + `","role":"server","peer_id":"token-probe","instance_id":"` + instanceID + `"}`,
		`{"kind":"server.register","instance_id":"` + instanceID + `","services":[{"name":"` + service + `","methods":[{"name":"ping","stream":"unary","balance":{"kind":"round_robin"}}]}]}`,
	} {
		if err := conn.Write(ctx, websocket.MessageText, []byte(f)); err != nil {
			conn.CloseNow()
			return nil, nil, err
		}
	}
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			conn.CloseNow()
			return nil, nil, err
		}
		if strings.Contains(string(data), "server.register.ack") {
			break
		}
	}
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		for {
			_, data, err := conn.Read(h.ctx)
			if err != nil {
				return
			}
			var f frame
			if json.Unmarshal(data, &f) != nil || f["kind"] != "call.open" {
				continue
			}
			reply, _ := json.Marshal(frame{"kind": "call.result", "call_id": f["call_id"], "payload": frame{"pong": true}})
			_ = conn.Write(h.ctx, websocket.MessageText, reply)
		}
	}()
	return conn, closed, nil
}
