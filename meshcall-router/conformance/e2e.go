package conformance

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// The harness side of end-to-end sealed tunnels (docs/protocol.md →
// End-to-end encryption): Noise_NK_25519_AESGCM_SHA256 and sealed frames,
// written from the protocol text alone so every runtime is checked against
// the document rather than against another runtime.

var b64 = base64.RawURLEncoding

const (
	tunnelHostName = "meshcall.router.v1.TunnelHost"
	e2ePrologue    = "meshcall-e2e/1"
)

type noise struct {
	h, ck []byte
	k     cipher.AEAD
	n     uint64
}

func newNoise(prologue []byte) *noise {
	name := make([]byte, 32) // "Noise_NK_25519_AESGCM_SHA256" is shorter than HASHLEN
	copy(name, "Noise_NK_25519_AESGCM_SHA256")
	s := &noise{h: name, ck: name}
	s.mixHash(prologue)
	return s
}

func (s *noise) mixHash(data []byte) {
	sum := sha256.Sum256(append(append([]byte{}, s.h...), data...))
	s.h = sum[:]
}

func hkdf2(ck, ikm []byte) ([]byte, []byte) {
	mac := func(key, data []byte) []byte {
		m := hmac.New(sha256.New, key)
		m.Write(data)
		return m.Sum(nil)
	}
	temp := mac(ck, ikm)
	a := mac(temp, []byte{1})
	return a, mac(temp, append(append([]byte{}, a...), 2))
}

func aesGCM(key []byte) cipher.AEAD {
	block, _ := aes.NewCipher(key)
	aead, _ := cipher.NewGCM(block)
	return aead
}

func noiseNonce(n uint64) []byte {
	nonce := make([]byte, 12)
	binary.BigEndian.PutUint64(nonce[4:], n)
	return nonce
}

func (s *noise) mixKey(ikm []byte) {
	var k []byte
	s.ck, k = hkdf2(s.ck, ikm)
	s.k, s.n = aesGCM(k), 0
}

func (s *noise) encryptAndHash(plaintext []byte) []byte {
	out := plaintext
	if s.k != nil {
		out = s.k.Seal(nil, noiseNonce(s.n), plaintext, s.h)
		s.n++
	}
	s.mixHash(out)
	return out
}

func (s *noise) decryptAndHash(ciphertext []byte) ([]byte, error) {
	out := ciphertext
	if s.k != nil {
		var err error
		if out, err = s.k.Open(nil, noiseNonce(s.n), ciphertext, s.h); err != nil {
			return nil, err
		}
		s.n++
	}
	s.mixHash(ciphertext)
	return out, nil
}

// nkInitiator is the client side: message 1 towards the pinned key.
type nkInitiator struct {
	sym *noise
	e   *ecdh.PrivateKey
}

func nkInitiate(remote *ecdh.PublicKey, ephemeral *ecdh.PrivateKey, prologue, payload []byte) (*nkInitiator, []byte, error) {
	sym := newNoise(prologue)
	sym.mixHash(remote.Bytes())
	if ephemeral == nil {
		var err error
		if ephemeral, err = ecdh.X25519().GenerateKey(rand.Reader); err != nil {
			return nil, nil, err
		}
	}
	sym.mixHash(ephemeral.PublicKey().Bytes())
	shared, err := ephemeral.ECDH(remote)
	if err != nil {
		return nil, nil, err
	}
	sym.mixKey(shared)
	message := append(ephemeral.PublicKey().Bytes(), sym.encryptAndHash(payload)...)
	return &nkInitiator{sym: sym, e: ephemeral}, message, nil
}

func (i *nkInitiator) finish(message []byte) ([]byte, *e2eSession, error) {
	if len(message) < 48 {
		return nil, nil, errors.New("short handshake message")
	}
	re, err := ecdh.X25519().NewPublicKey(message[:32])
	if err != nil {
		return nil, nil, err
	}
	i.sym.mixHash(message[:32])
	shared, err := i.e.ECDH(re)
	if err != nil {
		return nil, nil, err
	}
	i.sym.mixKey(shared)
	payload, err := i.sym.decryptAndHash(message[32:])
	if err != nil {
		return nil, nil, err
	}
	send, recv := hkdf2(i.sym.ck, nil)
	return payload, &e2eSession{hash: i.sym.h, id: hex.EncodeToString(i.sym.h[:16]), send: aesGCM(send), recv: aesGCM(recv)}, nil
}

// e2eSession is the client end of a session: sealing for the fixture,
// opening what it seals.
type e2eSession struct {
	hash       []byte
	id         string
	send, recv cipher.AEAD
	sendN      uint64
}

func sequenceText(v any) string {
	switch n := v.(type) {
	case nil:
		return ""
	case int:
		return strconv.Itoa(n)
	case float64:
		return strconv.FormatInt(int64(n), 10)
	}
	return fmt.Sprint(v)
}

func (s *e2eSession) ad(f frame, service, method string) []byte {
	direction, _ := f["direction"].(string)
	callID, _ := f["call_id"].(string)
	kind, _ := f["kind"].(string)
	return []byte(strings.Join([]string{e2ePrologue, kind, callID, service, method, direction, sequenceText(f["sequence"]), s.id}, "|"))
}

// seal moves a call.open's or stream.item's content into its sealed field.
func (s *e2eSession) seal(f frame, service, method string) frame {
	out := frame{}
	for k, v := range f {
		out[k] = v
	}
	parts := map[string]any{"payload": f["payload"]}
	if md, ok := f["metadata"]; ok {
		parts["metadata"] = md
	}
	out["payload"] = nil
	delete(out, "metadata")
	if f["kind"] == "call.open" {
		out["e2e_session"] = s.id
	}
	plaintext, _ := json.Marshal(parts)
	n := s.sendN
	s.sendN++
	counter := binary.BigEndian.AppendUint64(nil, n)
	out["sealed"] = b64.EncodeToString(s.send.Seal(counter, noiseNonce(n), plaintext, s.ad(out, service, method)))
	return out
}

// open restores what the fixture sealed; content in clear is refused.
func (s *e2eSession) open(f frame, service, method string) (frame, error) {
	sealed, _ := f["sealed"].(string)
	if sealed == "" {
		switch f["kind"] {
		case "call.result", "stream.item":
			return nil, fmt.Errorf("a %v of a sealed call arrived in clear: %v", f["kind"], f)
		}
		return f, nil
	}
	raw, err := b64.DecodeString(sealed)
	if err != nil || len(raw) < 24 {
		return nil, fmt.Errorf("malformed sealed field %q", sealed)
	}
	plaintext, err := s.recv.Open(nil, noiseNonce(binary.BigEndian.Uint64(raw)), raw[8:], s.ad(f, service, method))
	if err != nil {
		return nil, fmt.Errorf("the sealed %v does not open: %w", f["kind"], err)
	}
	var parts map[string]any
	if err := json.Unmarshal(plaintext, &parts); err != nil {
		return nil, fmt.Errorf("sealed content is not an object: %s", plaintext)
	}
	out := frame{}
	for k, v := range f {
		out[k] = v
	}
	delete(out, "sealed")
	if f["kind"] == "call.error" {
		e := map[string]any{}
		for k, v := range asMap(f["error"]) {
			e[k] = v
		}
		e["message"], e["details"] = parts["message"], parts["details"]
		out["error"] = e
	} else {
		out["payload"] = parts["payload"]
	}
	if md, ok := parts["metadata"]; ok {
		out["metadata"] = md
	} else {
		delete(out, "metadata")
	}
	return out, nil
}

// e2eHandshake runs TunnelHost/handshake towards pinned on a tunnel
// connection; on refusal it returns the error frame instead of a session.
func (c *client) e2eHandshake(pinned *ecdh.PublicKey) (*e2eSession, frame, error) {
	initiator, message, err := nkInitiate(pinned, nil, []byte(e2ePrologue), nil)
	if err != nil {
		return nil, nil, err
	}
	f, err := c.callService(tunnelHostName, "handshake", frame{"message": b64.EncodeToString(message)}, nil)
	if err != nil {
		return nil, nil, err
	}
	if f["kind"] != "call.result" {
		return nil, f, nil
	}
	reply, err := b64.DecodeString(fmt.Sprint(asMap(f["payload"])["message"]))
	if err != nil {
		return nil, nil, fmt.Errorf("handshake answer: %w", err)
	}
	if _, session, err := initiator.finish(reply); err != nil {
		return nil, nil, fmt.Errorf("message 2 does not come from the pinned key: %w", err)
	} else {
		return session, f, nil
	}
}

// sealedOpen opens a sealed call on the fixture; it returns the call id.
func (c *client) sealedOpen(s *e2eSession, method string, payload any, metadata map[string]string) (string, error) {
	id := c.callID()
	f := frame{"kind": "call.open", "call_id": id, "service": Service, "method": method, "payload": payload}
	if metadata != nil {
		f["metadata"] = metadata
	}
	return id, c.send(s.seal(f, Service, method))
}

// receiveCall returns the next frame of call id, opened, and the frame as
// the Router relayed it.
func (c *client) receiveCall(s *e2eSession, id, method string) (opened, relayed frame, err error) {
	for {
		f, err := c.receive()
		if err != nil {
			return nil, nil, err
		}
		if f["call_id"] != id {
			continue
		}
		opened, err := s.open(f, Service, method)
		return opened, f, err
	}
}

// inClear reports whether a secret travelled through the Router readable.
func inClear(f frame, secrets ...string) error {
	data, _ := json.Marshal(f)
	for _, secret := range secrets {
		if bytes.Contains(data, []byte(secret)) {
			return fmt.Errorf("the Router relayed %q in clear: %s", secret, data)
		}
	}
	return nil
}

func (h *harness) openE2ETunnel(issuer *client, key *ecdh.PublicKey, methods ...string) (tunnelCredential, frame, error) {
	f, err := issuer.callService(tunnelService, "open", frame{
		"instance_id": InstanceID, "principal": "user:alice", "ttl_seconds": 60,
		"scopes":  []frame{{"service": Service, "methods": methods}},
		"e2e_key": b64.EncodeToString(key.Bytes()),
	}, nil)
	if err != nil {
		return tunnelCredential{}, nil, err
	}
	var opened tunnelCredential
	if f["kind"] == "call.result" {
		data, _ := json.Marshal(f["payload"])
		_ = json.Unmarshal(data, &opened)
	}
	return opened, f, nil
}

// tunnelE2ESealed: on an end-to-end tunnel every shape works and the Router
// relays only sealed payloads, metadata and error messages.
func tunnelE2ESealed(h *harness) error {
	issuer, err := h.client()
	if err != nil {
		return err
	}
	defer issuer.close()
	opened, f, err := h.openE2ETunnel(issuer, h.e2e.PublicKey(), "echo", "count", "sum", "chat", "fail", "metadata_echo", "tunnel_info")
	if err != nil {
		return err
	}
	if f["kind"] != "call.result" || asMap(f["payload"])["e2e_key"] != b64.EncodeToString(h.e2e.PublicKey().Bytes()) {
		return fmt.Errorf("open with the fixture's key: %v", f)
	}
	defer issuer.callService(tunnelService, "revoke", frame{"tunnel_id": opened.TunnelID}, nil) //nolint:errcheck
	c, err := h.clientWith("Bearer "+opened.Credential, "")
	if err != nil {
		return err
	}
	defer c.close()
	s, refused, err := c.e2eHandshake(h.e2e.PublicKey())
	if err != nil || s == nil {
		return fmt.Errorf("handshake with the pinned key: %v %v", err, refused)
	}

	// Unary: payload and metadata sealed both ways.
	id, err := c.sealedOpen(s, "metadata_echo", frame{}, map[string]string{"note": "secret-metadata"})
	if err != nil {
		return err
	}
	got, relayed, err := c.receiveCall(s, id, "metadata_echo")
	if err != nil {
		return err
	}
	if err := inClear(relayed, "secret-metadata", "echoed"); err != nil {
		return err
	}
	md := asMap(asMap(got["payload"])["metadata"])
	trailers := asMap(got["metadata"])
	if got["kind"] != "call.result" || md["note"] != "secret-metadata" || md[tunnelIDKey] != opened.TunnelID || trailers["echoed"] != "yes" {
		return fmt.Errorf("sealed metadata_echo: %v", got)
	}
	id, _ = c.sealedOpen(s, "echo", frame{"value": 7, "text": "secret-payload"}, nil)
	if got, relayed, err = c.receiveCall(s, id, "echo"); err != nil {
		return err
	} else if err := inClear(relayed, "secret-payload"); err != nil {
		return err
	} else if p := asMap(got["payload"]); got["kind"] != "call.result" || p["text"] != "secret-payload" || number(p["value"]) != 7 {
		return fmt.Errorf("sealed echo: %v", got)
	}
	id, _ = c.sealedOpen(s, "tunnel_info", frame{}, nil)
	if got, _, err = c.receiveCall(s, id, "tunnel_info"); err != nil {
		return err
	} else if p := asMap(got["payload"]); p["tunnel_id"] != opened.TunnelID || p["sealed"] != true {
		return fmt.Errorf("ctx.tunnel of a sealed call: %v", got)
	}

	// An application error keeps its code in clear; its message and details are sealed.
	id, _ = c.sealedOpen(s, "fail", frame{"code": "app.secret", "message": "secret-message"}, nil)
	if got, relayed, err = c.receiveCall(s, id, "fail"); err != nil {
		return err
	} else if err := inClear(relayed, "secret-message", "fixture"); err != nil {
		return err
	} else if code, message, _, details := errorOf(got); code != "app.secret" || message != "secret-message" || details["from"] != "fixture" {
		return fmt.Errorf("sealed error: %v", got)
	}

	// Server stream: items sealed, windows and the end in clear.
	id, _ = c.sealedOpen(s, "count", frame{"n": 3}, nil)
	if err := c.send(frame{"kind": "stream.window", "call_id": id, "direction": "server", "credit": 8}); err != nil {
		return err
	}
	for index := 0; ; {
		got, relayed, err := c.receiveCall(s, id, "count")
		if err != nil {
			return fmt.Errorf("sealed server stream: %w", err)
		}
		if got["kind"] == "stream.item" {
			if relayed["sealed"] == nil || number(asMap(got["payload"])["index"]) != float64(index) {
				return fmt.Errorf("sealed item %d: %v", index, relayed)
			}
			index++
		}
		if got["kind"] == "call.result" {
			if index != 3 {
				return fmt.Errorf("sealed server stream ended after %d items", index)
			}
			break
		}
		if got["kind"] == "call.error" {
			return fmt.Errorf("sealed server stream: %v", got)
		}
	}

	// Client stream: sealed items in, sealed result out.
	id, _ = c.sealedOpen(s, "sum", frame{"offset": 1}, nil)
	if got, _, err = c.receiveCall(s, id, "sum"); err != nil || got["kind"] != "stream.window" {
		return fmt.Errorf("sealed client stream window: %v %v", err, got)
	}
	for i, v := range []int{2, 3} {
		item := frame{"kind": "stream.item", "call_id": id, "direction": "client", "sequence": i, "payload": frame{"value": v}}
		if err := c.send(s.seal(item, Service, "sum")); err != nil {
			return err
		}
	}
	if err := c.send(frame{"kind": "stream.end", "call_id": id, "direction": "client"}); err != nil {
		return err
	}
	for {
		got, _, err := c.receiveCall(s, id, "sum")
		if err != nil {
			return fmt.Errorf("sealed client stream: %w", err)
		}
		if got["kind"] == "call.result" {
			if number(asMap(got["payload"])["total"]) != 6 {
				return fmt.Errorf("sealed sum: %v", got)
			}
			break
		}
		if got["kind"] == "call.error" {
			return fmt.Errorf("sealed client stream: %v", got)
		}
	}

	// Duplex: sealed both ways, interleaved.
	id, _ = c.sealedOpen(s, "chat", frame{"prefix": "> "}, nil)
	if err := c.send(frame{"kind": "stream.window", "call_id": id, "direction": "server", "credit": 8}); err != nil {
		return err
	}
	sent := 0
	for want := []string{"> secret-line", "done:1"}; len(want) > 0; {
		got, relayed, err := c.receiveCall(s, id, "chat")
		if err != nil {
			return fmt.Errorf("sealed duplex: %w", err)
		}
		switch got["kind"] {
		case "stream.window":
			if sent == 0 {
				item := frame{"kind": "stream.item", "call_id": id, "direction": "client", "sequence": 0, "payload": frame{"text": "secret-line"}}
				if err := c.send(s.seal(item, Service, "chat")); err != nil {
					return err
				}
				sent++
			}
		case "stream.item":
			if err := inClear(relayed, "secret-line"); err != nil {
				return err
			}
			if text := asMap(got["payload"])["text"]; text != want[0] {
				return fmt.Errorf("sealed duplex item %v, want %q", text, want[0])
			}
			want = want[1:]
			if len(want) == 1 {
				if err := c.send(frame{"kind": "stream.end", "call_id": id, "direction": "client"}); err != nil {
					return err
				}
			}
		case "call.error":
			return fmt.Errorf("sealed duplex: %v", got)
		}
	}
	return nil
}

// tunnelE2ERefusals: the issuer's pin decides who may answer; a plain call,
// a wrong or unpinned key, an unknown session or an altered frame never
// reaches a handler.
func tunnelE2ERefusals(h *harness) error {
	issuer, err := h.client()
	if err != nil {
		return err
	}
	defer issuer.close()
	other, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	// The instance refuses an offer pinning a key it does not hold.
	if _, f, err := h.openE2ETunnel(issuer, other.PublicKey(), "echo"); err != nil {
		return err
	} else if err := expectReason(f, "permission_denied", "tunnel_rejected"); err != nil {
		return fmt.Errorf("an offer pinning another key: %w", err)
	}
	f, err := issuer.callService(tunnelService, "open", frame{
		"instance_id": InstanceID, "principal": "p", "ttl_seconds": 60,
		"scopes": []frame{{"service": Service, "methods": []string{"echo"}}}, "e2e_key": "short",
	}, nil)
	if err != nil {
		return err
	}
	if code, _, _, details := errorOf(f); code != "invalid_argument" || details["source"] != "router" {
		return fmt.Errorf("a malformed e2e_key: %v", f)
	}
	opened, _, err := h.openE2ETunnel(issuer, h.e2e.PublicKey(), "echo", "slow")
	if err != nil || opened.Credential == "" {
		return fmt.Errorf("open: %v", err)
	}
	defer issuer.callService(tunnelService, "revoke", frame{"tunnel_id": opened.TunnelID}, nil) //nolint:errcheck
	c, err := h.clientWith("Bearer "+opened.Credential, "")
	if err != nil {
		return err
	}
	defer c.close()

	// A plain call, over WebSocket or HTTP, is refused by the Router.
	if f, err := c.unary("echo", frame{"value": 1, "text": "t"}, nil); err != nil {
		return err
	} else if err := expectReason(f, "permission_denied", "tunnel_e2e_required"); err != nil {
		return fmt.Errorf("a plain call: %w", err)
	}
	resp, err := h.httpCall(h.ctx, "echo", `{"value": 1, "text": "t"}`, map[string]string{
		"Content-Type": "application/json", "Authorization": "Bearer " + opened.Credential,
	})
	if err != nil {
		return err
	}
	body, err := readJSON(resp)
	if err != nil {
		return err
	}
	if resp.StatusCode != 403 || asMap(asMap(body["error"])["details"])["reason"] != "tunnel_e2e_required" {
		return fmt.Errorf("a plain HTTP call: %d %v", resp.StatusCode, body)
	}

	// A client pinning another key: the instance cannot open message 1.
	if s, f, err := c.e2eHandshake(other.PublicKey()); err != nil || s != nil {
		return fmt.Errorf("a handshake towards another key: %v %v", err, s)
	} else if err := expectReason(f, "unauthenticated", "e2e_handshake_failed"); err != nil {
		return fmt.Errorf("a handshake towards another key: %w", err)
	}
	f, err = c.callService(tunnelHostName, "handshake", frame{"message": b64.EncodeToString(make([]byte, 20))}, nil)
	if err != nil {
		return err
	}
	if err := expectReason(f, "unauthenticated", "e2e_handshake_failed"); err != nil {
		return fmt.Errorf("a malformed handshake: %w", err)
	}

	s, refused, err := c.e2eHandshake(h.e2e.PublicKey())
	if err != nil || s == nil {
		return fmt.Errorf("handshake with the pinned key: %v %v", err, refused)
	}
	sealedSlow := func(tag string, alter func(frame) frame) (frame, error) {
		id := c.callID()
		open := alter(s.seal(frame{"kind": "call.open", "call_id": id, "service": Service, "method": "slow", "payload": frame{"ms": 1, "tag": tag}}, Service, "slow"))
		if err := c.send(open); err != nil {
			return nil, err
		}
		for {
			f, err := c.receive()
			if err != nil || f["call_id"] == id {
				return f, err
			}
		}
	}
	// An unknown session, an altered sealed field and content in clear are
	// all refused before the handler; the client handshakes again.
	cases := map[string]func(frame) frame{
		"e2e-unknown": func(f frame) frame { f["e2e_session"] = strings.Repeat("0", 32); return f },
		"e2e-altered": func(f frame) frame {
			raw, _ := b64.DecodeString(f["sealed"].(string))
			raw[len(raw)-1] ^= 1
			f["sealed"] = b64.EncodeToString(raw)
			return f
		},
		"e2e-moved": func(f frame) frame { f["method"] = "echo"; return f },
	}
	for _, tag := range []string{"e2e-unknown", "e2e-altered", "e2e-moved"} {
		f, err := sealedSlow(tag, cases[tag])
		if err != nil {
			return err
		}
		if err := expectReason(f, "unauthenticated", "e2e_session_unknown"); err != nil {
			return fmt.Errorf("%s: %w", tag, err)
		}
	}
	if f, err := sealedSlow("e2e-sealed", func(f frame) frame { return f }); err != nil || f["kind"] != "call.result" {
		return fmt.Errorf("a sealed slow call: %v %v", err, f)
	}
	f, err = issuer.unary("invoked_tags", frame{}, nil)
	if err != nil {
		return err
	}
	var ran []string
	for _, tag := range asList(asMap(f["payload"])["tags"]) {
		if t, _ := tag.(string); strings.HasPrefix(t, "e2e-") {
			ran = append(ran, t)
		}
	}
	if strings.Join(ran, ",") != "e2e-sealed" {
		return fmt.Errorf("handlers that ran: %v, want only e2e-sealed", ran)
	}
	return nil
}
