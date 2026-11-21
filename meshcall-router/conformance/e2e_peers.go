package conformance

import (
	"crypto/ecdh"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// The harness side of clients that authenticate themselves (docs/protocol.md
// → Client authentication): Noise IK and IKpsk2, the pairing confirmation,
// and the client keys and pairings the fixture is told to accept. Written
// from the protocol text, like the NK side.

const (
	protocolNK     = "Noise_NK_25519_AESGCM_SHA256"
	protocolIK     = "Noise_IK_25519_AESGCM_SHA256"
	protocolIKpsk2 = "Noise_IKpsk2_25519_AESGCM_SHA256"
	pairedName     = "conformance.v1.Paired"
)

// E2EPairing is a one-time pairing the fixture accepts.
type E2EPairing struct {
	ID  string `json:"id"`
	PSK string `json:"psk"`
}

// e2ePeers is what a fixture is told about clients: one key it accepts and
// a few pairings.
type e2ePeers struct {
	client   *ecdh.PrivateKey
	pairings []E2EPairing
}

func newE2EPeers(pairings int) (e2ePeers, error) {
	client, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return e2ePeers{}, err
	}
	p := e2ePeers{client: client}
	for i := 1; i <= pairings; i++ {
		psk := make([]byte, 32)
		if _, err := rand.Read(psk); err != nil {
			return e2ePeers{}, err
		}
		p.pairings = append(p.pairings, E2EPairing{ID: fmt.Sprintf("pair-%d", i), PSK: b64.EncodeToString(psk)})
	}
	return p, nil
}

func (p e2ePeers) env() []string {
	entries := make([]string, len(p.pairings))
	for i, pairing := range p.pairings {
		entries[i] = pairing.ID + ":" + pairing.PSK
	}
	return []string{
		"MESHCALL_E2E_PEERS=" + b64.EncodeToString(p.client.PublicKey().Bytes()),
		"MESHCALL_E2E_PAIRINGS=" + strings.Join(entries, ","),
	}
}

func (p e2ePeers) psk(id string) []byte {
	for _, pairing := range p.pairings {
		if pairing.ID == id {
			raw, _ := b64.DecodeString(pairing.PSK)
			return raw
		}
	}
	return nil
}

// handshake is one end of NK, IK or IKpsk2 (two messages, the responder's
// static key known beforehand).
type handshake struct {
	protocol  string
	initiator bool
	sym       *noise
	s, e      *ecdh.PrivateKey
	rs, re    *ecdh.PublicKey
	psk       []byte
	turn      int
}

var patterns = map[string][2][]string{
	protocolNK:     {{"e", "es"}, {"e", "ee"}},
	protocolIK:     {{"e", "es", "s", "ss"}, {"e", "ee", "se"}},
	protocolIKpsk2: {{"e", "es", "s", "ss"}, {"e", "ee", "se", "psk"}},
}

func newNoiseFor(protocol string, prologue []byte) *noise {
	name := make([]byte, 32) // every name here is at most HASHLEN: padded
	copy(name, protocol)
	s := &noise{h: name, ck: append([]byte{}, name...)}
	s.mixHash(prologue)
	return s
}

// newHandshake: s is this end's static key, rs the responder's public key on
// the initiator, e an ephemeral for test vectors (nil: random).
func newHandshake(protocol string, initiator bool, prologue []byte, s *ecdh.PrivateKey, rs *ecdh.PublicKey, psk []byte, e *ecdh.PrivateKey) *handshake {
	hs := &handshake{protocol: protocol, initiator: initiator, sym: newNoiseFor(protocol, prologue), s: s, rs: rs, psk: psk, e: e}
	if initiator {
		hs.sym.mixHash(rs.Bytes())
	} else {
		hs.sym.mixHash(s.PublicKey().Bytes())
	}
	return hs
}

func hkdf3(ck, ikm []byte) ([]byte, []byte, []byte) {
	mac := func(key, data []byte) []byte {
		m := hmac.New(sha256.New, key)
		m.Write(data)
		return m.Sum(nil)
	}
	temp := mac(ck, ikm)
	a := mac(temp, []byte{1})
	b := mac(temp, append(append([]byte{}, a...), 2))
	return a, b, mac(temp, append(append([]byte{}, b...), 3))
}

func (hs *handshake) dh(token string) ([]byte, error) {
	mine, theirs := token[0], token[1]
	if !hs.initiator {
		mine, theirs = theirs, mine
	}
	local := hs.s
	if mine == 'e' {
		local = hs.e
	}
	remote := hs.rs
	if theirs == 'e' {
		remote = hs.re
	}
	if local == nil || remote == nil {
		return nil, errors.New("missing key for " + token)
	}
	return local.ECDH(remote)
}

func (hs *handshake) write(payload []byte) ([]byte, error) {
	tokens := patterns[hs.protocol][hs.turn]
	hs.turn++
	var out []byte
	for _, token := range tokens {
		switch token {
		case "e":
			if hs.e == nil {
				var err error
				if hs.e, err = ecdh.X25519().GenerateKey(rand.Reader); err != nil {
					return nil, err
				}
			}
			pub := hs.e.PublicKey().Bytes()
			out = append(out, pub...)
			hs.sym.mixHash(pub)
			if hs.psk != nil {
				hs.sym.mixKey(pub)
			}
		case "s":
			out = append(out, hs.sym.encryptAndHash(hs.s.PublicKey().Bytes())...)
		case "psk":
			hs.mixKeyAndHash(hs.psk)
		default:
			shared, err := hs.dh(token)
			if err != nil {
				return nil, err
			}
			hs.sym.mixKey(shared)
		}
	}
	return append(out, hs.sym.encryptAndHash(payload)...), nil
}

func (hs *handshake) read(message []byte) ([]byte, error) {
	tokens := patterns[hs.protocol][hs.turn]
	hs.turn++
	rest := message
	for _, token := range tokens {
		switch token {
		case "e":
			if len(rest) < 32 {
				return nil, errors.New("short handshake message")
			}
			re, err := ecdh.X25519().NewPublicKey(rest[:32])
			if err != nil {
				return nil, err
			}
			hs.re = re
			hs.sym.mixHash(rest[:32])
			if hs.psk != nil {
				hs.sym.mixKey(rest[:32])
			}
			rest = rest[32:]
		case "s":
			size := 32
			if hs.sym.k != nil {
				size = 48
			}
			if len(rest) < size {
				return nil, errors.New("short handshake message")
			}
			raw, err := hs.sym.decryptAndHash(rest[:size])
			if err != nil {
				return nil, err
			}
			if hs.rs, err = ecdh.X25519().NewPublicKey(raw); err != nil {
				return nil, err
			}
			rest = rest[size:]
		case "psk":
			hs.mixKeyAndHash(hs.psk)
		default:
			shared, err := hs.dh(token)
			if err != nil {
				return nil, err
			}
			hs.sym.mixKey(shared)
		}
	}
	return hs.sym.decryptAndHash(rest)
}

func (hs *handshake) mixKeyAndHash(psk []byte) {
	var temp, k []byte
	hs.sym.ck, temp, k = hkdf3(hs.sym.ck, psk)
	hs.sym.mixHash(temp)
	hs.sym.k, hs.sym.n = aesGCM(k), 0
}

// split returns the session of this end once both messages are done.
func (hs *handshake) split() *e2eSession {
	toResponder, toInitiator := hkdf2(hs.sym.ck, nil)
	send, recv := toResponder, toInitiator
	if !hs.initiator {
		send, recv = toInitiator, toResponder
	}
	return &e2eSession{hash: hs.sym.h, id: hex.EncodeToString(hs.sym.h[:16]), send: aesGCM(send), recv: aesGCM(recv)}
}

// peerPrologue binds an IK / IKpsk2 handshake to the tunnel and the instance.
func peerPrologue(credential, instanceID string) []byte {
	return append(tunnelPrologue(credential), []byte("|"+instanceID)...)
}

// sealConfirm is the pairing confirmation: {} sealed with the associated
// data meshcall-e2e/1|confirm|<session>.
func (s *e2eSession) sealConfirm() string {
	n := s.sendN
	s.sendN++
	counter := binary.BigEndian.AppendUint64(nil, n)
	return b64.EncodeToString(s.send.Seal(counter, noiseNonce(n), []byte("{}"), []byte(e2ePrologue+"|confirm|"+s.id)))
}

// openConfirm is the instance side of sealConfirm, for the test vector.
func (s *e2eSession) openConfirm(sealed string) ([]byte, error) {
	raw, err := b64.DecodeString(sealed)
	if err != nil || len(raw) < 24 {
		return nil, errors.New("malformed confirmation")
	}
	return s.recv.Open(nil, noiseNonce(binary.BigEndian.Uint64(raw)), raw[8:], []byte(e2ePrologue+"|confirm|"+s.id))
}

// peerHandshake is TunnelHost/handshake with a client key of our own; with
// pairingID and psk it pairs (and does not confirm: see confirm). It returns
// the session, or the error frame the instance answered.
func (c *client) peerHandshake(pinned *ecdh.PublicKey, credential, instanceID string, key *ecdh.PrivateKey, pairingID string, psk []byte) (*e2eSession, frame, error) {
	protocol := protocolIK
	request := frame{"protocol": protocolIK}
	if psk != nil {
		protocol = protocolIKpsk2
		request = frame{"protocol": protocolIKpsk2, "pairing_id": pairingID}
	}
	hs := newHandshake(protocol, true, peerPrologue(credential, instanceID), key, pinned, psk, nil)
	message, err := hs.write(nil)
	if err != nil {
		return nil, nil, err
	}
	request["message"] = b64.EncodeToString(message)
	return c.finishHandshake(hs, request)
}

// rawHandshake sends a handshake request as given; with hs it finishes it.
func (c *client) finishHandshake(hs *handshake, request frame) (*e2eSession, frame, error) {
	f, err := c.callService(tunnelHostName, "handshake", request, nil)
	if err != nil {
		return nil, nil, err
	}
	if f["kind"] != "call.result" || hs == nil {
		return nil, f, nil
	}
	reply, err := b64.DecodeString(fmt.Sprint(asMap(f["payload"])["message"]))
	if err != nil {
		return nil, nil, fmt.Errorf("handshake answer: %w", err)
	}
	if _, err := hs.read(reply); err != nil {
		return nil, f, fmt.Errorf("message 2 does not come from the pinned key: %w", err)
	}
	return hs.split(), f, nil
}

// confirm sends a pairing confirmation for session id (sealed by s, which
// may be another session's to forge one); it returns the answer frame.
func (c *client) confirm(id string, s *e2eSession) (frame, error) {
	return c.callService(tunnelHostName, "handshake", frame{"session": id, "confirm": s.sealConfirm()}, nil)
}

func (c *client) sealedCall(s *e2eSession, service, method string, payload any) (frame, error) {
	id, err := c.sealedOpenOn(s, service, method, payload)
	if err != nil {
		return nil, err
	}
	opened, _, err := c.receiveCallOn(s, id, service, method)
	return opened, err
}

// peerOf calls Paired/peer in session s and checks what it reports.
func (c *client) peerOf(s *e2eSession, protocol string, key *ecdh.PublicKey, pairingID any) error {
	f, err := c.sealedCall(s, pairedName, "peer", frame{})
	if err != nil {
		return err
	}
	got := asMap(f["payload"])
	if f["kind"] != "call.result" || got["protocol"] != protocol || got["peer_key"] != b64.EncodeToString(key.Bytes()) || got["pairing_id"] != pairingID {
		return fmt.Errorf("Paired/peer: %v, want %s, %s, %v", f, protocol, b64.EncodeToString(key.Bytes()), pairingID)
	}
	return nil
}

func (h *harness) peerTunnel(issuer *client, principal string) (tunnelCredential, *client, error) {
	scopes := []frame{{"service": Service, "methods": []string{"echo"}}, {"service": pairedName, "methods": []string{"peer"}}}
	opened, f, err := h.openE2ETunnelAs(issuer, h.e2e.PublicKey(), principal, scopes)
	if err != nil || opened.Credential == "" {
		return opened, nil, fmt.Errorf("open an end-to-end tunnel: %v %v", err, f)
	}
	c, err := h.clientWith("Bearer "+opened.Credential, "")
	return opened, c, err
}

func newKey() *ecdh.PrivateKey {
	k, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}
	return k
}

// tunnelE2EPeerAdmission: IK admits the client keys the instance accepts;
// every refusal reads the same, so a client cannot probe which failed.
func tunnelE2EPeerAdmission(h *harness) error {
	issuer, err := h.client()
	if err != nil {
		return err
	}
	defer issuer.close()
	opened, c, err := h.peerTunnel(issuer, "user:alice")
	if err != nil {
		return err
	}
	defer issuer.callService(tunnelService, "revoke", frame{"tunnel_id": opened.TunnelID}, nil) //nolint:errcheck
	defer c.close()
	key, known := h.e2e.PublicKey(), h.peers.client

	s, f, err := c.peerHandshake(key, opened.Credential, InstanceID, known, "", nil)
	if err != nil || s == nil {
		return fmt.Errorf("IK with a known key: %v %v", err, f)
	}
	if err := c.peerOf(s, protocolIK, known.PublicKey(), nil); err != nil {
		return err
	}

	var messages []string
	refused := func(what string, f frame, err error) error {
		if err != nil {
			return fmt.Errorf("%s: %w", what, err)
		}
		if err := expectReason(f, "unauthenticated", "e2e_handshake_failed"); err != nil {
			return fmt.Errorf("%s: %w", what, err)
		}
		messages = append(messages, fmt.Sprint(asMap(f["error"])["message"]))
		return nil
	}
	_, f, err = c.peerHandshake(key, opened.Credential, InstanceID, newKey(), "", nil)
	if err := refused("an unknown client key", f, err); err != nil {
		return err
	}
	_, f, err = c.peerHandshake(key, opened.Credential, "another-instance", known, "", nil)
	if err := refused("a handshake bound to another instance id", f, err); err != nil {
		return err
	}
	// The Router, or anyone on the path, changing the protocol name: the
	// message does not open under another handshake.
	hs := newHandshake(protocolIK, true, peerPrologue(opened.Credential, InstanceID), known, key, nil, nil)
	m1, _ := hs.write(nil)
	for _, protocol := range []any{nil, protocolNK, protocolIKpsk2, "Noise_XX_25519_AESGCM_SHA256"} {
		request := frame{"message": b64.EncodeToString(m1)}
		if protocol != nil {
			request["protocol"] = protocol
		}
		if protocol == protocolIKpsk2 {
			request["pairing_id"] = "no-such-pairing"
		}
		_, f, err = c.finishHandshake(nil, request)
		if err := refused(fmt.Sprintf("an IK message 1 sent as %v", protocol), f, err); err != nil {
			return err
		}
	}
	_, f, err = c.finishHandshake(nil, frame{"protocol": protocolIK, "message": b64.EncodeToString(make([]byte, 96))})
	if err := refused("a malformed IK message", f, err); err != nil {
		return err
	}
	// A handshake made for this tunnel does not complete through another.
	other, oc, err := h.peerTunnel(issuer, "user:bob")
	if err != nil {
		return err
	}
	defer issuer.callService(tunnelService, "revoke", frame{"tunnel_id": other.TunnelID}, nil) //nolint:errcheck
	defer oc.close()
	_, f, err = oc.peerHandshake(key, opened.Credential, InstanceID, known, "", nil)
	if err := refused("alice's IK handshake through bob's tunnel", f, err); err != nil {
		return err
	}
	for _, m := range messages[1:] {
		if m != messages[0] {
			return fmt.Errorf("refusals tell their causes apart: %q", messages)
		}
	}
	return nil
}

// tunnelE2EPairing: a pairing session waits for proof of the PSK; only then
// is the key accepted and the pairing spent, once.
func tunnelE2EPairing(h *harness) error {
	issuer, err := h.client()
	if err != nil {
		return err
	}
	defer issuer.close()
	opened, c, err := h.peerTunnel(issuer, "user:alice")
	if err != nil {
		return err
	}
	defer issuer.callService(tunnelService, "revoke", frame{"tunnel_id": opened.TunnelID}, nil) //nolint:errcheck
	defer c.close()
	key := h.e2e.PublicKey()
	pairing := h.peers.pairings[0]
	psk := h.peers.psk(pairing.ID)

	// Someone who saw the pairing id but not the PSK: message 1 is answered
	// (it cannot prove anything), message 2 does not open, and whatever it
	// sends as a confirmation is refused. The pairing is not spent.
	mallory := newKey()
	wrong := make([]byte, 32)
	if _, err := rand.Read(wrong); err != nil {
		return err
	}
	if s, f, err := c.peerHandshake(key, opened.Credential, InstanceID, mallory, pairing.ID, wrong); s != nil || err == nil || f["kind"] != "call.result" {
		return fmt.Errorf("a pairing with a wrong PSK: %v %v %v", s, f, err)
	}
	known, _, err := c.peerHandshake(key, opened.Credential, InstanceID, h.peers.client, "", nil)
	if err != nil || known == nil {
		return fmt.Errorf("IK with the known key: %v", err)
	}
	if f, err := c.confirm(strings.Repeat("ab", 16), known); err != nil {
		return err
	} else if err := expectReason(f, "unauthenticated", "e2e_handshake_failed"); err != nil {
		return fmt.Errorf("a forged confirmation: %w", err)
	}
	if _, f, err := c.peerHandshake(key, opened.Credential, InstanceID, mallory, "", nil); err != nil {
		return err
	} else if err := expectReason(f, "unauthenticated", "e2e_handshake_failed"); err != nil {
		return fmt.Errorf("IK with the key of a failed pairing: %w", err)
	}

	alice := newKey()
	s, f, err := c.peerHandshake(key, opened.Credential, InstanceID, alice, pairing.ID, psk)
	if err != nil || s == nil {
		return fmt.Errorf("pairing with the right PSK: %v %v", err, f)
	}
	// Pending: not usable before the confirmation.
	if f, err := c.sealedCall(s, pairedName, "peer", frame{}); err != nil {
		return err
	} else if err := expectReason(f, "unauthenticated", "e2e_session_unknown"); err != nil {
		return fmt.Errorf("a call in an unconfirmed pairing session: %w", err)
	}
	if f, err := c.confirm(s.id, s); err != nil || f["kind"] != "call.result" {
		return fmt.Errorf("the confirmation: %v %v", err, f)
	}
	if err := c.peerOf(s, protocolIKpsk2, alice.PublicKey(), pairing.ID); err != nil {
		return err
	}
	if f, err := c.confirm(s.id, s); err != nil {
		return err
	} else if err := expectReason(f, "unauthenticated", "e2e_handshake_failed"); err != nil {
		return fmt.Errorf("a second confirmation: %w", err)
	}
	// Spent: the instance no longer finds its PSK, even for its holder.
	if _, f, err := c.peerHandshake(key, opened.Credential, InstanceID, mallory, pairing.ID, psk); err != nil {
		return err
	} else if err := expectReason(f, "unauthenticated", "e2e_handshake_failed"); err != nil {
		return fmt.Errorf("a spent pairing: %w", err)
	}
	// The paired key now connects with IK.
	again, err := h.clientWith("Bearer "+opened.Credential, "")
	if err != nil {
		return err
	}
	defer again.close()
	s3, f, err := again.peerHandshake(key, opened.Credential, InstanceID, alice, "", nil)
	if err != nil || s3 == nil {
		return fmt.Errorf("IK after pairing: %v %v", err, f)
	}
	return again.peerOf(s3, protocolIK, alice.PublicKey(), nil)
}

// tunnelE2EPeerRequired: the instance serves Paired only to authenticated
// clients, refusing everything else itself.
func tunnelE2EPeerRequired(h *harness) error {
	issuer, err := h.client()
	if err != nil {
		return err
	}
	defer issuer.close()
	opened, c, err := h.peerTunnel(issuer, "user:alice")
	if err != nil {
		return err
	}
	defer issuer.callService(tunnelService, "revoke", frame{"tunnel_id": opened.TunnelID}, nil) //nolint:errcheck
	defer c.close()
	s, f, err := c.e2eHandshake(h.e2e.PublicKey(), opened.Credential)
	if err != nil || s == nil {
		return fmt.Errorf("NK handshake: %v %v", err, f)
	}
	if f, err := c.sealedCall(s, pairedName, "peer", frame{}); err != nil {
		return err
	} else if err := expectReason(f, "permission_denied", "e2e_peer_required"); err != nil {
		return fmt.Errorf("Paired through an NK session: %w", err)
	}
	if f, err := c.sealedCall(s, Service, "echo", frame{"value": 1, "text": "nk"}); err != nil || f["kind"] != "call.result" {
		return fmt.Errorf("Fixture through an NK session: %v %v", err, f)
	}
	// Without a tunnel: the account's own targeted call.
	f, err = issuer.callService(pairedName, "peer", frame{}, frame{"instance_id": InstanceID})
	if err != nil {
		return err
	}
	if err := expectReason(f, "permission_denied", "e2e_peer_required"); err != nil {
		return fmt.Errorf("Paired without a tunnel: %w", err)
	}
	// A plain tunnel is refused at open: the service requires end-to-end.
	_, f, err = h.openTunnel(issuer, "user:plain", 60, []frame{{"service": pairedName, "methods": []string{"peer"}}})
	if err != nil {
		return err
	}
	return expectReason(f, "permission_denied", "tunnel_rejected")
}
