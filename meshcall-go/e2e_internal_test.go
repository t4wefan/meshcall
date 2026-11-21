package meshcall

import (
	"context"
	"crypto/ecdh"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// The Noise test vectors (cacophony) for the three protocols: handshake
// messages, handshake hash and the transport keys' first messages.
func TestNoiseMatchesTheTestVectors(t *testing.T) {
	for _, name := range []string{"nk", "ik", "ikpsk2"} {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile("../meshcall-router/conformance/testdata/noise_" + name + "_25519_aesgcm_sha256.json")
			if err != nil {
				t.Fatal(err)
			}
			var v struct {
				Protocol      string   `json:"protocol_name"`
				Prologue      string   `json:"init_prologue"`
				PSKs          []string `json:"init_psks"`
				InitStatic    string   `json:"init_static"`
				InitEphemeral string   `json:"init_ephemeral"`
				InitRemote    string   `json:"init_remote_static"`
				RespStatic    string   `json:"resp_static"`
				RespEphemeral string   `json:"resp_ephemeral"`
				HandshakeHash string   `json:"handshake_hash"`
				Messages      []struct {
					Payload    string `json:"payload"`
					Ciphertext string `json:"ciphertext"`
				} `json:"messages"`
			}
			if err := json.Unmarshal(data, &v); err != nil {
				t.Fatal(err)
			}
			unhex := func(s string) []byte { b, _ := hex.DecodeString(s); return b }
			key := func(s string) *ecdh.PrivateKey {
				if s == "" {
					return nil
				}
				k, err := ecdh.X25519().NewPrivateKey(unhex(s))
				if err != nil {
					t.Fatal(err)
				}
				return k
			}
			rs, _ := ecdh.X25519().NewPublicKey(unhex(v.InitRemote))
			var psk []byte
			if len(v.PSKs) > 0 {
				psk = unhex(v.PSKs[0])
			}
			prologue := unhex(v.Prologue)
			initiator, err := newHandshake(v.Protocol, true, prologue, key(v.InitStatic), rs, psk, key(v.InitEphemeral))
			if err != nil {
				t.Fatal(err)
			}
			responder, err := newHandshake(v.Protocol, false, prologue, key(v.RespStatic), nil, psk, key(v.RespEphemeral))
			if err != nil {
				t.Fatal(err)
			}
			msg1, err := initiator.writeMessage(unhex(v.Messages[0].Payload))
			if err != nil || hex.EncodeToString(msg1) != v.Messages[0].Ciphertext {
				t.Fatalf("message 1: %x %v", msg1, err)
			}
			got1, err := responder.readMessage(msg1)
			if err != nil || hex.EncodeToString(got1) != v.Messages[0].Payload {
				t.Fatalf("read message 1: %x %v", got1, err)
			}
			if v.InitStatic != "" && !responder.rs.Equal(key(v.InitStatic).PublicKey()) {
				t.Fatal("the responder did not learn the initiator's static key")
			}
			msg2, err := responder.writeMessage(unhex(v.Messages[1].Payload))
			if err != nil || hex.EncodeToString(msg2) != v.Messages[1].Ciphertext {
				t.Fatalf("message 2: %x %v", msg2, err)
			}
			got2, err := initiator.readMessage(msg2)
			if err != nil || hex.EncodeToString(got2) != v.Messages[1].Payload {
				t.Fatalf("read message 2: %x %v", got2, err)
			}
			client, instance := initiator.split(), responder.split()
			if hex.EncodeToString(client.hash[:]) != v.HandshakeHash || client.id != instance.id || client.protocol != v.Protocol {
				t.Fatalf("handshake hash %x", client.hash)
			}
			// Transport messages alternate initiator, responder, counting from 0 in each direction.
			var n [2]uint64
			for i, m := range v.Messages[2:] {
				from, to := []*e2eSession{client, instance}[i%2], []*e2eSession{instance, client}[i%2]
				sealed := from.send.Seal(nil, gcmNonce(n[i%2]), unhex(m.Payload), nil)
				if hex.EncodeToString(sealed) != m.Ciphertext {
					t.Fatalf("transport message %d: %x", i, sealed)
				}
				if opened, err := to.recv.Open(nil, gcmNonce(n[i%2]), sealed, nil); err != nil || hex.EncodeToString(opened) != m.Payload {
					t.Fatalf("transport message %d does not open: %v", i, err)
				}
				n[i%2]++
			}
		})
	}
}

// MeshCall's own handshakes (testdata/e2e_handshakes.json): the client's
// TunnelHost/handshake request, the instance's answer, the session id and
// the pairing confirmation, byte for byte from fixed keys, through the code
// paths the client and the instance use.
func TestMeshCallHandshakesMatchTheVectors(t *testing.T) {
	data, err := os.ReadFile("../meshcall-router/conformance/testdata/e2e_handshakes.json")
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Credential        string `json:"credential"`
		CredentialDigest  string `json:"credential_digest"`
		InstanceID        string `json:"instance_id"`
		InstancePrivate   string `json:"instance_private_key"`
		InstancePublic    string `json:"instance_public_key"`
		ClientPrivate     string `json:"client_private_key"`
		ClientPublic      string `json:"client_public_key"`
		InstanceEphemeral string `json:"instance_ephemeral"`
		ClientEphemeral   string `json:"client_ephemeral"`
		Pairing           struct {
			ID  string `json:"id"`
			PSK string `json:"psk"`
		} `json:"pairing"`
		Cases []struct {
			Protocol       string            `json:"protocol"`
			Prologue       string            `json:"prologue"`
			Request        map[string]string `json:"request"`
			Answer         map[string]string `json:"answer"`
			HandshakeHash  string            `json:"handshake_hash"`
			Session        string            `json:"session"`
			ConfirmRequest map[string]string `json:"confirm_request"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatal(err)
	}
	if e2eCredentialDigest(v.Credential) != v.CredentialDigest {
		t.Fatalf("credential digest %s", e2eCredentialDigest(v.Credential))
	}
	if pub, _ := E2EPublicKey(v.InstancePrivate); pub != v.InstancePublic {
		t.Fatalf("instance public key %s", pub)
	}
	if pub, _ := E2EPublicKey(v.ClientPrivate); pub != v.ClientPublic {
		t.Fatalf("client public key %s", pub)
	}
	priv := func(s string) *ecdh.PrivateKey {
		k, err := parsePrivateKey(s)
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	pinned, _ := parsePublicKey(v.InstancePublic)
	identity := &E2EIdentity{PrivateKey: v.ClientPrivate, InstanceID: v.InstanceID, Pairing: &E2EPairing{ID: v.Pairing.ID, PSK: v.Pairing.PSK}}
	for _, c := range v.Cases {
		t.Run(c.Protocol, func(t *testing.T) {
			var asked []E2EPeer
			var mu sync.Mutex
			host := newTunnelHost(map[string]*TunnelPolicy{"s.v1.S": {}})
			host.key, host.instanceID, host.ephemeral = priv(v.InstancePrivate), v.InstanceID, priv(v.InstanceEphemeral)
			host.authorize = func(_ context.Context, peer E2EPeer) bool {
				mu.Lock()
				defer mu.Unlock()
				asked = append(asked, peer)
				return true
			}
			host.pairingPSK = func(_ context.Context, id string) (string, bool) {
				return v.Pairing.PSK, id == v.Pairing.ID
			}
			offer, _ := json.Marshal(map[string]any{"tunnel_id": "t1", "instance_id": v.InstanceID, "principal": "p",
				"scopes": []TunnelScope{{Service: "s.v1.S", Methods: []string{"*"}}}, "e2e_key": v.InstancePublic,
				"credential_digest": v.CredentialDigest})
			ctx := context.Background()
			if accepted, err := host.handle(ctx, "offer", offer, nil); err != nil || accepted.(map[string]bool)["accept"] != true {
				t.Fatalf("offer: %v %v", accepted, err)
			}
			metadata := map[string]string{tunnelIDKey: "t1", tunnelPrincipalKey: "p"}

			var who *E2EIdentity
			if c.Protocol != E2EProtocolNK {
				who = identity
			}
			hs, request, pairing, err := startE2EHandshake(v.Credential, pinned, who, c.Protocol == E2EProtocolIK, priv(v.ClientEphemeral))
			if err != nil {
				t.Fatal(err)
			}
			if got, _ := json.Marshal(request); string(got) != mustJSON(c.Request) {
				t.Fatalf("request %s, want %s", got, mustJSON(c.Request))
			}
			prologue := string(e2eTunnelPrologue(v.CredentialDigest))
			if c.Protocol != E2EProtocolNK {
				prologue = string(e2ePeerPrologue(v.CredentialDigest, v.InstanceID))
			}
			if c.Prologue != prologue {
				t.Fatalf("prologue %s, want %s", prologue, c.Prologue)
			}
			raw, _ := json.Marshal(request)
			answer, err := host.handle(ctx, "handshake", raw, metadata)
			if err != nil {
				t.Fatal(err)
			}
			if got, _ := json.Marshal(answer); string(got) != mustJSON(c.Answer) {
				t.Fatalf("answer %s, want %s", got, mustJSON(c.Answer))
			}
			msg2, _ := b64.DecodeString(c.Answer["message"])
			if _, err := hs.readMessage(msg2); err != nil {
				t.Fatal(err)
			}
			sess := hs.split()
			if hex.EncodeToString(sess.hash[:]) != c.HandshakeHash || sess.id != c.Session {
				t.Fatalf("session %s", sess.id)
			}
			if c.ConfirmRequest == nil {
				if pairing != nil {
					t.Fatal("a session without a pairing needs a confirmation")
				}
				if found := host.sessions.find("t1", c.Session); found == nil || (c.Protocol == E2EProtocolIK) != (found.peerKey == v.ClientPublic) {
					t.Fatalf("instance session: %+v", found)
				}
				return
			}
			// A pairing session is pending until its client confirms it.
			if pairing == nil || host.sessions.find("t1", c.Session) != nil || len(asked) != 0 {
				t.Fatal("a pairing session was trusted before its confirmation")
			}
			confirm := e2eConfirmRequest(sess)
			if got, _ := json.Marshal(confirm); string(got) != mustJSON(c.ConfirmRequest) {
				t.Fatalf("confirm %s, want %s", got, mustJSON(c.ConfirmRequest))
			}
			raw, _ = json.Marshal(confirm)
			if _, err := host.handle(ctx, "handshake", raw, metadata); err != nil {
				t.Fatal(err)
			}
			found := host.sessions.find("t1", c.Session)
			if found == nil || len(asked) != 1 || asked[0].PeerKey != v.ClientPublic || asked[0].PairingID != v.Pairing.ID ||
				asked[0].Protocol != E2EProtocolIKpsk2 || asked[0].Session != c.Session || asked[0].Tunnel == nil {
				t.Fatalf("after the confirmation: %+v %+v", found, asked)
			}
			// Each pairing session is confirmed once.
			if _, err := host.handle(ctx, "handshake", raw, metadata); err == nil {
				t.Fatal("a pairing was confirmed twice")
			}
		})
	}
}

func mustJSON(v map[string]string) string {
	out, _ := json.Marshal(v)
	return string(out)
}

// The instance refuses every bad handshake with the same answer, and keeps
// pending sessions apart from usable ones.
func TestInstanceRefusesBadHandshakesAlike(t *testing.T) {
	instancePriv, instancePub, _ := GenerateE2EKey()
	clientPriv, clientPub, _ := GenerateE2EKey()
	psk, _ := GenerateE2EPSK()
	ctx := context.Background()
	setup := func(authorize func(context.Context, E2EPeer) bool) (*tunnelHost, map[string]string, string) {
		host := newTunnelHost(map[string]*TunnelPolicy{"s.v1.S": {}})
		host.key, _ = parsePrivateKey(instancePriv)
		host.instanceID = "dev-a"
		host.authorize = authorize
		host.pairingPSK = func(_ context.Context, id string) (string, bool) { return psk, id == "pair-1" }
		credential := "mtun_e2e.test"
		offer, _ := json.Marshal(map[string]any{"tunnel_id": "t1", "principal": "p", "scopes": []TunnelScope{{Service: "s.v1.S"}},
			"e2e_key": instancePub, "credential_digest": e2eCredentialDigest(credential)})
		if _, err := host.handle(ctx, "offer", offer, nil); err != nil {
			t.Fatal(err)
		}
		return host, map[string]string{tunnelIDKey: "t1", tunnelPrincipalKey: "p"}, credential
	}
	pinned, _ := parsePublicKey(instancePub)
	handshakeFailed := func(err error) bool {
		e, ok := AsError(err)
		return ok && e.Code == CodeUnauthenticated && e.Reason() == "e2e_handshake_failed"
	}
	send := func(host *tunnelHost, metadata, request map[string]string) error {
		raw, _ := json.Marshal(request)
		_, err := host.handle(ctx, "handshake", raw, metadata)
		return err
	}
	refuse := func(context.Context, E2EPeer) bool { return false }
	hang := func(ctx context.Context, _ E2EPeer) bool { <-ctx.Done(); return true }
	explode := func(context.Context, E2EPeer) bool { panic("hook failed") }
	var messages []string
	for name, c := range map[string]struct {
		authorize func(context.Context, E2EPeer) bool
		identity  *E2EIdentity
		edit      func(map[string]string)
	}{
		"unknown protocol":     {nil, nil, func(r map[string]string) { r["protocol"] = "Noise_XX_25519_AESGCM_SHA256" }},
		"refused key":          {refuse, &E2EIdentity{PrivateKey: clientPriv, InstanceID: "dev-a"}, nil},
		"hook panics":          {explode, &E2EIdentity{PrivateKey: clientPriv, InstanceID: "dev-a"}, nil},
		"other instance":       {nil, &E2EIdentity{PrivateKey: clientPriv, InstanceID: "dev-b"}, nil},
		"unknown pairing":      {nil, &E2EIdentity{PrivateKey: clientPriv, InstanceID: "dev-a", Pairing: &E2EPairing{ID: "pair-2", PSK: psk}}, nil},
		"empty pairing id":     {nil, &E2EIdentity{PrivateKey: clientPriv, InstanceID: "dev-a", Pairing: &E2EPairing{ID: "pair-1", PSK: psk}}, func(r map[string]string) { r["pairing_id"] = "" }},
		"IK sent as IKpsk2":    {nil, &E2EIdentity{PrivateKey: clientPriv, InstanceID: "dev-a"}, func(r map[string]string) { r["protocol"], r["pairing_id"] = E2EProtocolIKpsk2, "pair-1" }},
		"NK sent as IK":        {nil, nil, func(r map[string]string) { r["protocol"] = E2EProtocolIK }},
		"malformed message":    {nil, nil, func(r map[string]string) { r["message"] = "!!" }},
		"hook does not answer": {hang, &E2EIdentity{PrivateKey: clientPriv, InstanceID: "dev-a"}, nil},
	} {
		host, metadata, credential := setup(c.authorize)
		_, request, _, err := startE2EHandshake(credential, pinned, c.identity, false, nil)
		if err != nil {
			t.Fatal(err)
		}
		if c.edit != nil {
			c.edit(request)
		}
		hookCtx := ctx
		if name == "hook does not answer" {
			var cancel context.CancelFunc
			hookCtx, cancel = context.WithTimeout(ctx, 50*time.Millisecond)
			defer cancel()
		}
		raw, _ := json.Marshal(request)
		_, err = host.handle(hookCtx, "handshake", raw, metadata)
		if !handshakeFailed(err) {
			t.Fatalf("%s: %v", name, err)
		}
		e, _ := AsError(err)
		messages = append(messages, e.Message)
		if len(host.sessions.byTunnel["t1"])+len(host.sessions.pending["t1"]) != 0 {
			t.Fatalf("%s left a session", name)
		}
	}
	for _, m := range messages {
		if m != messages[0] {
			t.Fatalf("refusals differ: %q, %q", m, messages[0])
		}
	}

	// A pairing with the wrong PSK: message 2 does not open on the client,
	// and a confirmation that does not open drops the pending session.
	var asked int
	host, metadata, credential := setup(func(context.Context, E2EPeer) bool { asked++; return true })
	wrong, _ := GenerateE2EPSK()
	hs, request, _, _ := startE2EHandshake(credential, pinned, &E2EIdentity{PrivateKey: clientPriv, InstanceID: "dev-a", Pairing: &E2EPairing{ID: "pair-1", PSK: wrong}}, false, nil)
	raw, _ := json.Marshal(request)
	answer, err := host.handle(ctx, "handshake", raw, metadata)
	if err != nil {
		t.Fatal(err)
	}
	msg2, _ := b64.DecodeString(answer.(map[string]string)["message"])
	if _, err := hs.readMessage(msg2); err == nil {
		t.Fatal("message 2 opened with the wrong PSK")
	}
	if len(host.sessions.pending["t1"]) != 1 || asked != 0 {
		t.Fatal("a pairing session must stay pending, unasked")
	}
	forged := host.sessions.pending["t1"][0].id
	if err := send(host, metadata, map[string]string{"session": forged, "confirm": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}); !handshakeFailed(err) {
		t.Fatalf("a forged confirmation: %v", err)
	}
	if len(host.sessions.pending["t1"]) != 0 || host.sessions.find("t1", forged) != nil || asked != 0 {
		t.Fatal("a confirmation that does not open must drop the pending session")
	}
	// A confirmation the hook refuses is refused too.
	host, metadata, credential = setup(refuse)
	hs, request, _, _ = startE2EHandshake(credential, pinned, &E2EIdentity{PrivateKey: clientPriv, InstanceID: "dev-a", Pairing: &E2EPairing{ID: "pair-1", PSK: psk}}, false, nil)
	raw, _ = json.Marshal(request)
	answer, err = host.handle(ctx, "handshake", raw, metadata)
	if err != nil {
		t.Fatal(err)
	}
	msg2, _ = b64.DecodeString(answer.(map[string]string)["message"])
	if _, err := hs.readMessage(msg2); err != nil {
		t.Fatal(err)
	}
	sess := hs.split()
	if err := send(host, metadata, e2eConfirmRequest(sess)); !handshakeFailed(err) {
		t.Fatalf("a refused pairing: %v", err)
	}
	if host.sessions.find("t1", sess.id) != nil {
		t.Fatal("a refused pairing left a session")
	}
	// Pending sessions are bounded per tunnel and dropped with it.
	for range e2eMaxSessions + 3 {
		_, request, _, _ = startE2EHandshake(credential, pinned, &E2EIdentity{PrivateKey: clientPriv, InstanceID: "dev-a", Pairing: &E2EPairing{ID: "pair-1", PSK: psk}}, false, nil)
		if err := send(host, metadata, request); err != nil {
			t.Fatal(err)
		}
	}
	if len(host.sessions.pending["t1"]) != e2eMaxSessions {
		t.Fatalf("%d pending sessions", len(host.sessions.pending["t1"]))
	}
	closed, _ := json.Marshal(map[string]string{"tunnel_id": "t1", "reason": "revoked"})
	if _, err := host.handle(ctx, "closed", closed, nil); err != nil || len(host.sessions.pending["t1"]) != 0 {
		t.Fatalf("closing the tunnel kept its pending sessions: %v", err)
	}
	_ = clientPub
}

func TestE2EIdentityIsCheckedAtConfiguration(t *testing.T) {
	priv, _, _ := GenerateE2EKey()
	psk, _ := GenerateE2EPSK()
	for name, identity := range map[string]E2EIdentity{
		"no instance id":  {PrivateKey: priv},
		"bad private key": {PrivateKey: "short", InstanceID: "a"},
		"short PSK":       {PrivateKey: priv, InstanceID: "a", Pairing: &E2EPairing{ID: "p", PSK: "short"}},
		"empty pairing":   {PrivateKey: priv, InstanceID: "a", Pairing: &E2EPairing{PSK: psk}},
		"long pairing id": {PrivateKey: priv, InstanceID: "a", Pairing: &E2EPairing{ID: strings.Repeat("x", 129), PSK: psk}},
	} {
		if checkE2EIdentity(identity) == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	if err := checkE2EIdentity(E2EIdentity{PrivateKey: priv, InstanceID: "a", Pairing: &E2EPairing{ID: strings.Repeat("x", 128), PSK: psk}}); err != nil {
		t.Fatal(err)
	}
}

func TestSealedFramesBindTheirCallAndRefuseReplays(t *testing.T) {
	priv, pub, err := GenerateE2EKey()
	if err != nil {
		t.Fatal(err)
	}
	s, _ := parsePrivateKey(priv)
	p, _ := parsePublicKey(pub)
	initiator, _ := newHandshake(E2EProtocolNK, true, []byte(e2ePrologue), nil, p, nil, nil)
	responder, _ := newHandshake(E2EProtocolNK, false, []byte(e2ePrologue), s, nil, nil, nil)
	msg1, _ := initiator.writeMessage(nil)
	if _, err := responder.readMessage(msg1); err != nil {
		t.Fatal(err)
	}
	msg2, _ := responder.writeMessage(nil)
	if _, err := initiator.readMessage(msg2); err != nil {
		t.Fatal(err)
	}
	client, instance := initiator.split(), responder.split()
	out := &e2eCall{session: client, service: "s.v1.S", method: "m"}
	in := &e2eCall{session: instance, service: "s.v1.S", method: "m"}
	open := out.sealFrame(frame{Kind: "call.open", CallID: "c1", Service: "s.v1.S", Method: "m",
		Payload: json.RawMessage(`{"secret":1}`), Metadata: map[string]string{"trace": "t"}})
	if string(open.Payload) != "null" || open.Metadata != nil || open.E2ESession != client.id {
		t.Fatalf("sealed call.open leaks: %+v", open)
	}
	got, err := in.openFrame(open)
	if err != nil || string(got.Payload) != `{"secret":1}` || got.Metadata["trace"] != "t" {
		t.Fatalf("open: %+v %v", got, err)
	}
	if _, err := in.openFrame(open); err == nil {
		t.Fatal("a replayed frame opened")
	}
	moved := out.sealFrame(frame{Kind: "call.open", CallID: "c2", Service: "s.v1.S", Method: "m", Payload: json.RawMessage(`1`)})
	moved.CallID = "c3"
	if _, err := in.openFrame(moved); err == nil {
		t.Fatal("a sealed part moved to another call opened")
	}
	if _, err := in.openFrame(frame{Kind: "call.result", CallID: "c1", Payload: json.RawMessage(`1`)}); err == nil {
		t.Fatal("a clear result was accepted on a sealed call")
	}
	// Out of order within the window is fine; far behind it is not.
	a := out.sealFrame(frame{Kind: "stream.item", CallID: "c1", Direction: "client", Sequence: int64p(0), Payload: json.RawMessage(`1`)})
	b := out.sealFrame(frame{Kind: "stream.item", CallID: "c1", Direction: "client", Sequence: int64p(1), Payload: json.RawMessage(`2`)})
	if _, err := in.openFrame(b); err != nil {
		t.Fatal(err)
	}
	if _, err := in.openFrame(a); err != nil {
		t.Fatalf("reordered item: %v", err)
	}
	var old frame
	for i := 0; i < e2eWindow+2; i++ {
		f := out.sealFrame(frame{Kind: "stream.item", CallID: "c1", Direction: "client", Sequence: int64p(int64(i)), Payload: json.RawMessage(`1`)})
		if i == 0 {
			old = f
			continue
		}
		if _, err := in.openFrame(f); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := in.openFrame(old); err == nil {
		t.Fatal("a frame older than the window opened")
	}
	e := in.sealFrame(frame{Kind: "call.error", CallID: "c1", Error: &errorPayload{Code: CodeInvalidArgument, Message: "bad secret 42", Details: map[string]any{"field": "secret"}}})
	if e.Error.Message != "sealed" || e.Error.Details != nil || e.Error.Code != CodeInvalidArgument {
		t.Fatalf("sealed error leaks: %+v", e.Error)
	}
	if got, err := out.openFrame(e); err != nil || got.Error.Message != "bad secret 42" || got.Error.Details["field"] != "secret" {
		t.Fatalf("open error: %+v %v", got.Error, err)
	}
}
