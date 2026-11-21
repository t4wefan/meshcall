package conformance

import (
	"crypto/ecdh"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"
)

// The harness's own Noise NK must match the published test vector before it
// can judge any runtime.
func TestHarnessNoiseMatchesTheVector(t *testing.T) {
	data, err := os.ReadFile("testdata/noise_nk_25519_aesgcm_sha256.json")
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		InitPrologue     string `json:"init_prologue"`
		InitEphemeral    string `json:"init_ephemeral"`
		InitRemoteStatic string `json:"init_remote_static"`
		HandshakeHash    string `json:"handshake_hash"`
		Messages         []struct{ Payload, Ciphertext string }
	}
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatal(err)
	}
	b := func(h string) []byte { out, _ := hex.DecodeString(h); return out }
	remote, _ := ecdh.X25519().NewPublicKey(b(v.InitRemoteStatic))
	ephemeral, _ := ecdh.X25519().NewPrivateKey(b(v.InitEphemeral))
	initiator, msg1, err := nkInitiate(remote, ephemeral, b(v.InitPrologue), b(v.Messages[0].Payload))
	if err != nil || hex.EncodeToString(msg1) != v.Messages[0].Ciphertext {
		t.Fatalf("message 1: %v %x", err, msg1)
	}
	payload, session, err := initiator.finish(b(v.Messages[1].Ciphertext))
	if err != nil || hex.EncodeToString(payload) != v.Messages[1].Payload || hex.EncodeToString(session.hash) != v.HandshakeHash {
		t.Fatalf("message 2: %v %x", err, payload)
	}
	// Transport messages alternate, initiator first, each with its own counter.
	var sent, received uint64
	for i, m := range v.Messages[2:] {
		if i%2 == 0 {
			if got := session.send.Seal(nil, noiseNonce(sent), b(m.Payload), nil); hex.EncodeToString(got) != m.Ciphertext {
				t.Fatalf("transport message %d: %x", i, got)
			}
			sent++
		} else {
			if got, err := session.recv.Open(nil, noiseNonce(received), b(m.Ciphertext), nil); err != nil || hex.EncodeToString(got) != m.Payload {
				t.Fatalf("transport message %d: %v", i, err)
			}
			received++
		}
	}
}

// The harness's IK and IKpsk2 must match the published vectors too.
func TestHarnessPeerHandshakesMatchTheVectors(t *testing.T) {
	for _, name := range []string{"nk", "ik", "ikpsk2"} {
		data, err := os.ReadFile("testdata/noise_" + name + "_25519_aesgcm_sha256.json")
		if err != nil {
			t.Fatal(err)
		}
		var v struct {
			Protocol         string   `json:"protocol_name"`
			InitPrologue     string   `json:"init_prologue"`
			InitStatic       string   `json:"init_static"`
			InitEphemeral    string   `json:"init_ephemeral"`
			InitRemoteStatic string   `json:"init_remote_static"`
			InitPSKs         []string `json:"init_psks"`
			RespStatic       string   `json:"resp_static"`
			RespEphemeral    string   `json:"resp_ephemeral"`
			HandshakeHash    string   `json:"handshake_hash"`
			Messages         []struct{ Payload, Ciphertext string }
		}
		if err := json.Unmarshal(data, &v); err != nil {
			t.Fatal(err)
		}
		b := func(h string) []byte { out, _ := hex.DecodeString(h); return out }
		priv := func(h string) *ecdh.PrivateKey {
			if h == "" {
				return nil
			}
			k, _ := ecdh.X25519().NewPrivateKey(b(h))
			return k
		}
		var psk []byte
		if len(v.InitPSKs) > 0 {
			psk = b(v.InitPSKs[0])
		}
		rs, _ := ecdh.X25519().NewPublicKey(b(v.InitRemoteStatic))
		i := newHandshake(v.Protocol, true, b(v.InitPrologue), priv(v.InitStatic), rs, psk, priv(v.InitEphemeral))
		r := newHandshake(v.Protocol, false, b(v.InitPrologue), priv(v.RespStatic), nil, psk, priv(v.RespEphemeral))
		m1, err := i.write(b(v.Messages[0].Payload))
		if err != nil || hex.EncodeToString(m1) != v.Messages[0].Ciphertext {
			t.Fatalf("%s message 1: %v %x", name, err, m1)
		}
		if p, err := r.read(m1); err != nil || hex.EncodeToString(p) != v.Messages[0].Payload {
			t.Fatalf("%s read 1: %v", name, err)
		}
		m2, err := r.write(b(v.Messages[1].Payload))
		if err != nil || hex.EncodeToString(m2) != v.Messages[1].Ciphertext {
			t.Fatalf("%s message 2: %v %x", name, err, m2)
		}
		if p, err := i.read(m2); err != nil || hex.EncodeToString(p) != v.Messages[1].Payload {
			t.Fatalf("%s read 2: %v", name, err)
		}
		if s := i.split(); hex.EncodeToString(s.hash) != v.HandshakeHash {
			t.Fatalf("%s handshake hash %x", name, s.hash)
		}
	}
}

// The MeshCall layer on top: prologues, request and answer JSON, session ids
// and the pairing confirmation (testdata/e2e_handshakes.json).
func TestHarnessMatchesTheMeshCallHandshakes(t *testing.T) {
	data, err := os.ReadFile("testdata/e2e_handshakes.json")
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Credential      string `json:"credential"`
		InstanceID      string `json:"instance_id"`
		InstancePrivate string `json:"instance_private_key"`
		ClientPrivate   string `json:"client_private_key"`
		ClientPublic    string `json:"client_public_key"`
		InstanceEph     string `json:"instance_ephemeral"`
		ClientEph       string `json:"client_ephemeral"`
		Pairing         E2EPairing
		Cases           []struct {
			Protocol       string            `json:"protocol"`
			Prologue       string            `json:"prologue"`
			Request        map[string]string `json:"request"`
			Answer         map[string]string `json:"answer"`
			Session        string            `json:"session"`
			ConfirmRequest map[string]string `json:"confirm_request"`
		}
	}
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatal(err)
	}
	key := func(s string) *ecdh.PrivateKey {
		raw, _ := b64.DecodeString(s)
		k, err := ecdh.X25519().NewPrivateKey(raw)
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	inst, cl := key(v.InstancePrivate), key(v.ClientPrivate)
	psk, _ := b64.DecodeString(v.Pairing.PSK)
	for _, c := range v.Cases {
		prologue := tunnelPrologue(v.Credential)
		var static *ecdh.PrivateKey
		var p []byte
		if c.Protocol != protocolNK {
			prologue, static = peerPrologue(v.Credential, v.InstanceID), cl
		}
		if c.Protocol == protocolIKpsk2 {
			p = psk
		}
		if string(prologue) != c.Prologue {
			t.Fatalf("%s prologue %q", c.Protocol, prologue)
		}
		i := newHandshake(c.Protocol, true, prologue, static, inst.PublicKey(), p, key(v.ClientEph))
		r := newHandshake(c.Protocol, false, prologue, inst, nil, p, key(v.InstanceEph))
		m1, _ := i.write(nil)
		if b64.EncodeToString(m1) != c.Request["message"] {
			t.Fatalf("%s request message", c.Protocol)
		}
		if _, err := r.read(m1); err != nil {
			t.Fatal(err)
		}
		if c.Protocol != protocolNK && b64.EncodeToString(r.rs.Bytes()) != v.ClientPublic {
			t.Fatalf("%s: the instance did not learn the client key", c.Protocol)
		}
		m2, _ := r.write(nil)
		if b64.EncodeToString(m2) != c.Answer["message"] {
			t.Fatalf("%s answer message", c.Protocol)
		}
		if _, err := i.read(m2); err != nil {
			t.Fatal(err)
		}
		client, instance := i.split(), r.split()
		if client.id != c.Session || instance.id != c.Session {
			t.Fatalf("%s session %s", c.Protocol, client.id)
		}
		if c.ConfirmRequest != nil {
			if got := client.sealConfirm(); got != c.ConfirmRequest["confirm"] {
				t.Fatalf("confirm %s", got)
			}
			if opened, err := instance.openConfirm(c.ConfirmRequest["confirm"]); err != nil || string(opened) != "{}" {
				t.Fatalf("open confirm: %v %q", err, opened)
			}
		}
	}
}
