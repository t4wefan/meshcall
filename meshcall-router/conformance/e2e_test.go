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
