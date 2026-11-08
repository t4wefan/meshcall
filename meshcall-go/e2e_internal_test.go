package meshcall

import (
	"crypto/ecdh"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"
)

// The Noise test vector (cacophony) for this protocol: handshake messages,
// handshake hash and the transport keys' first messages.
func TestNoiseNKMatchesTheTestVector(t *testing.T) {
	data, err := os.ReadFile("../meshcall-router/conformance/testdata/noise_nk_25519_aesgcm_sha256.json")
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Prologue      string `json:"init_prologue"`
		InitEphemeral string `json:"init_ephemeral"`
		InitRemote    string `json:"init_remote_static"`
		RespStatic    string `json:"resp_static"`
		RespEphemeral string `json:"resp_ephemeral"`
		HandshakeHash string `json:"handshake_hash"`
		Messages      []struct {
			Payload    string `json:"payload"`
			Ciphertext string `json:"ciphertext"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatal(err)
	}
	unhex := func(s string) []byte { b, _ := hex.DecodeString(s); return b }
	key := func(s string) *ecdh.PrivateKey { k, _ := ecdh.X25519().NewPrivateKey(unhex(s)); return k }
	rs, _ := ecdh.X25519().NewPublicKey(unhex(v.InitRemote))
	prologue := unhex(v.Prologue)

	initiator, msg1, err := nkInitiate(rs, key(v.InitEphemeral), prologue, unhex(v.Messages[0].Payload))
	if err != nil || hex.EncodeToString(msg1) != v.Messages[0].Ciphertext {
		t.Fatalf("message 1: %x %v", msg1, err)
	}
	msg2, got1, responder, err := nkRespond(key(v.RespStatic), key(v.RespEphemeral), prologue, msg1, unhex(v.Messages[1].Payload))
	if err != nil || hex.EncodeToString(msg2) != v.Messages[1].Ciphertext || hex.EncodeToString(got1) != v.Messages[0].Payload {
		t.Fatalf("message 2: %x %v", msg2, err)
	}
	got2, client, err := initiator.finish(msg2)
	if err != nil || hex.EncodeToString(got2) != v.Messages[1].Payload {
		t.Fatalf("finish: %v", err)
	}
	if hex.EncodeToString(client.hash[:]) != v.HandshakeHash || client.id != responder.id {
		t.Fatalf("handshake hash %x", client.hash)
	}
	// Transport messages alternate initiator, responder, counting from 0 in each direction.
	var n [2]uint64
	for i, m := range v.Messages[2:] {
		from := []*e2eSession{client, responder}[i%2]
		sealed := from.send.Seal(nil, gcmNonce(n[i%2]), unhex(m.Payload), nil)
		n[i%2]++
		if hex.EncodeToString(sealed) != m.Ciphertext {
			t.Fatalf("transport message %d: %x", i, sealed)
		}
	}
}

func TestSealedFramesBindTheirCallAndRefuseReplays(t *testing.T) {
	priv, pub, err := GenerateE2EKey()
	if err != nil {
		t.Fatal(err)
	}
	s, _ := parsePrivateKey(priv)
	p, _ := parsePublicKey(pub)
	initiator, msg1, _ := nkInitiate(p, nil, []byte(e2ePrologue), nil)
	msg2, _, instance, err := nkRespond(s, nil, []byte(e2ePrologue), msg1, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, client, err := initiator.finish(msg2)
	if err != nil {
		t.Fatal(err)
	}
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
