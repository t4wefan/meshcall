package meshcall

import (
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
	"strconv"
	"strings"
	"sync"
)

// End-to-end sealed tunnels (docs/protocol.md → End-to-end encryption): a
// Noise_NK_25519_AESGCM_SHA256 handshake per client connection, then the
// payloads, metadata and error messages of every call travel sealed while
// the Router keeps routing on the clear header.

const (
	noiseProtocol   = "Noise_NK_25519_AESGCM_SHA256"
	e2ePrologue     = "meshcall-e2e/1"
	e2eWindow       = 1024
	e2eMaxSessions  = 16
	e2eSealedNotice = "sealed"
)

var b64 = base64.RawURLEncoding

// GenerateE2EKey makes an instance key pair for end-to-end sealed tunnels:
// configure privateKey on the instance (WithE2EPrivateKey) and give
// publicKey to the issuer, which pins it in Tunnels().Open (E2EKey). Both
// are base64url without padding.
func GenerateE2EKey() (privateKey, publicKey string, err error) {
	key, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return "", "", err
	}
	return b64.EncodeToString(key.Bytes()), b64.EncodeToString(key.PublicKey().Bytes()), nil
}

// E2EPublicKey returns the public key of an instance's private key.
func E2EPublicKey(privateKey string) (string, error) {
	key, err := parsePrivateKey(privateKey)
	if err != nil {
		return "", err
	}
	return b64.EncodeToString(key.PublicKey().Bytes()), nil
}

func parsePrivateKey(s string) (*ecdh.PrivateKey, error) {
	raw, err := b64.DecodeString(s)
	if err != nil || len(raw) != 32 {
		return nil, errors.New("meshcall: an E2E private key is 32 bytes, base64url without padding")
	}
	return ecdh.X25519().NewPrivateKey(raw)
}

func parsePublicKey(s string) (*ecdh.PublicKey, error) {
	raw, err := b64.DecodeString(s)
	if err != nil || len(raw) != 32 {
		return nil, errors.New("meshcall: an E2E key is a 32-byte X25519 public key, base64url without padding")
	}
	return ecdh.X25519().NewPublicKey(raw)
}

// --- Noise NK ---------------------------------------------------------------

type symmetricState struct {
	ck, h [32]byte
	k     []byte // nil until the first MixKey
	n     uint64
}

func newSymmetricState(prologue []byte) *symmetricState {
	s := &symmetricState{}
	copy(s.h[:], noiseProtocol) // the name is shorter than HASHLEN: zero padded
	s.ck = s.h
	s.mixHash(prologue)
	return s
}

func (s *symmetricState) mixHash(data []byte) {
	sum := sha256.New()
	sum.Write(s.h[:])
	sum.Write(data)
	copy(s.h[:], sum.Sum(nil))
}

func hkdf2(ck []byte, ikm []byte) (a, b [32]byte) {
	mac := hmac.New(sha256.New, ck)
	mac.Write(ikm)
	temp := mac.Sum(nil)
	mac = hmac.New(sha256.New, temp)
	mac.Write([]byte{1})
	copy(a[:], mac.Sum(nil))
	mac = hmac.New(sha256.New, temp)
	mac.Write(a[:])
	mac.Write([]byte{2})
	copy(b[:], mac.Sum(nil))
	return a, b
}

func (s *symmetricState) mixKey(ikm []byte) {
	ck, k := hkdf2(s.ck[:], ikm)
	s.ck, s.k, s.n = ck, k[:], 0
}

func gcmNonce(n uint64) []byte {
	nonce := make([]byte, 12) // four zero bytes, then the big-endian counter
	binary.BigEndian.PutUint64(nonce[4:], n)
	return nonce
}

func newGCM(key []byte) cipher.AEAD {
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err) // 32-byte keys only
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		panic(err)
	}
	return aead
}

func (s *symmetricState) encryptAndHash(plaintext []byte) []byte {
	out := plaintext
	if s.k != nil {
		out = newGCM(s.k).Seal(nil, gcmNonce(s.n), plaintext, s.h[:])
		s.n++
	}
	s.mixHash(out)
	return out
}

func (s *symmetricState) decryptAndHash(ciphertext []byte) ([]byte, error) {
	out := ciphertext
	if s.k != nil {
		var err error
		if out, err = newGCM(s.k).Open(nil, gcmNonce(s.n), ciphertext, s.h[:]); err != nil {
			return nil, err
		}
		s.n++
	}
	s.mixHash(ciphertext)
	return out, nil
}

func (s *symmetricState) split() (initiatorToResponder, responderToInitiator [32]byte) {
	return hkdf2(s.ck[:], nil)
}

// nkInitiator is the client side of a handshake in progress.
type nkInitiator struct {
	sym *symmetricState
	e   *ecdh.PrivateKey
}

// nkInitiate writes message 1 (-> e, es) towards the pinned responder key.
// ephemeral is for test vectors only; nil draws a fresh key.
func nkInitiate(rs *ecdh.PublicKey, ephemeral *ecdh.PrivateKey, prologue, payload []byte) (*nkInitiator, []byte, error) {
	sym := newSymmetricState(prologue)
	sym.mixHash(rs.Bytes())
	e := ephemeral
	if e == nil {
		var err error
		if e, err = ecdh.X25519().GenerateKey(rand.Reader); err != nil {
			return nil, nil, err
		}
	}
	msg := append([]byte{}, e.PublicKey().Bytes()...)
	sym.mixHash(e.PublicKey().Bytes())
	es, err := e.ECDH(rs)
	if err != nil {
		return nil, nil, err
	}
	sym.mixKey(es)
	msg = append(msg, sym.encryptAndHash(payload)...)
	return &nkInitiator{sym: sym, e: e}, msg, nil
}

// finish reads message 2 (<- e, ee) and returns the transport keys.
func (i *nkInitiator) finish(msg []byte) (payload []byte, sess *e2eSession, err error) {
	if len(msg) < 32+16 {
		return nil, nil, errors.New("short handshake message")
	}
	re, err := ecdh.X25519().NewPublicKey(msg[:32])
	if err != nil {
		return nil, nil, err
	}
	i.sym.mixHash(msg[:32])
	ee, err := i.e.ECDH(re)
	if err != nil {
		return nil, nil, err
	}
	i.sym.mixKey(ee)
	if payload, err = i.sym.decryptAndHash(msg[32:]); err != nil {
		return nil, nil, err
	}
	send, recv := i.sym.split()
	return payload, newE2ESession(i.sym.h, send, recv), nil
}

// nkRespond reads message 1 with the instance's static key and writes
// message 2.
func nkRespond(s *ecdh.PrivateKey, ephemeral *ecdh.PrivateKey, prologue, msg, payload []byte) (reply, received []byte, sess *e2eSession, err error) {
	if len(msg) < 32+16 {
		return nil, nil, nil, errors.New("short handshake message")
	}
	sym := newSymmetricState(prologue)
	sym.mixHash(s.PublicKey().Bytes())
	re, err := ecdh.X25519().NewPublicKey(msg[:32])
	if err != nil {
		return nil, nil, nil, err
	}
	sym.mixHash(msg[:32])
	es, err := s.ECDH(re)
	if err != nil {
		return nil, nil, nil, err
	}
	sym.mixKey(es)
	if received, err = sym.decryptAndHash(msg[32:]); err != nil {
		return nil, nil, nil, err
	}
	e := ephemeral
	if e == nil {
		if e, err = ecdh.X25519().GenerateKey(rand.Reader); err != nil {
			return nil, nil, nil, err
		}
	}
	reply = append([]byte{}, e.PublicKey().Bytes()...)
	sym.mixHash(e.PublicKey().Bytes())
	ee, err := e.ECDH(re)
	if err != nil {
		return nil, nil, nil, err
	}
	sym.mixKey(ee)
	reply = append(reply, sym.encryptAndHash(payload)...)
	toResponder, toInitiator := sym.split()
	return reply, received, newE2ESession(sym.h, toInitiator, toResponder), nil
}

// --- sessions and sealed frames ----------------------------------------------

// e2eSession holds one direction's key and counter for sending and the
// other's for receiving, with a replay window (frames may be dropped or
// reordered by the Router, never accepted twice).
type e2eSession struct {
	id   string
	hash [32]byte
	send cipher.AEAD
	recv cipher.AEAD

	mu     sync.Mutex
	sendN  uint64
	top    uint64 // highest counter received + 1; 0: none yet
	window [e2eWindow / 64]uint64
}

func newE2ESession(h [32]byte, send, recv [32]byte) *e2eSession {
	return &e2eSession{id: hex.EncodeToString(h[:16]), hash: h, send: newGCM(send[:]), recv: newGCM(recv[:])}
}

// e2eAD is the associated data binding a sealed part to its frame.
// e2eTunnelPrologue binds a handshake to one tunnel: the hex SHA-256 of its
// credential, which the client hashes itself and the instance learns from
// the offer.
func e2eTunnelPrologue(credentialDigest string) []byte {
	return []byte(e2ePrologue + "|" + credentialDigest)
}

func e2eCredentialDigest(credential string) string {
	sum := sha256.Sum256([]byte(credential))
	return hex.EncodeToString(sum[:])
}

// routerServicePrefix: the Router answers these services itself, so calls
// to them are never sealed.
const routerServicePrefix = "meshcall.router."

// isE2ECredential: end-to-end tunnel credentials say so ("." never occurs
// in a plain one).
func isE2ECredential(credential string) bool { return strings.HasPrefix(credential, "mtun_e2e.") }

func e2eAD(kind, callID, service, method, direction string, sequence *int64, session string) []byte {
	seq := ""
	if sequence != nil {
		seq = strconv.FormatInt(*sequence, 10)
	}
	return []byte(strings.Join([]string{e2ePrologue, kind, callID, service, method, direction, seq, session}, "|"))
}

func (s *e2eSession) seal(plaintext, ad []byte) string {
	s.mu.Lock()
	n := s.sendN
	s.sendN++
	s.mu.Unlock()
	out := make([]byte, 8, 8+len(plaintext)+16)
	binary.BigEndian.PutUint64(out, n)
	return b64.EncodeToString(s.send.Seal(out, gcmNonce(n), plaintext, ad))
}

func (s *e2eSession) open(sealed string, ad []byte) ([]byte, error) {
	raw, err := b64.DecodeString(sealed)
	if err != nil || len(raw) < 8+16 {
		return nil, errors.New("malformed sealed field")
	}
	n := binary.BigEndian.Uint64(raw[:8])
	plaintext, err := s.recv.Open(nil, gcmNonce(n), raw[8:], ad)
	if err != nil {
		return nil, errors.New("sealed field does not open")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.accept(n) {
		return nil, errors.New("sealed field replayed or too old")
	}
	return plaintext, nil
}

// accept records counter n, refusing one seen before or below the window.
// Called under s.mu, after the frame authenticated.
func (s *e2eSession) accept(n uint64) bool {
	if n >= s.top {
		shift := n + 1 - s.top
		if shift >= e2eWindow {
			s.window = [e2eWindow / 64]uint64{}
		} else {
			for ; shift > 0; shift-- {
				var carry uint64
				for i := range s.window { // shift the bitmap left by one
					next := s.window[i] >> 63
					s.window[i] = s.window[i]<<1 | carry
					carry = next
				}
			}
		}
		s.top = n + 1
		s.window[0] |= 1
		return true
	}
	age := s.top - 1 - n
	if age >= e2eWindow {
		return false
	}
	word, bit := age/64, age%64
	if s.window[word]&(1<<bit) != 0 {
		return false
	}
	s.window[word] |= 1 << bit
	return true
}

// sealedParts is the JSON a sealed field carries.
type sealedParts struct {
	Payload  json.RawMessage   `json:"payload,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
	Message  *string           `json:"message,omitempty"`
	Details  map[string]any    `json:"details,omitempty"`
}

// e2eCall is what both ends know of a sealed call: the fields the
// associated data of its later frames repeats.
type e2eCall struct {
	session         *e2eSession
	service, method string
}

func (c *e2eCall) ad(f frame) []byte {
	return e2eAD(f.Kind, f.CallID, c.service, c.method, f.Direction, f.Sequence, c.session.id)
}

// sealFrame moves f's content into its sealed field. clearMetadata stays
// in clear (the Router's own keys on a call.open never come from here).
func (c *e2eCall) sealFrame(f frame) frame {
	var parts sealedParts
	switch f.Kind {
	case "call.open", "stream.item", "call.result":
		parts.Payload, parts.Metadata = f.Payload, f.Metadata
		f.Payload, f.Metadata = json.RawMessage("null"), nil
	case "call.error":
		if f.Error == nil {
			return f
		}
		message := f.Error.Message
		parts.Message, parts.Details, parts.Metadata = &message, f.Error.Details, f.Metadata
		e := *f.Error
		e.Message, e.Details = e2eSealedNotice, nil
		f.Error, f.Metadata = &e, nil
	default:
		return f // windows, ends and cancels carry no content
	}
	if f.Kind == "call.open" {
		f.E2ESession = c.session.id
	}
	plaintext, _ := json.Marshal(parts)
	f.Sealed = c.session.seal(plaintext, c.ad(f))
	return f
}

// openFrame restores f's content from its sealed field. Content frames
// without one did not come from the other end and are refused; a clear
// call.error (the Router's own) is kept as it is.
func (c *e2eCall) openFrame(f frame) (frame, error) {
	if f.Sealed == "" {
		switch f.Kind {
		case "stream.item", "call.result", "call.open":
			return f, errors.New("a frame of a sealed call arrived in clear")
		}
		return f, nil
	}
	plaintext, err := c.session.open(f.Sealed, c.ad(f))
	if err != nil {
		return f, err
	}
	var parts sealedParts
	if err := json.Unmarshal(plaintext, &parts); err != nil {
		return f, errors.New("sealed field is not a sealed object")
	}
	for key := range parts.Metadata {
		if isReservedKey(key) { // reserved keys are the Router's, in clear
			delete(parts.Metadata, key)
		}
	}
	f.Sealed = ""
	switch f.Kind {
	case "call.error":
		if f.Error != nil {
			e := *f.Error
			if parts.Message != nil {
				e.Message = *parts.Message
			}
			e.Details = parts.Details
			f.Error = &e
		}
		f.Metadata = parts.Metadata
	default:
		payload := parts.Payload
		if payload == nil {
			payload = json.RawMessage("null")
		}
		f.Payload = payload
		if f.Kind == "call.open" {
			// The Router's clear keys (tunnel identity) win over sealed ones.
			merged := map[string]string{}
			for k, v := range parts.Metadata {
				merged[k] = v
			}
			for k, v := range f.Metadata {
				merged[k] = v
			}
			f.Metadata = merged
		} else {
			f.Metadata = parts.Metadata
		}
	}
	return f, nil
}

// e2eSessions is an instance's sessions under each tunnel, oldest first.
type e2eSessions struct {
	mu       sync.Mutex
	byTunnel map[string][]*e2eSession
}

func (s *e2eSessions) add(tunnelID string, sess *e2eSession) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.byTunnel == nil {
		s.byTunnel = map[string][]*e2eSession{}
	}
	list := append(s.byTunnel[tunnelID], sess)
	if len(list) > e2eMaxSessions {
		list = list[len(list)-e2eMaxSessions:]
	}
	s.byTunnel[tunnelID] = list
}

func (s *e2eSessions) find(tunnelID, id string) *e2eSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sess := range s.byTunnel[tunnelID] {
		if sess.id == id {
			return sess
		}
	}
	return nil
}

func (s *e2eSessions) drop(tunnelID string) {
	s.mu.Lock()
	delete(s.byTunnel, tunnelID)
	s.mu.Unlock()
}
