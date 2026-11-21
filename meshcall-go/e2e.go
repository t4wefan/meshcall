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
// Noise handshake per client connection (NK, or IK / IKpsk2 when the client
// authenticates itself), then the payloads, metadata and error messages of every call travel sealed while
// the Router keeps routing on the clear header.

const (
	e2ePrologue     = "meshcall-e2e/1"
	e2eWindow       = 1024
	e2eMaxSessions  = 16
	e2eSealedNotice = "sealed"
)

var b64 = base64.RawURLEncoding

// GenerateE2EKey makes a key pair for end-to-end sealed tunnels. For an
// instance: configure privateKey on it (WithE2EPrivateKey) and give
// publicKey to the issuer, which pins it in Tunnels().Open (E2EKey). For a
// client: its E2EIdentity.PrivateKey. Both are base64url without padding.
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

// GenerateE2EPSK makes a pairing secret: 32 random bytes, base64url without
// padding. The instance's owner hands it to a new client out of band (never
// through the Router) with a pairing id; see WithE2EPairingPSK.
func GenerateE2EPSK() (string, error) {
	psk := make([]byte, 32)
	if _, err := rand.Read(psk); err != nil {
		return "", err
	}
	return b64.EncodeToString(psk), nil
}

// E2EPairing is a one-time pairing the instance's owner handed the client
// out of band (never through the Router): its id (1 to 128 bytes) and its
// 32-byte PSK, base64url without padding (GenerateE2EPSK).
type E2EPairing struct {
	ID  string
	PSK string
}

// E2EIdentity is a client's own key for end-to-end tunnels: with it the
// client authenticates itself to the instance (Noise IK).
//
// PrivateKey is this client's static X25519 key (GenerateE2EKey, kept by
// the app); InstanceID the instance whose key the client pinned with
// WithE2EKey, bound in the handshake. Pairing makes the first handshake a
// pairing (IKpsk2); once the instance confirmed it, every later handshake
// of the Mesh uses IK.
type E2EIdentity struct {
	PrivateKey string
	InstanceID string
	Pairing    *E2EPairing
}

// E2EPeer is the end-to-end session of a call (Context.E2E), or the client
// an instance is asked to accept (WithAuthorizeE2EPeer).
type E2EPeer struct {
	// Protocol is the handshake: E2EProtocolNK, E2EProtocolIK or E2EProtocolIKpsk2.
	Protocol string
	// Session is the session id (hex).
	Session string
	// PeerKey is the client's static public key (base64url); empty for an NK
	// session, whose client has none.
	PeerKey string
	// PairingID is the pairing the session was made with; empty otherwise.
	PairingID string
	// Tunnel is the tunnel the session belongs to.
	Tunnel *Tunnel
}

const maxPairingID = 128

func checkPairingID(id string) error {
	if len(id) < 1 || len(id) > maxPairingID {
		return errors.New("meshcall: a pairing id is 1 to 128 bytes")
	}
	return nil
}

func parsePSK(s string) ([]byte, error) {
	raw, err := b64.DecodeString(s)
	if err != nil || len(raw) != 32 {
		return nil, errors.New("meshcall: a pairing PSK is 32 bytes, base64url without padding")
	}
	return raw, nil
}

func checkE2EIdentity(identity E2EIdentity) error {
	if _, err := parsePrivateKey(identity.PrivateKey); err != nil {
		return err
	}
	if identity.InstanceID == "" {
		return errors.New("meshcall: E2EIdentity needs the InstanceID whose key you pinned")
	}
	if identity.Pairing != nil {
		if err := checkPairingID(identity.Pairing.ID); err != nil {
			return err
		}
		if _, err := parsePSK(identity.Pairing.PSK); err != nil {
			return err
		}
	}
	return nil
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

// --- Noise NK, IK, IKpsk2 ---------------------------------------------------

// The handshakes a client may use (docs/protocol.md → End-to-end encryption,
// Client authentication): NK when the client has no static key of its own,
// IK when it authenticates itself, IKpsk2 to pair a client the instance does
// not know yet.
const (
	E2EProtocolNK     = "Noise_NK_25519_AESGCM_SHA256"
	E2EProtocolIK     = "Noise_IK_25519_AESGCM_SHA256"
	E2EProtocolIKpsk2 = "Noise_IKpsk2_25519_AESGCM_SHA256"
)

// noisePatterns are the message patterns after the responder's static key
// (<- s), which the initiator of all three knows beforehand.
var noisePatterns = map[string][2][]string{
	E2EProtocolNK:     {{"e", "es"}, {"e", "ee"}},
	E2EProtocolIK:     {{"e", "es", "s", "ss"}, {"e", "ee", "se"}},
	E2EProtocolIKpsk2: {{"e", "es", "s", "ss"}, {"e", "ee", "se", "psk"}},
}

type symmetricState struct {
	ck, h [32]byte
	k     []byte // nil until the first MixKey
	n     uint64
}

func newSymmetricState(protocol string, prologue []byte) *symmetricState {
	s := &symmetricState{}
	copy(s.h[:], protocol) // every name here is at most HASHLEN: zero padded
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

// hkdf is Noise's HKDF with HMAC-SHA256 and n (2 or 3) outputs.
func hkdf(ck []byte, ikm []byte, n int) [][32]byte {
	mac := hmac.New(sha256.New, ck)
	mac.Write(ikm)
	temp := mac.Sum(nil)
	out := make([][32]byte, n)
	var prev []byte
	for i := range n {
		mac = hmac.New(sha256.New, temp)
		mac.Write(prev)
		mac.Write([]byte{byte(i + 1)})
		copy(out[i][:], mac.Sum(nil))
		prev = out[i][:]
	}
	return out
}

func (s *symmetricState) mixKey(ikm []byte) {
	out := hkdf(s.ck[:], ikm, 2)
	s.ck, s.k, s.n = out[0], out[1][:], 0
}

func (s *symmetricState) mixKeyAndHash(ikm []byte) {
	out := hkdf(s.ck[:], ikm, 3)
	s.ck = out[0]
	s.mixHash(out[1][:])
	s.k, s.n = out[2][:], 0
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

// handshake is one end of a two-message Noise handshake towards (or as) a
// responder whose static key the initiator knows: NK, IK or IKpsk2.
type handshake struct {
	protocol  string
	initiator bool
	sym       *symmetricState
	s         *ecdh.PrivateKey // this end's static key (the initiator has one for IK and IKpsk2)
	rs        *ecdh.PublicKey  // the other end's static key: pinned, or read from message 1
	psk       []byte           // IKpsk2 only
	e         *ecdh.PrivateKey
	re        *ecdh.PublicKey
	turn      int
}

// newHandshake starts a handshake. ephemeral is for test vectors only; nil
// draws a fresh key when one is needed.
func newHandshake(protocol string, initiator bool, prologue []byte, s *ecdh.PrivateKey, rs *ecdh.PublicKey, psk []byte, ephemeral *ecdh.PrivateKey) (*handshake, error) {
	if _, ok := noisePatterns[protocol]; !ok {
		return nil, errors.New("unknown handshake " + protocol)
	}
	if (protocol == E2EProtocolIKpsk2) != (psk != nil) {
		return nil, errors.New("IKpsk2, and only IKpsk2, takes a PSK")
	}
	if psk != nil && len(psk) != 32 {
		return nil, errors.New("a pairing PSK is 32 bytes")
	}
	if (protocol != E2EProtocolNK || !initiator) && s == nil {
		return nil, errors.New(protocol + " needs this end's static key")
	}
	if initiator && rs == nil {
		return nil, errors.New("the initiator needs the responder's static key")
	}
	hs := &handshake{protocol: protocol, initiator: initiator, sym: newSymmetricState(protocol, prologue), s: s, psk: psk, e: ephemeral}
	responder := rs
	if initiator {
		hs.rs = rs
	} else {
		responder = s.PublicKey()
	}
	hs.sym.mixHash(responder.Bytes())
	return hs, nil
}

func (hs *handshake) tokens() []string {
	tokens := noisePatterns[hs.protocol][hs.turn]
	hs.turn++
	return tokens
}

// dh computes a DH token: its first letter is the initiator's key, the
// second the responder's.
func (hs *handshake) dh(token string) ([]byte, error) {
	mine, theirs := token[0], token[1]
	if !hs.initiator {
		mine, theirs = theirs, mine
	}
	local, remote := hs.s, hs.rs
	if mine == 'e' {
		local = hs.e
	}
	if theirs == 'e' {
		remote = hs.re
	}
	if local == nil || remote == nil {
		return nil, errors.New("handshake key missing")
	}
	return local.ECDH(remote)
}

func (hs *handshake) writeMessage(payload []byte) ([]byte, error) {
	var out []byte
	for _, token := range hs.tokens() {
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
			hs.sym.mixKeyAndHash(hs.psk)
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

var errShortHandshake = errors.New("short handshake message")

func (hs *handshake) readMessage(msg []byte) ([]byte, error) {
	rest := msg
	for _, token := range hs.tokens() {
		switch token {
		case "e":
			if len(rest) < 32 {
				return nil, errShortHandshake
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
				return nil, errShortHandshake
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
			hs.sym.mixKeyAndHash(hs.psk)
		default:
			shared, err := hs.dh(token)
			if err != nil {
				return nil, err
			}
			hs.sym.mixKey(shared)
		}
	}
	if hs.sym.k != nil && len(rest) < 16 {
		return nil, errShortHandshake
	}
	return hs.sym.decryptAndHash(rest)
}

// split returns the session once both messages are done: the initiator
// sends with the first key, the responder with the second.
func (hs *handshake) split() *e2eSession {
	out := hkdf(hs.sym.ck[:], nil, 2)
	toResponder, toInitiator := out[0], out[1]
	if hs.initiator {
		return newE2ESession(hs.protocol, hs.sym.h, toResponder, toInitiator)
	}
	return newE2ESession(hs.protocol, hs.sym.h, toInitiator, toResponder)
}

// startE2EHandshake writes the client's message 1 and the
// TunnelHost/handshake request carrying it: NK without an identity, IK with
// one, IKpsk2 while its pairing is not confirmed. pairing is non-nil when the
// session needs a confirmation. ephemeral is for test vectors only.
func startE2EHandshake(credential string, pinned *ecdh.PublicKey, identity *E2EIdentity, paired bool, ephemeral *ecdh.PrivateKey) (hs *handshake, request map[string]string, pairing *E2EPairing, err error) {
	digest := e2eCredentialDigest(credential)
	protocol, prologue := E2EProtocolNK, e2eTunnelPrologue(digest)
	var static *ecdh.PrivateKey
	var psk []byte
	if identity != nil {
		protocol, prologue = E2EProtocolIK, e2ePeerPrologue(digest, identity.InstanceID)
		if static, err = parsePrivateKey(identity.PrivateKey); err != nil {
			return nil, nil, nil, err
		}
		if identity.Pairing != nil && !paired {
			pairing = identity.Pairing
			protocol = E2EProtocolIKpsk2
			if psk, err = parsePSK(pairing.PSK); err != nil {
				return nil, nil, nil, err
			}
		}
	}
	if hs, err = newHandshake(protocol, true, prologue, static, pinned, psk, ephemeral); err != nil {
		return nil, nil, nil, err
	}
	msg1, err := hs.writeMessage(nil)
	if err != nil {
		return nil, nil, nil, err
	}
	request = map[string]string{"message": b64.EncodeToString(msg1)}
	if protocol != E2EProtocolNK {
		request["protocol"] = protocol
	}
	if pairing != nil {
		request["pairing_id"] = pairing.ID
	}
	return hs, request, pairing, nil
}

// e2eConfirmRequest proves a pairing client holds the PSK: the sealed {}
// under the confirmation's associated data, the first counter of its
// direction.
func e2eConfirmRequest(sess *e2eSession) map[string]string {
	return map[string]string{"session": sess.id, "confirm": sess.seal([]byte("{}"), e2eConfirmAD(sess.id))}
}

// --- sessions and sealed frames ----------------------------------------------

// e2eSession holds one direction's key and counter for sending and the
// other's for receiving, with a replay window (frames may be dropped or
// reordered by the Router, never accepted twice).
type e2eSession struct {
	id       string
	hash     [32]byte
	protocol string
	send     cipher.AEAD
	recv     cipher.AEAD
	// On the instance, for IK and IKpsk2: the client's static public key
	// (base64url) and the pairing the session was made with.
	peerKey   string
	pairingID string

	mu     sync.Mutex
	sendN  uint64
	top    uint64 // highest counter received + 1; 0: none yet
	window [e2eWindow / 64]uint64
}

func newE2ESession(protocol string, h [32]byte, send, recv [32]byte) *e2eSession {
	return &e2eSession{id: hex.EncodeToString(h[:16]), hash: h, protocol: protocol, send: newGCM(send[:]), recv: newGCM(recv[:])}
}

// peer is what a handler (Context.E2E) and the authorization hook see of
// the session.
func (s *e2eSession) peer(t *Tunnel) E2EPeer {
	return E2EPeer{Protocol: s.protocol, Session: s.id, PeerKey: s.peerKey, PairingID: s.pairingID, Tunnel: t}
}

// e2eTunnelPrologue binds a handshake to one tunnel: the hex SHA-256 of its
// credential, which the client hashes itself and the instance learns from
// the offer.
func e2eTunnelPrologue(credentialDigest string) []byte {
	return []byte(e2ePrologue + "|" + credentialDigest)
}

// e2ePeerPrologue is the IK / IKpsk2 prologue: the tunnel, and the instance
// whose key the client pinned.
func e2ePeerPrologue(credentialDigest, instanceID string) []byte {
	return []byte(e2ePrologue + "|" + credentialDigest + "|" + instanceID)
}

// e2eConfirmAD is the associated data of a pairing confirmation.
func e2eConfirmAD(session string) []byte {
	return []byte(e2ePrologue + "|confirm|" + session)
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

// e2eAD is the associated data binding a sealed part to its frame.
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

// e2eSessions is an instance's sessions under each tunnel, oldest first,
// and as many pairing sessions waiting for their confirmation.
type e2eSessions struct {
	mu       sync.Mutex
	byTunnel map[string][]*e2eSession
	pending  map[string][]*e2eSession
}

func putSession(table map[string][]*e2eSession, tunnelID string, sess *e2eSession) {
	list := append(table[tunnelID], sess)
	if len(list) > e2eMaxSessions {
		list = list[len(list)-e2eMaxSessions:]
	}
	table[tunnelID] = list
}

func (s *e2eSessions) add(tunnelID string, sess *e2eSession) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.byTunnel == nil {
		s.byTunnel = map[string][]*e2eSession{}
	}
	putSession(s.byTunnel, tunnelID, sess)
}

// addPending keeps a pairing session until its client confirms it; calls
// naming it meanwhile find no session.
func (s *e2eSessions) addPending(tunnelID string, sess *e2eSession) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending == nil {
		s.pending = map[string][]*e2eSession{}
	}
	putSession(s.pending, tunnelID, sess)
}

// takePending removes and returns a pairing session: each is confirmed at
// most once.
func (s *e2eSessions) takePending(tunnelID, id string) *e2eSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.pending[tunnelID]
	for i, sess := range list {
		if sess.id == id {
			s.pending[tunnelID] = append(list[:i:i], list[i+1:]...)
			return sess
		}
	}
	return nil
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
	delete(s.pending, tunnelID)
	s.mu.Unlock()
}
