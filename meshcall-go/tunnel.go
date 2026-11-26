package meshcall

import (
	"context"
	"crypto/ecdh"
	"encoding/json"
	"errors"
	"strings"
	"sync"
)

// Tunnels: instance-scoped connections through the Router (docs/protocol.md
// → Tunnels). An issuer opens one with Mesh.Tunnels().Open; the Router offers
// it to the instance, which answers according to WithAcceptTunnels given at
// Mount; calls through an accepted tunnel see it as ctx.Tunnel.

const (
	tunnelService      = "meshcall.router.v1.TunnelService"
	tunnelHostService  = "meshcall.router.v1.TunnelHost"
	tunnelCapability   = "tunnels"
	tunnelPrefix       = "mtun_"
	tunnelIDKey        = "meshcall-tunnel-id"
	tunnelPrincipalKey = "meshcall-tunnel-principal"
)

// TunnelScope names one service of the target instance and the methods a
// tunnel allows on it ("*": every method).
type TunnelScope struct {
	Service string   `json:"service"`
	Methods []string `json:"methods"`
}

// TunnelInfo is a tunnel as the Router lists it, never with its credential.
type TunnelInfo struct {
	TunnelID        string        `json:"tunnel_id"`
	InstanceID      string        `json:"instance_id"`
	Principal       string        `json:"principal"`
	Scopes          []TunnelScope `json:"scopes"`
	ExpiresAtUnixMS int64         `json:"expires_at_unix_ms"`
	MaxConnections  int           `json:"max_connections,omitempty"`
	E2EKey          string        `json:"e2e_key,omitempty"`
	Connections     int           `json:"connections"`
	Online          bool          `json:"online"`
}

// OpenedTunnel is the result of TunnelsClient.Open: hand Credential to the
// client once.
type OpenedTunnel struct {
	Credential string `json:"credential"`
	TunnelInfo
}

// OpenTunnelRequest asks for a tunnel to one instance.
type OpenTunnelRequest struct {
	Instance string
	// Scopes names services of the instance; Methods nil allows every method.
	Scopes    []TunnelScope
	Principal string
	// TTLSeconds bounds new connections, 1..86400 (0: 3600).
	TTLSeconds     int
	MaxConnections int
	// E2EKey pins the instance's public key (base64url): every call on the
	// tunnel is then sealed end to end. Use the key you provisioned the
	// instance with, not one learnt from the Router.
	E2EKey string
}

// TunnelOffer is what an instance is asked to accept before a tunnel
// becomes usable.
type TunnelOffer struct {
	TunnelID        string        `json:"tunnel_id"`
	InstanceID      string        `json:"instance_id"`
	Principal       string        `json:"principal"`
	Scopes          []TunnelScope `json:"scopes"`
	ExpiresAtUnixMS int64         `json:"expires_at_unix_ms"`
	// Resumed: offered again after this instance reconnected.
	Resumed bool `json:"resumed"`
	// E2EKey is the public key the issuer pinned for end-to-end sealing;
	// empty for a plain tunnel. An offer pinning another key is refused.
	E2EKey string `json:"e2e_key,omitempty"`
}

// Tunnel is the tunnel a call arrived through, as the instance accepted it.
type Tunnel struct {
	ID        string
	Principal string
	Scopes    []TunnelScope
	// Sealed: the tunnel carries end-to-end sealed calls only.
	Sealed bool
}

// TunnelClosed tells an instance that a tunnel it accepted has ended.
type TunnelClosed struct {
	TunnelID  string
	Principal string
	Reason    string
}

// TunnelPolicy is how the services of one Mount call answer tunnel offers.
// Accept nil accepts every offer naming them.
type TunnelPolicy struct {
	Accept   func(context.Context, TunnelOffer) bool
	OnClosed func(context.Context, TunnelClosed)
	// RequireE2E refuses offers without an e2e_key that scope these
	// services, so they are never served in clear through a tunnel.
	RequireE2E bool
	// RequireE2EPeer serves these services only inside end-to-end sessions
	// whose client authenticated itself (IK, or a confirmed IKpsk2 pairing)
	// and was accepted by ServerOptions.AuthorizeE2EPeer; it implies
	// RequireE2E. Every other call is refused before any handler with
	// CodePermissionDenied, reason "e2e_peer_required".
	RequireE2EPeer bool
}

// MountOption configures the services of one Mesh.Mount call only.
type MountOption interface{ applyMount(*mountConfig) }

type mountConfig struct{ tunnels *TunnelPolicy }

type mountOptFunc func(*mountConfig)

func (f mountOptFunc) applyMount(c *mountConfig) { f(c) }

// WithAcceptTunnels opts the services of this Mount call in to tunnels:
// decide answers each offer (nil accepts all). An offer is accepted only if
// every service it scopes accepts.
func WithAcceptTunnels(decide func(context.Context, TunnelOffer) bool) MountOption {
	return mountOptFunc(func(c *mountConfig) {
		if c.tunnels == nil {
			c.tunnels = &TunnelPolicy{}
		}
		c.tunnels.Accept = decide
		if decide == nil {
			c.tunnels.Accept = func(context.Context, TunnelOffer) bool { return true }
		}
	})
}

// WithTunnelClosed is told when a tunnel these services accepted ends; it
// needs WithAcceptTunnels in the same Mount call.
func WithTunnelClosed(fn func(context.Context, TunnelClosed)) MountOption {
	return mountOptFunc(func(c *mountConfig) {
		if c.tunnels == nil {
			c.tunnels = &TunnelPolicy{}
		}
		c.tunnels.OnClosed = fn
	})
}

// WithRequireE2E makes the services of this Mount call accept end-to-end
// tunnels only (offers without e2e_key are refused); it needs
// WithAcceptTunnels and WithE2EPrivateKey.
func WithRequireE2E() MountOption {
	return mountOptFunc(func(c *mountConfig) {
		if c.tunnels == nil {
			c.tunnels = &TunnelPolicy{}
		}
		c.tunnels.RequireE2E = true
	})
}

// WithRequireE2EPeer serves the services of this Mount call only to clients
// that authenticated themselves end to end (IK, or a confirmed IKpsk2
// pairing) and that WithAuthorizeE2EPeer accepted. Every other call — through
// a plain tunnel, an NK session, or without a tunnel at all — is refused
// before any handler with CodePermissionDenied, reason "e2e_peer_required".
// It implies WithRequireE2E and needs WithAcceptTunnels, WithE2EPrivateKey
// and WithAuthorizeE2EPeer.
func WithRequireE2EPeer() MountOption {
	return mountOptFunc(func(c *mountConfig) {
		if c.tunnels == nil {
			c.tunnels = &TunnelPolicy{}
		}
		c.tunnels.RequireE2EPeer = true
	})
}

// WithE2EIdentity gives a tunnel client its own static key: it then
// authenticates itself to the instance (Noise IK) instead of staying
// anonymous (NK). With identity.Pairing the first handshake pairs the
// client (IKpsk2) and confirms it before the first call; once the instance
// accepted it, every later handshake of this Mesh (reconnects too) uses IK.
// It never falls back to NK. It needs WithE2EKey; identity.InstanceID is the
// instance whose key you pinned.
func WithE2EIdentity(identity E2EIdentity) MeshOption {
	if err := checkE2EIdentity(identity); err != nil {
		panic(err.Error())
	}
	if identity.Pairing != nil {
		pairing := *identity.Pairing
		identity.Pairing = &pairing
	}
	return meshOptFunc(func(c *meshConfig) { c.e2eIdentity = &identity })
}

// WithTunnel connects the Mesh with a tunnel credential ("mtun_…", from
// TunnelsClient.Open): it then reaches only the tunnel's instance and cannot
// mount services.
func WithTunnel(credential string) MeshOption {
	return meshOptFunc(func(c *meshConfig) { c.tunnel = credential })
}

// WithTunnelSource connects the Mesh with tunnel credentials from source (a
// new tunnel of the same instance), asked before every connection; a Router
// refusing the old one asks it again (docs/protocol.md → Credential
// sources). An end-to-end key pinned with WithE2EKey stays the same.
func WithTunnelSource(source CredentialSource) MeshOption {
	return meshOptFunc(func(c *meshConfig) { c.tunnelSource = source })
}

// WithE2EKey seals every call on the tunnel end to end with the instance's
// public key, as the issuer pinned it (OpenTunnelRequest.E2EKey). Take the
// key from the issuer, never from the Router: the handshake succeeds only
// with the instance that holds the matching private key.
func WithE2EKey(publicKey string) MeshOption {
	return meshOptFunc(func(c *meshConfig) { c.e2eKey = publicKey })
}

// TunnelsClient is the issuer API for the Router's TunnelService.
type TunnelsClient struct{ caller Caller }

type openTunnelWire struct {
	InstanceID     string        `json:"instance_id"`
	Scopes         []TunnelScope `json:"scopes"`
	Principal      string        `json:"principal"`
	TTLSeconds     int           `json:"ttl_seconds"`
	MaxConnections int           `json:"max_connections,omitempty"`
	E2EKey         string        `json:"e2e_key,omitempty"`
}

// Open opens a tunnel to one instance and returns once the instance accepted
// it. A refusal is CodePermissionDenied with reason "tunnel_rejected"; an
// unknown instance CodeUnavailable with reason "instance_unknown".
func (tc *TunnelsClient) Open(ctx context.Context, request OpenTunnelRequest, opts ...CallOption) (OpenedTunnel, error) {
	// The Router checks the request (CodeInvalidArgument), so every runtime fails alike.
	wire := openTunnelWire{InstanceID: request.Instance, Principal: request.Principal, TTLSeconds: request.TTLSeconds,
		MaxConnections: request.MaxConnections, E2EKey: request.E2EKey}
	if wire.TTLSeconds == 0 {
		wire.TTLSeconds = 3600
	}
	for _, s := range request.Scopes {
		name := s.Service
		if normalized, _, err := normalizeServiceName(name); err == nil {
			name = normalized
		}
		methods := s.Methods
		if len(methods) == 0 {
			methods = []string{"*"}
		}
		wire.Scopes = append(wire.Scopes, TunnelScope{Service: name, Methods: methods})
	}
	return Unary[openTunnelWire, OpenedTunnel](ctx, tc.caller, tunnelService, "open", wire, opts...)
}

// Revoke closes a tunnel and every connection on it; false if it is not the
// caller's or already gone.
func (tc *TunnelsClient) Revoke(ctx context.Context, tunnelID string, opts ...CallOption) (bool, error) {
	type request struct {
		TunnelID string `json:"tunnel_id"`
	}
	result, err := Unary[request, struct {
		Revoked bool `json:"revoked"`
	}](ctx, tc.caller, tunnelService, "revoke", request{tunnelID}, opts...)
	return result.Revoked, err
}

// List returns the tunnels the caller opened that are still open.
func (tc *TunnelsClient) List(ctx context.Context, opts ...CallOption) ([]TunnelInfo, error) {
	result, err := Unary[struct{}, struct {
		Tunnels []TunnelInfo `json:"tunnels"`
	}](ctx, tc.caller, tunnelService, "list", struct{}{}, opts...)
	return result.Tunnels, err
}

// tunnelHost answers the Router's TunnelHost calls and keeps accepted tunnels.
type tunnelHost struct {
	policies map[string]*TunnelPolicy
	mu       sync.Mutex
	tunnels  map[string]*Tunnel
	digests  map[string]string // tunnel id → credential digest, for E2E tunnels
	key      *ecdh.PrivateKey  // nil: no end-to-end tunnels
	sessions e2eSessions
	// instanceID is bound in the IK / IKpsk2 prologue.
	instanceID string
	authorize  func(context.Context, E2EPeer) bool
	pairingPSK func(context.Context, string) (string, bool)
	// ephemeral is for test vectors only (nil: a fresh key per handshake).
	ephemeral *ecdh.PrivateKey
}

func newTunnelHost(policies map[string]*TunnelPolicy) *tunnelHost {
	return &tunnelHost{policies: policies, tunnels: map[string]*Tunnel{}, digests: map[string]string{}}
}

func (h *tunnelHost) enabled() bool { return len(h.policies) > 0 }

// lookup returns the accepted tunnel a call names. The instance's own table
// is authoritative: an id it never accepted (a forged key) is no tunnel.
func (h *tunnelHost) lookup(metadata map[string]string) *Tunnel {
	id, ok := metadata[tunnelIDKey]
	if !ok {
		return nil
	}
	h.mu.Lock()
	t := h.tunnels[id]
	h.mu.Unlock()
	if t == nil || metadata[tunnelPrincipalKey] != t.Principal {
		return nil
	}
	return t
}

func (h *tunnelHost) handle(ctx context.Context, method string, payload json.RawMessage, metadata map[string]string) (any, error) {
	switch method {
	case "offer":
		var offer TunnelOffer
		var binding struct {
			CredentialDigest string `json:"credential_digest"`
		}
		if err := json.Unmarshal(payload, &offer); err != nil || offer.TunnelID == "" {
			return nil, errors.New("invalid tunnel offer")
		}
		_ = json.Unmarshal(payload, &binding)
		// An E2E tunnel pins a key: only the instance that holds it may accept.
		if offer.E2EKey != "" && (!h.ownKey(offer.E2EKey) || binding.CredentialDigest == "") {
			return map[string]any{"accept": false, "reason": "e2e_key_mismatch"}, nil
		}
		if offer.E2EKey == "" && h.requiresE2E(offer) {
			return map[string]any{"accept": false, "reason": "e2e_required"}, nil
		}
		accept := h.decide(ctx, offer)
		if accept {
			h.mu.Lock()
			h.tunnels[offer.TunnelID] = &Tunnel{ID: offer.TunnelID, Principal: offer.Principal, Scopes: offer.Scopes, Sealed: offer.E2EKey != ""}
			if offer.E2EKey != "" {
				h.digests[offer.TunnelID] = binding.CredentialDigest
			}
			h.mu.Unlock()
		}
		return map[string]bool{"accept": accept}, nil
	case "handshake":
		return h.handshake(ctx, metadata, payload)
	case "closed":
		var closed struct {
			TunnelID string `json:"tunnel_id"`
			Reason   string `json:"reason"`
		}
		_ = json.Unmarshal(payload, &closed)
		h.mu.Lock()
		t := h.tunnels[closed.TunnelID]
		delete(h.tunnels, closed.TunnelID)
		delete(h.digests, closed.TunnelID)
		h.mu.Unlock()
		h.sessions.drop(closed.TunnelID)
		if t != nil {
			notified := map[*TunnelPolicy]bool{} // services mounted together share a policy
			for _, scope := range t.Scopes {
				policy := h.policies[scope.Service]
				if policy == nil || policy.OnClosed == nil || notified[policy] {
					continue
				}
				notified[policy] = true
				policy.OnClosed(ctx, TunnelClosed{TunnelID: t.ID, Principal: t.Principal, Reason: closed.Reason})
			}
		}
		return struct{}{}, nil
	}
	return nil, errors.New("unknown " + tunnelHostService + " method " + method)
}

func (h *tunnelHost) ownKey(publicKey string) bool {
	if h.key == nil {
		return false
	}
	pinned, err := parsePublicKey(publicKey)
	return err == nil && pinned.Equal(h.key.PublicKey())
}

// handshake answers TunnelHost/handshake on an E2E tunnel this instance
// accepted: a client's message 1 (NK, IK or IKpsk2), or a pairing's
// confirmation. Sessions are kept under that tunnel; a pairing's waits for
// its confirmation.
func (h *tunnelHost) handshake(ctx context.Context, metadata map[string]string, payload json.RawMessage) (any, error) {
	failed := func(why string) error {
		return detailed(CodeUnauthenticated, "End-to-end handshake failed: "+why, "server", "e2e_handshake_failed", false)
	}
	t := h.lookup(metadata)
	h.mu.Lock()
	digest := ""
	if t != nil {
		digest = h.digests[t.ID]
	}
	h.mu.Unlock()
	if t == nil || !t.Sealed || h.key == nil || digest == "" {
		return nil, failed("not an end-to-end tunnel of this instance")
	}
	var request struct {
		Message   string  `json:"message"`
		Protocol  string  `json:"protocol"`
		PairingID string  `json:"pairing_id"`
		Session   string  `json:"session"`
		Confirm   *string `json:"confirm"`
	}
	if err := json.Unmarshal(payload, &request); err != nil {
		return nil, failed("malformed request")
	}
	if request.Confirm != nil {
		return h.confirm(ctx, t, request.Session, *request.Confirm)
	}
	// Every refusal reads the same: a client cannot probe what failed.
	refused := failed("message 1 does not open with this instance's key on this tunnel")
	protocol := request.Protocol
	if protocol == "" {
		protocol = E2EProtocolNK
	}
	if _, ok := noisePatterns[protocol]; !ok {
		return nil, refused
	}
	var psk []byte
	pairingID := ""
	if protocol == E2EProtocolIKpsk2 {
		pairingID = request.PairingID
		if checkPairingID(pairingID) != nil || h.pairingPSK == nil {
			return nil, refused
		}
		found, answered := askHook(ctx, func(ctx context.Context) hookAnswer {
			value, ok := h.pairingPSK(ctx, pairingID)
			return hookAnswer{value, ok}
		})
		if !answered || !found.ok {
			return nil, refused
		}
		var err error
		if psk, err = parsePSK(found.value); err != nil {
			return nil, refused
		}
	}
	message, err := b64.DecodeString(request.Message)
	if err != nil {
		return nil, refused
	}
	// The prologue binds the tunnel the handshake came through (and, for IK
	// and IKpsk2, this instance): one made for another does not open here.
	prologue := e2eTunnelPrologue(digest)
	if protocol != E2EProtocolNK {
		prologue = e2ePeerPrologue(digest, h.instanceID)
	}
	hs, err := newHandshake(protocol, false, prologue, h.key, nil, psk, h.ephemeral)
	if err != nil {
		return nil, refused
	}
	if _, err := hs.readMessage(message); err != nil {
		return nil, refused
	}
	reply, err := hs.writeMessage(nil)
	if err != nil {
		return nil, refused
	}
	sess := hs.split()
	if protocol != E2EProtocolNK {
		sess.peerKey = b64.EncodeToString(hs.rs.Bytes())
	}
	sess.pairingID = pairingID
	switch protocol {
	case E2EProtocolIKpsk2:
		// Message 1 does not prove the client knows the PSK: wait for its
		// confirmation before anyone trusts this key.
		h.sessions.addPending(t.ID, sess)
	case E2EProtocolIK:
		if !h.authorized(ctx, t, sess) {
			return nil, refused
		}
		h.sessions.add(t.ID, sess)
	default:
		h.sessions.add(t.ID, sess)
	}
	return map[string]string{"message": b64.EncodeToString(reply)}, nil
}

// confirm: a pairing client proves it holds the PSK; only then is the
// application asked to accept it.
func (h *tunnelHost) confirm(ctx context.Context, t *Tunnel, session, confirm string) (any, error) {
	refused := detailed(CodeUnauthenticated, "End-to-end handshake failed: pairing not confirmed", "server", "e2e_handshake_failed", false)
	sess := h.sessions.takePending(t.ID, session)
	if sess == nil {
		return nil, refused
	}
	opened, err := sess.open(confirm, e2eConfirmAD(sess.id))
	if err != nil || string(opened) != "{}" || !h.authorized(ctx, t, sess) {
		return nil, refused
	}
	h.sessions.add(t.ID, sess)
	return struct{}{}, nil
}

// authorized asks the application whether it accepts the session's client;
// no hook accepts any client key.
func (h *tunnelHost) authorized(ctx context.Context, t *Tunnel, sess *e2eSession) bool {
	if h.authorize == nil {
		return true
	}
	accepted, answered := askHook(ctx, func(ctx context.Context) bool { return h.authorize(ctx, sess.peer(t)) })
	return answered && accepted
}

// hookAnswer is a lookup hook's two results.
type hookAnswer struct {
	value string
	ok    bool
}

// askHook runs an application hook, which may wait for a person: a panic,
// or no answer before ctx ends, is no answer (answered false).
func askHook[T any](ctx context.Context, hook func(context.Context) T) (result T, answered bool) {
	type outcome struct {
		value T
		ok    bool
	}
	done := make(chan outcome, 1)
	go func() {
		defer func() {
			if recover() != nil {
				done <- outcome{}
			}
		}()
		done <- outcome{hook(ctx), true}
	}()
	select {
	case o := <-done:
		return o.value, o.ok
	case <-ctx.Done():
		return result, false
	}
}

// requiresPeer: service was mounted with WithRequireE2EPeer.
func (h *tunnelHost) requiresPeer(service string) bool {
	policy := h.policies[service]
	return policy != nil && policy.RequireE2EPeer
}

// requiresE2E: a scoped service was mounted with WithRequireE2E.
func (h *tunnelHost) requiresE2E(offer TunnelOffer) bool {
	for _, scope := range offer.Scopes {
		if policy := h.policies[scope.Service]; policy != nil && (policy.RequireE2E || policy.RequireE2EPeer) {
			return true
		}
	}
	return false
}

// decide: every scoped service must have opted in, and every hook agree.
func (h *tunnelHost) decide(ctx context.Context, offer TunnelOffer) bool {
	for _, scope := range offer.Scopes {
		policy := h.policies[scope.Service]
		if policy == nil || (policy.Accept != nil && !policy.Accept(ctx, offer)) {
			return false
		}
	}
	return true
}

func checkTunnelCredential(credential string) {
	if !strings.HasPrefix(credential, tunnelPrefix) || strings.ContainsAny(credential, " \r\n") {
		panic("meshcall: a tunnel credential starts with \"mtun_\"; it is the Credential returned by Mesh.Tunnels().Open")
	}
}
