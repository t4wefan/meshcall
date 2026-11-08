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

// WithTunnel connects the Mesh with a tunnel credential ("mtun_…", from
// TunnelsClient.Open): it then reaches only the tunnel's instance and cannot
// mount services.
func WithTunnel(credential string) MeshOption {
	return meshOptFunc(func(c *meshConfig) { c.tunnel = credential })
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
	key      *ecdh.PrivateKey // nil: no end-to-end tunnels
	sessions e2eSessions
}

func newTunnelHost(policies map[string]*TunnelPolicy) *tunnelHost {
	return &tunnelHost{policies: policies, tunnels: map[string]*Tunnel{}}
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
		if err := json.Unmarshal(payload, &offer); err != nil || offer.TunnelID == "" {
			return nil, errors.New("invalid tunnel offer")
		}
		// An E2E tunnel pins a key: only the instance that holds it may accept.
		accept := (offer.E2EKey == "" || h.ownKey(offer.E2EKey)) && h.decide(ctx, offer)
		if accept {
			h.mu.Lock()
			h.tunnels[offer.TunnelID] = &Tunnel{ID: offer.TunnelID, Principal: offer.Principal, Scopes: offer.Scopes, Sealed: offer.E2EKey != ""}
			h.mu.Unlock()
		}
		return map[string]bool{"accept": accept}, nil
	case "handshake":
		return h.handshake(metadata, payload)
	case "closed":
		var closed struct {
			TunnelID string `json:"tunnel_id"`
			Reason   string `json:"reason"`
		}
		_ = json.Unmarshal(payload, &closed)
		h.mu.Lock()
		t := h.tunnels[closed.TunnelID]
		delete(h.tunnels, closed.TunnelID)
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

// handshake answers a client's Noise NK message 1 on an E2E tunnel this
// instance accepted, and keeps the session under that tunnel.
func (h *tunnelHost) handshake(metadata map[string]string, payload json.RawMessage) (any, error) {
	failed := func(why string) error {
		return detailed(CodeUnauthenticated, "End-to-end handshake failed: "+why, "server", "e2e_handshake_failed", false)
	}
	t := h.lookup(metadata)
	if t == nil || !t.Sealed || h.key == nil {
		return nil, failed("not an end-to-end tunnel of this instance")
	}
	var request struct {
		Message string `json:"message"`
	}
	message, err := []byte(nil), json.Unmarshal(payload, &request)
	if err == nil {
		message, err = b64.DecodeString(request.Message)
	}
	if err != nil {
		return nil, failed("malformed message")
	}
	reply, _, sess, err := nkRespond(h.key, nil, []byte(e2ePrologue), message, nil)
	if err != nil {
		return nil, failed("message 1 does not open with this instance's key")
	}
	h.sessions.add(t.ID, sess)
	return map[string]string{"message": b64.EncodeToString(reply)}, nil
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
