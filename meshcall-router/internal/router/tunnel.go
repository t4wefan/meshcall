package router

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"time"
)

// Tunnels (docs/protocol.md → Tunnels). An issuer opens a tunnel for one
// instance; once the instance accepts the offer, a client holding the tunnel
// credential connects and the Router behaves towards it like that instance's
// own Router: names resolve only to the target instance within the tunnel's
// scopes, the Registry and OpenAPI show only that view, and every call.open
// carries the tunnel's identity in reserved metadata the client cannot set.
//
// Tunnels live in this Router's memory only. After a Router restart clients
// ask the issuer again; instances rebuild their tunnel table from offers.

const (
	TunnelService     = "meshcall.router.v1.TunnelService"
	TunnelHostService = "meshcall.router.v1.TunnelHost" // served by instances
	tunnelPrefix      = "mtun_"
	// TunnelCapability in a server's hello says the instance answers offers.
	TunnelCapability   = "tunnels"
	tunnelIDKey        = "meshcall-tunnel-id"
	tunnelPrincipalKey = "meshcall-tunnel-principal"
	tunnelMetaPrefix   = "meshcall-tunnel-"
	offerTimeout       = 10 * time.Second
	maxTunnels         = 65536
	maxTunnelTTL       = 86400
	maxPrincipal       = 512
)

// TunnelScope names one service of the target instance and the methods a
// tunnel allows on it ("*": every method the instance serves).
type TunnelScope struct {
	Service string   `json:"service"`
	Methods []string `json:"methods"`
}

type OpenTunnelRequest struct {
	InstanceID     string        `json:"instance_id"`
	Scopes         []TunnelScope `json:"scopes"`
	Principal      string        `json:"principal"`
	TTLSeconds     int           `json:"ttl_seconds"`
	MaxConnections int           `json:"max_connections,omitempty"`
	// E2EKey pins the instance's X25519 public key (base64url): calls on the
	// tunnel must then be sealed end to end (docs/protocol.md → End-to-end
	// encryption).
	E2EKey string `json:"e2e_key,omitempty"`
}

type tunnel struct {
	id             string
	digest         [32]byte
	owner          string // the issuer's username; "" in development mode
	instanceID     string
	principal      string
	scopes         []TunnelScope
	expires        time.Time
	maxConnections int // 0: unlimited
	e2eKey         string
	connections    int // WebSocket connections and HTTP requests in progress
	host           *peer
	hostUser       string // identity that accepted; a re-registration must match
	closed         bool
	expiry         *time.Timer
	user           *User // the identity of every connection on the tunnel
}

// TunnelInfo is a tunnel as TunnelService/list and Snapshot report it,
// without its credential.
type TunnelInfo struct {
	TunnelID       string        `json:"tunnel_id"`
	InstanceID     string        `json:"instance_id"`
	Principal      string        `json:"principal"`
	Scopes         []TunnelScope `json:"scopes"`
	ExpiresUnixMS  int64         `json:"expires_at_unix_ms"`
	MaxConnections int           `json:"max_connections,omitempty"`
	E2EKey         string        `json:"e2e_key,omitempty"`
	Connections    int           `json:"connections"`
	Online         bool          `json:"online"`
}

func (t *tunnel) info() TunnelInfo {
	return TunnelInfo{TunnelID: t.id, InstanceID: t.instanceID, Principal: t.principal, Scopes: t.scopes,
		ExpiresUnixMS: t.expires.UnixMilli(), MaxConnections: t.maxConnections, E2EKey: t.e2eKey, Connections: t.connections, Online: t.host != nil}
}

// offer is a TunnelHost/offer call in flight to an instance. client is the
// issuer waiting on TunnelService/open; nil for a re-offer after the
// instance reconnected, or once the issuer gave up.
type offer struct {
	callID     string
	tunnel     *tunnel
	instance   *peer
	client     *peer
	clientCall string
	credential string
	timer      *time.Timer
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func isTunnelCredential(header string) bool {
	scheme, token, ok := strings.Cut(header, " ")
	return ok && strings.EqualFold(scheme, "Bearer") && strings.HasPrefix(token, tunnelPrefix)
}

// lookupTunnel finds the open, unexpired tunnel for an Authorization value.
// Called under r.mu.
func (r *Router) lookupTunnel(header string) *tunnel {
	_, token, _ := strings.Cut(header, " ")
	t := r.tunnelsByDigest[sha256.Sum256([]byte(token))]
	if t == nil || t.closed || !time.Now().Before(t.expires) {
		return nil
	}
	return t
}

// checkTunnel authenticates a tunnel credential without admitting a
// connection; admit counts it. Expiry bounds new connections only.
func (r *Router) checkTunnel(header string) (*User, *authFailure) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t := r.lookupTunnel(header); t != nil {
		if t.maxConnections > 0 && t.connections >= t.maxConnections {
			return nil, &authFailure{status: http.StatusTooManyRequests, message: "tunnel connection limit reached"}
		}
		return t.user, nil
	}
	return nil, &authFailure{status: http.StatusUnauthorized, message: "authentication failed"}
}

// authenticateRequest is Auth.check that also accepts tunnel credentials,
// which belong to the Router (they name live instances), not to the auth file.
func (r *Router) authenticateRequest(header string) (*User, *authFailure) {
	if isTunnelCredential(header) {
		return r.checkTunnel(header)
	}
	return r.config.Auth.checkHeader(header)
}

func (u *User) tunnelOrNil() *tunnel {
	if u == nil {
		return nil
	}
	return u.tunnel
}

// admit counts a new connection (or HTTP request) against its identity.
// Called under r.mu; remove releases it.
func (r *Router) admit(p *peer) bool {
	if !r.config.Auth.admits(p.user) {
		return false
	}
	if p.user == nil || p.user.tunnel == nil {
		if p.user != nil && p.user.tokenID != "" && !p.tokenCounted {
			r.config.Auth.countConnection(p.user, 1)
			p.tokenCounted = true
		}
		return true
	}
	t := p.user.tunnel
	if t.closed || !time.Now().Before(t.expires) || (t.maxConnections > 0 && t.connections >= t.maxConnections) {
		return false
	}
	t.connections++
	p.admitted = true
	return true
}

// release undoes admit. A tunnel that has expired closes with its last
// connection: nothing can use it any more. Called under r.mu.
func (r *Router) release(p *peer) {
	if p.tokenCounted {
		r.config.Auth.countConnection(p.user, -1)
		p.tokenCounted = false
	}
	if p.user == nil || p.user.tunnel == nil || !p.admitted {
		return
	}
	p.admitted = false
	t := p.user.tunnel
	t.connections--
	if !t.closed && t.connections == 0 && !time.Now().Before(t.expires) {
		r.closeTunnel(t, "expired")
	}
}

// mayOpenTunnels: accounts and API keys may issue; temporary tokens, the
// anonymous user and tunnel connections are delegated or public identities.
func (u *User) mayOpenTunnels() bool {
	return u == nil || u.kind == identityAccount || u.kind == identityAPIKey
}

func (u *User) tunnelOf() *tunnel {
	if u == nil {
		return nil
	}
	return u.tunnel
}

func (u *User) owner() string {
	if u == nil {
		return ""
	}
	return u.Username
}

// tunnelService serves meshcall.router.v1.TunnelService. Called under r.mu.
func (r *Router) tunnelService(p *peer, f frame) {
	fail := func(code, message string) { p.send(sourcedError(f.CallID, code, message)) }
	if !p.user.mayOpenTunnels() {
		fail("permission_denied", "This identity cannot issue tunnels")
		return
	}
	var result any
	switch f.Method {
	case "open":
		var request OpenTunnelRequest
		if !readRequest(f.Payload, &request) {
			fail("invalid_argument", "Invalid open request")
			return
		}
		r.openTunnel(p, f, request)
		return
	case "revoke":
		var request struct {
			TunnelID string `json:"tunnel_id"`
		}
		if !readRequest(f.Payload, &request) || request.TunnelID == "" {
			fail("invalid_argument", "tunnel_id is required")
			return
		}
		t := r.tunnels[request.TunnelID]
		revoked := t != nil && t.owner == p.user.owner()
		if revoked {
			r.closeTunnel(t, "revoked")
		}
		result = struct {
			Revoked bool `json:"revoked"`
		}{revoked}
	case "list":
		var request struct{}
		if !readRequest(f.Payload, &request) {
			fail("invalid_argument", "list expects an empty object")
			return
		}
		tunnels := []TunnelInfo{}
		for _, t := range r.tunnels {
			if t.owner == p.user.owner() {
				tunnels = append(tunnels, t.info())
			}
		}
		slices.SortFunc(tunnels, func(a, b TunnelInfo) int { return strings.Compare(a.TunnelID, b.TunnelID) })
		result = struct {
			Tunnels []TunnelInfo `json:"tunnels"`
		}{tunnels}
	default:
		fail("method_not_found", "Unknown TunnelService method "+f.Method)
		return
	}
	payload, _ := json.Marshal(result)
	p.send(frame{Kind: "call.result", CallID: f.CallID, Payload: payload})
}

// openTunnel validates a request and offers the tunnel to its instance; the
// issuer's call is answered when the instance does. Called under r.mu.
func (r *Router) openTunnel(p *peer, f frame, request OpenTunnelRequest) {
	fail := func(code, message string) { p.send(sourcedError(f.CallID, code, message)) }
	if request.InstanceID == "" || request.Principal == "" || len(request.Principal) > maxPrincipal {
		fail("invalid_argument", "instance_id and a principal of 1..512 bytes are required")
		return
	}
	if request.TTLSeconds < 1 || request.TTLSeconds > maxTunnelTTL || request.MaxConnections < 0 ||
		len(request.Scopes) == 0 || len(request.Scopes) > 128 {
		fail("invalid_argument", "ttl_seconds must be 1..86400, max_connections non-negative and scopes 1..128 services")
		return
	}
	if request.E2EKey != "" {
		if key, err := base64.RawURLEncoding.DecodeString(request.E2EKey); err != nil || len(key) != 32 {
			fail("invalid_argument", "e2e_key must be a 32-byte X25519 public key, base64url without padding")
			return
		}
	}
	target := r.instances[request.InstanceID]
	if target == nil {
		p.send(routerError(f.CallID, "unavailable", "Instance "+request.InstanceID+" is not registered", false, "instance_unknown", request.InstanceID))
		return
	}
	if target.draining {
		p.send(routerError(f.CallID, "unavailable", "Instance "+request.InstanceID+" is draining", true, "instance_draining", request.InstanceID))
		return
	}
	seen := map[string]bool{}
	for _, scope := range request.Scopes {
		if scope.Service == "" || seen[scope.Service] || len(scope.Methods) == 0 ||
			!slices.ContainsFunc(target.services, func(s service) bool { return s.Name == scope.Service }) {
			fail("invalid_argument", "scopes require unique services the instance registers, each with methods")
			return
		}
		seen[scope.Service] = true
		for _, m := range scope.Methods {
			if m == "" || strings.Contains(m, "/") || (strings.Contains(m, "*") && m != "*") || (m != "*" && !target.serves(scope.Service, m)) {
				fail("invalid_argument", "scope method "+scope.Service+"/"+m+" is not served by the instance")
				return
			}
			// A tunnel delegates the issuer's own call permission, no more.
			if !p.user.allowsCall(scope.Service, m) {
				fail("permission_denied", "requested tunnel scope exceeds issuer permissions")
				return
			}
		}
	}
	if !slices.Contains(target.capabilities, TunnelCapability) {
		p.send(routerError(f.CallID, "permission_denied", "Instance "+request.InstanceID+" does not accept tunnels", false, "tunnel_rejected", request.InstanceID))
		return
	}
	if len(r.tunnels)+len(r.offers) >= maxTunnels {
		p.send(routerError(f.CallID, "resource_exhausted", "Router tunnel capacity reached", true, "overloaded", ""))
		return
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		fail("internal", "Could not create a tunnel credential")
		return
	}
	credential := tunnelPrefix + base64.RawURLEncoding.EncodeToString(secret)
	if request.E2EKey != "" {
		// Clients tell an end-to-end credential from a plain one before they
		// connect; "." never occurs in a plain one.
		credential = tunnelPrefix + "e2e." + base64.RawURLEncoding.EncodeToString(secret)
	}
	t := &tunnel{
		id: "tun_" + randomHex(12), digest: sha256.Sum256([]byte(credential)), owner: p.user.owner(),
		instanceID: request.InstanceID, principal: request.Principal, scopes: request.Scopes,
		expires: time.Now().Add(time.Duration(request.TTLSeconds) * time.Second), maxConnections: request.MaxConnections,
		e2eKey: request.E2EKey,
	}
	call := []string{RegistryService + "/list", RegistryService + "/describe"}
	if t.e2eKey != "" {
		call = append(call, TunnelHostService+"/handshake")
	}
	for _, scope := range t.scopes {
		for _, m := range scope.Methods {
			call = append(call, scope.Service+"/"+m)
		}
	}
	t.user = &User{Username: "tunnel:" + t.id, Roles: []string{"client"}, Call: call, kind: identityTunnel, tunnel: t}
	r.sendOffer(t, target, p, f.CallID, credential, false)
}

// sendOffer asks an instance to accept a tunnel. Called under r.mu.
func (r *Router) sendOffer(t *tunnel, instance *peer, client *peer, clientCall, credential string, resumed bool) {
	o := &offer{callID: "meshcall-router-offer-" + randomHex(16), tunnel: t, instance: instance, client: client, clientCall: clientCall, credential: credential}
	payload, _ := json.Marshal(struct {
		TunnelID      string        `json:"tunnel_id"`
		InstanceID    string        `json:"instance_id"`
		Principal     string        `json:"principal"`
		Scopes        []TunnelScope `json:"scopes"`
		ExpiresUnixMS int64         `json:"expires_at_unix_ms"`
		Resumed       bool          `json:"resumed"`
		E2EKey        string        `json:"e2e_key,omitempty"`
		// The handshake prologue binds a session to this credential; the
		// instance learns its digest, never the credential.
		CredentialDigest string `json:"credential_digest,omitempty"`
	}{t.id, t.instanceID, t.principal, t.scopes, t.expires.UnixMilli(), resumed, t.e2eKey, t.credentialDigest()})
	timeout := offerTimeout.Milliseconds()
	r.offers[o.callID] = o
	if client != nil {
		r.opening[clientCall] = o
	}
	o.timer = time.AfterFunc(offerTimeout, func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.offers[o.callID] == o {
			r.endOffer(o)
			o.instance.send(frame{Kind: "call.cancel", CallID: o.callID, Reason: "deadline_exceeded"})
			r.offerRefused(o, deadlineError(o.clientCall), "offer_timeout")
		}
	})
	instance.send(frame{Kind: "call.open", CallID: o.callID, Service: TunnelHostService, Method: "offer", Payload: payload, TimeoutMS: &timeout})
}

func (r *Router) endOffer(o *offer) {
	o.timer.Stop()
	delete(r.offers, o.callID)
	if o.client != nil && r.opening[o.clientCall] == o {
		delete(r.opening, o.clientCall)
	}
}

// answerOffer handles the instance's terminal frame for an offer. Called
// under r.mu.
func (r *Router) answerOffer(o *offer, f frame) {
	if f.Kind != "call.result" && f.Kind != "call.error" {
		return
	}
	r.endOffer(o)
	var answer struct {
		Accept bool   `json:"accept"`
		Reason string `json:"reason"`
	}
	accepted := f.Kind == "call.result" && json.Unmarshal(f.Payload, &answer) == nil && answer.Accept
	t := o.tunnel
	if !accepted {
		message, reason := "Instance "+t.instanceID+" refused the tunnel", "rejected"
		switch answer.Reason {
		case "e2e_key_mismatch":
			message, reason = message+": it does not hold the pinned e2e_key", "key_changed"
		case "e2e_required":
			message += ": it requires end-to-end tunnels (open with e2e_key)"
		}
		r.offerRefused(o, routerError(o.clientCall, "permission_denied", message, false, "tunnel_rejected", t.instanceID), reason)
		return
	}
	if t.closed || r.instances[t.instanceID] != o.instance {
		r.notifyClosed(o.instance, t, "revoked")
		return
	}
	if r.tunnels[t.id] == t { // a re-offer: the reconnected instance takes over
		t.host, t.hostUser = o.instance, o.instance.user.owner()
		return
	}
	if o.client == nil { // the issuer gave up while the instance decided
		r.notifyClosed(o.instance, t, "abandoned")
		return
	}
	t.host, t.hostUser = o.instance, o.instance.user.owner()
	r.tunnels[t.id], r.tunnelsByDigest[t.digest] = t, t
	t.expiry = time.AfterFunc(time.Until(t.expires), func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if !t.closed && t.connections == 0 {
			r.closeTunnel(t, "expired")
		}
	})
	payload, _ := json.Marshal(struct {
		Credential string `json:"credential"`
		TunnelInfo
	}{o.credential, t.info()})
	o.client.send(frame{Kind: "call.result", CallID: o.clientCall, Payload: payload})
}

// offerRefused ends an offer the instance refused, did not answer, or lost.
// A refused re-offer closes the tunnel. Called under r.mu.
func (r *Router) offerRefused(o *offer, issuerError frame, reason string) {
	t := o.tunnel
	if r.tunnels[t.id] == t {
		if reason != "instance_lost" {
			r.closeTunnel(t, reason)
		}
		return
	}
	if o.client != nil {
		o.client.send(issuerError)
	}
}

// abandonOffer: the issuer cancelled or disconnected before the instance
// answered. Keep the offer so an acceptance is followed by a close notice.
func (r *Router) abandonOffer(o *offer) {
	delete(r.opening, o.clientCall)
	o.client = nil
}

// closeTunnel ends a tunnel: its connections close, any offer in flight is
// cancelled and the instance is told. Called under r.mu.
func (r *Router) closeTunnel(t *tunnel, reason string) {
	if t.closed {
		return
	}
	t.closed = true
	if t.expiry != nil {
		t.expiry.Stop()
	}
	delete(r.tunnels, t.id)
	delete(r.tunnelsByDigest, t.digest)
	for _, o := range r.offers {
		if o.tunnel == t {
			r.endOffer(o)
			o.instance.send(frame{Kind: "call.cancel", CallID: o.callID, Reason: "tunnel_closed"})
		}
	}
	// Close with 4401 and an error reason, so clients can tell a revoked
	// tunnel from a network loss and do not reconnect (docs/protocol.md →
	// Tunnels). Close waits for the handshake, hence outside r.mu.
	closing := "tunnel_closed"
	if reason == "revoked" {
		closing = "tunnel_revoked"
	}
	for candidate := range r.peers {
		if candidate.user != nil && candidate.user.tunnel == t {
			go func(p *peer) {
				_ = p.conn.Close(StatusUnauthenticated, closing)
				p.cancel()
			}(candidate)
		}
	}
	if t.host != nil {
		r.notifyClosed(t.host, t, reason)
	}
}

// notifyClosed tells an instance a tunnel it accepted is gone. The answer is
// not awaited. Called under r.mu.
func (r *Router) notifyClosed(instance *peer, t *tunnel, reason string) {
	payload, _ := json.Marshal(map[string]string{"tunnel_id": t.id, "reason": reason})
	timeout := offerTimeout.Milliseconds()
	instance.send(frame{Kind: "call.open", CallID: "meshcall-router-closed-" + randomHex(16), Service: TunnelHostService, Method: "closed", Payload: payload, TimeoutMS: &timeout})
}

// instanceRegistered re-offers the tunnels of a reconnected instance. The
// same identity must come back: a tunnel is never handed to whoever
// registers the id next. Called under r.mu.
func (r *Router) instanceRegistered(p *peer) {
	for _, t := range r.tunnels {
		if t.instanceID != p.instanceID || t.host != nil {
			continue
		}
		if !slices.Contains(p.capabilities, TunnelCapability) || p.user.owner() != t.hostUser {
			r.closeTunnel(t, "instance_changed")
			continue
		}
		r.sendOffer(t, p, nil, "", "", true)
	}
}

// tunnelPeerGone marks the tunnels of a disconnected instance offline, ends
// the offers it had not answered and abandons the opens p was waiting on.
// Called under r.mu.
func (r *Router) tunnelPeerGone(p *peer) {
	for _, t := range r.tunnels {
		if t.host == p {
			t.host = nil
		}
	}
	for _, o := range r.offers {
		if o.instance == p {
			r.endOffer(o)
			r.offerRefused(o, routerError(o.clientCall, "unavailable", "Instance "+p.instanceID+" disconnected", true, "instance_lost", p.instanceID), "instance_lost")
		}
	}
	for _, o := range r.opening {
		if o.client == p {
			r.abandonOffer(o)
		}
	}
}

// tunnelCall applies a tunnel's scope to a call.open: the instance is
// implied, and the reserved identity metadata is set by the Router. It
// returns the frame to route, or the error to answer. Called under r.mu.
func (r *Router) tunnelCall(t *tunnel, f frame) (frame, *frame) {
	if f.InstanceID != "" && f.InstanceID != t.instanceID {
		// Other instances do not exist for this connection.
		e := tunnelScopeError(f.CallID, t)
		return f, &e
	}
	if t.e2eKey != "" && !isHandshake(f) && (f.E2ESession == "" || f.Sealed == "") {
		// Plain calls (HTTP ones included) would expose what the issuer asked
		// to keep end to end.
		e := routerError(f.CallID, "permission_denied", "This tunnel carries end-to-end sealed calls only", false, "tunnel_e2e_required", t.instanceID)
		return f, &e
	}
	if t.host == nil || r.instances[t.instanceID] != t.host {
		e := routerError(f.CallID, "unavailable", "Instance "+t.instanceID+" is offline", true, "instance_offline", t.instanceID)
		return f, &e
	}
	rewritten, err := rewriteCallOpen(f, t)
	if err != nil {
		e := callError(f.CallID, "protocol_error", "Invalid call.open frame", false)
		return f, &e
	}
	return rewritten, nil
}

// tunnelScopeError refuses anything outside a tunnel's view. It names only
// the tunnel's own target, which the caller already knows, and reads the
// same whether or not what was asked for exists.
func tunnelScopeError(callID string, t *tunnel) frame {
	return routerError(callID, "permission_denied", "Outside this tunnel's scope (instance "+t.instanceID+")", false, "tunnel_scope", t.instanceID)
}

// credentialDigest is the hex SHA-256 of an end-to-end tunnel's credential,
// which the handshake prologue binds; empty for a plain tunnel.
func (t *tunnel) credentialDigest() string {
	if t.e2eKey == "" {
		return ""
	}
	return hex.EncodeToString(t.digest[:])
}

// isHandshake reports an end-to-end session handshake, the one TunnelHost
// method a tunnel connection may call: it is routed to the tunnel's target.
func isHandshake(f frame) bool { return f.Service == TunnelHostService && f.Method == "handshake" }

// reservedTunnelKey matches meshcall-tunnel-* in any letter case: metadata
// lookups (meta() in the runtimes) may ignore case, so a mixed-case key must
// not survive to forge what only the Router sets.
func reservedTunnelKey(key string) bool {
	return len(key) >= len(tunnelMetaPrefix) && strings.EqualFold(key[:len(tunnelMetaPrefix)], tunnelMetaPrefix)
}

// stripTunnelMetadata removes meshcall-tunnel-* keys a caller sent on an
// ordinary connection: only the Router may set them.
func stripTunnelMetadata(f frame) frame {
	if f.Metadata == nil {
		return f
	}
	var metadata map[string]string
	if json.Unmarshal(f.Metadata, &metadata) != nil {
		return f
	}
	for key := range metadata {
		if reservedTunnelKey(key) {
			if rewritten, err := rewriteCallOpen(f, nil); err == nil {
				return rewritten
			}
			return f
		}
	}
	return f
}

// rewriteCallOpen re-encodes a call.open without caller-supplied tunnel
// metadata and, for t, with the tunnel's identity and target instance. Every
// other field, known or not, is kept.
func rewriteCallOpen(f frame, t *tunnel) (frame, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(f.raw, &fields); err != nil {
		return f, err
	}
	var metadata map[string]string
	if f.Metadata != nil {
		if err := json.Unmarshal(f.Metadata, &metadata); err != nil {
			return f, err
		}
	}
	if metadata == nil { // absent, or "metadata": null
		metadata = map[string]string{}
	}
	for key := range metadata {
		if reservedTunnelKey(key) {
			delete(metadata, key)
		}
	}
	if t != nil {
		metadata[tunnelIDKey], metadata[tunnelPrincipalKey] = t.id, t.principal
		fields["instance_id"], _ = json.Marshal(t.instanceID)
	}
	if len(metadata) == 0 {
		delete(fields, "metadata")
	} else {
		fields["metadata"], _ = json.Marshal(metadata)
	}
	data, err := json.Marshal(fields)
	if err != nil {
		return f, err
	}
	return decode(data)
}

// sees reports whether u may call service/method and, on a tunnel, whether
// the target instance serves it: the view the Registry and OpenAPI show.
// Called under r.mu.
func (r *Router) sees(u *User, service, method string) bool {
	if !u.allowsCall(service, method) {
		return false
	}
	if u == nil || u.tunnel == nil {
		return true
	}
	host := r.instances[u.tunnel.instanceID]
	return host != nil && host.serves(service, method)
}

// seesAny reports whether u sees at least one method of the pool.
func (r *Router) seesAny(u *User, service string, pool *pool) bool {
	for name := range pool.methods {
		if r.sees(u, service, name) {
			return true
		}
	}
	return false
}

// seesInstance reports whether u's view includes the instance.
func seesInstance(u *User, instanceID string) bool {
	return u == nil || u.tunnel == nil || u.tunnel.instanceID == instanceID
}

// TunnelSnapshot lists every tunnel, for tooling and tests.
func (r *Router) tunnelSnapshot() []TunnelInfo {
	out := make([]TunnelInfo, 0, len(r.tunnels))
	for _, t := range r.tunnels {
		out = append(out, t.info())
	}
	slices.SortFunc(out, func(a, b TunnelInfo) int { return strings.Compare(a.TunnelID, b.TunnelID) })
	return out
}
