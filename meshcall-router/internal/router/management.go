package router

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/t4wefan/meshcall/meshcall-router/internal/contract"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"
)

const AuthService = "meshcall.router.v1.AuthService"
const RegistryService = "meshcall.router.v1.Registry"
const reservedPrefix = "meshcall.router."

type Scope struct {
	Service  string   `json:"service"`
	Methods  []string `json:"methods,omitempty"`
	Register bool     `json:"register,omitempty"`
	// InstanceIDs restricts a registration scope to these instance ids. An
	// issuer restricted to instance ids must name a subset; omitted, the
	// token inherits the issuer's restriction.
	InstanceIDs []string `json:"instance_ids,omitempty"`
}

type IssueTokenRequest struct {
	TTLSeconds int     `json:"ttl_seconds"`
	Scopes     []Scope `json:"scopes"`
}

type IssueTokenResult struct {
	Token         string  `json:"token"`
	TokenID       string  `json:"token_id"`
	ExpiresUnixMS int64   `json:"expires_at_unix_ms"`
	Scopes        []Scope `json:"scopes"`
}

// ttlError: ttl_seconds below 1 (tooShort) or above the Router's maximum,
// which the answer then names.
type ttlError struct {
	max      time.Duration
	tooShort bool
}

func (e *ttlError) Error() string {
	if e.tooShort {
		return "ttl_seconds must be at least 1"
	}
	return "ttl_seconds must be at most " + strconv.Itoa(int(e.max/time.Second)) + " (the Router's --token-max-ttl)"
}

// capacityError: the Router holds its maximum of outstanding tokens.
type capacityError struct{}

func (capacityError) Error() string { return "temporary token capacity reached" }

func (a *Auth) checkTTL(seconds int) error {
	a.mu.Lock()
	limit := a.tokenMaxTTL
	a.mu.Unlock()
	if seconds < 1 {
		return &ttlError{tooShort: true}
	}
	if time.Duration(seconds)*time.Second > limit {
		return &ttlError{max: limit}
	}
	return nil
}

type scopeError struct{ message string }

func (e *scopeError) Error() string { return e.message }

func (a *Auth) issue(issuer *User, request IssueTokenRequest) (IssueTokenResult, error) {
	var result IssueTokenResult
	if issuer.tokenID != "" {
		return result, errors.New("temporary tokens cannot issue tokens")
	}
	if err := a.checkTTL(request.TTLSeconds); err != nil {
		return result, err
	}
	if len(request.Scopes) == 0 || len(request.Scopes) > 128 {
		return result, errors.New("scopes must contain 1..128 services")
	}
	user := &User{Username: issuer.Username}
	seen := make(map[string]bool)
	// The result reports the effective scopes: inherited instance_ids included.
	effective := slices.Clone(request.Scopes)
	for i, scope := range request.Scopes {
		// Require concrete services. A token cannot grant a namespace wildcard
		// that might later include new services or the router's management API.
		if scope.Service == "" || strings.ContainsAny(scope.Service, "*/") || strings.HasPrefix(scope.Service, reservedPrefix) || seen[scope.Service] {
			return result, errors.New("scopes require unique, concrete business service names")
		}
		seen[scope.Service] = true
		if len(scope.Methods) == 0 && !scope.Register {
			return result, errors.New("scope grants no permissions")
		}
		for _, method := range scope.Methods {
			if method == "" || strings.Contains(method, "/") || (strings.Contains(method, "*") && method != "*") {
				return result, errors.New("invalid method scope")
			}
			if !issuer.allowsRole("client") || !issuer.allowsCall(scope.Service, method) {
				return result, &scopeError{"requested call scope exceeds issuer permissions"}
			}
			user.Call = append(user.Call, scope.Service+"/"+method)
		}
		if scope.Register {
			if !issuer.allowsRole("server") || !issuer.allowsRegistration(scope.Service) {
				return result, &scopeError{"requested registration scope exceeds issuer permissions"}
			}
			user.Register = append(user.Register, scope.Service)
			ids := scope.InstanceIDs
			if err := validateInstanceIDs(ids); err != nil {
				return result, err
			}
			if ids == nil {
				ids = issuer.InstanceIDs
				effective[i].InstanceIDs = slices.Clone(ids)
			}
			for _, id := range ids {
				if !issuer.allowsInstance(scope.Service, id) {
					return result, &scopeError{"requested instance_ids exceed issuer permissions"}
				}
			}
			if ids != nil {
				if user.registerInstances == nil {
					user.registerInstances = map[string][]string{}
				}
				user.registerInstances[scope.Service] = slices.Clone(ids)
			}
		} else if scope.InstanceIDs != nil {
			return result, errors.New("instance_ids applies to registration scopes only")
		}
	}
	if len(user.Call) > 0 {
		user.Roles = append(user.Roles, "client")
	}
	if len(user.Register) > 0 {
		user.Roles = append(user.Roles, "server")
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return result, err
	}
	// The prefix keeps temporary tokens and API keys (mk_) apart by format.
	token := "mt_" + base64.RawURLEncoding.EncodeToString(secret)
	// The id is shown and logged; it must not be derived from the secret.
	user.tokenID = "tok_" + randomHex(12)
	user.tokenDigest = sha256.Sum256([]byte(token))
	user.kind = identityToken
	user.expires = time.Now().Add(time.Duration(request.TTLSeconds) * time.Second)
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pruneLocked()
	if len(a.tokens) >= a.maxTokens {
		return result, capacityError{}
	}
	a.tokens[user.tokenID] = user
	a.tokenDigests[user.tokenDigest] = user.tokenID
	return IssueTokenResult{Token: token, TokenID: user.tokenID, ExpiresUnixMS: user.expires.UnixMilli(), Scopes: effective}, nil
}

// pruneLocked forgets expired tokens no connection uses any more; one that
// still has connections stays so revoke_token can end them. Called under a.mu.
func (a *Auth) pruneLocked() {
	for id, candidate := range a.tokens {
		if !time.Now().Before(candidate.expires) && candidate.connections == 0 {
			a.forgetLocked(id, candidate)
		}
	}
}

func (a *Auth) forgetLocked(id string, user *User) {
	delete(a.tokens, id)
	delete(a.tokenDigests, user.tokenDigest)
}

// TokenInfo describes an outstanding temporary token, never its secret.
type TokenInfo struct {
	TokenID       string   `json:"token_id"`
	Issuer        string   `json:"issuer"`
	InstanceIDs   []string `json:"instance_ids,omitempty"`
	ExpiresUnixMS int64    `json:"expires_at_unix_ms"`
	Expired       bool     `json:"expired"`
	Connections   int      `json:"connections"`
}

// boundInstances: the instance ids registration is restricted to, whether by
// the account or by a token's scopes.
func (u *User) boundInstances() []string {
	ids := slices.Clone(u.InstanceIDs)
	for _, list := range u.registerInstances {
		for _, id := range list {
			if !slices.Contains(ids, id) {
				ids = append(ids, id)
			}
		}
	}
	slices.Sort(ids)
	return ids
}

func (u *User) tokenInfo(now time.Time) TokenInfo {
	return TokenInfo{TokenID: u.tokenID, Issuer: u.Username, InstanceIDs: u.boundInstances(), ExpiresUnixMS: u.expires.UnixMilli(),
		Expired: !now.Before(u.expires), Connections: u.connections}
}

// renew moves a token's expiry; the secret, scopes and instance ids stay.
func (a *Auth) renew(issuer *User, tokenID string, seconds int) (TokenInfo, error) {
	if err := a.checkTTL(seconds); err != nil {
		return TokenInfo{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	user := a.tokens[tokenID]
	now := time.Now()
	// Unknown, someone else's, revoked (forgotten) or expired: one answer, so
	// an issuer learns nothing about other issuers' tokens.
	if user == nil || user.Username != issuer.Username || !now.Before(user.expires) {
		return TokenInfo{}, errTokenExpired
	}
	user.expires = now.Add(time.Duration(seconds) * time.Second)
	return user.tokenInfo(now), nil
}

var errTokenExpired = errors.New("Token is unknown, expired or revoked; issue a new one")

// tokenRefusal answers issue_token / renew_token failures.
func tokenRefusal(id string, err error) frame {
	var tooLong *ttlError
	var denied *scopeError
	switch {
	case errors.As(err, &tooLong) && tooLong.tooShort:
		return routerError(id, "invalid_argument", err.Error(), false, "ttl_invalid", "")
	case errors.As(err, &tooLong):
		f := callError(id, "invalid_argument", err.Error(), false)
		f.Error.Details, _ = json.Marshal(map[string]any{"source": "router", "reason": "ttl_too_long",
			"max_ttl_seconds": int(tooLong.max / time.Second)})
		return f
	case errors.Is(err, errTokenExpired):
		return routerError(id, "invalid_argument", err.Error(), false, "token_expired", "")
	case errors.As(err, &denied):
		return callError(id, "permission_denied", err.Error(), false)
	case errors.As(err, new(capacityError)):
		return routerError(id, "resource_exhausted", "Router temporary token capacity reached", true, "overloaded", "")
	}
	return callError(id, "invalid_argument", err.Error(), false)
}

// Called under Router.mu, as are all forwarding and registration mutations.
func (r *Router) management(p *peer, f frame) {
	fail := func(code, message string) { p.send(callError(f.CallID, code, message, false)) }
	if f.Service == RegistryService {
		r.registry(p, f)
		return
	}
	if f.Service == TunnelService {
		r.tunnelService(p, f)
		return
	}
	if p.user == nil {
		fail("unauthenticated", "Router accounts are not configured")
		return
	}
	if f.Service != AuthService {
		fail("method_not_found", "Unknown router service")
		return
	}
	var result any
	switch f.Method {
	case "whoami":
		var request struct{}
		if !readRequest(f.Payload, &request) {
			fail("invalid_argument", "whoami expects an empty object")
			return
		}
		var expires *int64
		if !p.user.expires.IsZero() {
			value := p.user.expires.UnixMilli()
			expires = &value
		}
		var tunnel *TunnelInfo
		if p.user.tunnel != nil {
			info := p.user.tunnel.info()
			tunnel = &info
		}
		result = struct {
			Username    string      `json:"username"`
			Roles       []string    `json:"roles"`
			Register    []string    `json:"register"`
			Call        []string    `json:"call"`
			InstanceIDs []string    `json:"instance_ids,omitempty"`
			Expires     *int64      `json:"expires_at_unix_ms"`
			Tunnel      *TunnelInfo `json:"tunnel,omitempty"`
		}{p.user.Username, p.user.Roles, nonNil(p.user.Register), nonNil(p.user.Call), p.user.boundInstances(), expires, tunnel}
	case "issue_token":
		if !p.user.mayIssueTokens() {
			fail("permission_denied", "Only accounts may issue temporary tokens")
			return
		}
		var request IssueTokenRequest
		if !readRequest(f.Payload, &request) {
			fail("invalid_argument", "Invalid issue_token request")
			return
		}
		issued, err := r.config.Auth.issue(p.user, request)
		if err != nil {
			p.send(tokenRefusal(f.CallID, err))
			return
		}
		result = issued
	case "renew_token":
		if !p.user.mayIssueTokens() {
			fail("permission_denied", "Only accounts may renew temporary tokens")
			return
		}
		var request struct {
			TokenID    string `json:"token_id"`
			TTLSeconds int    `json:"ttl_seconds"`
		}
		if !readRequest(f.Payload, &request) || request.TokenID == "" {
			fail("invalid_argument", "token_id and ttl_seconds are required")
			return
		}
		// Under r.mu and a.mu: every reader of a token's expiry holds one.
		renewed, err := r.config.Auth.renew(p.user, request.TokenID, request.TTLSeconds)
		if err != nil {
			p.send(tokenRefusal(f.CallID, err))
			return
		}
		result = struct {
			TokenID       string `json:"token_id"`
			ExpiresUnixMS int64  `json:"expires_at_unix_ms"`
		}{renewed.TokenID, renewed.ExpiresUnixMS}
	case "list_tokens":
		if !p.user.mayIssueTokens() {
			fail("permission_denied", "Only accounts may list temporary tokens")
			return
		}
		var request struct{}
		if !readRequest(f.Payload, &request) {
			fail("invalid_argument", "list_tokens expects an empty object")
			return
		}
		a := r.config.Auth
		a.mu.Lock()
		a.pruneLocked()
		now := time.Now()
		tokens := []TokenInfo{}
		for _, user := range a.tokens {
			if user.Username == p.user.Username {
				tokens = append(tokens, user.tokenInfo(now))
			}
		}
		a.mu.Unlock()
		slices.SortFunc(tokens, func(x, y TokenInfo) int { return int(x.ExpiresUnixMS - y.ExpiresUnixMS) })
		result = struct {
			Tokens []TokenInfo `json:"tokens"`
		}{tokens}
	case "revoke_token":
		if !p.user.mayIssueTokens() {
			fail("permission_denied", "Only accounts may revoke temporary tokens")
			return
		}
		var request struct {
			TokenID string `json:"token_id"`
		}
		if !readRequest(f.Payload, &request) || request.TokenID == "" {
			fail("invalid_argument", "token_id is required")
			return
		}
		r.config.Auth.mu.Lock()
		user := r.config.Auth.tokens[request.TokenID]
		revoked := user != nil && user.Username == p.user.Username
		if revoked {
			r.config.Auth.forgetLocked(request.TokenID, user)
		}
		r.config.Auth.mu.Unlock()
		if revoked {
			for candidate := range r.peers {
				if candidate.user != nil && candidate.user.tokenID == request.TokenID {
					candidate.refuse("token_revoked")
				}
			}
		}
		result = struct {
			Revoked bool `json:"revoked"`
		}{revoked}
	default:
		fail("method_not_found", "Unknown router method")
		return
	}
	payload, err := json.Marshal(result)
	if err != nil {
		fail("internal", "Could not encode router result")
		return
	}
	p.send(frame{Kind: "call.result", CallID: f.CallID, Payload: payload})
}

func readRequest(payload []byte, value any) bool {
	if !bytes.HasPrefix(bytes.TrimSpace(payload), []byte("{")) {
		return false
	}
	d := json.NewDecoder(bytes.NewReader(payload))
	d.DisallowUnknownFields()
	return d.Decode(value) == nil && d.Decode(new(any)) == io.EOF
}

func nonNil(values []string) []string {
	return append([]string{}, slices.Clone(values)...)
}

// --- Registry: what is registered, in what shape, and how healthy --------

// RegistryInstance is one instance as reported by the Registry service.
type RegistryInstance struct {
	ID          string `json:"id"`
	Inflight    int    `json:"inflight"`
	MaxInflight int    `json:"max_inflight,omitempty"`
	Breaker     string `json:"breaker"`
	Draining    bool   `json:"draining"`
}

type registryMethod struct {
	Name        string          `json:"name"`
	Stream      string          `json:"stream"`
	Balance     balance         `json:"balance"`
	Description *string         `json:"description,omitempty"`
	Annotations json.RawMessage `json:"annotations,omitempty"`
	Deprecated  *string         `json:"deprecated,omitempty"`
	Parameters  []string        `json:"parameters,omitempty"`
	// DuplexReason is why a duplex method needs the shape (from the contract).
	DuplexReason *string `json:"duplex_reason,omitempty"`
	// Instances lists the ids serving this method; during a rolling upgrade
	// a new method is served by a subset of the service's instances.
	Instances []string `json:"instances"`
}

type registryService struct {
	Name        string             `json:"name"`
	Version     string             `json:"version,omitempty"`
	Description *string            `json:"description,omitempty"`
	Targeted    bool               `json:"targeted"`
	Instances   []RegistryInstance `json:"instances"`
	Methods     []registryMethod   `json:"methods"`
}

// contractEntry is the subset of a portable contract entry the Registry reads.
type contractEntry struct {
	Version     string  `json:"version"`
	Description *string `json:"description"`
	Methods     []struct {
		Name         string          `json:"name"`
		Description  *string         `json:"description"`
		Annotations  json.RawMessage `json:"annotations"`
		Deprecated   *string         `json:"deprecated"`
		Parameters   []string        `json:"parameters"`
		DuplexReason *string         `json:"duplex_reason"`
	} `json:"methods"`
}

// Called under Router.mu. Readable without an account so that tooling works in
// development mode; with accounts configured it needs a call permission on
// meshcall.router.v1.Registry like any other service.
//
// Results are filtered by the caller's call permissions: a service appears
// only if the caller may call at least one of its methods, and only those
// methods (and their contract entries) are shown. Describing a service the
// caller may not call is answered exactly like describing one that does not
// exist, so the Registry cannot be used to probe for services.
func (r *Router) registry(p *peer, f frame) {
	fail := func(code, message string) { p.send(callError(f.CallID, code, message, false)) }
	var result any
	switch f.Method {
	case "list":
		var request struct{}
		if !readRequest(f.Payload, &request) {
			fail("invalid_argument", "list expects an empty object")
			return
		}
		names := make([]string, 0, len(r.services))
		for name := range r.services {
			if r.seesAny(p.user, name, r.services[name]) {
				names = append(names, name)
			}
		}
		slices.Sort(names)
		services := make([]registryService, 0, len(names))
		for _, name := range names {
			services = append(services, r.describeService(name, p.user))
		}
		result = struct {
			Services []registryService `json:"services"`
		}{services}
	case "describe":
		var request struct {
			Service string `json:"service"`
		}
		if !readRequest(f.Payload, &request) || request.Service == "" {
			fail("invalid_argument", "describe expects {\"service\": name}")
			return
		}
		pool := r.services[request.Service]
		if pool == nil || !r.seesAny(p.user, request.Service, pool) {
			fail("method_not_found", "No instances registered for "+request.Service)
			return
		}
		described := r.describeService(request.Service, p.user)
		result = struct {
			registryService
			Contract json.RawMessage `json:"contract,omitempty"`
		}{described, r.visibleContract(pool.contract, request.Service, p.user)}
	default:
		fail("method_not_found", "Unknown Registry method "+f.Method)
		return
	}
	payload, err := json.Marshal(result)
	if err != nil {
		fail("internal", "Could not encode registry result")
		return
	}
	p.send(frame{Kind: "call.result", CallID: f.CallID, Payload: payload})
}

// visibleContract keeps only the method entries u sees, preserving the
// document's key order so schemas read the same as the service registered them.
func (r *Router) visibleContract(raw json.RawMessage, service string, u *User) json.RawMessage {
	if raw == nil || u == nil {
		return raw
	}
	decoded, err := contract.DecodeOrdered(raw)
	doc, ok := decoded.(*contract.Object)
	if err != nil || !ok {
		return nil
	}
	methods, _ := doc.Values["methods"].([]any)
	kept := make([]any, 0, len(methods))
	for _, m := range methods {
		if entry, ok := m.(*contract.Object); ok && r.sees(u, service, entry.Str("name")) {
			kept = append(kept, m)
		}
	}
	doc.Set("methods", kept)
	return contract.Encode(doc)
}

func (r *Router) describeService(name string, u *User) registryService {
	pool := r.services[name]
	out := registryService{Name: name, Targeted: pool.targeted, Instances: []RegistryInstance{}, Methods: []registryMethod{}}
	now := time.Now()
	for _, inst := range pool.instances {
		if !seesInstance(u, inst.instanceID) {
			continue
		}
		out.Instances = append(out.Instances, RegistryInstance{
			ID: inst.instanceID, Inflight: inst.inflight, MaxInflight: inst.maxInflight,
			Breaker: string(inst.health.state(now)), Draining: inst.draining,
		})
	}
	// Draining instances have left the pool but still hold the id.
	for id, inst := range r.instances {
		if inst.draining && seesInstance(u, id) && slices.ContainsFunc(inst.services, func(s service) bool { return s.Name == name }) {
			out.Instances = append(out.Instances, RegistryInstance{ID: id, Inflight: inst.inflight, MaxInflight: inst.maxInflight,
				Breaker: string(inst.health.state(now)), Draining: true})
		}
	}
	var entry contractEntry
	if pool.contract != nil && json.Unmarshal(pool.contract, &entry) == nil {
		out.Version, out.Description = entry.Version, entry.Description
	}
	for _, m := range sortedMethods(pool.methods) {
		if !r.sees(u, name, m.Name) {
			continue
		}
		rm := registryMethod{Name: m.Name, Stream: m.Stream, Balance: m.Balance, Instances: []string{}}
		for _, inst := range pool.instances {
			if inst.serves(name, m.Name) && seesInstance(u, inst.instanceID) {
				rm.Instances = append(rm.Instances, inst.instanceID)
			}
		}
		for _, cm := range entry.Methods {
			if cm.Name == m.Name {
				rm.Description, rm.Annotations, rm.Deprecated, rm.Parameters = cm.Description, cm.Annotations, cm.Deprecated, cm.Parameters
				rm.DuplexReason = cm.DuplexReason
			}
		}
		out.Methods = append(out.Methods, rm)
	}
	return out
}

func sortedMethods(methods map[string]method) []method {
	out := make([]method, 0, len(methods))
	for _, m := range methods {
		out = append(out, m)
	}
	slices.SortFunc(out, func(a, b method) int { return strings.Compare(a.Name, b.Name) })
	return out
}
