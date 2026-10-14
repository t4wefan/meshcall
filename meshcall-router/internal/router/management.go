package router

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/t4wefan/meshcall/meshcall-router/internal/contract"
	"io"
	"slices"
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

type scopeError struct{ message string }

func (e *scopeError) Error() string { return e.message }

func (a *Auth) issue(issuer *User, request IssueTokenRequest) (IssueTokenResult, error) {
	var result IssueTokenResult
	if issuer.tokenID != "" {
		return result, errors.New("temporary tokens cannot issue tokens")
	}
	if request.TTLSeconds < 1 || request.TTLSeconds > 3600 || len(request.Scopes) == 0 || len(request.Scopes) > 128 {
		return result, errors.New("ttl_seconds must be 1..3600 and scopes must contain 1..128 services")
	}
	user := &User{Username: issuer.Username}
	seen := make(map[string]bool)
	for _, scope := range request.Scopes {
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
	digest := sha256.Sum256([]byte(token))
	user.tokenID = hex.EncodeToString(digest[:])
	user.kind = identityToken
	user.expires = time.Now().Add(time.Duration(request.TTLSeconds) * time.Second)
	a.mu.Lock()
	defer a.mu.Unlock()
	for id, candidate := range a.tokens {
		if !time.Now().Before(candidate.expires) {
			delete(a.tokens, id)
		}
	}
	if len(a.tokens) >= 4096 {
		return result, errors.New("temporary token capacity reached")
	}
	a.tokens[user.tokenID] = user
	return IssueTokenResult{Token: token, TokenID: user.tokenID, ExpiresUnixMS: user.expires.UnixMilli(), Scopes: request.Scopes}, nil
}

// Called under Router.mu, as are all forwarding and registration mutations.
func (r *Router) management(p *peer, f frame) {
	fail := func(code, message string) { p.send(callError(f.CallID, code, message, false)) }
	if f.Service == RegistryService {
		r.registry(p, f)
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
		result = struct {
			Username string   `json:"username"`
			Roles    []string `json:"roles"`
			Register []string `json:"register"`
			Call     []string `json:"call"`
			Expires  *int64   `json:"expires_at_unix_ms"`
		}{p.user.Username, p.user.Roles, nonNil(p.user.Register), nonNil(p.user.Call), expires}
	case "issue_token":
		if !p.user.mayIssueTokens() {
			fail("permission_denied", "Temporary tokens cannot issue tokens")
			return
		}
		var request IssueTokenRequest
		if !readRequest(f.Payload, &request) {
			fail("invalid_argument", "Invalid issue_token request")
			return
		}
		issued, err := r.config.Auth.issue(p.user, request)
		if err != nil {
			var denied *scopeError
			if errors.As(err, &denied) {
				fail("permission_denied", err.Error())
			} else {
				fail("invalid_argument", err.Error())
			}
			return
		}
		result = issued
	case "revoke_token":
		if !p.user.mayIssueTokens() {
			fail("permission_denied", "Temporary tokens cannot revoke tokens")
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
			delete(r.config.Auth.tokens, request.TokenID)
		}
		r.config.Auth.mu.Unlock()
		if revoked {
			for candidate := range r.peers {
				if candidate.user != nil && candidate.user.tokenID == request.TokenID {
					candidate.cancel()
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
		Name        string          `json:"name"`
		Description *string         `json:"description"`
		Annotations json.RawMessage `json:"annotations"`
		Deprecated  *string         `json:"deprecated"`
		Parameters  []string        `json:"parameters"`
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
			if p.user.allowsAnyCall(name, r.services[name]) {
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
		if pool == nil || !p.user.allowsAnyCall(request.Service, pool) {
			fail("method_not_found", "No instances registered for "+request.Service)
			return
		}
		described := r.describeService(request.Service, p.user)
		result = struct {
			registryService
			Contract json.RawMessage `json:"contract,omitempty"`
		}{described, visibleContract(pool.contract, request.Service, p.user)}
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

// allowsAnyCall reports whether u may call at least one method of the pool.
func (u *User) allowsAnyCall(service string, pool *pool) bool {
	for name := range pool.methods {
		if u.allowsCall(service, name) {
			return true
		}
	}
	return false
}

// visibleContract keeps only the method entries u may call, preserving the
// document's key order so schemas read the same as the service registered them.
func visibleContract(raw json.RawMessage, service string, u *User) json.RawMessage {
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
		if entry, ok := m.(*contract.Object); ok && u.allowsCall(service, entry.Str("name")) {
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
		out.Instances = append(out.Instances, RegistryInstance{
			ID: inst.instanceID, Inflight: inst.inflight, MaxInflight: inst.maxInflight,
			Breaker: string(inst.health.state(now)), Draining: inst.draining,
		})
	}
	// Draining instances have left the pool but still hold the id.
	for id, inst := range r.instances {
		if inst.draining && slices.ContainsFunc(inst.services, func(s service) bool { return s.Name == name }) {
			out.Instances = append(out.Instances, RegistryInstance{ID: id, Inflight: inst.inflight, MaxInflight: inst.maxInflight,
				Breaker: string(inst.health.state(now)), Draining: true})
		}
	}
	var entry contractEntry
	if pool.contract != nil && json.Unmarshal(pool.contract, &entry) == nil {
		out.Version, out.Description = entry.Version, entry.Description
	}
	for _, m := range sortedMethods(pool.methods) {
		if !u.allowsCall(name, m.Name) {
			continue
		}
		rm := registryMethod{Name: m.Name, Stream: m.Stream, Balance: m.Balance, Instances: []string{}}
		for _, inst := range pool.instances {
			if inst.serves(name, m.Name) {
				rm.Instances = append(rm.Instances, inst.instanceID)
			}
		}
		for _, cm := range entry.Methods {
			if cm.Name == m.Name {
				rm.Description, rm.Annotations, rm.Deprecated, rm.Parameters = cm.Description, cm.Annotations, cm.Deprecated, cm.Parameters
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
