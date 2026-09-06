package router

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
	"time"
)

const AuthService = "meshcall.router.v1.AuthService"
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
	token := base64.RawURLEncoding.EncodeToString(secret)
	digest := sha256.Sum256([]byte(token))
	user.tokenID = hex.EncodeToString(digest[:])
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
		if p.user.tokenID != "" {
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
		if p.user.tokenID != "" {
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
