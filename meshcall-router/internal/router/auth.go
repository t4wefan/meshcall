package router

import (
	"bytes"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"
)

const passwordIterations = 600000

type User struct {
	Username     string   `json:"username"`
	PasswordHash string   `json:"password_hash"`
	Roles        []string `json:"roles"`
	Register     []string `json:"register,omitempty"`
	Call         []string `json:"call,omitempty"`
	salt         []byte
	key          []byte
	expires      time.Time
	tokenID      string
	kind         identityKind
}

// identityKind says how a caller authenticated. Only accounts may issue
// temporary tokens; every other kind is already a delegated or public identity.
type identityKind int

const (
	identityAccount identityKind = iota
	identityToken
	identityAPIKey
	identityAnonymous
)

// AnonymousUser is the reserved username for callers without credentials.
const AnonymousUser = "anonymous"

// APIKey is a long-lived credential for machine callers, configured in the
// auth file. Keys are 256-bit random strings, so a SHA-256 digest is enough
// (PBKDF2 exists to slow guessing of human passwords).
type APIKey struct {
	ID       string   `json:"id"`
	KeyHash  string   `json:"key_hash"`
	Roles    []string `json:"roles"`
	Register []string `json:"register,omitempty"`
	Call     []string `json:"call,omitempty"`
}

const apiKeyPrefix = "mk_"

// mayIssueTokens: tokens, API keys and anonymous callers cannot delegate.
func (u *User) mayIssueTokens() bool { return u != nil && u.kind == identityAccount }

// Accounts are immutable after startup; temporary tokens are synchronized separately.
type Auth struct {
	users     map[string]*User
	apiKeys   map[string]apiKeyEntry
	anonymous *User
	slots     chan struct{}
	dummy     *User
	mu        sync.Mutex
	tokens    map[string]*User
	verified  map[[32]byte]verifiedBasic // successful Basic checks, a cache
}

type apiKeyEntry struct {
	digest [32]byte
	user   *User
}

type verifiedBasic struct {
	user    *User
	expires time.Time
}

const (
	basicCacheTTL  = time.Minute
	basicCacheSize = 1024
)

func HashPassword(password string) (string, error) {
	if len(password) == 0 || len(password) > 1024 {
		return "", errors.New("password must contain 1 to 1024 UTF-8 bytes")
	}
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	key, err := pbkdf2.Key(sha256.New, password, salt, passwordIterations, 32)
	if err != nil {
		return "", err
	}
	return "pbkdf2-sha256$600000$" + hex.EncodeToString(salt) + "$" + hex.EncodeToString(key), nil
}

func LoadAuth(path string) (*Auth, error) {
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read auth file: %w", err)
	}
	return ParseAuth(data)
}

// ParseAuth reads an auth document (the contents of an auth file), for
// deployments that pass it inline, e.g. through MESHCALL_ROUTER_AUTH.
func ParseAuth(data []byte) (*Auth, error) {
	var config struct {
		Users   []User   `json:"users"`
		APIKeys []APIKey `json:"api_keys"`
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&config); err != nil {
		return nil, errors.New("invalid auth file")
	}
	if d.Decode(new(any)) != io.EOF || len(config.Users)+len(config.APIKeys) == 0 {
		return nil, errors.New("auth file must contain at least one user or API key")
	}
	auth := &Auth{
		users: make(map[string]*User), apiKeys: make(map[string]apiKeyEntry), slots: make(chan struct{}, 8),
		tokens: make(map[string]*User), verified: make(map[[32]byte]verifiedBasic),
		// Without an "anonymous" entry, callers without credentials may connect but call nothing.
		anonymous: &User{Username: AnonymousUser, Roles: []string{"client"}, kind: identityAnonymous},
	}
	for i := range config.Users {
		u := &config.Users[i]
		if u.Username == "" || strings.ContainsAny(u.Username, ":\r\n") || len(u.Roles) == 0 {
			return nil, errors.New("auth user requires a username and roles")
		}
		if _, exists := auth.users[u.Username]; exists || (u.Username == AnonymousUser && auth.anonymous.Call != nil) {
			return nil, errors.New("duplicate auth username")
		}
		if err := validatePermissions(u.Roles, u.Register, u.Call); err != nil {
			return nil, err
		}
		if u.Username == AnonymousUser {
			// Reserved: no password, client only, never registers or issues.
			if u.PasswordHash != "" || len(u.Register) > 0 || !slices.Equal(u.Roles, []string{"client"}) {
				return nil, errors.New(`the "anonymous" user takes no password_hash, no register list, and roles ["client"] only`)
			}
			auth.anonymous = &User{Username: AnonymousUser, Roles: u.Roles, Call: nonNilRules(u.Call), kind: identityAnonymous}
			continue
		}
		parts := strings.Split(u.PasswordHash, "$")
		if len(parts) != 4 || parts[0] != "pbkdf2-sha256" || parts[1] != "600000" {
			return nil, errors.New("unsupported password hash; use hash-password")
		}
		var err error
		u.salt, err = hex.DecodeString(parts[2])
		if err != nil || len(u.salt) != 16 {
			return nil, errors.New("invalid password hash salt")
		}
		u.key, err = hex.DecodeString(parts[3])
		if err != nil || len(u.key) != 32 {
			return nil, errors.New("invalid password hash key")
		}
		auth.users[u.Username] = u
		auth.dummy = u
	}
	if auth.dummy == nil { // equal-cost rejection needs some password to compare
		hash, err := HashPassword("unused-dummy-password")
		if err != nil {
			return nil, err
		}
		parts := strings.Split(hash, "$")
		salt, _ := hex.DecodeString(parts[2])
		key, _ := hex.DecodeString(parts[3])
		auth.dummy = &User{salt: salt, key: key}
	}
	for _, k := range config.APIKeys {
		if k.ID == "" || strings.ContainsAny(k.ID, "_:\r\n ") || len(k.Roles) == 0 {
			return nil, errors.New("API key requires an id (no '_' or spaces) and roles")
		}
		if _, exists := auth.apiKeys[k.ID]; exists {
			return nil, errors.New("duplicate API key id")
		}
		digest, err := hex.DecodeString(k.KeyHash)
		if err != nil || len(digest) != 32 {
			return nil, errors.New("API key key_hash must be a SHA-256 hex digest; use new-api-key")
		}
		if err := validatePermissions(k.Roles, k.Register, k.Call); err != nil {
			return nil, err
		}
		entry := apiKeyEntry{user: &User{Username: "api-key:" + k.ID, Roles: k.Roles, Register: k.Register, Call: k.Call, kind: identityAPIKey}}
		copy(entry.digest[:], digest)
		auth.apiKeys[k.ID] = entry
	}
	return auth, nil
}

func nonNilRules(rules []string) []string {
	if rules == nil {
		return []string{}
	}
	return rules
}

func validatePermissions(roles, register, call []string) error {
	for _, role := range roles {
		if role != "client" && role != "server" {
			return errors.New("auth role must be client or server")
		}
	}
	for _, rule := range register {
		if rule != "*" && (rule == "" || strings.ContainsAny(rule, "*/")) {
			return errors.New("register ACL requires exact service names or *")
		}
	}
	for _, rule := range call {
		if rule == "*" {
			continue
		}
		service, method, ok := strings.Cut(rule, "/")
		if !ok || service == "" || strings.Contains(service, "*") || method == "" ||
			strings.Contains(method, "/") || (strings.Contains(method, "*") && method != "*") {
			return errors.New("call ACL requires service/method, service/*, or *")
		}
	}
	return nil
}

// NewAPIKey generates a key for id and the digest to put in the auth file.
func NewAPIKey(id string) (key, keyHash string, err error) {
	if id == "" || strings.ContainsAny(id, "_:\r\n ") {
		return "", "", errors.New("API key id must be non-empty without '_', ':' or spaces")
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return "", "", err
	}
	key = apiKeyPrefix + id + "_" + hex.EncodeToString(secret)
	digest := sha256.Sum256([]byte(key))
	return key, hex.EncodeToString(digest[:]), nil
}

// authFailure says why a request was not authenticated; challenge asks the
// caller to send Basic credentials.
type authFailure struct {
	status    int
	message   string
	challenge bool
}

// check authenticates an HTTP request (a WebSocket upgrade or an HTTP call).
// It never writes a response, so each entry point answers in its own format.
func (a *Auth) check(r *http.Request) (*User, *authFailure) {
	if a == nil {
		return nil, nil
	}
	header := r.Header.Get("Authorization")
	if header == "" {
		// No credentials at all: the anonymous user. Presented credentials
		// that fail are refused below, never downgraded to anonymous.
		return a.anonymous, nil
	}
	if scheme, token, ok := strings.Cut(header, " "); ok && strings.EqualFold(scheme, "Bearer") && strings.HasPrefix(token, apiKeyPrefix) {
		rest := strings.TrimPrefix(token, apiKeyPrefix)
		id, _, _ := strings.Cut(rest, "_")
		entry, exists := a.apiKeys[id]
		digest := sha256.Sum256([]byte(token))
		if exists && subtle.ConstantTimeCompare(digest[:], entry.digest[:]) == 1 {
			return entry.user, nil
		}
		return nil, &authFailure{status: http.StatusUnauthorized, message: "authentication failed"}
	}
	if scheme, token, ok := strings.Cut(header, " "); ok && strings.EqualFold(scheme, "Bearer") {
		digest := sha256.Sum256([]byte(token))
		id := hex.EncodeToString(digest[:])
		a.mu.Lock()
		user := a.tokens[id]
		valid := user != nil && time.Now().Before(user.expires)
		a.mu.Unlock()
		if valid {
			return user, nil
		}
		return nil, &authFailure{status: http.StatusUnauthorized, message: "authentication failed"}
	}
	// A recent successful check is reused: the cache key is a digest of the
	// credentials, never the credentials themselves.
	cacheKey := sha256.Sum256([]byte(header))
	a.mu.Lock()
	cached, hit := a.verified[cacheKey]
	a.mu.Unlock()
	if hit && time.Now().Before(cached.expires) {
		return cached.user, nil
	}
	// Bound expensive password checks. Do not retain credentials or log headers.
	select {
	case a.slots <- struct{}{}:
		defer func() { <-a.slots }()
	default:
		return nil, &authFailure{status: http.StatusServiceUnavailable, message: "authentication busy"}
	}
	username, password, ok := r.BasicAuth()
	if !ok || len(password) > 1024 {
		return nil, &authFailure{status: http.StatusUnauthorized, message: "authentication required", challenge: true}
	}
	u, exists := a.users[username]
	if !exists {
		u = a.dummy // Equal-cost rejection for unknown usernames.
	}
	key, err := pbkdf2.Key(sha256.New, password, u.salt, passwordIterations, 32)
	if err != nil || subtle.ConstantTimeCompare(key, u.key) != 1 || !exists {
		return nil, &authFailure{status: http.StatusUnauthorized, message: "authentication failed", challenge: true}
	}
	a.mu.Lock()
	if len(a.verified) >= basicCacheSize {
		clear(a.verified) // bounded; refilled by the next checks
	}
	a.verified[cacheKey] = verifiedBasic{user: u, expires: time.Now().Add(basicCacheTTL)}
	a.mu.Unlock()
	return u, nil
}

// authenticate is check for WebSocket upgrades: failures are plain-text HTTP
// errors before the upgrade.
func (a *Auth) authenticate(w http.ResponseWriter, r *http.Request) (*User, bool) {
	user, failure := a.check(r)
	if failure == nil {
		return user, true
	}
	if failure.challenge {
		w.Header().Set("WWW-Authenticate", `Basic realm="meshcall", charset="UTF-8"`)
	}
	http.Error(w, failure.message, failure.status)
	return nil, false
}

func (a *Auth) valid(user *User) bool {
	if user == nil || user.tokenID == "" {
		return true
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.tokens[user.tokenID] == user && time.Now().Before(user.expires)
}

func (u *User) allowsRole(role string) bool {
	return u == nil || slices.Contains(u.Roles, role)
}

func (u *User) allowsRegistration(name string) bool {
	return u == nil || slices.Contains(u.Register, "*") || slices.Contains(u.Register, name)
}

func (u *User) allowsCall(service, method string) bool {
	return u == nil || slices.Contains(u.Call, "*") || slices.Contains(u.Call, service+"/*") || slices.Contains(u.Call, service+"/"+method)
}
