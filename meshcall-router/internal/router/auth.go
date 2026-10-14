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
}

// Accounts are immutable after startup; temporary tokens are synchronized separately.
type Auth struct {
	users  map[string]*User
	slots  chan struct{}
	dummy  *User
	mu     sync.Mutex
	tokens map[string]*User
}

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
	var config struct {
		Users []User `json:"users"`
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&config); err != nil {
		return nil, errors.New("invalid auth file")
	}
	if d.Decode(new(any)) != io.EOF || len(config.Users) == 0 {
		return nil, errors.New("auth file must contain at least one user")
	}
	auth := &Auth{users: make(map[string]*User), slots: make(chan struct{}, 8), tokens: make(map[string]*User)}
	for i := range config.Users {
		u := &config.Users[i]
		if u.Username == "" || strings.ContainsAny(u.Username, ":\r\n") || len(u.Roles) == 0 {
			return nil, errors.New("auth user requires a username and roles")
		}
		if _, exists := auth.users[u.Username]; exists {
			return nil, errors.New("duplicate auth username")
		}
		for _, role := range u.Roles {
			if role != "client" && role != "server" {
				return nil, errors.New("auth role must be client or server")
			}
		}
		for _, rule := range u.Register {
			if rule != "*" && (rule == "" || strings.ContainsAny(rule, "*/")) {
				return nil, errors.New("register ACL requires exact service names or *")
			}
		}
		for _, rule := range u.Call {
			if rule == "*" {
				continue
			}
			service, method, ok := strings.Cut(rule, "/")
			if !ok || service == "" || strings.Contains(service, "*") || method == "" ||
				strings.Contains(method, "/") || (strings.Contains(method, "*") && method != "*") {
				return nil, errors.New("call ACL requires service/method, service/*, or *")
			}
		}
		parts := strings.Split(u.PasswordHash, "$")
		if len(parts) != 4 || parts[0] != "pbkdf2-sha256" || parts[1] != "600000" {
			return nil, errors.New("unsupported password hash; use hash-password")
		}
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
	return auth, nil
}

func (a *Auth) authenticate(w http.ResponseWriter, r *http.Request) (*User, bool) {
	if a == nil {
		return nil, true
	}
	if scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " "); ok && strings.EqualFold(scheme, "Bearer") {
		digest := sha256.Sum256([]byte(token))
		id := hex.EncodeToString(digest[:])
		a.mu.Lock()
		user := a.tokens[id]
		valid := user != nil && time.Now().Before(user.expires)
		a.mu.Unlock()
		if valid {
			return user, true
		}
		http.Error(w, "authentication failed", http.StatusUnauthorized)
		return nil, false
	}
	// Bound expensive password checks. Do not retain credentials or log headers.
	select {
	case a.slots <- struct{}{}:
		defer func() { <-a.slots }()
	default:
		http.Error(w, "authentication busy", http.StatusServiceUnavailable)
		return nil, false
	}
	username, password, ok := r.BasicAuth()
	if !ok || len(password) > 1024 {
		w.Header().Set("WWW-Authenticate", `Basic realm="meshcall", charset="UTF-8"`)
		http.Error(w, "authentication required", http.StatusUnauthorized)
		return nil, false
	}
	u, exists := a.users[username]
	if !exists {
		u = a.dummy // Equal-cost rejection for unknown usernames.
	}
	key, err := pbkdf2.Key(sha256.New, password, u.salt, passwordIterations, 32)
	if err != nil || subtle.ConstantTimeCompare(key, u.key) != 1 || !exists {
		w.Header().Set("WWW-Authenticate", `Basic realm="meshcall", charset="UTF-8"`)
		http.Error(w, "authentication failed", http.StatusUnauthorized)
		return nil, false
	}
	return u, true
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
