package router

import (
	"bytes"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
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
}

// Auth is immutable after startup. An absent file explicitly means development mode.
type Auth struct {
	users map[string]*User
	slots chan struct{}
	dummy *User
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
	auth := &Auth{users: make(map[string]*User), slots: make(chan struct{}, 8)}
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
		for _, rule := range append(slices.Clone(u.Register), u.Call...) {
			if rule == "" || (strings.Contains(rule, "*") && rule != "*" && (!strings.HasSuffix(rule, "/*") || strings.Count(rule, "*") != 1)) {
				return nil, errors.New("ACL rules must be exact names, service/*, or *")
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

func (u *User) allowsRole(role string) bool {
	return u == nil || slices.Contains(u.Roles, role)
}

func (u *User) allowsRegistration(name string) bool {
	return u == nil || slices.Contains(u.Register, "*") || slices.Contains(u.Register, name)
}

func (u *User) allowsCall(service, method string) bool {
	return u == nil || slices.Contains(u.Call, "*") || slices.Contains(u.Call, service+"/*") || slices.Contains(u.Call, service+"/"+method)
}

// Referenced by tests to exercise UTF-8 credentials exactly as both SDKs encode them.
func basicHeader(username, password string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
}
