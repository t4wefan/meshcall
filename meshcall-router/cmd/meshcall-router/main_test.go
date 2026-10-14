package main

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/t4wefan/meshcall/meshcall-router/internal/router"
)

const authDoc = `{"users":[{"username":"app","password_hash":"pbkdf2-sha256$600000$00112233445566778899aabbccddeeff$` +
	`00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff","roles":["client"]}]}`

func withFlags(t *testing.T, args ...string) *time.Duration {
	t.Helper()
	old := flag.CommandLine
	t.Cleanup(func() { flag.CommandLine = old })
	flag.CommandLine = flag.NewFlagSet("meshcall-router", flag.ContinueOnError)
	idle := flag.Duration("idle-timeout", 45*time.Second, "")
	flag.Int("max-connections", 10000, "")
	flag.String("auth-file", "", "")
	if err := flag.CommandLine.Parse(args); err != nil {
		t.Fatal(err)
	}
	return idle
}

func TestEnvironmentFillsFlagsTheCommandLineDidNotSet(t *testing.T) {
	t.Setenv("MESHCALL_ROUTER_IDLE_TIMEOUT", "30s")
	idle := withFlags(t)
	if err := applyEnvironment(); err != nil || *idle != 30*time.Second {
		t.Fatalf("env not applied: %v %v", err, *idle)
	}
	// The command line wins.
	idle = withFlags(t, "--idle-timeout", "10s")
	if err := applyEnvironment(); err != nil || *idle != 10*time.Second {
		t.Fatalf("flag should win over env: %v %v", err, *idle)
	}
	// A bad value names the variable.
	t.Setenv("MESHCALL_ROUTER_MAX_CONNECTIONS", "lots")
	withFlags(t)
	if err := applyEnvironment(); err == nil || !strings.Contains(err.Error(), "MESHCALL_ROUTER_MAX_CONNECTIONS") {
		t.Fatalf("expected an error naming the variable, got %v", err)
	}
}

func TestAuthFromAFileOrInlineButNotBoth(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth.json")
	if err := os.WriteFile(path, []byte(authDoc), 0o600); err != nil {
		t.Fatal(err)
	}
	if auth, err := loadAuth(path); err != nil || auth == nil {
		t.Fatalf("file: %v", err)
	}
	t.Setenv("MESHCALL_ROUTER_AUTH", authDoc)
	if auth, err := loadAuth(""); err != nil || auth == nil {
		t.Fatalf("inline: %v", err)
	}
	if _, err := loadAuth(path); err == nil || !strings.Contains(err.Error(), "not both") {
		t.Fatalf("expected a conflict error, got %v", err)
	}
	t.Setenv("MESHCALL_ROUTER_AUTH", `{"users": []}`)
	if _, err := loadAuth(""); err == nil || !strings.Contains(err.Error(), "MESHCALL_ROUTER_AUTH") {
		t.Fatalf("expected an error naming the variable, got %v", err)
	}
	var _ *router.Auth
}
