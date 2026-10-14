package localsock

import (
	"strings"
	"testing"
)

func TestPipeName(t *testing.T) {
	for in, want := range map[string]string{"meshcall": "meshcall", `\\.\pipe\meshcall-1`: "meshcall-1", "a/b": "a/b"} {
		if got, err := PipeName(in); err != nil || got != want {
			t.Errorf("PipeName(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"", `\\.\pipe\`, `a\b`, strings.Repeat("x", 257)} {
		if _, err := PipeName(bad); err == nil {
			t.Errorf("PipeName(%q) accepted", bad)
		}
	}
}

func TestParse(t *testing.T) {
	e, err := Parse("ws://127.0.0.1:8765")
	if err != nil || e.Dial != nil || e.URL != "ws://127.0.0.1:8765" {
		t.Fatalf("ws: %+v %v", e, err)
	}
	if _, err := Parse("unix://"); err == nil && UnixSupported() {
		t.Fatal("empty unix path accepted")
	}
	_, unixErr := Parse("unix:///tmp/meshcall.sock")
	_, pipeErr := Parse("pipe://meshcall")
	if UnixSupported() {
		if unixErr != nil || pipeErr != ErrPipeOffWindows {
			t.Fatalf("unix: %v, pipe: %v", unixErr, pipeErr)
		}
	} else if pipeErr != nil || unixErr != ErrUnixOnWindows {
		t.Fatalf("unix: %v, pipe: %v", unixErr, pipeErr)
	}
}
