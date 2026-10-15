package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/t4wefan/meshcall/meshcall-router/internal/router"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		slog.Error("meshcall-router", "error", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) > 1 && os.Args[1] == "new-api-key" {
		if len(os.Args) != 3 {
			return fmt.Errorf("usage: meshcall-router new-api-key ID")
		}
		key, keyHash, err := router.NewAPIKey(os.Args[2])
		if err != nil {
			return err
		}
		// The key goes to the caller once; only its digest goes in the auth file.
		entry, _ := json.MarshalIndent(map[string]any{
			"id": os.Args[2], "key_hash": keyHash, "roles": []string{"client"}, "call": []string{},
		}, "", "  ")
		fmt.Fprintln(os.Stderr, "API key (give it to the caller; it is not stored anywhere):")
		fmt.Println(key)
		fmt.Fprintln(os.Stderr, "Add to \"api_keys\" in the auth file, then fill in \"call\":")
		fmt.Fprintln(os.Stderr, string(entry))
		return nil
	}
	if len(os.Args) > 1 && os.Args[1] == "hash-password" {
		password, err := io.ReadAll(io.LimitReader(os.Stdin, 1027))
		if err != nil {
			return err
		}
		hash, err := router.HashPassword(strings.TrimRight(string(password), "\r\n"))
		if err != nil {
			return err
		}
		fmt.Println(hash)
		return nil
	}
	var config router.Config
	var authPath string
	var parentStdin, showVersion bool
	flag.StringVar(&config.Host, "host", "", "TCP host (default 127.0.0.1)")
	flag.IntVar(&config.Port, "port", 0, "TCP port (0 chooses an available port)")
	flag.StringVar(&config.UnixPath, "unix-socket", "", "Unix socket path, exclusive with host/port (not on Windows: use --pipe)")
	flag.StringVar(&config.Pipe, "pipe", "", `named pipe \\.\pipe\NAME, exclusive with host/port (Windows only: elsewhere use --unix-socket)`)
	flag.IntVar(&config.MaxFrameSize, "max-frame-size", router.DefaultMaxFrameSize, "maximum frame bytes (at most 64 MiB)")
	flag.IntVar(&config.OutboxSize, "outbox-size", router.DefaultOutboxSize, "bytes queued for a peer that is not reading before it is disconnected (at least max-frame-size)")
	flag.IntVar(&config.MaxCallsPerConnection, "max-calls-per-connection", router.DefaultMaxCallsPerConnection, "calls one connection may have in flight (0: no limit)")
	flag.DurationVar(&config.ShutdownTimeout, "shutdown-timeout", router.DefaultShutdownTimeout, "on SIGTERM/SIGINT, how long calls in flight may finish before they are ended (0: end them at once)")
	flag.StringVar(&authPath, "auth-file", "", "JSON account and permission configuration")
	flag.DurationVar(&config.DeadlineGrace, "deadline-grace", router.DefaultDeadlineGrace, "added to each call's timeout before the router enforces it")
	flag.DurationVar(&config.IdleTimeout, "idle-timeout", router.DefaultIdleTimeout, "close connections that send nothing for this long")
	flag.IntVar(&config.BreakerThreshold, "breaker-threshold", router.DefaultBreakerThreshold, "consecutive failures that open an instance's circuit")
	flag.DurationVar(&config.BreakerCooldown, "breaker-cooldown", router.DefaultBreakerCooldown, "how long an open circuit excludes an instance before one probe call")
	flag.DurationVar(&config.HTTPDefaultTimeout, "http-default-timeout", router.DefaultHTTPDefaultTimeout, "timeout for HTTP unary and client-stream calls that send none")
	flag.DurationVar(&config.HTTPMaxTimeout, "http-max-timeout", 0, "refuse HTTP calls asking for a longer timeout (0: no maximum)")
	flag.IntVar(&config.HTTPMaxInflight, "http-max-inflight", router.DefaultHTTPMaxInflight, "concurrent HTTP calls; more get 429")
	flag.Func("http-cors-origin", "browser origin allowed to call over HTTP (repeatable or comma-separated; * for any)", func(v string) error {
		for _, origin := range strings.Split(v, ",") {
			if origin = strings.TrimSpace(origin); origin != "" {
				config.HTTPCORSOrigins = append(config.HTTPCORSOrigins, origin)
			}
		}
		return nil
	})
	flag.IntVar(&config.MaxAnonymousConnections, "max-anonymous-connections", router.DefaultMaxAnonymousConnections, "WebSocket connections without credentials, within max-connections")
	flag.IntVar(&config.MaxConnections, "max-connections", router.DefaultMaxConnections, "open WebSocket connections; more are refused with 503 (HTTP calls are not counted)")
	flag.StringVar(&config.MetricsListen, "metrics-listen", "", "serve Prometheus /metrics on this TCP address (e.g. 127.0.0.1:9464); off by default, never on the public port")
	flag.BoolVar(&parentStdin, "shutdown-on-stdin-close", false, "exit when the owning process closes stdin")
	flag.BoolVar(&showVersion, "version", false, "print version and exit")
	flag.Usage = func() {
		out := flag.CommandLine.Output()
		fmt.Fprintln(out, "usage: meshcall-router [flags]")
		fmt.Fprintln(out, "Every flag can also be set as MESHCALL_ROUTER_<FLAG> (e.g. MESHCALL_ROUTER_IDLE_TIMEOUT=30s);")
		fmt.Fprintln(out, "a flag on the command line wins. MESHCALL_ROUTER_AUTH holds an auth document inline instead of a file.")
		flag.PrintDefaults()
	}
	flag.Parse()
	if err := applyEnvironment(); err != nil {
		return err
	}
	if showVersion {
		fmt.Println("meshcall-router " + version)
		return nil
	}
	if flag.NArg() != 0 {
		return fmt.Errorf("unexpected arguments")
	}
	if config.UnixPath != "" || config.Pipe != "" {
		conflict := false
		flag.Visit(func(f *flag.Flag) {
			if f.Name == "host" || f.Name == "port" {
				conflict = true
			}
		})
		if conflict {
			return fmt.Errorf("a Unix socket or named pipe cannot be combined with host or port")
		}
	}
	// The flags spell "none" as 0; Config reserves 0 for the default.
	if config.MaxCallsPerConnection == 0 {
		config.MaxCallsPerConnection = -1
	}
	if config.ShutdownTimeout == 0 {
		config.ShutdownTimeout = -1
	}
	var err error
	if config.Auth, err = loadAuth(authPath); err != nil {
		return err
	}
	r, err := router.Start(config)
	if err != nil {
		return err
	}
	defer r.Close()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	parentGone := make(chan struct{})
	if parentStdin {
		go func() { _, _ = io.Copy(io.Discard, os.Stdin); close(parentGone) }()
	}
	// stdout is a versioned machine readiness channel; diagnostics use stderr.
	if err := json.NewEncoder(os.Stdout).Encode(r.Ready()); err != nil {
		return err
	}
	select {
	case <-signals:
		go func() { <-signals; r.Close() }() // a second signal skips the drain
		r.Shutdown()
	case <-parentGone:
	case <-r.Done():
		return fmt.Errorf("router listener stopped unexpectedly")
	}
	return nil
}

const envPrefix = "MESHCALL_ROUTER_"

// envName is the environment variable for a flag: auth-file → MESHCALL_ROUTER_AUTH_FILE.
func envName(flagName string) string {
	return envPrefix + strings.ToUpper(strings.ReplaceAll(flagName, "-", "_"))
}

// applyEnvironment fills every flag not given on the command line from its
// environment variable, so containers can be configured without arguments.
func applyEnvironment() error {
	given := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { given[f.Name] = true })
	var err error
	flag.VisitAll(func(f *flag.Flag) {
		if err != nil || given[f.Name] || f.Name == "version" {
			return
		}
		value, ok := os.LookupEnv(envName(f.Name))
		if !ok {
			return
		}
		if setErr := flag.Set(f.Name, value); setErr != nil {
			err = fmt.Errorf("%s: %w", envName(f.Name), setErr)
		}
	})
	return err
}

// loadAuth reads accounts from --auth-file / MESHCALL_ROUTER_AUTH_FILE or
// from an inline document in MESHCALL_ROUTER_AUTH; giving both is an error
// so it is never unclear which one is in force.
func loadAuth(path string) (*router.Auth, error) {
	inline, hasInline := os.LookupEnv(envPrefix + "AUTH")
	if hasInline && path != "" {
		return nil, fmt.Errorf("set either %sAUTH or an auth file, not both", envPrefix)
	}
	if hasInline {
		auth, err := router.ParseAuth([]byte(inline))
		if err != nil {
			return nil, fmt.Errorf("%sAUTH: %w", envPrefix, err)
		}
		return auth, nil
	}
	return router.LoadAuth(path)
}
