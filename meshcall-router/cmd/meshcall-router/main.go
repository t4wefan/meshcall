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
	flag.StringVar(&config.UnixPath, "unix-socket", "", "Unix socket path, exclusive with host/port")
	flag.IntVar(&config.MaxFrameSize, "max-frame-size", 1024*1024, "maximum frame bytes")
	flag.StringVar(&authPath, "auth-file", "", "JSON account and permission configuration")
	flag.BoolVar(&parentStdin, "shutdown-on-stdin-close", false, "exit when the owning process closes stdin")
	flag.BoolVar(&showVersion, "version", false, "print version and exit")
	flag.Parse()
	if showVersion {
		fmt.Println("meshcall-router " + version)
		return nil
	}
	if flag.NArg() != 0 {
		return fmt.Errorf("unexpected arguments")
	}
	if config.UnixPath != "" {
		conflict := false
		flag.Visit(func(f *flag.Flag) {
			if f.Name == "host" || f.Name == "port" {
				conflict = true
			}
		})
		if conflict {
			return fmt.Errorf("Unix socket cannot be combined with host or port")
		}
	}
	var err error
	config.Auth, err = router.LoadAuth(authPath)
	if err != nil {
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
	case <-parentGone:
	case <-r.Done():
		return fmt.Errorf("router listener stopped unexpectedly")
	}
	return nil
}
