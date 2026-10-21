// Command meshcall explores and exercises a MeshCall mesh through the Router.
//
//	meshcall router inspect [service]        what is registered, how healthy, which methods
//	meshcall call svc.method ['{json}']       perform a call; streams print one JSON line per item
//	meshcall version
//
// Global flags: --router URL (ws://, unix:///path, pipe://NAME; default $MESHCALL_ROUTER_URL or ws://127.0.0.1:8765),
// --auth user:password, --token BEARER, --json, --timeout DURATION.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/t4wefan/meshcall/meshcall-router/internal/cli"
	"github.com/t4wefan/meshcall/meshcall-router/internal/skill"
)

var version = "dev"

// asJSON mirrors --json so failures are machine-readable too.
var asJSON bool

func main() {
	if err := run(os.Args[1:]); err != nil {
		var failure *cli.Failure
		if errors.As(err, &failure) {
			if asJSON {
				_ = json.NewEncoder(os.Stderr).Encode(failure)
			} else {
				fmt.Fprintln(os.Stderr, "error", failure.Error())
			}
			os.Exit(1)
		}
		fmt.Fprintln(os.Stderr, "meshcall:", err)
		os.Exit(2)
	}
}

func run(args []string) error {
	global := flag.NewFlagSet("meshcall", flag.ContinueOnError)
	var router, auth, token string
	var timeout time.Duration
	global.StringVar(&router, "router", envOr("MESHCALL_ROUTER_URL", "ws://127.0.0.1:8765"), "Router URL: ws://host:port, unix:///path or pipe://NAME (Windows)")
	global.StringVar(&auth, "auth", "", "user:password for Basic authentication")
	global.StringVar(&token, "token", "", "temporary token for Bearer authentication")
	global.BoolVar(&asJSON, "json", false, "machine-readable output")
	global.DurationVar(&timeout, "timeout", 30*time.Second, "call timeout (also the Router-enforced deadline)")
	global.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: meshcall [--router URL] [--auth user:pass | --token T] [--json] [--timeout D] <command>")
		fmt.Fprintln(os.Stderr, "  router inspect [service]       list services, instances, health and methods")
		fmt.Fprintln(os.Stderr, "  call svc.method ['{json}']     call a method; with --stdin, client streams and duplex calls read JSON lines from stdin")
		fmt.Fprintln(os.Stderr, "    --metadata k=v (repeatable)  --idempotency-key K   --idle-timeout D   --instance ID")
		fmt.Fprintln(os.Stderr, "    --stdin  input items as JSON lines (a duplex call sends each line as it is read)   --http  through the Router's HTTP entry point (not for duplex)")
		fmt.Fprintln(os.Stderr, "  generate <source>... --output PATH [--language python|typescript|go] [--package]")
		fmt.Fprintln(os.Stderr, "    [--service NAME] [--class-name N] [--package-name N] [--package-version V]")
		fmt.Fprintln(os.Stderr, "    [--runtime-import M] [--runtime-version V] [--meshcall-requirement R]")
		fmt.Fprintln(os.Stderr, "  export <source>... --output contract.json     write portable contract via inspect-only Router")
		fmt.Fprintln(os.Stderr, "  init [dir] [--agent universal|claude|cursor|copilot|windsurf|all] [--force]")
		fmt.Fprintln(os.Stderr, "                                 install the MeshCall skill for coding agents into a project")
		fmt.Fprintln(os.Stderr, "  version")
	}
	// Allow global flags before or after the subcommand.
	var positional, flags []string
	for _, a := range args {
		if strings.HasPrefix(a, "-") || (len(flags) > 0 && strings.HasPrefix(flags[len(flags)-1], "-") && !strings.Contains(flags[len(flags)-1], "=") && needsValue(flags[len(flags)-1])) {
			flags = append(flags, a)
		} else {
			positional = append(positional, a)
		}
	}
	callFlags := extractCallFlags(&flags)
	if err := global.Parse(flags); err != nil {
		return err
	}
	if len(positional) == 0 {
		global.Usage()
		return errors.New("a command is required")
	}
	opts := cli.Options{Router: router, Timeout: 10 * time.Second}
	switch {
	case auth != "":
		opts.Authorization = "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
	case token != "":
		opts.Authorization = "Bearer " + token
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	switch positional[0] {
	case "version":
		fmt.Println("meshcall " + version)
		return nil
	case "generate":
		if len(positional) < 2 {
			return errors.New("usage: meshcall generate <source>... --output PATH")
		}
		opts := cli.GenerateOptions{
			Sources: positional[1:], Service: callFlags.service, Language: callFlags.language, Output: callFlags.output,
			Package: callFlags.pkg, SingleFile: callFlags.singleFile, ClassName: callFlags.className, PackageName: callFlags.packageName,
			PackageVersion: callFlags.packageVersion, RuntimeImport: callFlags.runtimeImport,
			RuntimeVersion: callFlags.runtimeVersion, MeshcallRequirement: callFlags.meshcallRequirement,
		}
		written, err := cli.Generate(ctx, opts)
		if err != nil {
			return err
		}
		if asJSON {
			return json.NewEncoder(os.Stdout).Encode(map[string]any{"written": written})
		}
		fmt.Println(opts.Output)
		return nil
	case "export":
		if len(positional) < 2 || callFlags.output == "" {
			return errors.New("usage: meshcall export <source>... --output contract.json")
		}
		if err := cli.Export(ctx, positional[1:], callFlags.output); err != nil {
			return err
		}
		fmt.Println(callFlags.output)
		return nil
	case "init":
		dir := "."
		if len(positional) > 1 {
			dir = positional[1]
		}
		report, err := skill.InstallForAgents(dir, callFlags.agents, callFlags.force)
		if err != nil {
			return err
		}
		if asJSON {
			return json.NewEncoder(os.Stdout).Encode(report)
		}
		for _, f := range report.Files {
			fmt.Printf("%-9s %s\n", f.Outcome, f.Path)
		}
		if report.HasKept() {
			fmt.Println("an existing skill file differs (edited, or from another version); rerun with --force to replace it")
		}
		for _, step := range report.NextSteps {
			fmt.Println("- " + step)
		}
		return nil
	case "router":
		if len(positional) < 2 || positional[1] != "inspect" {
			return errors.New("usage: meshcall router inspect [service]")
		}
		service := ""
		if len(positional) > 2 {
			service = positional[2]
		}
		result, err := cli.Inspect(ctx, opts, service)
		if err != nil {
			return err
		}
		if asJSON {
			return json.NewEncoder(os.Stdout).Encode(result)
		}
		cli.FormatInspect(result, os.Stdout)
		return nil
	case "call":
		if len(positional) < 2 {
			return errors.New("usage: meshcall call svc.method ['{json}']")
		}
		dot := strings.LastIndexByte(positional[1], '.')
		if dot <= 0 || dot == len(positional[1])-1 {
			return fmt.Errorf("expected service.method (e.g. chat.v1.Chat.send), got %q", positional[1])
		}
		call := cli.CallOptions{
			Service: positional[1][:dot], Method: positional[1][dot+1:], Timeout: timeout, JSON: asJSON,
			Metadata: callFlags.metadata, IdempotencyKey: callFlags.idempotencyKey, IdleTimeout: callFlags.idleTimeout,
			Instance: callFlags.instance, HTTP: callFlags.http,
		}
		if len(positional) > 2 {
			if err := json.Unmarshal([]byte(positional[2]), &call.Payload); err != nil {
				return fmt.Errorf("payload is not JSON: %w", err)
			}
		}
		if callFlags.stdinItems {
			call.Input = os.Stdin
		}
		return cli.Call(ctx, opts, call, os.Stdout)
	default:
		global.Usage()
		return fmt.Errorf("unknown command %q", positional[0])
	}
}

type callFlagSet struct {
	metadata       map[string]string
	idempotencyKey string
	idleTimeout    time.Duration
	stdinItems     bool
	http           bool
	force          bool
	agents         []string
	instance       string
	// generate / export
	output, service, language, className, packageName, packageVersion string
	runtimeImport, runtimeVersion, meshcallRequirement                string
	pkg, singleFile                                                   bool
}

// extractCallFlags pulls call-specific flags out before the global parse.
func extractCallFlags(flags *[]string) callFlagSet {
	out := callFlagSet{metadata: map[string]string{}}
	var rest []string
	for i := 0; i < len(*flags); i++ {
		a := (*flags)[i]
		name, value, hasValue := strings.Cut(strings.TrimLeft(a, "-"), "=")
		next := func() string {
			if hasValue {
				return value
			}
			if i+1 < len(*flags) {
				i++
				return (*flags)[i]
			}
			return ""
		}
		switch name {
		case "metadata":
			k, v, _ := strings.Cut(next(), "=")
			out.metadata[k] = v
		case "idempotency-key":
			out.idempotencyKey = next()
		case "instance":
			out.instance = next()
		case "idle-timeout":
			out.idleTimeout, _ = time.ParseDuration(next())
		case "items-from-stdin", "stdin":
			out.stdinItems = true
		case "http":
			out.http = true
		case "force":
			out.force = true
		case "agent":
			out.agents = append(out.agents, next())
		case "output", "o":
			out.output = next()
		case "service":
			out.service = next()
		case "language":
			out.language = next()
		case "class-name":
			out.className = next()
		case "package-name":
			out.packageName = next()
		case "package-version":
			out.packageVersion = next()
		case "runtime-import":
			out.runtimeImport = next()
		case "runtime-version":
			out.runtimeVersion = next()
		case "meshcall-requirement":
			out.meshcallRequirement = next()
		case "package":
			out.pkg = true
		case "single-file":
			out.singleFile = true
		default:
			rest = append(rest, a)
		}
	}
	*flags = rest
	return out
}

func needsValue(flag string) bool {
	switch strings.TrimLeft(flag, "-") {
	case "router", "auth", "token", "timeout", "metadata", "idempotency-key", "idle-timeout", "instance",
		"agent", "output", "o", "service", "language", "class-name", "package-name", "package-version",
		"runtime-import", "runtime-version", "meshcall-requirement":
		return true
	}
	return false
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
