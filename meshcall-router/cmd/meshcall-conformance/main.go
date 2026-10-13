// Command meshcall-conformance checks a MeshCall runtime against the protocol.
//
//	meshcall-conformance --fixture "uv run python conformance/fixture.py"
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/t4wefan/meshcall/meshcall-router/conformance"
)

func main() {
	var fixture, only string
	var verbose, asJSON bool
	var timeout time.Duration
	flag.StringVar(&fixture, "fixture", "", "command that starts the runtime under test (required)")
	flag.StringVar(&only, "only", "", "run only scenarios whose name contains this text")
	flag.BoolVar(&verbose, "verbose", false, "print fixture output even on success")
	flag.BoolVar(&asJSON, "json", false, "print results as JSON instead of text")
	flag.DurationVar(&timeout, "timeout", 5*time.Second, "maximum wait for any single frame")
	flag.Parse()
	if fixture == "" {
		fmt.Fprintln(os.Stderr, "usage: meshcall-conformance --fixture \"<command>\" [--only text] [--json]")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	out := os.Stdout
	if asJSON {
		out = os.Stderr
	}
	results, err := conformance.Run(ctx, conformance.Options{
		Fixture: strings.Fields(fixture), Only: only, Verbose: verbose, Timeout: timeout,
	}, out)
	if err != nil {
		fmt.Fprintln(os.Stderr, "conformance:", err)
		os.Exit(2)
	}
	if asJSON {
		_ = json.NewEncoder(os.Stdout).Encode(results)
	}
	for _, r := range results {
		if !r.Passed {
			os.Exit(1)
		}
	}
}
