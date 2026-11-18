package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/t4wefan/meshcall/meshcall-router/conformance"
)

// envMain serves a client environment (conformance/CLIENTS.md):
//
//	meshcall-conformance env --fixture "meshcall-go/bin/conformance-fixture"
//
// It prints {"kind":"env.ready","router_url":…} and then answers one
// command per stdin line until stdin ends.
func envMain(args []string) {
	flags := flag.NewFlagSet("env", flag.ExitOnError)
	fixture := flags.String("fixture", "", "command that starts the fixture service the client calls (required)")
	_ = flags.Parse(args)
	if *fixture == "" {
		fmt.Fprintln(os.Stderr, "usage: meshcall-conformance env --fixture \"<command>\"")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := conformance.Serve(ctx, conformance.EnvOptions{Fixture: strings.Fields(*fixture)}, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "conformance env:", err)
		os.Exit(1)
	}
}
