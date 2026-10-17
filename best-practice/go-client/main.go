// Command go-client calls the best-practice LLM service through the Router.
//
// It connects with meshcall.NewRouter (MESHCALL_ROUTER_URL, default
// ws://127.0.0.1:8765), creates a session, streams one answer, and lists the
// sessions it can see, using the generated typed client.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	meshcall "github.com/t4wefan/meshcall/meshcall-go"

	"github.com/t4wefan/meshcall/best-practice/go-client/llmclient"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "go-client:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	router := meshcall.NewRouter()
	defer func() { _ = router.Close() }()

	client := llmclient.NewLlmServiceClient(router).WithOptions(meshcall.CallOptions{
		Metadata: map[string]string{"x-caller-id": "go-client"},
		Timeout:  20 * time.Second,
	})

	created, err := client.NewSession(ctx, nil)
	if err != nil {
		return err
	}
	fmt.Printf("session %s · %s\n", created.Session.ID, created.Session.Title)

	assembled, err := client.AssemblePrompt(ctx, func(yield func(llmclient.PromptChunk) bool) {
		for _, chunk := range strings.Fields("Why stream tokens? Because the first one arrives sooner.") {
			if !yield(llmclient.PromptChunk{Text: chunk + " "}) {
				return
			}
		}
	})
	if err != nil {
		return err
	}
	count, err := client.CountTokens(ctx, assembled.Text)
	if err != nil {
		return err
	}
	fmt.Printf("prompt chunks: %d · prompt tokens: %d (%s)\n",
		assembled.ChunkCount, count.Count, count.Tokenizer)

	maxTokens := int64(24)
	stream, err := client.StreamChat(ctx, created.Session.ID, assembled.Text, &maxTokens)
	if err != nil {
		return err
	}
	var reply strings.Builder
	for chunk, err := range stream.All() {
		if err != nil {
			return err
		}
		reply.WriteString(chunk.Delta)
	}
	fmt.Printf("assistant> %s\n", strings.TrimSpace(reply.String()))

	sessions, err := client.ListSessions(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("sessions: %d\n", len(sessions.Sessions))
	return nil
}
