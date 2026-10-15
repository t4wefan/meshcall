// Package cli implements the meshcall command: explore and exercise a mesh
// through the Router's Registry without generated code.
package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/t4wefan/meshcall/meshcall-router/internal/wire"
)

const registryService = "meshcall.router.v1.Registry"

// Options shared by every command.
type Options struct {
	Router        string
	Authorization string
	Timeout       time.Duration
}

func (o Options) dial(ctx context.Context) (*wire.Conn, error) {
	return wire.Dial(ctx, o.Router, wire.Options{Authorization: o.Authorization, PeerID: "meshcall-cli", Timeout: o.Timeout})
}

// Failure is a call-level error with a machine-readable code and a hint.
type Failure struct {
	Code      string         `json:"code"`
	Message   string         `json:"message"`
	Retryable bool           `json:"retryable,omitempty"`
	Details   map[string]any `json:"details,omitempty"`
	Hint      string         `json:"hint,omitempty"`
}

func (f *Failure) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %s", f.Code, f.Message)
	if len(f.Details) > 0 {
		d, _ := json.Marshal(f.Details)
		fmt.Fprintf(&b, "\n  details: %s", d)
	}
	if f.Hint != "" {
		fmt.Fprintf(&b, "\n  hint: %s", f.Hint)
	}
	return b.String()
}

func failureOf(f wire.Frame, hint string) *Failure {
	code, message, retryable, details := wire.Error(f)
	return &Failure{Code: code, Message: message, Retryable: retryable, Details: details, Hint: hint}
}

// --- inspect ---------------------------------------------------------------

// Inspect returns the Registry's view: every service, or one described in
// full (including its contract) when service is non-empty.
func Inspect(ctx context.Context, opts Options, service string) (map[string]any, error) {
	conn, err := opts.dial(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return registry(conn, service)
}

func registry(conn *wire.Conn, service string) (map[string]any, error) {
	method, payload := "list", any(map[string]any{})
	if service != "" {
		method, payload = "describe", map[string]any{"service": service}
	}
	f, err := conn.Unary(registryService, method, payload, nil)
	if err != nil {
		return nil, err
	}
	if f["kind"] == "call.error" {
		code, _, _, _ := wire.Error(f)
		hint := ""
		switch code {
		case "method_not_found":
			if service != "" {
				hint = "nothing is registered under that name; run `meshcall router inspect` to list services (names carry a version, e.g. chat.v1)"
			} else {
				hint = "this Router predates the Registry service; upgrade meshcall-router"
			}
		case "permission_denied":
			hint = "the account needs a call permission on " + registryService + "/*"
		}
		return nil, failureOf(f, hint)
	}
	result, _ := f["payload"].(map[string]any)
	return result, nil
}

// FormatInspect renders a human-readable table for Inspect's result.
func FormatInspect(result map[string]any, w io.Writer) {
	services, _ := result["services"].([]any)
	if services == nil { // describe
		services = []any{result}
	}
	for i, s := range services {
		svc, _ := s.(map[string]any)
		if i > 0 {
			fmt.Fprintln(w)
		}
		name, _ := svc["name"].(string)
		fmt.Fprintf(w, "%s", name)
		if targeted, _ := svc["targeted"].(bool); targeted {
			fmt.Fprint(w, "  [targeted]")
		}
		if d, ok := svc["description"].(string); ok && d != "" {
			fmt.Fprintf(w, "  — %s", firstLine(d))
		}
		fmt.Fprintln(w)
		instances, _ := svc["instances"].([]any)
		if len(instances) == 0 {
			fmt.Fprintln(w, "  instances: none")
		}
		for _, in := range instances {
			inst, _ := in.(map[string]any)
			line := fmt.Sprintf("  instance %-20s inflight=%v", inst["id"], number(inst["inflight"]))
			if m := number(inst["max_inflight"]); m > 0 {
				line += fmt.Sprintf("/%v", m)
			}
			if b, _ := inst["breaker"].(string); b != "" && b != "closed" {
				line += "  breaker=" + b
			}
			if d, _ := inst["draining"].(bool); d {
				line += "  draining"
			}
			fmt.Fprintln(w, line)
		}
		methods, _ := svc["methods"].([]any)
		for _, mm := range methods {
			m, _ := mm.(map[string]any)
			params, _ := m["parameters"].([]any)
			names := make([]string, 0, len(params))
			for _, p := range params {
				names = append(names, fmt.Sprint(p))
			}
			tags := annotationTags(m["annotations"])
			if dep, ok := m["deprecated"].(string); ok {
				tags = append(tags, "deprecated→"+dep)
			}
			line := fmt.Sprintf("  %-14s %s(%s)", m["stream"], m["name"], strings.Join(names, ", "))
			if len(tags) > 0 {
				line += "  [" + strings.Join(tags, ", ") + "]"
			}
			if d, ok := m["description"].(string); ok && d != "" {
				line += "  — " + firstLine(d)
			}
			if reason, ok := m["duplex_reason"].(string); ok {
				line += "  (duplex: " + firstLine(reason) + ")"
			}
			// A method on a subset of instances is a rolling upgrade in progress.
			if on := asList(m["instances"]); len(on) > 0 && len(on) < len(instances) {
				ids := make([]string, 0, len(on))
				for _, id := range on {
					ids = append(ids, fmt.Sprint(id))
				}
				line += "  on: " + strings.Join(ids, ", ")
			}
			fmt.Fprintln(w, line)
		}
	}
}

func annotationTags(v any) []string {
	a, _ := v.(map[string]any)
	var tags []string
	for _, key := range []string{"read_only", "idempotent", "destructive"} {
		if b, _ := a[key].(bool); b {
			tags = append(tags, key)
		}
	}
	return tags
}

// --- call ------------------------------------------------------------------

// CallOptions describes one call made from the command line.
type CallOptions struct {
	Service        string
	Method         string
	Payload        any
	Timeout        time.Duration
	IdleTimeout    time.Duration
	Metadata       map[string]string
	IdempotencyKey string
	// Instance addresses one service instance; required for targeted services.
	Instance string
	// Items feeds a client stream; each element is one item payload.
	Items []any
	// Input, when set, supplies items as JSON lines: read whole for a client
	// stream (unless Items is set), line by line as they arrive for a duplex
	// call. A duplex call without Input sends no items.
	Input io.Reader
	// JSON prints machine-readable lines instead of text.
	JSON bool
	// HTTP calls through the Router's HTTP entry point instead of WebSocket.
	HTTP bool
}

// CallResult is the outcome of a unary or client-stream call.
type CallResult struct {
	Payload  any               `json:"payload"`
	Trailers map[string]string `json:"trailers,omitempty"`
}

// Call looks the method up in the Registry, performs the call and streams
// output to w: the result as JSON, or one JSON line per streamed item.
func Call(ctx context.Context, opts Options, c CallOptions, w io.Writer) error {
	if c.HTTP {
		return callHTTP(ctx, opts, c, w)
	}
	conn, err := opts.dial(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	described, err := registry(conn, c.Service)
	if err != nil {
		return err
	}
	var method map[string]any
	var names []string
	for _, mm := range asList(described["methods"]) {
		m, _ := mm.(map[string]any)
		names = append(names, fmt.Sprint(m["name"]))
		if m["name"] == c.Method {
			method = m
		}
	}
	if method == nil {
		sort.Strings(names)
		return &Failure{Code: "method_not_found", Message: fmt.Sprintf("%s has no method %q", c.Service, c.Method),
			Hint: "methods: " + strings.Join(names, ", ")}
	}
	stream, _ := method["stream"].(string)
	hints := callHints{schema: requestHint(described, c.Method), instances: instanceHint(described)}
	if stream == "duplex" && c.IdempotencyKey != "" {
		return &Failure{Code: "invalid_argument", Message: "duplex calls take no idempotency key",
			Details: map[string]any{"source": "client", "reason": "idempotency_key_on_duplex"},
			Hint:    "a duplex call is never retried or deduplicated; drop --idempotency-key"}
	}
	if stream == "client_stream" && c.Items == nil && c.Input != nil {
		if c.Items, err = ReadItems(c.Input); err != nil {
			return err
		}
	}
	extra := wire.Frame{}
	if c.Timeout > 0 {
		extra["timeout_ms"] = c.Timeout.Milliseconds()
	}
	if c.IdleTimeout > 0 {
		extra["idle_timeout_ms"] = c.IdleTimeout.Milliseconds()
	}
	if len(c.Metadata) > 0 {
		extra["metadata"] = c.Metadata
	}
	if c.IdempotencyKey != "" {
		extra["idempotency_key"] = c.IdempotencyKey
	}
	if c.Instance != "" {
		extra["instance_id"] = c.Instance
	}
	if targeted, _ := described["targeted"].(bool); targeted && c.Instance == "" {
		return &Failure{Code: "invalid_argument", Message: c.Service + " is a targeted service: name the instance",
			Details: map[string]any{"source": "client", "reason": "instance_required"},
			Hint:    instanceHint(described)}
	}
	payload := c.Payload
	if payload == nil {
		payload = map[string]any{}
	}
	id := conn.NewCallID()
	open := wire.Frame{"kind": "call.open", "call_id": id, "service": c.Service, "method": c.Method, "payload": payload}
	for k, v := range extra {
		open[k] = v
	}
	if err := conn.Send(open); err != nil {
		return err
	}
	switch stream {
	case "server_stream":
		return streamOut(conn, id, c, w, hints)
	case "client_stream":
		return streamIn(conn, id, c, w, hints)
	case "duplex":
		return duplex(conn, id, c, w, hints)
	default:
		f, err := waitTerminal(conn, id, c.Timeout)
		if err != nil {
			return err
		}
		return emitTerminal(f, c, w, hints)
	}
}

// callHints is what the CLI learned from the Registry before calling, used to
// explain failures in the caller's terms.
type callHints struct {
	schema    string
	instances string
}

// windowCredit is the output credit the CLI grants a stream: how many items
// it lets the Router buffer for it (docs/protocol.md → Flow control), small
// enough that a burst of them stays well within the Router's byte budget.
const windowCredit = 128

func streamOut(conn *wire.Conn, id string, c CallOptions, w io.Writer, hint callHints) error {
	grant := func(n int) error {
		return conn.Send(wire.Frame{"kind": "stream.window", "call_id": id, "direction": "server", "credit": n})
	}
	if err := grant(windowCredit); err != nil {
		return err
	}
	consumed := 0
	for {
		f, err := recvFor(conn, id, c.Timeout)
		if err != nil {
			return err
		}
		switch f["kind"] {
		case "stream.item":
			line, _ := json.Marshal(f["payload"])
			fmt.Fprintln(w, string(line))
			consumed++
			if consumed%(windowCredit/2) == 0 {
				if err := grant(windowCredit / 2); err != nil {
					return err
				}
			}
		case "stream.end":
		case "call.result":
			// A server stream's terminal result carries no payload; only
			// trailers are worth showing.
			if f["payload"] == nil && len(asMap(f["metadata"])) == 0 {
				return nil
			}
			return emitTerminal(f, c, w, hint)
		case "call.error":
			return emitTerminal(f, c, w, hint)
		}
	}
}

func streamIn(conn *wire.Conn, id string, c CallOptions, w io.Writer, hint callHints) error {
	credit := 0
	sent := 0
	for sent < len(c.Items) || credit < 0 {
		for credit > 0 && sent < len(c.Items) {
			if err := conn.Send(wire.Frame{"kind": "stream.item", "call_id": id, "direction": "client", "sequence": sent, "payload": c.Items[sent]}); err != nil {
				return err
			}
			sent++
			credit--
		}
		if sent == len(c.Items) {
			break
		}
		f, err := recvFor(conn, id, c.Timeout)
		if err != nil {
			return err
		}
		switch f["kind"] {
		case "stream.window":
			credit += int(number(f["credit"]))
		case "call.result", "call.error":
			return emitTerminal(f, c, w, hint)
		}
	}
	if err := conn.Send(wire.Frame{"kind": "stream.end", "call_id": id, "direction": "client"}); err != nil {
		return err
	}
	f, err := waitTerminal(conn, id, c.Timeout)
	if err != nil {
		return err
	}
	return emitTerminal(f, c, w, hint)
}

// duplex sends stdin lines as input items, one per credit, while printing
// output items as they arrive; stdin's end half-closes the input. There is
// no wait limit without --timeout: an interactive session may idle.
func duplex(conn *wire.Conn, id string, c CallOptions, w io.Writer, hint callHints) error {
	var credit atomic.Int64
	granted := make(chan struct{}, 1)
	done := make(chan struct{})
	defer close(done)
	var failure atomic.Pointer[error]
	go func() {
		fail := func(err error) {
			failure.Store(&err)
			_ = conn.Send(wire.Frame{"kind": "call.cancel", "call_id": id, "reason": "invalid_input"})
			conn.Close()
		}
		sequence := 0
		if c.Input != nil {
			scanner := bufio.NewScanner(c.Input)
			scanner.Buffer(make([]byte, 1024*1024), 64*1024*1024)
			for scanner.Scan() {
				line := strings.TrimSpace(scanner.Text())
				if line == "" {
					continue
				}
				var item any
				if err := json.Unmarshal([]byte(line), &item); err != nil {
					fail(fmt.Errorf("input item %d is not JSON: %w", sequence+1, err))
					return
				}
				for credit.Load() == 0 {
					select {
					case <-granted:
					case <-done:
						return
					}
				}
				credit.Add(-1)
				if conn.Send(wire.Frame{"kind": "stream.item", "call_id": id, "direction": "client", "sequence": sequence, "payload": item}) != nil {
					return
				}
				sequence++
			}
			if err := scanner.Err(); err != nil {
				fail(err)
				return
			}
		}
		_ = conn.Send(wire.Frame{"kind": "stream.end", "call_id": id, "direction": "client"})
	}()
	if err := conn.Send(wire.Frame{"kind": "stream.window", "call_id": id, "direction": "server", "credit": windowCredit}); err != nil {
		return err
	}
	wait := 24 * time.Hour
	if c.Timeout > 0 {
		wait = c.Timeout + 2*time.Second
	}
	consumed := 0
	for {
		f, err := conn.RecvWithin(wait)
		if p := failure.Load(); p != nil {
			return *p
		}
		if errors.Is(err, wire.ErrNoFrame) {
			return &Failure{Code: "deadline_exceeded", Message: "no answer from the service"}
		}
		if err != nil {
			return err
		}
		if f["call_id"] != id {
			continue
		}
		switch f["kind"] {
		case "stream.window":
			credit.Add(int64(number(f["credit"])))
			select {
			case granted <- struct{}{}:
			default:
			}
		case "stream.item":
			line, _ := json.Marshal(f["payload"])
			fmt.Fprintln(w, string(line))
			if consumed++; consumed%(windowCredit/2) == 0 {
				if err := conn.Send(wire.Frame{"kind": "stream.window", "call_id": id, "direction": "server", "credit": windowCredit / 2}); err != nil {
					return err
				}
			}
		case "call.result":
			if len(asMap(f["metadata"])) == 0 {
				return nil
			}
			return emitTerminal(f, c, w, hint)
		case "call.error":
			return emitTerminal(f, c, w, hint)
		}
	}
}

func waitTerminal(conn *wire.Conn, id string, timeout time.Duration) (wire.Frame, error) {
	for {
		f, err := recvFor(conn, id, timeout)
		if err != nil {
			return nil, err
		}
		if kind := f["kind"]; kind == "call.result" || kind == "call.error" {
			return f, nil
		}
	}
}

func recvFor(conn *wire.Conn, id string, timeout time.Duration) (wire.Frame, error) {
	wait := conn.Timeout
	if timeout > 0 {
		wait = timeout + 2*time.Second
	}
	for {
		f, err := conn.RecvWithin(wait)
		if errors.Is(err, wire.ErrNoFrame) {
			return nil, &Failure{Code: "deadline_exceeded", Message: "no answer from the service",
				Hint: "the Router accepted the call but nothing came back; check `meshcall router inspect` for a draining or tripped instance, or pass --timeout so the Router cancels it"}
		}
		if err != nil {
			return nil, err
		}
		if f["call_id"] == id {
			return f, nil
		}
	}
}

func emitTerminal(f wire.Frame, c CallOptions, w io.Writer, hints callHints) error {
	trailers := map[string]string{}
	for k, v := range asMap(f["metadata"]) {
		trailers[k] = fmt.Sprint(v)
	}
	if f["kind"] == "call.error" {
		code, _, _, _ := wire.Error(f)
		hint := ""
		switch code {
		case "invalid_argument":
			hint = violationHint(f, hints.schema)
			if _, _, _, details := wire.Error(f); details["reason"] == "instance_required" {
				hint = "this is a targeted service; pass --instance ID. " + hints.instances
			}
		case "unavailable":
			hint = "no healthy instance took the call; `meshcall router inspect` shows instances, breakers and draining"
			if _, _, _, details := wire.Error(f); details["reason"] == "instance_unknown" || details["reason"] == "instance_draining" {
				hint = hints.instances
			}
		case "permission_denied":
			hint = "the account lacks a call permission for this method"
		case "internal":
			if _, _, _, details := wire.Error(f); details["reason"] == "invalid_response" || details["reason"] == "invalid_output_item" {
				hint = "the service answered with data that breaks its own contract; the violations are in the service's log"
			}
		}
		return failureOf(f, hint)
	}
	if c.JSON {
		line, _ := json.Marshal(CallResult{Payload: f["payload"], Trailers: trailers})
		fmt.Fprintln(w, string(line))
		return nil
	}
	pretty, _ := json.MarshalIndent(f["payload"], "", "  ")
	fmt.Fprintln(w, string(pretty))
	if len(trailers) > 0 {
		t, _ := json.Marshal(trailers)
		fmt.Fprintf(w, "trailers: %s\n", t)
	}
	return nil
}

// requestHint summarises the method's request schema for error messages.
func requestHint(described map[string]any, method string) string {
	var entry struct {
		Methods []struct {
			Name    string `json:"name"`
			Request struct {
				Schema map[string]any `json:"schema"`
			} `json:"request"`
		} `json:"methods"`
	}
	raw, err := json.Marshal(described["contract"])
	if err != nil || json.Unmarshal(raw, &entry) != nil {
		return ""
	}
	for _, m := range entry.Methods {
		if m.Name != method {
			continue
		}
		props, _ := m.Request.Schema["properties"].(map[string]any)
		required := map[string]bool{}
		for _, r := range asList(m.Request.Schema["required"]) {
			required[fmt.Sprint(r)] = true
		}
		var parts []string
		for _, name := range sortedKeys(props) {
			p, _ := props[name].(map[string]any)
			t := fmt.Sprint(p["type"])
			if ref, ok := p["$ref"].(string); ok {
				t = strings.TrimPrefix(ref, "#/$defs/")
			}
			if required[name] {
				parts = append(parts, name+": "+t)
			} else {
				parts = append(parts, name+"?: "+t)
			}
		}
		return "request is {" + strings.Join(parts, ", ") + "}"
	}
	return ""
}

// ReadItems parses JSON Lines (one item per line) for client streams.
func ReadItems(r io.Reader) ([]any, error) {
	var items []any
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1024*1024), 64*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var item any
		if err := json.Unmarshal([]byte(line), &item); err != nil {
			return nil, fmt.Errorf("item %d is not JSON: %w", len(items)+1, err)
		}
		items = append(items, item)
	}
	return items, scanner.Err()
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

func asList(v any) []any         { l, _ := v.([]any); return l }
func asMap(v any) map[string]any { m, _ := v.(map[string]any); return m }
func number(v any) float64       { n, _ := v.(float64); return n }

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// violationHint turns details.violations into one line per offending field,
// followed by the request shape from the contract.
func violationHint(f wire.Frame, schemaHint string) string {
	_, _, _, details := wire.Error(f)
	raw, _ := details["violations"].([]any)
	var lines []string
	for _, item := range raw {
		v, _ := item.(map[string]any)
		path, _ := v["path"].(string)
		if path == "" {
			path = "(request)"
		}
		lines = append(lines, fmt.Sprintf("%s: %v (%v)", path, v["message"], v["code"]))
	}
	if len(lines) == 0 {
		return schemaHint
	}
	return strings.Join(lines, "; ") + "\n  " + schemaHint
}

// instanceHint lists the instances the Registry currently knows for the service.
func instanceHint(described map[string]any) string {
	var ids []string
	for _, in := range asList(described["instances"]) {
		inst, _ := in.(map[string]any)
		id := fmt.Sprint(inst["id"])
		if d, _ := inst["draining"].(bool); d {
			id += " (draining)"
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return "no instances are registered right now"
	}
	sort.Strings(ids)
	return "instances: " + strings.Join(ids, ", ")
}
