package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/t4wefan/meshcall/meshcall-router/internal/localsock"
	"github.com/t4wefan/meshcall/meshcall-router/internal/wire"
)

// HTTP mode (`meshcall call --http`): the same call through the Router's HTTP
// entry point (docs/protocol.md → HTTP calls), with the same output, so it
// doubles as a working reference for HTTP callers.

func httpBase(router string) (string, error) {
	if endpoint, err := localsock.Parse(router); err != nil {
		return "", err
	} else if endpoint.Dial != nil {
		return "http://localhost", nil // the client's transport dials the socket or pipe
	}
	u, err := url.Parse(router)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "ws", "http":
		u.Scheme = "http"
	case "wss", "https":
		u.Scheme = "https"
	default:
		return "", fmt.Errorf("--http needs an http(s)://, ws(s)://, unix:// or pipe:// Router URL, got %q", router)
	}
	u.Path, u.RawQuery = "", ""
	return strings.TrimRight(u.String(), "/"), nil
}

func (o Options) httpDo(ctx context.Context, method, path, contentType string, body io.Reader, headers map[string]string) (*http.Response, error) {
	base, err := httpBase(o.Router)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, base+path, body)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if o.Authorization != "" {
		req.Header.Set("Authorization", o.Authorization)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	endpoint, err := localsock.Parse(o.Router)
	if err != nil {
		return nil, err
	}
	return endpoint.HTTPClient().Do(req)
}

// httpFrame turns a terminal HTTP response into the frame emitTerminal reads.
func httpFrame(resp *http.Response, body []byte) wire.Frame {
	trailers := map[string]any{}
	for name, values := range resp.Header {
		if strings.HasPrefix(strings.ToLower(name), "meshcall-trailer-") && len(values) > 0 {
			trailers[strings.ToLower(name[len("Meshcall-Trailer-"):])] = values[0]
		}
	}
	var decoded map[string]any
	_ = json.Unmarshal(body, &decoded)
	if resp.StatusCode != http.StatusOK {
		if e, ok := decoded["error"].(map[string]any); ok {
			return wire.Frame{"kind": "call.error", "error": e, "metadata": trailers}
		}
		return wire.Frame{"kind": "call.error", "metadata": trailers, "error": map[string]any{
			"code": "unavailable", "message": fmt.Sprintf("HTTP %d: %s", resp.StatusCode, bytes.TrimSpace(body)),
		}}
	}
	var payload any
	_ = json.Unmarshal(body, &payload)
	return wire.Frame{"kind": "call.result", "payload": payload, "metadata": trailers}
}

func callHTTP(ctx context.Context, opts Options, c CallOptions, w io.Writer) error {
	resp, err := opts.httpDo(ctx, http.MethodGet, "/v1/services/"+url.PathEscape(c.Service), "", nil, nil)
	if err != nil {
		return err
	}
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return failureOf(httpFrame(resp, data), "")
	}
	var described map[string]any
	if err := json.Unmarshal(data, &described); err != nil {
		return err
	}
	var stream string
	var names []string
	for _, mm := range asList(described["methods"]) {
		m, _ := mm.(map[string]any)
		names = append(names, fmt.Sprint(m["name"]))
		if m["name"] == c.Method {
			stream, _ = m["stream"].(string)
		}
	}
	if stream == "" {
		sort.Strings(names)
		return &Failure{Code: "method_not_found", Message: fmt.Sprintf("%s has no method %q", c.Service, c.Method),
			Hint: "methods: " + strings.Join(names, ", ")}
	}
	if stream == "duplex" {
		return &Failure{Code: "method_not_found", Message: "duplex methods are WebSocket-only", Hint: "drop --http"}
	}
	hints := callHints{schema: requestHint(described, c.Method), instances: instanceHint(described)}

	headers := map[string]string{}
	if c.Timeout > 0 {
		headers["MeshCall-Timeout-Ms"] = strconv.FormatInt(c.Timeout.Milliseconds(), 10)
	}
	if c.IdleTimeout > 0 {
		headers["MeshCall-Idle-Timeout-Ms"] = strconv.FormatInt(c.IdleTimeout.Milliseconds(), 10)
	}
	if c.IdempotencyKey != "" {
		headers["Idempotency-Key"] = c.IdempotencyKey
	}
	if c.Instance != "" {
		headers["MeshCall-Instance"] = c.Instance
	}
	for k, v := range c.Metadata {
		headers["MeshCall-Meta-"+k] = v
	}
	payload := c.Payload
	if payload == nil {
		payload = map[string]any{}
	}
	request, _ := json.Marshal(payload)
	path := "/v1/call/" + url.PathEscape(c.Service) + "/" + url.PathEscape(c.Method)

	var body io.Reader = bytes.NewReader(request)
	contentType := "application/json"
	if stream == "client_stream" {
		if c.Items == nil && c.Input != nil {
			if c.Items, err = ReadItems(c.Input); err != nil {
				return err
			}
		}
		var b bytes.Buffer
		b.Write(request)
		b.WriteByte('\n')
		for _, item := range c.Items {
			line, _ := json.Marshal(item)
			b.Write(line)
			b.WriteByte('\n')
		}
		body, contentType = &b, "application/x-ndjson"
	}
	resp, err = opts.httpDo(ctx, http.MethodPost, path, contentType, body, headers)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if stream != "server_stream" || resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		return emitTerminal(httpFrame(resp, data), c, w, hints)
	}
	// Server stream: one typed envelope per line, {"item"}, then {"end"} or {"error"}.
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 64*1024*1024)
	for scanner.Scan() {
		var envelope struct {
			Item  json.RawMessage `json:"item"`
			End   json.RawMessage `json:"end"`
			Error map[string]any  `json:"error"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &envelope); err != nil {
			return fmt.Errorf("stream line is not an envelope: %q", scanner.Text())
		}
		switch {
		case envelope.Item != nil:
			fmt.Fprintln(w, string(envelope.Item))
		case envelope.Error != nil:
			return emitTerminal(wire.Frame{"kind": "call.error", "error": envelope.Error}, c, w, hints)
		case envelope.End != nil:
			return nil
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("stream broke: %w", err)
	}
	return fmt.Errorf("stream ended without an end or error line")
}
