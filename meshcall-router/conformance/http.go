package conformance

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// HTTP scenarios drive the fixture through the Router's HTTP entry point
// (docs/protocol.md → HTTP calls). The runtimes do not know the call came
// over HTTP, so these check that the translation preserves each shape.

func (h *harness) httpCall(ctx context.Context, method, body string, headers map[string]string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("http://127.0.0.1:%d/v1/call/%s/%s", h.port, Service, method), strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return http.DefaultClient.Do(req)
}

func readJSON(resp *http.Response) (map[string]any, error) {
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("status %d, undecodable body: %w", resp.StatusCode, err)
	}
	return out, nil
}

func httpUnary(h *harness) error {
	ctx := h.ctx
	resp, err := h.httpCall(ctx, "echo", `{"value": 7, "text": "héllo"}`, map[string]string{"Content-Type": "application/json"})
	if err != nil {
		return err
	}
	body, err := readJSON(resp)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 || number(body["value"]) != 7 || body["text"] != "héllo" {
		return fmt.Errorf("echo over HTTP: %d %v", resp.StatusCode, body)
	}
	// Headers become metadata; trailers come back as headers.
	resp, err = h.httpCall(ctx, "metadata_echo", ``, map[string]string{"MeshCall-Meta-Traceparent": "00-abc"})
	if err != nil {
		return err
	}
	body, err = readJSON(resp)
	if err != nil {
		return err
	}
	meta, _ := body["metadata"].(map[string]any)
	if resp.StatusCode != 200 || meta["traceparent"] != "00-abc" || resp.Header.Get("MeshCall-Trailer-echoed") != "yes" {
		return fmt.Errorf("metadata over HTTP: %d %v trailers=%v", resp.StatusCode, body, resp.Header)
	}
	// A validation failure keeps its envelope and is a 400.
	resp, err = h.httpCall(ctx, "echo", `{"value": "x", "text": "t"}`, nil)
	if err != nil {
		return err
	}
	body, err = readJSON(resp)
	if err != nil {
		return err
	}
	e, _ := body["error"].(map[string]any)
	details, _ := e["details"].(map[string]any)
	if resp.StatusCode != 400 || e["code"] != "invalid_argument" || details["reason"] != "request" {
		return fmt.Errorf("invalid request over HTTP: %d %v", resp.StatusCode, body)
	}
	// The service's own deadline is a 504.
	resp, err = h.httpCall(ctx, "slow", `{"ms": 2000, "tag": "http-timeout"}`, map[string]string{"MeshCall-Timeout-Ms": "100"})
	if err != nil {
		return err
	}
	body, err = readJSON(resp)
	if err != nil {
		return err
	}
	if e, _ := body["error"].(map[string]any); resp.StatusCode != 504 || e["code"] != "deadline_exceeded" {
		return fmt.Errorf("timeout over HTTP: %d %v", resp.StatusCode, body)
	}
	return nil
}

func httpServerStream(h *harness) error {
	resp, err := h.httpCall(h.ctx, "count", `{"n": 300}`, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "application/x-ndjson" {
		return fmt.Errorf("count over HTTP: %d %s", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	// More items than one credit window: the Router must keep granting.
	scanner := bufio.NewScanner(resp.Body)
	n := 0
	for scanner.Scan() {
		var item map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &item); err != nil || number(item["index"]) != float64(n) {
			return fmt.Errorf("line %d: %q", n, scanner.Text())
		}
		n++
	}
	if n != 300 {
		return fmt.Errorf("got %d items, want 300", n)
	}
	// SSE when asked for.
	resp, err = h.httpCall(h.ctx, "count", `{"n": 2}`, map[string]string{"Accept": "text/event-stream"})
	if err != nil {
		return err
	}
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	want := "event: item\ndata: {\"index\":0}\n\nevent: item\ndata: {\"index\":1}\n\nevent: end\ndata: {}\n\n"
	if string(data) != want {
		return fmt.Errorf("SSE body %q", data)
	}
	return nil
}

func httpClientStream(h *harness) error {
	var b strings.Builder
	b.WriteString("{\"offset\": 10}\n")
	for i := 1; i <= 40; i++ { // beyond the initial 16 credits
		fmt.Fprintf(&b, "{\"value\": %d}\n", i)
	}
	resp, err := h.httpCall(h.ctx, "sum", b.String(), map[string]string{"Content-Type": "application/x-ndjson"})
	if err != nil {
		return err
	}
	body, err := readJSON(resp)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 || number(body["total"]) != 10+40*41/2 {
		return fmt.Errorf("sum over HTTP: %d %v", resp.StatusCode, body)
	}
	return nil
}

func httpCancelOnDisconnect(h *harness) error {
	ctx, cancel := context.WithCancel(h.ctx)
	go func() {
		resp, err := h.httpCall(ctx, "slow", `{"ms": 5000, "tag": "http-cancel"}`, nil)
		if err == nil {
			resp.Body.Close()
		}
	}()
	time.Sleep(150 * time.Millisecond)
	cancel()
	c, err := h.client()
	if err != nil {
		return err
	}
	defer c.close()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		result, err := c.unary("cancelled_tags", frame{}, nil)
		if err != nil {
			return err
		}
		tags, _ := result["payload"].(map[string]any)["tags"].([]any)
		for _, t := range tags {
			if t == "http-cancel" {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("closing the HTTP request did not cancel the handler")
}
