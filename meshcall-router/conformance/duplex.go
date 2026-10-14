package conformance

import (
	"errors"
	"fmt"
	"time"
)

// Duplex scenarios (docs/protocol.md → Duplex calls). The fixture's `chat`
// echoes each input as `prefix + text` and, once the input ends, sends
// `done:<count>`; `announce` sends `hello`, half-closes its output, then
// reads the input to its end.

// nextData returns the next frame for id that is not an input-credit grant:
// a duplex server grants input credit as it reads, interleaved with output.
func (c *client) nextData(id string, within time.Duration) (frame, error) {
	deadline := time.Now().Add(within)
	for {
		f, err := c.receiveWithin(time.Until(deadline))
		if err != nil {
			return nil, err
		}
		if f["call_id"] != id || (f["kind"] == "stream.window" && f["direction"] == "client") {
			continue
		}
		return f, nil
	}
}

// expectNoData fails if anything but an input-credit grant arrives for id within d.
func (c *client) expectNoData(id string, d time.Duration) error {
	f, err := c.nextData(id, d)
	if err == nil {
		return fmt.Errorf("unexpected frame %v", f)
	}
	if errors.Is(err, errNoFrame) {
		return nil
	}
	return err
}

func (c *client) expectOutput(id string, sequence int, text string) error {
	f, err := c.nextData(id, c.h.opts.Timeout)
	if err != nil {
		return fmt.Errorf("output %d: %w", sequence, err)
	}
	payload, _ := f["payload"].(map[string]any)
	if f["kind"] != "stream.item" || f["direction"] != "server" || int(number(f["sequence"])) != sequence || payload["text"] != text {
		return fmt.Errorf("expected output %d %q, got %v", sequence, text, f)
	}
	return nil
}

func (c *client) expectEnd(id string) error {
	end, err := c.nextData(id, c.h.opts.Timeout)
	if err != nil {
		return err
	}
	if end["kind"] != "stream.end" || end["direction"] != "server" {
		return fmt.Errorf("expected the output's stream.end, got %v", end)
	}
	result, err := c.nextData(id, c.h.opts.Timeout)
	if err != nil {
		return err
	}
	if result["kind"] != "call.result" || result["payload"] != nil {
		return fmt.Errorf("expected call.result without a payload, got %v", result)
	}
	return nil
}

// openChat opens a duplex call and waits for the initial input credit.
func (c *client) openChat(method string, payload, extra frame) (string, error) {
	id, err := c.open(method, payload, extra)
	if err != nil {
		return "", err
	}
	window, err := c.receive()
	if err != nil {
		return "", err
	}
	if window["kind"] != "stream.window" || window["direction"] != "client" || number(window["credit"]) != 16 {
		return "", fmt.Errorf("expected initial input window of 16, got %v", window)
	}
	return id, nil
}

func (c *client) input(id string, sequence int, text string) error {
	return c.send(frame{"kind": "stream.item", "call_id": id, "direction": "client", "sequence": sequence, "payload": frame{"text": text}})
}

func duplexInterleaved(h *harness) error {
	c, err := h.client()
	if err != nil {
		return err
	}
	defer c.close()
	id, err := c.openChat("chat", frame{"prefix": ">"}, nil)
	if err != nil {
		return err
	}
	if err := c.send(frame{"kind": "stream.window", "call_id": id, "direction": "server", "credit": 1}); err != nil {
		return err
	}
	// Output answers input while the input is still open.
	if err := c.input(id, 0, "a"); err != nil {
		return err
	}
	if err := c.expectOutput(id, 0, ">a"); err != nil {
		return err
	}
	// Output credit is spent: the server holds its answer while input flows.
	if err := c.input(id, 1, "b"); err != nil {
		return err
	}
	if err := c.expectNoData(id, 300*time.Millisecond); err != nil {
		return fmt.Errorf("server sent beyond its output credit: %w", err)
	}
	if err := c.send(frame{"kind": "stream.window", "call_id": id, "direction": "server", "credit": 8}); err != nil {
		return err
	}
	if err := c.expectOutput(id, 1, ">b"); err != nil {
		return err
	}
	// Half-closing the input does not end the output.
	if err := c.send(frame{"kind": "stream.end", "call_id": id, "direction": "client"}); err != nil {
		return err
	}
	if err := c.expectOutput(id, 2, "done:2"); err != nil {
		return err
	}
	return c.expectEnd(id)
}

func duplexServerHalfClose(h *harness) error {
	c, err := h.client()
	if err != nil {
		return err
	}
	defer c.close()
	id, err := c.openChat("announce", frame{}, nil)
	if err != nil {
		return err
	}
	if err := c.send(frame{"kind": "stream.window", "call_id": id, "direction": "server", "credit": 4}); err != nil {
		return err
	}
	if err := c.expectOutput(id, 0, "hello"); err != nil {
		return err
	}
	end, err := c.nextData(id, c.h.opts.Timeout)
	if err != nil {
		return err
	}
	if end["kind"] != "stream.end" || end["direction"] != "server" {
		return fmt.Errorf("expected the output to end first, got %v", end)
	}
	// The input stays open after the output ended; the call ends with it.
	for i := 0; i < 3; i++ {
		if err := c.input(id, i, "still here"); err != nil {
			return err
		}
	}
	if err := c.expectNoData(id, 200*time.Millisecond); err != nil {
		return fmt.Errorf("call ended before its input did: %w", err)
	}
	if err := c.send(frame{"kind": "stream.end", "call_id": id, "direction": "client"}); err != nil {
		return err
	}
	result, err := c.nextData(id, c.h.opts.Timeout)
	if err != nil {
		return err
	}
	if result["kind"] != "call.result" || result["payload"] != nil {
		return fmt.Errorf("expected call.result without a payload, got %v", result)
	}
	return nil
}

func duplexIdle(h *harness) error {
	c, err := h.client()
	if err != nil {
		return err
	}
	defer c.close()
	id, err := c.openChat("chat", frame{}, frame{"idle_timeout_ms": 150})
	if err != nil {
		return err
	}
	f, err := c.nextData(id, c.h.opts.Timeout)
	if err != nil {
		return err
	}
	code, _, _, details := errorOf(f)
	if f["kind"] != "call.error" || code != "deadline_exceeded" || details["reason"] != "idle" || details["source"] != "server" {
		return fmt.Errorf("expected idle deadline_exceeded from the server, got %v", f)
	}
	return nil
}

func duplexIdempotencyKey(h *harness) error {
	c, err := h.client()
	if err != nil {
		return err
	}
	defer c.close()
	id, err := c.open("chat", frame{}, frame{"idempotency_key": "k"})
	if err != nil {
		return err
	}
	f, err := c.nextData(id, c.h.opts.Timeout)
	if err != nil {
		return err
	}
	code, _, _, details := errorOf(f)
	if f["kind"] != "call.error" || code != "invalid_argument" || details["reason"] != "idempotency_key_on_duplex" {
		return fmt.Errorf("expected idempotency_key_on_duplex, got %v", f)
	}
	return nil
}
