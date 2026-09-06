package router

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"unicode/utf16"
)

type pool struct {
	signature service
	instances []*peer
	cursors   map[string]int
}

func (r *Router) selectInstance(f frame) (*peer, *frame) {
	fail := func(code, message string) (*peer, *frame) {
		err := callError(f.CallID, code, message, code == "unavailable")
		return nil, &err
	}
	p := r.services[f.Service]
	if p == nil || len(p.instances) == 0 {
		return fail("unavailable", "No instances registered for "+f.Service)
	}
	var selected *method
	for i := range p.signature.Methods {
		if p.signature.Methods[i].Name == f.Method {
			selected = &p.signature.Methods[i]
			break
		}
	}
	if selected == nil {
		return fail("method_not_found", "Unknown method "+f.Service+"."+f.Method)
	}
	switch selected.Balance.Kind {
	case "round_robin":
		cursor := p.cursors[f.Method] % len(p.instances)
		p.cursors[f.Method] = (cursor + 1) % len(p.instances)
		return p.instances[cursor], nil
	case "least_inflight":
		best := p.instances[0]
		for _, candidate := range p.instances[1:] {
			if candidate.inflight < best.inflight {
				best = candidate
			}
		}
		return best, nil
	case "random":
		return p.instances[rand.IntN(len(p.instances))], nil
	case "sticky":
		var value any
		d := json.NewDecoder(bytes.NewReader(f.Payload))
		d.UseNumber()
		if err := d.Decode(&value); err != nil {
			return fail("invalid_argument", "Invalid sticky request")
		}
		for _, part := range strings.Split(*selected.Balance.Key, ".")[1:] {
			object, ok := value.(map[string]any)
			if !ok {
				return fail("invalid_argument", "Sticky key is missing")
			}
			value, ok = object[part]
			if !ok {
				return fail("invalid_argument", "Sticky key is missing")
			}
		}
		var canonical strings.Builder
		if err := canonicalJSON(&canonical, value); err != nil {
			return fail("invalid_argument", "Invalid sticky key")
		}
		digest := sha256.Sum256([]byte(canonical.String()))
		return p.instances[binary.BigEndian.Uint64(digest[:8])%uint64(len(p.instances))], nil
	default: // disabled
		if len(p.instances) != 1 {
			return fail("unavailable", "Balance is disabled for "+f.Service+"."+f.Method)
		}
		return p.instances[0], nil
	}
}

// Preserve the original Python router's sorted compact JSON / ASCII escaping.
// Integers retain full precision; floats follow Python's fixed/scientific thresholds.
func canonicalJSON(out *strings.Builder, value any) error {
	switch v := value.(type) {
	case nil:
		out.WriteString("null")
	case bool:
		out.WriteString(strconv.FormatBool(v))
	case string:
		out.WriteByte('"')
		for _, c := range v {
			switch c {
			case '"', '\\':
				out.WriteByte('\\')
				out.WriteRune(c)
			case '\b':
				out.WriteString("\\b")
			case '\f':
				out.WriteString("\\f")
			case '\n':
				out.WriteString("\\n")
			case '\r':
				out.WriteString("\\r")
			case '\t':
				out.WriteString("\\t")
			default:
				if c < 32 || c > 126 {
					if c > 0xffff {
						a, b := utf16.EncodeRune(c)
						fmt.Fprintf(out, "\\u%04x\\u%04x", a, b)
					} else {
						fmt.Fprintf(out, "\\u%04x", c)
					}
				} else {
					out.WriteRune(c)
				}
			}
		}
		out.WriteByte('"')
	case json.Number:
		if !strings.ContainsAny(string(v), ".eE") {
			if v == "-0" {
				out.WriteByte('0')
			} else {
				out.WriteString(string(v))
			}
			return nil
		}
		number, err := strconv.ParseFloat(string(v), 64)
		if err != nil || math.IsInf(number, 0) {
			return fmt.Errorf("invalid number")
		}
		format := byte('f')
		if absolute := math.Abs(number); absolute != 0 && (absolute < 1e-4 || absolute >= 1e16) {
			format = 'e'
		}
		text := strconv.FormatFloat(number, format, -1, 64)
		if format == 'f' && !strings.Contains(text, ".") {
			text += ".0"
		}
		out.WriteString(text)
	case []any:
		out.WriteByte('[')
		for i, item := range v {
			if i > 0 {
				out.WriteByte(',')
			}
			if err := canonicalJSON(out, item); err != nil {
				return err
			}
		}
		out.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		out.WriteByte('{')
		for i, key := range keys {
			if i > 0 {
				out.WriteByte(',')
			}
			if err := canonicalJSON(out, key); err != nil {
				return err
			}
			out.WriteByte(':')
			if err := canonicalJSON(out, v[key]); err != nil {
				return err
			}
		}
		out.WriteByte('}')
	}
	return nil
}
