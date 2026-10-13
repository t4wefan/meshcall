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
	"time"
	"unicode/utf16"
)

// pool is every live instance of one service. Methods are the union of what
// those instances registered, so instances may serve supersets of each other
// (a rolling upgrade adds a method on new instances first); a call is
// balanced over the instances that registered its method.
type pool struct {
	targeted bool
	methods  map[string]method
	contract json.RawMessage
	// contractMethods is how many methods the registration that supplied
	// contract had; a later, larger registration replaces it.
	contractMethods int
	instances       []*peer
	cursors         map[string]int
	leastCursor     int
}

// serves reports whether p registered method on service.
func (p *peer) serves(service, name string) bool {
	for _, s := range p.services {
		if s.Name == service {
			return slices.ContainsFunc(s.Methods, func(m method) bool { return m.Name == name })
		}
	}
	return false
}

// rebuildMethods recomputes the union after an instance left.
func (p *pool) rebuildMethods() {
	p.methods = make(map[string]method)
	for _, inst := range p.instances {
		for _, s := range inst.services {
			for _, m := range s.Methods {
				p.methods[m.Name] = m
			}
		}
	}
}

// instanceCost estimates the marginal cost of assigning one more call to p.
// Quantizing latency and oldest in-flight age into 5 ms buckets keeps sub-ms
// localhost jitter from perturbing deterministic tie-breaking while still
// penalizing remote hops, high-RTT peers, slow instances, and stalling calls.
func instanceCost(p *peer, now time.Time) time.Duration {
	const quantum = 5 * time.Millisecond
	var gatewayRTT time.Duration
	if p.gateway != nil {
		gatewayRTT = p.gateway.rtt
	}
	var ewma time.Duration
	if p.inflight > 0 || p.ewma >= 100*time.Millisecond {
		ewma = p.ewma
	}
	lat := ((ewma + gatewayRTT) / quantum) * quantum
	if age := (p.oldestInflightAge(now) / quantum) * quantum; age > lat {
		lat = age
	}
	unit := quantum + time.Duration(p.hops)*5*quantum + lat
	return unit * time.Duration(p.inflight+1)
}

// selectInstance picks an instance for f. A non-nil avoid is skipped when any
// other instance exists (redelivery after that instance lost the call).
func (r *Router) selectInstance(caller *peer, f frame, avoid *peer) (*peer, *frame) {
	fail := func(code, message string) (*peer, *frame) {
		reason := ""
		if code == "unavailable" {
			reason = "no_instances"
		}
		err := routerError(f.CallID, code, message, code == "unavailable", reason, "")
		return nil, &err
	}
	p := r.services[f.Service]
	if p == nil || (len(p.instances) == 0 && f.InstanceID == "") {
		return fail("unavailable", "No instances registered for "+f.Service)
	}
	selected, known := p.methods[f.Method]
	if !known {
		return fail("method_not_found", "Unknown method "+f.Service+"."+f.Method)
	}
	if f.InstanceID != "" {
		return r.targetInstance(caller, f)
	}
	if p.targeted {
		err := routerError(f.CallID, "invalid_argument", f.Service+" is a targeted service: call.open needs instance_id", false, "instance_required", "")
		return nil, &err
	}
	// Eligibility: registered this method, not bounced back to caller peer,
	// not avoided, not at its cap, breaker admits it.
	now := time.Now()
	serving := make([]*peer, 0, len(p.instances))
	for _, c := range p.instances {
		if c.serves(f.Service, f.Method) && (caller == nil || (c != caller && c.gateway != caller)) {
			serving = append(serving, c)
		}
	}
	eligible := make([]*peer, 0, len(serving))
	full, tripped := 0, 0
	for _, c := range serving {
		switch {
		case c == avoid && len(serving) > 1:
		case c.maxInflight > 0 && c.inflight >= c.maxInflight:
			full++
		case !c.health.admit(now):
			tripped++
		default:
			eligible = append(eligible, c)
		}
	}
	if len(eligible) == 0 {
		if full > 0 {
			err := routerError(f.CallID, "resource_exhausted", "Every instance of "+f.Service+" is at its max_inflight", true, "overloaded", "")
			return nil, &err
		}
		if tripped > 0 {
			err := routerError(f.CallID, "unavailable", "Every instance of "+f.Service+" has an open circuit", true, "circuit_open", "")
			return nil, &err
		}
		return fail("unavailable", "No instances registered for "+f.Service)
	}
	// Locality tiering: for non-cost-based policies, prefer the closest hop
	// tier (local instances hops==0 first) and spill over to remote instances
	// only when no closer instance is eligible.
	candidates := eligible
	if selected.Balance.Kind != "least_inflight" {
		minHops := candidates[0].hops
		for _, c := range candidates[1:] {
			if c.hops < minHops {
				minHops = c.hops
			}
		}
		near := make([]*peer, 0, len(candidates))
		for _, c := range candidates {
			if c.hops == minHops {
				near = append(near, c)
			}
		}
		candidates = near
	}
	switch selected.Balance.Kind {
	case "round_robin":
		cursor := p.cursors[f.Method] % len(candidates)
		p.cursors[f.Method] = (cursor + 1) % len(candidates)
		return candidates[cursor], nil
	case "least_inflight":
		n := len(candidates)
		start := p.leastCursor % n
		p.leastCursor = (start + 1) % n
		best := candidates[start]
		bestCost := instanceCost(best, now)
		for i := 1; i < n; i++ {
			candidate := candidates[(start+i)%n]
			if cost := instanceCost(candidate, now); cost < bestCost {
				best = candidate
				bestCost = cost
			}
		}
		return best, nil
	case "random":
		return candidates[rand.IntN(len(candidates))], nil
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
		return candidates[binary.BigEndian.Uint64(digest[:8])%uint64(len(candidates))], nil
	default: // disabled
		if len(candidates) != 1 {
			return fail("unavailable", "Balance is disabled for "+f.Service+"."+f.Method)
		}
		return candidates[0], nil
	}
}

// targetInstance routes a call that names its instance. The caller chose the
// instance, so the balance policy and the circuit breaker do not apply; only
// existence, draining and the hard capacity cap do.
func (r *Router) targetInstance(caller *peer, f frame) (*peer, *frame) {
	target := r.instances[f.InstanceID]
	if target == nil || (caller != nil && (target == caller || target.gateway == caller)) || !slices.ContainsFunc(target.services, func(s service) bool { return s.Name == f.Service }) {
		err := routerError(f.CallID, "unavailable", "Instance "+f.InstanceID+" is not registered for "+f.Service, false, "instance_unknown", f.InstanceID)
		return nil, &err
	}
	if !target.serves(f.Service, f.Method) {
		err := routerError(f.CallID, "method_not_found", "Instance "+f.InstanceID+" does not serve "+f.Service+"."+f.Method, false, "", f.InstanceID)
		return nil, &err
	}
	if target.draining {
		err := routerError(f.CallID, "unavailable", "Instance "+f.InstanceID+" is draining", true, "instance_draining", f.InstanceID)
		return nil, &err
	}
	if target.maxInflight > 0 && target.inflight >= target.maxInflight {
		err := routerError(f.CallID, "resource_exhausted", "Instance "+f.InstanceID+" is at its max_inflight", true, "overloaded", f.InstanceID)
		return nil, &err
	}
	return target, nil
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
