package router

import "time"

// Per-instance health, kept in memory and rebuilt on reconnect.
//
// An instance "fails" when the Router's own deadline fires on one of its
// calls or when it answers with `internal` or `unavailable`; application
// errors are not failures. After BreakerThreshold consecutive failures the
// instance is excluded from selection for BreakerCooldown, then one probe
// call is admitted: success closes the breaker, failure reopens it.

const (
	DefaultBreakerThreshold = 5
	DefaultBreakerCooldown  = 10 * time.Second
)

type breakerState string

const (
	breakerClosed   breakerState = "closed"
	breakerOpen     breakerState = "open"
	breakerHalfOpen breakerState = "half_open"
)

type health struct {
	failures  int       // consecutive
	openUntil time.Time // zero when closed
	probing   bool      // a half-open probe is in flight
}

func (h *health) state(now time.Time) breakerState {
	switch {
	case h.openUntil.IsZero():
		return breakerClosed
	case now.Before(h.openUntil):
		return breakerOpen
	default:
		return breakerHalfOpen
	}
}

// admit reports whether the instance may receive a call now; for a half-open
// breaker only one probe is admitted at a time.
func (h *health) admit(now time.Time) bool {
	switch h.state(now) {
	case breakerClosed:
		return true
	case breakerHalfOpen:
		if h.probing {
			return false
		}
		h.probing = true
		return true
	default:
		return false
	}
}

// observe records the outcome of a call routed to the instance. Called under
// Router.mu.
func (r *Router) observe(p *peer, ok bool) {
	h := &p.health
	if ok {
		h.failures, h.openUntil, h.probing = 0, time.Time{}, false
		return
	}
	h.failures++
	h.probing = false
	if h.failures >= r.config.BreakerThreshold {
		h.openUntil = time.Now().Add(r.config.BreakerCooldown)
	}
}

func instanceFailed(f frame) bool {
	return f.Kind == "call.error" && f.Error != nil && (f.Error.Code == "internal" || f.Error.Code == "unavailable")
}
