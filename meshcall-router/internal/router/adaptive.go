package router

import (
	"math/rand/v2"
	"time"
)

// The `adaptive` balance policy: a black box whose only objective is the
// shortest expected completion time. For a candidate instance
//
//	expected = service(instance, method) × (inflight + 1) + rtt(instance)
//
// where service is an EWMA of observed call durations per (instance, method)
// and rtt is the measured round trip to the instance (zero for instances on
// this Router; a peer Router's link RTT once peering lands). Two candidates
// are drawn at random and the better one wins (power of two choices): idle
// instances share load instead of herding on one, and a cold instance keeps
// receiving probe traffic because it starts from the pool's average, not from
// zero. No constant here is configuration; the model is the contract.

// methodStats is what the Router has learned about one method on one instance.
type methodStats struct {
	ewma    time.Duration
	samples int
}

// observeDuration records a completed call. Called under Router.mu.
func (p *peer) observeDuration(method string, d time.Duration) {
	if d <= 0 {
		d = time.Microsecond
	}
	if p.stats == nil {
		p.stats = make(map[string]*methodStats)
	}
	s := p.stats[method]
	if s == nil {
		s = &methodStats{}
		p.stats[method] = s
	}
	s.samples++
	if s.samples == 1 {
		s.ewma = d
		return
	}
	// α = 1/4: a few calls move the estimate, one outlier does not own it.
	s.ewma = (s.ewma*3 + d) / 4
}

// serviceTime is the instance's estimate for method, or the pool-wide average
// across instances that have one (cold start), or zero when nobody knows yet.
func serviceTime(p *peer, method string, candidates []*peer) time.Duration {
	if s := p.stats[method]; s != nil {
		return s.ewma
	}
	var sum time.Duration
	n := 0
	for _, c := range candidates {
		if s := c.stats[method]; s != nil {
			sum += s.ewma
			n++
		}
	}
	if n == 0 {
		return 0
	}
	return sum / time.Duration(n)
}

// expectedCompletion scores one candidate; lower is better.
func expectedCompletion(p *peer, method string, candidates []*peer) time.Duration {
	return serviceTime(p, method, candidates)*time.Duration(p.inflight+1) + p.rtt
}

// pickAdaptive chooses among eligible candidates with power-of-two choices.
// pick draws indices; tests inject a deterministic source.
func pickAdaptive(candidates []*peer, method string, pick func(n int) int) *peer {
	switch len(candidates) {
	case 0:
		return nil
	case 1:
		return candidates[0]
	}
	i := pick(len(candidates))
	j := pick(len(candidates) - 1)
	if j >= i {
		j++
	}
	a, b := candidates[i], candidates[j]
	ca, cb := expectedCompletion(a, method, candidates), expectedCompletion(b, method, candidates)
	switch {
	case ca < cb:
		return a
	case cb < ca:
		return b
	default:
		return a // both drawn uniformly; the tie is already random
	}
}

func defaultPick(n int) int { return rand.IntN(n) }
