package router

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Prometheus metrics on a separate, opt-in listener (--metrics-listen): the
// labels name services and methods, so they are not served on the public
// port. Counters live in memory and reset with the process, like everything
// else in the Router. The exposition format is written by hand to keep the
// Router free of dependencies.

var durationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}

type callKey struct{ service, method, code string }

type methodKey struct{ service, method string }

type histogram struct {
	counts []uint64 // per bucket, cumulative only when written
	sum    float64
	total  uint64
}

type metrics struct {
	mu       sync.Mutex
	calls    map[callKey]uint64
	duration map[methodKey]*histogram
	// outboxDrops counts peers disconnected because their outbox overflowed.
	outboxDrops atomic.Uint64
}

func newMetrics() *metrics {
	return &metrics{calls: map[callKey]uint64{}, duration: map[methodKey]*histogram{}}
}

// call records one finished call. service and method are empty for calls
// to something not registered, so callers cannot inflate the label set.
func (m *metrics) call(service, method, code string, started time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls[callKey{service, method, code}]++
	if started.IsZero() {
		return
	}
	h := m.duration[methodKey{service, method}]
	if h == nil {
		h = &histogram{counts: make([]uint64, len(durationBuckets))}
		m.duration[methodKey{service, method}] = h
	}
	seconds := time.Since(started).Seconds()
	for i, bound := range durationBuckets {
		if seconds <= bound {
			h.counts[i]++
			break
		}
	}
	h.sum += seconds
	h.total++
}

// refuse records a call.open answered by the Router itself. Called under r.mu.
func (r *Router) refuse(f frame, code string) {
	service, method := "", ""
	if r.methodStream(f.Service, f.Method) != "" {
		service, method = f.Service, f.Method
	}
	r.metrics.call(service, method, code, time.Time{})
}

// finishRoute records a routed call that ended with code. Called under r.mu.
func (r *Router) finishRoute(rt *route, code string) {
	r.metrics.call(rt.service, rt.method, code, rt.started)
}

func (r *Router) startMetrics(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("metrics listen: %w", err)
	}
	r.metricsServer = &http.Server{
		Handler:           http.HandlerFunc(r.serveMetrics),
		ReadHeaderTimeout: 5 * time.Second,
	}
	r.ready.Metrics = listener.Addr().String()
	go func() { _ = r.metricsServer.Serve(listener) }()
	return nil
}

func (r *Router) serveMetrics(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path != "/metrics" {
		http.NotFound(w, req)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	r.writeMetrics(w)
}

func (r *Router) writeMetrics(w io.Writer) {
	var b strings.Builder
	head := func(name, kind, help string) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, kind)
	}

	r.mu.Lock()
	roles := map[string]int{"client": 0, "server": 0}
	for p := range r.peers {
		roles[p.role]++
	}
	routes := len(r.routes)
	type instance struct {
		service, id, breaker string
		inflight             int
	}
	var instances []instance
	now := time.Now()
	for name, pool := range r.services {
		for _, p := range pool.instances {
			instances = append(instances, instance{name, p.instanceID, string(p.health.state(now)), p.inflight})
		}
	}
	r.mu.Unlock()

	head("meshcall_router_connections", "gauge", "Open connections by role (HTTP calls count as clients while they run).")
	for _, role := range []string{"client", "server"} {
		fmt.Fprintf(&b, "meshcall_router_connections{role=%q} %d\n", role, roles[role])
	}
	head("meshcall_router_anonymous_connections", "gauge", "Open WebSocket connections without credentials.")
	fmt.Fprintf(&b, "meshcall_router_anonymous_connections %d\n", r.anonymous.Load())
	head("meshcall_router_http_inflight", "gauge", "HTTP calls in progress.")
	fmt.Fprintf(&b, "meshcall_router_http_inflight %d\n", r.httpInflight.Load())
	head("meshcall_router_routes", "gauge", "Calls currently routed to an instance.")
	fmt.Fprintf(&b, "meshcall_router_routes %d\n", routes)
	head("meshcall_router_outbox_drops_total", "counter", "Peers disconnected because they read too slowly.")
	fmt.Fprintf(&b, "meshcall_router_outbox_drops_total %d\n", r.metrics.outboxDrops.Load())

	slices.SortFunc(instances, func(a, b instance) int {
		return strings.Compare(a.service+"\x00"+a.id, b.service+"\x00"+b.id)
	})
	head("meshcall_router_instance_inflight", "gauge", "Calls in flight per registered instance.")
	for _, in := range instances {
		fmt.Fprintf(&b, "meshcall_router_instance_inflight{service=%q,instance=%q} %d\n", in.service, in.id, in.inflight)
	}
	head("meshcall_router_instance_breaker_open", "gauge", "1 when the instance's circuit breaker is open or half-open.")
	for _, in := range instances {
		open := 0
		if in.breaker != string(breakerClosed) {
			open = 1
		}
		fmt.Fprintf(&b, "meshcall_router_instance_breaker_open{service=%q,instance=%q} %d\n", in.service, in.id, open)
	}

	r.metrics.mu.Lock()
	calls := make([]callKey, 0, len(r.metrics.calls))
	for k := range r.metrics.calls {
		calls = append(calls, k)
	}
	slices.SortFunc(calls, func(a, b callKey) int {
		return strings.Compare(a.service+"\x00"+a.method+"\x00"+a.code, b.service+"\x00"+b.method+"\x00"+b.code)
	})
	head("meshcall_router_calls_total", "counter", "Calls by service, method and result code (ok or the error code); empty service and method for calls to nothing registered.")
	for _, k := range calls {
		fmt.Fprintf(&b, "meshcall_router_calls_total{service=%q,method=%q,code=%q} %d\n", k.service, k.method, k.code, r.metrics.calls[k])
	}
	methods := make([]methodKey, 0, len(r.metrics.duration))
	for k := range r.metrics.duration {
		methods = append(methods, k)
	}
	slices.SortFunc(methods, func(a, b methodKey) int {
		return strings.Compare(a.service+"\x00"+a.method, b.service+"\x00"+b.method)
	})
	head("meshcall_router_call_duration_seconds", "histogram", "Duration of routed calls, from call.open to the terminal frame.")
	for _, k := range methods {
		h := r.metrics.duration[k]
		labels := fmt.Sprintf("service=%q,method=%q", k.service, k.method)
		var cumulative uint64
		for i, bound := range durationBuckets {
			cumulative += h.counts[i]
			fmt.Fprintf(&b, "meshcall_router_call_duration_seconds_bucket{%s,le=%q} %d\n", labels, strconv.FormatFloat(bound, 'g', -1, 64), cumulative)
		}
		fmt.Fprintf(&b, "meshcall_router_call_duration_seconds_bucket{%s,le=\"+Inf\"} %d\n", labels, h.total)
		fmt.Fprintf(&b, "meshcall_router_call_duration_seconds_sum{%s} %g\n", labels, h.sum)
		fmt.Fprintf(&b, "meshcall_router_call_duration_seconds_count{%s} %d\n", labels, h.total)
	}
	r.metrics.mu.Unlock()
	_, _ = io.WriteString(w, b.String())
}
