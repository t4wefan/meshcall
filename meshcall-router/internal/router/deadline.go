package router

import (
	"container/heap"
	"time"
)

// deadlineHeap orders routes by deadline so one timer serves every call. Routes
// without a timeout never enter it. Mutated only under Router.mu.
type deadlineHeap []*route

func (h deadlineHeap) Len() int           { return len(h) }
func (h deadlineHeap) Less(i, j int) bool { return h[i].deadline.Before(h[j].deadline) }
func (h deadlineHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}

func (h *deadlineHeap) Push(x any) {
	rt := x.(*route)
	rt.index = len(*h)
	*h = append(*h, rt)
}

func (h *deadlineHeap) Pop() any {
	old := *h
	n := len(old)
	rt := old[n-1]
	old[n-1] = nil
	rt.index = -1
	*h = old[:n-1]
	return rt
}

// track registers a deadline for a newly opened route. Called under r.mu.
func (r *Router) track(rt *route, timeout time.Duration) {
	rt.deadline = time.Now().Add(timeout + r.config.DeadlineGrace)
	heap.Push(&r.deadlines, rt)
	if rt.index == 0 {
		r.wake()
	}
}

// untrack removes a route from the heap when it ends early. Called under r.mu.
func (r *Router) untrack(rt *route) {
	if rt.index >= 0 {
		heap.Remove(&r.deadlines, rt.index)
	}
}

// endRoute forgets a route for any reason. Called under r.mu.
func (r *Router) endRoute(id string, rt *route) {
	delete(r.routes, id)
	rt.instance.inflight--
	r.untrack(rt)
}

func (r *Router) wake() {
	select {
	case r.deadlineWake <- struct{}{}:
	default:
	}
}

// expireLoop fires the router-side deadline for calls whose service instance has
// not answered in time. The server's own deadline normally wins because the
// router adds DeadlineGrace on top of the requested timeout.
func (r *Router) expireLoop() {
	defer r.wg.Done()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		r.mu.Lock()
		now := time.Now()
		for len(r.deadlines) > 0 && !r.deadlines[0].deadline.After(now) {
			rt := heap.Pop(&r.deadlines).(*route)
			id := rt.id
			if r.routes[id] != rt {
				continue
			}
			delete(r.routes, id)
			rt.instance.inflight--
			r.observe(rt.instance, false)
			if rt.dedup != nil && rt.dedup.route == rt {
				r.dedupFail(rt.dedup, deadlineError(id), nil)
			}
			rt.client.send(deadlineError(id))
			rt.instance.send(frame{Kind: "call.cancel", CallID: id, Reason: "deadline_exceeded"})
		}
		next := time.Hour
		if len(r.deadlines) > 0 {
			next = max(time.Until(r.deadlines[0].deadline), time.Millisecond)
		}
		r.mu.Unlock()
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(next)
		select {
		case <-r.ctx.Done():
			return
		case <-r.deadlineWake:
		case <-timer.C:
		}
	}
}
