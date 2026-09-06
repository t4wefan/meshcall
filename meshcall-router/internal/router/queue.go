package router

import "sync"

// A bounded, per-call FIFO with round-robin scheduling. Control frames at a
// call's head have priority, but never overtake that call's queued call.open.
type outbox struct {
	mu     sync.Mutex
	calls  map[string][]frame
	order  []string
	bytes  int
	count  int
	limit  int
	closed bool
	ready  chan struct{}
}

func newOutbox(limit int) *outbox {
	return &outbox{calls: make(map[string][]frame), limit: limit, ready: make(chan struct{}, 1)}
}

func (q *outbox) push(f frame) bool {
	data := f.bytes()
	f.raw = data
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.count >= 256 || q.bytes+len(data) > q.limit {
		return false
	}
	if len(q.calls[f.CallID]) == 0 {
		q.order = append(q.order, f.CallID)
	}
	q.calls[f.CallID] = append(q.calls[f.CallID], f)
	q.bytes += len(data)
	q.count++
	select {
	case q.ready <- struct{}{}:
	default:
	}
	return true
}

func (q *outbox) pop() (frame, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.order) == 0 {
		return frame{}, false
	}
	index := 0
	for i, id := range q.order {
		if q.calls[id][0].priority() {
			index = i
			break
		}
	}
	id := q.order[index]
	f := q.calls[id][0]
	q.calls[id][0] = frame{}
	q.calls[id] = q.calls[id][1:]
	q.order = append(q.order[:index], q.order[index+1:]...)
	if len(q.calls[id]) == 0 {
		delete(q.calls, id)
	} else {
		q.order = append(q.order, id)
	}
	q.bytes -= len(f.raw)
	q.count--
	return f, true
}

func (q *outbox) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.calls = make(map[string][]frame)
	q.order = nil
	q.bytes, q.count = 0, 0
}
