package router

import (
	"container/list"
	"time"
)

// Idempotency deduplication for unary calls. The table is a cache: losing it
// costs at most one extra execution against a handler that honours the key.
//
// Keyed by (service, method, idempotency_key). While the first call is in
// flight, later calls with the same key join it and receive the same terminal
// frame; after completion the terminal frame is replayed for dedupTTL.

const (
	dedupTTL      = 60 * time.Second
	dedupCapacity = 4096
)

type dedupWaiter struct {
	client *peer
	callID string
}

type dedupEntry struct {
	key          string
	route        *route // in flight, or nil once settled
	waiters      []dedupWaiter
	result       *frame // settled terminal frame (call_id to be rewritten)
	expires      time.Time
	lastInstance *peer // instance that lost this call; avoided on redelivery
	element      *list.Element
}

func dedupKey(f frame) string {
	// A targeted duplicate is only a duplicate on the same instance.
	return f.Service + "\x00" + f.Method + "\x00" + f.Idempotency + "\x00" + f.InstanceID
}

// dedupLookup returns the entry for a keyed call.open, evicting expired ones.
// Called under r.mu.
func (r *Router) dedupLookup(key string) *dedupEntry {
	entry := r.dedup[key]
	if entry != nil && entry.route == nil && time.Now().After(entry.expires) {
		r.dedupRemove(entry)
		return nil
	}
	return entry
}

func (r *Router) dedupInsert(key string, rt *route) *dedupEntry {
	for r.dedupOrder.Len() >= dedupCapacity {
		oldest := r.dedupOrder.Front().Value.(*dedupEntry)
		if oldest.route != nil {
			break // never evict an in-flight join
		}
		r.dedupRemove(oldest)
	}
	entry := &dedupEntry{key: key, route: rt}
	entry.element = r.dedupOrder.PushBack(entry)
	r.dedup[key] = entry
	rt.dedup = entry
	return entry
}

func (r *Router) dedupRemove(entry *dedupEntry) {
	if entry.element != nil {
		r.dedupOrder.Remove(entry.element)
		entry.element = nil
	}
	delete(r.dedup, entry.key)
}

// dedupSettle records the terminal frame for an entry and fans it out to
// every joined waiter. Called under r.mu.
func (r *Router) dedupSettle(entry *dedupEntry, terminal frame) {
	settled := terminal
	settled.raw = nil
	entry.route = nil
	entry.result = &settled
	entry.expires = time.Now().Add(dedupTTL)
	for _, w := range entry.waiters {
		w.client.send(withCallID(settled, w.callID))
		r.callEnded(w.client)
	}
	entry.waiters = nil
}

// dedupFail is the in-flight call failing before a terminal frame from the
// instance (instance lost, router deadline). Waiters get the same error and
// the entry is dropped so a retry executes again, preferring another instance.
func (r *Router) dedupFail(entry *dedupEntry, err frame, lost *peer) {
	for _, w := range entry.waiters {
		w.client.send(withCallID(err, w.callID))
		r.callEnded(w.client)
	}
	entry.waiters = nil
	entry.route = nil
	entry.lastInstance = lost
	// Keep a short-lived tombstone only to remember which instance to avoid.
	entry.expires = time.Now().Add(dedupTTL)
	entry.result = nil
}

func withCallID(f frame, id string) frame {
	f.CallID = id
	f.raw = nil
	return f
}
