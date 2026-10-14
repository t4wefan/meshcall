package router

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"
)

// Router peering: one connection between two Routers is one ordinary
// instance on each side. The dialling Router authenticates with an account
// (or token) on the accepting Router, so that Router's `register` and `call`
// lists bound what the link may advertise and forward; the dialler trusts the
// Router it chose to dial. Over the link each side sends `server.register`
// describing the services reachable through it (this version: its local
// instances) and re-sends it whenever that set changes. Calls, streams and
// cancellations cross the link with the same per-call forwarding as any
// client↔instance pair, so deadlines, breakers, drain and `instance_lost`
// need no special cases. Nothing here survives a disconnect: the other side
// forgets the link's instance and the peer re-registers on reconnect.

// peerRegistrationLocked summarises what this Router can serve: the union of
// its local instances' methods per service, the widest contract it has seen,
// and the sum of their caps (unlimited if any is). Called under r.mu.
func (r *Router) peerRegistrationLocked() frame {
	byName := map[string]*service{}
	var names []string
	capSum, unlimited := 0, false
	for _, inst := range r.instances {
		if inst.role == "router" || inst.draining {
			continue
		}
		if inst.maxInflight == 0 {
			unlimited = true
		}
		capSum += inst.maxInflight
		for _, s := range inst.services {
			merged := byName[s.Name]
			if merged == nil {
				merged = &service{Name: s.Name, Targeted: s.Targeted}
				byName[s.Name] = merged
				names = append(names, s.Name)
			}
			for _, m := range s.Methods {
				if !slices.ContainsFunc(merged.Methods, func(x method) bool { return x.Name == m.Name }) {
					merged.Methods = append(merged.Methods, m)
				}
			}
			if pool := r.services[s.Name]; pool != nil && pool.contract != nil {
				merged.Contract = pool.contract
			}
		}
	}
	slices.Sort(names)
	services := make([]service, 0, len(names))
	for _, name := range names {
		s := byName[name]
		slices.SortFunc(s.Methods, func(a, b method) int { return strings.Compare(a.Name, b.Name) })
		services = append(services, *s)
	}
	f := frame{Kind: "server.register", InstanceID: r.peerID, Services: services}
	if !unlimited {
		f.MaxInflight = capSum
	}
	return f
}

// syncPeersLocked re-advertises this Router to every peer link whose view is
// stale. Called under r.mu after anything that changes the local instance set.
func (r *Router) syncPeersLocked() {
	if len(r.routerPeers) == 0 {
		return
	}
	raw := r.peerRegistrationLocked().bytes()
	for link := range r.routerPeers {
		if !bytes.Equal(link.lastSentRegistration, raw) {
			link.lastSentRegistration = raw
			link.send(frame{Kind: "server.register", raw: raw})
		}
	}
}

// registerLink applies a peer Router's (re-)registration: the link becomes,
// or updates, the instance named by the remote peer_id.
func (r *Router) registerLink(p *peer, f frame) error {
	if f.InstanceID != p.peerID {
		return errors.New("peer registration must use the peer's own peer_id")
	}
	// Services the link's account may not register are dropped, not fatal:
	// an unrelated service appearing behind the peer must not sever the link.
	allowed := make([]service, 0, len(f.Services))
	var dropped []string
	for _, s := range f.Services {
		if p.user.allowsRegistration(s.Name) {
			allowed = append(allowed, s)
		} else {
			dropped = append(dropped, s.Name)
		}
	}
	if len(dropped) > 0 && !slices.Equal(dropped, p.lastDropped) {
		slog.Warn("peer advertises services its account may not register", "peer", p.peerID, "services", dropped)
	}
	p.lastDropped = dropped
	f.Services = allowed
	if err := validateServices(f.Services, p.user); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing := r.instances[f.InstanceID]; existing != nil && existing != p {
		return errors.New("peer_id collides with a registered instance")
	}
	if r.instances[f.InstanceID] == p {
		r.leavePools(p)
	}
	if err := r.compatibleWithPoolsLocked(f.Services); err != nil {
		// Keep the link up but out of the pools; the peer may fix its fleet.
		slog.Warn("peer registration incompatible", "peer", p.peerID, "reason", err)
		p.services = nil
		return nil
	}
	p.instanceID, p.services, p.maxInflight = f.InstanceID, f.Services, f.MaxInflight
	r.instances[p.instanceID] = p
	if !p.draining {
		r.joinPoolsLocked(p)
	}
	p.send(frame{Kind: "server.register.ack", InstanceID: p.instanceID})
	return nil
}

// dialPeerLoop keeps one outbound link to peerURL alive with backoff.
func (r *Router) dialPeerLoop(peerURL string) {
	defer r.wg.Done()
	delay := 200 * time.Millisecond
	for {
		if r.ctx.Err() != nil {
			return
		}
		started := time.Now()
		slog.Debug("dialing peer", "url", redactURL(peerURL))
		if err := r.dialPeer(peerURL); err != nil {
			slog.Warn("peer link failed", "peer", redactURL(peerURL), "reason", err)
		}
		if time.Since(started) > time.Minute {
			delay = 200 * time.Millisecond // the link was healthy for a while
		}
		select {
		case <-r.ctx.Done():
			return
		case <-time.After(delay):
		}
		delay = min(delay*2, 30*time.Second)
	}
}

// dialPeer runs one outbound link to completion.
func (r *Router) dialPeer(peerURL string) error {
	u, err := url.Parse(peerURL)
	if err != nil {
		return err
	}
	var opts *websocket.DialOptions
	if u.User != nil {
		header := make(http.Header)
		password, _ := u.User.Password()
		if u.User.Username() == "" {
			header.Set("Authorization", "Bearer "+password)
		} else {
			header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(u.User.Username()+":"+password)))
		}
		opts = &websocket.DialOptions{HTTPHeader: header}
		clean := *u
		clean.User = nil
		peerURL = clean.String()
	}
	dialCtx, cancelDial := context.WithTimeout(r.ctx, 10*time.Second)
	conn, _, err := websocket.Dial(dialCtx, peerURL, opts)
	cancelDial()
	if err != nil {
		return err
	}
	conn.SetReadLimit(int64(r.config.MaxFrameSize))
	ctx, cancel := context.WithCancel(r.ctx)
	p := &peer{conn: conn, ctx: ctx, cancel: cancel, role: "router", out: newOutbox(max(8*1024*1024, 2*r.config.MaxFrameSize))}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		cancel()
		_ = conn.CloseNow()
		return errors.New("router closed")
	}
	r.peers[p] = struct{}{}
	r.mu.Unlock()
	writerDone := make(chan struct{})
	go func() { defer close(writerDone); p.writeLoop(r.config.MaxFrameSize) }()
	defer func() {
		cancel()
		p.out.close()
		_ = conn.CloseNow()
		<-writerDone
		r.remove(p)
	}()
	handshake, stop := context.WithTimeout(ctx, 5*time.Second)
	p.send(frame{Kind: "hello", Protocol: Protocol, Role: "router", PeerID: r.peerID})
	ack, err := readFrame(handshake, conn)
	stop()
	if err != nil {
		return fmt.Errorf("handshake: %w", err)
	}
	if ack.Kind != "hello.ack" || ack.PeerID == "" || ack.PeerID == r.peerID {
		return errors.New("peer did not complete a router handshake")
	}
	p.id, p.peerID = ack.Connection, ack.PeerID
	slog.Info("peer link established", "peer", p.peerID, "url", redactURL(peerURL))
	r.mu.Lock()
	r.routerPeers[p] = struct{}{}
	r.syncPeersLocked()
	r.mu.Unlock()
	r.readLoop(p)
	slog.Info("peer link closed", "peer", p.peerID)
	return nil
}

// pingLink measures the link's round trip from both ends; the nonce carries
// the send time so no per-ping state is kept.
func (r *Router) pingLink(p *peer) {
	interval := min(r.config.IdleTimeout/3, 15*time.Second)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		p.send(frame{Kind: "ping", Nonce: "t:" + strconv.FormatInt(time.Now().UnixNano(), 10)})
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *Router) observeLinkRTT(p *peer, nonce string) {
	if !strings.HasPrefix(nonce, "t:") {
		return
	}
	sent, err := strconv.ParseInt(nonce[2:], 10, 64)
	if err != nil {
		return
	}
	rtt := time.Since(time.Unix(0, sent))
	if rtt <= 0 {
		return
	}
	r.mu.Lock()
	if p.rtt == 0 {
		p.rtt = rtt
	} else {
		p.rtt = (p.rtt*3 + rtt) / 4
	}
	r.mu.Unlock()
}

// Path-qualified instance ids: `peer/instance` addresses an instance behind a
// peer Router. Each Router strips its own prefix before routing locally.

// resolveInstanceID returns the id to look up here and the id the next hop
// should see (unchanged for a remote path, stripped when this Router is the
// named prefix).
func (r *Router) resolveInstanceID(id string) (lookup, forward string) {
	prefix, rest, ok := strings.Cut(id, "/")
	if !ok {
		return id, id
	}
	if prefix == r.peerID {
		return rest, rest
	}
	return prefix, id
}

// withInstanceID rewrites only the instance_id field of a raw call.open,
// leaving every other field (including ones this Router does not know) as is.
func withInstanceID(f frame, id string) frame {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(f.bytes(), &fields); err != nil {
		return f
	}
	value, _ := json.Marshal(id)
	fields["instance_id"] = value
	raw, err := json.Marshal(fields)
	if err != nil {
		return f
	}
	f.raw, f.InstanceID = raw, id
	return f
}

func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	u.User = nil
	return u.String()
}
