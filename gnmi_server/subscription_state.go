// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package gnmi

// Subscription state publishing for dial-in subscriptions: the server
// row's counters, what is captured about a subscription at accept, and the
// hooks the Subscribe handler calls at accept and close. The generic
// publisher, the tables and the stats handlers are the substate package.

import (
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
	"github.com/sonic-net/sonic-gnmi/common_utils"
	"github.com/sonic-net/sonic-gnmi/substate"
)

const (
	stateServerTable = substate.ServerTable
	stateSubTable    = substate.SubscriptionTable
	statePathTable   = substate.PathTable
)

// stateIDFor is the id of subscription n for a server named name. The
// default server uses the bare number, as it always has. Any other name is
// put in front, so a row says by its key which server wrote it, and a
// server's startup clear can tell its own leftover rows from another
// server's.
func stateIDFor(name string, n uint64) string {
	id := strconv.FormatUint(n, 10)
	if name == substate.ServerGNMI {
		return id
	}
	return name + "_" + id
}

// subscriptionInfo is what the server records about a subscription from
// its first request. Run sets user after authentication, before the row is
// written; nothing in it changes after that.
type subscriptionInfo struct {
	peer, local, user string
	listMode, prefix  string
	origin, target    string
	encoding          string
	startTime         int64
	paths             []pathState
}

type pathState struct {
	path        string
	mode        string
	sampleNs    uint64
	heartbeatNs uint64
	suppress    bool
}

// statePublisher is one server's publisher: its server row's live counts
// and the per-subscription events. NewServer opens it and hands it to every
// Client the server creates; nothing else holds it.
type statePublisher struct {
	pub     *substate.Publisher // its Server() is the server's state name
	handler *substate.ServerStatsHandler
	// admission is the connection manager streams are admitted by, for the
	// server row's admitted count. Subscribe sets it; it is read without
	// connectionManagerMu, which is held across Redis I/O.
	admission atomic.Pointer[ConnectionManager]

	activeSubs atomic.Int64 // accepted and not yet closed
}

// newStatePublisher opens the publisher for a server's rows; open is how,
// nil meaning substate.New. handler is the stats handler already registered
// on the gRPC servers. An open that returns no publisher and no error gives
// (nil, nil): the server runs without publishing.
func newStatePublisher(name string, open func(string) (*substate.Publisher, error), handler *substate.ServerStatsHandler) (*statePublisher, error) {
	if open == nil {
		open = substate.New
	}
	p := &statePublisher{handler: handler}
	pub, err := open(name)
	if err != nil || pub == nil {
		return nil, err
	}
	p.pub = pub
	// A connection opening or closing rewrites the row at once.
	handler.OnConnChange = pub.UpdateServer
	return p, nil
}

// counters are the server row's live fields, read whenever the row is written.
// active_connections counts gRPC connections on every listener, including
// ones that carry no Subscribe; it is not the quantity threshold limits.
func (p *statePublisher) counters() map[string]int64 {
	row := map[string]int64{
		// Streams the server is serving: counted past admission and the
		// authentication and policy checks.
		"active_subscriptions": p.activeSubs.Load(),
		// The admission limiter's own count, the one threshold is enforced
		// against. The limiter is shared by every server in the process, so
		// with two servers each row shows the process's count. It counts
		// streams past admission, including those still in the
		// source, authentication or policy checks; two identical requests
		// from one collector in the same second share a slot.
		"admitted_subscriptions": int64(p.admittedCount()),
		"active_connections":     p.handler.Conns(),
		// Always 0 while admission refuses at the limit instead of
		// queueing; the field is part of the row's contract so a reader
		// does not change when admission does.
		"waiting_subscriptions": 0,
	}
	return row
}

// admittedCount is the admission limiter's count of taken slots.
func (p *statePublisher) admittedCount() int {
	if cm := p.admission.Load(); cm != nil {
		return cm.Count()
	}
	return 0
}

// stateID is the STATE_DB id of this server's subscription n.
func (p *statePublisher) stateID(n uint64) string { return stateIDFor(p.pub.Server(), n) }

// ownsID reports whether a leftover subscription row is this server's, for
// the startup flush. Any other id belongs to another producer, such as the
// dial-out client's dialout_<name> rows or another server's, and is left
// alone. A name is used by one live server at a time, and a restart stops
// the old server before the new one starts, so every row of its own found
// at startup is a leftover.
func (p *statePublisher) ownsID(id string) bool { return ownsIDFor(p.pub.Server(), id) }

// ownsIDFor reports whether id is a subscription id of a server named name:
// a bare number for the default server, name_<number> for any other.
func ownsIDFor(name, id string) bool {
	if name != substate.ServerGNMI {
		rest, ok := strings.CutPrefix(id, name+"_")
		if !ok {
			return false
		}
		id = rest
	}
	_, err := strconv.ParseUint(id, 10, 64)
	return err == nil
}

// start starts the publisher loop, which first flushes the rows a previous
// run of this server left and writes the server row. listen and unixSocket
// are empty when not served; vrf is the TCP listener's VRF; threshold is
// the admission limit, 0 for none.
func (p *statePublisher) start(listen, unixSocket, vrf string, threshold int) {
	p.pub.Start(substate.Row{
		"threshold":      substate.Num(threshold),
		"listen_address": listen,
		"unix_socket":    unixSocket,
		"vrf":            vrf,
	}, p.counters, p.ownsID)
}

// --- events ---------------------------------------------------------------

func (p *statePublisher) onAccept(c *Client) {
	p.activeSubs.Add(1)
	id := p.stateID(c.id)
	rows := map[string]substate.Row{p.pub.Key(stateSubTable, id): c.stateRow(p.pub.Server())}
	for n, ps := range c.state.paths {
		rows[p.pub.Key(statePathTable, id, strconv.Itoa(n))] = substate.PathRow(ps.path, ps.mode, ps.sampleNs, ps.heartbeatNs, ps.suppress)
	}
	p.pub.WriteRows(id, rows, c.counterRow)
}

// onClose is called when the subscription's stream has ended; it does not
// close anything. It deletes the subscription's rows.
func (p *statePublisher) onClose(c *Client) {
	p.activeSubs.Add(-1)
	p.pub.Delete(p.stateID(c.id))
}

// --- rows from a Client ----------------------------------------------------

func (c *Client) stateRow(server string) substate.Row {
	s := c.state
	return substate.Row{
		"direction":     "dial-in",
		"peer":          s.peer,
		"local_address": s.local,
		"server":        server,
		"user":          s.user,
		"list_mode":     s.listMode,
		"prefix":        s.prefix,
		"origin":        s.origin,
		"target":        s.target,
		"encoding":      s.encoding,
		"start_time":    substate.Num(s.startTime),
	}
}

// counterRow is the tick snapshot: the fields that move while the stream lives.
func (c *Client) counterRow() substate.Row {
	var tx, rx int64
	if c.bytes != nil {
		tx, rx = c.bytes.Load()
	}
	depth := 0
	c.mu.RLock()
	if c.q != nil && !c.q.Disposed() {
		depth = c.q.Len()
	}
	c.mu.RUnlock()
	return substate.Row{
		"tx_msgs":     substate.Num(c.sendCount.Load()),
		"rx_msgs":     substate.Num(c.recvCount.Load()),
		"tx_bytes":    substate.Num(tx),
		"rx_bytes":    substate.Num(rx),
		"error_count": substate.Num(c.errorCount.Load()),
		"queue_depth": substate.Num(depth),
	}
}

// newSubscriptionInfo records a parsed request. Run calls it once, before
// any data client sees the request, and sets user itself after
// authentication. b is the backend Run resolved for the request; each path
// renders as common_utils.RenderPath shows it for that backend.
func newSubscriptionInfo(sub *gnmipb.SubscriptionList, origin, target string, b common_utils.Backend, peer, local string, start time.Time) *subscriptionInfo {
	s := &subscriptionInfo{
		peer:      peer,
		local:     local,
		listMode:  sub.GetMode().String(),
		prefix:    common_utils.ElemsString(sub.GetPrefix()), // the path part only; origin and target are fields
		origin:    origin,
		target:    target,
		encoding:  sub.GetEncoding().String(),
		startTime: start.Unix(),
	}
	stream := sub.GetMode() == gnmipb.SubscriptionList_STREAM
	// The per-path mode and intervals exist only in a STREAM list; a POLL
	// or ONCE path row carries the path and an empty mode, nothing else.
	for _, su := range sub.GetSubscription() {
		ps := pathState{path: common_utils.RenderPath(sub.GetPrefix(), su.GetPath(), target, b)}
		if stream {
			ps.mode = su.GetMode().String()
			ps.sampleNs = su.GetSampleInterval()
			ps.heartbeatNs = su.GetHeartbeatInterval()
			ps.suppress = su.GetSuppressRedundant()
		}
		s.paths = append(s.paths, ps)
	}
	return s
}
