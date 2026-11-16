// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package gnmi

// Dial-in side of subscription state publishing: what is recorded about
// each Subscribe stream, and the hooks Client.Run calls. The publisher
// itself lives in the substate package.

import (
	"strconv"
	"time"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
	"github.com/sonic-net/sonic-gnmi/common_utils"
	"github.com/sonic-net/sonic-gnmi/substate"
)

const (
	stateSubTable = substate.SubscriptionTable
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
// its first request. It is not changed afterwards.
type subscriptionInfo struct {
	peer, local      string
	listMode, prefix string
	origin, target   string
	encoding         string
	startTime        int64
}

// statePublisher is one server's publisher, with the gNMI server's
// per-subscription hooks. NewServer opens it and hands it to every Client
// the server creates; nothing else holds it.
type statePublisher struct {
	pub *substate.Publisher // its Server() is the server's state name
}

// newStatePublisher opens the publisher for a server's rows; open is how,
// nil meaning substate.New. An open that returns no publisher and no error
// gives (nil, nil): the server runs without publishing.
func newStatePublisher(name string, open func(string) (*substate.Publisher, error)) (*statePublisher, error) {
	if open == nil {
		open = substate.New
	}
	pub, err := open(name)
	if err != nil || pub == nil {
		return nil, err
	}
	return &statePublisher{pub: pub}, nil
}

// stateID is the STATE_DB id of this server's subscription n.
func (p *statePublisher) stateID(n uint64) string { return stateIDFor(p.pub.Server(), n) }

// --- events ---------------------------------------------------------------

func (p *statePublisher) onAccept(c *Client) {
	id := p.stateID(c.id)
	p.pub.WriteRows(id, map[string]substate.Row{p.pub.Key(stateSubTable, id): c.stateRow(p.pub.Server())})
}

// onClose is called when the subscription's stream has ended; it does not
// close anything. It deletes the subscription's rows.
func (p *statePublisher) onClose(c *Client) {
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
		"list_mode":     s.listMode,
		"prefix":        s.prefix,
		"origin":        s.origin,
		"target":        s.target,
		"encoding":      s.encoding,
		"start_time":    substate.Num(s.startTime),
	}
}

// newSubscriptionInfo records a parsed request. Run calls it once, before
// any data client sees the request; nothing in it changes afterwards.
func newSubscriptionInfo(sub *gnmipb.SubscriptionList, origin, target, peer, local string, start time.Time) *subscriptionInfo {
	return &subscriptionInfo{
		peer:      peer,
		local:     local,
		listMode:  sub.GetMode().String(),
		prefix:    common_utils.ElemsString(sub.GetPrefix()), // the path part only; origin and target are fields
		origin:    origin,
		target:    target,
		encoding:  sub.GetEncoding().String(),
		startTime: start.Unix(),
	}
}
