// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package substate

import (
	"context"
	"sync/atomic"

	"google.golang.org/grpc/stats"
)

// Bytes counts the wire bytes a stream or connection has sent and
// received. Only atomic adds happen on the hot path; the counts are read
// with Load and carried over with Store.
type Bytes struct {
	tx, rx atomic.Int64
}

// Load returns the current counts; a nil counter reads as zero.
func (b *Bytes) Load() (tx, rx int64) {
	if b == nil {
		return 0, 0
	}
	return b.tx.Load(), b.rx.Load()
}

// Store sets both counts, for a counter that continues another's totals;
// a nil counter stores nothing.
func (b *Bytes) Store(tx, rx int64) {
	if b == nil {
		return
	}
	b.tx.Store(tx)
	b.rx.Store(rx)
}

func (b *Bytes) add(s stats.RPCStats) {
	if b == nil {
		return
	}
	switch v := s.(type) {
	case *stats.OutPayload:
		b.tx.Add(int64(v.WireLength))
	case *stats.InPayload:
		b.rx.Add(int64(v.WireLength))
	}
}

// ConnTag is what the server-side handler records about a connection.
type ConnTag struct {
	Local, Remote string
}

// RPCBytes is the per-RPC byte counter the ServerStatsHandler attaches to
// an RPC's context, with the connection it arrived on; an RPC handler
// reads it back with RPCBytesFrom.
type RPCBytes struct {
	Bytes
	Conn *ConnTag
}

type connTagKey struct{}
type rpcBytesKey struct{}

// SubscribeMethod is the gNMI Subscribe RPC, the only RPC whose bytes the
// ServerStatsHandler counts.
const SubscribeMethod = "/gnmi.gNMI/Subscribe"

// ServerStatsHandler is the gRPC stats handler for a server: it keeps the
// live connection count and a byte counter per Subscribe RPC. Every other
// RPC on the server (Get, Set, gNSI, gNOI) is left untagged, so it costs no
// allocation and reads back an inert counter.
type ServerStatsHandler struct {
	conns atomic.Int64
	// OnConnChange, when set, is called after a connection opens or closes,
	// so a row that shows the count can be rewritten without waiting for a
	// timer. gRPC calls it on the connection's own goroutine, so it must not
	// block or touch Redis itself: it should enqueue the work.
	OnConnChange func()
}

// Conns is the number of connections open right now. It may be read from
// any goroutine.
func (h *ServerStatsHandler) Conns() int64 {
	return h.conns.Load()
}

func (h *ServerStatsHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	t := &ConnTag{}
	if info.LocalAddr != nil {
		t.Local = info.LocalAddr.String()
	}
	if info.RemoteAddr != nil {
		t.Remote = info.RemoteAddr.String()
	}
	return context.WithValue(ctx, connTagKey{}, t)
}

func (h *ServerStatsHandler) HandleConn(ctx context.Context, s stats.ConnStats) {
	switch s.(type) {
	case *stats.ConnBegin:
		h.conns.Add(1)
	case *stats.ConnEnd:
		h.conns.Add(-1)
	default:
		return
	}
	if h.OnConnChange != nil {
		h.OnConnChange()
	}
}

func (h *ServerStatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	if info.FullMethodName != SubscribeMethod {
		return ctx
	}
	rb := &RPCBytes{}
	if t, ok := ctx.Value(connTagKey{}).(*ConnTag); ok {
		rb.Conn = t
	}
	return context.WithValue(ctx, rpcBytesKey{}, rb)
}

func (h *ServerStatsHandler) HandleRPC(ctx context.Context, s stats.RPCStats) {
	if rb, ok := ctx.Value(rpcBytesKey{}).(*RPCBytes); ok {
		rb.add(s)
	}
}

// RPCBytesFrom returns the byte counter a ServerStatsHandler attached to an
// RPC's context, or an inert one when there is no handler.
func RPCBytesFrom(ctx context.Context) *RPCBytes {
	if rb, ok := ctx.Value(rpcBytesKey{}).(*RPCBytes); ok {
		return rb
	}
	return &RPCBytes{}
}

// ClientStatsHandler is the gRPC stats handler for one client connection,
// for a process that dials out: every RPC on the connection adds to the one
// Bytes it was created with. A nil Bytes counts nothing.
type ClientStatsHandler struct {
	B *Bytes
}

func (h *ClientStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (h *ClientStatsHandler) HandleConn(context.Context, stats.ConnStats) {}
func (h *ClientStatsHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}
func (h *ClientStatsHandler) HandleRPC(_ context.Context, s stats.RPCStats) { h.B.add(s) }
