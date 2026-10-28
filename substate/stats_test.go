// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package substate

import (
	"context"
	"net"
	"testing"
	"time"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/stats"
)

func TestServerStatsHandlerConnsAndBytes(t *testing.T) {
	changes := 0
	h := &ServerStatsHandler{OnConnChange: func() { changes++ }}
	local, _ := net.ResolveTCPAddr("tcp", "10.0.0.1:50051")
	remote, _ := net.ResolveTCPAddr("tcp", "10.0.0.25:52144")
	ctx := h.TagConn(context.Background(), &stats.ConnTagInfo{LocalAddr: local, RemoteAddr: remote})
	h.HandleConn(ctx, &stats.ConnBegin{})
	if h.Conns() != 1 {
		t.Errorf("Conns after begin = %d", h.Conns())
	}

	// Every RPC on the connection gets its own counter, tagged with the
	// connection's addresses.
	rpc := h.TagRPC(ctx, subscribeRPC)
	rb := RPCBytesFrom(rpc)
	if rb.Conn == nil || rb.Conn.Local != "10.0.0.1:50051" || rb.Conn.Remote != "10.0.0.25:52144" {
		t.Fatalf("connection tag = %+v", rb.Conn)
	}
	h.HandleRPC(rpc, &stats.OutPayload{WireLength: 128})
	h.HandleRPC(rpc, &stats.OutPayload{WireLength: 2})
	h.HandleRPC(rpc, &stats.InPayload{WireLength: 7})
	h.HandleRPC(rpc, &stats.Begin{}) // not a payload: ignored
	if tx, rx := rb.Load(); tx != 130 || rx != 7 {
		t.Errorf("bytes = %d/%d, want 130/7", tx, rx)
	}
	other := RPCBytesFrom(h.TagRPC(ctx, subscribeRPC))
	if tx, _ := other.Load(); tx != 0 {
		t.Errorf("a second RPC shares the first one's counter")
	}

	h.HandleConn(ctx, &stats.ConnEnd{})
	if h.Conns() != 0 {
		t.Errorf("Conns after end = %d", h.Conns())
	}
	if changes != 2 {
		t.Errorf("OnConnChange called %d times, want 2", changes)
	}
}

// A handler built without a callback, which is how a server that does not
// publish state would use it, counts connections and does not panic; an RPC
// whose context carries no connection tag still gets a counter.
func TestServerStatsHandlerWithoutCallbackOrConnTag(t *testing.T) {
	h := &ServerStatsHandler{}
	h.HandleConn(context.Background(), &stats.ConnBegin{})
	if h.Conns() != 1 {
		t.Errorf("Conns = %d, want 1", h.Conns())
	}
	rpc := h.TagRPC(context.Background(), subscribeRPC)
	rb := RPCBytesFrom(rpc)
	if rb.Conn != nil {
		t.Errorf("connection tag without TagConn = %+v, want nil", rb.Conn)
	}
	h.HandleRPC(rpc, &stats.InPayload{WireLength: 9})
	if _, rx := rb.Load(); rx != 9 {
		t.Errorf("rx = %d, want 9", rx)
	}
	h.HandleConn(context.Background(), &stats.ConnEnd{})
	if h.Conns() != 0 {
		t.Errorf("Conns after end = %d, want 0", h.Conns())
	}
}

var subscribeRPC = &stats.RPCTagInfo{FullMethodName: SubscribeMethod}

// getServer answers Subscribe with one notification and Get with one
// update, and keeps the counter the handler attached to each RPC's
// context, so the test can read it after the call.
type getServer struct {
	gnmipb.UnimplementedGNMIServer
	rb    chan *RPCBytes
	getRb chan *RPCBytes
}

func (g *getServer) Subscribe(stream gnmipb.GNMI_SubscribeServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	g.rb <- RPCBytesFrom(stream.Context())
	path := req.GetSubscribe().GetSubscription()[0].GetPath()
	if err := stream.Send(&gnmipb.SubscribeResponse{Response: &gnmipb.SubscribeResponse_Update{
		Update: &gnmipb.Notification{Update: []*gnmipb.Update{{Path: path}}}}}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return nil
}

func (g *getServer) Get(ctx context.Context, req *gnmipb.GetRequest) (*gnmipb.GetResponse, error) {
	g.getRb <- RPCBytesFrom(ctx)
	return &gnmipb.GetResponse{Notification: []*gnmipb.Notification{{Update: []*gnmipb.Update{{Path: req.GetPath()[0]}}}}}, nil
}

// The contract the package exists for, driven through gRPC rather than by
// hand: the server-side chain tags the connection, the counter it attaches
// to the RPC's context is the one RPCBytesFrom finds in the handler, the
// bytes on the wire land in it, and the connection count follows the
// connection.
func TestServerStatsHandlerThroughGRPC(t *testing.T) {
	changes := make(chan int64, 8)
	h := &ServerStatsHandler{}
	h.OnConnChange = func() { changes <- h.Conns() }
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.StatsHandler(h))
	gs := &getServer{rb: make(chan *RPCBytes, 1), getRb: make(chan *RPCBytes, 1)}
	gnmipb.RegisterGNMIServer(srv, gs)
	go srv.Serve(lis)
	defer srv.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() // the explicit Close below is the one the test observes; this covers an early Fatal
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	path := &gnmipb.Path{Elem: []*gnmipb.PathElem{{Name: "interfaces"}, {Name: "interface", Key: map[string]string{"name": "Ethernet0"}}}}
	client := gnmipb.NewGNMIClient(conn)
	// A Get is left untagged: it reads back an inert counter.
	if _, err := client.Get(ctx, &gnmipb.GetRequest{Path: []*gnmipb.Path{path}}); err != nil {
		t.Fatal(err)
	}
	if grb := <-gs.getRb; grb.Conn != nil {
		t.Errorf("a Get was tagged: %+v", grb.Conn)
	}
	stream, err := client.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&gnmipb.SubscribeRequest{Request: &gnmipb.SubscribeRequest_Subscribe{Subscribe: &gnmipb.SubscriptionList{
		Subscription: []*gnmipb.Subscription{{Path: path}}}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	rb := <-gs.rb
	if rb.Conn == nil || rb.Conn.Remote == "" || rb.Conn.Local != lis.Addr().String() {
		t.Fatalf("connection tag through gRPC = %+v", rb.Conn)
	}
	// When the stats handler sees each payload relative to the RPC's
	// completion is grpc-go's business; the counts are only required to
	// land, so they are waited for.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if tx, rx := rb.Load(); tx > 0 && rx > 0 {
			break
		}
		if time.Now().After(deadline) {
			tx, rx := rb.Load()
			t.Fatalf("bytes through gRPC = %d/%d, want both non-zero", tx, rx)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if h.Conns() != 1 {
		t.Errorf("Conns with one client = %d", h.Conns())
	}
	if n := <-changes; n != 1 {
		t.Errorf("OnConnChange saw %d on open, want 1", n)
	}
	conn.Close()
	select {
	case n := <-changes:
		if n != 0 {
			t.Errorf("OnConnChange saw %d on close, want 0", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no OnConnChange after the client closed")
	}
}

func TestRPCBytesFromWithoutHandler(t *testing.T) {
	rb := RPCBytesFrom(context.Background())
	if rb == nil || rb.Conn != nil {
		t.Fatalf("want an inert counter, got %+v", rb)
	}
	if tx, rx := rb.Load(); tx != 0 || rx != 0 {
		t.Errorf("inert counter = %d/%d", tx, rx)
	}
	var none *Bytes
	if tx, rx := none.Load(); tx != 0 || rx != 0 {
		t.Errorf("nil Bytes = %d/%d", tx, rx)
	}
}

func TestClientStatsHandlerBytes(t *testing.T) {
	b := &Bytes{}
	h := &ClientStatsHandler{B: b}
	ctx := h.TagRPC(h.TagConn(context.Background(), &stats.ConnTagInfo{}), &stats.RPCTagInfo{})
	h.HandleConn(ctx, &stats.ConnBegin{})
	h.HandleRPC(ctx, &stats.OutPayload{WireLength: 10})
	h.HandleRPC(ctx, &stats.InPayload{WireLength: 300})
	if tx, rx := b.Load(); tx != 10 || rx != 300 {
		t.Errorf("bytes = %d/%d, want 10/300", tx, rx)
	}
	// A handler built without a counter counts nothing and does not panic.
	(&ClientStatsHandler{}).HandleRPC(ctx, &stats.OutPayload{WireLength: 10})
}

// The hot path the feature adds to every message is one atomic add; it
// must stay allocation-free. The benchmark reports it, the test enforces it.
func TestStatsHandlerNoAllocs(t *testing.T) {
	h := &ServerStatsHandler{}
	ctx := h.TagRPC(h.TagConn(context.Background(), &stats.ConnTagInfo{}), subscribeRPC)
	out := &stats.OutPayload{WireLength: 128}
	if n := testing.AllocsPerRun(1000, func() { h.HandleRPC(ctx, out) }); n != 0 {
		t.Errorf("HandleRPC allocates %.1f times per message, want 0", n)
	}
}

func BenchmarkStatsHandler(b *testing.B) {
	h := &ServerStatsHandler{}
	ctx := h.TagRPC(h.TagConn(context.Background(), &stats.ConnTagInfo{}), subscribeRPC)
	out := &stats.OutPayload{WireLength: 128}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		h.HandleRPC(ctx, out)
	}
}

// Store carries totals into a counter that continues another's, keeps tx
// and rx apart, and is a no-op on a nil counter like Load and add.
func TestBytesStore(t *testing.T) {
	var b Bytes
	b.Store(30, 4)
	if tx, rx := b.Load(); tx != 30 || rx != 4 {
		t.Errorf("after Store(30, 4): Load = %d/%d", tx, rx)
	}
	b.add(&stats.OutPayload{WireLength: 5})
	b.add(&stats.InPayload{WireLength: 1})
	if tx, rx := b.Load(); tx != 35 || rx != 5 {
		t.Errorf("adds after Store: Load = %d/%d, want 35/5", tx, rx)
	}
	var none *Bytes
	none.Store(1, 1) // must not panic
	if tx, rx := none.Load(); tx != 0 || rx != 0 {
		t.Errorf("nil counter reads %d/%d", tx, rx)
	}
}

// Only Subscribe gets a counter: any other RPC on the server keeps its
// context, so it allocates nothing and RPCBytesFrom returns an inert one.
func TestServerStatsHandlerTagsSubscribeOnly(t *testing.T) {
	h := &ServerStatsHandler{}
	ctx := h.TagConn(context.Background(), &stats.ConnTagInfo{})
	for _, m := range []string{"/gnmi.gNMI/Get", "/gnmi.gNMI/Set", "/gnoi.file.File/Put", ""} {
		rpc := h.TagRPC(ctx, &stats.RPCTagInfo{FullMethodName: m})
		if rpc != ctx {
			t.Errorf("%q: context was tagged", m)
		}
		h.HandleRPC(rpc, &stats.OutPayload{WireLength: 5}) // must not panic
		if tx, _ := RPCBytesFrom(rpc).Load(); tx != 0 {
			t.Errorf("%q: bytes counted on an untagged RPC", m)
		}
	}
	if n := testing.AllocsPerRun(1000, func() { h.TagRPC(ctx, &stats.RPCTagInfo{FullMethodName: "/gnmi.gNMI/Get"}) }); n != 0 {
		t.Errorf("TagRPC allocates %.1f times for a Get, want 0", n)
	}
	if RPCBytesFrom(h.TagRPC(ctx, subscribeRPC)).Conn == nil {
		t.Errorf("Subscribe was not tagged")
	}
}
