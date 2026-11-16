// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package gnmi

// Tests for subscription state publishing. They run against the same
// redis-server as the rest of the suite and read STATE_DB back.

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "github.com/openconfig/gnmi/proto/gnmi"
	"github.com/redis/go-redis/v9"
	sdcfg "github.com/sonic-net/sonic-gnmi/sonic_db_config"
	"github.com/sonic-net/sonic-gnmi/substate"
	testcert "github.com/sonic-net/sonic-gnmi/testdata/tls"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const stateTestPort = 8081

func stateDb(t *testing.T) *redis.Client {
	t.Helper()
	ns, _ := sdcfg.GetDbDefaultNamespace()
	return getRedisClientN(t, 6, ns)
}

// waitFor polls cond until it holds or the deadline passes. Every write
// happens on the publisher goroutine, so an assertion on a row polls.
func waitFor(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func hget(t *testing.T, rc *redis.Client, key, field string) string {
	t.Helper()
	v, _ := rc.HGet(context.Background(), key, field).Result()
	return v
}

func dialState(t *testing.T) (*grpc.ClientConn, pb.GNMIClient) {
	t.Helper()
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	conn, err := grpc.Dial("127.0.0.1:"+strconv.Itoa(stateTestPort), grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn, pb.NewGNMIClient(conn)
}

// clearStateTables removes the rows this feature writes, so a test never
// inherits a row from an earlier server.
func clearStateTables(t *testing.T) {
	t.Helper()
	rc := stateDb(t)
	defer rc.Close()
	ctx := context.Background()
	for _, pat := range []string{stateSubTable + "|*"} {
		if keys, _ := rc.Keys(ctx, pat).Result(); len(keys) > 0 {
			rc.Del(ctx, keys...)
		}
	}
}

// newStateServer builds the server the state tests run against. It starts
// from createServer's configuration and differs in two ways: adjust runs
// before NewServer; and a failure stops the test, since nothing after it can
// run.
func newStateServer(t *testing.T, port int64, adjust func(*Config)) *Server {
	t.Helper()
	certificate, err := testcert.NewCert()
	if err != nil {
		t.Fatalf("could not load server key pair: %s", err)
	}
	tlsCfg := &tls.Config{ClientAuth: tls.RequestClientCert, Certificates: []tls.Certificate{certificate}}
	cfg := testServerConfig(port)
	if adjust != nil {
		adjust(cfg)
	}
	// The previous test's server may still be releasing the port.
	var s *Server
	defer func() {
		if s != nil {
			t.Cleanup(func() { s.ForceStop() }) // a Fatalf must not leave the name claimed
		}
	}()
	for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		s, err = NewServer(cfg, []grpc.ServerOption{grpc.Creds(credentials.NewTLS(tlsCfg))}, nil)
		if err == nil || !strings.Contains(err.Error(), "no listener configured") || time.Now().After(deadline) {
			break
		}
	}
	if err != nil {
		t.Fatalf("Failed to create gNMI server: %v", err)
	}
	return s
}

// startStateServer clears the databases, runs seed, starts a server with the
// given threshold (0 keeps the default) and waits for its publisher loop.
func startStateServer(t *testing.T, threshold int, seed func()) *Server {
	t.Helper()
	return startStateServerWith(t, func(c *Config) {
		if threshold > 0 {
			c.Threshold = threshold
		}
	}, seed)
}

// waitServing waits until the test server answers, so a test that stops
// it at once does not race Serve.
func waitServing(t *testing.T, s *Server) {
	t.Helper()
	if s.lis == nil {
		return // Unix socket only
	}
	conn, err := grpc.Dial(s.lis.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewGNMIClient(conn)
	waitFor(t, "server serving", 3*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		_, err := client.Capabilities(ctx, &pb.CapabilityRequest{})
		return status.Code(err) != codes.Unavailable // an auth refusal is an answer too
	})
}

func startStateServerWith(t *testing.T, adjust func(*Config), seed func()) *Server {
	t.Helper()
	ns, _ := sdcfg.GetDbDefaultNamespace()
	prepareDb(t, ns)
	clearStateTables(t)
	if seed != nil {
		seed()
	}
	s := newStateServer(t, stateTestPort, adjust)
	go runServer(t, s)
	waitServing(t, s)
	if s.statePub != nil {
		onPublisherLoop(t, s.statePub.pub)
	}
	return s
}

// onPublisherLoop waits until the publisher loop has run everything queued
// before it.
func onPublisherLoop(t *testing.T, pub *substate.Publisher) {
	t.Helper()
	done := make(chan struct{})
	if !pub.Enqueue(func() { close(done) }) {
		t.Fatal("publisher queue full")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("publisher loop did not run")
	}
}

// stateNameInUse reports whether a live server in the process holds name.
func stateNameInUse(name string) bool {
	stateNames.Lock()
	defer stateNames.Unlock()
	return stateNames.inUse[name]
}

// Stopping the server stops its publisher and frees its state name.
func TestSubscriptionStateStopStopsPublisher(t *testing.T) {
	s := startStateServer(t, 0, nil)
	pub := s.statePub
	s.Stop()
	if stateNameInUse(substate.ServerGNMI) {
		t.Errorf("state name still held after Stop")
	}
	if !pub.pub.Stopped() {
		t.Errorf("publisher loop still running after Stop")
	}
}

// ForceStop stops the publisher and frees the name too.
func TestSubscriptionStateForceStopStopsPublisher(t *testing.T) {
	s := startStateServer(t, 0, nil)
	rc := stateDb(t)
	defer rc.Close()
	// A row the publisher holds stays: ForceStop does not touch Redis.
	own := s.statePub.pub.Key(stateSubTable, "999")
	s.statePub.pub.WriteRows("999", map[string]substate.Row{own: {"direction": "dial-in"}})
	waitFor(t, "the registered row", 3*time.Second, func() bool { return hget(t, rc, own, "direction") == "dial-in" })
	pub := s.statePub
	s.ForceStop()
	if stateNameInUse(substate.ServerGNMI) || !pub.pub.Stopped() {
		t.Errorf("publisher still held or running after ForceStop")
	}
	if n, _ := rc.Exists(context.Background(), own).Result(); n != 1 {
		t.Errorf("ForceStop deleted a row")
	}
}

// A NewServer that fails installs no publisher: the publisher starts after
// every error return.
func TestSubscriptionStateFailedNewServerInstallsNothing(t *testing.T) {
	ns, _ := sdcfg.GetDbDefaultNamespace()
	prepareDb(t, ns)
	clearStateTables(t)
	if _, err := NewServer(&Config{Port: -1}, nil, nil); err == nil || !strings.Contains(err.Error(), "no listener configured") {
		t.Fatalf("NewServer with no listener: err = %v", err)
	}
	if stateNameInUse(substate.ServerGNMI) {
		t.Errorf("a NewServer that failed kept a state name")
	}
}

// A NewServer that fails building the CONFIG_DB journal installs no
// publisher either: the publisher starts after that error return too.
func TestSubscriptionStateJournalFailureInstallsNothing(t *testing.T) {
	ns, _ := sdcfg.GetDbDefaultNamespace()
	prepareDb(t, ns)
	clearStateTables(t)
	savedFlag := *enableConfigDbJournal
	*enableConfigDbJournal = true
	defer func() { *enableConfigDbJournal = savedFlag }()
	cfg := testServerConfig(stateTestPort)
	cfg.newDbJournal = func(string) (*DbJournal, error) { return nil, errors.New("journal unavailable") }
	if _, err := NewServer(cfg, nil, nil); err == nil || !strings.Contains(err.Error(), "failed to create CONFIG_DB Journal") {
		t.Fatalf("NewServer with a failing journal: err = %v", err)
	}
	if stateNameInUse(substate.ServerGNMI) {
		t.Errorf("a NewServer that failed on the journal kept a state name")
	}
	// What the failing NewServer opened is closed again: the port is free.
	if l, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(stateTestPort)); err != nil {
		t.Errorf("the port of a NewServer that failed is still taken: %v", err)
	} else {
		l.Close()
	}
}

// A STATE_DB that cannot be resolved disables publishing and nothing else:
// the server still serves and has no publisher. It still holds its state
// name while it runs, so a second server cannot take it.
func TestSubscriptionStateServesWithoutPublisher(t *testing.T) {
	clearStateTables(t)
	s := newStateServer(t, stateTestPort, func(c *Config) {
		c.Threshold = 1
		c.openStatePublisher = func(string) (*substate.Publisher, error) {
			return nil, errors.New("no STATE_DB in this database configuration")
		}
	})
	defer s.Stop()
	go runServer(t, s)
	if s.statePub != nil {
		t.Errorf("a publisher was kept although it could not be opened")
	}
	if !stateNameInUse(substate.ServerGNMI) {
		t.Errorf("a server without a publisher does not hold its state name")
	}
	conn, client := dialState(t)
	defer conn.Close()
	if _, err := client.Capabilities(context.Background(), &pb.CapabilityRequest{}); err != nil {
		t.Fatalf("server not serving without a publisher: %v", err)
	}
	// A Subscribe is accepted and served, one past the limit is refused,
	// the first is closed, and a rejected one is answered, all with nothing
	// to publish to: the guards around the publisher hooks in Run hold on
	// every path (accept, refusal, close, rejection).
	stream, cancel := openStream(t, client, countersStreamRequest(uint64(time.Second)))
	refusedCtx, refusedCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer refusedCancel()
	refused, err := client.Subscribe(refusedCtx)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	refused.Send(countersStreamRequest(uint64(time.Second)))
	if _, err := refused.Recv(); status.Code(err) != codes.Unavailable {
		t.Errorf("second subscription at threshold 1 without a publisher: %v, want Unavailable", err)
	}
	cancel()
	var recvErr error
	for recvErr == nil { // updates sent before the cancel may still be buffered
		_, recvErr = stream.Recv()
	}
	if status.Code(recvErr) != codes.Canceled {
		t.Errorf("stream after cancel: %v, want Canceled", recvErr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// A refused stream never takes a slot; the slot to wait for is the
	// canceled first stream's, released when its Run returns.
	waitFor(t, "a free admission slot", 3*time.Second, func() bool {
		r, err := client.Subscribe(ctx)
		if err != nil {
			return false
		}
		req := countersStreamRequest(uint64(time.Second))
		req.GetSubscribe().Prefix.Target = "SHOW"
		r.Send(req)
		_, err = r.Recv()
		if status.Code(err) == codes.Unavailable {
			return false
		}
		if status.Code(err) != codes.Unimplemented {
			t.Errorf("rejection without a publisher: %v, want Unimplemented", err)
		}
		return true
	})
	rc := stateDb(t)
	defer rc.Close()
	if len(subscriptionKeys(t, rc)) != 0 {
		t.Errorf("a subscription row was written with no publisher")
	}
}

func subscriptionKeys(t *testing.T, rc *redis.Client) []string {
	t.Helper()
	keys, _ := rc.Keys(context.Background(), stateSubTable+"|*").Result()
	return keys
}

func countersStreamRequest(sampleNs uint64, extra ...*pb.Subscription) *pb.SubscribeRequest {
	subs := []*pb.Subscription{{
		Path:           &pb.Path{Elem: []*pb.PathElem{{Name: "COUNTERS"}, {Name: "Ethernet68"}}},
		Mode:           pb.SubscriptionMode_SAMPLE,
		SampleInterval: sampleNs,
	}}
	subs = append(subs, extra...)
	return &pb.SubscribeRequest{Request: &pb.SubscribeRequest_Subscribe{Subscribe: &pb.SubscriptionList{
		Prefix:       &pb.Path{Target: "COUNTERS_DB"},
		Mode:         pb.SubscriptionList_STREAM,
		Encoding:     pb.Encoding_JSON_IETF,
		Subscription: subs,
	}}}
}

// openStream sends one Subscribe request and waits for the first response,
// so the server has admitted the subscription by the time it returns.
func openStream(t *testing.T, client pb.GNMIClient, req *pb.SubscribeRequest) (pb.GNMI_SubscribeClient, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.Subscribe(ctx)
	if err != nil {
		cancel()
		t.Fatalf("subscribe: %v", err)
	}
	if err := stream.Send(req); err != nil {
		cancel()
		t.Fatalf("send: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		cancel()
		t.Fatalf("first response: %v", err)
	}
	return stream, cancel
}

func TestSubscriptionStateRows(t *testing.T) {
	s := startStateServer(t, 0, nil)
	defer s.Stop()
	rc := stateDb(t)
	defer rc.Close()

	conn, client := dialState(t)
	defer conn.Close()
	onChange := &pb.Subscription{
		Path: &pb.Path{Elem: []*pb.PathElem{{Name: "COUNTERS"}, {Name: "Ethernet70"}}},
		Mode: pb.SubscriptionMode_ON_CHANGE,
	}
	_, cancel := openStream(t, client, countersStreamRequest(uint64(time.Second), onChange))

	var subKey string
	waitFor(t, "subscription row", 3*time.Second, func() bool {
		keys := subscriptionKeys(t, rc)
		if len(keys) == 1 {
			subKey = keys[0]
			return true
		}
		return false
	})
	row, _ := rc.HGetAll(context.Background(), subKey).Result()
	want := map[string]string{
		"direction": "dial-in", "server": substate.ServerGNMI, "list_mode": "STREAM",
		"target": "COUNTERS_DB", "encoding": "JSON_IETF", "prefix": "", "origin": "",
	}
	for f, v := range want {
		if row[f] != v {
			t.Errorf("row %s = %q, want %q", f, row[f], v)
		}
	}
	if !strings.HasPrefix(row["peer"], "127.0.0.1:") {
		t.Errorf("peer = %q", row["peer"])
	}
	if !strings.HasSuffix(row["local_address"], ":"+strconv.Itoa(stateTestPort)) {
		t.Errorf("local_address = %q", row["local_address"])
	}
	if row["start_time"] == "" {
		t.Errorf("start_time not set")
	}
	// TELEMETRY_CONNECTIONS is written alongside GNMI_SUBSCRIPTION.
	if n, _ := rc.HLen(context.Background(), "TELEMETRY_CONNECTIONS").Result(); n != 1 {
		t.Errorf("TELEMETRY_CONNECTIONS has %d fields while one subscription is open, want 1", n)
	}

	cancel()
	waitFor(t, "row deleted at close", 3*time.Second, func() bool { return len(subscriptionKeys(t, rc)) == 0 })
}

func TestSubscriptionStateTwoStreamsOneCollector(t *testing.T) {
	s := startStateServer(t, 0, nil)
	defer s.Stop()
	rc := stateDb(t)
	defer rc.Close()
	conn, client := dialState(t)
	defer conn.Close()
	_, cancel1 := openStream(t, client, countersStreamRequest(uint64(time.Second)))
	defer cancel1()
	_, cancel2 := openStream(t, client, countersStreamRequest(uint64(time.Second)))
	defer cancel2()
	waitFor(t, "two rows", 3*time.Second, func() bool { return len(subscriptionKeys(t, rc)) == 2 })
	keys := subscriptionKeys(t, rc)
	if hget(t, rc, keys[0], "peer") != hget(t, rc, keys[1], "peer") {
		t.Errorf("one connection should give two rows with the same peer: %v", keys)
	}
}

// TestSubscriptionStateRedisUnavailable: with no reachable Redis the publisher
// must neither block nor panic; streams are not its concern.
func TestSubscriptionStateRedisUnavailable(t *testing.T) {
	p := &statePublisher{pub: substate.NewWithClient(substate.ServerGNMI, redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 100 * time.Millisecond}), "|")}
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.pub.Start()
		c := NewClient(&stateTestAddr{"10.0.0.9:1"})
		c.state = &subscriptionInfo{peer: "10.0.0.9:1"}
		for i := 0; i < 2000; i++ { // more than the queue holds: must drop, not block
			p.onAccept(c)
			p.onClose(c)
		}
		p.pub.Stop()
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("publisher blocked without Redis")
	}
	if p.pub.Dropped() == 0 {
		t.Errorf("expected dropped events with a full queue")
	}
}

type stateTestAddr struct{ s string }

func (a *stateTestAddr) Network() string { return "tcp" }

func (a *stateTestAddr) String() string { return a.s }

// recvStream is a Subscribe stream that hands Run one request and then
// waits for the test to end it.
type recvStream struct {
	pb.GNMI_SubscribeServer
	ctx  context.Context
	req  *pb.SubscribeRequest
	sent bool
}

func (r *recvStream) Context() context.Context         { return r.ctx }
func (r *recvStream) Send(*pb.SubscribeResponse) error { return nil }
func (r *recvStream) Recv() (*pb.SubscribeRequest, error) {
	if !r.sent {
		r.sent = true
		return r.req, nil
	}
	<-r.ctx.Done()
	return nil, r.ctx.Err()
}

// With Redis unreachable the server still serves: accepting and closing a
// stream never wait on the publisher, and neither does the stop.
func TestSubscriptionStateServesWithRedisDown(t *testing.T) {
	ns, _ := sdcfg.GetDbDefaultNamespace()
	prepareDb(t, ns)
	s := newStateServer(t, stateTestPort, func(c *Config) {
		c.openStatePublisher = func(server string) (*substate.Publisher, error) {
			return substate.NewWithClient(server, redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 100 * time.Millisecond}), "|"), nil
		}
	})
	go runServer(t, s)
	defer s.ForceStop() // also when the test fails before its own stop
	waitServing(t, s)
	conn, client := dialState(t)
	defer conn.Close()
	start := time.Now()
	_, cancel := openStream(t, client, countersStreamRequest(uint64(time.Second)))
	cancel()
	s.Stop()
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("a stream and a stop took %v with Redis down", d)
	}
}

// Two servers in one process: a second one with the same state name fails
// to build and leaves the first one's publisher and rows alone; with its own
// name it publishes beside the first.
func TestSubscriptionStateTwoServersInOneProcess(t *testing.T) {
	first := startStateServer(t, 0, nil)
	defer first.Stop()
	rc := stateDb(t)
	defer rc.Close()
	own := first.statePub.pub.Key(stateSubTable, "999")
	first.statePub.pub.WriteRows("999", map[string]substate.Row{own: {"direction": "dial-in"}})
	waitFor(t, "the first server's row", 3*time.Second, func() bool { return hget(t, rc, own, "direction") == "dial-in" })

	if _, err := NewServer(testServerConfig(stateTestPort+1), nil, nil); err == nil || !strings.Contains(err.Error(), "already used") {
		t.Errorf("a second server with the same state name: err = %v, want the name refused", err)
	}
	if first.statePub.pub.Stopped() || !stateNameInUse(substate.ServerGNMI) {
		t.Errorf("a second server with the same name stopped the first one's publisher or freed its name")
	}
	if hget(t, rc, own, "direction") != "dial-in" {
		t.Errorf("a second server with the same name removed the first one's row")
	}

	// A server with its own name publishes beside the first: each stream's
	// row is keyed and labelled by the server that serves it, and a stream
	// that ends deletes only its own row.
	other := newStateServer(t, stateTestPort+1, func(c *Config) { c.StateName = "second" })
	go runServer(t, other)
	defer other.Stop()
	waitServing(t, other)
	firstConn, firstClient := dialState(t)
	defer firstConn.Close()
	otherConn, err := grpc.Dial("127.0.0.1:"+strconv.Itoa(stateTestPort+1), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer otherConn.Close()
	_, cancelFirst := openStream(t, firstClient, countersStreamRequest(uint64(time.Second)))
	defer cancelFirst()
	_, cancelOther := openStream(t, pb.NewGNMIClient(otherConn), countersStreamRequest(uint64(time.Second)))
	defer cancelOther()
	var firstKey, otherKey string
	waitFor(t, "a row from each server", 3*time.Second, func() bool {
		firstKey, otherKey = "", ""
		for _, k := range subscriptionKeys(t, rc) {
			switch id := strings.TrimPrefix(k, stateSubTable+"|"); {
			case strings.HasPrefix(id, "second_"):
				otherKey = k
			case id != "999":
				firstKey = k
			}
		}
		return firstKey != "" && otherKey != ""
	})
	if got := hget(t, rc, otherKey, "server"); got != "second" {
		t.Errorf("the named server's row %s has server %q, want second", otherKey, got)
	}
	if got := hget(t, rc, firstKey, "server"); got != substate.ServerGNMI {
		t.Errorf("the first server's row %s has server %q, want %s", firstKey, got, substate.ServerGNMI)
	}
	if _, err := strconv.ParseUint(strings.TrimPrefix(firstKey, stateSubTable+"|"), 10, 64); err != nil {
		t.Errorf("the default server's row %s is not numbered", firstKey)
	}
	cancelOther()
	waitFor(t, "the named server's row gone", 3*time.Second, func() bool {
		n, _ := rc.Exists(context.Background(), otherKey).Result()
		return n == 0
	})
	if n, _ := rc.Exists(context.Background(), firstKey).Result(); n != 1 {
		t.Errorf("a stream ending on one server deleted the other server's row")
	}
}

// A state name that would break a key, or take the dial-out client's ids,
// is refused before anything is opened.
func TestSubscriptionStateInvalidName(t *testing.T) {
	for _, name := range []string{"a|b", substate.ServerDialout, substate.ServerDialout + "_x"} {
		cfg := testServerConfig(stateTestPort + 2)
		cfg.StateName = name
		if _, err := NewServer(cfg, nil, nil); err == nil || !strings.Contains(err.Error(), "invalid state name") {
			t.Errorf("state name %q: err = %v, want it refused", name, err)
		}
		if stateNameInUse(name) {
			t.Errorf("a refused state name %q stayed reserved", name)
		}
	}
}

// A server refused for a taken name touches nothing: the live server's Unix
// socket, at the same path, is still there.
func TestSubscriptionStateRefusedNameKeepsSocket(t *testing.T) {
	sock := t.TempDir() + "/gnmi.sock"
	first := startStateServerWith(t, func(c *Config) { c.UnixSocket = sock }, nil)
	defer first.Stop()
	cfg := testServerConfig(stateTestPort + 1)
	cfg.UnixSocket = sock
	if _, err := NewServer(cfg, nil, nil); err == nil || !strings.Contains(err.Error(), "already used") {
		t.Fatalf("a second server with the same name: err = %v, want the name refused", err)
	}
	if _, err := os.Stat(sock); err != nil {
		t.Errorf("the refused server removed the live server's socket: %v", err)
	}
}

// An open that returns no publisher and no error leaves the server without
// one, as an error does, instead of a wrapper around nil.
func TestSubscriptionStateNilPublisher(t *testing.T) {
	clearStateTables(t)
	s := newStateServer(t, stateTestPort, func(c *Config) {
		c.openStatePublisher = func(string) (*substate.Publisher, error) { return nil, nil }
	})
	defer s.Stop()
	if s.statePub != nil {
		t.Errorf("a nil publisher was wrapped")
	}
}

// A prefix with path elements is recorded as the collector sent it.
func TestSubscriptionStatePrefixRecorded(t *testing.T) {
	s := startStateServer(t, 0, nil)
	defer s.Stop()
	rc := stateDb(t)
	defer rc.Close()
	conn, client := dialState(t)
	defer conn.Close()
	req := &pb.SubscribeRequest{Request: &pb.SubscribeRequest_Subscribe{Subscribe: &pb.SubscriptionList{
		Prefix:   &pb.Path{Target: "COUNTERS_DB", Elem: []*pb.PathElem{{Name: "COUNTERS"}}},
		Mode:     pb.SubscriptionList_STREAM,
		Encoding: pb.Encoding_JSON_IETF,
		Subscription: []*pb.Subscription{{
			Path:           &pb.Path{Elem: []*pb.PathElem{{Name: "Ethernet68"}}},
			Mode:           pb.SubscriptionMode_SAMPLE,
			SampleInterval: uint64(time.Second),
		}},
	}}}
	_, cancel := openStream(t, client, req)
	defer cancel()
	var prefix string
	waitFor(t, "subscription row", 3*time.Second, func() bool {
		keys := subscriptionKeys(t, rc)
		if len(keys) != 1 {
			return false
		}
		prefix = hget(t, rc, keys[0], "prefix")
		return true
	})
	if prefix != "/COUNTERS" {
		t.Errorf("prefix = %q, want /COUNTERS", prefix)
	}
}

// A Subscribe over the Unix socket gets the socket as its local address:
// the stats handler is on that server too.
func TestSubscriptionStateUnixSocketLocalAddress(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "gnmi.sock")
	s := startStateServerWith(t, func(c *Config) { c.UnixSocket = sock }, nil)
	defer s.Stop()
	rc := stateDb(t)
	defer rc.Close()
	conn, err := grpc.Dial("unix://"+sock, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial %s: %v", sock, err)
	}
	defer conn.Close()
	_, cancel := openStream(t, pb.NewGNMIClient(conn), countersStreamRequest(uint64(time.Second)))
	defer cancel()
	var local string
	waitFor(t, "subscription row", 3*time.Second, func() bool {
		keys := subscriptionKeys(t, rc)
		if len(keys) != 1 {
			return false
		}
		local = hget(t, rc, keys[0], "local_address")
		return true
	})
	if local != sock {
		t.Errorf("local_address = %q, want the socket %s", local, sock)
	}
}
