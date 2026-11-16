// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package gnmi

// Tests for subscription state publishing. They run against the same
// redis-server as the rest of the suite and read STATE_DB back.

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "github.com/openconfig/gnmi/proto/gnmi"
	"github.com/redis/go-redis/v9"
	"github.com/sonic-net/sonic-gnmi/common_utils"
	sdcfg "github.com/sonic-net/sonic-gnmi/sonic_db_config"
	"github.com/sonic-net/sonic-gnmi/substate"
	"github.com/sonic-net/sonic-gnmi/swsscommon"
	testcert "github.com/sonic-net/sonic-gnmi/testdata/tls"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
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

func hgetInt(t *testing.T, rc *redis.Client, key, field string) int64 {
	t.Helper()
	n, _ := strconv.ParseInt(hget(t, rc, key, field), 10, 64)
	return n
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
	for _, pat := range []string{stateServerTable + "|*", stateSubTable + "|*", statePathTable + "|*"} {
		if keys, _ := rc.Keys(ctx, pat).Result(); len(keys) > 0 {
			rc.Del(ctx, keys...)
		}
	}
}

// fastTick sets s's publisher to the shortest interval, so a test sees a
// tick within a second.
func fastTick(s *Server) {
	s.statePub.pub.SetInterval(substate.MinPublishInterval)
}

// newStateServer builds the server the state tests run against. It starts
// from createServer's configuration and differs in two ways: adjust runs
// before NewServer, which is when the server row's static fields are taken;
// and a failure stops the test, since nothing after it can run.
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
// given threshold (0 keeps the default) and waits for its server row.
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
	waitFor(t, "server row", 3*time.Second, func() bool {
		rc := stateDb(t)
		defer rc.Close()
		return hget(t, rc, stateServerTable+"|"+substate.ServerGNMI, "pid") != ""
	})
	return s
}

// stateNameInUse reports whether a live server in the process holds name.
func stateNameInUse(name string) bool {
	stateNames.Lock()
	defer stateNames.Unlock()
	return stateNames.inUse[name]
}

func TestSubscriptionStateStartupFlush(t *testing.T) {
	rc := stateDb(t)
	defer rc.Close()
	ctx := context.Background()
	s := startStateServer(t, 7, func() {
		rc.HSet(ctx, stateSubTable+"|999", "peer", "stale")
		rc.HSet(ctx, statePathTable+"|999|0", "path", "stale")
		rc.HSet(ctx, stateSubTable+"|dialout_x", "peer", "not ours")
	})
	defer s.Stop()
	if n, _ := rc.Exists(ctx, stateSubTable+"|999", statePathTable+"|999|0").Result(); n != 0 {
		t.Errorf("startup left %d stale keys behind", n)
	}
	if n, _ := rc.Exists(ctx, stateSubTable+"|dialout_x").Result(); n != 1 {
		t.Errorf("startup flushed the dial-out client's row")
	}
	key := stateServerTable + "|" + substate.ServerGNMI
	// waitServing's connection may still be counted until its close is
	// written.
	waitFor(t, "no connection counted", 3*time.Second, func() bool { return hget(t, rc, key, "active_connections") == "0" })
	for _, f := range []string{"pid", "start_time", "last_updated"} {
		if hget(t, rc, key, f) == "" {
			t.Errorf("server row missing %s", f)
		}
	}
	for f, want := range map[string]string{
		"threshold":             "7",
		"publish_interval":      "10",
		"listen_address":        s.lis.Addr().String(),
		"vrf":                   "default",
		"unix_socket":           "",
		"active_connections":    "0",
		"waiting_subscriptions": "0",
	} {
		if got := hget(t, rc, key, f); got != want {
			t.Errorf("server row %s = %q, want %q", f, got, want)
		}
	}
	if hget(t, rc, key, "pid") != strconv.Itoa(os.Getpid()) {
		t.Errorf("pid = %s, want this process", hget(t, rc, key, "pid"))
	}
}

// A server with no TCP listener has no VRF to report.
func TestSubscriptionStateUnixSocketOnlyRow(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "gnmi.sock")
	s := startStateServerWith(t, func(c *Config) { c.Port = 0; c.UnixSocket = sock; c.GnmiVrf = "mgmt" }, nil)
	defer s.Stop()
	rc := stateDb(t)
	defer rc.Close()
	key := stateServerTable + "|" + substate.ServerGNMI
	if hget(t, rc, key, "listen_address") != "" || hget(t, rc, key, "vrf") != "" || hget(t, rc, key, "unix_socket") != sock {
		t.Errorf("UDS-only row: listen_address %q vrf %q unix_socket %q", hget(t, rc, key, "listen_address"), hget(t, rc, key, "vrf"), hget(t, rc, key, "unix_socket"))
	}
}

func TestSubscriptionStateUnixSocketOnServerRow(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "gnmi.sock")
	s := startStateServerWith(t, func(c *Config) { c.UnixSocket = sock; c.Threshold = 0 }, nil)
	defer s.Stop()
	rc := stateDb(t)
	defer rc.Close()
	key := stateServerTable + "|" + substate.ServerGNMI
	if got := hget(t, rc, key, "unix_socket"); got != sock {
		t.Errorf("unix_socket = %q, want %q", got, sock)
	}
	if got := hget(t, rc, key, "listen_address"); got != s.lis.Addr().String() {
		t.Errorf("listen_address = %q, want the TCP listener", got)
	}
	// --threshold 0 is the server's "no limit"; the row carries it as is.
	if got := hget(t, rc, stateServerTable+"|"+substate.ServerGNMI, "threshold"); got != "0" {
		t.Errorf("threshold = %q on a server with no limit, want 0", got)
	}
}

func TestVrfName(t *testing.T) {
	if vrfName("") != "default" || vrfName("default") != "default" || vrfName("mgmt") != "mgmt" {
		t.Errorf("vrfName: %q %q %q", vrfName(""), vrfName("default"), vrfName("mgmt"))
	}
}

// A connection that carries no Subscribe still shows on the server row,
// which is what makes a connection flood visible.
func TestSubscriptionStateConnections(t *testing.T) {
	s := startStateServer(t, 0, nil)
	defer s.Stop()
	rc := stateDb(t)
	defer rc.Close()
	serverKey := stateServerTable + "|" + substate.ServerGNMI
	conn, client := dialState(t)
	if _, err := client.Capabilities(context.Background(), &pb.CapabilityRequest{}); err != nil {
		t.Fatalf("capabilities: %v", err)
	}
	waitFor(t, "connection counted", 3*time.Second, func() bool { return hgetInt(t, rc, serverKey, "active_connections") == 1 })
	conn.Close()
	waitFor(t, "connection released", 3*time.Second, func() bool { return hgetInt(t, rc, serverKey, "active_connections") == 0 })
}

// Stopping the server stops its publisher and frees its state name.
func TestSubscriptionStateStopStopsPublisher(t *testing.T) {
	s := startStateServer(t, 0, nil)
	fastTick(s)
	rc := stateDb(t)
	defer rc.Close()
	serverKey := stateServerTable + "|" + substate.ServerGNMI
	pub := s.statePub
	s.Stop()
	if stateNameInUse(substate.ServerGNMI) {
		t.Errorf("state name still held after Stop")
	}
	if !pub.pub.Stopped() {
		t.Errorf("publisher loop still running after Stop")
	}
	// The row of a stopped server stays: its frozen last_updated is how a
	// reader tells it is gone.
	if n, _ := rc.Exists(context.Background(), serverKey).Result(); n != 1 {
		t.Fatalf("Stop removed the server row")
	}
}

// ForceStop stops the publisher too, clears the rows it holds, and frees the name.
func TestSubscriptionStateForceStopStopsPublisher(t *testing.T) {
	s := startStateServer(t, 0, nil)
	fastTick(s)
	rc := stateDb(t)
	defer rc.Close()
	serverKey := stateServerTable + "|" + substate.ServerGNMI
	// A row the publisher holds, which ForceStop must clear.
	own := s.statePub.pub.Key(stateSubTable, "999")
	s.statePub.pub.WriteRows("999", map[string]substate.Row{own: {"direction": "dial-in"}},
		nil)
	waitFor(t, "the registered row", 3*time.Second, func() bool { return hget(t, rc, own, "direction") == "dial-in" })
	pub := s.statePub
	s.ForceStop()
	if stateNameInUse(substate.ServerGNMI) || !pub.pub.Stopped() {
		t.Errorf("state name still held or publisher running after ForceStop")
	}
	if n, _ := rc.Exists(context.Background(), own).Result(); n != 0 {
		t.Errorf("ForceStop left a row its publisher holds")
	}
	// The row of a stopped server stays: its frozen last_updated is how a
	// reader tells it is gone.
	if n, _ := rc.Exists(context.Background(), serverKey).Result(); n != 1 {
		t.Errorf("ForceStop removed the server row")
	}
}

// A graceful Stop removes the subscription rows its publisher holds, and
// only those: a numeric row it never wrote, as a server in another process
// would have, and the dial-out client's row stay.
func TestSubscriptionStateStopRemovesOwnRows(t *testing.T) {
	s := startStateServer(t, 0, nil)
	rc := stateDb(t)
	defer rc.Close()
	ctx := context.Background()
	pub := s.statePub.pub
	own, ownPath := pub.Key(stateSubTable, "41"), pub.Key(statePathTable, "41", "0")
	pub.WriteRows("41", map[string]substate.Row{own: {"peer": "live"}, ownPath: {"path": "/a"}},
		nil)
	waitFor(t, "own rows", 3*time.Second, func() bool { return hget(t, rc, ownPath, "path") == "/a" })
	rc.HSet(ctx, stateSubTable+"|999", "peer", "another server's")
	rc.HSet(ctx, stateSubTable+"|dialout_x", "peer", "not ours")
	s.Stop()
	if n, _ := rc.Exists(ctx, own, ownPath).Result(); n != 0 {
		t.Errorf("own rows survived Stop")
	}
	if n, _ := rc.Exists(ctx, stateSubTable+"|999").Result(); n != 1 {
		t.Errorf("Stop deleted a numeric row its publisher never wrote")
	}
	if n, _ := rc.Exists(ctx, stateSubTable+"|dialout_x").Result(); n != 1 {
		t.Errorf("Stop flushed the dial-out client's row")
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
	rc := stateDb(t)
	defer rc.Close()
	if n, _ := rc.Exists(context.Background(), stateServerTable+"|"+substate.ServerGNMI).Result(); n != 0 {
		t.Errorf("a NewServer that failed wrote a server row")
	}
}

// ForceStop waits for a job the publisher's loop is in the middle of, then
// clears and frees the state name, so a new server with the same name can
// be built as soon as it returns.
func TestSubscriptionStateForceStopWaitsForLoop(t *testing.T) {
	s := startStateServer(t, 0, nil)
	release := make(chan struct{})
	running := make(chan struct{})
	s.statePub.pub.Enqueue(func() {
		close(running)
		<-release
	})
	<-running
	stopped := make(chan struct{})
	go func() { s.ForceStop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatalf("ForceStop returned while the publisher was still running a job")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatalf("ForceStop did not return once the job ended")
	}
	if stateNameInUse(substate.ServerGNMI) {
		t.Errorf("ForceStop returned with the state name still taken")
	}
	later := newStateServer(t, stateTestPort+1, nil)
	defer later.Stop()
	if later.statePub == nil {
		t.Errorf("a new server got no publisher after ForceStop")
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
	if hget(t, rc, stateServerTable+"|"+substate.ServerGNMI, "pid") != "" {
		t.Errorf("a server row was written with no publisher")
	}
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
	serverKey := stateServerTable + "|" + substate.ServerGNMI

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
	if row["start_time"] == "" || row["last_updated"] != "" {
		t.Errorf("start_time %q (want set), last_updated %q (want unset until the first tick)", row["start_time"], row["last_updated"])
	}
	id := strings.TrimPrefix(subKey, stateSubTable+"|")
	p0, _ := rc.HGetAll(context.Background(), statePathTable+"|"+id+"|0").Result()
	p1, _ := rc.HGetAll(context.Background(), statePathTable+"|"+id+"|1").Result()
	if p0["path"] != "COUNTERS_DB/COUNTERS/Ethernet68" || p0["mode"] != "SAMPLE" || p0["sample_interval_ns"] != strconv.FormatInt(int64(time.Second), 10) || p0["suppress_redundant"] != "false" {
		t.Errorf("path row 0 = %v", p0)
	}
	if p1["path"] != "COUNTERS_DB/COUNTERS/Ethernet70" || p1["mode"] != "ON_CHANGE" || p1["sample_interval_ns"] != "0" {
		t.Errorf("path row 1 = %v", p1)
	}
	waitFor(t, "server counts after accept", 3*time.Second, func() bool {
		return hgetInt(t, rc, serverKey, "active_subscriptions") == 1 && hgetInt(t, rc, serverKey, "active_connections") >= 1
	})
	// TELEMETRY_CONNECTIONS is written alongside GNMI_SUBSCRIPTION.
	if n, _ := rc.HLen(context.Background(), "TELEMETRY_CONNECTIONS").Result(); n != 1 {
		t.Errorf("TELEMETRY_CONNECTIONS has %d fields while one subscription is open, want 1", n)
	}

	cancel()
	waitFor(t, "row deleted at close", 3*time.Second, func() bool { return len(subscriptionKeys(t, rc)) == 0 })
	if n, _ := rc.Exists(context.Background(), statePathTable+"|"+id+"|0", statePathTable+"|"+id+"|1").Result(); n != 0 {
		t.Errorf("path rows survived the close")
	}
	waitFor(t, "server counts after close", 3*time.Second, func() bool {
		return hgetInt(t, rc, serverKey, "active_subscriptions") == 0
	})
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

func TestSubscriptionStateTickAndInterval(t *testing.T) {
	s := startStateServer(t, 0, nil)
	fastTick(s)
	defer s.Stop()
	rc := stateDb(t)
	defer rc.Close()
	serverKey := stateServerTable + "|" + substate.ServerGNMI
	waitFor(t, "publish_interval 1 on the row", 3*time.Second, func() bool { return hget(t, rc, serverKey, "publish_interval") == "1" })
	conn, client := dialState(t)
	defer conn.Close()
	stream, cancel := openStream(t, client, countersStreamRequest(uint64(time.Second))) // 1 s is the server's minimum sample interval
	defer cancel()
	go func() {
		for {
			if _, err := stream.Recv(); err != nil {
				return
			}
		}
	}()
	var subKey string
	waitFor(t, "row", 3*time.Second, func() bool {
		keys := subscriptionKeys(t, rc)
		if len(keys) == 1 {
			subKey = keys[0]
			return true
		}
		return false
	})
	waitFor(t, "tick-written counters", 6*time.Second, func() bool {
		return hgetInt(t, rc, subKey, "tx_msgs") > 1 && hgetInt(t, rc, subKey, "tx_bytes") > 0 && hget(t, rc, subKey, "last_updated") != ""
	})
	// The request is the one message received; its bytes are counted; no
	// error has happened; the queue depth is published as a number.
	if hgetInt(t, rc, subKey, "rx_msgs") != 1 || hgetInt(t, rc, subKey, "rx_bytes") <= 0 {
		t.Errorf("rx_msgs = %s, rx_bytes = %s; want 1 and > 0", hget(t, rc, subKey, "rx_msgs"), hget(t, rc, subKey, "rx_bytes"))
	}
	if hget(t, rc, subKey, "error_count") != "0" {
		t.Errorf("error_count = %q, want 0", hget(t, rc, subKey, "error_count"))
	}
	if _, err := strconv.Atoi(hget(t, rc, subKey, "queue_depth")); err != nil {
		t.Errorf("queue_depth = %q, want a number", hget(t, rc, subKey, "queue_depth"))
	}
	if hgetInt(t, rc, subKey, "tx_bytes") <= hgetInt(t, rc, subKey, "rx_bytes") {
		t.Errorf("tx_bytes %s should exceed rx_bytes %s on a stream that only sends", hget(t, rc, subKey, "tx_bytes"), hget(t, rc, subKey, "rx_bytes"))
	}
	first := hgetInt(t, rc, serverKey, "last_updated")
	waitFor(t, "server row liveness stamp", 4*time.Second, func() bool { return hgetInt(t, rc, serverKey, "last_updated") > first })

	// A new interval takes effect without a restart.
	s.statePub.pub.SetInterval(3 * time.Second)
	waitFor(t, "interval change", 4*time.Second, func() bool { return hget(t, rc, serverKey, "publish_interval") == "3" })
	if got := s.statePub.pub.Interval(); got != 3*time.Second {
		t.Errorf("interval = %v, want 3s", got)
	}
}

// TestSubscriptionStateRedisUnavailable: with no reachable Redis the publisher
// must neither block nor panic; streams are not its concern.
func TestSubscriptionStateRedisUnavailable(t *testing.T) {
	p := &statePublisher{handler: &substate.ServerStatsHandler{}}
	p.pub = substate.NewWithClient(substate.ServerGNMI, redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 100 * time.Millisecond}), "|")
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.start("10.0.0.1:50051", "", "default", 0)
		// The tick runs on the publisher loop, as the timer's would; queued
		// first, it runs right after startup, ahead of the flood below.
		ticked := make(chan struct{})
		p.pub.Enqueue(func() { p.pub.Tick(); close(ticked) })
		c := NewClient(&stateTestAddr{"10.0.0.9:1"})
		c.state = &subscriptionInfo{peer: "10.0.0.9:1"}
		for i := 0; i < 2000; i++ { // more than the queue holds: must drop, not block
			p.onAccept(c)
			p.onClose(c)
		}
		<-ticked
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

// The stats handler is on the Unix-socket listener too: a connection over
// the socket is counted on the row.
func TestSubscriptionStateUnixSocketConnectionCounted(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "gnmi.sock")
	s := startStateServerWith(t, func(c *Config) { c.Port = 0; c.UnixSocket = sock }, nil)
	defer s.Stop()
	rc := stateDb(t)
	defer rc.Close()
	key := stateServerTable + "|" + substate.ServerGNMI
	conn, err := grpc.NewClient("unix://"+sock, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial %s: %v", sock, err)
	}
	pb.NewGNMIClient(conn).Capabilities(context.Background(), &pb.CapabilityRequest{})
	waitFor(t, "the socket connection counted", 3*time.Second, func() bool { return hgetInt(t, rc, key, "active_connections") == 1 })
	conn.Close()
	waitFor(t, "the socket connection released", 3*time.Second, func() bool { return hgetInt(t, rc, key, "active_connections") == 0 })
}

type stateTestAddr struct{ s string }

func (a *stateTestAddr) Network() string { return "tcp" }

func (a *stateTestAddr) String() string { return a.s }

// subscriptionInfoFor is newSubscriptionInfo with the backend resolved the
// way Run resolves it.
func subscriptionInfoFor(sub *pb.SubscriptionList, origin, target, peer, local string) *subscriptionInfo {
	return newSubscriptionInfo(sub, origin, target, common_utils.ResolveBackend(origin, target, sub.GetMode() == pb.SubscriptionList_STREAM), peer, local, time.Now())
}

func TestNewSubscriptionInfo(t *testing.T) {
	prefix := &pb.Path{Origin: "openconfig", Target: "OC-YANG", Elem: []*pb.PathElem{{Name: "interfaces"}, {Name: "interface", Key: map[string]string{"name": "*"}}}}
	path := &pb.Path{Elem: []*pb.PathElem{{Name: "state"}, {Name: "counters"}}}
	st := subscriptionInfoFor(&pb.SubscriptionList{Prefix: prefix, Mode: pb.SubscriptionList_STREAM, Encoding: pb.Encoding_JSON_IETF,
		Subscription: []*pb.Subscription{{Path: path, Mode: pb.SubscriptionMode_SAMPLE, SampleInterval: 5, HeartbeatInterval: 7, SuppressRedundant: true}}},
		"openconfig", "OC-YANG", "10.0.0.26:39010", "10.0.0.1:50051")
	if st.prefix != "/interfaces/interface[name=*]" || st.listMode != "STREAM" || st.encoding != "JSON_IETF" || st.origin != "openconfig" {
		t.Errorf("subscriptionInfo = %+v", st)
	}
	if len(st.paths) != 1 || st.paths[0].path != "/interfaces/interface[name=*]/state/counters" || st.paths[0].mode != "SAMPLE" || st.paths[0].sampleNs != 5 || st.paths[0].heartbeatNs != 7 || !st.paths[0].suppress {
		t.Errorf("paths = %+v", st.paths)
	}
	// The YANG tree addressed by target alone renders the same bare path.
	st = subscriptionInfoFor(&pb.SubscriptionList{Prefix: &pb.Path{Target: "OC_YANG", Elem: prefix.Elem}, Mode: pb.SubscriptionList_STREAM,
		Subscription: []*pb.Subscription{{Path: path, Mode: pb.SubscriptionMode_ON_CHANGE}}}, "", "OC_YANG", "", "")
	if st.paths[0].path != "/interfaces/interface[name=*]/state/counters" {
		t.Errorf("OC_YANG-by-target path = %q", st.paths[0].path)
	}
	// Origin sonic-db renders bare even with a database target on the
	// prefix: the native client ignores the prefix target.
	st = subscriptionInfoFor(&pb.SubscriptionList{Prefix: &pb.Path{Origin: "sonic-db", Target: "APPL_DB"}, Mode: pb.SubscriptionList_STREAM,
		Subscription: []*pb.Subscription{{Path: &pb.Path{Elem: []*pb.PathElem{{Name: "CONFIG_DB"}, {Name: "localhost"}, {Name: "PORT"}}}, Mode: pb.SubscriptionMode_ON_CHANGE}}},
		"sonic-db", "APPL_DB", "", "")
	if st.paths[0].path != "/CONFIG_DB/localhost/PORT" {
		t.Errorf("sonic-db path with a prefix target = %q, want bare", st.paths[0].path)
	}
	// A database target renders first, with the prefix elements, and a root
	// path on a database target renders as TARGET/.
	st = subscriptionInfoFor(&pb.SubscriptionList{Prefix: &pb.Path{Target: "COUNTERS_DB", Elem: []*pb.PathElem{{Name: "COUNTERS"}}}, Mode: pb.SubscriptionList_STREAM,
		Subscription: []*pb.Subscription{{Path: &pb.Path{Elem: []*pb.PathElem{{Name: "Ethernet0"}}}, Mode: pb.SubscriptionMode_SAMPLE}, {Path: &pb.Path{}, Mode: pb.SubscriptionMode_SAMPLE}}},
		"", "COUNTERS_DB", "", "")
	if st.prefix != "/COUNTERS" || st.paths[0].path != "COUNTERS_DB/COUNTERS/Ethernet0" || st.paths[1].path != "COUNTERS_DB/COUNTERS" {
		t.Errorf("database target paths = %+v (prefix %q)", st.paths, st.prefix)
	}
	st = subscriptionInfoFor(&pb.SubscriptionList{Prefix: &pb.Path{Target: "COUNTERS_DB"}, Mode: pb.SubscriptionList_STREAM,
		Subscription: []*pb.Subscription{{Path: &pb.Path{}, Mode: pb.SubscriptionMode_SAMPLE}}}, "", "COUNTERS_DB", "", "")
	if st.paths[0].path != "COUNTERS_DB/" {
		t.Errorf("root of a database target = %q, want COUNTERS_DB/", st.paths[0].path)
	}
	// A POLL list has no per-path mode or interval, whatever the entries say.
	st = subscriptionInfoFor(&pb.SubscriptionList{Prefix: &pb.Path{Target: "APPL_DB"}, Mode: pb.SubscriptionList_POLL,
		Subscription: []*pb.Subscription{{Path: &pb.Path{Elem: []*pb.PathElem{{Name: "PORT_TABLE"}}}, Mode: pb.SubscriptionMode_SAMPLE, SampleInterval: 5}}}, "", "APPL_DB", "", "")
	if st.listMode != "POLL" || st.paths[0].path != "APPL_DB/PORT_TABLE" || st.paths[0].mode != "" || st.paths[0].sampleNs != 0 {
		t.Errorf("POLL subscriptionInfo = %+v paths %+v", st, st.paths)
	}
}

// A POLL subscription's path row carries no mode and no interval.
func TestSubscriptionStatePollPathRow(t *testing.T) {
	s := startStateServer(t, 0, nil)
	defer s.Stop()
	rc := stateDb(t)
	defer rc.Close()
	conn, client := dialState(t)
	defer conn.Close()
	req := countersStreamRequest(uint64(time.Second))
	req.GetSubscribe().Mode = pb.SubscriptionList_POLL
	_, cancel := openStream(t, client, req)
	defer cancel()
	var subKey string
	waitFor(t, "poll row", 3*time.Second, func() bool {
		keys := subscriptionKeys(t, rc)
		if len(keys) == 1 {
			subKey = keys[0]
			return true
		}
		return false
	})
	if hget(t, rc, subKey, "list_mode") != "POLL" {
		t.Errorf("list_mode = %q, want POLL", hget(t, rc, subKey, "list_mode"))
	}
	id := strings.TrimPrefix(subKey, stateSubTable+"|")
	p0, _ := rc.HGetAll(context.Background(), statePathTable+"|"+id+"|0").Result()
	if _, has := p0["sample_interval_ns"]; p0["path"] != "COUNTERS_DB/COUNTERS/Ethernet68" || p0["mode"] != "" || has || len(p0) != 2 {
		t.Errorf("POLL path row = %v, want the path and an empty mode only", p0)
	}
}

// A server that does not authenticate writes no user, even when the
// collector presents a client certificate.
func TestSubscriptionStateNoUserWithoutAuth(t *testing.T) {
	s := startStateServer(t, 0, nil)
	defer s.Stop()
	rc := stateDb(t)
	defer rc.Close()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "collector"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	conn, err := grpc.Dial("127.0.0.1:"+strconv.Itoa(stateTestPort), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true, Certificates: []tls.Certificate{cert}})))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_, cancel := openStream(t, pb.NewGNMIClient(conn), countersStreamRequest(uint64(time.Second)))
	defer cancel()
	// The server does not authenticate, so the certificate names no user.
	waitFor(t, "row without a user", 3*time.Second, func() bool {
		keys := subscriptionKeys(t, rc)
		if len(keys) != 1 {
			return false
		}
		row, _ := rc.HGetAll(context.Background(), keys[0]).Result()
		u, ok := row["user"]
		return ok && u == ""
	})
}

// The user is whoever authentication recorded in the request context; a
// certificate the server did not verify for authentication names no one.
func TestSubscriptionUser(t *testing.T) {
	rc, ctx := common_utils.GetContext(context.Background())
	rc.Auth.User = "alice"
	if got := username(ctx); got != "alice" {
		t.Errorf("authenticated user = %q, want alice", got)
	}
	if got := username(context.Background()); got != "" {
		t.Errorf("no auth: user = %q, want empty", got)
	}
	// A certificate the server did not verify for authentication names no one.
	cert := &x509.Certificate{Subject: pkix.Name{CommonName: "collector"}}
	tlsCtx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}}})
	if got := username(tlsCtx); got != "" {
		t.Errorf("unauthenticated certificate: user = %q, want empty", got)
	}
}

// certAuthAfterFailedPassword runs authenticate for a collector that sends
// a wrong password for a real local user and a verified certificate whose
// common name is certname1; cn, when not empty, is put in GNMI_CLIENT_CERT.
func certAuthAfterFailedPassword(t *testing.T, cn string) (context.Context, error) {
	t.Helper()
	if !swsscommon.SonicDBConfigIsInit() {
		swsscommon.SonicDBConfigInitialize()
	}
	configDb := swsscommon.NewDBConnector("CONFIG_DB", uint(0), true)
	defer swsscommon.DeleteDBConnector(configDb)
	table := swsscommon.NewTable(configDb, "GNMI_CLIENT_CERT")
	defer swsscommon.DeleteTable(table)
	configDb.Flushdb()
	t.Cleanup(func() {
		db := swsscommon.NewDBConnector("CONFIG_DB", uint(0), true)
		db.Flushdb()
		swsscommon.DeleteDBConnector(db)
	})
	if cn != "" {
		table.Hset(cn, "role", "gnmi_readwrite")
	}
	ctx, cancel := CreateAuthorizationCtx()
	t.Cleanup(cancel)
	// root exists on any test host, so the password check fills in its
	// user and roles before the password is refused.
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("username", "root", "password", "wrong"))
	cfg := &Config{UserAuth: AuthTypes{"password": true, "cert": true}, ConfigTableName: "GNMI_CLIENT_CERT"}
	return authenticate(cfg, ctx, "gnmi", false)
}

// With certificate auth, user is the verified certificate's common name,
// also after a rejected password attempt that had recorded another name.
func TestSubscriptionStateUserFromCertAuth(t *testing.T) {
	ctx, err := certAuthAfterFailedPassword(t, "certname1")
	if err != nil {
		t.Fatalf("certificate auth after a rejected password: %v", err)
	}
	if got := username(ctx); got != "certname1" {
		t.Errorf("user = %q, want the certificate's certname1", got)
	}
}

// ForceStop removes the server's rows, so a server stopped that way leaves
// nothing behind.
func TestSubscriptionStateForceStopClearsRows(t *testing.T) {
	s := startStateServer(t, 0, nil)
	rc := stateDb(t)
	defer rc.Close()
	conn, client := dialState(t)
	defer conn.Close()
	_, cancel := openStream(t, client, countersStreamRequest(uint64(time.Second)))
	defer cancel()
	waitFor(t, "row", 3*time.Second, func() bool { return len(subscriptionKeys(t, rc)) == 1 })
	s.ForceStop()
	if keys := subscriptionKeys(t, rc); len(keys) != 0 {
		t.Errorf("rows left after ForceStop: %v", keys)
	}
}

// A graceful stop after a stream has gone leaves no row of it behind.
func TestSubscriptionStateStopRemovesStreamRows(t *testing.T) {
	s := startStateServer(t, 0, nil)
	rc := stateDb(t)
	defer rc.Close()
	conn, client := dialState(t)
	defer conn.Close()
	_, cancel := openStream(t, client, countersStreamRequest(uint64(time.Second)))
	waitFor(t, "row", 3*time.Second, func() bool { return len(subscriptionKeys(t, rc)) == 1 })
	cancel()
	s.Stop()
	if keys := subscriptionKeys(t, rc); len(keys) != 0 {
		t.Errorf("rows left after Stop: %v", keys)
	}
	if n, _ := rc.Keys(context.Background(), statePathTable+"|*").Result(); len(n) != 0 {
		t.Errorf("path rows left after Stop: %v", n)
	}
}

// A YANG subscription's path row shows the path as the collector sent it,
// not the module-prefixed form the YANG client rewrites it to.
func TestSubscriptionStateYangPathAsSent(t *testing.T) {
	s := startStateServer(t, 0, nil)
	defer s.Stop()
	rc := stateDb(t)
	defer rc.Close()
	conn, client := dialState(t)
	defer conn.Close()
	req := &pb.SubscribeRequest{Request: &pb.SubscribeRequest_Subscribe{Subscribe: &pb.SubscriptionList{
		Prefix:   &pb.Path{Origin: "openconfig"},
		Mode:     pb.SubscriptionList_STREAM,
		Encoding: pb.Encoding_JSON_IETF,
		Subscription: []*pb.Subscription{{
			Path: &pb.Path{Elem: []*pb.PathElem{
				{Name: "interfaces"}, {Name: "interface", Key: map[string]string{"name": "Ethernet0"}},
				{Name: "state"}, {Name: "counters"}}},
			Mode:           pb.SubscriptionMode_SAMPLE,
			SampleInterval: uint64(30 * time.Second),
		}},
	}}}
	_, cancel := openStream(t, client, req)
	defer cancel()
	var subKey string
	waitFor(t, "yang row", 5*time.Second, func() bool {
		keys := subscriptionKeys(t, rc)
		if len(keys) == 1 {
			subKey = keys[0]
			return true
		}
		return false
	})
	id := strings.TrimPrefix(subKey, stateSubTable+"|")
	p0, _ := rc.HGetAll(context.Background(), statePathTable+"|"+id+"|0").Result()
	if want := "/interfaces/interface[name=Ethernet0]/state/counters"; p0["path"] != want {
		t.Errorf("path = %q, want %q", p0["path"], want)
	}
	if got := hget(t, rc, subKey, "origin"); got != "openconfig" {
		t.Errorf("origin = %q, want openconfig", got)
	}
}

// rx_msgs counts messages received: a STREAM collector that half-closes
// after its SubscriptionList has sent one.
func TestSubscriptionStateRxMsgsAfterHalfClose(t *testing.T) {
	s := startStateServer(t, 0, nil)
	defer s.Stop()
	rc := stateDb(t)
	defer rc.Close()
	conn, client := dialState(t)
	defer conn.Close()
	stream, cancel := openStream(t, client, countersStreamRequest(uint64(time.Second)))
	defer cancel()
	stream.CloseSend()
	var subKey string
	waitFor(t, "row", 3*time.Second, func() bool {
		keys := subscriptionKeys(t, rc)
		if len(keys) == 1 {
			subKey = keys[0]
			return true
		}
		return false
	})
	tick := func() { // on the loop, which owns Tick
		done := make(chan struct{})
		s.statePub.pub.Enqueue(func() { s.statePub.pub.Tick(); close(done) })
		<-done
	}
	tick()
	time.Sleep(300 * time.Millisecond) // the half-close reaches recv
	tick()
	if got := hgetInt(t, rc, subKey, "rx_msgs"); got != 1 {
		t.Errorf("rx_msgs = %d after one message and a half-close, want 1", got)
	}
}

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

// admitted_subscriptions is the count the threshold is checked against: one
// admitted stream at threshold 1, and a refused one does not change it.
func TestSubscriptionStateAdmittedCount(t *testing.T) {
	s := startStateServer(t, 1, nil)
	defer s.Stop()
	rc := stateDb(t)
	defer rc.Close()
	serverKey := stateServerTable + "|" + substate.ServerGNMI
	conn, client := dialState(t)
	defer conn.Close()
	_, cancel := openStream(t, client, countersStreamRequest(uint64(time.Second)))
	defer cancel()
	waitFor(t, "one admitted", 3*time.Second, func() bool { return hgetInt(t, rc, serverKey, "admitted_subscriptions") == 1 })
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	refused, err := client.Subscribe(ctx)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	refused.Send(countersStreamRequest(uint64(time.Second)))
	if _, err := refused.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("second stream at threshold 1: %v, want Unavailable", err)
	}
	if n := s.statePub.admittedCount(); n != 1 {
		t.Errorf("admitted count after a refusal = %d, want 1", n)
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
	first.statePub.pub.WriteRows("999", map[string]substate.Row{own: {"direction": "dial-in"}}, nil)
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

// Run records on the row the user authentication found: a stream
// authenticated by its certificate gets the certificate's common name.
func TestSubscriptionStateRunRecordsCertUser(t *testing.T) {
	s := startStateServer(t, 0, nil)
	defer s.Stop()
	rc := stateDb(t)
	defer rc.Close()
	if !swsscommon.SonicDBConfigIsInit() {
		swsscommon.SonicDBConfigInitialize()
	}
	configDb := swsscommon.NewDBConnector("CONFIG_DB", uint(0), true)
	defer swsscommon.DeleteDBConnector(configDb)
	table := swsscommon.NewTable(configDb, "GNMI_CLIENT_CERT")
	defer swsscommon.DeleteTable(table)
	table.Hset("certname1", "role", "gnmi_readwrite")
	defer table.Hdel("certname1", "role")

	cfg := *s.config
	cfg.UserAuth = AuthTypes{"cert": true}
	cfg.ConfigTableName = "GNMI_CLIENT_CERT"
	ctx, cancel := CreateAuthorizationCtx()
	defer cancel()
	c := NewClient(&stateTestAddr{"10.0.0.9:1"})
	c.statePub = s.statePub
	c.setConnectionManager(cfg.Threshold)
	done := make(chan error, 1)
	go func() {
		done <- c.Run(&recvStream{ctx: ctx, req: countersStreamRequest(uint64(time.Second))}, &cfg)
	}()
	waitFor(t, "a row with the certificate's user", 5*time.Second, func() bool {
		keys := subscriptionKeys(t, rc)
		return len(keys) == 1 && hget(t, rc, keys[0], "user") == "certname1"
	})
	cancel()
	c.Close()
	<-done
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

// Stop waits for the open streams, and the server keeps serving them
// meanwhile, so their rows stay until the last one ends; then they are
// gone.
func TestSubscriptionStateStopKeepsRowsWhileDraining(t *testing.T) {
	s := startStateServer(t, 0, nil)
	rc := stateDb(t)
	defer rc.Close()
	conn, client := dialState(t)
	defer conn.Close()
	_, cancel := openStream(t, client, countersStreamRequest(uint64(time.Second)))
	defer cancel()
	waitFor(t, "subscription row", 3*time.Second, func() bool { return len(subscriptionKeys(t, rc)) == 1 })
	stopped := make(chan struct{})
	go func() { s.Stop(); close(stopped) }()
	time.Sleep(500 * time.Millisecond)
	select {
	case <-stopped:
		t.Fatalf("Stop returned while a stream was still open")
	default:
	}
	if n := len(subscriptionKeys(t, rc)); n != 1 {
		t.Errorf("%d subscription rows while Stop drains an open stream, want 1", n)
	}
	cancel()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop did not return after the stream ended")
	}
	if n := len(subscriptionKeys(t, rc)); n != 0 {
		t.Errorf("%d subscription rows after Stop, want 0", n)
	}
}

// username is what Run records as the subscription's user.
func username(ctx context.Context) string {
	var user string
	common_utils.GetUsername(ctx, &user)
	return user
}
