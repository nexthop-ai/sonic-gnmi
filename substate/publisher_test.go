// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package substate

// Tests against the suite's redis-server. Publishing is asynchronous by
// design, so assertions on STATE_DB poll.

import (
	"context"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sonic-net/sonic-gnmi/internal/redisopts"
	sdcfg "github.com/sonic-net/sonic-gnmi/sonic_db_config"
)

func stateDb(t *testing.T) (*redis.Client, string) {
	t.Helper()
	rc, sep, err := DbClient("STATE_DB")
	if err != nil {
		t.Fatalf("STATE_DB: %v", err)
	}
	if err := rc.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("STATE_DB not reachable: %v", err)
	}
	return rc, sep
}

func waitFor(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
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

func exists(t *testing.T, rc *redis.Client, keys ...string) int64 {
	t.Helper()
	n, _ := rc.Exists(context.Background(), keys...).Result()
	return n
}

// clearState removes every row the tables hold and the interval setting,
// so each test starts from nothing.
func clearState(t *testing.T) {
	t.Helper()
	rc, sep := stateDb(t)
	defer rc.Close()
	ctx := context.Background()
	for _, pat := range []string{ServerTable + sep + "*", SubscriptionTable + sep + "*", PathTable + sep + "*"} {
		if keys, _ := rc.Keys(ctx, pat).Result(); len(keys) > 0 {
			rc.Del(ctx, keys...)
		}
	}
	cdb := configDb(t)
	defer cdb.Close()
	cdb.HDel(ctx, ConfigTable+"|"+ConfigKey, ConfigIntervalField)
}

func configDb(t *testing.T) *redis.Client {
	t.Helper()
	rc, _, err := DbClient("CONFIG_DB")
	if err != nil {
		t.Fatalf("CONFIG_DB: %v", err)
	}
	return rc
}

// newTestPublisher is a publisher for server row server on a fresh
// STATE_DB client, whose counters come from live.
func newTestPublisher(t *testing.T, server string, live *atomic.Int64) *Publisher {
	t.Helper()
	rc, sep := stateDb(t)
	p := NewWithClient(server, rc, sep)
	p.counters = func() map[string]int64 {
		return map[string]int64{"active_subscriptions": live.Load()}
	}
	return p
}

func all(string) bool { return true }

// onLoop runs f on the publisher goroutine and waits for it, so a test can
// inspect loop-owned state without racing the loop. The queue must have
// room.
func onLoop(t *testing.T, p *Publisher, f func()) {
	t.Helper()
	done := make(chan struct{})
	deadline := time.Now().Add(5 * time.Second)
	for !p.Enqueue(func() { f(); close(done) }) {
		if time.Now().After(deadline) {
			t.Fatal("publisher queue stayed full")
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("publisher loop did not run the queued function")
	}
}

// tick runs one Tick on the publisher goroutine, as the timer would.
func tick(t *testing.T, p *Publisher) {
	t.Helper()
	onLoop(t, p, p.Tick)
}

// startAndWait starts p and waits for its startup job to have written the
// server row, which comes after the flush. The interval is read from
// CONFIG_DB on its own goroutine and may not be applied yet.
func startAndWait(t *testing.T, rc *redis.Client, p *Publisher, static Row, owns func(string) bool) {
	t.Helper()
	p.Start(static, p.counters, owns)
	waitFor(t, "server row", 3*time.Second, func() bool { return hget(t, rc, p.ServerKey(), "pid") != "" })
}

func TestPublisherStartFlushesOwnRowsAndWritesServerRow(t *testing.T) {
	clearState(t)
	rc, sep := stateDb(t)
	defer rc.Close()
	ctx := context.Background()
	// Leftovers from a previous run: two dial-in ids and one dial-out row.
	rc.HSet(ctx, SubscriptionTable+sep+"7", "peer", "stale")
	rc.HSet(ctx, PathTable+sep+"7"+sep+"0", "path", "stale")
	rc.HSet(ctx, SubscriptionTable+sep+"8", "peer", "stale")
	rc.HSet(ctx, SubscriptionTable+sep+"dialout_x", "peer", "keep")

	var live atomic.Int64
	live.Store(3)
	p := newTestPublisher(t, ServerGNMI, &live)
	before := time.Now()
	p.Start(Row{"threshold": "100", "listen_address": "10.0.0.1:50051"}, p.counters,
		func(id string) bool { return id != "dialout_x" })
	startTook := time.Since(before)
	defer p.Stop()
	waitFor(t, "server row", 3*time.Second, func() bool { return hget(t, rc, p.ServerKey(), "pid") != "" })

	// Start itself does no I/O; the startup job ran on the loop.
	if startTook > 100*time.Millisecond {
		t.Errorf("Start took %v", startTook)
	}
	if n := exists(t, rc, SubscriptionTable+sep+"7", PathTable+sep+"7"+sep+"0", SubscriptionTable+sep+"8"); n != 0 {
		t.Errorf("%d of this server's leftover rows survived the start", n)
	}
	if exists(t, rc, SubscriptionTable+sep+"dialout_x") != 1 {
		t.Errorf("another server's row was flushed")
	}
	key := p.ServerKey()
	if key != ServerTable+sep+ServerGNMI {
		t.Errorf("server key = %s", key)
	}
	row, _ := rc.HGetAll(ctx, key).Result()
	for _, f := range []string{"pid", "start_time", "last_updated"} {
		if _, ok := row[f]; !ok {
			t.Errorf("server row missing %s", f)
		}
	}
	for f, want := range map[string]string{"threshold": "100", "listen_address": "10.0.0.1:50051", "publish_interval": "10", "active_subscriptions": "3"} {
		if row[f] != want {
			t.Errorf("server row %s = %q, want %q", f, row[f], want)
		}
	}
	if p.Interval() != DefaultPublishInterval {
		t.Errorf("interval = %v, want the default", p.Interval())
	}
}

func TestPublisherRowsTickAndDelete(t *testing.T) {
	clearState(t)
	rc, _ := stateDb(t)
	defer rc.Close()
	var live, tx atomic.Int64
	p := newTestPublisher(t, ServerGNMI, &live)
	startAndWait(t, rc, p, nil, all)
	defer p.Stop()

	subKey := p.Key(SubscriptionTable, "7")
	pathKey := p.Key(PathTable, "7", "0")
	live.Store(1)
	p.WriteRows("7", map[string]Row{
		subKey:  {"peer": "10.0.0.25:52144", "list_mode": "STREAM"},
		pathKey: {"path": "COUNTERS_DB/COUNTERS/Ethernet*", "mode": "SAMPLE"},
	}, func() Row {
		return Row{"tx_msgs": Num(tx.Load())}
	})
	// The server row is rewritten by a queued write after the rows, so the
	// wait covers it too.
	waitFor(t, "rows and server row written", 3*time.Second, func() bool {
		return hget(t, rc, subKey, "peer") == "10.0.0.25:52144" && hget(t, rc, pathKey, "mode") == "SAMPLE" &&
			hget(t, rc, p.ServerKey(), "active_subscriptions") == "1"
	})
	if hget(t, rc, subKey, "last_updated") != "" {
		t.Errorf("last_updated set before the first tick")
	}

	// A tick writes the snapshot and the stamps.
	tx.Store(42)
	stamp := hgetInt(t, rc, p.ServerKey(), "last_updated")
	time.Sleep(1100 * time.Millisecond) // the stamps have one-second resolution
	tick(t, p)
	if got := hget(t, rc, subKey, "tx_msgs"); got != "42" {
		t.Errorf("tx_msgs after tick = %q, want 42", got)
	}
	if hget(t, rc, subKey, "last_updated") == "" || hgetInt(t, rc, p.ServerKey(), "last_updated") <= stamp {
		t.Errorf("tick did not advance the stamps")
	}

	// UpdateServer makes a counter change visible without a tick.
	live.Store(5)
	p.UpdateServer()
	waitFor(t, "server row updated", 3*time.Second, func() bool { return hget(t, rc, p.ServerKey(), "active_subscriptions") == "5" })

	// Delete removes the rows the subscription was registered with and
	// unregisters it: a later tick must not bring the rows back.
	live.Store(0)
	p.Delete("7")
	waitFor(t, "rows deleted", 3*time.Second, func() bool {
		return exists(t, rc, subKey, pathKey) == 0 && hget(t, rc, p.ServerKey(), "active_subscriptions") == "0"
	})
	tick(t, p)
	if exists(t, rc, subKey) != 0 {
		t.Errorf("a tick recreated a deleted row")
	}
	// Deleting an unknown id is a no-op.
	p.Delete("nope")
}

// A nil snapshot, or one that returns nil, must not take the loop down.
func TestPublisherNilSnapshot(t *testing.T) {
	clearState(t)
	rc, _ := stateDb(t)
	defer rc.Close()
	var live atomic.Int64
	p := newTestPublisher(t, ServerGNMI, &live)
	startAndWait(t, rc, p, nil, all)
	defer p.Stop()
	p.WriteRows("8", map[string]Row{p.Key(SubscriptionTable, "8"): {"peer": "a"}}, nil)
	p.WriteRows("9", map[string]Row{p.Key(SubscriptionTable, "9"): {"peer": "b"}}, nil)
	p.WriteRows("10", map[string]Row{p.Key(SubscriptionTable, "10"): {"peer": "c"}}, func() Row { return nil })
	waitFor(t, "rows", 3*time.Second, func() bool {
		return exists(t, rc, p.Key(SubscriptionTable, "8"), p.Key(SubscriptionTable, "9"), p.Key(SubscriptionTable, "10")) == 3
	})
	tick(t, p)
	if hget(t, rc, p.Key(SubscriptionTable, "10"), "peer") != "c" {
		t.Errorf("a row whose snapshot returns nil was damaged by the tick")
	}
	if hget(t, rc, p.Key(SubscriptionTable, "8"), "peer") != "a" || hget(t, rc, p.Key(SubscriptionTable, "9"), "peer") != "b" {
		t.Errorf("rows damaged by a tick with nil snapshots")
	}
}

func TestPublisherSetIntervalClamps(t *testing.T) {
	p := NewWithClient(ServerGNMI, redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}), "|")
	for _, c := range []struct{ in, want time.Duration }{
		{500 * time.Millisecond, MinPublishInterval},
		{0, DefaultPublishInterval},
		{-time.Second, DefaultPublishInterval},
		{7 * time.Second, 7 * time.Second},
	} {
		p.SetInterval(c.in)
		if p.Interval() != c.want {
			t.Errorf("SetInterval(%v) = %v, want %v", c.in, p.Interval(), c.want)
		}
	}
}

// UpdateServer queues at most one server-row write at a time: the write
// reads the counts when it runs, so later calls are covered by it.
func TestPublisherUpdateServerCoalesces(t *testing.T) {
	p := NewWithClient(ServerGNMI, redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}), "|")
	defer p.Stop()
	for i := 0; i < 100; i++ {
		p.UpdateServer()
	}
	if n := len(p.events); n != 1 {
		t.Errorf("%d server-row writes queued, want 1", n)
	}
}

// With no consumer running, the queue fills and events are dropped rather
// than blocking the caller; with no reachable Redis, flush and Tick return
// instead of hanging.
func TestPublisherNeverBlocks(t *testing.T) {
	p := NewWithClient(ServerGNMI, redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 100 * time.Millisecond}), "|")
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.flush(all, nil)
		for i := 0; i < 2*queueSize; i++ {
			p.Enqueue(func() {})
		}
		p.UpdateServer()
		p.Tick()
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("publisher blocked")
	}
	if p.Dropped() < queueSize {
		t.Errorf("dropped %d events, want at least %d once the queue is full", p.Dropped(), queueSize)
	}
	// The refused server-row write is not left marked queued, or no later
	// UpdateServer would queue one.
	if p.serverDirty.Load() {
		t.Errorf("a server-row write refused by a full queue left serverDirty set")
	}
}

func TestPublisherStopBeforeStartAndTwice(t *testing.T) {
	p := NewWithClient(ServerGNMI, redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}), "|")
	done := make(chan struct{})
	if p.Stopped() {
		t.Fatalf("Stopped before Stop")
	}
	go func() { p.Stop(); p.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Stop before Start, or a second Stop, blocked")
	}
	if !p.Stopped() {
		t.Errorf("Stopped false after Stop")
	}
}

func TestPublisherStartTwice(t *testing.T) {
	clearState(t)
	rc, _ := stateDb(t)
	defer rc.Close()
	var live atomic.Int64
	p := newTestPublisher(t, ServerGNMI, &live)
	startAndWait(t, rc, p, Row{"threshold": "1"}, all)
	defer p.Stop()
	first := p.serverFixed["start_time"]
	p.Start(Row{"threshold": "2"}, p.counters, all) // ignored: no second loop, no new static map
	if p.serverFixed["threshold"] != "1" || p.serverFixed["start_time"] != first {
		t.Errorf("a second Start changed the static fields: %v", p.serverFixed)
	}
}

// StopAndClear removes the publisher's rows after the loop has stopped, so
// an event that was still queued cannot bring them back.
func TestPublisherStopAndClearRegistered(t *testing.T) {
	clearState(t)
	rc, _ := stateDb(t)
	defer rc.Close()
	var live atomic.Int64
	p := newTestPublisher(t, ServerGNMI, &live)
	startAndWait(t, rc, p, nil, all)
	subKey := p.Key(SubscriptionTable, "3")
	p.WriteRows("3", map[string]Row{subKey: {"peer": "a"}}, func() Row { return Row{} })
	waitFor(t, "row", 3*time.Second, func() bool { return exists(t, rc, subKey) == 1 })
	p.StopAndClear()
	if exists(t, rc, subKey) != 0 {
		t.Errorf("row survived StopAndClear")
	}
	if !p.Stopped() {
		t.Errorf("not stopped")
	}
	p.Stop() // a second stop is a no-op
}

func TestPublisherKey(t *testing.T) {
	p := NewWithClient(ServerDialout, nil, "|")
	if got := p.Key(PathTable, "dialout_a", "2"); got != "GNMI_SUBSCRIPTION_PATH|dialout_a|2" {
		t.Errorf("Key = %q", got)
	}
	if p.ServerKey() != "GNMI_SERVER_STATE|dialout" || p.Server() != ServerDialout {
		t.Errorf("server key = %q, server = %q", p.ServerKey(), p.Server())
	}
	if p.idOf("GNMI_SUBSCRIPTION_PATH|dialout_a|2") != "dialout_a" || p.idOf("GNMI_SUBSCRIPTION|7") != "7" {
		t.Errorf("idOf wrong")
	}
}

// tcpProxy forwards a local TCP port to the test redis-server and can be
// stopped, which is how a test takes Redis away from a publisher.
type tcpProxy struct {
	target string
	addr   string
	mu     sync.Mutex
	ln     net.Listener
	conns  map[net.Conn]struct{}
	delay  time.Duration // added before every reply, to make Redis slow; under mu
}

func newTCPProxy(t *testing.T) *tcpProxy {
	t.Helper()
	ns, _ := sdcfg.GetDbDefaultNamespace()
	target, err := sdcfg.GetDbTcpAddr("STATE_DB", ns)
	if err != nil {
		t.Fatalf("STATE_DB tcp addr: %v", err)
	}
	px := &tcpProxy{target: target, conns: map[net.Conn]struct{}{}}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	px.addr = ln.Addr().String()
	px.serve(ln)
	return px
}

func (px *tcpProxy) serve(ln net.Listener) {
	px.mu.Lock()
	px.ln = ln
	px.mu.Unlock()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			up, err := net.Dial("tcp", px.target)
			if err != nil {
				c.Close()
				continue
			}
			px.mu.Lock()
			px.conns[c] = struct{}{}
			px.conns[up] = struct{}{}
			delay := px.delay
			px.mu.Unlock()
			go func() { io.Copy(up, c); up.Close() }()
			go func() { copyReplies(c, up, delay); c.Close() }()
		}
	}()
}

// copyReplies copies Redis's replies to the client, each after delay.
func copyReplies(dst, src net.Conn, delay time.Duration) {
	if delay == 0 {
		io.Copy(dst, src)
		return
	}
	buf := make([]byte, 64*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			time.Sleep(delay)
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// stop closes the listener and every open connection: Redis is gone.
func (px *tcpProxy) stop() {
	px.mu.Lock()
	defer px.mu.Unlock()
	px.ln.Close()
	for c := range px.conns {
		c.Close()
	}
	px.conns = map[net.Conn]struct{}{}
}

// StopAndClear clears only once the loop has ended, so a write the loop is
// in the middle of when the stop comes lands first and is cleared; one
// still queued is either dropped or written and cleared. The in-flight
// write here starts only after stop is closed and takes a while, so a
// clear that did not wait for the loop would run before it and leave the row.
func TestPublisherStopAndClearAfterQueuedWrite(t *testing.T) {
	clearState(t)
	rc, _ := stateDb(t)
	defer rc.Close()
	var live atomic.Int64
	p := newTestPublisher(t, ServerGNMI, &live)
	startAndWait(t, rc, p, nil, all)
	subKey := p.Key(SubscriptionTable, "3")
	p.mu.Lock() // registered, so the clear owns it; written only by the job below
	p.registered["3"] = &entry{rows: map[string]Row{subKey: {"peer": "a"}}}
	p.mu.Unlock()
	started := make(chan struct{})
	p.Enqueue(func() {
		close(started)
		<-p.stop
		time.Sleep(300 * time.Millisecond)
		p.logErr("in-flight write", p.rc.HSet(context.Background(), subKey, "peer", "a").Err())
	})
	<-started
	p.WriteRows("4", map[string]Row{p.Key(SubscriptionTable, "4"): {"peer": "b"}}, nil) // queued behind it
	p.StopAndClear()
	if n := exists(t, rc, subKey, p.Key(SubscriptionTable, "4")); n != 0 {
		t.Errorf("%d rows written around StopAndClear survived it", n)
	}
}

// Registering an id again with fewer rows deletes the rows the new
// registration dropped.
func TestPublisherReregisterWithFewerRowsDeletesTheRest(t *testing.T) {
	clearState(t)
	rc, _ := stateDb(t)
	defer rc.Close()
	var live atomic.Int64
	p := newTestPublisher(t, ServerDialout, &live)
	startAndWait(t, rc, p, nil, all)
	defer p.Stop()
	sub, path0, path1 := p.Key(SubscriptionTable, "dialout_S1"), p.Key(PathTable, "dialout_S1", "0"), p.Key(PathTable, "dialout_S1", "1")
	p.WriteRows("dialout_S1", map[string]Row{sub: {"state": "connecting"}, path0: {"path": "a"}, path1: {"path": "b"}}, nil)
	waitFor(t, "rows", 3*time.Second, func() bool { return exists(t, rc, sub, path0, path1) == 3 })
	p.WriteRows("dialout_S1", map[string]Row{sub: {"state": "connecting"}, path0: {"path": "a"}}, nil)
	onLoop(t, p, func() {}) // the registration's own write
	if exists(t, rc, path1) != 0 || exists(t, rc, sub, path0) != 2 {
		t.Errorf("after re-registering with one path: path1 %d, sub+path0 %d", exists(t, rc, path1), exists(t, rc, sub, path0))
	}
}

// A registration replaces the hash: fields a key held before, from a
// previous run or a superseded registration, do not survive.
func TestPublisherWritesReplaceTheHash(t *testing.T) {
	clearState(t)
	rc, _ := stateDb(t)
	defer rc.Close()
	ctx := context.Background()
	var live atomic.Int64
	p := newTestPublisher(t, ServerDialout, &live)
	startAndWait(t, rc, p, nil, func(string) bool { return false })
	defer p.Stop()
	key := p.Key(SubscriptionTable, "dialout_S1")
	rc.HSet(ctx, key, map[string]interface{}{"state": "old", "last_error": "stale"})
	p.WriteRows("dialout_S1", map[string]Row{key: {"state": "connecting"}}, nil)
	waitFor(t, "row", 3*time.Second, func() bool { return hget(t, rc, key, "state") == "connecting" })
	if hget(t, rc, key, "last_error") != "" {
		t.Errorf("the registration kept a field of the old hash")
	}
}

// The server row is replaced at startup, so a field an earlier run wrote
// and this one does not carry is gone.
func TestPublisherServerRowReplacedAtStart(t *testing.T) {
	clearState(t)
	rc, sep := stateDb(t)
	defer rc.Close()
	key := ServerTable + sep + ServerGNMI
	rc.HSet(context.Background(), key, "retired_field", "1")
	var live atomic.Int64
	p := newTestPublisher(t, ServerGNMI, &live)
	startAndWait(t, rc, p, nil, all)
	defer p.Stop()
	if hget(t, rc, key, "retired_field") != "" {
		t.Errorf("the server row kept a field from an earlier run")
	}
}

// The tick writes counters only on a subscription row the entry registered,
// so a snapshot-only entry leaves no row that Delete would not remove.
func TestPublisherCountersOnlyOnRegisteredRow(t *testing.T) {
	clearState(t)
	rc, _ := stateDb(t)
	defer rc.Close()
	var live atomic.Int64
	p := newTestPublisher(t, ServerGNMI, &live)
	startAndWait(t, rc, p, nil, all)
	defer p.Stop()
	sub, path := p.Key(SubscriptionTable, "9"), p.Key(PathTable, "9", "0")
	p.WriteRows("9", map[string]Row{path: {"path": "x"}},
		func() Row { return Row{"tx_msgs": "1"} })
	tick(t, p)
	if exists(t, rc, sub) != 0 {
		t.Errorf("the tick wrote a subscription row the entry did not register")
	}
	p.Delete("9")
	onLoop(t, p, func() {})
	if exists(t, rc, sub, path) != 0 {
		t.Errorf("rows left after Delete")
	}
}

// The interval is capped: a huge configured value, which would overflow a
// time.Duration in seconds, becomes the cap and the timer keeps firing.
func TestPublisherIntervalCap(t *testing.T) {
	var live atomic.Int64
	p := newTestPublisher(t, ServerGNMI, &live)
	defer p.Stop()
	p.SetInterval(48 * time.Hour)
	if p.Interval() != MaxPublishInterval {
		t.Errorf("SetInterval(48h) = %v, want the %v cap", p.Interval(), MaxPublishInterval)
	}
}

// A flush on a large STATE_DB takes many SCAN round trips. Each one gets
// its own timeout, so a walk that takes longer than one Redis operation's
// timeout still gets through.
func TestPublisherFlushSlowRedis(t *testing.T) {
	clearState(t)
	rc, sep := stateDb(t)
	defer rc.Close()
	defer clearState(t)
	ctx := context.Background()
	// Enough keys that the walk takes ~30 pages; at 150 ms a reply that is
	// well past the 2 s a single operation may take.
	pipe := rc.Pipeline()
	for i := 0; i < 3000; i++ {
		pipe.HSet(ctx, PathTable+sep+strconv.Itoa(1000+i)+sep+"0", "path", "stale")
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	ns, _ := sdcfg.GetDbDefaultNamespace()
	dbID, _ := sdcfg.GetDbId("STATE_DB", ns)
	px := newTCPProxy(t)
	defer px.stop()
	px.mu.Lock()
	px.delay = 150 * time.Millisecond
	px.mu.Unlock()
	p := NewWithClient(ServerGNMI, redis.NewClient(&redis.Options{Addr: px.addr, DB: dbID, MaxRetries: 0}), sep)
	start := time.Now()
	p.Start(nil, p.counters, all)
	defer p.Stop()
	waitFor(t, "leftovers flushed", flushTimeout, func() bool {
		keys, _ := rc.Keys(ctx, PathTable+sep+"*").Result()
		return len(keys) == 0
	})
	if took := time.Since(start); took < redisOpTimeout {
		t.Logf("flush took %v; the test needs it to take longer than %v to mean anything", took, redisOpTimeout)
	}
}

// StopAndClear deletes the rows this publisher holds, a registered
// subscription's and those of a delete still queued when the loop stopped,
// and leaves a row it never wrote.
func TestPublisherStopAndClear(t *testing.T) {
	clearState(t)
	rc, _ := stateDb(t)
	defer rc.Close()
	var live atomic.Int64
	p := newTestPublisher(t, ServerGNMI, &live)
	startAndWait(t, rc, p, nil, all)
	snap := func() Row { return Row{} }
	kept, gone := p.Key(SubscriptionTable, "1"), p.Key(SubscriptionTable, "2")
	p.WriteRows("1", map[string]Row{kept: {"peer": "a"}}, snap)
	p.WriteRows("2", map[string]Row{gone: {"peer": "b"}}, snap)
	waitFor(t, "rows", 3*time.Second, func() bool { return exists(t, rc, kept) == 1 && exists(t, rc, gone) == 1 })
	foreign := p.Key(SubscriptionTable, "9")
	rc.HSet(context.Background(), foreign, "peer", "another producer's")

	// Hold the loop so the delete of 2 is still queued when it stops.
	release, running := make(chan struct{}), make(chan struct{})
	p.Enqueue(func() { close(running); <-release })
	<-running
	p.Delete("2")
	stopped := make(chan struct{})
	go func() { p.StopAndClear(); close(stopped) }()
	waitFor(t, "the stop", time.Second, p.Stopped)
	close(release)
	<-stopped
	if exists(t, rc, kept) != 0 || exists(t, rc, gone) != 0 {
		t.Errorf("rows the publisher held survived StopAndClear")
	}
	if exists(t, rc, foreign) != 1 {
		t.Errorf("StopAndClear deleted a row the publisher never wrote")
	}
	if exists(t, rc, p.ServerKey()) != 1 {
		t.Errorf("StopAndClear deleted the server row")
	}
}

// After a stop nothing reads the queue, so Enqueue refuses the job instead
// of filling a buffer nobody drains, and does not count it as a full queue.
func TestPublisherEnqueueAfterStop(t *testing.T) {
	p := NewWithClient(ServerGNMI, redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}), "|")
	p.Start(nil, nil, all)
	p.Stop()
	for i := 0; i < 2*queueSize; i++ {
		if p.Enqueue(func() {}) {
			t.Fatalf("job %d queued after Stop", i)
		}
	}
	if n := p.Dropped(); n != 0 {
		t.Errorf("jobs refused after Stop counted as a full queue: %d", n)
	}
}

// DbClient builds its clients with GNMI_REDIS_POOL_SIZE applied.
func TestDbClientPoolSize(t *testing.T) {
	t.Setenv(redisopts.PoolSizeEnvVar, "7")
	rc, _, err := DbClient("STATE_DB")
	if err != nil {
		t.Fatalf("STATE_DB: %v", err)
	}
	defer rc.Close()
	if got := rc.Options().PoolSize; got != 7 {
		t.Errorf("PoolSize = %d, want 7 from %s", got, redisopts.PoolSizeEnvVar)
	}
}

// Each command gives up at its context's deadline, not go-redis's longer
// read timeout: a Redis that accepts and never answers holds a write for
// redisOpTimeout at most.
func TestDbClientHonoursContextDeadline(t *testing.T) {
	rc, _, err := DbClient("STATE_DB")
	if err != nil {
		t.Fatalf("STATE_DB: %v", err)
	}
	opts := *rc.Options()
	rc.Close()
	if !opts.ContextTimeoutEnabled {
		t.Fatalf("DbClient does not enable ContextTimeoutEnabled")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			defer c.Close() // accepted, never answered
		}
	}()
	opts.Network, opts.Addr, opts.Dialer, opts.MaxRetries = "tcp", ln.Addr().String(), nil, 0
	silent := redis.NewClient(&opts)
	defer silent.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	silent.HSet(ctx, "k", "f", "v")
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("a write to a silent Redis took %v, want about the 300ms deadline", took)
	}
}

// A publisher with no Redis client does not start its loop, so nothing
// dereferences the missing client.
func TestPublisherNilClientDoesNotStart(t *testing.T) {
	p := NewWithClient(ServerGNMI, nil, "|")
	p.Start(nil, nil, nil)
	p.WriteRows("1", map[string]Row{p.Key(SubscriptionTable, "1"): {"peer": "a"}}, nil)
	p.Stop()
	if p.started.Load() {
		t.Errorf("a publisher with no client started its loop")
	}
}

// A flush whose stop is already closed gives up before deleting anything.
func TestPublisherFlushGivesUpOnStop(t *testing.T) {
	clearState(t)
	rc, _ := stateDb(t)
	defer rc.Close()
	p := NewWithClient(ServerGNMI, rc, "|")
	key := p.Key(SubscriptionTable, "5")
	rc.HSet(context.Background(), key, "peer", "x")
	defer rc.Del(context.Background(), key)
	stop := make(chan struct{})
	close(stop)
	if p.flush(all, stop) {
		t.Errorf("a stopped flush reported it got through")
	}
	if exists(t, rc, key) != 1 {
		t.Errorf("a stopped flush deleted a row")
	}
}

// With no ownership predicate there is nothing to flush: it reports done
// without touching Redis, here one that refuses connections.
func TestPublisherFlushWithoutOwnsScansNothing(t *testing.T) {
	p := NewWithClient(ServerDialout, redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: 0}), "|")
	if !p.flush(nil, p.stop) {
		t.Errorf("a flush with nothing to own failed")
	}
	if p.errs.Load() != 0 {
		t.Errorf("a flush with nothing to own went to Redis")
	}
}

// idlePublisher is a publisher whose loop never runs, so a queued write or
// delete stays queued: it is a job that never landed. The test calls Tick
// directly.
func idlePublisher(t *testing.T) (*Publisher, *redis.Client) {
	t.Helper()
	clearState(t)
	rc, sep := stateDb(t)
	t.Cleanup(func() { rc.Close() })
	prc, _ := stateDb(t)
	t.Cleanup(func() { prc.Close() })
	return NewWithClient(ServerGNMI, prc, sep), rc
}

func hgetAll(t *testing.T, rc *redis.Client, key string) map[string]string {
	t.Helper()
	m, err := rc.HGetAll(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("hgetall %s: %v", key, err)
	}
	return m
}

// A registration whose write never landed is written by the tick, counters
// included, and a row changed or removed from outside is put back as it
// should stand.
func TestPublisherTickWritesRowsInFull(t *testing.T) {
	p, rc := idlePublisher(t)
	ctx := context.Background()
	sub, path := p.Key(SubscriptionTable, "7"), p.Key(PathTable, "7", "0")
	p.WriteRows("7", map[string]Row{sub: {"peer": "a"}, path: {"path": "x"}},
		func() Row { return Row{"tx_msgs": "3"} })
	p.Tick()
	if got := hgetAll(t, rc, sub); got["peer"] != "a" || got["tx_msgs"] != "3" || got["last_updated"] == "" {
		t.Fatalf("subscription row after tick = %v", got)
	}
	if hget(t, rc, path, "path") != "x" {
		t.Fatalf("path row not written by the tick")
	}
	rc.HSet(ctx, sub, "stray", "1")
	rc.Del(ctx, path)
	p.Tick()
	if hget(t, rc, sub, "stray") != "" {
		t.Errorf("a field the row does not carry survived the tick")
	}
	if hget(t, rc, path, "path") != "x" {
		t.Errorf("a path row removed from outside was not written again")
	}
}

// A delete that never landed is done by the tick, and its mark cleared.
func TestPublisherTickRetriesDelete(t *testing.T) {
	p, rc := idlePublisher(t)
	sub := p.Key(SubscriptionTable, "7")
	p.WriteRows("7", map[string]Row{sub: {"peer": "a"}}, nil)
	p.Tick()
	p.Delete("7")
	if exists(t, rc, sub) != 1 {
		t.Fatalf("the row went before any delete ran")
	}
	p.Tick()
	if exists(t, rc, sub) != 0 {
		t.Errorf("the tick did not delete the row of a delete that never landed")
	}
	if len(p.deleting) != 0 {
		t.Errorf("marks left after the tick's delete: %v", p.deleting)
	}
}

// A key registered again after a delete that never landed is written by the
// tick, not deleted.
func TestPublisherTickSparesReregisteredKey(t *testing.T) {
	p, rc := idlePublisher(t)
	sub := p.Key(SubscriptionTable, "7")
	p.WriteRows("7", map[string]Row{sub: {"peer": "a"}}, nil)
	p.Delete("7")
	p.WriteRows("7", map[string]Row{sub: {"peer": "b"}}, nil)
	p.Tick()
	if got := hget(t, rc, sub, "peer"); got != "b" {
		t.Errorf("peer after tick = %q, want b", got)
	}
}

// A key a registration dropped is deleted by the tick if the write that
// would have deleted it never landed.
func TestPublisherTickDeletesDroppedKey(t *testing.T) {
	p, rc := idlePublisher(t)
	sub, path := p.Key(SubscriptionTable, "7"), p.Key(PathTable, "7", "1")
	p.WriteRows("7", map[string]Row{sub: {"peer": "a"}, path: {"path": "x"}}, nil)
	p.Tick()
	p.WriteRows("7", map[string]Row{sub: {"peer": "a"}}, nil)
	p.Tick()
	if exists(t, rc, path) != 0 {
		t.Errorf("a key the new registration dropped is still there")
	}
	if exists(t, rc, sub) != 1 {
		t.Errorf("the kept row is gone")
	}
}

// A delete clears only its own marks: once a key belongs to a newer delete,
// an older one landing late leaves the mark for the newer one to clear.
func TestPublisherUnmarkKeepsNewerMark(t *testing.T) {
	p, _ := idlePublisher(t)
	sub := p.Key(SubscriptionTable, "7")
	p.WriteRows("7", map[string]Row{sub: {"peer": "a"}}, nil)
	p.Delete("7")
	older := p.deleting[sub]
	p.WriteRows("7", map[string]Row{sub: {"peer": "b"}}, nil)
	p.Delete("7")
	p.unmark([]string{sub}, older)
	if p.deleting[sub] == nil {
		t.Errorf("an older delete cleared the newer delete's mark")
	}
}

// The tick replaces the server row, so a field an earlier run wrote does
// not survive a startup write that failed.
func TestPublisherTickReplacesServerRow(t *testing.T) {
	p, rc := idlePublisher(t)
	rc.HSet(context.Background(), p.ServerKey(), "old_field", "1")
	p.Tick()
	if hget(t, rc, p.ServerKey(), "old_field") != "" {
		t.Errorf("an old server row field survived the tick")
	}
	if hget(t, rc, p.ServerKey(), "last_updated") == "" {
		t.Errorf("the tick did not write the server row")
	}
}

// An empty Row is not registered: an HSET with no fields would fail the
// whole tick.
func TestPublisherWriteRowsSkipsEmptyRow(t *testing.T) {
	p, rc := idlePublisher(t)
	sub, path := p.Key(SubscriptionTable, "7"), p.Key(PathTable, "7", "0")
	p.WriteRows("7", map[string]Row{sub: {"peer": "a"}, path: {}}, nil)
	p.Tick()
	if p.errs.Load() != 0 {
		t.Errorf("the tick failed with an empty row registered")
	}
	if hget(t, rc, sub, "peer") != "a" {
		t.Errorf("the subscription row was not written")
	}
}

// The interval comes from CONFIG_DB at start and follows its changes.
func TestPublisherIntervalFromConfig(t *testing.T) {
	clearState(t)
	rc, _ := stateDb(t)
	defer rc.Close()
	cdb := configDb(t)
	defer cdb.Close()
	ctx := context.Background()
	cfg := ConfigTable + "|" + ConfigKey
	defer cdb.HDel(ctx, cfg, ConfigIntervalField)

	cdb.HSet(ctx, cfg, ConfigIntervalField, "1")
	var live atomic.Int64
	p := newTestPublisher(t, ServerGNMI, &live)
	p.configEvery = 100 * time.Millisecond
	startAndWait(t, rc, p, nil, all)
	defer p.Stop()
	waitFor(t, "interval 1 s from CONFIG_DB", 3*time.Second, func() bool {
		return p.Interval() == time.Second && hget(t, rc, p.ServerKey(), "publish_interval") == "1"
	})
	// The timer runs at that interval: the stamp advances within about one.
	first := hgetInt(t, rc, p.ServerKey(), "last_updated")
	waitFor(t, "tick at 1 s", 4*time.Second, func() bool { return hgetInt(t, rc, p.ServerKey(), "last_updated") > first })

	cdb.HSet(ctx, cfg, ConfigIntervalField, "3")
	waitFor(t, "interval on the row", 3*time.Second, func() bool { return hget(t, rc, p.ServerKey(), "publish_interval") == "3" })
	cdb.HSet(ctx, cfg, ConfigIntervalField, "soon")
	waitFor(t, "bad value means the default", 3*time.Second, func() bool { return p.Interval() == DefaultPublishInterval })
	cdb.HSet(ctx, cfg, ConfigIntervalField, "2")
	waitFor(t, "interval 2", 3*time.Second, func() bool { return p.Interval() == 2*time.Second })
	cdb.HDel(ctx, cfg, ConfigIntervalField)
	waitFor(t, "default after hdel", 3*time.Second, func() bool { return p.Interval() == DefaultPublishInterval })
	cdb.HSet(ctx, cfg, ConfigIntervalField, "2")
	waitFor(t, "interval 2 again", 3*time.Second, func() bool { return p.Interval() == 2*time.Second })
	cdb.Del(ctx, cfg)
	waitFor(t, "default after del", 3*time.Second, func() bool { return p.Interval() == DefaultPublishInterval })
}

func TestParseInterval(t *testing.T) {
	for _, tt := range []struct {
		in   string
		want time.Duration
		bad  bool
	}{
		{"30", 30 * time.Second, false},
		{" 5 ", 5 * time.Second, false},
		{"0", 0, false},
		{"-3", 0, false},
		{"soon", 0, true},
		{"", 0, true},
		{"99999999999", MaxPublishInterval, true},
	} {
		got, bad := parseInterval(tt.in)
		if got != tt.want || (bad != "") != tt.bad {
			t.Errorf("parseInterval(%q) = %v, %q; want %v, bad=%v", tt.in, got, bad, tt.want, tt.bad)
		}
	}
}

// A Redis error on the read keeps the interval in force.
func TestPublisherConfigReadErrorKeepsInterval(t *testing.T) {
	p := NewWithClient(ServerGNMI, nil, "|")
	p.SetInterval(3 * time.Second)
	dead := newTCPProxy(t)
	dead.stop()
	rc := redis.NewClient(&redis.Options{Addr: dead.addr, DialTimeout: 300 * time.Millisecond, MaxRetries: 0})
	defer rc.Close()
	if got := p.readConfigInterval(rc, "|", "earlier"); got != "earlier" {
		t.Errorf("warning after a read error = %q, want the earlier one kept", got)
	}
	if p.Interval() != 3*time.Second {
		t.Errorf("interval = %v after a CONFIG_DB error, want the 3s in force", p.Interval())
	}
}

// A bad value is returned as the warning, which the caller passes back as
// warned, so the next read of the same value does not log it again; a good
// value clears it.
func TestPublisherConfigBadValueWarning(t *testing.T) {
	cdb := configDb(t)
	defer cdb.Close()
	ctx := context.Background()
	cfg := ConfigTable + "|" + ConfigKey
	defer cdb.HDel(ctx, cfg, ConfigIntervalField)
	p := NewWithClient(ServerGNMI, nil, "|")

	cdb.HSet(ctx, cfg, ConfigIntervalField, "soon")
	warned := p.readConfigInterval(cdb, "|", "")
	if !strings.Contains(warned, `"soon" is not a number of seconds`) {
		t.Fatalf("warning for a bad value = %q", warned)
	}
	if again := p.readConfigInterval(cdb, "|", warned); again != warned {
		t.Errorf("warning on the next read of the same value = %q, want %q", again, warned)
	}
	cdb.HSet(ctx, cfg, ConfigIntervalField, "5")
	if got := p.readConfigInterval(cdb, "|", warned); got != "" {
		t.Errorf("warning for a good value = %q, want none", got)
	}
}

// A CONFIG_DB that accepts connections and never answers does not hold up
// the writes, and delays the stop by at most one read's timeout.
func TestPublisherConfigHangDoesNotBlockLoop(t *testing.T) {
	clearState(t)
	rc, _ := stateDb(t)
	defer rc.Close()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	var held []net.Conn
	var heldMu sync.Mutex
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			heldMu.Lock()
			held = append(held, c) // accepted, never answered
			heldMu.Unlock()
		}
	}()
	defer func() {
		heldMu.Lock()
		for _, c := range held {
			c.Close()
		}
		heldMu.Unlock()
	}()
	var live atomic.Int64
	p := newTestPublisher(t, ServerGNMI, &live)
	p.cfgRC, p.cfgSep = redis.NewClient(&redis.Options{Addr: ln.Addr().String(), MaxRetries: 0}), "|"
	startAndWait(t, rc, p, nil, all)
	key := p.Key(SubscriptionTable, "5")
	for i := 0; i < 4; i++ {
		begin := time.Now()
		p.WriteRows("5", map[string]Row{key: {"n": Num(i)}}, nil)
		tick(t, p)
		if took := time.Since(begin); took > time.Second {
			t.Fatalf("a write and a tick took %v with CONFIG_DB hanging", took)
		}
		if hget(t, rc, key, "n") != strconv.Itoa(i) {
			t.Fatalf("write %d did not land", i)
		}
	}
	begin := time.Now()
	p.Stop()
	if took := time.Since(begin); took > 2*redisOpTimeout {
		t.Errorf("Stop took %v with CONFIG_DB hanging", took)
	}
	select {
	case <-p.cfgDone:
	default:
		t.Errorf("the CONFIG_DB reader is still running after Stop")
	}
}

// An update is written at once, on top of the row, and every later tick
// carries it.
func TestPublisherUpdateRow(t *testing.T) {
	clearState(t)
	rc, _ := stateDb(t)
	defer rc.Close()
	var live atomic.Int64
	p := newTestPublisher(t, ServerDialout, &live)
	startAndWait(t, rc, p, nil, all)
	defer p.Stop()
	key := p.Key(SubscriptionTable, "dialout_S1")
	p.WriteRows("dialout_S1", map[string]Row{key: {"state": "connecting", "peer": "a"}}, nil)
	onLoop(t, p, func() {}) // the registration's write has landed
	p.UpdateRow(key, Row{"state": "connected"})
	onLoop(t, p, func() {})
	if got := hgetAll(t, rc, key); got["state"] != "connected" || got["peer"] != "a" {
		t.Fatalf("row after the update = %v", got)
	}
	tick(t, p)
	if got := hget(t, rc, key, "state"); got != "connected" {
		t.Errorf("state after a tick = %q, want the update kept", got)
	}
}

// An update for a row deleted from STATE_DB out of band does not recreate
// it with only the updated fields; the next tick writes it in full.
func TestPublisherUpdateRowDeletedOutOfBand(t *testing.T) {
	clearState(t)
	rc, _ := stateDb(t)
	defer rc.Close()
	var live atomic.Int64
	p := newTestPublisher(t, ServerDialout, &live)
	startAndWait(t, rc, p, nil, all)
	defer p.Stop()
	key := p.Key(SubscriptionTable, "dialout_S1")
	p.WriteRows("dialout_S1", map[string]Row{key: {"state": "connecting", "peer": "a"}}, nil)
	onLoop(t, p, func() {})
	if err := rc.Del(context.Background(), key).Err(); err != nil {
		t.Fatal(err)
	}
	p.UpdateRow(key, Row{"state": "connected"})
	onLoop(t, p, func() {})
	if got := hgetAll(t, rc, key); len(got) != 0 {
		t.Fatalf("update recreated a deleted row as %v", got)
	}
	tick(t, p)
	if got := hgetAll(t, rc, key); got["state"] != "connected" || got["peer"] != "a" {
		t.Errorf("row after a tick = %v, want it written in full", got)
	}
}

// An update that never landed is written by the next tick with the rest of
// the row.
func TestPublisherUpdateRowCarriedIntoTick(t *testing.T) {
	p, rc := idlePublisher(t)
	key := p.Key(SubscriptionTable, "dialout_S1")
	p.WriteRows("dialout_S1", map[string]Row{key: {"state": "connecting", "peer": "a"}}, nil)
	p.Tick()
	p.UpdateRow(key, Row{"state": "connected"}) // queued, never runs
	if hget(t, rc, key, "state") != "connecting" {
		t.Fatalf("the update landed without the loop")
	}
	p.Tick()
	if got := hgetAll(t, rc, key); got["state"] != "connected" || got["peer"] != "a" {
		t.Errorf("row after the tick = %v, want the update and the rest of the row", got)
	}
}

// An update for a key that is not one of a registered id's rows, or for an
// id that was deleted, is ignored.
func TestPublisherUpdateRowUnregisteredKey(t *testing.T) {
	p, rc := idlePublisher(t)
	sub := p.Key(SubscriptionTable, "dialout_S1")
	p.WriteRows("dialout_S1", map[string]Row{sub: {"state": "connecting"}}, nil)
	other := p.Key(PathTable, "dialout_S1", "7")
	p.UpdateRow(other, Row{"path": "x"})
	// Ticked while dialout_S1 is still registered: a key recorded into its
	// rows would be written here.
	p.Tick()
	if n := exists(t, rc, other); n != 0 {
		t.Errorf("an update for a key the id does not hold was written")
	}
	p.Delete("dialout_S1")
	p.UpdateRow(sub, Row{"state": "connected"})
	p.Tick()
	if n := exists(t, rc, sub, other); n != 0 {
		t.Errorf("%d rows written by updates that should have been ignored", n)
	}
}
