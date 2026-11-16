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
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
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

func exists(t *testing.T, rc *redis.Client, keys ...string) int64 {
	t.Helper()
	n, _ := rc.Exists(context.Background(), keys...).Result()
	return n
}

// clearState removes every row the tables hold, so each test starts from
// nothing.
func clearState(t *testing.T) {
	t.Helper()
	rc, sep := stateDb(t)
	defer rc.Close()
	ctx := context.Background()
	for _, pat := range []string{SubscriptionTable + sep + "*", PathTable + sep + "*"} {
		if keys, _ := rc.Keys(ctx, pat).Result(); len(keys) > 0 {
			rc.Del(ctx, keys...)
		}
	}
}

// newTestPublisher is a publisher for producer server on a fresh STATE_DB
// client.
func newTestPublisher(t *testing.T, server string) *Publisher {
	t.Helper()
	rc, sep := stateDb(t)
	return NewWithClient(server, rc, sep)
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

// hold parks the publisher goroutine in a queued function until the
// returned release is called, so a test can line up events behind it.
func hold(t *testing.T, p *Publisher) (release func()) {
	t.Helper()
	parked, gate := make(chan struct{}), make(chan struct{})
	if !p.Enqueue(func() { close(parked); <-gate }) {
		t.Fatal("could not park the publisher loop")
	}
	select {
	case <-parked:
	case <-time.After(5 * time.Second):
		t.Fatal("publisher loop did not park")
	}
	var once sync.Once
	return func() { once.Do(func() { close(gate) }) }
}

// startAndWait starts p and waits for its startup flush to have run.
func startAndWait(t *testing.T, p *Publisher, owns func(string) bool) {
	t.Helper()
	p.Start(owns)
	onLoop(t, p, func() {})
}

func TestPublisherStartFlushesOwnRows(t *testing.T) {
	clearState(t)
	rc, sep := stateDb(t)
	defer rc.Close()
	ctx := context.Background()
	// Leftovers from a previous run: two dial-in ids and one dial-out row.
	rc.HSet(ctx, SubscriptionTable+sep+"7", "peer", "stale")
	rc.HSet(ctx, PathTable+sep+"7"+sep+"0", "path", "stale")
	rc.HSet(ctx, SubscriptionTable+sep+"8", "peer", "stale")
	rc.HSet(ctx, SubscriptionTable+sep+"dialout_x", "peer", "keep")

	p := newTestPublisher(t, ServerGNMI)
	before := time.Now()
	p.Start(func(id string) bool { return id != "dialout_x" })
	startTook := time.Since(before)
	defer p.Stop()
	onLoop(t, p, func() {})

	// Start itself does no I/O; the flush ran on the loop.
	if startTook > 100*time.Millisecond {
		t.Errorf("Start took %v", startTook)
	}
	if n := exists(t, rc, SubscriptionTable+sep+"7", PathTable+sep+"7"+sep+"0", SubscriptionTable+sep+"8"); n != 0 {
		t.Errorf("%d of this server's leftover rows survived the start", n)
	}
	if exists(t, rc, SubscriptionTable+sep+"dialout_x") != 1 {
		t.Errorf("another server's row was flushed")
	}
}

func TestPublisherRowsAndDelete(t *testing.T) {
	clearState(t)
	rc, _ := stateDb(t)
	defer rc.Close()
	p := newTestPublisher(t, ServerGNMI)
	startAndWait(t, p, all)
	defer p.Stop()

	subKey := p.Key(SubscriptionTable, "7")
	pathKey := p.Key(PathTable, "7", "0")
	p.WriteRows("7", map[string]Row{
		subKey:  {"peer": "10.0.0.25:52144", "list_mode": "STREAM"},
		pathKey: {"path": "COUNTERS_DB/COUNTERS/Ethernet*", "mode": "SAMPLE"},
	})
	waitFor(t, "rows written", 3*time.Second, func() bool {
		return hget(t, rc, subKey, "peer") == "10.0.0.25:52144" && hget(t, rc, pathKey, "mode") == "SAMPLE"
	})

	// Delete removes the rows the subscription was registered with.
	p.Delete("7")
	waitFor(t, "rows deleted", 3*time.Second, func() bool { return exists(t, rc, subKey, pathKey) == 0 })
	// Deleting an unknown id is a no-op.
	p.Delete("nope")
}

// With no consumer running, the queue fills and events are dropped rather
// than blocking the caller; with no reachable Redis, the flush returns
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
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("publisher blocked")
	}
	if p.Dropped() < queueSize {
		t.Errorf("dropped %d events, want at least %d once the queue is full", p.Dropped(), queueSize)
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
	p := newTestPublisher(t, ServerGNMI)
	startAndWait(t, p, all)
	defer p.Stop()
	p.Start(func(string) bool { return false }) // ignored: no second loop, no new owner
	if !p.owns("7") {
		t.Errorf("a second Start replaced the owner")
	}
}

// StopAndClear removes the publisher's rows after the loop has stopped, so
// an event that was still queued cannot bring them back.
func TestPublisherStopAndClearRegistered(t *testing.T) {
	clearState(t)
	rc, _ := stateDb(t)
	defer rc.Close()
	p := newTestPublisher(t, ServerGNMI)
	startAndWait(t, p, all)
	subKey := p.Key(SubscriptionTable, "3")
	p.WriteRows("3", map[string]Row{subKey: {"peer": "a"}})
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
	if p.Server() != ServerDialout {
		t.Errorf("server = %q", p.Server())
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
	p := newTestPublisher(t, ServerGNMI)
	startAndWait(t, p, all)
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
	p.WriteRows("4", map[string]Row{p.Key(SubscriptionTable, "4"): {"peer": "b"}}) // queued behind it
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
	p := newTestPublisher(t, ServerDialout)
	startAndWait(t, p, all)
	defer p.Stop()
	sub, path0, path1 := p.Key(SubscriptionTable, "dialout_S1"), p.Key(PathTable, "dialout_S1", "0"), p.Key(PathTable, "dialout_S1", "1")
	p.WriteRows("dialout_S1", map[string]Row{sub: {"state": "connecting"}, path0: {"path": "a"}, path1: {"path": "b"}})
	waitFor(t, "rows", 3*time.Second, func() bool { return exists(t, rc, sub, path0, path1) == 3 })
	p.WriteRows("dialout_S1", map[string]Row{sub: {"state": "connecting"}, path0: {"path": "a"}})
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
	p := newTestPublisher(t, ServerDialout)
	startAndWait(t, p, func(string) bool { return false })
	defer p.Stop()
	key := p.Key(SubscriptionTable, "dialout_S1")
	rc.HSet(ctx, key, map[string]interface{}{"state": "old", "last_error": "stale"})
	p.WriteRows("dialout_S1", map[string]Row{key: {"state": "connecting"}})
	waitFor(t, "row", 3*time.Second, func() bool { return hget(t, rc, key, "state") == "connecting" })
	if hget(t, rc, key, "last_error") != "" {
		t.Errorf("the registration kept a field of the old hash")
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
	p.Start(all)
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
	p := newTestPublisher(t, ServerGNMI)
	startAndWait(t, p, all)
	kept, gone := p.Key(SubscriptionTable, "1"), p.Key(SubscriptionTable, "2")
	p.WriteRows("1", map[string]Row{kept: {"peer": "a"}})
	p.WriteRows("2", map[string]Row{gone: {"peer": "b"}})
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
}

// After a stop nothing reads the queue, so Enqueue refuses the job instead
// of filling a buffer nobody drains, and does not count it as a full queue.
func TestPublisherEnqueueAfterStop(t *testing.T) {
	p := NewWithClient(ServerGNMI, redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}), "|")
	p.Start(all)
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
	p.Start(nil)
	p.WriteRows("1", map[string]Row{p.Key(SubscriptionTable, "1"): {"peer": "a"}})
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
