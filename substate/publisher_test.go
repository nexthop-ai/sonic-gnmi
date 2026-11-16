// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package substate

// Tests against the suite's redis-server. Publishing is asynchronous by
// design, so assertions on STATE_DB poll.

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
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

// startAndWait starts p and waits for its loop to be running.
func startAndWait(t *testing.T, p *Publisher) {
	t.Helper()
	p.Start()
	onLoop(t, p, func() {})
}

func TestPublisherRowsAndDelete(t *testing.T) {
	clearState(t)
	rc, _ := stateDb(t)
	defer rc.Close()
	p := newTestPublisher(t, ServerGNMI)
	startAndWait(t, p)
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
// than blocking the caller.
func TestPublisherNeverBlocks(t *testing.T) {
	p := NewWithClient(ServerGNMI, redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 100 * time.Millisecond}), "|")
	done := make(chan struct{})
	go func() {
		defer close(done)
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
	startAndWait(t, p)
	defer p.Stop()
	p.Start() // ignored: no second loop
	// With one loop, a job queued behind a busy one waits for it.
	parked, gate := make(chan struct{}), make(chan struct{})
	p.Enqueue(func() { close(parked); <-gate })
	<-parked
	ran := make(chan struct{})
	p.Enqueue(func() { close(ran) })
	select {
	case <-ran:
		t.Errorf("a second loop ran a job while the first was busy")
	case <-time.After(200 * time.Millisecond):
	}
	close(gate)
	<-ran
}

func TestPublisherKey(t *testing.T) {
	p := NewWithClient(ServerDialout, nil, "|")
	if got := p.Key(PathTable, "dialout_a", "2"); got != "GNMI_SUBSCRIPTION_PATH|dialout_a|2" {
		t.Errorf("Key = %q", got)
	}
	if p.Server() != ServerDialout {
		t.Errorf("server = %q", p.Server())
	}
}

// Registering an id again with fewer rows deletes the rows the new
// registration dropped.
func TestPublisherReregisterWithFewerRowsDeletesTheRest(t *testing.T) {
	clearState(t)
	rc, _ := stateDb(t)
	defer rc.Close()
	p := newTestPublisher(t, ServerDialout)
	startAndWait(t, p)
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
	startAndWait(t, p)
	defer p.Stop()
	key := p.Key(SubscriptionTable, "dialout_S1")
	rc.HSet(ctx, key, map[string]interface{}{"state": "old", "last_error": "stale"})
	p.WriteRows("dialout_S1", map[string]Row{key: {"state": "connecting"}})
	waitFor(t, "row", 3*time.Second, func() bool { return hget(t, rc, key, "state") == "connecting" })
	if hget(t, rc, key, "last_error") != "" {
		t.Errorf("the registration kept a field of the old hash")
	}
}

// After a stop nothing reads the queue, so Enqueue refuses the job instead
// of filling a buffer nobody drains, and does not count it as a full queue.
func TestPublisherEnqueueAfterStop(t *testing.T) {
	p := NewWithClient(ServerGNMI, redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}), "|")
	p.Start()
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
	p.Start()
	p.WriteRows("1", map[string]Row{p.Key(SubscriptionTable, "1"): {"peer": "a"}})
	p.Stop()
	if p.started.Load() {
		t.Errorf("a publisher with no client started its loop")
	}
}
