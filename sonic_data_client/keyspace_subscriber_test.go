package client

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/redis/go-redis/v9"
	spb "github.com/sonic-net/sonic-gnmi/proto"
)

// nextWithin returns the reader's next message, or false if none arrives within d.
func nextWithin(r *keyspaceReader, d time.Duration) (*redis.Message, bool) {
	timeout := make(chan struct{})
	timer := time.AfterFunc(d, func() { close(timeout) })
	defer timer.Stop()
	return r.next(timeout)
}

func keyspacePrefix() string {
	return "__keyspace@" + strconv.Itoa(int(spb.Target_value["COUNTERS_DB"])) + "__:"
}

func TestKeyspaceSubscriber(t *testing.T) {
	cleanup := setupTestTarget2RedisDb(t)
	defer cleanup()
	ctx := context.Background()
	rclient := Target2RedisDb[""]["COUNTERS_DB"]
	enableKeyspaceEvents(t, rclient)
	prefix := keyspacePrefix()
	pattern := prefix + "KEYSPACE_SUBSCRIBER_TEST:*"
	key := "KEYSPACE_SUBSCRIBER_TEST:a"
	defer rclient.Del(ctx, key)

	idsBefore := psubscribeClients(t, rclient)
	before := len(idsBefore)
	stop := make(chan struct{})
	defer close(stop)
	s := newKeyspaceSubscriber(stop)

	first, err := s.subscribe(rclient, pattern, time.Second)
	if err != nil {
		t.Fatalf("first subscribe failed: %v", err)
	}
	second, err := s.subscribe(rclient, pattern, time.Second)
	if err != nil {
		t.Fatalf("second subscribe failed: %v", err)
	}
	if got := psubscribeConnections(t, rclient) - before; got != 1 {
		t.Errorf("two readers of one pattern hold %d pattern-subscribed connections, want 1", got)
	}

	rclient.HSet(ctx, key, "f", "1")
	for name, r := range map[string]*keyspaceReader{"first": first, "second": second} {
		msg, ok := nextWithin(r, 2*time.Second)
		if !ok {
			t.Fatalf("%s reader got no message", name)
		}
		if msg.Channel != prefix+key || msg.Payload != "hset" {
			t.Errorf("%s reader got %s %s, want %s hset", name, msg.Channel, msg.Payload, prefix+key)
		}
	}

	s.unsubscribe(rclient, pattern, first)
	rclient.HSet(ctx, key, "f", "2")
	if _, ok := nextWithin(second, 2*time.Second); !ok {
		t.Errorf("the remaining reader got no message after the other reader left")
	}
	if msg, ok := nextWithin(first, 300*time.Millisecond); ok {
		t.Errorf("a reader that left got %s %s", msg.Channel, msg.Payload)
	}

	// A lost connection is replaced, and its patterns subscribed again.
	known := map[string]bool{}
	for _, id := range idsBefore {
		known[id] = true
	}
	for _, id := range psubscribeClients(t, rclient) {
		if !known[id] {
			if err := rclient.ClientKillByFilter(ctx, "ID", id).Err(); err != nil {
				t.Fatalf("CLIENT KILL ID %s failed: %v", id, err)
			}
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for i := 3; ; i++ {
		rclient.HSet(ctx, key, "f", strconv.Itoa(i))
		if _, ok := nextWithin(second, 200*time.Millisecond); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the reader got no message within 5s of its connection being killed")
		}
	}

	s.unsubscribe(rclient, pattern, second)
	deadline = time.Now().Add(2 * time.Second)
	for psubscribeConnections(t, rclient) != before {
		if time.Now().After(deadline) {
			t.Fatalf("the pattern is still subscribed after its last reader left")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestKeyspaceSubscriberPatternsShareConnection(t *testing.T) {
	cleanup := setupTestTarget2RedisDb(t)
	defer cleanup()
	ctx := context.Background()
	rclient := Target2RedisDb[""]["COUNTERS_DB"]
	enableKeyspaceEvents(t, rclient)
	prefix := keyspacePrefix()
	keysA := []string{"KEYSPACE_SUBSCRIBER_TEST_A:1", "KEYSPACE_SUBSCRIBER_TEST_A:2", "KEYSPACE_SUBSCRIBER_TEST_A:3"}
	keyB := "KEYSPACE_SUBSCRIBER_TEST_B:1"
	defer rclient.Del(ctx, append(keysA, keyB)...)

	before := psubscribeConnections(t, rclient)
	stop := make(chan struct{})
	defer close(stop)
	s := newKeyspaceSubscriber(stop)
	a, err := s.subscribe(rclient, prefix+"KEYSPACE_SUBSCRIBER_TEST_A:*", time.Second)
	if err != nil {
		t.Fatalf("subscribe to A failed: %v", err)
	}
	b, err := s.subscribe(rclient, prefix+"KEYSPACE_SUBSCRIBER_TEST_B:*", time.Second)
	if err != nil {
		t.Fatalf("subscribe to B failed: %v", err)
	}
	if got := psubscribeConnections(t, rclient) - before; got != 1 {
		t.Errorf("two patterns hold %d pattern-subscribed connections, want 1", got)
	}

	// A's reader is not drained until B's has its message.
	for _, key := range keysA {
		rclient.HSet(ctx, key, "f", "1")
	}
	rclient.HSet(ctx, keyB, "f", "1")
	msg, ok := nextWithin(b, 2*time.Second)
	if !ok {
		t.Fatalf("B's reader got no message while A's reader was not read")
	}
	if msg.Channel != prefix+keyB {
		t.Errorf("B's reader got %s, want %s", msg.Channel, prefix+keyB)
	}
	if msg, ok := nextWithin(b, 300*time.Millisecond); ok {
		t.Errorf("B's reader got %s, which is not on its pattern", msg.Channel)
	}
	for _, key := range keysA {
		msg, ok := nextWithin(a, 2*time.Second)
		if !ok {
			t.Fatalf("A's reader is missing the message for %s", key)
		}
		if msg.Channel != prefix+key {
			t.Errorf("A's reader got %s, want %s", msg.Channel, prefix+key)
		}
	}
}

func TestKeyspaceSubscriberPSubscribeError(t *testing.T) {
	cleanup := setupTestTarget2RedisDb(t)
	defer cleanup()
	ctx := context.Background()
	rclient := Target2RedisDb[""]["COUNTERS_DB"]
	enableKeyspaceEvents(t, rclient)
	prefix := keyspacePrefix()
	pattern := prefix + "KEYSPACE_SUBSCRIBER_TEST:*"
	key := "KEYSPACE_SUBSCRIBER_TEST:a"
	defer rclient.Del(ctx, key)

	stop := make(chan struct{})
	defer close(stop)
	s := newKeyspaceSubscriber(stop)

	patches := gomonkey.ApplyMethod(reflect.TypeOf(&redis.PubSub{}), "PSubscribe", func(_ *redis.PubSub, _ context.Context, _ ...string) error {
		return errors.New("psubscribe refused")
	})
	_, err := s.subscribe(rclient, pattern, time.Second)
	patches.Reset()
	if err == nil {
		t.Fatalf("subscribe returned no error when PSUBSCRIBE failed")
	}

	r, err := s.subscribe(rclient, pattern, time.Second)
	if err != nil {
		t.Fatalf("subscribe after a failed one returned %v", err)
	}
	rclient.HSet(ctx, key, "f", "1")
	if _, ok := nextWithin(r, 2*time.Second); !ok {
		t.Errorf("the reader got no message after an earlier subscribe of its pattern failed")
	}
}

func TestKeyspaceSubscriberConfirmationTimeout(t *testing.T) {
	cleanup := setupTestTarget2RedisDb(t)
	defer cleanup()
	ctx := context.Background()
	rclient := Target2RedisDb[""]["COUNTERS_DB"]
	enableKeyspaceEvents(t, rclient)
	prefix := keyspacePrefix()
	pattern := prefix + "KEYSPACE_SUBSCRIBER_TEST:*"
	key := "KEYSPACE_SUBSCRIBER_TEST:a"
	defer rclient.Del(ctx, key)

	stop := make(chan struct{})
	defer close(stop)
	s := newKeyspaceSubscriber(stop)

	// PSUBSCRIBE is never sent, so redis never confirms it.
	patches := gomonkey.ApplyMethod(reflect.TypeOf(&redis.PubSub{}), "PSubscribe", func(_ *redis.PubSub, _ context.Context, _ ...string) error {
		return nil
	})
	_, err := s.subscribe(rclient, pattern, 100*time.Millisecond)
	patches.Reset()
	if err == nil {
		t.Fatalf("subscribe returned no error without a confirmation")
	}

	r, err := s.subscribe(rclient, pattern, time.Second)
	if err != nil {
		t.Fatalf("subscribe after a timed-out one returned %v", err)
	}
	rclient.HSet(ctx, key, "f", "1")
	if _, ok := nextWithin(r, 2*time.Second); !ok {
		t.Errorf("the reader got no message after an earlier subscribe of its pattern timed out")
	}
}

func TestKeyspaceSubscriberWaitsAfterReceiveError(t *testing.T) {
	cleanup := setupTestTarget2RedisDb(t)
	defer cleanup()
	rclient := Target2RedisDb[""]["COUNTERS_DB"]

	kp := &keyspacePubSub{pubsub: rclient.PSubscribe(context.Background()), patterns: make(map[string]*keyspacePattern)}
	var calls int32
	patches := gomonkey.ApplyMethod(reflect.TypeOf(&redis.PubSub{}), "ReceiveTimeout", func(ps *redis.PubSub, _ context.Context, _ time.Duration) (interface{}, error) {
		if ps == kp.pubsub {
			atomic.AddInt32(&calls, 1)
		}
		return nil, errors.New("connection refused")
	})
	defer patches.Reset()

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		kp.receive(stop)
		close(done)
	}()
	time.Sleep(1500 * time.Millisecond)
	close(stop)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("receive did not return after stop")
	}
	n := atomic.LoadInt32(&calls)
	if n == 0 {
		t.Fatalf("receive never called ReceiveTimeout")
	}
	if n > 3 {
		t.Errorf("receive tried a failing connection %d times in 1.5s, want at most 3", n)
	}
}
