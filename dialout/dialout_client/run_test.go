// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package telemetry_dialout

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gpb "github.com/openconfig/gnmi/proto/gnmi"
	"github.com/openconfig/ygot/ygot"
	"github.com/redis/go-redis/v9"
	sds "github.com/sonic-net/sonic-gnmi/dialout/dialout_server"
	spb "github.com/sonic-net/sonic-gnmi/proto"
	sdc "github.com/sonic-net/sonic-gnmi/sonic_data_client"
	sdcfg "github.com/sonic-net/sonic-gnmi/sonic_db_config"
	"github.com/sonic-net/sonic-gnmi/substate"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/protobuf/proto"
)

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
		time.Sleep(100 * time.Millisecond)
	}
}

// fakeReceiver stands in for the CONFIG_DB subscription. It returns the
// scripted values in order, then err for every later receive.
type fakeReceiver struct {
	script []interface{}
	err    error
	calls  atomic.Int64
}

func (f *fakeReceiver) ReceiveTimeout(context.Context, time.Duration) (interface{}, error) {
	n := f.calls.Add(1)
	if int(n) <= len(f.script) {
		return f.script[n-1], nil
	}
	return nil, f.err
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

var _ net.Error = timeoutErr{}

// A CONFIG_DB connection that keeps failing is retried at a pace, not in a
// spin, and the wait ends when the run is cancelled; so does the wait on a
// healthy, quiet connection.
func TestDialOutNextConfigEvent(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		maxCalls int64
	}{
		{"lost connection", errors.New("redis: connection lost"), 8},
		{"quiet connection", timeoutErr{}, 1 << 30},
	} {
		f := &fakeReceiver{err: tc.err}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := nextConfigEvent(ctx, f)
			done <- err
		}()
		time.Sleep(500 * time.Millisecond)
		cancel()
		select {
		case err := <-done:
			if err != context.Canceled {
				t.Errorf("%s: err = %v, want context.Canceled", tc.name, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s: nextConfigEvent did not return after cancel", tc.name)
		}
		if n := f.calls.Load(); n > tc.maxCalls {
			t.Errorf("%s: %d receives in 500 ms, want at most %d", tc.name, n, tc.maxCalls)
		}
	}
}

// A value on the subscription that is not a keyspace message, such as a
// pong, is skipped; the next message is returned.
func TestDialOutNextConfigEventSkipsNonMessage(t *testing.T) {
	want := &redis.Message{Channel: "__keyspace@4__:TELEMETRY_CLIENT|Global", Payload: "hset"}
	f := &fakeReceiver{script: []interface{}{&redis.Pong{}, &redis.Subscription{}, want}, err: timeoutErr{}}
	got, err := nextConfigEvent(context.Background(), f)
	if err != nil || got != want {
		t.Errorf("nextConfigEvent = %v, %v; want the scripted message", got, err)
	}
}

// A dial that succeeds just as the instance is cancelled does not leak its
// connection: it is not cs.client yet, and it is closed on the way out.
func TestDialOutCancelAfterDialClosesConnection(t *testing.T) {
	collector := createServer(t, &sds.Config{Port: 8087})
	go runServer(t, collector)
	defer collector.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	var dialed *Client
	dialCollector = func(ctx context.Context, cfg ClientConfig, dest Destination, b *substate.Bytes, onConnect func(string)) (*Client, error) {
		c, err := newClient(ctx, cfg, dest, b, onConnect)
		dialed = c
		cancel() // the cancel lands between the dial and the check after it
		return c, err
	}
	defer func() { dialCollector = newClient }()
	cs := &clientSubscription{name: "L", entryState: &entryState{},
		cfg: ClientConfig{RetryInterval: 2 * time.Second, TLS: &tls.Config{InsecureSkipVerify: true}}}
	publishRun(ctx, cs, []Destination{{Addrs: "127.0.0.1:8087"}})
	if dialed == nil {
		t.Fatalf("the dial did not succeed")
	}
	if st := dialed.conn.GetState(); st != connectivity.Shutdown {
		t.Errorf("the dialed connection is %v after the cancel, want Shutdown", st)
	}
}

// Edits to TELEMETRY_CLIENT, applied one at a time as DialOutRun applies
// keyspace events: removing one field is an update, an update that does not
// parse leaves the running instance alone, deleting a group in use parks its
// entries until it returns, and Global falls back to the run's starting
// values when its fields go.
func TestDialOutConfigEdits(t *testing.T) {
	cdb := getConfigDbClient(t)
	defer cdb.Close()
	bg := context.Background()
	cdb.FlushDB(bg)
	defer cdb.FlushDB(bg)
	start := ClientConfig{RetryInterval: time.Second, TLS: &tls.Config{InsecureSkipVerify: true}}
	defer isolateClientState(t, start)()
	ctx, cancel := context.WithCancel(bg)
	defer cancel()

	apply := func(key, op string) error { return processTelemetryClientConfig(ctx, cdb, key, op) }
	instance := func(name string) *clientSubscription {
		configMu.Lock()
		defer configMu.Unlock()
		return ClientSubscriptionNameMap[name]
	}
	// watchCancel reports whether cs's context is cancelled from now on.
	watchCancel := func(cs *clientSubscription) *atomic.Bool {
		var cancelled atomic.Bool
		configMu.Lock()
		orig := cs.cancel
		cs.cancel = func() { cancelled.Store(true); orig() }
		configMu.Unlock()
		return &cancelled
	}

	// Nothing listens on port 1: the instances keep dialing, which is all
	// these checks need.
	cdb.HSet(bg, "TELEMETRY_CLIENT|DestinationGroup_G1", "dst_addr", "127.0.0.1:1")
	if err := apply("DestinationGroup_G1", "hset"); err != nil {
		t.Fatalf("group: %v", err)
	}
	cdb.HSet(bg, "TELEMETRY_CLIENT|Subscription_S1", "path_target", "COUNTERS_DB", "dst_group", "G1",
		"report_type", "periodic", "report_interval", "1000", "paths", "COUNTERS/Ethernet68")
	if err := apply("Subscription_S1", "hset"); err != nil || instance("S1") == nil {
		t.Fatalf("S1 not configured: %v", err)
	}

	// Removing one field is an update: the entry stays.
	cdb.HDel(bg, "TELEMETRY_CLIENT|Subscription_S1", "report_interval")
	if err := apply("Subscription_S1", "hdel"); err != nil || instance("S1") == nil {
		t.Fatalf("removing one field removed the entry: err %v", err)
	}

	// An update that does not parse is refused, and the instance serving
	// the entry keeps running: an unknown field, a report_type or
	// report_interval out of range, or no paths.
	running := instance("S1")
	cancelled := watchCancel(running)
	for _, bad := range []struct{ field, value string }{
		{"report_type", "sometimes"},
		{"report_interval", "0"},
		{"report_interval", "soon"},
		{"report_interval", "100000000"},
		{"paths", ""},
	} {
		good := cdb.HGet(bg, "TELEMETRY_CLIENT|Subscription_S1", bad.field).Val()
		if bad.value == "" {
			cdb.HDel(bg, "TELEMETRY_CLIENT|Subscription_S1", bad.field)
		} else {
			cdb.HSet(bg, "TELEMETRY_CLIENT|Subscription_S1", bad.field, bad.value)
		}
		if err := apply("Subscription_S1", "hset"); err == nil {
			t.Errorf("%s %q was accepted", bad.field, bad.value)
		}
		if instance("S1") != running || cancelled.Load() {
			t.Fatalf("a refused %s %q stopped the running instance", bad.field, bad.value)
		}
		if good == "" {
			cdb.HDel(bg, "TELEMETRY_CLIENT|Subscription_S1", bad.field)
		} else {
			cdb.HSet(bg, "TELEMETRY_CLIENT|Subscription_S1", bad.field, good)
		}
	}
	if running.interval != 5*time.Second {
		t.Errorf("interval %v after the refused report_interval updates, want the 5s in use", running.interval)
	}
	cdb.HSet(bg, "TELEMETRY_CLIENT|Subscription_S1", "bogus", "1")
	if err := apply("Subscription_S1", "hset"); err == nil {
		t.Errorf("an unknown field was accepted")
	}
	if instance("S1") != running || cancelled.Load() {
		t.Errorf("a refused update stopped the running instance")
	}
	cdb.HDel(bg, "TELEMETRY_CLIENT|Subscription_S1", "bogus")
	if err := apply("Subscription_S1", "hdel"); err != nil || instance("S1") == nil || instance("S1") == running || !cancelled.Load() {
		t.Fatalf("an update that parses did not replace the instance: err %v", err)
	}

	// Removing one field of a group is an update, not a delete: the group
	// stays and its entries keep running.
	cdb.HSet(bg, "TELEMETRY_CLIENT|DestinationGroup_G1", "bogus", "1")
	if err := apply("DestinationGroup_G1", "hset"); err == nil {
		t.Errorf("a group with an unknown field was accepted")
	}
	cdb.HDel(bg, "TELEMETRY_CLIENT|DestinationGroup_G1", "bogus")
	if err := apply("DestinationGroup_G1", "hdel"); err != nil {
		t.Errorf("removing one group field: %v", err)
	}
	configMu.Lock()
	_, groupStays := destGrpNameMap["G1"]
	configMu.Unlock()
	if !groupStays || instance("S1") == nil || !instance("S1").serving() {
		t.Fatalf("removing one group field deleted the group (kept %v) or stopped its entry", groupStays)
	}

	// Deleting a group in use stops its entries: they stay configured, wait
	// for the group, and start again when it comes back.
	running = instance("S1")
	cancelled = watchCancel(running)
	cdb.Del(bg, "TELEMETRY_CLIENT|DestinationGroup_G1")
	if err := apply("DestinationGroup_G1", "hdel"); err != nil {
		t.Errorf("deleting a group in use: %v", err)
	}
	configMu.Lock()
	_, groupKept := destGrpNameMap["G1"]
	configMu.Unlock()
	parked := instance("S1")
	if groupKept || !cancelled.Load() || parked == nil || parked == running {
		t.Fatalf("the deleted group's entry was not parked: group kept %v, old instance cancelled %v", groupKept, cancelled.Load())
	}
	cdb.HSet(bg, "TELEMETRY_CLIENT|DestinationGroup_G1", "dst_addr", "127.0.0.1:1")
	if err := apply("DestinationGroup_G1", "hset"); err != nil || instance("S1") == nil || instance("S1") == parked {
		t.Fatalf("the entry did not start again with its group: %v", err)
	}

	// An event for a key already gone is a delete: a group read back empty
	// is not created with no destinations.
	if err := apply("DestinationGroup_G2", "hset"); err != nil {
		t.Errorf("an emptied group key: %v", err)
	}
	configMu.Lock()
	_, ghost := destGrpNameMap["G2"]
	configMu.Unlock()
	if ghost {
		t.Errorf("a group was created from an empty key")
	}

	// An update whose data source cannot be opened is refused, and the
	// running instance keeps serving.
	running = instance("S1")
	cancelled = watchCancel(running)
	cdb.HSet(bg, "TELEMETRY_CLIENT|Subscription_S1", "path_target", "COUNTER_DB")
	if err := apply("Subscription_S1", "hset"); err == nil {
		t.Errorf("an update with an unknown target was accepted")
	}
	if instance("S1") != running || cancelled.Load() {
		t.Errorf("an update with an unknown target stopped the running instance")
	}
	cdb.HSet(bg, "TELEMETRY_CLIENT|Subscription_S1", "path_target", "COUNTERS_DB")
	if err := apply("Subscription_S1", "hset"); err != nil || instance("S1") == running || !cancelled.Load() {
		t.Fatalf("a good update did not replace the instance: %v", err)
	}

	// An update that clears the destination group stops the entry, and the
	// refusal says so.
	running = instance("S1")
	cancelled = watchCancel(running)
	cdb.HDel(bg, "TELEMETRY_CLIENT|Subscription_S1", "dst_group")
	if err := apply("Subscription_S1", "hdel"); err == nil || !strings.Contains(err.Error(), "stopped") {
		t.Errorf("an update that removed dst_group: got err %v, want a refusal that says the entry is stopped", err)
	}
	if instance("S1") != nil || !cancelled.Load() {
		t.Errorf("an entry whose destination group was removed is still running")
	}

	// Deleting an entry the client holds no instance for, here S1, is not
	// an error and must not touch a missing instance.
	cdb.Del(bg, "TELEMETRY_CLIENT|Subscription_S1")
	if err := apply("Subscription_S1", "hdel"); err != nil {
		t.Errorf("deleting an entry with no instance: %v", err)
	}

	// An entry a writer emptied holds only the NULL placeholder. It reads as
	// empty, so it is a delete, not a row with an unknown field.
	cdb.HSet(bg, "TELEMETRY_CLIENT|Subscription_S1", "path_target", "COUNTERS_DB", "dst_group", "G1", "paths", "COUNTERS/Ethernet68")
	if err := apply("Subscription_S1", "hset"); err != nil || instance("S1") == nil {
		t.Fatalf("S1 not configured again: %v", err)
	}
	running = instance("S1")
	cancelled = watchCancel(running)
	cdb.Del(bg, "TELEMETRY_CLIENT|Subscription_S1")
	cdb.HSet(bg, "TELEMETRY_CLIENT|Subscription_S1", "NULL", "NULL")
	if err := apply("Subscription_S1", "hset"); err != nil || instance("S1") != nil || !cancelled.Load() {
		t.Errorf("an entry holding only NULL: err %v, still registered %v, cancelled %v; want a delete", err, instance("S1") != nil, cancelled.Load())
	}
	cdb.Del(bg, "TELEMETRY_CLIENT|Subscription_S1")

	// Global: a field set, then the key deleted, goes back to the run's
	// starting value; a delete is not an error.
	retry := func() time.Duration {
		configMu.Lock()
		defer configMu.Unlock()
		return clientCfg.RetryInterval
	}
	cdb.HSet(bg, "TELEMETRY_CLIENT|Global", "retry_interval", "7")
	if err := apply("Global", "hset"); err != nil || retry() != 7*time.Second {
		t.Errorf("Global retry_interval: err %v, value %v", err, retry())
	}
	cdb.Del(bg, "TELEMETRY_CLIENT|Global")
	if err := apply("Global", "hdel"); err != nil || retry() != start.RetryInterval {
		t.Errorf("Global delete: err %v, retry_interval %v, want %v", err, retry(), start.RetryInterval)
	}

	// A Global row emptied by a CONFIG_DB writer holds the NULL placeholder
	// only: every setting goes back to its default.
	cdb.HSet(bg, "TELEMETRY_CLIENT|Global", "retry_interval", "7")
	apply("Global", "hset")
	cdb.Del(bg, "TELEMETRY_CLIENT|Global")
	cdb.HSet(bg, "TELEMETRY_CLIENT|Global", "NULL", "NULL")
	if err := apply("Global", "hset"); err != nil || retry() != start.RetryInterval {
		t.Errorf("Global with the NULL placeholder only: err %v, retry_interval %v, want %v", err, retry(), start.RetryInterval)
	}
	cdb.Del(bg, "TELEMETRY_CLIENT|Global")
	apply("Global", "hdel")

	// An unknown Global field is refused and changes nothing.
	cdb.HSet(bg, "TELEMETRY_CLIENT|Global", "retry_interval", "7", "bogus", "1")
	if err := apply("Global", "hset"); err == nil || retry() != start.RetryInterval {
		t.Errorf("Global with an unknown field: err %v, retry_interval %v, want %v", err, retry(), start.RetryInterval)
	}
	cdb.Del(bg, "TELEMETRY_CLIENT|Global")
	apply("Global", "hdel")

	// A retry_interval of 0 would retry a failed dial with no pause: the
	// row is refused and the current value stays.
	cdb.HSet(bg, "TELEMETRY_CLIENT|Global", "retry_interval", "0")
	if err := apply("Global", "hset"); err == nil || retry() != start.RetryInterval {
		t.Errorf("Global retry_interval 0: err %v, value %v, want %v", err, retry(), start.RetryInterval)
	}

	// A refused row keeps the last good value, not the run's default, and
	// neither it nor a Global write that changes nothing rebuilds the
	// running instances.
	cdb.HSet(bg, "TELEMETRY_CLIENT|DestinationGroup_G1", "dst_addr", "127.0.0.1:1")
	apply("DestinationGroup_G1", "hset")
	cdb.HSet(bg, "TELEMETRY_CLIENT|Subscription_S7", "path_target", "COUNTERS_DB", "dst_group", "G1",
		"report_type", "periodic", "report_interval", "1000", "paths", "COUNTERS/Ethernet68")
	apply("Subscription_S7", "hset")
	cdb.HSet(bg, "TELEMETRY_CLIENT|Global", "retry_interval", "7")
	apply("Global", "hset")
	snapshot := func() map[string]*clientSubscription {
		configMu.Lock()
		defer configMu.Unlock()
		m := map[string]*clientSubscription{}
		for k, v := range ClientSubscriptionNameMap {
			m[k] = v
		}
		return m
	}
	before := snapshot()
	cdb.HSet(bg, "TELEMETRY_CLIENT|Global", "retry_interval", "0")
	if err := apply("Global", "hset"); err == nil || retry() != 7*time.Second {
		t.Errorf("Global invalid retry_interval after 7: err %v, value %v, want 7s", err, retry())
	}
	if len(before) == 0 {
		t.Errorf("no instance to check the no-op Global write against")
	}
	for name, cs := range snapshot() {
		if before[name] != cs {
			t.Errorf("a refused Global row rebuilt %s", name)
		}
	}
	cdb.HSet(bg, "TELEMETRY_CLIENT|Global", "retry_interval", "7")
	if err := apply("Global", "hset"); err != nil {
		t.Errorf("Global retry_interval 7 again: %v", err)
	}
	for name, cs := range snapshot() {
		if before[name] != cs {
			t.Errorf("a Global write that changed nothing rebuilt %s", name)
		}
	}
	cdb.Del(bg, "TELEMETRY_CLIENT|Global")
	apply("Global", "hdel")
	cdb.Del(bg, "TELEMETRY_CLIENT|Subscription_S7")
	apply("Subscription_S7", "hdel")

	// The publisher runs periodic and stream only: report_type once is
	// refused when parsed.
	cdb.HSet(bg, "TELEMETRY_CLIENT|Subscription_S8", "path_target", "COUNTERS_DB", "dst_group", "G1",
		"report_type", "once", "paths", "COUNTERS/Ethernet68")
	if err := apply("Subscription_S8", "hset"); err == nil || instance("S8") != nil {
		t.Errorf("a report_type once entry was accepted: err %v", err)
	}
	cdb.Del(bg, "TELEMETRY_CLIENT|Subscription_S8")
	apply("Subscription_S8", "hdel")

	// A name with the key separator would make a row key with an extra
	// part: it is refused.
	cdb.HSet(bg, "TELEMETRY_CLIENT|Subscription_a|b", "path_target", "COUNTERS_DB", "dst_group", "G1",
		"report_type", "periodic", "paths", "COUNTERS/Ethernet68")
	if err := apply("Subscription_a|b", "hset"); err == nil || !strings.Contains(err.Error(), "STATE_DB key separator") || instance("a|b") != nil {
		t.Errorf("a subscription name with the key separator was accepted: err %v", err)
	}
	cdb.Del(bg, "TELEMETRY_CLIENT|Subscription_a|b")

	// An entry with no path_target can never run: it is refused when parsed.
	cdb.HSet(bg, "TELEMETRY_CLIENT|Subscription_S9", "dst_group", "G1", "report_type", "periodic", "paths", "COUNTERS/Ethernet68")
	if err := apply("Subscription_S9", "hset"); err == nil || instance("S9") != nil {
		t.Errorf("an entry with no path_target was accepted: err %v", err)
	}
	cdb.Del(bg, "TELEMETRY_CLIENT|Subscription_S9")
	apply("Subscription_S9", "hdel")

	// An edit to an entry that is not running, here one waiting for a
	// missing group with a mistyped target, is applied, not refused.
	cdb.HSet(bg, "TELEMETRY_CLIENT|DestinationGroup_G1", "dst_addr", "127.0.0.1:1")
	apply("DestinationGroup_G1", "hset")
	cdb.HSet(bg, "TELEMETRY_CLIENT|Subscription_S2", "path_target", "COUNTER_DB", "dst_group", "G9",
		"report_type", "periodic", "report_interval", "1000", "paths", "COUNTERS/Ethernet68")
	apply("Subscription_S2", "hset")
	cdb.HSet(bg, "TELEMETRY_CLIENT|Subscription_S2", "dst_group", "G1")
	apply("Subscription_S2", "hset")
	configMu.Lock()
	group := ClientSubscriptionNameMap["S2"].destGroupName
	configMu.Unlock()
	if group != "G1" {
		t.Errorf("an edit to an entry that was not running was refused: group %q, want G1", group)
	}
}

// An edit to a serving entry whose new data source cannot be opened leaves
// the instance serving and is applied again every retry_interval: once the
// source opens the edit takes effect with no further write. A later event
// for the row ends the retry. As in TestDialOutSourceRetried, a removed
// Target2RedisDb entry stands in for a source that fails and then recovers.
func TestDialOutRefusedEditRetried(t *testing.T) {
	cdb := getConfigDbClient(t)
	defer cdb.Close()
	bg := context.Background()
	cdb.FlushDB(bg)
	defer cdb.FlushDB(bg)
	start := ClientConfig{RetryInterval: 200 * time.Millisecond, TLS: &tls.Config{InsecureSkipVerify: true}}
	defer isolateClientState(t, start)()
	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	apply := func(key, op string) error { return processTelemetryClientConfig(ctx, cdb, key, op) }
	instance := func() *clientSubscription {
		configMu.Lock()
		defer configMu.Unlock()
		return ClientSubscriptionNameMap["S1"]
	}
	target := func() string {
		configMu.Lock()
		defer configMu.Unlock()
		if cs := ClientSubscriptionNameMap["S1"]; cs != nil && cs.serving() {
			return cs.prefix.GetTarget()
		}
		return ""
	}
	const row = "TELEMETRY_CLIENT|Subscription_S1"

	if dc, err := sdc.NewDbClient(nil, &gpb.Path{Target: "CONFIG_DB"}); err == nil {
		dc.Close()
	}
	cdb.HSet(bg, "TELEMETRY_CLIENT|DestinationGroup_G1", "dst_addr", "127.0.0.1:1")
	apply("DestinationGroup_G1", "hset")
	cdb.HSet(bg, row, "path_target", "CONFIG_DB", "dst_group", "G1",
		"report_type", "periodic", "report_interval", "1000", "paths", "PORT/Ethernet0")
	if err := apply("Subscription_S1", "hset"); err != nil || target() != "CONFIG_DB" {
		t.Fatalf("S1 is not serving: %v", err)
	}
	running := instance()

	ns, _ := sdcfg.GetDbDefaultNamespace()
	configMu.Lock()
	saved := sdc.Target2RedisDb[ns]["APPL_DB"]
	delete(sdc.Target2RedisDb[ns], "APPL_DB")
	configMu.Unlock()
	restore := func() {
		configMu.Lock()
		sdc.Target2RedisDb[ns]["APPL_DB"] = saved
		configMu.Unlock()
	}
	defer restore()
	cdb.HSet(bg, row, "path_target", "APPL_DB", "paths", "PORT_TABLE/Ethernet0")
	if err := apply("Subscription_S1", "hset"); err == nil {
		t.Fatalf("an edit whose data source cannot be opened was accepted")
	}
	time.Sleep(3 * start.RetryInterval)
	if instance() != running || target() != "CONFIG_DB" {
		t.Fatalf("the refused edit, or its retry, replaced the serving instance while the source was gone")
	}
	restore()
	waitFor(t, "the edit to apply once its source is back", 5*time.Second, func() bool { return target() == "APPL_DB" })

	// A write that puts a good row back ends the retry of a refused edit:
	// the instance that write started is not rebuilt by it.
	cdb.HSet(bg, row, "path_target", "COUNTER_DB")
	if err := apply("Subscription_S1", "hset"); err == nil {
		t.Fatalf("an edit with an unknown target was accepted")
	}
	cdb.HSet(bg, row, "path_target", "APPL_DB")
	if err := apply("Subscription_S1", "hset"); err != nil {
		t.Fatalf("the corrected row: %v", err)
	}
	corrected := instance()
	time.Sleep(3 * start.RetryInterval)
	if instance() != corrected {
		t.Errorf("the retry of a refused edit ran after the row was corrected")
	}
}

// An entry whose data source cannot be opened is tried again every
// retry_interval, so it starts once the source is back, with no edit. A
// real device cannot lose a Target2RedisDb entry; removing one here stands
// in for a source that fails to open and then recovers.
func TestDialOutSourceRetried(t *testing.T) {
	cdb := getConfigDbClient(t)
	defer cdb.Close()
	bg := context.Background()
	cdb.FlushDB(bg)
	defer cdb.FlushDB(bg)
	start := ClientConfig{RetryInterval: 200 * time.Millisecond, TLS: &tls.Config{InsecureSkipVerify: true}}
	defer isolateClientState(t, start)()
	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	apply := func(key, op string) error { return processTelemetryClientConfig(ctx, cdb, key, op) }

	// The first data client rebuilds Target2RedisDb; build one now, so the
	// map edited below is the one the entry's data source will use.
	if dc, err := sdc.NewDbClient(nil, &gpb.Path{Target: "CONFIG_DB"}); err == nil {
		dc.Close()
	}
	ns, _ := sdcfg.GetDbDefaultNamespace()
	saved := sdc.Target2RedisDb[ns]["APPL_DB"]
	delete(sdc.Target2RedisDb[ns], "APPL_DB")
	restored := false
	defer func() {
		if !restored {
			sdc.Target2RedisDb[ns]["APPL_DB"] = saved
		}
	}()
	cdb.HSet(bg, "TELEMETRY_CLIENT|DestinationGroup_G1", "dst_addr", "127.0.0.1:1")
	apply("DestinationGroup_G1", "hset")
	cdb.HSet(bg, "TELEMETRY_CLIENT|Subscription_S1", "path_target", "APPL_DB", "dst_group", "G1",
		"report_type", "periodic", "report_interval", "1000", "paths", "PORT_TABLE/Ethernet0")
	apply("Subscription_S1", "hset")
	serving := func() bool {
		configMu.Lock()
		cs := ClientSubscriptionNameMap["S1"]
		configMu.Unlock()
		return cs != nil && cs.serving()
	}
	// Kept gone for several retry intervals, so the entry can only start
	// through a retry that re-armed itself, not the first one.
	time.Sleep(3 * start.RetryInterval)
	if serving() {
		t.Fatalf("S1 started with its data source gone")
	}
	configMu.Lock()
	sdc.Target2RedisDb[ns]["APPL_DB"] = saved
	restored = true
	configMu.Unlock()
	waitFor(t, "S1 started once its source is back", 5*time.Second, serving)
}

// Opening the data source must not change the entry's own paths: an
// instance re-created on a group or Global edit opens its source from them
// again. This needs the OC_YANG data client; TestClonePaths covers the copy
// without it.
func TestDialOutOpenSourceKeepsPaths(t *testing.T) {
	p, err := ygot.StringToPath("/interfaces/interface/state/counters", ygot.StructuredPath)
	if err != nil {
		t.Fatal(err)
	}
	cs := &clientSubscription{entryState: &entryState{}, name: "S", prefix: &gpb.Path{Target: "OC_YANG"}, paths: []*gpb.Path{p}}
	want := proto.Clone(p)
	dc, err := cs.openSource(context.Background())
	if err != nil {
		t.Skipf("the OC_YANG data source cannot be built here: %v", err)
	}
	dc.Close()
	if !proto.Equal(cs.paths[0], want) {
		t.Errorf("openSource changed the entry's path: %v, want %v", cs.paths[0], want)
	}
}

// An instance torn down between the dial and the check after it does not
// keep the new connection: Close found no client to close, so the check
// closes it.
func TestDialOutCloseAfterDialClosesConnection(t *testing.T) {
	collector := createServer(t, &sds.Config{Port: 8088})
	go runServer(t, collector)
	defer collector.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cs := &clientSubscription{name: "L", entryState: &entryState{},
		cfg: ClientConfig{RetryInterval: 2 * time.Second, TLS: &tls.Config{InsecureSkipVerify: true}}}
	var dialed *Client
	dialCollector = func(ctx context.Context, cfg ClientConfig, dest Destination, b *substate.Bytes, onConnect func(string)) (*Client, error) {
		c, err := newClient(ctx, cfg, dest, b, onConnect)
		dialed = c
		cs.Close() // the teardown lands between the dial and the check after it
		return c, err
	}
	defer func() { dialCollector = newClient }()
	publishRun(ctx, cs, []Destination{{Addrs: "127.0.0.1:8088"}})
	if dialed == nil {
		t.Fatalf("the dial did not succeed")
	}
	if st := dialed.conn.GetState(); st != connectivity.Shutdown {
		t.Errorf("the dialed connection is %v after the teardown, want Shutdown", st)
	}
	if cs.client != nil {
		t.Errorf("the torn-down instance kept the new connection")
	}
}

// clonePaths returns paths a data client can rewrite without touching the
// entry's own.
func TestClonePaths(t *testing.T) {
	p, err := ygot.StringToPath("/interfaces/interface[name=Ethernet0]/state", ygot.StructuredPath)
	if err != nil {
		t.Fatal(err)
	}
	want := proto.Clone(p)
	out := clonePaths([]*gpb.Path{p})
	out[0].Elem[0].Name = "openconfig-interfaces:interfaces"
	out[0].Elem[1].Key["name"] = "Ethernet4"
	if !proto.Equal(p, want) {
		t.Errorf("changing the copy changed the entry's path: %v, want %v", p, want)
	}
}

// A running instance dials with the configuration it started with, not the
// shared one a later Global edit replaces.
func TestDialOutInstanceKeepsItsConfig(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refused := ln.Addr().String()
	ln.Close() // nothing listens: every dial lasts RetryInterval and fails
	t.Cleanup(isolateClientState(t, ClientConfig{RetryInterval: 100 * time.Millisecond, TLS: &tls.Config{InsecureSkipVerify: true}}))
	const group = "keeps_config"
	destGrpNameMap[group] = []Destination{{Addrs: refused}}
	cs := &clientSubscription{name: group, entryState: &entryState{}, destGroupName: group, prefix: &gpb.Path{Target: "COUNTERS_DB"},
		paths: []*gpb.Path{{Elem: []*gpb.PathElem{{Name: "COUNTERS"}, {Name: "Ethernet68"}}}}, reportType: Stream}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := cs.NewInstance(ctx, *clientCfg); err != nil {
		t.Fatalf("NewInstance: %v", err)
	}
	t.Cleanup(cs.Close)
	// A Global edit replaces the shared config while the instance runs.
	*clientCfg = ClientConfig{RetryInterval: time.Hour}
	before := cs.conTryCnt.Load()
	time.Sleep(time.Second)
	if n := cs.conTryCnt.Load() - before; n < 3 {
		t.Errorf("%d dial attempts in 1s, want at least 3 at the 100ms it started with", n)
	}
}

// failingSource is a data source whose every read fails.
type failingSource struct {
	sdc.Client
	gets atomic.Int64
}

func (f *failingSource) Get(*sync.WaitGroup) ([]*spb.Value, error) {
	f.gets.Add(1)
	return nil, errors.New("source down")
}

// A periodic instance whose reads fail reads once per report interval, not
// in a busy loop.
func TestDialOutPeriodicReadErrorsPaced(t *testing.T) {
	collector := createServer(t, &sds.Config{Port: 8089})
	go runServer(t, collector)
	defer collector.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	src := &failingSource{}
	cs := &clientSubscription{name: "P", entryState: &entryState{}, reportType: Periodic, interval: 200 * time.Millisecond, dc: src,
		cfg: ClientConfig{RetryInterval: 2 * time.Second, TLS: &tls.Config{InsecureSkipVerify: true}}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		publishRun(ctx, cs, []Destination{{Addrs: "127.0.0.1:8089"}})
	}()
	waitFor(t, "the first read", 5*time.Second, func() bool { return src.gets.Load() > 0 })
	first := src.gets.Load()
	time.Sleep(time.Second)
	n := src.gets.Load() - first
	cancel()
	cs.Close()
	<-done
	if n > 7 {
		t.Errorf("%d reads in 1s at a 200ms interval: the failing reads are not paced", n)
	}
	if n == 0 {
		t.Errorf("no reads in 1s: the instance stopped reading after a failure")
	}
}

// The retry stops once the Subscription row is deleted: a source that
// comes back later does not start the deleted entry.
func TestDialOutSourceRetryStopsOnDelete(t *testing.T) {
	cdb := getConfigDbClient(t)
	defer cdb.Close()
	bg := context.Background()
	cdb.FlushDB(bg)
	defer cdb.FlushDB(bg)
	start := ClientConfig{RetryInterval: 200 * time.Millisecond, TLS: &tls.Config{InsecureSkipVerify: true}}
	defer isolateClientState(t, start)()
	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	apply := func(key, op string) error { return processTelemetryClientConfig(ctx, cdb, key, op) }

	if dc, err := sdc.NewDbClient(nil, &gpb.Path{Target: "CONFIG_DB"}); err == nil {
		dc.Close()
	}
	ns, _ := sdcfg.GetDbDefaultNamespace()
	saved := sdc.Target2RedisDb[ns]["APPL_DB"]
	delete(sdc.Target2RedisDb[ns], "APPL_DB")
	restored := false
	defer func() {
		if !restored {
			sdc.Target2RedisDb[ns]["APPL_DB"] = saved
		}
	}()
	cdb.HSet(bg, "TELEMETRY_CLIENT|DestinationGroup_G1", "dst_addr", "127.0.0.1:1")
	apply("DestinationGroup_G1", "hset")
	cdb.HSet(bg, "TELEMETRY_CLIENT|Subscription_S1", "path_target", "APPL_DB", "dst_group", "G1",
		"report_type", "periodic", "report_interval", "1000", "paths", "PORT_TABLE/Ethernet0")
	apply("Subscription_S1", "hset")
	configMu.Lock()
	old := ClientSubscriptionNameMap["S1"]
	configMu.Unlock()
	if old == nil || old.serving() {
		t.Fatalf("S1 is not waiting for its data source")
	}
	cdb.Del(bg, "TELEMETRY_CLIENT|Subscription_S1")
	apply("Subscription_S1", "hdel")
	configMu.Lock()
	sdc.Target2RedisDb[ns]["APPL_DB"] = saved
	restored = true
	configMu.Unlock()
	time.Sleep(5 * start.RetryInterval)
	configMu.Lock()
	_, registered := ClientSubscriptionNameMap["S1"]
	configMu.Unlock()
	if registered || old.serving() {
		t.Errorf("the deleted S1 was started again once its source came back")
	}
}

// okSource is a data source whose every read returns one value.
type okSource struct {
	sdc.Client
}

func (okSource) Get(*sync.WaitGroup) ([]*spb.Value, error) {
	return []*spb.Value{{Timestamp: 1}}, nil
}

// A periodic instance stops within a report interval of a long wait,
// whether it is closed or only cancelled; with a failing source too.
func TestDialOutPeriodicWaitEndsOnStop(t *testing.T) {
	collector := createServer(t, &sds.Config{Port: 8090})
	go runServer(t, collector)
	defer collector.Stop()
	for _, tc := range []struct {
		name  string
		src   sdc.Client
		close bool
	}{
		{"read ok, Close", okSource{}, true},
		{"read ok, cancel", okSource{}, false},
		{"read fails, Close", &failingSource{}, true},
		{"read fails, cancel", &failingSource{}, false},
	} {
		ctx, cancel := context.WithCancel(context.Background())
		cs := &clientSubscription{name: "W", entryState: &entryState{}, reportType: Periodic, interval: time.Minute, dc: tc.src,
			cfg: ClientConfig{RetryInterval: 2 * time.Second, TLS: &tls.Config{InsecureSkipVerify: true}}}
		done := make(chan struct{})
		go func() {
			defer close(done)
			publishRun(ctx, cs, []Destination{{Addrs: "127.0.0.1:8090"}})
		}()
		waitFor(t, tc.name+": connected", 5*time.Second, func() bool {
			cs.cMu.Lock()
			defer cs.cMu.Unlock()
			return cs.client != nil
		})
		time.Sleep(200 * time.Millisecond) // into the wait after the first read
		if tc.close {
			cs.Close()
		} else {
			cancel()
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Errorf("%s: publishRun still running 1s after the stop, with a 1 minute interval", tc.name)
			cs.Close()
			<-done
		}
		cancel()
	}
}

// configCopy carries every Config Data field of the entry it copies, so a
// parked or rebuilt entry runs with its own paths and interval.
func TestConfigCopyKeepsConfigData(t *testing.T) {
	src := &clientSubscription{
		name:          "S1",
		destGroupName: "G1",
		prefix:        &gpb.Path{Target: "COUNTERS_DB"},
		paths:         []*gpb.Path{{Elem: []*gpb.PathElem{{Name: "COUNTERS"}, {Name: "Ethernet68"}}}},
		reportType:    Stream,
		interval:      7 * time.Second,
	}
	got := src.configCopy()
	if got == src || got.name != src.name || got.destGroupName != src.destGroupName || got.prefix != src.prefix ||
		len(got.paths) != 1 || got.paths[0] != src.paths[0] || got.reportType != src.reportType || got.interval != src.interval {
		t.Errorf("configCopy = %+v, want the Config Data of %+v", got, src)
	}
}
