// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package telemetry_dialout

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
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
	dialCollector = func(ctx context.Context, cfg ClientConfig, dest Destination) (*Client, error) {
		c, err := newClient(ctx, cfg, dest)
		dialed = c
		cancel() // the cancel lands between the dial and the check after it
		return c, err
	}
	defer func() { dialCollector = newClient }()
	cs := &clientSubscription{name: "L",
		cfg: ClientConfig{RetryInterval: 2 * time.Second, TLS: &tls.Config{InsecureSkipVerify: true}}}
	publishRun(ctx, cs, []Destination{{Addrs: "127.0.0.1:8087"}})
	if dialed == nil {
		t.Fatalf("the dial did not succeed")
	}
	if st := dialed.conn.GetState(); st != connectivity.Shutdown {
		t.Errorf("the dialed connection is %v after the cancel, want Shutdown", st)
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
	cs := &clientSubscription{name: "S", prefix: &gpb.Path{Target: "OC_YANG"}, paths: []*gpb.Path{p}}
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
	cs := &clientSubscription{name: "L",
		cfg: ClientConfig{RetryInterval: 2 * time.Second, TLS: &tls.Config{InsecureSkipVerify: true}}}
	var dialed *Client
	dialCollector = func(ctx context.Context, cfg ClientConfig, dest Destination) (*Client, error) {
		c, err := newClient(ctx, cfg, dest)
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
	cs := &clientSubscription{name: group, destGroupName: group, prefix: &gpb.Path{Target: "COUNTERS_DB"},
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
	cs := &clientSubscription{name: "P", reportType: Periodic, interval: 200 * time.Millisecond, dc: src,
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
		cs := &clientSubscription{name: "W", reportType: Periodic, interval: time.Minute, dc: tc.src,
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
