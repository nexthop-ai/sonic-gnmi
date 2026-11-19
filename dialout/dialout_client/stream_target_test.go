// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package telemetry_dialout

import (
	"context"
	"strings"
	"testing"
	"time"

	gpb "github.com/openconfig/gnmi/proto/gnmi"
	"github.com/openconfig/ygot/ygot"
	sds "github.com/sonic-net/sonic-gnmi/dialout/dialout_server"
	sdc "github.com/sonic-net/sonic-gnmi/sonic_data_client"
)

// A stream subscription is accepted for every target. The one refused when
// parsed is a stream to OTHERS whose report_interval, its sampling interval,
// is below what the OTHERS data client samples at.
func TestStreamSubscriptionTargets(t *testing.T) {
	rclient := getConfigDbClient(t)
	defer rclient.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	useTestClientConfig(t)

	const name = "STREAMTGT"
	const key = "TELEMETRY_CLIENT|Subscription_" + name
	cleanup := func() {
		if cs := ClientSubscriptionNameMap[name]; cs != nil {
			cs.Close()
			if cs.cancel != nil {
				cs.cancel()
			}
			unregisterSubscription(cs)
		}
		rclient.Del(ctx, key)
	}
	defer cleanup()

	for _, tt := range []struct {
		target, reportType, interval string
		refused                      bool
	}{
		{"OC_YANG", "stream", "", false},
		{"OC_YANG", "stream", "500", false},
		{"OTHERS", "stream", "", false},
		{"OTHERS", "stream", "1000", false},
		{"OTHERS", "stream", "999", true},
		{"OTHERS", "periodic", "999", false},
		{"COUNTERS_DB", "stream", "500", false},
	} {
		cleanup()
		// No such group: an accepted entry is registered and waits for it.
		fields := []interface{}{"dst_group", "STREAMTGT_NOGROUP", "path_target", tt.target,
			"report_type", tt.reportType, "paths", "COUNTERS/Ethernet0"}
		if tt.interval != "" {
			fields = append(fields, "report_interval", tt.interval)
		}
		rclient.HSet(ctx, key, fields...)
		err := processTelemetryClientConfig(ctx, rclient, "Subscription_"+name, "hset")
		_, registered := ClientSubscriptionNameMap[name]
		if tt.refused {
			if err == nil || !strings.Contains(err.Error(), "report_interval") || registered {
				t.Errorf("%s %s %q: err %v, registered %v; want it refused", tt.reportType, tt.target, tt.interval, err, registered)
			}
		} else if err != nil || !registered {
			t.Errorf("%s %s %q: err %v, registered %v; want it accepted", tt.reportType, tt.target, tt.interval, err, registered)
		}
	}
}

// The list a stream passes to its data client asks for a stream of the given
// paths, each in the given mode and sampled at the given interval, in the
// given encoding. The paths are the same pointers it was given: the data
// client finds each one by pointer.
func TestStreamList(t *testing.T) {
	prefix := &gpb.Path{Target: "OC_YANG"}
	var paths []*gpb.Path
	for _, s := range []string{"/interfaces/interface/state/counters", "/system/state"} {
		p, err := ygot.StringToPath(s, ygot.StructuredPath)
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, p)
	}
	l := streamList(prefix, paths, gpb.SubscriptionMode_SAMPLE, 2*time.Second, gpb.Encoding_JSON_IETF)
	if l.Mode != gpb.SubscriptionList_STREAM || l.Encoding != gpb.Encoding_JSON_IETF || l.Prefix != prefix {
		t.Errorf("list mode %v, encoding %v, prefix %v", l.Mode, l.Encoding, l.Prefix)
	}
	if len(l.Subscription) != len(paths) {
		t.Fatalf("%d subscriptions for %d paths", len(l.Subscription), len(paths))
	}
	for i, sub := range l.Subscription {
		if sub.Path != paths[i] || sub.Mode != gpb.SubscriptionMode_SAMPLE || sub.SampleInterval != uint64(2*time.Second) {
			t.Errorf("subscription %d: path %p (want %p), mode %v, interval %v", i, sub.Path, paths[i], sub.Mode, sub.SampleInterval)
		}
	}
}

// The source of a stream to OC_YANG or OTHERS is opened with the list its
// StreamRun needs, built from the paths the data client holds and the
// instance's own configuration.
func TestOpenSourceStreamList(t *testing.T) {
	for _, tt := range []struct {
		target, path string
		mode         gpb.SubscriptionMode
		interval     time.Duration
	}{
		{"OC_YANG", "/openconfig-system:system/state", gpb.SubscriptionMode_TARGET_DEFINED, 0},
		{"OTHERS", "proc/uptime", gpb.SubscriptionMode_SAMPLE, 7 * time.Second},
	} {
		p, err := ygot.StringToPath(tt.path, ygot.StructuredPath)
		if err != nil {
			t.Fatal(err)
		}
		cs := &clientSubscription{name: "LIST", entryState: &entryState{}, prefix: &gpb.Path{Target: tt.target}, paths: []*gpb.Path{p},
			reportType: Stream, interval: 7 * time.Second, cfg: ClientConfig{Encoding: gpb.Encoding_PROTO}}
		if _, err := cs.openSource(context.Background()); err != nil {
			t.Fatalf("%s: %v", tt.target, err)
		}
		l := cs.subList
		if l == nil || len(l.Subscription) != 1 {
			t.Fatalf("%s: list %v, want one subscription", tt.target, l)
		}
		sub := l.Subscription[0]
		if sub.Mode != tt.mode || sub.SampleInterval != uint64(tt.interval) || l.Encoding != gpb.Encoding_PROTO {
			t.Errorf("%s: mode %v, interval %v, encoding %v", tt.target, sub.Mode, sub.SampleInterval, l.Encoding)
		}
		if sub.Path == p {
			t.Errorf("%s: the list holds the entry's path, want the data client's copy", tt.target)
		}
	}
}

// startStreamCollector starts a collector on port that appends what it
// receives to the returned store.
func startStreamCollector(t *testing.T, port int64) (*sds.Server, *[]*gpb.SubscribeResponse) {
	collector := createServer(t, &sds.Config{Port: port})
	go runServer(t, collector)
	store := &[]*gpb.SubscribeResponse{}
	collector.SetDataStore(store)
	return collector, store
}

// waitForSync waits for a sync response at the collector and returns how
// many responses arrived up to it.
func waitForSync(t *testing.T, collector *sds.Server, store *[]*gpb.SubscribeResponse, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		n, synced := 0, false
		collector.ReadDataStore(func() {
			for i, r := range *store {
				if r.GetSyncResponse() {
					n, synced = i+1, true
					return
				}
			}
			n = len(*store)
		})
		if synced {
			return n
		}
		if time.Now().After(deadline) {
			t.Fatalf("no sync response at the collector in %v; got %d responses", timeout, n)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// A stream to OC_YANG reaches the collector: the translib client accepts
// the subscription list and sends at least the sync response. After the
// collector goes away and comes back, the same data client serves a second
// session. It is skipped where the OC_YANG data source cannot be built.
func TestOCYangStreamReachesCollector(t *testing.T) {
	collector, store := startStreamCollector(t, 8079)
	useTestClientConfig(t)
	cfg := *clientCfg
	cfg.Encoding = gpb.Encoding_JSON_IETF
	destGrpNameMap["OCYANG_STREAM"] = []Destination{{Addrs: "127.0.0.1:8079"}}
	defer delete(destGrpNameMap, "OCYANG_STREAM")

	p, err := ygot.StringToPath("/openconfig-system:system/state", ygot.StructuredPath)
	if err != nil {
		t.Fatal(err)
	}
	cs := &clientSubscription{name: "OCYANG_STREAM", entryState: &entryState{}, destGroupName: "OCYANG_STREAM",
		prefix: &gpb.Path{Target: "OC_YANG"}, paths: []*gpb.Path{p}, reportType: Stream}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := cs.NewInstance(ctx, cfg); err != nil {
		collector.Stop()
		t.Skipf("the OC_YANG data source cannot be built here: %v", err)
	}
	defer cs.Close()
	waitForSync(t, collector, store, 10*time.Second)

	collector.Stop()
	collector, store = startStreamCollector(t, 8079)
	defer collector.Stop()
	waitForSync(t, collector, store, 10*time.Second)
}

// A stream to OTHERS reaches the collector as a dial-in SAMPLE stream does:
// a first sample and the sync response, then a sample every report_interval.
func TestOthersStreamReachesCollector(t *testing.T) {
	collector, store := startStreamCollector(t, 8079)
	defer collector.Stop()
	useTestClientConfig(t)
	destGrpNameMap["OTHERS_STREAM"] = []Destination{{Addrs: "127.0.0.1:8079"}}
	defer delete(destGrpNameMap, "OTHERS_STREAM")

	p, err := ygot.StringToPath("proc/uptime", ygot.StructuredPath)
	if err != nil {
		t.Fatal(err)
	}
	cs := &clientSubscription{name: "OTHERS_STREAM", entryState: &entryState{}, destGroupName: "OTHERS_STREAM",
		prefix: &gpb.Path{Target: "OTHERS"}, paths: []*gpb.Path{p}, reportType: Stream, interval: sdc.MinSampleInterval}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := cs.NewInstance(ctx, *clientCfg); err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	synced := waitForSync(t, collector, store, 10*time.Second)
	if synced < 2 {
		t.Errorf("%d responses up to the sync response, want a sample before it", synced)
	}
	deadline := time.Now().Add(5 * sdc.MinSampleInterval)
	for {
		var after int
		collector.ReadDataStore(func() { after = len(*store) - synced })
		if after > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no sample after the sync response in %v", 5*sdc.MinSampleInterval)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// A stream whose subscription list the data client refuses is stopped, not
// redialled: here an OTHERS stream sampled faster than its data client
// allows, which the parser would have refused.
func TestRefusedStreamIsNotRetried(t *testing.T) {
	collector, store := startStreamCollector(t, 8079)
	defer collector.Stop()
	useTestClientConfig(t)
	destGrpNameMap["REFUSED_STREAM"] = []Destination{{Addrs: "127.0.0.1:8079"}}
	defer delete(destGrpNameMap, "REFUSED_STREAM")

	p, err := ygot.StringToPath("proc/uptime", ygot.StructuredPath)
	if err != nil {
		t.Fatal(err)
	}
	cs := &clientSubscription{name: "REFUSED_STREAM", entryState: &entryState{}, destGroupName: "REFUSED_STREAM",
		prefix: &gpb.Path{Target: "OTHERS"}, paths: []*gpb.Path{p}, reportType: Stream, interval: sdc.MinSampleInterval / 2}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := cs.NewInstance(ctx, *clientCfg); err != nil {
		t.Fatal(err)
	}
	defer cs.Close()

	deadline := time.Now().Add(10 * time.Second)
	for cs.errors.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the stream was not refused in 10s")
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Long enough for many retries at the test's 100ms retry interval.
	time.Sleep(time.Second)
	cs.cMu.Lock()
	opened := cs.opened
	cs.cMu.Unlock()
	var received int
	collector.ReadDataStore(func() { received = len(*store) })
	if tries := cs.conTryCnt.Load(); tries != 1 || opened || received != 0 {
		t.Errorf("%d dials, opened %v, %d responses at the collector; want one dial, closed, none", tries, opened, received)
	}
}
