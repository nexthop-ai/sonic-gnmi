// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package telemetry_dialout

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	gpb "github.com/openconfig/gnmi/proto/gnmi"
	"github.com/openconfig/ygot/ygot"
	sds "github.com/sonic-net/sonic-gnmi/dialout/dialout_server"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/protobuf/proto"
)

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
	saved := clientCfg
	t.Cleanup(func() {
		if saved != nil {
			clientCfg = saved
		}
	})
	start := ClientConfig{RetryInterval: 100 * time.Millisecond, TLS: &tls.Config{InsecureSkipVerify: true}}
	clientCfg = &ClientConfig{}
	*clientCfg = start
	const group = "keeps_config"
	destGrpNameMap[group] = []Destination{{Addrs: refused}}
	t.Cleanup(func() { delete(destGrpNameMap, group) })
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
