// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package telemetry_dialout

// Tests for dial-out subscription state publishing.
// Prerequisite, as for the rest of this package: redis-server running.

import (
	"context"
	"crypto/tls"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Workiva/go-datastructures/queue"
	pb "github.com/openconfig/gnmi/proto/gnmi"
	"github.com/redis/go-redis/v9"
	sds "github.com/sonic-net/sonic-gnmi/dialout/dialout_server"
	spb "github.com/sonic-net/sonic-gnmi/proto"
	sdc "github.com/sonic-net/sonic-gnmi/sonic_data_client"
	sdcfg "github.com/sonic-net/sonic-gnmi/sonic_db_config"
	"github.com/sonic-net/sonic-gnmi/substate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func stateDbClient(t *testing.T) *redis.Client {
	t.Helper()
	ns, _ := sdcfg.GetDbDefaultNamespace()
	addr, err := sdcfg.GetDbTcpAddr("STATE_DB", ns)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sdcfg.GetDbId("STATE_DB", ns)
	if err != nil {
		t.Fatal(err)
	}
	return redis.NewClient(&redis.Options{Network: "tcp", Addr: addr, DB: db})
}

func hget(rc *redis.Client, key, field string) string {
	v, _ := rc.HGet(context.Background(), key, field).Result()
	return v
}

func hgetInt(rc *redis.Client, key, field string) int64 {
	n, _ := strconv.ParseInt(hget(rc, key, field), 10, 64)
	return n
}

func TestDialOutSubscriptionState(t *testing.T) {
	prepareDb(t)
	cdb := getConfigDbClient(t)
	defer cdb.Close()
	cdb.FlushDB(context.Background())
	// The publish interval is read from CONFIG_DB GNMI|gnmi, as for the server.
	cdb.HSet(context.Background(), "GNMI|gnmi", "state_publish_interval", "1")
	rc := stateDbClient(t)
	defer rc.Close()
	rc.FlushDB(context.Background())
	// A stale row from a previous run must be flushed at start; a dial-in
	// row is not this process's and must survive.
	rc.HSet(context.Background(), "GNMI_SUBSCRIPTION|dialout_old", "state", "connected")
	rc.HSet(context.Background(), "GNMI_SUBSCRIPTION|7", "direction", "dial-in")

	collector := createServer(t, &sds.Config{Port: 8084})
	go runServer(t, collector)
	defer func() { collector.Stop() }()

	cfg := ClientConfig{
		RetryInterval:  2 * time.Second,
		Encoding:       pb.Encoding_JSON_IETF,
		Unidirectional: true,
		TLS:            &tls.Config{InsecureSkipVerify: true},
	}
	// The periodic read goes through a seam the test can fail, installed
	// before the client starts so nothing races the client's reads of it.
	var sourceDown atomic.Bool
	periodicGet = func(cs *clientSubscription) ([]*spb.Value, error) {
		if sourceDown.Load() {
			return nil, errors.New("simulated source outage")
		}
		return cs.dc.Get(nil)
	}
	defer func() { periodicGet = func(cs *clientSubscription) ([]*spb.Value, error) { return cs.dc.Get(nil) } }()
	// The run writes Global changes through &cfg, so the starting value is
	// read before the run starts.
	startRetry := cfg.RetryInterval
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- DialOutRun(ctx, &cfg) }()
	stopped := false
	stopRun := func() {
		// The client must leave promptly when cancelled, so a later test
		// (or a re-run) does not share CONFIG_DB events with a stale loop.
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case <-runDone:
		case <-time.After(5 * time.Second):
			t.Errorf("DialOutRun did not return after cancel")
		}
	}
	defer stopRun()

	serverKey := "GNMI_SERVER_STATE|" + substate.ServerDialout
	waitFor(t, "dialout server row", 5*time.Second, func() bool { return hget(rc, serverKey, "pid") != "" })
	if n, _ := rc.Exists(context.Background(), "GNMI_SUBSCRIPTION|dialout_old").Result(); n != 0 {
		t.Errorf("stale dial-out row survived the start")
	}
	if n, _ := rc.Exists(context.Background(), "GNMI_SUBSCRIPTION|7").Result(); n != 1 {
		t.Errorf("the dial-in row was flushed by the dial-out client")
	}
	if got := hget(rc, serverKey, "publish_interval"); got != "1" {
		t.Errorf("publish_interval = %s, want 1 from CONFIG_DB", got)
	}

	// A reachable collector: periodic report every second.
	exe_cmd(t, "redis-cli -n 4 hset TELEMETRY_CLIENT|DestinationGroup_G1 dst_addr 127.0.0.1:8084")
	exe_cmd(t, "redis-cli -n 4 hmset TELEMETRY_CLIENT|Subscription_S1 path_target COUNTERS_DB dst_group G1 report_type periodic report_interval 1000 paths COUNTERS/Ethernet68")
	subKey := "GNMI_SUBSCRIPTION|dialout_S1"
	waitFor(t, "S1 connected", 10*time.Second, func() bool { return hget(rc, subKey, "state") == "connected" })
	row, _ := rc.HGetAll(context.Background(), subKey).Result()
	want := map[string]string{
		"direction": "dial-out", "server": "dialout", "destination_group": "G1", "peer": "127.0.0.1:8084",
		"list_mode": "STREAM", "target": "COUNTERS_DB", "encoding": "JSON_IETF", "prefix": "", "last_error": "",
	}
	for f, v := range want {
		if row[f] != v {
			t.Errorf("S1 row %s = %q, want %q", f, row[f], v)
		}
	}
	if _, has := row["rx_bytes"]; has {
		t.Errorf("S1 row carries rx_bytes; the dial-out stream receives nothing")
	}
	if !strings.HasPrefix(row["local_address"], "127.0.0.1:") {
		t.Errorf("S1 local_address = %q, want the connection's local 127.0.0.1:port", row["local_address"])
	}
	if row["start_time"] == "" || hgetInt(rc, subKey, "connect_attempts") < 1 {
		t.Errorf("S1 row start_time %q, connect_attempts %q", row["start_time"], row["connect_attempts"])
	}
	p0, _ := rc.HGetAll(context.Background(), "GNMI_SUBSCRIPTION_PATH|dialout_S1|0").Result()
	if p0["path"] != "COUNTERS_DB/COUNTERS/Ethernet68" || p0["mode"] != "SAMPLE" || p0["sample_interval_ns"] != strconv.FormatInt(int64(time.Second), 10) {
		t.Errorf("S1 path row = %v", p0)
	}
	waitFor(t, "S1 counters on the tick", 6*time.Second, func() bool {
		return hgetInt(rc, subKey, "tx_msgs") >= 1 && hgetInt(rc, subKey, "tx_bytes") > 0 && hget(rc, subKey, "last_updated") != ""
	})
	if hget(rc, subKey, "rx_bytes") != "" {
		t.Errorf("the tick wrote rx_bytes on a dial-out row")
	}
	if d, err := strconv.ParseInt(hget(rc, subKey, "queue_depth"), 10, 64); err != nil || d < 0 {
		t.Errorf("S1 queue_depth = %q, want a number of at least 0", hget(rc, subKey, "queue_depth"))
	}
	waitFor(t, "server counts", 3*time.Second, func() bool {
		return hgetInt(rc, serverKey, "configured_subscriptions") == 1 && hgetInt(rc, serverKey, "connected_subscriptions") == 1
	})

	// The data source fails under a running periodic entry: the row goes
	// to connecting with the error, and the next good read brings it back.
	sourceDown.Store(true)
	waitFor(t, "S1 source down", 10*time.Second, func() bool {
		return hget(rc, subKey, "state") == "connecting" && strings.Contains(hget(rc, subKey, "last_error"), "simulated")
	})
	sourceDown.Store(false)
	waitFor(t, "S1 source back", 10*time.Second, func() bool {
		return hget(rc, subKey, "state") == "connected" && hgetInt(rc, serverKey, "connected_subscriptions") == 1
	})
	if hget(rc, subKey, "local_address") == "" {
		t.Errorf("S1 is connected again with no local_address; its connection never closed")
	}

	// The collector goes away under a connected stream: the row goes back
	// to connecting and the connected count drops; when a collector is back on the address the row reconnects.
	collector.Stop()
	waitFor(t, "S1 stream broken", 15*time.Second, func() bool {
		return hget(rc, subKey, "state") == "connecting" && hgetInt(rc, serverKey, "connected_subscriptions") == 0
	})
	collector = createServer(t, &sds.Config{Port: 8084})
	go runServer(t, collector)
	waitFor(t, "S1 reconnected", 20*time.Second, func() bool {
		return hget(rc, subKey, "state") == "connected" && hgetInt(rc, serverKey, "connected_subscriptions") == 1
	})

	// The destination group changes: the instance is rebuilt on the new
	// address, the row follows it, and the connected count does not drift.
	collector2 := createServer(t, &sds.Config{Port: 8085})
	go runServer(t, collector2)
	defer collector2.Stop()
	txBeforeMove := hgetInt(rc, subKey, "tx_msgs")
	bytesBeforeMove := hgetInt(rc, subKey, "tx_bytes")
	attemptsBeforeMove := hgetInt(rc, subKey, "connect_attempts")
	configMu.Lock()
	ClientSubscriptionNameMap["S1"].errors.Store(3) // so a dropped carry-over shows
	configMu.Unlock()
	exe_cmd(t, "redis-cli -n 4 hset TELEMETRY_CLIENT|DestinationGroup_G1 dst_addr 127.0.0.1:8085")
	waitFor(t, "S1 moved to the new destination", 20*time.Second, func() bool {
		return hget(rc, subKey, "peer") == "127.0.0.1:8085" && hget(rc, subKey, "state") == "connected"
	})
	waitFor(t, "connected count after the move", 5*time.Second, func() bool {
		return hgetInt(rc, serverKey, "connected_subscriptions") == 1 && hgetInt(rc, serverKey, "configured_subscriptions") == 1
	})
	// The counters belong to the entry: they carry over and keep moving.
	if hgetInt(rc, subKey, "tx_msgs") < txBeforeMove || hgetInt(rc, subKey, "tx_bytes") < bytesBeforeMove || hgetInt(rc, subKey, "connect_attempts") <= attemptsBeforeMove {
		t.Errorf("counters restarted after the move: tx_msgs %d (was %d), tx_bytes %d (was %d), connect_attempts %d (was %d)",
			hgetInt(rc, subKey, "tx_msgs"), txBeforeMove, hgetInt(rc, subKey, "tx_bytes"), bytesBeforeMove, hgetInt(rc, subKey, "connect_attempts"), attemptsBeforeMove)
	}
	waitFor(t, "error_count carried over the move", 5*time.Second, func() bool { return hgetInt(rc, subKey, "error_count") >= 3 })
	before := hgetInt(rc, subKey, "tx_msgs")
	waitFor(t, "counters come from the rebuilt instance", 10*time.Second, func() bool { return hgetInt(rc, subKey, "tx_msgs") > before })

	// A Global change while an entry is live rebuilds the entry's instance
	// under the new configuration; the row, its start_time and its counters
	// carry over, and the rebuilt instance serves.
	instance := func() *clientSubscription {
		configMu.Lock()
		defer configMu.Unlock()
		return ClientSubscriptionNameMap["S1"]
	}
	beforeGlobal, startBeforeGlobal, txBeforeGlobal := instance(), hgetInt(rc, subKey, "start_time"), hgetInt(rc, subKey, "tx_msgs")
	exe_cmd(t, "redis-cli -n 4 hset TELEMETRY_CLIENT|Global retry_interval 3")
	waitFor(t, "S1 rebuilt by the Global change", 10*time.Second, func() bool {
		cs := instance()
		return cs != nil && cs != beforeGlobal && hget(rc, subKey, "state") == "connected"
	})
	if hgetInt(rc, subKey, "start_time") != startBeforeGlobal || hgetInt(rc, subKey, "tx_msgs") < txBeforeGlobal {
		t.Errorf("a Global change restarted the entry: start_time %d (was %d), tx_msgs %d (was %d)",
			hgetInt(rc, subKey, "start_time"), startBeforeGlobal, hgetInt(rc, subKey, "tx_msgs"), txBeforeGlobal)
	}
	if !beforeGlobal.isRetired() {
		t.Errorf("the instance the Global change replaced was not retired")
	}
	exe_cmd(t, "redis-cli -n 4 del TELEMETRY_CLIENT|Global")
	waitFor(t, "S1 connected after Global is removed", 10*time.Second, func() bool { return hget(rc, subKey, "state") == "connected" })

	// Deleting a destination group that is in use stops its entry: the row
	// stays and says the entry waits for the group, and the entry starts
	// again when the group comes back.
	exe_cmd(t, "redis-cli -n 4 del TELEMETRY_CLIENT|DestinationGroup_G1")
	waitFor(t, "S1 waiting for its deleted group", 10*time.Second, func() bool {
		return hget(rc, subKey, "state") == "connecting" && strings.Contains(hget(rc, subKey, "last_error"), "doesn't exist") && hgetInt(rc, serverKey, "connected_subscriptions") == 0
	})
	exe_cmd(t, "redis-cli -n 4 hset TELEMETRY_CLIENT|DestinationGroup_G1 dst_addr 127.0.0.1:8085")
	waitFor(t, "S1 connected once its group is back", 20*time.Second, func() bool {
		return hget(rc, subKey, "state") == "connected" && hgetInt(rc, serverKey, "connected_subscriptions") == 1
	})

	// Removing one field from a group is an update: the group stays as long
	// as its key does. A stray field the client refused is removed here.
	groupExists := func(name string) bool {
		configMu.Lock()
		defer configMu.Unlock()
		_, ok := destGrpNameMap[name]
		return ok
	}
	// settle waits until the client has handled every earlier event, which
	// it does in order, and the publisher has written what they queued.
	settle := func() {
		exe_cmd(t, "redis-cli -n 4 hset TELEMETRY_CLIENT|DestinationGroup_Gsync dst_addr 127.0.0.1:8085")
		waitFor(t, "earlier events handled", 5*time.Second, func() bool { return groupExists("Gsync") })
		exe_cmd(t, "redis-cli -n 4 del TELEMETRY_CLIENT|DestinationGroup_Gsync")
		waitFor(t, "sentinel group removed", 5*time.Second, func() bool { return !groupExists("Gsync") })
		done := make(chan struct{})
		if currentStatePublisher().pub.Enqueue(func() { close(done) }) {
			<-done
		}
	}
	exe_cmd(t, "redis-cli -n 4 hset TELEMETRY_CLIENT|DestinationGroup_G7 dst_addr 127.0.0.1:8085")
	waitFor(t, "G7 configured", 5*time.Second, func() bool { return groupExists("G7") })
	exe_cmd(t, "redis-cli -n 4 hset TELEMETRY_CLIENT|DestinationGroup_G7 bogus 1")
	exe_cmd(t, "redis-cli -n 4 hdel TELEMETRY_CLIENT|DestinationGroup_G7 bogus")
	// Events are handled in order: once G8 is in, G7's edits are done.
	exe_cmd(t, "redis-cli -n 4 hset TELEMETRY_CLIENT|DestinationGroup_G8 dst_addr 127.0.0.1:8085")
	waitFor(t, "G8 configured", 5*time.Second, func() bool { return groupExists("G8") })
	if !groupExists("G7") {
		t.Errorf("removing one field of a destination group deleted the group")
	}
	exe_cmd(t, "redis-cli -n 4 del TELEMETRY_CLIENT|DestinationGroup_G7")
	exe_cmd(t, "redis-cli -n 4 del TELEMETRY_CLIENT|DestinationGroup_G8")
	waitFor(t, "G7 and G8 deleted with their keys", 5*time.Second, func() bool { return !groupExists("G7") && !groupExists("G8") })

	// An update the client cannot parse leaves the running entry serving
	// the old configuration.
	exe_cmd(t, "redis-cli -n 4 hset TELEMETRY_CLIENT|Subscription_S1 bogus 1")
	settle()
	if hget(rc, subKey, "state") != "connected" || hgetInt(rc, serverKey, "configured_subscriptions") != 1 || hgetInt(rc, serverKey, "connected_subscriptions") != 1 {
		t.Errorf("an unparseable update disturbed the entry: state %q, configured %d, connected %d", hget(rc, subKey, "state"), hgetInt(rc, serverKey, "configured_subscriptions"), hgetInt(rc, serverKey, "connected_subscriptions"))
	}
	// Taking the field out again is an update the client can parse: the
	// entry is rebuilt and serves on. An edit to the entry is a new
	// subscription: the row starts again, start_time and counters with it.
	startBeforeEdit, txBeforeEdit := hgetInt(rc, subKey, "start_time"), hgetInt(rc, subKey, "tx_msgs")
	exe_cmd(t, "redis-cli -n 4 hdel TELEMETRY_CLIENT|Subscription_S1 bogus")
	waitFor(t, "S1 connected after the field is removed", 15*time.Second, func() bool {
		return hget(rc, subKey, "state") == "connected" && hgetInt(rc, serverKey, "configured_subscriptions") == 1
	})
	waitFor(t, "the edited entry's row written again", 10*time.Second, func() bool { return hgetInt(rc, subKey, "start_time") > startBeforeEdit })
	if tx := hgetInt(rc, subKey, "tx_msgs"); tx >= txBeforeEdit {
		t.Errorf("after an edit tx_msgs = %d (was %d); want the counters restarted", tx, txBeforeEdit)
	}
	waitFor(t, "S1 connected after the edit", 15*time.Second, func() bool { return hget(rc, subKey, "state") == "connected" })

	// An entry whose destination group is not configured yet: the row says
	// it is waiting, and it starts when the group appears, whichever order
	// the rows arrive in.
	exe_cmd(t, "redis-cli -n 4 hmset TELEMETRY_CLIENT|Subscription_S5 path_target COUNTERS_DB dst_group G9 report_type periodic report_interval 1000 paths COUNTERS/Ethernet68")
	sub5 := "GNMI_SUBSCRIPTION|dialout_S5"
	waitFor(t, "S5 waiting for its group", 10*time.Second, func() bool {
		return strings.Contains(hget(rc, sub5, "last_error"), "doesn't exist") && hget(rc, sub5, "state") == "connecting"
	})
	exe_cmd(t, "redis-cli -n 4 hset TELEMETRY_CLIENT|DestinationGroup_G9 dst_addr 127.0.0.1:8085")
	waitFor(t, "S5 connected once its group exists", 20*time.Second, func() bool { return hget(rc, sub5, "state") == "connected" && hget(rc, sub5, "last_error") == "" })
	exe_cmd(t, "redis-cli -n 4 del TELEMETRY_CLIENT|Subscription_S5")
	waitFor(t, "S5 rows gone", 5*time.Second, func() bool {
		n, _ := rc.Exists(context.Background(), sub5).Result()
		return n == 0 && hgetInt(rc, serverKey, "connected_subscriptions") == 1
	})
	exe_cmd(t, "redis-cli -n 4 del TELEMETRY_CLIENT|DestinationGroup_G9")

	// An OC_YANG entry's path row is the bare path, as on the dial-in side.
	exe_cmd(t, "redis-cli -n 4 hmset TELEMETRY_CLIENT|Subscription_S6 path_target OC_YANG dst_group G1 report_type stream paths /interfaces/interface[name=Ethernet0]/state/counters")
	waitFor(t, "S6 path row", 5*time.Second, func() bool {
		return hget(rc, "GNMI_SUBSCRIPTION_PATH|dialout_S6|0", "path") == "/interfaces/interface[name=Ethernet0]/state/counters"
	})
	exe_cmd(t, "redis-cli -n 4 del TELEMETRY_CLIENT|Subscription_S6")
	waitFor(t, "S6 rows gone", 5*time.Second, func() bool {
		n, _ := rc.Exists(context.Background(), "GNMI_SUBSCRIPTION|dialout_S6").Result()
		return n == 0
	})

	// An entry the switch cannot serve: an unknown target, so the data
	// source cannot be opened. The row says why.
	exe_cmd(t, "redis-cli -n 4 hmset TELEMETRY_CLIENT|Subscription_S3 path_target NO_SUCH_DB dst_group G1 report_type periodic paths COUNTERS/Ethernet68")
	sub3 := "GNMI_SUBSCRIPTION|dialout_S3"
	waitFor(t, "S3 source failure", 10*time.Second, func() bool {
		return strings.Contains(hget(rc, sub3, "last_error"), "cannot open the data source") && hget(rc, sub3, "state") == "connecting"
	})
	exe_cmd(t, "redis-cli -n 4 del TELEMETRY_CLIENT|Subscription_S3")
	waitFor(t, "S3 rows gone", 5*time.Second, func() bool {
		n, _ := rc.Exists(context.Background(), sub3).Result()
		return n == 0 && hgetInt(rc, serverKey, "configured_subscriptions") == 1
	})

	configuredBefore := hgetInt(rc, serverKey, "configured_subscriptions")

	// Removing the destination group from a live entry stops it: its
	// instance is taken down and its rows go.
	exe_cmd(t, "redis-cli -n 4 hmset TELEMETRY_CLIENT|Subscription_S12 path_target COUNTERS_DB dst_group G1 report_type periodic report_interval 1000 paths COUNTERS/Ethernet68")
	sub12 := "GNMI_SUBSCRIPTION|dialout_S12"
	waitFor(t, "S12 connected", 15*time.Second, func() bool {
		return hget(rc, sub12, "state") == "connected" && hgetInt(rc, serverKey, "configured_subscriptions") == configuredBefore+1
	})
	exe_cmd(t, "redis-cli -n 4 hdel TELEMETRY_CLIENT|Subscription_S12 dst_group")
	waitFor(t, "S12 rows gone once its group is removed", 5*time.Second, func() bool {
		n, _ := rc.Exists(context.Background(), sub12).Result()
		return n == 0 && hgetInt(rc, serverKey, "configured_subscriptions") == configuredBefore
	})
	configMu.Lock()
	_, running := ClientSubscriptionNameMap["S12"]
	configMu.Unlock()
	if running {
		t.Errorf("S12 still has an instance after its destination group was removed")
	}
	exe_cmd(t, "redis-cli -n 4 del TELEMETRY_CLIENT|Subscription_S12")

	// An entry with an unknown target: the row explains.
	exe_cmd(t, "redis-cli -n 4 hmset TELEMETRY_CLIENT|Subscription_S8 path_target NOT_A_DB dst_group G1 report_type periodic report_interval 1000 paths COUNTERS/Ethernet68")
	sub8 := "GNMI_SUBSCRIPTION|dialout_S8"
	waitFor(t, "S8 unknown target", 10*time.Second, func() bool {
		return strings.Contains(hget(rc, sub8, "last_error"), "Invalid target dbName")
	})
	exe_cmd(t, "redis-cli -n 4 del TELEMETRY_CLIENT|Subscription_S8")
	waitFor(t, "S8 rows gone", 5*time.Second, func() bool {
		n, _ := rc.Exists(context.Background(), sub8).Result()
		return n == 0
	})

	// Removing one field of a configured entry is an update, not a removal:
	// the entry stays configured and its row stays.
	exe_cmd(t, "redis-cli -n 4 hdel TELEMETRY_CLIENT|Subscription_S1 report_interval")
	settle()
	if n, _ := rc.Exists(context.Background(), subKey).Result(); n != 1 || hgetInt(rc, serverKey, "configured_subscriptions") != configuredBefore {
		t.Errorf("removing one field removed the entry: row exists %d, configured %d", n, hgetInt(rc, serverKey, "configured_subscriptions"))
	}
	exe_cmd(t, "redis-cli -n 4 hset TELEMETRY_CLIENT|Subscription_S1 report_interval 1000")
	waitFor(t, "S1 connected after the field edits", 15*time.Second, func() bool { return hget(rc, subKey, "state") == "connected" })

	// A report type the client does not run is refused when parsed: the
	// entry gets no row and is not counted.
	exe_cmd(t, "redis-cli -n 4 hmset TELEMETRY_CLIENT|Subscription_S4 path_target COUNTERS_DB dst_group G1 report_type once paths COUNTERS/Ethernet68")
	settle()
	if n, _ := rc.Exists(context.Background(), "GNMI_SUBSCRIPTION|dialout_S4").Result(); n != 0 || hgetInt(rc, serverKey, "configured_subscriptions") != 1 {
		t.Errorf("a report_type once entry was accepted: row exists %d, configured %d", n, hgetInt(rc, serverKey, "configured_subscriptions"))
	}
	exe_cmd(t, "redis-cli -n 4 del TELEMETRY_CLIENT|Subscription_S4")
	settle()

	// An unreachable collector: the row stays, connecting, with the attempts
	// and the last error.
	exe_cmd(t, "redis-cli -n 4 hset TELEMETRY_CLIENT|DestinationGroup_G2 dst_addr 127.0.0.1:1")
	exe_cmd(t, "redis-cli -n 4 hmset TELEMETRY_CLIENT|Subscription_S2 path_target COUNTERS_DB dst_group G2 report_type stream paths COUNTERS/Ethernet68")
	sub2 := "GNMI_SUBSCRIPTION|dialout_S2"
	waitFor(t, "S2 dial failures", 15*time.Second, func() bool {
		return hget(rc, sub2, "state") == "connecting" && hgetInt(rc, sub2, "connect_attempts") >= 2 && hget(rc, sub2, "last_error") != ""
	})
	waitFor(t, "S2 error_count counts the failed dials", 5*time.Second, func() bool { return hgetInt(rc, sub2, "error_count") >= 2 })
	if le := hget(rc, sub2, "last_error"); !strings.Contains(le, "Dial") {
		t.Errorf("S2 last_error = %q", le)
	}
	if hget(rc, sub2, "peer") != "127.0.0.1:1" {
		t.Errorf("S2 peer = %q", hget(rc, sub2, "peer"))
	}
	if hget(rc, "GNMI_SUBSCRIPTION_PATH|dialout_S2|0", "mode") != "ON_CHANGE" {
		t.Errorf("S2 path mode = %q, want ON_CHANGE for a streaming report", hget(rc, "GNMI_SUBSCRIPTION_PATH|dialout_S2|0", "mode"))
	}
	if hgetInt(rc, serverKey, "configured_subscriptions") != 2 || hgetInt(rc, serverKey, "connected_subscriptions") != 1 {
		t.Errorf("server counts = configured %d connected %d, want 2 / 1", hgetInt(rc, serverKey, "configured_subscriptions"), hgetInt(rc, serverKey, "connected_subscriptions"))
	}

	// A connected stream entry is removed with its rows.
	exe_cmd(t, "redis-cli -n 4 hmset TELEMETRY_CLIENT|Subscription_S10 path_target COUNTERS_DB dst_group G1 report_type stream paths COUNTERS/Ethernet68")
	sub10 := "GNMI_SUBSCRIPTION|dialout_S10"
	waitFor(t, "S10 stream connected", 15*time.Second, func() bool { return hget(rc, sub10, "state") == "connected" })
	exe_cmd(t, "redis-cli -n 4 del TELEMETRY_CLIENT|Subscription_S10")
	waitFor(t, "S10 rows gone", 5*time.Second, func() bool {
		n, _ := rc.Exists(context.Background(), sub10).Result()
		return n == 0
	})
	// A streaming report to a live collector: the row reads connected, and
	// when the collector goes away the row goes back to connecting.
	exe_cmd(t, "redis-cli -n 4 hmset TELEMETRY_CLIENT|Subscription_S7 path_target COUNTERS_DB dst_group G1 report_type stream paths COUNTERS/Ethernet68")
	sub7 := "GNMI_SUBSCRIPTION|dialout_S7"
	waitFor(t, "S7 stream connected", 15*time.Second, func() bool {
		return hget(rc, sub7, "state") == "connected" && hgetInt(rc, serverKey, "connected_subscriptions") == 2
	})
	collector2.Stop()
	staleLocal := ""
	waitFor(t, "S7 stream broken", 15*time.Second, func() bool {
		row, _ := rc.HGetAll(context.Background(), sub7).Result()
		if row["state"] == "connecting" && row["local_address"] != "" {
			staleLocal = row["local_address"]
		}
		return row["state"] == "connecting" && hgetInt(rc, serverKey, "connected_subscriptions") == 0
	})
	if staleLocal != "" {
		t.Errorf("S7 read connecting with the closed connection's local address %s", staleLocal)
	}
	// While it waits to retry, the row shows no local address: that
	// connection is closed.
	waitFor(t, "S7 local address cleared", 5*time.Second, func() bool { return hget(rc, sub7, "local_address") == "" })
	exe_cmd(t, "redis-cli -n 4 del TELEMETRY_CLIENT|Subscription_S7")
	waitFor(t, "S7 rows gone", 5*time.Second, func() bool {
		n, _ := rc.Exists(context.Background(), sub7).Result()
		return n == 0
	})

	// Moving an entry to another group takes it off the old group's list:
	// an edit to the old group afterwards neither panics nor touches it.
	exe_cmd(t, "redis-cli -n 4 hset TELEMETRY_CLIENT|DestinationGroup_G3 dst_addr 127.0.0.1:8085")
	exe_cmd(t, "redis-cli -n 4 hset TELEMETRY_CLIENT|Subscription_S1 dst_group G3")
	waitFor(t, "S1 on G3", 10*time.Second, func() bool { return hget(rc, subKey, "destination_group") == "G3" })
	exe_cmd(t, "redis-cli -n 4 hset TELEMETRY_CLIENT|DestinationGroup_G1 dst_addr 127.0.0.1:8086")
	exe_cmd(t, "redis-cli -n 4 del TELEMETRY_CLIENT|DestinationGroup_G1")
	settle()
	if hget(rc, subKey, "destination_group") != "G3" || hgetInt(rc, serverKey, "configured_subscriptions") != 2 {
		t.Errorf("editing the old group disturbed the moved entry: group %q, configured %d", hget(rc, subKey, "destination_group"), hgetInt(rc, serverKey, "configured_subscriptions"))
	}

	// Removing the entries removes the rows; nothing survives a normal close.
	exe_cmd(t, "redis-cli -n 4 del TELEMETRY_CLIENT|Subscription_S1")
	waitFor(t, "S1 rows gone", 5*time.Second, func() bool {
		n, _ := rc.Exists(context.Background(), subKey, "GNMI_SUBSCRIPTION_PATH|dialout_S1|0").Result()
		return n == 0 && hgetInt(rc, serverKey, "configured_subscriptions") == 1 && hgetInt(rc, serverKey, "connected_subscriptions") == 0
	})
	exe_cmd(t, "redis-cli -n 4 del TELEMETRY_CLIENT|Subscription_S2")
	waitFor(t, "S2 rows gone", 5*time.Second, func() bool {
		n, _ := rc.Exists(context.Background(), sub2, "GNMI_SUBSCRIPTION_PATH|dialout_S2|0").Result()
		return n == 0 && hgetInt(rc, serverKey, "configured_subscriptions") == 0
	})
	exe_cmd(t, "redis-cli -n 4 del TELEMETRY_CLIENT|DestinationGroup_G2")
	exe_cmd(t, "redis-cli -n 4 del TELEMETRY_CLIENT|DestinationGroup_G3")

	// Removing one Global field is an update: that field goes back to the
	// value the run started with.
	retry := func() time.Duration {
		configMu.Lock()
		defer configMu.Unlock()
		return clientCfg.RetryInterval
	}
	exe_cmd(t, "redis-cli -n 4 hmset TELEMETRY_CLIENT|Global retry_interval 7 unidirectional true")
	waitFor(t, "Global retry_interval applied", 5*time.Second, func() bool { return retry() == 7*time.Second })
	exe_cmd(t, "redis-cli -n 4 hdel TELEMETRY_CLIENT|Global retry_interval")
	waitFor(t, "Global retry_interval back to the run's value", 5*time.Second, func() bool { return retry() == startRetry })
	// Deleting the whole key is the same: every field goes back.
	exe_cmd(t, "redis-cli -n 4 hset TELEMETRY_CLIENT|Global retry_interval 9")
	waitFor(t, "Global retry_interval applied again", 5*time.Second, func() bool { return retry() == 9*time.Second })
	exe_cmd(t, "redis-cli -n 4 del TELEMETRY_CLIENT|Global")
	waitFor(t, "Global retry_interval back after the key is deleted", 5*time.Second, func() bool { return retry() == startRetry })

	// A stopped client removes the rows of the entries still configured.
	// The server row stays, its last_updated no longer moving, so a reader
	// can tell the client stopped from one that never ran.
	exe_cmd(t, "redis-cli -n 4 hset TELEMETRY_CLIENT|DestinationGroup_G13 dst_addr 127.0.0.1:8084")
	exe_cmd(t, "redis-cli -n 4 hmset TELEMETRY_CLIENT|Subscription_S13 path_target COUNTERS_DB dst_group G13 report_type periodic report_interval 1000 paths COUNTERS/Ethernet68")
	sub13 := "GNMI_SUBSCRIPTION|dialout_S13"
	waitFor(t, "S13 connected", 15*time.Second, func() bool { return hget(rc, sub13, "state") == "connected" })
	stopRun()
	if n, _ := rc.Exists(context.Background(), sub13, "GNMI_SUBSCRIPTION_PATH|dialout_S13|0").Result(); n != 0 {
		t.Errorf("%d rows of a configured entry survived the client's stop", n)
	}
	if hget(rc, serverKey, "pid") == "" {
		t.Errorf("the server row was removed on stop")
	}
	exe_cmd(t, "redis-cli -n 4 del TELEMETRY_CLIENT|Subscription_S13")
	exe_cmd(t, "redis-cli -n 4 del TELEMETRY_CLIENT|DestinationGroup_G13")
	cdb.HDel(context.Background(), "GNMI|gnmi", "state_publish_interval")
}

// A run stops the publisher it started, not whichever is current: a late
// run must not uninstall a newer run's publisher.
func TestDialOutStatePublisherScoping(t *testing.T) {
	st1 := startStatePublisher()
	if st1 == nil {
		t.Skip("STATE_DB not available")
	}
	st2 := startStatePublisher()
	if currentStatePublisher() != st2 || !st1.pub.Stopped() {
		t.Fatalf("second publisher did not take over")
	}
	stopStatePublisher(st1)
	if currentStatePublisher() != st2 {
		t.Errorf("stopping a superseded publisher uninstalled the current one")
	}
	stopStatePublisher(st2)
	if currentStatePublisher() != nil || !st2.pub.Stopped() {
		t.Errorf("current publisher not stopped and uninstalled")
	}
}

// A run whose publisher a newer run has replaced stops it without
// deleting rows: the ids are the same in both runs, so the delete would
// take the newer run's rows.
func TestDialOutSupersededStopKeepsRows(t *testing.T) {
	st := startStatePublisher()
	if st == nil {
		t.Skip("STATE_DB not available")
	}
	rc := stateDbClient(t)
	defer rc.Close()
	key := "GNMI_SUBSCRIPTION|dialout_SX"
	defer rc.Del(context.Background(), key)
	st.pub.WriteRows("dialout_SX", map[string]substate.Row{key: {"state": "connecting"}}, nil)
	waitFor(t, "SX row", 5*time.Second, func() bool { return hget(rc, key, "state") == "connecting" })
	newer := &statePublisher{}
	statePub.Store(newer)
	defer statePub.CompareAndSwap(newer, nil)
	stopStatePublisher(st)
	if !st.pub.Stopped() {
		t.Errorf("the superseded publisher was not stopped")
	}
	if n, _ := rc.Exists(context.Background(), key).Result(); n != 1 {
		t.Errorf("stopping a superseded publisher deleted a row the newer run shares")
	}
}

// The path rows' shape: OTHERS renders target-first as the host backend,
// and a report type that is not a stream, once or unknown, carries no mode.
func TestDialOutPathRows(t *testing.T) {
	pub := substate.NewWithClient(substate.ServerDialout, nil, "|")
	path := &pb.Path{Elem: []*pb.PathElem{{Name: "proc"}, {Name: "meminfo"}}}
	cs := &clientSubscription{entryState: &entryState{}, name: "S", prefix: &pb.Path{Target: "OTHERS"}, paths: []*pb.Path{path},
		reportType: Stream, interval: 2 * time.Second}
	// A stream takes the mode its data client runs.
	for _, tc := range []struct{ target, mode, sample string }{
		{"OTHERS", "SAMPLE", "2000000000"},
		{"OC_YANG", "TARGET_DEFINED", "0"},
		{"COUNTERS_DB", "ON_CHANGE", "0"},
	} {
		cs.prefix = &pb.Path{Target: tc.target}
		row := cs.pathRows(pub)["GNMI_SUBSCRIPTION_PATH|dialout_S|0"]
		if row["mode"] != tc.mode || row["sample_interval_ns"] != tc.sample {
			t.Errorf("%s stream path row = %v, want mode %s, sample_interval_ns %s", tc.target, row, tc.mode, tc.sample)
		}
	}
	cs.prefix = &pb.Path{Target: "OTHERS"}
	row := cs.pathRows(pub)["GNMI_SUBSCRIPTION_PATH|dialout_S|0"]
	if row["path"] != "OTHERS/proc/meminfo" {
		t.Errorf("OTHERS stream path row = %v", row)
	}
	for _, rt := range []reportType{Once, Unknown} {
		cs.reportType = rt
		row = cs.pathRows(pub)["GNMI_SUBSCRIPTION_PATH|dialout_S|0"]
		if len(row) != 2 || row["mode"] != "" {
			t.Errorf("report type %v path row = %v, want the path and an empty mode only", rt, row)
		}
	}
}

// The tick snapshot of an entry that is gone is nil, so its row gets no
// fresh counters or last_updated.
func TestDialOutLiveCounterRowGoneEntry(t *testing.T) {
	if row := liveCounterRow("no-such-entry")(); row != nil {
		t.Errorf("snapshot of a gone entry = %v, want nil", row)
	}
}

// Every teardown retires the instance before closing it, so the send that
// fails on the disposed queue or the closed connection finds it retired.
func TestDialOutRetireThenClose(t *testing.T) {
	cs := &clientSubscription{entryState: &entryState{}, name: "R", opened: true, stop: make(chan struct{}, 1),
		state: stateConnected, local: "10.0.0.1:5000", peer: "10.0.0.2:8081"}
	var openAtRetire bool
	retireThenClose(cs, func(c *clientSubscription) {
		c.cMu.Lock()
		openAtRetire = c.opened
		c.cMu.Unlock()
		c.retireNow()
	})
	if !openAtRetire {
		t.Errorf("the instance was closed before it was retired")
	}
	cs.cMu.Lock()
	open := cs.opened
	cs.cMu.Unlock()
	if open || !cs.isRetired() {
		t.Errorf("after the teardown: open %v, retired %v; want closed and retired", open, cs.isRetired())
	}
	// The row the retire publishes must not pair connecting with the old
	// connection's addresses.
	if row := cs.connectionRow(); row["state"] != stateConnecting || row["local_address"] != "" || row["peer"] != "" {
		t.Errorf("connection row after the retire = %v, want connecting with no local_address and no peer", row)
	}
}

// The list mode is what the report type amounts to; a report type the
// client does not know has none, like its path rows.
func TestDialOutListMode(t *testing.T) {
	for rt, want := range map[reportType]string{Periodic: "STREAM", Stream: "STREAM", Once: "ONCE", Unknown: ""} {
		cs := &clientSubscription{entryState: &entryState{}, reportType: rt}
		if got := cs.listMode(); got != want {
			t.Errorf("listMode(%v) = %q, want %q", rt, got, want)
		}
	}
}

// A dial clears the local address until it connects, so the row never pairs
// a new peer with the last connection's local end.
func TestDialOutDialClearsLocalAddress(t *testing.T) {
	cs := &clientSubscription{name: "D", entryState: &entryState{}}
	cs.setLocal("127.0.0.1:40000")
	stateOnDial(cs, "127.0.0.1:8090")
	cs.connMu.Lock()
	local, peer := cs.local, cs.peer
	cs.connMu.Unlock()
	if local != "" || peer != "127.0.0.1:8090" {
		t.Errorf("after a dial: local %q, peer %q; want no local address and the new peer", local, peer)
	}
}

// A rebuild hands the entry's state to the new instance: every counter and
// the start time carry over without a copy list.
func TestDialOutRebuildSharesEntryState(t *testing.T) {
	defer isolateClientState(t)()
	configMu.Lock()
	old := &clientSubscription{name: "E", destGroupName: "GE", entryState: &entryState{startTime: 42}}
	old.recvMsg.Store(5)
	old.bytes.Store(7, 3)
	ClientSubscriptionNameMap["E"] = old
	DestGrp2ClientSubMap["GE"] = []string{"E"}
	setupDestGroupClients(context.Background(), "GE")
	cs := ClientSubscriptionNameMap["E"]
	configMu.Unlock()
	if cs == old || cs.entryState != old.entryState {
		t.Fatalf("the rebuilt instance does not share the entry's state")
	}
	if tx, rx := cs.bytes.Load(); cs.startTime != 42 || cs.recvMsg.Load() != 5 || tx != 7 || rx != 3 {
		t.Errorf("rebuilt instance: start %d, recvMsg %d, bytes %d/%d", cs.startTime, cs.recvMsg.Load(), tx, rx)
	}
}

// collectorCfg dials the test collectors.
var collectorCfg = ClientConfig{RetryInterval: time.Second, TLS: &tls.Config{InsecureSkipVerify: true}}

// A failed dial is one attempt and one error, and an instance retired
// meanwhile makes no further attempt. A dial cut short by the instance's
// cancel is not an error.
func TestDialOutConnectAttemptsAndErrors(t *testing.T) {
	defer func() { dialCollector = newClient }()
	for _, c := range []struct {
		name      string
		end       func(cs *clientSubscription, cancel context.CancelFunc)
		attempts  uint64
		errsAfter uint64
	}{
		{"retired after a failed dial", func(cs *clientSubscription, _ context.CancelFunc) { cs.retireNow() }, 1, 1},
		{"cancelled during the dial", func(_ *clientSubscription, cancel context.CancelFunc) { cancel() }, 1, 0},
	} {
		ctx, cancel := context.WithCancel(context.Background())
		cs := &clientSubscription{name: "D", entryState: &entryState{}, reportType: Stream, cfg: collectorCfg}
		dialCollector = func(context.Context, ClientConfig, Destination, *substate.Bytes, func(string)) (*Client, error) {
			c.end(cs, cancel)
			return nil, errors.New("connection refused")
		}
		done := make(chan struct{})
		go func() { publishRun(ctx, cs, []Destination{{Addrs: "127.0.0.1:1"}}); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: publishRun did not return", c.name)
		}
		cancel()
		if n := cs.conTryCnt.Load(); n != c.attempts {
			t.Errorf("%s: connect_attempts = %d, want %d", c.name, n, c.attempts)
		}
		if n := cs.errors.Load(); n != c.errsAfter {
			t.Errorf("%s: error_count = %d, want %d", c.name, n, c.errsAfter)
		}
	}
}

// A periodic send that fails because the instance is being stopped is not
// counted, and the row is not set back to connecting with its error.
func TestDialOutSendFailureOnStopNotCounted(t *testing.T) {
	collector := createServer(t, &sds.Config{Port: 8092})
	go runServer(t, collector)
	defer collector.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	periodicGet = func(cs *clientSubscription) ([]*spb.Value, error) {
		cancel() // the stop lands before the send
		return []*spb.Value{{Timestamp: time.Now().UnixNano()}}, nil
	}
	defer func() { periodicGet = func(cs *clientSubscription) ([]*spb.Value, error) { return cs.dc.Get(nil) } }()
	cs := &clientSubscription{name: "P", entryState: &entryState{}, reportType: Periodic, interval: 10 * time.Millisecond,
		cfg: ClientConfig{RetryInterval: time.Second, TLS: &tls.Config{InsecureSkipVerify: true}}}
	done := make(chan struct{})
	go func() { publishRun(ctx, cs, []Destination{{Addrs: "127.0.0.1:8092"}}); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("publishRun did not return after its context was cancelled")
	}
	if n := cs.errors.Load(); n != 0 {
		t.Errorf("error_count = %d after a stop, want 0", n)
	}
	cs.connMu.Lock()
	lastErr := cs.lastErr
	cs.connMu.Unlock()
	if lastErr != "" {
		t.Errorf("last_error %q after a stop, want none", lastErr)
	}
}

// failingPublishClient is a collector whose Publish fails; onPublish runs
// first.
type failingPublishClient struct {
	spb.GNMIDialOutClient
	onPublish func()
}

func (f failingPublishClient) Publish(context.Context, ...grpc.CallOption) (spb.GNMIDialOut_PublishClient, error) {
	f.onPublish()
	return nil, errors.New("publish refused")
}

// A Publish that fails after the dial is one error, with its reason on the
// row and the closed connection's address cleared. One that fails because
// the instance was cancelled is neither counted nor shown.
func TestDialOutPublishFailure(t *testing.T) {
	defer func() { dialCollector = newClient }()
	for _, c := range []struct {
		name        string
		cancelFirst bool
		errs        uint64
		shown       bool
	}{
		{"refused", false, 1, true},
		{"cancelled first", true, 0, false},
	} {
		ctx, cancel := context.WithCancel(context.Background())
		cfg := collectorCfg
		cfg.RetryInterval = 100 * time.Millisecond
		cs := &clientSubscription{name: "PF", entryState: &entryState{}, reportType: Stream, cfg: cfg}
		cs.revive()
		var dials atomic.Int32
		dialCollector = func(_ context.Context, _ ClientConfig, _ Destination, _ *substate.Bytes,
			onConnect func(string)) (*Client, error) {
			if dials.Add(1) > 1 {
				cancel() // one Publish is enough
				return nil, errors.New("not dialled again")
			}
			conn, err := grpc.NewClient("passthrough:///127.0.0.1:1", grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return nil, err
			}
			onConnect("10.0.0.1:5000")
			return &Client{conn: conn, client: failingPublishClient{onPublish: func() {
				if c.cancelFirst {
					cancel()
				}
			}}}, nil
		}
		done := make(chan struct{})
		go func() { publishRun(ctx, cs, []Destination{{Addrs: "127.0.0.1:1"}}); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: publishRun did not return", c.name)
		}
		cancel()
		if n := cs.errors.Load(); n != c.errs {
			t.Errorf("%s: error_count = %d, want %d", c.name, n, c.errs)
		}
		cs.connMu.Lock()
		lastErr, local := cs.lastErr, cs.local
		cs.connMu.Unlock()
		if shown := strings.Contains(lastErr, "publish refused"); shown != c.shown {
			t.Errorf("%s: last_error %q, want the Publish error shown: %v", c.name, lastErr, c.shown)
		}
		if local != "" {
			t.Errorf("%s: local_address %q after the connection closed, want none", c.name, local)
		}
	}
}

// sendFailingClient opens a Publish stream whose every Send fails.
type sendFailingClient struct{ spb.GNMIDialOutClient }

func (sendFailingClient) Publish(context.Context, ...grpc.CallOption) (spb.GNMIDialOut_PublishClient, error) {
	return failingStream{}, nil
}

// A periodic Send that fails while the instance is live is one error, and
// the row shows it.
func TestDialOutPeriodicSendFailureCounted(t *testing.T) {
	defer func() { dialCollector = newClient }()
	periodicGet = func(cs *clientSubscription) ([]*spb.Value, error) {
		return []*spb.Value{{Timestamp: time.Now().UnixNano()}}, nil
	}
	defer func() { periodicGet = func(cs *clientSubscription) ([]*spb.Value, error) { return cs.dc.Get(nil) } }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var dials atomic.Int32
	dialCollector = func(_ context.Context, _ ClientConfig, _ Destination, _ *substate.Bytes,
		onConnect func(string)) (*Client, error) {
		if dials.Add(1) > 1 {
			cancel() // one failed Send is enough
			return nil, errors.New("not dialled again")
		}
		conn, err := grpc.NewClient("passthrough:///127.0.0.1:1", grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		onConnect("10.0.0.1:5000")
		return &Client{conn: conn, client: sendFailingClient{}}, nil
	}
	cfg := collectorCfg
	cfg.RetryInterval = 100 * time.Millisecond
	cs := &clientSubscription{name: "PS", entryState: &entryState{}, reportType: Periodic, interval: 10 * time.Millisecond, cfg: cfg}
	cs.revive()
	done := make(chan struct{})
	go func() { publishRun(ctx, cs, []Destination{{Addrs: "127.0.0.1:1"}}); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("publishRun did not return")
	}
	if n := cs.errors.Load(); n != 1 {
		t.Errorf("error_count = %d, want 1", n)
	}
	cs.connMu.Lock()
	lastErr := cs.lastErr
	cs.connMu.Unlock()
	if !strings.Contains(lastErr, "transport is closing") {
		t.Errorf("last_error %q, want the Send error", lastErr)
	}
}

// An entry with a report type the client does not serve is not started,
// and its row stays at connecting with last_error naming the type.
func TestDialOutUnsupportedReportType(t *testing.T) {
	defer isolateClientState(t)()
	destGrpNameMap["GU"] = []Destination{{Addrs: "127.0.0.1:1"}}
	cs := &clientSubscription{name: "U", destGroupName: "GU", entryState: &entryState{}, reportType: Once,
		prefix: &pb.Path{Target: "COUNTERS_DB"}, paths: []*pb.Path{{Elem: []*pb.PathElem{{Name: "COUNTERS"}}}}}
	cs.revive()
	err := cs.NewInstance(context.Background(), *clientCfg)
	if err == nil || !strings.Contains(err.Error(), "Unsupported report type") {
		t.Fatalf("NewInstance = %v, want the report type refused", err)
	}
	if cs.serving() {
		t.Errorf("an entry with an unsupported report type was started")
	}
	cs.connMu.Lock()
	state, lastErr := cs.state, cs.lastErr
	cs.connMu.Unlock()
	if state != stateConnecting || !strings.Contains(lastErr, "report_type") {
		t.Errorf("row state %q, last_error %q; want connecting with the report type named", state, lastErr)
	}
}

// A retired instance writes nothing to the row: once retireNow has
// returned, it cannot overwrite what its replacement wrote.
func TestDialOutRetiredInstancePublishesNothing(t *testing.T) {
	defer isolateClientState(t)()
	st := startStatePublisher()
	if st == nil {
		t.Skip("STATE_DB not available")
	}
	defer stopStatePublisher(st)
	rc := stateDbClient(t)
	defer rc.Close()
	entry := func(name string, es *entryState) *clientSubscription {
		return &clientSubscription{name: name, destGroupName: "G", entryState: es, reportType: Stream,
			prefix: &pb.Path{Target: "COUNTERS_DB"}, paths: []*pb.Path{{Elem: []*pb.PathElem{{Name: "COUNTERS"}}}}}
	}
	es := &entryState{}
	replacement := entry("RP", es)
	stateOnConfigured(replacement)
	marker := entry("RP_MARK", &entryState{})
	stateOnConfigured(marker)
	key := st.pub.Key(substate.SubscriptionTable, replacement.stateID())
	markKey := st.pub.Key(substate.SubscriptionTable, marker.stateID())
	waitFor(t, "the rows", 5*time.Second, func() bool {
		return hget(rc, key, "state") == stateConnecting && hget(rc, markKey, "state") == stateConnecting
	})

	old := entry("RP", es)
	old.retireNow()
	old.connMu.Lock()
	old.state, old.lastErr = stateConnected, "stale"
	old.connMu.Unlock()
	old.publishConnection()
	// The publisher runs its jobs in order: once the marker's write has
	// landed, any write the retired instance queued has too.
	marker.setLastErr(errors.New("marker"))
	marker.publishConnection()
	waitFor(t, "the marker write", 5*time.Second, func() bool { return hget(rc, markKey, "last_error") == "marker" })
	if s, e := hget(rc, key, "state"), hget(rc, key, "last_error"); s != stateConnecting || e == "stale" {
		t.Errorf("row after a retired instance published: state %q, last_error %q; want the replacement's", s, e)
	}
}

// idleSource is a stream data client that sends nothing until it is stopped.
type idleSource struct{ sdc.Client }

func (idleSource) StreamRun(q *queue.PriorityQueue, stop chan struct{}, w *sync.WaitGroup, _ *pb.SubscriptionList) {
	defer w.Done()
	<-stop
}

func (idleSource) Close() error { return nil }

// A collector that ends an idle stream is one error, and the row goes to
// connecting with it, though send sees only its queue closed.
func TestDialOutStreamClosedByCollector(t *testing.T) {
	addr := startClosingCollector(t)
	cfg := collectorCfg
	cfg.RetryInterval = time.Minute // the stream is ended once before the checks
	cs := &clientSubscription{name: "C", entryState: &entryState{}, reportType: Stream, dc: idleSource{}, cfg: cfg}
	cs.revive()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { publishRun(ctx, cs, []Destination{{Addrs: addr}}); close(done) }()
	waitFor(t, "the collector to end the stream", 10*time.Second, func() bool { return cs.errors.Load() > 0 })
	cs.connMu.Lock()
	state, lastErr := cs.state, cs.lastErr
	cs.connMu.Unlock()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("publishRun did not return after cancel")
	}
	if n := cs.errors.Load(); n != 1 {
		t.Errorf("error_count = %d, want 1", n)
	}
	if state != stateConnecting || lastErr == "" {
		t.Errorf("row state %q, last_error %q; want connecting with the collector's error", state, lastErr)
	}
}
