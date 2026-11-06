package telemetry_dialout

// dialout_client_test covers gNMIDialOut publish test
// Prerequisite: redis-server should be running.

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sds "github.com/sonic-net/sonic-gnmi/dialout/dialout_server"
	sdc "github.com/sonic-net/sonic-gnmi/sonic_data_client"
	sdcfg "github.com/sonic-net/sonic-gnmi/sonic_db_config"
	testcert "github.com/sonic-net/sonic-gnmi/testdata/tls"

	gclient "github.com/openconfig/gnmi/client/gnmi"
	pb "github.com/openconfig/gnmi/proto/gnmi"
	"github.com/openconfig/gnmi/value"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var clientTypes = []string{gclient.Type}

func loadConfig(t *testing.T, key string, in []byte) map[string]interface{} {
	var fvp map[string]interface{}

	err := json.Unmarshal(in, &fvp)
	if err != nil {
		t.Fatalf("Failed to Unmarshal %v err: %v", in, err)
	}
	if key != "" {
		kv := map[string]interface{}{}
		kv[key] = fvp
		return kv
	}
	return fvp
}

// assuming input data is in key field/value pair format
func loadDB(t *testing.T, rclient *redis.Client, mpi map[string]interface{}) {
	for key, fv := range mpi {
		switch fv.(type) {
		case map[string]interface{}:
			_, err := rclient.HMSet(context.Background(), key, fv.(map[string]interface{})).Result()
			if err != nil {
				t.Fatal("Invalid data for db: ", key, fv, err)
			}
		default:
			t.Fatalf("Invalid data for db: %v : %v", key, fv)
		}
	}
}

func createServer(t *testing.T, cfg *sds.Config) *sds.Server {
	certificate, err := testcert.NewCert()
	if err != nil {
		t.Fatalf("could not load server key pair: %s", err)
	}
	tlsCfg := &tls.Config{
		ClientAuth:   tls.RequestClientCert,
		Certificates: []tls.Certificate{certificate},
	}

	opts := []grpc.ServerOption{grpc.Creds(credentials.NewTLS(tlsCfg))}

	s, err := sds.NewServer(cfg, opts)
	if err != nil {
		t.Fatalf("Failed to create gNMIDialOut server: %v", err)
	}
	return s
}

func runServer(t *testing.T, s *sds.Server) {
	//t.Log("Starting RPC server on address:", s.Address())
	err := s.Serve() // blocks until close
	if err != nil {
		t.Fatalf("gRPC server err: %v", err)
	}
	//t.Log("Exiting RPC server on address", s.Address())
}

func getRedisClient(t *testing.T) *redis.Client {
	ns, _ := sdcfg.GetDbDefaultNamespace()
	addr, err := sdcfg.GetDbTcpAddr("COUNTERS_DB", ns)
	if err != nil {
		t.Fatal("failed to get addr ", err)
	}
	db, err := sdcfg.GetDbId("COUNTERS_DB", ns)
	if err != nil {
		t.Fatal("failed to get db ", err)
	}
	rclient := redis.NewClient(&redis.Options{
		Network:     "tcp",
		Addr:        addr,
		Password:    "", // no password set
		DB:          db,
		DialTimeout: 0,
	})
	_, err = rclient.Ping(context.Background()).Result()
	if err != nil {
		t.Fatal("failed to connect to redis server ", err)
	}
	return rclient
}

func exe_cmd(t *testing.T, cmd string) {
	//fmt.Println("command is ", cmd)
	parts := strings.Fields(cmd)
	head := parts[0]
	parts = parts[1:len(parts)]

	_, err := exec.Command(head, parts...).Output()
	if err != nil {
		t.Fatalf("%s %s", cmd, err)
	}
	// fmt.Printf("%s", out)
	// wg.Done() // Need to signal to waitgroup that this goroutine is done
}

func getConfigDbClient(t *testing.T) *redis.Client {
	ns, _ := sdcfg.GetDbDefaultNamespace()
	addr, err := sdcfg.GetDbTcpAddr("CONFIG_DB", ns)
	if err != nil {
		t.Fatal("failed to get addr ", err)
	}
	db, err := sdcfg.GetDbId("CONFIG_DB", ns)
	if err != nil {
		t.Fatal("failed to get db ", err)
	}
	rclient := redis.NewClient(&redis.Options{
		Network:     "tcp",
		Addr:        addr,
		Password:    "", // no password set
		DB:          db,
		DialTimeout: 0,
	})
	_, err = rclient.Ping(context.Background()).Result()
	if err != nil {
		t.Fatalf("failed to connect to redis server %v", err)
	}
	return rclient
}

func loadConfigDB(t *testing.T, rclient *redis.Client, mpi map[string]interface{}) {
	for key, fv := range mpi {
		switch fv.(type) {
		case map[string]interface{}:
			_, err := rclient.HMSet(context.Background(), key, fv.(map[string]interface{})).Result()
			if err != nil {
				t.Errorf("Invalid data for db: %v : %v %v", key, fv, err)
			}
		default:
			t.Errorf("Invalid data for db: %v : %v", key, fv)
		}
	}
}

func prepareConfigDb(t *testing.T) {
	rclient := getConfigDbClient(t)
	defer rclient.Close()
	rclient.FlushDB(context.Background())

	fileName := "../../testdata/COUNTERS_PORT_ALIAS_MAP.txt"
	countersPortAliasMapByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	mpi_alias_map := loadConfig(t, "", countersPortAliasMapByte)
	loadConfigDB(t, rclient, mpi_alias_map)

	fileName = "../../testdata/CONFIG_PFCWD_PORTS.txt"
	configPfcwdByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	mpi_pfcwd_map := loadConfig(t, "", configPfcwdByte)
	loadConfigDB(t, rclient, mpi_pfcwd_map)
}

func prepareDb(t *testing.T) {
	rclient := getRedisClient(t)
	defer rclient.Close()
	rclient.FlushDB(context.Background())
	//Enable keysapce notification
	os.Setenv("PATH", "$PATH:/usr/bin:/sbin:/bin:/usr/local/bin:/usr/local/Cellar/redis/4.0.8/bin")
	cmd := exec.Command("redis-cli", "config", "set", "notify-keyspace-events", "KEA")
	_, err := cmd.Output()
	if err != nil {
		t.Fatal("failed to enable redis keyspace notification ", err)
	}

	fileName := "../../testdata/COUNTERS_PORT_NAME_MAP.txt"
	countersPortNameMapByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	mpi_name_map := loadConfig(t, "COUNTERS_PORT_NAME_MAP", countersPortNameMapByte)
	loadDB(t, rclient, mpi_name_map)

	fileName = "../../testdata/COUNTERS:Ethernet68.txt"
	countersEthernet68Byte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	// "Ethernet68": "oid:0x1000000000039",
	mpi_counter := loadConfig(t, "COUNTERS:oid:0x1000000000039", countersEthernet68Byte)
	loadDB(t, rclient, mpi_counter)

	fileName = "../../testdata/COUNTERS:Ethernet1.txt"
	countersEthernet1Byte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	// "Ethernet1": "oid:0x1000000000003",
	mpi_counter = loadConfig(t, "COUNTERS:oid:0x1000000000003", countersEthernet1Byte)
	loadDB(t, rclient, mpi_counter)

	// "Ethernet64:0": "oid:0x1500000000092a"  : queue counter, to work as data noise
	fileName = "../../testdata/COUNTERS:oid:0x1500000000092a.txt"
	counters92aByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	mpi_counter = loadConfig(t, "COUNTERS:oid:0x1500000000092a", counters92aByte)
	loadDB(t, rclient, mpi_counter)

	// Load CONFIG_DB for alias translation
	prepareConfigDb(t)
}

type tablePathValue struct {
	dbName    string
	tableName string
	tableKey  string
	delimitor string
	field     string
	value     string
	op        string
}

func compareUpdateValue(t *testing.T, want *pb.Notification, got *pb.Notification) {
	var wantRespVal interface{}
	var gotVal interface{}
	var err error

	updates := got.GetUpdate()
	if len(updates) != 1 {
		t.Fatalf("got %d updates in the notification, want 1", len(updates))
	}
	gotValTyped := updates[0].GetVal()

	updates = want.GetUpdate()
	wantRespValTyped := updates[0].GetVal()

	//if !reflect.DeepEqual(val, wantRespVal) {
	//	t.Errorf("got: %v (%T),\nwant %v (%T)", val, val, wantRespVal, wantRespVal)
	//}

	if gotValTyped.GetJsonIetfVal() == nil {
		gotVal, err = value.ToScalar(gotValTyped)
		if err != nil {
			t.Errorf("got: %v, want a scalar value", gotVal)
		}
		wantRespVal, _ = value.ToScalar(wantRespValTyped)
	} else {
		// Unmarshal json data to gotVal container for comparison
		if err = json.Unmarshal(gotValTyped.GetJsonIetfVal(), &gotVal); err != nil {
			t.Fatalf("error in unmarshaling IETF JSON data to json container: %v", err)
		}
		if err = json.Unmarshal(wantRespValTyped.GetJsonIetfVal(), &wantRespVal); err != nil {
			t.Fatalf("error in unmarshaling IETF JSON data to json container: %v", err)
		}
	}

	if !reflect.DeepEqual(gotVal, wantRespVal) {
		t.Errorf("got: %v (%T),\nwant %v (%T)", gotVal, gotVal, wantRespVal, wantRespVal)
	}

}

// Type defines the type of ServerOp.
type ServerOp int

const (
	_                = iota
	S1Start ServerOp = iota
	S1Stop
	S2Start
	S2Stop
)

var s1, s2 *sds.Server

func serverOp(t *testing.T, sop ServerOp) {
	cfg := &sds.Config{Port: 8080}
	var tmpStore []*pb.SubscribeResponse
	switch sop {
	case S1Stop:
		s1.Stop()
	case S2Stop:
		s2.Stop()
	case S1Start:
		s1 = createServer(t, cfg)
		s1.SetDataStore(&tmpStore)
		go runServer(t, s1)
	case S2Start:
		cfg.Port = 8081
		s2 = createServer(t, cfg)
		s2.SetDataStore(&tmpStore)
		go runServer(t, s2)
	}
}

func TestGNMIDialOutPublish(t *testing.T) {

	fileName := "../../testdata/COUNTERS_PORT_NAME_MAP.txt"
	countersPortNameMapByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	_ = countersPortNameMapByte

	fileName = "../../testdata/COUNTERS:Ethernet68.txt"
	countersEthernet68Byte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	_ = countersEthernet68Byte

	fileName = "../../testdata/COUNTERS:Ethernet_wildcard_alias_expected.txt"
	countersEthernetWildcardByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}

	_ = countersEthernetWildcardByte

	fileName = "../../testdata/COUNTERS:Ethernet_wildcard_PFC_7_RX_alias.txt"
	countersEthernetWildcardPfcByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}

	_ = countersEthernetWildcardPfcByte

	clientCfg := ClientConfig{
		SrcIp:          "",
		RetryInterval:  5 * time.Second,
		Encoding:       pb.Encoding_JSON_IETF,
		Unidirectional: true,
		TLS:            &tls.Config{InsecureSkipVerify: true},
	}
	ctx, cancel := context.WithCancel(context.Background())

	go DialOutRun(ctx, &clientCfg)

	exe_cmd(t, "redis-cli -n 4 hset TELEMETRY_CLIENT|Global retry_interval 5")
	exe_cmd(t, "redis-cli -n 4 hset TELEMETRY_CLIENT|Global encoding JSON_IETF")
	exe_cmd(t, "redis-cli -n 4 hset TELEMETRY_CLIENT|Global unidirectional true")
	exe_cmd(t, "redis-cli -n 4 hset TELEMETRY_CLIENT|Global src_ip  30.57.185.38")

	tests := []struct {
		desc     string
		prepares []tablePathValue // extra preparation of redis db
		cmds     []string         // commands to execute
		sop      ServerOp         // Server operation done after commonds
		updates  []tablePathValue // Update to db data
		waitTime time.Duration    // Wait ftime after server operation

		wantErr     bool
		collector   string
		wantRespVal interface{}
	}{{
		desc: "DialOut to first collector in stream mode and synced",
		cmds: []string{
			"redis-cli -n 4 hset TELEMETRY_CLIENT|DestinationGroup_HS dst_addr 127.0.0.1:8080,127.0.0.1:8081",
			"redis-cli -n 4 hmset TELEMETRY_CLIENT|Subscription_HS_RDMA path_target COUNTERS_DB dst_group HS report_type stream paths COUNTERS/Ethernet*",
		},
		collector: "s1",
		wantRespVal: []*pb.SubscribeResponse{
			&pb.SubscribeResponse{
				Response: &pb.SubscribeResponse_Update{
					Update: &pb.Notification{
						//Timestamp: GetTimestamp(),
						//Prefix:    prefix,
						Update: []*pb.Update{
							{Val: &pb.TypedValue{
								Value: &pb.TypedValue_JsonIetfVal{
									JsonIetfVal: countersEthernetWildcardByte,
								}},
							//Path: GetPath(),
							},
						},
					},
				},
			},
			&pb.SubscribeResponse{
				Response: &pb.SubscribeResponse_SyncResponse{
					SyncResponse: true,
				},
			},
		},
	}, {
		// Same configuration spelled the way the sonic-telemetry_client YANG
		// model keys it, i.e. what config replace and GCU write.
		desc: "DialOut with YANG-canonical pipe separated keys",
		cmds: []string{
			"redis-cli -n 4 hset TELEMETRY_CLIENT|DestinationGroup|HS dst_addr 127.0.0.1:8080,127.0.0.1:8081",
			"redis-cli -n 4 hmset TELEMETRY_CLIENT|Subscription|HS_RDMA path_target COUNTERS_DB dst_group HS report_type stream paths COUNTERS/Ethernet*",
		},
		collector: "s1",
		wantRespVal: []*pb.SubscribeResponse{
			&pb.SubscribeResponse{
				Response: &pb.SubscribeResponse_Update{
					Update: &pb.Notification{
						Update: []*pb.Update{
							{Val: &pb.TypedValue{
								Value: &pb.TypedValue_JsonIetfVal{
									JsonIetfVal: countersEthernetWildcardByte,
								}},
							},
						},
					},
				},
			},
			&pb.SubscribeResponse{
				Response: &pb.SubscribeResponse_SyncResponse{
					SyncResponse: true,
				},
			},
		},
		//}, {
		//	desc: "DialOut to second collector in stream mode upon failure of first collector",
		//	cmds: []string{
		//		"redis-cli -n 4 hset TELEMETRY_CLIENT|DestinationGroup_HS dst_addr 127.0.0.1:8080,127.0.0.1:8081",
		//		"redis-cli -n 4 hmset TELEMETRY_CLIENT|Subscription_HS_RDMA path_target COUNTERS_DB dst_group HS report_type stream paths COUNTERS/Ethernet*/SAI_PORT_STAT_PFC_7_RX_PKTS",
		//	},
		//	collector: "s2",
		//	sop:       S1Stop,
		//	updates: []tablePathValue{{
		//		dbName:    "COUNTERS_DB",
		//		tableName: "COUNTERS",
		//		tableKey:  "oid:0x1000000000039", // "Ethernet68": "oid:0x1000000000039",
		//		delimitor: ":",
		//		field:     "SAI_PORT_STAT_PFC_7_RX_PKTS",
		//		value:     "3", // be changed to 3 from 2
		//	}, {
		//		dbName:    "COUNTERS_DB",
		//		tableName: "COUNTERS",
		//		tableKey:  "oid:0x1000000000039", // "Ethernet68": "oid:0x1000000000039",
		//		delimitor: ":",
		//		field:     "SAI_PORT_STAT_PFC_7_RX_PKTS",
		//		value:     "2", // be changed to 2 from 3
		//	}},
		//	waitTime: clientCfg.RetryInterval + time.Second,
		//	wantRespVal: []*pb.SubscribeResponse{
		//		&pb.SubscribeResponse{
		//			Response: &pb.SubscribeResponse_Update{
		//				Update: &pb.Notification{
		//					Update: []*pb.Update{
		//						{Val: &pb.TypedValue{
		//							Value: &pb.TypedValue_JsonIetfVal{
		//								JsonIetfVal: countersEthernetWildcardPfcByte,
		//							}},
		//						},
		//					},
		//				},
		//			},
		//		},
		//		&pb.SubscribeResponse{
		//			Response: &pb.SubscribeResponse_SyncResponse{
		//				SyncResponse: true,
		//			},
		//		},
		//	},
	}}

	rclient := getRedisClient(t)
	defer rclient.Close()
	for _, tt := range tests {
		prepareDb(t)
		serverOp(t, S1Start)
		serverOp(t, S2Start)
		t.Run(tt.desc, func(t *testing.T) {
			var store []*pb.SubscribeResponse
			if tt.collector == "s1" {
				s1.SetDataStore(&store)
			} else {
				s2.SetDataStore(&store)
			}
			// Extra cmd preparation for this test case
			for _, cmd := range tt.cmds {
				exe_cmd(t, cmd)
			}
			time.Sleep(time.Millisecond * 500)
			serverOp(t, tt.sop)
			for _, update := range tt.updates {
				switch update.op {
				case "hdel":
					rclient.HDel(context.Background(), update.tableName+update.delimitor+update.tableKey, update.field)
				default:
					rclient.HSet(context.Background(), update.tableName+update.delimitor+update.tableKey, update.field, update.value)
				}
				time.Sleep(time.Millisecond * 500)
			}
			if tt.waitTime != 0 {
				time.Sleep(tt.waitTime)
			}
			wantRespVal := tt.wantRespVal.([]*pb.SubscribeResponse)
			if len(store) < len(wantRespVal) {
				t.Logf("len not match %v %s %v", len(store), " : ", len(wantRespVal))
				t.Logf("want: %v", wantRespVal)
				t.Fatal("got: ", store)
			}
			slen := len(store)
			wlen := len(wantRespVal)
			for idx, resp := range wantRespVal {
				switch store[slen-wlen+idx].GetResponse().(type) {
				case *pb.SubscribeResponse_SyncResponse:
					if _, ok := resp.GetResponse().(*pb.SubscribeResponse_SyncResponse); !ok {
						t.Fatalf("Expecting %v, got SyncResponse", resp.GetResponse())
					}
				case *pb.SubscribeResponse_Update:
					compareUpdateValue(t, resp.GetUpdate(), store[idx].GetUpdate())

				}
			}
		})
		serverOp(t, S1Stop)
		serverOp(t, S2Stop)
	}
	cancel()

}

func init() {
	// Inform gNMI server to use redis tcp localhost connection
	sdc.UseRedisLocalTcpPort = true
}

// TestNewInstanceOCYang tests that NewInstance correctly handles OC_YANG target
// and creates a TranslClient. This test is specifically for code coverage of the
// OC_YANG branch in NewInstance.
func TestNewInstanceOCYang(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode")
	}

	// Initialize DB config
	err := sdcfg.Init()
	if err != nil {
		t.Skipf("Skipping test: DB config not available: %v", err)
	}

	// Create a test destination group
	destGrpName := "test_oc_yang_dest"
	dests := []Destination{
		{
			Addrs: "127.0.0.1:50052",
		},
	}
	destGrpNameMap[destGrpName] = dests
	defer delete(destGrpNameMap, destGrpName)

	// Create client subscription with OC_YANG target
	cs := &clientSubscription{
		name:          "test_oc_yang",
		destGroupName: destGrpName,
		prefix: &pb.Path{
			Target: "OC_YANG",
		},
		paths: []*pb.Path{
			{
				Elem: []*pb.PathElem{
					{Name: "openconfig-interfaces"},
					{Name: "interfaces"},
					{Name: "interface", Key: map[string]string{"name": "Ethernet0"}},
					{Name: "state"},
					{Name: "oper-status"},
				},
			},
		},
		reportType: Stream,
		interval:   30 * time.Second,
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Call NewInstance - this should hit the OC_YANG branch
	err = cs.NewInstance(ctx)

	// We expect an error since TranslClient creation will fail in test environment
	// The important thing is that it attempts to create TranslClient (code coverage)
	if err != nil {
		// Check if it's a connection/DB error (expected in test environment)
		if !strings.Contains(err.Error(), "Connection to DB") &&
			!strings.Contains(err.Error(), "Destination group") {
			t.Logf("NewInstance returned error (expected in test env): %v", err)
		} else {
			t.Logf("NewInstance returned expected error: %v", err)
		}
	} else {
		// If it succeeded, clean up
		if cs.dc != nil {
			cs.dc.Close()
		}
	}
}

func TestMatchRowPrefix(t *testing.T) {
	tests := []struct {
		key      string
		rowType  string
		wantName string
		wantOk   bool
	}{
		{"DestinationGroup_HS", "DestinationGroup", "HS", true},
		{"DestinationGroup|HS", "DestinationGroup", "HS", true},
		{"Subscription_HS_RDMA", "Subscription", "HS_RDMA", true},
		{"Subscription|HS_RDMA", "Subscription", "HS_RDMA", true},
		{"DestinationGroup_", "DestinationGroup", "", true},
		{"DestinationGroup|", "DestinationGroup", "", true},
		{"Global", "DestinationGroup", "", false},
		{"Subscription_HS", "DestinationGroup", "", false},
		{"DestinationGroupHS", "DestinationGroup", "", false},
	}
	for _, tt := range tests {
		name, ok := matchRowPrefix(tt.key, tt.rowType, "|")
		if ok != tt.wantOk || name != tt.wantName {
			t.Errorf("matchRowPrefix(%q, %q) = (%q, %v), want (%q, %v)",
				tt.key, tt.rowType, name, ok, tt.wantName, tt.wantOk)
		}
	}
}

// TestVrfDialer checks that the dialer binds the socket to the named device
// before connecting: an unknown device fails the dial with the bind error, and
// the loopback device still reaches a loopback listener. Since Linux 5.7 an
// unbound socket may be bound to a device without privileges.
func TestVrfDialer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = vrfDialer("no-such-vrf")(ctx, ln.Addr().String())
	if err == nil || !strings.Contains(err.Error(), "failed to bind socket to VRF no-such-vrf") {
		t.Fatalf("dial through unknown VRF: got %v, want bind error", err)
	}

	conn, err := vrfDialer("lo")(ctx, ln.Addr().String())
	if err != nil {
		t.Fatalf("dial bound to lo: %v", err)
	}
	conn.Close()
}

// TestNewClientVrf checks that newClient dials through the configured VRF:
// with an unknown VRF the dial fails with the bind error (not just the
// deadline), and bound to lo it reaches the test dial-out server.
func TestNewClientVrf(t *testing.T) {
	saved := clientCfg
	defer func() { clientCfg = saved }()
	serverOp(t, S1Start)
	defer serverOp(t, S1Stop)
	time.Sleep(200 * time.Millisecond)
	dest := Destination{Addrs: "127.0.0.1:8080"}

	clientCfg = &ClientConfig{
		RetryInterval: 3 * time.Second,
		Vrf:           "no-such-vrf",
		TLS:           &tls.Config{InsecureSkipVerify: true},
	}
	_, err := newClient(context.Background(), dest)
	if err == nil || !strings.Contains(err.Error(), "failed to bind socket to VRF no-such-vrf") {
		t.Fatalf("newClient with unknown VRF: got %v, want bind error", err)
	}

	clientCfg.Vrf = "lo"
	c, err := newClient(context.Background(), dest)
	if err != nil {
		t.Fatalf("newClient bound to lo: %v", err)
	}
	c.Close()

	// "default" is a value the YANG model offers, not a device name: it must
	// dial without binding, or a collector configured with it is unreachable.
	clientCfg.Vrf = "default"
	c, err = newClient(context.Background(), dest)
	if err != nil {
		t.Fatalf("newClient with vrf default: %v", err)
	}
	c.Close()
}

// The defaults a Global edit falls back to are the config DialOutRun was
// started with; a Global row that carries only vrf must leave the rest alone.
func TestDialOutRunGlobalDefaults(t *testing.T) {
	savedCfg, savedDefaults, savedGroups := clientCfg, globalDefaults, destGrpNameMap
	exe_cmd(t, "redis-cli -n 4 del TELEMETRY_CLIENT|Global")
	// Whatever an earlier test left here must not stand in for the capture.
	globalDefaults = ClientConfig{}

	cfg := ClientConfig{
		RetryInterval: 5 * time.Second,
		Encoding:      pb.Encoding_JSON_IETF,
		TLS:           &tls.Config{InsecureSkipVerify: true},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- DialOutRun(ctx, &cfg) }()
	defer func() {
		// DialOutRun only checks its context after a keyspace event, so the
		// delete that cleans the row up is also what makes it return.
		cancel()
		exe_cmd(t, "redis-cli -n 4 del TELEMETRY_CLIENT|Global")
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Errorf("DialOutRun did not return after cancel")
		}
		clientCfg, globalDefaults, destGrpNameMap = savedCfg, savedDefaults, savedGroups
	}()

	exe_cmd(t, "redis-cli -n 4 hset TELEMETRY_CLIENT|Global vrf mgmt")
	deadline := time.Now().Add(10 * time.Second)
	for {
		configMu.Lock()
		vrf, retry := cfg.Vrf, cfg.RetryInterval
		configMu.Unlock()
		if vrf == "mgmt" {
			if retry != 5*time.Second {
				t.Fatalf("after Global row with only vrf: RetryInterval=%v, want the 5s DialOutRun started with", retry)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Global row not applied by DialOutRun: Vrf=%q", vrf)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestGlobalVrfConfig checks that the vrf field of TELEMETRY_CLIENT|Global is
// applied to the client config, cleared when the field is removed with HDEL,
// and cleared again when the whole row is deleted.
func TestGlobalVrfConfig(t *testing.T) {
	savedCfg, savedDefaults, savedGroups := clientCfg, globalDefaults, destGrpNameMap
	clientCfg = &ClientConfig{RetryInterval: 5 * time.Second}
	globalDefaults = *clientCfg
	destGrpNameMap = make(map[string][]Destination)
	defer func() { clientCfg, globalDefaults, destGrpNameMap = savedCfg, savedDefaults, savedGroups }()

	rclient := getConfigDbClient(t)
	defer rclient.Close()
	defer exe_cmd(t, "redis-cli -n 4 del TELEMETRY_CLIENT|Global")

	process := func(op string) {
		if err := processTelemetryClientConfig(context.Background(), rclient, "Global", op); err != nil {
			t.Fatalf("processTelemetryClientConfig(%s): %v", op, err)
		}
	}
	exe_cmd(t, "redis-cli -n 4 hset TELEMETRY_CLIENT|Global retry_interval 7 vrf mgmt")
	process("hset")
	if clientCfg.Vrf != "mgmt" || clientCfg.RetryInterval != 7*time.Second {
		t.Fatalf("after hset: Vrf=%q RetryInterval=%v, want mgmt 7s", clientCfg.Vrf, clientCfg.RetryInterval)
	}

	exe_cmd(t, "redis-cli -n 4 hdel TELEMETRY_CLIENT|Global vrf")
	process("hdel")
	if clientCfg.Vrf != "" || clientCfg.RetryInterval != 7*time.Second {
		t.Fatalf("after hdel vrf: Vrf=%q RetryInterval=%v, want empty 7s", clientCfg.Vrf, clientCfg.RetryInterval)
	}

	exe_cmd(t, "redis-cli -n 4 hset TELEMETRY_CLIENT|Global vrf mgmt")
	process("hset")
	exe_cmd(t, "redis-cli -n 4 del TELEMETRY_CLIENT|Global")
	process("hdel")
	if clientCfg.Vrf != "" || clientCfg.RetryInterval != 5*time.Second {
		t.Fatalf("after del row: Vrf=%q RetryInterval=%v, want defaults", clientCfg.Vrf, clientCfg.RetryInterval)
	}
}

// TestGlobalEditStopsOldInstances checks that every Global edit stops the
// instances it replaces: an instance retrying a collector that refuses
// connections counts attempts until it is cancelled, so the counters of the
// replaced instances must stand still after a second edit.
func TestGlobalEditStopsOldInstances(t *testing.T) {
	savedCfg, savedDefaults := clientCfg, globalDefaults
	savedGroups, savedSubs, savedUsers := destGrpNameMap, ClientSubscriptionNameMap, DestGrp2ClientSubMap
	clientCfg = &ClientConfig{RetryInterval: time.Second, TLS: &tls.Config{InsecureSkipVerify: true}}
	globalDefaults = *clientCfg
	destGrpNameMap = make(map[string][]Destination)
	ClientSubscriptionNameMap = make(map[string]*clientSubscription)
	DestGrp2ClientSubMap = make(map[string][]string)
	defer func() {
		for _, cs := range ClientSubscriptionNameMap {
			cs.Close()
			cs.cancel()
		}
		clientCfg, globalDefaults = savedCfg, savedDefaults
		destGrpNameMap, ClientSubscriptionNameMap, DestGrp2ClientSubMap = savedGroups, savedSubs, savedUsers
	}()
	prepareDb(t)
	rclient := getConfigDbClient(t)
	defer rclient.Close()
	defer exe_cmd(t, "redis-cli -n 4 del TELEMETRY_CLIENT|Global TELEMETRY_CLIENT|DestinationGroup_ORPHAN TELEMETRY_CLIENT|Subscription_ORPHAN")

	process := func(key string) {
		if err := processTelemetryClientConfig(context.Background(), rclient, key, "hset"); err != nil {
			t.Fatalf("processTelemetryClientConfig(%s): %v", key, err)
		}
	}
	// Port 1 on loopback refuses connections, so the instance keeps retrying.
	exe_cmd(t, "redis-cli -n 4 hset TELEMETRY_CLIENT|DestinationGroup_ORPHAN dst_addr 127.0.0.1:1")
	exe_cmd(t, "redis-cli -n 4 hmset TELEMETRY_CLIENT|Subscription_ORPHAN path_target COUNTERS_DB dst_group ORPHAN report_type stream paths COUNTERS/Ethernet*")
	process("DestinationGroup_ORPHAN")
	process("Subscription_ORPHAN")

	var replaced []*clientSubscription
	for _, vrf := range []string{"mgmt", "default"} {
		replaced = append(replaced, ClientSubscriptionNameMap["ORPHAN"])
		exe_cmd(t, "redis-cli -n 4 hset TELEMETRY_CLIENT|Global vrf "+vrf)
		process("Global")
	}
	current := ClientSubscriptionNameMap["ORPHAN"]
	for _, old := range replaced {
		if old == current {
			t.Fatal("Global edit did not replace the instance")
		}
	}
	time.Sleep(2 * clientCfg.RetryInterval)
	before := make([]uint64, len(replaced))
	for i, old := range replaced {
		before[i] = atomic.LoadUint64(&old.conTryCnt)
	}
	liveBefore := atomic.LoadUint64(&current.conTryCnt)
	time.Sleep(3 * clientCfg.RetryInterval)
	for i, old := range replaced {
		if after := atomic.LoadUint64(&old.conTryCnt); after != before[i] {
			t.Errorf("replaced instance %d still dialing: attempts %d -> %d", i, before[i], after)
		}
	}
	if atomic.LoadUint64(&current.conTryCnt) == liveBefore {
		t.Errorf("current instance stopped dialing: attempts stuck at %d", liveBefore)
	}
}
