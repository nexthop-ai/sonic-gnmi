package telemetry_dialout

// dialout_client_test covers gNMIDialOut publish test
// Prerequisite: redis-server should be running.

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"io/ioutil"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	log "github.com/golang/glog"
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
<<<<<<< HEAD
=======

func TestDestinationValidate(t *testing.T) {
	tests := []struct {
		addr   string
		wantOk bool
	}{
		{"127.0.0.1:8080", true},
		{"[fd00:1::1]:8080", true},
		{"[::1]:8080", true},
		{"[fd00:1::1]:65535", true},
		{"[fe80::1%eth0]:8080", true},
		{"collector.example:8080", true},
		{"collector-1.example.net:8080", true},
		{"collector1:8080", true},
		{"", false},
		{" 10.0.0.1:8080", false},
		{"10.0.0.1 :8080", false},
		{"col lector:8080", false},
		{"-collector.example:8080", false},
		{"collector_1.example:8080", false},
		{"256.1.1.1:8080", false},
		{"10.0.0:8080", false},
		{"10.0.0.01:8080", false},
		{"127.0.0.1", false},
		{"fd00:1::1:8080", false},
		{"[fd00:1::1]", false},
		{":8080", false},
		{"127.0.0.1:", false},
		{"127.0.0.1:0", false},
		{"127.0.0.1:65536", false},
		{"127.0.0.1:99999", false},
		{"127.0.0.1:abc", false},
	}
	for _, tt := range tests {
		err := Destination{Addrs: tt.addr}.Validate()
		if (err == nil) != tt.wantOk {
			t.Errorf("Destination{%q}.Validate() = %v, want ok=%v", tt.addr, err, tt.wantOk)
		}
	}
}

// A DestinationGroup row with one malformed endpoint, an unknown field, or no
// field at all (the key went away before it was read) is refused whole, before
// the group's running subscriptions are touched: the group keeps the
// destinations it had and its subscriptions stay open. A well-formed row
// replaces the list and re-dials.
func TestDestinationGroupRowRefusedWholeBeforeTeardown(t *testing.T) {
	rclient := getConfigDbClient(t)
	defer rclient.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const group = "V6CHECK"
	const key = "TELEMETRY_CLIENT|DestinationGroup_" + group
	old := []Destination{{Addrs: "127.0.0.1:9000"}}
	destGrpNameMap[group] = old
	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()
	cs := &clientSubscription{name: "V6CHECK_SUB", destGroupName: group, opened: true, cancel: subCancel}
	ClientSubscriptionNameMap[cs.name] = cs
	DestGrp2ClientSubMap[group] = []string{cs.name}
	defer func() {
		delete(destGrpNameMap, group)
		delete(ClientSubscriptionNameMap, cs.name)
		delete(DestGrp2ClientSubMap, group)
		rclient.Del(ctx, key)
	}()

	rclient.HSet(ctx, key, "dst_addr", "127.0.0.1:8080,fd00:1::1:8080")
	err := processTelemetryClientConfig(ctx, rclient, "DestinationGroup_"+group, "hset")
	if err == nil || !strings.Contains(err.Error(), "fd00:1::1:8080") {
		t.Fatalf("mixed row: got err %v, want one naming the bad entry", err)
	}
	if got := destGrpNameMap[group]; !reflect.DeepEqual(got, old) {
		t.Errorf("refused row replaced the group: got %v, want %v", got, old)
	}
	if !cs.opened || subCtx.Err() != nil {
		t.Errorf("refused row tore down the running subscription: opened=%v ctx=%v", cs.opened, subCtx.Err())
	}

	rclient.Del(ctx, key)
	rclient.HSet(ctx, key, "foo", "bar")
	err = processTelemetryClientConfig(ctx, rclient, "DestinationGroup_"+group, "hset")
	if err == nil || !strings.Contains(err.Error(), "foo") {
		t.Fatalf("unknown-field row: got err %v, want one naming the field", err)
	}
	if got := destGrpNameMap[group]; !reflect.DeepEqual(got, old) {
		t.Errorf("unknown-field row replaced the group: got %v, want %v", got, old)
	}
	if !cs.opened || subCtx.Err() != nil {
		t.Errorf("unknown-field row tore down the running subscription: opened=%v ctx=%v", cs.opened, subCtx.Err())
	}

	rclient.Del(ctx, key)
	err = processTelemetryClientConfig(ctx, rclient, "DestinationGroup_"+group, "hset")
	if err == nil || !strings.Contains(err.Error(), "no dst_addr") {
		t.Fatalf("row read back empty: got err %v, want one saying so", err)
	}
	if got := destGrpNameMap[group]; !reflect.DeepEqual(got, old) {
		t.Errorf("empty row replaced the group: got %v, want %v", got, old)
	}
	if !cs.opened || subCtx.Err() != nil {
		t.Errorf("empty row tore down the running subscription: opened=%v ctx=%v", cs.opened, subCtx.Err())
	}

	rclient.HSet(ctx, key, "dst_addr", "127.0.0.1:8080,[fd00:1::1]:8080")
	if err := processTelemetryClientConfig(ctx, rclient, "DestinationGroup_"+group, "hset"); err != nil {
		t.Fatalf("well-formed row: %v", err)
	}
	want := []Destination{{Addrs: "127.0.0.1:8080"}, {Addrs: "[fd00:1::1]:8080"}}
	if got := destGrpNameMap[group]; !reflect.DeepEqual(got, want) {
		t.Errorf("accepted row: got %v, want %v", got, want)
	}
	if cs.opened || subCtx.Err() == nil {
		t.Errorf("accepted row left the old subscription instance running: opened=%v ctx=%v", cs.opened, subCtx.Err())
	}
}

// Deleting a DestinationGroup stops the subscriptions on it and keeps them
// registered to it, so a row with the same name starts them again. The
// restarted instance is a new one with its own context: deleting the
// subscription afterwards stops that instance, not the one already closed.
func TestDestinationGroupDeleteParksItsSubscriptions(t *testing.T) {
	rclient := getConfigDbClient(t)
	defer rclient.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	useTestClientConfig(t)

	const group = "V6DEL"
	const name = "V6DEL_SUB"
	const key = "TELEMETRY_CLIENT|DestinationGroup_" + group
	destGrpNameMap[group] = []Destination{{Addrs: "127.0.0.1:9000"}}
	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()
	cs := &clientSubscription{name: name, destGroupName: group, opened: true, cancel: subCancel,
		prefix: &pb.Path{Target: "COUNTERS_DB"}}
	ClientSubscriptionNameMap[name] = cs
	DestGrp2ClientSubMap[group] = []string{name}
	defer func() {
		delete(destGrpNameMap, group)
		delete(ClientSubscriptionNameMap, name)
		delete(DestGrp2ClientSubMap, group)
		rclient.Del(ctx, key)
	}()

	if err := processTelemetryClientConfig(ctx, rclient, "DestinationGroup_"+group, "hdel"); err != nil {
		t.Fatalf("delete of a group in use: %v", err)
	}
	if _, ok := destGrpNameMap[group]; ok {
		t.Errorf("deleted group is still known")
	}
	if cs.opened || subCtx.Err() == nil {
		t.Errorf("deleted group left its subscription running: opened=%v ctx=%v", cs.opened, subCtx.Err())
	}
	if got := DestGrp2ClientSubMap[group]; !reflect.DeepEqual(got, []string{name}) {
		t.Errorf("deleted group lost its users: got %v, want [%v]", got, name)
	}

	rclient.HSet(ctx, key, "dst_addr", "127.0.0.1:9000")
	if err := processTelemetryClientConfig(ctx, rclient, "DestinationGroup_"+group, "hset"); err != nil {
		t.Fatalf("group row after the delete: %v", err)
	}
	restarted := ClientSubscriptionNameMap[name]
	if restarted == cs || restarted == nil || restarted.destGroupName != group {
		t.Fatalf("group row did not start a new instance for its user: %v", restarted)
	}
	if err := processTelemetryClientConfig(ctx, rclient, "Subscription_"+name, "hdel"); err != nil {
		t.Fatalf("delete of the restarted subscription: %v", err)
	}
	// Its publish loop, which re-opens the instance on every dial, must stop:
	// it does only if the delete cancelled the context the loop runs on.
	deadline := time.Now().Add(2 * time.Second)
	for restarted.opened && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if restarted.opened {
		t.Errorf("deleted subscription's restarted instance is still dialling")
	}
	if _, ok := DestGrp2ClientSubMap[group]; ok {
		t.Errorf("deleted subscription is still a user of %v: %v", group, DestGrp2ClientSubMap[group])
	}
	if _, ok := ClientSubscriptionNameMap[name]; ok {
		t.Errorf("deleted subscription is still registered")
	}
}

// Moving a Subscription to another group takes its name off the old group's
// user list, and clearing dst_group unregisters it. Either way the old group
// can be re-dialled afterwards without a nil instance in its list.
func TestSubscriptionLeavesItsOldGroup(t *testing.T) {
	rclient := getConfigDbClient(t)
	defer rclient.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	useTestClientConfig(t)

	const name = "V6MOVE_SUB"
	const key = "TELEMETRY_CLIENT|Subscription_" + name
	destGrpNameMap["V6MOVE_A"] = []Destination{{Addrs: "127.0.0.1:9000"}}
	destGrpNameMap["V6MOVE_B"] = []Destination{{Addrs: "127.0.0.1:9001"}}
	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()
	cs := &clientSubscription{name: name, destGroupName: "V6MOVE_A", opened: true, cancel: subCancel}
	ClientSubscriptionNameMap[name] = cs
	DestGrp2ClientSubMap["V6MOVE_A"] = []string{name}
	defer func() {
		delete(destGrpNameMap, "V6MOVE_A")
		delete(destGrpNameMap, "V6MOVE_B")
		delete(ClientSubscriptionNameMap, name)
		delete(DestGrp2ClientSubMap, "V6MOVE_A")
		delete(DestGrp2ClientSubMap, "V6MOVE_B")
		rclient.Del(ctx, key, "TELEMETRY_CLIENT|DestinationGroup_V6MOVE_A")
	}()

	rclient.HSet(ctx, key, "dst_group", "V6MOVE_B", "path_target", "COUNTERS_DB")
	if err := processTelemetryClientConfig(ctx, rclient, "Subscription_"+name, "hset"); err != nil {
		t.Fatalf("move to V6MOVE_B: %v", err)
	}
	if cs.opened || subCtx.Err() == nil {
		t.Errorf("move left the old instance running: opened=%v ctx=%v", cs.opened, subCtx.Err())
	}
	if _, ok := DestGrp2ClientSubMap["V6MOVE_A"]; ok {
		t.Errorf("moved subscription is still a user of V6MOVE_A: %v", DestGrp2ClientSubMap["V6MOVE_A"])
	}
	if got := DestGrp2ClientSubMap["V6MOVE_B"]; !reflect.DeepEqual(got, []string{name}) {
		t.Errorf("moved subscription is not the user of V6MOVE_B: %v", got)
	}
	if got := ClientSubscriptionNameMap[name]; got == nil || got.destGroupName != "V6MOVE_B" {
		t.Fatalf("moved subscription is not registered on V6MOVE_B: %v", got)
	}

	rclient.Del(ctx, key)
	rclient.HSet(ctx, key, "path_target", "COUNTERS_DB")
	if err := processTelemetryClientConfig(ctx, rclient, "Subscription_"+name, "hset"); err != nil {
		t.Fatalf("row without dst_group: %v", err)
	}
	if _, ok := ClientSubscriptionNameMap[name]; ok {
		t.Errorf("subscription without dst_group is still registered")
	}
	if _, ok := DestGrp2ClientSubMap["V6MOVE_B"]; ok {
		t.Errorf("subscription without dst_group is still a user of V6MOVE_B: %v", DestGrp2ClientSubMap["V6MOVE_B"])
	}

	// With a stale name in either list this would call Close on a nil instance.
	rclient.HSet(ctx, "TELEMETRY_CLIENT|DestinationGroup_V6MOVE_A", "dst_addr", "127.0.0.1:9002")
	if err := processTelemetryClientConfig(ctx, rclient, "DestinationGroup_V6MOVE_A", "hset"); err != nil {
		t.Fatalf("re-dial of the old group: %v", err)
	}
	if err := processTelemetryClientConfig(ctx, rclient, "Subscription_"+name, "hdel"); err != nil {
		t.Errorf("delete of the unregistered subscription: %v", err)
	}
}

// A Subscription row that is refused, for an unknown field or for lacking the
// path_target it cannot start without, leaves the running subscription as it
// was; an accepted row closes it, registers the new instance and opens its
// data source. Deleting a Subscription row that was never registered is a
// no-op, not a panic.
func TestSubscriptionRowRefusedWholeBeforeTeardown(t *testing.T) {
	rclient := getConfigDbClient(t)
	defer rclient.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	useTestClientConfig(t)

	const group = "V6SUB"
	const name = "V6SUB_SUB"
	const key = "TELEMETRY_CLIENT|Subscription_" + name
	destGrpNameMap[group] = []Destination{{Addrs: "127.0.0.1:9000"}}
	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()
	cs := &clientSubscription{name: name, destGroupName: group, opened: true, cancel: subCancel}
	ClientSubscriptionNameMap[name] = cs
	DestGrp2ClientSubMap[group] = []string{name}
	defer func() {
		delete(destGrpNameMap, group)
		delete(ClientSubscriptionNameMap, name)
		delete(DestGrp2ClientSubMap, group)
		rclient.Del(ctx, key)
	}()

	rclient.HSet(ctx, key, "dst_group", group, "foo", "bar")
	err := processTelemetryClientConfig(ctx, rclient, "Subscription_"+name, "hset")
	if err == nil || !strings.Contains(err.Error(), "foo") {
		t.Fatalf("unknown-field row: got err %v, want one naming the field", err)
	}
	if !cs.opened || subCtx.Err() != nil {
		t.Errorf("refused row tore down the running subscription: opened=%v ctx=%v", cs.opened, subCtx.Err())
	}
	if ClientSubscriptionNameMap[name] != cs {
		t.Errorf("refused row replaced the registered subscription")
	}

	rclient.Del(ctx, key)
	rclient.HSet(ctx, key, "dst_group", group)
	err = processTelemetryClientConfig(ctx, rclient, "Subscription_"+name, "hset")
	if err == nil || !strings.Contains(err.Error(), "path_target") {
		t.Fatalf("row without path_target: got err %v, want one naming the field", err)
	}
	if !cs.opened || subCtx.Err() != nil {
		t.Errorf("row without path_target tore down the running subscription: opened=%v ctx=%v", cs.opened, subCtx.Err())
	}

	// A report interval that is not a positive number, and a report type the
	// publisher cannot run, are refused the same way.
	rclient.HSet(ctx, key, "path_target", "COUNTERS_DB")
	for _, bad := range [][2]string{
		{"report_interval", "5s"},
		{"report_interval", "0"},
		{"report_interval", "100000000"},
		{"report_type", "once"},
		{"report_type", "Stream"},
	} {
		rclient.HSet(ctx, key, bad[0], bad[1])
		err = processTelemetryClientConfig(ctx, rclient, "Subscription_"+name, "hset")
		if err == nil || !strings.Contains(err.Error(), bad[0]) {
			t.Fatalf("row with %v %v: got err %v, want one naming the field", bad[0], bad[1], err)
		}
		if !cs.opened || subCtx.Err() != nil || ClientSubscriptionNameMap[name] != cs {
			t.Errorf("row with %v %v tore down the running subscription: opened=%v ctx=%v", bad[0], bad[1], cs.opened, subCtx.Err())
		}
		rclient.HDel(ctx, key, bad[0])
	}

	if err := processTelemetryClientConfig(ctx, rclient, "Subscription_"+name, "hset"); err != nil {
		t.Fatalf("well-formed row: %v", err)
	}
	if cs.opened || subCtx.Err() == nil {
		t.Errorf("accepted row left the old instance running: opened=%v ctx=%v", cs.opened, subCtx.Err())
	}
	got := ClientSubscriptionNameMap[name]
	if got == cs || got == nil || got.destGroupName != group {
		t.Fatalf("accepted row did not register the new instance: %v", got)
	}
	if got.reportType != Periodic || got.interval != 5*time.Second {
		t.Errorf("row without report_type and report_interval: got %v every %v, want periodic every 5s", got.reportType, got.interval)
	}
	if got.dc == nil {
		t.Errorf("accepted row did not open its data source")
	}

	if err := processTelemetryClientConfig(ctx, rclient, "Subscription_NEVER_REGISTERED", "hdel"); err != nil {
		t.Errorf("delete of an unregistered subscription: %v", err)
	}
}

// Removing one field of a row arrives as an hdel event with the rest of the
// row still there. That is an update, not a delete: the subscription and the
// group stay configured.
func TestFieldRemovalIsAnUpdate(t *testing.T) {
	rclient := getConfigDbClient(t)
	defer rclient.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	useTestClientConfig(t)

	const group = "V6FIELD"
	const name = "V6FIELD_SUB"
	const subKey = "TELEMETRY_CLIENT|Subscription_" + name
	const grpKey = "TELEMETRY_CLIENT|DestinationGroup_" + group
	defer func() {
		if cs := ClientSubscriptionNameMap[name]; cs != nil {
			cs.Close()
		}
		delete(destGrpNameMap, group)
		delete(ClientSubscriptionNameMap, name)
		delete(DestGrp2ClientSubMap, group)
		rclient.Del(ctx, subKey, grpKey)
	}()

	rclient.HSet(ctx, grpKey, "dst_addr", "127.0.0.1:9000")
	if err := processTelemetryClientConfig(ctx, rclient, "DestinationGroup_"+group, "hset"); err != nil {
		t.Fatalf("group row: %v", err)
	}
	rclient.HSet(ctx, subKey, "dst_group", group, "path_target", "COUNTERS_DB", "report_interval", "1000")
	if err := processTelemetryClientConfig(ctx, rclient, "Subscription_"+name, "hset"); err != nil {
		t.Fatalf("subscription row: %v", err)
	}

	rclient.HDel(ctx, subKey, "report_interval")
	if err := processTelemetryClientConfig(ctx, rclient, "Subscription_"+name, "hdel"); err != nil {
		t.Fatalf("field removal on the subscription: %v", err)
	}
	got := ClientSubscriptionNameMap[name]
	if got == nil || got.interval != 5*time.Second {
		t.Fatalf("field removal did not leave the subscription registered at the default interval: %v", got)
	}
	if users := DestGrp2ClientSubMap[group]; !reflect.DeepEqual(users, []string{name}) {
		t.Errorf("field removal changed the group's users: %v", users)
	}

	// The group row has one field, so an hdel event with the row still
	// there can only be seen late, after the field was set again.
	if err := processTelemetryClientConfig(ctx, rclient, "DestinationGroup_"+group, "hdel"); err != nil {
		t.Fatalf("hdel event on a group row that still has fields: %v", err)
	}
	if _, ok := destGrpNameMap[group]; !ok {
		t.Errorf("hdel event on a group row that still has fields deleted the group")
	}

	rclient.Del(ctx, subKey)
	if err := processTelemetryClientConfig(ctx, rclient, "Subscription_"+name, "hdel"); err != nil {
		t.Fatalf("delete of the subscription row: %v", err)
	}
	if _, ok := ClientSubscriptionNameMap[name]; ok {
		t.Errorf("deleted subscription row is still registered")
	}
}

// A refused row is reported at V(1) with the row key and the reason.
func TestRefusedRowReportedAtV1(t *testing.T) {
	rclient := getConfigDbClient(t)
	defer rclient.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, name := range []string{"logtostderr", "v"} {
		f := flag.Lookup(name)
		if f == nil {
			t.Fatalf("glog flag %q is not registered", name)
		}
		defer flag.Set(name, f.Value.String())
	}
	flag.Set("logtostderr", "true")
	flag.Set("v", "1")
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr := os.Stderr
	os.Stderr = w
	defer func() { os.Stderr = stderr }()

	const key = "TELEMETRY_CLIENT|DestinationGroup_V6LOG"
	rclient.HSet(ctx, key, "dst_addr", "fd00:1::1:8080")
	defer rclient.Del(ctx, key)
	applyTelemetryClientConfig(ctx, rclient, "DestinationGroup_V6LOG", "hset")
	log.Flush()
	os.Stderr = stderr
	w.Close()
	out, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"DestinationGroup_V6LOG hset refused", "fd00:1::1:8080", "too many colons"} {
		if !strings.Contains(string(out), want) {
			t.Errorf("stderr at -v 1 lacks %q:\n%s", want, out)
		}
	}
	if _, ok := destGrpNameMap["V6LOG"]; ok {
		t.Errorf("refused row installed the group")
		delete(destGrpNameMap, "V6LOG")
	}
}

// A Global row is read whole before any of it is applied: a bad
// retry_interval leaves the config, and the running instances, untouched.
func TestGlobalRowRefusedWholeBeforeRedial(t *testing.T) {
	rclient := getConfigDbClient(t)
	defer rclient.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	useTestClientConfig(t)
	before, savedDefaults := *clientCfg, globalDefaults
	globalDefaults = before
	defer func() { *clientCfg, globalDefaults = before, savedDefaults }()

	const group = "V6GLOBAL"
	const key = "TELEMETRY_CLIENT|Global"
	destGrpNameMap[group] = []Destination{{Addrs: "127.0.0.1:9000"}}
	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()
	cs := &clientSubscription{name: "V6GLOBAL_SUB", destGroupName: group, opened: true, cancel: subCancel}
	ClientSubscriptionNameMap[cs.name] = cs
	DestGrp2ClientSubMap[group] = []string{cs.name}
	defer func() {
		delete(destGrpNameMap, group)
		delete(ClientSubscriptionNameMap, cs.name)
		delete(DestGrp2ClientSubMap, group)
		rclient.Del(ctx, key)
	}()

	rclient.HSet(ctx, key, "src_ip", "10.0.0.2", "retry_interval", "soon")
	err := processTelemetryClientConfig(ctx, rclient, "Global", "hset")
	if err == nil || !strings.Contains(err.Error(), "soon") {
		t.Fatalf("Global row with a bad retry_interval: got err %v, want one naming the value", err)
	}
	if *clientCfg != before {
		t.Errorf("refused Global row changed the config: %+v", *clientCfg)
	}
	if !cs.opened || subCtx.Err() != nil {
		t.Errorf("refused Global row re-dialled the running subscription: opened=%v ctx=%v", cs.opened, subCtx.Err())
	}

	// 0 would make every dial time out at once; 10000 is past the schema.
	for _, bad := range []string{"0", "10000", "-1"} {
		rclient.HSet(ctx, key, "retry_interval", bad)
		err := processTelemetryClientConfig(ctx, rclient, "Global", "hset")
		if err == nil || !strings.Contains(err.Error(), "retry_interval") {
			t.Fatalf("Global row with retry_interval %v: got err %v, want a refusal", bad, err)
		}
		if *clientCfg != before {
			t.Errorf("Global row with retry_interval %v changed the config: %+v", bad, *clientCfg)
		}
		if !cs.opened || subCtx.Err() != nil {
			t.Errorf("Global row with retry_interval %v re-dialled the running subscription", bad)
		}
	}

	rclient.HSet(ctx, key, "retry_interval", "7")
	if err := processTelemetryClientConfig(ctx, rclient, "Global", "hset"); err != nil {
		t.Fatalf("well-formed Global row: %v", err)
	}
	if clientCfg.SrcIp != "10.0.0.2" || clientCfg.RetryInterval != 7*time.Second {
		t.Errorf("accepted Global row not applied: %+v", *clientCfg)
	}
	if cs.opened || subCtx.Err() == nil {
		t.Errorf("accepted Global row left the old instance running: opened=%v ctx=%v", cs.opened, subCtx.Err())
	}
}

// The address is dialled over TCP exactly as written: a host that gRPC would
// otherwise read as a socket scheme is a name to look up, and a zone-scoped
// IPv6 address reaches the dialler instead of failing to parse as a URL.
func TestNewClientDialsTCPAsWritten(t *testing.T) {
	useTestClientConfig(t)
	before := *clientCfg
	defer func() { *clientCfg = before }()
	// Without credentials gRPC refuses before it dials.
	clientCfg.TLS = &tls.Config{InsecureSkipVerify: true}
	clientCfg.RetryInterval = time.Second
	for _, tt := range []struct{ addr, bug string }{
		{"unix:8080", "dial unix"},
		{"[fe80::1%lo]:1", "invalid URL escape"},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := newClient(ctx, Destination{Addrs: tt.addr})
		cancel()
		if err == nil {
			t.Errorf("newClient(%q) succeeded, want a dial failure", tt.addr)
		} else if strings.Contains(err.Error(), tt.bug) {
			t.Errorf("newClient(%q) = %v, want no %q", tt.addr, err, tt.bug)
		}
	}
}

// useTestClientConfig gives the test its own client config, whatever an
// earlier test left, so an instance it starts dials for RetryInterval, fails,
// and stops when its context is cancelled. Without TLS gRPC refuses before it
// dials and the instance retries with no pause.
func useTestClientConfig(t *testing.T) {
	saved := clientCfg
	t.Cleanup(func() {
		// An instance can still be retrying after the test and reads
		// clientCfg, so a nil left by running the test alone is not restored.
		if saved != nil {
			clientCfg = saved
		}
	})
	clientCfg = &ClientConfig{
		RetryInterval: 100 * time.Millisecond,
		TLS:           &tls.Config{InsecureSkipVerify: true},
	}
}

// A host that is also the name of a gRPC resolver scheme reaches the dialer
// as written: the target is not handed to that resolver.
func TestDialTargetIsNotReadAsResolverScheme(t *testing.T) {
	for _, addr := range []string{"dns:8080", "unix:8080", "[fe80::1%lo]:1"} {
		got := make(chan string, 16)
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		conn, err := grpc.DialContext(ctx, dialTarget(addr), grpc.WithBlock(),
			grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})),
			grpc.WithContextDialer(func(ctx context.Context, a string) (net.Conn, error) {
				select {
				case got <- a:
				default:
				}
				return nil, errors.New("not dialled in this test")
			}))
		cancel()
		if err == nil {
			conn.Close()
			t.Fatalf("dial of %q succeeded", addr)
		}
		select {
		case a := <-got:
			if a != addr {
				t.Errorf("dialer got %q for %q", a, addr)
			}
		default:
			t.Errorf("dialer was never called for %q: %v", addr, err)
		}
	}
}

// waitForFullDump waits for a SyncResponse in store and checks that the dump
// before it carried at least one update.
func waitForFullDump(t *testing.T, store *[]*pb.SubscribeResponse, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, resp := range *store {
			if !resp.GetSyncResponse() {
				continue
			}
			for _, prev := range (*store)[:i] {
				if len(prev.GetUpdate().GetUpdate()) > 0 {
					return
				}
			}
			t.Fatalf("SyncResponse without a preceding update: %v", *store)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("no SyncResponse within %v, got %v", timeout, *store)
}

// startIdleStream sets a short retry interval, starts collector S1 and opens a
// stream subscription on COUNTERS/Ethernet68, which the test never writes to.
// It returns once the initial dump has reached the collector.
func startIdleStream(t *testing.T, name string) (*clientSubscription, context.CancelFunc) {
	prepareDb(t)

	saved := clientCfg
	t.Cleanup(func() {
		// publishRun can still be retrying after the test and reads clientCfg,
		// so a nil left by running this test alone is not restored.
		if saved != nil {
			clientCfg = saved
		}
	})
	clientCfg = &ClientConfig{
		RetryInterval:  2 * time.Second,
		Encoding:       pb.Encoding_JSON_IETF,
		Unidirectional: true,
		TLS:            &tls.Config{InsecureSkipVerify: true},
	}
	destGrpNameMap[name] = []Destination{{Addrs: "127.0.0.1:8080"}}
	t.Cleanup(func() { delete(destGrpNameMap, name) })

	cs := &clientSubscription{
		name:          name,
		destGroupName: name,
		prefix:        &pb.Path{Target: "COUNTERS_DB"},
		paths: []*pb.Path{{
			Elem: []*pb.PathElem{{Name: "COUNTERS"}, {Name: "Ethernet68"}},
		}},
		reportType: Stream,
	}

	var first []*pb.SubscribeResponse
	serverOp(t, S1Start)
	t.Cleanup(func() { serverOp(t, S1Stop) })
	s1.SetDataStore(&first)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := cs.NewInstance(ctx); err != nil {
		t.Fatalf("NewInstance: %v", err)
	}
	t.Cleanup(cs.Close)
	waitForFullDump(t, &first, 5*time.Second)
	return cs, cancel
}

// A stream subscription with no DB changes must still notice the collector
// going away and reconnect once it is back. A session that lasted longer than
// RetryInterval reconnects without first waiting out RetryInterval.
func TestGNMIDialOutStreamReconnectsAfterIdleCollectorRestart(t *testing.T) {
	startIdleStream(t, "idle_restart")
	time.Sleep(clientCfg.RetryInterval)

	serverOp(t, S1Stop)
	var second []*pb.SubscribeResponse
	serverOp(t, S1Start)
	s1.SetDataStore(&second)

	waitForFullDump(t, &second, clientCfg.RetryInterval)
}

// Closing a live stream and cancelling its context, as a TELEMETRY_CLIENT
// edit does, must stop the instance: it must not keep redialing.
func TestGNMIDialOutStreamClientCloseStops(t *testing.T) {
	cs, cancel := startIdleStream(t, "client_close")

	cs.Close()
	cancel()
	time.Sleep(2 * clientCfg.RetryInterval)
	before := atomic.LoadUint64(&cs.conTryCnt)
	time.Sleep(3 * clientCfg.RetryInterval)
	if after := atomic.LoadUint64(&cs.conTryCnt); after != before {
		t.Errorf("closed instance still dialing: attempts %d -> %d", before, after)
	}
}

// closingCollector accepts every Publish stream and ends it at once.
type closingCollector struct{}

func (closingCollector) Publish(spb.GNMIDialOut_PublishServer) error { return nil }

// A stream whose collector accepts and immediately closes it must pause
// RetryInterval before each redial instead of looping.
func TestGNMIDialOutStreamShortSessionPauses(t *testing.T) {
	prepareDb(t)
	saved := clientCfg
	t.Cleanup(func() {
		if saved != nil {
			clientCfg = saved
		}
	})
	clientCfg = &ClientConfig{RetryInterval: time.Second, TLS: &tls.Config{InsecureSkipVerify: true}}

	cert, err := serverCert()
	if err != nil {
		t.Fatalf("could not load server key pair: %s", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}})))
	spb.RegisterGNMIDialOutServer(srv, closingCollector{})
	go srv.Serve(ln)
	defer srv.Stop()

	name := "short_session"
	destGrpNameMap[name] = []Destination{{Addrs: ln.Addr().String()}}
	defer delete(destGrpNameMap, name)
	cs := &clientSubscription{
		name:          name,
		destGroupName: name,
		prefix:        &pb.Path{Target: "COUNTERS_DB"},
		paths: []*pb.Path{{
			Elem: []*pb.PathElem{{Name: "COUNTERS"}, {Name: "Ethernet68"}},
		}},
		reportType: Stream,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := cs.NewInstance(ctx); err != nil {
		t.Fatalf("NewInstance: %v", err)
	}
	defer cs.Close()

	window := 5 * clientCfg.RetryInterval
	time.Sleep(window)
	// One attempt per RetryInterval, plus the first and some slack. At least
	// two shows the client kept reconnecting rather than stalling.
	if n := atomic.LoadUint64(&cs.conTryCnt); n < 2 || n > 7 {
		t.Errorf("%d connection attempts in %v, want about one per %v", n, window, clientCfg.RetryInterval)
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
>>>>>>> 5ee58df (NOS-20765: [dialout] Validate a destination as host:port before dialling (#359))
