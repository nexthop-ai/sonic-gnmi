package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Workiva/go-datastructures/queue"
	"github.com/agiledragon/gomonkey/v2"
	"github.com/alicebob/miniredis/v2"
	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
	redis "github.com/redis/go-redis/v9"
	spb "github.com/sonic-net/sonic-gnmi/proto"
	sdcfg "github.com/sonic-net/sonic-gnmi/sonic_db_config"
)

func TestVirtualDbSIDLookup(t *testing.T) {
	t.Setenv("UNIT_TEST", "1")
	ClearMappings()
	t.Cleanup(ClearMappings)
	sdcfg.Init()
	mr := miniredis.RunT(t)
	mr.HSet("COUNTERS_DEBUG_NAME_SWITCH_STAT_MAP", "test", "oid:0x1")
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	clients := gomonkey.ApplyFuncReturn(GetRedisClientsForDb, map[string]*redis.Client{"": rdb}, nil)
	defer clients.Reset()
	var callCount int64
	var readErr error
	mock := gomonkey.ApplyFunc(GetCountersMap, func(tableName string) (map[string]string, error) {
		if tableName != "COUNTERS_SRV6_NAME_MAP" {
			return map[string]string{}, nil
		}
		if atomic.AddInt64(&callCount, 1) == 1 {
			return map[string]string{}, nil
		}
		if readErr != nil {
			return nil, readErr
		}
		return map[string]string{"fcbb:bbbb:1::/48": "oid:0x11110000001eb3"}, nil
	})
	defer mock.Reset()
	clock := useFakeNameMapClock(t)
	query := func() ([]TablePath, error) {
		return PopulateTablePaths(&gnmipb.Path{Target: "COUNTERS_DB"},
			&gnmipb.Path{Elem: []*gnmipb.PathElem{{Name: "COUNTERS"}, {Name: "SID:*"}}})
	}
	for i := 0; i < 10; i++ {
		clock.advance(nameMapMaxAge)
		paths, err := query()
		if err != nil || (i == 0 && len(paths) != 0) || (i > 0 && (len(paths) != 1 ||
			!strings.Contains(fmt.Sprint(paths), `key="oid:0x11110000001eb3"`))) {
			t.Fatalf("SID query %d: paths=%v err=%v", i, paths, err)
		}
	}
	if got := atomic.LoadInt64(&callCount); got != 10 {
		t.Fatalf("expected ten SID fetches, got %d", got)
	}
	readErr = errors.New("SID mapping unavailable")
	clock.advance(nameMapMaxAge)
	if paths, err := query(); err != nil || !strings.Contains(fmt.Sprint(paths), `key="oid:0x11110000001eb3"`) {
		t.Fatalf("SID lookup with the map unreadable: paths=%v err=%v, want the SID read last", paths, err)
	}
	ClearMappings()
	if _, err := query(); !errors.Is(err, readErr) || err.Error() != "read COUNTERS_SRV6_NAME_MAP: "+readErr.Error() {
		t.Fatalf("SID lookup lost read error: %v", err)
	}
	readErr = nil
	results := make([][]TablePath, 100)
	before := atomic.LoadInt64(&callCount)
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			paths, err := query()
			if err != nil || len(paths) != 1 || !strings.Contains(fmt.Sprint(paths), `key="oid:0x11110000001eb3"`) {
				t.Errorf("SID lookup: paths=%v err=%v", paths, err)
				return
			}
			results[i] = paths
		}(i)
	}
	wg.Wait()
	if got := atomic.LoadInt64(&callCount) - before; got != 1 {
		t.Errorf("expected one SID fetch for the concurrent queries, got %d", got)
	}
	if t.Failed() {
		return
	}
	results[0][0] = CreateTablePath("COUNTERS_DB", "COUNTERS", ":", "local-change")
	for _, paths := range results[1:] {
		if !strings.Contains(fmt.Sprint(paths), `key="oid:0x11110000001eb3"`) {
			t.Error("SID lookup results share mutable storage")
		}
	}
}

// nameMapStub holds the name maps the translators read, in place of Redis.
type nameMapStub struct {
	// COUNTERS_DB name maps by table name
	counters                               map[string]map[string]string
	fabric                                 map[string]string
	pfcwd                                  map[string]map[string]string
	alias2name, name2alias, port2namespace map[string]string
	voqNameToOid, voqOidToNamespace        map[string]string
	switchStat                             map[string]string
}

// stubNameMaps serves the name maps in s to the translators until the test ends.
func stubNameMaps(t *testing.T, s *nameMapStub) {
	t.Helper()
	resetNameMapCache(t)
	restoreNameMapFns(t)
	getCountersMapFn = func(tableName string) (map[string]string, error) { return s.counters[tableName], nil }
	getAliasMapFn = func() (map[string]string, map[string]string, map[string]string, error) {
		return s.alias2name, s.name2alias, s.port2namespace, nil
	}
	getFabricCountersMapFn = func(string) (map[string]string, error) { return s.fabric, nil }
	getPfcwdMapFn = func() (map[string]map[string]string, error) { return s.pfcwd, nil }
	getVoQCountersMapFn = func(string) (map[string]string, map[string]string, error) {
		return s.voqNameToOid, s.voqOidToNamespace, nil
	}
	getSwitchStatMapFn = func(string) (map[string]string, error) { return s.switchStat, nil }
}

// restoreNameMapFns restores the map reader functions when the test ends.
func restoreNameMapFns(t *testing.T) {
	t.Helper()
	counters, alias, fabric, pfcwd, voq, switchStat := getCountersMapFn, getAliasMapFn, getFabricCountersMapFn, getPfcwdMapFn, getVoQCountersMapFn, getSwitchStatMapFn
	t.Cleanup(func() {
		getCountersMapFn, getAliasMapFn, getFabricCountersMapFn, getPfcwdMapFn, getVoQCountersMapFn, getSwitchStatMapFn = counters, alias, fabric, pfcwd, voq, switchStat
	})
}

// setupPortMaps serves two ports without vendor aliases to the translators, and
// returns the name maps for the test to change.
func setupPortMaps(t *testing.T) *nameMapStub {
	t.Helper()
	stub := &nameMapStub{
		counters: map[string]map[string]string{
			"COUNTERS_PORT_NAME_MAP": {
				"Ethernet0":  "oid:0x1000000000001",
				"Ethernet68": "oid:0x1000000000039",
			},
		},
		port2namespace: map[string]string{
			"Ethernet0":  "",
			"Ethernet68": "",
		},
	}
	stubNameMaps(t, stub)
	return stub
}

// resetNameMapCache drops the cached name maps now and when the test ends.
func resetNameMapCache(t *testing.T) {
	t.Helper()
	resetNameMaps()
	t.Cleanup(resetNameMaps)
}

// fakeNameMapClock moves only when the test advances it.
type fakeNameMapClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeNameMapClock) time() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeNameMapClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// useFakeNameMapClock runs the name map cache on a fake clock, empty, until
// the test ends.
func useFakeNameMapClock(t *testing.T) *fakeNameMapClock {
	t.Helper()
	c := &fakeNameMapClock{now: time.Now()}
	setClock := func(now func() time.Time) {
		nameMapCache.Lock()
		nameMapCache.now = now
		nameMapCache.Unlock()
		resetNameMaps()
	}
	setClock(c.time)
	t.Cleanup(func() { setClock(time.Now) })
	return c
}

// A name map is read once per nameMapMaxAge, by one lookup at a time, and a
// failed read serves the map read last.
func TestNameMapCache(t *testing.T) {
	stubNameMaps(t, &nameMapStub{})
	var mu sync.Mutex
	reads := 0
	var readErr error
	getCountersMapFn = func(string) (map[string]string, error) {
		mu.Lock()
		defer mu.Unlock()
		reads++
		if readErr != nil {
			return nil, readErr
		}
		return map[string]string{"Ethernet0": fmt.Sprintf("oid:%d", reads)}, nil
	}
	setReadErr := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		readErr = err
	}
	clock := useFakeNameMapClock(t)
	lookup := func() string {
		t.Helper()
		m, err := readCountersNameMap("COUNTERS_PORT_NAME_MAP")
		if err != nil {
			t.Fatalf("lookup: %v", err)
		}
		return m["Ethernet0"]
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			readCountersNameMap("COUNTERS_PORT_NAME_MAP")
		}()
	}
	wg.Wait()
	mu.Lock()
	if reads != 1 {
		t.Errorf("8 concurrent lookups read the map %d times, want 1", reads)
	}
	mu.Unlock()

	clock.advance(nameMapMaxAge - time.Millisecond)
	if got := lookup(); got != "oid:1" {
		t.Errorf("lookup before the map is nameMapMaxAge old got %s, want the cached oid:1", got)
	}
	clock.advance(time.Millisecond)
	if got := lookup(); got != "oid:2" {
		t.Errorf("lookup once the map is nameMapMaxAge old got %s, want oid:2 read again", got)
	}

	clock.advance(nameMapMaxAge)
	setReadErr(errors.New("mock redis error"))
	if got := lookup(); got != "oid:2" {
		t.Errorf("lookup with redis failing got %s, want the oid:2 read last", got)
	}
	got := lookup()
	mu.Lock()
	n := reads
	mu.Unlock()
	if got != "oid:2" || n != 3 {
		t.Errorf("lookup right after a failed read got %s with %d reads, want oid:2 without reading again", got, n)
	}
	setReadErr(nil)
	clock.advance(nameMapMaxAge - time.Millisecond)
	if got := lookup(); got != "oid:2" {
		t.Errorf("lookup before nameMapMaxAge after a failed read got %s, want oid:2", got)
	}
	clock.advance(time.Millisecond)
	if got := lookup(); got != "oid:4" {
		t.Errorf("lookup nameMapMaxAge after a failed read got %s, want oid:4 read again", got)
	}

	t.Setenv("UNIT_TEST", "1")
	ClearMappings()
	if got := lookup(); got != "oid:5" {
		t.Errorf("lookup after ClearMappings got %s, want oid:5 read again", got)
	}

	// with no map read yet, a failed read returns the error and is not kept
	ClearMappings()
	setReadErr(errors.New("mock redis error"))
	var mapErr *nameMapReadError
	if _, err := readCountersNameMap("COUNTERS_PORT_NAME_MAP"); !errors.As(err, &mapErr) {
		t.Errorf("first lookup with redis failing got error %v, want a name map read error", err)
	}
	setReadErr(nil)
	if got := lookup(); got != "oid:7" {
		t.Errorf("lookup after a failed first read got %s, want oid:7 read again", got)
	}
}

// --------------------------------------------------------------------------
// Tests for countersDbHasTableKeys (db_client.go)
// --------------------------------------------------------------------------

func TestCountersDbHasTableKeys(t *testing.T) {
	tests := []struct {
		tableName string
		want      bool
	}{
		{"COUNTERS", true},
		{"PORT_PHY_ATTR", true},
		{"COUNTERS_PORT_NAME_MAP", false},
		{"RATES", false},
		{"PERIODIC_WATERMARKS", false},
		{"ACL_COUNTER_RULE_MAP", false},
		{"", false},
	}
	for _, tt := range tests {
		t.Run(tt.tableName, func(t *testing.T) {
			got := countersDbHasTableKeys(tt.tableName)
			if got != tt.want {
				t.Errorf("countersDbHasTableKeys(%q) = %v, want %v", tt.tableName, got, tt.want)
			}
		})
	}
}

// --------------------------------------------------------------------------
// Tests for v2rPortPhyAttrStats (virtual_db.go)
// --------------------------------------------------------------------------

func TestV2rPortPhyAttrStats_Wildcard(t *testing.T) {
	sdcfg.Init()
	setupPortMaps(t)

	paths := []string{"COUNTERS_DB", "PORT_PHY_ATTR", "Ethernet*"}
	tblPaths, err := v2rPortPhyAttrStats(paths)
	if err != nil {
		t.Fatalf("v2rPortPhyAttrStats returned error: %v", err)
	}
	if len(tblPaths) != 2 {
		t.Fatalf("expected 2 table paths, got %d", len(tblPaths))
	}

	// Sort for deterministic comparison (map iteration order is random)
	sort.Slice(tblPaths, func(i, j int) bool {
		return tblPaths[i].jsonTableKey < tblPaths[j].jsonTableKey
	})

	// Ethernet0
	tp := tblPaths[0]
	if tp.dbName != "COUNTERS_DB" {
		t.Errorf("dbName = %q, want COUNTERS_DB", tp.dbName)
	}
	if tp.tableName != "PORT_PHY_ATTR" {
		t.Errorf("tableName = %q, want PORT_PHY_ATTR", tp.tableName)
	}
	if tp.tableKey != "oid:0x1000000000001" {
		t.Errorf("tableKey = %q, want oid:0x1000000000001", tp.tableKey)
	}
	if tp.jsonTableKey != "Ethernet0" {
		t.Errorf("jsonTableKey = %q, want Ethernet0 (no alias translation)", tp.jsonTableKey)
	}

	// Ethernet68
	tp = tblPaths[1]
	if tp.tableKey != "oid:0x1000000000039" {
		t.Errorf("tableKey = %q, want oid:0x1000000000039", tp.tableKey)
	}
	if tp.jsonTableKey != "Ethernet68" {
		t.Errorf("jsonTableKey = %q, want Ethernet68 (no alias translation)", tp.jsonTableKey)
	}
}

func TestV2rPortPhyAttrStats_SinglePort(t *testing.T) {
	sdcfg.Init()
	setupPortMaps(t)

	paths := []string{"COUNTERS_DB", "PORT_PHY_ATTR", "Ethernet68"}
	tblPaths, err := v2rPortPhyAttrStats(paths)
	if err != nil {
		t.Fatalf("v2rPortPhyAttrStats returned error: %v", err)
	}
	if len(tblPaths) != 1 {
		t.Fatalf("expected 1 table path, got %d", len(tblPaths))
	}

	tp := tblPaths[0]
	if tp.dbName != "COUNTERS_DB" {
		t.Errorf("dbName = %q, want COUNTERS_DB", tp.dbName)
	}
	if tp.tableName != "PORT_PHY_ATTR" {
		t.Errorf("tableName = %q, want PORT_PHY_ATTR", tp.tableName)
	}
	if tp.tableKey != "oid:0x1000000000039" {
		t.Errorf("tableKey = %q, want oid:0x1000000000039", tp.tableKey)
	}
	// Single port mode should NOT set jsonTableKey
	if tp.jsonTableKey != "" {
		t.Errorf("jsonTableKey = %q, want empty for single port", tp.jsonTableKey)
	}
}

func TestV2rPortPhyAttrStats_InvalidPort(t *testing.T) {
	sdcfg.Init()
	setupPortMaps(t)

	paths := []string{"COUNTERS_DB", "PORT_PHY_ATTR", "EthernetXYZ"}
	_, err := v2rPortPhyAttrStats(paths)
	if err == nil {
		t.Fatal("expected error for invalid port name, got nil")
	}
}

func TestV2rPortPhyAttrStats_EmptyMap(t *testing.T) {
	sdcfg.Init()
	stub := setupPortMaps(t)

	// Clear the maps so no ports exist at all
	stub.counters["COUNTERS_PORT_NAME_MAP"] = map[string]string{}
	stub.port2namespace = map[string]string{}

	// Wildcard: should succeed with zero results (no ports to iterate)
	paths := []string{"COUNTERS_DB", "PORT_PHY_ATTR", "Ethernet*"}
	tblPaths, err := v2rPortPhyAttrStats(paths)
	if err != nil {
		t.Fatalf("expected no error for wildcard with empty map, got: %v", err)
	}
	if len(tblPaths) != 0 {
		t.Errorf("expected 0 table paths, got %d", len(tblPaths))
	}

	// Single port: should fail with "not a valid sonic interface"
	paths = []string{"COUNTERS_DB", "PORT_PHY_ATTR", "Ethernet0"}
	_, err = v2rPortPhyAttrStats(paths)
	if err == nil {
		t.Fatal("expected error for single port with empty map, got nil")
	}
}

func TestV2rPortPhyAttrStats_MissingNamespace(t *testing.T) {
	sdcfg.Init()
	stub := setupPortMaps(t)

	// Add a port to the OID map but NOT to the namespace map
	stub.counters["COUNTERS_PORT_NAME_MAP"]["Ethernet99"] = "oid:0x1000000000099"

	// Wildcard skips Ethernet99, which has no namespace
	paths := []string{"COUNTERS_DB", "PORT_PHY_ATTR", "Ethernet*"}
	tblPaths, err := v2rPortPhyAttrStats(paths)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tblPaths) != 2 {
		t.Errorf("expected the 2 ports with a namespace, got %v", tblPaths)
	}
}

func TestV2rPortPhyAttrStats_SingleMissingNamespace(t *testing.T) {
	sdcfg.Init()
	stub := setupPortMaps(t)

	// Ethernet68 has an oid. Remove its namespace.
	delete(stub.port2namespace, "Ethernet68")

	paths := []string{"COUNTERS_DB", "PORT_PHY_ATTR", "Ethernet68"}
	_, err := v2rPortPhyAttrStats(paths)
	if err == nil {
		t.Fatal("expected error for port missing namespace, got nil")
	}
}

// --------------------------------------------------------------------------
// Tests for v2rPortPhyAttrFieldStats (virtual_db.go)
// --------------------------------------------------------------------------

func TestV2rPortPhyAttrFieldStats_Wildcard(t *testing.T) {
	sdcfg.Init()
	setupPortMaps(t)

	paths := []string{"COUNTERS_DB", "PORT_PHY_ATTR", "Ethernet*", "phy_rx_signal_detect"}
	tblPaths, err := v2rPortPhyAttrFieldStats(paths)
	if err != nil {
		t.Fatalf("v2rPortPhyAttrFieldStats returned error: %v", err)
	}
	if len(tblPaths) != 2 {
		t.Fatalf("expected 2 table paths, got %d", len(tblPaths))
	}

	sort.Slice(tblPaths, func(i, j int) bool {
		return tblPaths[i].jsonTableKey < tblPaths[j].jsonTableKey
	})

	tp := tblPaths[0]
	if tp.tableName != "PORT_PHY_ATTR" {
		t.Errorf("tableName = %q, want PORT_PHY_ATTR", tp.tableName)
	}
	if tp.tableKey != "oid:0x1000000000001" {
		t.Errorf("tableKey = %q, want oid:0x1000000000001", tp.tableKey)
	}
	if tp.field != "phy_rx_signal_detect" {
		t.Errorf("field = %q, want phy_rx_signal_detect", tp.field)
	}
	if tp.jsonTableKey != "Ethernet0" {
		t.Errorf("jsonTableKey = %q, want Ethernet0 (no alias)", tp.jsonTableKey)
	}
	if tp.jsonField != "phy_rx_signal_detect" {
		t.Errorf("jsonField = %q, want phy_rx_signal_detect", tp.jsonField)
	}

	tp = tblPaths[1]
	if tp.jsonTableKey != "Ethernet68" {
		t.Errorf("jsonTableKey = %q, want Ethernet68 (no alias)", tp.jsonTableKey)
	}
	if tp.field != "phy_rx_signal_detect" {
		t.Errorf("field = %q, want phy_rx_signal_detect", tp.field)
	}
}

func TestV2rPortPhyAttrFieldStats_SinglePort(t *testing.T) {
	sdcfg.Init()
	setupPortMaps(t)

	paths := []string{"COUNTERS_DB", "PORT_PHY_ATTR", "Ethernet0", "rx_snr"}
	tblPaths, err := v2rPortPhyAttrFieldStats(paths)
	if err != nil {
		t.Fatalf("v2rPortPhyAttrFieldStats returned error: %v", err)
	}
	if len(tblPaths) != 1 {
		t.Fatalf("expected 1 table path, got %d", len(tblPaths))
	}

	tp := tblPaths[0]
	if tp.dbName != "COUNTERS_DB" {
		t.Errorf("dbName = %q, want COUNTERS_DB", tp.dbName)
	}
	if tp.tableName != "PORT_PHY_ATTR" {
		t.Errorf("tableName = %q, want PORT_PHY_ATTR", tp.tableName)
	}
	if tp.tableKey != "oid:0x1000000000001" {
		t.Errorf("tableKey = %q, want oid:0x1000000000001", tp.tableKey)
	}
	if tp.field != "rx_snr" {
		t.Errorf("field = %q, want rx_snr", tp.field)
	}
	// Single port mode should NOT set jsonTableKey or jsonField
	if tp.jsonTableKey != "" {
		t.Errorf("jsonTableKey = %q, want empty for single port", tp.jsonTableKey)
	}
	if tp.jsonField != "" {
		t.Errorf("jsonField = %q, want empty for single port", tp.jsonField)
	}
}

func TestV2rPortPhyAttrFieldStats_InvalidPort(t *testing.T) {
	sdcfg.Init()
	setupPortMaps(t)

	paths := []string{"COUNTERS_DB", "PORT_PHY_ATTR", "EthernetBad", "phy_rx_signal_detect"}
	_, err := v2rPortPhyAttrFieldStats(paths)
	if err == nil {
		t.Fatal("expected error for invalid port name, got nil")
	}
}

func TestV2rPortPhyAttrFieldStats_EmptyMap(t *testing.T) {
	sdcfg.Init()
	stub := setupPortMaps(t)

	stub.counters["COUNTERS_PORT_NAME_MAP"] = map[string]string{}
	stub.port2namespace = map[string]string{}

	// Wildcard: should succeed with zero results
	paths := []string{"COUNTERS_DB", "PORT_PHY_ATTR", "Ethernet*", "phy_rx_signal_detect"}
	tblPaths, err := v2rPortPhyAttrFieldStats(paths)
	if err != nil {
		t.Fatalf("expected no error for wildcard with empty map, got: %v", err)
	}
	if len(tblPaths) != 0 {
		t.Errorf("expected 0 table paths, got %d", len(tblPaths))
	}

	// Single port: should fail
	paths = []string{"COUNTERS_DB", "PORT_PHY_ATTR", "Ethernet0", "phy_rx_signal_detect"}
	_, err = v2rPortPhyAttrFieldStats(paths)
	if err == nil {
		t.Fatal("expected error for single port with empty map, got nil")
	}
}

func TestV2rPortPhyAttrFieldStats_MissingNamespace(t *testing.T) {
	sdcfg.Init()
	stub := setupPortMaps(t)

	// Add port to OID map but not namespace map
	stub.counters["COUNTERS_PORT_NAME_MAP"]["Ethernet99"] = "oid:0x1000000000099"

	paths := []string{"COUNTERS_DB", "PORT_PHY_ATTR", "Ethernet*", "phy_rx_signal_detect"}
	tblPaths, err := v2rPortPhyAttrFieldStats(paths)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tblPaths) != 2 {
		t.Errorf("expected the 2 ports with a namespace, got %v", tblPaths)
	}
}

func TestV2rPortPhyAttrFieldStats_SingleMissingNamespace(t *testing.T) {
	sdcfg.Init()
	stub := setupPortMaps(t)

	delete(stub.port2namespace, "Ethernet0")

	paths := []string{"COUNTERS_DB", "PORT_PHY_ATTR", "Ethernet0", "rx_snr"}
	_, err := v2rPortPhyAttrFieldStats(paths)
	if err == nil {
		t.Fatal("expected error for port missing namespace, got nil")
	}
}

// --------------------------------------------------------------------------
// Tests for name map read errors
// --------------------------------------------------------------------------

// mockError is a helper to swap a function variable, returning a restore func.
func setupErrorMock(t *testing.T) func() {
	t.Helper()
	resetNameMapCache(t)
	restoreNameMapFns(t)

	mockErr := fmt.Errorf("mock redis error")
	getCountersMapFn = func(string) (map[string]string, error) { return nil, mockErr }
	getAliasMapFn = func() (map[string]string, map[string]string, map[string]string, error) {
		return nil, nil, nil, mockErr
	}
	getFabricCountersMapFn = func(string) (map[string]string, error) { return nil, mockErr }
	getPfcwdMapFn = func() (map[string]map[string]string, error) { return nil, mockErr }
	getVoQCountersMapFn = func(string) (map[string]string, map[string]string, error) { return nil, nil, mockErr }
	getSwitchStatMapFn = func(string) (map[string]string, error) { return nil, mockErr }

	return func() {}
}

func TestV2rNameMapReadError(t *testing.T) {
	sdcfg.Init()
	restore := setupErrorMock(t)
	defer restore()

	for _, paths := range [][]string{
		{"COUNTERS_DB", "COUNTERS", "Ethernet*"},
		{"COUNTERS_DB", "COUNTERS", "Ethernet0", "SAI_PORT_STAT_IF_IN_OCTETS"},
		{"COUNTERS_DB", "INTERFACE_COUNTERS"},
		{"COUNTERS_DB", "COUNTERS", "Ethernet0", "Queues"},
		{"COUNTERS_DB", "COUNTERS", "Ethernet0", "Pfcwd"},
		{"COUNTERS_DB", "COUNTERS", "PORT*"},
		{"COUNTERS_DB", "PERIODIC_WATERMARKS", "Ethernet0", "PriorityGroups"},
		{"COUNTERS_DB", "COUNTERS", "SID*"},
		{"COUNTERS_DB", "COUNTERS", "ACL_RULE*"},
		{"COUNTERS_DB", "PORT_PHY_ATTR", "Ethernet0"},
		{"COUNTERS_DB", "PORT_PHY_ATTR", "Ethernet*", "phy_rx_signal_detect"},
		{"COUNTERS_DB", "COUNTERS", "SwitchName*", "VoQs"},
		{"COUNTERS_DB", "COUNTERS", "SWITCH*"},
	} {
		_, err := lookupV2R(paths)
		if !IsNameMapReadError(err) {
			t.Errorf("%v: got error %v, want a name map read error", paths, err)
		}
	}

	// the error names the map
	_, err := lookupV2R([]string{"COUNTERS_DB", "COUNTERS", "Ethernet*"})
	if want := "read COUNTERS_PORT_NAME_MAP: mock redis error"; err == nil || err.Error() != want {
		t.Errorf("got error %v, want %q", err, want)
	}

	// the path is not read as a physical COUNTERS key instead
	path := &gnmipb.Path{Elem: []*gnmipb.PathElem{{Name: "COUNTERS"}, {Name: "Ethernet0"}}}
	if paths, err := PopulateTablePaths(&gnmipb.Path{Target: "COUNTERS_DB"}, path); err == nil {
		t.Errorf("COUNTERS/Ethernet0 resolved to %v, want the read error", paths)
	}
}

// A path over all ports skips a port that has no CONFIG_DB row yet, while a
// path naming that port fails.
func TestWildcardPathsSkipPortWithoutNamespace(t *testing.T) {
	sdcfg.Init()
	eth0Oids := map[string]bool{"oid:0x1000000000001": true, "oid:0x15000000000003": true, "oid:0x1a000000000003": true}
	stubNameMaps(t, &nameMapStub{
		counters: map[string]map[string]string{
			"COUNTERS_PORT_NAME_MAP":  {"Ethernet0": "oid:0x1000000000001", "Ethernet4": "oid:0x1000000000005"},
			"COUNTERS_QUEUE_NAME_MAP": {"Ethernet0:3": "oid:0x15000000000003", "Ethernet4:3": "oid:0x15000000000043"},
			"COUNTERS_PG_NAME_MAP":    {"Ethernet0:3": "oid:0x1a000000000003", "Ethernet4:3": "oid:0x1a000000000043"},
		},
		pfcwd: map[string]map[string]string{
			"Ethernet0": {"Ethernet0:3": "oid:0x15000000000003"},
			"Ethernet4": {"Ethernet4:3": "oid:0x15000000000043"},
		},
		port2namespace: map[string]string{"Ethernet0": ""},
	})

	for _, paths := range [][]string{
		{"COUNTERS_DB", "COUNTERS", "Ethernet*"},
		{"COUNTERS_DB", "INTERFACE_COUNTERS"},
		{"COUNTERS_DB", "COUNTERS", "Ethernet*", "SAI_PORT_STAT_IF_IN_OCTETS"},
		{"COUNTERS_DB", "COUNTERS", "Ethernet*", "Pfcwd"},
		{"COUNTERS_DB", "COUNTERS", "Ethernet*", "Queues"},
		{"COUNTERS_DB", "PERIODIC_WATERMARKS", "Ethernet*", "PriorityGroups"},
		{"COUNTERS_DB", "PORT_PHY_ATTR", "Ethernet*"},
		{"COUNTERS_DB", "PORT_PHY_ATTR", "Ethernet*", "phy_rx_signal_detect"},
	} {
		tblPaths, err := lookupV2R(paths)
		if err != nil {
			t.Errorf("%v: %v", paths, err)
			continue
		}
		if len(tblPaths) == 0 {
			t.Errorf("%v resolved to no paths, want Ethernet0", paths)
		}
		for _, p := range tblPaths {
			if !eth0Oids[p.tableKey] {
				t.Errorf("%v resolved to %v, want Ethernet0 alone", paths, tblPaths)
				break
			}
		}
	}

	if tblPaths, err := lookupV2R([]string{"COUNTERS_DB", "COUNTERS", "Ethernet4"}); err == nil {
		t.Errorf("COUNTERS/Ethernet4 resolved to %v, want an error", tblPaths)
	}
}

func TestV2rFabricPortStats_EmptyMap(t *testing.T) {
	sdcfg.Init()
	stubNameMaps(t, &nameMapStub{fabric: map[string]string{}})

	paths := []string{"COUNTERS_DB", "COUNTERS", "PORT*"}
	tblPaths, err := v2rFabricPortStats(paths)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tblPaths) != 0 {
		t.Errorf("expected 0 paths, got %d", len(tblPaths))
	}
}

func TestV2rEthPortStats_EmptyMap(t *testing.T) {
	sdcfg.Init()
	stub := setupPortMaps(t)
	stub.counters["COUNTERS_PORT_NAME_MAP"] = map[string]string{}

	paths := []string{"COUNTERS_DB", "COUNTERS", "Ethernet*"}
	tblPaths, err := v2rEthPortStats(paths)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tblPaths) != 0 {
		t.Errorf("expected 0 paths, got %d", len(tblPaths))
	}
}

func TestV2rEthPortFieldStats_EmptyMap(t *testing.T) {
	sdcfg.Init()
	stub := setupPortMaps(t)
	stub.counters["COUNTERS_PORT_NAME_MAP"] = map[string]string{}

	paths := []string{"COUNTERS_DB", "COUNTERS", "Ethernet*", "SAI_PORT_STAT_IF_IN_OCTETS"}
	tblPaths, err := v2rEthPortFieldStats(paths)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tblPaths) != 0 {
		t.Errorf("expected 0 paths, got %d", len(tblPaths))
	}
}

func TestV2rEthPortPfcwdStats_EmptyMap(t *testing.T) {
	sdcfg.Init()
	stubNameMaps(t, &nameMapStub{pfcwd: map[string]map[string]string{}})

	paths := []string{"COUNTERS_DB", "COUNTERS", "Ethernet*", "Pfcwd"}
	tblPaths, err := v2rEthPortPfcwdStats(paths)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tblPaths) != 0 {
		t.Errorf("expected 0 paths, got %d", len(tblPaths))
	}
}

func TestV2rEthPortQueStats_EmptyMap(t *testing.T) {
	sdcfg.Init()
	stubNameMaps(t, &nameMapStub{counters: map[string]map[string]string{"COUNTERS_QUEUE_NAME_MAP": {}}})

	paths := []string{"COUNTERS_DB", "COUNTERS", "Ethernet*", "Queues"}
	tblPaths, err := v2rEthPortQueStats(paths)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tblPaths) != 0 {
		t.Errorf("expected 0 paths, got %d", len(tblPaths))
	}
}

func TestV2rAclRuleStats_EmptyMap(t *testing.T) {
	sdcfg.Init()
	stubNameMaps(t, &nameMapStub{counters: map[string]map[string]string{"ACL_COUNTER_RULE_MAP": {}}})

	paths := []string{"COUNTERS_DB", "COUNTERS", "ACL_RULE*"}
	tblPaths, err := v2rAclRuleStats(paths)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tblPaths) != 0 {
		t.Errorf("expected 0 paths, got %d", len(tblPaths))
	}
}

func TestAliasToPortNameMap(t *testing.T) {
	sdcfg.Init()
	stub := &nameMapStub{alias2name: map[string]string{"Eth0": "Ethernet0"}}
	stubNameMaps(t, stub)
	clock := useFakeNameMapClock(t)

	result := AliasToPortNameMap()
	if result["Eth0"] != "Ethernet0" {
		t.Errorf("expected Eth0->Ethernet0, got %v", result)
	}

	stub.alias2name = map[string]string{"Eth0": "Ethernet0", "Eth4": "Ethernet4"}
	clock.advance(nameMapMaxAge)
	if result := AliasToPortNameMap(); result["Eth4"] != "Ethernet4" {
		t.Errorf("expected Eth4->Ethernet4 once the port is added, got %v", result)
	}

	getAliasMapFn = func() (map[string]string, map[string]string, map[string]string, error) {
		return nil, nil, nil, fmt.Errorf("mock redis error")
	}
	clock.advance(nameMapMaxAge)
	if result := AliasToPortNameMap(); result["Eth4"] != "Ethernet4" {
		t.Errorf("expected the aliases read last when CONFIG_DB cannot be read, got %v", result)
	}

	resetNameMaps()
	if result := AliasToPortNameMap(); result == nil || len(result) != 0 {
		t.Errorf("expected an empty map when CONFIG_DB has never been read, got %#v", result)
	}
}

func TestClearMappings_Resets(t *testing.T) {
	t.Setenv("UNIT_TEST", "1")
	stub := &nameMapStub{voqNameToOid: map[string]string{"testvoq": "testoid"}}
	stubNameMaps(t, stub)
	useFakeNameMapClock(t)
	if m, err := readVoQNameMaps(); err != nil || m.nameToOid["testvoq"] != "testoid" {
		t.Fatalf("readVoQNameMaps = %v, %v", m, err)
	}

	stub.voqNameToOid = map[string]string{}
	ClearMappings()

	if m, err := readVoQNameMaps(); err != nil || len(m.nameToOid) != 0 {
		t.Errorf("expected ClearMappings to drop the cached VoQ map, got %v, %v", m, err)
	}
}

func TestResetRedisClients(t *testing.T) {
	// Verify ResetRedisClients resets the sync.Once and error
	ResetRedisClients()

	// After reset, the sync.Once should fire again on next NewDbClient call
	// Exercise the UseRedisLocalTcpPort path
	origFlag := UseRedisLocalTcpPort
	UseRedisLocalTcpPort = true
	defer func() { UseRedisLocalTcpPort = origFlag }()

	sdcfg.Init()

	useRedisTcpClientOnce.Do(func() {
		useRedisTcpClientErr = fmt.Errorf("mock error")
	})

	_, err := NewDbClient(nil, nil)
	if err == nil {
		t.Errorf("expected error from NewDbClient when useRedisTcpClient fails")
	}

	// Reset again to clean up for other tests
	ResetRedisClients()
}

// --------------------------------------------------------------------------
// Tests for v2rInterfaceCountersStats (virtual_db.go)
// --------------------------------------------------------------------------

func TestV2rInterfaceCountersStats(t *testing.T) {
	sdcfg.Init()
	setupPortMaps(t)

	paths := []string{"COUNTERS_DB", "INTERFACE_COUNTERS"}
	tblPaths, err := v2rInterfaceCountersStats(paths)
	if err != nil {
		t.Fatalf("v2rInterfaceCountersStats returned error: %v", err)
	}
	if len(tblPaths) != 2 {
		t.Fatalf("expected 2 table paths, got %d", len(tblPaths))
	}

	// Sort for deterministic comparison (map iteration order is random).
	sort.Slice(tblPaths, func(i, j int) bool {
		return tblPaths[i].jsonTableKey < tblPaths[j].jsonTableKey
	})

	// Ethernet0
	tp := tblPaths[0]
	if tp.dbName != "COUNTERS_DB" {
		t.Errorf("dbName = %q, want COUNTERS_DB", tp.dbName)
	}
	if tp.tableName != "COUNTERS" {
		t.Errorf("tableName = %q, want COUNTERS (real backing table)", tp.tableName)
	}
	if tp.tableKey != "oid:0x1000000000001" {
		t.Errorf("tableKey = %q, want oid:0x1000000000001", tp.tableKey)
	}
	if tp.jsonTableKey != "Ethernet0" {
		t.Errorf("jsonTableKey = %q, want Ethernet0", tp.jsonTableKey)
	}

	// Ethernet68
	tp = tblPaths[1]
	if tp.tableName != "COUNTERS" {
		t.Errorf("tableName = %q, want COUNTERS", tp.tableName)
	}
	if tp.tableKey != "oid:0x1000000000039" {
		t.Errorf("tableKey = %q, want oid:0x1000000000039", tp.tableKey)
	}
	if tp.jsonTableKey != "Ethernet68" {
		t.Errorf("jsonTableKey = %q, want Ethernet68", tp.jsonTableKey)
	}
}

// --------------------------------------------------------------------------
// Tests for Trie findNode backtracking (trie.go)
// --------------------------------------------------------------------------

func TestTrieBacktracking_VoQOverEthernet(t *testing.T) {
	// Ensure the global trie correctly routes VoQ paths through the "*" wildcard
	// when "Ethernet*" is also present at the same level.
	sdcfg.Init()

	// "str2-7804-lc7-1" with "VoQs" suffix should match "*" -> "VoQs" (VoQ handler)
	keys := []string{"COUNTERS_DB", "COUNTERS", "str2-7804-lc7-1", "VoQs"}
	node, ok := v2rTrie.Find(keys)
	if !ok || node == nil {
		t.Fatal("expected trie to find VoQ path for str2-7804-lc7-1/VoQs")
	}

	// "str2-7804-lc7-1|Asic0|Ethernet68" with "VoQs" should also match
	keys = []string{"COUNTERS_DB", "COUNTERS", "str2-7804-lc7-1|Asic0|Ethernet68", "VoQs"}
	node, ok = v2rTrie.Find(keys)
	if !ok || node == nil {
		t.Fatal("expected trie to find VoQ path for system port/VoQs")
	}
}

func TestTrieBacktracking_EthernetStillWorks(t *testing.T) {
	sdcfg.Init()

	// "Ethernet68" alone should still match "Ethernet*" (port stats)
	keys := []string{"COUNTERS_DB", "COUNTERS", "Ethernet68"}
	node, ok := v2rTrie.Find(keys)
	if !ok || node == nil {
		t.Fatal("expected trie to find Ethernet port stats path")
	}

	// "Ethernet68" with "Queues" should match "Ethernet*" -> "Queues"
	keys = []string{"COUNTERS_DB", "COUNTERS", "Ethernet68", "Queues"}
	node, ok = v2rTrie.Find(keys)
	if !ok || node == nil {
		t.Fatal("expected trie to find Ethernet queue stats path")
	}

	// "Ethernet68" with a field should match "Ethernet*" -> "*"
	keys = []string{"COUNTERS_DB", "COUNTERS", "Ethernet68", "SAI_PORT_STAT_IF_IN_OCTETS"}
	node, ok = v2rTrie.Find(keys)
	if !ok || node == nil {
		t.Fatal("expected trie to find Ethernet field stats path")
	}
}

func TestTrieBacktracking_NonExistentPath(t *testing.T) {
	sdcfg.Init()

	// Path that doesn't match anything
	keys := []string{"COUNTERS_DB", "COUNTERS", "str2-7804-lc7-1", "NonExistent"}
	_, ok := v2rTrie.Find(keys)
	if ok {
		t.Fatal("expected trie NOT to find non-existent path")
	}
}

// --------------------------------------------------------------------------
// Tests for parseVoQName (virtual_db.go)
// --------------------------------------------------------------------------

func TestParseVoQName_Valid(t *testing.T) {
	switchId, asicNs, ifName, voqIdx, err := parseVoQName("str2-7804-lc7-1|Asic0|Ethernet84:3")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if switchId != "str2-7804-lc7-1" {
		t.Errorf("switchId = %q, want str2-7804-lc7-1", switchId)
	}
	if asicNs != "Asic0" {
		t.Errorf("asicNamespace = %q, want Asic0", asicNs)
	}
	if ifName != "Ethernet84" {
		t.Errorf("interfaceName = %q, want Ethernet84", ifName)
	}
	if voqIdx != "3" {
		t.Errorf("voqIndex = %q, want 3", voqIdx)
	}
}

func TestParseVoQName_InvalidPipeParts(t *testing.T) {
	_, _, _, _, err := parseVoQName("str2-7804-lc7-1|Asic0")
	if err == nil {
		t.Fatal("expected error for only 2 pipe-separated parts")
	}
}

func TestParseVoQName_InvalidInterfaceFormat(t *testing.T) {
	_, _, _, _, err := parseVoQName("str2-7804-lc7-1|Asic0|Ethernet84")
	if err == nil {
		t.Fatal("expected error for missing colon in interface:voq")
	}
}

// --------------------------------------------------------------------------
// Tests for buildVoQJsonKey (virtual_db.go)
// --------------------------------------------------------------------------

func TestBuildVoQJsonKey(t *testing.T) {
	key := buildVoQJsonKey("str2-7804-lc7-1", "Asic0", "Ethernet84", "3")
	expected := "str2-7804-lc7-1|Asic0|Ethernet84:3"
	if key != expected {
		t.Errorf("got %q, want %q", key, expected)
	}
}

// --------------------------------------------------------------------------
// Tests for resolveVoQNamespace (virtual_db.go)
// --------------------------------------------------------------------------

func TestResolveVoQNamespace_FoundInMap(t *testing.T) {
	oidToNamespace := map[string]string{
		"oid:0x123": "asic0",
	}

	activeNs := map[string]*redis.Client{
		"asic0": redis.NewClient(&redis.Options{Addr: "localhost:6379"}),
	}

	ns, ok := resolveVoQNamespace(oidToNamespace, "oid:0x123", activeNs)
	if !ok {
		t.Fatalf("got ok=false, want ok=true")
	}
	if ns != "asic0" {
		t.Errorf("got %q, want asic0", ns)
	}
}

func TestResolveVoQNamespace_StaleNamespace(t *testing.T) {
	oidToNamespace := map[string]string{
		"oid:0x123": "asic2", // mapped to asic2
	}

	// activeNamespaces does NOT contain asic2
	activeNs := map[string]*redis.Client{
		"asic0": redis.NewClient(&redis.Options{Addr: "localhost:6379"}),
	}

	// Owning namespace is no longer active: do not guess, report ok=false.
	ns, ok := resolveVoQNamespace(oidToNamespace, "oid:0x123", activeNs)
	if ok {
		t.Errorf("got ok=true (ns=%q), want ok=false for stale namespace", ns)
	}
}

func TestResolveVoQNamespace_NotInMap(t *testing.T) {
	oidToNamespace := map[string]string{}

	activeNs := map[string]*redis.Client{
		"asic0": redis.NewClient(&redis.Options{Addr: "localhost:6379"}),
	}

	// Unknown owner: do not guess, report ok=false.
	ns, ok := resolveVoQNamespace(oidToNamespace, "oid:0x999", activeNs)
	if ok {
		t.Errorf("got ok=true (ns=%q), want ok=false for unknown OID", ns)
	}
}

// --------------------------------------------------------------------------
// Tests for v2rSystemPortVoQStats (virtual_db.go)
// --------------------------------------------------------------------------

// setupVoQMaps serves four VoQs to the translator, and returns the maps for the
// test to change before its first lookup.
func setupVoQMaps(t *testing.T) (*nameMapStub, func()) {
	t.Helper()

	origTarget2Redis := Target2RedisDb

	stub := &nameMapStub{}
	stub.voqNameToOid = map[string]string{
		"str2-7804-lc7-1|Asic0|Ethernet68:0": "oid:0x160000000091c",
		"str2-7804-lc7-1|Asic0|Ethernet68:1": "oid:0x160000000091d",
		"str2-7804-lc7-1|Asic0|Ethernet84:0": "oid:0x160000000091e",
		"str2-7804-lc7-1|Asic0|Ethernet84:1": "oid:0x160000000092a",
	}
	stub.voqOidToNamespace = map[string]string{
		"oid:0x160000000091c": "",
		"oid:0x160000000091d": "",
		"oid:0x160000000091e": "",
		"oid:0x160000000092a": "",
	}

	// Set up a minimal Target2RedisDb so GetRedisClientsForDb returns a client
	Target2RedisDb = map[string]map[string]*redis.Client{
		"": {
			"COUNTERS_DB": redis.NewClient(&redis.Options{Addr: "localhost:6379"}),
		},
	}

	stubNameMaps(t, stub)

	return stub, func() {
		Target2RedisDb = origTarget2Redis
	}
}

func TestV2rSystemPortVoQStats_Wildcard(t *testing.T) {
	sdcfg.Init()
	_, restore := setupVoQMaps(t)
	defer restore()

	paths := []string{"COUNTERS_DB", "COUNTERS", "SwitchName*", "VoQs"}
	tblPaths, err := v2rSystemPortVoQStats(paths)
	if err != nil {
		t.Fatalf("v2rSystemPortVoQStats returned error: %v", err)
	}
	if len(tblPaths) != 4 {
		t.Fatalf("expected 4 table paths, got %d", len(tblPaths))
	}

	// Verify all paths have expected fields
	for _, tp := range tblPaths {
		if tp.dbName != "COUNTERS_DB" {
			t.Errorf("dbName = %q, want COUNTERS_DB", tp.dbName)
		}
		if tp.tableName != "COUNTERS" {
			t.Errorf("tableName = %q, want COUNTERS", tp.tableName)
		}
		if tp.jsonTableKey == "" {
			t.Error("jsonTableKey should not be empty for wildcard VoQ query")
		}
	}
}

func TestV2rSystemPortVoQStats_SingleSwitch(t *testing.T) {
	sdcfg.Init()
	_, restore := setupVoQMaps(t)
	defer restore()

	paths := []string{"COUNTERS_DB", "COUNTERS", "str2-7804-lc7-1", "VoQs"}
	tblPaths, err := v2rSystemPortVoQStats(paths)
	if err != nil {
		t.Fatalf("v2rSystemPortVoQStats returned error: %v", err)
	}
	// All 4 VoQs belong to str2-7804-lc7-1
	if len(tblPaths) != 4 {
		t.Fatalf("expected 4 table paths for single switch, got %d", len(tblPaths))
	}
}

func TestV2rSystemPortVoQStats_SystemPort(t *testing.T) {
	sdcfg.Init()
	_, restore := setupVoQMaps(t)
	defer restore()

	paths := []string{"COUNTERS_DB", "COUNTERS", "str2-7804-lc7-1|Asic0|Ethernet68", "VoQs"}
	tblPaths, err := v2rSystemPortVoQStats(paths)
	if err != nil {
		t.Fatalf("v2rSystemPortVoQStats returned error: %v", err)
	}
	// Only 2 VoQs match Ethernet68
	if len(tblPaths) != 2 {
		t.Fatalf("expected 2 table paths for system port, got %d", len(tblPaths))
	}

	sort.Slice(tblPaths, func(i, j int) bool {
		return tblPaths[i].jsonTableKey < tblPaths[j].jsonTableKey
	})

	// Ethernet68:0
	tp := tblPaths[0]
	if tp.dbName != "COUNTERS_DB" {
		t.Errorf("dbName = %q, want COUNTERS_DB", tp.dbName)
	}
	if tp.tableName != "COUNTERS" {
		t.Errorf("tableName = %q, want COUNTERS", tp.tableName)
	}
	if tp.tableKey != "oid:0x160000000091c" {
		t.Errorf("tableKey = %q, want oid:0x160000000091c", tp.tableKey)
	}
	if tp.jsonTableKey != "str2-7804-lc7-1|Asic0|Ethernet68:0" {
		t.Errorf("jsonTableKey = %q, want str2-7804-lc7-1|Asic0|Ethernet68:0", tp.jsonTableKey)
	}

	// Ethernet68:1
	tp = tblPaths[1]
	if tp.tableName != "COUNTERS" {
		t.Errorf("tableName = %q, want COUNTERS", tp.tableName)
	}
	if tp.tableKey != "oid:0x160000000091d" {
		t.Errorf("tableKey = %q, want oid:0x160000000091d", tp.tableKey)
	}
	if tp.jsonTableKey != "str2-7804-lc7-1|Asic0|Ethernet68:1" {
		t.Errorf("jsonTableKey = %q, want str2-7804-lc7-1|Asic0|Ethernet68:1", tp.jsonTableKey)
	}
}

func TestV2rInterfaceCountersStats_EmptyMap(t *testing.T) {
	sdcfg.Init()
	stub := setupPortMaps(t)

	stub.counters["COUNTERS_PORT_NAME_MAP"] = map[string]string{}
	stub.port2namespace = map[string]string{}

	paths := []string{"COUNTERS_DB", "INTERFACE_COUNTERS"}
	tblPaths, err := v2rInterfaceCountersStats(paths)
	if err != nil {
		t.Fatalf("expected no error with empty map, got: %v", err)
	}
	if len(tblPaths) != 0 {
		t.Errorf("expected 0 table paths, got %d", len(tblPaths))
	}
}

func TestV2rInterfaceCountersStats_MissingNamespace(t *testing.T) {
	sdcfg.Init()
	stub := setupPortMaps(t)

	// Add a port to the OID map without a namespace entry.
	stub.counters["COUNTERS_PORT_NAME_MAP"]["Ethernet99"] = "oid:0x1000000000099"

	paths := []string{"COUNTERS_DB", "INTERFACE_COUNTERS"}
	tblPaths, err := v2rInterfaceCountersStats(paths)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tblPaths) != 2 {
		t.Errorf("expected the 2 ports with a namespace, got %v", tblPaths)
	}
}

// TestV2rInterfaceCountersTriePath verifies that the trie maps the virtual
// path [COUNTERS_DB INTERFACE_COUNTERS] to v2rInterfaceCountersStats.
func TestV2rInterfaceCountersTriePath(t *testing.T) {
	sdcfg.Init()
	setupPortMaps(t)

	tblPaths, err := lookupV2R([]string{"COUNTERS_DB", "INTERFACE_COUNTERS"})
	if err != nil {
		t.Fatalf("lookupV2R failed for INTERFACE_COUNTERS: %v", err)
	}
	if len(tblPaths) != 2 {
		t.Fatalf("expected 2 table paths from trie lookup, got %d", len(tblPaths))
	}
	for _, tp := range tblPaths {
		if tp.tableName != "COUNTERS" {
			t.Errorf("tableName = %q, want COUNTERS", tp.tableName)
		}
		if tp.jsonTableKey != "Ethernet0" && tp.jsonTableKey != "Ethernet68" {
			t.Errorf("jsonTableKey = %q, want Ethernet0 or Ethernet68", tp.jsonTableKey)
		}
	}
}

// TestV2rInterfaceCountersStats_FromTestdata loads the real COUNTERS_PORT_NAME_MAP
// fixture from testdata/ and verifies v2rInterfaceCountersStats produces one
// tablePath per port in the map, each pointing at the COUNTERS table by oid and
// keyed in JSON by the SONiC interface name (no alias translation).
func TestV2rInterfaceCountersStats_FromTestdata(t *testing.T) {
	sdcfg.Init()

	// Read the same fixture used by the gnmi_server integration tests.
	raw, err := os.ReadFile("../testdata/COUNTERS_PORT_NAME_MAP.txt")
	if err != nil {
		t.Fatalf("failed to read COUNTERS_PORT_NAME_MAP.txt: %v", err)
	}
	portMap := map[string]string{}
	if err := json.Unmarshal(raw, &portMap); err != nil {
		t.Fatalf("failed to parse COUNTERS_PORT_NAME_MAP.txt: %v", err)
	}
	if len(portMap) == 0 {
		t.Fatal("COUNTERS_PORT_NAME_MAP.txt produced empty map")
	}

	// Serve the testdata port map to the translator.
	stub := &nameMapStub{
		counters:       map[string]map[string]string{"COUNTERS_PORT_NAME_MAP": portMap},
		port2namespace: make(map[string]string, len(portMap)),
	}
	for port := range portMap {
		stub.port2namespace[port] = ""
	}
	stubNameMaps(t, stub)

	paths := []string{"COUNTERS_DB", "INTERFACE_COUNTERS"}
	tblPaths, err := v2rInterfaceCountersStats(paths)
	if err != nil {
		t.Fatalf("v2rInterfaceCountersStats returned error: %v", err)
	}
	if len(tblPaths) != len(portMap) {
		t.Fatalf("expected %d table paths (one per port in fixture), got %d",
			len(portMap), len(tblPaths))
	}

	// Index returned paths by SONiC interface name for easy lookup.
	got := make(map[string]tablePath, len(tblPaths))
	for _, tp := range tblPaths {
		if _, dup := got[tp.jsonTableKey]; dup {
			t.Errorf("duplicate jsonTableKey %q in result", tp.jsonTableKey)
		}
		got[tp.jsonTableKey] = tp
	}

	for port, wantOid := range portMap {
		tp, ok := got[port]
		if !ok {
			t.Errorf("port %q missing from result", port)
			continue
		}
		if tp.dbName != "COUNTERS_DB" {
			t.Errorf("port %q: dbName = %q, want COUNTERS_DB", port, tp.dbName)
		}
		// INTERFACE_COUNTERS is virtual; data lives in the real COUNTERS table.
		if tp.tableName != "COUNTERS" {
			t.Errorf("port %q: tableName = %q, want COUNTERS", port, tp.tableName)
		}
		if tp.tableKey != wantOid {
			t.Errorf("port %q: tableKey = %q, want %q", port, tp.tableKey, wantOid)
		}
		if tp.jsonTableKey != port {
			t.Errorf("jsonTableKey = %q, want %q (SONiC name, no alias)",
				tp.jsonTableKey, port)
		}
	}

	// Spot-check a few well-known entries from the fixture so a future change
	// to the testdata file doesn't silently weaken the assertion.
	for port, wantOid := range map[string]string{
		"Ethernet0":  "oid:0x1000000000002",
		"Ethernet1":  "oid:0x1000000000003",
		"Ethernet68": "oid:0x1000000000039",
	} {
		tp, ok := got[port]
		if !ok {
			t.Errorf("expected port %q from fixture not present in result", port)
			continue
		}
		if tp.tableKey != wantOid {
			t.Errorf("port %q: tableKey = %q, want %q", port, tp.tableKey, wantOid)
		}
	}
}

func TestV2rSystemPortVoQStats_SystemPort_JsonKeys(t *testing.T) {
	sdcfg.Init()
	_, restore := setupVoQMaps(t)
	defer restore()

	paths := []string{"COUNTERS_DB", "COUNTERS", "str2-7804-lc7-1|Asic0|Ethernet68", "VoQs"}
	tblPaths, err := v2rSystemPortVoQStats(paths)
	if err != nil {
		t.Fatalf("v2rSystemPortVoQStats returned error: %v", err)
	}
	// Only 2 VoQs match Ethernet68
	if len(tblPaths) != 2 {
		t.Fatalf("expected 2 table paths for system port, got %d", len(tblPaths))
	}

	sort.Slice(tblPaths, func(i, j int) bool {
		return tblPaths[i].jsonTableKey < tblPaths[j].jsonTableKey
	})

	if tblPaths[0].jsonTableKey != "str2-7804-lc7-1|Asic0|Ethernet68:0" {
		t.Errorf("jsonTableKey = %q, want str2-7804-lc7-1|Asic0|Ethernet68:0", tblPaths[0].jsonTableKey)
	}
	if tblPaths[1].jsonTableKey != "str2-7804-lc7-1|Asic0|Ethernet68:1" {
		t.Errorf("jsonTableKey = %q, want str2-7804-lc7-1|Asic0|Ethernet68:1", tblPaths[1].jsonTableKey)
	}
}

func TestV2rSystemPortVoQStats_NoMatch(t *testing.T) {
	sdcfg.Init()
	_, restore := setupVoQMaps(t)
	defer restore()

	// Query for a switch name that doesn't exist in the map
	paths := []string{"COUNTERS_DB", "COUNTERS", "nonexistent-switch", "VoQs"}
	tblPaths, err := v2rSystemPortVoQStats(paths)
	if err != nil {
		t.Fatalf("v2rSystemPortVoQStats returned error: %v", err)
	}
	if len(tblPaths) != 0 {
		t.Errorf("expected 0 table paths for non-existent switch, got %d", len(tblPaths))
	}
}

func TestV2rSystemPortVoQStats_SystemPortNoMatch(t *testing.T) {
	sdcfg.Init()
	_, restore := setupVoQMaps(t)
	defer restore()

	paths := []string{"COUNTERS_DB", "COUNTERS", "str2-7804-lc7-1|Asic0|Ethernet99", "VoQs"}
	tblPaths, err := v2rSystemPortVoQStats(paths)
	if err != nil {
		t.Fatalf("v2rSystemPortVoQStats returned error: %v", err)
	}
	if len(tblPaths) != 0 {
		t.Errorf("expected 0 table paths for non-existent system port, got %d", len(tblPaths))
	}
}

// --------------------------------------------------------------------------
// Tests for getVoQCountersMap (virtual_db.go) - uses miniredis
// --------------------------------------------------------------------------

func setupMiniredisForVoQ(t *testing.T) (*miniredis.Miniredis, func()) {
	t.Helper()

	mr := miniredis.RunT(t)

	// Populate COUNTERS_VOQ_NAME_MAP hash
	mr.HSet("COUNTERS_VOQ_NAME_MAP", "str2-7804-lc7-1|Asic0|Ethernet84:0", "oid:0x160000000091c")
	mr.HSet("COUNTERS_VOQ_NAME_MAP", "str2-7804-lc7-1|Asic0|Ethernet84:1", "oid:0x160000000091d")

	// Populate COUNTERS:oid:* keys so the second pass can find them
	mr.Set("COUNTERS:oid:0x160000000091c", "exists")
	mr.Set("COUNTERS:oid:0x160000000091d", "exists")

	origTarget := Target2RedisDb
	ns, _ := sdcfg.GetDbDefaultNamespace()
	Target2RedisDb = map[string]map[string]*redis.Client{
		ns: {
			"COUNTERS_DB": redis.NewClient(&redis.Options{Addr: mr.Addr()}),
		},
	}

	return mr, func() {
		Target2RedisDb = origTarget
	}
}

func TestGetVoQCountersMap_Success(t *testing.T) {
	sdcfg.Init()
	_, restore := setupMiniredisForVoQ(t)
	defer restore()

	counterMap, oidNsMap, err := getVoQCountersMap("COUNTERS_VOQ_NAME_MAP")
	if err != nil {
		t.Fatalf("getVoQCountersMap returned error: %v", err)
	}
	if len(counterMap) != 2 {
		t.Errorf("expected 2 entries in counterMap, got %d", len(counterMap))
	}
	if counterMap["str2-7804-lc7-1|Asic0|Ethernet84:0"] != "oid:0x160000000091c" {
		t.Errorf("unexpected OID for Ethernet84:0: %v", counterMap["str2-7804-lc7-1|Asic0|Ethernet84:0"])
	}
	if len(oidNsMap) != 2 {
		t.Errorf("expected 2 OID-namespace mappings, got %d", len(oidNsMap))
	}
}

func TestGetVoQCountersMap_EmptyTable(t *testing.T) {
	sdcfg.Init()
	mr := miniredis.RunT(t)

	origTarget := Target2RedisDb
	ns, _ := sdcfg.GetDbDefaultNamespace()
	Target2RedisDb = map[string]map[string]*redis.Client{
		ns: {
			"COUNTERS_DB": redis.NewClient(&redis.Options{Addr: mr.Addr()}),
		},
	}
	defer func() { Target2RedisDb = origTarget }()

	counterMap, oidNsMap, err := getVoQCountersMap("COUNTERS_VOQ_NAME_MAP")
	if err != nil {
		t.Fatalf("getVoQCountersMap returned error: %v", err)
	}
	if len(counterMap) != 0 {
		t.Errorf("expected 0 entries, got %d", len(counterMap))
	}
	if len(oidNsMap) != 0 {
		t.Errorf("expected 0 OID-namespace mappings, got %d", len(oidNsMap))
	}
}

func TestGetVoQCountersMap_NoRedisClients(t *testing.T) {
	sdcfg.Init()

	origTarget := Target2RedisDb
	Target2RedisDb = map[string]map[string]*redis.Client{}
	defer func() { Target2RedisDb = origTarget }()

	// With no Redis clients, GetRedisClientsForDb returns empty map
	counterMap, oidNsMap, err := getVoQCountersMap("COUNTERS_VOQ_NAME_MAP")
	if err != nil {
		t.Fatalf("getVoQCountersMap returned error: %v", err)
	}
	if len(counterMap) != 0 {
		t.Errorf("expected 0 entries, got %d", len(counterMap))
	}
	if len(oidNsMap) != 0 {
		t.Errorf("expected 0 OID-namespace mappings, got %d", len(oidNsMap))
	}
}

// --------------------------------------------------------------------------
// Tests for readVoQNameMaps (virtual_db.go)
// --------------------------------------------------------------------------

func TestReadVoQNameMaps_Success(t *testing.T) {
	sdcfg.Init()
	_, restore := setupMiniredisForVoQ(t)
	defer restore()
	resetNameMapCache(t)

	m, err := readVoQNameMaps()
	if err != nil {
		t.Fatalf("readVoQNameMaps returned error: %v", err)
	}
	if len(m.nameToOid) != 2 {
		t.Errorf("expected 2 VoQ entries, got %d", len(m.nameToOid))
	}
}

func TestReadVoQNameMaps_ReadAgainWhenOld(t *testing.T) {
	sdcfg.Init()
	mr, restore := setupMiniredisForVoQ(t)
	defer restore()
	clock := useFakeNameMapClock(t)

	if m, err := readVoQNameMaps(); err != nil || len(m.nameToOid) != 2 {
		t.Fatalf("readVoQNameMaps = %v, %v, want 2 VoQs", m, err)
	}
	mr.HSet("COUNTERS_VOQ_NAME_MAP", "str2-7804-lc7-1|Asic0|Ethernet84:2", "oid:0x160000000091e")
	mr.Set("COUNTERS:oid:0x160000000091e", "exists")
	if m, err := readVoQNameMaps(); err != nil || len(m.nameToOid) != 2 {
		t.Errorf("readVoQNameMaps within nameMapMaxAge = %v, %v, want the cached 2 VoQs", m, err)
	}
	clock.advance(nameMapMaxAge)
	m, err := readVoQNameMaps()
	if err != nil || len(m.nameToOid) != 3 || m.oidToNamespace["oid:0x160000000091e"] != "" {
		t.Errorf("readVoQNameMaps after nameMapMaxAge = %v, %v, want 3 VoQs with namespaces", m, err)
	}
}

// An OID keeps the namespace it was found in, so a later read looks up only new OIDs.
func TestGetVoQCountersMap_KeepsFoundNamespaces(t *testing.T) {
	sdcfg.Init()
	mr, restore := setupMiniredisForVoQ(t)
	defer restore()
	resetNameMapCache(t)

	if _, oidNsMap, err := getVoQCountersMap("COUNTERS_VOQ_NAME_MAP"); err != nil || len(oidNsMap) != 2 {
		t.Fatalf("getVoQCountersMap = %v, %v, want 2 namespaces", oidNsMap, err)
	}
	// were the OID looked up again, it would no longer be found
	mr.Del("COUNTERS:oid:0x160000000091c")
	mr.HSet("COUNTERS_VOQ_NAME_MAP", "str2-7804-lc7-1|Asic0|Ethernet84:2", "oid:0x160000000091e")
	mr.Set("COUNTERS:oid:0x160000000091e", "exists")
	mr.HSet("COUNTERS_VOQ_NAME_MAP", "str2-7804-lc7-1|Asic0|Ethernet84:3", "oid:0x160000000091f")

	_, oidNsMap, err := getVoQCountersMap("COUNTERS_VOQ_NAME_MAP")
	if err != nil {
		t.Fatalf("getVoQCountersMap returned error: %v", err)
	}
	for _, oid := range []string{"oid:0x160000000091c", "oid:0x160000000091d", "oid:0x160000000091e"} {
		if _, ok := oidNsMap[oid]; !ok {
			t.Errorf("no namespace for %s in %v", oid, oidNsMap)
		}
	}
	if ns, ok := oidNsMap["oid:0x160000000091f"]; ok {
		t.Errorf("namespace %q for an OID with no counters", ns)
	}
}

// --------------------------------------------------------------------------
// Tests for v2rSystemPortVoQStats with invalid VoQ names (error branches)
// --------------------------------------------------------------------------

func TestV2rSystemPortVoQStats_InvalidVoQName_Wildcard(t *testing.T) {
	sdcfg.Init()
	stub, restore := setupVoQMaps(t)
	defer restore()

	// Add an invalid VoQ name (missing colon separator)
	stub.voqNameToOid["badformat-no-pipes"] = "oid:0xBAD1"

	paths := []string{"COUNTERS_DB", "COUNTERS", "SwitchName*", "VoQs"}
	tblPaths, err := v2rSystemPortVoQStats(paths)
	if err != nil {
		t.Fatalf("v2rSystemPortVoQStats returned error: %v", err)
	}
	// Should still get 4 valid paths (the invalid one is skipped)
	if len(tblPaths) != 4 {
		t.Errorf("expected 4 valid table paths, got %d", len(tblPaths))
	}
}

func TestV2rSystemPortVoQStats_InvalidVoQName_SystemPort(t *testing.T) {
	sdcfg.Init()
	stub, restore := setupVoQMaps(t)
	defer restore()

	// Add an invalid VoQ name that contains the requested system port string
	stub.voqNameToOid["str2-7804-lc7-1|Asic0|Ethernet68-nocoIndex"] = "oid:0xBAD2"

	paths := []string{"COUNTERS_DB", "COUNTERS", "str2-7804-lc7-1|Asic0|Ethernet68", "VoQs"}
	tblPaths, err := v2rSystemPortVoQStats(paths)
	if err != nil {
		t.Fatalf("v2rSystemPortVoQStats returned error: %v", err)
	}
	// Should get 2 valid paths (Ethernet68:0 and Ethernet68:1), the invalid one is skipped
	if len(tblPaths) != 2 {
		t.Errorf("expected 2 valid table paths, got %d", len(tblPaths))
	}
}

func TestV2rSystemPortVoQStats_InvalidVoQName_SingleSwitch(t *testing.T) {
	sdcfg.Init()
	stub, restore := setupVoQMaps(t)
	defer restore()

	// Add an invalid VoQ name that starts with "str2-7804-lc7-1|" but has bad format
	stub.voqNameToOid["str2-7804-lc7-1|BadNoThirdPipe"] = "oid:0xBAD3"

	paths := []string{"COUNTERS_DB", "COUNTERS", "str2-7804-lc7-1", "VoQs"}
	tblPaths, err := v2rSystemPortVoQStats(paths)
	if err != nil {
		t.Fatalf("v2rSystemPortVoQStats returned error: %v", err)
	}
	// Should get 4 valid paths (the invalid one is skipped)
	if len(tblPaths) != 4 {
		t.Errorf("expected 4 valid table paths, got %d", len(tblPaths))
	}
}

// --------------------------------------------------------------------------
// Tests for trie findNode dead-end (trie.go line 144)
// --------------------------------------------------------------------------

func TestTrieBacktracking_DeadEndAtNonTerminal(t *testing.T) {
	sdcfg.Init()

	// Search for a path that matches "*" wildcard at COUNTERS level but
	// "*" node is non-terminal (it has "VoQs" child but no "" terminal).
	// This exercises line 144 (return nil when keys==0 at non-terminal).
	keys := []string{"COUNTERS_DB", "COUNTERS", "random-switch-name"}
	_, ok := v2rTrie.Find(keys)
	if ok {
		t.Fatal("expected trie NOT to find path ending at non-terminal node")
	}
}

// --------------------------------------------------------------------------
// Tests for GetPfcwdMap (virtual_db.go) - uses miniredis
// --------------------------------------------------------------------------

func setupMiniredisForConfigDb(t *testing.T) (*miniredis.Miniredis, string, func()) {
	t.Helper()

	mr := miniredis.RunT(t)
	sdcfg.Init()
	ns, _ := sdcfg.GetDbDefaultNamespace()
	sep, err := GetTableKeySeparator("CONFIG_DB", ns)
	if err != nil {
		t.Fatalf("GetTableKeySeparator: %v", err)
	}

	origTarget := Target2RedisDb
	Target2RedisDb = map[string]map[string]*redis.Client{
		ns: {
			"CONFIG_DB": redis.NewClient(&redis.Options{Addr: mr.Addr()}),
		},
	}

	return mr, sep, func() {
		Target2RedisDb = origTarget
	}
}

func TestGetPfcwdMap_PfcWdNotEnabled(t *testing.T) {
	_, _, restore := setupMiniredisForConfigDb(t)
	defer restore()

	m, err := GetPfcwdMap()
	if err != nil {
		t.Fatalf("GetPfcwdMap returned error: %v", err)
	}
	if m != nil {
		t.Fatalf("expected nil map when PFC_WD keys are absent, got %#v", m)
	}
}

func TestGetPfcwdMap_PortQosMapEmpty(t *testing.T) {
	mr, sep, restore := setupMiniredisForConfigDb(t)
	defer restore()

	// PFC_WD entries exist, but PORT_QOS_MAP* is absent.
	mr.Set("PFC_WD"+sep+"Ethernet0", "1")

	m, err := GetPfcwdMap()
	if err != nil {
		t.Fatalf("GetPfcwdMap returned error: %v", err)
	}
	if m != nil {
		t.Fatalf("expected nil map when PORT_QOS_MAP keys are absent, got %#v", m)
	}
}

// nameMapTestDb is the test redis before orchagent has written any COUNTERS
// name map: CONFIG_DB and COUNTERS_DB are empty.
type nameMapTestDb struct {
	counters, config *redis.Client
	clock            *fakeNameMapClock
}

func newNameMapTestDb(t *testing.T) nameMapTestDb {
	t.Helper()
	sdcfg.Init()
	t.Setenv("UNIT_TEST", "1")
	ClearMappings()
	t.Cleanup(ClearMappings)
	t.Cleanup(setupTestTarget2RedisDb(t))
	d := nameMapTestDb{
		counters: Target2RedisDb[""]["COUNTERS_DB"],
		config:   Target2RedisDb[""]["CONFIG_DB"],
		clock:    useFakeNameMapClock(t),
	}
	d.counters.FlushDB(context.Background())
	d.config.FlushDB(context.Background())
	return d
}

func (d nameMapTestDb) port(name, alias string) {
	d.config.HSet(context.Background(), "PORT|"+name, "alias", alias)
}

func (d nameMapTestDb) nameMap(table, name, oid string) {
	d.counters.HSet(context.Background(), table, name, oid)
}

func TestVirtualDbPathsFollowNameMaps(t *testing.T) {
	const (
		eth0Oid  = "oid:0x1000000000001"
		eth4Oid  = "oid:0x1000000000005"
		queueOid = "oid:0x15000000000003"
		pgOid    = "oid:0x1a000000000003"
		ruleOid  = "oid:0x9000000000711"
	)
	ctx := context.Background()
	query := func(path []string) ([]TablePath, error) {
		elems := make([]*gnmipb.PathElem, len(path))
		for i, name := range path {
			elems[i] = &gnmipb.PathElem{Name: name}
		}
		return PopulateTablePaths(&gnmipb.Path{Target: "COUNTERS_DB"}, &gnmipb.Path{Elem: elems})
	}
	tests := []struct {
		name    string
		path    []string
		before  func(d nameMapTestDb)
		after   func(d nameMapTestDb)
		want    []string
		notWant []string
	}{{
		name:   "port map written after the first query",
		path:   []string{"COUNTERS", "Ethernet0"},
		before: func(d nameMapTestDb) { d.port("Ethernet0", "Ethernet1/1") },
		after:  func(d nameMapTestDb) { d.nameMap("COUNTERS_PORT_NAME_MAP", "Ethernet0", eth0Oid) },
		want:   []string{eth0Oid},
	}, {
		name: "port added to a partly written port map",
		path: []string{"COUNTERS", "Ethernet*"},
		before: func(d nameMapTestDb) {
			d.port("Ethernet0", "Ethernet1/1")
			d.port("Ethernet4", "Ethernet2/1")
			d.nameMap("COUNTERS_PORT_NAME_MAP", "Ethernet0", eth0Oid)
		},
		after: func(d nameMapTestDb) { d.nameMap("COUNTERS_PORT_NAME_MAP", "Ethernet4", eth4Oid) },
		want:  []string{eth0Oid, eth4Oid},
	}, {
		name: "port OID changed after the first query",
		path: []string{"COUNTERS", "Ethernet0"},
		before: func(d nameMapTestDb) {
			d.port("Ethernet0", "Ethernet1/1")
			d.nameMap("COUNTERS_PORT_NAME_MAP", "Ethernet0", "oid:0x1000000000091")
		},
		after:   func(d nameMapTestDb) { d.nameMap("COUNTERS_PORT_NAME_MAP", "Ethernet0", eth0Oid) },
		want:    []string{eth0Oid},
		notWant: []string{"oid:0x1000000000091"},
	}, {
		name: "port added after the first query, queried by its alias",
		path: []string{"COUNTERS", "Ethernet2/1"},
		before: func(d nameMapTestDb) {
			d.port("Ethernet0", "Ethernet1/1")
			d.nameMap("COUNTERS_PORT_NAME_MAP", "Ethernet0", eth0Oid)
		},
		after: func(d nameMapTestDb) {
			d.port("Ethernet4", "Ethernet2/1")
			d.nameMap("COUNTERS_PORT_NAME_MAP", "Ethernet4", eth4Oid)
		},
		want: []string{eth4Oid},
	}, {
		name: "queue map written after the first query",
		path: []string{"COUNTERS", "Ethernet0", "Queues"},
		before: func(d nameMapTestDb) {
			d.port("Ethernet0", "Ethernet1/1")
			d.nameMap("COUNTERS_PORT_NAME_MAP", "Ethernet0", eth0Oid)
		},
		after: func(d nameMapTestDb) { d.nameMap("COUNTERS_QUEUE_NAME_MAP", "Ethernet0:3", queueOid) },
		want:  []string{queueOid},
	}, {
		name: "priority group map written after the first query",
		path: []string{"PERIODIC_WATERMARKS", "Ethernet0", "PriorityGroups"},
		before: func(d nameMapTestDb) {
			d.port("Ethernet0", "Ethernet1/1")
			d.nameMap("COUNTERS_PORT_NAME_MAP", "Ethernet0", eth0Oid)
		},
		after: func(d nameMapTestDb) { d.nameMap("COUNTERS_PG_NAME_MAP", "Ethernet0:3", pgOid) },
		want:  []string{pgOid},
	}, {
		name: "PFC watchdog started after the first query",
		path: []string{"COUNTERS", "Ethernet0", "Pfcwd"},
		before: func(d nameMapTestDb) {
			d.port("Ethernet0", "Ethernet1/1")
			d.nameMap("COUNTERS_PORT_NAME_MAP", "Ethernet0", eth0Oid)
			d.nameMap("COUNTERS_QUEUE_NAME_MAP", "Ethernet0:3", queueOid)
		},
		after: func(d nameMapTestDb) {
			d.config.HSet(ctx, "PFC_WD|Ethernet0", "action", "drop")
			d.config.HSet(ctx, "PORT_QOS_MAP|Ethernet0", "pfc_enable", "3")
			d.config.HSet(ctx, "MAP_PFC_PRIORITY_TO_QUEUE|AZURE", "3", "3")
		},
		want: []string{queueOid},
	}, {
		name:   "ACL rule counter added after the first query",
		path:   []string{"COUNTERS", "ACL_RULE*"},
		before: func(d nameMapTestDb) { d.port("Ethernet0", "Ethernet1/1") },
		after:  func(d nameMapTestDb) { d.nameMap("ACL_COUNTER_RULE_MAP", "DATAACL:RULE_1", ruleOid) },
		want:   []string{ruleOid},
	}, {
		name: "port map unreadable at the first query",
		path: []string{"COUNTERS", "Ethernet0"},
		before: func(d nameMapTestDb) {
			d.port("Ethernet0", "Ethernet1/1")
			d.counters.Set(ctx, "COUNTERS_PORT_NAME_MAP", "not a hash", 0)
		},
		after: func(d nameMapTestDb) {
			d.counters.Del(ctx, "COUNTERS_PORT_NAME_MAP")
			d.nameMap("COUNTERS_PORT_NAME_MAP", "Ethernet0", eth0Oid)
		},
		want: []string{eth0Oid},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newNameMapTestDb(t)
			tt.before(d)
			// the first query may find nothing; it must not decide what later ones find
			query(tt.path)
			tt.after(d)
			d.clock.advance(nameMapMaxAge)
			paths, err := query(tt.path)
			if err != nil {
				t.Fatalf("query after the maps changed: %v", err)
			}
			got := fmt.Sprint(paths)
			for _, oid := range tt.want {
				if !strings.Contains(got, fmt.Sprintf("key=%q", oid)) {
					t.Errorf("query after the maps changed resolved to %s, want key %s", got, oid)
				}
			}
			for _, oid := range tt.notWant {
				if strings.Contains(got, fmt.Sprintf("key=%q", oid)) {
					t.Errorf("query after the maps changed resolved to %s, still has key %s", got, oid)
				}
			}
		})
	}
}

// A PORT row without an alias, as a port deleted while the aliases are read
// looks, is left out of the alias maps rather than failing them.
func TestGetAliasMapSkipsPortWithoutAlias(t *testing.T) {
	d := newNameMapTestDb(t)
	d.port("Ethernet0", "Ethernet1/1")
	d.config.HSet(context.Background(), "PORT|Ethernet4", "speed", "100000")
	alias2name, name2alias, port2namespace, err := GetAliasMap()
	if err != nil {
		t.Fatalf("GetAliasMap: %v", err)
	}
	if alias2name["Ethernet1/1"] != "Ethernet0" || name2alias["Ethernet0"] != "Ethernet1/1" {
		t.Errorf("got aliases %v and %v, want Ethernet0 as Ethernet1/1", alias2name, name2alias)
	}
	if _, ok := port2namespace["Ethernet4"]; ok || len(name2alias) != 1 {
		t.Errorf("got %v and namespaces %v, want Ethernet4 left out", name2alias, port2namespace)
	}
}

func countersPath(names ...string) *gnmipb.Path {
	path := &gnmipb.Path{}
	for _, name := range names {
		path.Elem = append(path.Elem, &gnmipb.PathElem{Name: name})
	}
	return path
}

func subscriptionList(mode gnmipb.SubscriptionMode, updatesOnly bool, paths ...*gnmipb.Path) *gnmipb.SubscriptionList {
	list := &gnmipb.SubscriptionList{Mode: gnmipb.SubscriptionList_STREAM, UpdatesOnly: updatesOnly}
	for _, path := range paths {
		list.Subscription = append(list.Subscription, &gnmipb.Subscription{Path: path, Mode: mode, SampleInterval: uint64(time.Second)})
	}
	return list
}

// nameMapSubscription is a DbClient on COUNTERS_DB paths, and the values it queues.
type nameMapSubscription struct {
	t      *testing.T
	db     nameMapTestDb
	client *DbClient
	q      *queue.PriorityQueue
	values chan *spb.Value
}

// newNameMapSubscription opens a DbClient on paths once setup has written to
// the empty test redis.
func newNameMapSubscription(t *testing.T, setup func(d nameMapTestDb), paths ...*gnmipb.Path) *nameMapSubscription {
	t.Helper()
	d := newNameMapTestDb(t)
	setup(d)
	client, err := NewDbClient(paths, &gnmipb.Path{Target: "COUNTERS_DB"})
	if err != nil {
		t.Fatalf("NewDbClient: %v", err)
	}
	s := &nameMapSubscription{t: t, db: d, client: client.(*DbClient), q: queue.NewPriorityQueue(1, false), values: make(chan *spb.Value, 100)}
	done := make(chan struct{})
	go func() {
		for {
			items, err := s.q.Get(1)
			if err != nil {
				return
			}
			select {
			case s.values <- items[0].(Value).Value:
			case <-done:
				return
			}
		}
	}()
	t.Cleanup(func() {
		close(done)
		s.q.Dispose()
	})
	return s
}

// stream runs StreamRun until the test ends. A dial-out connection passes nil subs.
func (s *nameMapSubscription) stream(subs *gnmipb.SubscriptionList) {
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go s.client.StreamRun(s.q, stop, &wg, subs)
	s.t.Cleanup(func() {
		close(stop)
		wg.Wait()
	})
}

// poll runs PollRun until the test ends, and returns its poll channel.
func (s *nameMapSubscription) poll() chan struct{} {
	poll := make(chan struct{}, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go s.client.PollRun(s.q, poll, &wg, nil)
	s.t.Cleanup(func() {
		close(poll)
		wg.Wait()
	})
	return poll
}

func (s *nameMapSubscription) synced() {
	s.t.Helper()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case v := <-s.values:
			if v.GetSyncResponse() {
				return
			}
		case <-timeout:
			s.t.Fatalf("no sync response within 10s")
		}
	}
}

// sent waits until each path has been sent with a value containing want.
func (s *nameMapSubscription) sent(want string, wantPaths ...*gnmipb.Path) {
	s.t.Helper()
	missing := make(map[*gnmipb.Path]bool)
	for _, p := range wantPaths {
		missing[p] = true
	}
	timeout := time.After(10 * time.Second)
	for len(missing) > 0 {
		select {
		case v := <-s.values:
			if v.GetVal() != nil && strings.Contains(v.GetVal().String(), want) {
				delete(missing, v.GetPath())
			}
		case <-timeout:
			var names []string
			for p := range missing {
				names = append(names, fmt.Sprint(p.GetElem()))
			}
			s.t.Fatalf("not sent with %q within 10s: %v", want, names)
		}
	}
}

// notSent fails if a value containing want, or a delete, is sent within d.
func (s *nameMapSubscription) notSent(want string, d time.Duration) {
	s.t.Helper()
	timeout := time.After(d)
	for {
		select {
		case v := <-s.values:
			if len(v.GetDelete()) > 0 {
				s.t.Fatalf("delete sent: %v", v)
			}
			if v.GetVal() != nil && strings.Contains(v.GetVal().String(), want) {
				s.t.Fatalf("sent %v, want no value with %q", v, want)
			}
		case <-timeout:
			return
		}
	}
}

// enableKeyspaceEvents turns on the keyspace notifications that table-key subscriptions read.
func enableKeyspaceEvents(t *testing.T, rclient *redis.Client) {
	t.Helper()
	if err := rclient.ConfigSet(context.Background(), "notify-keyspace-events", "KEA").Err(); err != nil {
		t.Fatalf("enabling keyspace notifications failed: %v", err)
	}
}

// keyspaceListeners returns how many connections receive the keyspace
// notifications of key in the COUNTERS_DB of rclient.
func keyspaceListeners(t *testing.T, rclient *redis.Client, key string) int64 {
	t.Helper()
	channel := fmt.Sprintf("__keyspace@%d__:%s", spb.Target_value["COUNTERS_DB"], key)
	n, err := rclient.Publish(context.Background(), channel, "listener check").Result()
	if err != nil {
		t.Fatalf("publishing to %s failed: %v", channel, err)
	}
	return n
}

// A subscription opened before orchagent writes the name maps sends the
// counters once they appear, without the collector subscribing again.
func TestCountersSubscriptionFollowsNameMaps(t *testing.T) {
	const portOid, queueOid = "oid:0x1000000000001", "oid:0x15000000000003"
	ctx := context.Background()
	portPath := countersPath("COUNTERS", "Ethernet0")
	fieldPath := countersPath("COUNTERS", "Ethernet0", "SAI_PORT_STAT_IF_IN_OCTETS")
	queuesPath := countersPath("COUNTERS", "Ethernet0", "Queues")
	paths := []*gnmipb.Path{portPath, fieldPath, queuesPath}

	start := func(t *testing.T) *nameMapSubscription {
		return newNameMapSubscription(t, func(d nameMapTestDb) { d.port("Ethernet0", "Ethernet1/1") }, paths...)
	}
	fill := func(s *nameMapSubscription, value string) {
		d := s.db
		d.nameMap("COUNTERS_PORT_NAME_MAP", "Ethernet0", portOid)
		d.nameMap("COUNTERS_QUEUE_NAME_MAP", "Ethernet0:3", queueOid)
		d.counters.HSet(ctx, "COUNTERS:"+portOid, "SAI_PORT_STAT_IF_IN_OCTETS", value)
		d.counters.HSet(ctx, "COUNTERS:"+queueOid, "SAI_QUEUE_STAT_PACKETS", value)
		d.clock.advance(nameMapMaxAge)
	}
	// stoppedListening checks that the run on the path the subscription started
	// with, the raw key COUNTERS:Ethernet0, no longer listens on it. Redis drops
	// a closed connection's subscriptions when it reads the close, so it waits
	// for that.
	stoppedListening := func(t *testing.T, s *nameMapSubscription) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for keyspaceListeners(t, s.db.counters, "COUNTERS:Ethernet0") != 0 {
			if time.Now().After(deadline) {
				t.Fatalf("still listening on COUNTERS:Ethernet0")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	t.Run("POLL", func(t *testing.T) {
		s := start(t)
		poll := s.poll()
		poll <- struct{}{}
		s.synced()
		fill(s, "poll-value-1")
		poll <- struct{}{}
		s.sent("poll-value-1", paths...)
	})

	for _, mode := range []gnmipb.SubscriptionMode{gnmipb.SubscriptionMode_SAMPLE, gnmipb.SubscriptionMode_ON_CHANGE} {
		t.Run(mode.String(), func(t *testing.T) {
			s := start(t)
			enableKeyspaceEvents(t, s.db.counters)
			s.stream(subscriptionList(mode, false, paths...))
			s.synced()
			value := strings.ToLower(mode.String()) + "-value-1"
			fill(s, value)
			s.sent(value, paths...)
			stoppedListening(t, s)
		})
	}

	t.Run("dial-out", func(t *testing.T) {
		s := start(t)
		enableKeyspaceEvents(t, s.db.counters)
		s.stream(nil)
		s.synced()
		fill(s, "dial-out-value-1")
		s.sent("dial-out-value-1", paths...)
		stoppedListening(t, s)
	})
}

// An open subscription keeps its table paths when its path cannot be resolved
// again, instead of moving to the physical path a failed lookup falls back to.
func TestSubscriptionKeepsPathsWhenResolvingFails(t *testing.T) {
	const eth0Oid = "oid:0x1000000000001"
	ctx := context.Background()
	path := countersPath("COUNTERS", "Ethernet0")
	setup := func(d nameMapTestDb) {
		d.port("Ethernet0", "Ethernet1/1")
		d.nameMap("COUNTERS_PORT_NAME_MAP", "Ethernet0", eth0Oid)
		d.counters.HSet(ctx, "COUNTERS:"+eth0Oid, "SAI_PORT_STAT_IF_IN_OCTETS", "value-before")
	}
	failures := []struct {
		name  string
		apply func(d nameMapTestDb)
	}{{
		// the port translator fails on a port with no namespace
		name:  "port without a CONFIG_DB row",
		apply: func(d nameMapTestDb) { d.config.Del(ctx, "PORT|Ethernet0") },
	}, {
		// the cache serves the port map read last
		name: "port map unreadable",
		apply: func(d nameMapTestDb) {
			d.counters.Del(ctx, "COUNTERS_PORT_NAME_MAP")
			d.counters.Set(ctx, "COUNTERS_PORT_NAME_MAP", "not a hash", 0)
		},
	}}
	for _, failure := range failures {
		t.Run(failure.name+"/POLL", func(t *testing.T) {
			s := newNameMapSubscription(t, setup, path)
			poll := s.poll()
			poll <- struct{}{}
			s.synced()
			failure.apply(s.db)
			s.db.counters.HSet(ctx, "COUNTERS:"+eth0Oid, "SAI_PORT_STAT_IF_IN_OCTETS", "value-after")
			s.db.clock.advance(nameMapMaxAge)
			poll <- struct{}{}
			s.sent("value-after", path)
		})
		t.Run(failure.name+"/SAMPLE", func(t *testing.T) {
			s := newNameMapSubscription(t, setup, path)
			enableKeyspaceEvents(t, s.db.counters)
			s.stream(subscriptionList(gnmipb.SubscriptionMode_SAMPLE, false, path))
			s.synced()
			failure.apply(s.db)
			s.db.counters.HSet(ctx, "COUNTERS:"+eth0Oid, "SAI_PORT_STAT_IF_IN_OCTETS", "value-after")
			s.db.clock.advance(nameMapMaxAge)
			s.sent("value-after", path)
		})
	}
}

// A SAMPLE subscription resolves its path again on each sample, and keeps
// running while the path resolves to the same table paths.
func TestSampleSubscriptionKeepsRunningWhileMapsAreUnchanged(t *testing.T) {
	const eth0Oid = "oid:0x1000000000001"
	ctx := context.Background()
	path := countersPath("COUNTERS", "Ethernet0")
	s := newNameMapSubscription(t, func(d nameMapTestDb) {
		d.port("Ethernet0", "Ethernet1/1")
		d.nameMap("COUNTERS_PORT_NAME_MAP", "Ethernet0", eth0Oid)
		d.counters.HSet(ctx, "COUNTERS:"+eth0Oid, "SAI_PORT_STAT_IF_IN_OCTETS", "value-steady")
	}, path)
	s.stream(subscriptionList(gnmipb.SubscriptionMode_SAMPLE, true, path))
	s.synced()
	// with updates_only, only a new run sends the row again
	for i := 0; i < 3; i++ {
		s.db.clock.advance(nameMapMaxAge)
		s.notSent("value-steady", 1500*time.Millisecond)
	}
}

// An ON_CHANGE subscription to a table key resolves its path again on each tick
// of the interval ticker.
func TestOnChangeSubscriptionResolvesOnEachTick(t *testing.T) {
	const eth0Oid = "oid:0x1000000000001"
	ctx := context.Background()
	path := countersPath("COUNTERS", "Ethernet0")
	ticks := make(chan time.Time)
	ticker := GetIntervalTicker()
	SetIntervalTicker(func(time.Duration) <-chan time.Time { return ticks })
	t.Cleanup(func() { SetIntervalTicker(ticker) })
	s := newNameMapSubscription(t, func(d nameMapTestDb) { d.port("Ethernet0", "Ethernet1/1") }, path)
	enableKeyspaceEvents(t, s.db.counters)
	s.stream(subscriptionList(gnmipb.SubscriptionMode_ON_CHANGE, false, path))
	s.synced()
	s.db.nameMap("COUNTERS_PORT_NAME_MAP", "Ethernet0", eth0Oid)
	s.db.counters.HSet(ctx, "COUNTERS:"+eth0Oid, "SAI_PORT_STAT_IF_IN_OCTETS", "value-1")
	s.db.clock.advance(nameMapMaxAge)
	s.notSent("value-1", 1500*time.Millisecond)
	select {
	case ticks <- time.Now():
	case <-time.After(10 * time.Second):
		t.Fatalf("the subscription did not wait on a tick within 10s")
	}
	s.sent("value-1", path)
}

// A SAMPLE subscription to one field of every port follows the ports added to
// the port map, from one port to several.
func TestWildcardFieldSampleFollowsNameMaps(t *testing.T) {
	ctx := context.Background()
	ports := []string{"Ethernet0", "Ethernet4", "Ethernet8"}
	oids := map[string]string{"Ethernet0": "oid:0x1000000000001", "Ethernet4": "oid:0x1000000000005", "Ethernet8": "oid:0x1000000000009"}
	path := countersPath("COUNTERS", "Ethernet*", "SAI_PORT_STAT_IF_IN_OCTETS")
	s := newNameMapSubscription(t, func(d nameMapTestDb) {
		for i, port := range ports {
			d.port(port, fmt.Sprintf("Ethernet%d/1", i+1))
			d.counters.HSet(ctx, "COUNTERS:"+oids[port], "SAI_PORT_STAT_IF_IN_OCTETS", "octets-"+port)
		}
		d.nameMap("COUNTERS_PORT_NAME_MAP", "Ethernet0", oids["Ethernet0"])
	}, path)
	s.stream(subscriptionList(gnmipb.SubscriptionMode_SAMPLE, false, path))
	s.synced()
	for _, port := range ports[1:] {
		s.db.nameMap("COUNTERS_PORT_NAME_MAP", port, oids[port])
		s.db.clock.advance(nameMapMaxAge)
		s.sent("octets-"+port, path)
	}
}

// A client that serves Get more than once resolves its virtual paths each time.
func TestDbClientGetFollowsNameMaps(t *testing.T) {
	const portOid = "oid:0x1000000000001"
	ctx := context.Background()
	path := countersPath("COUNTERS", "Ethernet0")
	s := newNameMapSubscription(t, func(d nameMapTestDb) { d.port("Ethernet0", "Ethernet1/1") }, path)
	s.client.Get(nil)
	s.db.nameMap("COUNTERS_PORT_NAME_MAP", "Ethernet0", portOid)
	s.db.counters.HSet(ctx, "COUNTERS:"+portOid, "SAI_PORT_STAT_IF_IN_OCTETS", "get-value")
	s.db.clock.advance(nameMapMaxAge)
	values, err := s.client.Get(nil)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := fmt.Sprint(values); !strings.Contains(got, "get-value") {
		t.Errorf("Get after the port map was written returned %s, want get-value", got)
	}
}

func TestSameTablePaths(t *testing.T) {
	a := tablePath{dbName: "COUNTERS_DB", tableName: "COUNTERS", tableKey: "oid:0x1"}
	b := tablePath{dbName: "COUNTERS_DB", tableName: "COUNTERS", tableKey: "oid:0x2"}
	for _, tt := range []struct {
		name string
		x, y []tablePath
		want bool
	}{
		{"same order", []tablePath{a, b}, []tablePath{a, b}, true},
		{"other order", []tablePath{a, b}, []tablePath{b, a}, true},
		{"other length", []tablePath{a, b}, []tablePath{a}, false},
		{"duplicate", []tablePath{a, a}, []tablePath{a, b}, false},
		{"both empty", nil, []tablePath{}, true},
	} {
		if got := sameTablePaths(tt.x, tt.y); got != tt.want {
			t.Errorf("%s: sameTablePaths = %v, want %v", tt.name, got, tt.want)
		}
	}
}
