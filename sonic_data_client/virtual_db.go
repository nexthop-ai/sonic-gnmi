package client

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	sdcfg "github.com/sonic-net/sonic-gnmi/sonic_db_config"

	log "github.com/golang/glog"
	redis "github.com/redis/go-redis/v9"
)

// virtual db is to Handle
// <1> different set of redis db data aggreggation
// <2> or non default TARGET_DEFINED stream subscription

// For virtual db path
const (
	DbIdx    uint = iota // DB name is the first element (no. 0) in path slice.
	TblIdx               // Table name is the second element (no. 1) in path slice.
	KeyIdx               // Key name is the first element (no. 2) in path slice.
	FieldIdx             // Field name is the first element (no. 3) in path slice.
)

type v2rTranslate func([]string) ([]tablePath, error)

type pathTransFunc struct {
	path      []string
	transFunc v2rTranslate
}

var (
	v2rTrie *Trie

	// path2TFuncTbl is used to populate trie tree which is reponsible
	// for virtual path to real data path translation
	pathTransFuncTbl = []pathTransFunc{
		{ // stats for one or all Ethernet ports
			path:      []string{"COUNTERS_DB", "COUNTERS", "Ethernet*"},
			transFunc: v2rTranslate(v2rEthPortStats),
		}, { // per-interface counters keyed by SONiC interface name
			path:      []string{"COUNTERS_DB", "INTERFACE_COUNTERS"},
			transFunc: v2rTranslate(v2rInterfaceCountersStats),
		}, { // specific field stats for one or all Ethernet ports
			path:      []string{"COUNTERS_DB", "COUNTERS", "Ethernet*", "*"},
			transFunc: v2rTranslate(v2rEthPortFieldStats),
		}, { // Queue stats for one or all Ethernet ports
			path:      []string{"COUNTERS_DB", "COUNTERS", "Ethernet*", "Queues"},
			transFunc: v2rTranslate(v2rEthPortQueStats),
		}, { // VoQ stats for one or all System Ports
			path:      []string{"COUNTERS_DB", "COUNTERS", "*", "VoQs"},
			transFunc: v2rTranslate(v2rSystemPortVoQStats),
		}, { // PFC WD stats for one or all Ethernet ports
			path:      []string{"COUNTERS_DB", "COUNTERS", "Ethernet*", "Pfcwd"},
			transFunc: v2rTranslate(v2rEthPortPfcwdStats),
		}, { // stats for one or all Fabric ports
			path:      []string{"COUNTERS_DB", "COUNTERS", "PORT*"},
			transFunc: v2rTranslate(v2rFabricPortStats),
		}, { // Periodic PG watermarks for one or all Ethernet ports
			path:      []string{"COUNTERS_DB", "PERIODIC_WATERMARKS", "Ethernet*", "PriorityGroups"},
			transFunc: v2rTranslate(v2rEthPortPGPeriodicWMs),
		}, { // COUNTER_DB RATES Ethernet*
			path:      []string{"COUNTERS_DB", "RATES", "Ethernet*"},
			transFunc: v2rTranslate(v2rEthPortStats),
		}, { // COUNTER_DB RATES Ethernet* FEC_PRE_BER
			path:      []string{"COUNTERS_DB", "RATES", "Ethernet*", "*"},
			transFunc: v2rTranslate(v2rEthPortFieldStats),
		}, { // stats for one or all Fabric ports
			path:      []string{"COUNTERS_DB", "COUNTERS", "SWITCH*"},
			transFunc: v2rTranslate(v2rSwitchPacketIntegrityDrop),
		}, {
			path:      []string{"COUNTERS_DB", "COUNTERS", "SID*"},
			transFunc: v2rTranslate(v2rSRv6SidStats),
		}, { // ACL rule counters (virtual)
			// [COUNTERS_DB COUNTERS ACL_RULE*] or
			// [COUNTERS_DB COUNTERS ACL_RULE:DATAACL:RULE_1]
			path:      []string{"COUNTERS_DB", "COUNTERS", "ACL_RULE*"},
			transFunc: v2rTranslate(v2rAclRuleStats),
		}, { // PORT_PHY_ATTR stats for one or all Ethernet ports (no alias translation)
			path:      []string{"COUNTERS_DB", "PORT_PHY_ATTR", "Ethernet*"},
			transFunc: v2rTranslate(v2rPortPhyAttrStats),
		}, { // specific field stats for PORT_PHY_ATTR for one or all Ethernet ports (no alias translation)
			path:      []string{"COUNTERS_DB", "PORT_PHY_ATTR", "Ethernet*", "*"},
			transFunc: v2rTranslate(v2rPortPhyAttrFieldStats),
		},
	}
)

// Function variables for map fetching. The map readers call these instead of
// the concrete implementations, allowing tests to inject errors.
var (
	getCountersMapFn       = func(t string) (map[string]string, error) { return GetCountersMap(t) }
	getAliasMapFn          = func() (map[string]string, map[string]string, map[string]string, error) { return GetAliasMap() }
	getFabricCountersMapFn = func(t string) (map[string]string, error) { return GetFabricCountersMap(t) }
	getPfcwdMapFn          = func() (map[string]map[string]string, error) { return GetPfcwdMap() }
	getVoQCountersMapFn    = func(t string) (map[string]string, map[string]string, error) { return getVoQCountersMap(t) }
	getSwitchStatMapFn     = func(t string) (map[string]string, error) { return getSwitchStatMap(t) }
)

func (t *Trie) v2rTriePopulate() {
	for _, pt := range pathTransFuncTbl {
		n := t.Add(pt.path, pt.transFunc)
		if n.meta.(v2rTranslate) == nil {
			log.V(1).Infof("Failed to add trie node for %v with %v", pt.path, pt.transFunc)
		} else {
			log.V(2).Infof("Add trie node for %v with %v", pt.path, pt.transFunc)
		}

	}
}

// nameMapMaxAge is how long a name map serves lookups before the next lookup
// reads it again. The maps are written after gnmi starts and change while it runs.
const nameMapMaxAge = time.Second

// nameMapCache holds the name maps that virtual COUNTERS_DB paths resolve
// through. The maps it returns are shared and must not be changed.
var nameMapCache = struct {
	sync.Mutex
	now     func() time.Time
	entries map[string]*cachedNameMap
}{now: time.Now, entries: make(map[string]*cachedNameMap)}

type cachedNameMap struct {
	sync.Mutex
	value  any
	readAt time.Time
	// triedAt is when value was read, or a read of it last failed.
	triedAt time.Time
	loaded  bool
}

// readNameMap returns the map cached under key, and reads it with read first
// when it is older than nameMapMaxAge. Other lookups of the same key wait for
// that read. When the read fails, it returns the map read last and tries again
// nameMapMaxAge later, or the error when no read of key has succeeded.
func readNameMap[T any](key string, read func() (T, error)) (T, error) {
	nameMapCache.Lock()
	entry, ok := nameMapCache.entries[key]
	if !ok {
		entry = &cachedNameMap{}
		nameMapCache.entries[key] = entry
	}
	clock := nameMapCache.now
	nameMapCache.Unlock()

	entry.Lock()
	defer entry.Unlock()
	now := clock()
	cached, ok := entry.value.(T)
	ok = ok && entry.loaded
	if ok && now.Sub(entry.triedAt) < nameMapMaxAge {
		return cached, nil
	}
	value, err := read()
	if err != nil {
		if !ok {
			var none T
			return none, &nameMapReadError{name: key, err: err}
		}
		log.V(2).Infof("Reading %s failed, serving the map read %v ago: %v", key, now.Sub(entry.readAt), err)
		entry.triedAt = now
		return cached, nil
	}
	entry.value, entry.readAt, entry.triedAt, entry.loaded = value, now, now, true
	return value, nil
}

// nameMapNow returns the time on the clock of the name map cache.
func nameMapNow() time.Time {
	nameMapCache.Lock()
	clock := nameMapCache.now
	nameMapCache.Unlock()
	return clock()
}

// resetNameMaps drops every cached name map and the known VoQ counter namespaces.
func resetNameMaps() {
	nameMapCache.Lock()
	nameMapCache.entries = make(map[string]*cachedNameMap)
	nameMapCache.Unlock()
	voqCounterNamespaces.Lock()
	voqCounterNamespaces.byOid = make(map[string]string)
	voqCounterNamespaces.Unlock()
}

// readCountersNameMap reads a name to oid map in COUNTERS_DB, such as
// COUNTERS_PORT_NAME_MAP.
func readCountersNameMap(tableName string) (map[string]string, error) {
	return readNameMap(tableName, func() (map[string]string, error) { return getCountersMapFn(tableName) })
}

type aliasMaps struct {
	alias2name, name2alias, port2namespace map[string]string
}

// readAliasMap reads the vendor alias and namespace of each port from
// CONFIG_DB: alias to port name, port name to alias, and port name to namespace.
func readAliasMap() (map[string]string, map[string]string, map[string]string, error) {
	m, err := readNameMap("CONFIG_DB PORT aliases", func() (aliasMaps, error) {
		alias2name, name2alias, port2namespace, err := getAliasMapFn()
		return aliasMaps{alias2name, name2alias, port2namespace}, err
	})
	return m.alias2name, m.name2alias, m.port2namespace, err
}

// readCountersPGNameMap reads COUNTERS_PG_NAME_MAP as port name to priority
// group index to oid.
func readCountersPGNameMap() (map[string]map[string]string, error) {
	return readNameMap("COUNTERS_PG_NAME_MAP by port", func() (map[string]map[string]string, error) {
		pgOidMap, err := getCountersMapFn("COUNTERS_PG_NAME_MAP")
		if err != nil {
			return nil, err
		}
		countersPGNameMap := make(map[string]map[string]string)
		for pg, oid := range pgOidMap {
			// pg is in format of "Ethernet64:7"
			pg_parts := strings.Split(pg, ":")
			if len(pg_parts) != 2 {
				return nil, fmt.Errorf("invalid pg name %v", pg)
			}
			if _, ok := countersPGNameMap[pg_parts[0]]; !ok {
				countersPGNameMap[pg_parts[0]] = make(map[string]string)
			}
			countersPGNameMap[pg_parts[0]][pg_parts[1]] = oid
		}
		return countersPGNameMap, nil
	})
}

// readFabricPortNameMap reads COUNTERS_FABRIC_PORT_NAME_MAP from every namespace.
func readFabricPortNameMap() (map[string]string, error) {
	return readNameMap("COUNTERS_FABRIC_PORT_NAME_MAP in every namespace", func() (map[string]string, error) {
		return getFabricCountersMapFn("COUNTERS_FABRIC_PORT_NAME_MAP")
	})
}

// readPfcwdNameMap reads the PFC watchdog queues of each port.
func readPfcwdNameMap() (map[string]map[string]string, error) {
	return readNameMap("PFC watchdog queues", func() (map[string]map[string]string, error) { return getPfcwdMapFn() })
}

type voqNameMaps struct {
	// VoQ name, such as "str2-7804-lc7-1|Asic0|Ethernet84:3", to oid
	nameToOid map[string]string
	// VoQ oid to the namespace whose COUNTERS table holds its counters
	oidToNamespace map[string]string
}

// readVoQNameMaps reads COUNTERS_VOQ_NAME_MAP from every namespace, and the
// namespace that holds the counters of each VoQ.
func readVoQNameMaps() (voqNameMaps, error) {
	return readNameMap("COUNTERS_VOQ_NAME_MAP in every namespace", func() (voqNameMaps, error) {
		nameToOid, oidToNamespace, err := getVoQCountersMapFn("COUNTERS_VOQ_NAME_MAP")
		return voqNameMaps{nameToOid, oidToNamespace}, err
	})
}

// readSwitchStatMap reads COUNTERS_DEBUG_NAME_SWITCH_STAT_MAP from every namespace.
func readSwitchStatMap() (map[string]string, error) {
	return readNameMap("COUNTERS_DEBUG_NAME_SWITCH_STAT_MAP in every namespace", func() (map[string]string, error) {
		return getSwitchStatMapFn("COUNTERS_DEBUG_NAME_SWITCH_STAT_MAP")
	})
}

// voqCounterNamespaces holds the namespace found for each VoQ oid by the last
// getVoQCountersMap, so the next one looks up only the oids it has not found.
var voqCounterNamespaces = struct {
	sync.Mutex
	byOid map[string]string
}{byOid: make(map[string]string)}

// getVoQCountersMap loads the VoQ name-to-OID map and determines which Redis
// namespace actually holds the counter data for each OID. On multi-ASIC VoQ
// chassis devices, the name map is replicated across all namespace DBs but the
// actual counter OIDs may only exist in one namespace's COUNTERS table.
func getVoQCountersMap(tableName string) (map[string]string, map[string]string, error) {
	counterMap := make(map[string]string)
	oidNsMap := make(map[string]string)
	dbName := "COUNTERS_DB"
	redisClientMap, err := GetRedisClientsForDb(dbName)
	if err != nil {
		return nil, nil, err
	}

	// First pass: collect name->oid mapping from all namespaces
	for namespace, redisDb := range redisClientMap {
		fv, err := redisDb.HGetAll(context.Background(), tableName).Result()
		if err != nil {
			log.V(2).Infof("redis HGetAll failed for COUNTERS_DB in namespace %v, tableName: %s: %v", namespace, tableName, err)
			return nil, nil, err
		}
		addmap(counterMap, fv)
		log.V(6).Infof("tableName: %s in namespace %v, map entries: %d", tableName, namespace, len(fv))
	}

	// Second pass: find the namespace whose COUNTERS table has each OID. An OID
	// keeps the namespace it was found in, so only new OIDs, and ones not found
	// yet, are looked up.
	voqCounterNamespaces.Lock()
	defer voqCounterNamespaces.Unlock()
	var unknown []string
	for _, oid := range counterMap {
		if ns, ok := voqCounterNamespaces.byOid[oid]; ok && redisClientMap[ns] != nil {
			oidNsMap[oid] = ns
		} else {
			unknown = append(unknown, oid)
		}
	}
	for namespace, redisDb := range redisClientMap {
		if len(unknown) == 0 {
			break
		}
		pipe := redisDb.Pipeline()
		exists := make([]*redis.IntCmd, len(unknown))
		for i, oid := range unknown {
			exists[i] = pipe.Exists(context.Background(), "COUNTERS:"+oid)
		}
		// a failed command is reported through its own result below
		pipe.Exec(context.Background())
		var notFound []string
		for i, oid := range unknown {
			n, err := exists[i].Result()
			if err != nil {
				log.V(2).Infof("redis Exists failed for COUNTERS:%v in namespace %v: %v", oid, namespace, err)
			}
			if n == 1 {
				oidNsMap[oid] = namespace
			} else {
				notFound = append(notFound, oid)
			}
		}
		unknown = notFound
	}
	voqCounterNamespaces.byOid = oidNsMap

	log.V(4).Infof("getVoQCountersMap: %d VoQ entries, %d OID-namespace mappings", len(counterMap), len(oidNsMap))
	return counterMap, oidNsMap, nil
}

// Get the mapping between sonic interface name and oids of their PFC-WD enabled queues in COUNTERS_DB
func GetPfcwdMap() (map[string]map[string]string, error) {
	var pfcwdName_map = make(map[string]map[string]string)

	dbName := "CONFIG_DB"
	pfcwdTableName := "PFC_WD"
	redis_client_map, err := GetRedisClientsForDb(dbName)
	if err != nil {
		return nil, err
	}
	for namespace, redisDb := range redis_client_map {
		separator, _ := GetTableKeySeparator(dbName, namespace)
		_, err := redisDb.Ping(context.Background()).Result()
		if err != nil {
			log.V(1).Infof("Can not connect to %v in namsespace %v, err: %v", dbName, namespace, err)
			return nil, err
		}

		keyName := fmt.Sprintf("%s%v*", pfcwdTableName, separator)
		resp, err := redisDb.Keys(context.Background(), keyName).Result()
		if err != nil {
			log.V(1).Infof("redis get keys failed for %v in namsepace %v, key = %v, err: %v", dbName, namespace, keyName, err)
			return nil, err
		}

		if len(resp) == 0 {
			// PFC WD service not enabled on device
			log.V(3).Infof("PFC WD not enabled on device")
			return nil, nil
		}

		for _, key := range resp {
			if strings.Contains(key, "GLOBAL") || strings.Contains(key, "global") { // ignore PFC_WD|global / PFC_WD|GLOBAL
				continue
			}
			name := key[len(keyName)-1:]
			pfcwdName_map[name] = make(map[string]string)
		}

		// Get Queue indexes that are enabled with PFC-WD
		keyName = "PORT_QOS_MAP*"
		resp, err = redisDb.Keys(context.Background(), keyName).Result()
		if err != nil {
			log.V(1).Infof("redis get keys failed for %v in namespace %v, key = %v, err: %v", dbName, namespace, keyName, err)
			return nil, err
		}
		if len(resp) == 0 {
			log.V(3).Infof("PFC WD not enabled on device")
			return nil, nil
		}

		var qos_key string
		for _, key := range resp {
			if strings.Contains(key, "GLOBAL") || strings.Contains(key, "global") { // ignore PORT_QOS_MAP|global / PORT_QOS_MAP|GLOBAL
				continue
			}
			qos_key = key
			break
		}

		fieldName := "pfc_enable"
		priorities, err := redisDb.HGet(context.Background(), qos_key, fieldName).Result()
		if err != nil {
			log.V(1).Infof("redis get field failed for %v in namsepace %v, key = %v, field = %v, err: %v", dbName, namespace, qos_key, fieldName, err)
			return nil, err
		}

		keyName = fmt.Sprintf("MAP_PFC_PRIORITY_TO_QUEUE%vAZURE", separator)
		pfc_queue_map, err := redisDb.HGetAll(context.Background(), keyName).Result()
		if err != nil {
			log.V(1).Infof("redis get fields failed for %v in namsepace %v, key = %v, err: %v", dbName, namespace, keyName, err)
			return nil, err
		}

		var indices []string
		for _, p := range strings.Split(priorities, ",") {
			_, ok := pfc_queue_map[p]
			if !ok {
				log.V(1).Infof("Missing mapping between PFC priority %v to queue", p)
			} else {
				indices = append(indices, pfc_queue_map[p])
			}
		}

		countersQueueNameMap, err := readCountersNameMap("COUNTERS_QUEUE_NAME_MAP")
		if err != nil {
			return nil, err
		}
		if len(countersQueueNameMap) == 0 {
			log.V(1).Infof("COUNTERS_QUEUE_NAME_MAP is empty")
			return nil, nil
		}

		var queue_key string
		queue_separator, _ := GetTableKeySeparator("COUNTERS_DB", namespace)
		for port, _ := range pfcwdName_map {
			for _, indice := range indices {
				queue_key = port + queue_separator + indice
				oid, ok := countersQueueNameMap[queue_key]
				if !ok {
					return nil, fmt.Errorf("key %v not exists in COUNTERS_QUEUE_NAME_MAP", queue_key)
				}
				pfcwdName_map[port][queue_key] = oid
			}
		}
	}

	log.V(6).Infof("countersPfcwdNameMap: %v", pfcwdName_map)
	return pfcwdName_map, nil
}

// Get the mapping between sonic interface name and vendor alias and sonic-interface to namespace map
func GetAliasMap() (map[string]string, map[string]string, map[string]string, error) {
	var alias2name_map = make(map[string]string)
	var name2alias_map = make(map[string]string)
	var port2namespace_map = make(map[string]string)

	dbName := "CONFIG_DB"
	redis_client_map, err := GetRedisClientsForDb(dbName)
	if err != nil {
		return nil, nil, nil, err
	}
	for namespace, redisDb := range redis_client_map {
		separator, _ := GetTableKeySeparator(dbName, namespace)
		_, err := redisDb.Ping(context.Background()).Result()
		if err != nil {
			log.V(1).Infof("Can not connect to %v, in namsepace %v, err: %v", dbName, namespace, err)
			return nil, nil, nil, err
		}

		keyName := fmt.Sprintf("PORT%v*", separator)
		resp, err := redisDb.Keys(context.Background(), keyName).Result()
		if err != nil {
			log.V(1).Infof("redis get keys failed for %v in namsepace %v, key = %v, err: %v", dbName, namespace, keyName, err)
			return nil, nil, nil, err
		}
		pipe := redisDb.Pipeline()
		aliasCmds := make([]*redis.StringCmd, len(resp))
		for i, key := range resp {
			aliasCmds[i] = pipe.HGet(context.Background(), key, "alias")
		}
		// a failed command is reported through its own result below
		pipe.Exec(context.Background())
		for i, key := range resp {
			alias, err := aliasCmds[i].Result()
			if err == redis.Nil {
				// the port was deleted after KEYS, or has no alias
				log.V(2).Infof("%v in namespace %v has no alias, skipping it", key, namespace)
				continue
			}
			if err != nil {
				log.V(1).Infof("redis get field alias failed for %v in namsepace %v, key = %v, err: %v", dbName, namespace, key, err)
				// redis get alias failed so return nil for maps and the error
				return nil, nil, nil, err
			}
			alias2name_map[alias] = key[5:]
			name2alias_map[key[5:]] = alias
			port2namespace_map[key[5:]] = namespace
		}
	}
	log.V(6).Infof("alias2nameMap: %v", alias2name_map)
	log.V(6).Infof("name2aliasMap: %v", name2alias_map)
	log.V(6).Infof("port2namespaceMap: %v", port2namespace_map)
	return alias2name_map, name2alias_map, port2namespace_map, nil
}

// Ref: https://stackoverflow.com/questions/12172215/merging-maps-in-go
func addmap(a map[string]string, b map[string]string) {
	for k, v := range b {
		a[k] = v
	}
}

// Get the mapping between objects in counters DB, Ex. port name to oid in "COUNTERS_PORT_NAME_MAP" table.
func GetCountersMap(tableName string) (map[string]string, error) {
	counter_map := make(map[string]string)
	dbName := "COUNTERS_DB"
	redis_client_map, err := GetRedisClientsForDb(dbName)
	if err != nil {
		return nil, err
	}
	for namespace, redisDb := range redis_client_map {
		fv, err := redisDb.HGetAll(context.Background(), tableName).Result()
		if err != nil {
			log.V(2).Infof("redis HGetAll failed for COUNTERS_DB in namespace %v, tableName: %s", namespace, tableName)
			return nil, err
		}
		addmap(counter_map, fv)
		log.V(6).Infof("tableName: %s in namespace %v, map %v", tableName, namespace, fv)
	}
	return counter_map, nil
}

// Get the mapping between objects in counters DB, Ex. port name to oid in "COUNTERS_FABRIC_PORT_NAME_MAP" table.
func GetFabricCountersMap(tableName string) (map[string]string, error) {
	counter_map := make(map[string]string)
	dbName := "COUNTERS_DB"
	redis_client_map, err := GetRedisClientsForDb(dbName)
	if err != nil {
		return nil, err
	}
	for namespace, redisDb := range redis_client_map {
		fv, err := redisDb.HGetAll(context.Background(), tableName).Result()
		if err != nil {
			log.V(2).Infof("redis HGetAll failed for COUNTERS_DB in namespace %v, tableName: %s", namespace, tableName)
			return nil, err
		}
		namespaceFv := make(map[string]string)
		for k, v := range fv {
			// Fabric port names are not unique across asic namespace
			// To make them unique, add asic namesapce to the port name
			// For example, PORT0 in asic0 will be PORT0-asic0
			var namespace_str = ""
			if len(namespace) != 0 {
				namespace_str = string('-') + namespace
			}
			namespaceFv[k+namespace_str] = v
		}
		addmap(counter_map, namespaceFv)
		log.V(6).Infof("tableName: %s in namespace %v, map %v", tableName, namespace, namespaceFv)
	}
	return counter_map, nil
}

// Populate real data paths from paths like
// [COUNTER_DB COUNTERS PORT*] or [COUNTER_DB COUNTERS PORT0]
func v2rFabricPortStats(paths []string) ([]tablePath, error) {
	countersFabricPortNameMap, err := readFabricPortNameMap()
	if err != nil {
		return nil, err
	}
	var tblPaths []tablePath
	if strings.HasSuffix(paths[KeyIdx], "*") { // All Ethernet ports
		for port, oid := range countersFabricPortNameMap {
			var namespace string
			// Extract namespace from port name
			// multi-asic Linecard ex: PORT0-asic0
			if strings.Contains(port, "-") {
				namespace = strings.Split(port, "-")[1]
			} else {
				namespace = ""
			}
			separator, _ := GetTableKeySeparator(paths[DbIdx], namespace)
			tblPath := tablePath{
				dbNamespace:  namespace,
				dbName:       paths[DbIdx],
				tableName:    paths[TblIdx],
				tableKey:     oid,
				delimitor:    separator,
				jsonTableKey: port,
			}
			tblPaths = append(tblPaths, tblPath)
		}
	} else { //single port
		var port, namespace string
		port = paths[KeyIdx]
		oid, ok := countersFabricPortNameMap[port]
		if !ok {
			return nil, fmt.Errorf("%v not a valid sonic fabric interface.", port)
		}
		if strings.Contains(port, "-") {
			namespace = strings.Split(port, "-")[1]
		} else {
			namespace = ""
		}
		separator, _ := GetTableKeySeparator(paths[DbIdx], namespace)
		tblPaths = []tablePath{{
			dbNamespace: namespace,
			dbName:      paths[DbIdx],
			tableName:   paths[TblIdx],
			tableKey:    oid,
			delimitor:   separator,
		}}
	}
	log.V(6).Infof("v2rFabricPortStats: %v", tblPaths)
	return tblPaths, nil
}

// Get the mapping between objects in counters DB, Ex. port name to oid in "COUNTERS_FABRIC_PORT_NAME_MAP" table.
// Aussuming static port name to oid map in COUNTERS table
func getSwitchStatMap(tableName string) (map[string]string, error) {
	counter_map := make(map[string]string)
	dbName := "COUNTERS_DB"
	redis_client_map, err := GetRedisClientsForDb(dbName)
	if err != nil {
		return nil, err
	}
	for namespace, redisDb := range redis_client_map {
		fv, err := redisDb.HGetAll(context.Background(), tableName).Result()
		if err != nil {
			log.V(2).Infof("redis HGetAll failed for COUNTERS_DB in namespace %v, tableName: %s", namespace, tableName)
			return nil, err
		}
		namespaceFv := make(map[string]string)
		for k, v := range fv {
			// SWITCH_ID are not unique across asic namespace
			// To make them unique, add asic namesapce to the port name
			// For example, PORT0 in asic0 will be PORT0-asic0
			var namespace_str = ""
			if len(namespace) != 0 {
				namespace_str = string('-') + namespace
			}
			namespaceFv[k+namespace_str] = v
		}
		addmap(counter_map, namespaceFv)
		log.V(6).Infof("tableName: %s in namespace %v, map %v", tableName, namespace, namespaceFv)
	}
	return counter_map, nil
}

// Populate real data paths from paths like
// [COUNTER_DB COUNTERS PORT*] or [COUNTER_DB COUNTERS PORT0]
func v2rSwitchPacketIntegrityDrop(paths []string) ([]tablePath, error) {
	countersDebugNameSwitchStatMap, err := readSwitchStatMap()
	if err != nil {
		return nil, err
	}
	var tblPaths []tablePath
	if strings.HasSuffix(paths[KeyIdx], "*") { // All Ethernet ports
		for port, oid := range countersDebugNameSwitchStatMap {
			var namespace string
			// Extract namespace from port name
			// multi-asic Linecard ex: SWITCH_ID-asic0
			if strings.Contains(port, "-") {
				namespace = strings.Split(port, "-")[1]
			} else {
				namespace = ""
			}
			separator, _ := GetTableKeySeparator(paths[DbIdx], namespace)
			tblPath := tablePath{
				dbNamespace:  namespace,
				dbName:       paths[DbIdx],
				tableName:    paths[TblIdx],
				tableKey:     oid,
				delimitor:    separator,
				jsonTableKey: port,
			}
			tblPaths = append(tblPaths, tblPath)
		}
	} else { //single port
		var port, namespace string
		port = paths[KeyIdx]
		oid, ok := countersDebugNameSwitchStatMap[port]
		if !ok {
			return nil, fmt.Errorf("%v not a valid sonic SWITCH ID.", port)
		}
		if strings.Contains(port, "-") {
			namespace = strings.Split(port, "-")[1]
		} else {
			namespace = ""
		}
		separator, _ := GetTableKeySeparator(paths[DbIdx], namespace)
		tblPaths = []tablePath{{
			dbNamespace: namespace,
			dbName:      paths[DbIdx],
			tableName:   paths[TblIdx],
			tableKey:    oid,
			delimitor:   separator,
		}}
	}
	log.V(6).Infof("v2rSwitchPacketIntegrityDrop: %v", tblPaths)
	return tblPaths, nil
}

// Populate real data paths from paths like
// [COUNTER_DB COUNTERS Ethernet*] or [COUNTER_DB COUNTERS Ethernet68]
func v2rEthPortStats(paths []string) ([]tablePath, error) {
	countersPortNameMap, err := readCountersNameMap("COUNTERS_PORT_NAME_MAP")
	if err != nil {
		return nil, err
	}
	alias2nameMap, name2aliasMap, port2namespaceMap, err := readAliasMap()
	if err != nil {
		return nil, err
	}
	var tblPaths []tablePath
	if strings.HasSuffix(paths[KeyIdx], "*") { // All Ethernet ports
		for port, oid := range countersPortNameMap {
			var oport string
			if alias, ok := name2aliasMap[port]; ok {
				oport = alias
			} else {
				log.V(2).Infof("%v does not have a vendor alias", port)
				oport = port
			}
			namespace, ok := wildcardPortNamespace(port2namespaceMap, port)
			if !ok {
				continue
			}
			separator, _ := GetTableKeySeparator(paths[DbIdx], namespace)
			tblPath := tablePath{
				dbNamespace:  namespace,
				dbName:       paths[DbIdx],
				tableName:    paths[TblIdx],
				tableKey:     oid,
				delimitor:    separator,
				jsonTableKey: oport,
			}
			tblPaths = append(tblPaths, tblPath)
		}
	} else { //single port
		var alias, name string
		alias = paths[KeyIdx]
		name = alias
		if val, ok := alias2nameMap[alias]; ok {
			name = val
		}
		oid, ok := countersPortNameMap[name]
		if !ok {
			return nil, fmt.Errorf("%v not a valid sonic interface. Vendor alias is %v", name, alias)
		}
		namespace, ok := port2namespaceMap[name]
		if !ok {
			return nil, fmt.Errorf("%v does not have namespace associated", name)
		}
		separator, _ := GetTableKeySeparator(paths[DbIdx], namespace)
		tblPaths = []tablePath{{
			dbNamespace: namespace,
			dbName:      paths[DbIdx],
			tableName:   paths[TblIdx],
			tableKey:    oid,
			delimitor:   separator,
		}}
	}
	log.V(6).Infof("v2rEthPortStats: %v", tblPaths)
	return tblPaths, nil
}

// v2rInterfaceCountersStats resolves the virtual path
// [COUNTERS_DB INTERFACE_COUNTERS] to per-interface counter entries from the
// underlying COUNTERS table, keyed by SONiC interface name (e.g. "Ethernet0").
func v2rInterfaceCountersStats(paths []string) ([]tablePath, error) {
	countersPortNameMap, err := readCountersNameMap("COUNTERS_PORT_NAME_MAP")
	if err != nil {
		return nil, err
	}
	_, _, port2namespaceMap, err := readAliasMap()
	if err != nil {
		return nil, err
	}
	var tblPaths []tablePath
	for port, oid := range countersPortNameMap {
		namespace, ok := wildcardPortNamespace(port2namespaceMap, port)
		if !ok {
			continue
		}
		separator, _ := GetTableKeySeparator(paths[DbIdx], namespace)
		tblPath := tablePath{
			dbNamespace:  namespace,
			dbName:       paths[DbIdx],
			tableName:    "COUNTERS", // INTERFACE_COUNTERS is virtual; data lives in COUNTERS
			tableKey:     oid,
			delimitor:    separator,
			jsonTableKey: port,
		}
		tblPaths = append(tblPaths, tblPath)
	}
	log.V(6).Infof("v2rInterfaceCountersStats: %v", tblPaths)
	return tblPaths, nil
}

// Supported cases:
// <1> port name having suffix of "*" with specific field;
//
//	Ex. [COUNTER_DB COUNTERS Ethernet* SAI_PORT_STAT_PFC_0_RX_PKTS]
//
// <2> exact port name with specific field.
//
//	Ex. [COUNTER_DB COUNTERS Ethernet68 SAI_PORT_STAT_PFC_0_RX_PKTS]
//
// case of "*" field could be covered in v2rEthPortStats()
func v2rEthPortFieldStats(paths []string) ([]tablePath, error) {
	countersPortNameMap, err := readCountersNameMap("COUNTERS_PORT_NAME_MAP")
	if err != nil {
		return nil, err
	}
	alias2nameMap, name2aliasMap, port2namespaceMap, err := readAliasMap()
	if err != nil {
		return nil, err
	}
	var tblPaths []tablePath
	if strings.HasSuffix(paths[KeyIdx], "*") {
		for port, oid := range countersPortNameMap {
			var oport string
			if alias, ok := name2aliasMap[port]; ok {
				oport = alias
			} else {
				log.V(2).Infof("%v dose not have a vendor alias", port)
				oport = port
			}
			namespace, ok := wildcardPortNamespace(port2namespaceMap, port)
			if !ok {
				continue
			}
			separator, _ := GetTableKeySeparator(paths[DbIdx], namespace)
			tblPath := tablePath{
				dbNamespace:  namespace,
				dbName:       paths[DbIdx],
				tableName:    paths[TblIdx],
				tableKey:     oid,
				field:        paths[FieldIdx],
				delimitor:    separator,
				jsonTableKey: oport,
				jsonField:    paths[FieldIdx],
			}
			tblPaths = append(tblPaths, tblPath)
		}
	} else { //single port
		var alias, name string
		alias = paths[KeyIdx]
		name = alias
		if val, ok := alias2nameMap[alias]; ok {
			name = val
		}
		oid, ok := countersPortNameMap[name]
		if !ok {
			return nil, fmt.Errorf(" %v not a valid sonic interface. Vendor alias is %v ", name, alias)
		}
		namespace, ok := port2namespaceMap[name]
		if !ok {
			return nil, fmt.Errorf("%v does not have namespace associated", name)
		}
		separator, _ := GetTableKeySeparator(paths[DbIdx], namespace)
		tblPaths = []tablePath{{
			dbNamespace: namespace,
			dbName:      paths[DbIdx],
			tableName:   paths[TblIdx],
			tableKey:    oid,
			field:       paths[FieldIdx],
			delimitor:   separator,
		}}
	}
	log.V(6).Infof("v2rEthPortFieldStats: %+v", tblPaths)
	return tblPaths, nil
}

// Populate real data paths from paths like
// [COUNTER_DB COUNTERS Ethernet* Pfcwd] or [COUNTER_DB COUNTERS Ethernet68 Pfcwd]
func v2rEthPortPfcwdStats(paths []string) ([]tablePath, error) {
	countersPortNameMap, err := readCountersNameMap("COUNTERS_PORT_NAME_MAP")
	if err != nil {
		return nil, err
	}
	alias2nameMap, name2aliasMap, port2namespaceMap, err := readAliasMap()
	if err != nil {
		return nil, err
	}
	countersPfcwdNameMap, err := readPfcwdNameMap()
	if err != nil {
		return nil, err
	}
	var tblPaths []tablePath
	if strings.HasSuffix(paths[KeyIdx], "*") { // Pfcwd on all Ethernet ports
		for port, pfcqueues := range countersPfcwdNameMap {
			namespace, ok := wildcardPortNamespace(port2namespaceMap, port)
			if !ok {
				continue
			}
			separator, _ := GetTableKeySeparator(paths[DbIdx], namespace)
			for pfcque, oid := range pfcqueues {
				// pfcque is in format of "Interface:12"
				names := strings.Split(pfcque, separator)
				var oname string
				if alias, ok := name2aliasMap[names[0]]; ok {
					oname = alias
				} else {
					log.V(2).Infof(" %v does not have a vendor alias", names[0])
					oname = names[0]
				}
				que := strings.Join([]string{oname, names[1]}, separator)
				tblPath := tablePath{
					dbNamespace:  namespace,
					dbName:       paths[DbIdx],
					tableName:    paths[TblIdx],
					tableKey:     oid,
					delimitor:    separator,
					jsonTableKey: que,
				}
				tblPaths = append(tblPaths, tblPath)
			}
		}
	} else { // pfcwd counters on single port
		alias := paths[KeyIdx]
		name := alias
		if val, ok := alias2nameMap[alias]; ok {
			name = val
		}
		namespace, ok := port2namespaceMap[name]
		if !ok {
			return nil, fmt.Errorf("%v does not have namespace associated", name)
		}
		_, ok = countersPortNameMap[name]
		if !ok {
			return nil, fmt.Errorf("%v not a valid SONiC interface. Vendor alias is %v", name, alias)
		}
		separator, _ := GetTableKeySeparator(paths[DbIdx], namespace)

		pfcqueues, ok := countersPfcwdNameMap[name]
		if ok {
			for pfcque, oid := range pfcqueues {
				// pfcque is in format of Ethernet64:12
				names := strings.Split(pfcque, separator)
				que := strings.Join([]string{alias, names[1]}, separator)
				tblPath := tablePath{
					dbNamespace:  namespace,
					dbName:       paths[DbIdx],
					tableName:    paths[TblIdx],
					tableKey:     oid,
					delimitor:    separator,
					jsonTableKey: que,
				}
				tblPaths = append(tblPaths, tblPath)
			}
		}
	}
	log.V(6).Infof("v2rEthPortPfcwdStats: %v", tblPaths)
	return tblPaths, nil
}

func buildTablePath(namespace, dbName, tableName, tableKey, separator, field, jsonTableName, jsonTableKey, jsonField string) tablePath {
	return tablePath{
		dbNamespace:   namespace,
		dbName:        dbName,
		tableName:     tableName,
		tableKey:      tableKey,
		delimitor:     separator,
		field:         field,
		jsonTableName: jsonTableName,
		jsonTableKey:  jsonTableKey,
		jsonField:     jsonField,
	}
}

// Populate real data paths from paths like
// [COUNTERS_DB COUNTERS Ethernet* Queues] or [COUNTERS_DB COUNTERS Ethernet68 Queues]
func v2rEthPortQueStats(paths []string) ([]tablePath, error) {
	countersQueueNameMap, err := readCountersNameMap("COUNTERS_QUEUE_NAME_MAP")
	if err != nil {
		return nil, err
	}
	alias2nameMap, name2aliasMap, port2namespaceMap, err := readAliasMap()
	if err != nil {
		return nil, err
	}
	// paths[DbIdx] = "COUNTERS_DB"
	separator, _ := GetTableKeySeparator(paths[DbIdx], "")
	field := "SAI_QUEUE_STAT_SHARED_WATERMARK_BYTES"
	var tblPaths []tablePath
	if strings.HasSuffix(paths[KeyIdx], "*") { // queues on all Ethernet ports
		for que, oid := range countersQueueNameMap {
			names := strings.Split(que, separator)
			var oname string
			if alias, ok := name2aliasMap[names[0]]; ok {
				oname = alias
			} else {
				log.V(2).Infof(" %v dose not have a vendor alias", names[0])
				oname = names[0]
			}
			namespace, ok := wildcardPortNamespace(port2namespaceMap, names[0])
			if !ok {
				continue
			}
			que = strings.Join([]string{oname, names[1]}, separator)
			// que is in format of "Internal_Ethernet:12"
			tblPath := buildTablePath(namespace, paths[DbIdx], paths[TblIdx], oid, separator, "", "", que, "")
			tblPaths = append(tblPaths, tblPath)
			/*
			 * Adding the field "SAI_QUEUE_STAT_SHARED_WATERMARK_BYTES" from the PERIODIC_WATERMARKS table
			 * to the virtual path.
			 */
			periodic_que := strings.Join([]string{que, "periodic"}, separator)
			// periodic_que is in format of "Internal_Ethernet:12:periodic"
			tblPath = buildTablePath(namespace, paths[DbIdx], "PERIODIC_WATERMARKS", oid, separator,
				field, paths[TblIdx], periodic_que, field)
			tblPaths = append(tblPaths, tblPath)
		}
	} else { //queues on single port
		alias := paths[KeyIdx]
		name := alias
		if val, ok := alias2nameMap[alias]; ok {
			name = val
		}
		namespace, ok := port2namespaceMap[name]
		if !ok {
			return nil, fmt.Errorf("%v does not have namespace associated", name)
		}
		for que, oid := range countersQueueNameMap {
			names := strings.Split(que, separator)
			if name != names[0] {
				continue
			}
			que = strings.Join([]string{alias, names[1]}, separator)
			//que is in format of "Ethernet64:12"
			tblPath := buildTablePath(namespace, paths[DbIdx], paths[TblIdx], oid, separator, "", "", que, "")
			tblPaths = append(tblPaths, tblPath)
			/*
			 * Adding the field "SAI_QUEUE_STAT_SHARED_WATERMARK_BYTES" from the PERIODIC_WATERMARKS table
			 * to the virtual path.
			 */
			periodic_que := strings.Join([]string{que, "periodic"}, separator)
			//periodic_que is in format of "Ethernet64:12:periodic"
			tblPath = buildTablePath(namespace, paths[DbIdx], "PERIODIC_WATERMARKS", oid, separator,
				field, paths[TblIdx], periodic_que, field)
			tblPaths = append(tblPaths, tblPath)
		}
	}
	log.V(6).Infof("v2rEthPortQueStats: %v", tblPaths)
	return tblPaths, nil
}

// nameMapReadError distinguishes a name map that could not be read from an
// unmatched virtual path, which may otherwise fall back to a physical database path.
type nameMapReadError struct {
	name string
	err  error
}

func (e *nameMapReadError) Error() string { return fmt.Sprintf("read %s: %v", e.name, e.err) }
func (e *nameMapReadError) Unwrap() error { return e.err }

// IsNameMapReadError reports whether err comes from a COUNTERS_DB name map
// that could not be read, which can succeed on a later attempt.
func IsNameMapReadError(err error) bool {
	var mapErr *nameMapReadError
	return errors.As(err, &mapErr)
}

// Populate real data paths from paths like
// [COUNTERS_DB COUNTERS SID*] or [COUNTERS_DB COUNTERS SID:fcbb:bbbb:1::/48]
func v2rSRv6SidStats(paths []string) ([]tablePath, error) {
	countersSidMap, err := readCountersNameMap("COUNTERS_SRV6_NAME_MAP")
	if err != nil {
		return nil, err
	}
	var tblPaths []tablePath
	if strings.HasSuffix(paths[KeyIdx], "*") { // All SID Counters
		for sid, oid := range countersSidMap {
			namespace, _ := sdcfg.GetDbDefaultNamespace()
			separator, _ := GetTableKeySeparator(paths[DbIdx], namespace)
			tblPath := tablePath{
				dbNamespace:  namespace,
				dbName:       paths[DbIdx],
				tableName:    paths[TblIdx],
				tableKey:     oid,
				delimitor:    separator,
				jsonTableKey: sid,
			}
			tblPaths = append(tblPaths, tblPath)
		}
	} else {
		if !strings.HasPrefix(paths[KeyIdx], "SID:") {
			log.V(2).Infof("Key %v is not a valid SRv6 SID format!", paths[KeyIdx])
			return nil, fmt.Errorf("Key %v is not a valid SRv6 SID format!", paths[KeyIdx])
		}
		sid := paths[KeyIdx][4:] // Remove the "SID:" prefix
		if oid, ok := countersSidMap[sid]; ok {
			namespace, _ := sdcfg.GetDbDefaultNamespace()
			separator, _ := GetTableKeySeparator(paths[DbIdx], namespace)
			tblPath := tablePath{
				dbNamespace: namespace,
				dbName:      paths[DbIdx],
				tableName:   paths[TblIdx],
				tableKey:    oid,
				delimitor:   separator,
			}
			tblPaths = append(tblPaths, tblPath)
		} else {
			log.V(2).Infof("SID:%v doesn't exist in counter oid map!", sid)
			return nil, fmt.Errorf("SID:%v doesn't exist in counter oid map!", sid)
		}
	}
	return tblPaths, nil
}

// Populate real data paths from paths like
// [COUNTERS_DB COUNTERS ACL_RULE*] or
// [COUNTERS_DB COUNTERS ACL_RULE:DATAACL:RULE_1]
func v2rAclRuleStats(paths []string) ([]tablePath, error) {
	// ACL_COUNTER_RULE_MAP is a hash in COUNTERS_DB:
	//   "DATAACL:RULE_1" -> "oid:0x9000000000711"
	countersAclRuleMap, err := readCountersNameMap("ACL_COUNTER_RULE_MAP")
	if err != nil {
		return nil, err
	}
	var tblPaths []tablePath

	// Wildcard: list all ACL rules in the map
	if strings.HasSuffix(paths[KeyIdx], "*") {
		for ruleName, oid := range countersAclRuleMap {
			// ruleName here is "DATAACL:RULE_1"
			namespace, _ := sdcfg.GetDbDefaultNamespace()
			separator, _ := GetTableKeySeparator(paths[DbIdx], namespace)
			tblPath := tablePath{
				dbNamespace:  namespace,
				dbName:       paths[DbIdx],
				tableName:    paths[TblIdx], // "COUNTERS"
				tableKey:     oid,           // real COUNTERS:oid:... key
				delimitor:    separator,
				jsonTableKey: ruleName, // appear as "DATAACL:RULE_1" in JSON
			}
			tblPaths = append(tblPaths, tblPath)
		}
		return tblPaths, nil
	}

	// Single rule: expected format ACL_RULE:<ACL_TABLE>:<RULE_NAME>
	if !strings.HasPrefix(paths[KeyIdx], "ACL_RULE:") {
		return nil, fmt.Errorf("Key %v is not a valid ACL rule format!", paths[KeyIdx])
	}

	// Remove "ACL_RULE:" prefix; this should match the exact field in ACL_COUNTER_RULE_MAP
	ruleName := paths[KeyIdx][len("ACL_RULE:"):] // e.g. "DATAACL:RULE_1"

	if oid, ok := countersAclRuleMap[ruleName]; ok {
		namespace, _ := sdcfg.GetDbDefaultNamespace()
		separator, _ := GetTableKeySeparator(paths[DbIdx], namespace)
		tblPath := tablePath{
			dbNamespace: namespace,
			dbName:      paths[DbIdx],
			tableName:   paths[TblIdx],
			tableKey:    oid,
			delimitor:   separator,
		}
		tblPaths = append(tblPaths, tblPath)
		return tblPaths, nil
	}

	return nil, fmt.Errorf("ACL_RULE:%v doesn't exist in counter oid map!", ruleName)
}

// Populate real data paths from paths like
// [COUNTERS_DB PORT_PHY_ATTR Ethernet*] or [COUNTERS_DB PORT_PHY_ATTR Ethernet68]
// Unlike v2rEthPortStats, this does NOT apply vendor alias translation.
func v2rPortPhyAttrStats(paths []string) ([]tablePath, error) {
	countersPortNameMap, err := readCountersNameMap("COUNTERS_PORT_NAME_MAP")
	if err != nil {
		return nil, err
	}
	_, _, port2namespaceMap, err := readAliasMap()
	if err != nil {
		return nil, err
	}
	var tblPaths []tablePath
	if strings.HasSuffix(paths[KeyIdx], "*") { // All Ethernet ports
		for port, oid := range countersPortNameMap {
			namespace, ok := wildcardPortNamespace(port2namespaceMap, port)
			if !ok {
				continue
			}
			separator, _ := GetTableKeySeparator(paths[DbIdx], namespace)
			tblPath := tablePath{
				dbNamespace:  namespace,
				dbName:       paths[DbIdx],
				tableName:    paths[TblIdx],
				tableKey:     oid,
				delimitor:    separator,
				jsonTableKey: port, // Use SONiC interface name directly, no alias
			}
			tblPaths = append(tblPaths, tblPath)
		}
	} else { // single port
		name := paths[KeyIdx]
		oid, ok := countersPortNameMap[name]
		if !ok {
			return nil, fmt.Errorf("%v not a valid sonic interface", name)
		}
		namespace, ok := port2namespaceMap[name]
		if !ok {
			return nil, fmt.Errorf("%v does not have namespace associated", name)
		}
		separator, _ := GetTableKeySeparator(paths[DbIdx], namespace)
		tblPaths = []tablePath{{
			dbNamespace: namespace,
			dbName:      paths[DbIdx],
			tableName:   paths[TblIdx],
			tableKey:    oid,
			delimitor:   separator,
		}}
	}
	log.V(6).Infof("v2rPortPhyAttrStats: %v", tblPaths)
	return tblPaths, nil
}

// Populate real data paths from paths like
// [COUNTERS_DB PORT_PHY_ATTR Ethernet* phy_rx_signal_detect] or
// [COUNTERS_DB PORT_PHY_ATTR Ethernet68 phy_rx_signal_detect]
// Unlike v2rEthPortFieldStats, this does NOT apply vendor alias translation.
func v2rPortPhyAttrFieldStats(paths []string) ([]tablePath, error) {
	countersPortNameMap, err := readCountersNameMap("COUNTERS_PORT_NAME_MAP")
	if err != nil {
		return nil, err
	}
	_, _, port2namespaceMap, err := readAliasMap()
	if err != nil {
		return nil, err
	}
	var tblPaths []tablePath
	if strings.HasSuffix(paths[KeyIdx], "*") {
		for port, oid := range countersPortNameMap {
			namespace, ok := wildcardPortNamespace(port2namespaceMap, port)
			if !ok {
				continue
			}
			separator, _ := GetTableKeySeparator(paths[DbIdx], namespace)
			tblPath := tablePath{
				dbNamespace:  namespace,
				dbName:       paths[DbIdx],
				tableName:    paths[TblIdx],
				tableKey:     oid,
				field:        paths[FieldIdx],
				delimitor:    separator,
				jsonTableKey: port,
				jsonField:    paths[FieldIdx],
			}
			tblPaths = append(tblPaths, tblPath)
		}
	} else { // single port
		name := paths[KeyIdx]
		oid, ok := countersPortNameMap[name]
		if !ok {
			return nil, fmt.Errorf("%v not a valid sonic interface", name)
		}
		namespace, ok := port2namespaceMap[name]
		if !ok {
			return nil, fmt.Errorf("%v does not have namespace associated", name)
		}
		separator, _ := GetTableKeySeparator(paths[DbIdx], namespace)
		tblPaths = []tablePath{{
			dbNamespace: namespace,
			dbName:      paths[DbIdx],
			tableName:   paths[TblIdx],
			tableKey:    oid,
			field:       paths[FieldIdx],
			delimitor:   separator,
		}}
	}
	log.V(6).Infof("v2rPortPhyAttrFieldStats: %+v", tblPaths)
	return tblPaths, nil
}

func getVendorPortName(name2aliasMap map[string]string, port string) string {
	// Get vendor port name from name2aliasMap
	if alias, ok := name2aliasMap[port]; ok {
		return alias
	}
	log.V(2).Infof(" %v does not have a vendor alias", port)
	return port
}

func getSonicPortName(alias2nameMap map[string]string, port string) string {
	// Get sonic port name from alias2nameMap
	if sonic_name, ok := alias2nameMap[port]; ok {
		return sonic_name
	}
	return port
}

func getPortNamespace(port2namespaceMap map[string]string, port string) (string, error) {
	// Get namespace from port2namespaceMap
	namespace, ok := port2namespaceMap[port]
	if !ok {
		return "", fmt.Errorf("%v does not have an associated namespace", port)
	}
	return namespace, nil
}

// wildcardPortNamespace returns the namespace of port for a path over all
// ports, or false to skip a port with none. The port aliases and the counter
// name maps are read at different times, so a port being added or removed can
// be in one and not yet in the other.
func wildcardPortNamespace(port2namespaceMap map[string]string, port string) (string, bool) {
	namespace, ok := port2namespaceMap[port]
	if !ok {
		log.V(2).Infof("%v does not have namespace associated, skipping it", port)
	}
	return namespace, ok
}

// AliasToPortNameMap returns the port name for each vendor alias, or an empty
// map when CONFIG_DB cannot be read.
func AliasToPortNameMap() map[string]string {
	alias2nameMap, _, _, err := readAliasMap()
	if err != nil {
		log.V(2).Infof("Could not read the port aliases: %v", err)
		return map[string]string{}
	}
	output := make(map[string]string, len(alias2nameMap))
	for alias, portName := range alias2nameMap {
		output[alias] = portName
	}
	return output
}

// Populate real data paths from paths like
// [COUNTERS_DB PERIODIC_WATERMARKS Ethernet* PriorityGroups] or
// [COUNTERS_DB PERIODIC_WATERMARKS Ethernet64 PriorityGroups]
func v2rEthPortPGPeriodicWMs(paths []string) ([]tablePath, error) {
	countersPGNameMap, err := readCountersPGNameMap()
	if err != nil {
		return nil, err
	}
	alias2nameMap, name2aliasMap, port2namespaceMap, err := readAliasMap()
	if err != nil {
		return nil, err
	}
	// paths[DbIdx] = "COUNTERS_DB"
	separator, _ := GetTableKeySeparator(paths[DbIdx], "")
	var tblPaths []tablePath
	if strings.HasSuffix(paths[KeyIdx], "*") { // priority groups on all Ethernet ports
		for port, pg_index_to_oid := range countersPGNameMap {
			vendor_port := getVendorPortName(name2aliasMap, port)
			namespace, ok := wildcardPortNamespace(port2namespaceMap, port)
			if !ok {
				continue
			}
			for pg_index, oid := range pg_index_to_oid {
				pg := strings.Join([]string{vendor_port, pg_index}, separator)
				// pg is in format of "Internal_Ethernet:7"
				tblPath := buildTablePath(namespace, paths[DbIdx], paths[TblIdx], oid, separator, "", "", pg, "")
				tblPaths = append(tblPaths, tblPath)
			}
		}
	} else { // priority groups on a single port
		port := getSonicPortName(alias2nameMap, paths[KeyIdx])
		namespace, err := getPortNamespace(port2namespaceMap, port)
		if err != nil {
			return nil, err
		}
		if _, ok := countersPGNameMap[port]; !ok {
			return nil, fmt.Errorf("No priority groups associated with %v", port)
		}
		for pg_index, oid := range countersPGNameMap[port] {
			pg := strings.Join([]string{paths[KeyIdx], pg_index}, separator)
			// pg is in format of "Ethernet64:7" or "Ethernet64/1:7"
			tblPath := buildTablePath(namespace, paths[DbIdx], paths[TblIdx], oid, separator, "", "", pg, "")
			tblPaths = append(tblPaths, tblPath)
		}
	}
	log.V(6).Infof("v2rEthPortPGPeriodicWMs: %v", tblPaths)
	return tblPaths, nil
}

func ClearMappings() {
	value := os.Getenv("UNIT_TEST")
	if value != "1" {
		return
	}
	resetNameMaps()
}

// parseVoQName parses VoQ name in format "str2-7804-lc7-1|Asic0|Ethernet84:3"
// Returns: switchId, asicNamespace, interfaceName, voqIndex, error
func parseVoQName(voqName string) (string, string, string, string, error) {
	// Split by "|" to separate switch, asic, and interface parts
	parts := strings.Split(voqName, "|")
	if len(parts) != 3 {
		return "", "", "", "", fmt.Errorf("invalid VoQ name format: %v, expected format: switch|asic|interface:voq", voqName)
	}

	switchId := parts[0]      // "str2-7804-lc7-1"
	asicNamespace := parts[1] // "Asic0"
	interfacePart := parts[2] // "Ethernet84:3"

	// Split interface part by ":" to get interface and VoQ index
	interfaceVoq := strings.Split(interfacePart, ":")
	if len(interfaceVoq) != 2 {
		return "", "", "", "", fmt.Errorf("invalid interface:voq format in: %v", interfacePart)
	}

	interfaceName := interfaceVoq[0] // "Ethernet84"
	voqIndex := interfaceVoq[1]      // "3"

	return switchId, asicNamespace, interfaceName, voqIndex, nil
}

// buildVoQJsonKey builds the JSON key for VoQ output
func buildVoQJsonKey(switchId, asicNamespace, interfaceName, voqIndex string) string {
	// Return format: "str2-7804-lc7-1|Asic0|Ethernet84:3"
	return fmt.Sprintf("%s|%s|%s:%s", switchId, asicNamespace, interfaceName, voqIndex)
}

// resolveVoQNamespace determines the correct Redis namespace that owns a VoQ
// OID's counter data. It returns the namespace recorded in the pre-computed
// oid-to-namespace map only when that namespace is currently active (part of the
// active namespace set returned by GetRedisClientsForDb).
//
// If the OID's owning namespace is unknown or no longer active, it returns
// ok=false so the caller can skip the VoQ rather than guess a namespace.
// Guessing (e.g. deriving from the VoQ name) risks reading the wrong Redis
// instance (silent empty result) or, on a chassis, naming a non-local namespace
// that has no Redis client and nil-dereferencing in the reader.
func resolveVoQNamespace(oidToNamespace map[string]string, oid string, activeNamespaces map[string]*redis.Client) (string, bool) {
	if ns, ok := oidToNamespace[oid]; ok && activeNamespaces[ns] != nil {
		return ns, true
	}
	return "", false
}

// Populate real data paths from paths like
// [COUNTERS_DB COUNTERS SwitchName* VoQs] or [COUNTERS_DB COUNTERS SwitchName VoQs] or [COUNTERS_DB COUNTERS SwitchName|Asic|Port VoQs]
func v2rSystemPortVoQStats(paths []string) ([]tablePath, error) {
	voqMaps, err := readVoQNameMaps()
	if err != nil {
		return nil, err
	}
	// paths[DbIdx] = "COUNTERS_DB"
	separator, _ := GetTableKeySeparator(paths[DbIdx], "")
	// Get the currently active Redis clients for namespace validation
	activeNamespaces, _ := GetRedisClientsForDb(paths[DbIdx])
	var tblPaths []tablePath

	// VoQs on all SwitchName* (str2-7804*)
	if strings.HasSuffix(paths[KeyIdx], "*") {
		for voqName, oid := range voqMaps.nameToOid {
			switchId, asicNamespace, interfaceName, voqIndex, err := parseVoQName(voqName)
			if err != nil {
				log.V(2).Infof("Failed to parse VoQ name %v: %v", voqName, err)
				continue
			}

			// Look up the namespace where this OID's counter data actually lives.
			// Skip the VoQ if no active namespace owns it rather than guess.
			namespace, ok := resolveVoQNamespace(voqMaps.oidToNamespace, oid, activeNamespaces)
			if !ok {
				log.V(2).Infof("Skipping VoQ %v: no active namespace owns OID %v", voqName, oid)
				continue
			}

			// Build JSON key for output with vendor alias
			jsonVoqKey := buildVoQJsonKey(switchId, asicNamespace, interfaceName, voqIndex)

			// Create table path for VoQ stats
			tblPath := tablePath{
				dbNamespace:  namespace,
				dbName:       paths[DbIdx],
				tableName:    paths[TblIdx],
				tableKey:     oid,
				delimitor:    separator,
				jsonTableKey: jsonVoqKey,
			}
			tblPaths = append(tblPaths, tblPath)
		}
	} else if strings.Contains(paths[KeyIdx], "|") {
		//VoQs on single SwitchName-SystemPort (str2-7804-lc7-1|Asic0|Ethernet84)
		// Find all VoQs for this specific interface
		for voqName, oid := range voqMaps.nameToOid {
			// Filter: only process VoQs that match the requested system port exactly
			if !strings.HasPrefix(voqName, paths[KeyIdx]+":") {
				continue
			}

			switchId, asicNamespace, interfaceName, voqIndex, err := parseVoQName(voqName)
			if err != nil {
				log.V(2).Infof("Failed to parse VoQ name %v: %v", voqName, err)
				continue
			}

			// Look up the namespace where this OID's counter data actually lives.
			// Skip the VoQ if no active namespace owns it rather than guess.
			namespace, ok := resolveVoQNamespace(voqMaps.oidToNamespace, oid, activeNamespaces)
			if !ok {
				log.V(2).Infof("Skipping VoQ %v: no active namespace owns OID %v", voqName, oid)
				continue
			}

			// Build JSON key for output with requested interface name (could be alias)
			jsonVoqKey := buildVoQJsonKey(switchId, asicNamespace, interfaceName, voqIndex)

			// Create table path for VoQ stats
			tblPath := tablePath{
				dbNamespace:  namespace,
				dbName:       paths[DbIdx],
				tableName:    paths[TblIdx],
				tableKey:     oid,
				delimitor:    separator,
				jsonTableKey: jsonVoqKey,
			}
			tblPaths = append(tblPaths, tblPath)
		}
	} else {
		//VoQs on single SwitchName (str2-7804-lc7-1)
		// Find all VoQs for this specific SwitchName
		requestedSwitchName := paths[KeyIdx] // e.g., "str2-7804-lc7-1"
		for voqName, oid := range voqMaps.nameToOid {
			// Filter: only process VoQs that match the requested switch name
			if !strings.HasPrefix(voqName, requestedSwitchName+"|") {
				continue
			}

			switchId, asicNamespace, interfaceName, voqIndex, err := parseVoQName(voqName)
			if err != nil {
				log.V(2).Infof("Failed to parse VoQ name %v: %v", voqName, err)
				continue
			}

			// Look up the namespace where this OID's counter data actually lives.
			// Skip the VoQ if no active namespace owns it rather than guess.
			namespace, ok := resolveVoQNamespace(voqMaps.oidToNamespace, oid, activeNamespaces)
			if !ok {
				log.V(2).Infof("Skipping VoQ %v: no active namespace owns OID %v", voqName, oid)
				continue
			}

			// Build JSON key for output with requested interface name (could be alias)
			jsonVoqKey := buildVoQJsonKey(switchId, asicNamespace, interfaceName, voqIndex)

			// Create table path for VoQ stats
			tblPath := tablePath{
				dbNamespace:  namespace,
				dbName:       paths[DbIdx],
				tableName:    paths[TblIdx],
				tableKey:     oid,
				delimitor:    separator,
				jsonTableKey: jsonVoqKey,
			}
			tblPaths = append(tblPaths, tblPath)
		}
	}
	log.V(6).Infof("v2rSystemPortVoQStats: %v", tblPaths)
	return tblPaths, nil
}

func lookupV2R(paths []string) ([]tablePath, error) {
	n, ok := v2rTrie.Find(paths)
	if ok {
		v2rTrans := n.meta.(v2rTranslate)
		return v2rTrans(paths)
	}
	return nil, fmt.Errorf("%v not found in virtual path tree", paths)
}

func init() {
	v2rTrie = NewTrie()
	v2rTrie.v2rTriePopulate()
}
