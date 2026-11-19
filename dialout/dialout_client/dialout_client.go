package telemetry_dialout

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sonic-net/sonic-gnmi/internal/redisopts"
	spb "github.com/sonic-net/sonic-gnmi/proto"
	sdc "github.com/sonic-net/sonic-gnmi/sonic_data_client"
	sdcfg "github.com/sonic-net/sonic-gnmi/sonic_db_config"

	"github.com/Workiva/go-datastructures/queue"
	log "github.com/golang/glog"
	gpb "github.com/openconfig/gnmi/proto/gnmi"
	"github.com/openconfig/ygot/ygot"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	// Unknown is an unknown report and should always be treated as an error.
	Unknown reportType = iota
	// Once will perform a Once report against the agent.
	Once
	// Poll will perform a Periodic report against the agent.
	Periodic
	// Stream will perform a Streaming report against the agent.
	Stream
)

// Type defines the type of report.
type reportType int

// NewType returns a new reportType based on the provided string.
func NewReportType(s string) reportType {
	v, ok := typeConst[s]
	if !ok {
		return Unknown
	}
	return v
}

// String returns the string representation of the reportType.
func (r reportType) String() string {
	return typeString[r]
}

var (
	typeString = map[reportType]string{
		Unknown:  "unknown",
		Once:     "once",
		Periodic: "periodic",
		Stream:   "stream",
	}

	typeConst = map[string]reportType{
		"unknown":  Unknown,
		"once":     Once,
		"periodic": Periodic,
		"stream":   Stream,
	}
	clientCfg *ClientConfig
	// Global mutex for protecting the config data
	configMu sync.Mutex

	// Each Destination group may have more than one Destinations
	// Only one destination will be used at one time
	destGrpNameMap = make(map[string][]Destination)

	// For finding clientSubscription quickly
	ClientSubscriptionNameMap = make(map[string]*clientSubscription)

	// map for storing name of clientSubscription which are users of the destination group
	DestGrp2ClientSubMap = make(map[string][]string)
)

type Destination struct {
	Addrs string
}

func (d Destination) Validate() error {
	if len(d.Addrs) == 0 {
		return errors.New("Destination.Addrs is empty")
	}
	// TODO: validate Addrs is in format IP:PORT
	return nil
}

// Global config for all clients
type ClientConfig struct {
	SrcIp          string
	RetryInterval  time.Duration
	Encoding       gpb.Encoding
	Unidirectional bool        // by default, no reponse from remote server
	TLS            *tls.Config // TLS config to use when connecting to target. Optional.
	RedisConType   string      // "unix"  or "tcp"
}

// clientSubscription is the container for config data,
// it also keeps mapping from destination to running publish Client instance
type clientSubscription struct {
	// Config Data
	name          string
	destGroupName string
	prefix        *gpb.Path
	paths         []*gpb.Path
	reportType    reportType
	interval      time.Duration // report interval

	// Running time data
	cMu    sync.Mutex
	client *Client              // GNMIDialOutClient
	dc     sdc.Client           // SONiC data client
	stop   chan struct{}        // Inform publishRun routine to stop
	q      *queue.PriorityQueue // for data passing among go routine
	w      sync.WaitGroup       // Wait for all sub go routine to finish
	opened bool                 // whether there is opened instance for this client subscription
	cancel context.CancelFunc

	conTryCnt uint64 //Number of time trying to connect
	sendMsg   uint64
	recvMsg   uint64
	errors    uint64
}

// configCopy is a new entry with cs's configuration, the Config Data
// fields, and none of its runtime state. A field added to Config Data must
// be added here.
func (cs *clientSubscription) configCopy() *clientSubscription {
	return &clientSubscription{
		name:          cs.name,
		destGroupName: cs.destGroupName,
		prefix:        cs.prefix,
		paths:         cs.paths,
		reportType:    cs.reportType,
		interval:      cs.interval,
	}
}

// Client handles execution of the telemetry publish service.
type Client struct {
	conn *grpc.ClientConn

	mu      sync.Mutex
	client  spb.GNMIDialOutClient
	publish spb.GNMIDialOut_PublishClient

	// dataChan chan struct{} //to pass data struct pointer
	//
	// synced  sync.WaitGroup
	sendMsg uint64
	recvMsg uint64
}

func (cs *clientSubscription) Close() {
	cs.cMu.Lock()
	defer cs.cMu.Unlock()
	if cs.opened == false {
		log.V(5).Infof("Opened is false: %v", cs)
		return
	}
	if cs.stop != nil {
		close(cs.stop) //Inform the clientSubscription publish service routine to stop
	}

	if cs.q != nil {
		if !cs.q.Disposed() {
			cs.q.Dispose()
		}
	}
	if cs.client != nil {

		cs.client.Close() // Close GNMIDialOutClient
	}
	cs.opened = false
	log.V(2).Infof("Closed %v", cs)
}

func (cs *clientSubscription) NewInstance(ctx context.Context) error {
	cs.cMu.Lock()
	defer cs.cMu.Unlock()

	if cs.destGroupName == "" {
		log.V(2).Infof("Destination group is not set for %v", cs)
		return fmt.Errorf("Destination group is not set for %v", cs)
	}

	dests, ok := destGrpNameMap[cs.destGroupName]
	if !ok {
		log.V(2).Infof("Destination group %v doesn't exist", cs.destGroupName)
		return fmt.Errorf("Destination group %v doesn't exist", cs.destGroupName)
	}

	target := cs.prefix.GetTarget()
	if target == "" {
		return fmt.Errorf("Empty target data not supported yet")
	}

	// Connection to system data source
	var dc sdc.Client
	var err error
	if target == "OTHERS" {
		dc, err = sdc.NewNonDbClient(cs.paths, cs.prefix)
	} else if target == "OC_YANG" {
		dc, err = sdc.NewTranslClient(cs.prefix, cs.paths, ctx, nil, sdc.TranslWildcardOption{})
	} else {
		dc, err = sdc.NewDbClient(cs.paths, cs.prefix)
	}
	if err != nil {
		log.V(1).Infof("Connection to DB for %v failed: %v", *cs, err)
		return fmt.Errorf("Connection to DB for %v failed: %v", *cs, err)
	}
	cs.dc = dc
	go publishRun(ctx, cs, dests)
	log.V(2).Infof("publishRun for %v with destination %v", cs, dests)
	return nil
}

<<<<<<< HEAD
=======
// retryLater builds the instance again after RetryInterval when its data
// source could not be opened. Some of those errors clear on their own (a
// database that does not answer yet at boot); others, such as a mistyped
// target, last until the entry is edited, and are retried at the same pace,
// with the reason in the log. It gives up once the entry has been replaced or
// removed, or the instance cancelled.
func (cs *clientSubscription) retryLater(ctx context.Context) {
	time.AfterFunc(cs.cfg.RetryInterval, func() {
		configMu.Lock()
		defer configMu.Unlock()
		if ctx.Err() == nil && ClientSubscriptionNameMap[cs.name] == cs {
			cs.NewInstance(ctx, cs.cfg)
		}
	})
}

// serving reports whether the instance runs, as opposed to waiting for its
// destination group or its data source. dc is set only where NewInstance
// starts publishRun. Every teardown replaces or removes the entry: a group
// delete re-registers its subscriptions as copies that wait for the group.
// So for the entry in ClientSubscriptionNameMap a set dc means a running
// instance.
func (cs *clientSubscription) serving() bool {
	cs.cMu.Lock()
	defer cs.cMu.Unlock()
	return cs.dc != nil
}

// openSource opens the entry's data source and, for a stream to OC_YANG or
// OTHERS, sets the subscription list its StreamRun is given.
func (cs *clientSubscription) openSource(ctx context.Context) (sdc.Client, error) {
	target := cs.prefix.GetTarget()
	if target == "" {
		return nil, fmt.Errorf("Empty target data not supported yet")
	}
	// The data client may rewrite the paths it is given (module prefixes,
	// wildcard keys), and an instance re-created on a group or Global edit
	// opens its source from the entry's paths again, so it gets a copy.
	paths := clonePaths(cs.paths)
	var dc sdc.Client
	var err error
	if target == "OTHERS" {
		dc, err = sdc.NewNonDbClient(paths, cs.prefix)
		cs.subList = streamList(cs.prefix, paths, gpb.SubscriptionMode_SAMPLE, cs.interval, cs.cfg.Encoding)
	} else if target == "OC_YANG" {
		dc, err = sdc.NewTranslClient(cs.prefix, paths, ctx, nil, sdc.TranslWildcardOption{})
		cs.subList = streamList(cs.prefix, paths, gpb.SubscriptionMode_TARGET_DEFINED, 0, cs.cfg.Encoding)
	} else {
		dc, err = sdc.NewDbClient(paths, cs.prefix)
	}
	if err != nil {
		return nil, fmt.Errorf("cannot open the data source for target %s: %v", target, err)
	}
	return dc, nil
}

// clonePaths copies paths deeply.
func clonePaths(paths []*gpb.Path) []*gpb.Path {
	out := make([]*gpb.Path, len(paths))
	for i, p := range paths {
		out[i] = proto.Clone(p).(*gpb.Path)
	}
	return out
}

// streamList is the subscription list a stream passes to its data client,
// as a dial-in Subscribe would: every path in the given mode, sampled every
// interval (0 leaves the interval to the target). The paths must be the
// ones the data client was built with, since it looks them up by pointer.
func streamList(prefix *gpb.Path, paths []*gpb.Path, mode gpb.SubscriptionMode, interval time.Duration, enc gpb.Encoding) *gpb.SubscriptionList {
	subs := make([]*gpb.Subscription, len(paths))
	for i, p := range paths {
		subs[i] = &gpb.Subscription{Path: p, Mode: mode, SampleInterval: uint64(interval)}
	}
	return &gpb.SubscriptionList{Prefix: prefix, Subscription: subs, Mode: gpb.SubscriptionList_STREAM, Encoding: enc}
}

// refusedError is a stream ended by the data client refusing the entry's
// subscription list. A retry would be refused the same way.
type refusedError struct{ msg string }

func (e refusedError) Error() string { return e.msg }

// sourceError reports why the entry's data source cannot be opened, and
// closes it again when it can.
func (cs *clientSubscription) sourceError(ctx context.Context) error {
	dc, err := cs.openSource(ctx)
	if err != nil {
		return err
	}
	dc.Close()
	return nil
}

>>>>>>> 944266c (NOS-23562: [dialout_client] Keep a working subscription through a bad edit, and refuse rows that cannot run (#402))
// send runs until process Queue returns an error.
func (cs *clientSubscription) send(stream spb.GNMIDialOut_PublishClient) error {
	for {
		items, err := cs.q.Get(1)

		if items == nil {
			log.V(1).Infof("%v", err)
			return err
		}
		if err != nil {
			cs.errors++
			log.V(1).Infof("%v", err)
			return fmt.Errorf("unexpected queue Gext(1): %v", err)
		}

		var resp *gpb.SubscribeResponse
		switch v := items[0].(type) {
		case sdc.Value:
			if resp, err = sdc.ValToResp(v); err != nil {
				cs.errors++
				return err
			}
		default:
			log.V(1).Infof("Unknown data type %v for %s in queue", items[0], cs)
			cs.errors++
		}

		cs.sendMsg++
		err = stream.Send(resp)
		if err != nil {
			log.V(1).Infof("Client %s sending error:%v", cs, err)
			cs.errors++
			return err
		}
		log.V(5).Infof("Client %s done sending, msg count %d, msg %v", cs, cs.sendMsg, resp)
	}
}

// String returns the target the client is querying.
func (cs *clientSubscription) String() string {
	return fmt.Sprintf(" %s:%s:%s prefix %v paths %v interval %v, sendMsg %v, recvMsg %v",
		cs.name, cs.destGroupName, cs.reportType, cs.prefix.GetTarget(), cs.paths, cs.interval, cs.sendMsg, cs.recvMsg)
}

// newClient returns a new initialized GNMIDialout client.
// it connects to destination and publish service
// TODO: TLS credential support
func newClient(ctx context.Context, dest Destination) (*Client, error) {
	timeout := clientCfg.RetryInterval
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	opts := []grpc.DialOption{
		grpc.WithBlock(),
	}
	if clientCfg.TLS != nil {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(clientCfg.TLS)))
	}
	conn, err := grpc.DialContext(ctx, dest.Addrs, opts...)
	if err != nil {
		return nil, fmt.Errorf("Dial to (%s, timeout %v): %v", dest, timeout, err)
	}
	cl := spb.NewGNMIDialOutClient(conn)
	return &Client{
		conn:   conn,
		client: cl,
	}, nil
}

// Closing of client queue is triggered upon end of stream receive or stream error
// or fatal error of any client go routine .
// it will cause cancle of client context and exit of the send goroutines.
func (c *Client) Close() error {
	return c.conn.Close()
}

func publishRun(ctx context.Context, cs *clientSubscription, dests []Destination) {
	var err error
	var c *Client
	var destNum, destIdx int
	destNum = len(dests)
	destIdx = 0

restart: //Remote server might go down, in that case we restart with next destination in the group
	cs.cMu.Lock()
	cs.stop = make(chan struct{}, 1)
	cs.q = queue.NewPriorityQueue(1, false)
	cs.opened = true
	cs.client = nil
	cs.cMu.Unlock()

	cs.conTryCnt++
	dest := dests[destIdx]
	destIdx = (destIdx + 1) % destNum
	c, err = newClient(ctx, dest)
	select {
	case <-ctx.Done():
		cs.Close()
		log.V(1).Infof("%v: %v, cs.conTryCnt %v", cs, err, cs.conTryCnt)
		return
	default:
	}
	if err != nil {
		log.V(1).Infof("Dialout connection for %v failed for %v, %v cs.conTryCnt %v", dest, cs.name, err, cs.conTryCnt)
		goto restart
	}

	log.V(1).Infof("Dialout service connected to %v successfully for %v", dest, cs.name)
	pub, err := c.client.Publish(ctx)
	if err != nil {
		log.V(1).Infof("Publish to %v for %v failed: %v, retrying", dest, cs.name, err)
		c.Close()
		cs.Close()
		goto restart
	}

	cs.cMu.Lock()
	if cs.client == nil {
		cs.client = c
	} else {
		log.V(1).Infof("connection to %v already exists for %v, exiting publishRun", dest, cs)
		c.Close()
		cs.cMu.Unlock()
		return
	}
	cs.cMu.Unlock()

	switch cs.reportType {
	case Periodic:
		for {
			select {
			default:
				spbValues, err := cs.dc.Get(nil)
				if err != nil {
					// TODO: need to inform
					log.V(2).Infof("Data read error %v for %v", err, cs)
					continue
					//return nil, status.Error(codes.NotFound, err.Error())
				}
				var updates []*gpb.Update
				var spbValue *spb.Value
				for _, spbValue = range spbValues {
					update := &gpb.Update{
						Path: spbValue.GetPath(),
						Val:  spbValue.GetVal(),
					}
					updates = append(updates, update)
				}
				rs := &gpb.SubscribeResponse_Update{
					Update: &gpb.Notification{
						Timestamp: spbValue.GetTimestamp(),
						Prefix:    cs.prefix,
						Update:    updates,
					},
				}
				response := &gpb.SubscribeResponse{Response: rs}

				log.V(6).Infof("cs %s sending \n\t%v \n To %s", cs.name, response, dest)
				err = pub.Send(response)
				if err != nil {
					log.V(1).Infof("Client %v pub Send error:%v, cs.conTryCnt %v", cs.name, err, cs.conTryCnt)
					cs.Close()
					// Retry
					goto restart
				}
				log.V(6).Infof("cs %s to  %s done", cs.name, dest)
				cs.sendMsg++
				c.sendMsg++

				time.Sleep(cs.interval)
			case <-cs.stop:
				log.V(1).Infof("%v exiting publishRun routine for destination %s", cs, dest)
				return
			}
		}
	case Stream:
		select {
		default:
			cs.w.Add(1)
			go cs.dc.StreamRun(cs.q, cs.stop, &cs.w, nil)
			time.Sleep(100 * time.Millisecond)
			err = cs.send(pub)
			if err != nil {
				log.V(1).Infof("Client %v pub Send error:%v, cs.conTryCnt %v", cs.name, err, cs.conTryCnt)
			}
			cs.Close()
			cs.w.Wait()
			// Don't restart immediatly
			time.Sleep(clientCfg.RetryInterval)
			goto restart

		case <-cs.stop:
			log.V(1).Infof("%v exiting publishRun routine for destination %s", cs, dest)
			return
		}
	default:
		log.V(1).Infof("Unsupported report type %s in %v ", cs.reportType, cs)
	}
}

/*
	// telemetry client  global configuration
	Key         = TELEMETRY_CLIENT|Global
	src_ip      = IP
	retry_interval = 1*4DIGIT     ; In second
	encoding    = "JSON_IETF" / "ASCII" / "BYTES" / "PROTO"
	unidirectional = "true" / "false"    ; true by default
	; a Global row with any other field is refused whole. In every row
	; the NULL placeholder of an entry with no fields is skipped, not refused.

	// Destination group
	Key      = TELEMETRY_CLIENT|DestinationGroup_<name>
	dst_addr   = IP1:PORT2,IP2:PORT2       ;IP addresses separated by ","

	PORT = 1*5DIGIT
	IP = dec-octet "." dec-octet "." dec-octet "." dec-octet

	// Subscription group
	Key         = TELEMETRY_CLIENT|Subscription_<name>
<<<<<<< HEAD
	path_target = DbName
	paths       = PATH1,PATH2        ;PATH separated by ","
	dst_group   = <name>      ; // name of DestinationGroup
	report_type = "periodic" / "stream" / "once"
	report_interval = 1*8DIGIT      ; In millisecond,
=======
	path_target = DbName              ; required
	paths       = PATH1,PATH2         ; required; PATH separated by ","
	dst_group   = <name>              ; required; name of DestinationGroup
	report_type = "periodic" / "stream"   ; periodic by default
	report_interval = 1*8DIGIT      ; In millisecond, 1-99999999, 5000 by default.
	                                ; periodic: the pause between dumps of all paths.
	                                ; stream to OTHERS: the sampling interval, 1000 or more.
	                                ; stream to OC_YANG or a database: not used.
>>>>>>> 944266c (NOS-23562: [dialout_client] Keep a working subscription through a bad edit, and refuse rows that cannot run (#402))

	The sonic-telemetry_client YANG model declares the list key as "prefix name",
	so YANG-validated tooling (config replace, GCU, golden config) spells the same
	rows TELEMETRY_CLIENT|DestinationGroup|<name> and TELEMETRY_CLIENT|Subscription|<name>.
	Both spellings are accepted; see matchRowPrefix.
*/

// closeDestGroupClient close client instances for all clientSubscription using
// this Destination Group
func closeDestGroupClient(destGroupName string) {
	if names, ok := DestGrp2ClientSubMap[destGroupName]; ok {
		for _, name := range names {
			cs := ClientSubscriptionNameMap[name]
			cs.Close()
			cs.cancel()
		}
	}
}

<<<<<<< HEAD
=======
// closeInstance is every teardown of a live instance: it closes the
// instance and cancels its context, if it has one.
func closeInstance(cs *clientSubscription) {
	cs.Close()
	if cs.cancel != nil {
		cs.cancel()
	}
}

// takeDown stops an entry's instance and removes it from the maps.
func takeDown(cs *clientSubscription) {
	closeInstance(cs)
	unregisterSubscription(cs)
}

// editRetry holds, by Subscription name, the timer that applies the row
// again after an edit was refused because its data source could not be
// opened. Any event for the row, and the end of the run, stops it.
var editRetry = map[string]*time.Timer{}

// retryEditLater applies the Subscription row again after RetryInterval.
// The data clients do not tell an error that clears on its own from one
// that lasts until the row is edited, so both are retried, as retryLater
// does for an entry that is not serving.
func retryEditLater(ctx context.Context, redisDb *redis.Client, key string, name string) {
	var t *time.Timer
	t = time.AfterFunc(clientCfg.RetryInterval, func() {
		configMu.Lock()
		defer configMu.Unlock()
		if ctx.Err() != nil || editRetry[name] != t {
			return
		}
		delete(editRetry, name)
		if err := processTelemetryClientConfigLocked(ctx, redisDb, key, "hset"); err != nil {
			log.V(1).Infof("TELEMETRY_CLIENT %v retry refused: %v", key, err)
		}
	})
	editRetry[name] = t
}

// stopEditRetry drops the pending retry of a refused edit, if there is one.
func stopEditRetry(name string) {
	if t, ok := editRetry[name]; ok {
		t.Stop()
		delete(editRetry, name)
	}
}

>>>>>>> 944266c (NOS-23562: [dialout_client] Keep a working subscription through a bad edit, and refuse rows that cannot run (#402))
// setupDestGroupClients create client instances for all clientSubscription using
// this Destination Group
func setupDestGroupClients(ctx context.Context, destGroupName string) {
	if names, ok := DestGrp2ClientSubMap[destGroupName]; ok {
		for _, name := range names {
<<<<<<< HEAD
			// Create a copy of Client subscription, existing one might be closing, don't interfere with it.
			cs := *ClientSubscriptionNameMap[name]
=======
			// A new instance from the entry's configuration; the one being
			// closed keeps its own runtime state and is not touched. The new
			// one has its own context, so it can be cancelled on its own.
			old := ClientSubscriptionNameMap[name]
			if old == nil {
				continue
			}
			cs := old.configCopy()
			cctx, cancel := context.WithCancel(ctx)
			cs.cancel = cancel
>>>>>>> 944266c (NOS-23562: [dialout_client] Keep a working subscription through a bad edit, and refuse rows that cannot run (#402))
			log.V(2).Infof("NewInstance with destGroup change for %s to %s", name, destGroupName)
			cs.NewInstance(ctx)
			ClientSubscriptionNameMap[name] = &cs
		}
	}
}

// matchRowPrefix matches a TELEMETRY_CLIENT row key against a row type and returns
// the row name. The historical spelling joins the row type and the name with "_"
// (DestinationGroup_HS); the YANG model declares key "prefix name", so config
// written through YANG-validated tooling joins them with the CONFIG_DB key
// separator instead (DestinationGroup|HS). Accept both, so hand written config
// keeps working and dial-out becomes configurable through config replace / GCU.
func matchRowPrefix(key string, rowType string, separator string) (string, bool) {
	for _, sep := range []string{"_", separator} {
		if strings.HasPrefix(key, rowType+sep) {
			return strings.TrimPrefix(key, rowType+sep), true
		}
	}
	return "", false
}

<<<<<<< HEAD
=======
// rowUpdated reports whether a delete event left fields under the key. A
// keyspace hdel also fires when one field of a row is removed; the row is
// gone only when nothing is left.
func rowUpdated(op string, fv map[string]string) bool {
	return op == "hdel" && len(fv) > 0
}

// applyTelemetryClientConfig applies one TELEMETRY_CLIENT row change. The
// V(1) line here is the only trace of a refused row: it is not applied again
// until it is written again, except a Subscription edit whose data source
// could not be opened (see retryEditLater).
func applyTelemetryClientConfig(ctx context.Context, redisDb *redis.Client, key string, op string) {
	if err := processTelemetryClientConfig(ctx, redisDb, key, op); err != nil {
		log.V(1).Infof("TELEMETRY_CLIENT %v %v refused: %v", key, op, err)
	}
}

>>>>>>> 944266c (NOS-23562: [dialout_client] Keep a working subscription through a bad edit, and refuse rows that cannot run (#402))
// start/stop/update telemetry publist client as requested
// TODO: more validation on db data
func processTelemetryClientConfig(ctx context.Context, redisDb *redis.Client, key string, op string) error {
	configMu.Lock()
	defer configMu.Unlock()
	return processTelemetryClientConfigLocked(ctx, redisDb, key, op)
}

// processTelemetryClientConfigLocked is processTelemetryClientConfig with
// configMu held. The row is read under the lock, so the retry of a refused
// edit never applies a row older than one already applied.
func processTelemetryClientConfigLocked(ctx context.Context, redisDb *redis.Client, key string, op string) error {
	ns, _ := sdcfg.GetDbDefaultNamespace()
	separator, err := sdc.GetTableKeySeparator("CONFIG_DB", ns)
	if err != nil {
		return err
	}
	tableKey := "TELEMETRY_CLIENT" + separator + key
	fv, err := redisDb.HGetAll(context.Background(), tableKey).Result()
	if err != nil {
		log.V(2).Infof("redis HGetAll failed for %s with error %v", tableKey, err)
		return fmt.Errorf("redis HGetAll failed for %s with error %v", tableKey, err)
	}

	// NULL is the placeholder CONFIG_DB writers store for an entry with no
	// fields; such an entry reads as empty.
	delete(fv, "NULL")
	// A key already gone by the time it is read is a delete, whatever the
	// event said: an empty entry has nothing to apply.
	if len(fv) == 0 {
		op = "hdel"
	}
	log.V(2).Infof("Processing %v %v", tableKey, fv)

	ctx, cancel := context.WithCancel(ctx)

	if key == "Global" {
<<<<<<< HEAD
		if op == "hdel" {
			log.V(2).Infof("Invalid delete operation for %v", tableKey)
			return fmt.Errorf("Invalid delete operation for %v", tableKey)
		} else {
			for field, value := range fv {
				switch field {
				case "src_ip":
					clientCfg.SrcIp = value
				case "retry_interval":
					//TODO: check validity of the interval
					itvl, err := strconv.ParseUint(value, 10, 64)
					if err != nil {
						log.V(2).Infof("Invalid retry_interval %v %v", value, err)
						continue
					}
					clientCfg.RetryInterval = time.Second * time.Duration(itvl)
				case "encoding":
					//Flexible encoding Not supported yet
					clientCfg.Encoding = gpb.Encoding_JSON_IETF
				case "unidirectional":
					// No PublishResponse supported yet
					clientCfg.Unidirectional = true
				}
			}
			// Apply changes to all running instances
			for grpName := range destGrpNameMap {
				closeDestGroupClient(grpName)
				setupDestGroupClients(ctx, grpName)
			}
=======
		// fv is the whole entry, empty once the key is deleted. Global has
		// no lifetime of its own: a field it no longer has, or every field
		// after a delete, goes back to the value the run started with. The
		// row is checked whole before any of it is applied, so a refused row
		// changes nothing and re-dials nothing.
		cfg := globalDefaults
		for field, value := range fv {
			switch field {
			case "src_ip":
				cfg.SrcIp = value
			case "vrf":
				cfg.Vrf = value
			case "retry_interval":
				// At 0 every dial has already timed out and is retried at once.
				itvl, err := strconv.ParseUint(value, 10, 64)
				if err != nil || itvl == 0 || itvl > maxRetryIntervalSecs {
					return fmt.Errorf("Global refused: retry_interval %q is not a number of seconds from 1 to %v", value, maxRetryIntervalSecs)
				}
				cfg.RetryInterval = time.Second * time.Duration(itvl)
			case "encoding":
				//Flexible encoding Not supported yet
				cfg.Encoding = gpb.Encoding_JSON_IETF
			case "unidirectional":
				// No PublishResponse supported yet
				cfg.Unidirectional = true
			default:
				return fmt.Errorf("Global refused: unknown field %v value %v", field, value)
			}
		}
		if cfg == *clientCfg {
			return nil // nothing changed: leave the running instances alone
		}
		*clientCfg = cfg
		// Apply changes to all running instances
		for grpName := range destGrpNameMap {
			closeDestGroupClient(grpName)
			setupDestGroupClients(ctx, grpName)
>>>>>>> 944266c (NOS-23562: [dialout_client] Keep a working subscription through a bad edit, and refuse rows that cannot run (#402))
		}
	} else if destGroupName, matched := matchRowPrefix(key, "DestinationGroup", separator); matched {
		if destGroupName == "" {
			return fmt.Errorf("Empty  Destination Group name %v", key)
		}
		// Close any client intances targeting this Destination group
		closeDestGroupClient(destGroupName)
		//DestGrp2ClientSubMap
		if op == "hdel" {
			if _, ok := DestGrp2ClientSubMap[destGroupName]; ok {
				log.V(1).Infof("%v is being used: %v", destGroupName, DestGrp2ClientSubMap)
				return fmt.Errorf("%v is being used: %v", destGroupName, DestGrp2ClientSubMap)
			}
			delete(destGrpNameMap, destGroupName)
			log.V(3).Infof("Deleted  DestinationGroup %v", destGroupName)
			return nil
		} else {
			var dests []Destination
			for field, value := range fv {
				switch field {
				case "dst_addr":
					addrs := strings.Split(value, ",")
					for _, addr := range addrs {
						dst := Destination{Addrs: addr}
						if err = dst.Validate(); err != nil {
							log.V(2).Infof("Invalid destination address %v", addrs)
							return fmt.Errorf("Invalid destination address %v", addrs)
						}
						dests = append(dests, Destination{Addrs: addr})
					}
				default:
					log.V(2).Infof("Invalid DestinationGroup value %v", value)
					return fmt.Errorf("Invalid DestinationGroup value %v", value)
				}
			}
			destGrpNameMap[destGroupName] = dests
			setupDestGroupClients(ctx, destGroupName)
		}
<<<<<<< HEAD
=======
		if len(dests) == 0 {
			// A subscription started on an empty list panics the dial-out
			// client.
			return fmt.Errorf("DestinationGroup %v refused: no dst_addr", destGroupName)
		}
		// Close any client instances targeting this Destination group, then
		// re-dial them against the new list.
		closeDestGroupClient(destGroupName)
		destGrpNameMap[destGroupName] = dests
		setupDestGroupClients(ctx, destGroupName)
>>>>>>> 944266c (NOS-23562: [dialout_client] Keep a working subscription through a bad edit, and refuse rows that cannot run (#402))
	} else if name, matched := matchRowPrefix(key, "Subscription", separator); matched {
		if name == "" {
			return fmt.Errorf("Empty Subscription name %v", key)
		}
		csub, ok := ClientSubscriptionNameMap[name]
<<<<<<< HEAD
		if ok {
			csub.Close()
			csub.cancel()
=======
		stopEditRetry(name)
		if rowUpdated(op, fv) {
			op = "hset"
>>>>>>> 944266c (NOS-23562: [dialout_client] Keep a working subscription through a bad edit, and refuse rows that cannot run (#402))
		}

		if op == "hdel" {
			destGrpName := csub.destGroupName
			// Remove this ClientSubscrition from the list of the Destination group users
			csNames := DestGrp2ClientSubMap[destGrpName]
			for i, csName := range csNames {
				if name == csName {
					csNames = append(csNames[:i], csNames[i+1:]...)
					break
				}
			}
<<<<<<< HEAD
			DestGrp2ClientSubMap[destGrpName] = csNames
			// Delete clientSubscription from name map
			delete(ClientSubscriptionNameMap, name)
			log.V(3).Infof("Deleted  Client Subscription %v", name)
			return nil
		} else {
			// TODO: start one subscription publish routine for this request
			// Only start routine when DestGrp2ClientSubMap is not empty, or ...?
			cs := clientSubscription{
				interval: 5000 * time.Millisecond, // default to 5000 milliseconds
				name:     name,
				cancel:   cancel,
			}
			for field, value := range fv {
				switch field {
				case "dst_group":
					cs.destGroupName = value
				case "report_type":
					cs.reportType = NewReportType(value)
				case "report_interval":
					intvl, err := strconv.ParseUint(value, 10, 64)
					if err != nil {
						log.V(2).Infof("Invalid report_interval %v %v", value, err)
						continue
					}
					cs.interval = time.Duration(intvl) * time.Millisecond
				case "path_target":
					cs.prefix = &gpb.Path{
						Target: value,
					}
				case "paths":
					ps := strings.Split(value, ",")
					newPaths := []*gpb.Path{}
					for _, p := range ps {
						pp, err := ygot.StringToPath(p, ygot.StructuredPath)
						if err != nil {
							log.V(2).Infof("Invalid paths %v", value)
							return fmt.Errorf("Invalid paths %v", value)
=======
			takeDown(csub)
			log.V(3).Infof("Deleted  Client Subscription %v", name)
			return nil
		} else {
			// The instance's own context; cancelled here unless an instance
			// takes it.
			runCtx := ctx
			ctx, cancel := context.WithCancel(ctx)
			kept := false
			defer func() {
				if !kept {
					cancel()
				}
			}()
			cs := clientSubscription{
				interval:   5000 * time.Millisecond, // default to 5000 milliseconds
				reportType: Periodic,
				name:       name,
				cancel:     cancel,
			}
			parse := func() error {
				for field, value := range fv {
					switch field {
					case "dst_group":
						cs.destGroupName = value
					case "report_type":
						// The publisher runs these two only; any other type
						// would dial and never send.
						if cs.reportType = NewReportType(value); cs.reportType != Periodic && cs.reportType != Stream {
							return fmt.Errorf("report_type %q is not periodic or stream", value)
>>>>>>> 944266c (NOS-23562: [dialout_client] Keep a working subscription through a bad edit, and refuse rows that cannot run (#402))
						}
					case "report_interval":
						intvl, err := strconv.ParseUint(value, 10, 64)
						// 0 would read and send in a busy loop.
						if err != nil || intvl == 0 || intvl > maxReportIntervalMs {
							return fmt.Errorf("report_interval %q is not a number of milliseconds from 1 to %v", value, maxReportIntervalMs)
						}
						cs.interval = time.Duration(intvl) * time.Millisecond
					case "path_target":
						cs.prefix = &gpb.Path{
							Target: value,
						}
					case "paths":
						ps := strings.Split(value, ",")
						newPaths := []*gpb.Path{}
						for _, p := range ps {
							pp, err := ygot.StringToPath(p, ygot.StructuredPath)
							if err != nil {
								log.V(2).Infof("Invalid paths %v", value)
								return fmt.Errorf("Invalid paths %v", value)
							}
							// append *gpb.Path
							newPaths = append(newPaths, pp)
						}
						cs.paths = newPaths
					default:
						log.V(2).Infof("Invalid field %v value %v", field, value)
						return fmt.Errorf("Invalid field %v value %v", field, value)
					}
				}
<<<<<<< HEAD
			}
			log.V(2).Infof("New clientSubscription %v", cs)
			if cs.destGroupName == "" {
				// not destination configured, just return
=======
				if cs.destGroupName == "" {
					return fmt.Errorf("no destination group configured")
				}
				if cs.prefix.GetTarget() == "" {
					return fmt.Errorf("no path_target configured")
				}
				if len(cs.paths) == 0 {
					return fmt.Errorf("no paths configured")
				}
				// A stream to OTHERS is sampled every report_interval, and its
				// data client refuses to sample faster than MinSampleInterval.
				if cs.reportType == Stream && cs.prefix.GetTarget() == "OTHERS" && cs.interval < sdc.MinSampleInterval {
					return fmt.Errorf("a stream to OTHERS is sampled every report_interval, which cannot be less than %v", sdc.MinSampleInterval)
				}
>>>>>>> 944266c (NOS-23562: [dialout_client] Keep a working subscription through a bad edit, and refuse rows that cannot run (#402))
				return nil
			}
			if err := parse(); err != nil {
				// A row that is refused changes nothing: the registered
				// entry keeps its old configuration, running or waiting. Only
				// a row with no destination group stops it: the row is still
				// refused, and the error says the entry was stopped.
				if ok && fv["dst_group"] == "" {
					takeDown(csub)
					return fmt.Errorf("%v; the subscription under this name is stopped", err)
				}
				return err
			}
			log.V(2).Infof("New clientSubscription %v", &cs)
			// The entry parsed. An update whose data source cannot be opened
			// (a mistyped target, a database that does not answer) is refused
			// while an instance is serving, so it keeps serving, and the row
			// is applied again after RetryInterval. Otherwise the replaced
			// instance is taken down, and taken off its old group's list if
			// the group changed. A group not in CONFIG_DB yet is not refused:
			// rows may arrive in any order, so the entry waits for its group.
			if ok {
				if csub.serving() {
					if err := cs.sourceError(ctx); err != nil {
						retryEditLater(runCtx, redisDb, key, name)
						return fmt.Errorf("%v; the running instance is kept and the row applied again in %v", err, clientCfg.RetryInterval)
					}
				}
				takeDown(csub)
			}

<<<<<<< HEAD
			var found bool
			for _, na := range DestGrp2ClientSubMap[cs.destGroupName] {
				if na == cs.name {
					found = true
					break
				}
			}
			if !found {
				// Add this clientSubscription to the user list of Destination group
				DestGrp2ClientSubMap[cs.destGroupName] = append(DestGrp2ClientSubMap[cs.destGroupName], cs.name)
			}
			ClientSubscriptionNameMap[cs.name] = &cs
			log.V(2).Infof("NewInstance with Subscription change for %s to %s", cs.name, cs.destGroupName)
			cs.NewInstance(ctx)
=======
			DestGrp2ClientSubMap[cs.destGroupName] = append(DestGrp2ClientSubMap[cs.destGroupName], cs.name)
			kept = true
			ClientSubscriptionNameMap[cs.name] = &cs
			log.V(2).Infof("NewInstance with Subscription change for %s to %s", cs.name, cs.destGroupName)
			if err := cs.NewInstance(ctx, *clientCfg); err != nil {
				// The row stays registered: a group that does not exist
				// yet starts it when its row arrives.
				log.V(1).Infof("Subscription %v is registered but not started: %v", cs.name, err)
			}
>>>>>>> 944266c (NOS-23562: [dialout_client] Keep a working subscription through a bad edit, and refuse rows that cannot run (#402))
		}
	}
	return nil
}

<<<<<<< HEAD
=======
// resetClientSubscriptions stops every running instance and empties the
// configuration tables.
func resetClientSubscriptions() {
	configMu.Lock()
	defer configMu.Unlock()
	resetLocked()
}

// resetLocked is resetClientSubscriptions with configMu held.
func resetLocked() {
	for name, cs := range ClientSubscriptionNameMap {
		if cs == nil {
			continue
		}
		closeInstance(cs)
		delete(ClientSubscriptionNameMap, name)
	}
	DestGrp2ClientSubMap = map[string][]string{}
	destGrpNameMap = map[string][]Destination{}
	for name := range editRetry {
		stopEditRetry(name)
	}
}

>>>>>>> 944266c (NOS-23562: [dialout_client] Keep a working subscription through a bad edit, and refuse rows that cannot run (#402))
// read configDB data for telemetry client and start publishing service for client subscription
func DialOutRun(ctx context.Context, ccfg *ClientConfig) error {
	clientCfg = ccfg
	ns, _ := sdcfg.GetDbDefaultNamespace()
	dbn, err := sdcfg.GetDbId("CONFIG_DB", ns)
	if err != nil {
		return err
	}

	var redisDb *redis.Client
	if sdc.UseRedisLocalTcpPort == false {
		addr, err := sdcfg.GetDbSock("CONFIG_DB", ns)
		if err != nil {
			return err
		}
		optsUnix := redisopts.New(redis.Options{
			Network:     "unix",
			Addr:        addr,
			Password:    "", // no password set
			DB:          dbn,
			DialTimeout: 0,
		})
		redisDb = redis.NewClient(optsUnix)
	} else {
		addr, err := sdcfg.GetDbTcpAddr("CONFIG_DB", ns)
		if err != nil {
			return err
		}
		optsTcp := redisopts.New(redis.Options{
			Network:     "tcp",
			Addr:        addr,
			Password:    "", // no password set
			DB:          dbn,
			DialTimeout: 0,
		})
		redisDb = redis.NewClient(optsTcp)
	}

	separator, err := sdc.GetTableKeySeparator("CONFIG_DB", ns)
	if err != nil {
		return err
	}
	pattern := "__keyspace@" + strconv.Itoa(int(dbn)) + "__:TELEMETRY_CLIENT" + separator
	prefixLen := len(pattern)
	pattern += "*"

	pubsub := redisDb.PSubscribe(context.Background(), pattern)
	defer pubsub.Close()

	msgi, err := pubsub.ReceiveTimeout(context.Background(), time.Second)
	if err != nil {
		log.V(1).Infof("psubscribe to %s failed %v", pattern, err)
		return fmt.Errorf("psubscribe to %s failed %v", pattern, err)
	}
	subscr := msgi.(*redis.Subscription)
	if subscr.Channel != pattern {
		log.V(1).Infof("psubscribe to %s failed", pattern)
		return fmt.Errorf("psubscribe to %s", pattern)
	}
	log.V(2).Infof("Psubscribe succeeded: %v", subscr)

	var dbkeys []string
	dbkey_prefix := "TELEMETRY_CLIENT" + separator
	dbkeys, err = redisDb.Keys(context.Background(), dbkey_prefix+"*").Result()
	if err != nil {
		log.V(2).Infof("redis Keys failed for %v with err %v", pattern, err)
		return err
	}
	for _, dbkey := range dbkeys {
		dbkey = dbkey[len(dbkey_prefix):]
		processTelemetryClientConfig(ctx, redisDb, dbkey, "hset")
	}

	for {
		msgi, err := pubsub.ReceiveTimeout(context.Background(), time.Millisecond*1000)
		if err != nil {
			neterr, ok := err.(net.Error)
			if ok {
				if neterr.Timeout() == true {
					continue
				}
			}
			log.V(2).Infof("pubsub.ReceiveTimeout err %v", err)
			continue
		}
		subscr := msgi.(*redis.Message)
		dbkey := subscr.Channel[prefixLen:]
		if subscr.Payload == "del" || subscr.Payload == "hdel" {
			processTelemetryClientConfig(ctx, redisDb, dbkey, "hdel")
		} else if subscr.Payload == "hset" {
			processTelemetryClientConfig(ctx, redisDb, dbkey, "hset")
		} else {
			log.V(2).Infof("Invalid psubscribe payload notification:  %v", subscr)
			continue
		}
		// Check if ctx was canceled.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}
