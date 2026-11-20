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

<<<<<<< HEAD
	conTryCnt uint64 //Number of time trying to connect
	sendMsg   uint64
	recvMsg   uint64
	errors    uint64
=======
	// entryState holds the counters and start time shown on the entry's
	// STATE_DB row. A rebuilt instance is given the same entryState, so
	// they carry over instead of starting from zero.
	*entryState

	// The connection as shown on the entry's STATE_DB row. connMu guards
	// these fields.
	connMu  sync.Mutex
	peer    string // the collector address being tried or connected to
	local   string // the connection's local address
	state   string // connecting or connected
	lastErr string // why the entry is not connected: a failed dial or send, a broken stream, a missing destination group or a data source that cannot be read
	retired bool   // no longer serves its entry (replaced, removed, or the run ended); it publishes nothing more
}

// entryState holds the counters and start time shown on a subscription's
// STATE_DB row. When Global or the subscription's destination group
// changes, closeDestGroupClient stops the running instance and
// setupDestGroupClients starts a new one. The new instance gets the same
// entryState, so the numbers keep counting. Editing the Subscription entry
// itself makes it a new subscription, and it starts with a fresh
// entryState. Where the entryState is shared, the old instance may still
// be finishing a send when the new one starts; a message it delivers in
// that time is still counted on the subscription's row, but a send cut
// short by its own teardown is not.
type entryState struct {
	conTryCnt atomic.Uint64 // number of dial attempts
	sendMsg   atomic.Uint64
	recvMsg   atomic.Uint64
	errors    atomic.Uint64
	startTime int64
	// rejected holds the error from the most recent edit the client
	// refused. The entry keeps its previous config, running or waiting. The
	// row's last_error shows this error until the entry is edited again.
	rejected atomic.Value   // string
	bytes    substate.Bytes // counted by the gRPC stats handler
	// sourceDown is true while the data source is down. It is set on the
	// first failure and cleared by the next successful send or read, so an
	// outage is counted once however many retries, reconnects or instance
	// rebuilds happen while it lasts.
	sourceDown atomic.Bool
}

// configCopy is a new entry with cs's configuration, the Config Data
// fields, and its entry state, and none of its instance's runtime state. A field added to Config Data must
// be added here.
func (cs *clientSubscription) configCopy() *clientSubscription {
	return &clientSubscription{
		name:          cs.name,
		destGroupName: cs.destGroupName,
		prefix:        cs.prefix,
		paths:         cs.paths,
		reportType:    cs.reportType,
		interval:      cs.interval,
		// The counters and start time belong to the entry, not the
		// instance, so the new instance takes them over.
		entryState: cs.entryState,
	}
>>>>>>> e5fa59d (NOS-8352: [dialout_client] failure counts by cause, and rows for entries that cannot be served (#377))
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

// send runs until process Queue returns an error.
func (cs *clientSubscription) send(ctx context.Context, stream spb.GNMIDialOut_PublishClient) error {
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
<<<<<<< HEAD
		case sdc.Value:
			if resp, err = sdc.ValToResp(v); err != nil {
				cs.errors++
				return err
=======
		case sdc.RequestFatal:
			// The data client cannot serve an entry of the subscription
			// list, such as an OC_YANG path translib will not subscribe.
			// That is a mistake in the entry, not a data source failure, so
			// it counts as failed_other. The stream is stopped, not retried.
			_, err = sdc.ValToResp(v.Value)
			if ctx.Err() == nil && stateOnFailure(cs, substate.CauseRequest, err) {
				cs.errors.Add(1)
			}
			return shownError{refusedError{err.Error()}}
		case sdc.Value:
			if resp, err = sdc.ValToResp(v); err != nil {
				// ValToResp fails only for a Fatal value, which ends the
				// stream; it is counted as a source failure. The client then
				// reconnects, and may find the same failure, so one outage
				// can land here many times. Count it the first time only;
				// after that, just disconnect.
				// A stop that ends the source is not a failure.
				if ctx.Err() != nil {
					return shownError{err}
				}
				if cs.sourceDown.Load() {
					cs.disconnect(err)
					return shownError{err}
				}
				if stateOnFailure(cs, causeSource, err) {
					cs.errors.Add(1)
					cs.sourceDown.Store(true)
				}
				return shownError{err}
>>>>>>> e5fa59d (NOS-8352: [dialout_client] failure counts by cause, and rows for entries that cannot be served (#377))
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
<<<<<<< HEAD
		log.V(5).Infof("Client %s done sending, msg count %d, msg %v", cs, cs.sendMsg, resp)
=======
		cs.sendMsg.Add(1) // counted once delivered, as on the periodic path
		if !cs.isRetired() && cs.sourceDown.Swap(false) {
			stateOnConnected(cs) // the source works again
		}
		log.V(5).Infof("Client %s done sending, msg count %d, msg %v", cs, cs.sendMsg.Load(), resp)
>>>>>>> e5fa59d (NOS-8352: [dialout_client] failure counts by cause, and rows for entries that cannot be served (#377))
	}
}

// shownError is an error send has already counted and shown on the row,
// so publishRun does not count it again.
type shownError struct{ error }

func (e shownError) Unwrap() error { return e.error }

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
<<<<<<< HEAD
		log.V(1).Infof("Dialout connection for %v failed for %v, %v cs.conTryCnt %v", dest, cs.name, err, cs.conTryCnt)
=======
		log.V(1).Infof("Dialout connection for %v failed for %v, %v cs.conTryCnt %v", dest, cs.name, err, cs.conTryCnt.Load())
		cs.fail(ctx, causeConnect, err)
>>>>>>> e5fa59d (NOS-8352: [dialout_client] failure counts by cause, and rows for entries that cannot be served (#377))
		goto restart
	}

	log.V(1).Infof("Dialout service connected to %v successfully for %v", dest, cs.name)
	pub, err := c.client.Publish(ctx)
	if err != nil {
		log.V(1).Infof("Publish to %v for %v failed: %v, retrying", dest, cs.name, err)
<<<<<<< HEAD
=======
		cs.setLocal("") // the connection is closed; cleared before the row says so
		cs.fail(ctx, causeConnect, err)
>>>>>>> e5fa59d (NOS-8352: [dialout_client] failure counts by cause, and rows for entries that cannot be served (#377))
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
<<<<<<< HEAD
=======
		sessionStart := time.Now()
		// During a source outage the row stays at connecting until a read
		// succeeds, as for a stream.
		if !cs.sourceDown.Load() {
			stateOnConnected(cs)
		}
>>>>>>> e5fa59d (NOS-8352: [dialout_client] failure counts by cause, and rows for entries that cannot be served (#377))
		for {
			select {
			default:
				spbValues, err := cs.dc.Get(nil)
				if err != nil {
<<<<<<< HEAD
					// TODO: need to inform
					log.V(2).Infof("Data read error %v for %v", err, cs)
=======
					// The data source could not be read. The first failed
					// read of an outage is counted and sets the row to
					// connecting. Later failed reads, on this instance or on
					// one rebuilt during the outage, are not counted again
					// and only refresh last_error. The next good read sets
					// the row back to connected.
					log.V(2).Infof("Data read error %v for %v", err, cs)
					stateOnSourceDown(ctx, cs, err)
					waitInterval(ctx, cs)
					if ctx.Err() != nil {
						cs.Close()
						return
					}
>>>>>>> e5fa59d (NOS-8352: [dialout_client] failure counts by cause, and rows for entries that cannot be served (#377))
					continue
					//return nil, status.Error(codes.NotFound, err.Error())
				}
<<<<<<< HEAD
=======
				// The outage, if there was one, is over. A retired
				// instance's late read says nothing about its replacement's
				// source, so it leaves the flag and the row alone.
				if !cs.isRetired() {
					cs.sourceDown.Store(false)
					stateOnConnected(cs)
				}
>>>>>>> e5fa59d (NOS-8352: [dialout_client] failure counts by cause, and rows for entries that cannot be served (#377))
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
<<<<<<< HEAD
					log.V(1).Infof("Client %v pub Send error:%v, cs.conTryCnt %v", cs.name, err, cs.conTryCnt)
=======
					log.V(1).Infof("Client %v pub Send error:%v, cs.conTryCnt %v", cs.name, err, cs.conTryCnt.Load())
					cs.setLocal("") // the connection is closed; cleared before the row says so
					cs.fail(ctx, substate.SendCause(err), err)
>>>>>>> e5fa59d (NOS-8352: [dialout_client] failure counts by cause, and rows for entries that cannot be served (#377))
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
<<<<<<< HEAD
=======
		// While the source is down, the row stays at connecting until a
		// send succeeds, instead of flipping on every reconnect.
		if !cs.sourceDown.Load() {
			stateOnConnected(cs)
		}
>>>>>>> e5fa59d (NOS-8352: [dialout_client] failure counts by cause, and rows for entries that cannot be served (#377))
		select {
		default:
			cs.w.Add(1)
			go cs.dc.StreamRun(cs.q, cs.stop, &cs.w, nil)
			time.Sleep(100 * time.Millisecond)
<<<<<<< HEAD
			err = cs.send(pub)
			if err != nil {
				log.V(1).Infof("Client %v pub Send error:%v, cs.conTryCnt %v", cs.name, err, cs.conTryCnt)
=======
			err = cs.send(ctx, pub)
			cause := substate.SendCause(err)
			if e := closedByCollector.Load(); err == nil && e != nil {
				err, cause = *e, substate.CauseSend
				if !cs.isRetired() { // as send does for an error of its own
					cs.errors.Add(1)
				}
			}
			cs.setLocal("") // the connection is closed; cleared before the row says so
			if err != nil {
				log.V(1).Infof("Client %v pub Send error:%v, cs.conTryCnt %v", cs.name, err, cs.conTryCnt.Load())
				// Not fail(): the error is already in error_count. A source
				// or request failure send has also counted and shown
				// (shownError). Any other failure, a transport error or a
				// collector that ended the stream, is counted here under
				// its cause, whatever the row says now.
				if shown := (shownError{}); !errors.As(err, &shown) && ctx.Err() == nil {
					stateOnFailure(cs, cause, err)
				}
>>>>>>> e5fa59d (NOS-8352: [dialout_client] failure counts by cause, and rows for entries that cannot be served (#377))
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

	// Destination group
	Key      = TELEMETRY_CLIENT|DestinationGroup_<name>
	dst_addr   = IP1:PORT2,IP2:PORT2       ;IP addresses separated by ","

	PORT = 1*5DIGIT
	IP = dec-octet "." dec-octet "." dec-octet "." dec-octet

	// Subscription group
	Key         = TELEMETRY_CLIENT|Subscription_<name>
	path_target = DbName
	paths       = PATH1,PATH2        ;PATH separated by ","
	dst_group   = <name>      ; // name of DestinationGroup
	report_type = "periodic" / "stream" / "once"
	report_interval = 1*8DIGIT      ; In millisecond,

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

// setupDestGroupClients create client instances for all clientSubscription using
// this Destination Group
func setupDestGroupClients(ctx context.Context, destGroupName string) {
	if names, ok := DestGrp2ClientSubMap[destGroupName]; ok {
		for _, name := range names {
			// Create a copy of Client subscription, existing one might be closing, don't interfere with it.
			cs := *ClientSubscriptionNameMap[name]
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

// applyTelemetryClientConfig applies one TELEMETRY_CLIENT row change. A
// refused row is logged here at V(1). A refused Subscription row also shows
// on its STATE_DB row, as misconfigured or with last_error naming the
// rejected update, unless its name is empty or holds the STATE_DB key
// separator; then it is only logged. A refused row is not applied again until it is written
// again, except a Subscription edit whose data source could not be opened
// (see retryEditLater).
func applyTelemetryClientConfig(ctx context.Context, redisDb *redis.Client, key string, op string) {
	if err := processTelemetryClientConfig(ctx, redisDb, key, op); err != nil {
		log.V(1).Infof("TELEMETRY_CLIENT %v %v refused: %v", key, op, err)
	}
}

>>>>>>> e5fa59d (NOS-8352: [dialout_client] failure counts by cause, and rows for entries that cannot be served (#377))
// start/stop/update telemetry publist client as requested
// TODO: more validation on db data
func processTelemetryClientConfig(ctx context.Context, redisDb *redis.Client, key string, op string) error {
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

	log.V(2).Infof("Processing %v %v", tableKey, fv)
	configMu.Lock()
	defer configMu.Unlock()

	ctx, cancel := context.WithCancel(ctx)

	if key == "Global" {
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
	} else if name, matched := matchRowPrefix(key, "Subscription", separator); matched {
		if name == "" {
			return fmt.Errorf("Empty Subscription name %v", key)
		}
		csub, ok := ClientSubscriptionNameMap[name]
		if ok {
			csub.Close()
			csub.cancel()
		}

		if op == "hdel" {
<<<<<<< HEAD
			destGrpName := csub.destGroupName
			// Remove this ClientSubscrition from the list of the Destination group users
			csNames := DestGrp2ClientSubMap[destGrpName]
			for i, csName := range csNames {
				if name == csName {
					csNames = append(csNames[:i], csNames[i+1:]...)
					break
				}
=======
			if !ok {
				// A row that was refused or has no dst_group is not
				// registered: there is nothing to stop, only the row that
				// shows it misconfigured.
				stateOnUnconfigured(name)
				return nil
>>>>>>> e5fa59d (NOS-8352: [dialout_client] failure counts by cause, and rows for entries that cannot be served (#377))
			}
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
<<<<<<< HEAD
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
=======
			parse := func() error {
				// The name becomes part of the entry's STATE_DB key, where
				// STATE_DB's separator splits key parts.
				if holdsStateSeparator(name) {
					return fmt.Errorf("subscription name %q contains the STATE_DB key separator", name)
				}
				for field, value := range fv {
					switch field {
					case "dst_group":
						cs.destGroupName = value
					case "report_type":
						// The publisher runs these two only; any other type
						// would dial and never send.
						if cs.reportType = NewReportType(value); cs.reportType != Periodic && cs.reportType != Stream {
							return fmt.Errorf("report_type %q is not periodic or stream", value)
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
>>>>>>> e5fa59d (NOS-8352: [dialout_client] failure counts by cause, and rows for entries that cannot be served (#377))
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
			log.V(2).Infof("New clientSubscription %v", cs)
			if cs.destGroupName == "" {
				// not destination configured, just return
				return nil
			}
<<<<<<< HEAD
=======
			if err := parse(); err != nil {
				// The new config is invalid. What happens next:
				//   - The edit removed dst_group: the instance is stopped and
				//     the entry dropped, and the row is still refused; the
				//     error says the entry was stopped. The edit that fixes
				//     the row adds it again.
				//   - Otherwise a registered entry keeps its old config,
				//     running or waiting, and its row's last_error says the
				//     update was not applied.
				//   - No entry is registered: it gets a row with state
				//     misconfigured, so the bad entry shows in STATE_DB,
				//     unless its name contains the key separator (below).
				if ok && fv["dst_group"] == "" {
					takeDown(csub)
					ok = false
					err = fmt.Errorf("%v; the subscription under this name is stopped", err)
				}
				switch {
				case ok:
					stateOnRejectedUpdate(csub, err)
				case holdsStateSeparator(name):
					// The name cannot be part of a STATE_DB key, so the
					// entry gets no row; the error is only logged.
					log.Errorf("dial-out %s: %v", name, err)
				default:
					stateOnMisconfigured(name, err)
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
						stateOnRejectedUpdate(csub, err)
						retryEditLater(runCtx, redisDb, key, name)
						return fmt.Errorf("%v; the running instance is kept and the row applied again in %v", err, clientCfg.RetryInterval)
					}
				}
				takeDown(csub)
			}
>>>>>>> e5fa59d (NOS-8352: [dialout_client] failure counts by cause, and rows for entries that cannot be served (#377))

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
		}
	}
	return nil
}

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
