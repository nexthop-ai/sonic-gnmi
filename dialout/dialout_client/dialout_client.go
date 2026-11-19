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

<<<<<<< HEAD
	"github.com/sonic-net/sonic-gnmi/internal/redisopts"
=======
	"github.com/sonic-net/sonic-gnmi/common_utils"
>>>>>>> 015cfdc (NOS-8352: [dialout_client] publish one row per dial-out subscription to STATE_DB (#376))
	spb "github.com/sonic-net/sonic-gnmi/proto"
	sdc "github.com/sonic-net/sonic-gnmi/sonic_data_client"
	sdcfg "github.com/sonic-net/sonic-gnmi/sonic_db_config"
	"github.com/sonic-net/sonic-gnmi/substate"

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
	bytes     substate.Bytes // counted by the gRPC stats handler
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
>>>>>>> 015cfdc (NOS-8352: [dialout_client] publish one row per dial-out subscription to STATE_DB (#376))
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
		err := fmt.Errorf("Destination group is not set for %v", cs)
		stateOnNotReady(cs, err)
		return err
	}

	dests, ok := destGrpNameMap[cs.destGroupName]
	if !ok {
		// Not a failure: the group row may simply not have been seen yet
		// (the start-up replay walks the keys in no particular order), and
		// the entry is rebuilt when it appears.
		log.V(2).Infof("Destination group %v doesn't exist", cs.destGroupName)
		err := fmt.Errorf("Destination group %v doesn't exist", cs.destGroupName)
		stateOnNotReady(cs, err)
		return err
	}

<<<<<<< HEAD
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
=======
	if cs.reportType != Periodic && cs.reportType != Stream {
		// The parser refuses any other report type; this guards callers
		// that build an entry without it.
		stateOnUnsupported(cs) // nothing would be sent: no data source, no dial
		return fmt.Errorf("Unsupported report type %s in %v", cs.reportType, cs)
>>>>>>> 015cfdc (NOS-8352: [dialout_client] publish one row per dial-out subscription to STATE_DB (#376))
	}
	if err != nil {
<<<<<<< HEAD
		log.V(1).Infof("Connection to DB for %v failed: %v", *cs, err)
		return fmt.Errorf("Connection to DB for %v failed: %v", *cs, err)
=======
		log.V(1).Infof("%v", err)
		stateOnNotReady(cs, err)
		cs.retryLater(ctx)
		return err
>>>>>>> 015cfdc (NOS-8352: [dialout_client] publish one row per dial-out subscription to STATE_DB (#376))
	}
	cs.dc = dc
	go publishRun(ctx, cs, dests)
	log.V(2).Infof("publishRun for %v with destination %v", cs, dests)
	return nil
}

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

<<<<<<< HEAD
		cs.sendMsg++
		err = stream.Send(resp)
		if err != nil {
			log.V(1).Infof("Client %s sending error:%v", cs, err)
			cs.errors++
			return err
		}
		log.V(5).Infof("Client %s done sending, msg count %d, msg %v", cs, cs.sendMsg, resp)
=======
		err = stream.Send(resp)
		if err != nil {
			log.V(1).Infof("Client %s sending error:%v", cs, err)
			// A send cut short by the instance being retired is not an
			// error; see retireThenClose.
			if !cs.isRetired() {
				cs.errors.Add(1)
			}
			return err
		}
		cs.sendMsg.Add(1) // counted once delivered, as on the periodic path
		log.V(5).Infof("Client %s done sending, msg count %d, msg %v", cs, cs.sendMsg.Load(), resp)
>>>>>>> 015cfdc (NOS-8352: [dialout_client] publish one row per dial-out subscription to STATE_DB (#376))
	}
}

// String returns the target the client is querying.
func (cs *clientSubscription) String() string {
	var sent, recv uint64
	if cs.entryState != nil {
		sent, recv = cs.sendMsg.Load(), cs.recvMsg.Load()
	}
	return fmt.Sprintf(" %s:%s:%s prefix %v paths %v interval %v, sendMsg %v, recvMsg %v",
<<<<<<< HEAD
		cs.name, cs.destGroupName, cs.reportType, cs.prefix.GetTarget(), cs.paths, cs.interval, cs.sendMsg, cs.recvMsg)
=======
		cs.name, cs.destGroupName, cs.reportType, cs.prefix.GetTarget(), cs.paths, cs.interval, sent, recv)
}

// dialTarget hands addr to gRPC as written. The passthrough scheme stops a
// host such as "dns" from being read as a resolver scheme, and the zone
// separator of a link-local IPv6 address must be escaped in a URL.
func dialTarget(addr string) string {
	return "passthrough:///" + strings.ReplaceAll(addr, "%", "%25")
}

// vrfDialer returns a gRPC context dialer that dials TCP whatever the address
// looks like: gRPC's own dialer reads "unix:8080" as a socket path. For a VRF
// other than the default, its sockets are bound to the VRF device before
// connecting, so the connection is routed by that VRF's table. Binding by
// source address cannot do this: a Linux VRF only steers sockets bound to its
// device.
func vrfDialer(vrf string) func(context.Context, string) (net.Conn, error) {
	d := &net.Dialer{}
	if vrf != "" && vrf != "default" {
		d.Control = common_utils.BindToDevice(vrf)
	}
	return func(ctx context.Context, addr string) (net.Conn, error) {
		return d.DialContext(ctx, "tcp", addr)
	}
>>>>>>> 015cfdc (NOS-8352: [dialout_client] publish one row per dial-out subscription to STATE_DB (#376))
}

// newClient returns a new initialized GNMIDialout client.
// it connects to destination and publish service
// TODO: TLS credential support
<<<<<<< HEAD
func newClient(ctx context.Context, dest Destination) (*Client, error) {
	timeout := clientCfg.RetryInterval
=======
func newClient(ctx context.Context, cfg ClientConfig, dest Destination, bytes *substate.Bytes, onConnect func(local string)) (*Client, error) {
	timeout := cfg.RetryInterval
>>>>>>> 015cfdc (NOS-8352: [dialout_client] publish one row per dial-out subscription to STATE_DB (#376))
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	opts := []grpc.DialOption{
		grpc.WithBlock(),
<<<<<<< HEAD
=======
		grpc.WithContextDialer(vrfDialer(cfg.Vrf)),
		// Report the dialer's error, such as a bind to a VRF device that
		// does not exist, not only the deadline.
		grpc.WithReturnConnectionError(),
		grpc.WithStatsHandler(&substate.ClientStatsHandler{Bytes: bytes, OnConnect: onConnect}),
>>>>>>> 015cfdc (NOS-8352: [dialout_client] publish one row per dial-out subscription to STATE_DB (#376))
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

<<<<<<< HEAD
=======
// maxRetryIntervalSecs is the largest retry_interval this client accepts,
// and maxReportIntervalMs the largest report_interval; a larger value is
// refused. They follow the 1*4DIGIT and 1*8DIGIT rules below.
const (
	maxRetryIntervalSecs = 9999
	maxReportIntervalMs  = 99999999
)

// dialCollector dials a destination; a variable so a test can land a
// cancel between a dial that succeeded and the check that follows it.
var dialCollector = newClient

// periodicGet reads a periodic entry's data source; a variable so a test
// can make the source fail.
var periodicGet = func(cs *clientSubscription) ([]*spb.Value, error) { return cs.dc.Get(nil) }

// pauseBeforeRetry waits RetryInterval before the next attempt, or until
// the instance is cancelled.
func pauseBeforeRetry(ctx context.Context, cs *clientSubscription) {
	select {
	case <-ctx.Done():
	case <-time.After(cs.cfg.RetryInterval):
	}
}

// waitInterval waits the report interval, or until the instance stops.
func waitInterval(ctx context.Context, cs *clientSubscription) {
	select {
	case <-ctx.Done():
	case <-cs.stop:
	case <-time.After(cs.interval):
	}
}

>>>>>>> 015cfdc (NOS-8352: [dialout_client] publish one row per dial-out subscription to STATE_DB (#376))
func publishRun(ctx context.Context, cs *clientSubscription, dests []Destination) {
	var err error
	var c *Client
	var destNum, destIdx int
	destNum = len(dests)
	destIdx = 0

restart: //Remote server might go down, in that case we restart with next destination in the group
<<<<<<< HEAD
=======
	// A retired or cancelled instance stops here, before the attempt is
	// counted.
	if ctx.Err() != nil || cs.isRetired() {
		cs.Close()
		return
	}
>>>>>>> 015cfdc (NOS-8352: [dialout_client] publish one row per dial-out subscription to STATE_DB (#376))
	cs.cMu.Lock()
	cs.stop = make(chan struct{}, 1)
	cs.q = queue.NewPriorityQueue(1, false)
	cs.opened = true
	cs.client = nil
	cs.cMu.Unlock()

	cs.conTryCnt++
	dest := dests[destIdx]
	destIdx = (destIdx + 1) % destNum
<<<<<<< HEAD
	c, err = newClient(ctx, dest)
=======
	stateOnDial(cs, dest.Addrs)
	c, err = dialCollector(ctx, cs.cfg, dest, &cs.bytes, cs.setLocal)
>>>>>>> 015cfdc (NOS-8352: [dialout_client] publish one row per dial-out subscription to STATE_DB (#376))
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
		cs.errors.Add(1)
		stateOnDisconnected(cs, err)
>>>>>>> 015cfdc (NOS-8352: [dialout_client] publish one row per dial-out subscription to STATE_DB (#376))
		goto restart
	}

	pub, err := c.client.Publish(ctx)
	if err != nil {
		log.V(1).Infof("Publish to %v for %v failed: %v, retrying", dest, cs.name, err)
		cs.setLocal("") // the connection is closed; cleared before the row says so
		// A failure caused by the instance's own stop is not counted or shown.
		if ctx.Err() == nil {
			cs.errors.Add(1)
			stateOnDisconnected(cs, err)
		}
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
	log.V(1).Infof("Dialout service connected to %v successfully for %v", dest, cs.name)

	switch cs.reportType {
	case Periodic:
<<<<<<< HEAD
=======
		sessionStart := time.Now()
		stateOnConnected(cs)
>>>>>>> 015cfdc (NOS-8352: [dialout_client] publish one row per dial-out subscription to STATE_DB (#376))
		for {
			select {
			default:
				spbValues, err := periodicGet(cs)
				if err != nil {
<<<<<<< HEAD
					// TODO: need to inform
					log.V(2).Infof("Data read error %v for %v", err, cs)
=======
					// The source is down: the row leaves connected with the
					// error, and the next good read moves it back.
					log.V(2).Infof("Data read error %v for %v", err, cs)
					stateOnSourceDown(cs, err)
					waitInterval(ctx, cs)
					if ctx.Err() != nil {
						cs.Close()
						return
					}
>>>>>>> 015cfdc (NOS-8352: [dialout_client] publish one row per dial-out subscription to STATE_DB (#376))
					continue
					//return nil, status.Error(codes.NotFound, err.Error())
				}
				stateOnConnected(cs)
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
					// If the instance is being stopped, the send fails
					// because of the stop, not because the stream broke; that
					// is not counted.
					cs.setLocal("") // the connection is closed; cleared before the row says so
					if ctx.Err() == nil {
						cs.errors.Add(1)
						stateOnDisconnected(cs, err)
					}
>>>>>>> 015cfdc (NOS-8352: [dialout_client] publish one row per dial-out subscription to STATE_DB (#376))
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
		stateOnConnected(cs)
		select {
		default:
<<<<<<< HEAD
=======
			// An idle stream never calls Send, so Recv is the only way to see the
			// collector close the stream. Disposing the queue unblocks cs.send.
			q := cs.q
			// The collector ending the stream, when Recv sees it before a
			// Send fails; cs.send then returns nil, so it is counted below.
			var closedByCollector atomic.Pointer[error]
			go func() {
				for {
					if _, err := pub.Recv(); err != nil {
						// A Recv error is not logged once this instance has been
						// cancelled or closed: the teardown caused it.
						if !q.Disposed() && ctx.Err() == nil {
							log.V(1).Infof("Client %v pub Recv error:%v", cs.name, err)
							closedByCollector.Store(&err)
						}
						q.Dispose()
						return
					}
				}
			}()
>>>>>>> 015cfdc (NOS-8352: [dialout_client] publish one row per dial-out subscription to STATE_DB (#376))
			cs.w.Add(1)
			go cs.dc.StreamRun(cs.q, cs.stop, &cs.w, nil)
			time.Sleep(100 * time.Millisecond)
			err = cs.send(pub)
			if e := closedByCollector.Load(); err == nil && e != nil {
				err = *e
				if !cs.isRetired() { // as send does for an error of its own
					cs.errors.Add(1)
				}
			}
			cs.setLocal("") // the connection is closed; cleared before the row says so
			if err != nil {
<<<<<<< HEAD
				log.V(1).Infof("Client %v pub Send error:%v, cs.conTryCnt %v", cs.name, err, cs.conTryCnt)
=======
				log.V(1).Infof("Client %v pub Send error:%v, cs.conTryCnt %v", cs.name, err, cs.conTryCnt.Load())
				if cs.isConnected() && ctx.Err() == nil {
					stateOnDisconnected(cs, err)
				}
>>>>>>> 015cfdc (NOS-8352: [dialout_client] publish one row per dial-out subscription to STATE_DB (#376))
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
<<<<<<< HEAD
	path_target = DbName
	paths       = PATH1,PATH2        ;PATH separated by ","
	dst_group   = <name>      ; // name of DestinationGroup
	report_type = "periodic" / "stream" / "once"
	report_interval = 1*8DIGIT      ; In millisecond,
=======
	                                  ; <name> may not hold the STATE_DB key separator,
	                                  ; which splits its STATE_DB row key
	path_target = DbName              ; required
	paths       = PATH1,PATH2         ; required; PATH separated by ","
	dst_group   = <name>              ; required; name of DestinationGroup
	report_type = "periodic" / "stream"   ; periodic by default
	report_interval = 1*8DIGIT      ; In millisecond, 1-99999999, 5000 by default.
	                                ; periodic: the pause between dumps of all paths.
	                                ; stream to OTHERS: the sampling interval, 1000 or more.
	                                ; stream to OC_YANG or a database: not used.
>>>>>>> 015cfdc (NOS-8352: [dialout_client] publish one row per dial-out subscription to STATE_DB (#376))

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
<<<<<<< HEAD
			cs.Close()
			cs.cancel()
=======
			if cs == nil {
				continue
			}
			retireThenClose(cs, stateOnRetired)
>>>>>>> 015cfdc (NOS-8352: [dialout_client] publish one row per dial-out subscription to STATE_DB (#376))
		}
	}
}

<<<<<<< HEAD
=======
// retireThenClose is the one way a live instance is torn down. It retires
// the instance and cancels its context first, then closes it. The order
// matters: closing disposes the queue and the connection, and a send that
// fails because of that must not be counted as a broken stream or an
// error. Once the instance is retired and cancelled, it is not.
func retireThenClose(cs *clientSubscription, retire func(*clientSubscription)) {
	retire(cs)
	if cs.cancel != nil {
		cs.cancel()
	}
	cs.Close()
}

// takeDown stops an entry's instance and removes it from the maps. The
// rows are deleted as part of retiring the instance, before it is closed,
// so nothing the instance reports while shutting down can write them back.
func takeDown(cs *clientSubscription) {
	retireThenClose(cs, stateOnRemoved)
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

>>>>>>> 015cfdc (NOS-8352: [dialout_client] publish one row per dial-out subscription to STATE_DB (#376))
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
			cs.revive()
>>>>>>> 015cfdc (NOS-8352: [dialout_client] publish one row per dial-out subscription to STATE_DB (#376))
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
			destGrpName := csub.destGroupName
			// Remove this ClientSubscrition from the list of the Destination group users
			csNames := DestGrp2ClientSubMap[destGrpName]
			for i, csName := range csNames {
				if name == csName {
					csNames = append(csNames[:i], csNames[i+1:]...)
					break
				}
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
<<<<<<< HEAD
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
=======
				interval:   5000 * time.Millisecond, // default to 5000 milliseconds
				reportType: Periodic,
				name:       name,
				cancel:     cancel,
				entryState: &entryState{},
			}
			parse := func() error {
				// The name becomes part of the entry's STATE_DB key, where
				// STATE_DB's separator splits key parts.
				if stateSep, err := sdcfg.GetDbSeparator("STATE_DB", ns); err == nil && strings.Contains(name, stateSep) {
					return fmt.Errorf("subscription name %q contains the STATE_DB key separator %q", name, stateSep)
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
>>>>>>> 015cfdc (NOS-8352: [dialout_client] publish one row per dial-out subscription to STATE_DB (#376))
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
			stateOnConfigured(&cs)
			log.V(2).Infof("NewInstance with Subscription change for %s to %s", cs.name, cs.destGroupName)
			cs.NewInstance(ctx)
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
		retireThenClose(cs, func(c *clientSubscription) { c.retireNow() })
		delete(ClientSubscriptionNameMap, name)
	}
	DestGrp2ClientSubMap = map[string][]string{}
	destGrpNameMap = map[string][]Destination{}
	for name := range editRetry {
		stopEditRetry(name)
	}
}

>>>>>>> 015cfdc (NOS-8352: [dialout_client] publish one row per dial-out subscription to STATE_DB (#376))
// read configDB data for telemetry client and start publishing service for client subscription
func DialOutRun(ctx context.Context, ccfg *ClientConfig) error {
	clientCfg = ccfg
	ns, _ := sdcfg.GetDbDefaultNamespace()
	dbn, err := sdcfg.GetDbId("CONFIG_DB", ns)
	if err != nil {
		return err
	}

	// CONFIG_DB on its Unix socket when one is configured, else TCP, with a
	// dial timeout and context deadlines honoured.
	redisDb, separator, err := substate.DbClient("CONFIG_DB")
	if err != nil {
		return err
	}
<<<<<<< HEAD
=======
	// A run starts from an empty table and replays CONFIG_DB below; when
	// it ends, every instance it started is stopped and the table emptied,
	// so a later run (or a test) does not inherit entries, and the counts
	// derived from the table are this run's.
	configMu.Lock()
	if ccfg.RetryInterval <= 0 {
		ccfg.RetryInterval = time.Second // 0 would retry a failed dial with no pause
	}
	clientCfg = ccfg
	globalDefaults = *ccfg
	resetLocked()
	configMu.Unlock()
	st := startStatePublisher()
	defer stopStatePublisher(st)
	defer resetClientSubscriptions() // runs first: the instances stop before their rows go
>>>>>>> 015cfdc (NOS-8352: [dialout_client] publish one row per dial-out subscription to STATE_DB (#376))
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
