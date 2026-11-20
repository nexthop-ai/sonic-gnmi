// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package telemetry_dialout

// Dial-out subscription state in STATE_DB.
//
// The dial-out client writes one server row, GNMI_SERVER_STATE|dialout.
// For each TELEMETRY_CLIENT Subscription entry registered with the client
// it also writes a GNMI_SUBSCRIPTION|dialout_<name> row and its path rows.
// The rows exist while the entry's CONFIG_DB key does, whether or not the
// entry can run (see below). Only an entry whose name is empty or holds
// the STATE_DB key separator gets none.
//
// The subscription row shows whether the client is connected, which
// collector it is dialing, how many times it has tried, and the last
// error. This way an unreachable collector can be seen from the switch.
// A state of connecting means no data is being delivered right now. The
// client may be dialing, waiting to retry, or connected but unable to read
// the data source.
//
// An entry gets a row even when the client cannot send anything for it.
// The row's state and last_error then say what is wrong:
//   - A new entry that does not parse, or has no dst_group: state is
//     misconfigured.
//   - An edit that does not parse, to an entry already registered: the
//     entry keeps its previous config, running or waiting, with its row's
//     state, and last_error says the update was not applied. An edit that
//     clears dst_group stops the entry, and its row turns misconfigured.
//   - Its data source cannot be opened: state stays connecting, and the
//     client tries again every retry_interval.
//   - Its data source refuses its subscription list: state stays
//     connecting with the refusal in last_error, and the entry is not
//     tried again until its row or its destination group is edited.
//   - Its name is empty, or holds the STATE_DB key separator: there is no
//     row, since the name cannot make a STATE_DB key; the error is only
//     logged.
//
// The rows belong to the entry, not to the instance serving it. When a
// destination group or Global change replaces the instance, the new one
// keeps the counters and start_time, and the old one stops writing. When
// the entry itself is edited, it counts as a new subscription: the rows are
// removed and written again, so the hooks below run on every accepted edit,
// not only on an add or a delete.

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	log "github.com/golang/glog"
	gpb "github.com/openconfig/gnmi/proto/gnmi"
	"github.com/sonic-net/sonic-gnmi/common_utils"
	sdcfg "github.com/sonic-net/sonic-gnmi/sonic_db_config"
	"github.com/sonic-net/sonic-gnmi/substate"
)

const (
	dialoutIDPrefix = substate.ServerDialout + "_"

	stateConnecting    = "connecting"
	stateConnected     = "connected"
	stateMisconfigured = "misconfigured" // the entry cannot be served as written

	// The reasons a subscription can fail. Each one has its own counter on
	// the server row, named failed_<reason>. The names come from substate,
	// so the gNMI server and the dial-out client use the same ones.
	//
	// Waiting for a destination group that does not exist yet is not a
	// failure and is not counted. The row's last_error says it is waiting,
	// and the entry starts when the group is added.
	causeConnect = substate.CauseConnect // the collector was not reached, or the stream to it could not be opened
	causeSend    = substate.CauseSend    // a stream was up and then broke
	causeSource  = substate.CauseSource  // the data source aborted a running stream, or a periodic read of it failed; one that cannot be opened is not counted
	causeEncode  = substate.CauseEncode  // a value could not be encoded for the collector
)

// failureCauses lists the reasons that get their own counter. A failure
// for any other reason is counted in failed_other.
var failureCauses = []string{causeConnect, causeSend, causeSource, causeEncode}

// statePublisher writes the dial-out server row, GNMI_SERVER_STATE|dialout.
//
// The row's two counters, configured_subscriptions and
// connected_subscriptions, are not kept as running totals. Each time the
// row is refreshed, counters() counts the entries in
// ClientSubscriptionNameMap and how many of them are connected. Counting
// fresh every time means the numbers cannot go wrong when a subscription is
// removed or replaced along some path that forgets to decrement.
//
// The failed_<reason> counters work the other way. A failure happens once
// and cannot be counted again later, so each one is added to failedBy at
// the moment it happens.
//
// counters, and each entry's snapshot, read ClientSubscriptionNameMap under
// configMu, so a tick waits while a config edit holds it. An edit opens the
// entry's data source under configMu, so a source that is slow to open
// holds back every dial-out row, last_updated included, until it returns.
type statePublisher struct {
	pub *substate.Publisher

	failedBy *substate.FailureCounts
}

func newDialoutState() *statePublisher {
	return &statePublisher{failedBy: substate.NewFailureCounts(failureCauses...)}
}

// statePub is this process's publisher, or nil when publishing is off. It
// is an atomic pointer because the client's goroutines read it while a run
// starts or stops it.
var statePub atomic.Pointer[statePublisher]

func currentStatePublisher() *statePublisher {
	return statePub.Load()
}

// ownsDialoutID reports whether a subscription row id belongs to the
// dial-out client. At startup the publisher deletes rows left over from a
// previous run, and uses this to pick only the dial-out rows: their ids
// all start with dialout_.
func ownsDialoutID(id string) bool { return strings.HasPrefix(id, dialoutIDPrefix) }

// startStatePublisher sets up state publishing for one run of the client.
// It builds the STATE_DB client, starts the publisher, which writes the
// GNMI_SERVER_STATE|dialout row on its own goroutine, and stores it in
// statePub, which is where the subscription hooks (stateOnDial,
// stateOnConnected, ...) look for it.
//
// It returns the publisher it created, and DialOutRun hands that same
// value back to stopStatePublisher when the run ends. The reason to pass
// the value instead of reading statePub again: a new run can start before
// the old one has finished stopping. The old run must then stop its own
// publisher and leave the new run's alone.
//
// It returns nil, with a warning, only when the STATE_DB client cannot be
// built from the database configuration; the client then runs as usual and
// writes no state. A STATE_DB that is down does not make it return nil: the
// client is built without a probe, and its writes fail one by one.
func startStatePublisher() *statePublisher {
	st := newDialoutState()
	pub, err := substate.New(substate.ServerDialout)
	if err != nil {
		log.Warningf("dial-out subscription state publishing disabled: %v", err)
		return nil
	}
	st.pub = pub
	if old := statePub.Swap(st); old != nil {
		old.pub.Stop()
	}
	// listen_address, unix_socket, vrf and threshold describe a server's
	// listener and its subscription limit. The dial-out client has neither:
	// it connects out, and it serves every entry in CONFIG_DB. So they are
	// empty here; the VRF it connects through is the Global vrf in CONFIG_DB.
	pub.Start(substate.Row{"listen_address": "", "unix_socket": "", "vrf": "", "threshold": "0"}, st.counters, ownsDialoutID)
	return st
}

// stopStatePublisher stops st. If st is still the publisher subscriptions
// report to, it is uninstalled first, so nothing can write a row after
// the delete, and its rows are deleted. A publisher a newer run has
// replaced is only stopped: the ids are the entry names, the same in both
// runs, so its delete would take the newer run's rows.
func stopStatePublisher(st *statePublisher) {
	if st == nil {
		return
	}
	if statePub.CompareAndSwap(st, nil) {
		st.pub.StopAndClear()
	} else {
		st.pub.Stop()
	}
}

func (st *statePublisher) counters() map[string]int64 {
	configMu.Lock()
	configured := int64(len(ClientSubscriptionNameMap))
	var connected int64
	for _, cs := range ClientSubscriptionNameMap {
		if cs.isConnected() {
			connected++
		}
	}
	configMu.Unlock()
	row := map[string]int64{
		"configured_subscriptions": configured,
		"connected_subscriptions":  connected,
	}
	st.failedBy.Fields(row)
	return row
}

func (st *statePublisher) failed(cs *clientSubscription, cause string) {
	st.failedBy.Add(cause)
	log.V(1).Infof("dial-out subscription state: %s to %s failed, cause %s", cs.stateID(), cs.getPeer(), cause)
}

// holdsStateSeparator reports whether name holds STATE_DB's key separator.
// Such a name cannot be part of a row key.
func holdsStateSeparator(name string) bool {
	ns, err := sdcfg.GetDbDefaultNamespace()
	if err != nil {
		return false
	}
	sep, err := sdcfg.GetDbSeparator("STATE_DB", ns)
	return err == nil && sep != "" && strings.Contains(name, sep)
}

// --- per-subscription state -------------------------------------------------

func (cs *clientSubscription) stateID() string { return dialoutIDPrefix + cs.name }

func (cs *clientSubscription) setPeer(peer string) {
	cs.connMu.Lock()
	cs.peer = peer
	cs.connMu.Unlock()
}

func (cs *clientSubscription) getPeer() string {
	cs.connMu.Lock()
	defer cs.connMu.Unlock()
	return cs.peer
}

func (cs *clientSubscription) setState(state string) {
	cs.connMu.Lock()
	cs.state = state
	cs.connMu.Unlock()
}

func (cs *clientSubscription) isConnected() bool {
	cs.connMu.Lock()
	defer cs.connMu.Unlock()
	return cs.state == stateConnected
}

// retireNow marks the instance as retired: it no longer serves its entry,
// and anything it reports from now on is ignored. Its state goes back to
// connecting. It returns true the first time it is called on an instance
// and false after that, so a caller can act once.
func (cs *clientSubscription) retireNow() (first bool) {
	cs.connMu.Lock()
	defer cs.connMu.Unlock()
	if cs.retired {
		return false
	}
	cs.retired = true
	cs.state = stateConnecting
	cs.local = "" // its connection is being closed
	cs.peer = ""  // it dials nothing more; the new instance sets its own
	return true
}

// becomeConnected sets the state to connected and clears the last error.
// It returns true if the state actually changed. A retired instance, or
// one already connected, returns false, and the caller publishes nothing.
func (cs *clientSubscription) becomeConnected() bool {
	cs.connMu.Lock()
	defer cs.connMu.Unlock()
	if cs.retired || cs.state == stateConnected {
		return false
	}
	cs.state = stateConnected
	cs.lastErr = ""
	return true
}

// becomeDisconnected sets the state back to connecting and records err as
// the reason. The local address is left alone: a data source failure does
// not close the connection, and the row shows the address only while
// connected (see connectionRowLocked). live is false, and nothing changes,
// if the instance is retired. changed is false if the row already says
// exactly this, so a failure that repeats the last one is not published
// again.
func (cs *clientSubscription) becomeDisconnected(err error) (live, changed bool) {
	cs.connMu.Lock()
	defer cs.connMu.Unlock()
	if cs.retired {
		return false, false
	}
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	if cs.state == stateConnecting && cs.lastErr == msg {
		return true, false
	}
	cs.state = stateConnecting
	cs.lastErr = msg
	return true, true
}

// disconnect sets the row to connecting with err as last_error, and writes
// the row if anything changed. It does nothing for a retired instance.
func (cs *clientSubscription) disconnect(err error) {
	if live, changed := cs.becomeDisconnected(err); live && changed {
		cs.publishConnection()
	}
}

func (cs *clientSubscription) isRetired() bool {
	cs.connMu.Lock()
	defer cs.connMu.Unlock()
	return cs.retired
}

// revive resets the instance to its starting state: not retired,
// connecting, no error.
func (cs *clientSubscription) revive() {
	cs.connMu.Lock()
	cs.retired = false
	cs.state = stateConnecting
	cs.lastErr = ""
	cs.connMu.Unlock()
}

// connectionRow is the fields of the subscription row that change as the
// connection comes and goes: the addresses, the state, the attempt count
// and the last error.
func (cs *clientSubscription) connectionRow() substate.Row {
	cs.connMu.Lock()
	defer cs.connMu.Unlock()
	return cs.connectionRowLocked()
}

// connectionRowLocked is connectionRow for a caller that already holds
// cs.connMu. The local address is shown only while the row says connected.
func (cs *clientSubscription) connectionRowLocked() substate.Row {
	local := cs.local
	if cs.state != stateConnected {
		local = ""
	}
	return substate.Row{
		"local_address":    local,
		"peer":             cs.peer,
		"state":            cs.state,
		"connect_attempts": substate.Num(cs.conTryCnt.Load()),
		"last_error":       cs.lastErrorText(),
	}
}

// lastErrorText returns the text for the row's last_error. Two errors can
// apply at once: a refused edit, and a connection error. Both are shown,
// the refused edit first, so neither hides the other. The caller must hold
// cs.connMu.
func (cs *clientSubscription) lastErrorText() string {
	r, _ := cs.rejected.Load().(string)
	switch {
	case r == "":
		return cs.lastErr
	case cs.lastErr == "":
		return r
	}
	return r + "; " + cs.lastErr
}

// counterRow is what the publisher writes on every periodic tick: the
// message, byte and error counters, the queue depth, and the connection
// fields. The connection fields are included so that if an update was
// dropped, the next tick puts the row right.
func (cs *clientSubscription) counterRow() substate.Row {
	row := cs.connectionRow()
	tx, _ := cs.bytes.Load()
	depth := 0
	cs.cMu.Lock()
	if cs.q != nil && !cs.q.Disposed() {
		depth = cs.q.Len()
	}
	cs.cMu.Unlock()
	// There are no rx_msgs or rx_bytes fields: the dial-out stream carries
	// data one way, from the switch to the collector.
	row["tx_msgs"] = substate.Num(cs.sendMsg.Load())
	row["tx_bytes"] = substate.Num(tx)
	row["error_count"] = substate.Num(cs.errors.Load())
	row["queue_depth"] = substate.Num(depth)
	return row
}

// liveCounterRow returns the function the publisher calls on each tick to
// read an entry's counters. It looks the entry up by name every time, so
// after the instance is rebuilt the row shows the new instance. If the
// entry has been removed it returns nil, so the row gets no fresh counters
// or last_updated.
func liveCounterRow(name string) substate.Snapshot {
	return func() substate.Row {
		configMu.Lock()
		cs := ClientSubscriptionNameMap[name]
		configMu.Unlock()
		if cs == nil {
			return nil // no fresh counters or last_updated for a removed entry
		}
		return cs.counterRow()
	}
}

// listMode maps the entry's report type to the gNMI list mode shown on the
// row: periodic and stream reports are STREAM, once is ONCE. An unknown
// report type gets an empty mode, matching its path rows.
func (cs *clientSubscription) listMode() string {
	switch cs.reportType {
	case Periodic, Stream:
		return gpb.SubscriptionList_STREAM.String()
	case Once:
		return gpb.SubscriptionList_ONCE.String()
	default:
		return ""
	}
}

// stateRow is the full subscription row, written when the entry is first
// configured. The caller holds configMu: the encoding is read from
// clientCfg.
func (cs *clientSubscription) stateRow() substate.Row {
	target := cs.prefix.GetTarget()
	row := substate.Row{
		"direction":         "dial-out",
		"server":            substate.ServerDialout,
		"destination_group": cs.destGroupName,
		"list_mode":         cs.listMode(),
		"prefix":            "",
		"origin":            "",
		"target":            target,
		"encoding":          clientCfg.Encoding.String(),
		"start_time":        substate.Num(cs.startTime),
		"user":              "",
	}
	for k, v := range cs.connectionRow() {
		row[k] = v
	}
	return row
}

// dialoutBackend is the data client that serves a target: the host client
// for OTHERS, the YANG client for OC_YANG, and the database client for
// everything else. pathRows renders each path the way that client renders
// it, so this must match the client openSource builds.
func dialoutBackend(target string) common_utils.Backend {
	switch target {
	case "OTHERS":
		return common_utils.BackendHost
	case common_utils.TargetOCYANG:
		return common_utils.BackendYANG
	}
	return common_utils.BackendDB
}

// pathRows builds one GNMI_SUBSCRIPTION_PATH row per configured path,
// rendered the way the data client serving the entry renders it, the one
// dialoutBackend picks, so a reader can compare it with the dial-in rows. A periodic report maps to SAMPLE
// mode with the report interval. A stream report takes the mode its data
// client runs: ON_CHANGE for a database target, TARGET_DEFINED for OC_YANG
// (translib picks per path), and SAMPLE at the report interval for OTHERS,
// whose data client samples only. Any other report type (once, or one the
// client does not serve) has no per-path mode, so its rows carry the path
// and an empty mode only.
func (cs *clientSubscription) pathRows(pub *substate.Publisher) map[string]substate.Row {
	rows := map[string]substate.Row{}
	target := cs.prefix.GetTarget()
	for n, p := range cs.paths {
		mode, sample := "", uint64(0)
		switch cs.reportType {
		case Periodic:
			mode, sample = gpb.SubscriptionMode_SAMPLE.String(), uint64(cs.interval)
		case Stream:
			switch target {
			case common_utils.TargetOCYANG:
				mode = gpb.SubscriptionMode_TARGET_DEFINED.String()
			case "OTHERS":
				mode, sample = gpb.SubscriptionMode_SAMPLE.String(), uint64(cs.interval)
			default:
				mode = gpb.SubscriptionMode_ON_CHANGE.String()
			}
		}
		path := common_utils.RenderPath(nil, p, target, dialoutBackend(target))
		rows[pub.Key(substate.PathTable, cs.stateID(), fmt.Sprint(n))] = substate.PathRow(path, mode, sample, 0, false)
	}
	return rows
}

// --- hooks called by the client ------------------------------------------------

// stateOnConfigured writes the subscription row and its path rows when a
// Subscription entry is registered: when it is added, and again on every
// accepted edit, which replaces it as a new subscription.
func stateOnConfigured(cs *clientSubscription) {
	cs.startTime = time.Now().Unix()
	cs.revive()
	st := currentStatePublisher()
	if st == nil {
		return
	}
	rows := cs.pathRows(st.pub)
	rows[st.pub.Key(substate.SubscriptionTable, cs.stateID())] = cs.stateRow()
	st.pub.WriteRows(cs.stateID(), rows, liveCounterRow(cs.name))
}

// stateOnRemoved deletes the entry's rows when the entry is taken down: when
// it is deleted from CONFIG_DB, before every accepted edit re-registers it,
// and when an edit clears dst_group. It retires the instance first, so that
// if its goroutine reports something after this point, nothing is written.
// Calling it a second time on the same instance does nothing.
func stateOnRemoved(cs *clientSubscription) {
	first := cs.retireNow()
	st := currentStatePublisher()
	if !first || st == nil {
		return
	}
	st.pub.Delete(cs.stateID())
}

// stateOnRetired is called when an instance is replaced because its
// destination group or the Global configuration changed. The instance is
// retired, so it publishes nothing more and no longer counts as connected.
// Its rows are kept for the new instance; they show connecting until the
// new instance dials.
func stateOnRetired(cs *clientSubscription) {
	first := cs.retireNow()
	st := currentStatePublisher()
	if !first || st == nil {
		return
	}
	st.pub.UpdateRow(st.pub.Key(substate.SubscriptionTable, cs.stateID()), cs.connectionRow())
}

// stateOnNotReady is called when an entry cannot start yet: its destination
// group is not configured, or its data source cannot be opened. This is
// not counted as a failure. The row shows the reason in last_error. The
// entry starts when the group appears, or on a later retry (every
// retry_interval) once the source opens.
func stateOnNotReady(cs *clientSubscription, err error) {
	cs.disconnect(err)
}

// stateOnDial is called just before each dial attempt. It records dest as
// the peer, clears the local address, and publishes the row as connecting.
func stateOnDial(cs *clientSubscription, dest string) {
	if cs.isRetired() {
		return
	}
	cs.setPeer(dest)
	cs.setLocal("") // the previous connection's address no longer applies
	cs.setState(stateConnecting)
	cs.publishConnection()
}

// stateOnConnected is called when the stream to the collector is open, and
// again after every successful periodic read. It publishes only when the
// state actually changes to connected; if the row already says connected,
// nothing is written.
func stateOnConnected(cs *clientSubscription) {
	if cs.becomeConnected() {
		cs.publishConnection()
	}
}

// stateOnSourceDown is called when a periodic read of the data source
// fails. The first failed read of an outage is counted as a source failure
// and sets the row to connecting with the error. sourceDown then remembers
// that this outage has been counted: a later failed read is not counted
// again, and only refreshes last_error when its text differs. A good read
// clears sourceDown. A read that fails because the instance is stopping
// is not a failure, and changes nothing.
func stateOnSourceDown(ctx context.Context, cs *clientSubscription, err error) {
	if ctx.Err() != nil {
		return
	}
	if cs.sourceDown.Load() {
		cs.disconnect(err)
		return
	}
	// Only a failure that was counted marks the outage: a retired
	// instance counts nothing, and its replacement must count it.
	if stateOnFailure(cs, causeSource, err) {
		cs.errors.Add(1)
		cs.sourceDown.Store(true)
	}
}

// fail records a failure. It adds one to the subscription's error count
// and to the server row's counter for cause, and writes the row. If ctx is
// already cancelled, the instance is being stopped and the error comes from
// that, so nothing is counted.
func (cs *clientSubscription) fail(ctx context.Context, cause string, err error) {
	if ctx.Err() != nil {
		return
	}
	cs.errors.Add(1)
	stateOnFailure(cs, cause, err)
}

// stateOnFailure is called when a dial fails (causeConnect), an open
// stream breaks (causeSend), the data source fails (causeSource), or a
// value cannot be encoded (causeEncode). The row is set to connecting with
// the error, and the server row's counter for that cause goes up by one.
// It does nothing for a retired instance, and reports whether the instance
// was live, that is whether the failure was counted.
func stateOnFailure(cs *clientSubscription, cause string, err error) bool {
	live, changed := cs.becomeDisconnected(err)
	if !live {
		return false
	}
	if st := currentStatePublisher(); st != nil {
		st.failed(cs, cause)
	}
	if changed {
		cs.publishConnection()
	}
	return true
}

// stateOnRejectedUpdate is called when the client refuses an edit to a
// registered entry, because the new config is invalid, or because its data
// source cannot be opened while the entry is serving. The entry keeps its
// previous config, running or waiting. Until the entry is edited again, the
// row's last_error says the config in CONFIG_DB was not applied.
// An edit refused for its data source is applied again every
// retry_interval, so the refusal is logged only when its text changes. One
// refused as invalid is not applied again until the row is written again.
func stateOnRejectedUpdate(cs *clientSubscription, err error) {
	msg := fmt.Sprintf("update not applied: %v", err)
	if old, _ := cs.rejected.Swap(msg).(string); old != msg {
		log.Errorf("dial-out %s: update not applied, keeping the previous configuration: %v", cs.name, err)
	}
	cs.publishConnection()
}

// stateOnMisconfigured is called for a Subscription entry the client
// cannot run at all, because its config is invalid or it has no
// dst_group. No instance runs for it, so this function writes its row
// directly: state misconfigured, with the reason in last_error. That makes
// the bad entry visible in STATE_DB. It is not counted in
// configured_subscriptions. The row is deleted when the entry is deleted,
// and replaced when the entry is fixed.
func stateOnMisconfigured(name string, err error) {
	st := currentStatePublisher()
	if st == nil {
		return
	}
	log.Errorf("dial-out %s: %v", name, err)
	id := dialoutIDPrefix + name
	row := substate.Row{
		"direction":  "dial-out",
		"server":     substate.ServerDialout,
		"state":      stateMisconfigured,
		"last_error": err.Error(),
		"start_time": substate.Num(time.Now().Unix()),
	}
	// An empty snapshot: the tick stamps last_updated on the row, as on
	// every other subscription row, and adds nothing else.
	st.pub.WriteRows(id, map[string]substate.Row{st.pub.Key(substate.SubscriptionTable, id): row},
		func() substate.Row { return substate.Row{} })
}

// stateOnUnconfigured is called when a misconfigured entry is deleted. It
// deletes the row that stateOnMisconfigured wrote.
func stateOnUnconfigured(name string) {
	if st := currentStatePublisher(); st != nil {
		st.pub.Delete(dialoutIDPrefix + name)
	}
}

// stateOnUnsupported is called when the entry's report type is one the
// client does not implement. The parser refuses any such row, so config
// cannot reach this; it guards a caller that builds an entry without the
// parser. The row stays at connecting and last_error says why.
func stateOnUnsupported(cs *clientSubscription) {
	err := fmt.Errorf("report_type %q is not implemented; the subscription is not served", cs.reportType)
	log.Errorf("dial-out %s: %v", cs.name, err)
	cs.disconnect(err)
}

// publishConnection writes the connection fields to the row, unless the
// instance is retired. The retired check and the write happen under
// cs.connMu, the same lock retireNow takes. That way, once retireNow has
// returned, this instance cannot slip in one more update that would
// overwrite what its replacement has written.
func (cs *clientSubscription) publishConnection() {
	st := currentStatePublisher()
	if st == nil {
		return
	}
	cs.connMu.Lock()
	defer cs.connMu.Unlock()
	if cs.retired {
		return
	}
	st.pub.UpdateRow(st.pub.Key(substate.SubscriptionTable, cs.stateID()), cs.connectionRowLocked())
}

// setLocal records the local address of the instance's connection.
func (cs *clientSubscription) setLocal(local string) {
	cs.connMu.Lock()
	cs.local = local
	cs.connMu.Unlock()
}
