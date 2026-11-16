// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package substate publishes gNMI subscription state to STATE_DB, so an
// operator can see from the switch which collectors hold subscriptions,
// what they subscribed to and whether the switch is keeping up. The
// design is the gNMI Subscription State HLD under doc/mgmt/gnmi in the
// SONiC design repository.
//
// It is written for two producers, the gNMI server (dial-in) and the
// dial-out client, each running one Publisher for its own server row.
//
// Three tables. Keys use the STATE_DB separator. An id is the stream's numeric
// client id for the default gNMI server, <name>_<n> for a gNMI server with
// another state name (a name may not hold the separator), and
// dialout_<name> for dial-out, where <name> is a CONFIG_DB key part; so an
// id never contains the separator.
//
//	GNMI_SERVER_STATE|<server>       one row per publishing server, gnmi or dialout: pid,
//	                                 start_time, publish_interval, last_updated,
//	                                 listen_address, vrf, unix_socket, threshold (0 means
//	                                 no limit), the live counts and lifetime totals the
//	                                 server keeps, and its failures counted by cause as
//	                                 failed_<cause>
//	GNMI_SUBSCRIPTION|<id>           one row per subscription: direction, peer,
//	                                 local_address, server, user, list_mode, prefix,
//	                                 origin, target, encoding, start_time; dial-out adds
//	                                 destination_group, state, connect_attempts,
//	                                 last_error; refreshed each tick: tx_msgs, tx_bytes,
//	                                 error_count, queue_depth, last_updated, and for
//	                                 dial-in rx_msgs and rx_bytes (a dial-out stream is
//	                                 one way)
//	GNMI_SUBSCRIPTION_PATH|<id>|<n>  one row per path, n in request order: path, mode,
//	                                 and for a STREAM list sample_interval_ns,
//	                                 heartbeat_interval_ns, suppress_redundant
//
// start_time on the server row is when Start was called on this
// Publisher, and pid the process it runs in; a producer that starts a new
// Publisher within the same process gets a new start_time and the same pid.
//
// Every write that sets a row as it should stand (a registration, the
// server row) deletes the hash and writes it again in one transaction, so a
// row never keeps a field it no longer carries. Each tick writes every
// registered row this way, its counters included, and deletes again the
// rows of a Delete that did not land, so a write that was dropped or failed
// is put right within one interval. These tables are independent of
// TELEMETRY_CONNECTIONS, which this package neither reads nor writes.
//
// Publishing is best effort and never in the path of a stream, nor of the
// caller's startup: every STATE_DB write runs on the publisher's own
// goroutine, fed over a bounded queue, and a full queue drops the event
// and logs it. (A clean stop clears on the caller's goroutine once the
// loop has ended.) A producer's rows go when it calls Delete for their id.
// Only the startup flush scans the database. The server row's
// last_updated stamp is rewritten every tick; one that stops advancing
// means this publisher is no longer writing, because the process has
// stopped, the server was stopped (a stop leaves the server row), or its
// STATE_DB writes are failing.
package substate

import (
	"context"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/golang/glog"
	"github.com/redis/go-redis/v9"
	sdc "github.com/sonic-net/sonic-gnmi/sonic_data_client"
	sdcfg "github.com/sonic-net/sonic-gnmi/sonic_db_config"
)

const (
	ServerTable       = "GNMI_SERVER_STATE"
	SubscriptionTable = "GNMI_SUBSCRIPTION"
	PathTable         = "GNMI_SUBSCRIPTION_PATH"
	// Server row names: the default gNMI server's and the dial-out client's.
	// A gNMI server can be given another name (its Config.StateName).
	ServerGNMI    = "gnmi"
	ServerDialout = "dialout"

	DefaultPublishInterval = 10 * time.Second
	MinPublishInterval     = time.Second
	// MaxPublishInterval caps a configured interval, so a mistyped value
	// cannot stop last_updated, the liveness signal, for days.
	MaxPublishInterval = time.Hour

	queueSize      = 1024
	redisOpTimeout = 2 * time.Second
	logEvery       = 100
	// A flush walks STATE_DB one SCAN page at a time, each page under its
	// own redisOpTimeout; the whole walk gives up after this long.
	flushTimeout   = 30 * time.Second
	flushDelBatch  = 500
	flushScanCount = 200
)

// Snapshot returns the fields a subscription row refreshes on every tick.
type Snapshot func() Row

// Row is one STATE_DB hash, field to value, as Redis stores it.
type Row = map[string]string

// Num formats a number for a Row.
func Num[T ~int | ~int32 | ~int64 | ~uint32 | ~uint64](v T) string { return fmt.Sprint(v) }

// entry is a registered subscription: its rows as they should stand now and
// the snapshot the tick reads. An entry never changes once it is created; a
// new registration replaces the entry. The publisher's lock guards the
// registered map.
type entry struct {
	rows map[string]Row
	snap Snapshot
}

// Publisher owns the Redis connection for one server row and is the only
// writer of that row and of the subscription rows its producer registers.
type Publisher struct {
	server   string // the producer's name: a gNMI server's state name, or dialout; not an address
	rc       *redis.Client
	sep      string
	counters func() map[string]int64 // the server row's live counts, read on every server row write
	owns     func(id string) bool    // which subscription ids are this process's

	// events holds jobs for the loop goroutine, run in order; Enqueue never
	// blocks on it.
	events          chan func()
	stop            chan struct{}
	done            chan struct{}
	intervalChanged chan struct{}

	startOnce sync.Once
	stopOnce  sync.Once
	started   atomic.Bool

	mu sync.Mutex
	// registered holds each subscription the producer has registered and
	// not deleted since, by id, with the rows written for it.
	registered map[string]*entry
	// deleting holds the keys of a Delete, or the keys a registration
	// dropped, that have not been deleted yet, each with the entry they
	// belonged to; a mark is cleared only by a delete of that same entry.
	deleting map[string]*entry

	// serverFixed holds the server row fields set once at Start: pid,
	// start_time and the producer's own (listen_address, vrf, ...). Every
	// server row write includes them.
	serverFixed Row
	intervalNs  atomic.Int64

	serverDirty atomic.Bool // a server-row write is queued; UpdateServer queues no other
	dropped     atomic.Int64
	errs        atomic.Int64
}

// DbClient returns a client for a SONiC database, and its key separator:
// on its Unix socket when one is configured, else over TCP. It does not
// probe, so a database that is down when the client is built does not
// fail it; each command reports its own error.
func DbClient(dbName string) (*redis.Client, string, error) {
	ns, err := sdcfg.GetDbDefaultNamespace()
	if err != nil {
		return nil, "", err
	}
	id, err := sdcfg.GetDbId(dbName, ns)
	if err != nil {
		return nil, "", err
	}
	sep, err := sdcfg.GetDbSeparator(dbName, ns)
	if err != nil {
		return nil, "", err
	}
	tcp := func() (*redis.Client, string, error) {
		addr, err := sdcfg.GetDbTcpAddr(dbName, ns)
		if err != nil {
			return nil, "", err
		}
		return redis.NewClient(&redis.Options{Network: "tcp", Addr: addr, DB: id, DialTimeout: redisOpTimeout, ContextTimeoutEnabled: true}), sep, nil
	}
	// Tests set UseRedisLocalTcpPort; on a switch it is false and the
	// database's Unix socket is used when the configuration names one.
	if sdc.UseRedisLocalTcpPort {
		return tcp()
	}
	if sock, err := sdcfg.GetDbSock(dbName, ns); err == nil && sock != "" {
		return redis.NewClient(&redis.Options{Network: "unix", Addr: sock, DB: id, DialTimeout: redisOpTimeout, ContextTimeoutEnabled: true}), sep, nil
	}
	return tcp()
}

// CheckServerName reports why name cannot name a gNMI server's rows: it
// falls in the dial-out client's id space (dialout, or dialout_...), it
// holds the STATE_DB key separator, or that separator cannot be read from
// the database configuration.
func CheckServerName(name string) error {
	if name == ServerDialout || strings.HasPrefix(name, ServerDialout+"_") {
		return fmt.Errorf("invalid state name %q: dialout is the dial-out client's", name)
	}
	ns, err := sdcfg.GetDbDefaultNamespace()
	if err != nil {
		return fmt.Errorf("state name %q: %v", name, err)
	}
	sep, err := sdcfg.GetDbSeparator("STATE_DB", ns)
	if err != nil {
		return fmt.Errorf("state name %q: %v", name, err)
	}
	if strings.Contains(name, sep) {
		return fmt.Errorf("invalid state name %q: it holds the STATE_DB key separator %q", name, sep)
	}
	return nil
}

// New creates a publisher for the named server row. It only opens the
// STATE_DB connection; Start takes the server row's content.
func New(server string) (*Publisher, error) {
	rc, sep, err := DbClient("STATE_DB")
	if err != nil {
		return nil, err
	}
	return NewWithClient(server, rc, sep), nil
}

// NewWithClient is New with a caller-supplied Redis client, for tests.
func NewWithClient(server string, rc *redis.Client, sep string) *Publisher {
	p := &Publisher{
		server:          server,
		rc:              rc,
		sep:             sep,
		events:          make(chan func(), queueSize),
		stop:            make(chan struct{}),
		done:            make(chan struct{}),
		intervalChanged: make(chan struct{}, 1),
		registered:      map[string]*entry{},
		deleting:        map[string]*entry{},
		serverFixed:     Row{},
	}
	p.intervalNs.Store(int64(DefaultPublishInterval))
	return p
}

// Key builds a STATE_DB key with this database's separator.
func (p *Publisher) Key(table string, parts ...string) string {
	return table + p.sep + strings.Join(parts, p.sep)
}

// ServerKey is this publisher's server row key.
func (p *Publisher) ServerKey() string { return p.Key(ServerTable, p.server) }

// Server is the server row name.
func (p *Publisher) Server() string { return p.server }

// Start records the server row's fixed fields and the function that
// returns its live counts, and starts the publisher goroutine, which
// begins with the startup sequence, ahead of anything queued before Start:
// flush the rows a previous run of this process left behind (those whose
// id satisfies owns; with no owns, nothing is flushed or scanned), and
// write the server row. Start itself does no I/O, so a slow or absent
// Redis never delays the caller. A second call does nothing, and so does a
// call on a publisher with no Redis client: it publishes nothing, and its
// queue only fills and drops.
func (p *Publisher) Start(fixed Row, counters func() map[string]int64, owns func(id string) bool) {
	p.startOnce.Do(func() {
		if p.rc == nil {
			log.Errorf("substate[%s]: no Redis client; not publishing", p.server)
			return
		}
		p.counters = counters
		p.serverFixed = Row{"pid": Num(os.Getpid()), "start_time": Num(time.Now().Unix())}
		for k, v := range fixed {
			p.serverFixed[k] = v
		}
		p.owns = owns
		p.started.Store(true)
		go p.run()
	})
}

// startup is the first thing the publisher goroutine does: it removes the
// rows a previous run of this process left, then writes the server row,
// replacing the hash so no field an earlier run wrote survives.
func (p *Publisher) startup() {
	if !p.flush(p.owns, p.stop) && !p.Stopped() {
		log.Warningf("substate[%s]: rows left by an earlier run were not all removed", p.server)
	}
	if p.Stopped() {
		return // stopped during startup: nothing more to write
	}
	ctx, cancel := p.ctx()
	defer cancel()
	pipe := p.rc.TxPipeline()
	pipe.Del(ctx, p.ServerKey())
	pipe.HSet(ctx, p.ServerKey(), p.serverRow())
	_, err := pipe.Exec(ctx)
	p.logErr("server row", err)
}

// Stop ends the publisher loop and closes its connections, and leaves
// every row in place. Safe to call twice, and safe before Start.
func (p *Publisher) Stop() { p.stopLoop(false) }

// StopAndClear is Stop for a clean shutdown: once the loop has stopped and
// nothing can write any more, it deletes the rows this publisher still
// holds, those of the subscriptions still registered and those whose
// delete has not landed, and no others, so it is safe beside other
// producers in the same tables. The server row stays.
func (p *Publisher) StopAndClear() { p.stopLoop(true) }

// stopLoop is Stop and StopAndClear. Only the first of them does anything.
func (p *Publisher) stopLoop(clear bool) {
	p.stopOnce.Do(func() {
		close(p.stop)
		if p.started.Load() {
			<-p.done
		}
		if p.rc != nil {
			if clear {
				p.clearOwn()
			}
			p.rc.Close()
		}
	})
}

// clearOwn deletes the keys of every registered subscription and of every
// delete that has not run. Only once the loop has ended.
func (p *Publisher) clearOwn() {
	p.mu.Lock()
	var keys []string
	for k := range p.deleting {
		keys = append(keys, k)
	}
	for _, e := range p.registered {
		for k := range e.rows {
			keys = append(keys, k)
		}
	}
	p.mu.Unlock()
	if len(keys) == 0 {
		return
	}
	ctx, cancel := p.ctx()
	defer cancel()
	p.logErr("clear rows", p.rc.Del(ctx, keys...).Err())
}

// Stopped reports whether the publisher has been stopped.
func (p *Publisher) Stopped() bool {
	select {
	case <-p.stop:
		return true
	default:
		return false
	}
}

// Interval is the current publish interval.
func (p *Publisher) Interval() time.Duration { return time.Duration(p.intervalNs.Load()) }

// SetInterval changes the publish interval, clamped between the floor and
// the cap; 0 or less restores the default. The timer re-arms at once.
func (p *Publisher) SetInterval(d time.Duration) {
	if d <= 0 {
		d = DefaultPublishInterval
	}
	if d < MinPublishInterval {
		d = MinPublishInterval
	}
	if d > MaxPublishInterval {
		d = MaxPublishInterval
	}
	if time.Duration(p.intervalNs.Swap(int64(d))) == d {
		return
	}
	log.V(1).Infof("substate[%s]: publish interval %v", p.server, d)
	select {
	case p.intervalChanged <- struct{}{}:
	default:
	}
}

// Enqueue hands work to the publisher goroutine without ever blocking the
// caller, and reports whether it was queued. After a stop nothing reads the
// queue, so the job is refused. A stop that clears removes the rows it
// would have touched; after a stop that does not, the next start's flush
// does. A full queue means Redis has been stuck for
// a while, which should not happen in normal operation, so every dropped
// job is logged; the next tick writes what it would have.
func (p *Publisher) Enqueue(f func()) bool {
	if p.Stopped() {
		return false
	}
	select {
	case p.events <- f:
		return true
	default:
		n := p.dropped.Add(1)
		log.Warningf("substate[%s]: publish queue full, job dropped (%d so far)", p.server, n)
		return false
	}
}

// Dropped is the number of events dropped because the queue was full.
func (p *Publisher) Dropped() int64 { return p.dropped.Load() }

func (p *Publisher) run() {
	defer close(p.done)
	p.startup()
	t := time.NewTimer(p.Interval())
	defer t.Stop()
	for {
		// A stop wins over queued jobs: they are dropped. A delete among them
		// is not lost: Delete records its keys before queuing, and
		// StopAndClear removes them once the loop has ended. After a stop that
		// does not clear, the next start's flush removes them.
		select {
		case <-p.stop:
			return
		default:
		}
		select {
		case f := <-p.events:
			f()
		case <-t.C:
			p.Tick()
			t.Reset(p.Interval())
		case <-p.intervalChanged:
			if !t.Stop() {
				select {
				case <-t.C:
				default:
				}
			}
			p.Tick()
			t.Reset(p.Interval())
		case <-p.stop:
			return
		}
	}
}

func (p *Publisher) logErr(what string, err error) {
	if err == nil {
		return
	}
	n := p.errs.Add(1)
	if n == 1 || n%logEvery == 0 {
		log.Warningf("substate[%s]: %s: %v (%d errors so far)", p.server, what, err, n)
	} else {
		log.V(2).Infof("substate[%s]: %s: %v", p.server, what, err)
	}
}

func (p *Publisher) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), redisOpTimeout)
}

// idOf returns the subscription id in a GNMI_SUBSCRIPTION or
// GNMI_SUBSCRIPTION_PATH key. Ids never contain the separator (see the
// package doc), so the second component is the whole id.
func (p *Publisher) idOf(key string) string {
	parts := strings.Split(key, p.sep)
	if len(parts) < 2 {
		return ""
	}
	return parts[1]
}

// flush deletes the subscription and path rows whose id satisfies owns, and
// reports whether it got through, giving up when stop closes. With no
// owns nothing is this process's, so it scans nothing. MATCH filters
// each SCAN page after it is fetched, so a large STATE_DB takes many pages;
// each page and DEL batch has its own timeout, the whole walk flushTimeout.
func (p *Publisher) flush(owns func(id string) bool, stop <-chan struct{}) bool {
	if owns == nil || p.rc == nil {
		return owns == nil // nothing is this process's: nothing to scan
	}
	deadline := time.Now().Add(flushTimeout)
	giveUp := func(what string) bool {
		select {
		case <-stop:
			return true
		default:
		}
		if time.Now().After(deadline) {
			p.logErr(what, fmt.Errorf("gave up after %v", flushTimeout))
			return true
		}
		return false
	}
	var stale []string
	for _, table := range []string{SubscriptionTable, PathTable} {
		var cursor uint64
		for {
			if giveUp("flush scan " + table) {
				return false
			}
			ctx, cancel := p.ctx()
			keys, next, err := p.rc.Scan(ctx, cursor, p.Key(table, "*"), flushScanCount).Result()
			cancel()
			if err != nil {
				p.logErr("flush scan "+table, err)
				return false
			}
			for _, k := range keys {
				if id := p.idOf(k); id != "" && owns(id) {
					stale = append(stale, k)
				}
			}
			if cursor = next; cursor == 0 {
				break
			}
		}
	}
	for i := 0; i < len(stale); i += flushDelBatch {
		if giveUp("flush") {
			return false
		}
		batch := stale[i:min(i+flushDelBatch, len(stale))]
		ctx, cancel := p.ctx()
		err := p.rc.Del(ctx, batch...).Err()
		cancel()
		if err != nil {
			p.logErr("flush", err)
			return false
		}
	}
	if len(stale) > 0 {
		log.V(1).Infof("substate[%s]: removed %d leftover rows", p.server, len(stale))
	}
	return true
}

func (p *Publisher) serverRow() Row {
	row := Row{}
	for k, v := range p.serverFixed {
		row[k] = v
	}
	row["publish_interval"] = Num(int64(p.Interval() / time.Second))
	if p.counters != nil {
		for k, v := range p.counters() {
			row[k] = Num(v)
		}
	}
	row["last_updated"] = Num(time.Now().Unix())
	return row
}

func (p *Publisher) writeServerRow() {
	ctx, cancel := p.ctx()
	defer cancel()
	p.logErr("server row", p.rc.HSet(ctx, p.ServerKey(), p.serverRow()).Err())
}

// UpdateServer rewrites the server row's live fields. Call it after a
// counter the row shows has changed on an event, so the change is visible
// before the next tick.
func (p *Publisher) UpdateServer() {
	// At most one write is queued: it reads the counts when it runs, so
	// later events are covered by it, and a burst of connection events or
	// rejections cannot fill the queue that subscription rows share.
	if !p.serverDirty.CompareAndSwap(false, true) {
		return
	}
	if !p.Enqueue(func() {
		p.serverDirty.Store(false)
		p.writeServerRow()
	}) {
		p.serverDirty.Store(false)
	}
}

func copyRows(rows map[string]Row) map[string]Row {
	out := make(map[string]Row, len(rows))
	for k, fields := range rows {
		out[k] = maps.Clone(fields)
	}
	return out
}

// WriteRows registers subscription id with its rows and the snapshot the
// tick refreshes it from, then writes the rows, each replacing its hash, in
// one transaction; the server row follows through a queued, coalesced
// server-row write. A key the id's previous
// registration had and this one does not is deleted in the same write.
// The write reads the entry when it runs: it does nothing if the id was
// deleted or registered again since, and otherwise writes the rows as
// they stand then, updates included. An empty Row is skipped: Redis
// refuses an HSET with no fields.
func (p *Publisher) WriteRows(id string, rows map[string]Row, snap Snapshot) {
	rows = maps.Clone(rows)
	maps.DeleteFunc(rows, func(_ string, r Row) bool { return len(r) == 0 })
	e := &entry{rows: copyRows(rows), snap: snap}
	var dropped []string
	p.mu.Lock()
	old := p.registered[id]
	if old != nil {
		for k := range old.rows {
			if _, kept := rows[k]; !kept {
				dropped = append(dropped, k)
				p.deleting[k] = old
			}
		}
	}
	for k := range rows {
		delete(p.deleting, k) // registered again: the tick writes it, not deletes it
	}
	p.registered[id] = e
	p.mu.Unlock()
	p.Enqueue(func() { p.writeEntry(id, e, dropped, old) })
}

// writeEntry writes a registration's rows as they stand now and the delete
// of the keys it dropped from old in one transaction, then queues a
// server-row write.
func (p *Publisher) writeEntry(id string, e *entry, dropped []string, old *entry) {
	p.mu.Lock()
	if p.registered[id] != e {
		p.mu.Unlock()
		return // deleted, or registered again, since this write was queued
	}
	rows := copyRows(e.rows)
	p.mu.Unlock()
	ctx, cancel := p.ctx()
	defer cancel()
	pipe := p.rc.TxPipeline()
	for k, fields := range rows {
		pipe.Del(ctx, k)
		pipe.HSet(ctx, k, fields)
	}
	if len(dropped) > 0 {
		pipe.Del(ctx, dropped...)
	}
	_, err := pipe.Exec(ctx)
	p.logErr("write rows", err)
	p.UpdateServer() // coalesced: one queued server-row write covers a burst of jobs
	if err == nil {
		p.unmark(dropped, old)
	}
}

// unmark clears the deleting marks of keys that still belong to e.
func (p *Publisher) unmark(keys []string, e *entry) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, k := range keys {
		if p.deleting[k] == e {
			delete(p.deleting, k)
		}
	}
}

// Delete unregisters subscription id at once, then deletes the rows it was
// registered with and queues a server-row write.
// Until the delete runs its keys are kept aside, so a clean stop that
// comes first still clears them.
func (p *Publisher) Delete(id string) {
	p.mu.Lock()
	e, ok := p.registered[id]
	delete(p.registered, id)
	var keys []string
	if ok {
		for k := range e.rows {
			keys = append(keys, k)
			p.deleting[k] = e
		}
	}
	p.mu.Unlock()
	if !ok {
		return
	}
	p.Enqueue(func() {
		ctx, cancel := p.ctx()
		defer cancel()
		pipe := p.rc.Pipeline()
		del := pipe.Del(ctx, keys...)
		_, err := pipe.Exec(ctx)
		p.logErr("delete rows", err)
		p.UpdateServer() // coalesced: one queued server-row write covers a burst of jobs
		if del.Err() != nil {
			return // still marked: the tick or a clean stop deletes them
		}
		p.unmark(keys, e)
	})
}

// Tick writes every registered row in full, the subscription row with its
// counters, deletes again the keys of a delete that has not landed, and
// writes the server row, in one transaction. It runs on the publisher
// goroutine, so it is ordered with the queued writes and deletes.
func (p *Publisher) Tick() {
	type item struct {
		rows map[string]Row
		sub  string // the subscription row, which gets the counters
		snap Snapshot
	}
	p.mu.Lock()
	items := make([]item, 0, len(p.registered))
	for id, e := range p.registered {
		items = append(items, item{rows: copyRows(e.rows), sub: p.Key(SubscriptionTable, id), snap: e.snap})
	}
	pending := maps.Clone(p.deleting) // never a registered key: WriteRows clears those
	p.mu.Unlock()

	ctx, cancel := p.ctx()
	defer cancel()
	pipe := p.rc.TxPipeline()
	now := Num(time.Now().Unix())
	for _, it := range items {
		if row := it.rows[it.sub]; row != nil && it.snap != nil {
			if fields := it.snap(); fields != nil {
				maps.Copy(row, fields)
				row["last_updated"] = now
			}
		}
		for k, fields := range it.rows {
			pipe.Del(ctx, k)
			pipe.HSet(ctx, k, fields)
		}
	}
	if len(pending) > 0 {
		pipe.Del(ctx, slices.Collect(maps.Keys(pending))...)
	}
	pipe.Del(ctx, p.ServerKey())
	pipe.HSet(ctx, p.ServerKey(), p.serverRow())
	_, err := pipe.Exec(ctx)
	p.logErr("tick", err)
	if err != nil || len(pending) == 0 {
		return
	}
	p.mu.Lock()
	for k, e := range pending {
		if p.deleting[k] == e {
			delete(p.deleting, k)
		}
	}
	p.mu.Unlock()
}
