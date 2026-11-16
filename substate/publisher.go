// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package substate publishes gNMI subscription state to STATE_DB, so an
// operator can see from the switch which collectors hold subscriptions,
// what they subscribed to and whether the switch is keeping up. The
// design is the gNMI Subscription State HLD under doc/mgmt/gnmi in the
// SONiC design repository.
//
// It is written for two producers, the gNMI server (dial-in) and the
// dial-out client, each running one Publisher.
//
// One table. Keys use the STATE_DB separator. An id is the stream's numeric
// client id for the default gNMI server, <name>_<n> for a gNMI server with
// another state name (a name may not hold the separator), and
// dialout_<name> for dial-out, where <name> is a CONFIG_DB key part; so an
// id never contains the separator.
//
//	GNMI_SUBSCRIPTION|<id>  one row per subscription: direction, peer,
//	                        local_address, server, list_mode, prefix,
//	                        origin, target, encoding, start_time
//
// A registration deletes each hash and writes it again in one
// transaction, so a row never keeps a field it no longer carries. These
// tables are independent of TELEMETRY_CONNECTIONS, which this package
// neither reads nor writes.
//
// Publishing is best effort and never in the path of a stream, nor of the
// caller's startup: every STATE_DB write runs on the publisher's own
// goroutine, fed over a bounded queue, and a full queue drops the event
// and logs it. A producer's rows go when it calls Delete for
// their id.
package substate

import (
	"context"
	"fmt"
	"maps"
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
	SubscriptionTable = "GNMI_SUBSCRIPTION"
	// Publisher names: the default gNMI server's and the dial-out client's.
	// A gNMI server can be given another name (its Config.StateName).
	ServerGNMI    = "gnmi"
	ServerDialout = "dialout"

	queueSize      = 1024
	redisOpTimeout = 2 * time.Second
	logEvery       = 100
)

// Row is one STATE_DB hash, field to value, as Redis stores it.
type Row = map[string]string

// Num formats a number for a Row.
func Num[T ~int | ~int32 | ~int64 | ~uint32 | ~uint64](v T) string { return fmt.Sprint(v) }

// entry is a registered subscription: its rows as they should stand now.
// An entry's rows never change once it is created; a new registration
// replaces the entry. The publisher's lock guards the registered map.
type entry struct {
	rows map[string]Row
}

// Publisher owns the Redis connection for one producer and is the only
// writer of the subscription rows that producer registers.
type Publisher struct {
	server string // the producer's name: a gNMI server's state name, or dialout; not an address
	rc     *redis.Client
	sep    string

	// events holds jobs for the loop goroutine, run in order; Enqueue never
	// blocks on it.
	events chan func()
	stop   chan struct{}
	done   chan struct{}

	startOnce sync.Once
	stopOnce  sync.Once
	started   atomic.Bool

	mu sync.Mutex
	// registered holds each subscription the producer has registered and
	// not deleted since, by id, with the rows written for it.
	registered map[string]*entry

	dropped atomic.Int64
	errs    atomic.Int64
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

// New creates a publisher for the named producer, on STATE_DB.
func New(server string) (*Publisher, error) {
	rc, sep, err := DbClient("STATE_DB")
	if err != nil {
		return nil, err
	}
	return NewWithClient(server, rc, sep), nil
}

// NewWithClient is New with a caller-supplied Redis client, for tests.
func NewWithClient(server string, rc *redis.Client, sep string) *Publisher {
	return &Publisher{
		server:     server,
		rc:         rc,
		sep:        sep,
		events:     make(chan func(), queueSize),
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
		registered: map[string]*entry{},
	}
}

// Key builds a STATE_DB key with this database's separator.
func (p *Publisher) Key(table string, parts ...string) string {
	return table + p.sep + strings.Join(parts, p.sep)
}

// Server is the producer's name.
func (p *Publisher) Server() string { return p.server }

// Start starts the publisher goroutine. Start itself does no I/O, so a
// slow or absent Redis never delays the caller. A second call does
// nothing, and so does a call on a publisher with no Redis client: it
// publishes nothing, and its queue only fills and drops.
func (p *Publisher) Start() {
	p.startOnce.Do(func() {
		if p.rc == nil {
			log.Errorf("substate[%s]: no Redis client; not publishing", p.server)
			return
		}
		p.started.Store(true)
		go p.run()
	})
}

// Stop ends the publisher loop, once any job it is running has finished,
// and closes its connection. Jobs still queued are dropped, not run. Safe
// to call twice, and safe before Start.
func (p *Publisher) Stop() {
	p.stopOnce.Do(func() {
		close(p.stop)
		if p.started.Load() {
			<-p.done
		}
		if p.rc != nil {
			p.rc.Close()
		}
	})
}

// Stopped reports whether Stop has run.
func (p *Publisher) Stopped() bool {
	select {
	case <-p.stop:
		return true
	default:
		return false
	}
}

// Enqueue hands work to the publisher goroutine without ever blocking the
// caller, and reports whether it was queued. After a stop nothing reads the
// queue, so the job is refused. A full queue means Redis has been stuck for
// a while, which should not happen in normal operation, so every dropped
// job is logged.
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
	for {
		// A stop wins over queued jobs: Stop drops them.
		select {
		case <-p.stop:
			return
		default:
		}
		select {
		case f := <-p.events:
			f()
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

func copyRows(rows map[string]Row) map[string]Row {
	out := make(map[string]Row, len(rows))
	for k, fields := range rows {
		out[k] = maps.Clone(fields)
	}
	return out
}

// WriteRows registers subscription id with its rows, then writes the rows,
// each replacing its hash, in one transaction. A key the id's previous
// registration had and this one does not is deleted in the same write.
// The write reads the entry when it runs: it does nothing if the id was
// deleted or registered again since.
func (p *Publisher) WriteRows(id string, rows map[string]Row) {
	e := &entry{rows: copyRows(rows)}
	var dropped []string
	p.mu.Lock()
	if old := p.registered[id]; old != nil {
		for k := range old.rows {
			if _, kept := rows[k]; !kept {
				dropped = append(dropped, k)
			}
		}
	}
	p.registered[id] = e
	p.mu.Unlock()
	p.Enqueue(func() { p.writeEntry(id, e, dropped) })
}

// writeEntry writes a registration's rows as they stand now and the delete
// of the keys it dropped, in one transaction.
func (p *Publisher) writeEntry(id string, e *entry, dropped []string) {
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
}

// Delete unregisters subscription id at once, then deletes the rows it was
// registered with.
func (p *Publisher) Delete(id string) {
	p.mu.Lock()
	e, ok := p.registered[id]
	delete(p.registered, id)
	p.mu.Unlock()
	if !ok {
		return
	}
	keys := make([]string, 0, len(e.rows))
	for k := range e.rows {
		keys = append(keys, k)
	}
	p.Enqueue(func() {
		ctx, cancel := p.ctx()
		defer cancel()
		p.logErr("delete rows", p.rc.Del(ctx, keys...).Err())
	})
}
