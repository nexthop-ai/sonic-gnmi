// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package gnmi

import (
	"context"
	"net"
	"testing"
	"time"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
	sdc "github.com/sonic-net/sonic-gnmi/sonic_data_client"
)

// pollStream is a Subscribe stream whose collector sends Poll requests
// as fast as the server reads them.
type pollStream struct {
	gnmipb.GNMI_SubscribeServer
	ctx context.Context
}

func (s *pollStream) Recv() (*gnmipb.SubscribeRequest, error) {
	return &gnmipb.SubscribeRequest{Request: &gnmipb.SubscribeRequest_Poll{Poll: &gnmipb.Poll{}}}, nil
}

func (s *pollStream) Context() context.Context { return s.ctx }

// A Close from the send side while recv is handing a Poll request to the
// data client must not panic with a send on the closed polled channel, and
// recv must return once the client is closing.
func TestClientPollRequestDuringClose(t *testing.T) {
	for i := 0; i < 200; i++ {
		c := NewClient(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1})
		c.subscribe = &gnmipb.SubscriptionList{Mode: gnmipb.SubscriptionList_POLL}
		c.polled = make(chan struct{}, 1)
		// The data client's PollRun: it reads polled until it is closed.
		go func(polled chan struct{}) {
			for range polled {
			}
		}(c.polled)
		done := make(chan struct{})
		go func() {
			c.recv(&pollStream{ctx: context.Background()})
			close(done)
		}()
		time.Sleep(time.Duration(i%5) * 100 * time.Microsecond)
		c.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("recv did not return after Close (iteration %d)", i)
		}
	}
}

// stalledPollStream hands recv the Poll requests the test feeds it, then
// blocks like a collector that has gone quiet.
type stalledPollStream struct {
	gnmipb.GNMI_SubscribeServer
	ctx   context.Context
	polls chan struct{}
}

func (s *stalledPollStream) Recv() (*gnmipb.SubscribeRequest, error) {
	select {
	case <-s.polls:
		return &gnmipb.SubscribeRequest{Request: &gnmipb.SubscribeRequest_Poll{Poll: &gnmipb.Poll{}}}, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *stalledPollStream) Context() context.Context { return s.ctx }

// startStalledPoll returns a POLL client whose data client read one Poll
// request and then stopped reading, with recv blocked handing it a third.
func startStalledPoll(t *testing.T) (c *Client, recvDone chan struct{}) {
	c = NewClient(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1})
	c.subscribe = &gnmipb.SubscriptionList{Mode: gnmipb.SubscriptionList_POLL}
	c.polled = make(chan struct{}, 1)
	// The data client's PollRun: it takes one Poll request and returns,
	// without reading polled again, closing it or queueing a Fatal.
	read := make(chan struct{})
	go func(polled chan struct{}) {
		<-polled
		close(read)
	}(c.polled)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream := &stalledPollStream{ctx: ctx, polls: make(chan struct{})}
	recvDone = make(chan struct{})
	go func() {
		c.recv(stream)
		close(recvDone)
	}()

	stream.polls <- struct{}{}
	<-read
	// The second request fills polled; the third blocks recv on it.
	stream.polls <- struct{}{}
	stream.polls <- struct{}{}
	time.Sleep(20 * time.Millisecond)
	return c, recvDone
}

func waitOrFatal(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not return", what)
	}
}

// Close must not wait forever on a recv that is blocked on polled because
// the data client stopped reading it.
func TestClientCloseWithStalledPollReader(t *testing.T) {
	c, recvDone := startStalledPoll(t)
	closed := make(chan struct{})
	go func() {
		c.Close()
		close(closed)
	}()
	waitOrFatal(t, closed, "Close")
	waitOrFatal(t, recvDone, "recv")
}

// Abort must queue its Fatal while recv is blocked on polled; the server
// calls it for every client with its client map locked.
func TestClientAbortWithStalledPollReader(t *testing.T) {
	c, recvDone := startStalledPoll(t)
	aborted := make(chan struct{})
	go func() {
		c.Abort(ConfigReloadRequested)
		close(aborted)
	}()
	waitOrFatal(t, aborted, "Abort")

	items, err := c.q.Get(1)
	if err != nil {
		t.Fatalf("no Fatal queued by Abort: %v", err)
	}
	if v, ok := items[0].(sdc.Value); !ok || v.GetFatal() != ConfigReloadRequested {
		t.Fatalf("Abort queued %v, want Fatal %q", items[0], ConfigReloadRequested)
	}

	// Run calls Close once send returns on that Fatal.
	c.Close()
	waitOrFatal(t, recvDone, "recv")
}

// Abort's check of the queue and its Put must not interleave with Close's
// Dispose, or the Fatal is dropped and Run returns InvalidArgument instead
// of Unavailable. Abort therefore waits while Close holds mu.
func TestClientAbortWaitsForClose(t *testing.T) {
	c := NewClient(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1})
	c.mu.Lock() // Close is between its Disposed check and Dispose
	aborted := make(chan struct{})
	go func() {
		c.Abort(ConfigReloadRequested)
		close(aborted)
	}()
	select {
	case <-aborted:
		c.mu.Unlock()
		t.Fatal("Abort returned while Close held mu")
	case <-time.After(100 * time.Millisecond):
	}
	c.q.Dispose()
	c.mu.Unlock()
	waitOrFatal(t, aborted, "Abort")
}

// Close works on a Client that NewClient did not build, as it did before
// closing was added.
func TestClientCloseWithoutClosingChannel(t *testing.T) {
	c := &Client{addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}}
	c.Close()
}
