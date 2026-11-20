// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package telemetry_dialout

import (
	"context"
	"errors"
	"testing"
)

// A retry that fails the same way as the last one changes nothing on the
// row, so it must not be published.
func TestBecomeDisconnectedRepeat(t *testing.T) {
	cs := &clientSubscription{entryState: &entryState{}}
	cs.revive()
	err := errors.New("dial tcp: connection refused")
	if live, changed := cs.becomeDisconnected(err); !live || !changed {
		t.Fatalf("first failure: live %v, changed %v; want true, true", live, changed)
	}
	if live, changed := cs.becomeDisconnected(err); !live || changed {
		t.Fatalf("same failure again: live %v, changed %v; want true, false", live, changed)
	}
	if live, changed := cs.becomeDisconnected(errors.New("other")); !live || !changed {
		t.Fatalf("different error: live %v, changed %v; want true, true", live, changed)
	}
	if !cs.becomeConnected() {
		t.Fatalf("becomeConnected after a failure reported no change")
	}
	if cs.becomeConnected() {
		t.Fatalf("becomeConnected on a connected instance reported a change")
	}
	if live, changed := cs.becomeDisconnected(err); !live || !changed {
		t.Fatalf("failure after connected: live %v, changed %v; want true, true", live, changed)
	}
	cs.retireNow()
	if live, changed := cs.becomeDisconnected(err); live || changed {
		t.Fatalf("retired instance: live %v, changed %v; want false, false", live, changed)
	}
	if cs.becomeConnected() {
		t.Fatalf("becomeConnected on a retired instance reported a change")
	}
}

// A source that fails while the row is already connecting leaves it
// connecting, and records the error for the next tick to show.
func TestStateOnSourceDownWhileConnecting(t *testing.T) {
	cs := &clientSubscription{entryState: &entryState{}}
	cs.revive()
	stateOnSourceDown(context.Background(), cs, errors.New("source down"))
	cs.connMu.Lock()
	state, lastErr := cs.state, cs.lastErr
	cs.connMu.Unlock()
	if state != stateConnecting || lastErr != "source down" {
		t.Errorf("state %q, last_error %q; want connecting with the source error", state, lastErr)
	}
	// The outage is one error, however many reads fail while it lasts.
	stateOnSourceDown(context.Background(), cs, errors.New("source down"))
	stateOnSourceDown(context.Background(), cs, errors.New("source still down"))
	if n := cs.errors.Load(); n != 1 {
		t.Errorf("error_count = %d over one outage, want 1", n)
	}
	if row := cs.connectionRow(); row["last_error"] != "source still down" {
		t.Errorf("last_error = %q, want the latest failure's text", row["last_error"])
	}
}

// A read that fails because the instance is stopping changes nothing: it
// is not counted, does not mark an outage, and leaves the row alone.
func TestStateOnSourceDownWhileStopping(t *testing.T) {
	cs := &clientSubscription{entryState: &entryState{}}
	cs.revive()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stateOnSourceDown(ctx, cs, errors.New("source down"))
	cs.connMu.Lock()
	lastErr := cs.lastErr
	cs.connMu.Unlock()
	if cs.errors.Load() != 0 || cs.sourceDown.Load() || lastErr != "" {
		t.Errorf("after a stop: error_count %d, sourceDown %v, last_error %q; want nothing", cs.errors.Load(), cs.sourceDown.Load(), lastErr)
	}
}

// The row shows the local address only while it says connected, so no path
// can publish connecting with an address.
func TestLocalAddressShownOnlyWhileConnected(t *testing.T) {
	cs := &clientSubscription{entryState: &entryState{}}
	cs.revive()
	cs.setLocal("10.0.0.1:5000") // set by the dial's stats callback
	cs.becomeDisconnected(errors.New("dial tcp: connection refused"))
	if row := cs.connectionRow(); row["state"] != stateConnecting || row["local_address"] != "" {
		t.Errorf("connection row = %v, want connecting with no local_address", row)
	}
	// A data source failure leaves the connection up, so the address is
	// back on the row when the next good read reconnects it.
	cs.becomeConnected()
	cs.becomeDisconnected(errors.New("source down"))
	if row := cs.connectionRow(); row["state"] != stateConnecting || row["local_address"] != "" {
		t.Errorf("connection row with the source down = %v, want connecting with no local_address", row)
	}
	cs.becomeConnected()
	if row := cs.connectionRow(); row["state"] != stateConnected || row["local_address"] != "10.0.0.1:5000" {
		t.Errorf("connection row after the source is back = %v, want connected with the connection's address", row)
	}
}
