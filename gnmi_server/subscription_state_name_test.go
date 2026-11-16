// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package gnmi

import (
	"testing"

	"github.com/sonic-net/sonic-gnmi/substate"
)

// Two servers that publish at the same time use different names, so their
// subscription ids never meet.
func TestSubscriptionStateNames(t *testing.T) {
	if got := stateIDFor(substate.ServerGNMI, 3); got != "3" {
		t.Errorf("default id = %q, want 3", got)
	}
	if got := stateIDFor("telemetry", 3); got != "telemetry_3" {
		t.Errorf("named id = %q, want telemetry_3", got)
	}

	for _, c := range []struct {
		name, id string
		want     bool
	}{
		{substate.ServerGNMI, "3", true},
		{substate.ServerGNMI, "telemetry_3", false},
		{substate.ServerGNMI, "dialout_S1", false},
		{"telemetry", "telemetry_3", true},
		{"telemetry", "3", false},
		{"telemetry", "telemetry_x", false},
		{"telemetry", "dialout_S1", false},
	} {
		if got := ownsIDFor(c.name, c.id); got != c.want {
			t.Errorf("ownsIDFor(%q, %q) = %v, want %v", c.name, c.id, got, c.want)
		}
	}
}
