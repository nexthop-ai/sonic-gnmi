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
}
