// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package substate

import (
	"testing"
)

func TestPathRow(t *testing.T) {
	stream := PathRow("COUNTERS_DB/COUNTERS/Ethernet0", "SAMPLE", 1e9, 30e9, true)
	if len(stream) != 5 || stream["mode"] != "SAMPLE" || stream["sample_interval_ns"] != "1000000000" || stream["heartbeat_interval_ns"] != "30000000000" || stream["suppress_redundant"] != "true" {
		t.Errorf("STREAM path row = %v", stream)
	}
	poll := PathRow("COUNTERS_DB/COUNTERS/Ethernet0", "", 1e9, 30e9, true)
	if len(poll) != 2 || poll["path"] != "COUNTERS_DB/COUNTERS/Ethernet0" || poll["mode"] != "" {
		t.Errorf("non-STREAM path row = %v, want path and an empty mode only", poll)
	}
}
