// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package substate

import (
	"strconv"
)

// PathRow is a GNMI_SUBSCRIPTION_PATH row. A path in a STREAM list has a
// mode and its intervals as the collector sent them, where 0 means not set.
// A POLL or ONCE list has no per-path mode or interval at all, so its row
// carries the path and an empty mode only.
func PathRow(path, mode string, sampleNs, heartbeatNs uint64, suppress bool) Row {
	if mode == "" {
		return Row{"path": path, "mode": ""}
	}
	return Row{
		"path":                  path,
		"mode":                  mode,
		"sample_interval_ns":    Num(sampleNs),
		"heartbeat_interval_ns": Num(heartbeatNs),
		"suppress_redundant":    strconv.FormatBool(suppress),
	}
}
