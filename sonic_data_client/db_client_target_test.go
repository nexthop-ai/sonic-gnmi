// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"testing"

	sdcfg "github.com/sonic-net/sonic-gnmi/sonic_db_config"
)

// IsTargetDb: the name must be a database of the Target enum and at most
// one slash is allowed. On a multi-namespace database configuration a
// target with a slash selects the namespace it carries, even an empty one;
// on a single-namespace one the suffix is ignored and the default
// namespace is returned without the namespace flag.
func TestIsTargetDb(t *testing.T) {
	defaultNs, _ := sdcfg.GetDbDefaultNamespace()
	multi, _ := sdcfg.CheckDbMultiNamespace()
	type want struct {
		db    string
		ok    bool
		ns    string
		nsSet bool
	}
	pick := func(single, multiNs want) want {
		if multi {
			return multiNs
		}
		return single
	}
	cases := []struct {
		target string
		want   want
	}{
		{"COUNTERS_DB", want{"COUNTERS_DB", true, defaultNs, false}},
		{"COUNTERS_DB/asic0", pick(want{"COUNTERS_DB", true, defaultNs, false}, want{"COUNTERS_DB", true, "asic0", true})},
		{"COUNTERS_DB/", pick(want{"COUNTERS_DB", true, defaultNs, false}, want{"COUNTERS_DB", true, "", true})},
		{"COUNTERS_DB/asic0/extra", want{"COUNTERS_DB", false, defaultNs, false}},
		{"OC_YANG", want{"OC_YANG", false, defaultNs, false}},
		{"", want{"", false, defaultNs, false}},
	}
	for _, tc := range cases {
		db, ok, ns, nsSet := IsTargetDb(tc.target)
		got := want{db, ok, ns, nsSet}
		if got != tc.want {
			t.Errorf("IsTargetDb(%q) (multi-namespace=%v) = %+v, want %+v", tc.target, multi, got, tc.want)
		}
	}
}
