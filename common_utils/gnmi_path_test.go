// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package common_utils

import (
	"testing"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
)

func TestResolveBackend(t *testing.T) {
	for _, c := range []struct {
		origin, target string
		stream         bool
		want           Backend
	}{
		// origin decides first
		{"openconfig", "", true, BackendYANG}, {"openconfig", "COUNTERS_DB", true, BackendYANG},
		{"sonic-db", "", true, BackendNativeDB}, {"sonic-db", "APPL_DB", true, BackendNativeDB},
		{"foo", "", true, RejectOrigin}, {"OpenConfig", "", true, RejectOrigin}, // no case folding
		// then the target
		{"", "", true, RejectEmptyTarget},
		{"", "OTHERS", true, BackendHost}, {"", "OTHERS", false, BackendHost},
		{"", "SHOW", true, RejectShow},
		{"", "EVENTS", true, BackendEvents}, {"", "EVENTS", false, BackendYANG}, // served only in STREAM mode
		{"", "COUNTERS_DB", true, BackendDB}, {"", "COUNTERS_DB/asic0", false, BackendDB},
		{"", "COUNTERS_DB/asic0/extra", true, BackendYANG}, // not a database target
		{"", "counters_db", true, BackendYANG},             // database names are exact
		{"", TargetOCYANG, true, BackendYANG}, {"", "OC-YANG", true, BackendYANG}, {"", "oc_yang", false, BackendYANG},
	} {
		if got := ResolveBackend(c.origin, c.target, c.stream); got != c.want {
			t.Errorf("ResolveBackend(%q, %q, stream=%v) = %v, want %v", c.origin, c.target, c.stream, got, c.want)
		}
	}
	for _, b := range []Backend{BackendYANG, BackendNativeDB, BackendHost, BackendEvents, BackendDB} {
		if b.Rejected() {
			t.Errorf("%v reports rejected", b)
		}
	}
	for _, b := range []Backend{RejectOrigin, RejectEmptyTarget, RejectShow} {
		if !b.Rejected() {
			t.Errorf("%v does not report rejected", b)
		}
	}
}

func TestSplitDbTarget(t *testing.T) {
	for _, c := range []struct {
		target, db, ns string
		ok             bool
	}{
		{"COUNTERS_DB", "COUNTERS_DB", "", true},
		{"COUNTERS_DB/asic0", "COUNTERS_DB", "asic0", true},
		{"COUNTERS_DB/asic0/extra", "COUNTERS_DB", "", false},
		{"OTHERS", "OTHERS", "", true}, // OTHERS is in the Target enum, which is why the dispatch checks it first
		{"OC-YANG", "OC-YANG", "", false},
		{"", "", "", false},
	} {
		db, ns, ok := SplitDbTarget(c.target)
		if db != c.db || ns != c.ns || ok != c.ok {
			t.Errorf("SplitDbTarget(%q) = (%q, %q, %v), want (%q, %q, %v)", c.target, db, ns, ok, c.db, c.ns, c.ok)
		}
	}
}

func TestRenderPath(t *testing.T) {
	oc := &gnmipb.Path{Elem: []*gnmipb.PathElem{{Name: "interfaces"}, {Name: "interface", Key: map[string]string{"name": "*"}}}}
	counters := &gnmipb.Path{Elem: []*gnmipb.PathElem{{Name: "state"}, {Name: "counters"}}}
	db := &gnmipb.Path{Elem: []*gnmipb.PathElem{{Name: "COUNTERS"}, {Name: "Ethernet*"}}}
	dbPrefix := &gnmipb.Path{Target: "COUNTERS_DB", Elem: []*gnmipb.PathElem{{Name: "COUNTERS"}}}
	eth := &gnmipb.Path{Elem: []*gnmipb.PathElem{{Name: "Ethernet*"}}}
	native := &gnmipb.Path{Elem: []*gnmipb.PathElem{{Name: "CONFIG_DB"}, {Name: "localhost"}, {Name: "PORT"}}}
	events := &gnmipb.Path{Elem: []*gnmipb.PathElem{{Name: "all"}}}
	for _, c := range []struct {
		name         string
		prefix, path *gnmipb.Path
		target       string
		backend      Backend
		want         string
	}{
		{"YANG tree", oc, counters, TargetOCYANG, BackendYANG, "/interfaces/interface[name=*]/state/counters"},
		{"YANG tree by another spelling", nil, oc, "OC-YANG", BackendYANG, "/interfaces/interface[name=*]"},
		{"database target on the prefix", &gnmipb.Path{Target: "COUNTERS_DB"}, db, "COUNTERS_DB", BackendDB, "COUNTERS_DB/COUNTERS/Ethernet*"},
		{"prefix and path elements with a database target", dbPrefix, eth, "COUNTERS_DB", BackendDB, "COUNTERS_DB/COUNTERS/Ethernet*"},
		{"namespace-suffixed database target", nil, db, "COUNTERS_DB/asic0", BackendDB, "COUNTERS_DB/asic0/COUNTERS/Ethernet*"},
		{"native database: database is the first element, prefix target ignored", &gnmipb.Path{Target: "APPL_DB"}, native, "APPL_DB", BackendNativeDB, "/CONFIG_DB/localhost/PORT"},
		{"host target", nil, &gnmipb.Path{Elem: []*gnmipb.PathElem{{Name: "proc"}}}, "OTHERS", BackendHost, "OTHERS/proc"},
		{"events in STREAM mode", nil, events, "EVENTS", BackendEvents, "EVENTS/all"},
		{"events in another mode go to the YANG tree", nil, events, "EVENTS", BackendYANG, "/all"},
		{"root of the YANG tree", nil, nil, TargetOCYANG, BackendYANG, "/"},
		{"root of a database", nil, nil, "COUNTERS_DB", BackendDB, "COUNTERS_DB/"},
		{"rejected: SHOW", nil, oc, "SHOW", RejectShow, "/interfaces/interface[name=*]"},
		{"rejected: no origin and no target", nil, oc, "", RejectEmptyTarget, "/interfaces/interface[name=*]"},
		{"rejected: unknown origin", nil, oc, "", RejectOrigin, "/interfaces/interface[name=*]"},
	} {
		if got := RenderPath(c.prefix, c.path, c.target, c.backend); got != c.want {
			t.Errorf("%s: RenderPath(target %q, %v) = %q, want %q", c.name, c.target, c.backend, got, c.want)
		}
	}
}

func TestElemsString(t *testing.T) {
	// Keys render in sorted order, every time.
	p := &gnmipb.Path{Elem: []*gnmipb.PathElem{{Name: "x", Key: map[string]string{"b": "2", "a": "1", "c": "3"}}}}
	for i := 0; i < 20; i++ {
		if got := ElemsString(p); got != "/x[a=1][b=2][c=3]" {
			t.Fatalf("ElemsString = %q", got)
		}
	}
	if got := ElemsString(&gnmipb.Path{Elem: []*gnmipb.PathElem{{Name: "a"}, {Name: "b"}}}); got != "/a/b" {
		t.Errorf("keyless = %q", got)
	}
	// A path with no elements renders empty; the root form is AppendPath's.
	if ElemsString(nil) != "" || ElemsString(&gnmipb.Path{Target: "COUNTERS_DB"}) != "" {
		t.Errorf("elementless path renders as %q", ElemsString(nil))
	}
	// The deprecated Element field is ignored, like the data clients do.
	if got := ElemsString(&gnmipb.Path{Elem: []*gnmipb.PathElem{{Name: "a"}}, Element: []string{"b"}}); got != "/a" {
		t.Errorf("Element form rendered: %q", got)
	}
}

func TestAppendPath(t *testing.T) {
	eth := &gnmipb.Path{Elem: []*gnmipb.PathElem{{Name: "Ethernet0"}}}
	for _, c := range []struct {
		prefix string
		path   *gnmipb.Path
		want   string
	}{
		{"", nil, "/"},
		{"", &gnmipb.Path{}, "/"},
		{"/COUNTERS", eth, "/COUNTERS/Ethernet0"},
		{"", eth, "/Ethernet0"},
		{"/COUNTERS", nil, "/COUNTERS"},
	} {
		if got := AppendPath(c.prefix, c.path); got != c.want {
			t.Errorf("AppendPath(%q, %v) = %q, want %q", c.prefix, c.path, got, c.want)
		}
	}
}
