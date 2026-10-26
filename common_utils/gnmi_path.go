// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package common_utils

import (
	"maps"
	"slices"
	"strings"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
	spb "github.com/sonic-net/sonic-gnmi/proto"
)

// TargetOCYANG is the YANG-tree target spelling the dial-out client
// requires.
const TargetOCYANG = "OC_YANG"

// Backend is the data source the Subscribe dispatch selects for a request,
// or the reason it rejects one.
type Backend int

const (
	// BackendYANG is the YANG tree through Translib: origin openconfig, or
	// no origin and a target that is not a host or database name.
	BackendYANG Backend = iota
	// BackendNativeDB is the native database client, origin sonic-db; the
	// database is the first path element and the prefix target is ignored.
	BackendNativeDB
	// BackendHost is the host client, target OTHERS.
	BackendHost
	// BackendEvents is the event client, target EVENTS in STREAM mode.
	BackendEvents
	// BackendDB is a database client, target DBNAME or DBNAME/namespace.
	BackendDB
	// RejectOrigin is an origin other than openconfig or sonic-db.
	RejectOrigin
	// RejectEmptyTarget is no origin and no target.
	RejectEmptyTarget
	// RejectShow is target SHOW, which Subscribe does not serve.
	RejectShow
)

// Rejected reports whether the dispatch refuses the request.
func (b Backend) Rejected() bool {
	return b == RejectOrigin || b == RejectEmptyTarget || b == RejectShow
}

// IsNativeOrigin reports whether origin names the native database tree,
// where the database is the first path element.
func IsNativeOrigin(origin string) bool { return origin == "sonic-db" }

// Prefixed reports whether a rendered path names the target first: a host,
// event or database backend, where the path is relative to the target.
func (b Backend) Prefixed() bool {
	return b == BackendHost || b == BackendEvents || b == BackendDB
}

// ResolveBackend is the Subscribe dispatch's choice of backend for a
// request. origin is the resolved origin: the prefix's, or else the one all
// paths share. stream says whether the subscription list is in STREAM mode,
// the only mode that serves EVENTS; in any other mode EVENTS is an
// unrecognised target and goes to the YANG tree. Comparisons are exact.
// OTHERS is checked before the database names because the Target enum
// lists it too.
func ResolveBackend(origin, target string, stream bool) Backend {
	switch {
	case origin == "openconfig":
		return BackendYANG
	case IsNativeOrigin(origin):
		return BackendNativeDB
	case origin != "":
		return RejectOrigin
	case target == "":
		return RejectEmptyTarget
	case target == "OTHERS":
		return BackendHost
	case target == "SHOW":
		return RejectShow
	case target == "EVENTS" && stream:
		return BackendEvents
	}
	if _, _, ok := SplitDbTarget(target); ok {
		return BackendDB
	}
	return BackendYANG
}

// SplitDbTarget splits a database target, DBNAME or DBNAME/namespace, into
// its database name and namespace. ok is false when the name is not a
// database of the sonic proto's Target enum or the target has more than one
// slash; namespace is empty when the target carries none. The namespace is
// returned as given and is not validated.
func SplitDbTarget(target string) (db, namespace string, ok bool) {
	parts := strings.Split(target, "/")
	if len(parts) > 2 {
		return parts[0], "", false
	}
	if _, isDb := spb.Target_value[parts[0]]; !isDb {
		return parts[0], "", false
	}
	if len(parts) == 2 {
		namespace = parts[1]
	}
	return parts[0], namespace, true
}

// ElemsString renders a path's elements as /a/b[k=v], with an element's
// keys in sorted order so the same path always renders the same way, and
// "" for a path with no elements. It is a display form: key values are not
// escaped, so a value containing ] or = renders ambiguously, and the result
// must not be parsed back or used as a key. The deprecated Element field is
// ignored, as the data clients ignore it.
func ElemsString(p *gnmipb.Path) string {
	var b strings.Builder
	writeElems(&b, p)
	return b.String()
}

func writeElems(b *strings.Builder, p *gnmipb.Path) {
	for _, e := range p.GetElem() {
		b.WriteString("/" + e.GetName())
		for _, k := range slices.Sorted(maps.Keys(e.GetKey())) {
			b.WriteString("[" + k + "=" + e.GetKey()[k] + "]")
		}
	}
}

// AppendPath appends a path's elements to an already rendered prefix. A
// request whose prefix and path both carry no elements addresses the root,
// which renders as "/".
func AppendPath(prefix string, path *gnmipb.Path) string {
	var b strings.Builder
	b.WriteString(prefix)
	writeElems(&b, path)
	if b.Len() == 0 {
		return "/"
	}
	return b.String()
}

// RenderPath is the prefix-applied path with the backend the request was
// resolved to. For a host, event or database backend the target comes
// first, with its namespace suffix if any (COUNTERS_DB/asic0/COUNTERS/
// Ethernet*). For the YANG tree, for the native database client, where the
// database is a path element, and for a rejected request, the path renders
// bare. The root renders as "/", or as TARGET/.
func RenderPath(prefix, path *gnmipb.Path, target string, b Backend) string {
	s := AppendPath(ElemsString(prefix), path)
	if b.Prefixed() {
		return target + s
	}
	return s
}
