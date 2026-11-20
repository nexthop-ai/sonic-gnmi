// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package substate

import (
	"strconv"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Causes a subscription can fail on. Each is the suffix of a server-row
// field, failed_<cause>, so the names are part of the row's format.
const (
	CauseConnect = "connect" // the collector was not reached, or the stream to it could not be opened
	CauseSend    = "send"    // the send to the collector failed while the collector was still there; on a dial-out row, where the switch is the client, also a collector that ended or dropped the stream
	CauseSource  = "source"  // the data source aborted the stream; on a dial-out row, also a periodic read of the data source that failed
	CauseEncode  = "encode"  // a value could not be encoded for the collector, or was over the send size limit
)

// CauseRequest is a cause with no server-row field of its own: after the
// stream started, the data client found the collector's request unusable.
// A producer counts it under failed_other.
const CauseRequest = "request"

// FailedField is the server row field that counts a cause.
func FailedField(cause string) string { return "failed_" + cause }

// CauseOther collects every failure whose reason has no counter of its
// own, so the failed_* fields always add up to the total.
const CauseOther = "other"

// FailureCounts counts subscription failures by reason: one counter for
// each reason the caller lists, and failed_other for everything else. It
// is safe to use from several goroutines.
type FailureCounts struct {
	by map[string]*atomic.Int64
}

// NewFailureCounts returns a FailureCounts with one counter for each
// listed cause, plus one for CauseOther.
func NewFailureCounts(causes ...string) *FailureCounts {
	f := &FailureCounts{by: map[string]*atomic.Int64{CauseOther: new(atomic.Int64)}}
	for _, c := range causes {
		f.by[c] = new(atomic.Int64)
	}
	return f
}

// Add counts one failure of cause.
func (f *FailureCounts) Add(cause string) {
	c, ok := f.by[cause]
	if !ok {
		c = f.by[CauseOther]
	}
	c.Add(1)
}

// Fields sets a failed_<cause> field in row for every counter.
func (f *FailureCounts) Fields(row map[string]int64) {
	for cause, c := range f.by {
		row[FailedField(cause)] = c.Load()
	}
}

// SendCause classifies a stream Send error. grpc-go reports a message it
// could not marshal as codes.Internal and a message over the send size limit
// as codes.ResourceExhausted, both before anything reaches the wire, so both
// are encode failures; every other error is the transport, that is, a send
// failure. On a client stream, as dial-out's, grpc-go reports any stream
// or transport failure from Send as io.EOF (the status comes only from
// Recv), so those count as send failures. TestSendCauseFromGRPC pins this
// against grpc-go.
func SendCause(err error) string {
	switch status.Code(err) {
	case codes.Internal, codes.ResourceExhausted:
		return CauseEncode
	}
	return CauseSend
}

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
