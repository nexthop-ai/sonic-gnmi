// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package substate

import (
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Causes a subscription can fail on. Each is the suffix of a server-row
// field, failed_<cause>, so the names are part of the row's format.
const (
	CauseConnect = "connect" // the collector was not reached, or the stream to it could not be opened
	CauseSend    = "send"    // the send to the collector failed while the collector was still there
	CauseSource  = "source"  // the data source aborted the stream
	CauseEncode  = "encode"  // a value could not be encoded for the collector, or was over the send size limit
)

// CauseRequest is a cause with no server-row field of its own: after the
// stream started, the data client found the collector's request unusable.
// A producer counts it under failed_other.
const CauseRequest = "request"

// FailedField is the server row field that counts a cause.
func FailedField(cause string) string { return "failed_" + cause }

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
