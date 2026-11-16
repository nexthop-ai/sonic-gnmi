// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package substate

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestSendCause(t *testing.T) {
	for _, c := range []struct {
		err  error
		want string
	}{
		{status.Error(codes.Internal, "grpc: error while marshaling: proto: oops"), CauseEncode},
		{status.Error(codes.ResourceExhausted, "grpc: trying to send message larger than max (5000000 vs. 4194304)"), CauseEncode},
		{status.Error(codes.Unavailable, "transport is closing"), CauseSend},
		{status.Error(codes.Canceled, "context canceled"), CauseSend},
		{errors.New("plain error"), CauseSend},
		{nil, CauseSend},
	} {
		if got := SendCause(c.err); got != c.want {
			t.Errorf("SendCause(%v) = %q, want %q", c.err, got, c.want)
		}
	}
}

// The cause names are server-row field names: pin every one.
func TestFailedFields(t *testing.T) {
	for cause, want := range map[string]string{
		CauseConnect: "failed_connect", CauseSend: "failed_send", CauseSource: "failed_source",
		CauseEncode: "failed_encode",
	} {
		if got := FailedField(cause); got != want {
			t.Errorf("FailedField(%q) = %q, want %q", cause, got, want)
		}
	}
}

// subscribeServer answers a Subscribe with one response, elem as its one
// path element name, and hands back the error its Send returned.
type subscribeServer struct {
	gnmipb.UnimplementedGNMIServer
	elem    string
	sendErr chan error
}

func (s *subscribeServer) Subscribe(stream gnmipb.GNMI_SubscribeServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	path := &gnmipb.Path{Elem: []*gnmipb.PathElem{{Name: s.elem}}}
	err := stream.Send(&gnmipb.SubscribeResponse{Response: &gnmipb.SubscribeResponse_Update{
		Update: &gnmipb.Notification{Update: []*gnmipb.Update{{Path: path}}}}})
	s.sendErr <- err
	return err
}

// SendCause against grpc-go itself: a server-side Send of a message that
// cannot be marshalled, or that is over the send size limit, is an encode
// failure.
func TestSendCauseFromGRPC(t *testing.T) {
	for _, c := range []struct {
		name string
		elem string
	}{
		{"not valid UTF-8", string([]byte{0xff, 0xfe})},
		{"over the size limit", strings.Repeat("x", 1024)},
	} {
		t.Run(c.name, func(t *testing.T) { sendCauseFromGRPC(t, c.elem) })
	}
}

func sendCauseFromGRPC(t *testing.T, elem string) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.MaxSendMsgSize(512))
	ss := &subscribeServer{elem: elem, sendErr: make(chan error, 1)}
	gnmipb.RegisterGNMIServer(srv, ss)
	go srv.Serve(lis)
	defer srv.Stop()
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := gnmipb.NewGNMIClient(conn).Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stream.Send(&gnmipb.SubscribeRequest{})
	select {
	case err := <-ss.sendErr:
		if err == nil {
			t.Fatal("Send succeeded")
		}
		if got := SendCause(err); got != CauseEncode {
			t.Errorf("SendCause(%v) = %q, want %q", err, got, CauseEncode)
		}
	case <-ctx.Done():
		t.Fatal("the server never sent")
	}
}

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
