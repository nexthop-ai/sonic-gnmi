// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package gnmi

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/openconfig/gnmi/proto/gnmi"
	sdcfg "github.com/sonic-net/sonic-gnmi/sonic_db_config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const dispatchTestPort = 8095

// TestSubscribeRejectArms drives one Subscribe into each reject arm of the
// dispatch and checks the code and message the arm returns, so the mapping
// from a Backend to its answer is pinned at the server, not only in
// common_utils.
func TestSubscribeRejectArms(t *testing.T) {
	s := createServer(t, dispatchTestPort)
	go runServer(t, s)
	defer s.ForceStop()
	ns, _ := sdcfg.GetDbDefaultNamespace()
	prepareDb(t, ns)
	conn := createClient(t, dispatchTestPort)
	client := pb.NewGNMIClient(conn)

	counters := &pb.Subscription{Path: &pb.Path{Elem: []*pb.PathElem{{Name: "COUNTERS"}, {Name: "Ethernet0"}}},
		Mode: pb.SubscriptionMode_SAMPLE, SampleInterval: uint64(time.Second)}
	cases := []struct {
		name   string
		prefix *pb.Path
		msg    string
	}{
		{"unknown origin", &pb.Path{Origin: "no-such-origin", Target: "COUNTERS_DB"}, "Unsupported origin: no-such-origin"},
		{"no origin and no target", &pb.Path{}, "Empty target data not supported"},
		{"SHOW target", &pb.Path{Target: "SHOW"}, "SHOW does not support subscribe operations"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream, err := client.Subscribe(ctx)
			if err != nil {
				t.Fatalf("subscribe: %v", err)
			}
			req := &pb.SubscribeRequest{Request: &pb.SubscribeRequest_Subscribe{Subscribe: &pb.SubscriptionList{
				Prefix: tc.prefix, Mode: pb.SubscriptionList_STREAM, Encoding: pb.Encoding_JSON_IETF,
				Subscription: []*pb.Subscription{counters}}}}
			if err := stream.Send(req); err != nil {
				t.Fatalf("send: %v", err)
			}
			_, err = stream.Recv()
			if status.Code(err) != codes.Unimplemented {
				t.Fatalf("got %v, want Unimplemented", err)
			}
			if !strings.Contains(status.Convert(err).Message(), tc.msg) {
				t.Errorf("message %q, want it to contain %q", status.Convert(err).Message(), tc.msg)
			}
		})
	}
}
