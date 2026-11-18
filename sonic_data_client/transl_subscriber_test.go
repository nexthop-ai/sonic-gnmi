package client

import (
	"sync"
	"testing"

	"github.com/Azure/sonic-mgmt-common/translib"
	"github.com/Workiva/go-datastructures/queue"
	spb "github.com/openconfig/gnmi/proto/gnmi"
)

func TestNotify(t *testing.T) {
	tests := []struct {
		name        string
		v           *translib.SubscribeResponse
		builderMsg  *spb.Notification
		builderErr  error
		hasSuperSub bool
		expectedErr bool
		expectedLen int
	}{
		{
			name: "Standard notification path",
			v:    &translib.SubscribeResponse{},
			builderMsg: &spb.Notification{
				Update: []*spb.Update{{Path: &spb.Path{Target: "test"}}},
			},
			expectedErr: false,
			expectedLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 1. Initialize the Subscriber and Client pointers
			ts := &translSubscriber{}
			ts.client = &TranslClient{}

			// 2. Fix the Queue Assignment
			ts.client.q = queue.NewPriorityQueue(10, false)

			// 3. Mock the msgBuilder function
			ts.msgBuilder = func(resp *translib.SubscribeResponse, s *translSubscriber) (*spb.Notification, error) {
				return tt.builderMsg, tt.builderErr
			}

			// 4. Handle SuperSub if needed
			if tt.hasSuperSub {
				ts.client.superSub = &superSubscription{}
			}

			// Execute
			ts.notify(tt.v)
		})
	}
}
func TestNotifyNil(t *testing.T) {
	tests := []struct {
		name        string
		v           *translib.SubscribeResponse
		builderMsg  *spb.Notification
		builderErr  error
		hasSuperSub bool
		expectedErr bool
		expectedLen int
	}{
		{
			name: "Standard notification path",
			v:    &translib.SubscribeResponse{},
			builderMsg: &spb.Notification{
				Update: []*spb.Update{{Path: &spb.Path{Target: "test"}}},
			},
			expectedErr: false,
			expectedLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 1. Initialize the Subscriber and Client pointers
			ts := &translSubscriber{}
			ts.client = &TranslClient{}

			// 2. Fix the Queue Assignment
			ts.client.q = queue.NewPriorityQueue(10, false)

			// 3. Mock the msgBuilder function
			ts.msgBuilder = func(resp *translib.SubscribeResponse, s *translSubscriber) (*spb.Notification, error) {
				if resp == nil {
					// Return a valid notification to pass the (len == 0) check
					return &spb.Notification{
						Update: []*spb.Update{{}},
					}, nil
				}
				return nil, nil
			}

			// 4. Handle SuperSub if needed
			if tt.hasSuperSub {
				ts.client.superSub = &superSubscription{}
			}

			// Execute
			ts.notify(tt.v)
		})
	}
}

// A tick of a superSubscription's primary sends its updates to every
// client; an update that is not from a tick goes to its own client only.
func TestNotifyShared(t *testing.T) {
	for _, shared := range []bool{true, false} {
		primary := &TranslClient{q: queue.NewPriorityQueue(10, false)}
		other := &TranslClient{q: queue.NewPriorityQueue(10, false)}
		ss := &superSubscription{
			mu:      &sync.RWMutex{},
			clients: map[*TranslClient]struct{}{primary: {}, other: {}},
		}
		primary.superSub, other.superSub = ss, ss
		ts := &translSubscriber{client: primary, shared: shared}
		ts.msgBuilder = func(*translib.SubscribeResponse, *translSubscriber) (*spb.Notification, error) {
			return &spb.Notification{Update: []*spb.Update{{}}}, nil
		}
		if err := ts.notify(&translib.SubscribeResponse{}); err != nil {
			t.Fatal(err)
		}
		want := 0
		if shared {
			want = 1
		}
		if primary.q.Len() != 1 || other.q.Len() != want {
			t.Errorf("shared %v: %d updates for the primary, %d for the other client, want 1 and %d",
				shared, primary.q.Len(), other.q.Len(), want)
		}
	}
}

// The subscriber a tick samples with sends its updates to every client of
// the superSubscription, and filters duplicates only off a heartbeat.
func TestTickSubscriberShared(t *testing.T) {
	primary := &TranslClient{q: queue.NewPriorityQueue(10, false)}
	other := &TranslClient{q: queue.NewPriorityQueue(10, false)}
	ss := &superSubscription{
		mu:      &sync.RWMutex{},
		clients: map[*TranslClient]struct{}{primary: {}, other: {}},
	}
	primary.superSub, other.superSub = ss, ss
	sub := &spb.Subscription{SuppressRedundant: true}

	ts := tickSubscriber(primary, nil, nil, &ticker_info{sub: sub}, &sync.WaitGroup{})
	if !ts.shared || !ts.filterDups {
		t.Errorf("sample tick: shared %v, filterDups %v, want both true", ts.shared, ts.filterDups)
	}
	if hb := tickSubscriber(primary, nil, nil, &ticker_info{sub: sub, heartbeat: true}, &sync.WaitGroup{}); !hb.shared || hb.filterDups {
		t.Errorf("heartbeat tick: shared %v, filterDups %v, want true and false", hb.shared, hb.filterDups)
	}

	ts.msgBuilder = func(*translib.SubscribeResponse, *translSubscriber) (*spb.Notification, error) {
		return &spb.Notification{Update: []*spb.Update{{}}}, nil
	}
	if err := ts.notify(&translib.SubscribeResponse{}); err != nil {
		t.Fatal(err)
	}
	if primary.q.Len() != 1 || other.q.Len() != 1 {
		t.Errorf("%d updates for the primary, %d for the other client, want 1 and 1", primary.q.Len(), other.q.Len())
	}
}

// When translib reports its backend terminated, the stream gets a Fatal and
// the stop channel is left for its owner to close.
func TestProcessResponsesTerminatedLeavesStopOpen(t *testing.T) {
	c := &TranslClient{q: queue.NewPriorityQueue(10, false), channel: make(chan struct{}), w: &sync.WaitGroup{}}
	ts := &translSubscriber{client: c}
	ts.synced.Add(1)
	c.w.Add(1)
	q := queue.NewPriorityQueue(1, false)
	q.Put(&translib.SubscribeResponse{IsTerminated: true})
	ts.processResponses(q)

	items, err := c.q.Get(1)
	if err != nil || len(items) != 1 || items[0].(Value).GetFatal() == "" {
		t.Fatalf("queued %v (err %v), want one Fatal", items, err)
	}
	close(c.channel) // panics if processResponses closed it
}
