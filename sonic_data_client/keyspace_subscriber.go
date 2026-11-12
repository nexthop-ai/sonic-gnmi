package client

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/redis/go-redis/v9"
)

// keyspaceSubscriber holds one pub/sub connection per redis client for all the
// keyspace patterns of one subscription, so the number of connections does not
// grow with the number of keys its paths expand to. The connections close when
// stop is closed.
type keyspaceSubscriber struct {
	stop    <-chan struct{}
	mu      sync.Mutex
	pubsubs map[*redis.Client]*keyspacePubSub
}

type keyspacePubSub struct {
	pubsub *redis.PubSub
	// mu guards patterns, and is held while sending PSUBSCRIBE and PUNSUBSCRIBE so
	// that redis receives them in the order patterns changed.
	mu       sync.Mutex
	patterns map[string]*keyspacePattern
}

type keyspacePattern struct {
	confirmed     chan struct{}
	confirmedOnce sync.Once
	readers       []*keyspaceReader
}

// keyspaceReader queues the messages of one pattern for one reader, so that a
// slow reader does not hold up the other patterns on the connection.
type keyspaceReader struct {
	mu      sync.Mutex
	pending []*redis.Message
	ready   chan struct{}
}

func newKeyspaceSubscriber(stop <-chan struct{}) *keyspaceSubscriber {
	return &keyspaceSubscriber{stop: stop, pubsubs: make(map[*redis.Client]*keyspacePubSub)}
}

// subscribe subscribes to pattern on the connection for redisDb and returns a
// reader of the pattern's messages once redis confirms the subscription.
func (s *keyspaceSubscriber) subscribe(redisDb *redis.Client, pattern string, timeout time.Duration) (*keyspaceReader, error) {
	s.mu.Lock()
	kp := s.pubsubs[redisDb]
	if kp == nil {
		kp = &keyspacePubSub{pubsub: redisDb.PSubscribe(context.Background()), patterns: make(map[string]*keyspacePattern)}
		s.pubsubs[redisDb] = kp
		go kp.receive(s.stop)
	}
	s.mu.Unlock()

	r := &keyspaceReader{ready: make(chan struct{}, 1)}
	kp.mu.Lock()
	p := kp.patterns[pattern]
	first := p == nil
	if first {
		p = &keyspacePattern{confirmed: make(chan struct{})}
		kp.patterns[pattern] = p
	}
	p.readers = append(p.readers, r)
	var err error
	if first {
		err = kp.pubsub.PSubscribe(context.Background(), pattern)
	}
	kp.mu.Unlock()

	if err != nil {
		s.unsubscribe(redisDb, pattern, r)
		return nil, err
	}
	select {
	case <-p.confirmed:
		return r, nil
	case <-time.After(timeout):
		s.unsubscribe(redisDb, pattern, r)
		return nil, fmt.Errorf("no confirmation within %v", timeout)
	}
}

// unsubscribe stops delivering pattern's messages to r, and unsubscribes from
// pattern when r was its last reader.
func (s *keyspaceSubscriber) unsubscribe(redisDb *redis.Client, pattern string, r *keyspaceReader) {
	s.mu.Lock()
	kp := s.pubsubs[redisDb]
	s.mu.Unlock()
	if kp == nil {
		return
	}

	kp.mu.Lock()
	defer kp.mu.Unlock()
	p := kp.patterns[pattern]
	if p == nil {
		return
	}
	for i, reader := range p.readers {
		if reader == r {
			p.readers = append(p.readers[:i], p.readers[i+1:]...)
			break
		}
	}
	if len(p.readers) > 0 {
		return
	}
	delete(kp.patterns, pattern)

	select {
	case <-s.stop:
		// the connection is closing
	default:
		if err := kp.pubsub.PUnsubscribe(context.Background(), pattern); err != nil {
			log.V(2).Infof("punsubscribe from %s failed: %v", pattern, err)
		}
	}
}

// receive hands each message on the connection to the readers of its pattern
// until stop is closed.
func (kp *keyspacePubSub) receive(stop <-chan struct{}) {
	defer kp.pubsub.Close()
	for {
		select {
		case <-stop:
			return
		default:
		}

		msgi, err := kp.pubsub.ReceiveTimeout(context.Background(), time.Millisecond*500)
		if err != nil {
			if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
				continue
			}
			log.V(2).Infof("pubsub.ReceiveTimeout err %v", err)
			// go-redis reconnects with every pattern; wait so a refused connection is not retried in a tight loop
			select {
			case <-stop:
				return
			case <-time.After(time.Second):
			}
			continue
		}

		switch msg := msgi.(type) {
		case *redis.Subscription:
			if msg.Kind == "psubscribe" {
				kp.mu.Lock()
				if p := kp.patterns[msg.Channel]; p != nil {
					p.confirmedOnce.Do(func() { close(p.confirmed) })
				}
				kp.mu.Unlock()
			}
		case *redis.Message:
			kp.mu.Lock()
			if p := kp.patterns[msg.Pattern]; p != nil {
				for _, r := range p.readers {
					r.push(msg)
				}
			}
			kp.mu.Unlock()
		}
	}
}

func (r *keyspaceReader) push(msg *redis.Message) {
	r.mu.Lock()
	r.pending = append(r.pending, msg)
	r.mu.Unlock()
	select {
	case r.ready <- struct{}{}:
	default:
	}
}

// next returns the oldest queued message, waiting for one, or false once stop
// is closed.
func (r *keyspaceReader) next(stop <-chan struct{}) (*redis.Message, bool) {
	for {
		r.mu.Lock()
		if len(r.pending) > 0 {
			msg := r.pending[0]
			r.pending[0] = nil
			r.pending = r.pending[1:]
			r.mu.Unlock()
			return msg, true
		}
		r.mu.Unlock()

		select {
		case <-r.ready:
		case <-stop:
			return nil, false
		}
	}
}
