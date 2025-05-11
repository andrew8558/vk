package subpub

import (
	"context"
	"errors"
	"log"
	"os"
	"strconv"
	"sync"
)

var ErrClosed = errors.New("subpub is closed")

type MessageHandler func(msg interface{})

type Subscription interface {
	Unsubscribe()
}

type SubPub interface {
	Subscribe(subject string, cb MessageHandler) (Subscription, error)
	Publish(subject string, msg interface{}) error
	Close(ctx context.Context) error
}

type subPub struct {
	mu               sync.RWMutex
	subscribers      map[string]map[*subscription]struct{}
	closed           chan struct{}
	wg               *sync.WaitGroup
	subsChanCapacity int
}

func NewSubPub() SubPub {
	subsChanCapacityStr := os.Getenv("SUBSCRIBERS_CHANNEL_CAPACITY")
	subsChanCapacity, _ := strconv.Atoi(subsChanCapacityStr)

	return &subPub{
		subscribers:      make(map[string]map[*subscription]struct{}),
		wg:               &sync.WaitGroup{},
		closed:           make(chan struct{}),
		subsChanCapacity: subsChanCapacity,
	}
}

type subscription struct {
	mu           sync.Mutex
	subject      string
	ch           chan interface{}
	subPub       *subPub
	unsubscribed bool
}

func (s *subscription) Unsubscribe() {
	s.mu.Lock()
	if s.unsubscribed {
		s.mu.Unlock()
		return
	}
	s.unsubscribed = true
	s.mu.Unlock()

	s.subPub.mu.Lock()
	delete(s.subPub.subscribers[s.subject], s)
	if len(s.subPub.subscribers[s.subject]) == 0 {
		delete(s.subPub.subscribers, s.subject)
	}
	s.subPub.mu.Unlock()

	close(s.ch)
}

func (s *subscription) start(wg *sync.WaitGroup, handler MessageHandler) {
	defer wg.Done()
	for msg := range s.ch {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("panic in handler for subject %s: %v", s.subject, r)
				}
			}()
			handler(msg)
		}()
	}
}

func (sp *subPub) Subscribe(subject string, cb MessageHandler) (Subscription, error) {
	select {
	case <-sp.closed:
		return nil, ErrClosed
	default:
	}

	sub := &subscription{
		subject: subject,
		ch:      make(chan interface{}, sp.subsChanCapacity),
		subPub:  sp,
	}

	sp.mu.Lock()
	if sp.subscribers[subject] == nil {
		sp.subscribers[subject] = map[*subscription]struct{}{}
	}
	sp.subscribers[subject][sub] = struct{}{}
	sp.mu.Unlock()

	sp.wg.Add(1)
	go sub.start(sp.wg, cb)

	return sub, nil
}

func (sp *subPub) Publish(subject string, msg interface{}) error {
	select {
	case <-sp.closed:
		return ErrClosed
	default:
	}

	sp.wg.Add(1)
	defer sp.wg.Done()

	sp.mu.RLock()
	for subscriber := range sp.subscribers[subject] {
		select {
		case subscriber.ch <- msg:
		default:
			log.Printf("subscriber channel is full, skipped message for subject %s", subject)
		}
	}
	sp.mu.RUnlock()

	return nil
}

func (sp *subPub) Close(ctx context.Context) error {
	close(sp.closed)

	doneCh := make(chan struct{})
	go func() {
		sp.wg.Wait()
		close(doneCh)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-doneCh:
	}

	sp.mu.Lock()
	for _, subscribers := range sp.subscribers {
		for subscriber := range subscribers {
			subscriber.Unsubscribe()
		}
	}
	sp.mu.Unlock()

	return nil
}
