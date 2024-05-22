package model

import (
	"context"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type Watcher interface {
	OnEtcdEventBase(events []EtcdEvent)
	OnEtcdEventInc(event EtcdEvent)
}

// EtcdWatcher watches events from etcd and dispatches them to registered watchers.
type EtcdWatcher struct {
	logger      *zap.Logger
	wg          *sync.WaitGroup
	client      *clientv3.Client
	baseBatch   int
	watchPrefix string
	groups      []*WatcherGroup
	lock        sync.Mutex
}

func NewEtcdWatcher(logger *zap.Logger, client *clientv3.Client, prefix string, baseBatch int) *EtcdWatcher {
	return &EtcdWatcher{
		logger:      logger,
		wg:          &sync.WaitGroup{},
		client:      client,
		watchPrefix: prefix,
		baseBatch:   baseBatch,
	}
}

func (ew *EtcdWatcher) AddGroup() *WatcherGroup {
	g := &WatcherGroup{baseBatch: ew.baseBatch}
	ew.groups = append(ew.groups, g)
	return g
}

func (ew *EtcdWatcher) Start(ctx context.Context) {
	ew.logger.Info("etcd watcher is going to start", zap.String("prefix", ew.watchPrefix),
		zap.Int("baseBatch", ew.baseBatch))
	rev, err := ew.loadBase(ctx)
	if err != nil {
		ew.logger.Panic("fail to load base", zap.Error(err))
	}
	ew.wg.Add(1)
	go func(rev int64) {
		defer ew.wg.Done()
		for {
			rch := ew.client.Watch(ctx, ew.watchPrefix, clientv3.WithRev(rev+1), clientv3.WithPrefix(), clientv3.WithPrevKV())
			for resp := range rch {
				if len(resp.Events) == 1 {
					rev = resp.Events[0].Kv.ModRevision
					ew.dispatchEvent(buildEvent(resp.Events[0]))
				} else {
					var events []EtcdEvent
					for _, e := range resp.Events {
						events = append(events, buildEvent(e))
						rev = e.Kv.ModRevision
					}
					ew.dispatchEvents(events)
				}
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			ew.logger.Warn("etcd watcher is going to restart")
			time.Sleep(3 * time.Second)
		}
	}(rev)
}

func (ew *EtcdWatcher) Stop() {
	ew.logger.Info("etcd watcher is going to stop")
	ew.wg.Wait()
}

func (ew *EtcdWatcher) loadBase(ctx context.Context) (int64, error) {
	// Get most recent version.
	resp, err := ew.client.Get(ctx, ew.watchPrefix)
	if err != nil {
		return 0, err
	}
	rev := resp.Header.Revision

	opts := []clientv3.OpOption{
		clientv3.WithLimit(int64(ew.baseBatch)),
		clientv3.WithRev(rev),
		clientv3.WithRange(clientv3.GetPrefixRangeEnd(ew.watchPrefix)),
	}
	key := ew.watchPrefix
	start := time.Now()
	for {
		resp, err := ew.client.Get(ctx, key, opts...)
		if err != nil {
			return 0, err
		}
		for _, ev := range resp.Kvs {
			event := EtcdEvent{
				EventType: mvccpb.PUT,
				Key:       string(ev.Key),
				Value:     ev.Value,
				Revision:  ev.ModRevision,
				LeaseID:   clientv3.LeaseID(ev.Lease),
			}
			for _, g := range ew.groups {
				for _, dispatcher := range g.dispatchers {
					if dispatcher.bufferBase(event) {
						break
					}
				}
			}
		}
		if !resp.More {
			ew.logger.Info("loaded base data", zap.Duration("elapse", time.Since(start)))
			for _, g := range ew.groups {
				for _, dispatcher := range g.dispatchers {
					count := dispatcher.clearBase()
					ew.logger.Info("loaded base data to watcher",
						zap.String("prefix", dispatcher.keyPrefix), zap.Int("count", count))
				}
			}
			return rev, nil
		}
		key = string(append(resp.Kvs[len(resp.Kvs)-1].Key, 0))
	}
}

func (ew *EtcdWatcher) dispatchEvent(event EtcdEvent) {
	for _, g := range ew.groups {
		for _, dispatcher := range g.dispatchers {
			if dispatcher.dispatchInc(event) {
				break
			}
		}
	}
}

// dispatchEvents dispatches events from a same transaction to watchers.
// This method is called by many go-routines. One caller is the routine watching etcd.
// Another caller is EtcdModel.Transaction() which dispatches events as a fast path to fill indexes.
// Therefore, a lock is necessary to guard concurrent access.
func (ew *EtcdWatcher) dispatchEvents(events []EtcdEvent) {
	ew.lock.Lock()
	defer ew.lock.Unlock()
	for _, g := range ew.groups {
		for _, e := range events {
			for _, dispatcher := range g.dispatchers {
				if dispatcher.bufferInc(e) {
					break
				}
			}
		}
		for _, dispatcher := range g.dispatchers {
			dispatcher.clearInc()
		}
	}
}

func buildEvent(ev *clientv3.Event) EtcdEvent {
	if ev.Type == mvccpb.PUT {
		return EtcdEvent{
			EventType: ev.Type,
			Key:       string(ev.Kv.Key),
			Value:     ev.Kv.Value,
			Revision:  ev.Kv.ModRevision,
			LeaseID:   clientv3.LeaseID(ev.Kv.Lease),
		}
	} else if ev.Type == mvccpb.DELETE {
		return EtcdEvent{
			EventType: ev.Type,
			Key:       string(ev.PrevKv.Key),
			Value:     ev.PrevKv.Value,
			Revision:  ev.Kv.ModRevision,
			LeaseID:   clientv3.LeaseID(ev.PrevKv.Lease),
		}
	}
	// Impossible to reach here.
	return EtcdEvent{}
}

type WatcherGroup struct {
	dispatchers []*Dispatcher
	baseBatch   int
}

func (wg *WatcherGroup) AddWatcher(keyPrefix string, watcher Watcher) {
	wg.dispatchers = append(wg.dispatchers, &Dispatcher{
		keyPrefix: keyPrefix,
		watcher:   watcher,
		baseBatch: wg.baseBatch,
	})
}

type Dispatcher struct {
	keyPrefix string
	watcher   Watcher
	baseBatch int
	events    []EtcdEvent
	baseAcc   int
}

func (d *Dispatcher) bufferBase(event EtcdEvent) bool {
	if !strings.HasPrefix(event.Key, d.keyPrefix) {
		return false
	}
	d.events = append(d.events, event)
	if len(d.events) >= d.baseBatch {
		d.baseAcc += len(d.events)
		d.watcher.OnEtcdEventBase(d.events)
		d.events = nil
	}
	return true
}

func (d *Dispatcher) clearBase() int {
	if len(d.events) > 0 {
		d.baseAcc += len(d.events)
		d.watcher.OnEtcdEventBase(d.events)
		d.events = nil
	}
	// Send an empty batch to tell watchers that base loading is finished.
	d.watcher.OnEtcdEventBase(nil)
	return d.baseAcc
}

func (d *Dispatcher) bufferInc(event EtcdEvent) bool {
	if !strings.HasPrefix(event.Key, d.keyPrefix) {
		return false
	}
	d.events = append(d.events, event)
	return true
}

func (d *Dispatcher) clearInc() {
	if len(d.events) > 0 {
		for _, e := range d.events {
			d.watcher.OnEtcdEventInc(e)
		}
		d.events = nil
	}
}

func (d *Dispatcher) dispatchInc(event EtcdEvent) bool {
	if !strings.HasPrefix(event.Key, d.keyPrefix) {
		return false
	}
	d.watcher.OnEtcdEventInc(event)
	return true
}
