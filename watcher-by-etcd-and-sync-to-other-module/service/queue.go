package watching

import (
	"sort"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"

	"go-tools/watcher-by-etcd-and-sync-to-other-module/pb"
	"go-tools/watcher-by-etcd/watcher"
)

type Queue struct {
	logger *zap.Logger
	cond   *sync.Cond

	keyPrefix   string   // keyPrefix will be returned by interface method of Watcher.GetWatchPrefix()
	keyPrefixes []string // Only events with keyPrefixes will be inserted into queue

	events              []*pb.WatchEventProto
	revisions           map[string]int64
	obsoleteCount       int
	compactionThreshold float64
}

func NewQueue(logger *zap.Logger, keyPrefix string, keyPrefixes []string, compactionThreshold float64) *Queue {
	return &Queue{
		logger:              logger,
		cond:                sync.NewCond(&sync.Mutex{}),
		keyPrefix:           keyPrefix,
		keyPrefixes:         keyPrefixes,
		compactionThreshold: compactionThreshold,
		revisions:           make(map[string]int64),
	}
}

func (q *Queue) GetWatchPrefix() string {
	return q.keyPrefix
}

func (q *Queue) OnEtcdEventBase(events []watcher.EtcdEvent) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	for _, event := range events {
		q.OnEtcdEvent(event, false)
	}
	// Empty events mean that base loading is finished.
	if len(events) == 0 {
		sort.Slice(q.events, func(i, j int) bool {
			return q.events[i].Revision < q.events[j].Revision
		})
		for _, e := range q.events {
			q.revisions[e.Key] = e.Revision
		}
	}
}

func (q *Queue) OnEtcdEventInc(event watcher.EtcdEvent) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	q.OnEtcdEvent(event, true)
	q.cond.Broadcast()
}

func (q *Queue) OnEtcdEvent(event watcher.EtcdEvent, putRevision bool) {
	var message []byte
	for _, prefix := range q.keyPrefixes {
		if strings.HasPrefix(event.Key, prefix) {
			message = event.Value
			break
		}
	}
	if len(message) == 0 {
		return
	}
	r := &pb.WatchEventProto{
		Key:      event.Key,
		Revision: event.Revision,
		Message:  message,
	}
	if event.EventType == mvccpb.PUT {
		r.Type = pb.WatchEventProto_ET_PUT
	} else {
		r.Type = pb.WatchEventProto_ET_DELETE
	}
	q.events = append(q.events, r)
	if putRevision {
		if _, ok := q.revisions[r.Key]; ok {
			q.obsoleteCount++
		}
		q.revisions[r.Key] = r.Revision
	}
}

func (q *Queue) Watch(rev int64, limit int) []*pb.WatchEventProto {
	q.cond.L.Lock()
	if len(q.events) == 0 || q.events[len(q.events)-1].Revision < rev {
		q.cond.Wait()
	}
	b := sort.Search(len(q.events), func(i int) bool {
		return q.events[i].Revision >= rev
	})
	var r []*pb.WatchEventProto
	for i := b; i < len(q.events) && i < b+limit; i++ {
		r = append(r, q.events[i])
	}
	q.cond.L.Unlock()
	return r
}

func (q *Queue) Get(rev int64, limit int) []*pb.WatchEventProto {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	b := sort.Search(len(q.events), func(i int) bool {
		return q.events[i].Revision >= rev
	})
	var r []*pb.WatchEventProto
	for i := b; i < len(q.events) && i < b+limit; i++ {
		r = append(r, q.events[i])
	}
	return r
}

func (q *Queue) Compaction() {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if q.obsoleteCount < int(float64(len(q.events))*q.compactionThreshold) {
		q.logger.Info("skipped compaction of watching queue", zap.Int("obsolete", q.obsoleteCount),
			zap.Int("total", len(q.events)), zap.Float64("thresh", q.compactionThreshold))
		return
	}
	start := time.Now()
	var compacted []*pb.WatchEventProto
	for _, e := range q.events {
		if rev, ok := q.revisions[e.Key]; !ok || rev < e.Revision {
			compacted = append(compacted, e)
		}
	}

	q.logger.Info("compacted watching queue", zap.Duration("elapse", time.Since(start)))
}
