package watching

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"

	"go-tools/watcher-by-etcd-and-sync-to-other-module/pb"
)

type Service struct {
	pb.UnimplementedWatcherServer

	logger *zap.Logger
	q      *Queue
	batch  int
}

var (
	watcherCount = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "storage",
		Name:      "watcher_count",
		Help:      "The watcher count",
	})
)

func NewService(logger *zap.Logger, q *Queue, batch int) *Service {
	return &Service{
		logger: logger,
		q:      q,
		batch:  batch,
	}
}

func (s *Service) Get(req *pb.GetRequest, stream pb.Watcher_GetServer) error {
	rev := int64(0)
	for {
		events := s.q.Get(rev, s.batch)
		for _, e := range events {
			for _, prefix := range req.GetKeyPrefixes() {
				if strings.HasPrefix(e.Key, prefix) {
					if err := stream.Send(e); err != nil {
						return err
					}
					break
				}
			}
			rev = e.Revision + 1
		}
		if len(events) < s.batch {
			return nil
		}
	}
}

func (s *Service) Watch(req *pb.WatchRequest, stream pb.Watcher_WatchServer) error {
	rev := req.GetRevision()
	watcherCount.Inc()
	defer watcherCount.Dec()
	for {
		events := s.q.Watch(rev, s.batch)
		for _, e := range events {
			for _, prefix := range req.GetKeyPrefixes() {
				if strings.HasPrefix(e.Key, prefix) {
					if err := stream.Send(e); err != nil {
						return err
					}
					break
				}
			}
			rev = e.Revision + 1
		}
	}
}
