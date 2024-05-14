package watching

import (
	"context"
	"io"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"go-tools/watcher-by-etcd-and-sync-to-other-module/pb"
)

type Watcher struct {
	logger      *zap.Logger
	host        string
	keyPrefixes []string
	conn        *grpc.ClientConn
	wg          *sync.WaitGroup
	watchers    []EventWatcher
}

func NewWatcher(logger *zap.Logger, host string, keyPrefixes []string) *Watcher {
	return &Watcher{
		logger:      logger,
		host:        host,
		keyPrefixes: keyPrefixes,
		wg:          &sync.WaitGroup{},
	}
}

type EventWatcher interface {
	OnWatchEvent(e *pb.WatchEventProto)
}

func (w *Watcher) Start() {
	w.logger.Info("watcher is going to start")
	conn, err := grpc.Dial(w.host, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		w.logger.Panic("fail to dial", zap.Error(err))
	}
	w.conn = conn

	rev := int64(0)
	client := pb.NewWatcherClient(w.conn)
	count := 0
	start := time.Now()
	if stream, err := client.Get(context.Background(), &pb.GetRequest{KeyPrefixes: w.keyPrefixes}); err == nil {
		for {
			e, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					w.logger.Panic("fail to get base", zap.Error(err))
				}
				break
			}
			rev = e.Revision + 1
			w.NotifyWatchers(e)
			count++
		}
	} else {
		w.logger.Panic("fail to get base", zap.Error(err))
	}
	w.logger.Info("loaded base events", zap.Int("count", count), zap.Duration("elapse", time.Since(start)))

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		for {
			req := &pb.WatchRequest{Revision: rev, KeyPrefixes: w.keyPrefixes}
			if stream, err := client.Watch(context.Background(), req); err == nil {
				for {
					e, err := stream.Recv()
					if err != nil {
						if err != io.EOF && status.Code(err) != codes.Canceled {
							w.logger.Warn("fail to watch", zap.Error(err))
						}
						break
					}
					rev = e.Revision + 1
					w.NotifyWatchers(e)
				}
			} else {
				w.logger.Warn("fail to watch", zap.Error(err))
			}
			if w.conn.GetState() != connectivity.Shutdown {
				w.logger.Info("watcher is going to restart", zap.Int64("rev", rev))
				time.Sleep(3 * time.Second)
			} else {
				return
			}
		}
	}()
}

func (w *Watcher) Stop() {
	w.logger.Info("watcher is going to stop")
	if err := w.conn.Close(); err != nil {
		w.logger.Panic("fail to close conn", zap.Error(err))
	}
	w.wg.Wait()
}

func (w *Watcher) AddWatcher(watcher EventWatcher) {
	w.watchers = append(w.watchers, watcher)
}

func (w *Watcher) NotifyWatchers(e *pb.WatchEventProto) {
	for _, watcher := range w.watchers {
		watcher.OnWatchEvent(e)
	}
}
