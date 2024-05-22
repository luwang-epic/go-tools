package model

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"go-tools/watcher-by-etcd/util"
)

type EtcdModel interface {
	Add(ctx context.Context, key string, message proto.Message) (int64, error)
	Update(ctx context.Context, key string, message proto.Message, preRevision int64) (int64, error)
	UpdateIgnoreRevision(ctx context.Context, key string, m proto.Message) (int64, error)
	Get(ctx context.Context, key string, message proto.Message) (int64, error)
	Put(ctx context.Context, key string, m proto.Message) (int64, error)
	PutWithLease(ctx context.Context, key string, m proto.Message, leaseID clientv3.LeaseID) (int64, error)
	DeleteIgnoreRevision(ctx context.Context, key string, prev proto.Message) (int64, error)
	GrantLease(ctx context.Context, ttl int64) (clientv3.LeaseID, error)
	KeepAliveLease(ctx context.Context, id clientv3.LeaseID) error
	RevokeLease(ctx context.Context, id clientv3.LeaseID) error
}

type EtcdModelImpl struct {
	logger       *zap.Logger
	client       *clientv3.Client
	queryTimeout time.Duration
	watcher      *EtcdWatcher
}

var (
	etcdReqSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: "etcd",
		Name:      "request_seconds",
		Help:      "The latency of etcd requests",
		Buckets:   prometheus.ExponentialBuckets(0.005, 2, 11),
	}, []string{"method", "status"})
)

func NewEtcdModel(logger *zap.Logger, client *clientv3.Client, queryTimeout time.Duration, watcher *EtcdWatcher) *EtcdModelImpl {
	return &EtcdModelImpl{
		logger:       logger,
		client:       client,
		queryTimeout: queryTimeout,
		watcher:      watcher,
	}
}

func (em *EtcdModelImpl) Add(ctx context.Context, key string, message proto.Message) (int64, error) {
	value, err := proto.Marshal(message)
	if err != nil {
		return 0, err
	}
	c, cancel := context.WithTimeout(ctx, em.queryTimeout)
	defer cancel()
	req := clientv3.OpPut(key, string(value))
	cond := clientv3.Compare(clientv3.Version(key), "=", 0)
	resp, err := withLatency("put", func() (*clientv3.TxnResponse, error) {
		return em.client.Txn(c).If(cond).Then(req).Commit()
	})
	if err != nil {
		return 0, err
	}
	if !resp.Succeeded {
		return 0, &util.EtcdKeyExistsError{Key: key}
	}
	return resp.Header.Revision, nil
}

func (em *EtcdModelImpl) Update(ctx context.Context, key string, message proto.Message, preRevision int64) (int64, error) {
	value, err := proto.Marshal(message)
	if err != nil {
		return 0, err
	}
	c, cancel := context.WithTimeout(ctx, em.queryTimeout)
	defer cancel()
	req := clientv3.OpPut(key, string(value), clientv3.WithPrevKV())
	cond := clientv3.Compare(clientv3.ModRevision(key), "=", preRevision)
	resp, err := withLatency("put", func() (*clientv3.TxnResponse, error) {
		return em.client.Txn(c).If(cond).Then(req).Commit()
	})
	if err != nil {
		return 0, err
	}
	if !resp.Succeeded {
		return 0, &util.RevisionConflictError{Given: preRevision}
	}
	return resp.Header.Revision, nil
}

func (em *EtcdModelImpl) UpdateIgnoreRevision(ctx context.Context, key string, message proto.Message) (int64, error) {
	value, err := proto.Marshal(message)
	if err != nil {
		return 0, err
	}
	c, cancel := context.WithTimeout(ctx, em.queryTimeout)
	defer cancel()
	req := clientv3.OpPut(key, string(value), clientv3.WithPrevKV())
	cond := clientv3.Compare(clientv3.ModRevision(key), "!=", 0)
	resp, err := withLatency("put", func() (*clientv3.TxnResponse, error) {
		return em.client.Txn(c).If(cond).Then(req).Commit()
	})
	if err != nil {
		return 0, err
	}
	if !resp.Succeeded {
		return 0, &util.EtcdKeyExistsError{Key: key}
	}
	return resp.Header.Revision, nil
}

func (em *EtcdModelImpl) DeleteIgnoreRevision(ctx context.Context, key string, prev proto.Message) (int64, error) {
	c, cancel := context.WithTimeout(ctx, em.queryTimeout)
	defer cancel()
	resp, err := withLatency("delete", func() (*clientv3.DeleteResponse, error) {
		return em.client.Delete(c, key, clientv3.WithPrevKV())
	})
	if err != nil {
		return 0, err
	}
	if resp.Deleted <= 0 {
		return 0, &util.EtcdKeyNotFoundError{Key: key}
	}
	if prev != nil {
		if err := proto.Unmarshal(resp.PrevKvs[0].Value, prev); err != nil {
			return 0, err
		}
	}
	return resp.Header.Revision, nil
}

func (em *EtcdModelImpl) Get(ctx context.Context, key string, message proto.Message) (int64, error) {
	c, cancel := context.WithTimeout(ctx, em.queryTimeout)
	defer cancel()
	resp, err := withLatency("get", func() (*clientv3.GetResponse, error) {
		return em.client.Get(c, key)
	})
	if err != nil {
		return 0, err
	}
	if len(resp.Kvs) == 0 {
		return 0, &util.EtcdKeyNotFoundError{Key: key}
	}
	if err := proto.Unmarshal(resp.Kvs[0].Value, message); err != nil {
		return 0, err
	}
	return resp.Kvs[0].ModRevision, nil
}

func (em *EtcdModelImpl) Put(ctx context.Context, key string, m proto.Message) (int64, error) {
	value, err := proto.Marshal(m)
	if err != nil {
		return 0, err
	}
	c, cancel := context.WithTimeout(ctx, em.queryTimeout)
	defer cancel()
	resp, err := withLatency("put", func() (*clientv3.PutResponse, error) {
		return em.client.Put(c, key, string(value))
	})
	if err != nil {
		return 0, err
	}
	return resp.Header.Revision, nil
}

func (em *EtcdModelImpl) PutWithLease(ctx context.Context, key string, m proto.Message, leaseID clientv3.LeaseID) (int64, error) {
	value, err := proto.Marshal(m)
	if err != nil {
		return 0, err
	}
	c, cancel := context.WithTimeout(ctx, em.queryTimeout)
	defer cancel()
	resp, err := withLatency("put", func() (*clientv3.PutResponse, error) {
		return em.client.Put(c, key, string(value), clientv3.WithLease(leaseID))
	})
	if err != nil {
		return 0, err
	}
	return resp.Header.Revision, nil
}

func (em *EtcdModelImpl) GrantLease(ctx context.Context, ttl int64) (clientv3.LeaseID, error) {
	c, cancel := context.WithTimeout(ctx, em.queryTimeout)
	defer cancel()
	resp, err := withLatency("grant", func() (*clientv3.LeaseGrantResponse, error) {
		return em.client.Grant(c, ttl)
	})
	if err != nil {
		return 0, err
	}
	return resp.ID, nil
}

func (em *EtcdModelImpl) KeepAliveLease(ctx context.Context, id clientv3.LeaseID) error {
	c, cancel := context.WithTimeout(ctx, em.queryTimeout)
	defer cancel()
	_, err := withLatency("keep_alive", func() (*clientv3.LeaseKeepAliveResponse, error) {
		return em.client.KeepAliveOnce(c, id)
	})
	return err
}

func (em *EtcdModelImpl) RevokeLease(ctx context.Context, id clientv3.LeaseID) error {
	c, cancel := context.WithTimeout(ctx, em.queryTimeout)
	defer cancel()
	_, err := withLatency("revoke", func() (*clientv3.LeaseRevokeResponse, error) {
		return em.client.Revoke(c, id)
	})
	return err
}

func withLatency[R any](method string, f func() (R, error)) (R, error) {
	start := time.Now()
	resp, err := f()
	latency := time.Since(start)
	if err != nil {
		etcdReqSeconds.With(prometheus.Labels{"method": method, "status": "failed"}).Observe(latency.Seconds())
		return resp, err
	}
	etcdReqSeconds.With(prometheus.Labels{"method": method, "status": "ok"}).Observe(latency.Seconds())
	return resp, err
}
