package election

import (
	"context"
	"sync"
	"time"

	"github.com/hashicorp/go-uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

type Elector struct {
	logger      *zap.Logger
	cli         *clientv3.Client
	wg          *sync.WaitGroup
	electionKey string
	id          string
	routines    []LeaderRoutine
}

type LeaderRoutine interface {
	Start(ctx context.Context)
	Stop()
}

var (
	masterFlag = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "oos",
		Name:      "master",
		Help:      "Whether is master node",
	})
)

func NewElector(logger *zap.Logger, cli *clientv3.Client, key string) *Elector {
	c := &Elector{
		logger:      logger,
		cli:         cli,
		wg:          &sync.WaitGroup{},
		electionKey: key,
	}
	id, err := uuid.GenerateUUID()
	if err != nil {
		logger.Panic("fail to generate id for election coordinator", zap.Error(err))
	}
	c.id = id
	return c
}

func (e *Elector) RegisterLeaderRoutine(routine LeaderRoutine) {
	// The routine registered first will be the first to start and the last to stop.
	e.routines = append(e.routines, routine)
}

func (e *Elector) Start(ctx context.Context) {
	e.logger.Info("leader election is going to start")
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		for {
			e.coordinate(ctx)
			select {
			case <-ctx.Done():
				// Quit if the context is canceled.
				return
			default:
				// Some other error, wait for a while.
				time.Sleep(5 * time.Second)
			}
		}
	}()
}

func (e *Elector) coordinate(ctx context.Context) {
	e.logger.Info("start to participate in leader election")
	s, err := concurrency.NewSession(e.cli, concurrency.WithContext(ctx))
	if err != nil {
		e.logger.Warn("fail to new etcd session", zap.Error(err))
		return
	}
	defer func() { _ = s.Close() }()

	election := concurrency.NewElection(s, e.electionKey)
	err = election.Campaign(ctx, e.id)
	if err != nil {
		select {
		case <-ctx.Done():
		default:
			e.logger.Warn("fail to campaign", zap.Error(err))
		}
		return
	}
	e.logger.Info("succeed to be elected to leader", zap.String("id", e.id))
	masterFlag.Set(1)

	// Start all services bound to leader.
	routineContext, cancel := context.WithCancel(context.Background())
	e.startRoutines(routineContext)

	// Wait until the session is disrupted.
	<-s.Done()

	// Stop dag scheduler.
	cancel()
	e.stopRoutines()
	masterFlag.Set(0)

	// Resign so other candidate can be elected.
	tCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := election.Resign(tCtx); err != nil {
		e.logger.Warn("fail to resign from leader")
	} else {
		e.logger.Info("succeed to resign from leader")
	}
}

func (e *Elector) Stop() {
	e.logger.Info("leader election is going to stop")
	e.wg.Wait()
}

func (e *Elector) startRoutines(ctx context.Context) {
	for _, routine := range e.routines {
		routine.Start(ctx)
	}
}

func (e *Elector) stopRoutines() {
	for i := len(e.routines) - 1; i >= 0; i-- {
		e.routines[i].Stop()
	}
}
