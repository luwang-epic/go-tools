package executor

import (
	"go.uber.org/zap"

	"go-tools/scheduler-by-etcd-and-kafka/pb"
	"go-tools/scheduler-by-etcd-and-kafka/worker/util"
)

type Executor struct {
	logger *zap.Logger
}

func NewExecutor(logger *zap.Logger) *Executor {
	return &Executor{
		logger: logger,
	}
}

func (e *Executor) Run(task *pb.AlarmTaskProto) {
	e.logger.Info("run alarm task in routine", zap.Uint64("id", util.GetGoroutineID()))
	e.logger.Info("run alarm task", zap.Any("task", task))
}
