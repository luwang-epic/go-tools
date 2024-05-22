package kafka

import (
	"github.com/IBM/sarama"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"go-tools/scheduler-by-etcd-and-kafka/pb"
	"go-tools/scheduler-by-etcd-and-kafka/worker/executor"
	"go-tools/scheduler-by-etcd-and-kafka/worker/util"
)

type Handler struct {
	logger                 *zap.Logger
	ready                  chan bool
	executor               *executor.Executor
	executorWorkersLimitCh chan struct{}
}

func NewHandler(logger *zap.Logger, executor *executor.Executor, executorWorkerCount int) *Handler {
	if executorWorkerCount <= 0 {
		executorWorkerCount = 16
	}
	return &Handler{
		logger:                 logger,
		executor:               executor,
		ready:                  make(chan bool),
		executorWorkersLimitCh: make(chan struct{}, executorWorkerCount),
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (h *Handler) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(h.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
// but before the offsets are committed for the very last time.
func (h *Handler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (h *Handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE: Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine.
	for message := range claim.Messages() {
		h.logger.Info("wait to execute executor task",
			zap.Int64("offset", message.Offset), zap.Int32("partition", message.Partition))
		h.logger.Info("call ConsumeClaim method in routine", zap.Uint64("id", util.GetGoroutineID()))
		// 控制并发数，如果并发满了，将会阻塞在这里
		h.executorWorkersLimitCh <- struct{}{}
		go func(msg *sarama.ConsumerMessage) {
			task := &pb.AlarmTaskProto{}
			if err := proto.Unmarshal(msg.Value, task); err != nil {
				h.logger.Warn("fail to parse AlarmTaskProto", zap.Error(err))
				// Continue to process other messages.
			} else {
				h.logger.Info("start to execute executor task", zap.String("policyId", task.GetPolicy().GetId()))
				h.executor.Run(task)
				session.MarkMessage(msg, "")
				h.logger.Info("end to execute executor task", zap.String("policyId", task.GetPolicy().GetId()))
				<-h.executorWorkersLimitCh
			}
		}(message)
	}
	return nil
}
