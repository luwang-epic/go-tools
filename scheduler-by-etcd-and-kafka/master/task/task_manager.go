package alarm

import (
	"context"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"

	"go-tools/scheduler-by-etcd-and-kafka/master/index"
	"go-tools/scheduler-by-etcd-and-kafka/master/kafka"
	"go-tools/scheduler-by-etcd-and-kafka/master/model"
	"go-tools/scheduler-by-etcd-and-kafka/master/util"
	"go-tools/scheduler-by-etcd-and-kafka/pb"
)

type TaskManager struct {
	logger *zap.Logger
	heap   *util.MinHeap[string, model.VersionedAlarmPolicy]
	lock   sync.Mutex
	wakeCh chan bool
	am     model.AlarmPolicyModel
	ai     index.AlarmPolicyIndex

	producer kafka.Producer
	topic    string
}

var (
	alarmPolicyScheCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "alarm",
		Name:      "policy_sche_count",
		Help:      "The alarm policy sche count",
	}, []string{"status"})
	alarmTaskScheCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "alarm",
		Name:      "task_sche_count",
		Help:      "The alarm task sche count",
	}, []string{"status"})
)

func NewTaskManager(logger *zap.Logger, am model.AlarmPolicyModel, ai index.AlarmPolicyIndex,
	producer kafka.Producer, topic string) *TaskManager {
	return &TaskManager{
		logger: logger,
		heap: util.NewMinHeap[string, model.VersionedAlarmPolicy](
			func(vp model.VersionedAlarmPolicy) string { return vp.Policy.GetId() },
			func(l model.VersionedAlarmPolicy, r model.VersionedAlarmPolicy) bool {
				return l.Policy.GetNextScheduleTimestamp() < r.Policy.GetNextScheduleTimestamp()
			}),
		wakeCh:   make(chan bool, 1),
		am:       am,
		ai:       ai,
		producer: producer,
		topic:    topic,
	}
}

func (tm *TaskManager) Peek() int64 {
	tm.lock.Lock()
	defer tm.lock.Unlock()
	if p, ok := tm.heap.Peak(); ok {
		return p.Policy.GetNextScheduleTimestamp()
	}
	return 0
}

func (tm *TaskManager) Run(ctx context.Context) error {
	tm.lock.Lock()
	vPolicy, ok := tm.heap.Peak()
	if !ok || vPolicy.Policy.GetNextScheduleTimestamp() > time.Now().UnixMilli() {
		tm.lock.Unlock()
		return nil
	}
	tm.heap.Pop()
	tm.lock.Unlock()

	policy := vPolicy.Policy
	logger := tm.logger.With(zap.String("userid", policy.GetUserid()),
		zap.String("name", policy.GetName()),
		zap.String("scheduleTime", util.TimeFormat(policy.GetNextScheduleTimestamp())))

	task := &pb.AlarmTaskProto{
		Policy:            policy,
		ScheduleTimestamp: policy.GetNextScheduleTimestamp(),
	}

	if value, err := proto.Marshal(task); err == nil {
		tm.producer.Produce(tm.topic, policy.GetId(), value)
		tm.logger.Info("produce scheduler task", zap.Any("task", task))
		alarmTaskScheCount.With(prometheus.Labels{"status": "ok"}).Inc()
	} else {
		alarmTaskScheCount.With(prometheus.Labels{"status": "fail"}).Inc()
	}

	policy.NextScheduleTimestamp = util.GetNextScheduleTimestamp(policy)
	// Use model to update policy as we don't want to modify updated timestamp
	rev, err := tm.am.Update(ctx, policy, vPolicy.Revision)
	if err != nil {
		// If succeeding to update etcd, the updated policy will be inserted into heap by watching mechanism.
		// Otherwise, we have to re-insert the latest policy back to heap to assure the policy isn't lost.
		tm.ai.Notify(policy.GetUserid(), policy.GetId())
		logger.Warn("fail to update policy", zap.Error(err))
		alarmPolicyScheCount.With(prometheus.Labels{"status": "fail"}).Inc()
		return err
	}
	logger.Info("updated alarm policy nextScheduleTime", zap.Int64("rev", rev),
		zap.String("nextScheduleTime", util.TimeFormat(policy.GetNextScheduleTimestamp())))
	alarmPolicyScheCount.With(prometheus.Labels{"status": "ok"}).Inc()
	return nil
}

func (tm *TaskManager) WakeCh() chan bool {
	return tm.wakeCh
}

func (tm *TaskManager) OnAlarmPolicyBase(vPolicies []model.VersionedAlarmPolicy) {
	tm.lock.Lock()
	for _, vp := range vPolicies {
		// 只需要加入enable状态的报警策略
		if vp.Policy.State == pb.AlarmPolicyProto_AS_ENABLED {
			tm.heap.Insert(vp)
		}
	}
	tm.lock.Unlock()
	tm.wake()
}

func (tm *TaskManager) OnAlarmPolicyInc(vPolicy model.VersionedAlarmPolicy, eventType mvccpb.Event_EventType) {
	tm.lock.Lock()
	// 只有enable状态才会加入到堆中，如果是disable的状态，从堆中移除
	if eventType == mvccpb.PUT && vPolicy.Policy.State == pb.AlarmPolicyProto_AS_ENABLED {
		tm.heap.Insert(vPolicy)
	} else {
		tm.heap.Remove(vPolicy)
	}
	tm.lock.Unlock()
	tm.wake()
}

func (tm *TaskManager) wake() {
	select {
	case tm.wakeCh <- true:
	default:
	}
}
