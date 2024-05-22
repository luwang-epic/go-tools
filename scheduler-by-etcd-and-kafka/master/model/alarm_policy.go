package model

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"

	"go-tools/scheduler-by-etcd-and-kafka/pb"
)

type AlarmPolicyModel interface {
	Create(ctx context.Context, policy *pb.AlarmPolicyProto) (int64, error)
	Update(ctx context.Context, policy *pb.AlarmPolicyProto, preRevision int64) (int64, error)
	UpdateIgnoreRevision(ctx context.Context, policy *pb.AlarmPolicyProto) (int64, error)
	DeleteIgnoreRevision(ctx context.Context, policy *pb.AlarmPolicyProto) (int64, error)
	Get(ctx context.Context, userid string, name string) (*VersionedAlarmPolicy, error)
}

type AlarmPolicyModelImpl struct {
	AlarmPolicySubject

	logger    *zap.Logger
	em        EtcdModel
	keyPrefix string
}

func NewAlarmPolicyModel(logger *zap.Logger, em EtcdModel, keyPrefix string) *AlarmPolicyModelImpl {
	return &AlarmPolicyModelImpl{
		logger:    logger,
		em:        em,
		keyPrefix: keyPrefix,
	}
}

func (am *AlarmPolicyModelImpl) GetWatchPrefix() string {
	return am.keyPrefix
}

func (am *AlarmPolicyModelImpl) OnEtcdEventBase(events []EtcdEvent) {
	var vPolicies []VersionedAlarmPolicy
	for _, event := range events {
		p := &pb.AlarmPolicyProto{}
		if err := proto.Unmarshal(event.Value, p); err != nil {
			am.logger.Warn("fail to unmarshal AlarmPolicyProto", zap.Error(err))
			continue
		}
		vPolicies = append(vPolicies, VersionedAlarmPolicy{Policy: p, Key: event.Key, Revision: event.Revision})
	}
	am.NotifyBase(vPolicies)
}

func (am *AlarmPolicyModelImpl) OnEtcdEventInc(event EtcdEvent) {
	p := &pb.AlarmPolicyProto{}
	if err := proto.Unmarshal(event.Value, p); err != nil {
		am.logger.Warn("fail to unmarshal AlarmPolicyProto", zap.Error(err))
		return
	}
	am.NotifyInc(VersionedAlarmPolicy{Policy: p, Key: event.Key, Revision: event.Revision}, event.EventType)
}

func (am *AlarmPolicyModelImpl) Create(ctx context.Context, policy *pb.AlarmPolicyProto) (int64, error) {
	key := am.key(policy.GetUserid(), policy.GetName())
	rev, err := am.em.Add(ctx, key, policy)
	if err != nil {
		return 0, err
	}
	am.NotifyInc(VersionedAlarmPolicy{Policy: policy, Key: key, Revision: rev}, mvccpb.PUT)
	return rev, nil
}

func (am *AlarmPolicyModelImpl) Update(ctx context.Context, policy *pb.AlarmPolicyProto, preRevision int64) (int64, error) {
	key := am.key(policy.GetUserid(), policy.GetName())
	rev, err := am.em.Update(ctx, key, policy, preRevision)
	if err != nil {
		return 0, err
	}
	am.NotifyInc(VersionedAlarmPolicy{Policy: policy, Key: key, Revision: rev}, mvccpb.PUT)
	return rev, nil
}

func (am *AlarmPolicyModelImpl) UpdateIgnoreRevision(ctx context.Context, policy *pb.AlarmPolicyProto) (int64, error) {
	key := am.key(policy.GetUserid(), policy.GetName())
	rev, err := am.em.UpdateIgnoreRevision(ctx, key, policy)
	if err != nil {
		return 0, err
	}
	am.NotifyInc(VersionedAlarmPolicy{Policy: policy, Key: key, Revision: rev}, mvccpb.PUT)
	return rev, nil
}

func (am *AlarmPolicyModelImpl) DeleteIgnoreRevision(ctx context.Context, policy *pb.AlarmPolicyProto) (int64, error) {
	key := am.key(policy.GetUserid(), policy.GetName())
	rev, err := am.em.DeleteIgnoreRevision(ctx, key, policy)
	if err != nil {
		return 0, err
	}
	am.NotifyInc(VersionedAlarmPolicy{Policy: policy, Key: key, Revision: rev}, mvccpb.DELETE)
	return rev, nil
}

func (am *AlarmPolicyModelImpl) Get(ctx context.Context, userid string, name string) (*VersionedAlarmPolicy, error) {
	policy := &pb.AlarmPolicyProto{}
	key := am.key(userid, name)
	revision, err := am.em.Get(ctx, key, policy)
	if err != nil {
		return nil, err
	}
	return &VersionedAlarmPolicy{Policy: policy, Key: key, Revision: revision}, nil
}

func (am *AlarmPolicyModelImpl) key(userid string, name string) string {
	return fmt.Sprintf("%s%s/%s", am.keyPrefix, userid, name)
}
