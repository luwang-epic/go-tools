package index

import (
	"sync"

	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/api/v3/mvccpb"

	"go-tools/scheduler-by-etcd-and-kafka/master/model"
	"go-tools/scheduler-by-etcd-and-kafka/pb"
)

type AlarmPolicyIndex interface {
	Get(userid string, id string) (*model.VersionedAlarmPolicy, bool)
	Notify(userid string, id string)
}

type AlarmPolicyIndexImpl struct {
	model.AlarmPolicySubject

	userIndex UserAlarmPolicyIndex
	lock      sync.Mutex
}

type UserAlarmPolicyIndex map[string]struct {
	policies map[string]model.VersionedAlarmPolicy
	nameIDs  map[string]string
}

func NewAlarmPolicyIndex() *AlarmPolicyIndexImpl {
	return &AlarmPolicyIndexImpl{
		userIndex: make(UserAlarmPolicyIndex),
	}
}

func (ai *AlarmPolicyIndexImpl) OnAlarmPolicyBase(vPolicies []model.VersionedAlarmPolicy) {
	ai.lock.Lock()
	for _, vp := range vPolicies {
		ai.updateIndex(vp, mvccpb.PUT)
	}
	ai.lock.Unlock()
	ai.NotifyBase(vPolicies)
}

func (ai *AlarmPolicyIndexImpl) OnAlarmPolicyInc(vPolicy model.VersionedAlarmPolicy, eventType mvccpb.Event_EventType) {
	ai.lock.Lock()
	updated := ai.updateIndex(vPolicy, eventType)
	ai.lock.Unlock()
	if updated {
		ai.NotifyInc(vPolicy, eventType)
	}
}

func (ai *AlarmPolicyIndexImpl) updateIndex(vp model.VersionedAlarmPolicy, eventType mvccpb.Event_EventType) bool {
	policy := vp.Policy
	index, ok := ai.userIndex[policy.GetUserid()]
	if !ok {
		index.policies = make(map[string]model.VersionedAlarmPolicy)
		index.nameIDs = make(map[string]string)
		ai.userIndex[policy.GetUserid()] = index
	}
	if old, ok := index.policies[policy.GetId()]; ok && old.Revision >= vp.Revision {
		return false
	}
	if eventType == mvccpb.PUT {
		index.policies[policy.GetId()] = vp
		index.nameIDs[policy.GetName()] = policy.GetId()
	} else {
		delete(index.policies, policy.GetId())
		delete(index.nameIDs, policy.GetName())
	}
	return true
}

func (ai *AlarmPolicyIndexImpl) Get(userid string, id string) (*model.VersionedAlarmPolicy, bool) {
	ai.lock.Lock()
	defer ai.lock.Unlock()
	index, ok := ai.userIndex[userid]
	if !ok {
		return nil, false
	}
	vp, ok := index.policies[id]
	if !ok {
		return nil, false
	}
	return &model.VersionedAlarmPolicy{
		Policy:   proto.Clone(vp.Policy).(*pb.AlarmPolicyProto),
		Key:      vp.Key,
		Revision: vp.Revision,
	}, true
}

func (ai *AlarmPolicyIndexImpl) Notify(userid string, id string) {
	if vp, ok := ai.Get(userid, id); ok {
		ai.NotifyInc(*vp, mvccpb.PUT)
	}
}
