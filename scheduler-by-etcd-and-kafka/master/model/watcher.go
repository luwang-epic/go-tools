package model

import (
	"go.etcd.io/etcd/api/v3/mvccpb"
)

type AlarmPolicySubject struct {
	watchers []AlarmPolicyWatcher
}

type AlarmPolicyWatcher interface {
	OnAlarmPolicyBase(vPolicies []VersionedAlarmPolicy)
	OnAlarmPolicyInc(vPolicy VersionedAlarmPolicy, eventType mvccpb.Event_EventType)
}

func (s *AlarmPolicySubject) AddWatcher(watcher AlarmPolicyWatcher) {
	s.watchers = append(s.watchers, watcher)
}

func (s *AlarmPolicySubject) NotifyBase(vPolicies []VersionedAlarmPolicy) {
	for _, watcher := range s.watchers {
		watcher.OnAlarmPolicyBase(vPolicies)
	}
}

func (s *AlarmPolicySubject) NotifyInc(vPolicy VersionedAlarmPolicy, eventType mvccpb.Event_EventType) {
	for _, watcher := range s.watchers {
		watcher.OnAlarmPolicyInc(vPolicy, eventType)
	}
}
