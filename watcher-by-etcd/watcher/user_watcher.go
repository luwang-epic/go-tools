package watcher

import "go.etcd.io/etcd/api/v3/mvccpb"

// UserWatcher 用户信息改变的监听
type UserWatcher interface {
	OnLogStoreBase(vUsers []VersionedUser)
	OnLogStoreInc(vUser VersionedUser, eventType mvccpb.Event_EventType)
}

// UserSubject 对用户监听的统一实现，实现了UserWatcher类的对象可以继承该类，从而方便实现监听机制
type UserSubject struct {
	watchers []UserWatcher
}

func (s *UserSubject) AddWatcher(watcher UserWatcher) {
	s.watchers = append(s.watchers, watcher)
}

func (s *UserSubject) NotifyBase(vUsers []VersionedUser) {
	for _, watcher := range s.watchers {
		watcher.OnLogStoreBase(vUsers)
	}
}

func (s *UserSubject) NotifyInc(vUser VersionedUser, eventType mvccpb.Event_EventType) {
	for _, watcher := range s.watchers {
		watcher.OnLogStoreInc(vUser, eventType)
	}
}
