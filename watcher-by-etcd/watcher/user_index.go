package watcher

import (
	"sync"

	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"

	"go-tools/watcher-by-etcd/pb"
)

// UserIndex is an index to access users.
// All methods without 'unsafe' suffix will return cloned objects that are SAFE to modify.
type UserIndex interface {
	Get(name string) (*VersionedUser, bool)
	GetAllUnsafe() []VersionedUser
}

type UserIndexImpl struct {
	// 如果没有对象监听UserIndexImpl中存储的用户信息改变，可以不需要继承UserSubject对象的
	// 一般情况下，这个用户rpc通信，让系统的其他模块可以同步用户信息，如果没有其他模块需要同步用户信息，一般情况下不需要实现
	//UserSubject

	logger    *zap.Logger
	userIndex map[string]VersionedUser
	lock      sync.Mutex
}

func NewUserIndex(logger *zap.Logger) *UserIndexImpl {
	return &UserIndexImpl{
		logger:    logger,
		userIndex: make(map[string]VersionedUser),
	}
}

func (lsi *UserIndexImpl) OnLogStoreBase(vUsers []VersionedUser) {
	lsi.lock.Lock()
	for _, vUser := range vUsers {
		lsi.updateIndex(vUser, mvccpb.PUT)
	}
	lsi.lock.Unlock()

	// 没有继承UserSubject对象，不需要这部分代码
	//lsi.NotifyBase(vUsers)
}

func (lsi *UserIndexImpl) OnLogStoreInc(vUser VersionedUser, eventType mvccpb.Event_EventType) {
	lsi.lock.Lock()
	updated := lsi.updateIndex(vUser, eventType)
	lsi.lock.Unlock()

	if updated {
		// 没有继承UserSubject对象，不需要这部分代码
		//lsi.NotifyInc(vUser, eventType)
	}
}

func (lsi *UserIndexImpl) updateIndex(vUser VersionedUser, eventType mvccpb.Event_EventType) bool {
	user := vUser.User
	index := lsi.userIndex[user.Name]
	// 没有这个key，index.Revision是0，不影响判断
	if index.Revision >= vUser.Revision {
		return false
	}
	if eventType == mvccpb.PUT {
		lsi.userIndex[user.Name] = vUser
	} else {
		delete(lsi.userIndex, user.Name)
	}
	lsi.logger.Debug("user index", zap.Any("index", lsi.userIndex))
	return true
}

func (lsi *UserIndexImpl) Get(name string) (*VersionedUser, bool) {
	lsi.lock.Lock()
	defer lsi.lock.Unlock()
	user, ok := lsi.userIndex[name]
	if !ok {
		return nil, false
	}
	return &VersionedUser{
		User:     proto.Clone(user.User).(*pb.UserProto),
		Key:      user.Key,
		Revision: user.Revision,
	}, true
}

func (lsi *UserIndexImpl) GetAllUnsafe() []VersionedUser {
	lsi.lock.Lock()
	defer lsi.lock.Unlock()
	var users []VersionedUser
	for _, index := range lsi.userIndex {
		users = append(users, index)
	}
	return users
}
