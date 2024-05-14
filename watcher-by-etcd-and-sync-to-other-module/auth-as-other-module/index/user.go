package index

import (
	"strings"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"go-tools/watcher-by-etcd-and-sync-to-other-module/pb"
)

type UserIndex interface {
	Get(name string) (VersionedUser, bool)
}

type AlarmPolicyIndexImpl struct {
	logger        *zap.Logger
	userKeyPrefix string
	userIndex     sync.Map
}

type VersionedUser struct {
	User     *pb.UserProto
	Key      string
	Revision int64
}

func NewUserIndex(logger *zap.Logger, userKeyPrefix string) *AlarmPolicyIndexImpl {
	return &AlarmPolicyIndexImpl{
		logger:        logger,
		userKeyPrefix: userKeyPrefix,
	}
}

func (idx *AlarmPolicyIndexImpl) OnWatchEvent(e *pb.WatchEventProto) {
	if strings.HasPrefix(e.GetKey(), idx.userKeyPrefix) {
		idx.OnAlarmPolicy(e.GetMessage(), e.GetRevision(), e.GetType())
	}
}

func (idx *AlarmPolicyIndexImpl) OnAlarmPolicy(message []byte, revision int64, eventType pb.WatchEventProto_EventType) {
	user := &pb.UserProto{}
	if err := proto.Unmarshal(message, user); err != nil {
		idx.logger.Warn("fail to unmarshal UserProto", zap.Error(err))
		return
	}

	var vUser VersionedUser
	if old, ok := idx.userIndex.Load(user.Name); ok {
		vUser = old.(VersionedUser)
		if vUser.Revision >= revision {
			return
		}
	} else {
		vUser = VersionedUser{}
	}

	if eventType == pb.WatchEventProto_ET_DELETE {
		idx.userIndex.Delete(user.Name)
		idx.printUsers()
		return
	}
	vUser.User = user
	vUser.Revision = revision
	idx.userIndex.Store(user.Name, vUser)
	idx.printUsers()
}

func (idx *AlarmPolicyIndexImpl) printUsers() {
	pf := func(key, value any) bool {
		idx.logger.Debug("auth as other module user index", zap.Any("user", value))
		return true
	}
	idx.userIndex.Range(pf)
	idx.logger.Debug("================ end ================")
}

func (idx *AlarmPolicyIndexImpl) Get(name string) (VersionedUser, bool) {
	v, ok := idx.userIndex.Load(name)
	if !ok {
		return VersionedUser{}, false
	}
	return v.(VersionedUser), true
}
