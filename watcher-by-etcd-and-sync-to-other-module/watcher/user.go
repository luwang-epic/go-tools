package watcher

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"

	"go-tools/watcher-by-etcd/pb"
)

// UserModel 用户接口，用户修改用户信息，并通知其他需要用户信息的类，比如UserIndex
type UserModel interface {
	Add(ctx context.Context, user *pb.UserProto) (int64, error)
	Update(ctx context.Context, user *pb.UserProto, preRevision int64) (int64, error)
	Get(ctx context.Context, name string) (*VersionedUser, error)
	Delete(ctx context.Context, name string) (int64, error)
}

type UserModelImpl struct {
	UserSubject

	logger           *zap.Logger
	em               EtcdModel
	keyPrefix        string
	deletedKeyPrefix string
}

func NewUserModel(logger *zap.Logger, em EtcdModel, keyPrefix string) *UserModelImpl {
	return &UserModelImpl{
		logger:    logger,
		em:        em,
		keyPrefix: keyPrefix,
	}
}

func (lsm *UserModelImpl) GetWatchPrefix() string {
	return lsm.keyPrefix
}

func (lsm *UserModelImpl) OnEtcdEventBase(events []EtcdEvent) {
	var vUsers []VersionedUser
	for _, event := range events {
		user := &pb.UserProto{}
		if err := proto.Unmarshal(event.Value, user); err != nil {
			lsm.logger.Warn("fail to unmarshal LogStoreProto", zap.Error(err))
			continue
		}
		vUsers = append(vUsers, VersionedUser{User: user, Key: event.Key, Revision: event.Revision})
	}
	lsm.NotifyBase(vUsers)
}

func (lsm *UserModelImpl) OnEtcdEventInc(event EtcdEvent) {
	user := &pb.UserProto{}
	if err := proto.Unmarshal(event.Value, user); err != nil {
		lsm.logger.Warn("fail to unmarshal LogStoreProto", zap.Error(err))
		return
	}
	lsm.NotifyInc(VersionedUser{User: user, Key: event.Key, Revision: event.Revision}, event.EventType)
}

func (lsm *UserModelImpl) Add(ctx context.Context, user *pb.UserProto) (int64, error) {
	key := lsm.key(user.Name)
	rev, err := lsm.em.Add(ctx, key, user)
	if err != nil {
		return 0, err
	}
	lsm.NotifyInc(VersionedUser{User: user, Key: key, Revision: rev}, mvccpb.PUT)
	return rev, nil
}

func (lsm *UserModelImpl) Get(ctx context.Context, name string) (*VersionedUser, error) {
	user := &pb.UserProto{}
	key := lsm.key(name)
	revision, err := lsm.em.Get(ctx, key, user)
	if err != nil {
		return nil, err
	}
	return &VersionedUser{User: user, Key: key, Revision: revision}, nil
}

func (lsm *UserModelImpl) Update(ctx context.Context, user *pb.UserProto, preRevision int64) (int64, error) {
	key := lsm.key(user.GetName())
	rev, err := lsm.em.Update(ctx, key, user, preRevision)
	if err != nil {
		return 0, err
	}
	lsm.NotifyInc(VersionedUser{User: user, Key: key, Revision: rev}, mvccpb.PUT)
	return rev, nil
}

func (lsm *UserModelImpl) Delete(ctx context.Context, name string) (int64, error) {
	key := lsm.key(name)
	user := &pb.UserProto{}
	rev, err := lsm.em.DeleteIgnoreRevision(ctx, key, user)
	if err != nil {
		return 0, err
	}
	lsm.NotifyInc(VersionedUser{User: user, Key: key, Revision: rev}, mvccpb.DELETE)
	return rev, nil
}

func (lsm *UserModelImpl) key(name string) string {
	return fmt.Sprintf("%s%s", lsm.keyPrefix, name)
}
