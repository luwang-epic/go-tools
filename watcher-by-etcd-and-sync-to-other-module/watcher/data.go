package watcher

import (
	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/v3"

	"go-tools/watcher-by-etcd/pb"
)

type EtcdEvent struct {
	EventType mvccpb.Event_EventType
	Key       string
	Value     []byte
	Revision  int64
	LeaseID   clientv3.LeaseID
}

type VersionedMessage struct {
	Key      string
	Message  proto.Message
	Revision int64
}

type TransactionOperator struct {
	Adds    []VersionedMessage
	Updates []VersionedMessage
	Deletes []VersionedMessage
}

type VersionedUser struct {
	User     *pb.UserProto
	Key      string
	Revision int64
}
