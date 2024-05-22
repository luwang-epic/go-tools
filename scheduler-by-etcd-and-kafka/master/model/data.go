package model

import (
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"go-tools/scheduler-by-etcd-and-kafka/pb"
)

type EtcdEvent struct {
	EventType mvccpb.Event_EventType
	Key       string
	Value     []byte
	Revision  int64
	LeaseID   clientv3.LeaseID
}

type VersionedAlarmPolicy struct {
	Policy   *pb.AlarmPolicyProto
	Key      string
	Revision int64
}
