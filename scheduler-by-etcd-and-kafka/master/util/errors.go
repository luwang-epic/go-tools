package util

import "fmt"

type RevisionConflictError struct {
	Expect int64
	Given  int64
}

func (e *RevisionConflictError) Error() string {
	return fmt.Sprintf("revision conflicts, expect=%d, given=%d", e.Expect, e.Given)
}

type EtcdKeyExistsError struct {
	Key string
}

func (e *EtcdKeyExistsError) Error() string {
	return fmt.Sprintf("etcd key exists, key=%s", e.Key)
}

type EtcdKeyNotFoundError struct {
	Key string
}

func (e *EtcdKeyNotFoundError) Error() string {
	return fmt.Sprintf("etcd key not found, key=%s", e.Key)
}

type IndexExistsError struct {
}

func (e *IndexExistsError) Error() string {
	return fmt.Sprintf("index exists")
}

type IndexNotFoundError struct {
}

func (e *IndexNotFoundError) Error() string {
	return fmt.Sprintf("index not found")
}

type LogStoreNotFoundError struct {
}

func (e *LogStoreNotFoundError) Error() string {
	return fmt.Sprintf("logstore not found")
}

type AlarmPolicyNotFoundError struct {
}

func (e *AlarmPolicyNotFoundError) Error() string {
	return fmt.Sprintf("alarmPolicy not found")
}
