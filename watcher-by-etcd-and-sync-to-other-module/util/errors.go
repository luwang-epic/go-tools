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
