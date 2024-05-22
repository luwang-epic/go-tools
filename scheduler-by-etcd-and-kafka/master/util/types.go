package util

type BiPredicate[T any, U any] func(t T, u U) bool
type Predicate[T any] func(t T) bool
type Function[T any, R any] func(t T) R
type Consumer[T any] func(t T)
type BiConsumer[T any, U any] func(t T, u U)
