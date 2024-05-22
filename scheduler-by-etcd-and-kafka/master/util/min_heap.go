package util

import (
	"container/heap"
)

type MinHeap[K comparable, V any] struct {
	keyFunc Function[V, K]
	keys    map[K]bool
	slice   *HeapSlice[V]
}

func NewMinHeap[K comparable, V any](keyFunc Function[V, K], lessFunc BiPredicate[V, V]) *MinHeap[K, V] {
	return &MinHeap[K, V]{
		keyFunc: keyFunc,
		keys:    make(map[K]bool),
		slice:   &HeapSlice[V]{lessFunc: lessFunc},
	}
}

func (h *MinHeap[K, V]) Insert(v V) {
	key := h.keyFunc(v)
	if h.keys[key] {
		for i, old := range h.slice.arr {
			if h.keyFunc(old) == key {
				h.slice.arr[i] = v
				heap.Fix(h.slice, i)
				return
			}
		}
	}
	h.keys[key] = true
	heap.Push(h.slice, v)
}

func (h *MinHeap[K, V]) Remove(v V) {
	key := h.keyFunc(v)
	if !h.keys[key] {
		return
	}
	delete(h.keys, key)
	for i, old := range h.slice.arr {
		if h.keyFunc(old) == key {
			heap.Remove(h.slice, i)
			return
		}
	}
}

func (h *MinHeap[K, V]) Peak() (V, bool) {
	if len(h.slice.arr) == 0 {
		var v V
		return v, false
	}
	return h.slice.arr[0], true
}

func (h *MinHeap[K, V]) Pop() (V, bool) {
	if h.slice.Len() == 0 {
		var v V
		return v, false
	}
	v := heap.Pop(h.slice).(V)
	delete(h.keys, h.keyFunc(v))
	return v, true
}

type HeapSlice[V any] struct {
	lessFunc BiPredicate[V, V]
	arr      []V
}

func (s *HeapSlice[V]) Len() int {
	return len(s.arr)
}

func (s *HeapSlice[V]) Less(i, j int) bool {
	return s.lessFunc(s.arr[i], s.arr[j])
}

func (s *HeapSlice[V]) Swap(i, j int) {
	s.arr[i], s.arr[j] = s.arr[j], s.arr[i]
}

func (s *HeapSlice[V]) Push(v any) {
	s.arr = append(s.arr, v.(V))
}

func (s *HeapSlice[V]) Pop() any {
	n := len(s.arr)
	v := s.arr[n-1]
	s.arr = s.arr[:n-1]
	return v
}
