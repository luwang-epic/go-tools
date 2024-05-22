package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMinHeap_Insert(t *testing.T) {
	h := NewMinHeap[int, int](
		func(i int) int { return i },
		func(i, j int) bool { return i < j })
	h.Insert(2)
	h.Insert(3)
	h.Insert(0)
	h.Insert(1)

	for i := 0; i < 4; i++ {
		x, ok := h.Peak()
		require.True(t, ok)
		require.Equal(t, i, x)

		y, ok := h.Pop()
		require.True(t, ok)
		require.Equal(t, i, y)
	}
}
