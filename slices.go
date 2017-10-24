package streamsort

import (
	"sort"
)

type bufferSlice struct {
	n int
	s [][]byte
}

// Append appends a data slice
func (b *bufferSlice) Append(data []byte) {
	b.n += len(data)

	next := len(b.s)
	if sz := next + 1; sz <= cap(b.s) {
		b.s = b.s[:sz]
	} else {
		b.s = append(b.s, make([]byte, 0, len(data)))
	}
	b.s[next] = append(b.s[next][:0], data...)
}

// Len return the buffer length
func (b *bufferSlice) Len() int { return b.n }

// Reset resets the buffer
func (b *bufferSlice) Reset() {
	b.n = 0
	b.s = b.s[:0]
}

// Sort sorts buffer contents
func (b *bufferSlice) Sort(c Comparer) {
	sort.Slice(b.s, func(i, j int) bool {
		return c.Compare(b.s[i], b.s[j]) < 0
	})
}

// --------------------------------------------------------------------

type sortedItem struct {
	orig int
	data []byte
}

type sortedSlice []sortedItem

// Insert adds an item to the slice
func (s sortedSlice) Insert(orig int, data []byte, c Comparer) sortedSlice {
	item := sortedItem{orig: orig, data: data}

	if pos := sort.Search(len(s), func(i int) bool { return c.Compare(s[i].data, item.data) < 0 }); pos < len(s) {
		s = append(s, sortedItem{})
		copy(s[pos+1:], s[pos:])
		s[pos] = item
	} else {
		s = append(s, item)
	}
	return s
}
