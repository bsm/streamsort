package streamsort

import (
	"bytes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("bufferSlice", func() {

	It("should append (and copy)", func() {
		s := new(bufferSlice)
		b := []byte("foo")

		s.Append(b)
		b[0] = 'b'
		s.Append(b)
		b[1] = 'a'
		s.Append(b)
		b[2] = 'r'
		s.Append(b)

		Expect(s.Len()).To(Equal(12))
		Expect(s.s).To(Equal([][]byte{
			[]byte("foo"),
			[]byte("boo"),
			[]byte("bao"),
			[]byte("bar"),
		}))

		s.Reset()
		Expect(s.Len()).To(Equal(0))
		Expect(s.s).To(Equal([][]byte{}))
	})

})
var _ = Describe("sortedSlice", func() {
	c := ComparerFunc(bytes.Compare)

	It("should write and read data", func() {
		var s sortedSlice

		s = s.Insert(1, []byte("foo"), c)
		s = s.Insert(2, []byte("bar"), c)
		s = s.Insert(3, []byte("baz"), c)
		s = s.Insert(4, []byte("dau"), c)

		Expect(s).To(Equal(sortedSlice{
			{orig: 1, data: []byte("foo")},
			{orig: 4, data: []byte("dau")},
			{orig: 3, data: []byte("baz")},
			{orig: 2, data: []byte("bar")},
		}))
	})

})
