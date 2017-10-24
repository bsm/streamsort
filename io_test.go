package streamsort

import (
	"bytes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("writer/reader", func() {

	It("should write and read data", func() {
		b := new(bytes.Buffer)
		w := new(writer)
		w.Reset(b)

		Expect(w.Append([]byte("foo"))).To(Succeed())
		Expect(w.Append([]byte("bar"))).To(Succeed())
		Expect(w.Append([]byte("baz"))).To(Succeed())
		Expect(w.Append([]byte("dau"))).To(Succeed())

		r := newReader(b)
		Expect(r.Next()).To(BeTrue())
		Expect(r.Text()).To(Equal("foo"))
		Expect(r.Next()).To(BeTrue())
		Expect(r.Text()).To(Equal("bar"))
		Expect(r.Next()).To(BeTrue())
		Expect(r.Text()).To(Equal("baz"))
		Expect(r.Next()).To(BeTrue())
		Expect(r.Text()).To(Equal("dau"))
		Expect(r.Next()).To(BeFalse())

		Expect(r.Err()).NotTo(HaveOccurred())
	})

})
