package streamsort

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
)

var _ = Describe("Sorter", func() {
	var subject *Sorter
	var workDir string

	BeforeEach(func() {
		var err error
		workDir, err = ioutil.TempDir("", "streamsort-test")
		Expect(err).NotTo(HaveOccurred())

		subject = New(&Options{
			MaxMemBuffer: 1024 * 1024,
			TempDir:      workDir,
			Compression:  CompressionGzip,
			MaxOpenFiles: 4,
		})
	})

	AfterEach(func() {
		Expect(subject.Close()).To(Succeed())
		Expect(filepath.Glob(workDir + "/*")).To(BeEmpty())
		Expect(os.RemoveAll(workDir)).To(Succeed())
	})

	It("should append/sort data", func() {
		Expect(subject.Append([]byte("foo"))).To(Succeed())
		Expect(subject.Append([]byte("bar"))).To(Succeed())
		Expect(subject.Append([]byte("baz"))).To(Succeed())
		Expect(subject.Append([]byte("dau"))).To(Succeed())

		it, err := subject.Sort(context.Background())
		Expect(err).NotTo(HaveOccurred())
		defer it.Close()

		Expect(it.Next()).To(BeTrue())
		Expect(string(it.Bytes())).To(Equal("bar"))
		Expect(it.Next()).To(BeTrue())
		Expect(string(it.Bytes())).To(Equal("baz"))
		Expect(it.Next()).To(BeTrue())
		Expect(string(it.Bytes())).To(Equal("dau"))
		Expect(it.Next()).To(BeTrue())
		Expect(string(it.Bytes())).To(Equal("foo"))
		Expect(it.Next()).To(BeFalse())

		Expect(it.Err()).NotTo(HaveOccurred())
	})

	It("should append/sort large data sets", func() {
		rnd := rand.New(rand.NewSource(33))
		b64 := base64.NewEncoding("0123456789@ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz~").WithPadding(base64.NoPadding)
		buf := make([]byte, 100)
		val := make([]byte, b64.EncodedLen(len(buf)))

		for i := 0; i < 2*1e5; i++ {
			buf = buf[:50+rnd.Intn(50)]
			val = val[:b64.EncodedLen(len(buf))]

			_, err := rnd.Read(buf)
			Expect(err).NotTo(HaveOccurred())

			b64.Encode(val, buf)
			Expect(subject.Append(val)).To(Succeed())
		}
		Expect(subject.fnames).To(HaveLen(18))

		it, err := subject.Sort(context.Background())
		Expect(err).NotTo(HaveOccurred())
		defer it.Close()
		Expect(subject.fnames).To(HaveLen(3))

		var prev []byte
		for it.Next() {
			Expect(prev).To(bytesCompareWith(-1, it.Bytes()))
			prev = append(prev[:0], it.Bytes()...)
		}
		Expect(it.Err()).NotTo(HaveOccurred())
	})

})

// --------------------------------------------------------------------

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "streamsort")
}

func bytesCompareWith(v int, b []byte) types.GomegaMatcher {
	return &bytesCompareWithMatcher{b: b, v: v}
}

type bytesCompareWithMatcher struct {
	b []byte
	v int
}

func (m *bytesCompareWithMatcher) Match(actual interface{}) (bool, error) {
	a, ok := actual.([]byte)
	if !ok {
		return false, fmt.Errorf("bytesCompareWith matcher expects []byte")
	}
	return bytes.Compare(a, m.b) == m.v, nil
}

func (m *bytesCompareWithMatcher) FailureMessage(a interface{}) string {
	return fmt.Sprintf("Expected\n\tbytes.Compare(%q, %q)\nto return\n\t%#v", a, m.b, m.v)
}

func (m *bytesCompareWithMatcher) NegatedFailureMessage(a interface{}) string {
	return fmt.Sprintf("Expected\n\tbytes.Compare(%q, %q)\nnot to return\n\t%#v", a, m.b, m.v)
}
