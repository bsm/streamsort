package streamsort

import (
	"bytes"
	"encoding/base64"
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
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
		})
	})

	AfterEach(func() {
		Expect(subject.Close()).To(Succeed())
		Expect(filepath.Glob(workDir + "/*")).To(BeEmpty())
	})

	It("should append/sort data", func() {
		Expect(subject.Append([]byte("foo"))).To(Succeed())
		Expect(subject.Append([]byte("bar"))).To(Succeed())
		Expect(subject.Append([]byte("baz"))).To(Succeed())
		Expect(subject.Append([]byte("dau"))).To(Succeed())

		it, err := subject.Sort()
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
		if testing.Short() {
			return
		}

		rnd := rand.New(rand.NewSource(33))
		b64 := base64.NewEncoding("0123456789@ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz~").WithPadding(base64.NoPadding)
		buf := make([]byte, 100)
		val := make([]byte, b64.EncodedLen(len(buf)))

		for i := 0; i < 1e5; i++ {
			buf = buf[:50+rnd.Intn(50)]
			val = val[:b64.EncodedLen(len(buf))]

			_, err := rnd.Read(buf)
			Expect(err).NotTo(HaveOccurred())

			b64.Encode(val, buf)
			Expect(subject.Append(val)).To(Succeed())
		}
		Expect(subject.files).To(HaveLen(9))

		it, err := subject.Sort()
		Expect(err).NotTo(HaveOccurred())
		defer it.Close()
		Expect(subject.files).To(HaveLen(10))

		var prev []byte
		for it.Next() {
			Expect(bytes.Compare(prev, it.Bytes())).To(BeNumerically("<", 0), "expected %q to be >= than %q", it.Bytes(), prev)
			prev = append(prev[:0], it.Bytes()...)
		}
		Expect(it.Err()).NotTo(HaveOccurred())
	})

})

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "streamsort")
}
