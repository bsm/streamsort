package streamsort

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
)

var errCRCMismatch = errors.New("streamsort: bad data stream")

type reader struct {
	src *bufio.Reader
	cur []byte
	err error
	tmp [4]byte
}

func newReader(r io.Reader) *reader {
	return &reader{src: bufio.NewReader(r)}
}

// Next advances the cursor to the next
// chunk.
func (r *reader) Next() bool {
	if r.err != nil {
		return false
	}

	n64, err := binary.ReadVarint(r.src)
	if err != nil {
		r.err = err
		return false
	}

	n := int(n64)
	if cap(r.cur) < n {
		r.cur = make([]byte, n)
	} else {
		r.cur = r.cur[:n]
	}

	if _, err := io.ReadFull(r.src, r.cur); err != nil {
		r.err = err
		return false
	}
	if _, err := io.ReadFull(r.src, r.tmp[:4]); err != nil {
		r.err = err
		return false
	}

	if crc32.ChecksumIEEE(r.cur) != binary.LittleEndian.Uint32(r.tmp[:4]) {
		r.err = errCRCMismatch
		return false
	}

	return true
}

// Bytes returns a current chunk.
func (r *reader) Bytes() []byte {
	if r.err != nil {
		return nil
	}
	return r.cur
}

// Text returns a current chunk as a string.
func (r *reader) Text() string {
	return string(r.Bytes())
}

// Err returns a reader error.
func (r *reader) Err() error {
	if r.err == io.EOF {
		return nil
	}
	return r.err
}

// --------------------------------------------------------------------

type fileReader struct {
	*reader
	f *os.File
	z *gzip.Reader
}

func openFile(name string, comp Compression) (*fileReader, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	fr := fileReader{f: f}
	switch comp {
	case CompressionGzip:
		z, err := gzip.NewReader(f)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		fr.z = z
		fr.reader = newReader(z)
	default:
		fr.reader = newReader(f)
	}
	return &fr, nil
}

// Close closes the reader
func (r *fileReader) Close() (err error) {
	if r.z != nil {
		if e := r.z.Close(); e != nil {
			err = e
		}
	}
	if e := r.f.Close(); e != nil {
		err = e
	}
	return
}

// --------------------------------------------------------------------

type writer struct {
	out io.Writer
	tmp [binary.MaxVarintLen64]byte
}

func newWriter(out io.Writer) *writer {
	return &writer{out: out}
}

// Append appends data to the writer
func (w *writer) Append(data []byte) error {
	n := binary.PutVarint(w.tmp[:], int64(len(data)))
	if _, err := w.out.Write(w.tmp[:n]); err != nil {
		return err
	}
	if _, err := w.out.Write(data); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(w.tmp[:4], crc32.ChecksumIEEE(data))
	if _, err := w.out.Write(w.tmp[:4]); err != nil {
		return err
	}
	return nil
}

// Reset resets the writer
func (w *writer) Reset(out io.Writer) { w.out = out }

// --------------------------------------------------------------------

type fileWriter struct {
	*writer
	f *os.File
	z *gzip.Writer
}

func createFile(dir string, comp Compression) (*fileWriter, error) {
	f, err := ioutil.TempFile(dir, "streamsort.")
	if err != nil {
		return nil, err
	}

	fw := fileWriter{f: f}
	switch comp {
	case CompressionGzip:
		fw.z = gzip.NewWriter(f)
		fw.writer = newWriter(fw.z)
	default:
		fw.writer = newWriter(fw.f)
	}
	return &fw, nil
}

// Name returns the file name
func (w *fileWriter) Name() string { return w.f.Name() }

// Flush flushes the remaining buffer
func (w *fileWriter) Flush() error {
	if w.z != nil {
		return w.z.Flush()
	}
	return nil
}

// Close closes the writer
func (w *fileWriter) Close() (err error) {
	if w.z != nil {
		if e := w.z.Close(); e != nil {
			err = e
		}
	}
	if e := w.f.Close(); e != nil {
		err = e
	}
	return
}
