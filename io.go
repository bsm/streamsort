package streamsort

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
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

type writer struct {
	w   io.Writer
	tmp [binary.MaxVarintLen64]byte
}

// Append appends data to the writer
func (w *writer) Append(data []byte) error {
	n := binary.PutVarint(w.tmp[:], int64(len(data)))
	if _, err := w.w.Write(w.tmp[:n]); err != nil {
		return err
	}
	if _, err := w.w.Write(data); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(w.tmp[:4], crc32.ChecksumIEEE(data))
	if _, err := w.w.Write(w.tmp[:4]); err != nil {
		return err
	}
	return nil
}

// Reset resets the writer
func (w *writer) Reset(wr io.Writer) { w.w = wr }
