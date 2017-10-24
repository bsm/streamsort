package streamsort

import (
	"compress/gzip"
	"errors"
	"os"
)

var errClosed = errors.New("streamsort: iterator is closed")

// Iterator allows to iterate over sorted outputs
type Iterator struct {
	files   []*os.File
	zwraps  []*gzip.Reader
	readers []*reader
	items   sortedSlice

	comparer    Comparer
	compression Compression

	cur []byte
	err error
}

func (i *Iterator) append(orig int, data []byte) {
	i.items = i.items.Insert(orig, data, i.comparer)
}

func (i *Iterator) addFile(f *os.File) (*reader, int, error) {
	o := len(i.readers)
	i.files = append(i.files, f)

	var r *reader
	switch i.compression {
	case CompressionGzip:
		z, err := gzip.NewReader(f)
		if err != nil {
			return nil, 0, err
		}
		i.zwraps = append(i.zwraps, z)
		r = newReader(z)
	default:
		r = newReader(f)
	}
	i.readers = append(i.readers, r)

	return r, o, nil
}

// Next advances the cursor to the next entry
func (i *Iterator) Next() bool {
	if i.err != nil {
		return false
	}

	pos := len(i.items) - 1
	if pos < 0 {
		return false
	}

	item := i.items[pos]
	i.items = i.items[:pos]
	i.cur = append(i.cur[:0], item.data...)

	src := i.readers[item.orig]
	if src.Next() {
		i.items = i.items.Insert(item.orig, src.Bytes(), i.comparer)
	}
	if err := src.Err(); err != nil {
		i.err = err
		return false
	}
	return true
}

// Bytes returns the chunk at the current position
func (i *Iterator) Bytes() []byte {
	if i.err != nil {
		return nil
	}
	return i.cur
}

// Err returns the iterator error
func (i *Iterator) Err() error { return i.err }

// Close closes and releases the iterator
func (i *Iterator) Close() error {
	var err error

	for _, f := range i.files {
		if e := f.Close(); e != nil {
			err = e
		}
	}

	i.files = i.files[:0]
	i.readers = i.readers[:0]
	i.err = errClosed

	return err
}
