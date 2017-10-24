package streamsort

import (
	"errors"
	"os"
)

var errClosed = errors.New("streamsort: iterator is closed")

// Iterator allows to iterate over sorted outputs
type Iterator struct {
	files    []*os.File
	readers  []*reader
	items    sortedSlice
	comparer Comparer

	cur []byte
	err error
}

func newIterator(names []string, c Comparer) (*Iterator, error) {
	iter := &Iterator{
		files:    make([]*os.File, 0, len(names)),
		readers:  make([]*reader, 0, len(names)),
		items:    make(sortedSlice, 0, len(names)),
		comparer: c,
	}

	for _, fname := range names {
		f, err := os.Open(fname)
		if err != nil {
			_ = iter.Close()
			return nil, err
		}

		r := newReader(f)
		o := len(iter.readers)
		iter.files = append(iter.files, f)
		iter.readers = append(iter.readers, r)

		if r.Next() {
			iter.items = iter.items.Insert(o, r.Bytes(), c)
		}
		if err := r.Err(); err != nil {
			_ = iter.Close()
			return nil, err
		}
	}

	return iter, nil
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

	reader := i.readers[item.orig]
	if reader.Next() {
		i.items = i.items.Insert(item.orig, reader.Bytes(), i.comparer)
	}
	if err := reader.Err(); err != nil {
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
