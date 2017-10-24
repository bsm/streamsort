package streamsort

import (
	"errors"
)

var errClosed = errors.New("streamsort: iterator is closed")

// Iterator allows to iterate over sorted outputs
type Iterator struct {
	sources  []*fileReader
	stash    sortedSlice
	comparer Comparer

	cur []byte
	err error
}

func newIterator(fnames []string, comparer Comparer, compression Compression) (*Iterator, error) {
	i := &Iterator{
		sources:  make([]*fileReader, 0, len(fnames)),
		comparer: comparer,
	}
	for _, fname := range fnames {
		src, srcID, err := i.openFile(fname, compression)
		if err != nil {
			_ = i.Close()
			return nil, err
		}
		if src.Next() {
			i.insert(srcID, src.Bytes())
		}
		if err := src.Err(); err != nil {
			_ = i.Close()
			return nil, err
		}
	}
	return i, nil
}

func (i *Iterator) insert(srcID int, data []byte) {
	i.stash = i.stash.Insert(srcID, data, i.comparer)
}

func (i *Iterator) openFile(fname string, comp Compression) (*fileReader, int, error) {
	srcID := len(i.sources)
	src, err := openFile(fname, comp)
	if err != nil {
		return nil, 0, err
	}
	i.sources = append(i.sources, src)
	return src, srcID, nil
}

// Next advances the cursor to the next entry
func (i *Iterator) Next() bool {
	if i.err != nil {
		return false
	}

	last := len(i.stash) - 1
	if last < 0 {
		return false
	}

	item := i.stash[last]
	i.stash = i.stash[:last]
	i.cur = append(i.cur[:0], item.data...)

	src := i.sources[item.srcID]
	if src.Next() {
		i.insert(item.srcID, src.Bytes())
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

	for _, src := range i.sources {
		if e := src.Close(); e != nil {
			err = e
		}
	}

	i.sources = i.sources[:0]
	i.err = errClosed

	return err
}
