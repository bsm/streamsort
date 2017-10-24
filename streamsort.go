package streamsort

import (
	"compress/gzip"
	"io"
	"io/ioutil"
	"os"
)

// Sorter is responsible for sorting a stream
type Sorter struct {
	opt Options
	buf bufferSlice
	wrt writer

	files []string
}

// New inits a sorter
func New(opt *Options) *Sorter {
	s := new(Sorter)
	if opt != nil {
		s.opt = *opt
	}
	s.opt.norm()
	return s
}

// Append appends data to the sorter
func (s *Sorter) Append(data []byte) error {
	if sz := s.buf.Len(); sz > 0 && sz+len(data) > s.opt.MaxMemBuffer {
		if err := s.flush(); err != nil {
			return err
		}
	}

	s.buf.Append(data)
	return nil
}

// Sort sorts the inputs and returns an interator output iterator.
// You must close the iterator after use.
func (s *Sorter) Sort() (*Iterator, error) {
	if s.buf.Len() > 0 {
		if err := s.flush(); err != nil {
			return nil, err
		}
	}

	iter := &Iterator{
		files:   make([]*os.File, 0, len(s.files)),
		readers: make([]*reader, 0, len(s.files)),
		items:   make(sortedSlice, 0, len(s.files)),

		comparer:    s.opt.Comparer,
		compression: s.opt.Compression,
	}

	for _, fname := range s.files {
		f, err := os.Open(fname)
		if err != nil {
			_ = iter.Close()
			return nil, err
		}

		r, o, err := iter.addFile(f)
		if err != nil {
			_ = iter.Close()
			return nil, err
		}

		if r.Next() {
			iter.append(o, r.Bytes())
		}
		if err := r.Err(); err != nil {
			_ = iter.Close()
			return nil, err
		}
	}

	return iter, nil
}

// Close stops the processing and removes all temporary files
func (s *Sorter) Close() error {
	var err error

	for _, f := range s.files {
		if e := os.Remove(f); e != nil {
			err = e
		}
	}
	s.files = s.files[:0]

	return err
}

func (s *Sorter) flush() error {
	f, err := ioutil.TempFile(s.opt.TempDir, "streamsort")
	if err != nil {
		return err
	}
	defer f.Close()

	var (
		w io.Writer
		z *gzip.Writer
	)

	switch s.opt.Compression {
	case CompressionGzip:
		z = gzip.NewWriter(f)
		defer z.Close()

		w = z
	default:
		w = f
	}

	s.buf.Sort(s.opt.Comparer)
	s.files = append(s.files, f.Name())
	s.wrt.Reset(w)

	for _, b := range s.buf.s {
		if err := s.wrt.Append(b); err != nil {
			return err
		}
	}

	if z != nil {
		if err := z.Flush(); err != nil {
			return err
		}
	}

	s.buf.Reset()
	return nil
}
