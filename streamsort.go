package streamsort

import (
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
	return newIterator(s.files, s.opt.Comparer)
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

	s.buf.Sort(s.opt.Comparer)
	s.files = append(s.files, f.Name())
	s.wrt.Reset(f)

	for _, data := range s.buf.s {
		if err := s.wrt.Append(data); err != nil {
			return err
		}
	}

	s.buf.Reset()
	return nil
}
