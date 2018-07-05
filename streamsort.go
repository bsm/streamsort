package streamsort

import (
	"context"
	"os"
)

// Sorter is responsible for sorting a stream
type Sorter struct {
	opt Options
	buf bufferSlice

	fnames fileNames
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
func (s *Sorter) Sort(ctx context.Context) (*Iterator, error) {
	if s.buf.Len() > 0 {
		if err := s.flush(); err != nil {
			return nil, err
		}
	}

	perGroup := s.opt.MaxOpenFiles - 1
	for len(s.fnames) > perGroup {
		if err := s.compact(ctx, perGroup); err != nil {
			return nil, err
		}
	}

	return newIterator(s.fnames, s.opt.Comparer, s.opt.Compression)
}

// Close stops the processing and removes all temporary fnames
func (s *Sorter) Close() error {
	err := s.fnames.RemoveAll()
	s.fnames = s.fnames[:0]
	return err
}

func (s *Sorter) compact(ctx context.Context, perGroup int) error {
	result := make(fileNames, 0, len(s.fnames)/perGroup+1)
	for x := 0; x < len(s.fnames); x += perGroup {
		group := s.fnames[x:]
		if len(group) > perGroup {
			group = group[:perGroup]
		}

		fname, err := s.merge(ctx, group)
		if err != nil {
			_ = result.RemoveAll()
			return err
		}

		result = append(result, fname)
	}

	if err := s.fnames.RemoveAll(); err != nil {
		_ = result.RemoveAll()
		return err
	}

	s.fnames = result
	return nil
}

func (s *Sorter) merge(ctx context.Context, names []string) (string, error) {
	it, err := newIterator(names, s.opt.Comparer, s.opt.Compression)
	if err != nil {
		return "", err
	}
	defer it.Close()

	fw, err := createFile(s.opt.TempDir, s.opt.Compression)
	if err != nil {
		return "", err
	}
	defer fw.Close()

	for it.Next() {
		if err := ctx.Err(); err != nil {
			_ = os.Remove(fw.Name())
			return "", err
		}

		if err := fw.Append(it.Bytes()); err != nil {
			_ = os.Remove(fw.Name())
			return "", err
		}
	}

	if err := fw.Flush(); err != nil {
		_ = os.Remove(fw.Name())
		return "", err
	}
	if err := fw.Close(); err != nil {
		_ = os.Remove(fw.Name())
		return "", err
	}
	return fw.Name(), nil
}

func (s *Sorter) flush() error {
	fw, err := createFile(s.opt.TempDir, s.opt.Compression)
	if err != nil {
		return err
	}
	defer fw.Close()

	s.fnames = append(s.fnames, fw.Name())
	s.buf.Sort(s.opt.Comparer)

	for _, b := range s.buf.s {
		if err := fw.Append(b); err != nil {
			return err
		}
	}

	if err := fw.Flush(); err != nil {
		return err
	}
	if err := fw.Close(); err != nil {
		return err
	}

	s.buf.Reset()
	return nil
}

// --------------------------------------------------------------------

type fileNames []string

func (fns fileNames) RemoveAll() (err error) {
	for _, fn := range fns {
		if e := os.Remove(fn); e != nil && !os.IsNotExist(e) {
			err = e
		}
	}
	return
}
