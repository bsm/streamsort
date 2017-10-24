package streamsort

import (
	"context"
	"os"
	"sync"

	"golang.org/x/sync/errgroup"
)

// Sorter is responsible for sorting a stream
type Sorter struct {
	opt Options
	buf bufferSlice

	fnames []string
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
	err := unlinkAll(s.fnames...)
	s.fnames = s.fnames[:0]
	return err
}

func (s *Sorter) compact(ctx context.Context, perGroup int) error {
	var mu sync.Mutex
	result := make([]string, 0, len(s.fnames)/perGroup+1)

	errs, ctx := errgroup.WithContext(ctx)
	for x := 0; x < len(s.fnames); x += perGroup {
		group := s.fnames[x:]
		if len(group) > perGroup {
			group = group[:perGroup]
		}

		errs.Go(func() error {
			fname, err := s.merge(ctx, group)
			if err != nil {
				return err
			}

			mu.Lock()
			result = append(result, fname)
			mu.Unlock()
			return nil
		})
	}
	if err := errs.Wait(); err != nil {
		_ = unlinkAll(result...)
		return err
	}

	if err := unlinkAll(s.fnames...); err != nil {
		_ = unlinkAll(result...)
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
			_ = fw.Close()
			_ = unlinkAll(fw.Name())
			return "", err
		}

		if err := fw.Append(it.Bytes()); err != nil {
			_ = fw.Close()
			_ = unlinkAll(fw.Name())
			return "", err
		}
	}

	if err := fw.Flush(); err != nil {
		_ = fw.Close()
		_ = unlinkAll(fw.Name())
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

	s.buf.Reset()
	return nil
}

func unlinkAll(names ...string) (err error) {
	for _, fn := range names {
		if e := os.Remove(fn); e != nil {
			err = e
		}
	}
	return
}
