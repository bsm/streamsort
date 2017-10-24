package streamsort

import "bytes"

// TODO
// * allow to limit the number of intermediate files using a generational sort approach

// Options contains sorting options
type Options struct {
	// TempDir specifies the working directory.
	// By default standard temp is used
	TempDir string

	// Compararer defines the sort order.
	// Default: bytes.Compare
	Comparer Comparer

	// Compression is used for intermediate files.
	// Default: CompressionNone
	Compression Compression

	// MaxOpenFiles limits the number of open files; must be >1.
	// Default: 100
	MaxOpenFiles int

	// MaxMemBuffer limits the memory used for sorting
	// Default: 64M (must be at least 1M = 1024*1024)
	MaxMemBuffer int
}

const oneMB = 1024 * 1024

func (o *Options) norm() {
	if o.Comparer == nil {
		o.Comparer = ComparerFunc(bytes.Compare)
	}

	if o.Compression < CompressionNone || o.Compression >= nCompression {
		o.Compression = CompressionNone
	}

	if o.MaxOpenFiles < 2 {
		o.MaxOpenFiles = 100
	}

	if o.MaxMemBuffer < 1 {
		o.MaxMemBuffer = 64 * oneMB
	} else if o.MaxMemBuffer < oneMB {
		o.MaxMemBuffer = oneMB
	}
}

// --------------------------------------------------------------------

type Compression uint8

const (
	CompressionNone Compression = iota
	CompressionGzip
	nCompression
)

// Comparer is used to compare data chunks for ordering
type Comparer interface {
	// Compare returns -1 when a is 'less than', 0 when a is 'equal to' or
	// +1' when a is 'greater than' b.
	Compare(a, b []byte) int
}

type ComparerFunc func(a, b []byte) int

func (f ComparerFunc) Compare(a, b []byte) int { return f(a, b) }
