# StreamSort

[![Build Status](https://travis-ci.org/bsm/streamsort.png?branch=master)](https://travis-ci.org/bsm/streamsort)
[![GoDoc](https://godoc.org/github.com/bsm/streamsort?status.png)](http://godoc.org/github.com/bsm/streamsort)
[![Go Report Card](https://goreportcard.com/badge/github.com/bsm/streamsort)](https://goreportcard.com/report/github.com/bsm/streamsort)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Sort arbitrarily large data sets with a predictable amount of memory using temporary files.

### Example:

```go
import(
  "fmt"

  "github.com/bsm/streamsort"
)

func main() {
	// Init a Sorter with default options
	sorter := streamsort.New(nil)
	defer sorter.Close()

	// Append data
	_ = sorter.Append([]byte("foo"))
	_ = sorter.Append([]byte("bar"))
	_ = sorter.Append([]byte("baz"))
	_ = sorter.Append([]byte("boo"))

	// Sort and iterate
	iter, err := sorter.Sort(context.Background())
	if err != nil {
		panic(err)
	}
	defer iter.Close()

	for iter.Next() {
		fmt.Println(string(iter.Bytes()))
	}
	if err := iter.Err(); err != nil {
		panic(err)
	}

}
```

For more complex examples, please see our [Documentation](https://godoc.org/github.com/bsm/streamsort)
