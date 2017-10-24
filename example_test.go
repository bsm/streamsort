package streamsort_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	"github.com/bsm/streamsort"
)

func Example() {
	// Init a Sorter with default options
	sorter := streamsort.New(nil)
	defer sorter.Close()

	// Append data
	sorter.Append([]byte("foo"))
	sorter.Append([]byte("bar"))
	sorter.Append([]byte("baz"))
	sorter.Append([]byte("boo"))

	// Sort and iterate
	iter, err := sorter.Sort()
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

	// Output:
	// bar
	// baz
	// boo
	// foo
}

func Example_JSON() {
	// Define a custom comparer.
	// Sort by year ascending, then by price descending
	comparer := streamsort.ComparerFunc(func(b1, b2 []byte) int {
		var s1, s2 Stock

		if e1, e2 := json.Unmarshal(b1, &s1), json.Unmarshal(b2, &s2); e1 != nil && e2 != nil {
			return 0 // equal if both a and b are invalid
		} else if e2 != nil {
			return -1 // a before b if a is valid but not b
		} else if e1 != nil {
			return 1 // b before a if b is valid but not a
		}

		if s1.Year < s2.Year {
			return -1
		} else if s2.Year < s1.Year {
			return 1
		} else if s1.Price < s2.Price {
			return 1
		} else if s2.Price < s1.Price {
			return -1
		}
		return 0
	})

	// Init a new Sorter, use compression and no more than 1M of memory
	sorter := streamsort.New(&streamsort.Options{
		MaxMemBuffer: 1024 * 1024,
		Comparer:     comparer,
		Compression:  streamsort.CompressionGzip,
	})
	defer sorter.Close()

	// Open input JSON file
	file, err := os.Open("testdata/stocks.json")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Scan it line by line
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if err := sorter.Append(scanner.Bytes()); err != nil {
			panic(err)
		}
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}

	// Sort intput, retrieve iterator
	iter, err := sorter.Sort()
	if err != nil {
		panic(err)
	}
	defer iter.Close()

	// Iterate over the sorted results,
	// abort after the first five.
	n := 0
	for iter.Next() {
		fmt.Println(string(iter.Bytes()))

		if n++; n == 5 {
			break
		}
	}
	if err := iter.Err(); err != nil {
		panic(err)
	}

	// Output:
	// {"id":32663,"company":"Macejkovic-Feest","year":1988,"price":99.97}
	// {"id":26921,"company":"Wuckert, West and Skiles","year":1988,"price":99.7}
	// {"id":33631,"company":"Stiedemann, Senger and McLaughlin","year":1988,"price":99.48}
	// {"id":11931,"company":"Nitzsche-Corkery","year":1988,"price":98.87}
	// {"id":67013,"company":"Mills, Olson and Effertz","year":1988,"price":98.75}
}

type Stock struct {
	ID      int
	Company string
	Year    int
	Price   float64
}
