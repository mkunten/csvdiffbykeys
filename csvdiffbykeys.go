package csvdiffbykeys

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"encoding/csv"
)

//// reader

// Reader wraps csv.Reader
type Reader struct {
	Reader  *csv.Reader
	Headers []string
	ColLen  int
	Keys    []string
}

// Row holds columns with sort keys
type Row struct {
	Columns map[string]string
	SortKey string
}

// NewReader wraps csv.NewReader
func NewReader(r io.Reader) *Reader {
	return &Reader{
		Reader: csv.NewReader(r),
	}
}

// ReadHeader sets header
func (r *Reader) ReadHeader(keys []string) (err error) {
	r.Headers, err = r.Reader.Read()
	if err != nil {
		return err
	}
	r.ColLen = len(r.Headers)

	// duplication check
	defined := make(map[string]bool)
	for _, v := range r.Headers {
		if _, exists := defined[v]; exists {
			return fmt.Errorf("Multiple Indices with the same name '%s'", v)
		}
		defined[v] = true
	}

	// key check
	r.Keys = keys
	for _, v := range r.Keys {
		if _, exists := defined[v]; !exists {
			return fmt.Errorf("No key '%s' in the header", v)
		}
	}
	return nil
}

// Read wraps csv.Read
func (r *Reader) Read() (row *Row, err error) {
	if r.ColLen == 0 {
		return nil, fmt.Errorf("No header read")
	}
	var columns []string
	columns, err = r.Reader.Read()
	if err != nil {
		return nil, err
	}
	row = &Row{
		make(map[string]string, r.ColLen),
		"",
	}
	for i, column := range columns {
		row.Columns[r.Headers[i]] = column
	}

	keys := make([]string, len(r.Keys))
	for i, key := range r.Keys {
		keys[i] = row.Columns[key]
	}
	row.SortKey = strings.Join(keys, "\n")

	return row, nil
}

// ReadAll read all lines
func (r *Reader) ReadAll() (rows []*Row, err error) {
	if r.ColLen == 0 {
		return nil, fmt.Errorf("No header read")
	}

	defined := make(map[string]bool)
	warn := &Warn{}
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		if _, exists := defined[row.SortKey]; exists {
			warn.Add(fmt.Sprintf("%s", row.Columns))
		} else {
			rows = append(rows, row)
			defined[row.SortKey] = true
		}
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i].SortKey < rows[j].SortKey
	})

	if len(warn.Warns) > 0 {
		err = warn
	}
	return
}

//// common
type Warn struct {
	Warns []string
}

func (warn *Warn) Add(w string) {
	warn.Warns = append(warn.Warns, w)
}

func (warn *Warn) Error() string {
	return fmt.Sprintf("WARN: duplicated: %s",
		strings.Join(warn.Warns, "\n      "))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
