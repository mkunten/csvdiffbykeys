package csvdiffbykeys

import (
	"bytes"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func getReader(tb testing.TB, key string) *Reader {
	data := map[string]*bytes.Buffer{
		"file1": bytes.NewBufferString(`id,name,date
1,aaa,20180101
2,bbb,20180102
3,ccc,20180103
4,ddd,20180104
5,eee,20180105
`),
		"file2": bytes.NewBufferString(`id,name,date
1,aaa,20180101
2,BBB,20180102
6,fff,20180106
3,ccc,20180103
5,eee,20180105
`),
		"file3": bytes.NewBufferString(`id,name,date
1,aaa,20180101
2,BBB,20180102
1,aaa,20180101a
2,BBB,20180102a
1,aaa,20180101b
`),
	}
	r := NewReader(data[key])
	return r
}

func TestRead(t *testing.T) {
	r1 := getReader(t, "file1")
	err := r1.ReadHeader([]string{"id"})
	if err != nil {
		t.Fatalf("ReadHeader: %+v", err)
	}

	t.Run("header 0", func(t *testing.T) {
		expected := []string{"id", "name", "date"}
		if !cmp.Equal(expected, r1.Headers) {
			t.Fatalf("expected: %+v; actual: %+v", expected, r1.Headers)
		}
	})

	r2 := getReader(t, "file2")
	err = r2.ReadHeader([]string{"id"})
	if err != nil {
		t.Fatal("error")
	}
	t.Run("header 1", func(t *testing.T) {
		expected := []string{"id", "name", "date"}
		if !cmp.Equal(expected, r2.Headers) {
			t.Fatalf("expected: %+v; actual: %+v", expected, r2.Headers)
		}
	})

	actual, _ := r1.Read()
	expected2 := &Row{
		map[string]string{"id": "1", "name": "aaa", "date": "20180101"},
		"1",
	}
	if !cmp.Equal(expected2, actual) {
		t.Fatalf("\nexpected: %+v\nactual:   %+v", expected2, actual)
	}

	actual, _ = r1.Read()
	expected2 = &Row{
		map[string]string{"id": "2", "name": "bbb", "date": "20180102"},
		"2",
	}
	if !cmp.Equal(expected2, actual) {
		t.Fatalf("\nexpected: %+v\nactual:   %+v", expected2, actual)
	}
}

func TestReadWithUnexpectedKey(t *testing.T) {
	r1 := getReader(t, "file1")
	err := r1.ReadHeader([]string{"noid"})
	if err == nil {
		t.Fatalf("must be error", err)
	}
}

func TestReadAll(t *testing.T) {
	r := getReader(t, "file2")
	err := r.ReadHeader([]string{"id"})
	if err != nil {
		t.Fatalf("ReadHeader: %+v", err)
	}

	t.Run("read all 2", func(t *testing.T) {
		rows, _ := r.ReadAll()
		actual := rows[2]
		expected := &Row{
			map[string]string{"id": "3", "name": "ccc", "date": "20180103"},
			"3",
		}
		if !cmp.Equal(expected, actual) {
			t.Fatalf("\nexpected: %+v\nactual:   %+v", expected, actual)
		}
	})
}

func TestReadAllDuplicatedLines(t *testing.T) {
	r := getReader(t, "file3")
	err := r.ReadHeader([]string{"id"})
	if err != nil {
		t.Fatalf("ReadHeader: %+v", err)
	}

	t.Run("read all including duplicated lines", func(t *testing.T) {
		rows, err := r.ReadAll()

		if err != nil {
			switch err.(type) {
			case *Warn:
			default:
				t.Fatalf("must have Warn")
			}
		}
		// /* order of columns embeded in (Warn)err.Error is random, so... */
		// actual := err.Error()
		// expected := fmt.Sprintf("")
		// if !cmp.Equal(expected, actual) {
		// 	t.Fatalf("\nexpected: %+v\nactual:   %+v", expected, actual)
		// }

		actual := len(rows)
		expected := 2
		if !cmp.Equal(expected, actual) {
			t.Fatalf("\nexpected: %+v\nactual:   %+v", expected, actual)
		}

		actual2 := rows[0]
		expected2 := &Row{
			map[string]string{"id": "1", "name": "aaa", "date": "20180101"},
			"1",
		}
		if !cmp.Equal(expected, actual) {
			t.Fatalf("\nexpected: %+v\nactual:   %+v", expected2, actual2)
		}

		actual2 = rows[1]
		expected2 = &Row{
			map[string]string{"id": "2", "name": "bbb", "date": "20180102"},
			"2",
		}
		if !cmp.Equal(expected, actual) {
			t.Fatalf("\nexpected: %+v\nactual:   %+v", expected2, actual2)
		}
	})
}
