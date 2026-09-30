package acquisition

import (
	"io"
	"strings"
	"testing"
)

type brokenReader struct{}

func (brokenReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestJSONFullStreamAndBounds(t *testing.T) {
	for _, valid := range []string{`{}`, `[]`, ` {"a":[true,false,null,-12.3e+4,"escaped\"quote"],"b":{}} `, `"string"`, `0`, `["😀"]`} {
		if err := ValidateJSON(strings.NewReader(valid), 1024, 32); err != nil {
			t.Fatal(valid, err)
		}
	}
	for _, bad := range []string{"", `{"a":}`, `[1,]`, `{"a":1,}`, `[1 2]`, `{"a" 1}`, `{a:1}`, `true false`, `01`, `[1`, `"unterminated`, `"\x"`, `[}`, "\"\xff\""} {
		if err := ValidateJSON(strings.NewReader(bad), 1024, 32); err == nil {
			t.Fatalf("invalid JSON accepted %q", bad)
		}
	}
	if err := ValidateJSON(strings.NewReader(`"123456789"`), 5, 32); err == nil {
		t.Fatal("token limit ignored")
	}
	if err := ValidateJSON(strings.NewReader(`[[[[0]]]]`), 1024, 2); err == nil {
		t.Fatal("depth limit ignored")
	}
	if err := ValidateJSON(brokenReader{}, 1024, 32); err == nil {
		t.Fatal("read error ignored")
	}
	if err := ValidateJSON(strings.NewReader("0"), 0, 1); err == nil {
		t.Fatal("invalid limits")
	}
}

func TestCSVQuotedRecordsAndBounds(t *testing.T) {
	for _, valid := range []string{"a,b\n1,2\n", "a,b\r\n\"multi\nline\",2\r\n", "\"a\"\"b\",c\n\"x\"\"y\",d\n", "a,b"} {
		if err := ValidateCSV(strings.NewReader(valid), 1024); err != nil {
			t.Fatal(valid, err)
		}
	}
	for _, bad := range []string{"", "  <html>error</html>", "a,b\n1\n", "\"unclosed\n", "a\"bad,b\n", "\"closed\"junk,b\n"} {
		if err := ValidateCSV(strings.NewReader(bad), 1024); err == nil {
			t.Fatalf("invalid CSV accepted %q", bad)
		}
	}
	if err := ValidateCSV(strings.NewReader("\""+strings.Repeat("a\n", 100)+"\",b\n"), 20); err == nil {
		t.Fatal("multiline record bypassed budget")
	}
	if err := ValidateCSV(brokenReader{}, 20); err == nil {
		t.Fatal("read error ignored")
	}
	if err := ValidateCSV(strings.NewReader("a"), 0); err == nil {
		t.Fatal("invalid limits")
	}
}

// repeatedArray emits arbitrarily many values without allocating the document.
type repeatedArray struct {
	remaining         int
	started, finished bool
}

func (r *repeatedArray) Read(out []byte) (int, error) {
	if r.finished {
		return 0, io.EOF
	}
	n := 0
	for n < len(out) {
		if !r.started {
			out[n] = '['
			r.started = true
			n++
			continue
		}
		if r.remaining > 0 {
			if len(out)-n < 2 {
				return n, nil
			}
			out[n] = '0'
			r.remaining--
			if r.remaining > 0 {
				out[n+1] = ','
				n += 2
			} else {
				n++
			}
			continue
		}
		out[n] = ']'
		n++
		r.finished = true
		break
	}
	return n, nil
}

func TestJSONLargeDocumentWithSmallTokenBudget(t *testing.T) {
	if err := ValidateJSON(&repeatedArray{remaining: 100000}, 8, 4); err != nil {
		t.Fatal(err)
	}
}
