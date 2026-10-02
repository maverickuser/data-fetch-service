package acquisition

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
)

// ValidateCSV checks strict comma-separated records with bounded logical record sizes.
func ValidateCSV(input io.Reader, maxRecord int64) error {
	if maxRecord < 1 {
		return fmt.Errorf("positive CSV record limit required")
	}
	buffered := bufio.NewReaderSize(input, 32<<10)
	if bom, _ := buffered.Peek(3); string(bom) == "\xef\xbb\xbf" {
		if _, err := buffered.Discard(3); err != nil {
			return err
		}
	}
	bounded := &recordReader{reader: buffered, max: maxRecord, fieldStart: true}
	reader := csv.NewReader(bounded)
	reader.ReuseRecord = true
	records := 0
	for {
		_, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		records++
	}
	if records == 0 {
		return fmt.Errorf("empty CSV")
	}
	return nil
}

// recordReader bounds quoted multiline records before encoding/csv can allocate them.
type recordReader struct {
	reader                         *bufio.Reader
	max, count                     int64
	quoted, afterQuote, fieldStart bool
	seenContent                    bool
}

// Read tracks record boundaries while leaving strict syntax validation to encoding/csv.
func (r *recordReader) Read(out []byte) (int, error) {
	for n := range out {
		b, err := r.reader.ReadByte()
		if err != nil {
			return n, err
		}
		if !r.seenContent && b != ' ' && b != '\t' && b != '\r' && b != '\n' {
			if b == '<' {
				return n, fmt.Errorf("HTML/XML is not CSV")
			}
			r.seenContent = true
		}
		r.count++
		if r.count > r.max {
			return n, fmt.Errorf("CSV record limit exceeded")
		}
		if r.quoted {
			if r.afterQuote {
				if b == '"' {
					r.afterQuote = false
				} else {
					r.quoted = false
					r.afterQuote = false
				}
			} else if b == '"' {
				r.afterQuote = true
			}
		} else if r.fieldStart && b == '"' {
			r.quoted = true
		}
		if !r.quoted {
			if b == '\n' {
				r.count = 0
				r.fieldStart = true
			} else if b == ',' {
				r.fieldStart = true
			} else {
				r.fieldStart = false
			}
		} else {
			r.fieldStart = false
		}
		out[n] = b
	}
	return len(out), nil
}
