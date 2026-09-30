// Package acquisition downloads and validates bounded source artifacts.
package acquisition

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"unicode/utf8"
)

// ValidateJSON checks the complete stream using bounded tokens and nesting depth.
func ValidateJSON(input io.Reader, maxToken int64, maxDepth int) error {
	if maxToken < 1 || maxDepth < 1 {
		return fmt.Errorf("positive JSON limits required")
	}
	parser := jsonStream{reader: bufio.NewReaderSize(input, 32<<10), maxToken: maxToken, maxDepth: maxDepth}
	if err := parser.value(0); err != nil {
		return err
	}
	if _, err := parser.peek(); err != io.EOF {
		if err != nil {
			return err
		}
		return fmt.Errorf("trailing JSON data")
	}
	return nil
}

type jsonStream struct {
	reader   *bufio.Reader
	maxToken int64
	maxDepth int
}

// peek consumes JSON whitespace and leaves the next structural byte buffered.
func (p *jsonStream) peek() (byte, error) {
	for {
		b, err := p.reader.Peek(1)
		if err != nil {
			return 0, err
		}
		switch b[0] {
		case ' ', '\r', '\n', '\t':
			if _, err := p.reader.ReadByte(); err != nil {
				return 0, err
			}
		default:
			return b[0], nil
		}
	}
}

// take consumes one exact structural byte after optional whitespace.
func (p *jsonStream) take(want byte) error {
	b, err := p.peek()
	if err != nil {
		return err
	}
	if b != want {
		return fmt.Errorf("expected JSON %q", want)
	}
	_, err = p.reader.ReadByte()
	return err
}

// value parses one JSON value without materializing objects or arrays.
func (p *jsonStream) value(depth int) error {
	b, err := p.peek()
	if err != nil {
		return err
	}
	switch b {
	case '{', '[':
		if depth >= p.maxDepth {
			return fmt.Errorf("JSON nesting limit exceeded")
		}
		return p.container(depth, b)
	case '"':
		return p.token(true)
	default:
		return p.token(false)
	}
}

// container validates separators and recursively visits values under the depth budget.
func (p *jsonStream) container(depth int, open byte) error {
	if err := p.take(open); err != nil {
		return err
	}
	close := byte(']')
	if open == '{' {
		close = '}'
	}
	b, err := p.peek()
	if err != nil {
		return err
	}
	if b == close {
		return p.take(close)
	}
	for {
		if open == '{' {
			b, err := p.peek()
			if err != nil {
				return err
			}
			if b != '"' {
				return fmt.Errorf("JSON object key must be string")
			}
			if err := p.token(true); err != nil {
				return err
			}
			if err := p.take(':'); err != nil {
				return err
			}
		}
		if err := p.value(depth + 1); err != nil {
			return err
		}
		b, err := p.peek()
		if err != nil {
			return err
		}
		if b == close {
			return p.take(close)
		}
		if err := p.take(','); err != nil {
			return err
		}
	}
}

// token validates one bounded scalar; strings retain no more than maxToken bytes.
func (p *jsonStream) token(quoted bool) error {
	token := make([]byte, 0, 256)
	escaped := false
	for {
		next, err := p.reader.Peek(1)
		if err != nil {
			if err == io.EOF && !quoted {
				break
			}
			return err
		}
		b := next[0]
		if !quoted && (b == ' ' || b == '\r' || b == '\n' || b == '\t' || b == ',' || b == ']' || b == '}' || b == ':') {
			break
		}
		if int64(len(token)) >= p.maxToken {
			return fmt.Errorf("JSON token limit exceeded")
		}
		if _, err := p.reader.ReadByte(); err != nil {
			return err
		}
		token = append(token, b)
		if quoted && len(token) > 1 {
			if !escaped && b == '"' {
				break
			}
			if !escaped && b == '\\' {
				escaped = true
			} else {
				escaped = false
			}
		}
	}
	if !utf8.Valid(token) || !json.Valid(token) {
		return fmt.Errorf("invalid JSON scalar")
	}
	return nil
}
