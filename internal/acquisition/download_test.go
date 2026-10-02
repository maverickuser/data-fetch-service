package acquisition

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

type clientFunc func(*http.Request) (*http.Response, error)

func (f clientFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }

type trackedBody struct {
	io.Reader
	closed   bool
	closeErr error
}

func (b *trackedBody) Close() error { b.closed = true; return b.closeErr }

func TestDownloadCompleteAndCleanup(t *testing.T) {
	for _, size := range []int64{-1, 7} {
		t.Run(string(rune(size+70)), func(t *testing.T) {
			body := &trackedBody{Reader: strings.NewReader(`{"a":1}`)}
			client := clientFunc(func(r *http.Request) (*http.Response, error) {
				if r.Header.Get("Accept-Encoding") != "identity" {
					t.Fatal("encoding")
				}
				return &http.Response{StatusCode: 200, ContentLength: size, Header: http.Header{"Content-Type": {"application/json"}}, Body: body}, nil
			})
			budget := NewBudget(10)
			dir := t.TempDir()
			d, status, err := DownloadSource(context.Background(), client, budget, Limits{MaxDownloadBytes: 10, RequestTimeout: time.Second}, "https://example.com/a.json", "json", dir, time.Now())
			if err != nil || status != 200 {
				t.Fatal(status, err)
			}
			bytes, err := io.ReadAll(d.File)
			if err != nil || string(bytes) != `{"a":1}` || d.Bytes != 7 || !body.closed {
				t.Fatal(string(bytes), err)
			}
			if budget.used == 0 {
				t.Fatal("reservation released before file cleanup")
			}
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 0 || budget.used != 0 {
				t.Fatal(entries, err, budget.used)
			}
		})
	}
}

func TestDownloadFailureCleanup(t *testing.T) {
	for _, test := range []struct {
		name            string
		body            io.Reader
		length          int64
		status          int
		encoding, media string
		closeErr        error
	}{
		{name: "overflow", body: strings.NewReader("123456"), length: -1, status: 200, media: "text/csv"},
		{name: "lying length", body: strings.NewReader("1234"), length: 2, status: 200, media: "text/csv"},
		{name: "truncated", body: strings.NewReader("1"), length: 3, status: 200, media: "text/csv"},
		{name: "read failure", body: brokenReader{}, length: -1, status: 200, media: "text/csv"},
		{name: "close failure", body: strings.NewReader("1"), length: 1, status: 200, media: "text/csv", closeErr: io.ErrUnexpectedEOF},
		{name: "partial", body: strings.NewReader("1"), length: 1, status: 206, media: "text/csv"},
		{name: "gzip", body: strings.NewReader("1"), length: 1, status: 200, media: "text/csv", encoding: "gzip"},
		{name: "html", body: strings.NewReader("1"), length: 1, status: 200, media: "text/html"},
		{name: "declared oversize", body: strings.NewReader("1"), length: 6, status: 200, media: "text/csv"},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := &trackedBody{Reader: test.body, closeErr: test.closeErr}
			budget := NewBudget(5)
			dir := t.TempDir()
			client := clientFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: test.status, ContentLength: test.length, Header: http.Header{"Content-Type": {test.media}, "Content-Encoding": {test.encoding}}, Body: body}, nil
			})
			_, _, err := DownloadSource(context.Background(), client, budget, Limits{MaxDownloadBytes: 5, RequestTimeout: time.Second}, "https://example.com/a.csv", "csv", dir, time.Now())
			if err == nil || !body.closed || budget.used != 0 {
				t.Fatal(err, body.closed, budget.used)
			}
			entries, e := os.ReadDir(dir)
			if e != nil || len(entries) != 0 {
				t.Fatal(entries, e)
			}
		})
	}
}

func TestSourceStatusRetryClassification(t *testing.T) {
	for _, status := range []int{206, 400, 404, 408, 429, 500, 503} {
		err := checkResponse(&http.Response{StatusCode: status, Header: http.Header{"Retry-After": {"999"}}}, "json", time.Now())
		var failure *Failure
		wantCode := "SOURCE_HTTP"
		if status == 404 {
			wantCode = "SOURCE_NOT_FOUND"
		}
		if !errors.As(err, &failure) || failure.Code != wantCode || failure.Retryable != (status == 408 || status == 429 || status >= 500) || failure.RetryAfter != 999*time.Second {
			t.Fatal(status, err)
		}
	}
}

func TestDownloadRejectsUnsafeInitialURL(t *testing.T) {
	client := clientFunc(func(*http.Request) (*http.Response, error) { t.Fatal("unsafe URL reached network"); return nil, nil })
	for _, source := range []string{"http://example.com/a", "https://user@example.com/a", "https://example.com/a#fragment"} {
		_, _, err := DownloadSource(context.Background(), client, NewBudget(5), Limits{MaxDownloadBytes: 5, RequestTimeout: time.Second}, source, "json", t.TempDir(), time.Now())
		if !errors.Is(err, ErrUnsafeDestination) {
			t.Fatal(err)
		}
	}
}
