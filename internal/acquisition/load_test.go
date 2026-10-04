//go:build load

package acquisition

import (
	"archive/zip"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"runtime"
	"testing"
	"time"
)

const loadMiB = 1 << 20

// repeatingCSV streams a valid fixture without holding the full dataset in memory.
type repeatingCSV struct{ offset int }

func (r *repeatingCSV) Read(dst []byte) (int, error) {
	const row = "1,2\n"
	for i := range dst {
		dst[i] = row[r.offset%len(row)]
		r.offset++
	}
	return len(dst), nil
}

type loadClientFunc func(*http.Request) (*http.Response, error)

func (f loadClientFunc) Do(request *http.Request) (*http.Response, error) { return f(request) }

type slowFixture struct {
	context context.Context
	first   bool
}

func (r *slowFixture) Read(out []byte) (int, error) {
	if !r.first {
		<-r.context.Done()
		return 0, r.context.Err()
	}
	r.first = false
	return copy(out, "1,2\n"), nil
}

func (r *slowFixture) Close() error { return nil }

func TestLoadLargeStream(t *testing.T) {
	const size = 64 * loadMiB
	client := loadClientFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, ContentLength: size, Header: http.Header{"Content-Type": {"text/csv"}}, Body: io.NopCloser(io.LimitReader(&repeatingCSV{}, size))}, nil
	})
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	budget := NewBudget(size)
	download, status, err := DownloadSource(context.Background(), client, budget, Limits{MaxDownloadBytes: size, RequestTimeout: time.Minute}, "https://fixture.example/large.csv", "csv", t.TempDir(), time.Now())
	if err != nil || status != http.StatusOK || download == nil {
		t.Fatalf("large stream: status=%d error=%v", status, err)
	}
	if download.Bytes != size {
		t.Fatalf("downloaded %d bytes, want %d", download.Bytes, size)
	}
	if err := ValidateCSV(download.File, 1024); err != nil {
		t.Fatal(err)
	}
	if err := download.Close(); err != nil || budget.used != 0 {
		t.Fatalf("cleanup: %v, reserved=%d", err, budget.used)
	}
	runtime.ReadMemStats(&after)
	t.Logf("stream_bytes=%d elapsed=%s allocated_bytes=%d", size, time.Since(start), after.TotalAlloc-before.TotalAlloc)
}

func TestLoadLargeZIP(t *testing.T) {
	const size = 32 * loadMiB
	file, err := os.CreateTemp(t.TempDir(), "large-*.zip")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	writer := zip.NewWriter(file)
	member, err := writer.CreateHeader(&zip.FileHeader{Name: "fgroup.csv", Method: zip.Store})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.CopyN(member, &repeatingCSV{}, size); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	stat, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	selected, err := zipMember(file, stat.Size(), "fgroup.csv", Limits{MaxZipEntries: 10, MaxZipMetadataBytes: loadMiB, MaxExtractedBytes: size, MaxCompressionRatio: 100})
	if err != nil {
		t.Fatal(err)
	}
	input, err := selected.Open()
	if err != nil {
		t.Fatal(err)
	}
	validationErr := ValidateCSV(input, 1024)
	closeErr := input.Close()
	if err := errors.Join(validationErr, closeErr); err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&after)
	t.Logf("zip_bytes=%d extracted_bytes=%d elapsed=%s allocated_bytes=%d", stat.Size(), size, time.Since(start), after.TotalAlloc-before.TotalAlloc)
}

func TestLoadSlowResponseDeadline(t *testing.T) {
	client := loadClientFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, ContentLength: -1, Header: http.Header{"Content-Type": {"text/csv"}}, Body: &slowFixture{context: request.Context()}}, nil
	})
	start := time.Now()
	budget := NewBudget(loadMiB)
	_, _, err := DownloadSource(context.Background(), client, budget, Limits{MaxDownloadBytes: loadMiB, RequestTimeout: 100 * time.Millisecond}, "https://fixture.example/slow.csv", "csv", t.TempDir(), time.Now())
	var failure *Failure
	if !errors.As(err, &failure) || failure.Code != "SOURCE_READ" {
		t.Fatalf("want bounded source read failure, got %v", err)
	}
	if budget.used != 0 {
		t.Fatalf("temporary capacity leaked: %d", budget.used)
	}
	t.Logf("slow_response_timeout=%s elapsed=%s", 100*time.Millisecond, time.Since(start))
}
