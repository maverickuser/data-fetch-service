package acquisition

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/config"
)

type recordFunc func(context.Context, string, []byte) error

func (f recordFunc) Create(ctx context.Context, key string, data []byte) error {
	return f(ctx, key, data)
}

func TestStateRecorderPersistsAttempt(t *testing.T) {
	called := false
	store := recordFunc(func(_ context.Context, key string, data []byte) error {
		called = true
		var got Attempt
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatal(err)
		}
		if key != "runs/run/pulls/job/attempts/2/finished.json" || got.StatusCode != 429 || got.Number != 2 {
			t.Fatal(key, got)
		}
		return nil
	})
	recorder := StateRecorder{Store: store}
	if err := recorder.Record(context.Background(), "run", "job", 2, "finished", Attempt{Number: 2, StatusCode: 429}); err != nil || !called {
		t.Fatal(err)
	}
	if err := recorder.Record(context.Background(), "run", "job", 0, "invalid", Attempt{}); err == nil {
		t.Fatal("invalid attempt")
	}
	if err := recorder.Record(context.Background(), "run", "job", 1, "started", Attempt{Number: 1, StartedAt: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)}); err == nil {
		t.Fatal("unserializable timestamp")
	}
	failure := errors.New("state unavailable")
	recorder.Store = recordFunc(func(context.Context, string, []byte) error { return failure })
	if err := recorder.Record(context.Background(), "run", "job", 1, "started", Attempt{Number: 1}); !errors.Is(err, failure) {
		t.Fatal(err)
	}
}

func TestConfiguredLimits(t *testing.T) {
	d := config.Defaults{MaxDownloadBytes: 100, MaxExtractedBytes: 200, MaxValidationTokenBytes: 10, MaxZipMetadataBytes: 80, MaxZipEntries: 3, MaxCompressionRatio: 4, MaxJSONDepth: 5, SourceMaxAttempts: 3, RequestTimeoutSeconds: 2}
	got := ConfiguredLimits(d)
	if got.MaxDownloadBytes != 100 || got.MaxExtractedBytes != 200 || got.MaxTokenBytes != 10 || got.MaxZipMetadataBytes != 80 || got.MaxZipEntries != 3 || got.MaxCompressionRatio != 4 || got.MaxJSONDepth != 5 || got.MaxAttempts != 3 || got.RequestTimeout != 2*time.Second {
		t.Fatal(got)
	}
}

func TestOwnedSourceClosedOnSetupFailure(t *testing.T) {
	source := &trackedBody{Reader: strings.NewReader("{}")}
	if _, err := UploadValidated(context.Background(), nil, "key", "json", source, Limits{}); err == nil || !source.closed {
		t.Fatal(err, source.closed)
	}
}

func TestCleanupRetainsLeakedDiskReservation(t *testing.T) {
	budget := NewBudget(10)
	release, err := budget.Acquire(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "source-")
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(file.Name()); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(file.Name(), 0700); err != nil {
		t.Fatal(err)
	}
	child := file.Name() + "/occupied"
	if err := os.WriteFile(child, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	download := &Download{File: file, release: release}
	if err := download.Close(); err == nil || budget.used != 10 {
		t.Fatal(err, budget.used)
	}
	if err := os.Remove(child); err != nil {
		t.Fatal(err)
	}
	if err := download.Close(); err != nil || budget.used != 0 {
		t.Fatal(err, budget.used)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrShortWrite }
func TestLocalWriteFailureIsNotSourceRetry(t *testing.T) {
	_, err := (localWriter{failingWriter{}}).Write([]byte("x"))
	var failure *Failure
	if !errors.As(err, &failure) || failure.Retryable || failure.Code != "TEMPORARY_WRITE" || !errors.Is(err, io.ErrShortWrite) {
		t.Fatal(err)
	}
}
