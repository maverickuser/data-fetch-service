package acquisition

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/config"
)

type attemptRecorder struct {
	phases   []string
	attempts []Attempt
	fail     string
}

func (r *attemptRecorder) Record(_ context.Context, _, _ string, _ int, phase string, a Attempt) error {
	if phase == r.fail {
		return errors.New("record failed")
	}
	r.phases = append(r.phases, phase)
	r.attempts = append(r.attempts, a)
	return nil
}

func fetchFixture(t *testing.T, body []byte, statuses []int) (*Fetcher, *attemptRecorder, map[string][]byte) {
	t.Helper()
	recorder := &attemptRecorder{}
	objects := map[string][]byte{}
	calls := 0
	client := clientFunc(func(*http.Request) (*http.Response, error) {
		status := statuses[min(calls, len(statuses)-1)]
		calls++
		return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/octet-stream"}}, ContentLength: int64(len(body)), Body: io.NopCloser(bytes.NewReader(body))}, nil
	})
	store := storageFunc(func(_ context.Context, key string, r io.Reader) (Artifact, error) {
		data, err := io.ReadAll(r)
		if err != nil {
			return Artifact{}, err
		}
		objects[key] = data
		hash := sha256.Sum256(data)
		return Artifact{Key: key, Bytes: int64(len(data)), SHA256: hex.EncodeToString(hash[:])}, nil
	})
	return &Fetcher{Client: client, Storage: store, Recorder: recorder, Budget: NewBudget(1 << 20), Limits: Limits{MaxDownloadBytes: 1 << 20, MaxExtractedBytes: 1 << 20, MaxTokenBytes: 1024, MaxZipMetadataBytes: 4096, MaxZipEntries: 10, MaxCompressionRatio: 100, MaxJSONDepth: 10, MaxAttempts: 3, RequestTimeout: time.Second}, TempRoot: t.TempDir(), Wait: func(context.Context, time.Duration) error { return nil }}, recorder, objects
}

func TestFetchJSONRetriesAndKeepsISINName(t *testing.T) {
	f, recorder, objects := fetchFixture(t, []byte(`{"ISIN":"INE121A07QY9"}`), []int{503, 429, 200})
	result, err := f.Fetch(context.Background(), "run-1", config.ResolvedJob{ID: "details", URL: "https://example.com/api", Filename: "INE121A07QY9_details.json", Format: "json"})
	if err != nil || result.Attempts != 3 || result.Filename != "INE121A07QY9_details.json" || len(objects) != 1 || len(recorder.attempts) != 6 {
		t.Fatal(result, err, recorder)
	}
	if recorder.attempts[1].StatusCode != 503 || recorder.attempts[3].StatusCode != 429 || recorder.attempts[5].ErrorCode != "" {
		t.Fatal(recorder)
	}
	entries, err := os.ReadDir(f.TempRoot)
	if err != nil || len(entries) != 0 || f.Budget.used != 0 {
		t.Fatal(entries, err)
	}
}

func TestFetchBSEPreservesArchiveAndOnlySelectedCSV(t *testing.T) {
	raw := testArchive(t, []string{"fgroup21092026.csv", "icdm21092026.csv", "wdm21092026.csv"})
	f, _, objects := fetchFixture(t, raw, []int{200})
	result, err := f.Fetch(context.Background(), "run-1", config.ResolvedJob{ID: "debt", URL: "https://example.com/DEBTBHAVCOPY21092026.zip", Filename: "BSE_fgroup21092026.csv", MemberPath: "fgroup21092026.csv", Format: "csv"})
	if err != nil || result.Archive == nil || len(objects) != 2 {
		t.Fatal(result, err, objects)
	}
	if !bytes.Equal(objects[result.Archive.Key], raw) || string(objects[result.Artifact.Key]) != "a,b\n1,2\n" || !strings.HasSuffix(result.Artifact.Key, "/BSE_fgroup21092026.csv") {
		t.Fatal(result)
	}
}

func TestFetchTerminalFailuresAndAttemptEvidence(t *testing.T) {
	for _, test := range []struct {
		status  int
		body    string
		records int
	}{{404, "missing", 2}, {503, "busy", 6}, {200, "{", 2}} {
		f, r, _ := fetchFixture(t, []byte(test.body), []int{test.status})
		if _, err := f.Fetch(context.Background(), "run", config.ResolvedJob{ID: "job", URL: "https://example.com/a.json", Format: "json"}); err == nil || len(r.attempts) != test.records {
			t.Fatal(test, err, r)
		}
	}
	for _, phase := range []string{"started", "finished"} {
		f, r, _ := fetchFixture(t, []byte(`{}`), []int{200})
		r.fail = phase
		if _, err := f.Fetch(context.Background(), "run", config.ResolvedJob{ID: "job", URL: "https://example.com/a.json", Format: "json"}); err == nil {
			t.Fatal(phase)
		}
	}
	f, _, _ := fetchFixture(t, []byte(`{}`), []int{503})
	f.Wait = func(context.Context, time.Duration) error { return context.Canceled }
	if _, err := f.Fetch(context.Background(), "run", config.ResolvedJob{ID: "job", URL: "https://example.com/a.json", Format: "json"}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := f.Fetch(context.Background(), "../run", config.ResolvedJob{ID: "job"}); err == nil {
		t.Fatal("unsafe run ID")
	}
}

func TestFetchZIPValidationAndStorageFailures(t *testing.T) {
	job := config.ResolvedJob{ID: "job", URL: "https://example.com/a.zip", MemberPath: "good.csv", Format: "csv"}
	for _, raw := range [][]byte{[]byte("not a ZIP"), testArchive(t, []string{"other.csv"})} {
		f, _, objects := fetchFixture(t, raw, []int{200})
		if _, err := f.Fetch(context.Background(), "run", job); err == nil || len(objects) != 0 {
			t.Fatal(err, objects)
		}
	}
	for _, failArchive := range []bool{true, false} {
		f, _, _ := fetchFixture(t, testArchive(t, []string{"good.csv"}), []int{200})
		original := f.Storage
		f.Storage = storageFunc(func(ctx context.Context, key string, r io.Reader) (Artifact, error) {
			if strings.Contains(key, "/downloads/") == failArchive {
				return Artifact{}, errors.New("storage unavailable")
			}
			return original.Upload(ctx, key, r)
		})
		if _, err := f.Fetch(context.Background(), "run", job); err == nil {
			t.Fatal("storage failure ignored")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitContext(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := waitContext(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
}
