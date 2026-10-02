package telemetry

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/state"
)

type snapshotReader func(string) (state.Object, error)

func (f snapshotReader) Read(_ context.Context, key string, _ time.Time) (state.Object, error) {
	return f(key)
}

func TestCorrelationUsesPinnedRootAndFailsClosed(t *testing.T) {
	reader := snapshotReader(func(key string) (state.Object, error) {
		switch key {
		case "runs/root/snapshot.json":
			return state.Object{Data: []byte(`{"run_id":"root","correlation_id":"root"}`)}, nil
		case "runs/child/snapshot.json":
			return state.Object{Data: []byte(`{"run_id":"child","correlation_id":"root"}`)}, nil
		case "runs/old/snapshot.json":
			return state.Object{Data: []byte(`{"run_id":"old"}`)}, nil
		case "runs/mismatch/snapshot.json":
			return state.Object{Data: []byte(`{"run_id":"wrong","correlation_id":"root"}`)}, nil
		case "runs/bad/snapshot.json":
			return state.Object{Data: []byte(`{`)}, nil
		case "runs/unsafe/snapshot.json":
			return state.Object{Data: []byte(`{"run_id":"unsafe","correlation_id":"bad\nsecret"}`)}, nil
		default:
			return state.Object{}, errors.New("unavailable")
		}
	})
	var logs bytes.Buffer
	recorder := &JSON{Output: &logs}
	for _, run := range []string{"root", "child"} {
		recorder.Record(Entry{Component: "api", Operation: "request", Outcome: "completed", RunID: run, Correlation: Correlation(context.Background(), reader, run)})
	}
	if strings.Count(logs.String(), `"correlation_id":"root"`) != 2 || !strings.Contains(logs.String(), `"run_id":"child"`) {
		t.Fatal(logs.String())
	}
	for _, run := range []string{"", ".", "..", "bad/path", "missing", "mismatch", "bad", "unsafe"} {
		if got := Correlation(context.Background(), reader, run); got != "" {
			t.Fatal(run, got)
		}
	}
	if got := Correlation(context.Background(), reader, "old"); got != "old" {
		t.Fatal(got)
	}
	if got := Correlation(context.Background(), nil, "root"); got != "" {
		t.Fatal(got)
	}
}

func TestCorrelationKeepsInvocationHeadroom(t *testing.T) {
	reads := 0
	blocking := snapshotReader(func(string) (state.Object, error) {
		reads++
		return state.Object{}, errors.New("unexpected read")
	})
	nearDeadline, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if got := Correlation(nearDeadline, blocking, "run"); got != "" || reads != 0 {
		t.Fatal(got, reads)
	}
	cancelled, stop := context.WithCancel(context.Background())
	stop()
	if got := Correlation(cancelled, blocking, "run"); got != "" || reads != 0 {
		t.Fatal(got, reads)
	}
}
