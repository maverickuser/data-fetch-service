//go:build integration

package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/domain"
)

func TestDeliveryHTTPComposition(t *testing.T) {
	svc, repo, coord, _ := fixture(t, true)
	var requests [][]byte
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/event-ingestions" || r.Header.Get("Idempotency-Key") != "run_1" || r.Header.Get("Content-Type") != "application/cloudevents+json" {
			t.Errorf("unexpected request: %s %s %v", r.Method, r.URL.Path, r.Header)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		requests = append(requests, body)
		var event Submission
		if err := json.Unmarshal(body, &event); err != nil {
			t.Error(err)
			return
		}
		if event.ID != "urn:bond-platform:submission:run_1" || event.Data.Manifest.Bucket != "artifacts" || event.Data.Manifest.Key != "runs/run_1/manifest.json" || event.Data.DatasetFingerprint != fingerprint || bytes.Contains(body, []byte(`"files"`)) {
			t.Errorf("incorrect submission: %s", body)
		}
		if len(requests) == 1 {
			w.Header().Set("Content-Type", "application/problem+json")
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"type":"urn:bond-platform:problem:service-unavailable","title":"Service Unavailable","status":503,"detail":"temporary","instance":"urn:uuid:test","code":"SERVICE_UNAVAILABLE","correlationId":"test"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Location", serverURL(r)+"/v1/processing-jobs/job-1")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"jobId":"job-1","status":"ACCEPTED","eventId":"urn:bond-platform:submission:run_1","runId":"run_1","createdAt":"2026-10-01T01:00:00Z","statusUrl":"` + serverURL(r) + `/v1/processing-jobs/job-1"}`))
	})
	svc.Client = &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, r)
		return recorder.Result(), nil
	})}
	if err := svc.Run(context.Background(), "run_1", "token", fixedTime.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 2 || !bytes.Equal(requests[0], requests[1]) || coord.state.Phase != domain.Completed {
		t.Fatal(len(requests), coord.state.Phase)
	}
	if len(repo.objects["runs/run_1/delivery/1/finished.json"]) == 0 || len(repo.objects["runs/run_1/delivery/2/finished.json"]) == 0 {
		t.Fatal("missing attempt evidence")
	}
}

func serverURL(r *http.Request) string { return "https://" + r.Host }

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
