package delivery

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/config"
	"github.com/maverickuser/data-fetch-service/internal/domain"
	"github.com/maverickuser/data-fetch-service/internal/events"
	"github.com/maverickuser/data-fetch-service/internal/pull"
	"github.com/maverickuser/data-fetch-service/internal/state"
)

var fixedTime = time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC)
var fingerprint = "sha256:" + strings.Repeat("a", 64)

type memoryRepository struct {
	objects     map[string][]byte
	createError string
	readError   string
}

func (r *memoryRepository) Read(_ context.Context, key string, _ time.Time) (state.Object, error) {
	if key == r.readError {
		return state.Object{}, errors.New("read unavailable")
	}
	data, ok := r.objects[key]
	if !ok {
		return state.Object{}, state.ErrNotFound
	}
	return state.Object{Data: append([]byte(nil), data...), Modified: fixedTime}, nil
}
func (r *memoryRepository) Create(_ context.Context, key string, data []byte) error {
	if key == r.createError {
		return errors.New("write unavailable")
	}
	if old, ok := r.objects[key]; ok {
		if !bytes.Equal(old, data) {
			return state.ErrIntegrity
		}
		return nil
	}
	r.objects[key] = append([]byte(nil), data...)
	return nil
}

type memoryManifest struct {
	data []byte
	err  error
}

func (m memoryManifest) Read(context.Context, string) ([]byte, error) { return m.data, m.err }

type memoryCoordinator struct {
	state     state.Coordination
	commits   []state.Transition
	claimErr  error
	commitErr error
	failPhase domain.State
	loadError error
}

func (c *memoryCoordinator) ClaimDispatched(_ context.Context, _, run, queue, token string, _ time.Time) (state.Lease, error) {
	if c.claimErr != nil {
		return state.Lease{}, c.claimErr
	}
	if run != "run_1" || queue != "delivery" || c.state.Phase != domain.DeliveryPending {
		return state.Lease{}, state.ErrConflict
	}
	c.state.OwnerToken = token
	c.state.Generation++
	return state.Lease{RunID: run, Generation: c.state.Generation, Token: token}, nil
}
func (c *memoryCoordinator) ClaimExpiredDelivery(_ context.Context, _, run, token string, _ time.Time) (state.Lease, error) {
	if c.state.Phase != domain.Delivering || c.state.ActiveRunID != run {
		return state.Lease{}, state.ErrConflict
	}
	c.state.Generation++
	c.state.OwnerToken = token
	return state.Lease{RunID: run, Generation: c.state.Generation, Token: token}, nil
}
func (c *memoryCoordinator) Load(context.Context, string) (state.Coordination, string, error) {
	if c.loadError != nil {
		return state.Coordination{}, "", c.loadError
	}
	return c.state, "etag", nil
}

func TestDeliveryRecoverySurfacesCoordinationAndStorageFailures(t *testing.T) {
	for _, scenario := range []string{"run-load", "resume-disabled", "resume-submission-write", "resume-submission-read", "resume-bad-profile", "failure-load"} {
		t.Run(scenario, func(t *testing.T) {
			svc, repo, coord, raw := fixture(t, true)
			svc.Client = clientFunc(func(*http.Request) (*http.Response, error) { t.Fatal("unexpected processor call"); return nil, nil })
			if scenario == "run-load" {
				coord.loadError = errors.New("coordination unavailable")
			}
			if scenario == "run-load" {
				if err := svc.Run(context.Background(), "run_1", "worker", fixedTime.Add(time.Minute)); err == nil {
					t.Fatal("load failure hidden")
				}
				return
			}
			coord.state.Phase = domain.Delivering
			coord.state.LastSequence = 5
			switch scenario {
			case "resume-disabled":
				var snapshot events.Snapshot
				_ = json.Unmarshal(repo.objects["runs/run_1/snapshot.json"], &snapshot)
				snapshot.Config.Processor.Enabled = false
				repo.objects["runs/run_1/snapshot.json"], _ = json.Marshal(snapshot)
			case "resume-submission-write":
				repo.createError = "runs/run_1/delivery/submission.json"
			case "resume-submission-read":
				repo.readError = "runs/run_1/delivery/submission.json"
			case "resume-bad-profile":
				var manifest pull.Manifest
				_ = json.Unmarshal(raw, &manifest)
				manifest.DataSchema = "unsupported"
				data, _ := json.Marshal(manifest)
				svc.Manifests = memoryManifest{data: data}
			case "failure-load":
				coord.loadError = errors.New("coordination unavailable")
			}
			err := svc.Resume(context.Background(), "run_1", "recovery", fixedTime.Add(time.Minute))
			if scenario == "resume-disabled" || scenario == "resume-bad-profile" {
				if err != nil || coord.state.Phase != domain.Failed {
					t.Fatal(scenario, err, coord.state.Phase)
				}
			} else if err == nil {
				t.Fatal("failure hidden", scenario)
			}
		})
	}
}
func (c *memoryCoordinator) Repair(context.Context, string) error { return nil }
func (c *memoryCoordinator) Commit(_ context.Context, _ string, _ state.Lease, tr state.Transition) error {
	if c.commitErr != nil || c.failPhase == tr.Phase {
		if c.commitErr == nil {
			return errors.New("phase commit unavailable")
		}
		return c.commitErr
	}
	if tr.Sequence != c.state.LastSequence+1 {
		return state.ErrConflict
	}
	c.commits = append(c.commits, tr)
	c.state.LastSequence = tr.Sequence
	c.state.Phase = tr.Phase
	return nil
}

type clientFunc func(*http.Request) (*http.Response, error)

func (f clientFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }

func fixture(t *testing.T, enabled bool) (*Service, *memoryRepository, *memoryCoordinator, []byte) {
	t.Helper()
	manifest := pull.Manifest{SpecVersion: "1.0", ID: "urn:bond-platform:manifest:run_1", Source: "urn:bond-platform:service:data-fetch-service", Type: "com.bondplatform.dataset.manifest.v1", DataSchema: "urn:bond-platform:dataset:nsdl-security", Time: fixedTime, Subject: "isin/INE121A07QY9", DataContentType: "application/json", Data: pull.ManifestData{SchemaVersion: 1, EventType: "nsdl-bond-data", EventID: "event-1", RunID: "run_1", ConfigRevision: "rev1", Inputs: map[string]string{"isin_code": "INE121A07QY9"}, DatasetFingerprint: fingerprint, Files: []pull.File{{JobID: "job-1"}}}}
	manifestBytes, _ := json.Marshal(manifest)
	snapshot := events.Snapshot{SchemaVersion: 1, RunID: "run_1", ExecutionKey: "exec1", ConfigRevision: "rev1", Event: events.Normalized{EventID: "event-1"}, Config: config.Config{Processor: config.Processor{Enabled: enabled, URL: "https://processor.example/v1/event-ingestions", RequestTimeoutSeconds: 5, MaxAttempts: 3}}}
	snapshotBytes, _ := json.Marshal(snapshot)
	handoff := state.Transition{Phase: domain.DeliveryPending, Details: json.RawMessage(`{"bucket":"artifacts","manifest_key":"runs/run_1/manifest.json","dataset_fingerprint":"` + fingerprint + `"}`)}
	handoffBytes, _ := json.Marshal(handoff)
	repo := &memoryRepository{objects: map[string][]byte{"runs/run_1/snapshot.json": snapshotBytes, "runs/run_1/history/00000000000000000004.json": handoffBytes}}
	coord := &memoryCoordinator{state: state.Coordination{ActiveRunID: "run_1", Phase: domain.DeliveryPending, LastSequence: 4}}
	svc := &Service{Repository: repo, Manifests: memoryManifest{data: manifestBytes}, Coordinator: coord, ArtifactBucket: "artifacts", Now: func() time.Time { return fixedTime }, Sleep: func(context.Context, time.Duration) error { return nil }}
	return svc, repo, coord, manifestBytes
}

func acceptedResponse(runID string) *http.Response {
	body := `{"jobId":"job-1","status":"ACCEPTED","eventId":"urn:bond-platform:submission:` + runID + `","runId":"` + runID + `","createdAt":"2026-10-01T01:00:00Z","statusUrl":"https://processor.example/v1/processing-jobs/job-1"}`
	return &http.Response{StatusCode: 202, Header: http.Header{"Location": []string{"https://processor.example/v1/processing-jobs/job-1"}, "Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}
}

func TestSubmissionProfilesAndLimits(t *testing.T) {
	svc, _, _, raw := fixture(t, true)
	_ = svc
	var manifest pull.Manifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	ref := Reference{Bucket: "artifacts", Key: "runs/run_1/manifest.json"}
	data, err := BuildSubmission(manifest, ref)
	if err != nil {
		t.Fatal(err)
	}
	var event Submission
	if err := json.Unmarshal(data, &event); err != nil {
		t.Fatal(err)
	}
	if event.ID == manifest.ID || event.Data.Manifest != ref || event.Data.DatasetFingerprint != fingerprint || event.Data.RunID != "run_1" {
		t.Fatal(event)
	}
	manifest.DataSchema = "urn:bond-platform:dataset:bse-debt-trades"
	manifest.Data.EventType = "daily-bhavcopy"
	manifest.Data.Inputs = map[string]string{"exchangeName": "BSE", "tradeDate": "2026-09-21"}
	manifest.Subject = "exchange/BSE/trade-date/2026-09-21"
	if _, err := BuildSubmission(manifest, ref); err != nil {
		t.Fatal(err)
	}
	manifest.Data.Inputs["exchangeName"] = "NSE"
	if _, err := BuildSubmission(manifest, ref); err == nil {
		t.Fatal("non-BSE processor profile accepted")
	}
	manifest.Data.Inputs["exchangeName"] = "BSE"
	manifest.Data.Inputs["tradeDate"] = "bad"
	if _, err := BuildSubmission(manifest, ref); err == nil {
		t.Fatal("bad date accepted")
	}
	manifest.DataSchema = "other"
	if _, err := BuildSubmission(manifest, ref); err == nil {
		t.Fatal("unknown schema accepted")
	}
	manifest.DataSchema = "urn:bond-platform:dataset:nsdl-security"
	manifest.Data.EventType = "nsdl-bond-data"
	manifest.Data.Inputs = map[string]string{"isin_code": "lower"}
	manifest.Subject = "isin/lower"
	if _, err := BuildSubmission(manifest, ref); err == nil {
		t.Fatal("lowercase ISIN accepted")
	}
	manifest.Data.Inputs = map[string]string{"isin_code": "INE121A07QY9"}
	manifest.Subject = "isin/INE121A07QY9"
	manifest.Data.DatasetFingerprint = "invalid"
	if _, err := BuildSubmission(manifest, ref); err == nil {
		t.Fatal("fingerprint accepted")
	}
	manifest.Data.DatasetFingerprint = fingerprint
	manifest.Data.EventID = strings.Repeat("x", 70000)
	if _, err := BuildSubmission(manifest, ref); err == nil {
		t.Fatal("oversize submission accepted")
	}
	manifest.Data.EventID = "event-1"
	manifest.Data.DatasetFingerprint = "sha256:" + strings.Repeat("z", 64)
	if _, err := BuildSubmission(manifest, ref); err == nil {
		t.Fatal("non-hex fingerprint accepted")
	}
	manifest.Data.DatasetFingerprint = fingerprint
	manifest.Data.RunID = ".."
	if _, err := BuildSubmission(manifest, ref); err == nil {
		t.Fatal("unsafe run ID accepted")
	}
}

func TestDeliveryRunIDsAreSafeForStateAndProcessor(t *testing.T) {
	for _, id := range []string{"run_1", "RUN-99.test", "run_" + strings.Repeat("a0", 16)} {
		if !validRunID(id) {
			t.Fatal(id)
		}
	}
	for _, id := range []string{"", ".", "..", "run/1", "run 1", "run:1", "run?1", strings.Repeat("x", 129)} {
		if validRunID(id) {
			t.Fatal(id)
		}
	}
}

func TestDeliveryAcceptanceAndRetryEnvelope(t *testing.T) {
	svc, repo, coord, _ := fixture(t, true)
	var bodies [][]byte
	svc.Client = clientFunc(func(req *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(req.Body)
		bodies = append(bodies, body)
		if req.Header.Get("Idempotency-Key") != "run_1" || req.Header.Get("Content-Type") != "application/cloudevents+json" || req.GetBody != nil {
			t.Fatal(req.Header)
		}
		if len(bodies) == 1 {
			return nil, errors.New("connection lost")
		}
		return acceptedResponse("run_1"), nil
	})
	if err := svc.Run(context.Background(), "run_1", "token", fixedTime.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 2 || !bytes.Equal(bodies[0], bodies[1]) || !bytes.Equal(repo.objects["runs/run_1/delivery/submission.json"], bodies[0]) {
		t.Fatal("retries changed request")
	}
	if len(coord.commits) != 2 || coord.commits[1].Phase != domain.Completed || coord.commits[1].Acceptance == nil || coord.commits[1].Acceptance.Fingerprint != fingerprint || len(repo.objects["runs/run_1/delivery/accepted.json"]) == 0 {
		t.Fatal(coord.commits)
	}
}

func TestDeliveryFailureModes(t *testing.T) {
	for _, scenario := range []string{"disabled", "http200", "http400", "http503", "bad202", "write_failure", "deadline"} {
		t.Run(scenario, func(t *testing.T) {
			svc, repo, coord, _ := fixture(t, scenario != "disabled")
			calls := 0
			svc.Client = clientFunc(func(*http.Request) (*http.Response, error) {
				calls++
				status := 200
				switch scenario {
				case "http400":
					status = 400
				case "http503":
					status = 503
				case "bad202":
					status = 202
				}
				return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
			})
			if scenario == "write_failure" {
				repo.createError = "runs/run_1/delivery/1/finished.json"
			}
			deadline := fixedTime.Add(time.Minute)
			if scenario == "deadline" {
				deadline = fixedTime.Add(time.Second)
			}
			err := svc.Run(context.Background(), "run_1", "token", deadline)
			if scenario == "write_failure" || scenario == "deadline" {
				if err == nil || len(coord.commits) != 1 {
					t.Fatal(err, coord.commits)
				}
				return
			}
			if err != nil || coord.state.Phase != domain.Failed || coord.commits[len(coord.commits)-1].Acceptance != nil {
				t.Fatal(err, coord.commits)
			}
			if scenario == "disabled" && calls != 0 || scenario == "http503" && calls != 3 || scenario == "http200" && calls != 1 || scenario == "http400" && calls != 1 || scenario == "bad202" && calls != 3 {
				t.Fatal(calls)
			}
		})
	}
}

func TestDeliveryClaimAndManifestIntegrity(t *testing.T) {
	svc, _, coord, _ := fixture(t, true)
	svc.Client = clientFunc(func(*http.Request) (*http.Response, error) { t.Fatal("unexpected HTTP"); return nil, nil })
	coord.claimErr = state.ErrConflict
	coord.state.ActiveRunID = "other"
	if !errors.Is(svc.Run(context.Background(), "run_1", "token", fixedTime.Add(time.Minute)), ErrStaleDeliveryMessage) {
		t.Fatal("stale claim")
	}
	coord.claimErr = nil
	coord.state.ActiveRunID = "run_1"
	svc.Manifests = memoryManifest{err: errors.New("S3 unavailable")}
	if err := svc.Run(context.Background(), "run_1", "token", fixedTime.Add(time.Minute)); err == nil {
		t.Fatal("missing manifest should be retryable")
	}
	svc, _, coord, _ = fixture(t, true)
	svc.Client = clientFunc(func(*http.Request) (*http.Response, error) { t.Fatal("unexpected HTTP"); return nil, nil })
	coord.claimErr = state.ErrConflict
	coord.state.Phase = domain.Delivering
	if err := svc.Run(context.Background(), "run_1", "token", fixedTime.Add(time.Minute)); !errors.Is(err, state.ErrConflict) || errors.Is(err, ErrStaleDeliveryMessage) {
		t.Fatal("unfinished delivery must remain retryable", err)
	}
}

func TestAcceptedReceiptSurvivesCommitInterruption(t *testing.T) {
	svc, repo, coord, _ := fixture(t, true)
	coord.failPhase = domain.Completed
	svc.Client = clientFunc(func(*http.Request) (*http.Response, error) { return acceptedResponse("run_1"), nil })
	if err := svc.Run(context.Background(), "run_1", "token", fixedTime.Add(time.Minute)); err == nil {
		t.Fatal("expected interrupted commit")
	}
	if coord.state.Phase != domain.Delivering || len(repo.objects["runs/run_1/delivery/accepted.json"]) == 0 || len(repo.objects["runs/run_1/delivery/1/finished.json"]) == 0 {
		t.Fatal("acceptance evidence lost or false baseline")
	}
}

func TestLongRetryAfterNeverSendsEarly(t *testing.T) {
	svc, repo, coord, _ := fixture(t, true)
	calls := 0
	svc.Client = clientFunc(func(*http.Request) (*http.Response, error) {
		calls++
		body := `{"type":"urn:bond-platform:problem:service-unavailable","title":"Unavailable","status":503,"detail":"wait","instance":"urn:uuid:test","code":"SERVICE_UNAVAILABLE","correlationId":"test"}`
		return &http.Response{StatusCode: 503, Header: http.Header{"Retry-After": []string{"300"}, "Content-Type": []string{"application/problem+json"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	if err := svc.Run(context.Background(), "run_1", "token", fixedTime.Add(time.Minute)); err == nil {
		t.Fatal("long delay should leave recoverable work")
	}
	if calls != 1 || coord.state.Phase != domain.Delivering {
		t.Fatal(calls, coord.state.Phase)
	}
	var outcome Outcome
	if err := json.Unmarshal(repo.objects["runs/run_1/delivery/1/finished.json"], &outcome); err != nil {
		t.Fatal(err)
	}
	if outcome.RetryAfterSeconds != 300 || outcome.FinishedAt.IsZero() {
		t.Fatal(outcome)
	}
}

func TestUnknownAttemptExhaustionKeepsOldBaseline(t *testing.T) {
	svc, repo, coord, _ := fixture(t, true)
	calls := 0
	svc.Client = clientFunc(func(*http.Request) (*http.Response, error) { calls++; return nil, errors.New("connection lost") })
	if err := svc.Run(context.Background(), "run_1", "token", fixedTime.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if calls != 3 || coord.state.Phase != domain.Failed || coord.commits[1].Acceptance != nil || !bytes.Contains(coord.commits[1].Details, []byte("DELIVERY_OUTCOME_UNKNOWN")) {
		t.Fatal(calls, coord.commits)
	}
	for attempt := 1; attempt <= 3; attempt++ {
		if len(repo.objects[fmt.Sprintf("runs/run_1/delivery/%d/finished.json", attempt)]) == 0 {
			t.Fatal(attempt)
		}
	}
}

func TestAcceptedRecoveryRejectsIncompleteEvidenceAndWrongOwnership(t *testing.T) {
	for _, scenario := range []string{"invalid-run", "snapshot-missing", "snapshot-invalid", "submission-missing", "submission-invalid", "accepted-missing", "finished-invalid", "finished-read-error", "accepted-invalid", "receipt-mismatch", "pending-other", "completed-other", "wrong-token"} {
		t.Run(scenario, func(t *testing.T) {
			svc, repo, coord, _ := fixture(t, true)
			coord.failPhase = domain.Completed
			svc.Client = clientFunc(func(*http.Request) (*http.Response, error) { return acceptedResponse("run_1"), nil })
			if err := svc.Run(context.Background(), "run_1", "token", fixedTime.Add(time.Minute)); err == nil {
				t.Fatal("missing injected completion error")
			}
			lease := state.Lease{RunID: "run_1", Generation: coord.state.Generation, Token: "token"}
			run := "run_1"
			switch scenario {
			case "invalid-run":
				run = ".."
			case "snapshot-missing":
				delete(repo.objects, "runs/run_1/snapshot.json")
			case "snapshot-invalid":
				repo.objects["runs/run_1/snapshot.json"] = []byte(`{}`)
			case "submission-missing":
				delete(repo.objects, "runs/run_1/delivery/submission.json")
			case "submission-invalid":
				repo.objects["runs/run_1/delivery/submission.json"] = []byte(`{}`)
			case "accepted-missing":
				delete(repo.objects, "runs/run_1/delivery/accepted.json")
				delete(repo.objects, "runs/run_1/delivery/1/finished.json")
			case "finished-invalid":
				delete(repo.objects, "runs/run_1/delivery/accepted.json")
				repo.objects["runs/run_1/delivery/1/finished.json"] = []byte(`[`)
			case "finished-read-error":
				delete(repo.objects, "runs/run_1/delivery/accepted.json")
				repo.readError = "runs/run_1/delivery/1/finished.json"
			case "accepted-invalid":
				repo.objects["runs/run_1/delivery/accepted.json"] = []byte(`{}`)
			case "receipt-mismatch":
				var outcome Outcome
				if err := json.Unmarshal(repo.objects["runs/run_1/delivery/accepted.json"], &outcome); err != nil {
					t.Fatal(err)
				}
				outcome.Receipt.RunID = "other"
				repo.objects["runs/run_1/delivery/accepted.json"] = mustJSON(t, outcome)
			case "pending-other":
				coord.state.Pending = &state.Transition{RunID: "run_1", Phase: domain.Failed}
			case "completed-other":
				coord.state.Phase = domain.Completed
				coord.state.AcceptedBaseline = &state.Baseline{RunID: "other"}
			case "wrong-token":
				lease.Token = "other"
			}
			if err := svc.RecoverAccepted(context.Background(), run, lease); err == nil {
				t.Fatal("invalid evidence was accepted")
			}
		})
	}
}

func TestDeliveryResumeKeepsAttemptBudgetAndSubmission(t *testing.T) {
	for _, scenario := range []string{"lost-response", "lost-finished", "lost-accepted", "terminal-commit", "exhausted-commit"} {
		t.Run(scenario, func(t *testing.T) {
			svc, repo, coord, _ := fixture(t, true)
			calls := 0
			svc.Client = clientFunc(func(req *http.Request) (*http.Response, error) {
				calls++
				if req.GetBody != nil || req.Header.Get("Idempotency-Key") != "run_1" {
					t.Fatal("unstable replay")
				}
				switch scenario {
				case "lost-response", "exhausted-commit":
					if scenario == "lost-response" && calls == 2 {
						return acceptedResponse("run_1"), nil
					}
					return nil, errors.New("response lost")
				case "terminal-commit":
					return &http.Response{StatusCode: 400, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
				default:
					return acceptedResponse("run_1"), nil
				}
			})
			switch scenario {
			case "lost-response":
				svc.Sleep = func(context.Context, time.Duration) error { return context.Canceled }
			case "lost-finished":
				repo.createError = "runs/run_1/delivery/1/finished.json"
			case "lost-accepted":
				repo.createError = "runs/run_1/delivery/accepted.json"
			case "terminal-commit", "exhausted-commit":
				coord.failPhase = domain.Failed
			}
			if err := svc.Run(context.Background(), "run_1", "first", fixedTime.Add(time.Minute)); err == nil {
				t.Fatal("expected interruption")
			}
			firstCalls := calls
			repo.createError = ""
			coord.failPhase = domain.State("")
			svc.Sleep = func(context.Context, time.Duration) error { return nil }
			if err := svc.Resume(context.Background(), "run_1", "resumer", fixedTime.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			if scenario == "lost-accepted" || scenario == "terminal-commit" || scenario == "exhausted-commit" {
				if calls != firstCalls {
					t.Fatal("recovery made extra HTTP call", calls, firstCalls)
				}
			} else if calls != firstCalls+1 || len(repo.objects["runs/run_1/delivery/2/started.json"]) == 0 {
				t.Fatal("unknown attempt was reused", calls, firstCalls)
			}
			if scenario == "terminal-commit" || scenario == "exhausted-commit" {
				if coord.state.Phase != domain.Failed || coord.commits[len(coord.commits)-1].Acceptance != nil {
					t.Fatal(coord.commits)
				}
			} else if coord.state.Phase != domain.Completed || coord.commits[len(coord.commits)-1].Acceptance == nil {
				t.Fatal(coord.commits)
			}
		})
	}
}

func TestDeliveryResumeHonorsPersistedRetryAfter(t *testing.T) {
	svc, repo, _, _ := fixture(t, true)
	now := fixedTime
	svc.Now = func() time.Time { return now }
	calls := 0
	svc.Client = clientFunc(func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			body := `{"type":"urn:bond-platform:problem:service-unavailable","title":"Unavailable","status":503,"detail":"wait","instance":"urn:uuid:test","code":"SERVICE_UNAVAILABLE","correlationId":"test"}`
			return &http.Response{StatusCode: 503, Header: http.Header{"Retry-After": []string{"300"}, "Content-Type": []string{"application/problem+json"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
		}
		return acceptedResponse("run_1"), nil
	})
	if err := svc.Run(context.Background(), "run_1", "first", now.Add(time.Minute)); err == nil {
		t.Fatal("expected deferred retry")
	}
	if err := svc.Resume(context.Background(), "run_1", "second", now.Add(time.Minute)); !errors.Is(err, ErrRetryNotReady) {
		t.Fatal(err)
	}
	if calls != 1 || len(repo.objects["runs/run_1/delivery/2/started.json"]) != 0 {
		t.Fatal("retry sent before delay")
	}
	now = now.Add(5 * time.Minute)
	if err := svc.Resume(context.Background(), "run_1", "third", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatal(calls)
	}
}

func TestDeliveryResumeRebuildsMissingSubmissionAfterPhaseCommit(t *testing.T) {
	svc, repo, coord, _ := fixture(t, true)
	repo.createError = "runs/run_1/delivery/submission.json"
	calls := 0
	svc.Client = clientFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return acceptedResponse("run_1"), nil
	})
	if err := svc.Run(context.Background(), "run_1", "first", fixedTime.Add(time.Minute)); err == nil || coord.state.Phase != domain.Delivering || calls != 0 {
		t.Fatal(err, coord.state.Phase, calls)
	}
	repo.createError = ""
	if err := svc.Resume(context.Background(), "run_1", "recovery", fixedTime.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || coord.state.Phase != domain.Completed || len(repo.objects["runs/run_1/delivery/submission.json"]) == 0 {
		t.Fatal(calls, coord.state.Phase)
	}
}

func TestDeliveryResumeRejectsMissingOrCorruptRecoveryEvidence(t *testing.T) {
	for _, scenario := range []string{"invalid-invocation", "missing-snapshot", "corrupt-snapshot", "not-delivering", "missing-manifest", "bad-handoff", "bad-submission", "corrupt-started", "corrupt-finished"} {
		t.Run(scenario, func(t *testing.T) {
			svc, repo, coord, _ := fixture(t, true)
			coord.state.Phase = domain.Delivering
			coord.state.LastSequence = 5
			calls := 0
			svc.Client = clientFunc(func(*http.Request) (*http.Response, error) { calls++; return acceptedResponse("run_1"), nil })
			submissionKey := "runs/run_1/delivery/submission.json"
			switch scenario {
			case "invalid-invocation":
				svc.Client = nil
			case "missing-snapshot":
				delete(repo.objects, "runs/run_1/snapshot.json")
			case "corrupt-snapshot":
				repo.objects["runs/run_1/snapshot.json"] = []byte("{")
			case "not-delivering":
				coord.state.Phase = domain.Failed
			case "missing-manifest":
				svc.Manifests = memoryManifest{err: state.ErrNotFound}
			case "bad-handoff":
				repo.objects["runs/run_1/history/00000000000000000004.json"] = []byte("{}")
			case "bad-submission":
				repo.objects[submissionKey] = []byte("{}")
			case "corrupt-started", "corrupt-finished":
				body, err := svc.prepareSubmission(context.Background(), events.Snapshot{RunID: "run_1", ConfigRevision: "rev1", Event: events.Normalized{EventID: "event-1"}}, 4)
				if err != nil {
					t.Fatal(err)
				}
				repo.objects[submissionKey] = body
				repo.objects["runs/run_1/delivery/1/started.json"] = []byte("{}")
				if scenario == "corrupt-started" {
					repo.objects["runs/run_1/delivery/1/started.json"] = nil
				} else {
					repo.objects["runs/run_1/delivery/1/finished.json"] = []byte("{")
				}
			}
			if err := svc.Resume(context.Background(), "run_1", "recovery", fixedTime.Add(time.Minute)); err == nil || calls != 0 {
				t.Fatal(scenario, err, calls)
			}
		})
	}
}

func TestDeliveryLedgerRejectsAmbiguousEvidence(t *testing.T) {
	for _, scenario := range []string{"started-read-error", "finished-read-error", "missing-finished", "receipt-without-acceptance", "retry-without-time", "future-finished"} {
		t.Run(scenario, func(t *testing.T) {
			svc, repo, coord, _ := fixture(t, true)
			coord.state.Phase = domain.Delivering
			started := "runs/run_1/delivery/1/started.json"
			finished := "runs/run_1/delivery/1/finished.json"
			repo.objects[started] = []byte("{}")
			outcome := Outcome{Code: "TRANSPORT_UNKNOWN", Retry: true, FinishedAt: fixedTime}
			switch scenario {
			case "started-read-error":
				repo.readError = started
			case "finished-read-error":
				repo.readError = finished
			case "missing-finished":
			case "receipt-without-acceptance":
				outcome.Receipt = &Receipt{RunID: "run_1"}
			case "retry-without-time":
				outcome.RetryAfterSeconds = 10
				outcome.FinishedAt = time.Time{}
			case "future-finished":
				outcome.RetryAfterSeconds = 10
				outcome.FinishedAt = fixedTime.Add(time.Hour)
			}
			if scenario != "missing-finished" {
				data, _ := json.Marshal(outcome)
				repo.objects[finished] = data
			}
			next, _, err := svc.nextAttempt(context.Background(), "coordination/exec1.json", state.Lease{RunID: "run_1"}, events.Snapshot{RunID: "run_1", Config: config.Config{Processor: config.Processor{MaxAttempts: 3}}})
			if scenario == "missing-finished" {
				if err != nil || next != 2 {
					t.Fatal(next, err)
				}
			} else if err == nil {
				t.Fatal("ambiguous ledger accepted", scenario)
			}
		})
	}
}

func TestDeliveryRunRequiresPinnedHandoffEvidence(t *testing.T) {
	for _, scenario := range []string{"missing-snapshot", "bad-snapshot-json", "wrong-snapshot-run", "missing-handoff", "bad-handoff-json", "bad-manifest-json", "mismatched-manifest", "invalid-profile", "bad-attempt-budget"} {
		t.Run(scenario, func(t *testing.T) {
			svc, repo, coord, raw := fixture(t, true)
			calls := 0
			svc.Client = clientFunc(func(*http.Request) (*http.Response, error) { calls++; return acceptedResponse("run_1"), nil })
			switch scenario {
			case "missing-snapshot":
				delete(repo.objects, "runs/run_1/snapshot.json")
			case "bad-snapshot-json":
				repo.objects["runs/run_1/snapshot.json"] = []byte("{")
			case "wrong-snapshot-run":
				var snapshot events.Snapshot
				_ = json.Unmarshal(repo.objects["runs/run_1/snapshot.json"], &snapshot)
				snapshot.RunID = "other"
				repo.objects["runs/run_1/snapshot.json"], _ = json.Marshal(snapshot)
			case "missing-handoff":
				delete(repo.objects, "runs/run_1/history/00000000000000000004.json")
			case "bad-handoff-json":
				repo.objects["runs/run_1/history/00000000000000000004.json"] = []byte("{")
			case "bad-manifest-json":
				svc.Manifests = memoryManifest{data: []byte("{")}
			case "mismatched-manifest":
				var manifest pull.Manifest
				_ = json.Unmarshal(raw, &manifest)
				manifest.Data.ConfigRevision = "other"
				data, _ := json.Marshal(manifest)
				svc.Manifests = memoryManifest{data: data}
			case "invalid-profile":
				var manifest pull.Manifest
				_ = json.Unmarshal(raw, &manifest)
				manifest.DataSchema = "unsupported"
				data, _ := json.Marshal(manifest)
				svc.Manifests = memoryManifest{data: data}
			case "bad-attempt-budget":
				var snapshot events.Snapshot
				_ = json.Unmarshal(repo.objects["runs/run_1/snapshot.json"], &snapshot)
				snapshot.Config.Processor.MaxAttempts = 0
				repo.objects["runs/run_1/snapshot.json"], _ = json.Marshal(snapshot)
			}
			err := svc.Run(context.Background(), "run_1", "worker", fixedTime.Add(time.Minute))
			if calls != 0 || scenario != "invalid-profile" && err == nil || scenario == "invalid-profile" && (err != nil || coord.state.Phase != domain.Failed) {
				t.Fatal(scenario, err, calls, coord.state.Phase)
			}
		})
	}
}

func TestProcessorResponseClassification(t *testing.T) {
	svc, _, _, _ := fixture(t, true)
	for _, scenario := range []string{"transport", "nil", "no-body", "oversize", "bad202", "rate-limit", "service-error", "redirect", "close-error", "accepted"} {
		t.Run(scenario, func(t *testing.T) {
			svc.Client = clientFunc(func(*http.Request) (*http.Response, error) {
				switch scenario {
				case "transport":
					return nil, errors.New("lost response")
				case "nil":
					return nil, nil
				case "no-body":
					return &http.Response{StatusCode: 202}, nil
				case "oversize":
					return &http.Response{StatusCode: 202, Body: io.NopCloser(strings.NewReader(strings.Repeat("x", 65537)))}, nil
				case "bad202":
					return &http.Response{StatusCode: 202, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
				case "rate-limit":
					return &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": []string{"2"}}, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
				case "service-error":
					return &http.Response{StatusCode: 500, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
				case "redirect":
					return &http.Response{StatusCode: 302, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
				case "close-error":
					response := acceptedResponse("run_1")
					response.Body = closeErrorBody{response.Body}
					return response, nil
				default:
					return acceptedResponse("run_1"), nil
				}
			})
			out := svc.send(context.Background(), "https://processor.example/v1/event-ingestions", []byte(`{}`), "run_1", time.Second)
			if scenario == "accepted" && (out.Receipt == nil || out.Retry) {
				t.Fatal(out)
			}
			if scenario == "rate-limit" && (!out.Retry || out.RetryAfterSeconds != 2) {
				t.Fatal(out)
			}
			if scenario == "redirect" && (out.Retry || out.Code != "PROCESSOR_HTTP_302") {
				t.Fatal(out)
			}
			if scenario != "accepted" && scenario != "redirect" && scenario != "rate-limit" && !out.Retry {
				t.Fatal(out)
			}
		})
	}
	if out := svc.send(context.Background(), "http://processor.example/v1/event-ingestions", nil, "run_1", time.Second); out.Code != "INVALID_PROCESSOR_URL" {
		t.Fatal(out)
	}
	if out := svc.send(context.Background(), "https://processor.example/wrong", nil, "run_1", time.Second); out.Code != "INVALID_PROCESSOR_URL" {
		t.Fatal(out)
	}
}

func TestProcessorMalformedAcceptedBodyIsUnknownOutcome(t *testing.T) {
	svc, _, _, _ := fixture(t, true)
	svc.Client = clientFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 202, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader("{"))}, nil
	})
	outcome := svc.send(context.Background(), "https://processor.example/v1/event-ingestions", []byte("{}"), "run_1", time.Second)
	if outcome.Code != "INVALID_RECEIPT" || !outcome.Retry || outcome.Receipt != nil {
		t.Fatal(outcome)
	}
}

func TestProcessorProblemValidation(t *testing.T) {
	svc, _, _, _ := fixture(t, true)
	for _, status := range []int{400, 409, 413, 415, 429, 500, 503} {
		svc.Client = clientFunc(func(*http.Request) (*http.Response, error) {
			body := fmt.Sprintf(`{"type":"urn:bond-platform:problem:test","title":"Failure","status":%d,"detail":"test failure","instance":"urn:uuid:test","code":"TEST_FAILURE","correlationId":"test"}`, status)
			return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/problem+json"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
		})
		out := svc.send(context.Background(), "https://processor.example/v1/event-ingestions", []byte(`{}`), "run_1", time.Second)
		if out.Code != "TEST_FAILURE" || out.Retry != (status == 429 || status == 500 || status == 503) {
			t.Fatal(status, out)
		}
	}
}

func TestOneRecordedAttemptCannotTriggerTransportReplay(t *testing.T) {
	var dials atomic.Int32
	transport := &http.Transport{MaxConnsPerHost: 1, DialTLSContext: func(context.Context, string, string) (net.Conn, error) {
		if dials.Add(1) > 1 {
			return nil, errors.New("hidden retry attempted")
		}
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			reader := bufio.NewReader(server)
			first, err := http.ReadRequest(reader)
			if err != nil {
				return
			}
			_, _ = io.Copy(io.Discard, first.Body)
			_ = first.Body.Close()
			_, _ = io.WriteString(server, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
			second, err := http.ReadRequest(reader)
			if err != nil {
				return
			}
			_, _ = io.Copy(io.Discard, second.Body)
			_ = second.Body.Close()
			// Drop the accepted-looking request before sending an HTTP response.
		}()
		return client, nil
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	prime, err := client.Get("https://processor.example/ready")
	if err != nil {
		t.Fatal(err)
	}
	if err := prime.Body.Close(); err != nil {
		t.Fatal(err)
	}
	svc, _, _, _ := fixture(t, true)
	svc.Client = client
	result := svc.send(context.Background(), "https://processor.example/v1/event-ingestions", []byte(`{"run_id":"run_1"}`), "run_1", time.Second)
	if result.Code != "TRANSPORT_UNKNOWN" || dials.Load() != 1 {
		t.Fatal(result, dials.Load())
	}
}

type closeErrorBody struct{ io.ReadCloser }

func (b closeErrorBody) Close() error { return errors.New("close failed") }

func TestDeliveryPersistenceAndInvocationFailures(t *testing.T) {
	for _, scenario := range []string{"invalid", "snapshot-missing", "snapshot-mismatch", "claim", "commit", "history-missing", "history-invalid", "fingerprint", "submission-conflict", "started-failure", "accepted-failure", "complete-failure", "fail-failure"} {
		t.Run(scenario, func(t *testing.T) {
			svc, repo, coord, raw := fixture(t, true)
			svc.Client = clientFunc(func(*http.Request) (*http.Response, error) { return acceptedResponse("run_1"), nil })
			switch scenario {
			case "invalid":
				svc.Now = nil
			case "snapshot-missing":
				delete(repo.objects, "runs/run_1/snapshot.json")
			case "snapshot-mismatch":
				repo.objects["runs/run_1/snapshot.json"] = []byte(`{"schema_version":1,"run_id":"other"}`)
			case "claim":
				coord.claimErr = errors.New("CAS unavailable")
			case "commit":
				coord.commitErr = errors.New("CAS unavailable")
			case "history-missing":
				delete(repo.objects, "runs/run_1/history/00000000000000000004.json")
			case "history-invalid":
				repo.objects["runs/run_1/history/00000000000000000004.json"] = []byte(`{}`)
			case "fingerprint":
				var manifest pull.Manifest
				_ = json.Unmarshal(raw, &manifest)
				manifest.Data.DatasetFingerprint = "wrong"
				svc.Manifests = memoryManifest{data: mustJSON(t, manifest)}
			case "submission-conflict":
				repo.objects["runs/run_1/delivery/submission.json"] = []byte(`wrong`)
			case "started-failure":
				repo.createError = "runs/run_1/delivery/1/started.json"
			case "accepted-failure":
				repo.createError = "runs/run_1/delivery/accepted.json"
			case "complete-failure":
				coord.commitErr = errors.New("CAS unavailable")
			case "fail-failure":
				coord.commitErr = errors.New("CAS unavailable")
			}
			if scenario == "fail-failure" {
				svc.Client = clientFunc(func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: 400, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
				})
			}
			if err := svc.Run(context.Background(), "run_1", "token", fixedTime.Add(time.Minute)); err == nil {
				t.Fatal("expected failure")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(sleepContext(ctx, time.Hour), context.Canceled) {
		t.Fatal("cancellation ignored")
	}
	if err := sleepContext(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
