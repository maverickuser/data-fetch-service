package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/domain"
	"github.com/maverickuser/data-fetch-service/internal/events"
	"github.com/maverickuser/data-fetch-service/internal/pull"
	"github.com/maverickuser/data-fetch-service/internal/state"
)

// Repository stores the admitted snapshot, stable submission, and delivery evidence.
type Repository interface {
	Read(context.Context, string, time.Time) (state.Object, error)
	Create(context.Context, string, []byte) error
}

// ManifestReader retrieves the completed manifest artifact with a strict byte bound.
type ManifestReader interface {
	Read(context.Context, string) ([]byte, error)
}

// Coordinator fences a delivery owner and commits recoverable terminal transitions.
type Coordinator interface {
	ClaimDispatched(context.Context, string, string, string, string, time.Time) (state.Lease, error)
	Commit(context.Context, string, state.Lease, state.Transition) error
	Load(context.Context, string) (state.Coordination, string, error)
	Repair(context.Context, string) error
}

// HTTPClient is the private processor transport; production disables redirects/proxies.
type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

// Service delivers one accepted complete dataset under a fenced execution claim.
type Service struct {
	Repository     Repository
	Manifests      ManifestReader
	Coordinator    Coordinator
	Client         HTTPClient
	ArtifactBucket string
	Now            func() time.Time
	Sleep          func(context.Context, time.Duration) error
}

var ErrStaleDeliveryMessage = errors.New("delivery message already owned or completed")

// Run reserves each HTTP attempt before sending and commits success only after durable 202 evidence.
func (s *Service) Run(ctx context.Context, runID, token string, deadline time.Time) error {
	if s.Repository == nil || s.Manifests == nil || s.Coordinator == nil || s.Client == nil || s.ArtifactBucket == "" || s.Now == nil || !validRunID(runID) || token == "" || !deadline.After(s.Now()) {
		return fmt.Errorf("invalid delivery invocation")
	}
	snapshotObject, err := s.Repository.Read(ctx, "runs/"+runID+"/snapshot.json", s.Now())
	if err != nil {
		return err
	}
	var snapshot events.Snapshot
	if err := json.Unmarshal(snapshotObject.Data, &snapshot); err != nil {
		return err
	}
	if snapshot.SchemaVersion != 1 || snapshot.RunID != runID || snapshot.ExecutionKey == "" {
		return state.ErrIntegrity
	}
	key := "coordination/" + snapshot.ExecutionKey + ".json"
	lease, err := s.Coordinator.ClaimDispatched(ctx, key, runID, "delivery", token, deadline.Add(time.Minute))
	if err != nil {
		if errors.Is(err, state.ErrConflict) {
			current, _, readErr := s.Coordinator.Load(ctx, key)
			if readErr == nil && (current.ActiveRunID != runID || current.Phase.Terminal() || current.Phase == domain.DeliveryPending && current.OwnerToken != "" && current.OwnerToken != token && s.Now().Before(current.OwnerDeadline)) {
				return fmt.Errorf("%w: %w", ErrStaleDeliveryMessage, err)
			}
		}
		return err
	}
	current, _, err := s.Coordinator.Load(ctx, key)
	if err != nil {
		return err
	}
	if err := s.Coordinator.Commit(ctx, key, lease, state.Transition{RunID: runID, Sequence: current.LastSequence + 1, Phase: domain.Delivering, At: s.Now().UTC()}); err != nil {
		return err
	}
	if !snapshot.Config.Processor.Enabled {
		return s.fail(ctx, key, lease, runID, "PROCESSOR_NOT_CONFIGURED")
	}
	manifestObject, err := s.Repository.Read(ctx, fmt.Sprintf("runs/%s/history/%020d.json", runID, current.LastSequence), s.Now())
	if err != nil {
		return err
	}
	var handoff state.Transition
	if err := json.Unmarshal(manifestObject.Data, &handoff); err != nil {
		return err
	}
	var details struct {
		Bucket      string `json:"bucket"`
		Key         string `json:"manifest_key"`
		Fingerprint string `json:"dataset_fingerprint"`
	}
	if handoff.Phase != domain.DeliveryPending || json.Unmarshal(handoff.Details, &details) != nil || details.Bucket != s.ArtifactBucket || details.Key != "runs/"+runID+"/manifest.json" {
		return state.ErrIntegrity
	}
	manifestBytes, err := s.Manifests.Read(ctx, details.Key)
	if err != nil {
		return err
	}
	var manifest pull.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return err
	}
	if manifest.Data.RunID != runID || manifest.Data.ConfigRevision != snapshot.ConfigRevision || manifest.Data.DatasetFingerprint != details.Fingerprint || manifest.Data.EventID != snapshot.Event.EventID {
		return state.ErrIntegrity
	}
	body, err := BuildSubmission(manifest, Reference{Bucket: details.Bucket, Key: details.Key})
	if err != nil {
		return s.fail(ctx, key, lease, runID, "INVALID_SUBMISSION")
	}
	submissionKey := "runs/" + runID + "/delivery/submission.json"
	if err := s.Repository.Create(ctx, submissionKey, body); err != nil {
		return err
	}
	maxAttempts := snapshot.Config.Processor.MaxAttempts
	if maxAttempts < 1 || maxAttempts > 3 {
		return state.ErrIntegrity
	}
	allUnknown := true
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if !s.Now().Add(time.Duration(snapshot.Config.Processor.RequestTimeoutSeconds) * time.Second).Before(deadline.Add(-5 * time.Second)) {
			return fmt.Errorf("insufficient delivery budget")
		}
		startedKey := fmt.Sprintf("runs/%s/delivery/%d/started.json", runID, attempt)
		started, _ := json.Marshal(struct {
			RunID         string `json:"run_id"`
			Attempt       int    `json:"attempt"`
			SubmissionKey string `json:"submission_key"`
		}{runID, attempt, submissionKey})
		if err := s.Repository.Create(ctx, startedKey, started); err != nil {
			return err
		}
		outcome := s.send(ctx, snapshot.Config.Processor.URL, body, runID, time.Duration(snapshot.Config.Processor.RequestTimeoutSeconds)*time.Second)
		outcome.FinishedAt = s.Now().UTC()
		finished, _ := json.Marshal(outcome)
		if err := s.Repository.Create(ctx, fmt.Sprintf("runs/%s/delivery/%d/finished.json", runID, attempt), finished); err != nil {
			return err
		}
		if outcome.Receipt != nil {
			if err := s.Repository.Create(ctx, "runs/"+runID+"/delivery/accepted.json", finished); err != nil {
				return err
			}
			return s.RecoverAccepted(ctx, runID, lease)
		}
		if !outcome.Retry {
			return s.fail(ctx, key, lease, runID, outcome.Code)
		}
		if outcome.Code != "TRANSPORT_UNKNOWN" && outcome.Code != "INVALID_RESPONSE" && outcome.Code != "INVALID_RECEIPT" && outcome.Status != 500 {
			allUnknown = false
		}
		if attempt < maxAttempts {
			if outcome.RetryAfterSeconds > int64(deadline.Add(-5*time.Second).Sub(s.Now())/time.Second) {
				return fmt.Errorf("processor Retry-After exceeds invocation budget")
			}
			sleep := s.Sleep
			if sleep == nil {
				sleep = sleepContext
			}
			if err := sleep(ctx, time.Duration(outcome.RetryAfterSeconds)*time.Second); err != nil {
				return err
			}
		}
	}
	if allUnknown {
		return s.fail(ctx, key, lease, runID, "DELIVERY_OUTCOME_UNKNOWN")
	}
	return s.fail(ctx, key, lease, runID, "DELIVERY_ATTEMPTS_EXHAUSTED")
}

// validRunID confines every delivery evidence key to its admitted run directory.
func validRunID(runID string) bool {
	if runID == "" || runID == "." || runID == ".." || len(runID) > 128 {
		return false
	}
	for _, c := range runID {
		if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

// Outcome records the observed HTTP result independently of the attempt reservation.
type Outcome struct {
	Status            int       `json:"status,omitempty"`
	Code              string    `json:"code"`
	Retry             bool      `json:"retry"`
	RetryAfterSeconds int64     `json:"retry_after_seconds,omitempty"`
	FinishedAt        time.Time `json:"finished_at,omitempty"`
	Receipt           *Receipt  `json:"receipt,omitempty"`
}

// Receipt is the durable processor admission acknowledgement.
type Receipt struct {
	JobID     string    `json:"jobId"`
	Status    string    `json:"status"`
	EventID   string    `json:"eventId"`
	RunID     string    `json:"runId"`
	CreatedAt time.Time `json:"createdAt"`
	StatusURL string    `json:"statusUrl"`
}

// Problem is the processor's RFC 9457 error envelope for documented HTTP failures.
type Problem struct {
	Type          string `json:"type"`
	Title         string `json:"title"`
	Status        int    `json:"status"`
	Detail        string `json:"detail"`
	Instance      string `json:"instance"`
	Code          string `json:"code"`
	CorrelationID string `json:"correlationId"`
}

// send performs one bounded submission and accepts only a complete HTTP 202 receipt.
func (s *Service) send(ctx context.Context, target string, body []byte, runID string, timeout time.Duration) (result Outcome) {
	parsed, err := url.Parse(target)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Path != "/v1/event-ingestions" || parsed.User != nil {
		return Outcome{Code: "INVALID_PROCESSOR_URL"}
	}
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return Outcome{Code: "INVALID_PROCESSOR_URL"}
	}
	req.Header.Set("Content-Type", "application/cloudevents+json")
	req.Header.Set("Idempotency-Key", runID)
	req.GetBody = nil // Each recorded attempt owns exactly one transport send.
	response, err := s.Client.Do(req)
	if err != nil {
		return Outcome{Code: "TRANSPORT_UNKNOWN", Retry: true}
	}
	if response == nil || response.Body == nil {
		return Outcome{Code: "INVALID_RESPONSE", Retry: true}
	}
	defer func() {
		if err := response.Body.Close(); err != nil {
			result = Outcome{Status: response.StatusCode, Code: "INVALID_RESPONSE", Retry: true}
		}
	}()
	result = Outcome{Status: response.StatusCode}
	responseBytes, err := io.ReadAll(io.LimitReader(response.Body, 65537))
	if err != nil || len(responseBytes) > 65536 {
		result.Code = "INVALID_RESPONSE"
		result.Retry = true
		return result
	}
	if response.StatusCode == http.StatusAccepted {
		var receipt Receipt
		if json.Unmarshal(responseBytes, &receipt) != nil {
			result.Code = "INVALID_RECEIPT"
			result.Retry = true
			return result
		}
		statusURL, urlErr := url.Parse(receipt.StatusURL)
		if urlErr != nil || statusURL.Scheme != "https" || statusURL.Host == "" || statusURL.User != nil || !strings.HasPrefix(response.Header.Get("Content-Type"), "application/json") || receipt.JobID == "" || receipt.Status != "ACCEPTED" || receipt.EventID != "urn:bond-platform:submission:"+runID || receipt.RunID != runID || receipt.CreatedAt.IsZero() || response.Header.Get("Location") != receipt.StatusURL {
			result.Code = "INVALID_RECEIPT"
			result.Retry = true
			return result
		}
		result.Code = "ACCEPTED"
		result.Receipt = &receipt
		return result
	}
	result.Retry = response.StatusCode == 429 || response.StatusCode == 500 || response.StatusCode == 503
	result.Code = "PROCESSOR_HTTP_" + strconv.Itoa(response.StatusCode)
	if result.Retry {
		seconds, err := strconv.ParseInt(response.Header.Get("Retry-After"), 10, 64)
		if err == nil && seconds > 0 {
			result.RetryAfterSeconds = seconds
		}
	}
	if response.StatusCode == 400 || response.StatusCode == 409 || response.StatusCode == 413 || response.StatusCode == 415 || result.Retry {
		var problem Problem
		if !strings.HasPrefix(response.Header.Get("Content-Type"), "application/problem+json") || json.Unmarshal(responseBytes, &problem) != nil || problem.Status != response.StatusCode || problem.Type == "" || problem.Title == "" || problem.Detail == "" || problem.Instance == "" || problem.Code == "" || problem.CorrelationID == "" {
			result.Code = "INVALID_PROBLEM_RESPONSE"
		} else {
			result.Code = problem.Code
		}
	}
	return result
}

// RecoverAccepted finishes a durable 202 without another HTTP send after interruption.
// PR 08 may invoke it after restoring ownership of an abandoned delivery.
func (s *Service) RecoverAccepted(ctx context.Context, runID string, lease state.Lease) error {
	if s.Repository == nil || s.Coordinator == nil || s.Now == nil || !validRunID(runID) || lease.RunID != runID {
		return fmt.Errorf("invalid accepted-delivery recovery")
	}
	snapshotObject, err := s.Repository.Read(ctx, "runs/"+runID+"/snapshot.json", s.Now())
	if err != nil {
		return err
	}
	var snapshot events.Snapshot
	if json.Unmarshal(snapshotObject.Data, &snapshot) != nil || snapshot.ExecutionKey == "" || snapshot.RunID != runID {
		return state.ErrIntegrity
	}
	submissionObject, err := s.Repository.Read(ctx, "runs/"+runID+"/delivery/submission.json", s.Now())
	if err != nil {
		return err
	}
	var submission Submission
	if json.Unmarshal(submissionObject.Data, &submission) != nil || submission.ID != "urn:bond-platform:submission:"+runID || submission.Data.RunID != runID || submission.Data.DatasetFingerprint == "" {
		return state.ErrIntegrity
	}
	acceptedKey := "runs/" + runID + "/delivery/accepted.json"
	accepted, err := s.Repository.Read(ctx, acceptedKey, s.Now())
	if errors.Is(err, state.ErrNotFound) {
		for attempt := 1; attempt <= snapshot.Config.Processor.MaxAttempts && attempt <= 3; attempt++ {
			finished, readErr := s.Repository.Read(ctx, fmt.Sprintf("runs/%s/delivery/%d/finished.json", runID, attempt), s.Now())
			if errors.Is(readErr, state.ErrNotFound) {
				continue
			}
			if readErr != nil {
				return readErr
			}
			var outcome Outcome
			if json.Unmarshal(finished.Data, &outcome) != nil {
				return state.ErrIntegrity
			}
			if outcome.Receipt != nil {
				if err := s.Repository.Create(ctx, acceptedKey, finished.Data); err != nil {
					return err
				}
				accepted = finished
				err = nil
				break
			}
		}
	}
	if err != nil {
		return err
	}
	var outcome Outcome
	if json.Unmarshal(accepted.Data, &outcome) != nil || outcome.Status != 202 || outcome.Receipt == nil || outcome.Receipt.Status != "ACCEPTED" || outcome.Receipt.EventID != submission.ID || outcome.Receipt.RunID != runID || outcome.Receipt.CreatedAt.IsZero() {
		return state.ErrIntegrity
	}
	key := "coordination/" + snapshot.ExecutionKey + ".json"
	current, _, err := s.Coordinator.Load(ctx, key)
	if err != nil {
		return err
	}
	if current.Pending != nil {
		if current.Pending.Phase != domain.Completed || current.Pending.RunID != runID || current.Pending.Acceptance == nil {
			return state.ErrConflict
		}
		return s.Coordinator.Repair(ctx, key)
	}
	if current.Phase == domain.Completed {
		if current.AcceptedBaseline == nil || current.AcceptedBaseline.RunID != runID {
			return state.ErrConflict
		}
		return nil
	}
	if current.Phase != domain.Delivering || current.ActiveRunID != runID || current.Generation != lease.Generation || current.OwnerToken != lease.Token {
		return state.ErrConflict
	}
	return s.Coordinator.Commit(ctx, key, lease, state.Transition{RunID: lease.RunID, Sequence: current.LastSequence + 1, Phase: domain.Completed, At: s.Now().UTC(), Acceptance: &state.Acceptance{RunID: lease.RunID, Fingerprint: submission.Data.DatasetFingerprint, DatasetURN: submission.DataSchema, ConfigRevision: snapshot.ConfigRevision, AcceptedAt: outcome.Receipt.CreatedAt}})
}

// fail records a terminal delivery reason without creating an accepted baseline.
func (s *Service) fail(ctx context.Context, key string, lease state.Lease, runID, code string) error {
	current, _, err := s.Coordinator.Load(ctx, key)
	if err != nil {
		return err
	}
	details, _ := json.Marshal(map[string]string{"stage": "delivery", "code": code, "retry_guidance": "manual_delivery_retry"})
	return s.Coordinator.Commit(ctx, key, lease, state.Transition{RunID: runID, Sequence: current.LastSequence + 1, Phase: domain.Failed, At: s.Now().UTC(), Details: details})
}

// sleepContext waits between retryable attempts while honoring invocation cancellation.
func sleepContext(ctx context.Context, delay time.Duration) error {
	select {
	case <-time.After(delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
