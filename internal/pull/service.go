package pull

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/acquisition"
	"github.com/maverickuser/data-fetch-service/internal/config"
	"github.com/maverickuser/data-fetch-service/internal/domain"
	"github.com/maverickuser/data-fetch-service/internal/events"
	"github.com/maverickuser/data-fetch-service/internal/state"
)

// Repository reads admitted snapshots and creates immutable per-job results.
type Repository interface {
	Read(context.Context, string, time.Time) (state.Object, error)
	Create(context.Context, string, []byte) error
}

// Coordinator is the fenced run boundary used by one Pull invocation.
type Coordinator interface {
	ClaimDispatched(context.Context, string, string, string, string, time.Time) (state.Lease, error)
	Commit(context.Context, string, state.Lease, state.Transition) error
	Load(context.Context, string) (state.Coordination, string, error)
	PublishPending(context.Context, string, state.Publisher) error
}

// Fetcher acquires one admitted source job and returns only completed artifacts.
type Fetcher interface {
	Fetch(context.Context, string, config.ResolvedJob) (acquisition.Result, error)
}

// Service owns one complete-event group while the coordinator fences its state changes.
type Service struct {
	Repository         Repository
	Coordinator        Coordinator
	Fetcher            Fetcher
	FetcherForSnapshot func(config.Defaults) Fetcher
	ManifestStorage    acquisition.Storage
	Publisher          state.Publisher
	ArtifactBucket     string
	Now                func() time.Time
}

var ErrOutcomePersistence = errors.New("pull result persistence failed")
var ErrStalePullMessage = errors.New("pull message already owned or completed")

// Run claims a queued pull, stores every job outcome, and publishes only a full dataset.
// The deadline is the hard Lambda deadline; 60 seconds are reserved for state cleanup.
func (s *Service) Run(ctx context.Context, runID, token string, deadline time.Time) error {
	if s.Repository == nil || s.Coordinator == nil || s.Fetcher == nil && s.FetcherForSnapshot == nil || s.ManifestStorage == nil || s.Publisher == nil || s.ArtifactBucket == "" || !validRunID(runID) || token == "" || deadline.IsZero() {
		return fmt.Errorf("invalid pull dependencies or invocation")
	}
	now := s.Now
	if now == nil {
		now = time.Now
	}
	budgetDeadline := deadline.Add(-60 * time.Second)
	if !budgetDeadline.After(now()) {
		return fmt.Errorf("insufficient pull execution budget")
	}
	object, err := s.Repository.Read(ctx, "runs/"+runID+"/snapshot.json", now())
	if err != nil {
		return err
	}
	var snapshot events.Snapshot
	if err := json.Unmarshal(object.Data, &snapshot); err != nil {
		return err
	}
	if snapshot.SchemaVersion != 1 || snapshot.RunID != runID || snapshot.ExecutionKey == "" || len(snapshot.Jobs) == 0 {
		return state.ErrIntegrity
	}
	if err := snapshot.Config.Validate(); err != nil {
		return err
	}
	key := "coordination/" + snapshot.ExecutionKey + ".json"
	lease, err := s.Coordinator.ClaimDispatched(ctx, key, runID, "pull", token, deadline.Add(time.Minute))
	if err != nil {
		if errors.Is(err, state.ErrConflict) {
			current, _, readErr := s.Coordinator.Load(ctx, key)
			if readErr != nil {
				return errors.Join(err, readErr)
			}
			if current.ActiveRunID != runID || current.Phase != domain.Queued || current.OwnerToken != "" && current.OwnerToken != token && now().Before(current.OwnerDeadline) {
				return fmt.Errorf("%w: %w", ErrStalePullMessage, err)
			}
		}
		return err
	}
	current, _, err := s.Coordinator.Load(ctx, key)
	if err != nil {
		return err
	}
	if err := s.Coordinator.Commit(ctx, key, lease, state.Transition{RunID: runID, Sequence: current.LastSequence + 1, Phase: domain.Pulling, At: now().UTC()}); err != nil {
		return err
	}
	groupCtx, cancel := context.WithDeadline(ctx, budgetDeadline)
	defer cancel()
	results, groupErr := s.group(groupCtx, ctx, snapshot)
	if groupErr != nil {
		if ctx.Err() != nil || groupCtx.Err() != nil || errors.Is(groupErr, ErrOutcomePersistence) {
			return errors.Join(groupErr, groupCtx.Err())
		}
		code := "PULL_GROUP_FAILED"
		var sourceFailure *acquisition.Failure
		if errors.As(groupErr, &sourceFailure) {
			code = sourceFailure.Code
		}
		details := failureDetails("pull", code, "source acquisition failed; inspect job results")
		if err := s.commitPhase(ctx, key, lease, domain.Failed, now(), details); err != nil {
			return errors.Join(groupErr, err)
		}
		return nil
	}
	manifest, err := BuildManifest(snapshot, results, s.ArtifactBucket)
	if err != nil {
		details := failureDetails("manifest", "MANIFEST_INVALID", "complete dataset manifest validation failed")
		if commitErr := s.commitPhase(ctx, key, lease, domain.Failed, now(), details); commitErr != nil {
			return errors.Join(err, commitErr)
		}
		return nil
	}
	// Recheck ownership after the potentially long source group, before manifest publication.
	owned, err := s.owned(ctx, key, lease, domain.Pulling, now())
	if err != nil {
		return err
	}
	encoded, err := MarshalManifest(manifest)
	if err != nil {
		return err
	}
	manifestKey := "runs/" + runID + "/manifest.json"
	artifact, err := s.ManifestStorage.Upload(ctx, manifestKey, bytes.NewReader(encoded))
	if err != nil {
		return err
	}
	manifestHash := sha256.Sum256(encoded)
	if artifact.Key != manifestKey || artifact.Bytes != int64(len(encoded)) || artifact.SHA256 != hex.EncodeToString(manifestHash[:]) {
		return state.ErrIntegrity
	}
	if _, err := s.owned(ctx, key, lease, domain.Pulling, now()); err != nil {
		return err
	}
	details, err := json.Marshal(struct {
		Bucket      string `json:"bucket"`
		Key         string `json:"manifest_key"`
		Fingerprint string `json:"dataset_fingerprint"`
	}{s.ArtifactBucket, manifestKey, manifest.Data.DatasetFingerprint})
	if err != nil {
		return err
	}
	if err := s.Coordinator.Commit(ctx, key, lease, state.Transition{RunID: runID, Sequence: owned.LastSequence + 1, Phase: domain.DownloadsCompleted, At: now().UTC(), Details: details}); err != nil {
		return err
	}
	phase := domain.DeliveryPending
	if !snapshot.Force && owned.AcceptedBaseline != nil && owned.AcceptedBaseline.Fingerprint == manifest.Data.DatasetFingerprint {
		phase = domain.SkippedUnchanged
	}
	if err := s.commitPhase(ctx, key, lease, phase, now(), details); err != nil {
		return err
	}
	if phase == domain.DeliveryPending {
		return s.Coordinator.PublishPending(ctx, key, s.Publisher)
	}
	return nil
}

// failureDetails gives operators a bounded, stable reason and manual retry guidance.
func failureDetails(stage, code, message string) json.RawMessage {
	data, _ := json.Marshal(struct {
		Stage         string `json:"stage"`
		Code          string `json:"code"`
		Message       string `json:"message"`
		RetryGuidance string `json:"retry_guidance"`
	}{stage, code, message, "manual_rerun"})
	return data
}

// owned verifies the current generation, token, deadline, and phase before side effects.
func (s *Service) owned(ctx context.Context, key string, lease state.Lease, phase domain.State, now time.Time) (state.Coordination, error) {
	current, _, err := s.Coordinator.Load(ctx, key)
	if err != nil {
		return current, err
	}
	if current.ActiveRunID != lease.RunID || current.Generation != lease.Generation || current.OwnerToken != lease.Token || !now.Before(current.OwnerDeadline) || current.Phase != phase || current.Pending != nil {
		return current, state.ErrConflict
	}
	return current, nil
}

// commitPhase advances one fenced run sequence after rereading durable authority.
func (s *Service) commitPhase(ctx context.Context, key string, lease state.Lease, phase domain.State, at time.Time, details json.RawMessage) error {
	current, err := s.owned(ctx, key, lease, domain.Pulling, at)
	if phase == domain.DeliveryPending || phase == domain.SkippedUnchanged {
		current, err = s.owned(ctx, key, lease, domain.DownloadsCompleted, at)
	}
	if err != nil {
		return err
	}
	return s.Coordinator.Commit(ctx, key, lease, state.Transition{RunID: lease.RunID, Sequence: current.LastSequence + 1, Phase: phase, At: at.UTC(), Details: details})
}

// JobOutcome is immutable evidence for a completed, failed, canceled, or budget-interrupted group job.
type JobOutcome struct {
	JobID     string              `json:"job_id"`
	Status    string              `json:"status"`
	Result    *acquisition.Result `json:"result,omitempty"`
	ErrorCode string              `json:"error_code,omitempty"`
}

type jobReturn struct {
	job    config.ResolvedJob
	result acquisition.Result
	err    error
}

// group runs at most the admitted concurrency and waits for owned workers to stop.
func (s *Service) group(groupCtx, recordCtx context.Context, snapshot events.Snapshot) ([]acquisition.Result, error) {
	ctx, cancel := context.WithCancel(groupCtx)
	defer cancel()
	parallel := snapshot.Config.Defaults.PullConcurrency
	fetcher := s.Fetcher
	if s.FetcherForSnapshot != nil {
		fetcher = s.FetcherForSnapshot(snapshot.Config.Defaults)
	}
	if fetcher == nil {
		return nil, fmt.Errorf("snapshot fetcher missing")
	}
	if parallel > len(snapshot.Jobs) {
		parallel = len(snapshot.Jobs)
	}
	if parallel < 1 {
		return nil, fmt.Errorf("invalid pull concurrency")
	}
	jobs := make(chan config.ResolvedJob)
	out := make(chan jobReturn, len(snapshot.Jobs))
	var workers sync.WaitGroup
	for range parallel {
		workers.Go(func() {
			for job := range jobs {
				result, err := fetcher.Fetch(ctx, snapshot.RunID, job)
				out <- jobReturn{job: job, result: result, err: err}
				if err != nil {
					cancel()
				}
			}
		})
	}
	go func() {
		defer close(jobs)
		for _, job := range snapshot.Jobs {
			select {
			case <-ctx.Done():
				return
			case jobs <- job:
			}
		}
	}()
	workers.Wait()
	close(out)
	returns := make(map[string]jobReturn, len(snapshot.Jobs))
	for item := range out {
		returns[item.job.ID] = item
	}
	results := make([]acquisition.Result, 0, len(snapshot.Jobs))
	var groupErr error
	for _, job := range snapshot.Jobs {
		item, seen := returns[job.ID]
		outcome := JobOutcome{JobID: job.ID, Status: "canceled", ErrorCode: "GROUP_CANCELED"}
		if seen && item.err == nil {
			outcome.Status = "completed"
			outcome.ErrorCode = ""
			outcome.Result = &item.result
			results = append(results, item.result)
		}
		if seen && item.err != nil && errors.Is(item.err, context.Canceled) {
			outcome.Status = "canceled"
		}
		if seen && item.err != nil && errors.Is(groupCtx.Err(), context.DeadlineExceeded) && errors.Is(item.err, context.DeadlineExceeded) {
			outcome.Status = "interrupted"
			outcome.ErrorCode = "GROUP_BUDGET_EXCEEDED"
			groupErr = errors.Join(groupErr, item.err)
		} else if seen && item.err != nil && !errors.Is(item.err, context.Canceled) {
			outcome.Status = "failed"
			outcome.ErrorCode = "ACQUISITION_FAILED"
			var failure *acquisition.Failure
			if errors.As(item.err, &failure) {
				outcome.ErrorCode = failure.Code
			}
			groupErr = errors.Join(groupErr, item.err)
		}
		if !seen && groupErr == nil {
			groupErr = ctx.Err()
		}
		data, err := json.Marshal(outcome)
		if err != nil {
			return nil, err
		}
		if err := s.Repository.Create(recordCtx, fmt.Sprintf("runs/%s/pulls/%s/result.json", snapshot.RunID, job.ID), data); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrOutcomePersistence, err)
		}
	}
	if groupErr != nil {
		return nil, groupErr
	}
	if len(results) != len(snapshot.Jobs) {
		return nil, fmt.Errorf("incomplete pull group")
	}
	return results, nil
}

// validRunID protects artifact and state paths derived from an SQS run reference.
func validRunID(runID string) bool {
	return runID != "" && runID != "." && runID != ".." && !strings.ContainsAny(runID, "/\\:\r\n\x00")
}
