// Package recovery repairs durable intents and expired worker ownership in bounded scans.
package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/delivery"
	"github.com/maverickuser/data-fetch-service/internal/domain"
	"github.com/maverickuser/data-fetch-service/internal/events"
	"github.com/maverickuser/data-fetch-service/internal/state"
)

const cursorKey = "coordination/reconciler.json"

// Repository is the paginated state boundary used by each reconciliation pass.
type Repository interface {
	Read(context.Context, string, time.Time) (state.Object, error)
	Scan(context.Context, string, string, int32) (state.Page, error)
	CompareAndSwap(context.Context, string, []byte, string) (string, error)
}

// Coordinator repairs reserved state and performs fenced pull retry transfers.
type Coordinator interface {
	Load(context.Context, string) (state.Coordination, string, error)
	Repair(context.Context, string) error
	ReservePullRetry(context.Context, string, string) (string, error)
	RequeueExpiredDispatch(context.Context, string) error
	ClaimExpiredDownloadsCompleted(context.Context, string, string, time.Time) (state.Lease, error)
	Commit(context.Context, string, state.Lease, state.Transition) error
	PublishPending(context.Context, string, state.Publisher) error
	ReadRequest(context.Context, string) (state.RequestIntent, error)
	RequestResolution(context.Context, string) (state.Resolution, error)
	Admit(context.Context, state.RequestIntent) (state.Resolution, error)
}

// DeliveryResumer resumes only expired delivery ownership using its existing attempt ledger.
type DeliveryResumer interface {
	Resume(context.Context, string, string, time.Time) error
}

// Cursor persists one bounded position across the coordination and request prefixes.
type Cursor struct {
	SchemaVersion int    `json:"schema_version"`
	Stage         string `json:"stage"`
	Token         string `json:"token"`
}

// Service scans a bounded page per invocation and converges recoverable work.
type Service struct {
	Repository  Repository
	Coordinator Coordinator
	Publisher   state.Publisher
	Delivery    DeliveryResumer
	Now         func() time.Time
	NewID       func() string
	PageLimit   int32
}

// Run scans one page, then conditionally saves the next cursor only after all work succeeds.
func (s *Service) Run(ctx context.Context) error {
	if s.Repository == nil || s.Coordinator == nil || s.Publisher == nil || s.Delivery == nil || s.Now == nil || s.NewID == nil {
		return fmt.Errorf("recovery dependencies missing")
	}
	limit := s.PageLimit
	if limit == 0 {
		limit = 100
	}
	if limit < 1 || limit > 1000 {
		return fmt.Errorf("invalid recovery page limit")
	}
	cursor, etag, err := s.loadCursor(ctx)
	if err != nil {
		return err
	}
	prefix := "coordination/"
	if cursor.Stage == "requests" {
		prefix = "requests/"
	}
	page, err := s.Repository.Scan(ctx, prefix, cursor.Token, limit)
	if err != nil {
		return err
	}
	for _, key := range page.Keys {
		if !strings.HasPrefix(key, prefix) {
			return state.ErrIntegrity
		}
		if cursor.Stage == "coordination" {
			if key == cursorKey {
				continue
			}
			if err := s.repairCoordination(ctx, key); err != nil {
				return err
			}
		} else if strings.HasSuffix(key, "/intent.json") {
			if err := s.repairRequest(ctx, key); err != nil {
				return err
			}
		}
	}
	next := Cursor{SchemaVersion: 1, Stage: cursor.Stage, Token: page.NextToken}
	if page.NextToken == "" {
		if cursor.Stage == "coordination" {
			next.Stage = "requests"
		} else {
			next.Stage = "coordination"
		}
	}
	data, err := json.Marshal(next)
	if err != nil {
		return err
	}
	_, err = s.Repository.CompareAndSwap(ctx, cursorKey, data, etag)
	return err
}

// loadCursor initializes a valid cursor under a dedicated coordination key.
func (s *Service) loadCursor(ctx context.Context) (Cursor, string, error) {
	object, err := s.Repository.Read(ctx, cursorKey, s.Now())
	if errors.Is(err, state.ErrNotFound) {
		initial := Cursor{SchemaVersion: 1, Stage: "coordination"}
		data, _ := json.Marshal(initial)
		etag, createErr := s.Repository.CompareAndSwap(ctx, cursorKey, data, "")
		if createErr != nil {
			return Cursor{}, "", createErr
		}
		return initial, etag, nil
	}
	if err != nil {
		return Cursor{}, "", err
	}
	var cursor Cursor
	if json.Unmarshal(object.Data, &cursor) != nil || cursor.SchemaVersion != 1 || cursor.Stage != "coordination" && cursor.Stage != "requests" {
		return Cursor{}, "", state.ErrIntegrity
	}
	return cursor, object.ETag, nil
}

// repairCoordination finishes intents before deciding whether a worker has expired.
func (s *Service) repairCoordination(ctx context.Context, key string) error {
	current, _, err := s.Coordinator.Load(ctx, key)
	if err != nil {
		return err
	}
	if current.Admission != nil || current.Pending != nil || current.Retry != nil {
		if err := s.Coordinator.Repair(ctx, key); err != nil {
			return err
		}
		current, _, err = s.Coordinator.Load(ctx, key)
		if err != nil {
			return err
		}
	}
	if current.Dispatch != nil && current.Dispatch.State == "pending" {
		if err := s.Coordinator.PublishPending(ctx, key, s.Publisher); err != nil {
			return err
		}
		current, _, err = s.Coordinator.Load(ctx, key)
		if err != nil {
			return err
		}
	}
	if current.ActiveRunID == "" || current.OwnerDeadline.IsZero() || s.Now().Before(current.OwnerDeadline) {
		return nil
	}
	switch current.Phase {
	case domain.Queued, domain.DeliveryPending:
		if err := s.Coordinator.RequeueExpiredDispatch(ctx, key); err != nil {
			return err
		}
		return s.Coordinator.PublishPending(ctx, key, s.Publisher)
	case domain.DownloadsCompleted:
		return s.resumeDownloaded(ctx, key, current)
	case domain.Pulling:
		_, err = s.Coordinator.ReservePullRetry(ctx, key, s.NewID())
		if err != nil {
			return err
		}
		return s.Coordinator.PublishPending(ctx, key, s.Publisher)
	case domain.Delivering:
		deadline, ok := ctx.Deadline()
		if !ok {
			return fmt.Errorf("reconciler deadline missing")
		}
		err := s.Delivery.Resume(ctx, current.ActiveRunID, s.NewID(), deadline)
		if errors.Is(err, delivery.ErrRetryNotReady) {
			return nil
		}
		return err
	default:
		return nil
	}
}

// resumeDownloaded restores the fingerprint decision from the complete manifest metadata.
func (s *Service) resumeDownloaded(ctx context.Context, key string, current state.Coordination) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		return fmt.Errorf("reconciler deadline missing")
	}
	lease, err := s.Coordinator.ClaimExpiredDownloadsCompleted(ctx, key, s.NewID(), deadline.Add(time.Minute))
	if err != nil {
		return err
	}
	object, err := s.Repository.Read(ctx, "runs/"+lease.RunID+"/snapshot.json", s.Now())
	if err != nil {
		return err
	}
	var snapshot events.Snapshot
	if json.Unmarshal(object.Data, &snapshot) != nil || snapshot.RunID != lease.RunID || snapshot.ExecutionKey == "" || key != "coordination/"+snapshot.ExecutionKey+".json" {
		return state.ErrIntegrity
	}
	historyKey := fmt.Sprintf("runs/%s/history/%020d.json", lease.RunID, current.LastSequence)
	history, err := s.Repository.Read(ctx, historyKey, s.Now())
	if err != nil {
		return err
	}
	var completed state.Transition
	if json.Unmarshal(history.Data, &completed) != nil || completed.RunID != lease.RunID || completed.Phase != domain.DownloadsCompleted {
		return state.ErrIntegrity
	}
	var details struct {
		Fingerprint string `json:"dataset_fingerprint"`
	}
	if json.Unmarshal(completed.Details, &details) != nil || details.Fingerprint == "" {
		return state.ErrIntegrity
	}
	phase := domain.DeliveryPending
	if !snapshot.Force && current.AcceptedBaseline != nil && current.AcceptedBaseline.Fingerprint == details.Fingerprint {
		phase = domain.SkippedUnchanged
	}
	if err := s.Coordinator.Commit(ctx, key, lease, state.Transition{RunID: lease.RunID, Sequence: current.LastSequence + 1, Phase: phase, At: s.Now().UTC(), Details: completed.Details}); err != nil {
		return err
	}
	if phase == domain.DeliveryPending {
		return s.Coordinator.PublishPending(ctx, key, s.Publisher)
	}
	return nil
}

// repairRequest resolves a stranded request intent without changing its original candidate.
func (s *Service) repairRequest(ctx context.Context, key string) error {
	requestKey := strings.TrimSuffix(strings.TrimPrefix(key, "requests/"), "/intent.json")
	if requestKey == "" || strings.Contains(requestKey, "/") {
		return state.ErrIntegrity
	}
	if _, err := s.Coordinator.RequestResolution(ctx, requestKey); err == nil {
		return nil
	} else if errors.Is(err, state.ErrExpired) {
		return nil
	} else if !errors.Is(err, state.ErrNotFound) {
		return err
	}
	intent, err := s.Coordinator.ReadRequest(ctx, requestKey)
	if errors.Is(err, state.ErrExpired) || errors.Is(err, state.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	_, err = s.Coordinator.Admit(ctx, intent)
	if errors.Is(err, state.ErrConflict) {
		return nil
	}
	return err
}
