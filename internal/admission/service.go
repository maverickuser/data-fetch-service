// Package admission shares request validation and durable admission across transports.
package admission

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/acquisition"
	"github.com/maverickuser/data-fetch-service/internal/config"
	"github.com/maverickuser/data-fetch-service/internal/domain"
	"github.com/maverickuser/data-fetch-service/internal/events"
	"github.com/maverickuser/data-fetch-service/internal/pull"
	"github.com/maverickuser/data-fetch-service/internal/state"
)

var ErrInvalid = errors.New("invalid admission request")

// Result tells adapters whether an existing request or active run was reused.
type Result struct {
	RunID           string       `json:"run_id"`
	Status          domain.State `json:"status"`
	Reused          bool         `json:"reused"`
	StatusURL       string       `json:"status_url"`
	CompletedReplay bool         `json:"-"`
}

// Service binds validated configuration and durable coordination to clocks and IDs.
type Service struct {
	Config      config.Config
	Coordinator Repository
	Publisher   state.Publisher
	Now         func() time.Time
	NewID       func() string
	Artifacts   interface {
		Read(context.Context, string) ([]byte, error)
		Stat(context.Context, string, int64) error
	}
	ManifestStorage acquisition.Storage
	ArtifactBucket  string
	Records         interface {
		Read(context.Context, string, time.Time) (state.Object, error)
	}
}

// Repository is the durable admission boundary; conflict retries reload its authority.
type Repository interface {
	ReadRequest(context.Context, string) (state.RequestIntent, error)
	Admit(context.Context, state.RequestIntent) (state.Resolution, error)
	Repair(context.Context, string) error
	PublishPending(context.Context, string, state.Publisher) error
	ReadRun(context.Context, string) (state.RunView, error)
	History(context.Context, string, string, int32) (state.HistoryPage, error)
	Load(context.Context, string) (state.Coordination, string, error)
}

// DeliveryRetry admits a linked delivery-only run after validating its retained source files.
func (s *Service) DeliveryRetry(ctx context.Context, parentRunID, key string, useCurrent bool) (Result, error) {
	if s.Coordinator == nil || s.Artifacts == nil || s.ManifestStorage == nil || s.Records == nil || s.ArtifactBucket == "" || s.Now == nil || s.NewID == nil {
		return Result{}, ErrInvalid
	}
	if key == "" {
		key = s.NewID()
	}
	requestKey, err := domain.RequestKey("urn:bond-platform:delivery-retry:"+parentRunID, key)
	if err != nil {
		return Result{}, ErrInvalid
	}
	// Replays use the original immutable candidate, even after configuration or files change.
	existing, err := s.Coordinator.ReadRequest(ctx, requestKey)
	if err == nil {
		var pinned events.Snapshot
		if json.Unmarshal(existing.Snapshot, &pinned) != nil || pinned.DeliveryRetry == nil || pinned.DeliveryRetry.SourceRunID != parentRunID || pinned.DeliveryRetry.UseCurrentProcessor != useCurrent {
			return Result{}, state.ErrIntegrity
		}
		return s.admitRecovery(ctx, existing, pinned.RunID, true)
	}
	if !errors.Is(err, state.ErrNotFound) {
		return Result{}, err
	}
	view, err := s.Coordinator.ReadRun(ctx, parentRunID)
	if err != nil {
		return Result{}, err
	}
	if view.Phase != domain.Failed || view.LatestRunID != "" && !view.LatestPhase.Terminal() {
		return Result{}, state.ErrConflict
	}
	history, err := s.Coordinator.History(ctx, parentRunID, "", 100)
	if err != nil {
		return Result{}, err
	}
	if history.NextToken != "" || len(history.Transitions) == 0 {
		return Result{}, state.ErrIntegrity
	}
	last := history.Transitions[len(history.Transitions)-1]
	var failure struct {
		Stage string `json:"stage"`
	}
	if last.Phase != domain.Failed || json.Unmarshal(last.Details, &failure) != nil || failure.Stage != "delivery" {
		return Result{}, state.ErrConflict
	}
	var parent events.Snapshot
	if json.Unmarshal(view.Snapshot, &parent) != nil || parent.SchemaVersion != 1 || parent.RunID != parentRunID {
		return Result{}, state.ErrIntegrity
	}
	coord, _, err := s.Coordinator.Load(ctx, "coordination/"+parent.ExecutionKey+".json")
	if err != nil {
		return Result{}, err
	}
	if coord.ActiveRunID != "" {
		return Result{}, state.ErrConflict
	}
	baselineID := ""
	if coord.AcceptedBaseline != nil {
		baselineID = coord.AcceptedBaseline.RunID
	}
	rawManifest, err := s.Artifacts.Read(ctx, "runs/"+parentRunID+"/manifest.json")
	if err != nil {
		return Result{}, err
	}
	var original pull.Manifest
	if json.Unmarshal(rawManifest, &original) != nil {
		return Result{}, state.ErrIntegrity
	}
	if coord.AcceptedBaseline != nil && coord.AcceptedBaseline.Fingerprint != original.Data.DatasetFingerprint {
		accepted, err := s.Records.Read(ctx, coord.AcceptedBaseline.AcceptanceKey, s.Now())
		if err != nil {
			if errors.Is(err, state.ErrNotFound) || errors.Is(err, state.ErrExpired) {
				return Result{}, state.ErrIntegrity
			}
			return Result{}, err
		}
		var evidence state.Acceptance
		if json.Unmarshal(accepted.Data, &evidence) != nil || evidence.RunID != baselineID || evidence.Fingerprint != coord.AcceptedBaseline.Fingerprint {
			return Result{}, state.ErrIntegrity
		}
		if evidence.AcceptedAt.After(view.CreatedAt) {
			return Result{}, state.ErrConflict
		}
	}
	if err := pull.VerifyRetainedFiles(ctx, original, s.ArtifactBucket, s.Artifacts); err != nil {
		return Result{}, err
	}
	var current *config.Config
	if useCurrent {
		current = &s.Config
	}
	candidateID := s.NewID()
	raw, err := events.CloneForDeliveryRetry(parent, candidateID, key, current, baselineID)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	var child events.Snapshot
	if json.Unmarshal(raw, &child) != nil {
		return Result{}, state.ErrIntegrity
	}
	manifest, err := pull.ReuseManifest(parent, child, original, s.ArtifactBucket)
	if err != nil {
		return Result{}, state.ErrIntegrity
	}
	encoded, err := pull.MarshalManifest(manifest)
	if err != nil {
		return Result{}, err
	}
	artifact, err := s.ManifestStorage.Upload(ctx, "runs/"+candidateID+"/manifest.json", bytes.NewReader(encoded))
	if err != nil {
		return Result{}, err
	}
	hash := sha256.Sum256(encoded)
	if artifact.Key != "runs/"+candidateID+"/manifest.json" || artifact.Bytes != int64(len(encoded)) || artifact.SHA256 != hex.EncodeToString(hash[:]) {
		return Result{}, state.ErrIntegrity
	}
	request, err := state.NewRequest(raw, s.Now())
	if err != nil {
		return Result{}, err
	}
	return s.admitRecovery(ctx, request, candidateID, false)
}

// admitRecovery publishes a reserved manual recovery request using the normal durable dispatch.
func (s *Service) admitRecovery(ctx context.Context, request state.RequestIntent, candidateID string, replay bool) (Result, error) {
	var receipt state.Resolution
	var err error
	for attempt := 0; attempt < 8; attempt++ {
		if err = ctx.Err(); err != nil {
			return Result{}, err
		}
		receipt, err = s.Coordinator.Admit(ctx, request)
		if !errors.Is(err, state.ErrConflict) {
			break
		}
	}
	if err != nil {
		return Result{}, err
	}
	if receipt.Joined {
		return Result{}, state.ErrConflict
	}
	coordKey := "coordination/" + receipt.ExecutionKey + ".json"
	if err = s.Coordinator.Repair(ctx, coordKey); err != nil {
		return Result{}, err
	}
	if err = s.Coordinator.PublishPending(ctx, coordKey, s.Publisher); err != nil {
		return Result{}, err
	}
	created, err := s.Coordinator.ReadRun(ctx, receipt.RunID)
	if err != nil {
		return Result{}, err
	}
	reused := replay || receipt.RunID != candidateID
	return Result{RunID: receipt.RunID, Status: created.Phase, Reused: reused, StatusURL: "/v1/runs/" + receipt.RunID, CompletedReplay: reused && created.Phase.Terminal()}, nil
}

// Manual pins the first logical time/configuration across idempotent request retries.
func (s *Service) Manual(ctx context.Context, eventType, key string, inputs map[string]any, force bool) (Result, error) {
	if key == "" {
		key = s.NewID()
	}
	event := events.Normalized{EventID: key, Source: "urn:bond-platform:manual", Type: events.PullRequested, OccurredAt: s.Now().UTC(), EventType: eventType, Inputs: inputs, Force: force}
	return s.admit(ctx, event, true)
}

// FullRerun admits a linked run from the original resolved snapshot without re-resolving its source date or jobs.
func (s *Service) FullRerun(ctx context.Context, parentRunID, key string, force *bool) (Result, error) {
	if s.Coordinator == nil || s.Now == nil || s.NewID == nil {
		return Result{}, ErrInvalid
	}
	view, err := s.Coordinator.ReadRun(ctx, parentRunID)
	if err != nil {
		return Result{}, err
	}
	if !view.Phase.Terminal() || view.LatestRunID != "" && !view.LatestPhase.Terminal() {
		return Result{}, state.ErrConflict
	}
	var parent events.Snapshot
	if json.Unmarshal(view.Snapshot, &parent) != nil || parent.SchemaVersion != 1 || parent.RunID != parentRunID {
		return Result{}, state.ErrIntegrity
	}
	if key == "" {
		key = s.NewID()
	}
	choice := parent.Force
	if force != nil {
		choice = *force
	}
	candidateID := s.NewID()
	raw, err := events.CloneForFullRerun(parent, candidateID, key, choice)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	request, err := state.NewRequest(raw, s.Now())
	if err != nil {
		return Result{}, err
	}
	var receipt state.Resolution
	for attempt := 0; attempt < 8; attempt++ {
		if err = ctx.Err(); err != nil {
			return Result{}, err
		}
		receipt, err = s.Coordinator.Admit(ctx, request)
		if !errors.Is(err, state.ErrConflict) {
			break
		}
	}
	if err != nil {
		return Result{}, err
	}
	if receipt.Joined {
		return Result{}, state.ErrConflict
	}
	coordKey := "coordination/" + receipt.ExecutionKey + ".json"
	if err = s.Coordinator.Repair(ctx, coordKey); err != nil {
		return Result{}, err
	}
	if err = s.Coordinator.PublishPending(ctx, coordKey, s.Publisher); err != nil {
		return Result{}, err
	}
	created, err := s.Coordinator.ReadRun(ctx, receipt.RunID)
	if err != nil {
		return Result{}, err
	}
	reused := receipt.RunID != candidateID
	return Result{RunID: receipt.RunID, Status: created.Phase, Reused: reused, StatusURL: "/v1/runs/" + receipt.RunID, CompletedReplay: reused && created.Phase.Terminal()}, nil
}

// External admits a normalized producer event; external transports cannot request force.
func (s *Service) External(ctx context.Context, event events.Normalized) (Result, error) {
	if event.Force {
		return Result{}, ErrInvalid
	}
	return s.admit(ctx, event, false)
}

// admit resolves against pinned configuration on replays and retries only fresh CAS decisions.
func (s *Service) admit(ctx context.Context, event events.Normalized, manual bool) (Result, error) {
	return s.admitPinned(ctx, event, manual, true)
}

// admitPinned rebinds a racing first request once to the winning durable configuration.
func (s *Service) admitPinned(ctx context.Context, event events.Normalized, manual, repin bool) (Result, error) {
	requestKey, err := domain.RequestKey(event.Source, event.EventID)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	cfg := s.Config
	created := s.Now().UTC()
	runID := s.NewID()
	existing, err := s.Coordinator.ReadRequest(ctx, requestKey)
	if err == nil {
		var pinned events.Snapshot
		if err := json.Unmarshal(existing.Snapshot, &pinned); err != nil {
			return Result{}, err
		}
		cfg = pinned.Config
		created = existing.CreatedAt
		runID = existing.CandidateRunID
		if manual {
			event.OccurredAt = pinned.Event.OccurredAt
		}
	} else if !errors.Is(err, state.ErrNotFound) {
		return Result{}, err
	}
	snapshot, err := events.BuildSnapshot(cfg, event, runID)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	request, err := state.NewRequest(snapshot, created)
	if err != nil {
		return Result{}, err
	}
	var receipt state.Resolution
	for attempt := 0; attempt < 8; attempt++ {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		receipt, err = s.Coordinator.Admit(ctx, request)
		if !errors.Is(err, state.ErrConflict) {
			break
		}
	}
	if err != nil {
		if repin && existing.RequestKey == "" && errors.Is(err, state.ErrIntegrity) {
			return s.admitPinned(ctx, event, manual, false)
		}
		return Result{}, err
	}
	coordKey := "coordination/" + receipt.ExecutionKey + ".json"
	// A resolution can survive a crash before its admission lock is finalized.
	if err := s.Coordinator.Repair(ctx, coordKey); err != nil {
		return Result{}, err
	}
	if err := s.Coordinator.PublishPending(ctx, coordKey, s.Publisher); err != nil {
		return Result{}, err
	}
	view, err := s.Coordinator.ReadRun(ctx, receipt.RunID)
	if err != nil {
		return Result{}, err
	}
	reused := receipt.Joined || existing.RequestKey != ""
	return Result{RunID: receipt.RunID, Status: view.Phase, Reused: reused, StatusURL: "/v1/runs/" + receipt.RunID, CompletedReplay: reused && view.Phase.Terminal()}, nil
}
