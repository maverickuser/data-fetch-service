// Package admission shares request validation and durable admission across transports.
package admission

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/config"
	"github.com/maverickuser/data-fetch-service/internal/domain"
	"github.com/maverickuser/data-fetch-service/internal/events"
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
}

// Repository is the durable admission boundary; conflict retries reload its authority.
type Repository interface {
	ReadRequest(context.Context, string) (state.RequestIntent, error)
	Admit(context.Context, state.RequestIntent) (state.Resolution, error)
	Repair(context.Context, string) error
	PublishPending(context.Context, string, state.Publisher) error
	ReadRun(context.Context, string) (state.RunView, error)
}

// Manual pins the first logical time/configuration across idempotent request retries.
func (s *Service) Manual(ctx context.Context, eventType, key string, inputs map[string]any, force bool) (Result, error) {
	if key == "" {
		key = s.NewID()
	}
	event := events.Normalized{EventID: key, Source: "urn:bond-platform:manual", Type: events.PullRequested, OccurredAt: s.Now().UTC(), EventType: eventType, Inputs: inputs, Force: force}
	return s.admit(ctx, event, true)
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
