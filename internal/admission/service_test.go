package admission

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/config"
	"github.com/maverickuser/data-fetch-service/internal/domain"
	"github.com/maverickuser/data-fetch-service/internal/events"
	"github.com/maverickuser/data-fetch-service/internal/state"
)

type repositoryFake struct {
	existing                                          state.RequestIntent
	readErr, admitErr, repairErr, publishErr, viewErr error
	attempts, conflicts                               int
	captured                                          state.RequestIntent
	phase                                             domain.State
	joined                                            bool
}

func (f *repositoryFake) ReadRequest(context.Context, string) (state.RequestIntent, error) {
	if f.readErr != nil {
		return state.RequestIntent{}, f.readErr
	}
	if f.existing.RequestKey == "" {
		return state.RequestIntent{}, state.ErrNotFound
	}
	return f.existing, nil
}
func (f *repositoryFake) Admit(_ context.Context, r state.RequestIntent) (state.Resolution, error) {
	f.attempts++
	f.captured = r
	if f.attempts <= f.conflicts {
		return state.Resolution{}, state.ErrConflict
	}
	return state.Resolution{RunID: r.CandidateRunID, ExecutionKey: r.ExecutionKey, Joined: f.joined}, f.admitErr
}
func (f *repositoryFake) Repair(context.Context, string) error { return f.repairErr }
func (f *repositoryFake) PublishPending(context.Context, string, state.Publisher) error {
	return f.publishErr
}
func (f *repositoryFake) ReadRun(_ context.Context, id string) (state.RunView, error) {
	return state.RunView{RunID: id, Phase: f.phase}, f.viewErr
}

func serviceFixture(t *testing.T) (*Service, *repositoryFake) {
	t.Helper()
	raw, err := os.ReadFile("../../config/events.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(raw)
	if err != nil {
		t.Fatal(err)
	}
	repo := &repositoryFake{phase: domain.Queued}
	now := time.Date(2026, 9, 21, 14, 30, 0, 0, time.UTC)
	return &Service{Config: cfg, Coordinator: repo, Now: func() time.Time { return now }, NewID: func() string { return "run" }}, repo
}

func TestManualReplayPinsTimeConfigAndForce(t *testing.T) {
	s, repo := serviceFixture(t)
	ctx := context.Background()
	repo.conflicts = 2
	result, err := s.Manual(ctx, "daily-bhavcopy", "key", map[string]any{"exchangeName": "BSE"}, true)
	if err != nil || result.Reused || result.RunID != "run" || repo.attempts != 3 {
		t.Fatal(result, err, repo.attempts)
	}
	var snapshot events.Snapshot
	if err := json.Unmarshal(repo.captured.Snapshot, &snapshot); err != nil {
		t.Fatal(err)
	}
	if !snapshot.Force || snapshot.Inputs["run_date"] != "2026-09-21" {
		t.Fatal(snapshot)
	}
	originalHash := repo.captured.PayloadHash
	repo.existing = repo.captured
	repo.phase = domain.Completed
	s.Now = func() time.Time { return time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC) }
	s.Config.Events = nil
	result, err = s.Manual(ctx, "daily-bhavcopy", "key", map[string]any{"exchangeName": "BSE"}, true)
	if err != nil || !result.CompletedReplay || !result.Reused || repo.captured.PayloadHash != originalHash {
		t.Fatal(result, err)
	}
	if _, err := s.Manual(ctx, "daily-bhavcopy", "key", map[string]any{"exchangeName": "BSE"}, false); err != nil {
		t.Fatal(err)
	}
	if repo.captured.PayloadHash == originalHash {
		t.Fatal("force change not part of identity")
	}
}

func TestAdmissionErrorsAndCancellation(t *testing.T) {
	failure := errors.New("unavailable")
	for _, field := range []string{"read", "admit", "repair", "publish", "view", "conflicts", "bad config", "bad ID", "corrupt snapshot"} {
		t.Run(field, func(t *testing.T) {
			s, r := serviceFixture(t)
			switch field {
			case "read":
				r.readErr = failure
			case "admit":
				r.admitErr = failure
			case "repair":
				r.repairErr = failure
			case "publish":
				r.publishErr = failure
			case "view":
				r.viewErr = failure
			case "conflicts":
				r.conflicts = 20
			case "bad config":
				s.Config.Events = nil
			case "bad ID":
				s.NewID = func() string { return "../bad" }
			case "corrupt snapshot":
				r.existing = state.RequestIntent{RequestKey: "key", Snapshot: json.RawMessage(`{`)}
			}
			if _, err := s.Manual(context.Background(), "daily-bhavcopy", "key", map[string]any{"exchangeName": "BSE"}, false); err == nil {
				t.Fatal("error swallowed")
			}
			if field == "conflicts" && r.attempts != 8 {
				t.Fatal("retry budget", r.attempts)
			}
		})
	}
	s, _ := serviceFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Manual(ctx, "daily-bhavcopy", "key", map[string]any{"exchangeName": "BSE"}, false); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.External(context.Background(), events.Normalized{Force: true}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := s.External(context.Background(), events.Normalized{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func TestExternalAndUnkeyedManualAdmission(t *testing.T) {
	s, r := serviceFixture(t)
	r.joined = true
	event := events.Normalized{EventID: "event", Source: "urn:test", EventType: "nsdl-bond-data", OccurredAt: s.Now(), Inputs: map[string]any{"isin_code": "INE121A07QY9"}}
	result, err := s.External(context.Background(), event)
	if err != nil || !result.Reused {
		t.Fatal(result, err)
	}
	if _, err := s.Manual(context.Background(), "daily-bhavcopy", "", map[string]any{"exchangeName": "BSE"}, false); err != nil {
		t.Fatal(err)
	}
}
