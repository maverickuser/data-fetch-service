package admission

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/acquisition"
	"github.com/maverickuser/data-fetch-service/internal/config"
	"github.com/maverickuser/data-fetch-service/internal/domain"
	"github.com/maverickuser/data-fetch-service/internal/events"
	"github.com/maverickuser/data-fetch-service/internal/state"
)

type retryArtifactFake struct{}

func (retryArtifactFake) Read(context.Context, string) ([]byte, error) { return nil, state.ErrExpired }
func (retryArtifactFake) Stat(context.Context, string, int64) error    { return state.ErrExpired }
func (retryArtifactFake) Upload(context.Context, string, io.Reader) (acquisition.Artifact, error) {
	return acquisition.Artifact{}, state.ErrExpired
}

type retryRecordFake struct{}

func (retryRecordFake) Read(context.Context, string, time.Time) (state.Object, error) {
	return state.Object{}, state.ErrNotFound
}

func TestDeliveryRetryReplaysPinnedIntentAndPropagatesFailures(t *testing.T) {
	for _, scenario := range []string{"replay", "changed-option", "admit", "conflicts", "joined", "repair", "publish", "child-read", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			s, repo := serviceFixture(t)
			parentRaw, err := events.BuildSnapshot(s.Config, events.Normalized{EventID: "original", Source: "urn:manual", OccurredAt: s.Now(), EventType: "daily-bhavcopy", Inputs: map[string]any{"exchangeName": "BSE"}}, "parent")
			if err != nil {
				t.Fatal(err)
			}
			var parent events.Snapshot
			if err := json.Unmarshal(parentRaw, &parent); err != nil {
				t.Fatal(err)
			}
			childRaw, err := events.CloneForDeliveryRetry(parent, "child", "retry-key", nil, "")
			if err != nil {
				t.Fatal(err)
			}
			repo.existing, err = state.NewRequest(childRaw, s.Now())
			if err != nil {
				t.Fatal(err)
			}
			s.Artifacts = retryArtifactFake{}
			s.ManifestStorage = retryArtifactFake{}
			s.Records = retryRecordFake{}
			s.ArtifactBucket = "artifacts"
			ctx := context.Background()
			current := false
			switch scenario {
			case "changed-option":
				current = true
			case "admit":
				repo.admitErr = errors.New("state unavailable")
			case "conflicts":
				repo.conflicts = 20
			case "joined":
				repo.joined = true
			case "repair":
				repo.repairErr = errors.New("repair unavailable")
			case "publish":
				repo.publishErr = errors.New("SQS unavailable")
			case "child-read":
				repo.childViewErr = errors.New("history unavailable")
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			result, err := s.DeliveryRetry(ctx, "parent", "retry-key", current)
			if scenario == "replay" {
				if err != nil || !result.Reused || result.RunID != "child" {
					t.Fatal(result, err)
				}
			} else if err == nil {
				t.Fatal("expected failure")
			}
		})
	}
}

type repositoryFake struct {
	existing                                          state.RequestIntent
	readErr, admitErr, repairErr, publishErr, viewErr error
	attempts, conflicts                               int
	captured                                          state.RequestIntent
	phase                                             domain.State
	joined                                            bool
	parentView                                        state.RunView
	childViewErr                                      error
	historyPage                                       state.HistoryPage
	historyErr                                        error
	coord                                             state.Coordination
	loadErr                                           error
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
	if f.parentView.RunID == id {
		return f.parentView, f.viewErr
	}
	if f.childViewErr != nil {
		return state.RunView{}, f.childViewErr
	}
	return state.RunView{RunID: id, Phase: f.phase}, f.viewErr
}
func (f *repositoryFake) History(context.Context, string, string, int32) (state.HistoryPage, error) {
	return f.historyPage, f.historyErr
}
func (f *repositoryFake) Load(context.Context, string) (state.Coordination, string, error) {
	return f.coord, "etag", f.loadErr
}

func TestDeliveryRetryRejectsInvalidParentAndUnavailableState(t *testing.T) {
	for _, scenario := range []string{"parent-unavailable", "nonterminal", "history-unavailable", "empty-history", "wrong-stage", "corrupt-snapshot", "coordination-unavailable", "active-run"} {
		t.Run(scenario, func(t *testing.T) {
			s, repo := serviceFixture(t)
			parentRaw, err := events.BuildSnapshot(s.Config, events.Normalized{EventID: "original", Source: "urn:manual", OccurredAt: s.Now(), EventType: "daily-bhavcopy", Inputs: map[string]any{"exchangeName": "BSE"}}, "parent")
			if err != nil {
				t.Fatal(err)
			}
			repo.parentView = state.RunView{RunID: "parent", Phase: domain.Failed, LatestRunID: "parent", LatestPhase: domain.Failed, Snapshot: parentRaw}
			repo.historyPage = state.HistoryPage{Transitions: []state.Transition{{Phase: domain.Failed, Details: json.RawMessage(`{"stage":"delivery"}`)}}}
			s.Artifacts = retryArtifactFake{}
			s.ManifestStorage = retryArtifactFake{}
			s.Records = retryRecordFake{}
			s.ArtifactBucket = "artifacts"
			switch scenario {
			case "parent-unavailable":
				repo.viewErr = errors.New("S3 unavailable")
			case "nonterminal":
				repo.parentView.Phase = domain.Pulling
			case "history-unavailable":
				repo.historyErr = errors.New("history unavailable")
			case "empty-history":
				repo.historyPage.Transitions = nil
			case "wrong-stage":
				repo.historyPage.Transitions[0].Details = json.RawMessage(`{"stage":"pull"}`)
			case "corrupt-snapshot":
				repo.parentView.Snapshot = json.RawMessage(`{`)
			case "coordination-unavailable":
				repo.loadErr = errors.New("coordination unavailable")
			case "active-run":
				repo.coord.ActiveRunID = "another"
			}
			if _, err := s.DeliveryRetry(context.Background(), "parent", "retry-key", false); err == nil {
				t.Fatal("admitted invalid recovery")
			}
		})
	}
}

func TestDeliveryRetryRequiresGeneratedKeyAndReadableRequestState(t *testing.T) {
	s, repo := serviceFixture(t)
	s.Artifacts = retryArtifactFake{}
	s.ManifestStorage = retryArtifactFake{}
	s.Records = retryRecordFake{}
	s.ArtifactBucket = "artifacts"
	s.NewID = func() string { return "" }
	if _, err := s.DeliveryRetry(context.Background(), "parent", "", false); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	repo.readErr = errors.New("request store unavailable")
	if _, err := s.DeliveryRetry(context.Background(), "parent", "retry-key", false); !errors.Is(err, repo.readErr) {
		t.Fatal(err)
	}
}

func TestFullRerunRejectsInvalidOrUnavailableParent(t *testing.T) {
	for _, scenario := range []string{"missing-dependency", "parent-read", "nonterminal", "corrupt-snapshot", "no-jobs", "same-child-id", "zero-time", "admit", "conflicts", "repair", "publish", "child-read", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			s, repo := serviceFixture(t)
			parentRaw, err := events.BuildSnapshot(s.Config, events.Normalized{EventID: "original", Source: "urn:manual", OccurredAt: s.Now(), EventType: "daily-bhavcopy", Inputs: map[string]any{"exchangeName": "BSE"}}, "parent")
			if err != nil {
				t.Fatal(err)
			}
			repo.parentView = state.RunView{RunID: "parent", Phase: domain.Failed, LatestRunID: "parent", LatestPhase: domain.Failed, Snapshot: parentRaw}
			s.NewID = func() string { return "child" }
			ctx := context.Background()
			switch scenario {
			case "missing-dependency":
				s.NewID = nil
			case "parent-read":
				repo.viewErr = errors.New("state unavailable")
			case "nonterminal":
				repo.parentView.Phase = domain.Pulling
			case "corrupt-snapshot":
				repo.parentView.Snapshot = json.RawMessage(`{`)
			case "no-jobs":
				var original events.Snapshot
				_ = json.Unmarshal(parentRaw, &original)
				original.Jobs = nil
				repo.parentView.Snapshot, _ = json.Marshal(original)
			case "same-child-id":
				s.NewID = func() string { return "parent" }
			case "zero-time":
				s.Now = func() time.Time { return time.Time{} }
			case "admit":
				repo.admitErr = errors.New("S3 unavailable")
			case "conflicts":
				repo.conflicts = 20
			case "repair":
				repo.repairErr = errors.New("repair unavailable")
			case "publish":
				repo.publishErr = errors.New("SQS unavailable")
			case "child-read":
				repo.childViewErr = errors.New("history unavailable")
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if _, err := s.FullRerun(ctx, "parent", "retry-key", nil); err == nil {
				t.Fatal("failure hidden", scenario)
			}
		})
	}
}

func TestFullRerunPinsOriginalResolvedJobsAndForce(t *testing.T) {
	s, repo := serviceFixture(t)
	parentRaw, err := events.BuildSnapshot(s.Config, events.Normalized{EventID: "original", Source: "urn:manual", OccurredAt: s.Now(), EventType: "daily-bhavcopy", Inputs: map[string]any{"exchangeName": "BSE"}}, "parent")
	if err != nil {
		t.Fatal(err)
	}
	repo.parentView = state.RunView{RunID: "parent", Phase: domain.Failed, LatestRunID: "parent", LatestPhase: domain.Failed, Snapshot: parentRaw}
	s.NewID = func() string { return "child" }
	force := true
	result, err := s.FullRerun(context.Background(), "parent", "rerun-key", &force)
	if err != nil || result.RunID != "child" || result.Status != domain.Queued {
		t.Fatal(result, err)
	}
	var original, child events.Snapshot
	if json.Unmarshal(parentRaw, &original) != nil || json.Unmarshal(repo.captured.Snapshot, &child) != nil {
		t.Fatal("invalid snapshot")
	}
	if child.RunID != "child" || child.ParentRunID != "parent" || child.ExecutionKey != original.ExecutionKey || child.ConfigRevision != original.ConfigRevision || !child.Force || child.Inputs["tradeDate"] != original.Inputs["tradeDate"] || child.Jobs[0].URL != original.Jobs[0].URL {
		t.Fatal(child)
	}
	if child.RequestKey == original.RequestKey || child.PayloadHash == original.PayloadHash {
		t.Fatal("rerun retained old request identity")
	}
	repo.joined = true
	if _, err := s.FullRerun(context.Background(), "parent", "another-key", nil); !errors.Is(err, state.ErrConflict) {
		t.Fatal(err)
	}
	repo.joined = false
	repo.parentView.LatestPhase = domain.Queued
	if _, err := s.FullRerun(context.Background(), "parent", "blocked-key", nil); !errors.Is(err, state.ErrConflict) {
		t.Fatal(err)
	}
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
