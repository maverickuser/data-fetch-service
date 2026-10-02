package pull

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	lambdaevents "github.com/aws/aws-lambda-go/events"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/acquisition"
	"github.com/maverickuser/data-fetch-service/internal/config"
	"github.com/maverickuser/data-fetch-service/internal/domain"
	"github.com/maverickuser/data-fetch-service/internal/events"
	"github.com/maverickuser/data-fetch-service/internal/state"
)

type memoryObjects struct {
	mu      sync.Mutex
	items   map[string]state.Object
	clock   time.Time
	serial  int
	failKey string
}

func (m *memoryObjects) Get(_ context.Context, key string) (state.Object, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	item, ok := m.items[key]
	if !ok {
		return state.Object{}, state.ErrNotFound
	}
	return item, nil
}
func (m *memoryObjects) Put(_ context.Context, key string, data []byte, etag string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if key == m.failKey {
		return "", errors.New("state write unavailable")
	}
	old, ok := m.items[key]
	if etag == "" && ok || etag != "" && (!ok || etag != old.ETag) {
		return "", state.ErrConflict
	}
	m.serial++
	tag := fmt.Sprintf("etag-%d", m.serial)
	m.items[key] = state.Object{Data: bytes.Clone(data), ETag: tag, Modified: m.clock}
	return tag, nil
}
func (m *memoryObjects) List(_ context.Context, prefix, _ string, _ int32) (state.Page, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var keys []string
	for key := range m.items {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return state.Page{Keys: keys}, nil
}

type fetchFunc func(context.Context, string, config.ResolvedJob) (acquisition.Result, error)

func (f fetchFunc) Fetch(ctx context.Context, run string, job config.ResolvedJob) (acquisition.Result, error) {
	return f(ctx, run, job)
}

type uploadFunc func(context.Context, string, io.Reader) (acquisition.Artifact, error)

func (f uploadFunc) Upload(ctx context.Context, key string, reader io.Reader) (acquisition.Artifact, error) {
	return f(ctx, key, reader)
}

type publishFake struct {
	messages []string
	err      error
}

type claimConflict struct{ Coordinator }

type sourceClientFunc func(*http.Request) (*http.Response, error)

func (f sourceClientFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }

type failTerminalCommit struct{ Coordinator }

func (f failTerminalCommit) Commit(ctx context.Context, key string, lease state.Lease, tr state.Transition) error {
	if tr.Phase == domain.Failed {
		return errors.New("interrupted before terminal reservation")
	}
	return f.Coordinator.Commit(ctx, key, lease, tr)
}

func (claimConflict) ClaimDispatched(context.Context, string, string, string, string, time.Time) (state.Lease, error) {
	return state.Lease{}, state.ErrConflict
}

func (p *publishFake) Publish(_ context.Context, queue, run string) error {
	if p.err != nil {
		return p.err
	}
	p.messages = append(p.messages, queue+":"+run)
	return nil
}

func serviceFixture(t *testing.T, eventType string, force bool, baseline string) (*Service, *memoryObjects, *publishFake, *int, *events.Snapshot, []acquisition.Result) {
	t.Helper()
	snapshot, results := manifestFixture(t, eventType)
	snapshot.Force = force
	now := time.Now().UTC()
	objects := &memoryObjects{items: map[string]state.Object{}, clock: now}
	store := state.New(objects)
	coordinator := state.NewCoordinator(store, func() time.Time { return now })
	serialized, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := store.Create(ctx, "runs/run/snapshot.json", serialized); err != nil {
		t.Fatal(err)
	}
	coord := state.Coordination{SchemaVersion: 1, ActiveRunID: "run", Phase: domain.Queued, LastSequence: 1, Dispatch: &state.Dispatch{RunID: "run", Queue: "pull", State: "pending"}}
	if baseline != "" {
		coord.AcceptedBaseline = &state.Baseline{RunID: "previous", Fingerprint: baseline, AcceptanceKey: "acceptance/key/previous.json"}
	}
	data, err := json.Marshal(coord)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompareAndSwap(ctx, "coordination/"+snapshot.ExecutionKey+".json", data, ""); err != nil {
		t.Fatal(err)
	}
	history, err := json.Marshal(state.Transition{RunID: "run", Sequence: 1, Phase: domain.Queued, At: now})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Create(ctx, "runs/run/history/00000000000000000001.json", history); err != nil {
		t.Fatal(err)
	}
	publisher := &publishFake{}
	uploads := new(int)
	storage := uploadFunc(func(_ context.Context, key string, reader io.Reader) (acquisition.Artifact, error) {
		*uploads++
		data, err := io.ReadAll(reader)
		if err != nil {
			return acquisition.Artifact{}, err
		}
		hash := sha256.Sum256(data)
		return acquisition.Artifact{Key: key, Bytes: int64(len(data)), SHA256: hex.EncodeToString(hash[:])}, nil
	})
	selected := map[string]acquisition.Result{}
	for _, result := range results {
		selected[result.JobID] = result
	}
	fetch := fetchFunc(func(_ context.Context, _ string, job config.ResolvedJob) (acquisition.Result, error) {
		return selected[job.ID], nil
	})
	service := &Service{Repository: store, Coordinator: coordinator, Fetcher: fetch, ManifestStorage: storage, Publisher: publisher, ArtifactBucket: "artifacts", Now: func() time.Time { return now }}
	return service, objects, publisher, uploads, &snapshot, results
}

func TestPullCompleteBSEAndNSDL(t *testing.T) {
	for _, eventType := range []string{"daily-bhavcopy", "nsdl-bond-data"} {
		t.Run(eventType, func(t *testing.T) {
			service, objects, publisher, uploads, snapshot, _ := serviceFixture(t, eventType, false, "")
			if err := service.Run(context.Background(), "run", "invocation", service.Now().Add(15*time.Minute)); err != nil {
				t.Fatal(err)
			}
			key := "coordination/" + snapshot.ExecutionKey + ".json"
			current, _, err := service.Coordinator.Load(context.Background(), key)
			if err != nil || current.Phase != domain.DeliveryPending || len(publisher.messages) != 1 || *uploads != 1 {
				t.Fatal(current, err, publisher.messages, *uploads)
			}
			expected := len(snapshot.Jobs)
			count := 0
			for key := range objects.items {
				if strings.HasSuffix(key, "/result.json") {
					count++
				}
			}
			if count != expected {
				t.Fatal("incomplete job evidence", count, expected)
			}
			view, err := state.NewCoordinator(service.Repository.(*state.Store), service.Now).ReadRun(context.Background(), "run")
			if err != nil || view.Phase != domain.DeliveryPending || view.Sequence != 4 {
				t.Fatal(view, err)
			}
		})
	}
}

func TestPullSkipsAcceptedUnlessForced(t *testing.T) {
	snapshot, results := manifestFixture(t, "daily-bhavcopy")
	manifest, err := BuildManifest(snapshot, results, "artifacts")
	if err != nil {
		t.Fatal(err)
	}
	for _, forced := range []bool{false, true} {
		service, _, publisher, uploads, s, _ := serviceFixture(t, "daily-bhavcopy", forced, manifest.Data.DatasetFingerprint)
		if err := service.Run(context.Background(), "run", "worker", service.Now().Add(15*time.Minute)); err != nil {
			t.Fatal(err)
		}
		current, _, err := service.Coordinator.Load(context.Background(), "coordination/"+s.ExecutionKey+".json")
		expected := domain.SkippedUnchanged
		if forced {
			expected = domain.DeliveryPending
		}
		if err != nil || current.Phase != expected || *uploads != 1 || len(publisher.messages) != map[bool]int{false: 0, true: 1}[forced] {
			t.Fatal(current, err)
		}
	}
}

func TestPullPartialFailureNeverPublishesManifest(t *testing.T) {
	service, objects, publisher, uploads, snapshot, results := serviceFixture(t, "nsdl-bond-data", false, "")
	service.Fetcher = fetchFunc(func(ctx context.Context, _ string, job config.ResolvedJob) (acquisition.Result, error) {
		if job.ID == snapshot.Jobs[0].ID {
			return acquisition.Result{}, &acquisition.Failure{Code: "SOURCE_HTTP"}
		}
		<-ctx.Done()
		return acquisition.Result{}, ctx.Err()
	})
	if err := service.Run(context.Background(), "run", "worker", service.Now().Add(15*time.Minute)); err != nil {
		t.Fatal("terminal source failure was not durably recorded", err)
	}
	current, _, err := service.Coordinator.Load(context.Background(), "coordination/"+snapshot.ExecutionKey+".json")
	if err != nil || current.Phase != domain.Failed || *uploads != 0 || len(publisher.messages) != 0 {
		t.Fatal(current, err, publisher.messages)
	}
	if len(results) != 6 {
		t.Fatal("fixture incomplete")
	}
	for key := range objects.items {
		if strings.HasSuffix(key, "/manifest.json") {
			t.Fatal("partial manifest")
		}
	}
	assertFailedHistory(t, objects, "pull", "SOURCE_HTTP")
}

func TestPullOwnershipAndStorageFailures(t *testing.T) {
	service, _, _, uploads, snapshot, _ := serviceFixture(t, "daily-bhavcopy", false, "")
	service.ManifestStorage = uploadFunc(func(context.Context, string, io.Reader) (acquisition.Artifact, error) {
		return acquisition.Artifact{}, errors.New("S3 failed")
	})
	if err := service.Run(context.Background(), "run", "worker", service.Now().Add(15*time.Minute)); err == nil || *uploads != 0 {
		t.Fatal(err)
	}
	current, _, err := service.Coordinator.Load(context.Background(), "coordination/"+snapshot.ExecutionKey+".json")
	if err != nil || current.Phase != domain.Pulling {
		t.Fatal(current, err)
	}
	service, _, _, uploads, _, _ = serviceFixture(t, "daily-bhavcopy", false, "")
	service.Fetcher = fetchFunc(func(context.Context, string, config.ResolvedJob) (acquisition.Result, error) {
		return acquisition.Result{}, errors.New("fetch unavailable")
	})
	if err := service.Run(context.Background(), "run", "worker", service.Now().Add(15*time.Minute)); err != nil || *uploads != 0 {
		t.Fatal(err)
	}
}

func TestPullRejectsIncompleteRuntimeAndSnapshots(t *testing.T) {
	service, objects, _, _, snapshot, _ := serviceFixture(t, "daily-bhavcopy", false, "")
	if err := (&Service{}).Run(context.Background(), "run", "worker", time.Now().Add(time.Minute)); err == nil {
		t.Fatal("invalid service accepted")
	}
	if err := service.Run(context.Background(), "../run", "worker", time.Now().Add(time.Minute)); err == nil {
		t.Fatal("unsafe run ID")
	}
	if err := service.Run(context.Background(), "run", "worker", service.Now().Add(59*time.Second)); err == nil {
		t.Fatal("short budget accepted")
	}
	key := "runs/run/snapshot.json"
	saved := objects.items[key]
	for _, raw := range [][]byte{[]byte("{"), []byte(`{"schema_version":2}`), nil} {
		objects.items[key] = state.Object{Data: raw, Modified: saved.Modified, ETag: saved.ETag}
		if err := service.Run(context.Background(), "run", "worker", service.Now().Add(15*time.Minute)); err == nil {
			t.Fatal("bad snapshot accepted")
		}
	}
	objects.items[key] = saved
	delete(objects.items, key)
	if err := service.Run(context.Background(), "run", "worker", service.Now().Add(15*time.Minute)); !errors.Is(err, state.ErrNotFound) {
		t.Fatal(err)
	}
	objects.items[key] = saved
	if snapshot.ExecutionKey == "" {
		t.Fatal("fixture missing execution key")
	}
}

func TestPullFencesManifestWhenOwnershipChanges(t *testing.T) {
	service, objects, _, uploads, snapshot, results := serviceFixture(t, "daily-bhavcopy", false, "")
	original := results[0]
	service.Fetcher = fetchFunc(func(_ context.Context, _ string, _ config.ResolvedJob) (acquisition.Result, error) {
		key := "coordination/" + snapshot.ExecutionKey + ".json"
		objects.mu.Lock()
		item := objects.items[key]
		var coordination state.Coordination
		if err := json.Unmarshal(item.Data, &coordination); err != nil {
			t.Fatal(err)
		}
		coordination.OwnerToken = "other-invocation"
		item.Data, _ = json.Marshal(coordination)
		objects.items[key] = item
		objects.mu.Unlock()
		return original, nil
	})
	if err := service.Run(context.Background(), "run", "worker", service.Now().Add(15*time.Minute)); !errors.Is(err, state.ErrConflict) || *uploads != 0 {
		t.Fatal(err, *uploads)
	}
}

func TestPullDurableFailuresRemainRecoverable(t *testing.T) {
	service, objects, _, _, snapshot, _ := serviceFixture(t, "daily-bhavcopy", false, "")
	objects.failKey = "runs/run/pulls/" + snapshot.Jobs[0].ID + "/result.json"
	err := service.Run(context.Background(), "run", "worker", service.Now().Add(15*time.Minute))
	if !errors.Is(err, ErrOutcomePersistence) {
		t.Fatal(err)
	}
	current, _, err := service.Coordinator.Load(context.Background(), "coordination/"+snapshot.ExecutionKey+".json")
	if err != nil || current.Phase != domain.Pulling {
		t.Fatal(current, err)
	}
	service, _, publisher, _, snapshot, _ := serviceFixture(t, "daily-bhavcopy", false, "")
	publisher.err = errors.New("publish unavailable")
	if err := service.Run(context.Background(), "run", "worker", service.Now().Add(15*time.Minute)); err == nil {
		t.Fatal("lost delivery dispatch")
	}
	current, _, err = service.Coordinator.Load(context.Background(), "coordination/"+snapshot.ExecutionKey+".json")
	if err != nil || current.Phase != domain.DeliveryPending || current.Dispatch == nil || current.Dispatch.State != "pending" {
		t.Fatal(current, err)
	}
}

func TestPullRejectsFalseManifestStorageReceipt(t *testing.T) {
	for _, artifact := range []acquisition.Artifact{{Key: "wrong", Bytes: 1, SHA256: strings.Repeat("a", 64)}, {Key: "runs/run/manifest.json", Bytes: 1, SHA256: strings.Repeat("a", 64)}} {
		service, _, _, _, snapshot, _ := serviceFixture(t, "daily-bhavcopy", false, "")
		service.ManifestStorage = uploadFunc(func(context.Context, string, io.Reader) (acquisition.Artifact, error) { return artifact, nil })
		if err := service.Run(context.Background(), "run", "worker", service.Now().Add(15*time.Minute)); !errors.Is(err, state.ErrIntegrity) {
			t.Fatal(err)
		}
		current, _, err := service.Coordinator.Load(context.Background(), "coordination/"+snapshot.ExecutionKey+".json")
		if err != nil || current.Phase != domain.Pulling {
			t.Fatal(current, err)
		}
	}
}

func TestPullManifestContractFailureIsTerminal(t *testing.T) {
	service, objects, _, uploads, snapshot, _ := serviceFixture(t, "daily-bhavcopy", false, "")
	service.Fetcher = fetchFunc(func(_ context.Context, _ string, job config.ResolvedJob) (acquisition.Result, error) {
		return acquisition.Result{JobID: job.ID, Filename: job.Filename, Format: "json", Artifact: acquisition.Artifact{Key: "bad", Bytes: 1, SHA256: strings.Repeat("a", 64)}}, nil
	})
	if err := service.Run(context.Background(), "run", "worker", service.Now().Add(15*time.Minute)); err != nil || *uploads != 0 {
		t.Fatal(err)
	}
	current, _, err := service.Coordinator.Load(context.Background(), "coordination/"+snapshot.ExecutionKey+".json")
	if err != nil || current.Phase != domain.Failed {
		t.Fatal(current, err)
	}
	assertFailedHistory(t, objects, "manifest", "MANIFEST_INVALID")
}

func assertFailedHistory(t *testing.T, objects *memoryObjects, stage, code string) {
	t.Helper()
	coordinator := state.NewCoordinator(state.New(objects), func() time.Time { return objects.clock })
	history, err := coordinator.History(context.Background(), "run", "", 100)
	if err != nil || len(history.Transitions) < 3 {
		t.Fatal(history, err)
	}
	final := history.Transitions[len(history.Transitions)-1]
	var details struct {
		Stage         string `json:"stage"`
		Code          string `json:"code"`
		Message       string `json:"message"`
		RetryGuidance string `json:"retry_guidance"`
	}
	if err := json.Unmarshal(final.Details, &details); err != nil {
		t.Fatal(err)
	}
	if final.Phase != domain.Failed || details.Stage != stage || details.Code != code || details.Message == "" || details.RetryGuidance != "manual_rerun" {
		t.Fatal(final, details)
	}
}

func TestPullCanceledExecutionRetainsClaim(t *testing.T) {
	service, _, _, uploads, snapshot, _ := serviceFixture(t, "daily-bhavcopy", false, "")
	ctx, cancel := context.WithCancel(context.Background())
	service.Fetcher = fetchFunc(func(ctx context.Context, _ string, _ config.ResolvedJob) (acquisition.Result, error) {
		cancel()
		<-ctx.Done()
		return acquisition.Result{}, ctx.Err()
	})
	if err := service.Run(ctx, "run", "worker", service.Now().Add(15*time.Minute)); !errors.Is(err, context.Canceled) || *uploads != 0 {
		t.Fatal(err)
	}
	current, _, err := service.Coordinator.Load(context.Background(), "coordination/"+snapshot.ExecutionKey+".json")
	if err != nil || current.Phase != domain.Pulling {
		t.Fatal(current, err)
	}
}

func TestPullOutcomesControlExpiredExecutionRecovery(t *testing.T) {
	for _, scenario := range []string{"source-404", "group-budget"} {
		t.Run(scenario, func(t *testing.T) {
			service, objects, _, _, snapshot, _ := serviceFixture(t, "daily-bhavcopy", false, "")
			store := service.Repository.(*state.Store)
			if scenario == "source-404" {
				service.Fetcher = &acquisition.Fetcher{Client: sourceClientFunc(func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: 404, Body: io.NopCloser(strings.NewReader("")), Header: http.Header{}}, nil
				}), Storage: service.ManifestStorage, Recorder: acquisition.StateRecorder{Store: store}, Budget: acquisition.NewBudget(snapshot.Config.Defaults.MaxTempBytes), Limits: acquisition.ConfiguredLimits(snapshot.Config.Defaults), TempRoot: t.TempDir(), Now: service.Now}
				service.Coordinator = failTerminalCommit{Coordinator: service.Coordinator}
			} else {
				service.Fetcher = fetchFunc(func(ctx context.Context, _ string, _ config.ResolvedJob) (acquisition.Result, error) {
					<-ctx.Done()
					return acquisition.Result{}, ctx.Err()
				})
			}
			deadline := time.Now().Add(15 * time.Minute)
			if scenario == "group-budget" {
				deadline = time.Now().Add(60*time.Second + 100*time.Millisecond)
			}
			if err := service.Run(context.Background(), "run", "worker", deadline); err == nil {
				t.Fatal("expected interrupted run")
			}
			resultKey := "runs/run/pulls/" + snapshot.Jobs[0].ID + "/result.json"
			var outcome JobOutcome
			if err := json.Unmarshal(objects.items[resultKey].Data, &outcome); err != nil {
				t.Fatal(err)
			}
			wantStatus, wantCode := "failed", "SOURCE_HTTP"
			if scenario == "group-budget" {
				wantStatus, wantCode = "interrupted", "GROUP_BUDGET_EXCEEDED"
			}
			if outcome.Status != wantStatus || outcome.ErrorCode != wantCode {
				t.Fatal(outcome)
			}
			coordinator := state.NewCoordinator(store, func() time.Time { return deadline.Add(2 * time.Minute) })
			key := "coordination/" + snapshot.ExecutionKey + ".json"
			child, err := coordinator.ReservePullRetry(context.Background(), key, "child")
			if err != nil {
				t.Fatal(err)
			}
			current, _, err := coordinator.Load(context.Background(), key)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "source-404" {
				if child != "" || current.Phase != domain.Failed || current.ActiveRunID != "" {
					t.Fatal(child, current)
				}
			} else if child != "child" || current.Phase != domain.Queued || current.ActiveRunID != "child" {
				t.Fatal(child, current)
			}
		})
	}
}

func TestPullUsesAdmittedSnapshotLimits(t *testing.T) {
	service, _, _, _, snapshot, results := serviceFixture(t, "daily-bhavcopy", false, "")
	admitted := snapshot.Config.Defaults
	expected := results[0]
	called := false
	service.Fetcher = nil
	service.FetcherForSnapshot = func(got config.Defaults) Fetcher {
		called = true
		if got != admitted {
			t.Fatal("current deployment limits replaced admitted limits", got, admitted)
		}
		return fetchFunc(func(context.Context, string, config.ResolvedJob) (acquisition.Result, error) { return expected, nil })
	}
	if err := service.Run(context.Background(), "run", "worker", service.Now().Add(15*time.Minute)); err != nil || !called {
		t.Fatal(err, called)
	}
}

func TestPullStaleMessageAndBadSnapshotConfiguration(t *testing.T) {
	service, _, _, _, _, _ := serviceFixture(t, "daily-bhavcopy", false, "")
	if err := service.Run(context.Background(), "run", "worker", service.Now().Add(15*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := service.Run(context.Background(), "run", "new-worker", service.Now().Add(15*time.Minute)); !errors.Is(err, state.ErrConflict) {
		t.Fatal("stale pull was not fenced", err)
	}
	if err := service.Run(context.Background(), "run", "new-worker", service.Now().Add(15*time.Minute)); !errors.Is(err, ErrStalePullMessage) {
		t.Fatal("stale claim not classified", err)
	}
	service, objects, _, _, _, _ := serviceFixture(t, "daily-bhavcopy", false, "")
	object := objects.items["runs/run/snapshot.json"]
	var snapshot events.Snapshot
	if err := json.Unmarshal(object.Data, &snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot.Config.Defaults.PullConcurrency = 0
	object.Data, _ = json.Marshal(snapshot)
	objects.items["runs/run/snapshot.json"] = object
	if err := service.Run(context.Background(), "run", "worker", service.Now().Add(15*time.Minute)); err == nil {
		t.Fatal("invalid admitted config accepted")
	}
}

func TestPullSnapshotFetcherCannotDisappear(t *testing.T) {
	service, _, _, uploads, snapshot, _ := serviceFixture(t, "daily-bhavcopy", false, "")
	service.Fetcher = nil
	service.FetcherForSnapshot = func(config.Defaults) Fetcher { return nil }
	if err := service.Run(context.Background(), "run", "worker", service.Now().Add(15*time.Minute)); err != nil || *uploads != 0 {
		t.Fatal(err)
	}
	current, _, err := service.Coordinator.Load(context.Background(), "coordination/"+snapshot.ExecutionKey+".json")
	if err != nil || current.Phase != domain.Failed {
		t.Fatal(current, err)
	}
}

func TestPullRechecksOwnerAfterManifestUpload(t *testing.T) {
	service, objects, publisher, _, snapshot, _ := serviceFixture(t, "daily-bhavcopy", false, "")
	service.ManifestStorage = uploadFunc(func(_ context.Context, key string, source io.Reader) (acquisition.Artifact, error) {
		data, err := io.ReadAll(source)
		if err != nil {
			return acquisition.Artifact{}, err
		}
		coordKey := "coordination/" + snapshot.ExecutionKey + ".json"
		objects.mu.Lock()
		item := objects.items[coordKey]
		var authority state.Coordination
		if err := json.Unmarshal(item.Data, &authority); err != nil {
			t.Fatal(err)
		}
		authority.OwnerToken = "successor"
		item.Data, _ = json.Marshal(authority)
		objects.items[coordKey] = item
		objects.mu.Unlock()
		hash := sha256.Sum256(data)
		return acquisition.Artifact{Key: key, Bytes: int64(len(data)), SHA256: hex.EncodeToString(hash[:])}, nil
	})
	if err := service.Run(context.Background(), "run", "worker", service.Now().Add(15*time.Minute)); !errors.Is(err, state.ErrConflict) {
		t.Fatal(err)
	}
	current, _, err := service.Coordinator.Load(context.Background(), "coordination/"+snapshot.ExecutionKey+".json")
	if err != nil || current.Phase != domain.Pulling || len(publisher.messages) != 0 {
		t.Fatal(current, err)
	}
}

func TestPullPendingAdmissionConflictRetriesQueueMessage(t *testing.T) {
	service, objects, _, uploads, snapshot, _ := serviceFixture(t, "daily-bhavcopy", false, "")
	key := "coordination/" + snapshot.ExecutionKey + ".json"
	item := objects.items[key]
	var authority state.Coordination
	if err := json.Unmarshal(item.Data, &authority); err != nil {
		t.Fatal(err)
	}
	authority.Admission = &state.AdmissionDecision{}
	authority.Dispatch.State = "sent"
	item.Data, _ = json.Marshal(authority)
	objects.items[key] = item
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(15*time.Minute))
	defer cancel()
	response, err := (&Ingress{Runner: service, Store: service.Repository, NewToken: func() string { return "worker" }}).Handle(ctx, lambdaevents.SQSEvent{Records: []lambdaevents.SQSMessage{{MessageId: "only-message", Body: `{"run_id":"run"}`}}})
	if err != nil || len(response.BatchItemFailures) != 1 || response.BatchItemFailures[0].ItemIdentifier != "only-message" || *uploads != 0 {
		t.Fatal(response, err)
	}
	current, _, err := service.Coordinator.Load(context.Background(), key)
	if err != nil || current.OwnerToken != "" || current.Phase != domain.Queued || current.Dispatch.State != "sent" {
		t.Fatal(current, err)
	}
}

func TestPullOwnerlessClaimCASConflictIsRetryable(t *testing.T) {
	service, _, _, _, _, _ := serviceFixture(t, "daily-bhavcopy", false, "")
	service.Coordinator = claimConflict{service.Coordinator}
	err := service.Run(context.Background(), "run", "worker", service.Now().Add(15*time.Minute))
	if !errors.Is(err, state.ErrConflict) || errors.Is(err, ErrStalePullMessage) {
		t.Fatal("ownerless CAS conflict was acknowledged", err)
	}
}

func TestPullRecoversPersistedSameTokenClaim(t *testing.T) {
	service, objects, _, _, snapshot, _ := serviceFixture(t, "daily-bhavcopy", false, "")
	deadline := service.Now().Add(15 * time.Minute)
	key := "coordination/" + snapshot.ExecutionKey + ".json"
	item := objects.items[key]
	var authority state.Coordination
	if err := json.Unmarshal(item.Data, &authority); err != nil {
		t.Fatal(err)
	}
	authority.OwnerToken = "worker"
	authority.OwnerDeadline = deadline.Add(time.Minute)
	authority.Generation = 1
	authority.Dispatch.State = "claimed"
	item.Data, _ = json.Marshal(authority)
	objects.items[key] = item
	if err := service.Run(context.Background(), "run", "worker", deadline); err != nil {
		t.Fatal("persisted same-token claim not resumed", err)
	}
	current, _, err := service.Coordinator.Load(context.Background(), key)
	if err != nil || current.Phase != domain.DeliveryPending {
		t.Fatal(current, err)
	}
}
