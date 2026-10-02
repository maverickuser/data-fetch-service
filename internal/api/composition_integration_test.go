//go:build integration

package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/acquisition"
	"github.com/maverickuser/data-fetch-service/internal/admission"
	"github.com/maverickuser/data-fetch-service/internal/config"
	"github.com/maverickuser/data-fetch-service/internal/domain"
	"github.com/maverickuser/data-fetch-service/internal/events"
	"github.com/maverickuser/data-fetch-service/internal/pull"
	"github.com/maverickuser/data-fetch-service/internal/state"
)

type retryArtifacts struct{ data map[string][]byte }
type unavailableStat struct{ *retryArtifacts }

func (unavailableStat) Stat(context.Context, string, int64) error {
	return errors.New("S3 unavailable")
}

type invalidUpload struct{ *retryArtifacts }

func (a invalidUpload) Upload(context.Context, string, io.Reader) (acquisition.Artifact, error) {
	return acquisition.Artifact{Key: "wrong", Bytes: 1, SHA256: strings.Repeat("a", 64)}, nil
}

type integrationFetch func(context.Context, string, config.ResolvedJob) (acquisition.Result, error)

func (f integrationFetch) Fetch(ctx context.Context, run string, job config.ResolvedJob) (acquisition.Result, error) {
	return f(ctx, run, job)
}
func (a *retryArtifacts) Upload(_ context.Context, key string, source io.Reader) (acquisition.Artifact, error) {
	data, err := io.ReadAll(source)
	if err != nil {
		return acquisition.Artifact{}, err
	}
	if _, exists := a.data[key]; exists {
		return acquisition.Artifact{}, state.ErrIntegrity
	}
	a.data[key] = data
	hash := sha256.Sum256(data)
	return acquisition.Artifact{Key: key, Bytes: int64(len(data)), SHA256: hex.EncodeToString(hash[:])}, nil
}
func (a *retryArtifacts) Read(_ context.Context, key string) ([]byte, error) {
	data, ok := a.data[key]
	if !ok {
		return nil, state.ErrExpired
	}
	return data, nil
}
func (a *retryArtifacts) Stat(_ context.Context, key string, size int64) error {
	data, ok := a.data[key]
	if !ok || int64(len(data)) != size {
		return state.ErrExpired
	}
	return nil
}

func retryComposition(t *testing.T) (*Handler, *admission.Service, *integrationObjects, *integrationQueue, *retryArtifacts, events.Snapshot) {
	t.Helper()
	now := time.Date(2026, 9, 21, 14, 30, 0, 0, time.UTC)
	objects := &integrationObjects{objects: map[string]state.Object{}, now: now}
	queue := &integrationQueue{}
	h, service := integrationHandler(t, objects, queue, now, "root")
	if response := call(h, "POST", "/v1/events/daily-bhavcopy/runs", `{"inputs":{"exchangeName":"BSE"}}`); response.Code != 202 {
		t.Fatal(response.Code, response.Body.String())
	}
	view, err := h.Coordinator.ReadRun(context.Background(), "root")
	if err != nil {
		t.Fatal(err)
	}
	var parent events.Snapshot
	if json.Unmarshal(view.Snapshot, &parent) != nil {
		t.Fatal("snapshot")
	}
	artifacts := &retryArtifacts{data: map[string][]byte{}}
	raw := []byte("trade data")
	fileKey := "runs/root/raw/" + parent.Jobs[0].ID + "/" + parent.Jobs[0].Filename
	artifacts.data[fileKey] = raw
	hash := sha256.Sum256(raw)
	manifest, err := pull.BuildManifest(parent, []acquisition.Result{{JobID: parent.Jobs[0].ID, Filename: parent.Jobs[0].Filename, Format: parent.Jobs[0].Format, Artifact: acquisition.Artifact{Key: fileKey, Bytes: int64(len(raw)), SHA256: hex.EncodeToString(hash[:])}}}, "artifacts")
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := pull.MarshalManifest(manifest)
	artifacts.data["runs/root/manifest.json"] = encoded
	coord := service.Coordinator.(*state.Coordinator)
	coordKey := "coordination/" + parent.ExecutionKey + ".json"
	lease, err := coord.ClaimDispatched(context.Background(), coordKey, "root", "pull", "worker", now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	for _, transition := range []state.Transition{{RunID: "root", Sequence: 2, Phase: domain.Pulling, At: now}, {RunID: "root", Sequence: 3, Phase: domain.DownloadsCompleted, At: now}, {RunID: "root", Sequence: 4, Phase: domain.DeliveryPending, At: now}} {
		if err := coord.Commit(context.Background(), coordKey, lease, transition); err != nil {
			t.Fatal(err)
		}
	}
	if err := coord.PublishPending(context.Background(), coordKey, queue); err != nil {
		t.Fatal(err)
	}
	deliveryLease, err := coord.ClaimDispatched(context.Background(), coordKey, "root", "delivery", "delivery-worker", now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := coord.Commit(context.Background(), coordKey, deliveryLease, state.Transition{RunID: "root", Sequence: 5, Phase: domain.Failed, At: now, Details: json.RawMessage(`{"stage":"delivery","code":"PROCESSOR_UNAVAILABLE"}`)}); err != nil {
		t.Fatal(err)
	}
	service.Artifacts = artifacts
	service.ManifestStorage = artifacts
	service.ArtifactBucket = "artifacts"
	service.Records = state.New(objects)
	service.NewID = func() string { return "child" }
	h.DeliveryRecovery = service
	return h, service, objects, queue, artifacts, parent
}

func TestHTTPCompositionDeliveryRetryReusesRetainedFiles(t *testing.T) {
	h, service, objects, queue, artifacts, parent := retryComposition(t)
	service.Config.Processor.Enabled = true
	service.Config.Processor.URL = "https://processor.internal/v1/event-ingestions"
	response := call(h, "POST", "/v1/runs/root/delivery-retries", `{"use_current_processor_config":true}`)
	if response.Code != 202 {
		t.Fatal(response.Code, response.Body.String())
	}
	child, err := h.Coordinator.ReadRun(context.Background(), "child")
	if err != nil {
		t.Fatal(err)
	}
	var pinned events.Snapshot
	if json.Unmarshal(child.Snapshot, &pinned) != nil || pinned.DeliveryRetry == nil || pinned.DeliveryRetry.OldProcessorURL != parent.Config.Processor.URL || pinned.DeliveryRetry.NewProcessorURL != service.Config.Processor.URL {
		t.Fatal(pinned)
	}
	if got := call(h, "POST", "/v1/runs/root/delivery-retries", `{"use_current_processor_config":true}`); got.Code != 202 || !strings.Contains(got.Body.String(), `"reused":true`) {
		t.Fatal(got.Code, got.Body.String())
	}
	if got := call(h, "POST", "/v1/runs/root/delivery-retries", `{}`); got.Code != 409 {
		t.Fatal(got.Code)
	}
	fetches := 0
	worker := &pull.Service{Repository: state.New(objects), Coordinator: service.Coordinator.(*state.Coordinator), Fetcher: integrationFetch(func(context.Context, string, config.ResolvedJob) (acquisition.Result, error) {
		fetches++
		return acquisition.Result{}, errors.New("unexpected source call")
	}), ManifestStorage: artifacts, ArtifactReader: artifacts, Publisher: queue, ArtifactBucket: "artifacts", Now: service.Now}
	if err := worker.Run(context.Background(), "child", "worker-child", service.Now().Add(15*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if fetches != 0 || len(queue.runs) != 4 || queue.runs[3] != "delivery:child" {
		t.Fatal(fetches, queue.runs)
	}
	child, err = h.Coordinator.ReadRun(context.Background(), "child")
	if err != nil || child.Phase != domain.DeliveryPending {
		t.Fatal(child, err)
	}
	var reused pull.Manifest
	if json.Unmarshal(artifacts.data["runs/child/manifest.json"], &reused) != nil || reused.Data.Files[0].Key != "runs/root/raw/"+parent.Jobs[0].ID+"/"+parent.Jobs[0].Filename {
		t.Fatal(reused)
	}
}

func TestHTTPCompositionDeliveryRetryExpiryAndStaleBaseline(t *testing.T) {
	h, service, objects, _, artifacts, parent := retryComposition(t)
	fileKey := "runs/root/raw/" + parent.Jobs[0].ID + "/" + parent.Jobs[0].Filename
	delete(artifacts.data, fileKey)
	if got := call(h, "POST", "/v1/runs/root/delivery-retries", `{}`); got.Code != 410 {
		t.Fatal(got.Code, got.Body.String())
	}
	artifacts.data[fileKey] = []byte("trade data")
	coord := service.Coordinator.(*state.Coordinator)
	key := "coordination/" + parent.ExecutionKey + ".json"
	current, etag, err := coord.Load(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	accepted := state.Acceptance{RunID: "newer", Fingerprint: "sha256:different", AcceptedAt: service.Now().Add(time.Minute)}
	data, _ := json.Marshal(accepted)
	store := state.New(objects)
	acceptanceKey := "acceptance/" + parent.ExecutionKey + "/newer.json"
	if err := store.Create(context.Background(), acceptanceKey, data); err != nil {
		t.Fatal(err)
	}
	current.AcceptedBaseline = &state.Baseline{RunID: "newer", Fingerprint: accepted.Fingerprint, AcceptanceKey: acceptanceKey}
	data, _ = json.Marshal(current)
	if _, err := store.CompareAndSwap(context.Background(), key, data, etag); err != nil {
		t.Fatal(err)
	}
	if got := call(h, "POST", "/v1/runs/root/delivery-retries", `{}`); got.Code != 409 {
		t.Fatal(got.Code, got.Body.String())
	}
}

func TestHTTPCompositionDeliveryRetryRejectsWrongStageAndExpiredManifest(t *testing.T) {
	h, _, objects, _, artifacts, _ := retryComposition(t)
	delete(artifacts.data, "runs/root/manifest.json")
	if got := call(h, "POST", "/v1/runs/root/delivery-retries", `{}`); got.Code != 410 {
		t.Fatal(got.Code)
	}
	// A recorded pull failure must require a full rerun even if a manifest happens to exist.
	key := "runs/root/history/00000000000000000005.json"
	entry := state.Transition{RunID: "root", Sequence: 5, Phase: domain.Failed, At: objects.now, Details: json.RawMessage(`{"stage":"pull"}`)}
	raw, _ := json.Marshal(entry)
	object := objects.objects[key]
	object.Data = raw
	objects.objects[key] = object
	if got := call(h, "POST", "/v1/runs/root/delivery-retries", `{}`); got.Code != 409 {
		t.Fatal(got.Code, got.Body.String())
	}
}

func TestHTTPCompositionDeliveryRetryFileExpiresAfterAdmission(t *testing.T) {
	h, service, objects, queue, artifacts, parent := retryComposition(t)
	if got := call(h, "POST", "/v1/runs/root/delivery-retries", `{}`); got.Code != 202 {
		t.Fatal(got.Code, got.Body.String())
	}
	delete(artifacts.data, "runs/root/raw/"+parent.Jobs[0].ID+"/"+parent.Jobs[0].Filename)
	fetches := 0
	worker := &pull.Service{Repository: state.New(objects), Coordinator: service.Coordinator.(*state.Coordinator), Fetcher: integrationFetch(func(context.Context, string, config.ResolvedJob) (acquisition.Result, error) {
		fetches++
		return acquisition.Result{}, errors.New("unexpected source call")
	}), ManifestStorage: artifacts, ArtifactReader: artifacts, Publisher: queue, ArtifactBucket: "artifacts", Now: service.Now}
	if err := worker.Run(context.Background(), "child", "worker-child", service.Now().Add(15*time.Minute)); err != nil {
		t.Fatal(err)
	}
	view, err := h.Coordinator.ReadRun(context.Background(), "child")
	if err != nil || view.Phase != domain.Failed || fetches != 0 || len(queue.runs) != 3 {
		t.Fatal(view, err, fetches, queue.runs)
	}
}

func TestHTTPCompositionDeliveryRetryPreflightFailures(t *testing.T) {
	for _, scenario := range []string{"missing-dependencies", "missing-parent", "active-run", "corrupt-manifest", "manifest-upload-conflict", "invalid-current-config"} {
		t.Run(scenario, func(t *testing.T) {
			h, service, objects, _, artifacts, parent := retryComposition(t)
			path := "/v1/runs/root/delivery-retries"
			body := `{}`
			want := 409
			switch scenario {
			case "missing-dependencies":
				service.Artifacts = nil
				want = 400
			case "missing-parent":
				path = "/v1/runs/unknown/delivery-retries"
				want = 404
			case "active-run":
				coord := service.Coordinator.(*state.Coordinator)
				key := "coordination/" + parent.ExecutionKey + ".json"
				current, etag, err := coord.Load(context.Background(), key)
				if err != nil {
					t.Fatal(err)
				}
				current.ActiveRunID = "another"
				current.Phase = domain.Pulling
				raw, _ := json.Marshal(current)
				if _, err := state.New(objects).CompareAndSwap(context.Background(), key, raw, etag); err != nil {
					t.Fatal(err)
				}
			case "corrupt-manifest":
				artifacts.data["runs/root/manifest.json"] = []byte(`{`)
			case "manifest-upload-conflict":
				artifacts.data["runs/child/manifest.json"] = []byte(`occupied`)
			case "invalid-current-config":
				service.Config.Processor.URL = "http://insecure.example"
				body = `{"use_current_processor_config":true}`
				want = 400
			}
			if got := call(h, "POST", path, body); got.Code != want {
				t.Fatal(got.Code, got.Body.String())
			}
		})
	}
}

func TestHTTPCompositionDeliveryRetryAllowsOlderAcceptedBaseline(t *testing.T) {
	h, service, objects, _, _, parent := retryComposition(t)
	coord := service.Coordinator.(*state.Coordinator)
	key := "coordination/" + parent.ExecutionKey + ".json"
	current, etag, err := coord.Load(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	evidence := state.Acceptance{RunID: "older", Fingerprint: "sha256:old-dataset", AcceptedAt: service.Now().Add(-time.Hour)}
	data, _ := json.Marshal(evidence)
	store := state.New(objects)
	acceptanceKey := "acceptance/" + parent.ExecutionKey + "/older.json"
	if err := store.Create(context.Background(), acceptanceKey, data); err != nil {
		t.Fatal(err)
	}
	current.AcceptedBaseline = &state.Baseline{RunID: "older", Fingerprint: evidence.Fingerprint, AcceptanceKey: acceptanceKey}
	data, _ = json.Marshal(current)
	if _, err := store.CompareAndSwap(context.Background(), key, data, etag); err != nil {
		t.Fatal(err)
	}
	if got := call(h, "POST", "/v1/runs/root/delivery-retries", `{}`); got.Code != 202 {
		t.Fatal(got.Code, got.Body.String())
	}
}

func TestHTTPCompositionDeliveryRetryWorkerValidatesRetainedManifests(t *testing.T) {
	for _, scenario := range []string{"parent-manifest-expired", "child-manifest-expired", "parent-manifest-corrupt", "child-manifest-corrupt", "reader-missing", "parent-snapshot-expired", "parent-snapshot-corrupt", "parent-manifest-changed", "stat-unavailable"} {
		t.Run(scenario, func(t *testing.T) {
			h, service, objects, queue, artifacts, _ := retryComposition(t)
			if got := call(h, "POST", "/v1/runs/root/delivery-retries", `{}`); got.Code != 202 {
				t.Fatal(got.Code, got.Body.String())
			}
			worker := &pull.Service{Repository: state.New(objects), Coordinator: service.Coordinator.(*state.Coordinator), Fetcher: integrationFetch(func(context.Context, string, config.ResolvedJob) (acquisition.Result, error) {
				t.Fatal("source fetched")
				return acquisition.Result{}, nil
			}), ManifestStorage: artifacts, ArtifactReader: artifacts, Publisher: queue, ArtifactBucket: "artifacts", Now: service.Now}
			switch scenario {
			case "parent-manifest-expired":
				delete(artifacts.data, "runs/root/manifest.json")
			case "child-manifest-expired":
				delete(artifacts.data, "runs/child/manifest.json")
			case "parent-manifest-corrupt":
				artifacts.data["runs/root/manifest.json"] = []byte(`{`)
			case "child-manifest-corrupt":
				artifacts.data["runs/child/manifest.json"] = []byte(`{`)
			case "reader-missing":
				worker.ArtifactReader = nil
			case "parent-snapshot-expired":
				delete(objects.objects, "runs/root/snapshot.json")
			case "parent-snapshot-corrupt":
				object := objects.objects["runs/root/snapshot.json"]
				object.Data = []byte(`{`)
				objects.objects["runs/root/snapshot.json"] = object
			case "parent-manifest-changed":
				var manifest pull.Manifest
				if err := json.Unmarshal(artifacts.data["runs/root/manifest.json"], &manifest); err != nil {
					t.Fatal(err)
				}
				manifest.Data.DatasetFingerprint = "sha256:tampered"
				artifacts.data["runs/root/manifest.json"], _ = json.Marshal(manifest)
			case "stat-unavailable":
				worker.ArtifactReader = unavailableStat{artifacts}
			}
			err := worker.Run(context.Background(), "child", "worker-child", service.Now().Add(15*time.Minute))
			if strings.Contains(scenario, "expired") {
				view, readErr := h.Coordinator.ReadRun(context.Background(), "child")
				if err != nil || readErr != nil || view.Phase != domain.Failed {
					t.Fatal(err, readErr, view.Phase)
				}
			} else if scenario == "stat-unavailable" {
				if err == nil || errors.Is(err, state.ErrExpired) {
					t.Fatal(err)
				}
			} else if !errors.Is(err, state.ErrIntegrity) {
				t.Fatal(err)
			}
		})
	}
}

func TestHTTPCompositionDeliveryRetryRejectsCorruptEvidence(t *testing.T) {
	for _, scenario := range []string{"missing-acceptance", "corrupt-acceptance", "changed-fingerprint", "wrong-upload-result"} {
		t.Run(scenario, func(t *testing.T) {
			h, service, objects, _, artifacts, parent := retryComposition(t)
			if strings.Contains(scenario, "acceptance") {
				coord := service.Coordinator.(*state.Coordinator)
				key := "coordination/" + parent.ExecutionKey + ".json"
				current, etag, err := coord.Load(context.Background(), key)
				if err != nil {
					t.Fatal(err)
				}
				acceptanceKey := "acceptance/" + parent.ExecutionKey + "/missing.json"
				current.AcceptedBaseline = &state.Baseline{RunID: "older", Fingerprint: "sha256:old", AcceptanceKey: acceptanceKey}
				raw, _ := json.Marshal(current)
				if _, err := state.New(objects).CompareAndSwap(context.Background(), key, raw, etag); err != nil {
					t.Fatal(err)
				}
				if scenario == "corrupt-acceptance" {
					objects.objects[acceptanceKey] = state.Object{Data: []byte(`{`), Modified: objects.now}
				}
			}
			if scenario == "changed-fingerprint" {
				var manifest pull.Manifest
				if err := json.Unmarshal(artifacts.data["runs/root/manifest.json"], &manifest); err != nil {
					t.Fatal(err)
				}
				manifest.Data.DatasetFingerprint = "sha256:tampered"
				artifacts.data["runs/root/manifest.json"], _ = json.Marshal(manifest)
			}
			if scenario == "wrong-upload-result" {
				service.ManifestStorage = invalidUpload{artifacts}
			}
			if got := call(h, "POST", "/v1/runs/root/delivery-retries", `{}`); got.Code != 409 {
				t.Fatal(got.Code, got.Body.String())
			}
		})
	}
}

type integrationObjects struct {
	mu         sync.Mutex
	objects    map[string]state.Object
	version    int
	now        time.Time
	firstReads int
	barrier    chan struct{}
}

func (m *integrationObjects) Get(ctx context.Context, key string) (state.Object, error) {
	m.mu.Lock()
	if m.barrier != nil && strings.HasSuffix(key, "/intent.json") && m.firstReads < 2 {
		m.firstReads++
		if m.firstReads == 2 {
			close(m.barrier)
		}
		barrier := m.barrier
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return state.Object{}, ctx.Err()
		case <-barrier:
			return state.Object{}, state.ErrNotFound
		}
	}
	defer m.mu.Unlock()
	o, ok := m.objects[key]
	if !ok {
		return state.Object{}, state.ErrNotFound
	}
	o.Data = append([]byte(nil), o.Data...)
	return o, nil
}
func (m *integrationObjects) Put(_ context.Context, key string, data []byte, match string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	old, exists := m.objects[key]
	if (match == "" && exists) || (match != "" && (!exists || old.ETag != match)) {
		return "", state.ErrConflict
	}
	m.version++
	etag := fmt.Sprint(m.version)
	m.objects[key] = state.Object{Data: append([]byte(nil), data...), ETag: etag, Modified: m.now}
	return etag, nil
}
func (m *integrationObjects) List(_ context.Context, prefix, token string, limit int32) (state.Page, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := []string{}
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) && k > token {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	page := state.Page{Keys: keys}
	if len(keys) > int(limit) {
		page.Keys = keys[:limit]
		page.NextToken = page.Keys[len(page.Keys)-1]
	}
	return page, nil
}

type integrationQueue struct {
	mu   sync.Mutex
	runs []string
	fail bool
}

func (q *integrationQueue) Publish(_ context.Context, queue, run string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.runs = append(q.runs, queue+":"+run)
	if q.fail {
		return fmt.Errorf("lost queue reply")
	}
	return nil
}

func integrationHandler(t *testing.T, objects *integrationObjects, queue *integrationQueue, now time.Time, id string) (*Handler, *admission.Service) {
	t.Helper()
	unit, _ := handlerFixture(t)
	store := state.New(objects)
	clock := func() time.Time { return now }
	coord := state.NewCoordinator(store, clock)
	service := &admission.Service{Config: unit.Config, Coordinator: coord, Publisher: queue, Now: clock, NewID: func() string { return id }}
	return &Handler{Service: service, Recovery: service, Store: store, Coordinator: coord, Config: unit.Config, Now: clock}, service
}

func TestHTTPCompositionFullRerunUsesPinnedSnapshot(t *testing.T) {
	now := time.Date(2026, 9, 21, 14, 30, 0, 0, time.UTC)
	objects := &integrationObjects{objects: map[string]state.Object{}, now: now}
	queue := &integrationQueue{}
	h, service := integrationHandler(t, objects, queue, now, "root")
	if response := call(h, "POST", "/v1/events/daily-bhavcopy/runs", `{"inputs":{"exchangeName":"BSE"}}`); response.Code != 202 {
		t.Fatal(response.Code, response.Body.String())
	}
	root, err := h.Coordinator.ReadRun(context.Background(), "root")
	if err != nil {
		t.Fatal(err)
	}
	var original events.Snapshot
	if json.Unmarshal(root.Snapshot, &original) != nil {
		t.Fatal("missing snapshot")
	}
	coord := service.Coordinator.(*state.Coordinator)
	key := "coordination/" + original.ExecutionKey + ".json"
	lease, err := coord.ClaimDispatched(context.Background(), key, "root", "pull", "worker", now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := coord.Commit(context.Background(), key, lease, state.Transition{RunID: "root", Sequence: 2, Phase: domain.Failed, At: now}); err != nil {
		t.Fatal(err)
	}
	service.Now = func() time.Time { return now.Add(3 * 24 * time.Hour) }
	service.Config.Events = nil // The rerun must use the pinned snapshot, not the current event catalog.
	nextID := 0
	service.NewID = func() string {
		nextID++
		if nextID == 1 {
			return "child"
		}
		return fmt.Sprintf("candidate-%d", nextID)
	}
	response := call(h, "POST", "/v1/runs/root/reruns", `{"force":true}`)
	if response.Code != 202 {
		t.Fatal(response.Code, response.Body.String())
	}
	child, err := h.Coordinator.ReadRun(context.Background(), "child")
	if err != nil {
		t.Fatal(err)
	}
	var rerun events.Snapshot
	if json.Unmarshal(child.Snapshot, &rerun) != nil || rerun.ParentRunID != "root" || rerun.ExecutionKey != original.ExecutionKey || rerun.Inputs["run_date"] != original.Inputs["run_date"] || rerun.Jobs[0].URL != original.Jobs[0].URL || !rerun.Force {
		t.Fatal(rerun)
	}
	if response := call(h, "POST", "/v1/runs/root/reruns", `{"force":true}`); response.Code != 202 || !strings.Contains(response.Body.String(), `"reused":true`) {
		t.Fatal("idempotent replay changed", response.Code, response.Body.String())
	}
	request := httptest.NewRequest("POST", "/v1/runs/root/reruns", strings.NewReader(`{"force":true}`))
	request.Header.Set("Idempotency-Key", "different-rerun")
	blocked := httptest.NewRecorder()
	h.Routes().ServeHTTP(blocked, request)
	if blocked.Code != 409 {
		t.Fatal("second active rerun admitted", blocked.Code)
	}
	if len(queue.runs) != 2 || queue.runs[0] != "pull:root" || queue.runs[1] != "pull:child" {
		t.Fatal(queue.runs)
	}
}

func TestHTTPCompositionConcurrentFirstRequestAcrossMidnight(t *testing.T) {
	// Both first reads see no intent. Server clocks straddle midnight in Asia/Kolkata.
	before := time.Date(2026, 9, 21, 18, 29, 59, 0, time.UTC)
	after := before.Add(2 * time.Second)
	objects := &integrationObjects{objects: map[string]state.Object{}, now: after, barrier: make(chan struct{})}
	queue := &integrationQueue{}
	h1, _ := integrationHandler(t, objects, queue, before, "candidate-1")
	h2, _ := integrationHandler(t, objects, queue, after, "candidate-2")
	results := make(chan *httptest.ResponseRecorder, 2)
	var wg sync.WaitGroup
	for _, handler := range []*Handler{h1, h2} {
		wg.Go(func() {
			results <- call(handler, "POST", "/v1/events/daily-bhavcopy/runs", `{"inputs":{"exchangeName":"BSE"}}`)
		})
	}
	wg.Wait()
	close(results)
	chosen := ""
	for response := range results {
		if response.Code != 202 {
			t.Fatal(response.Code, response.Body.String())
		}
		var result admission.Result
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if chosen == "" {
			chosen = result.RunID
		}
		if chosen != result.RunID {
			t.Fatal("requests diverged")
		}
	}
	view, err := h1.Coordinator.ReadRun(context.Background(), chosen)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot events.Snapshot
	if err := json.Unmarshal(view.Snapshot, &snapshot); err != nil {
		t.Fatal(err)
	}
	date := "2026-09-21"
	if chosen == "candidate-2" {
		date = "2026-09-22"
	}
	if snapshot.Inputs["run_date"] != date {
		t.Fatal(snapshot.Inputs)
	}
	// The same key with a real change remains a conflict, even after race recovery.
	if response := call(h1, "POST", "/v1/events/daily-bhavcopy/runs", `{"inputs":{"exchangeName":"BSE"},"force":true}`); response.Code != 409 {
		t.Fatal(response.Code, response.Body.String())
	}
}

func TestHTTPCompositionTimeoutRecoveryReadsAndPagination(t *testing.T) {
	now := time.Date(2026, 9, 21, 14, 30, 0, 0, time.UTC)
	objects := &integrationObjects{objects: map[string]state.Object{}, now: now}
	queue := &integrationQueue{fail: true}
	h, service := integrationHandler(t, objects, queue, now, "run-1")
	body := `{"inputs":{"exchangeName":"BSE"}}`
	if response := call(h, "POST", "/v1/events/daily-bhavcopy/runs", body); response.Code != 503 {
		t.Fatal(response.Code)
	}
	queue.fail = false
	response := call(h, "POST", "/v1/events/daily-bhavcopy/runs", body)
	if response.Code != 202 {
		t.Fatal(response.Code, response.Body.String())
	}
	for _, path := range []string{"/v1/runs/run-1", "/v1/runs/run-1/history", "/v1/runs/run-1/pulls", "/v1/runs/run-1/delivery", "/v1/requests/urn%3Abond-platform%3Amanual/request-key", "/v1/events/daily-bhavcopy/active?inputs=" + url.QueryEscape(`{"exchangeName":"BSE"}`)} {
		if r := call(h, "GET", path, ""); r.Code != 200 {
			t.Fatal(path, r.Code, r.Body.String())
		}
	}
	service.NewID = func() string { return "run-2" }
	r := httptest.NewRequest("POST", "/v1/events/daily-bhavcopy/runs", strings.NewReader(`{"inputs":{"exchangeName":"BSE2"}}`))
	r.Header.Set("Idempotency-Key", "second")
	w := httptest.NewRecorder()
	h.Routes().ServeHTTP(w, r)
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	first := call(h, "GET", "/v1/events/daily-bhavcopy/runs?limit=1", "")
	var page struct {
		Items []state.RunView `json:"items"`
		Next  string          `json:"next_cursor"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Next == "" {
		t.Fatal(first.Body.String())
	}
	firstID := page.Items[0].RunID
	second := call(h, "GET", "/v1/events/daily-bhavcopy/runs?limit=1&cursor="+page.Next, "")
	if err := json.Unmarshal(second.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].RunID == firstID || page.Next != "" {
		t.Fatal(second.Body.String())
	}
	snapshot, err := h.Coordinator.ReadRun(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	var saved events.Snapshot
	if err := json.Unmarshal(snapshot.Snapshot, &saved); err != nil {
		t.Fatal(err)
	}
	coord := service.Coordinator.(*state.Coordinator)
	key := "coordination/" + saved.ExecutionKey + ".json"
	lease, err := coord.ClaimDispatched(context.Background(), key, "run-1", "pull", "worker", now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := coord.Commit(context.Background(), key, lease, state.Transition{RunID: "run-1", Sequence: 2, Phase: domain.Failed, At: now}); err != nil {
		t.Fatal(err)
	}
	if result := call(h, "POST", "/v1/events/daily-bhavcopy/runs", body); result.Code != 200 {
		t.Fatal(result.Code, result.Body.String())
	}
}

func TestHTTPCompositionExternalRaceAcrossConfigVersions(t *testing.T) {
	now := time.Date(2026, 9, 21, 14, 30, 0, 0, time.UTC)
	objects := &integrationObjects{objects: map[string]state.Object{}, now: now, barrier: make(chan struct{})}
	queue := &integrationQueue{}
	_, first := integrationHandler(t, objects, queue, now, "first")
	_, second := integrationHandler(t, objects, queue, now, "second")
	changed := second.Config.Events[0].Inputs["run_date"]
	changed.Default = "2026-09-23"
	second.Config.Events[0].Inputs["run_date"] = changed
	second.Config.DeploymentCommit = "new-version"
	event := events.Normalized{EventID: "same", Source: "urn:test", Type: events.PullRequested, EventType: "daily-bhavcopy", OccurredAt: now, Inputs: map[string]any{"exchangeName": "BSE"}}
	results := make(chan admission.Result, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, service := range []*admission.Service{first, second} {
		wg.Go(func() { result, err := service.External(context.Background(), event); results <- result; errs <- err })
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	run := ""
	for result := range results {
		if run == "" {
			run = result.RunID
		}
		if result.RunID != run {
			t.Fatal("external replay diverged")
		}
	}
	view, err := first.Coordinator.ReadRun(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot events.Snapshot
	if err := json.Unmarshal(view.Snapshot, &snapshot); err != nil {
		t.Fatal(err)
	}
	if !snapshot.Event.OccurredAt.Equal(now) {
		t.Fatal("producer timestamp changed")
	}
}
