package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/delivery"
	"github.com/maverickuser/data-fetch-service/internal/domain"
	"github.com/maverickuser/data-fetch-service/internal/events"
	"github.com/maverickuser/data-fetch-service/internal/state"
)

var testNow = time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC)

type fakeRepository struct {
	objects              map[string]state.Object
	writes               int
	scanError, errorRead error
	casError             error
}

func (r *fakeRepository) Read(_ context.Context, key string, _ time.Time) (state.Object, error) {
	if r.errorRead != nil {
		return state.Object{}, r.errorRead
	}
	object, ok := r.objects[key]
	if !ok {
		return state.Object{}, state.ErrNotFound
	}
	return object, nil
}
func (r *fakeRepository) Scan(_ context.Context, prefix, token string, limit int32) (state.Page, error) {
	if r.scanError != nil {
		return state.Page{}, r.scanError
	}
	var keys []string
	for key := range r.objects {
		if strings.HasPrefix(key, prefix) && key > token {
			keys = append(keys, key)
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
func (r *fakeRepository) CompareAndSwap(_ context.Context, key string, data []byte, etag string) (string, error) {
	if r.casError != nil {
		return "", r.casError
	}
	old, exists := r.objects[key]
	if etag == "" && exists || etag != "" && (!exists || old.ETag != etag) {
		return "", state.ErrConflict
	}
	r.writes++
	tag := string(rune('a' + r.writes))
	r.objects[key] = state.Object{Data: append([]byte(nil), data...), ETag: tag, Modified: testNow}
	return tag, nil
}

func TestReconcilerDoesNotAdvanceCursorOnStateFailure(t *testing.T) {
	for _, scenario := range []string{"missing-coordination", "missing-cursor-read", "cursor-save-failed", "cursor-create-failed", "invalid-request-key"} {
		t.Run(scenario, func(t *testing.T) {
			svc, repo, _, _ := testService()
			svc.PageLimit = 10
			key := "coordination/exec.json"
			repo.objects[key] = state.Object{Data: []byte("{}")}
			switch scenario {
			case "missing-cursor-read":
				repo.errorRead = errors.New("S3 unavailable")
			case "cursor-save-failed":
				repo.objects[cursorKey] = state.Object{Data: []byte(`{"schema_version":1,"stage":"coordination"}`), ETag: "etag"}
				repo.casError = errors.New("CAS failed")
				delete(repo.objects, key)
			case "cursor-create-failed":
				repo.casError = errors.New("CAS failed")
			case "invalid-request-key":
				delete(repo.objects, key)
				repo.objects[cursorKey] = state.Object{Data: []byte(`{"schema_version":1,"stage":"requests"}`), ETag: "etag"}
				repo.objects["requests/a/b/intent.json"] = state.Object{Data: []byte("{}")}
			}
			ctx, cancel := recoveryContext()
			err := svc.Run(ctx)
			cancel()
			if err == nil {
				t.Fatal("failure hidden", scenario)
			}
		})
	}
}

type fakeCoordinator struct {
	runs                                                                        map[string]state.Coordination
	repair, publish, retry, admit, requeue                                      int
	commits                                                                     []state.Transition
	resolutions                                                                 map[string]state.Resolution
	readRequestError, resolutionError                                           error
	loadError, repairError, publishError, retryError, requeueError, commitError error
}

func (c *fakeCoordinator) Load(_ context.Context, key string) (state.Coordination, string, error) {
	if c.loadError != nil {
		return state.Coordination{}, "", c.loadError
	}
	value, ok := c.runs[key]
	if !ok {
		return state.Coordination{}, "", state.ErrNotFound
	}
	return value, "etag", nil
}
func (c *fakeCoordinator) Repair(_ context.Context, key string) error {
	if c.repairError != nil {
		return c.repairError
	}
	c.repair++
	value := c.runs[key]
	value.Pending = nil
	value.Admission = nil
	value.Retry = nil
	c.runs[key] = value
	return nil
}
func (c *fakeCoordinator) ReservePullRetry(_ context.Context, key, child string) (string, error) {
	if c.retryError != nil {
		return "", c.retryError
	}
	c.retry++
	value := c.runs[key]
	value.ActiveRunID = child
	value.Phase = domain.Queued
	value.Dispatch = &state.Dispatch{RunID: child, Queue: "pull", State: "pending"}
	c.runs[key] = value
	return child, nil
}
func (c *fakeCoordinator) RequeueExpiredDispatch(_ context.Context, key string) error {
	if c.requeueError != nil {
		return c.requeueError
	}
	c.requeue++
	value := c.runs[key]
	value.OwnerToken = ""
	value.OwnerDeadline = time.Time{}
	value.Dispatch.State = "pending"
	c.runs[key] = value
	return nil
}
func (c *fakeCoordinator) ClaimExpiredDownloadsCompleted(_ context.Context, key, token string, _ time.Time) (state.Lease, error) {
	value := c.runs[key]
	value.Generation++
	value.OwnerToken = token
	c.runs[key] = value
	return state.Lease{RunID: value.ActiveRunID, Generation: value.Generation, Token: token}, nil
}
func (c *fakeCoordinator) Commit(_ context.Context, key string, _ state.Lease, tr state.Transition) error {
	if c.commitError != nil {
		return c.commitError
	}
	c.commits = append(c.commits, tr)
	value := c.runs[key]
	value.Phase = tr.Phase
	value.LastSequence = tr.Sequence
	if tr.Phase == domain.DeliveryPending {
		value.Dispatch = &state.Dispatch{RunID: tr.RunID, Queue: "delivery", State: "pending"}
	}
	c.runs[key] = value
	return nil
}
func (c *fakeCoordinator) PublishPending(_ context.Context, key string, _ state.Publisher) error {
	if c.publishError != nil {
		return c.publishError
	}
	c.publish++
	value := c.runs[key]
	if value.Dispatch != nil {
		value.Dispatch.State = "sent"
	}
	c.runs[key] = value
	return nil
}

func TestReconcilerSurfacesStateRepairFailures(t *testing.T) {
	for _, scenario := range []string{"load", "repair", "publish", "retry", "requeue", "download-commit", "delivery-deadline", "download-deadline"} {
		t.Run(scenario, func(t *testing.T) {
			svc, repo, coord, _ := testService()
			key := "coordination/exec.json"
			repo.objects[key] = state.Object{Data: []byte("{}")}
			current := state.Coordination{ActiveRunID: "run", Phase: domain.Pulling, OwnerDeadline: testNow.Add(-time.Minute)}
			failure := errors.New("state unavailable")
			switch scenario {
			case "load":
				coord.loadError = failure
			case "repair":
				current.Pending = &state.Transition{RunID: "run"}
				coord.repairError = failure
			case "publish":
				current.Dispatch = &state.Dispatch{RunID: "run", Queue: "pull", State: "pending"}
				coord.publishError = failure
			case "retry":
				coord.retryError = failure
			case "requeue":
				current.Phase = domain.Queued
				current.Dispatch = &state.Dispatch{RunID: "run", Queue: "pull", State: "claimed"}
				coord.requeueError = failure
			case "download-commit":
				current.Phase = domain.DownloadsCompleted
				coord.commitError = failure
			case "delivery-deadline":
				current.Phase = domain.Delivering
			case "download-deadline":
				current.Phase = domain.DownloadsCompleted
			}
			coord.runs[key] = current
			if scenario == "download-commit" {
				snapshot, _ := json.Marshal(events.Snapshot{SchemaVersion: 1, RunID: "run", ExecutionKey: "exec"})
				repo.objects["runs/run/snapshot.json"] = state.Object{Data: snapshot}
				transition, _ := json.Marshal(state.Transition{RunID: "run", Phase: domain.DownloadsCompleted, Details: json.RawMessage(`{"dataset_fingerprint":"sha256:abc"}`)})
				repo.objects["runs/run/history/00000000000000000000.json"] = state.Object{Data: transition}
			}
			ctx := context.Background()
			if scenario != "delivery-deadline" && scenario != "download-deadline" {
				var cancel context.CancelFunc
				ctx, cancel = recoveryContext()
				defer cancel()
			}
			if err := svc.repairCoordination(ctx, key); err == nil {
				t.Fatal("state failure hidden", scenario)
			}
		})
	}
}
func (c *fakeCoordinator) ReadRequest(_ context.Context, key string) (state.RequestIntent, error) {
	if c.readRequestError != nil {
		return state.RequestIntent{}, c.readRequestError
	}
	return state.RequestIntent{RequestKey: key}, nil
}
func (c *fakeCoordinator) RequestResolution(_ context.Context, key string) (state.Resolution, error) {
	if c.resolutionError != nil {
		return state.Resolution{}, c.resolutionError
	}
	value, ok := c.resolutions[key]
	if !ok {
		return state.Resolution{}, state.ErrNotFound
	}
	return value, nil
}

func TestReconcilerRequestRetentionDoesNotStallCursor(t *testing.T) {
	for _, scenario := range []string{"expired-resolution", "expired-intent", "deleted-intent", "backend-error"} {
		t.Run(scenario, func(t *testing.T) {
			svc, repo, coord, _ := testService()
			repo.objects[cursorKey] = state.Object{Data: []byte(`{"schema_version":1,"stage":"requests"}`), ETag: "etag"}
			repo.objects["requests/old/intent.json"] = state.Object{Data: []byte("{}")}
			switch scenario {
			case "expired-resolution":
				coord.resolutionError = state.ErrExpired
			case "expired-intent":
				coord.readRequestError = state.ErrExpired
			case "deleted-intent":
				coord.readRequestError = state.ErrNotFound
			case "backend-error":
				coord.resolutionError = errors.New("unavailable")
			}
			ctx, cancel := recoveryContext()
			err := svc.Run(ctx)
			cancel()
			if scenario == "backend-error" {
				if err == nil || coord.admit != 0 {
					t.Fatal(err, coord.admit)
				}
			} else if err != nil || coord.admit != 0 {
				t.Fatal(err, coord.admit)
			}
		})
	}
}

func TestReconcilerPublishesPendingWithoutRetryingLiveWorker(t *testing.T) {
	svc, repo, coord, _ := testService()
	repo.objects["coordination/exec.json"] = state.Object{Data: []byte("{}")}
	coord.runs["coordination/exec.json"] = state.Coordination{ActiveRunID: "run", Phase: domain.Queued, Dispatch: &state.Dispatch{RunID: "run", Queue: "pull", State: "pending"}}
	ctx, cancel := recoveryContext()
	err := svc.Run(ctx)
	cancel()
	if err != nil || coord.publish != 1 || coord.retry != 0 || coord.requeue != 0 {
		t.Fatal(err, coord)
	}
}
func (c *fakeCoordinator) Admit(_ context.Context, r state.RequestIntent) (state.Resolution, error) {
	c.admit++
	c.resolutions[r.RequestKey] = state.Resolution{RequestKey: r.RequestKey, RunID: "run"}
	return c.resolutions[r.RequestKey], nil
}

type fakePublisher struct{}

func (fakePublisher) Publish(context.Context, string, string) error { return nil }

type fakeDelivery struct {
	calls int
	err   error
}

func (d *fakeDelivery) Resume(context.Context, string, string, time.Time) error {
	d.calls++
	return d.err
}

func testService() (*Service, *fakeRepository, *fakeCoordinator, *fakeDelivery) {
	repo := &fakeRepository{objects: map[string]state.Object{}}
	coord := &fakeCoordinator{runs: map[string]state.Coordination{}, resolutions: map[string]state.Resolution{}}
	deliveryWorker := &fakeDelivery{}
	svc := &Service{Repository: repo, Coordinator: coord, Publisher: fakePublisher{}, Delivery: deliveryWorker, Now: func() time.Time { return testNow }, NewID: func() string { return "child" }, PageLimit: 1}
	return svc, repo, coord, deliveryWorker
}

func recoveryContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), time.Hour)
}

func TestBoundedCursorAlternatesPrefixesAndResolvesRequests(t *testing.T) {
	svc, repo, coord, _ := testService()
	repo.objects["coordination/exec.json"] = state.Object{Data: []byte("{}")}
	coord.runs["coordination/exec.json"] = state.Coordination{Phase: domain.Queued}
	repo.objects["requests/request/intent.json"] = state.Object{Data: []byte("{}")}
	ctx, cancel := recoveryContext()
	defer cancel()
	for range 5 {
		if err := svc.Run(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if coord.admit != 1 || repo.writes < 2 {
		t.Fatal(coord.admit, repo.writes)
	}
	var cursor Cursor
	if err := json.Unmarshal(repo.objects[cursorKey].Data, &cursor); err != nil || cursor.SchemaVersion != 1 {
		t.Fatal(cursor, err)
	}
	if _, err := coord.RequestResolution(ctx, "request"); err != nil {
		t.Fatal(err)
	}
}

func TestReconcilerRepairsBeforeExpiredPullRetry(t *testing.T) {
	svc, repo, coord, _ := testService()
	svc.PageLimit = 10
	key := "coordination/exec.json"
	repo.objects[key] = state.Object{Data: []byte("{}")}
	coord.runs[key] = state.Coordination{ActiveRunID: "root", Phase: domain.Pulling, OwnerDeadline: testNow.Add(-time.Minute), Pending: &state.Transition{RunID: "root"}}
	ctx, cancel := recoveryContext()
	defer cancel()
	if err := svc.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if coord.repair != 1 || coord.retry != 1 || coord.publish != 1 || coord.runs[key].ActiveRunID != "child" {
		t.Fatal(coord)
	}
}

func TestReconcilerHonorsDeliveryDelayAndSurfacesFailures(t *testing.T) {
	for _, scenario := range []string{"not-ready", "failed"} {
		t.Run(scenario, func(t *testing.T) {
			svc, repo, coord, worker := testService()
			svc.PageLimit = 10
			key := "coordination/exec.json"
			repo.objects[key] = state.Object{Data: []byte("{}")}
			coord.runs[key] = state.Coordination{ActiveRunID: "run", Phase: domain.Delivering, OwnerDeadline: testNow.Add(-time.Minute)}
			if scenario == "not-ready" {
				worker.err = delivery.ErrRetryNotReady
			} else {
				worker.err = errors.New("S3 failed")
			}
			ctx, cancel := recoveryContext()
			defer cancel()
			err := svc.Run(ctx)
			if scenario == "not-ready" && err != nil || scenario == "failed" && err == nil || worker.calls != 1 {
				t.Fatal(err, worker.calls)
			}
		})
	}
}

func TestReconcilerRejectsInvalidDependenciesAndCursor(t *testing.T) {
	svc, repo, _, _ := testService()
	if err := (&Service{}).Run(context.Background()); err == nil {
		t.Fatal("missing dependencies")
	}
	svc.PageLimit = 1001
	if err := svc.Run(context.Background()); err == nil {
		t.Fatal("bad page limit")
	}
	svc.PageLimit = 1
	repo.objects[cursorKey] = state.Object{Data: []byte(`{"schema_version":2,"stage":"bad"}`), ETag: "etag"}
	if err := svc.Run(context.Background()); !errors.Is(err, state.ErrIntegrity) {
		t.Fatal(err)
	}
	repo.objects[cursorKey] = state.Object{Data: []byte(`{"schema_version":1,"stage":"coordination"}`), ETag: "etag"}
	repo.scanError = errors.New("scan unavailable")
	if err := svc.Run(context.Background()); err == nil {
		t.Fatal("scan failure hidden")
	}
}

func TestReconcilerRequeuesExpiredPreworkClaims(t *testing.T) {
	for _, phase := range []domain.State{domain.Queued, domain.DeliveryPending} {
		svc, repo, coord, _ := testService()
		svc.PageLimit = 10
		key := "coordination/exec.json"
		repo.objects[key] = state.Object{Data: []byte("{}")}
		queue := "pull"
		if phase == domain.DeliveryPending {
			queue = "delivery"
		}
		coord.runs[key] = state.Coordination{ActiveRunID: "run", Phase: phase, OwnerToken: "abandoned", OwnerDeadline: testNow.Add(-time.Minute), Dispatch: &state.Dispatch{RunID: "run", Queue: queue, State: "claimed"}}
		ctx, cancel := recoveryContext()
		err := svc.Run(ctx)
		cancel()
		if err != nil || coord.requeue != 1 || coord.publish != 1 || coord.runs[key].OwnerToken != "" {
			t.Fatal(phase, err, coord)
		}
	}
}

func TestReconcilerFinishesPostDownloadDecision(t *testing.T) {
	for _, unchanged := range []bool{false, true} {
		svc, repo, coord, _ := testService()
		svc.PageLimit = 10
		key := "coordination/exec.json"
		repo.objects[key] = state.Object{Data: []byte("{}")}
		fingerprint := "sha256:abc"
		current := state.Coordination{ActiveRunID: "run", Phase: domain.DownloadsCompleted, LastSequence: 3, OwnerToken: "old", OwnerDeadline: testNow.Add(-time.Minute)}
		if unchanged {
			current.AcceptedBaseline = &state.Baseline{Fingerprint: fingerprint}
		}
		coord.runs[key] = current
		snapshot, _ := json.Marshal(events.Snapshot{SchemaVersion: 1, RunID: "run", ExecutionKey: "exec"})
		repo.objects["runs/run/snapshot.json"] = state.Object{Data: snapshot}
		transition, _ := json.Marshal(state.Transition{RunID: "run", Phase: domain.DownloadsCompleted, Details: json.RawMessage(`{"dataset_fingerprint":"sha256:abc"}`)})
		repo.objects["runs/run/history/00000000000000000003.json"] = state.Object{Data: transition}
		ctx, cancel := recoveryContext()
		err := svc.Run(ctx)
		cancel()
		want := domain.DeliveryPending
		publish := 1
		if unchanged {
			want = domain.SkippedUnchanged
			publish = 0
		}
		if err != nil || len(coord.commits) != 1 || coord.commits[0].Phase != want || coord.publish != publish {
			t.Fatal(unchanged, err, coord)
		}
	}
}

func TestReconcilerSkipsExpiredRequestRecords(t *testing.T) {
	svc, repo, coord, _ := testService()
	repo.objects[cursorKey] = state.Object{Data: []byte(`{"schema_version":1,"stage":"requests"}`), ETag: "etag"}
	repo.objects["requests/expired/intent.json"] = state.Object{Data: []byte("{}")}
	coord.resolutions["expired"] = state.Resolution{RequestKey: "expired"}
	ctx, cancel := recoveryContext()
	err := svc.Run(ctx)
	cancel()
	if err != nil || coord.admit != 0 {
		t.Fatal(err, coord.admit)
	}
}

func TestReconcilerRejectsIncompleteDownloadEvidence(t *testing.T) {
	for _, scenario := range []string{"missing-snapshot", "bad-snapshot", "missing-history", "bad-history", "missing-fingerprint", "wrong-phase"} {
		t.Run(scenario, func(t *testing.T) {
			svc, repo, coord, _ := testService()
			key := "coordination/exec.json"
			current := state.Coordination{ActiveRunID: "run", Phase: domain.DownloadsCompleted, LastSequence: 3, OwnerDeadline: testNow.Add(-time.Minute)}
			coord.runs[key] = current
			snapshot := events.Snapshot{SchemaVersion: 1, RunID: "run", ExecutionKey: "exec"}
			if scenario == "bad-snapshot" {
				snapshot.ExecutionKey = "other"
			}
			if scenario != "missing-snapshot" {
				data, _ := json.Marshal(snapshot)
				repo.objects["runs/run/snapshot.json"] = state.Object{Data: data}
			}
			phase := domain.DownloadsCompleted
			if scenario == "wrong-phase" {
				phase = domain.Pulling
			}
			details := json.RawMessage(`{"dataset_fingerprint":"sha256:abc"}`)
			if scenario == "missing-fingerprint" {
				details = json.RawMessage(`{}`)
			}
			transition, _ := json.Marshal(state.Transition{RunID: "run", Phase: phase, Details: details})
			if scenario == "bad-history" {
				transition = []byte("{")
			}
			if scenario != "missing-history" {
				repo.objects["runs/run/history/00000000000000000003.json"] = state.Object{Data: transition}
			}
			ctx, cancel := recoveryContext()
			err := svc.resumeDownloaded(ctx, key, current)
			cancel()
			if err == nil || len(coord.commits) != 0 {
				t.Fatal(scenario, err, coord.commits)
			}
		})
	}
}
