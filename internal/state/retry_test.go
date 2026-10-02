package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/config"
	"github.com/maverickuser/data-fetch-service/internal/domain"
	"github.com/maverickuser/data-fetch-service/internal/events"
)

func pullingRetryFixture(t *testing.T) (*memoryObjects, *Coordinator) {
	t.Helper()
	m := newMemory()
	c := NewCoordinator(New(m), func() time.Time { return m.now })
	admitUntilResolved(t, c, request(t, m, "request", "root"))
	ctx := context.Background()
	lease, err := c.ClaimDispatched(ctx, coordKey, "root", "pull", "worker", m.now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Commit(ctx, coordKey, lease, Transition{RunID: "root", Sequence: 2, Phase: domain.Pulling, At: m.now}); err != nil {
		t.Fatal(err)
	}
	m.now = m.now.Add(2 * time.Minute)
	return m, c
}

func TestAutomaticRetryTransfersWithoutReleasingClaim(t *testing.T) {
	m, c := pullingRetryFixture(t)
	ctx := context.Background()
	child, err := c.ReservePullRetry(ctx, coordKey, "child-1")
	if err != nil || child != "child-1" {
		t.Fatal(child, err)
	}
	current, _, err := c.Load(ctx, coordKey)
	if err != nil || current.ActiveRunID != child || current.Phase != domain.Queued || current.Dispatch == nil || current.Dispatch.State != "pending" || current.Retry != nil || current.LastSequence != 1 {
		t.Fatal(current, err)
	}
	parent, err := c.ReadRun(ctx, "root")
	if err != nil || parent.Phase != domain.Failed || parent.Sequence != 3 || parent.ChildRunID != child || parent.LatestRunID != child || parent.LatestPhase != domain.Queued {
		t.Fatal(parent, err)
	}
	var link AutomaticRetryLink
	if err := json.Unmarshal(m.objects["runs/root/automatic-retry.json"].Data, &link); err != nil || link.ChildRunID != child || link.RootRunID != "root" || link.Index != 1 {
		t.Fatal(link, err)
	}
	var snapshot events.Snapshot
	if err := json.Unmarshal(m.objects["runs/child-1/snapshot.json"].Data, &snapshot); err != nil || snapshot.RunID != child || snapshot.ParentRunID != "root" || snapshot.RetryRootRunID != "root" || snapshot.ExecutionRetryIndex != 1 || snapshot.RequestKey != "request" {
		t.Fatal(snapshot, err)
	}
	if resolution, err := c.RequestResolution(ctx, "request"); err != nil || resolution.RunID != "root" {
		t.Fatal(resolution, err)
	}
	if _, err := c.ReservePullRetry(ctx, coordKey, "other"); !errors.Is(err, ErrConflict) {
		t.Fatal("duplicate child admitted", err)
	}
}

func TestDeliveryRetryWorkerTerminationDoesNotStartSourceRetry(t *testing.T) {
	m, c := pullingRetryFixture(t)
	key := "runs/root/snapshot.json"
	object := m.objects[key]
	var snapshot events.Snapshot
	if err := json.Unmarshal(object.Data, &snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot.DeliveryRetry = &events.DeliveryRetry{SourceRunID: "source"}
	object.Data, _ = json.Marshal(snapshot)
	m.objects[key] = object
	child, err := c.ReservePullRetry(context.Background(), coordKey, "unwanted-child")
	if err != nil || child != "" {
		t.Fatal(child, err)
	}
	view, err := c.ReadRun(context.Background(), "root")
	if err != nil || view.Phase != domain.Failed || view.ChildRunID != "" {
		t.Fatal(view, err)
	}
	if _, exists := m.objects["runs/unwanted-child/snapshot.json"]; exists {
		t.Fatal("source retry was created")
	}
}

func TestAutomaticRetryRepairsInterruptedWrites(t *testing.T) {
	for boundary := 1; boundary <= 7; boundary++ {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d-after-%t", boundary, after), func(t *testing.T) {
				m, c := pullingRetryFixture(t)
				ctx := context.Background()
				c.store = New(&interruptObjects{Objects: m, failAt: boundary, after: after, failure: errors.New("interrupted")})
				_, _ = c.ReservePullRetry(ctx, coordKey, "child-1")
				c.store = New(m)
				current, _, err := c.Load(ctx, coordKey)
				if err != nil {
					t.Fatal(err)
				}
				if current.Retry != nil {
					if err := c.Repair(ctx, coordKey); err != nil {
						t.Fatal(err)
					}
				} else if current.ActiveRunID == "root" {
					if _, err := c.ReservePullRetry(ctx, coordKey, "child-1"); err != nil {
						t.Fatal(err)
					}
				}
				current, _, err = c.Load(ctx, coordKey)
				if err != nil || current.ActiveRunID != "child-1" || current.Retry != nil {
					t.Fatal(boundary, after, current, err)
				}
				if _, err := c.ReadRun(ctx, "root"); err != nil {
					t.Fatal(err)
				}
				if _, err := c.ReadRun(ctx, "child-1"); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestRetryProjectionShowsReservedChildBeforeChildHistory(t *testing.T) {
	m, c := pullingRetryFixture(t)
	c.store = New(&interruptObjects{Objects: m, failAt: 4, failure: errors.New("interrupted")})
	if _, err := c.ReservePullRetry(context.Background(), coordKey, "child"); err == nil {
		t.Fatal("expected interruption")
	}
	c.store = New(m)
	current, _, err := c.Load(context.Background(), coordKey)
	if err != nil || current.Retry == nil {
		t.Fatal(current, err)
	}
	view, err := c.ReadRun(context.Background(), "root")
	if err != nil || view.ChildRunID != "child" || view.LatestRunID != "root" {
		t.Fatal(view, err)
	}
	if err := c.Repair(context.Background(), coordKey); err != nil {
		t.Fatal(err)
	}
	view, err = c.ReadRun(context.Background(), "root")
	if err != nil || view.LatestRunID != "child" {
		t.Fatal(view, err)
	}
}

func TestAutomaticExecutionBudgetStopsAfterThreeChildren(t *testing.T) {
	m, c := pullingRetryFixture(t)
	ctx := context.Background()
	parent := "root"
	for index := 1; index <= 3; index++ {
		child := fmt.Sprintf("child-%d", index)
		actual, err := c.ReservePullRetry(ctx, coordKey, child)
		if err != nil || actual != child {
			t.Fatal(actual, err)
		}
		lease, err := c.ClaimDispatched(ctx, coordKey, child, "pull", fmt.Sprintf("worker-%d", index), m.now.Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Commit(ctx, coordKey, lease, Transition{RunID: child, Sequence: 2, Phase: domain.Pulling, At: m.now}); err != nil {
			t.Fatal(err)
		}
		m.now = m.now.Add(2 * time.Minute)
		var link AutomaticRetryLink
		if err := json.Unmarshal(m.objects["runs/"+parent+"/automatic-retry.json"].Data, &link); err != nil || link.ChildRunID != child || link.Index != index {
			t.Fatal(link, err)
		}
		parent = child
	}
	child, err := c.ReservePullRetry(ctx, coordKey, "forbidden-fourth-child")
	if err != nil || child != "" {
		t.Fatal(child, err)
	}
	current, _, err := c.Load(ctx, coordKey)
	if err != nil || current.ActiveRunID != "" || current.Phase != domain.Failed || current.AcceptedBaseline != nil {
		t.Fatal(current, err)
	}
	view, err := c.ReadRun(ctx, parent)
	if err != nil || view.Phase != domain.Failed {
		t.Fatal(view, err)
	}
	root, err := c.ReadRun(ctx, "root")
	if err != nil || root.LatestRunID != parent || root.LatestPhase != domain.Failed || root.ChildRunID != "child-1" {
		t.Fatal(root, err)
	}
	history, err := c.History(ctx, parent, "", 10)
	if err != nil || string(history.Transitions[len(history.Transitions)-1].Details) == "" {
		t.Fatal(history, err)
	}
	if _, exists := m.objects["runs/forbidden-fourth-child/snapshot.json"]; exists {
		t.Fatal("fifth execution created")
	}
}

func TestRetryProjectionRejectsBrokenLinks(t *testing.T) {
	for _, scenario := range []string{"invalid-json", "wrong-parent", "missing-child", "wrong-index", "wrong-root"} {
		t.Run(scenario, func(t *testing.T) {
			m, c := pullingRetryFixture(t)
			if _, err := c.ReservePullRetry(context.Background(), coordKey, "child"); err != nil {
				t.Fatal(err)
			}
			key := "runs/root/automatic-retry.json"
			link := AutomaticRetryLink{ParentRunID: "root", ChildRunID: "child", RootRunID: "root", Index: 1}
			switch scenario {
			case "invalid-json":
				m.objects[key] = Object{Data: []byte("{")}
			case "wrong-parent":
				link.ParentRunID = "other"
			case "missing-child":
				link.ChildRunID = "absent"
			case "wrong-index":
				link.Index = 2
			case "wrong-root":
				link.RootRunID = "other"
			}
			if scenario != "invalid-json" {
				data, _ := json.Marshal(link)
				m.objects[key] = Object{Data: data}
			}
			if _, err := c.ReadRun(context.Background(), "root"); err == nil {
				t.Fatal("broken retry link accepted")
			}
		})
	}
}

func TestAutomaticRetryRejectsLiveAndDeterministicFailures(t *testing.T) {
	m, c := pullingRetryFixture(t)
	ctx := context.Background()
	m.now = m.now.Add(-2 * time.Minute)
	if _, err := c.ReservePullRetry(ctx, coordKey, "child"); !errors.Is(err, ErrConflict) {
		t.Fatal("live owner stolen", err)
	}
	m.now = m.now.Add(2 * time.Minute)
	current, etag, err := c.Load(ctx, coordKey)
	if err != nil {
		t.Fatal(err)
	}
	current.Phase = domain.Failed
	data, _ := json.Marshal(current)
	if _, err := c.store.CompareAndSwap(ctx, coordKey, data, etag); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ReservePullRetry(ctx, coordKey, "child"); !errors.Is(err, ErrConflict) {
		t.Fatal("business failure retried", err)
	}
}

func TestAutomaticRetryRejectsUnsafeOrMissingParentEvidence(t *testing.T) {
	for _, scenario := range []string{"empty-child", "same-child", "missing-snapshot", "corrupt-snapshot", "wrong-execution-key"} {
		t.Run(scenario, func(t *testing.T) {
			m, c := pullingRetryFixture(t)
			child := "child"
			switch scenario {
			case "empty-child":
				child = ""
			case "same-child":
				child = "root"
			case "missing-snapshot":
				delete(m.objects, "runs/root/snapshot.json")
			case "corrupt-snapshot":
				m.objects["runs/root/snapshot.json"] = Object{Data: []byte("{")}
			case "wrong-execution-key":
				var snapshot events.Snapshot
				if err := json.Unmarshal(m.objects["runs/root/snapshot.json"].Data, &snapshot); err != nil {
					t.Fatal(err)
				}
				snapshot.ExecutionKey = "other"
				data, _ := json.Marshal(snapshot)
				m.objects["runs/root/snapshot.json"] = Object{Data: data}
			}
			if _, err := c.ReservePullRetry(context.Background(), coordKey, child); err == nil {
				t.Fatal("invalid retry accepted", scenario)
			}
			current, _, err := c.Load(context.Background(), coordKey)
			if err != nil || current.ActiveRunID != "root" || current.Retry != nil {
				t.Fatal(current, err)
			}
		})
	}
}

func TestRecordedSourceFailureFinishesWithoutAutomaticChild(t *testing.T) {
	for _, failAt := range []int{0, 1, 2, 3} {
		t.Run(fmt.Sprintf("interrupt-%d", failAt), func(t *testing.T) {
			m, c := pullingRetryFixture(t)
			ctx := context.Background()
			var snapshot events.Snapshot
			if err := json.Unmarshal(m.objects["runs/root/snapshot.json"].Data, &snapshot); err != nil {
				t.Fatal(err)
			}
			snapshot.Jobs = []config.ResolvedJob{{ID: "bse-fgroup"}}
			encoded, _ := json.Marshal(snapshot)
			m.objects["runs/root/snapshot.json"] = Object{Data: encoded, Modified: m.now}
			outcome := []byte(`{"job_id":"bse-fgroup","status":"failed","error_code":"SOURCE_HTTP"}`)
			if err := c.store.Create(ctx, "runs/root/pulls/bse-fgroup/result.json", outcome); err != nil {
				t.Fatal(err)
			}
			if failAt > 0 {
				c.store = New(&interruptObjects{Objects: m, failAt: failAt, failure: errors.New("interrupted")})
			}
			_, _ = c.ReservePullRetry(ctx, coordKey, "forbidden-child")
			c.store = New(m)
			if err := c.Repair(ctx, coordKey); err != nil {
				t.Fatal(err)
			}
			current, _, err := c.Load(ctx, coordKey)
			if err != nil {
				t.Fatal(err)
			}
			if current.Phase == domain.Pulling {
				if _, err := c.ReservePullRetry(ctx, coordKey, "forbidden-child"); err != nil {
					t.Fatal(err)
				}
				current, _, err = c.Load(ctx, coordKey)
			}
			if err != nil || current.Phase != domain.Failed || current.ActiveRunID != "" || current.Retry != nil {
				t.Fatal(current, err)
			}
			if _, ok := m.objects["runs/forbidden-child/snapshot.json"]; ok {
				t.Fatal("source failure created child")
			}
			view, err := c.ReadRun(ctx, "root")
			if err != nil || view.Phase != domain.Failed {
				t.Fatal(view, err)
			}
			history, err := c.History(ctx, "root", "", 10)
			if err != nil || !strings.Contains(string(history.Transitions[len(history.Transitions)-1].Details), "SOURCE_HTTP") {
				t.Fatal(history, err)
			}
		})
	}
}

func TestRecordedPullFailureValidatesJobEvidence(t *testing.T) {
	for _, scenario := range []string{"unsafe-job", "missing", "completed", "canceled", "interrupted", "bad-interruption-code", "corrupt", "wrong-job", "missing-code", "unknown-status"} {
		t.Run(scenario, func(t *testing.T) {
			m := newMemory()
			c := NewCoordinator(New(m), func() time.Time { return m.now })
			jobID := "source"
			if scenario == "unsafe-job" {
				jobID = "../source"
			}
			snapshot := events.Snapshot{RunID: "root", Jobs: []config.ResolvedJob{{ID: jobID}}}
			key := "runs/root/pulls/source/result.json"
			status := "completed"
			switch scenario {
			case "canceled":
				status = "canceled"
			case "interrupted", "bad-interruption-code":
				status = "interrupted"
			case "missing-code":
				status = "failed"
			case "unknown-status":
				status = "unknown"
			}
			outcome, _ := json.Marshal(map[string]string{"job_id": "source", "status": status})
			if scenario == "interrupted" {
				outcome = []byte(`{"job_id":"source","status":"interrupted","error_code":"GROUP_BUDGET_EXCEEDED"}`)
			}
			if scenario == "corrupt" {
				outcome = []byte("{")
			}
			if scenario == "wrong-job" {
				outcome = []byte(`{"job_id":"other","status":"completed"}`)
			}
			if scenario != "missing" && scenario != "unsafe-job" {
				if err := c.store.Create(context.Background(), key, outcome); err != nil {
					t.Fatal(err)
				}
			}
			code, err := c.recordedPullFailure(context.Background(), snapshot)
			bad := scenario == "unsafe-job" || scenario == "corrupt" || scenario == "wrong-job" || scenario == "missing-code" || scenario == "unknown-status" || scenario == "bad-interruption-code"
			if bad && !errors.Is(err, ErrIntegrity) || !bad && (err != nil || code != "") {
				t.Fatal(scenario, code, err)
			}
		})
	}
}

func TestExpiredDeliveryReclaimFencesOldInvocation(t *testing.T) {
	m := newMemory()
	c := NewCoordinator(New(m), func() time.Time { return m.now })
	ctx := context.Background()
	key := coordKey
	admitUntilResolved(t, c, request(t, m, "request", "run"))
	pullLease, err := c.ClaimDispatched(ctx, key, "run", "pull", "pull-worker", m.now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []domain.State{domain.Pulling, domain.DownloadsCompleted, domain.DeliveryPending} {
		current, _, err := c.Load(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Commit(ctx, key, pullLease, Transition{RunID: "run", Sequence: current.LastSequence + 1, Phase: phase, At: m.now}); err != nil {
			t.Fatal(err)
		}
	}
	deliveryLease, err := c.ClaimDispatched(ctx, key, "run", "delivery", "old-delivery", m.now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	current, _, err := c.Load(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Commit(ctx, key, deliveryLease, Transition{RunID: "run", Sequence: current.LastSequence + 1, Phase: domain.Delivering, At: m.now}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ClaimExpiredDelivery(ctx, key, "run", "new", m.now.Add(time.Minute)); !errors.Is(err, ErrConflict) {
		t.Fatal("live owner replaced", err)
	}
	m.now = m.now.Add(2 * time.Minute)
	reclaimed, err := c.ClaimExpiredDelivery(ctx, key, "run", "new", m.now.Add(time.Minute))
	if err != nil || reclaimed.Generation <= deliveryLease.Generation {
		t.Fatal(reclaimed, err)
	}
	if err := c.Commit(ctx, key, deliveryLease, Transition{RunID: "run", Sequence: 6, Phase: domain.Failed, At: m.now}); !errors.Is(err, ErrConflict) {
		t.Fatal("old owner committed", err)
	}
}

func TestExpiredPreworkClaimCanBeRepublished(t *testing.T) {
	m := newMemory()
	c := NewCoordinator(New(m), func() time.Time { return m.now })
	ctx := context.Background()
	admitUntilResolved(t, c, request(t, m, "request", "run"))
	lease, err := c.ClaimDispatched(ctx, coordKey, "run", "pull", "old", m.now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.RequeueExpiredDispatch(ctx, coordKey); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	m.now = m.now.Add(2 * time.Minute)
	if err := c.RequeueExpiredDispatch(ctx, coordKey); err != nil {
		t.Fatal(err)
	}
	current, _, err := c.Load(ctx, coordKey)
	if err != nil || current.OwnerToken != "" || current.Dispatch.State != "pending" || current.Generation <= lease.Generation {
		t.Fatal(current, err)
	}
	if _, err := c.ClaimDispatched(ctx, coordKey, "run", "pull", "new", m.now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
}

func TestExpiredDownloadsDecisionClaimFencesOldOwner(t *testing.T) {
	m := newMemory()
	c := NewCoordinator(New(m), func() time.Time { return m.now })
	ctx := context.Background()
	admitUntilResolved(t, c, request(t, m, "request", "run"))
	old, err := c.ClaimDispatched(ctx, coordKey, "run", "pull", "old", m.now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []domain.State{domain.Pulling, domain.DownloadsCompleted} {
		current, _, err := c.Load(ctx, coordKey)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Commit(ctx, coordKey, old, Transition{RunID: "run", Sequence: current.LastSequence + 1, Phase: phase, At: m.now}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.ClaimExpiredDownloadsCompleted(ctx, coordKey, "new", m.now.Add(time.Minute)); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	m.now = m.now.Add(2 * time.Minute)
	lease, err := c.ClaimExpiredDownloadsCompleted(ctx, coordKey, "new", m.now.Add(time.Minute))
	if err != nil || lease.Generation <= old.Generation {
		t.Fatal(lease, err)
	}
	if err := c.Commit(ctx, coordKey, old, Transition{RunID: "run", Sequence: 4, Phase: domain.DeliveryPending, At: m.now}); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if err := c.Commit(ctx, coordKey, lease, Transition{RunID: "run", Sequence: 4, Phase: domain.DeliveryPending, At: m.now}); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryClaimsRejectInvalidOrDeletedCoordination(t *testing.T) {
	m := newMemory()
	c := NewCoordinator(New(m), func() time.Time { return m.now })
	ctx := context.Background()
	missing := "coordination/deleted.json"
	if _, err := c.ClaimDispatched(ctx, missing, "run", "pull", "", m.now.Add(time.Minute)); err == nil {
		t.Fatal("empty claim token accepted")
	}
	if _, err := c.ClaimExpiredDelivery(ctx, missing, "run", "", m.now.Add(time.Minute)); err == nil {
		t.Fatal("empty delivery token accepted")
	}
	if _, err := c.ClaimExpiredDownloadsCompleted(ctx, missing, "", m.now.Add(time.Minute)); err == nil {
		t.Fatal("empty download token accepted")
	}
	if _, err := c.ClaimDispatched(ctx, missing, "run", "pull", "token", m.now.Add(time.Minute)); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := c.ClaimExpiredDelivery(ctx, missing, "run", "token", m.now.Add(time.Minute)); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := c.ClaimExpiredDownloadsCompleted(ctx, missing, "token", m.now.Add(time.Minute)); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if err := c.RequeueExpiredDispatch(ctx, missing); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := c.ReservePullRetry(ctx, missing, "child"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if err := c.PublishPending(ctx, missing, nil); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}
