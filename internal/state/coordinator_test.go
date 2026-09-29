package state

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/domain"
)

const coordKey = "coordination/execution.json"

func seededCoordinator(t *testing.T) (*Coordinator, *memoryObjects) {
	t.Helper()
	m := newMemory()
	c := NewCoordinator(New(m), func() time.Time { return m.now })
	initial := Coordination{SchemaVersion: 1, Generation: 3, ActiveRunID: "run", Phase: domain.Queued, LastSequence: 1, AcceptedBaseline: &Baseline{RunID: "previous", Fingerprint: "hash", AcceptanceKey: "acceptance/k/previous.json"}}
	if err := c.save(context.Background(), coordKey, initial, ""); err != nil {
		t.Fatal(err)
	}
	return c, m
}

func TestConcurrentRepairConvergesWithoutChangingOwner(t *testing.T) {
	c, m := seededCoordinator(t)
	ctx := context.Background()
	lease := claim(t, c, m)
	current, etag, err := c.Load(ctx, coordKey)
	if err != nil {
		t.Fatal(err)
	}
	current.Pending = &Transition{RunID: "run", Sequence: 2, Phase: domain.Pulling, At: m.now}
	if err := c.save(ctx, coordKey, current, etag); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			if err := c.Repair(ctx, coordKey); err != nil && !errors.Is(err, ErrConflict) {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	current, _, err = c.Load(ctx, coordKey)
	if err != nil {
		t.Fatal(err)
	}
	if current.LastSequence != 2 || current.Pending != nil || current.Generation != lease.Generation || current.OwnerToken != lease.Token || current.AcceptedBaseline.Fingerprint != "hash" {
		t.Fatal(current)
	}
	page, err := c.store.Scan(ctx, "runs/run/history/", "", 100)
	if err != nil || len(page.Keys) != 1 {
		t.Fatal(page, err)
	}
}

func TestConcurrentCommitReservesOnlyOneSequence(t *testing.T) {
	c, m := seededCoordinator(t)
	ctx := context.Background()
	lease := claim(t, c, m)
	var wg sync.WaitGroup
	results := make(chan error, 20)
	for range 20 {
		wg.Go(func() {
			results <- c.Commit(ctx, coordKey, lease, Transition{RunID: "run", Sequence: 2, Phase: domain.Pulling, At: m.now})
		})
	}
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatal("multiple authoritative committers", successes)
	}
	page, err := c.store.Scan(ctx, "runs/run/history/", "", 100)
	if err != nil || len(page.Keys) != 1 {
		t.Fatal(page, err)
	}
}

func claim(t *testing.T, c *Coordinator, m *memoryObjects) Lease {
	t.Helper()
	lease, err := c.Claim(context.Background(), coordKey, "run", "invocation", m.now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	return lease
}

func TestFencedTransitionsAndTerminalBaseline(t *testing.T) {
	c, m := seededCoordinator(t)
	ctx := context.Background()
	lease := claim(t, c, m)
	duplicate, err := c.Claim(ctx, coordKey, "run", "invocation", m.now.Add(time.Minute))
	if err != nil || duplicate != lease {
		t.Fatal(duplicate, err)
	}
	if _, err := c.Claim(ctx, coordKey, "run", "another", m.now.Add(time.Minute)); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	for index, phase := range []domain.State{domain.Pulling, domain.DownloadsCompleted, domain.DeliveryPending, domain.Delivering, domain.Completed} {
		if phase == domain.Delivering {
			lease = claim(t, c, m)
		}
		next := Transition{RunID: "run", Sequence: uint64(index + 2), Phase: phase, At: m.now}
		if phase == domain.Completed {
			next.Acceptance = &Acceptance{RunID: "run", Fingerprint: "new-hash", DatasetURN: "urn:test", ConfigRevision: "revision", AcceptedAt: m.now}
		}
		if err := c.Commit(ctx, coordKey, lease, next); err != nil {
			t.Fatal(err)
		}
	}
	current, _, err := c.Load(ctx, coordKey)
	if err != nil {
		t.Fatal(err)
	}
	if current.ActiveRunID != "" || current.OwnerToken != "" || current.Pending != nil || current.Generation != lease.Generation || current.AcceptedBaseline.Fingerprint != "new-hash" {
		t.Fatalf("terminal release lost retained state %+v", current)
	}
	if _, err := c.store.Read(ctx, current.AcceptedBaseline.AcceptanceKey, m.now.Add(365*24*time.Hour)); err != nil {
		t.Fatal("accepted evidence expired", err)
	}
	if err := c.Commit(ctx, coordKey, lease, Transition{RunID: "run", Sequence: 7, Phase: domain.Pulling, At: m.now}); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if err := c.Repair(ctx, coordKey); err != nil {
		t.Fatal(err)
	}
}

// interruptObjects injects a failure at a write boundary, optionally after persistence.
type interruptObjects struct {
	Objects
	calls, failAt int
	after         bool
	failure       error
}

func (f *interruptObjects) Put(ctx context.Context, key string, data []byte, match string) (string, error) {
	f.calls++
	if f.calls == f.failAt && !f.after {
		return "", f.failure
	}
	etag, err := f.Objects.Put(ctx, key, data, match)
	if f.calls == f.failAt && f.after {
		return "", f.failure
	}
	return etag, err
}

func TestRepairAcrossEveryTransitionWriteBoundary(t *testing.T) {
	for _, after := range []bool{false, true} {
		for boundary := 1; boundary <= 3; boundary++ {
			c, m := seededCoordinator(t)
			lease := claim(t, c, m)
			ctx := context.Background()
			fault := &interruptObjects{Objects: m, failAt: boundary, after: after, failure: errors.New("interrupted")}
			c.store = New(fault)
			next := Transition{RunID: "run", Sequence: 2, Phase: domain.Pulling, At: m.now}
			err := c.Commit(ctx, coordKey, lease, next)
			if err == nil && !(boundary == 2 && after) {
				t.Fatalf("missing injected failure boundary %d after=%v", boundary, after)
			}
			c.store = New(m)
			current, _, err := c.Load(ctx, coordKey)
			if err != nil {
				t.Fatal(err)
			}
			if current.Pending == nil && current.LastSequence == 1 {
				if err := c.Commit(ctx, coordKey, lease, next); err != nil {
					t.Fatal(err)
				}
			} else if err := c.Repair(ctx, coordKey); err != nil {
				t.Fatal(err)
			}
			current, _, err = c.Load(ctx, coordKey)
			if err != nil || current.LastSequence != 2 || current.Phase != domain.Pulling || current.Pending != nil {
				t.Fatal(current, err)
			}
			page, err := c.store.Scan(ctx, "runs/run/history/", "", 100)
			if err != nil || len(page.Keys) != 1 {
				t.Fatal(page, err)
			}
		}
	}
}

func TestLeaseAndPendingGuards(t *testing.T) {
	c, m := seededCoordinator(t)
	ctx := context.Background()
	for _, args := range []struct {
		run, token string
		deadline   time.Time
	}{{"run", "", m.now.Add(time.Minute)}, {"run", "token", m.now}, {"other", "token", m.now.Add(time.Minute)}} {
		if _, err := c.Claim(ctx, coordKey, args.run, args.token, args.deadline); err == nil {
			t.Fatal(args)
		}
	}
	lease := claim(t, c, m)
	next := Transition{RunID: "run", Sequence: 2, Phase: domain.Pulling, At: m.now}
	for _, bad := range []Lease{{"run", lease.Generation - 1, lease.Token}, {"run", lease.Generation, "stale"}, {"other", lease.Generation, lease.Token}} {
		if err := c.Commit(ctx, coordKey, bad, next); !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
	}
	bad := next
	bad.Phase = domain.Completed
	if err := c.Commit(ctx, coordKey, lease, bad); err == nil {
		t.Fatal("invalid phase jump")
	}
	bad = next
	bad.Details = json.RawMessage(`{`)
	if err := c.Commit(ctx, coordKey, lease, bad); err == nil {
		t.Fatal("invalid JSON persisted")
	}
	current, etag, err := c.Load(ctx, coordKey)
	if err != nil {
		t.Fatal(err)
	}
	current.Pending = &next
	if err := c.save(ctx, coordKey, current, etag); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Claim(ctx, coordKey, "run", "next", m.now.Add(time.Hour)); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if err := c.Commit(ctx, coordKey, lease, next); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	m.now = m.now.Add(time.Hour)
	if err := c.Commit(ctx, coordKey, lease, next); !errors.Is(err, ErrConflict) {
		t.Fatal("expired owner committed", err)
	}
	// Reserved intent remains authoritative and recoverable after owner expiry.
	if err := c.Repair(ctx, coordKey); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Claim(ctx, coordKey, "run", "replacement", m.now.Add(time.Minute)); !errors.Is(err, ErrConflict) {
		t.Fatal("expired owner stolen", err)
	}
}

func TestCorruptAndUnavailableCoordination(t *testing.T) {
	ctx := context.Background()
	for _, key := range []string{"runs/run/snapshot.json", "coordination/a/b.json", "coordination/"} {
		c, m := seededCoordinator(t)
		if err := c.Repair(ctx, key); err == nil {
			t.Fatal("invalid coordination location accepted")
		}
		if len(m.objects) != 1 {
			t.Fatal("repair wrote outside coordination")
		}
	}
	for _, raw := range []string{"{", `{"schema_version":2}`, `{"schema_version":1,"active_run_id":"../escape","pending_transition":{"run_id":"../escape","sequence":1}}`} {
		m := newMemory()
		m.objects[coordKey] = Object{Data: []byte(raw), ETag: "one", Modified: m.now}
		c := NewCoordinator(New(m), func() time.Time { return m.now })
		if err := c.Repair(ctx, coordKey); err == nil {
			t.Fatal(raw)
		}
	}
	c, m := seededCoordinator(t)
	m.getErr = errors.New("unavailable")
	if _, err := c.Claim(ctx, coordKey, "run", "token", m.now.Add(time.Minute)); !errors.Is(err, m.getErr) {
		t.Fatal(err)
	}
	if err := c.Commit(ctx, coordKey, Lease{}, Transition{}); !errors.Is(err, m.getErr) {
		t.Fatal(err)
	}
	m.getErr = nil
	m.putErr = errors.New("write unavailable")
	if _, err := c.Claim(ctx, coordKey, "run", "token", m.now.Add(time.Minute)); !errors.Is(err, m.putErr) {
		t.Fatal(err)
	}
	if allowedTransition(domain.Completed, domain.Failed) || allowedTransition("invalid", domain.Completed) || !allowedTransition(domain.Pulling, domain.Failed) || !allowedTransition(domain.DownloadsCompleted, domain.SkippedUnchanged) {
		t.Fatal("transition graph")
	}
}

func TestAcceptanceRepairBeforeHistoryAndBaseline(t *testing.T) {
	for _, after := range []bool{false, true} {
		for boundary := 1; boundary <= 4; boundary++ {
			c, m := seededCoordinator(t)
			ctx := context.Background()
			current, etag, err := c.Load(ctx, coordKey)
			if err != nil {
				t.Fatal(err)
			}
			current.Phase = domain.Delivering
			if err := c.save(ctx, coordKey, current, etag); err != nil {
				t.Fatal(err)
			}
			lease := claim(t, c, m)
			next := Transition{RunID: "run", Sequence: 2, Phase: domain.Completed, At: m.now, Acceptance: &Acceptance{RunID: "run", Fingerprint: "accepted", DatasetURN: "urn:test", ConfigRevision: "revision", AcceptedAt: m.now}}
			fault := &interruptObjects{Objects: m, failAt: boundary, after: after, failure: errors.New("interrupted")}
			c.store = New(fault)
			// Successful verification can absorb a lost reply from an immutable write.
			_ = c.Commit(ctx, coordKey, lease, next)
			c.store = New(m)
			current, _, err = c.Load(ctx, coordKey)
			if err != nil {
				t.Fatal(err)
			}
			if current.Phase != domain.Completed && current.AcceptedBaseline.Fingerprint != "hash" {
				t.Fatal("baseline advanced before finalization")
			}
			if current.Pending == nil && current.Phase != domain.Completed {
				if err := c.Commit(ctx, coordKey, lease, next); err != nil {
					t.Fatal(err)
				}
			} else if err := c.Repair(ctx, coordKey); err != nil {
				t.Fatal(err)
			}
			current, _, err = c.Load(ctx, coordKey)
			if err != nil || current.Phase != domain.Completed || current.AcceptedBaseline.Fingerprint != "accepted" {
				t.Fatal(current, err)
			}
			if _, err := c.store.Read(ctx, current.AcceptedBaseline.AcceptanceKey, m.now); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestAcceptanceAndFailedBaselineGuards(t *testing.T) {
	c, m := seededCoordinator(t)
	ctx := context.Background()
	lease := claim(t, c, m)
	next := Transition{RunID: "run", Sequence: 2, Phase: domain.Failed, At: m.now, Acceptance: &Acceptance{RunID: "run"}}
	if err := c.Commit(ctx, coordKey, lease, next); err == nil {
		t.Fatal("failed run acceptance allowed")
	}
	next.Acceptance = nil
	if err := c.Commit(ctx, coordKey, lease, next); err != nil {
		t.Fatal(err)
	}
	current, _, err := c.Load(ctx, coordKey)
	if err != nil || current.AcceptedBaseline.Fingerprint != "hash" {
		t.Fatal(current, err)
	}
	if err := validateAcceptance(Transition{Phase: domain.Completed}); err == nil {
		t.Fatal("completion without receipt")
	}
}
