package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/domain"
	"github.com/maverickuser/data-fetch-service/internal/events"
)

func request(t *testing.T, m *memoryObjects, id, run string) RequestIntent {
	t.Helper()
	snapshot := events.Snapshot{SchemaVersion: 1, RunID: run, RequestKey: id, ExecutionKey: "execution", PayloadHash: "payload-" + id, Event: events.Normalized{EventType: "daily-bhavcopy"}}
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewRequest(data, m.now)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func admitUntilResolved(t *testing.T, c *Coordinator, r RequestIntent) Resolution {
	t.Helper()
	for range 200 {
		result, err := c.Admit(context.Background(), r)
		if err == nil {
			return result
		}
		if !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
	}
	t.Fatal("admission did not converge")
	return Resolution{}
}

func TestAdmissionRepairsEveryWriteBoundary(t *testing.T) {
	for _, after := range []bool{false, true} {
		for boundary := 1; boundary <= 7; boundary++ {
			t.Run(fmt.Sprintf("%d/after=%v", boundary, after), func(t *testing.T) {
				m := newMemory()
				c := NewCoordinator(New(m), func() time.Time { return m.now })
				r := request(t, m, "request", "candidate")
				f := &interruptObjects{Objects: m, failAt: boundary, after: after, failure: errors.New("interrupted")}
				c.store = New(f)
				_, _ = c.Admit(context.Background(), r)
				c.store = New(m)
				// The retry has a different generated ID but must retain the first persisted one.
				retry := request(t, m, "request", "retry-candidate")
				receipt := admitUntilResolved(t, c, retry)
				if boundary != 1 || after {
					if receipt.RunID != "candidate" {
						t.Fatal(receipt)
					}
				}
				// A receipt may precede finalization; reconciliation must finish its pinned intent.
				if err := c.Repair(context.Background(), coordKey); err != nil {
					t.Fatal(err)
				}
				view, err := c.ReadRun(context.Background(), receipt.RunID)
				if err != nil || view.Phase != domain.Queued || view.Sequence != 1 {
					t.Fatal(view, err)
				}
				current, _, err := c.Load(context.Background(), coordKey)
				if err != nil || current.Admission != nil || current.Dispatch == nil || current.Dispatch.State != "pending" {
					t.Fatal(current, err)
				}
				history, err := c.History(context.Background(), receipt.RunID, "", 10)
				if err != nil || len(history.Transitions) != 1 {
					t.Fatal(history, err)
				}
			})
		}
	}
}

func TestConcurrentRequestsConvergeOnOneRun(t *testing.T) {
	for _, sameRequest := range []bool{true, false} {
		m := newMemory()
		c := NewCoordinator(New(m), func() time.Time { return m.now })
		var wg sync.WaitGroup
		results := make(chan Resolution, 20)
		for i := range 20 {
			id := fmt.Sprintf("request-%d", i)
			if sameRequest {
				id = "request"
			}
			r := request(t, m, id, fmt.Sprintf("candidate-%d", i))
			wg.Go(func() { results <- admitUntilResolved(t, c, r) })
		}
		wg.Wait()
		close(results)
		chosen := ""
		for receipt := range results {
			if chosen == "" {
				chosen = receipt.RunID
			}
			if receipt.RunID != chosen {
				t.Fatal("admission diverged", chosen, receipt)
			}
		}
		page, err := c.store.Scan(context.Background(), "runs/", "", 100)
		if err != nil || len(page.Keys) != 2 {
			t.Fatal("extra candidate run materialized", page, err)
		}
	}
}

func TestJoinDecisionBlocksTerminalRelease(t *testing.T) {
	m := newMemory()
	c := NewCoordinator(New(m), func() time.Time { return m.now })
	ctx := context.Background()
	original := request(t, m, "one", "run")
	admitUntilResolved(t, c, original)
	lease, err := c.ClaimDispatched(ctx, coordKey, "run", "pull", "worker", m.now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	joining := request(t, m, "two", "unused")
	fault := &interruptObjects{Objects: m, failAt: 3, failure: errors.New("stop before resolution")}
	c.store = New(fault)
	if _, err := c.Admit(ctx, joining); err == nil {
		t.Fatal("expected interrupted join")
	}
	c.store = New(m)
	failed := Transition{RunID: "run", Sequence: 2, Phase: domain.Failed, At: m.now}
	if err := c.Commit(ctx, coordKey, lease, failed); !errors.Is(err, ErrConflict) {
		t.Fatal("released pending admission", err)
	}
	if _, err := c.Claim(ctx, coordKey, "run", "worker", m.now.Add(time.Minute)); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if err := c.Repair(ctx, coordKey); err != nil {
		t.Fatal(err)
	}
	receipt, err := c.RequestResolution(ctx, "two")
	if err != nil || !receipt.Joined || receipt.RunID != "run" {
		t.Fatal(receipt, err)
	}
	if err := c.Commit(ctx, coordKey, lease, failed); err != nil {
		t.Fatal(err)
	}
	replay := admitUntilResolved(t, c, joining)
	if replay != receipt {
		t.Fatal("resolution changed after terminal", replay, receipt)
	}
	next := admitUntilResolved(t, c, request(t, m, "three", "next"))
	if next.RunID != "next" || next.Joined {
		t.Fatal(next)
	}
}

func TestRequestValidationConflictsAndExpiry(t *testing.T) {
	m := newMemory()
	c := NewCoordinator(New(m), func() time.Time { return m.now })
	ctx := context.Background()
	r := request(t, m, "one", "run")
	admitUntilResolved(t, c, r)
	conflict := r
	conflict.PayloadHash = "changed"
	var snapshot events.Snapshot
	if err := json.Unmarshal(conflict.Snapshot, &snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot.PayloadHash = "changed"
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	conflict.Snapshot = data
	if _, err := c.Admit(ctx, conflict); !errors.Is(err, ErrIntegrity) {
		t.Fatal(err)
	}
	for _, mutate := range []func(*RequestIntent){func(r *RequestIntent) { r.CandidateRunID = "../bad" }, func(r *RequestIntent) { r.Snapshot = json.RawMessage(`{`) }, func(r *RequestIntent) { r.PayloadHash = "mismatch" }, func(r *RequestIntent) { r.CreatedAt = time.Time{} }} {
		copy := r
		mutate(&copy)
		if _, err := c.EnsureRequest(ctx, copy); err == nil {
			t.Fatal("invalid request accepted")
		}
	}
	for _, data := range []string{`{`, `{"schema_version":2}`} {
		if _, err := NewRequest([]byte(data), m.now); err == nil {
			t.Fatal(data)
		}
	}
	if _, err := c.RequestResolution(ctx, "../bad"); err == nil {
		t.Fatal("invalid path")
	}
	m.now = m.now.Add(30 * 24 * time.Hour)
	if _, err := c.Admit(ctx, r); !errors.Is(err, ErrExpired) {
		t.Fatal(err)
	}
}

type readHook struct {
	Objects
	key    string
	before func()
	fired  bool
}

func (h *readHook) Get(ctx context.Context, key string) (Object, error) {
	if key == h.key && !h.fired {
		h.fired = true
		h.before()
	}
	return h.Objects.Get(ctx, key)
}

func TestCompletedResolutionWinsAgainstStaleAdmissionRead(t *testing.T) {
	m := newMemory()
	ctx := context.Background()
	other := NewCoordinator(New(m), func() time.Time { return m.now })
	r := request(t, m, "same", "first")
	hook := &readHook{Objects: m, key: coordKey, before: func() {
		receipt := admitUntilResolved(t, other, request(t, m, "same", "losing-candidate"))
		if receipt.RunID != "first" {
			t.Fatal(receipt)
		}
		lease, err := other.ClaimDispatched(ctx, coordKey, "first", "pull", "worker", m.now.Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		if err := other.Commit(ctx, coordKey, lease, Transition{RunID: "first", Sequence: 2, Phase: domain.Failed, At: m.now}); err != nil {
			t.Fatal(err)
		}
	}}
	c := NewCoordinator(New(hook), func() time.Time { return m.now })
	receipt, err := c.Admit(ctx, r)
	if err != nil || receipt.RunID != "first" || receipt.Joined {
		t.Fatal(receipt, err)
	}
	current, _, err := c.Load(ctx, coordKey)
	if err != nil || current.ActiveRunID != "" || current.Admission != nil {
		t.Fatal("stale admission resurrected terminal run", current, err)
	}
}

type failRead struct {
	Objects
	count, failAt int
	failure       error
}

func (f *failRead) Get(ctx context.Context, key string) (Object, error) {
	f.count++
	if f.count == f.failAt {
		return Object{}, f.failure
	}
	return f.Objects.Get(ctx, key)
}

func TestAdmissionReadFailuresNeverBecomeNotFound(t *testing.T) {
	for boundary := 1; boundary <= 5; boundary++ {
		m := newMemory()
		failure := errors.New("read unavailable")
		f := &failRead{Objects: m, failAt: boundary, failure: failure}
		c := NewCoordinator(New(f), func() time.Time { return m.now })
		r := request(t, m, "one", "run")
		if _, err := c.Admit(context.Background(), r); !errors.Is(err, failure) {
			t.Fatal(boundary, err)
		}
		c.store = New(m)
		receipt := admitUntilResolved(t, c, r)
		if receipt.RunID != "run" {
			t.Fatal(receipt)
		}
	}
}

func TestReadRequestIsReadOnlyAndRejectsCorruption(t *testing.T) {
	m := newMemory()
	c := NewCoordinator(New(m), func() time.Time { return m.now })
	ctx := context.Background()
	r := request(t, m, "one", "run")
	if _, err := c.ReadRequest(ctx, "../bad"); err == nil {
		t.Fatal("invalid request path")
	}
	if _, err := c.ReadRequest(ctx, "one"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := c.EnsureRequest(ctx, r); err != nil {
		t.Fatal(err)
	}
	version := m.version
	actual, err := c.ReadRequest(ctx, "one")
	if err != nil || actual.CandidateRunID != "run" || m.version != version {
		t.Fatal(actual, err)
	}
	m.objects["requests/one/intent.json"] = Object{Data: []byte("{"), Modified: m.now}
	if _, err := c.ReadRequest(ctx, "one"); err == nil {
		t.Fatal("invalid JSON accepted")
	}
	r.RequestKey = "other"
	replaceRecord(t, m, "requests/one/intent.json", r)
	if _, err := c.ReadRequest(ctx, "one"); !errors.Is(err, ErrIntegrity) {
		t.Fatal(err)
	}
}
