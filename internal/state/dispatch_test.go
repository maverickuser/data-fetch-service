package state

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/domain"
)

type queueFake struct {
	messages []Dispatch
	err      error
	after    func()
}

func (q *queueFake) Publish(_ context.Context, queue, run string) error {
	q.messages = append(q.messages, Dispatch{RunID: run, Queue: queue})
	if q.after != nil {
		q.after()
	}
	return q.err
}

func TestDispatchLostResponseAndDuplicateClaim(t *testing.T) {
	m := newMemory()
	c := NewCoordinator(New(m), func() time.Time { return m.now })
	ctx := context.Background()
	admitUntilResolved(t, c, request(t, m, "one", "run"))
	q := &queueFake{err: errors.New("reply lost")}
	if err := c.PublishPending(ctx, coordKey, q); !errors.Is(err, q.err) {
		t.Fatal(err)
	}
	q.err = nil
	if err := c.PublishPending(ctx, coordKey, q); err != nil {
		t.Fatal(err)
	}
	if err := c.PublishPending(ctx, coordKey, q); err != nil || len(q.messages) != 2 {
		t.Fatal(q.messages, err)
	}
	lease, err := c.ClaimDispatched(ctx, coordKey, "run", "pull", "worker", m.now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ClaimDispatched(ctx, coordKey, "run", "pull", "duplicate", m.now.Add(time.Minute)); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	for i, phase := range []domain.State{domain.Pulling, domain.DownloadsCompleted, domain.DeliveryPending} {
		if err := c.Commit(ctx, coordKey, lease, Transition{RunID: "run", Sequence: uint64(i + 2), Phase: phase, At: m.now}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.ClaimDispatched(ctx, coordKey, "run", "pull", "late-pull", m.now.Add(time.Minute)); !errors.Is(err, ErrConflict) {
		t.Fatal("late pull stole delivery", err)
	}
	if err := c.PublishPending(ctx, coordKey, q); err != nil || q.messages[2].Queue != "delivery" {
		t.Fatal(q.messages, err)
	}
	delivery, err := c.ClaimDispatched(ctx, coordKey, "run", "delivery", "delivery", m.now.Add(time.Minute))
	if err != nil || delivery.Generation <= lease.Generation {
		t.Fatal(delivery, err)
	}
}

func TestDispatchConsumerWinsFinalizationRace(t *testing.T) {
	m := newMemory()
	c := NewCoordinator(New(m), func() time.Time { return m.now })
	ctx := context.Background()
	admitUntilResolved(t, c, request(t, m, "one", "run"))
	q := &queueFake{after: func() {
		if _, err := c.ClaimDispatched(ctx, coordKey, "run", "pull", "worker", m.now.Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
	}}
	if err := c.PublishPending(ctx, coordKey, q); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if err := c.PublishPending(ctx, coordKey, q); err != nil || len(q.messages) != 1 {
		t.Fatal(err, q.messages)
	}
	current, _, err := c.Load(ctx, coordKey)
	if err != nil || current.Dispatch.State != "claimed" || current.OwnerToken != "worker" {
		t.Fatal(current, err)
	}
}

func TestDispatchFinalCASLostResponse(t *testing.T) {
	for _, after := range []bool{false, true} {
		m := newMemory()
		c := NewCoordinator(New(m), func() time.Time { return m.now })
		ctx := context.Background()
		admitUntilResolved(t, c, request(t, m, "one", "run"))
		c.store = New(&interruptObjects{Objects: m, failAt: 1, after: after, failure: errors.New("interrupted")})
		q := &queueFake{}
		if err := c.PublishPending(ctx, coordKey, q); err == nil {
			t.Fatal("expected finalization failure")
		}
		c.store = New(m)
		if err := c.PublishPending(ctx, coordKey, q); err != nil {
			t.Fatal(err)
		}
		expected := 2
		if after {
			expected = 1
		}
		if len(q.messages) != expected {
			t.Fatal(q.messages)
		}
	}
}

func TestClaimRecoversPersistedResponseLoss(t *testing.T) {
	m := newMemory()
	c := NewCoordinator(New(m), func() time.Time { return m.now })
	ctx := context.Background()
	admitUntilResolved(t, c, request(t, m, "one", "run"))
	if _, err := c.Claim(ctx, coordKey, "run", "bypass", m.now.Add(time.Minute)); !errors.Is(err, ErrConflict) {
		t.Fatal("generic claim bypassed dispatch", err)
	}
	c.store = New(&interruptObjects{Objects: m, failAt: 1, after: true, failure: errors.New("lost response")})
	if _, err := c.ClaimDispatched(ctx, coordKey, "run", "pull", "worker", m.now.Add(time.Minute)); err == nil {
		t.Fatal("expected injected failure")
	}
	c.store = New(m)
	lease, err := c.ClaimDispatched(ctx, coordKey, "run", "pull", "worker", m.now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	current, _, err := c.Load(ctx, coordKey)
	if err != nil || current.Generation != lease.Generation || current.OwnerToken != lease.Token {
		t.Fatal(current, lease, err)
	}
	if _, err := c.ClaimDispatched(ctx, coordKey, "run", "pull", "worker", m.now.Add(2*time.Minute)); !errors.Is(err, ErrConflict) {
		t.Fatal("lease extended", err)
	}
}
