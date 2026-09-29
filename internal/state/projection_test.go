package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/domain"
)

func TestProjectionUsesHistoryNotReservedIntent(t *testing.T) {
	for _, historyExists := range []bool{false, true} {
		m := newMemory()
		c := NewCoordinator(New(m), func() time.Time { return m.now })
		ctx := context.Background()
		admitUntilResolved(t, c, request(t, m, "one", "run"))
		lease, err := c.ClaimDispatched(ctx, coordKey, "run", "pull", "worker", m.now.Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		boundary := 2
		if historyExists {
			boundary = 3
		}
		c.store = New(&interruptObjects{Objects: m, failAt: boundary, failure: errors.New("interrupted")})
		if err := c.Commit(ctx, coordKey, lease, Transition{RunID: "run", Sequence: 2, Phase: domain.Pulling, At: m.now}); err == nil {
			t.Fatal("expected interrupted transition")
		}
		c.store = New(m)
		version := m.version
		view, err := c.ReadRun(ctx, "run")
		if err != nil {
			t.Fatal(err)
		}
		expected := domain.Queued
		if historyExists {
			expected = domain.Pulling
		}
		if view.Phase != expected {
			t.Fatal(view)
		}
		if m.version != version {
			t.Fatal("read performed recovery mutation")
		}
		current, _, err := c.Load(ctx, coordKey)
		if err != nil || current.Pending == nil || current.LastSequence != 1 {
			t.Fatal(current, err)
		}
		if historyExists {
			first, err := c.History(ctx, "run", "", 1)
			if err != nil || len(first.Transitions) != 1 || first.NextToken == "" {
				t.Fatal(first, err)
			}
			second, err := c.History(ctx, "run", first.NextToken, 1)
			if err != nil || second.Transitions[0].Sequence != 2 || second.NextToken != "" {
				t.Fatal(second, err)
			}
		}
	}
}

func replaceRecord(t *testing.T, m *memoryObjects, key string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	m.objects[key] = Object{Data: data, ETag: "replaced", Modified: m.now}
}

func TestProjectionRejectsCorruptionAndExpiredRuns(t *testing.T) {
	ctx := context.Background()
	historyKey := "runs/run/history/00000000000000000001.json"
	for _, scenario := range []string{"snapshot JSON", "snapshot identity", "history JSON", "history identity", "missing history", "gap", "phase", "time", "expired", "budget"} {
		t.Run(scenario, func(t *testing.T) {
			m := newMemory()
			c := NewCoordinator(New(m), func() time.Time { return m.now })
			admitUntilResolved(t, c, request(t, m, "one", "run"))
			switch scenario {
			case "snapshot JSON":
				m.objects["runs/run/snapshot.json"] = Object{Data: []byte("{"), Modified: m.now}
			case "snapshot identity":
				replaceRecord(t, m, "runs/run/snapshot.json", map[string]any{"schema_version": 1, "run_id": "other"})
			case "history JSON":
				m.objects[historyKey] = Object{Data: []byte("{"), Modified: m.now}
			case "history identity":
				replaceRecord(t, m, historyKey, Transition{RunID: "other", Sequence: 1, Phase: domain.Queued, At: m.now})
			case "missing history":
				delete(m.objects, historyKey)
			case "gap":
				delete(m.objects, historyKey)
				replaceRecord(t, m, "runs/run/history/00000000000000000002.json", Transition{RunID: "run", Sequence: 2, Phase: domain.Pulling, At: m.now})
			case "phase":
				replaceRecord(t, m, historyKey, Transition{RunID: "run", Sequence: 1, Phase: domain.Completed, At: m.now})
			case "time":
				replaceRecord(t, m, historyKey, Transition{RunID: "run", Sequence: 1, Phase: domain.Queued})
			case "expired":
				replaceRecord(t, m, historyKey, Transition{RunID: "run", Sequence: 1, Phase: domain.Queued, At: m.now.Add(-30 * 24 * time.Hour)})
			case "budget":
				for i := uint64(2); i <= 101; i++ {
					replaceRecord(t, m, fmt.Sprintf("runs/run/history/%020d.json", i), Transition{RunID: "run", Sequence: i, Phase: domain.Pulling, At: m.now})
				}
			}
			if _, err := c.ReadRun(ctx, "run"); err == nil {
				t.Fatal("invalid projection accepted")
			}
		})
	}
	m := newMemory()
	c := NewCoordinator(New(m), func() time.Time { return m.now })
	if _, err := c.ReadRun(ctx, "../bad"); err == nil {
		t.Fatal("invalid path")
	}
	if _, err := c.History(ctx, "../bad", "", 1); err == nil {
		t.Fatal("invalid history path")
	}
	if _, err := c.ReadRun(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	m.listErr = errors.New("list unavailable")
	if _, err := c.History(ctx, "run", "", 1); !errors.Is(err, m.listErr) {
		t.Fatal(err)
	}
}

type unexpectedPage struct {
	Objects
	page Page
}

func (o unexpectedPage) List(context.Context, string, string, int32) (Page, error) {
	return o.page, nil
}

func TestHistoryRejectsWrongPrefixAndMissingObjects(t *testing.T) {
	m := newMemory()
	c := NewCoordinator(New(m), func() time.Time { return m.now })
	for _, key := range []string{"runs/other/history/1.json", "runs/run/history/00000000000000000001.json"} {
		c.store = New(unexpectedPage{Objects: m, page: Page{Keys: []string{key}}})
		if _, err := c.History(context.Background(), "run", "", 1); err == nil {
			t.Fatal(key)
		}
	}
}
