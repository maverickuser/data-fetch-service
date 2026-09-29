package state

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/domain"
	"github.com/maverickuser/data-fetch-service/internal/events"
)

// RunView is derived only from immutable history, never from an uncommitted reservation.
type RunView struct {
	RunID     string          `json:"run_id"`
	Phase     domain.State    `json:"phase"`
	Sequence  uint64          `json:"sequence"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
	Snapshot  json.RawMessage `json:"snapshot,omitempty"`
}

// HistoryPage is a bounded immutable history page with an opaque continuation token.
type HistoryPage struct {
	Transitions []Transition
	NextToken   string
}

// History reads a single page without repairing or changing coordination.
func (c *Coordinator) History(ctx context.Context, runID, token string, limit int32) (HistoryPage, error) {
	if !safeRunID(runID) {
		return HistoryPage{}, fmt.Errorf("invalid run ID")
	}
	prefix := "runs/" + runID + "/history/"
	page, err := c.store.Scan(ctx, prefix, token, limit)
	if err != nil {
		return HistoryPage{}, err
	}
	result := HistoryPage{NextToken: page.NextToken}
	for _, key := range page.Keys {
		if !strings.HasPrefix(key, prefix) {
			return HistoryPage{}, ErrIntegrity
		}
		object, err := c.store.Read(ctx, key, c.now())
		if err != nil {
			return HistoryPage{}, err
		}
		var transition Transition
		if err := json.Unmarshal(object.Data, &transition); err != nil {
			return HistoryPage{}, err
		}
		if transition.RunID != runID || transition.Sequence == 0 || key != fmt.Sprintf("%s%020d.json", prefix, transition.Sequence) {
			return HistoryPage{}, ErrIntegrity
		}
		result.Transitions = append(result.Transitions, transition)
	}
	return result, nil
}

// ReadRun projects at most 100 run-level transitions; job attempts are separate records.
func (c *Coordinator) ReadRun(ctx context.Context, runID string) (RunView, error) {
	if !safeRunID(runID) {
		return RunView{}, fmt.Errorf("invalid run ID")
	}
	object, err := c.store.Read(ctx, "runs/"+runID+"/snapshot.json", c.now())
	if err != nil {
		return RunView{}, err
	}
	var snapshot events.Snapshot
	if err := json.Unmarshal(object.Data, &snapshot); err != nil {
		return RunView{}, err
	}
	if snapshot.SchemaVersion != 1 || snapshot.RunID != runID {
		return RunView{}, ErrIntegrity
	}
	history, err := c.History(ctx, runID, "", 100)
	if err != nil {
		return RunView{}, err
	}
	if history.NextToken != "" {
		return RunView{}, fmt.Errorf("run history exceeds projection budget")
	}
	if len(history.Transitions) == 0 {
		return RunView{}, ErrNotFound
	}
	view := RunView{RunID: runID, Snapshot: object.Data}
	for index, t := range history.Transitions {
		if t.Sequence != uint64(index+1) || t.At.IsZero() || (index == 0 && t.Phase != domain.Queued) || (index > 0 && !allowedTransition(view.Phase, t.Phase)) {
			return RunView{}, ErrIntegrity
		}
		if index == 0 {
			view.CreatedAt = t.At
		}
		view.Phase = t.Phase
		view.Sequence = t.Sequence
		view.UpdatedAt = t.At
	}
	// Lifecycle deletion is asynchronous; retained later history does not extend a run.
	if !view.CreatedAt.Add(30 * 24 * time.Hour).After(c.now()) {
		return RunView{}, ErrExpired
	}
	return view, nil
}
