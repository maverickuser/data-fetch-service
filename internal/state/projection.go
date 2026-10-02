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
	RunID               string          `json:"run_id"`
	Phase               domain.State    `json:"phase"`
	Sequence            uint64          `json:"sequence"`
	CreatedAt           time.Time       `json:"created_at"`
	UpdatedAt           time.Time       `json:"updated_at"`
	Snapshot            json.RawMessage `json:"snapshot,omitempty"`
	ParentRunID         string          `json:"parent_run_id,omitempty"`
	RetryRootRunID      string          `json:"retry_root_run_id,omitempty"`
	ExecutionRetryIndex int             `json:"execution_retry_index"`
	ChildRunID          string          `json:"child_run_id,omitempty"`
	LatestRunID         string          `json:"latest_run_id"`
	LatestPhase         domain.State    `json:"latest_phase"`
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
	view, err := c.readRunBase(ctx, runID)
	if err != nil {
		return RunView{}, err
	}
	latest := view
	seen := map[string]bool{runID: true}
	for i := view.ExecutionRetryIndex; i < 3; i++ {
		object, err := c.store.Read(ctx, "runs/"+latest.RunID+"/automatic-retry.json", c.now())
		if err == ErrNotFound {
			break
		}
		if err != nil {
			return RunView{}, err
		}
		var link AutomaticRetryLink
		if json.Unmarshal(object.Data, &link) != nil || link.ParentRunID != latest.RunID || !safeRunID(link.ChildRunID) || seen[link.ChildRunID] || link.Index != latest.ExecutionRetryIndex+1 {
			return RunView{}, ErrIntegrity
		}
		if view.RetryRootRunID != "" && link.RootRunID != view.RetryRootRunID || view.RetryRootRunID == "" && link.RootRunID != view.RunID {
			return RunView{}, ErrIntegrity
		}
		child, err := c.readRunBase(ctx, link.ChildRunID)
		if err != nil {
			if err == ErrNotFound {
				coordination, _, loadErr := c.Load(ctx, "coordination/"+snapshotExecutionKey(latest.Snapshot)+".json")
				if loadErr == nil && coordination.Retry != nil && coordination.Retry.ParentRunID == link.ParentRunID && coordination.Retry.ChildRunID == link.ChildRunID {
					if latest.RunID == view.RunID {
						view.ChildRunID = link.ChildRunID
					}
					break
				}
			}
			return RunView{}, err
		}
		if child.ParentRunID != latest.RunID || child.RetryRootRunID != link.RootRunID || child.ExecutionRetryIndex != link.Index {
			return RunView{}, ErrIntegrity
		}
		seen[child.RunID] = true
		if latest.RunID == view.RunID {
			view.ChildRunID = child.RunID
		}
		latest = child
	}
	view.LatestRunID = latest.RunID
	view.LatestPhase = latest.Phase
	return view, nil
}

// snapshotExecutionKey extracts the admitted coordination identity for a reserved retry.
func snapshotExecutionKey(raw json.RawMessage) string {
	var snapshot events.Snapshot
	if json.Unmarshal(raw, &snapshot) != nil {
		return ""
	}
	return snapshot.ExecutionKey
}

// readRunBase validates one run's immutable snapshot and transition history.
func (c *Coordinator) readRunBase(ctx context.Context, runID string) (RunView, error) {
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
	view := RunView{RunID: runID, Snapshot: object.Data, ParentRunID: snapshot.ParentRunID, RetryRootRunID: snapshot.RetryRootRunID, ExecutionRetryIndex: snapshot.ExecutionRetryIndex}
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
