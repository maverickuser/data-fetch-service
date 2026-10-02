package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/domain"
	"github.com/maverickuser/data-fetch-service/internal/events"
)

// RetryIntent pins one child identity and snapshot before any parent is terminated.
type RetryIntent struct {
	ParentRunID    string          `json:"parent_run_id"`
	ChildRunID     string          `json:"child_run_id"`
	RootRunID      string          `json:"retry_root_run_id"`
	Index          int             `json:"execution_retry_index"`
	At             time.Time       `json:"at"`
	ParentSequence uint64          `json:"parent_sequence"`
	ChildSnapshot  json.RawMessage `json:"child_snapshot"`
}

// AutomaticRetryLink lets readers follow the original request to its one reserved child.
type AutomaticRetryLink struct {
	ParentRunID string `json:"parent_run_id"`
	ChildRunID  string `json:"child_run_id"`
	RootRunID   string `json:"retry_root_run_id"`
	Index       int    `json:"execution_retry_index"`
}

// ReservePullRetry fences one expired pull and repairs its child without releasing the claim.
// An exhausted fourth execution is terminal and returns an empty child ID.
func (c *Coordinator) ReservePullRetry(ctx context.Context, key, childID string) (string, error) {
	current, etag, err := c.Load(ctx, key)
	if err != nil {
		return "", err
	}
	if current.Retry != nil {
		child := current.Retry.ChildRunID
		return child, c.Repair(ctx, key)
	}
	if current.Pending != nil || current.Admission != nil {
		if err := c.Repair(ctx, key); err != nil {
			return "", err
		}
		return "", ErrConflict
	}
	if current.Phase != domain.Pulling || !safeRunID(current.ActiveRunID) || current.OwnerDeadline.IsZero() || c.now().Before(current.OwnerDeadline) {
		return "", ErrConflict
	}
	parentObject, err := c.store.Read(ctx, "runs/"+current.ActiveRunID+"/snapshot.json", c.now())
	if err != nil {
		return "", err
	}
	var parent events.Snapshot
	if json.Unmarshal(parentObject.Data, &parent) != nil || parent.SchemaVersion != 1 || parent.RunID != current.ActiveRunID || parent.ExecutionKey == "" || key != "coordination/"+parent.ExecutionKey+".json" || parent.ExecutionRetryIndex < 0 || parent.ExecutionRetryIndex > 3 {
		return "", ErrIntegrity
	}
	if parent.DeliveryRetry != nil {
		details, _ := json.Marshal(map[string]string{"stage": "delivery", "code": "WORKER_TERMINATED", "retry_guidance": "manual_delivery_retry"})
		current.Pending = &Transition{RunID: parent.RunID, Sequence: current.LastSequence + 1, Phase: domain.Failed, At: c.now().UTC(), Details: details}
		if err := c.save(ctx, key, current, etag); err != nil {
			return "", err
		}
		return "", c.Repair(ctx, key)
	}
	failedCode, err := c.recordedPullFailure(ctx, parent)
	if err != nil {
		return "", err
	}
	if failedCode != "" {
		details, _ := json.Marshal(map[string]string{"stage": "pull", "code": failedCode, "message": "source acquisition failed; inspect job results", "retry_guidance": "manual_rerun"})
		current.Pending = &Transition{RunID: current.ActiveRunID, Sequence: current.LastSequence + 1, Phase: domain.Failed, At: c.now().UTC(), Details: details}
		if err := c.save(ctx, key, current, etag); err != nil {
			return "", err
		}
		return "", c.Repair(ctx, key)
	}
	if parent.ExecutionRetryIndex == 3 {
		details, _ := json.Marshal(map[string]string{"stage": "pull", "code": "EXECUTION_RETRIES_EXHAUSTED", "cause": "WORKER_TERMINATED", "retry_guidance": "manual_rerun"})
		current.Pending = &Transition{RunID: current.ActiveRunID, Sequence: current.LastSequence + 1, Phase: domain.Failed, At: c.now().UTC(), Details: details}
		if err := c.save(ctx, key, current, etag); err != nil {
			return "", err
		}
		return "", c.Repair(ctx, key)
	}
	if !safeRunID(childID) || childID == current.ActiveRunID {
		return "", fmt.Errorf("invalid retry child ID")
	}
	root := parent.RetryRootRunID
	if root == "" {
		root = parent.RunID
	}
	child := parent
	child.RunID = childID
	if child.CorrelationID == "" {
		child.CorrelationID = root
	}
	child.ParentRunID = parent.RunID
	child.RetryRootRunID = root
	child.ExecutionRetryIndex = parent.ExecutionRetryIndex + 1
	raw, err := json.Marshal(child)
	if err != nil {
		return "", err
	}
	current.Retry = &RetryIntent{ParentRunID: parent.RunID, ChildRunID: childID, RootRunID: root, Index: child.ExecutionRetryIndex, At: c.now().UTC(), ParentSequence: current.LastSequence + 1, ChildSnapshot: raw}
	if err := c.save(ctx, key, current, etag); err != nil {
		return "", err
	}
	return childID, c.Repair(ctx, key)
}

// recordedPullFailure finds a durable source failure that must finish terminally instead of spawning a child.
func (c *Coordinator) recordedPullFailure(ctx context.Context, snapshot events.Snapshot) (string, error) {
	for _, job := range snapshot.Jobs {
		if !safeRunID(job.ID) {
			return "", ErrIntegrity
		}
		object, err := c.store.Read(ctx, "runs/"+snapshot.RunID+"/pulls/"+job.ID+"/result.json", c.now())
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return "", err
		}
		var outcome struct {
			JobID     string `json:"job_id"`
			Status    string `json:"status"`
			ErrorCode string `json:"error_code"`
		}
		if json.Unmarshal(object.Data, &outcome) != nil || outcome.JobID != job.ID {
			return "", ErrIntegrity
		}
		if outcome.Status == "failed" {
			if outcome.ErrorCode == "" || len(outcome.ErrorCode) > 128 {
				return "", ErrIntegrity
			}
			return outcome.ErrorCode, nil
		}
		if outcome.Status == "interrupted" && outcome.ErrorCode != "GROUP_BUDGET_EXCEEDED" {
			return "", ErrIntegrity
		}
		if outcome.Status != "completed" && outcome.Status != "canceled" && outcome.Status != "interrupted" {
			return "", ErrIntegrity
		}
	}
	return "", nil
}

// repairRetry writes every immutable parent/child record before atomically transferring authority.
func (c *Coordinator) repairRetry(ctx context.Context, key string, current Coordination, etag string) error {
	intent := current.Retry
	if intent == nil || !safeRunID(intent.ParentRunID) || !safeRunID(intent.ChildRunID) || !safeRunID(intent.RootRunID) || intent.ChildRunID == intent.ParentRunID || intent.Index < 1 || intent.Index > 3 || intent.At.IsZero() || intent.ParentSequence != current.LastSequence+1 || current.ActiveRunID != intent.ParentRunID || current.Phase != domain.Pulling || current.Pending != nil || current.Admission != nil {
		return ErrIntegrity
	}
	var snapshot events.Snapshot
	if json.Unmarshal(intent.ChildSnapshot, &snapshot) != nil || snapshot.SchemaVersion != 1 || snapshot.RunID != intent.ChildRunID || snapshot.ParentRunID != intent.ParentRunID || snapshot.RetryRootRunID != intent.RootRunID || snapshot.ExecutionRetryIndex != intent.Index || key != "coordination/"+snapshot.ExecutionKey+".json" {
		return ErrIntegrity
	}
	details, _ := json.Marshal(map[string]string{"stage": "pull", "code": "WORKER_TERMINATED", "retry_guidance": "automatic_retry"})
	failed := Transition{RunID: intent.ParentRunID, Sequence: intent.ParentSequence, Phase: domain.Failed, At: intent.At, Details: details}
	if err := c.createJSON(ctx, fmt.Sprintf("runs/%s/history/%020d.json", intent.ParentRunID, intent.ParentSequence), failed); err != nil {
		return err
	}
	link := AutomaticRetryLink{ParentRunID: intent.ParentRunID, ChildRunID: intent.ChildRunID, RootRunID: intent.RootRunID, Index: intent.Index}
	if err := c.createJSON(ctx, "runs/"+intent.ParentRunID+"/automatic-retry.json", link); err != nil {
		return err
	}
	if err := c.store.Create(ctx, "runs/"+intent.ChildRunID+"/snapshot.json", intent.ChildSnapshot); err != nil {
		return err
	}
	queued := Transition{RunID: intent.ChildRunID, Sequence: 1, Phase: domain.Queued, At: intent.At}
	if err := c.createJSON(ctx, fmt.Sprintf("runs/%s/history/%020d.json", intent.ChildRunID, 1), queued); err != nil {
		return err
	}
	listing := Listing{RunID: intent.ChildRunID, EventType: snapshot.Event.EventType, ExecutionKey: snapshot.ExecutionKey, CreatedAt: intent.At}
	listingKey := fmt.Sprintf("listings/%s/%s/%s-%s.json", listing.EventType, intent.At.Format("2006-01-02"), intent.At.Format("20060102T150405.000000000Z"), listing.RunID)
	if err := c.createJSON(ctx, listingKey, listing); err != nil {
		return err
	}
	current.Generation++
	current.ActiveRunID = intent.ChildRunID
	current.Phase = domain.Queued
	current.OwnerToken = ""
	current.OwnerDeadline = time.Time{}
	current.LastSequence = 1
	current.Dispatch = &Dispatch{RunID: intent.ChildRunID, Queue: "pull", State: "pending"}
	current.Retry = nil
	return c.save(ctx, key, current, etag)
}
