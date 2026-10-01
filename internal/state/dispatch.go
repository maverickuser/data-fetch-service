package state

import (
	"context"
	"fmt"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/domain"
)

// Dispatch is durable before publishing; its run and queue identify one phase handoff.
type Dispatch struct {
	RunID string `json:"run_id"`
	Queue string `json:"queue"`
	State string `json:"state"`
}

// Publisher sends a run-ID message to a logical queue and may have ambiguous failures.
type Publisher interface {
	Publish(context.Context, string, string) error
}

// PublishPending sends at least once; a failed final CAS leaves repairable pending work.
func (c *Coordinator) PublishPending(ctx context.Context, key string, publisher Publisher) error {
	current, etag, err := c.Load(ctx, key)
	if err != nil {
		return err
	}
	if current.Admission != nil || current.Pending != nil || current.Retry != nil {
		return ErrConflict
	}
	d := current.Dispatch
	if d == nil || d.State != "pending" {
		return nil
	}
	if d.RunID != current.ActiveRunID || !dispatchMatches(d.Queue, current.Phase) {
		return ErrIntegrity
	}
	if err := publisher.Publish(ctx, d.Queue, d.RunID); err != nil {
		return err
	}
	d.State = "sent"
	return c.save(ctx, key, current, etag)
}

// ClaimDispatched atomically checks the expected queue phase and fences duplicate messages.
func (c *Coordinator) ClaimDispatched(ctx context.Context, key, runID, queue, token string, deadline time.Time) (Lease, error) {
	if token == "" || !deadline.After(c.now()) {
		return Lease{}, fmt.Errorf("token and future deadline required")
	}
	current, etag, err := c.Load(ctx, key)
	if err != nil {
		return Lease{}, err
	}
	if current.Admission != nil || current.Pending != nil || current.Retry != nil || current.ActiveRunID != runID || !dispatchMatches(queue, current.Phase) || current.Dispatch == nil || current.Dispatch.RunID != runID || current.Dispatch.Queue != queue {
		return Lease{}, ErrConflict
	}
	if current.OwnerToken != "" {
		if current.OwnerToken == token && current.OwnerDeadline.Equal(deadline) && current.Dispatch.State == "claimed" {
			return Lease{runID, current.Generation, token}, nil
		}
		return Lease{}, ErrConflict
	}
	current.Generation++
	current.OwnerToken = token
	current.OwnerDeadline = deadline
	current.Dispatch.State = "claimed"
	if err := c.save(ctx, key, current, etag); err != nil {
		return Lease{}, err
	}
	return Lease{runID, current.Generation, token}, nil
}

// dispatchMatches prevents stale pull messages from claiming a delivery phase.
func dispatchMatches(queue string, phase domain.State) bool {
	return (queue == "pull" && phase == domain.Queued) || (queue == "delivery" && phase == domain.DeliveryPending)
}

// ClaimExpiredDelivery fences an abandoned delivery without resetting its HTTP attempt ledger.
func (c *Coordinator) ClaimExpiredDelivery(ctx context.Context, key, runID, token string, deadline time.Time) (Lease, error) {
	if token == "" || !deadline.After(c.now()) {
		return Lease{}, fmt.Errorf("token and future deadline required")
	}
	current, etag, err := c.Load(ctx, key)
	if err != nil {
		return Lease{}, err
	}
	if current.ActiveRunID != runID || current.Phase != domain.Delivering || current.OwnerDeadline.IsZero() || c.now().Before(current.OwnerDeadline) || current.Pending != nil || current.Admission != nil || current.Retry != nil {
		return Lease{}, ErrConflict
	}
	current.Generation++
	current.OwnerToken = token
	current.OwnerDeadline = deadline
	if err := c.save(ctx, key, current, etag); err != nil {
		return Lease{}, err
	}
	return Lease{RunID: runID, Generation: current.Generation, Token: token}, nil
}

// RequeueExpiredDispatch releases only a pre-work claim whose phase never advanced.
func (c *Coordinator) RequeueExpiredDispatch(ctx context.Context, key string) error {
	current, etag, err := c.Load(ctx, key)
	if err != nil {
		return err
	}
	if current.Pending != nil || current.Admission != nil || current.Retry != nil || current.Dispatch == nil || current.ActiveRunID != current.Dispatch.RunID || !dispatchMatches(current.Dispatch.Queue, current.Phase) || current.OwnerDeadline.IsZero() || c.now().Before(current.OwnerDeadline) {
		return ErrConflict
	}
	current.Generation++
	current.OwnerToken = ""
	current.OwnerDeadline = time.Time{}
	current.Dispatch.State = "pending"
	return c.save(ctx, key, current, etag)
}

// ClaimExpiredDownloadsCompleted fences an abandoned post-download decision without rerunning sources.
func (c *Coordinator) ClaimExpiredDownloadsCompleted(ctx context.Context, key, token string, deadline time.Time) (Lease, error) {
	if token == "" || !deadline.After(c.now()) {
		return Lease{}, fmt.Errorf("token and future deadline required")
	}
	current, etag, err := c.Load(ctx, key)
	if err != nil {
		return Lease{}, err
	}
	if current.Phase != domain.DownloadsCompleted || !safeRunID(current.ActiveRunID) || current.OwnerDeadline.IsZero() || c.now().Before(current.OwnerDeadline) || current.Pending != nil || current.Admission != nil || current.Retry != nil {
		return Lease{}, ErrConflict
	}
	current.Generation++
	current.OwnerToken = token
	current.OwnerDeadline = deadline
	if err := c.save(ctx, key, current, etag); err != nil {
		return Lease{}, err
	}
	return Lease{RunID: current.ActiveRunID, Generation: current.Generation, Token: token}, nil
}
