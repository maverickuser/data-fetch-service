package state

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/domain"
)

// Transition contains the complete immutable payload needed to repair a reserved change.
type Transition struct {
	RunID      string          `json:"run_id"`
	Sequence   uint64          `json:"sequence"`
	Phase      domain.State    `json:"phase"`
	At         time.Time       `json:"at"`
	Details    json.RawMessage `json:"details,omitempty"`
	Acceptance *Acceptance     `json:"acceptance,omitempty"`
}

// Acceptance pins compact downstream-202 evidence beyond artifact retention.
type Acceptance struct {
	RunID          string    `json:"run_id"`
	Fingerprint    string    `json:"dataset_fingerprint"`
	DatasetURN     string    `json:"dataschema"`
	ConfigRevision string    `json:"config_revision"`
	AcceptedAt     time.Time `json:"accepted_at"`
}

// Baseline survives terminal release and run-record retention expiry.
type Baseline struct {
	RunID         string `json:"run_id"`
	Fingerprint   string `json:"dataset_fingerprint"`
	AcceptanceKey string `json:"acceptance_key"`
}

// Coordination is the mutable authority for one execution key.
type Coordination struct {
	SchemaVersion    int                `json:"schema_version"`
	Generation       uint64             `json:"generation"`
	ActiveRunID      string             `json:"active_run_id"`
	Phase            domain.State       `json:"phase"`
	OwnerToken       string             `json:"owner_token"`
	OwnerDeadline    time.Time          `json:"owner_deadline"`
	LastSequence     uint64             `json:"last_sequence"`
	Pending          *Transition        `json:"pending_transition,omitempty"`
	AcceptedBaseline *Baseline          `json:"accepted_baseline,omitempty"`
	Admission        *AdmissionDecision `json:"pending_admission,omitempty"`
	Dispatch         *Dispatch          `json:"dispatch,omitempty"`
	Retry            *RetryIntent       `json:"pending_retry,omitempty"`
}

// Lease fences authoritative writes by run, generation and invocation identity.
type Lease struct {
	RunID      string
	Generation uint64
	Token      string
}

// Coordinator reserves transitions before publishing history, then finalizes with CAS.
type Coordinator struct {
	store *Store
	now   func() time.Time
}

// NewCoordinator injects time so lease deadlines and recovery tests are deterministic.
func NewCoordinator(store *Store, now func() time.Time) *Coordinator { return &Coordinator{store, now} }

// Load reads authority without mutating it; recovery is an explicit operation.
func (c *Coordinator) Load(ctx context.Context, key string) (Coordination, string, error) {
	if !validCoordinationKey(key) {
		return Coordination{}, "", fmt.Errorf("invalid coordination key %q", key)
	}
	o, err := c.store.Read(ctx, key, c.now())
	if err != nil {
		return Coordination{}, "", err
	}
	var current Coordination
	if err := json.Unmarshal(o.Data, &current); err != nil {
		return Coordination{}, "", err
	}
	if current.SchemaVersion != 1 {
		return Coordination{}, "", fmt.Errorf("unsupported coordination schema")
	}
	return current, o.ETag, nil
}

// Claim fences a new invocation; expired owners require recovery, never direct takeover.
func (c *Coordinator) Claim(ctx context.Context, key, runID, token string, deadline time.Time) (Lease, error) {
	if token == "" || !deadline.After(c.now()) {
		return Lease{}, fmt.Errorf("owner token and future hard deadline plus grace required")
	}
	current, etag, err := c.Load(ctx, key)
	if err != nil {
		return Lease{}, err
	}
	if current.Dispatch != nil || current.Admission != nil || current.Pending != nil || current.Retry != nil || current.ActiveRunID != runID || runID == "" || current.Phase.Terminal() {
		return Lease{}, ErrConflict
	}
	if current.OwnerToken != "" {
		if current.OwnerToken == token && current.OwnerDeadline.Equal(deadline) {
			return Lease{runID, current.Generation, token}, nil
		}
		return Lease{}, ErrConflict
	}
	current.Generation++
	current.OwnerToken = token
	current.OwnerDeadline = deadline
	if err := c.save(ctx, key, current, etag); err != nil {
		return Lease{}, err
	}
	return Lease{runID, current.Generation, token}, nil
}

// Commit reserves the supplied next sequence; on any error callers reload before retrying.
func (c *Coordinator) Commit(ctx context.Context, key string, lease Lease, next Transition) error {
	current, etag, err := c.Load(ctx, key)
	if err != nil {
		return err
	}
	if current.ActiveRunID != lease.RunID || current.Generation != lease.Generation || current.OwnerToken != lease.Token || lease.Token == "" || !c.now().Before(current.OwnerDeadline) {
		return ErrConflict
	}
	if current.Admission != nil || current.Pending != nil || current.Retry != nil {
		return ErrConflict
	}
	if next.RunID != lease.RunID || !safeRunID(next.RunID) || next.Sequence != current.LastSequence+1 || next.At.IsZero() || !allowedTransition(current.Phase, next.Phase) {
		return fmt.Errorf("invalid transition")
	}
	if err := validateAcceptance(next); err != nil {
		return err
	}
	current.Pending = &next
	if err := c.save(ctx, key, current, etag); err != nil {
		return err
	}
	return c.Repair(ctx, key)
}

// Repair completes reserved history before finalizing authority; repeated repair is safe.
func (c *Coordinator) Repair(ctx context.Context, key string) error {
	current, etag, err := c.Load(ctx, key)
	if err != nil {
		return err
	}
	if current.Admission != nil {
		return c.repairAdmission(ctx, key, current, etag)
	}
	if current.Retry != nil {
		return c.repairRetry(ctx, key, current, etag)
	}
	if current.Pending == nil {
		return nil
	}
	next := current.Pending
	if next.RunID != current.ActiveRunID || next.Sequence != current.LastSequence+1 || !safeRunID(next.RunID) || next.At.IsZero() || !allowedTransition(current.Phase, next.Phase) {
		return fmt.Errorf("invalid pending transition")
	}
	if err := validateAcceptance(*next); err != nil {
		return err
	}
	if next.Acceptance != nil {
		data, err := json.Marshal(next.Acceptance)
		if err != nil {
			return err
		}
		acceptanceKey := "acceptance/" + strings.TrimSuffix(strings.TrimPrefix(key, "coordination/"), ".json") + "/" + next.RunID + ".json"
		if err := c.store.Create(ctx, acceptanceKey, data); err != nil {
			return err
		}
		current.AcceptedBaseline = &Baseline{RunID: next.RunID, Fingerprint: next.Acceptance.Fingerprint, AcceptanceKey: acceptanceKey}
	}
	data, err := json.Marshal(next)
	if err != nil {
		return err
	}
	historyKey := fmt.Sprintf("runs/%s/history/%020d.json", next.RunID, next.Sequence)
	if err := c.store.Create(ctx, historyKey, data); err != nil {
		return err
	}
	current.Phase = next.Phase
	current.LastSequence = next.Sequence
	current.Pending = nil
	if next.Phase.Terminal() {
		current.ActiveRunID = ""
		current.Dispatch = nil
	}
	if next.Phase == domain.DeliveryPending {
		current.Dispatch = &Dispatch{RunID: next.RunID, Queue: "delivery", State: "pending"}
	}
	if next.Phase.Terminal() || next.Phase == domain.DeliveryPending {
		current.OwnerToken = ""
		current.OwnerDeadline = time.Time{}
	}
	return c.save(ctx, key, current, etag)
}

// validateAcceptance allows baseline advancement only with complete completed-run evidence.
func validateAcceptance(next Transition) error {
	if next.Phase != domain.Completed {
		if next.Acceptance != nil {
			return fmt.Errorf("acceptance requires completed transition")
		}
		return nil
	}
	a := next.Acceptance
	if a == nil || a.RunID != next.RunID || a.Fingerprint == "" || a.DatasetURN == "" || a.ConfigRevision == "" || a.AcceptedAt.IsZero() {
		return fmt.Errorf("completed transition requires acceptance evidence")
	}
	return nil
}

// save never retries a stale ETag; an ambiguous response also requires a fresh read.
func (c *Coordinator) save(ctx context.Context, key string, current Coordination, etag string) error {
	data, err := json.Marshal(current)
	if err != nil {
		return err
	}
	_, err = c.store.CompareAndSwap(ctx, key, data, etag)
	return err
}

// safeRunID ensures immutable history remains under its owning run prefix.
func safeRunID(id string) bool { return validKey(id) && !strings.Contains(id, "/") }

// allowedTransition keeps terminal histories closed and phase changes explicit.
func allowedTransition(from, to domain.State) bool {
	if from.Terminal() {
		return false
	}
	if to == domain.Failed {
		return from == domain.Queued || from == domain.Pulling || from == domain.DownloadsCompleted || from == domain.DeliveryPending || from == domain.Delivering
	}
	switch from {
	case domain.Queued:
		return to == domain.Pulling
	case domain.Pulling:
		return to == domain.DownloadsCompleted
	case domain.DownloadsCompleted:
		return to == domain.DeliveryPending || to == domain.SkippedUnchanged
	case domain.DeliveryPending:
		return to == domain.Delivering
	case domain.Delivering:
		return to == domain.Completed
	}
	return false
}
