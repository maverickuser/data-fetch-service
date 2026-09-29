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

// RequestIntent pins the first admission candidate and its complete resolved snapshot.
type RequestIntent struct {
	RequestKey     string          `json:"request_key"`
	ExecutionKey   string          `json:"execution_key"`
	PayloadHash    string          `json:"payload_hash"`
	CandidateRunID string          `json:"candidate_run_id"`
	EventType      string          `json:"event_type"`
	CreatedAt      time.Time       `json:"created_at"`
	Snapshot       json.RawMessage `json:"snapshot"`
}

// Resolution permanently maps a retained request to its selected execution.
type Resolution struct {
	RequestKey   string `json:"request_key"`
	ExecutionKey string `json:"execution_key"`
	PayloadHash  string `json:"payload_hash"`
	RunID        string `json:"run_id"`
	Joined       bool   `json:"joined"`
}

// AdmissionDecision prevents terminal release until the immutable resolution exists.
type AdmissionDecision struct {
	Request    RequestIntent `json:"request"`
	Resolution Resolution    `json:"resolution"`
}

// Listing records the immutable admission-time index for a run.
type Listing struct {
	RunID        string    `json:"run_id"`
	EventType    string    `json:"event_type"`
	ExecutionKey string    `json:"execution_key"`
	CreatedAt    time.Time `json:"created_at"`
}

// NewRequest derives durable request metadata from an already resolved snapshot.
func NewRequest(snapshot []byte, createdAt time.Time) (RequestIntent, error) {
	var s events.Snapshot
	if err := json.Unmarshal(snapshot, &s); err != nil {
		return RequestIntent{}, err
	}
	request := RequestIntent{s.RequestKey, s.ExecutionKey, s.PayloadHash, s.RunID, s.Event.EventType, createdAt.UTC(), append(json.RawMessage(nil), snapshot...)}
	if s.SchemaVersion != 1 {
		return RequestIntent{}, fmt.Errorf("unsupported snapshot schema")
	}
	return request, validateRequest(request)
}

// validateRequest confines object paths and verifies snapshot identities before persistence.
func validateRequest(r RequestIntent) error {
	if !safeRunID(r.RequestKey) || !safeRunID(r.ExecutionKey) || !safeRunID(r.CandidateRunID) || !safeRunID(r.EventType) || r.PayloadHash == "" || r.CreatedAt.IsZero() {
		return fmt.Errorf("invalid request identity or time")
	}
	var s events.Snapshot
	if err := json.Unmarshal(r.Snapshot, &s); err != nil {
		return err
	}
	if s.SchemaVersion != 1 || s.RequestKey != r.RequestKey || s.ExecutionKey != r.ExecutionKey || s.PayloadHash != r.PayloadHash || s.RunID != r.CandidateRunID || s.Event.EventType != r.EventType {
		return fmt.Errorf("request does not match snapshot")
	}
	return nil
}

// EnsureRequest converges concurrent candidates on the first immutable request intent.
func (c *Coordinator) EnsureRequest(ctx context.Context, r RequestIntent) (RequestIntent, error) {
	if err := validateRequest(r); err != nil {
		return RequestIntent{}, err
	}
	data, err := json.Marshal(r)
	if err != nil {
		return RequestIntent{}, err
	}
	key := "requests/" + r.RequestKey + "/intent.json"
	if err := c.store.Create(ctx, key, data); err != nil && !errors.Is(err, ErrIntegrity) {
		return RequestIntent{}, err
	}
	object, err := c.store.Read(ctx, key, c.now())
	if err != nil {
		return RequestIntent{}, err
	}
	var winner RequestIntent
	if err := json.Unmarshal(object.Data, &winner); err != nil {
		return RequestIntent{}, err
	}
	if err := validateRequest(winner); err != nil {
		return RequestIntent{}, err
	}
	if winner.RequestKey != r.RequestKey || winner.ExecutionKey != r.ExecutionKey || winner.PayloadHash != r.PayloadHash {
		return RequestIntent{}, ErrIntegrity
	}
	return winner, nil
}

// Admit reserves one admission decision; conflicts are retried only after fresh evaluation.
func (c *Coordinator) Admit(ctx context.Context, candidate RequestIntent) (Resolution, error) {
	r, err := c.EnsureRequest(ctx, candidate)
	if err != nil {
		return Resolution{}, err
	}
	if receipt, err := c.RequestResolution(ctx, r.RequestKey); err == nil {
		if receipt.ExecutionKey != r.ExecutionKey || receipt.PayloadHash != r.PayloadHash {
			return Resolution{}, ErrIntegrity
		}
		return receipt, nil
	} else if !errors.Is(err, ErrNotFound) {
		return Resolution{}, err
	}
	key := "coordination/" + r.ExecutionKey + ".json"
	current, etag, err := c.Load(ctx, key)
	if errors.Is(err, ErrNotFound) {
		current = Coordination{SchemaVersion: 1}
	} else if err != nil {
		return Resolution{}, err
	}
	if current.Admission != nil || current.Pending != nil {
		if err := c.Repair(ctx, key); err != nil {
			return Resolution{}, err
		}
		return Resolution{}, ErrConflict
	}
	// A competing admission may have finalized between our first receipt read and Load.
	// Checking again under this ETag prevents reserving a contradictory join decision.
	if receipt, err := c.RequestResolution(ctx, r.RequestKey); err == nil {
		if receipt.ExecutionKey != r.ExecutionKey || receipt.PayloadHash != r.PayloadHash {
			return Resolution{}, ErrIntegrity
		}
		return receipt, nil
	} else if !errors.Is(err, ErrNotFound) {
		return Resolution{}, err
	}
	receipt := Resolution{RequestKey: r.RequestKey, ExecutionKey: r.ExecutionKey, PayloadHash: r.PayloadHash, RunID: current.ActiveRunID, Joined: current.ActiveRunID != ""}
	if !receipt.Joined {
		receipt.RunID = r.CandidateRunID
		current.Generation++
		current.ActiveRunID = r.CandidateRunID
		current.Phase = domain.Queued
		current.LastSequence = 0
		current.OwnerToken = ""
		current.OwnerDeadline = time.Time{}
		current.Dispatch = nil
	}
	current.Admission = &AdmissionDecision{r, receipt}
	if err := c.save(ctx, key, current, etag); err != nil {
		return Resolution{}, err
	}
	if err := c.Repair(ctx, key); err != nil {
		return Resolution{}, err
	}
	return receipt, nil
}

// repairAdmission finishes all immutable records before releasing the admission lock.
func (c *Coordinator) repairAdmission(ctx context.Context, key string, current Coordination, etag string) error {
	decision := current.Admission
	r := decision.Request
	receipt := decision.Resolution
	if err := validateRequest(r); err != nil {
		return err
	}
	if key != "coordination/"+r.ExecutionKey+".json" || receipt.RunID != current.ActiveRunID || receipt.RequestKey != r.RequestKey || receipt.ExecutionKey != r.ExecutionKey || receipt.PayloadHash != r.PayloadHash || (!receipt.Joined && receipt.RunID != r.CandidateRunID) {
		return ErrIntegrity
	}
	if !receipt.Joined {
		if err := c.store.Create(ctx, "runs/"+receipt.RunID+"/snapshot.json", r.Snapshot); err != nil {
			return err
		}
		first := Transition{RunID: receipt.RunID, Sequence: 1, Phase: domain.Queued, At: r.CreatedAt}
		if err := c.createJSON(ctx, fmt.Sprintf("runs/%s/history/%020d.json", receipt.RunID, 1), first); err != nil {
			return err
		}
		listing := Listing{receipt.RunID, r.EventType, r.ExecutionKey, r.CreatedAt}
		listingKey := fmt.Sprintf("listings/%s/%s/%s-%s.json", r.EventType, r.CreatedAt.Format("2006-01-02"), r.CreatedAt.Format("20060102T150405.000000000Z"), receipt.RunID)
		if err := c.createJSON(ctx, listingKey, listing); err != nil {
			return err
		}
		current.LastSequence = 1
		current.Dispatch = &Dispatch{RunID: receipt.RunID, Queue: "pull", State: "pending"}
	}
	if err := c.createJSON(ctx, "requests/"+r.RequestKey+"/resolution.json", receipt); err != nil {
		return err
	}
	current.Admission = nil
	return c.save(ctx, key, current, etag)
}

// createJSON canonically encodes immutable metadata before conditional persistence.
func (c *Coordinator) createJSON(ctx context.Context, key string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.store.Create(ctx, key, data)
}

// RequestResolution reads an existing receipt without running admission or recovery.
func (c *Coordinator) RequestResolution(ctx context.Context, requestKey string) (Resolution, error) {
	if !safeRunID(requestKey) {
		return Resolution{}, fmt.Errorf("invalid request key")
	}
	object, err := c.store.Read(ctx, "requests/"+requestKey+"/resolution.json", c.now())
	if err != nil {
		return Resolution{}, err
	}
	var receipt Resolution
	if err := json.Unmarshal(object.Data, &receipt); err != nil {
		return Resolution{}, err
	}
	if receipt.RequestKey != requestKey || !safeRunID(receipt.RunID) || !safeRunID(receipt.ExecutionKey) || receipt.PayloadHash == "" {
		return Resolution{}, ErrIntegrity
	}
	return receipt, nil
}

// ReadRequest loads the pinned intent without creating or repairing admission state.
func (c *Coordinator) ReadRequest(ctx context.Context, requestKey string) (RequestIntent, error) {
	if !safeRunID(requestKey) {
		return RequestIntent{}, fmt.Errorf("invalid request key")
	}
	object, err := c.store.Read(ctx, "requests/"+requestKey+"/intent.json", c.now())
	if err != nil {
		return RequestIntent{}, err
	}
	var r RequestIntent
	if err := json.Unmarshal(object.Data, &r); err != nil {
		return RequestIntent{}, err
	}
	if r.RequestKey != requestKey {
		return RequestIntent{}, ErrIntegrity
	}
	return r, validateRequest(r)
}
