package events

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/maverickuser/data-fetch-service/internal/config"
	"github.com/maverickuser/data-fetch-service/internal/domain"
)

// Snapshot is serialized before dispatch; retries consume these bytes without re-resolution.
type Snapshot struct {
	SchemaVersion       int                  `json:"schema_version"`
	RunID               string               `json:"run_id"`
	RequestKey          string               `json:"request_key"`
	ExecutionKey        string               `json:"execution_key"`
	PayloadHash         string               `json:"payload_hash"`
	ConfigRevision      string               `json:"config_revision"`
	Event               Normalized           `json:"event"`
	Inputs              map[string]string    `json:"inputs"`
	Jobs                []config.ResolvedJob `json:"jobs"`
	Config              config.Config        `json:"config"`
	Force               bool                 `json:"force"`
	ParentRunID         string               `json:"parent_run_id,omitempty"`
	RetryRootRunID      string               `json:"retry_root_run_id,omitempty"`
	ExecutionRetryIndex int                  `json:"execution_retry_index,omitempty"`
}

// CloneForFullRerun preserves resolved jobs and dates while assigning a fresh manual request identity.
func CloneForFullRerun(parent Snapshot, runID, requestID string, force bool) ([]byte, error) {
	if parent.SchemaVersion != 1 || parent.RunID == "" || parent.ExecutionKey == "" || len(parent.Jobs) == 0 || runID == "" || runID == parent.RunID || requestID == "" {
		return nil, fmt.Errorf("invalid full rerun snapshot")
	}
	child := parent
	child.RunID = runID
	child.ParentRunID = parent.RunID
	child.RetryRootRunID = ""
	child.ExecutionRetryIndex = 0
	child.Force = force
	child.Event.Source = "urn:bond-platform:manual-rerun:" + parent.RunID
	child.Event.EventID = requestID
	child.Event.Force = force
	child.Event.Original = nil
	child.Event.OccurredAt = parent.Event.OccurredAt.UTC()
	requestKey, err := domain.RequestKey(child.Event.Source, requestID)
	if err != nil {
		return nil, err
	}
	child.RequestKey = requestKey
	canonical := child.Event
	canonical.Inputs = make(map[string]any, len(child.Inputs))
	for name, value := range child.Inputs {
		canonical.Inputs[name] = value
	}
	child.PayloadHash, err = digest(canonical)
	if err != nil {
		return nil, err
	}
	return json.Marshal(child)
}

// BuildSnapshot returns detached immutable JSON with resolved jobs and conflict identities.
func BuildSnapshot(cfg config.Config, event Normalized, runID string) ([]byte, error) {
	if runID == "" {
		return nil, fmt.Errorf("run id is required")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	def, ok := cfg.Event(event.EventType)
	if !ok {
		return nil, fmt.Errorf("unknown event %s", event.EventType)
	}
	inputs, jobs, err := def.Resolve(event.OccurredAt, event.Inputs)
	if err != nil {
		return nil, err
	}
	key, err := domain.RequestKey(event.Source, event.EventID)
	if err != nil {
		return nil, err
	}
	execution, err := digest(struct {
		EventType string
		Inputs    map[string]string
	}{event.EventType, inputs})
	if err != nil {
		return nil, err
	}
	// Original transport data is excluded so queue redelivery cannot alter identity.
	canonical := event
	canonical.Original = nil
	canonical.OccurredAt = event.OccurredAt.UTC()
	canonical.Inputs = make(map[string]any, len(inputs))
	for name, value := range inputs {
		canonical.Inputs[name] = value
	}
	payloadHash, err := digest(canonical)
	if err != nil {
		return nil, err
	}
	return json.Marshal(Snapshot{SchemaVersion: 1, RunID: runID, RequestKey: key, ExecutionKey: execution, PayloadHash: payloadHash, ConfigRevision: cfg.Revision(), Event: event, Inputs: inputs, Jobs: jobs, Config: cfg, Force: event.Force})
}

// digest hashes canonical JSON with stable map ordering.
func digest(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:]), nil
}
