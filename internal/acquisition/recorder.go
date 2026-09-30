package acquisition

import (
	"context"
	"encoding/json"
	"fmt"
)

// Records creates immutable attempt evidence, verifying identical collisions in its adapter.
type Records interface {
	Create(context.Context, string, []byte) error
}

// StateRecorder stores started and finished attempts under the run's canonical state prefix.
type StateRecorder struct{ Store Records }

// Record validates path components and persists one immutable source-attempt phase.
func (r StateRecorder) Record(ctx context.Context, runID, jobID string, number int, phase string, attempt Attempt) error {
	if r.Store == nil || !safeID(runID) || !safeID(jobID) || number < 1 || number != attempt.Number || (phase != "started" && phase != "finished") {
		return fmt.Errorf("invalid attempt record")
	}
	data, err := json.Marshal(attempt)
	if err != nil {
		return err
	}
	return r.Store.Create(ctx, fmt.Sprintf("runs/%s/pulls/%s/attempts/%d/%s.json", runID, jobID, number, phase), data)
}
