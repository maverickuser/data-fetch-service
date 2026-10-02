package telemetry

import (
	"context"
	"encoding/json"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/state"
)

// SnapshotReader reads pinned run metadata without participating in its state transitions.
type SnapshotReader interface {
	Read(context.Context, string, time.Time) (state.Object, error)
}

// Correlation resolves a run's root identity; telemetry lookup failures never affect work.
func Correlation(ctx context.Context, reader SnapshotReader, runID string) string {
	if reader == nil || runID == "" || runID == "." || runID == ".." || safe(runID) != runID {
		return ""
	}
	if ctx.Err() != nil {
		return ""
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < 250*time.Millisecond {
		return ""
	}
	lookupCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	object, err := reader.Read(lookupCtx, "runs/"+runID+"/snapshot.json", time.Now())
	if err != nil {
		return ""
	}
	var snapshot struct {
		RunID         string `json:"run_id"`
		CorrelationID string `json:"correlation_id"`
	}
	if json.Unmarshal(object.Data, &snapshot) != nil || snapshot.RunID != runID {
		return ""
	}
	if snapshot.CorrelationID == "" {
		return runID // Snapshots written before correlation was introduced remain readable.
	}
	return safe(snapshot.CorrelationID)
}
