package recovery

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/maverickuser/data-fetch-service/internal/telemetry"
)

// Handler is the scheduled Lambda entry point for one bounded reconciliation page.
type Handler struct {
	Service   *Service
	Telemetry telemetry.Recorder
}

// Handle ignores scheduler envelope fields; the persisted cursor is authoritative.
func (h *Handler) Handle(ctx context.Context, _ json.RawMessage) error {
	if h.Service == nil {
		h.record("retry", "MISSING_DEPENDENCY")
		return fmt.Errorf("reconciler service missing")
	}
	err := h.Service.Run(ctx)
	if err != nil {
		h.record("retry", "RECONCILE_FAILED")
	} else {
		h.record("completed", "")
	}
	return err
}

// record reports the bounded page outcome without scheduler payloads.
func (h *Handler) record(outcome, code string) {
	if h.Telemetry != nil {
		h.Telemetry.Record(telemetry.Entry{Component: "reconciler", Operation: "scan", Outcome: outcome, ErrorCode: code})
	}
}
