package recovery

import (
	"context"
	"encoding/json"
	"fmt"
)

// Handler is the scheduled Lambda entry point for one bounded reconciliation page.
type Handler struct{ Service *Service }

// Handle ignores scheduler envelope fields; the persisted cursor is authoritative.
func (h *Handler) Handle(ctx context.Context, _ json.RawMessage) error {
	if h.Service == nil {
		return fmt.Errorf("reconciler service missing")
	}
	return h.Service.Run(ctx)
}
