package recovery

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/maverickuser/data-fetch-service/internal/telemetry"
	"strings"
	"testing"
)

func TestScheduledHandlerUsesPersistedCursor(t *testing.T) {
	var logs bytes.Buffer
	if err := (&Handler{Telemetry: &telemetry.JSON{Output: &logs}}).Handle(context.Background(), json.RawMessage(`{}`)); err == nil {
		t.Fatal("missing service")
	}
	service, _, _, _ := testService()
	if err := (&Handler{Service: service, Telemetry: &telemetry.JSON{Output: &logs}}).Handle(context.Background(), json.RawMessage(`{"detail":{"untrusted":"ignored"}}`)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), `"outcome":"retry"`) || !strings.Contains(logs.String(), `"outcome":"completed"`) || strings.Contains(logs.String(), "untrusted") {
		t.Fatal(logs.String())
	}
}
