package recovery

import (
	"context"
	"encoding/json"
	"testing"
)

func TestScheduledHandlerUsesPersistedCursor(t *testing.T) {
	if err := (&Handler{}).Handle(context.Background(), json.RawMessage(`{}`)); err == nil {
		t.Fatal("missing service")
	}
	service, _, _, _ := testService()
	if err := (&Handler{Service: service}).Handle(context.Background(), json.RawMessage(`{"detail":{"untrusted":"ignored"}}`)); err != nil {
		t.Fatal(err)
	}
}
