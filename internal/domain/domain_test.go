package domain

import (
	"errors"
	"testing"
)

func TestTerminalStates(t *testing.T) {
	for _, s := range []State{Completed, SkippedUnchanged, Failed} {
		if !s.Terminal() {
			t.Errorf("%s must be terminal", s)
		}
	}
	for _, s := range []State{Queued, Pulling, DownloadsCompleted, DeliveryPending, Delivering, "unknown", ""} {
		if s.Terminal() {
			t.Errorf("%s must not be terminal", s)
		}
	}
}

func TestRequestIdentity(t *testing.T) {
	first, err := RequestKey("urn:producer", "event-1")
	if err != nil || len(first) != 64 {
		t.Fatalf("key=%q error=%v", first, err)
	}
	again, _ := RequestKey("urn:producer", "event-1")
	if first != again {
		t.Fatal("redelivery changed business identity")
	}
	pairs := [][2]string{{"urn:producer2", "event-1"}, {"urn:producer", "event-2"}, {"ab", "c"}, {"a", "bc"}}
	seen := map[string]bool{first: true}
	for _, pair := range pairs {
		key, err := RequestKey(pair[0], pair[1])
		if err != nil || seen[key] {
			t.Fatalf("collision or failure for %v", pair)
		}
		seen[key] = true
	}
	for _, pair := range [][2]string{{"", "id"}, {"source", ""}, {"  ", "id"}, {"source", "\t"}} {
		if _, err := RequestKey(pair[0], pair[1]); !errors.Is(err, ErrInvalidIdentity) {
			t.Fatalf("expected invalid identity for %v", pair)
		}
	}
}
