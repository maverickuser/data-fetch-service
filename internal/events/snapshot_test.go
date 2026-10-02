package events

import (
	"encoding/json"
	"github.com/maverickuser/data-fetch-service/internal/config"
	"os"
	"reflect"
	"testing"
	"time"
)

func readSnapshot(t *testing.T, c config.Config, e Normalized) Snapshot {
	t.Helper()
	data, err := BuildSnapshot(c, e, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestDeliveryRetrySnapshotRequiresAdmittedParent(t *testing.T) {
	if _, err := CloneForDeliveryRetry(Snapshot{}, "child", "retry", nil, ""); err == nil {
		t.Fatal("accepted empty source snapshot")
	}
}

func TestScheduledGoldenAndRetryAfterMidnight(t *testing.T) {
	raw, err := os.ReadFile("testdata/bse-scheduled.json")
	if err != nil {
		t.Fatal(err)
	}
	mapping := []RuleMapping{{RuleARN: "arn:aws:events:ap-south-1:123456789012:rule/bse", EventType: "daily-bhavcopy", Inputs: map[string]any{"exchangeName": "BSE"}}}
	event, err := ParseTransport(raw, mapping)
	if err != nil {
		t.Fatal(err)
	}
	c := eventConfig(t)
	snapshot := readSnapshot(t, c, event)
	expected, err := os.ReadFile("testdata/bse-job.json")
	if err != nil {
		t.Fatal(err)
	}
	var jobs []config.ResolvedJob
	if err := json.Unmarshal(expected, &jobs); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(snapshot.Jobs, jobs) {
		t.Fatalf("golden mismatch: %+v", snapshot.Jobs)
	}
	// No wall-clock dependency: re-admission after midnight uses original event time.
	replay := readSnapshot(t, c, event)
	if replay.ExecutionKey != snapshot.ExecutionKey || replay.Inputs["run_date"] != "2026-09-21" {
		t.Fatal("logical date changed")
	}
	// An actual next-day logical event does select the next day's archive.
	event.OccurredAt = event.OccurredAt.Add(24 * time.Hour)
	next := readSnapshot(t, c, event)
	if next.Jobs[0].Filename != "BSE_fgroup22092026.csv" {
		t.Fatal(next.Jobs)
	}
}

func TestEquivalentISINAndTimezoneHaveSameIdentity(t *testing.T) {
	c := eventConfig(t)
	event := Normalized{Source: "urn:test", EventID: "1", Type: PullRequested, EventType: "nsdl-bond-data", OccurredAt: time.Date(2026, 9, 21, 14, 30, 0, 0, time.UTC), Inputs: map[string]any{"isin_code": " ine121a07qy9 "}}
	first := readSnapshot(t, c, event)
	event.Inputs["isin_code"] = "INE121A07QY9"
	event.OccurredAt = event.OccurredAt.In(time.FixedZone("IST", 19800))
	second := readSnapshot(t, c, event)
	if first.PayloadHash != second.PayloadHash || first.ExecutionKey != second.ExecutionKey {
		t.Fatal("equivalent normalized input changed identity")
	}
	c.DeploymentCommit = "new-version"
	third := readSnapshot(t, c, event)
	if third.ExecutionKey != second.ExecutionKey || third.ConfigRevision == second.ConfigRevision {
		t.Fatal("config revision must not change execution identity")
	}
}
