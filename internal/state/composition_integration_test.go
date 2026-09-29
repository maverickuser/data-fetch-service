//go:build integration

package state

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/config"
	"github.com/maverickuser/data-fetch-service/internal/domain"
	"github.com/maverickuser/data-fetch-service/internal/events"
)

func TestStateCompositionBSEAndNSDL(t *testing.T) {
	for _, eventType := range []string{"daily-bhavcopy", "nsdl-bond-data"} {
		t.Run(eventType, func(t *testing.T) {
			ctx := context.Background()
			m := newMemory()
			c := NewCoordinator(New(m), func() time.Time { return m.now })
			raw, err := os.ReadFile("../../config/events.yaml")
			if err != nil {
				t.Fatal(err)
			}
			cfg, err := config.Load(raw)
			if err != nil {
				t.Fatal(err)
			}
			cfg.DeploymentCommit = "integration-test"
			inputs := map[string]any{"exchangeName": "BSE"}
			if eventType == "nsdl-bond-data" {
				inputs = map[string]any{"isin_code": " ine121a07qy9 "}
			}
			business, err := json.Marshal(events.Data{SchemaVersion: 1, EventType: eventType, Inputs: inputs})
			if err != nil {
				t.Fatal(err)
			}
			envelope, err := json.Marshal(events.CloudEvent{SpecVersion: "1.0", ID: "event", Source: "urn:test", Type: events.PullRequested, Time: time.Date(2026, 9, 21, 14, 30, 0, 0, time.UTC), DataContentType: "application/json", Data: business})
			if err != nil {
				t.Fatal(err)
			}
			event, err := events.ParseCloudEvent(envelope)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := events.BuildSnapshot(cfg, event, "run")
			if err != nil {
				t.Fatal(err)
			}
			request, err := NewRequest(snapshot, m.now)
			if err != nil {
				t.Fatal(err)
			}
			receipt := admitUntilResolved(t, c, request)
			key := "coordination/" + receipt.ExecutionKey + ".json"
			queue := &queueFake{}
			if err := c.PublishPending(ctx, key, queue); err != nil {
				t.Fatal(err)
			}
			if len(queue.messages) != 1 || queue.messages[0].RunID != "run" || queue.messages[0].Queue != "pull" {
				t.Fatal(queue.messages)
			}
			lease, err := c.ClaimDispatched(ctx, key, "run", "pull", "worker", m.now.Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Commit(ctx, key, lease, Transition{RunID: "run", Sequence: 2, Phase: domain.Pulling, At: m.now}); err != nil {
				t.Fatal(err)
			}
			view, err := c.ReadRun(ctx, "run")
			if err != nil || view.Phase != domain.Pulling {
				t.Fatal(view, err)
			}
			var saved events.Snapshot
			if err := json.Unmarshal(view.Snapshot, &saved); err != nil {
				t.Fatal(err)
			}
			if eventType == "daily-bhavcopy" {
				if saved.Jobs[0].Filename != "BSE_fgroup21092026.csv" || saved.Inputs["run_date"] != "2026-09-21" {
					t.Fatal(saved.Jobs, saved.Inputs)
				}
			} else if len(saved.Jobs) != 6 || saved.Inputs["isin_code"] != "INE121A07QY9" {
				t.Fatal(saved.Jobs, saved.Inputs)
			}
			replay := admitUntilResolved(t, c, request)
			if replay != receipt {
				t.Fatal(replay, receipt)
			}
			if err := c.PublishPending(ctx, key, queue); err != nil || len(queue.messages) != 1 {
				t.Fatal("replay republished claimed dispatch", err)
			}
		})
	}
}
