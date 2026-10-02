package pull

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/acquisition"
	"github.com/maverickuser/data-fetch-service/internal/config"
	"github.com/maverickuser/data-fetch-service/internal/domain"
	"github.com/maverickuser/data-fetch-service/internal/events"
	"github.com/maverickuser/data-fetch-service/internal/state"
)

type retainedReader struct {
	manifests        map[string][]byte
	readErr, statErr error
	fileKey          string
	fileSize         int64
}

func (r *retainedReader) Read(_ context.Context, key string) ([]byte, error) {
	if r.readErr != nil {
		return nil, r.readErr
	}
	data, ok := r.manifests[key]
	if !ok {
		return nil, state.ErrExpired
	}
	return data, nil
}
func (r *retainedReader) Stat(_ context.Context, key string, size int64) error {
	if r.statErr != nil {
		return r.statErr
	}
	if key != r.fileKey || size != r.fileSize {
		return state.ErrExpired
	}
	return nil
}

func retainedWorkerFixture(t *testing.T) (*Service, *memoryObjects, *publishFake, *int, *retainedReader, events.Snapshot) {
	t.Helper()
	s, objects, publisher, uploads, admitted, results := serviceFixture(t, "daily-bhavcopy", false, "")
	parent := *admitted
	parent.RunID = "parent"
	parentResults := append([]acquisition.Result(nil), results...)
	for i := range parentResults {
		parentResults[i].Artifact.Key = strings.Replace(parentResults[i].Artifact.Key, "runs/run/", "runs/parent/", 1)
	}
	original, err := BuildManifest(parent, parentResults, "artifacts")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := events.CloneForDeliveryRetry(parent, "run", "retry-key", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	var child events.Snapshot
	if err := json.Unmarshal(raw, &child); err != nil {
		t.Fatal(err)
	}
	reused, err := ReuseManifest(parent, child, original, "artifacts")
	if err != nil {
		t.Fatal(err)
	}
	oldBytes, _ := MarshalManifest(original)
	childBytes, _ := MarshalManifest(reused)
	reader := &retainedReader{manifests: map[string][]byte{"runs/parent/manifest.json": oldBytes, "runs/run/manifest.json": childBytes}, fileKey: original.Data.Files[0].Key, fileSize: original.Data.Files[0].SizeBytes}
	objects.items["runs/run/snapshot.json"] = state.Object{Data: raw, Modified: objects.clock}
	parentRaw, _ := json.Marshal(parent)
	if err := s.Repository.Create(context.Background(), "runs/parent/snapshot.json", parentRaw); err != nil {
		t.Fatal(err)
	}
	fetches := 0
	s.Fetcher = fetchFunc(func(context.Context, string, config.ResolvedJob) (acquisition.Result, error) {
		fetches++
		return acquisition.Result{}, errors.New("source called")
	})
	s.ArtifactReader = reader
	return s, objects, publisher, uploads, reader, child
}

func TestDeliveryRetryWorkerSkipsSourceAndPublishesRetainedManifest(t *testing.T) {
	s, _, publisher, uploads, reader, child := retainedWorkerFixture(t)
	if err := s.Run(context.Background(), "run", "worker", s.Now().Add(15*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if *uploads != 0 || len(publisher.messages) != 1 || publisher.messages[0] != "delivery:run" {
		t.Fatal(*uploads, publisher.messages)
	}
	current, _, err := s.Coordinator.Load(context.Background(), "coordination/"+child.ExecutionKey+".json")
	if err != nil || current.Phase != domain.DeliveryPending {
		t.Fatal(current, err)
	}
	if reader.manifests["runs/run/manifest.json"] == nil {
		t.Fatal("child manifest missing")
	}
	history, err := s.Coordinator.(*state.Coordinator).History(context.Background(), "run", "", 10)
	if err != nil || len(history.Transitions) != 4 {
		t.Fatal(history, err)
	}
	var handoff struct {
		Bucket      string `json:"bucket"`
		Key         string `json:"manifest_key"`
		Fingerprint string `json:"dataset_fingerprint"`
	}
	if err := json.Unmarshal(history.Transitions[3].Details, &handoff); err != nil || handoff.Bucket != "artifacts" || handoff.Key != "runs/run/manifest.json" || handoff.Fingerprint == "" {
		t.Fatal(handoff, err)
	}
}

func TestDeliveryRetryWorkerHandlesRetainedArtifactLossAndCorruption(t *testing.T) {
	for _, scenario := range []string{"file-expired", "old-manifest-expired", "child-manifest-expired", "parent-snapshot-expired", "old-manifest-corrupt", "child-manifest-corrupt", "parent-snapshot-corrupt", "parent-manifest-changed", "reader-missing", "stat-unavailable"} {
		t.Run(scenario, func(t *testing.T) {
			s, objects, publisher, uploads, reader, child := retainedWorkerFixture(t)
			switch scenario {
			case "file-expired":
				reader.statErr = state.ErrExpired
			case "old-manifest-expired":
				delete(reader.manifests, "runs/parent/manifest.json")
			case "child-manifest-expired":
				delete(reader.manifests, "runs/run/manifest.json")
			case "parent-snapshot-expired":
				delete(objects.items, "runs/parent/snapshot.json")
			case "old-manifest-corrupt":
				reader.manifests["runs/parent/manifest.json"] = []byte(`{`)
			case "child-manifest-corrupt":
				reader.manifests["runs/run/manifest.json"] = []byte(`{`)
			case "parent-snapshot-corrupt":
				object := objects.items["runs/parent/snapshot.json"]
				object.Data = []byte(`{`)
				objects.items["runs/parent/snapshot.json"] = object
			case "parent-manifest-changed":
				var manifest Manifest
				if err := json.Unmarshal(reader.manifests["runs/parent/manifest.json"], &manifest); err != nil {
					t.Fatal(err)
				}
				manifest.Data.DatasetFingerprint = "sha256:tampered"
				reader.manifests["runs/parent/manifest.json"], _ = json.Marshal(manifest)
			case "reader-missing":
				s.ArtifactReader = nil
			case "stat-unavailable":
				reader.statErr = errors.New("S3 unavailable")
			}
			err := s.Run(context.Background(), "run", "worker", s.Now().Add(15*time.Minute))
			current, _, readErr := s.Coordinator.Load(context.Background(), "coordination/"+child.ExecutionKey+".json")
			if readErr != nil {
				t.Fatal(readErr)
			}
			if strings.Contains(scenario, "expired") {
				if err != nil || current.Phase != domain.Failed || len(publisher.messages) != 0 {
					t.Fatal(err, current, publisher.messages)
				}
			} else if err == nil || current.Phase != domain.Pulling {
				t.Fatal(err, current)
			}
			if *uploads != 0 {
				t.Fatal("retried source or manifest upload")
			}
		})
	}
}

func TestDeliveryRetryWorkerLeavesDispatchRecoverableWhenPublisherFails(t *testing.T) {
	s, _, publisher, _, _, child := retainedWorkerFixture(t)
	publisher.err = errors.New("SQS unavailable")
	if err := s.Run(context.Background(), "run", "worker", s.Now().Add(15*time.Minute)); err == nil {
		t.Fatal("expected dispatch failure")
	}
	current, _, err := s.Coordinator.Load(context.Background(), "coordination/"+child.ExecutionKey+".json")
	if err != nil || current.Phase != domain.DeliveryPending || current.Dispatch == nil || current.Dispatch.State != "pending" {
		t.Fatal(current, err)
	}
}

func TestDeliveryRetryWorkerPreservesExpiryWhenTerminalCommitFails(t *testing.T) {
	s, _, _, _, reader, _ := retainedWorkerFixture(t)
	reader.statErr = state.ErrExpired
	s.Coordinator = failTerminalCommit{s.Coordinator}
	err := s.Run(context.Background(), "run", "worker", s.Now().Add(15*time.Minute))
	if !errors.Is(err, state.ErrExpired) {
		t.Fatal(err)
	}
}

func TestDeliveryRetryWorkerCanRetryFailedDeliveryRetry(t *testing.T) {
	s, objects, publisher, uploads, reader, _ := retainedWorkerFixture(t)
	var source events.Snapshot
	if err := json.Unmarshal(objects.items["runs/parent/snapshot.json"].Data, &source); err != nil {
		t.Fatal(err)
	}
	var sourceManifest Manifest
	if err := json.Unmarshal(reader.manifests["runs/parent/manifest.json"], &sourceManifest); err != nil {
		t.Fatal(err)
	}
	firstRaw, err := events.CloneForDeliveryRetry(source, "retry1", "first", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	var first events.Snapshot
	if err := json.Unmarshal(firstRaw, &first); err != nil {
		t.Fatal(err)
	}
	firstManifest, err := ReuseManifest(source, first, sourceManifest, "artifacts")
	if err != nil {
		t.Fatal(err)
	}
	secondRaw, err := events.CloneForDeliveryRetry(first, "run", "second", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	var second events.Snapshot
	if err := json.Unmarshal(secondRaw, &second); err != nil {
		t.Fatal(err)
	}
	secondManifest, err := ReuseManifest(first, second, firstManifest, "artifacts")
	if err != nil {
		t.Fatal(err)
	}
	if second.ParentRunID != "retry1" || second.DeliveryRetry.SourceRunID != "parent" || secondManifest.Data.Files[0].Key != sourceManifest.Data.Files[0].Key {
		t.Fatal(second, secondManifest)
	}
	firstBytes, _ := MarshalManifest(firstManifest)
	secondBytes, _ := MarshalManifest(secondManifest)
	reader.manifests["runs/retry1/manifest.json"] = firstBytes
	reader.manifests["runs/run/manifest.json"] = secondBytes
	objects.items["runs/run/snapshot.json"] = state.Object{Data: secondRaw, Modified: objects.clock}
	if err := s.Repository.Create(context.Background(), "runs/retry1/snapshot.json", firstRaw); err != nil {
		t.Fatal(err)
	}
	if err := s.Run(context.Background(), "run", "worker", s.Now().Add(15*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if *uploads != 0 || len(publisher.messages) != 1 || publisher.messages[0] != "delivery:run" {
		t.Fatal(*uploads, publisher.messages)
	}
}
