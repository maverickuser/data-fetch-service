package admission

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/acquisition"
	"github.com/maverickuser/data-fetch-service/internal/domain"
	"github.com/maverickuser/data-fetch-service/internal/events"
	"github.com/maverickuser/data-fetch-service/internal/pull"
	"github.com/maverickuser/data-fetch-service/internal/state"
)

type deliveryArtifacts struct {
	manifest                    []byte
	fileKey                     string
	fileSize                    int64
	readErr, statErr, uploadErr error
	uploadKey                   string
	uploaded                    []byte
	wrongUpload                 bool
}

func (a *deliveryArtifacts) Read(_ context.Context, key string) ([]byte, error) {
	if a.readErr != nil {
		return nil, a.readErr
	}
	if key != "runs/parent/manifest.json" {
		return nil, state.ErrNotFound
	}
	return a.manifest, nil
}
func (a *deliveryArtifacts) Stat(_ context.Context, key string, size int64) error {
	if a.statErr != nil {
		return a.statErr
	}
	if key != a.fileKey || size != a.fileSize {
		return state.ErrExpired
	}
	return nil
}
func (a *deliveryArtifacts) Upload(_ context.Context, key string, reader io.Reader) (acquisition.Artifact, error) {
	if a.uploadErr != nil {
		return acquisition.Artifact{}, a.uploadErr
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return acquisition.Artifact{}, err
	}
	a.uploadKey = key
	a.uploaded = data
	hash := sha256.Sum256(data)
	if a.wrongUpload {
		return acquisition.Artifact{Key: "wrong", Bytes: int64(len(data)), SHA256: hex.EncodeToString(hash[:])}, nil
	}
	return acquisition.Artifact{Key: key, Bytes: int64(len(data)), SHA256: hex.EncodeToString(hash[:])}, nil
}

type acceptanceRecords struct {
	data []byte
	err  error
}

func (r acceptanceRecords) Read(context.Context, string, time.Time) (state.Object, error) {
	return state.Object{Data: r.data}, r.err
}

func deliveryFixture(t *testing.T) (*Service, *repositoryFake, *deliveryArtifacts, events.Snapshot, pull.Manifest) {
	t.Helper()
	s, repo := serviceFixture(t)
	parentRaw, err := events.BuildSnapshot(s.Config, events.Normalized{EventID: "source", Source: "urn:test", Type: events.PullRequested, OccurredAt: s.Now(), EventType: "daily-bhavcopy", Inputs: map[string]any{"exchangeName": "BSE"}}, "parent")
	if err != nil {
		t.Fatal(err)
	}
	var parent events.Snapshot
	if err := json.Unmarshal(parentRaw, &parent); err != nil {
		t.Fatal(err)
	}
	repo.parentView = state.RunView{RunID: "parent", Phase: domain.Failed, LatestRunID: "parent", LatestPhase: domain.Failed, CreatedAt: s.Now(), Snapshot: parentRaw}
	repo.historyPage = state.HistoryPage{Transitions: []state.Transition{{Phase: domain.Failed, Details: json.RawMessage(`{"stage":"delivery"}`)}}}
	data := []byte("csv rows")
	hash := sha256.Sum256(data)
	fileKey := "runs/parent/raw/" + parent.Jobs[0].ID + "/1/" + parent.Jobs[0].Filename
	manifest, err := pull.BuildManifest(parent, []acquisition.Result{{JobID: parent.Jobs[0].ID, Filename: parent.Jobs[0].Filename, Format: parent.Jobs[0].Format, Artifact: acquisition.Artifact{Key: fileKey, Bytes: int64(len(data)), SHA256: hex.EncodeToString(hash[:])}}}, "artifacts")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := pull.MarshalManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	artifacts := &deliveryArtifacts{manifest: encoded, fileKey: fileKey, fileSize: int64(len(data))}
	s.Artifacts = artifacts
	s.ManifestStorage = artifacts
	s.ArtifactBucket = "artifacts"
	s.Records = acceptanceRecords{}
	s.NewID = func() string { return "child" }
	return s, repo, artifacts, parent, manifest
}

func TestDeliveryRetryPinsFilesAndExplicitProcessorChoice(t *testing.T) {
	for _, current := range []bool{false, true} {
		s, repo, artifacts, parent, original := deliveryFixture(t)
		if current {
			s.Config.Processor.Enabled = true
			s.Config.Processor.URL = "https://processor.internal/v1/event-ingestions"
		}
		result, err := s.DeliveryRetry(context.Background(), "parent", "retry-key", current)
		if err != nil || result.RunID != "child" || result.Reused || repo.captured.CandidateRunID != "child" {
			t.Fatal(result, err)
		}
		var child events.Snapshot
		if err := json.Unmarshal(repo.captured.Snapshot, &child); err != nil {
			t.Fatal(err)
		}
		if child.ParentRunID != "parent" || child.ExecutionKey != parent.ExecutionKey || child.DeliveryRetry == nil || child.DeliveryRetry.UseCurrentProcessor != current || child.DeliveryRetry.OldConfigRevision != parent.ConfigRevision || child.DeliveryRetry.NewConfigRevision != child.ConfigRevision {
			t.Fatal(child)
		}
		if current && child.Config.Processor.URL != s.Config.Processor.URL || !current && child.Config.Processor.URL != parent.Config.Processor.URL {
			t.Fatal(child.Config.Processor.URL)
		}
		if artifacts.uploadKey != "runs/child/manifest.json" {
			t.Fatal(artifacts.uploadKey)
		}
		var manifest pull.Manifest
		if err := json.Unmarshal(artifacts.uploaded, &manifest); err != nil {
			t.Fatal(err)
		}
		if manifest.Data.RunID != "child" || manifest.Data.Files[0].Key != original.Data.Files[0].Key || manifest.Data.EventID != "retry-key" {
			t.Fatal(manifest)
		}
		if !current && manifest.Data.DatasetFingerprint != original.Data.DatasetFingerprint {
			t.Fatal("source fingerprint changed")
		}
		if current && manifest.Data.DatasetFingerprint == original.Data.DatasetFingerprint {
			t.Fatal("processor destination did not affect fingerprint")
		}
	}
}

func TestDeliveryRetryValidatesArtifactsAndAcceptanceBeforeAdmission(t *testing.T) {
	for _, scenario := range []string{"manifest-missing", "manifest-invalid", "file-expired", "source-mismatch", "newer-accepted", "older-accepted", "acceptance-missing", "acceptance-corrupt", "upload-failed", "upload-mismatch", "current-config-invalid", "candidate-invalid"} {
		t.Run(scenario, func(t *testing.T) {
			s, repo, artifacts, parent, manifest := deliveryFixture(t)
			wantSuccess := scenario == "older-accepted"
			useCurrent := false
			switch scenario {
			case "manifest-missing":
				artifacts.readErr = state.ErrExpired
			case "manifest-invalid":
				artifacts.manifest = []byte(`{`)
			case "file-expired":
				artifacts.statErr = state.ErrExpired
			case "source-mismatch":
				manifest.Data.DatasetFingerprint = "sha256:tampered"
				artifacts.manifest, _ = json.Marshal(manifest)
			case "newer-accepted", "older-accepted", "acceptance-missing", "acceptance-corrupt":
				accepted := state.Acceptance{RunID: "other", Fingerprint: "sha256:other", AcceptedAt: s.Now().Add(time.Minute)}
				if scenario == "older-accepted" {
					accepted.AcceptedAt = s.Now().Add(-time.Hour)
				}
				repo.coord.AcceptedBaseline = &state.Baseline{RunID: "other", Fingerprint: accepted.Fingerprint, AcceptanceKey: "acceptance/execution/other.json"}
				raw, _ := json.Marshal(accepted)
				s.Records = acceptanceRecords{data: raw}
				if scenario == "acceptance-missing" {
					s.Records = acceptanceRecords{err: state.ErrNotFound}
				}
				if scenario == "acceptance-corrupt" {
					s.Records = acceptanceRecords{data: []byte(`{`)}
				}
			case "upload-failed":
				artifacts.uploadErr = errors.New("S3 unavailable")
			case "upload-mismatch":
				artifacts.wrongUpload = true
			case "current-config-invalid":
				s.Config.Processor.URL = "http://invalid"
				useCurrent = true
			case "candidate-invalid":
				s.NewID = func() string { return parent.RunID }
			}
			result, err := s.DeliveryRetry(context.Background(), "parent", "retry-key", useCurrent)
			if wantSuccess {
				if err != nil || result.RunID != "child" {
					t.Fatal(result, err)
				}
			} else if err == nil || repo.attempts != 0 {
				t.Fatal(result, err, repo.attempts)
			}
		})
	}
}

func TestDeliveryRetryReplayKeepsOriginalBytes(t *testing.T) {
	s, repo, artifacts, _, _ := deliveryFixture(t)
	first, err := s.DeliveryRetry(context.Background(), "parent", "retry-key", false)
	if err != nil {
		t.Fatal(err)
	}
	repo.existing = repo.captured
	artifacts.readErr = state.ErrExpired
	s.Config.Events = nil
	second, err := s.DeliveryRetry(context.Background(), "parent", "retry-key", false)
	if err != nil || !second.Reused || second.RunID != first.RunID || !bytes.Equal(repo.captured.Snapshot, repo.existing.Snapshot) {
		t.Fatal(second, err)
	}
}
