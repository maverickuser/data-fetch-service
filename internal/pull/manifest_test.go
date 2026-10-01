package pull

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/acquisition"
	"github.com/maverickuser/data-fetch-service/internal/config"
	"github.com/maverickuser/data-fetch-service/internal/events"
)

func manifestFixture(t *testing.T, eventType string) (events.Snapshot, []acquisition.Result) {
	t.Helper()
	raw, err := os.ReadFile("../../config/events.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(raw)
	if err != nil {
		t.Fatal(err)
	}
	occur := time.Date(2026, 9, 21, 14, 30, 0, 0, time.UTC)
	input := map[string]any{"exchangeName": "BSE"}
	if eventType == "nsdl-bond-data" {
		input = map[string]any{"isin_code": "ine121a07qy9"}
	}
	normalized := events.Normalized{EventID: "evt", Source: "urn:test", Type: events.PullRequested, OccurredAt: occur, EventType: eventType, Inputs: input}
	data, err := events.BuildSnapshot(cfg, normalized, "run")
	if err != nil {
		t.Fatal(err)
	}
	var snapshot events.Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte("raw"))
	results := make([]acquisition.Result, 0, len(snapshot.Jobs))
	for _, job := range snapshot.Jobs {
		results = append(results, acquisition.Result{JobID: job.ID, Filename: job.Filename, Format: job.Format, Artifact: acquisition.Artifact{Key: "runs/run/raw/" + job.ID + "/1/" + job.Filename, Bytes: 3, SHA256: hex.EncodeToString(hash[:])}})
	}
	return snapshot, results
}

func TestManifestBSEAndNSDLCompleteFileSets(t *testing.T) {
	for _, test := range []struct {
		event   string
		files   int
		subject string
		inputs  map[string]string
	}{{"daily-bhavcopy", 1, "exchange/BSE/trade-date/2026-09-21", map[string]string{"exchangeName": "BSE", "tradeDate": "2026-09-21"}}, {"nsdl-bond-data", 6, "isin/INE121A07QY9", map[string]string{"isin_code": "INE121A07QY9"}}} {
		snapshot, results := manifestFixture(t, test.event)
		manifest, err := BuildManifest(snapshot, results, "artifacts")
		if err != nil {
			t.Fatal(err)
		}
		if manifest.Subject != test.subject || manifest.DataSchema == "" || manifest.Data.DatasetFingerprint[:7] != "sha256:" || len(manifest.Data.Files) != test.files || manifest.Data.Inputs["run_date"] != "" {
			t.Fatal(manifest)
		}
		for name, value := range test.inputs {
			if manifest.Data.Inputs[name] != value {
				t.Fatal(manifest.Data.Inputs)
			}
		}
		if test.event == "daily-bhavcopy" {
			if manifest.Data.Files[0].SelectedMemberPath != "fgroup21092026.csv" || !strings.HasSuffix(manifest.Data.Files[0].Key, "/BSE_fgroup21092026.csv") {
				t.Fatal(manifest)
			}
		}
		encoded, err := MarshalManifest(manifest)
		if err != nil || !json.Valid(encoded) {
			t.Fatal(err)
		}
	}
}

func TestFingerprintIgnoresArchiveMetadataAndObjectPaths(t *testing.T) {
	snapshot, results := manifestFixture(t, "daily-bhavcopy")
	baseline, err := BuildManifest(snapshot, results, "artifacts")
	if err != nil {
		t.Fatal(err)
	}
	changed := append([]acquisition.Result(nil), results...)
	changed[0].Artifact.Key = "runs/run/raw/debt-bhavcopy/2/BSE_fgroup21092026.csv"
	changed[0].Archive = &acquisition.Artifact{Key: "archive", SHA256: "changed", Bytes: 10}
	same, err := BuildManifest(snapshot, changed, "artifacts")
	if err != nil || same.Data.DatasetFingerprint != baseline.Data.DatasetFingerprint {
		t.Fatal(err, same)
	}
	changed[0].Artifact.SHA256 = strings.Repeat("a", 64)
	different, err := BuildManifest(snapshot, changed, "artifacts")
	if err != nil || different.Data.DatasetFingerprint == baseline.Data.DatasetFingerprint {
		t.Fatal(err)
	}
	snapshot.Config.Processor.URL = "https://other.example/v1"
	changedDestination, err := BuildManifest(snapshot, results, "artifacts")
	if err != nil || changedDestination.Data.DatasetFingerprint == baseline.Data.DatasetFingerprint {
		t.Fatal(err)
	}
	snapshot, results = manifestFixture(t, "daily-bhavcopy")
	snapshot.Force = true
	snapshot.RunID = "next"
	results[0].Artifact.Key = strings.Replace(results[0].Artifact.Key, "runs/run/", "runs/next/", 1)
	same, err = BuildManifest(snapshot, results, "artifacts")
	if err != nil || same.Data.DatasetFingerprint != baseline.Data.DatasetFingerprint {
		t.Fatal(err)
	}
}

func TestManifestRejectsIncompleteOrInconsistentArtifacts(t *testing.T) {
	snapshot, results := manifestFixture(t, "nsdl-bond-data")
	if _, err := BuildManifest(snapshot, results[:5], "artifacts"); err == nil {
		t.Fatal("partial set accepted")
	}
	for _, mutate := range []func(*events.Snapshot, []acquisition.Result){
		func(_ *events.Snapshot, r []acquisition.Result) { r[0].JobID = r[1].JobID },
		func(_ *events.Snapshot, r []acquisition.Result) { r[0].Artifact.SHA256 = "not-a-hash" },
		func(_ *events.Snapshot, r []acquisition.Result) { r[0].Artifact.Bytes = 0 },
		func(_ *events.Snapshot, r []acquisition.Result) { r[0].Filename = "other.json" },
		func(s *events.Snapshot, _ []acquisition.Result) { s.Inputs["isin_code"] = "" },
		func(s *events.Snapshot, _ []acquisition.Result) { s.Jobs[0].Filename = "wrong.json" },
	} {
		s, r := manifestFixture(t, "nsdl-bond-data")
		mutate(&s, r)
		if _, err := BuildManifest(s, r, "artifacts"); err == nil {
			t.Fatal("inconsistent manifest accepted")
		}
	}
	bse, r := manifestFixture(t, "daily-bhavcopy")
	bse.Inputs["run_date"] = "2026-09-22"
	if _, err := BuildManifest(bse, r, "artifacts"); err == nil {
		t.Fatal("BSE date mismatch")
	}
}

func TestFingerprintChangesForTradingInputsAndSourceContract(t *testing.T) {
	snapshot, results := manifestFixture(t, "daily-bhavcopy")
	baseline, err := BuildManifest(snapshot, results, "artifacts")
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Inputs["run_date"] = "2026-09-22"
	snapshot.Jobs[0].Filename = "BSE_fgroup22092026.csv"
	results[0].Filename = snapshot.Jobs[0].Filename
	changed, err := BuildManifest(snapshot, results, "artifacts")
	if err != nil || changed.Data.DatasetFingerprint == baseline.Data.DatasetFingerprint || changed.Data.Inputs["tradeDate"] != "2026-09-22" {
		t.Fatal(changed, err)
	}
	snapshot, results = manifestFixture(t, "daily-bhavcopy")
	snapshot.Jobs[0].URL = "https://new.example/archive.zip"
	changed, err = BuildManifest(snapshot, results, "artifacts")
	if err != nil || changed.Data.DatasetFingerprint == baseline.Data.DatasetFingerprint {
		t.Fatal(changed, err)
	}
}

func TestManifestRejectsBadDatasetDefinitions(t *testing.T) {
	snapshot, results := manifestFixture(t, "daily-bhavcopy")
	if _, err := BuildManifest(snapshot, results, ""); err == nil {
		t.Fatal("missing bucket accepted")
	}
	for _, mutate := range []func(*events.Snapshot, []acquisition.Result){
		func(s *events.Snapshot, _ []acquisition.Result) { s.SchemaVersion = 2 },
		func(s *events.Snapshot, _ []acquisition.Result) { s.Event.EventType = "other" },
		func(s *events.Snapshot, _ []acquisition.Result) { s.Inputs["run_date"] = "2026-02-30" },
		func(s *events.Snapshot, _ []acquisition.Result) { delete(s.Inputs, "exchangeName") },
		func(s *events.Snapshot, _ []acquisition.Result) { s.Jobs[0].Filename = "wrong.csv" },
		func(_ *events.Snapshot, r []acquisition.Result) { r[0].Artifact.SHA256 = strings.Repeat("A", 64) },
		func(_ *events.Snapshot, r []acquisition.Result) { r[0].Format = "json" },
		func(_ *events.Snapshot, r []acquisition.Result) { r[0].Artifact.Key = "runs/other/raw/job/1/file.csv" },
	} {
		snapshot, results := manifestFixture(t, "daily-bhavcopy")
		mutate(&snapshot, results)
		if _, err := BuildManifest(snapshot, results, "artifacts"); err == nil {
			t.Fatal("bad dataset accepted")
		}
	}
	for _, mutate := range []func(*events.Snapshot){
		func(s *events.Snapshot) { delete(s.Inputs, "isin_code") },
		func(s *events.Snapshot) { s.Jobs = s.Jobs[:5] },
		func(s *events.Snapshot) { s.Jobs[0].Filename = "wrong.json" },
	} {
		snapshot, results := manifestFixture(t, "nsdl-bond-data")
		mutate(&snapshot)
		if _, err := BuildManifest(snapshot, results, "artifacts"); err == nil {
			t.Fatal("invalid NSDL manifest accepted")
		}
	}
}
