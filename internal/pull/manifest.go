// Package pull coordinates complete acquired datasets and processor handoff intents.
package pull

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/acquisition"
	"github.com/maverickuser/data-fetch-service/internal/events"
)

// File is one complete raw artifact the processor may read.
type File struct {
	JobID              string `json:"job_id"`
	Bucket             string `json:"bucket"`
	Key                string `json:"key"`
	Format             string `json:"format"`
	SHA256             string `json:"sha256"`
	SizeBytes          int64  `json:"size_bytes"`
	SourceURL          string `json:"source_url"`
	SelectedMemberPath string `json:"selected_member_path,omitempty"`
}

// ManifestData carries the complete processor input and its accepted-baseline candidate.
type ManifestData struct {
	SchemaVersion      int               `json:"schema_version"`
	EventType          string            `json:"event_type"`
	EventID            string            `json:"event_id"`
	RunID              string            `json:"run_id"`
	Inputs             map[string]string `json:"inputs"`
	ConfigRevision     string            `json:"config_revision"`
	DatasetFingerprint string            `json:"dataset_fingerprint"`
	Files              []File            `json:"files"`
}

// Manifest is the CloudEvents 1.0 artifact submitted by location to processing.
type Manifest struct {
	SpecVersion     string       `json:"specversion"`
	ID              string       `json:"id"`
	Source          string       `json:"source"`
	Type            string       `json:"type"`
	DataSchema      string       `json:"dataschema"`
	Time            time.Time    `json:"time"`
	Subject         string       `json:"subject"`
	DataContentType string       `json:"datacontenttype"`
	Data            ManifestData `json:"data"`
}

type fingerprintJob struct {
	JobID  string `json:"job_id"`
	URL    string `json:"resolved_url"`
	Member string `json:"selected_member_path"`
	Format string `json:"format"`
	SHA256 string `json:"raw_sha256"`
}
type fingerprintData struct {
	DataSchema   string            `json:"dataschema"`
	EventType    string            `json:"event_type"`
	Inputs       map[string]string `json:"inputs"`
	ProcessorURL string            `json:"processor_url"`
	Jobs         []fingerprintJob  `json:"jobs"`
}

// BuildManifest requires exactly one validated processing artifact per configured job.
// Its fingerprint excludes object paths, run time, retries, and ZIP archive metadata.
func BuildManifest(snapshot events.Snapshot, results []acquisition.Result, bucket string) (Manifest, error) {
	if snapshot.SchemaVersion != 1 || snapshot.RunID == "" || snapshot.Event.EventID == "" || snapshot.Event.OccurredAt.IsZero() || snapshot.ConfigRevision == "" || bucket == "" || len(snapshot.Jobs) == 0 || len(results) != len(snapshot.Jobs) {
		return Manifest{}, fmt.Errorf("incomplete dataset snapshot or results")
	}
	def, ok := snapshot.Config.Event(snapshot.Event.EventType)
	if !ok {
		return Manifest{}, fmt.Errorf("unconfigured dataset type")
	}
	inputs, subject, err := manifestInputs(snapshot)
	if err != nil {
		return Manifest{}, err
	}
	manifest := Manifest{SpecVersion: "1.0", ID: "urn:bond-platform:manifest:" + snapshot.RunID, Source: "urn:bond-platform:service:data-fetch-service", Type: "com.bondplatform.dataset.manifest.v1", DataSchema: def.DatasetType, Time: snapshot.Event.OccurredAt.UTC(), Subject: subject, DataContentType: "application/json", Data: ManifestData{SchemaVersion: 1, EventType: snapshot.Event.EventType, EventID: snapshot.Event.EventID, RunID: snapshot.RunID, Inputs: inputs, ConfigRevision: snapshot.ConfigRevision, Files: make([]File, 0, len(results))}}
	byJob := make(map[string]acquisition.Result, len(results))
	for _, result := range results {
		if result.JobID == "" || byJob[result.JobID].JobID != "" {
			return Manifest{}, fmt.Errorf("duplicate or unnamed job result")
		}
		byJob[result.JobID] = result
	}
	fp := fingerprintData{DataSchema: def.DatasetType, EventType: snapshot.Event.EventType, Inputs: inputs, ProcessorURL: snapshot.Config.Processor.URL, Jobs: make([]fingerprintJob, 0, len(results))}
	for _, job := range snapshot.Jobs {
		result, ok := byJob[job.ID]
		if !ok || result.Format != job.Format || result.Artifact.Bytes < 1 || !validHash(result.Artifact.SHA256) || !strings.HasPrefix(result.Artifact.Key, "runs/"+snapshot.RunID+"/raw/"+job.ID+"/") || result.Filename == "" {
			return Manifest{}, fmt.Errorf("missing or invalid job artifact %s", job.ID)
		}
		if job.Filename != "" && job.Filename != result.Filename {
			return Manifest{}, fmt.Errorf("artifact filename differs from admitted job")
		}
		file := File{JobID: job.ID, Bucket: bucket, Key: result.Artifact.Key, Format: job.Format, SHA256: result.Artifact.SHA256, SizeBytes: result.Artifact.Bytes, SourceURL: job.URL, SelectedMemberPath: job.MemberPath}
		manifest.Data.Files = append(manifest.Data.Files, file)
		fp.Jobs = append(fp.Jobs, fingerprintJob{JobID: job.ID, URL: job.URL, Member: job.MemberPath, Format: job.Format, SHA256: result.Artifact.SHA256})
	}
	sort.Slice(manifest.Data.Files, func(i, j int) bool { return manifest.Data.Files[i].JobID < manifest.Data.Files[j].JobID })
	sort.Slice(fp.Jobs, func(i, j int) bool { return fp.Jobs[i].JobID < fp.Jobs[j].JobID })
	canonical, err := json.Marshal(fp)
	if err != nil {
		return Manifest{}, err
	}
	hash := sha256.Sum256(canonical)
	manifest.Data.DatasetFingerprint = "sha256:" + hex.EncodeToString(hash[:])
	return manifest, nil
}

// manifestInputs pins processor-facing values to the admission-time logical inputs.
func manifestInputs(snapshot events.Snapshot) (map[string]string, string, error) {
	switch snapshot.Event.EventType {
	case "daily-bhavcopy":
		exchange, date := snapshot.Inputs["exchangeName"], snapshot.Inputs["run_date"]
		if exchange == "" || date == "" {
			return nil, "", fmt.Errorf("missing BSE exchange or trade date")
		}
		parsed, err := time.Parse("2006-01-02", date)
		if err != nil || parsed.Format("2006-01-02") != date {
			return nil, "", fmt.Errorf("invalid BSE trade date")
		}
		if len(snapshot.Jobs) != 1 || snapshot.Jobs[0].Filename != exchange+"_fgroup"+parsed.Format("02012006")+".csv" {
			return nil, "", fmt.Errorf("BSE file does not match manifest inputs")
		}
		return map[string]string{"exchangeName": exchange, "tradeDate": date}, "exchange/" + exchange + "/trade-date/" + date, nil
	case "nsdl-bond-data":
		isin := snapshot.Inputs["isin_code"]
		if isin == "" || len(snapshot.Jobs) != 6 {
			return nil, "", fmt.Errorf("incomplete NSDL dataset")
		}
		for _, job := range snapshot.Jobs {
			if !strings.HasPrefix(job.Filename, isin+"_") || job.Format != "json" {
				return nil, "", fmt.Errorf("NSDL filename does not match ISIN")
			}
		}
		return map[string]string{"isin_code": isin}, "isin/" + isin, nil
	default:
		return nil, "", fmt.Errorf("unsupported dataset event type")
	}
}

// validHash accepts lowercase raw SHA-256 digests only.
func validHash(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		if c < '0' || c > '9' && c < 'a' || c > 'f' {
			return false
		}
	}
	return true
}

// MarshalManifest encodes the complete CloudEvent for immutable artifact storage.
func MarshalManifest(manifest Manifest) ([]byte, error) { return json.Marshal(manifest) }
