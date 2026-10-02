// Package delivery submits immutable dataset manifests to the processing service.
package delivery

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/pull"
)

// Reference identifies the exact stored manifest sent to the processor.
type Reference struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

// SubmissionData is the processor's admission payload, without artifact file details.
type SubmissionData struct {
	SchemaVersion      int               `json:"schema_version"`
	EventType          string            `json:"event_type"`
	EventID            string            `json:"event_id"`
	RunID              string            `json:"run_id"`
	Inputs             map[string]string `json:"inputs"`
	Manifest           Reference         `json:"manifest"`
	DatasetFingerprint string            `json:"dataset_fingerprint"`
}

// Submission is a distinct CloudEvent whose identity and time remain stable on replay.
type Submission struct {
	SpecVersion     string         `json:"specversion"`
	ID              string         `json:"id"`
	Source          string         `json:"source"`
	Type            string         `json:"type"`
	DataSchema      string         `json:"dataschema"`
	Time            time.Time      `json:"time"`
	Subject         string         `json:"subject"`
	DataContentType string         `json:"datacontenttype"`
	Data            SubmissionData `json:"data"`
}

// BuildSubmission validates manifest metadata against the processor's dataset profile.
func BuildSubmission(manifest pull.Manifest, ref Reference) ([]byte, error) {
	if !validRunID(manifest.Data.RunID) || ref.Bucket == "" || ref.Key != "runs/"+manifest.Data.RunID+"/manifest.json" || manifest.SpecVersion != "1.0" || manifest.ID != "urn:bond-platform:manifest:"+manifest.Data.RunID || manifest.Source != "urn:bond-platform:service:data-fetch-service" || manifest.Type != "com.bondplatform.dataset.manifest.v1" || manifest.DataContentType != "application/json" || manifest.Time.IsZero() || manifest.Data.SchemaVersion != 1 || manifest.Data.EventID == "" || manifest.Data.ConfigRevision == "" || len(manifest.Data.DatasetFingerprint) != 71 || !strings.HasPrefix(manifest.Data.DatasetFingerprint, "sha256:") || len(manifest.Data.Files) == 0 {
		return nil, fmt.Errorf("invalid stored manifest metadata")
	}
	for _, c := range manifest.Data.DatasetFingerprint[7:] {
		if c < '0' || c > '9' && c < 'a' || c > 'f' {
			return nil, fmt.Errorf("invalid dataset fingerprint")
		}
	}
	switch manifest.DataSchema {
	case "urn:bond-platform:dataset:bse-debt-trades":
		date := manifest.Data.Inputs["tradeDate"]
		parsed, err := time.Parse("2006-01-02", date)
		if err != nil || parsed.Format("2006-01-02") != date || manifest.Data.EventType != "daily-bhavcopy" || manifest.Data.Inputs["exchangeName"] != "BSE" || len(manifest.Data.Inputs) != 2 || manifest.Subject != "exchange/BSE/trade-date/"+date {
			return nil, fmt.Errorf("invalid BSE subject or inputs")
		}
	case "urn:bond-platform:dataset:nsdl-security":
		isin := manifest.Data.Inputs["isin_code"]
		if manifest.Data.EventType != "nsdl-bond-data" || len(manifest.Data.Inputs) != 1 || isin == "" || isin != strings.ToUpper(strings.TrimSpace(isin)) || manifest.Subject != "isin/"+isin {
			return nil, fmt.Errorf("invalid NSDL subject or inputs")
		}
	default:
		return nil, fmt.Errorf("unsupported dataset schema")
	}
	submission := Submission{SpecVersion: manifest.SpecVersion, ID: "urn:bond-platform:submission:" + manifest.Data.RunID, Source: manifest.Source, Type: manifest.Type, DataSchema: manifest.DataSchema, Time: manifest.Time, Subject: manifest.Subject, DataContentType: manifest.DataContentType, Data: SubmissionData{SchemaVersion: 1, EventType: manifest.Data.EventType, EventID: manifest.Data.EventID, RunID: manifest.Data.RunID, Inputs: manifest.Data.Inputs, Manifest: ref, DatasetFingerprint: manifest.Data.DatasetFingerprint}}
	encoded, err := json.Marshal(submission)
	if err != nil {
		return nil, err
	}
	if len(encoded) > 65536 {
		return nil, fmt.Errorf("submission exceeds 64 KiB")
	}
	return encoded, nil
}
