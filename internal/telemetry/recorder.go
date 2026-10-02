// Package telemetry emits bounded operational events without request or response payloads.
package telemetry

import (
	"encoding/json"
	"io"
	"strings"
	"sync"
	"time"
)

// Entry is a fixed-schema outcome from one Lambda operation.
type Entry struct {
	Component   string `json:"component"`
	Operation   string `json:"operation"`
	Outcome     string `json:"outcome"`
	RunID       string `json:"run_id,omitempty"`
	RequestKey  string `json:"request_key,omitempty"`
	RequestID   string `json:"request_id,omitempty"`
	Correlation string `json:"correlation_id,omitempty"`
	ErrorCode   string `json:"error_code,omitempty"`
}

// Recorder accepts completed operational events without affecting the business result.
type Recorder interface{ Record(Entry) }

// JSON writes one CloudWatch Embedded Metric Format log line per event.
type JSON struct {
	Output io.Writer
	Now    func() time.Time
	mu     sync.Mutex
}

type metric struct {
	Name string `json:"Name"`
	Unit string `json:"Unit"`
}

type metricDefinition struct {
	Namespace  string     `json:"Namespace"`
	Dimensions [][]string `json:"Dimensions"`
	Metrics    []metric   `json:"Metrics"`
}

type metricEnvelope struct {
	Timestamp         int64              `json:"Timestamp"`
	CloudWatchMetrics []metricDefinition `json:"CloudWatchMetrics"`
}

// Record bounds identity fields and emits a low-cardinality invocation metric.
func (r *JSON) Record(entry Entry) {
	if r == nil || r.Output == nil {
		return
	}
	entry.RunID = safe(entry.RunID)
	entry.RequestKey = safe(entry.RequestKey)
	entry.RequestID = safe(entry.RequestID)
	entry.Correlation = safe(entry.Correlation)
	entry.Component = safe(entry.Component)
	entry.Operation = safe(entry.Operation)
	entry.Outcome = safe(entry.Outcome)
	entry.ErrorCode = safe(entry.ErrorCode)
	if entry.Component == "" || entry.Operation == "" || entry.Outcome == "" {
		return
	}
	if entry.Correlation == "" {
		entry.Correlation = entry.RunID
	}
	now := time.Now()
	if r.Now != nil {
		now = r.Now()
	}
	line := struct {
		Entry
		Metric metricEnvelope `json:"_aws"`
		Count  int            `json:"OperationCount"`
	}{entry, metricEnvelope{Timestamp: now.UnixMilli(), CloudWatchMetrics: []metricDefinition{{Namespace: "DataFetchService", Dimensions: [][]string{{"component", "operation", "outcome"}}, Metrics: []metric{{Name: "OperationCount", Unit: "Count"}}}}}, 1}
	encoded, err := json.Marshal(line)
	if err != nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	_, _ = r.Output.Write(append(encoded, '\n'))
}

// safe retains bounded log-safe identity characters and discards untrusted text.
func safe(value string) string {
	if len(value) > 128 || strings.TrimSpace(value) != value {
		return ""
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9' || character >= 'A' && character <= 'Z' || character >= 'a' && character <= 'z' || character == '-' || character == '_' || character == ':' || character == '.') {
			return ""
		}
	}
	return value
}
