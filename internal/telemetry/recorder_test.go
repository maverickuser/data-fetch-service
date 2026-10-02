package telemetry

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("logs unavailable") }

func TestJSONRecordEmitsBoundedEMFWithoutPayload(t *testing.T) {
	var output bytes.Buffer
	now := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	recorder := &JSON{Output: &output, Now: func() time.Time { return now }}
	recorder.Record(Entry{Component: "pull", Operation: "consume", Outcome: "retry", RunID: "run_123", RequestKey: strings.Repeat("a", 64), RequestID: "message-1", ErrorCode: "SOURCE_FAILED"})
	var line struct {
		Entry
		AWS   metricEnvelope `json:"_aws"`
		Count int            `json:"OperationCount"`
	}
	if err := json.Unmarshal(output.Bytes(), &line); err != nil {
		t.Fatal(err)
	}
	if line.Correlation != "run_123" || line.RequestKey != strings.Repeat("a", 64) || line.Count != 1 || line.AWS.Timestamp != now.UnixMilli() || line.AWS.CloudWatchMetrics[0].Namespace != "DataFetchService" {
		t.Fatal(line)
	}
	if got := line.AWS.CloudWatchMetrics[0].Dimensions; len(got) != 1 || len(got[0]) != 3 {
		t.Fatal(got)
	}
	if strings.Contains(output.String(), "body") {
		t.Fatal("payload leaked")
	}
	before := output.Len()
	recorder.Record(Entry{Component: "pull", Operation: "consume", Outcome: "completed", RunID: "bad\nsecret", RequestID: strings.Repeat("x", 129)})
	if output.Len() <= before || strings.Contains(output.String(), "secret") || strings.Contains(output.String(), strings.Repeat("x", 129)) {
		t.Fatal(output.String())
	}
	before = output.Len()
	recorder.Record(Entry{Component: "bad\nname", Operation: "consume", Outcome: "completed"})
	if output.Len() != before {
		t.Fatal("invalid metric dimension recorded")
	}
	(&JSON{Output: failingWriter{}}).Record(Entry{Component: "api", Operation: "request", Outcome: "completed"})
	(&JSON{}).Record(Entry{Component: "api", Operation: "request", Outcome: "completed"})
	(*JSON)(nil).Record(Entry{Component: "api", Operation: "request", Outcome: "completed"})
}
