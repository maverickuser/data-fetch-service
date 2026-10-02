package pull

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	lambdaevents "github.com/aws/aws-lambda-go/events"
	"github.com/maverickuser/data-fetch-service/internal/telemetry"
)

// Runner executes one fenced run referenced by an internal SQS message.
type Runner interface {
	Run(context.Context, string, string, time.Time) error
}

// Ingress translates pull queue records into independent invocation attempts.
type Ingress struct {
	Runner    Runner
	Store     Repository
	NewToken  func() string
	Telemetry telemetry.Recorder
}

// Handle reports transient per-record failures and persists permanent malformed inputs.
func (i *Ingress) Handle(ctx context.Context, batch lambdaevents.SQSEvent) (lambdaevents.SQSEventResponse, error) {
	response := lambdaevents.SQSEventResponse{BatchItemFailures: []lambdaevents.SQSBatchItemFailure{}}
	if i.Runner == nil || i.Store == nil || i.NewToken == nil {
		i.record(telemetry.Entry{Component: "pull", Operation: "consume", Outcome: "retry", ErrorCode: "MISSING_DEPENDENCY"})
		return response, fmt.Errorf("pull ingress dependencies missing")
	}
	deadline, hasDeadline := ctx.Deadline()
	for _, record := range batch.Records {
		if record.MessageId == "" {
			i.record(telemetry.Entry{Component: "pull", Operation: "consume", Outcome: "retry", ErrorCode: "MISSING_MESSAGE_ID"})
			return response, fmt.Errorf("SQS message ID missing")
		}
		var envelope struct {
			RunID string `json:"run_id"`
		}
		err := json.Unmarshal([]byte(record.Body), &envelope)
		outcome := "completed"
		if err == nil && validRunID(envelope.RunID) && len(record.Body) <= 256<<10 && hasDeadline {
			err = i.Runner.Run(ctx, envelope.RunID, i.NewToken(), deadline)
			if errors.Is(err, ErrStalePullMessage) {
				outcome = "stale"
				err = nil
			} // Only a conflict at the initial claim is a verified stale message.
		} else if !hasDeadline {
			err = fmt.Errorf("lambda deadline missing")
		} else {
			outcome = "rejected"
			digest := sha256.Sum256([]byte(record.EventSourceARN + "\x00" + record.MessageId + "\x00" + record.Body))
			key := "requests/rejected-pull-" + hex.EncodeToString(digest[:]) + "/rejection.json"
			err = i.Store.Create(ctx, key, []byte(`{"status":"rejected","code":"INVALID_PULL_MESSAGE"}`))
		}
		if err != nil {
			outcome = "retry"
			response.BatchItemFailures = append(response.BatchItemFailures, lambdaevents.SQSBatchItemFailure{ItemIdentifier: record.MessageId})
		}
		if i.Telemetry != nil {
			i.record(telemetry.Entry{Component: "pull", Operation: "consume", Outcome: outcome, RunID: envelope.RunID, RequestID: record.MessageId, Correlation: telemetry.Correlation(ctx, i.Store, envelope.RunID)})
		}
	}
	return response, nil
}

// record reports one bounded message outcome when telemetry is configured.
func (i *Ingress) record(entry telemetry.Entry) {
	if i.Telemetry != nil {
		i.Telemetry.Record(entry)
	}
}
