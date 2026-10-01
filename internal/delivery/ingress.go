package delivery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	lambdaevents "github.com/aws/aws-lambda-go/events"
)

// Runner handles one dispatched delivery under its own fenced invocation token.
type Runner interface {
	Run(context.Context, string, string, time.Time) error
}

// Ingress converts internal SQS messages to partial-batch delivery outcomes.
type Ingress struct {
	Runner   Runner
	Store    Repository
	NewToken func() string
}

// Handle acknowledges only completed, durably rejected, or verified stale messages.
func (i *Ingress) Handle(ctx context.Context, batch lambdaevents.SQSEvent) (lambdaevents.SQSEventResponse, error) {
	response := lambdaevents.SQSEventResponse{BatchItemFailures: []lambdaevents.SQSBatchItemFailure{}}
	if i.Runner == nil || i.Store == nil || i.NewToken == nil {
		return response, fmt.Errorf("delivery ingress dependencies missing")
	}
	deadline, hasDeadline := ctx.Deadline()
	for _, record := range batch.Records {
		if record.MessageId == "" {
			return response, fmt.Errorf("SQS message ID missing")
		}
		var envelope struct {
			RunID string `json:"run_id"`
		}
		err := json.Unmarshal([]byte(record.Body), &envelope)
		if err == nil && validRunID(envelope.RunID) && len(record.Body) <= 256<<10 && hasDeadline {
			err = i.Runner.Run(ctx, envelope.RunID, i.NewToken(), deadline)
			if errors.Is(err, ErrStaleDeliveryMessage) {
				err = nil
			}
		} else if !hasDeadline {
			err = fmt.Errorf("lambda deadline missing")
		} else {
			digest := sha256.Sum256([]byte(record.EventSourceARN + "\x00" + record.MessageId + "\x00" + record.Body))
			key := "requests/rejected-delivery-" + hex.EncodeToString(digest[:]) + "/rejection.json"
			err = i.Store.Create(ctx, key, []byte(`{"status":"rejected","code":"INVALID_DELIVERY_MESSAGE"}`))
		}
		if err != nil {
			response.BatchItemFailures = append(response.BatchItemFailures, lambdaevents.SQSBatchItemFailure{ItemIdentifier: record.MessageId})
		}
	}
	return response, nil
}
