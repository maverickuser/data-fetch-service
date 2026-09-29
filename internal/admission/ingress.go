package admission

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	lambdaevents "github.com/aws/aws-lambda-go/events"
	"github.com/maverickuser/data-fetch-service/internal/events"
	"github.com/maverickuser/data-fetch-service/internal/state"
)

// Ingress processes independent SQS records and durably records permanent rejections.
type Ingress struct {
	Service  *Service
	Store    *state.Store
	Mappings []events.RuleMapping
}

// Handle retries transient failures individually; permanent errors are acknowledged after recording.
func (i *Ingress) Handle(ctx context.Context, batch lambdaevents.SQSEvent) (lambdaevents.SQSEventResponse, error) {
	response := lambdaevents.SQSEventResponse{BatchItemFailures: []lambdaevents.SQSBatchItemFailure{}}
	for _, record := range batch.Records {
		if record.MessageId == "" {
			return response, fmt.Errorf("SQS message ID missing")
		}
		var err error
		if len(record.Body) > 256<<10 {
			err = ErrInvalid
		} else {
			event, parseErr := events.ParseTransport([]byte(record.Body), i.Mappings)
			if parseErr != nil {
				err = ErrInvalid
			} else {
				_, err = i.Service.External(ctx, event)
			}
		}
		if errors.Is(err, ErrInvalid) || errors.Is(err, state.ErrIntegrity) || errors.Is(err, state.ErrExpired) {
			// Never persist arbitrary source payloads or changing timestamps in rejection receipts.
			digest := sha256.Sum256([]byte(record.EventSourceARN + "\x00" + record.MessageId + "\x00" + record.Body))
			key := "requests/rejected-" + hex.EncodeToString(digest[:]) + "/rejection.json"
			err = i.Store.Create(ctx, key, []byte(`{"status":"rejected","code":"INVALID_EVENT"}`))
		}
		if err != nil {
			response.BatchItemFailures = append(response.BatchItemFailures, lambdaevents.SQSBatchItemFailure{ItemIdentifier: record.MessageId})
		}
	}
	return response, nil
}
