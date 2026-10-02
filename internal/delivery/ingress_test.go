package delivery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	lambdaevents "github.com/aws/aws-lambda-go/events"
	"github.com/maverickuser/data-fetch-service/internal/telemetry"
)

type runnerFunc func(context.Context, string, string, time.Time) error

func (f runnerFunc) Run(ctx context.Context, run, token string, deadline time.Time) error {
	return f(ctx, run, token, deadline)
}

func TestIngressPartialBatchAndRejectEvidence(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), fixedTime.Add(24*time.Hour))
	defer cancel()
	repo := &memoryRepository{objects: map[string][]byte{}}
	var logs bytes.Buffer
	calls := 0
	ingress := Ingress{Store: repo, Telemetry: &telemetry.JSON{Output: &logs}, NewToken: func() string { return "token" }, Runner: runnerFunc(func(_ context.Context, run, token string, deadline time.Time) error {
		calls++
		if run != "run_1" || token != "token" || deadline.IsZero() {
			t.Fatal(run, token, deadline)
		}
		if calls == 1 {
			return errors.New("temporary")
		}
		return ErrStaleDeliveryMessage
	})}
	batch := lambdaevents.SQSEvent{Records: []lambdaevents.SQSMessage{{MessageId: "a", Body: `{"run_id":"run_1"}`}, {MessageId: "b", Body: `{"run_id":"run_1"}`}, {MessageId: "c", Body: `bad`, EventSourceARN: "arn:test"}}}
	response, err := ingress.Handle(ctx, batch)
	if err != nil || calls != 2 || len(response.BatchItemFailures) != 1 || response.BatchItemFailures[0].ItemIdentifier != "a" || len(repo.objects) != 1 {
		t.Fatal(response, err, calls, repo.objects)
	}
	for _, outcome := range []string{`"outcome":"retry"`, `"outcome":"stale"`, `"outcome":"rejected"`} {
		if !strings.Contains(logs.String(), outcome) {
			t.Fatal(outcome, logs.String())
		}
	}
	digest := sha256.Sum256([]byte("arn:test\x00c\x00bad"))
	repo.createError = "requests/rejected-delivery-" + hex.EncodeToString(digest[:]) + "/rejection.json"
	response, err = ingress.Handle(ctx, lambdaevents.SQSEvent{Records: batch.Records[2:]})
	if err != nil || len(response.BatchItemFailures) != 1 {
		t.Fatal(response, err)
	}
	noDeadline := Ingress{Store: repo, NewToken: ingress.NewToken, Runner: ingress.Runner}
	response, err = noDeadline.Handle(context.Background(), lambdaevents.SQSEvent{Records: batch.Records[:1]})
	if err != nil || len(response.BatchItemFailures) != 1 {
		t.Fatal(response, err)
	}
	if _, err = ingress.Handle(ctx, lambdaevents.SQSEvent{Records: []lambdaevents.SQSMessage{{Body: "x"}}}); err == nil {
		t.Fatal("missing message ID")
	}
	if _, err = (&Ingress{}).Handle(ctx, batch); err == nil {
		t.Fatal("missing dependencies")
	}
}
