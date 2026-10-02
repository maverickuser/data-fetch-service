package pull

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	lambdaevents "github.com/aws/aws-lambda-go/events"
	"github.com/maverickuser/data-fetch-service/internal/state"
	"github.com/maverickuser/data-fetch-service/internal/telemetry"
)

type runnerFunc func(context.Context, string, string, time.Time) error

func (f runnerFunc) Run(ctx context.Context, run, token string, deadline time.Time) error {
	return f(ctx, run, token, deadline)
}

func TestPullIngressPartialBatchAndPermanentRejection(t *testing.T) {
	now := time.Now()
	objects := &memoryObjects{items: map[string]state.Object{}, clock: now}
	store := state.New(objects)
	runner := runnerFunc(func(_ context.Context, run, token string, deadline time.Time) error {
		if token != "token" || deadline.IsZero() {
			t.Fatal("missing invocation ownership")
		}
		if run == "retry" {
			return errors.New("transient")
		}
		if run == "stale" {
			return ErrStalePullMessage
		}
		if run == "cas-conflict" {
			return state.ErrConflict
		}
		return nil
	})
	var logs bytes.Buffer
	ingress := Ingress{Runner: runner, Store: store, NewToken: func() string { return "token" }, Telemetry: &telemetry.JSON{Output: &logs}}
	ctx, cancel := context.WithDeadline(context.Background(), now.Add(time.Minute))
	defer cancel()
	batch := lambdaevents.SQSEvent{Records: []lambdaevents.SQSMessage{{MessageId: "a", Body: `{"run_id":"run"}`}, {MessageId: "b", Body: `{"run_id":"retry"}`}, {MessageId: "c", Body: `{"run_id":"stale"}`}, {MessageId: "d", Body: `{`}, {MessageId: "e", Body: `{"run_id":"../bad"}`}, {MessageId: "f", Body: `{"run_id":"cas-conflict"}`}}}
	response, err := ingress.Handle(ctx, batch)
	if err != nil || len(response.BatchItemFailures) != 2 || response.BatchItemFailures[0].ItemIdentifier != "b" || response.BatchItemFailures[1].ItemIdentifier != "f" {
		t.Fatal(response, err)
	}
	rejected := 0
	for key := range objects.items {
		if len(key) >= 23 && key[:23] == "requests/rejected-pull-" {
			rejected++
		}
	}
	if rejected != 2 {
		t.Fatal("missing durable rejection receipts", rejected)
	}
	for _, outcome := range []string{`"outcome":"completed"`, `"outcome":"retry"`, `"outcome":"stale"`, `"outcome":"rejected"`} {
		if !strings.Contains(logs.String(), outcome) {
			t.Fatal(outcome, logs.String())
		}
	}
}

func TestPullIngressRejectsMissingDependenciesAndDeadline(t *testing.T) {
	if _, err := (&Ingress{}).Handle(context.Background(), lambdaevents.SQSEvent{}); err == nil {
		t.Fatal("missing dependencies")
	}
	now := time.Now()
	objects := &memoryObjects{items: map[string]state.Object{}, clock: now}
	ingress := Ingress{Runner: runnerFunc(func(context.Context, string, string, time.Time) error { t.Fatal("run without deadline"); return nil }), Store: state.New(objects), NewToken: func() string { return "token" }}
	response, err := ingress.Handle(context.Background(), lambdaevents.SQSEvent{Records: []lambdaevents.SQSMessage{{MessageId: "x", Body: `{"run_id":"run"}`}}})
	if err != nil || len(response.BatchItemFailures) != 1 {
		t.Fatal(response, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := ingress.Handle(ctx, lambdaevents.SQSEvent{Records: []lambdaevents.SQSMessage{{Body: `{}`}}}); err == nil {
		t.Fatal("missing message ID")
	}
}
