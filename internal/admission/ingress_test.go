package admission

import (
	"context"
	"errors"
	"strings"
	"testing"

	lambdaevents "github.com/aws/aws-lambda-go/events"
	"github.com/maverickuser/data-fetch-service/internal/state"
)

type rejectionObjects struct {
	keys []string
	fail bool
}

func (f *rejectionObjects) Put(_ context.Context, key string, _ []byte, _ string) (string, error) {
	f.keys = append(f.keys, key)
	if f.fail {
		return "", errors.New("unavailable")
	}
	return "etag", nil
}
func (f *rejectionObjects) Get(context.Context, string) (state.Object, error) {
	return state.Object{}, state.ErrNotFound
}
func (f *rejectionObjects) List(context.Context, string, string, int32) (state.Page, error) {
	return state.Page{}, nil
}

func TestIngressPartialFailureAndDurableRejection(t *testing.T) {
	s, repo := serviceFixture(t)
	objects := &rejectionObjects{}
	ingress := Ingress{Service: s, Store: state.New(objects)}
	good := `{"specversion":"1.0","id":"event","source":"urn:test","type":"com.bondplatform.data.pull.requested.v1","time":"2026-09-21T14:30:00Z","datacontenttype":"application/json","data":{"schema_version":1,"event_type":"daily-bhavcopy","inputs":{"exchangeName":"BSE"}}}`
	batch := lambdaevents.SQSEvent{Records: []lambdaevents.SQSMessage{{MessageId: "good", Body: good}, {MessageId: "bad", Body: "invalid"}, {MessageId: "huge", Body: strings.Repeat("x", (256<<10)+1)}}}
	result, err := ingress.Handle(context.Background(), batch)
	if err != nil || len(result.BatchItemFailures) != 0 || len(objects.keys) != 2 {
		t.Fatal(result, err, objects.keys)
	}
	if objects.keys[0] == objects.keys[1] {
		t.Fatal("rejections collided")
	}
	objects.fail = true
	repo.admitErr = errors.New("S3 unavailable")
	result, err = ingress.Handle(context.Background(), batch)
	if err != nil || len(result.BatchItemFailures) != 3 {
		t.Fatal(result, err)
	}
	if _, err := ingress.Handle(context.Background(), lambdaevents.SQSEvent{Records: []lambdaevents.SQSMessage{{Body: good}}}); err == nil {
		t.Fatal("missing transport identity")
	}
}
