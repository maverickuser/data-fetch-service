package queue

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type clientFake struct {
	input *sqs.SendMessageInput
	err   error
}

func (f *clientFake) SendMessage(_ context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	f.input = in
	return &sqs.SendMessageOutput{}, f.err
}
func TestPublishPinsQueueAndRunReference(t *testing.T) {
	f := &clientFake{}
	p := Publisher{Client: f, URLs: map[string]string{"pull": "https://sqs.example/queue"}}
	if err := p.Publish(context.Background(), "pull", "run-1"); err != nil {
		t.Fatal(err)
	}
	if aws.ToString(f.input.QueueUrl) != "https://sqs.example/queue" || aws.ToString(f.input.MessageBody) != `{"run_id":"run-1"}` {
		t.Fatal(f.input)
	}
	f.err = errors.New("ambiguous send")
	if err := p.Publish(context.Background(), "pull", "run-1"); !errors.Is(err, f.err) {
		t.Fatal(err)
	}
	for _, args := range [][2]string{{"unknown", "run"}, {"pull", ""}} {
		if err := p.Publish(context.Background(), args[0], args[1]); err == nil {
			t.Fatal(args)
		}
	}
}
