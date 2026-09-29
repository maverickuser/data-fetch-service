// Package queue publishes internal run references through the AWS SQS SDK.
package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// Client is the SDK operation used by durable dispatch.
type Client interface {
	SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

// Publisher maps logical queues to deployment-provided URLs.
type Publisher struct {
	Client Client
	URLs   map[string]string
}

// Publish sends only a run reference; consumers load their immutable execution snapshot.
func (p *Publisher) Publish(ctx context.Context, queue, runID string) error {
	target := p.URLs[queue]
	if target == "" || runID == "" {
		return fmt.Errorf("queue URL and run ID required")
	}
	body, err := json.Marshal(struct {
		RunID string `json:"run_id"`
	}{runID})
	if err != nil {
		return err
	}
	_, err = p.Client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(target), MessageBody: aws.String(string(body))})
	return err
}
