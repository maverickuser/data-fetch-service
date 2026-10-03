//go:build aws

package awsverify

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/maverickuser/data-fetch-service/internal/queue"
	"github.com/maverickuser/data-fetch-service/internal/state"
)

// required fails instead of skipping: missing disposable resources must not pass the gate.
func required(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required; these tests need disposable AWS resources", name)
	}
	return value
}

func load(t *testing.T) aws.Config {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func objects(t *testing.T) *state.S3Objects {
	t.Helper()
	store, err := state.NewS3(s3.NewFromConfig(load(t)), required(t, "AWS_VERIFY_BUCKET"), 2<<20)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestConcurrentConditionalCreateHasOneWinner(t *testing.T) {
	store := objects(t)
	key := fmt.Sprintf("coordination/race-%d.json", time.Now().UnixNano())
	const writers = 8
	var wg sync.WaitGroup
	results := make(chan error, writers)
	for writer := 0; writer < writers; writer++ {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			_, err := store.Put(context.Background(), key, []byte(fmt.Sprintf(`{"writer":%d}`, writer)), "")
			results <- err
		}(writer)
	}
	wg.Wait()
	close(results)
	wins := 0
	for err := range results {
		switch {
		case err == nil:
			wins++
		case !errors.Is(err, state.ErrConflict):
			t.Fatalf("losing writer got %v, want a conflict", err)
		}
	}
	if wins != 1 {
		t.Fatalf("%d writers created the object, want exactly 1", wins)
	}
}

func TestCompareAndSwapRejectsStaleGeneration(t *testing.T) {
	store := objects(t)
	ctx := context.Background()
	key := fmt.Sprintf("coordination/cas-%d.json", time.Now().UnixNano())
	first, err := store.Put(ctx, key, []byte(`{"generation":1}`), "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Put(ctx, key, []byte(`{"generation":2}`), first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, key, []byte(`{"generation":3}`), first); !errors.Is(err, state.ErrConflict) {
		t.Fatalf("stale ETag write returned %v, want a conflict", err)
	}
	current, err := store.Get(ctx, key)
	if err != nil || string(current.Data) != `{"generation":2}` || current.ETag != second {
		t.Fatalf("current object %q %q, %v", current.Data, current.ETag, err)
	}
}

func TestImmutableRecordsAndMissingKeys(t *testing.T) {
	ctx := context.Background()
	records := state.New(objects(t))
	key := fmt.Sprintf("runs/verify-%d/history/00000000000000000001.json", time.Now().UnixNano())
	if err := records.Create(ctx, key, []byte(`{"phase":"queued"}`)); err != nil {
		t.Fatal(err)
	}
	if err := records.Create(ctx, key, []byte(`{"phase":"queued"}`)); err != nil {
		t.Fatalf("identical retry returned %v, want a no-op", err)
	}
	if err := records.Create(ctx, key, []byte(`{"phase":"failed"}`)); !errors.Is(err, state.ErrIntegrity) {
		t.Fatalf("differing bytes returned %v, want an integrity error", err)
	}
	// With the service's IAM shape S3 must report a missing key as not found, not access denied.
	if _, err := objects(t).Get(ctx, key+".missing"); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("missing key returned %v, want not found", err)
	}
}

func TestUnacknowledgedMessageIsRedelivered(t *testing.T) {
	ctx := context.Background()
	url := required(t, "AWS_VERIFY_QUEUE_URL")
	client := sqs.NewFromConfig(load(t))
	runID := fmt.Sprintf("run_verify%d", time.Now().UnixNano())
	if err := (&queue.Publisher{Client: client, URLs: map[string]string{"pull": url}}).Publish(ctx, "pull", runID); err != nil {
		t.Fatal(err)
	}
	receive := func() sqstypes.Message {
		t.Helper()
		deadline := time.Now().Add(time.Minute)
		for time.Now().Before(deadline) {
			out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(url), MaxNumberOfMessages: 1, VisibilityTimeout: 2, WaitTimeSeconds: 10,
				MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{sqstypes.MessageSystemAttributeNameApproximateReceiveCount}})
			if err != nil {
				t.Fatal(err)
			}
			if len(out.Messages) == 1 {
				return out.Messages[0]
			}
		}
		t.Fatal("no message received within a minute")
		return sqstypes.Message{}
	}
	first := receive()
	want := fmt.Sprintf(`{"run_id":%q}`, runID)
	if aws.ToString(first.Body) != want {
		t.Fatalf("body %q, want %q", aws.ToString(first.Body), want)
	}
	// Not deleting the message simulates a consumer that crashed before acknowledging it.
	second := receive()
	if aws.ToString(second.Body) != want || second.Attributes["ApproximateReceiveCount"] != "2" {
		t.Fatalf("redelivery body %q count %q", aws.ToString(second.Body), second.Attributes["ApproximateReceiveCount"])
	}
	if _, err := client.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(url), ReceiptHandle: second.ReceiptHandle}); err != nil {
		t.Fatal(err)
	}
}
