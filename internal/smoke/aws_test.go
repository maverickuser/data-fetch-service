package smoke

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/smithy-go"
)

type awsFake struct {
	err      error
	queueURL string
	body     string
	bucket   string
	key      string
	name     string
}

func (f *awsFake) SendMessage(_ context.Context, input *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	f.queueURL, f.body = aws.ToString(input.QueueUrl), aws.ToString(input.MessageBody)
	return &sqs.SendMessageOutput{}, f.err
}

func (f *awsFake) HeadObject(_ context.Context, input *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	f.bucket, f.key = aws.ToString(input.Bucket), aws.ToString(input.Key)
	return &s3.HeadObjectOutput{}, f.err
}

func (f *awsFake) GetSchedule(_ context.Context, input *scheduler.GetScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.GetScheduleOutput, error) {
	f.name = aws.ToString(input.Name)
	if f.err != nil {
		return nil, f.err
	}
	return &scheduler.GetScheduleOutput{ScheduleExpression: aws.String(scheduleCron), ScheduleExpressionTimezone: aws.String(scheduleZone), State: schedulertypes.ScheduleStateEnabled}, nil
}

func TestAWSAdaptersUseConfiguredResources(t *testing.T) {
	fake := &awsFake{}
	if err := (SQSQueue{Client: fake, URL: "https://sqs/ingress"}).Send(context.Background(), "body"); err != nil || fake.queueURL != "https://sqs/ingress" || fake.body != "body" {
		t.Fatal(fake, err)
	}
	objects := S3Objects{Client: fake, Bucket: "artifacts"}
	if found, err := objects.Exists(context.Background(), "runs/a/manifest.json"); err != nil || !found || fake.bucket != "artifacts" || fake.key != "runs/a/manifest.json" {
		t.Fatal(fake, found, err)
	}
	schedule, err := (SchedulerSchedules{Client: fake}).Get(context.Background(), scheduleName)
	if err != nil || fake.name != scheduleName || schedule != (Schedule{Expression: scheduleCron, Timezone: scheduleZone, State: "ENABLED"}) {
		t.Fatal(schedule, err)
	}
}

func TestAWSAdaptersSeparateMissingFromDenied(t *testing.T) {
	missing := &awsFake{err: &smithy.GenericAPIError{Code: "NotFound"}}
	if found, err := (S3Objects{Client: missing, Bucket: "b"}).Exists(context.Background(), "k"); found || err != nil {
		t.Fatal(found, err)
	}
	denied := &awsFake{err: &smithy.GenericAPIError{Code: "AccessDenied"}}
	if found, err := (S3Objects{Client: denied, Bucket: "b"}).Exists(context.Background(), "k"); found || err == nil {
		t.Fatal(found, err)
	}
	if _, err := (SchedulerSchedules{Client: denied}).Get(context.Background(), "x"); err == nil {
		t.Fatal("schedule failure ignored")
	}
	if err := (SQSQueue{Client: denied}).Send(context.Background(), ""); !errors.Is(err, denied.err) {
		t.Fatal(err)
	}
}

func TestLoadEnvironmentRequiresExplicitInputs(t *testing.T) {
	now := time.Date(2026, 10, 2, 9, 30, 0, 0, time.UTC)
	values := map[string]string{"SMOKE_API_URL": "https://fetch.kagent.app", "SMOKE_NSDL_ISIN": isin, "SMOKE_COMMIT": "abc", "SMOKE_INGRESS_QUEUE_URL": "https://sqs/ingress", "SMOKE_ARTIFACT_BUCKET": "artifacts"}
	env := func(name string) string { return values[name] }
	loaded, err := LoadEnvironment(env, now)
	if err != nil || loaded.Config.FallbackWeekdays != 3 || loaded.Config.Timeout != 75*time.Minute || loaded.Config.SuiteID != "smoke-20261002T093000Z" || loaded.ReportPath != "smoke-report.json" || loaded.QueueURL == "" || loaded.ArtifactBucket != "artifacts" {
		t.Fatalf("%+v %v", loaded, err)
	}
	values["SMOKE_BSE_FALLBACK_WEEKDAYS"], values["SMOKE_TIMEOUT_SECONDS"], values["SMOKE_SUITE_ID"], values["SMOKE_REPORT_PATH"] = "5", "120", "suite-9", "out.json"
	loaded, err = LoadEnvironment(env, now)
	if err != nil || loaded.Config.FallbackWeekdays != 5 || loaded.Config.Timeout != 2*time.Minute || loaded.Config.SuiteID != "suite-9" || loaded.ReportPath != "out.json" {
		t.Fatalf("%+v %v", loaded, err)
	}
	for name, bad := range map[string]string{"SMOKE_BSE_FALLBACK_WEEKDAYS": "three", "SMOKE_TIMEOUT_SECONDS": "long", "SMOKE_API_URL": "", "SMOKE_ARTIFACT_BUCKET": ""} {
		original := values[name]
		values[name] = bad
		if _, err := LoadEnvironment(env, now); err == nil || !strings.Contains(err.Error(), name) {
			t.Fatal(name, err)
		}
		values[name] = original
	}
}
