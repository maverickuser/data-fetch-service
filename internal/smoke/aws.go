package smoke

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/smithy-go"
)

// SendAPI is the SQS boundary used to publish the NSDL smoke event.
type SendAPI interface {
	SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

// HeadAPI is the S3 boundary used to confirm stored artifacts.
type HeadAPI interface {
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

// ScheduleAPI is the EventBridge Scheduler boundary used to read the deployed schedule.
type ScheduleAPI interface {
	GetSchedule(context.Context, *scheduler.GetScheduleInput, ...func(*scheduler.Options)) (*scheduler.GetScheduleOutput, error)
}

// SQSQueue publishes to the deployed ingress queue.
type SQSQueue struct {
	Client SendAPI
	URL    string
}

// Send publishes one CloudEvent body to the ingress queue.
func (q SQSQueue) Send(ctx context.Context, body string) error {
	_, err := q.Client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(q.URL), MessageBody: aws.String(body)})
	return err
}

// S3Objects checks keys in the private artifact bucket.
type S3Objects struct {
	Client HeadAPI
	Bucket string
}

// Exists reports false for a missing key and an error for any other failure, including access denial.
func (o S3Objects) Exists(ctx context.Context, key string) (bool, error) {
	_, err := o.Client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(o.Bucket), Key: aws.String(key)})
	if err != nil {
		var api smithy.APIError
		if errors.As(err, &api) && (api.ErrorCode() == "NotFound" || api.ErrorCode() == "NoSuchKey" || api.ErrorCode() == "404") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// SchedulerSchedules reads schedules from the default schedule group.
type SchedulerSchedules struct{ Client ScheduleAPI }

// Get returns the deployed expression, timezone, and state of one schedule.
func (s SchedulerSchedules) Get(ctx context.Context, name string) (Schedule, error) {
	output, err := s.Client.GetSchedule(ctx, &scheduler.GetScheduleInput{Name: aws.String(name)})
	if err != nil {
		return Schedule{}, err
	}
	return Schedule{Expression: aws.ToString(output.ScheduleExpression), Timezone: aws.ToString(output.ScheduleExpressionTimezone), State: string(output.State)}, nil
}

// Environment is the explicit smoke configuration read from workflow variables.
type Environment struct {
	Config         Config
	QueueURL       string
	ArtifactBucket string
	ReportPath     string
}

// LoadEnvironment reads required smoke inputs; missing values fail instead of being defaulted to mocks.
func LoadEnvironment(env func(string) string, now time.Time) (Environment, error) {
	out := Environment{QueueURL: env("SMOKE_INGRESS_QUEUE_URL"), ArtifactBucket: env("SMOKE_ARTIFACT_BUCKET"), ReportPath: env("SMOKE_REPORT_PATH")}
	out.Config = Config{BaseURL: env("SMOKE_API_URL"), NSDLISIN: env("SMOKE_NSDL_ISIN"), Commit: env("SMOKE_COMMIT"), SuiteID: env("SMOKE_SUITE_ID"), FallbackWeekdays: 3, PollInterval: 15 * time.Second, Timeout: 75 * time.Minute}
	for _, name := range []string{"SMOKE_API_URL", "SMOKE_NSDL_ISIN", "SMOKE_COMMIT", "SMOKE_INGRESS_QUEUE_URL", "SMOKE_ARTIFACT_BUCKET"} {
		if env(name) == "" {
			return Environment{}, fmt.Errorf("smoke preflight: %s is required", name)
		}
	}
	if raw := env("SMOKE_BSE_FALLBACK_WEEKDAYS"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil {
			return Environment{}, fmt.Errorf("smoke preflight: SMOKE_BSE_FALLBACK_WEEKDAYS must be an integer")
		}
		out.Config.FallbackWeekdays = value
	}
	if raw := env("SMOKE_TIMEOUT_SECONDS"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil {
			return Environment{}, fmt.Errorf("smoke preflight: SMOKE_TIMEOUT_SECONDS must be an integer")
		}
		out.Config.Timeout = time.Duration(value) * time.Second
	}
	if out.Config.SuiteID == "" {
		out.Config.SuiteID = "smoke-" + now.UTC().Format("20060102T150405Z")
	}
	if out.ReportPath == "" {
		out.ReportPath = "smoke-report.json"
	}
	return out, nil
}
