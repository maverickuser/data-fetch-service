// Command smoke runs the real-endpoint smoke suite against a deployed service.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/maverickuser/data-fetch-service/internal/smoke"
)

// dependencies makes the command testable without credentials or a deployment.
type dependencies struct {
	Env       func(string) string
	LoadAWS   func(context.Context, ...func(*awsconfig.LoadOptions) error) (aws.Config, error)
	WriteFile func(string, []byte, os.FileMode) error
	Run       func(context.Context, *smoke.Runner) (smoke.Report, error)
	Now       func() time.Time
}

var (
	production = dependencies{Env: os.Getenv, LoadAWS: awsconfig.LoadDefaultConfig, WriteFile: os.WriteFile, Now: time.Now,
		Run: func(ctx context.Context, runner *smoke.Runner) (smoke.Report, error) { return runner.Run(ctx) }}
	exit = os.Exit
)

// main exits non-zero when preflight or any smoke check fails.
func main() {
	if err := execute(context.Background(), production); err != nil {
		log.Print(err)
		exit(1)
	}
}

// execute wires real AWS clients, runs the suite, and always writes the report it produced.
func execute(ctx context.Context, d dependencies) error {
	environment, err := smoke.LoadEnvironment(d.Env, d.Now())
	if err != nil {
		return err
	}
	awsCfg, err := d.LoadAWS(ctx)
	if err != nil {
		return err
	}
	runner := &smoke.Runner{Config: environment.Config, HTTP: &http.Client{Timeout: 30 * time.Second},
		Queue:     smoke.SQSQueue{Client: sqs.NewFromConfig(awsCfg), URL: environment.QueueURL},
		Objects:   smoke.S3Objects{Client: s3.NewFromConfig(awsCfg), Bucket: environment.ArtifactBucket},
		Schedules: smoke.SchedulerSchedules{Client: scheduler.NewFromConfig(awsCfg)},
		Now:       d.Now, Sleep: sleep}
	report, runErr := d.Run(ctx, runner)
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if err := d.WriteFile(environment.ReportPath, data, 0o600); err != nil {
		return err
	}
	return runErr
}

// sleep waits for the poll interval or returns early when the suite is canceled.
func sleep(ctx context.Context, wait time.Duration) error {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
