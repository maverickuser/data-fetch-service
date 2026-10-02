package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/maverickuser/data-fetch-service/internal/smoke"
)

func fixture(values map[string]string, result error) (dependencies, *[]byte, **smoke.Runner) {
	written, runner := new([]byte), new(*smoke.Runner)
	return dependencies{
		Env: func(name string) string { return values[name] },
		LoadAWS: func(context.Context, ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
			return aws.Config{Region: "ap-south-1"}, nil
		},
		WriteFile: func(_ string, data []byte, _ os.FileMode) error { *written = data; return nil },
		Run: func(_ context.Context, r *smoke.Runner) (smoke.Report, error) {
			*runner = r
			return smoke.Report{Commit: r.Config.Commit, Passed: result == nil}, result
		},
		Now: func() time.Time { return time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC) },
	}, written, runner
}

func complete() map[string]string {
	return map[string]string{"SMOKE_API_URL": "https://fetch.kagent.app", "SMOKE_NSDL_ISIN": "INE121A07QY9", "SMOKE_COMMIT": "abc", "SMOKE_INGRESS_QUEUE_URL": "https://sqs/ingress", "SMOKE_ARTIFACT_BUCKET": "artifacts"}
}

func TestExecuteWiresRealClientsAndWritesReport(t *testing.T) {
	d, written, runner := fixture(complete(), nil)
	if err := execute(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	var report smoke.Report
	if err := json.Unmarshal(*written, &report); err != nil || !report.Passed || report.Commit != "abc" {
		t.Fatal(string(*written), err)
	}
	r := *runner
	if r.HTTP == nil || r.Queue.(smoke.SQSQueue).URL != "https://sqs/ingress" || r.Objects.(smoke.S3Objects).Bucket != "artifacts" || r.Schedules == nil || r.Preflight() != nil {
		t.Fatalf("%+v", r)
	}
}

func TestExecuteReportsFailures(t *testing.T) {
	failed := errors.New("smoke BSE: failed")
	d, written, _ := fixture(complete(), failed)
	if err := execute(context.Background(), d); !errors.Is(err, failed) || len(*written) == 0 {
		t.Fatal(err, "failure report must still be written")
	}
	missing := complete()
	delete(missing, "SMOKE_NSDL_ISIN")
	d, written, runner := fixture(missing, nil)
	if err := execute(context.Background(), d); err == nil || *runner != nil || len(*written) != 0 {
		t.Fatal("missing input must fail before any call", err)
	}
	d, _, runner = fixture(complete(), nil)
	d.LoadAWS = func(context.Context, ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{}, errors.New("no credentials")
	}
	if err := execute(context.Background(), d); err == nil || *runner != nil {
		t.Fatal("credential failure ignored", err)
	}
	d, _, _ = fixture(complete(), nil)
	d.WriteFile = func(string, []byte, os.FileMode) error { return errors.New("disk full") }
	if err := execute(context.Background(), d); err == nil {
		t.Fatal("report write failure ignored")
	}
}

func TestMainExitsNonZeroOnFailure(t *testing.T) {
	originalProduction, originalExit := production, exit
	defer func() { production, exit = originalProduction, originalExit }()
	code := -1
	exit = func(value int) { code = value }
	production, _, _ = fixture(complete(), errors.New("failed"))
	main()
	if code != 1 {
		t.Fatal(code)
	}
	code = -1
	production, _, _ = fixture(complete(), nil)
	main()
	if code != -1 {
		t.Fatal("successful suite exited non-zero")
	}
}

func TestSleepStopsOnCancellation(t *testing.T) {
	if err := sleep(context.Background(), time.Millisecond); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleep(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
