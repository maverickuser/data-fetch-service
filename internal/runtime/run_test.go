package runtime

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/maverickuser/data-fetch-service/internal/acquisition"
	"github.com/maverickuser/data-fetch-service/internal/config"
	"github.com/maverickuser/data-fetch-service/internal/storage"
)

func dependencies(t *testing.T) (Dependencies, *int) {
	t.Helper()
	started := 0
	d := Dependencies{ReadFile: func(path string) ([]byte, error) { return os.ReadFile("../../" + path) }, Env: func(name string) string {
		switch name {
		case "DEPLOYMENT_COMMIT":
			return "test-commit"
		case "STATE_BUCKET":
			return "state"
		case "PULL_QUEUE_URL", "DELIVERY_QUEUE_URL":
			return "https://sqs.example/queue"
		case "ARTIFACT_BUCKET":
			return "artifacts"
		}
		return ""
	}, LoadAWS: func(_ context.Context, options ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		var o awsconfig.LoadOptions
		for _, apply := range options {
			if err := apply(&o); err != nil {
				t.Fatal(err)
			}
		}
		if o.Region != "ap-south-1" || o.RetryMaxAttempts != 3 {
			t.Fatal(o)
		}
		return aws.Config{Region: o.Region, Credentials: aws.AnonymousCredentials{}}, nil
	}, Start: func(handler any) {
		if handler == nil {
			t.Fatal("nil handler")
		}
		started++
	}}
	return d, &started
}

func TestRuntimeBuildsBothHandlers(t *testing.T) {
	for _, kind := range []string{"api", "admission", "pull", "delivery", "reconciler"} {
		d, started := dependencies(t)
		if err := Start(context.Background(), kind, d); err != nil || *started != 1 {
			t.Fatal(kind, err, *started)
		}
	}
	first, second := newID(), newID()
	if first == second || !strings.HasPrefix(first, "run_") {
		t.Fatal(first, second)
	}
}

func TestRuntimeRejectsIncompleteConfiguration(t *testing.T) {
	for _, scenario := range []string{"kind", "base", "overlay", "config", "environment", "mapping file", "mapping data", "AWS"} {
		t.Run(scenario, func(t *testing.T) {
			d, started := dependencies(t)
			kind := "api"
			read := d.ReadFile
			switch scenario {
			case "kind":
				kind = "unknown"
			case "environment":
				d.Env = func(string) string { return "" }
			case "AWS":
				d.LoadAWS = func(context.Context, ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
					return aws.Config{}, errors.New("load failed")
				}
			default:
				d.ReadFile = func(path string) ([]byte, error) {
					if (scenario == "base" && path == "config/events.yaml") || (scenario == "overlay" && path == "config/environments/prod.yaml") || (scenario == "mapping file" && path == "config/native-events.yaml") {
						return nil, errors.New("missing file")
					}
					if (scenario == "config" && path == "config/events.yaml") || (scenario == "mapping data" && path == "config/native-events.yaml") {
						return []byte("["), nil
					}
					return read(path)
				}
			}
			if err := Start(context.Background(), kind, d); err == nil || *started != 0 {
				t.Fatal(err, *started)
			}
		})
	}
	if err := Run("unknown"); err == nil {
		t.Fatal("unknown runtime")
	}
}

func TestRuntimeRegionOverrideAndDefault(t *testing.T) {
	for _, region := range []string{"eu-west-1", ""} {
		d, _ := dependencies(t)
		env := d.Env
		read := d.ReadFile
		d.Env = func(key string) string {
			if key == "AWS_REGION" {
				return region
			}
			return env(key)
		}
		d.ReadFile = func(path string) ([]byte, error) {
			if path == "config/environments/prod.yaml" {
				return []byte("environment: prod\naws_region: ''\nresource_prefix: data-fetch-service\n"), nil
			}
			return read(path)
		}
		d.LoadAWS = func(_ context.Context, options ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
			var o awsconfig.LoadOptions
			for _, apply := range options {
				if err := apply(&o); err != nil {
					t.Fatal(err)
				}
			}
			expected := region
			if expected == "" {
				expected = "ap-south-1"
			}
			if o.Region != expected {
				t.Fatal(o.Region)
			}
			return aws.Config{Region: o.Region}, nil
		}
		if err := Start(context.Background(), "api", d); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRuntimePullRequiresArtifactBucket(t *testing.T) {
	for _, kind := range []string{"pull", "delivery", "reconciler"} {
		d, started := dependencies(t)
		environment := d.Env
		d.Env = func(name string) string {
			if name == "ARTIFACT_BUCKET" {
				return ""
			}
			return environment(name)
		}
		if err := Start(context.Background(), kind, d); err == nil || *started != 0 {
			t.Fatal(kind, err, *started)
		}
	}
}

func TestPinnedPullFetcherKeepsAdmittedStorageLimit(t *testing.T) {
	client := s3.New(s3.Options{Region: "ap-south-1"})
	current := &acquisition.Fetcher{Storage: &storage.Multipart{Client: client, Bucket: "artifacts", MaxBytes: 100}, Limits: acquisition.Limits{MaxExtractedBytes: 100}, Budget: acquisition.NewBudget(100)}
	admitted := config.Defaults{MaxExtractedBytes: 200, MaxDownloadBytes: 80, MaxTempBytes: 300, MaxValidationTokenBytes: 10, MaxJSONDepth: 8, MaxZipEntries: 2, MaxZipMetadataBytes: 128, MaxCompressionRatio: 50, SourceMaxAttempts: 3, RequestTimeoutSeconds: 10}
	pinned := pinnedPullFetcher(current, client, "artifacts", admitted)
	artifactStore, ok := pinned.Storage.(*storage.Multipart)
	if !ok || artifactStore.MaxBytes != 200 || artifactStore.Bucket != "artifacts" || artifactStore.Client != client || pinned.Limits.MaxExtractedBytes != 200 || current.Storage.(*storage.Multipart).MaxBytes != 100 {
		t.Fatal("deployment limit replaced admitted storage limit")
	}
	release, err := pinned.Budget.Acquire(context.Background(), 300)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if _, err := current.Budget.Acquire(context.Background(), 300); err == nil {
		t.Fatal("base budget mutated")
	}
}
