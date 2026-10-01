// Package runtime wires Lambda entry points to the configured AWS adapters.
package runtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/maverickuser/data-fetch-service/internal/acquisition"
	"github.com/maverickuser/data-fetch-service/internal/admission"
	"github.com/maverickuser/data-fetch-service/internal/api"
	"github.com/maverickuser/data-fetch-service/internal/config"
	"github.com/maverickuser/data-fetch-service/internal/delivery"
	"github.com/maverickuser/data-fetch-service/internal/events"
	"github.com/maverickuser/data-fetch-service/internal/pull"
	"github.com/maverickuser/data-fetch-service/internal/queue"
	"github.com/maverickuser/data-fetch-service/internal/recovery"
	"github.com/maverickuser/data-fetch-service/internal/state"
	"github.com/maverickuser/data-fetch-service/internal/storage"
)

// Dependencies makes cold-start assembly testable without credentials or a Lambda loop.
type Dependencies struct {
	ReadFile func(string) ([]byte, error)
	Env      func(string) string
	LoadAWS  func(context.Context, ...func(*awsconfig.LoadOptions) error) (aws.Config, error)
	Start    func(any)
}

// Run initializes the selected handler and enters the AWS Lambda invocation loop.
func Run(kind string) error {
	return Start(context.Background(), kind, Dependencies{os.ReadFile, os.Getenv, awsconfig.LoadDefaultConfig, lambda.Start})
}

// Start validates all local/deployment inputs before constructing network clients.
func Start(ctx context.Context, kind string, d Dependencies) error {
	if kind != "api" && kind != "admission" && kind != "pull" && kind != "delivery" && kind != "reconciler" {
		return fmt.Errorf("unknown Lambda kind")
	}
	base, err := d.ReadFile("config/events.yaml")
	if err != nil {
		return err
	}
	overlay, err := d.ReadFile("config/environments/prod.yaml")
	if err != nil {
		return err
	}
	cfg, err := config.LoadEffective(base, overlay)
	if err != nil {
		return err
	}
	cfg.DeploymentCommit = d.Env("DEPLOYMENT_COMMIT")
	bucket, pullQueueURL, deliveryQueueURL := d.Env("STATE_BUCKET"), d.Env("PULL_QUEUE_URL"), d.Env("DELIVERY_QUEUE_URL")
	if cfg.DeploymentCommit == "" || bucket == "" || pullQueueURL == "" || deliveryQueueURL == "" {
		return fmt.Errorf("deployment commit, state bucket and queue URLs required")
	}
	mappingData, err := d.ReadFile("config/native-events.yaml")
	if err != nil {
		return err
	}
	mappings, err := events.LoadMappings(mappingData, cfg)
	if err != nil {
		return err
	}
	region := d.Env("AWS_REGION")
	if region == "" {
		region = cfg.AWSRegion
	}
	if region == "" {
		region = "ap-south-1"
	}
	cfg.AWSRegion = region
	awsCfg, err := d.LoadAWS(ctx, awsconfig.WithRegion(region), awsconfig.WithRetryMaxAttempts(3))
	if err != nil {
		return err
	}
	objects, err := state.NewS3(s3.NewFromConfig(awsCfg), bucket, 2<<20)
	if err != nil {
		return err
	}
	store := state.New(objects)
	coordinator := state.NewCoordinator(store, time.Now)
	publisher := &queue.Publisher{Client: sqs.NewFromConfig(awsCfg), URLs: map[string]string{"pull": pullQueueURL, "delivery": deliveryQueueURL}}
	service := &admission.Service{Config: cfg, Coordinator: coordinator, Publisher: publisher, Now: time.Now, NewID: newID}
	if kind == "pull" {
		artifactBucket := d.Env("ARTIFACT_BUCKET")
		if artifactBucket == "" {
			return fmt.Errorf("artifact bucket required for pull")
		}
		limits := acquisition.ConfiguredLimits(cfg.Defaults)
		artifactClient := s3.NewFromConfig(awsCfg)
		fetcher := &acquisition.Fetcher{Client: acquisition.GuardedClient(net.DefaultResolver, &net.Dialer{Timeout: 10 * time.Second}), Storage: &storage.Multipart{Client: artifactClient, Bucket: artifactBucket, MaxBytes: cfg.Defaults.MaxExtractedBytes}, Recorder: acquisition.StateRecorder{Store: store}, Budget: acquisition.NewBudget(cfg.Defaults.MaxTempBytes), Limits: limits, TempRoot: "/tmp", Now: time.Now}
		worker := &pull.Service{Repository: store, Coordinator: coordinator, ManifestStorage: fetcher.Storage, Publisher: publisher, ArtifactBucket: artifactBucket, Now: time.Now, FetcherForSnapshot: func(defaults config.Defaults) pull.Fetcher {
			return pinnedPullFetcher(fetcher, artifactClient, artifactBucket, defaults)
		}}
		d.Start((&pull.Ingress{Runner: worker, Store: store, NewToken: newID}).Handle)
	} else if kind == "delivery" || kind == "reconciler" {
		artifactBucket := d.Env("ARTIFACT_BUCKET")
		if artifactBucket == "" {
			return fmt.Errorf("artifact bucket required for delivery or reconciler")
		}
		worker := deliveryWorker(store, coordinator, s3.NewFromConfig(awsCfg), artifactBucket)
		if kind == "delivery" {
			d.Start((&delivery.Ingress{Runner: worker, Store: store, NewToken: newID}).Handle)
		} else {
			reconciler := &recovery.Service{Repository: store, Coordinator: coordinator, Publisher: publisher, Delivery: worker, Now: time.Now, NewID: newID, PageLimit: 100}
			d.Start((&recovery.Handler{Service: reconciler}).Handle)
		}
	} else if kind == "admission" {
		handler := &admission.Ingress{Service: service, Store: store, Mappings: mappings}
		d.Start(handler.Handle)
	} else {
		handler := &api.Handler{Service: service, Recovery: service, Store: store, Coordinator: coordinator, Config: cfg, Now: time.Now}
		adapter := api.Lambda{Handler: handler.Routes()}
		d.Start(adapter.Handle)
	}
	return nil
}

// deliveryWorker uses a private HTTPS client and bounded manifest reader for both delivery paths.
func deliveryWorker(store *state.Store, coordinator *state.Coordinator, client *s3.Client, bucket string) *delivery.Service {
	processorClient := &http.Client{Transport: &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext, TLSHandshakeTimeout: 10 * time.Second}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &delivery.Service{Repository: store, Manifests: &storage.Reader{Client: client, Bucket: bucket, MaxBytes: 1 << 20}, Coordinator: coordinator, Client: processorClient, ArtifactBucket: bucket, Now: time.Now}
}

// pinnedPullFetcher uses admitted source, disk, and S3 byte limits for a queued run.
func pinnedPullFetcher(base *acquisition.Fetcher, client *s3.Client, bucket string, defaults config.Defaults) *acquisition.Fetcher {
	pinned := *base
	pinned.Limits = acquisition.ConfiguredLimits(defaults)
	pinned.Budget = acquisition.NewBudget(defaults.MaxTempBytes)
	pinned.Storage = &storage.Multipart{Client: client, Bucket: bucket, MaxBytes: defaults.MaxExtractedBytes}
	return &pinned
}

// newID generates an independent cryptographic run/request identifier.
func newID() string {
	var data [16]byte
	rand.Read(data[:])
	return "run_" + hex.EncodeToString(data[:])
}
