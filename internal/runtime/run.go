// Package runtime wires Lambda entry points to the configured AWS adapters.
package runtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/maverickuser/data-fetch-service/internal/admission"
	"github.com/maverickuser/data-fetch-service/internal/api"
	"github.com/maverickuser/data-fetch-service/internal/config"
	"github.com/maverickuser/data-fetch-service/internal/events"
	"github.com/maverickuser/data-fetch-service/internal/queue"
	"github.com/maverickuser/data-fetch-service/internal/state"
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
	if kind != "api" && kind != "admission" {
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
	bucket, pull, delivery := d.Env("STATE_BUCKET"), d.Env("PULL_QUEUE_URL"), d.Env("DELIVERY_QUEUE_URL")
	if cfg.DeploymentCommit == "" || bucket == "" || pull == "" || delivery == "" {
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
	publisher := &queue.Publisher{Client: sqs.NewFromConfig(awsCfg), URLs: map[string]string{"pull": pull, "delivery": delivery}}
	service := &admission.Service{Config: cfg, Coordinator: coordinator, Publisher: publisher, Now: time.Now, NewID: newID}
	if kind == "admission" {
		handler := &admission.Ingress{Service: service, Store: store, Mappings: mappings}
		d.Start(handler.Handle)
	} else {
		handler := &api.Handler{Service: service, Store: store, Coordinator: coordinator, Config: cfg, Now: time.Now}
		adapter := api.Lambda{Handler: handler.Routes()}
		d.Start(adapter.Handle)
	}
	return nil
}

// newID generates an independent cryptographic run/request identifier.
func newID() string {
	var data [16]byte
	rand.Read(data[:])
	return "run_" + hex.EncodeToString(data[:])
}
