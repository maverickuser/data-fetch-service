package delivery

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

type requestSignerFunc func(context.Context, *http.Request, []byte) error

func (f requestSignerFunc) Sign(ctx context.Context, request *http.Request, body []byte) error {
	return f(ctx, request, body)
}

func TestSigV4SignerAuthorizesExactProcessorBody(t *testing.T) {
	now := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	signer := &SigV4Signer{Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "secret", SessionToken: "session"}, nil
	}), Region: "ap-south-1", Now: func() time.Time { return now }}
	request, err := http.NewRequest(http.MethodPost, "https://processing.kagent.app/v1/event-ingestions", strings.NewReader("body"))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Idempotency-Key", "run_1")
	if err := signer.Sign(context.Background(), request, []byte("body")); err != nil {
		t.Fatal(err)
	}
	authorization := request.Header.Get("Authorization")
	if !strings.Contains(authorization, "Credential=AKIDEXAMPLE/20261002/ap-south-1/execute-api/aws4_request") || request.Header.Get("X-Amz-Date") != "20261002T000000Z" || request.Header.Get("X-Amz-Security-Token") != "session" {
		t.Fatal(request.Header)
	}
	changed, _ := http.NewRequest(http.MethodPost, request.URL.String(), strings.NewReader("changed"))
	changed.Header.Set("Idempotency-Key", "run_1")
	if err := signer.Sign(context.Background(), changed, []byte("changed")); err != nil || changed.Header.Get("Authorization") == authorization {
		t.Fatal("signature did not bind body", err)
	}
	for _, invalid := range []*SigV4Signer{nil, {}, {Credentials: signer.Credentials, Region: "", Now: signer.Now}, {Credentials: signer.Credentials, Region: "ap-south-1"}} {
		if err := invalid.Sign(context.Background(), request, nil); err == nil {
			t.Fatal("invalid signer accepted")
		}
	}
	failed := &SigV4Signer{Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) { return aws.Credentials{}, errors.New("unavailable") }), Region: "ap-south-1", Now: signer.Now}
	if err := failed.Sign(context.Background(), request, nil); err == nil {
		t.Fatal("credential failure ignored")
	}
}

func TestSignedSubmissionTreatsGateway403AsTerminal(t *testing.T) {
	svc := &Service{Signer: &SigV4Signer{Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "secret"}, nil
	}), Region: "ap-south-1", Now: func() time.Time { return fixedTime }}}
	svc.Client = clientFunc(func(request *http.Request) (*http.Response, error) {
		if !strings.Contains(request.Header.Get("Authorization"), "/execute-api/aws4_request") || request.Header.Get("Idempotency-Key") != "run_1" {
			t.Fatal(request.Header)
		}
		return &http.Response{StatusCode: http.StatusForbidden, Body: io.NopCloser(strings.NewReader(`{"message":"Forbidden"}`)), Header: make(http.Header)}, nil
	})
	outcome := svc.send(context.Background(), "https://processing.kagent.app/v1/event-ingestions", []byte(`{"id":"run_1"}`), "run_1", time.Second)
	if outcome.Code != "PROCESSOR_HTTP_403" || outcome.Retry || outcome.Receipt != nil {
		t.Fatal(outcome)
	}
	svc.Signer = requestSignerFunc(func(context.Context, *http.Request, []byte) error { return errors.New("credential failure") })
	if outcome := svc.send(context.Background(), "https://processing.kagent.app/v1/event-ingestions", nil, "run_1", time.Second); outcome.Code != "SIGNING_FAILED" || !outcome.Retry {
		t.Fatal(outcome)
	}
}
