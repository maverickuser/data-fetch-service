//go:build integration

package state

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestSDKConditionalProtocol runs the real SDK serializer and error decoder without AWS.
func TestSDKConditionalProtocol(t *testing.T) {
	var stored []byte
	etag := "\"opaque-1\""
	puts := 0
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/state/runs/r/snapshot.json" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		status, body := 200, ""
		headers := http.Header{"Etag": []string{etag}, "Last-Modified": []string{"Tue, 29 Sep 2026 00:00:00 GMT"}}
		switch r.Method {
		case http.MethodPut:
			puts++
			if r.Header.Get("If-None-Match") != "*" {
				t.Fatal("missing creation precondition")
			}
			data, err := io.ReadAll(r.Body)
			if err != nil {
				return nil, err
			}
			if stored == nil {
				stored = data
			} else {
				status = 412
				body = "<Error><Code>PreconditionFailed</Code><Message>exists</Message></Error>"
			}
		case http.MethodGet:
			body = string(stored)
		default:
			t.Fatal(r.Method)
		}
		return &http.Response{StatusCode: status, Header: headers, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})
	client := s3.New(s3.Options{Region: "ap-south-1", BaseEndpoint: aws.String("https://s3.example.invalid"), UsePathStyle: true, RetryMaxAttempts: 1, HTTPClient: &http.Client{Transport: transport}, Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
	})})
	objects, err := NewS3(client, "state", 1024)
	if err != nil {
		t.Fatal(err)
	}
	store := New(objects)
	ctx := context.Background()
	for range 2 {
		if err := store.Create(ctx, "runs/r/snapshot.json", []byte(`{"id":"r"}`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Create(ctx, "runs/r/snapshot.json", []byte(`{"id":"different"}`)); !errors.Is(err, ErrIntegrity) {
		t.Fatal(err)
	}
	if puts != 3 {
		t.Fatalf("unexpected hidden retry count: %d", puts)
	}
}
