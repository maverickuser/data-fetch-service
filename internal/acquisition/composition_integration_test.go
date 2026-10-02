//go:build integration

package acquisition_test

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/maverickuser/data-fetch-service/internal/acquisition"
	"github.com/maverickuser/data-fetch-service/internal/config"
	"github.com/maverickuser/data-fetch-service/internal/storage"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type records map[string][]byte

func (r records) Create(_ context.Context, key string, data []byte) error {
	r[key] = bytes.Clone(data)
	return nil
}

func TestAcquisitionSDKComposition(t *testing.T) {
	var archive bytes.Buffer
	writer := zip.NewWriter(&archive)
	for _, name := range []string{"fgroup21092026.csv", "icdm21092026.csv", "wdm21092026.csv"} {
		entry, err := writer.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(entry, "isin,price\nINE121A07QY9,100\n"); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		body []byte
		job  config.ResolvedJob
		want int
	}{
		{"BSE", archive.Bytes(), config.ResolvedJob{ID: "debt", URL: "https://bse.example/DEBTBHAVCOPY21092026.zip", MemberPath: "fgroup21092026.csv", Filename: "BSE_fgroup21092026.csv", Format: "csv"}, 2},
		{"NSDL", []byte(`{"isin":"INE121A07QY9"}`), config.ResolvedJob{ID: "details", URL: "https://nsdl.example/api", Filename: "INE121A07QY9_details.json", Format: "json"}, 1},
		{"invalid NSDL", []byte(`{"isin":`), config.ResolvedJob{ID: "details", URL: "https://nsdl.example/api", Filename: "INE121A07QY9_details.json", Format: "json"}, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			objects := map[string][]byte{}
			parts := map[string][]byte{}
			aborted := 0
			sdkTransport := transportFunc(func(r *http.Request) (*http.Response, error) {
				response := ""
				headers := http.Header{}
				switch {
				case r.Method == "POST" && r.URL.Query().Has("uploads"):
					response = "<InitiateMultipartUploadResult><UploadId>u1</UploadId></InitiateMultipartUploadResult>"
				case r.Method == "PUT":
					data, err := io.ReadAll(r.Body)
					if err != nil {
						return nil, err
					}
					parts[r.URL.Path] = append(parts[r.URL.Path], data...)
					headers.Set("ETag", "\"part\"")
				case r.Method == "POST":
					if r.Header.Get("If-None-Match") != "*" {
						t.Fatal("missing immutable completion condition")
					}
					objects[r.URL.Path] = bytes.Clone(parts[r.URL.Path])
					response = "<CompleteMultipartUploadResult><ETag>opaque</ETag></CompleteMultipartUploadResult>"
				case r.Method == "DELETE":
					aborted++
				default:
					t.Fatal("unexpected S3 request", r.Method, r.URL)
				}
				return &http.Response{StatusCode: 200, Header: headers, Body: io.NopCloser(strings.NewReader(response)), Request: r}, nil
			})
			client := s3.New(s3.Options{Region: "ap-south-1", BaseEndpoint: aws.String("https://s3.example.invalid"), UsePathStyle: true, RetryMaxAttempts: 1, RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired, HTTPClient: &http.Client{Transport: sdkTransport}, Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
				return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
			})})
			state := records{}
			fetcher := acquisition.Fetcher{Client: &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/octet-stream"}}, ContentLength: int64(len(test.body)), Body: io.NopCloser(bytes.NewReader(test.body)), Request: r}, nil
			})}, Storage: &storage.Multipart{Client: client, Bucket: "artifacts", MaxBytes: 1 << 20}, Recorder: acquisition.StateRecorder{Store: state}, Budget: acquisition.NewBudget(1 << 20), Limits: acquisition.Limits{MaxAttempts: 3, RequestTimeout: time.Second, MaxDownloadBytes: 1 << 20, MaxExtractedBytes: 1 << 20, MaxTokenBytes: 1024, MaxZipEntries: 10, MaxZipMetadataBytes: 4096, MaxCompressionRatio: 100, MaxJSONDepth: 10}, TempRoot: t.TempDir()}
			result, err := fetcher.Fetch(context.Background(), "run", test.job)
			if (err == nil) != (test.want > 0) || len(objects) != test.want || len(state) != 2 {
				t.Fatal(result, err, objects, state)
			}
			if test.want == 0 && aborted != 1 {
				t.Fatal("invalid stream did not abort", aborted)
			}
			if test.want == 1 && !bytes.Equal(objects["/artifacts/"+result.Artifact.Key], test.body) {
				t.Fatal("JSON bytes changed")
			}
			if test.want == 2 && !bytes.Equal(objects["/artifacts/"+result.Archive.Key], test.body) {
				t.Fatal("archive bytes changed")
			}
		})
	}
}
