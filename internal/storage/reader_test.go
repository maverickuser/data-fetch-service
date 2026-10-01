package storage

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type readerAPI func(context.Context, *s3.GetObjectInput) (*s3.GetObjectOutput, error)

func (f readerAPI) GetObject(ctx context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return f(ctx, in)
}

func TestManifestReaderBoundsAndErrors(t *testing.T) {
	ctx := context.Background()
	reader := &Reader{Bucket: "artifacts", MaxBytes: 5}
	key := "runs/run_1/manifest.json"
	if _, err := reader.Read(ctx, key); err == nil {
		t.Fatal("missing SDK")
	}
	reader.Client = readerAPI(func(_ context.Context, in *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
		if aws.ToString(in.Bucket) != "artifacts" || aws.ToString(in.Key) != key {
			t.Fatal(in)
		}
		return &s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader("valid")), ContentLength: aws.Int64(5)}, nil
	})
	data, err := reader.Read(ctx, key)
	if err != nil || string(data) != "valid" {
		t.Fatal(string(data), err)
	}
	for _, bad := range []string{"other", "runs/../manifest.json", "runs/run_1/other.json"} {
		if _, err := reader.Read(ctx, bad); err == nil {
			t.Fatal(bad)
		}
	}
	for _, scenario := range []string{"SDK", "nil", "large-header", "large-body", "empty", "read-error", "close-error"} {
		t.Run(scenario, func(t *testing.T) {
			reader.Client = readerAPI(func(context.Context, *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
				switch scenario {
				case "SDK":
					return nil, errors.New("S3 unavailable")
				case "nil":
					return nil, nil
				case "large-header":
					return &s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader("valid")), ContentLength: aws.Int64(6)}, nil
				case "large-body":
					return &s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader("123456"))}, nil
				case "empty":
					return &s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader(""))}, nil
				case "read-error":
					return &s3.GetObjectOutput{Body: brokenBody{readErr: errors.New("broken")}}, nil
				default:
					return &s3.GetObjectOutput{Body: brokenBody{data: "valid", closeErr: errors.New("close")}}, nil
				}
			})
			if _, err := reader.Read(ctx, key); err == nil {
				t.Fatal("expected failure")
			}
		})
	}
}

type brokenBody struct {
	data              string
	readErr, closeErr error
}

func (b brokenBody) Read(p []byte) (int, error) {
	if b.readErr != nil {
		return 0, b.readErr
	}
	return strings.NewReader(b.data).Read(p)
}
func (b brokenBody) Close() error { return b.closeErr }
