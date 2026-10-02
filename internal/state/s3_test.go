package state

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

type sdkFake struct {
	get  func(*s3.GetObjectInput) (*s3.GetObjectOutput, error)
	put  func(*s3.PutObjectInput) (*s3.PutObjectOutput, error)
	list func(*s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error)
}

func (f sdkFake) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return f.get(in)
}
func (f sdkFake) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return f.put(in)
}
func (f sdkFake) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return f.list(in)
}

type trackedBody struct {
	io.Reader
	closed   bool
	closeErr error
}

func (b *trackedBody) Close() error { b.closed = true; return b.closeErr }

type failedReader struct{}

func (failedReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }

func TestS3ConditionalHeaders(t *testing.T) {
	for _, match := range []string{"", "opaque-etag"} {
		client := sdkFake{put: func(in *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
			if aws.ToString(in.Bucket) != "state" || aws.ToString(in.Key) != "key" || aws.ToString(in.ContentType) != "application/json" {
				t.Fatal(in)
			}
			if match == "" && (aws.ToString(in.IfNoneMatch) != "*" || in.IfMatch != nil) {
				t.Fatal("unconditional creation")
			}
			if match != "" && (aws.ToString(in.IfMatch) != match || in.IfNoneMatch != nil) {
				t.Fatal("unconditional update")
			}
			b, err := io.ReadAll(in.Body)
			if err != nil || string(b) != "{}" {
				t.Fatal(string(b), err)
			}
			return &s3.PutObjectOutput{ETag: aws.String("next")}, nil
		}}
		s, err := NewS3(client, "state", 20)
		if err != nil {
			t.Fatal(err)
		}
		etag, err := s.Put(context.Background(), "key", []byte("{}"), match)
		if err != nil || etag != "next" {
			t.Fatal(etag, err)
		}
	}
}

func TestS3BoundedReadClosesOnEveryOutcome(t *testing.T) {
	for _, scenario := range []string{"valid", "oversized", "read failure", "close failure", "missing metadata"} {
		t.Run(scenario, func(t *testing.T) {
			body := &trackedBody{Reader: strings.NewReader("{}")}
			if scenario == "oversized" {
				body.Reader = strings.NewReader(strings.Repeat("x", 21))
			}
			if scenario == "read failure" {
				body.Reader = failedReader{}
			}
			if scenario == "close failure" {
				body.closeErr = errors.New("close failed")
			}
			out := &s3.GetObjectOutput{Body: body, ETag: aws.String("opaque"), LastModified: aws.Time(time.Now())}
			if scenario == "missing metadata" {
				out.ETag = nil
			}
			s, err := NewS3(sdkFake{get: func(in *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
				if aws.ToString(in.Bucket) != "state" || aws.ToString(in.Key) != "key" {
					t.Fatal(in)
				}
				return out, nil
			}}, "state", 20)
			if err != nil {
				t.Fatal(err)
			}
			object, err := s.Get(context.Background(), "key")
			if !body.closed {
				t.Fatal("response body leaked")
			}
			if scenario == "valid" {
				if err != nil || string(object.Data) != "{}" || object.ETag != "opaque" {
					t.Fatal(object, err)
				}
			} else if err == nil {
				t.Fatal("invalid read accepted")
			}
		})
	}
}

func TestS3ErrorsLimitsAndPagination(t *testing.T) {
	ctx := context.Background()
	for code, want := range map[string]error{"NoSuchKey": ErrNotFound, "NotFound": ErrNotFound, "PreconditionFailed": ErrConflict, "ConditionalRequestConflict": ErrConflict, "AccessDenied": nil} {
		provider := &smithy.GenericAPIError{Code: code, Message: "provider detail"}
		client := sdkFake{get: func(*s3.GetObjectInput) (*s3.GetObjectOutput, error) { return nil, provider }, put: func(*s3.PutObjectInput) (*s3.PutObjectOutput, error) { return nil, provider }, list: func(*s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) { return nil, provider }}
		s, err := NewS3(client, "state", 20)
		if err != nil {
			t.Fatal(err)
		}
		_, getErr := s.Get(ctx, "k")
		_, putErr := s.Put(ctx, "k", nil, "")
		_, listErr := s.List(ctx, "k/", "", 1)
		for _, got := range []error{getErr, putErr, listErr} {
			if !errors.Is(got, provider) || (want != nil && !errors.Is(got, want)) {
				t.Fatal(got)
			}
		}
	}
	plain := errors.New("transport timeout")
	if classifyS3(plain) != plain {
		t.Fatal("lost transport error")
	}
	for _, tc := range []struct {
		client S3Client
		bucket string
		limit  int64
	}{{nil, "state", 1}, {sdkFake{}, "", 1}, {sdkFake{}, "state", 0}, {sdkFake{}, "state", 65 << 20}} {
		if _, err := NewS3(tc.client, tc.bucket, tc.limit); err == nil {
			t.Fatal("invalid adapter config")
		}
	}
	s, err := NewS3(sdkFake{put: func(*s3.PutObjectInput) (*s3.PutObjectOutput, error) { return &s3.PutObjectOutput{}, nil }}, "state", 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, "k", []byte("long"), ""); err == nil {
		t.Fatal("oversized write")
	}
	if _, err := s.Put(ctx, "k", nil, ""); err == nil {
		t.Fatal("missing ETag")
	}
	for _, token := range []string{"", "opaque-next"} {
		s, err := NewS3(sdkFake{list: func(in *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
			if aws.ToString(in.Bucket) != "state" || aws.ToString(in.Prefix) != "runs/" || aws.ToString(in.ContinuationToken) != token || aws.ToInt32(in.MaxKeys) != 1 {
				t.Fatal(in)
			}
			return &s3.ListObjectsV2Output{Contents: []types.Object{{Key: aws.String("runs/r")}}, IsTruncated: aws.Bool(true), NextContinuationToken: aws.String("next")}, nil
		}}, "state", 20)
		if err != nil {
			t.Fatal(err)
		}
		page, err := s.List(ctx, "runs/", token, 1)
		if err != nil || len(page.Keys) != 1 || page.Keys[0] != "runs/r" || page.NextToken != "next" {
			t.Fatal(page, err)
		}
	}
	s, err = NewS3(sdkFake{list: func(*s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
		return &s3.ListObjectsV2Output{IsTruncated: aws.Bool(true)}, nil
	}}, "state", 20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.List(ctx, "runs/", "", 1); err == nil {
		t.Fatal("truncated response accepted without cursor")
	}
}
