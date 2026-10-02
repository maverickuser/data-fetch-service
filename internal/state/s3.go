package state

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// S3Client exposes only the SDK operations needed for bounded state records.
type S3Client interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// S3Objects bounds state-record reads; large acquisition artifacts use another adapter.
type S3Objects struct {
	client   S3Client
	bucket   string
	maxBytes int64
}

// NewS3 validates the bucket and explicit metadata memory budget.
func NewS3(client S3Client, bucket string, maxBytes int64) (*S3Objects, error) {
	if client == nil || bucket == "" || maxBytes < 1 || maxBytes > 64<<20 {
		return nil, fmt.Errorf("S3 state requires client, bucket and byte limit within 1..64 MiB")
	}
	return &S3Objects{client, bucket, maxBytes}, nil
}

// Get fully reads bounded metadata and always closes the response body.
func (s *S3Objects) Get(ctx context.Context, key string) (Object, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		return Object{}, classifyS3(err)
	}
	data, readErr := io.ReadAll(io.LimitReader(out.Body, s.maxBytes+1))
	closeErr := out.Body.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return Object{}, err
	}
	if int64(len(data)) > s.maxBytes {
		return Object{}, fmt.Errorf("state record exceeds %d bytes", s.maxBytes)
	}
	if out.ETag == nil || *out.ETag == "" || out.LastModified == nil {
		return Object{}, fmt.Errorf("S3 state response missing ETag or modification time")
	}
	return Object{data, *out.ETag, *out.LastModified}, nil
}

// Put performs one conditional SDK operation; conflicts remain visible to callers.
func (s *S3Objects) Put(ctx context.Context, key string, data []byte, match string) (string, error) {
	if int64(len(data)) > s.maxBytes {
		return "", fmt.Errorf("state record exceeds %d bytes", s.maxBytes)
	}
	in := &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key), Body: bytes.NewReader(data), ContentType: aws.String("application/json")}
	if match == "" {
		in.IfNoneMatch = aws.String("*")
	} else {
		in.IfMatch = aws.String(match)
	}
	out, err := s.client.PutObject(ctx, in)
	if err != nil {
		return "", classifyS3(err)
	}
	if out.ETag == nil || *out.ETag == "" {
		return "", fmt.Errorf("S3 state write missing ETag")
	}
	return *out.ETag, nil
}

// List returns one provider page, keeping the continuation token opaque.
func (s *S3Objects) List(ctx context.Context, prefix, token string, limit int32) (Page, error) {
	in := &s3.ListObjectsV2Input{Bucket: aws.String(s.bucket), Prefix: aws.String(prefix), MaxKeys: aws.Int32(limit)}
	if token != "" {
		in.ContinuationToken = aws.String(token)
	}
	out, err := s.client.ListObjectsV2(ctx, in)
	if err != nil {
		return Page{}, classifyS3(err)
	}
	page := Page{NextToken: aws.ToString(out.NextContinuationToken)}
	for _, object := range out.Contents {
		page.Keys = append(page.Keys, aws.ToString(object.Key))
	}
	if aws.ToBool(out.IsTruncated) && page.NextToken == "" {
		return Page{}, fmt.Errorf("truncated S3 listing missing continuation token")
	}
	return page, nil
}

// classifyS3 preserves provider diagnostics while making expected conflicts testable.
func classifyS3(err error) error {
	var api smithy.APIError
	if errors.As(err, &api) {
		switch api.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return errors.Join(ErrNotFound, err)
		case "PreconditionFailed", "ConditionalRequestConflict":
			return errors.Join(ErrConflict, err)
		}
	}
	return err
}
