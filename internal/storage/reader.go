package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ReaderAPI is the single-object S3 boundary for complete immutable manifests.
type ReaderAPI interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// Reader enforces a small limit when loading the manifest for processor admission.
type Reader struct {
	Client   ReaderAPI
	Bucket   string
	MaxBytes int64
}

// Read retrieves only a run-specific manifest and rejects truncation or oversized data.
func (r *Reader) Read(ctx context.Context, key string) (_ []byte, resultErr error) {
	if r.Client == nil || r.Bucket == "" || r.MaxBytes < 1 || !strings.HasPrefix(key, "runs/") || !strings.HasSuffix(key, "/manifest.json") || strings.Contains(key, "..") {
		return nil, fmt.Errorf("invalid manifest reader or key")
	}
	object, err := r.Client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(r.Bucket), Key: aws.String(key)})
	if err != nil {
		return nil, err
	}
	if object == nil || object.Body == nil {
		return nil, fmt.Errorf("missing manifest body")
	}
	defer func() { resultErr = errors.Join(resultErr, object.Body.Close()) }()
	if object.ContentLength != nil && (*object.ContentLength < 1 || *object.ContentLength > r.MaxBytes) {
		return nil, fmt.Errorf("manifest size invalid")
	}
	data, err := io.ReadAll(io.LimitReader(object.Body, r.MaxBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 || int64(len(data)) > r.MaxBytes {
		return nil, fmt.Errorf("manifest size invalid")
	}
	return data, nil
}
