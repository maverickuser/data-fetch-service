// Package storage stores immutable source artifacts with bounded multipart buffers.
package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/maverickuser/data-fetch-service/internal/acquisition"
)

const partBytes = 5 << 20

// MultipartAPI is the SDK boundary for conditional artifact uploads and reconciliation.
type MultipartAPI interface {
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// Multipart uploads serial five-MiB parts and never overwrites an existing object.
type Multipart struct {
	Client   MultipartAPI
	Bucket   string
	MaxBytes int64
}

// Upload completes only after clean EOF; failures abort using an independent cleanup deadline.
func (m *Multipart) Upload(ctx context.Context, key string, source io.Reader) (_ acquisition.Artifact, resultErr error) {
	if m.Client == nil || m.Bucket == "" || key == "" || m.MaxBytes < 1 {
		return acquisition.Artifact{}, fmt.Errorf("invalid multipart configuration")
	}
	created, err := m.Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(m.Bucket), Key: aws.String(key)})
	if err != nil {
		return acquisition.Artifact{}, err
	}
	if created == nil || aws.ToString(created.UploadId) == "" {
		return acquisition.Artifact{}, fmt.Errorf("S3 returned no multipart upload ID")
	}
	id := created.UploadId
	completed := false
	defer func() {
		if !completed {
			resultErr = errors.Join(resultErr, m.abort(ctx, key, id))
		}
	}()
	artifact, parts, err := m.parts(ctx, key, id, source)
	if err != nil {
		return acquisition.Artifact{}, err
	}
	_, err = m.Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: aws.String(m.Bucket), Key: aws.String(key), UploadId: id, IfNoneMatch: aws.String("*"), MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}})
	if err == nil {
		completed = true
		return artifact, nil
	}
	// Both a conditional collision and a lost completion reply require byte verification.
	if verifyErr := m.verify(ctx, artifact); verifyErr != nil {
		return acquisition.Artifact{}, errors.Join(err, verifyErr)
	}
	return artifact, nil
}

// parts hashes unchanged bytes and holds only one part while awaiting each SDK upload.
func (m *Multipart) parts(ctx context.Context, key string, id *string, source io.Reader) (acquisition.Artifact, []types.CompletedPart, error) {
	artifact := acquisition.Artifact{Key: key}
	hash := sha256.New()
	buffer := make([]byte, partBytes)
	var parts []types.CompletedPart
	for {
		if err := ctx.Err(); err != nil {
			return artifact, nil, err
		}
		n, readErr := fillPart(source, buffer)
		if readErr != nil && readErr != io.EOF {
			return artifact, nil, readErr
		}
		if n > 0 {
			if int64(n) > m.MaxBytes-artifact.Bytes || len(parts) >= 10000 {
				return artifact, nil, fmt.Errorf("artifact upload limit exceeded")
			}
			artifact.Bytes += int64(n)
			if _, err := hash.Write(buffer[:n]); err != nil {
				return artifact, nil, err
			}
			number := int32(len(parts) + 1)
			part, err := m.Client.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String(m.Bucket), Key: aws.String(key), UploadId: id, PartNumber: aws.Int32(number), Body: bytes.NewReader(buffer[:n]), ContentLength: aws.Int64(int64(n))})
			if err != nil {
				return artifact, nil, err
			}
			if part == nil || aws.ToString(part.ETag) == "" {
				return artifact, nil, fmt.Errorf("S3 returned no part ETag")
			}
			parts = append(parts, types.CompletedPart{PartNumber: aws.Int32(number), ETag: part.ETag})
		}
		if readErr != nil {
			break
		}
	}
	if artifact.Bytes == 0 {
		return artifact, nil, fmt.Errorf("empty artifact")
	}
	artifact.SHA256 = hex.EncodeToString(hash.Sum(nil))
	return artifact, parts, nil
}

// fillPart preserves source errors, distinguishing clean short EOF from truncated streams.
func fillPart(source io.Reader, buffer []byte) (int, error) {
	total, empty := 0, 0
	for total < len(buffer) {
		n, err := source.Read(buffer[total:])
		total += n
		if err != nil {
			return total, err
		}
		if n == 0 {
			empty++
			if empty >= 100 {
				return total, io.ErrNoProgress
			}
		} else {
			empty = 0
		}
	}
	return total, nil
}

// verify reconciles ambiguous completion by bounded full-byte hashing, never by ETag.
func (m *Multipart) verify(ctx context.Context, want acquisition.Artifact) (resultErr error) {
	object, err := m.Client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(m.Bucket), Key: aws.String(want.Key)})
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, object.Body.Close()) }()
	hash := sha256.New()
	n, err := io.Copy(hash, io.LimitReader(object.Body, want.Bytes+1))
	if err != nil {
		return err
	}
	if n != want.Bytes || hex.EncodeToString(hash.Sum(nil)) != want.SHA256 {
		return fmt.Errorf("immutable artifact collision")
	}
	return nil
}

// abort retains a bounded cleanup opportunity after the invocation context is canceled.
func (m *Multipart) abort(parent context.Context, key string, id *string) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 10*time.Second)
	defer cancel()
	_, err := m.Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(m.Bucket), Key: aws.String(key), UploadId: id})
	var api smithy.APIError
	if errors.As(err, &api) && api.ErrorCode() == "NoSuchUpload" {
		return nil
	}
	return err
}
