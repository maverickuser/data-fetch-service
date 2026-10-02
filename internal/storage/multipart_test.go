package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type fakeS3 struct {
	data               []byte
	existing           string
	fail               string
	aborted, completed bool
	partSizes          []int
	noID, noETag       bool
}

func (f *fakeS3) CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	if f.fail == "create" {
		return nil, errors.New("create")
	}
	if f.noID {
		return &s3.CreateMultipartUploadOutput{}, nil
	}
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String("upload")}, nil
}
func (f *fakeS3) UploadPart(_ context.Context, in *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	if f.fail == "part" {
		return nil, errors.New("part")
	}
	raw, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	f.data = append(f.data, raw...)
	f.partSizes = append(f.partSizes, len(raw))
	if f.noETag {
		return &s3.UploadPartOutput{}, nil
	}
	return &s3.UploadPartOutput{ETag: aws.String("opaque")}, nil
}
func (f *fakeS3) CompleteMultipartUpload(_ context.Context, in *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	if aws.ToString(in.IfNoneMatch) != "*" {
		return nil, errors.New("missing conditional write")
	}
	if f.fail == "complete" || f.fail == "get" {
		return nil, errors.New("unknown completion")
	}
	f.completed = true
	return &s3.CompleteMultipartUploadOutput{}, nil
}
func (f *fakeS3) AbortMultipartUpload(ctx context.Context, _ *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	f.aborted = true
	if f.fail == "abort" {
		return nil, errors.New("abort")
	}
	if f.fail == "complete" {
		return nil, &smithy.GenericAPIError{Code: "NoSuchUpload"}
	}
	return &s3.AbortMultipartUploadOutput{}, nil
}
func (f *fakeS3) GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if f.fail == "get" {
		return nil, errors.New("get")
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader(f.existing))}, nil
}

func TestMultipartStreamsBoundedParts(t *testing.T) {
	api := &fakeS3{}
	store := &Multipart{Client: api, Bucket: "artifacts", MaxBytes: 2 * partBytes}
	raw := bytes.Repeat([]byte("a"), partBytes+23)
	artifact, err := store.Upload(context.Background(), "runs/test/raw/a.csv", bytes.NewReader(raw))
	if err != nil || artifact.Bytes != int64(len(raw)) || !bytes.Equal(api.data, raw) || !api.completed || api.aborted || len(api.partSizes) != 2 || api.partSizes[0] != partBytes || api.partSizes[1] != 23 {
		t.Fatal(artifact, err, api.partSizes)
	}
}

type failedReader struct{}

func (failedReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestMultipartAbortsIncompleteArtifacts(t *testing.T) {
	for _, test := range []struct {
		name   string
		api    fakeS3
		reader io.Reader
		max    int64
	}{
		{"source error", fakeS3{}, io.MultiReader(strings.NewReader("partial"), failedReader{}), 100},
		{"part failure", fakeS3{fail: "part"}, strings.NewReader("ok"), 100},
		{"missing etag", fakeS3{noETag: true}, strings.NewReader("ok"), 100},
		{"oversize", fakeS3{}, strings.NewReader("too large"), 2},
		{"empty", fakeS3{}, strings.NewReader(""), 100},
		{"abort failure", fakeS3{fail: "abort"}, failedReader{}, 100},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &Multipart{Client: &test.api, Bucket: "artifacts", MaxBytes: test.max}
			if _, err := store.Upload(context.Background(), "key", test.reader); err == nil || !test.api.aborted || test.api.completed {
				t.Fatal(err, test.api)
			}
		})
	}
}

func TestMultipartReconcilesImmutableCompletion(t *testing.T) {
	for _, test := range []struct {
		existing, fail string
		valid          bool
	}{{"same", "complete", true}, {"diff", "complete", false}, {"same!", "complete", false}, {"same", "get", false}} {
		api := &fakeS3{existing: test.existing, fail: test.fail}
		store := &Multipart{Client: api, Bucket: "artifacts", MaxBytes: 100}
		_, err := store.Upload(context.Background(), "key", strings.NewReader("same"))
		if (err == nil) != test.valid || !api.aborted {
			t.Fatal(test, err)
		}
	}
}

func TestMultipartInvalidSetupAndCancellation(t *testing.T) {
	for _, store := range []*Multipart{{}, {Client: &fakeS3{fail: "create"}, Bucket: "a", MaxBytes: 10}, {Client: &fakeS3{noID: true}, Bucket: "a", MaxBytes: 10}} {
		if _, err := store.Upload(context.Background(), "key", strings.NewReader("a")); err == nil {
			t.Fatal("invalid setup succeeded")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	api := &fakeS3{}
	store := &Multipart{Client: api, Bucket: "a", MaxBytes: 10}
	if _, err := store.Upload(ctx, "key", strings.NewReader("a")); !errors.Is(err, context.Canceled) || !api.aborted {
		t.Fatal(err, api.aborted)
	}
}
