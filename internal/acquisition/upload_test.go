package acquisition

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
)

type storageFunc func(context.Context, string, io.Reader) (Artifact, error)

func (f storageFunc) Upload(ctx context.Context, key string, r io.Reader) (Artifact, error) {
	return f(ctx, key, r)
}

type blockingSource struct {
	started, closed chan struct{}
	once            sync.Once
}

func (s *blockingSource) Read([]byte) (int, error) {
	close(s.started)
	<-s.closed
	return 0, io.ErrClosedPipe
}
func (s *blockingSource) Close() error { s.once.Do(func() { close(s.closed) }); return nil }

func TestUploadInterruptsBlockedSource(t *testing.T) {
	for _, cancelInstead := range []bool{false, true} {
		source := &blockingSource{started: make(chan struct{}), closed: make(chan struct{})}
		ctx, cancel := context.WithCancel(context.Background())
		store := storageFunc(func(_ context.Context, _ string, r io.Reader) (Artifact, error) {
			<-source.started
			if cancelInstead {
				cancel()
				_, err := io.Copy(io.Discard, r)
				return Artifact{}, err
			}
			return Artifact{}, errors.New("storage unavailable")
		})
		_, err := UploadValidated(ctx, store, "key", "json", source, Limits{MaxExtractedBytes: 100, MaxTokenBytes: 100, MaxJSONDepth: 2})
		cancel()
		if err == nil {
			t.Fatal("interrupted source succeeded")
		}
		select {
		case <-source.closed:
		default:
			t.Fatal("source not closed")
		}
	}
}

func TestValidatedUploadPreservesBytesAndRejectsPartial(t *testing.T) {
	for _, test := range []struct {
		body, format string
		max          int64
		valid        bool
	}{
		{"\xef\xbb\xbfa,b\r\n1,2\r\n", "csv", 100, true},
		{" {\"a\": 1}\n", "json", 100, true},
		{"{\"a\":", "json", 100, false},
		{"a,b\n1\n", "csv", 100, false},
		{"[0,1,2]", "json", 4, false},
		{"[0]", "json", 3, true},
		{"\xef\xbb\xbf<html>error</html>", "csv", 100, false},
	} {
		completed := false
		store := storageFunc(func(_ context.Context, key string, r io.Reader) (Artifact, error) {
			bytes, err := io.ReadAll(r)
			if err != nil {
				return Artifact{}, err
			}
			completed = true
			if string(bytes) != test.body {
				t.Fatal("bytes changed")
			}
			hash := sha256.Sum256(bytes)
			return Artifact{Key: key, Bytes: int64(len(bytes)), SHA256: hex.EncodeToString(hash[:])}, nil
		})
		_, err := UploadValidated(context.Background(), store, "raw/file", test.format, io.NopCloser(strings.NewReader(test.body)), Limits{MaxExtractedBytes: test.max, MaxTokenBytes: 100, MaxJSONDepth: 10})
		if (err == nil) != test.valid || completed != test.valid {
			t.Fatal(test, err, completed)
		}
	}
}

func TestValidatedUploadJoinsOnStorageFailure(t *testing.T) {
	failure := errors.New("upload failed")
	store := storageFunc(func(context.Context, string, io.Reader) (Artifact, error) { return Artifact{}, failure })
	_, err := UploadValidated(context.Background(), store, "raw/file", "json", io.NopCloser(&repeatedArray{remaining: 100000}), Limits{MaxExtractedBytes: 1000000, MaxTokenBytes: 100, MaxJSONDepth: 10})
	if !errors.Is(err, failure) {
		t.Fatal(err)
	}
}
