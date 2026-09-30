package acquisition

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
)

// UploadValidated streams unchanged bytes through validation and joins the owned parser.
// It owns source; Close must interrupt a blocked Read. Storage must abort on read errors.
func UploadValidated(ctx context.Context, storage Storage, key, format string, source io.ReadCloser, limits Limits) (Artifact, error) {
	if source == nil {
		return Artifact{}, fmt.Errorf("missing upload source")
	}
	if limits.MaxExtractedBytes < 1 || storage == nil {
		return Artifact{}, errors.Join(fmt.Errorf("invalid upload limits or storage"), source.Close())
	}
	reader, writer := io.Pipe()
	var sourceCloseErr error
	var closeOnce sync.Once
	closeSource := func() { closeOnce.Do(func() { sourceCloseErr = source.Close() }) }
	canceled := make(chan struct{})
	stopCancel := context.AfterFunc(ctx, func() {
		closeSource()
		_ = reader.CloseWithError(ctx.Err()) // Pipe close always returns nil.
		close(canceled)
	})
	defer func() {
		if !stopCancel() {
			<-canceled
		}
	}()
	finished := make(chan validationResult, 1)
	go func() {
		hash := sha256.New()
		bounded := &boundedInput{ctx: ctx, source: source, remaining: limits.MaxExtractedBytes}
		input := io.TeeReader(bounded, io.MultiWriter(writer, hash))
		var err error
		switch format {
		case "json":
			err = ValidateJSON(input, limits.MaxTokenBytes, limits.MaxJSONDepth)
		case "csv":
			err = ValidateCSV(input, limits.MaxTokenBytes)
		default:
			err = fmt.Errorf("unsupported validation format")
		}
		if err != nil {
			err = fmt.Errorf("source validation failed: %w", err)
		}
		closeErr := writer.CloseWithError(err)
		finished <- validationResult{bytes: bounded.read, hash: hex.EncodeToString(hash.Sum(nil)), err: errors.Join(err, closeErr)}
	}()
	artifact, uploadErr := storage.Upload(ctx, key, reader)
	// Unblock the parser if storage fails or incorrectly stops before EOF.
	closeErr := reader.CloseWithError(io.ErrClosedPipe)
	closeSource()
	validation := <-finished
	if err := errors.Join(uploadErr, closeErr, sourceCloseErr, validation.err, ctx.Err()); err != nil {
		return Artifact{}, err
	}
	if artifact.Key != key || artifact.Bytes != validation.bytes || artifact.SHA256 != validation.hash {
		return Artifact{}, fmt.Errorf("uploaded artifact does not match validated bytes")
	}
	return artifact, nil
}

type validationResult struct {
	bytes int64
	hash  string
	err   error
}

// boundedInput detects excess bytes instead of presenting a truncated stream as clean EOF.
type boundedInput struct {
	ctx             context.Context
	source          io.Reader
	remaining, read int64
}

// Read checks cancellation and probes at the byte boundary to detect oversized content.
func (r *boundedInput) Read(out []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if len(out) == 0 {
		return 0, nil
	}
	if r.remaining == 0 {
		var probe [1]byte
		n, err := r.source.Read(probe[:])
		if n > 0 {
			return 0, fmt.Errorf("extracted byte limit exceeded")
		}
		return 0, err
	}
	if int64(len(out)) > r.remaining {
		out = out[:r.remaining]
	}
	n, err := r.source.Read(out)
	r.remaining -= int64(n)
	r.read += int64(n)
	return n, err
}
