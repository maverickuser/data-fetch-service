package acquisition

import (
	"context"
	"io"
	"time"
)

// Limits bounds source parsing, network attempts and temporary-file consumption.
type Limits struct {
	MaxDownloadBytes, MaxExtractedBytes, MaxTokenBytes, MaxZipMetadataBytes int64
	MaxZipEntries, MaxCompressionRatio, MaxJSONDepth, MaxAttempts           int
	RequestTimeout                                                          time.Duration
}

// Artifact describes a completely uploaded object and the hash of its unchanged bytes.
type Artifact struct {
	Key    string `json:"key"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

// Storage completes an object only after the input stream reaches a clean EOF.
type Storage interface {
	Upload(context.Context, string, io.Reader) (Artifact, error)
}

// Attempt records source work separately from AWS SDK retries.
type Attempt struct {
	Number     int       `json:"attempt"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
	StatusCode int       `json:"status_code,omitempty"`
	ErrorCode  string    `json:"error_code,omitempty"`
}

// Recorder persists immutable per-job started/finished attempt records.
type Recorder interface {
	Record(context.Context, string, string, int, string, Attempt) error
}

// Result contains one processing artifact and an optional preserved source archive.
type Result struct {
	JobID    string    `json:"job_id"`
	Filename string    `json:"filename"`
	Format   string    `json:"format"`
	Artifact Artifact  `json:"artifact"`
	Archive  *Artifact `json:"archive,omitempty"`
	Attempts int       `json:"attempts"`
}

// Failure classifies retryable source failures without returning unbounded response bodies.
type Failure struct {
	Code       string
	Retryable  bool
	RetryAfter time.Duration
	Cause      error
}

// Error returns the stable acquisition error code.
func (f *Failure) Error() string { return f.Code }

// Unwrap preserves the underlying I/O or validation cause for local diagnostics.
func (f *Failure) Unwrap() error { return f.Cause }
