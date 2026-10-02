package acquisition

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path"
	"strings"
	"time"
	"unicode"

	"github.com/maverickuser/data-fetch-service/internal/config"
)

// Fetcher coordinates source attempts, bounded validation, and immutable artifact uploads.
type Fetcher struct {
	Client   HTTPClient
	Storage  Storage
	Recorder Recorder
	Budget   *Budget
	Limits   Limits
	TempRoot string
	Now      func() time.Time
	Wait     func(context.Context, time.Duration) error
}

// Fetch records every attempt and retries only classified transient source failures.
func (f *Fetcher) Fetch(ctx context.Context, runID string, job config.ResolvedJob) (Result, error) {
	if f.Client == nil || f.Storage == nil || f.Recorder == nil || f.Budget == nil || f.Limits.MaxAttempts < 1 || f.Limits.MaxAttempts > 3 || !safeID(runID) || !safeID(job.ID) {
		return Result{}, fmt.Errorf("invalid fetch dependencies or identity")
	}
	now := f.Now
	if now == nil {
		now = time.Now
	}
	wait := f.Wait
	if wait == nil {
		wait = waitContext
	}
	for number := 1; number <= f.Limits.MaxAttempts; number++ {
		attempt := Attempt{Number: number, StartedAt: now().UTC()}
		if err := f.Recorder.Record(ctx, runID, job.ID, number, "started", attempt); err != nil {
			return Result{}, err
		}
		result, status, err := f.once(ctx, runID, job, number, attempt.StartedAt)
		attempt.StatusCode = status
		attempt.FinishedAt = now().UTC()
		var failure *Failure
		if err != nil {
			attempt.ErrorCode = "ACQUISITION_FAILED"
			if errors.As(err, &failure) {
				attempt.ErrorCode = failure.Code
			}
		}
		if recordErr := f.Recorder.Record(ctx, runID, job.ID, number, "finished", attempt); recordErr != nil {
			return Result{}, errors.Join(err, recordErr)
		}
		if err == nil {
			result.Attempts = number
			return result, nil
		}
		if failure == nil || !failure.Retryable || number == f.Limits.MaxAttempts || ctx.Err() != nil {
			return Result{}, err
		}
		delay := time.Duration(1<<(number-1)) * time.Second
		delay = delay/2 + time.Duration(rand.Int64N(int64(delay/2)))
		if failure.RetryAfter > delay {
			delay = failure.RetryAfter
		}
		if waitErr := wait(ctx, delay); waitErr != nil {
			return Result{}, errors.Join(err, waitErr)
		}
	}
	return Result{}, fmt.Errorf("source attempts exhausted")
}

// once owns a unique temporary directory and returns only fully validated artifacts.
func (f *Fetcher) once(ctx context.Context, runID string, job config.ResolvedJob, attempt int, now time.Time) (_ Result, status int, resultErr error) {
	directory, err := os.MkdirTemp(f.TempRoot, fmt.Sprintf("%s-%s-%d-", runID, job.ID, attempt))
	if err != nil {
		return Result{}, 0, err
	}
	defer func() {
		if err := os.RemoveAll(directory); err != nil {
			resultErr = &Failure{Code: "TEMPORARY_CLEANUP", Cause: errors.Join(resultErr, err)}
		}
	}()
	sourceFormat := job.Format
	if job.MemberPath != "" {
		sourceFormat = "zip"
	}
	download, status, err := DownloadSource(ctx, f.Client, f.Budget, f.Limits, job.URL, sourceFormat, directory, now)
	if err != nil {
		return Result{}, status, err
	}
	defer func() {
		if err := download.Close(); err != nil {
			resultErr = &Failure{Code: "TEMPORARY_CLEANUP", Cause: errors.Join(resultErr, err)}
		}
	}()
	name := job.Filename
	if name == "" && job.MemberPath != "" {
		name = path.Base(job.MemberPath)
	}
	name, err = filename(name, job.URL, download.Disposition, job.ID, job.Format)
	if err != nil {
		return Result{}, status, err
	}
	key := fmt.Sprintf("runs/%s/raw/%s/%d/%s", runID, job.ID, attempt, name)
	result := Result{JobID: job.ID, Filename: name, Format: job.Format}
	if job.MemberPath == "" {
		input, err := os.Open(download.File.Name())
		if err != nil {
			return Result{}, status, err
		}
		result.Artifact, err = UploadValidated(ctx, f.Storage, key, job.Format, input, f.Limits)
		return result, status, err
	}
	member, err := zipMember(download.File, download.Bytes, job.MemberPath, f.Limits)
	if err != nil {
		return Result{}, status, err
	}
	input, err := member.Open()
	if err != nil {
		return Result{}, status, err
	}
	result.Artifact, err = UploadValidated(ctx, f.Storage, key, job.Format, input, f.Limits)
	if err != nil {
		return Result{}, status, err
	}
	archiveName, err := filename(job.SourceFilename, job.URL, download.Disposition, job.ID, "zip")
	if err != nil {
		return Result{}, status, err
	}
	archiveKey := fmt.Sprintf("runs/%s/downloads/%s/%d/%s", runID, job.ID, attempt, archiveName)
	archive, err := f.Storage.Upload(ctx, archiveKey, download.File)
	if err != nil {
		return Result{}, status, err
	}
	result.Archive = &archive
	return result, status, nil
}

// safeID keeps run and job identifiers inside a single storage/path component.
func safeID(value string) bool {
	return value != "" && value != "." && value != ".." && !strings.ContainsAny(value, "/\\:") && strings.IndexFunc(value, unicode.IsControl) < 0
}

// waitContext stops source backoff promptly when the owning execution is canceled.
func waitContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
