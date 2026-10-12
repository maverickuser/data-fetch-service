package acquisition

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// HTTPClient permits deterministic source responses without replacing acquisition logic.
type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

// Download owns a temporary file and its byte reservation until Close is called.
type Download struct {
	File        *os.File
	Bytes       int64
	Disposition string
	release     func()
}

// Close releases disk capacity only after removal succeeds; failed cleanup stays charged.
func (d *Download) Close() error {
	closeErr := d.File.Close()
	if errors.Is(closeErr, os.ErrClosed) {
		closeErr = nil
	}
	removeErr := os.Remove(d.File.Name())
	if removeErr == nil || errors.Is(removeErr, os.ErrNotExist) {
		d.release()
		removeErr = nil
	}
	return errors.Join(closeErr, removeErr)
}

// DownloadSource spools one complete identity-encoded response under a shared disk budget.
// The caller owns retries and must close every successful Download.
func DownloadSource(ctx context.Context, client HTTPClient, budget *Budget, limits Limits, source, format, directory string, now time.Time) (_ *Download, status int, resultErr error) {
	if limits.MaxDownloadBytes < 1 || limits.RequestTimeout <= 0 || budget == nil || client == nil {
		return nil, 0, fmt.Errorf("invalid download dependencies or limits")
	}
	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, limits.RequestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, source, nil)
	if err != nil {
		return nil, 0, err
	}
	if err := sourceURL(request.URL); err != nil {
		return nil, 0, err
	}
	request.Header.Set("Accept-Encoding", "identity")
	if limits.UserAgent != "" {
		request.Header.Set("User-Agent", limits.UserAgent)
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, 0, &Failure{Code: "SOURCE_TRANSPORT", Retryable: !errors.Is(err, ErrUnsafeDestination) && parent.Err() == nil, Cause: err}
	}
	var download *Download
	defer func() {
		resultErr = errors.Join(resultErr, response.Body.Close())
		if resultErr != nil && download != nil {
			if cleanupErr := download.Close(); cleanupErr != nil {
				resultErr = &Failure{Code: "TEMPORARY_CLEANUP", Cause: errors.Join(resultErr, cleanupErr)}
			}
		}
	}()
	status = response.StatusCode
	body, noRecord := noRecordBody(response, format)
	if !noRecord {
		if err := checkStatus(response, now); err != nil {
			return nil, status, err
		}
	}
	if err := checkRepresentation(response, format); err != nil {
		return nil, status, err
	}
	reservation := response.ContentLength
	if reservation <= 0 {
		reservation = limits.MaxDownloadBytes
	}
	if reservation > limits.MaxDownloadBytes {
		return nil, status, &Failure{Code: "DOWNLOAD_LIMIT"}
	}
	release, err := budget.Acquire(ctx, reservation)
	if err != nil {
		return nil, status, err
	}
	file, err := os.CreateTemp(directory, "source-*")
	if err != nil {
		release()
		return nil, status, err
	}
	download = &Download{File: file, Disposition: response.Header.Get("Content-Disposition"), release: release}
	// Limiting writes to the reservation also bounds bodies that lie about Content-Length.
	download.Bytes, err = io.Copy(localWriter{file}, io.LimitReader(body, reservation))
	if err != nil {
		var local *Failure
		if errors.As(err, &local) {
			return nil, status, err
		}
		return nil, status, &Failure{Code: "SOURCE_READ", Retryable: parent.Err() == nil, Cause: err}
	}
	var extra [1]byte
	_, err = io.ReadFull(body, extra[:])
	if err == nil {
		return nil, status, &Failure{Code: "DOWNLOAD_LIMIT"}
	}
	if err != io.EOF {
		return nil, status, &Failure{Code: "SOURCE_READ", Retryable: parent.Err() == nil, Cause: err}
	}
	if response.ContentLength >= 0 && download.Bytes != response.ContentLength {
		return nil, status, &Failure{Code: "SOURCE_TRUNCATED", Retryable: true}
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, status, err
	}
	return download, status, nil
}

// localWriter distinguishes temporary-storage failures from retryable network reads.
type localWriter struct{ io.Writer }

// Write labels disk errors without losing their underlying cause.
func (w localWriter) Write(data []byte) (int, error) {
	n, err := w.Writer.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return n, &Failure{Code: "TEMPORARY_WRITE", Cause: err}
	}
	return n, nil
}

// noRecordMessage is the message NSDL returns with HTTP 400 when an ISIN has no data for an endpoint.
const noRecordMessage = "No Record Found"

// noRecordPeekBytes bounds how much of a 400 body is buffered to recognise a no-record answer.
const noRecordPeekBytes = 4 << 10

// noRecordBody returns the reader to spool and whether a JSON 400 response is a source's
// "No Record Found" answer, which is stored as the job's document instead of failing the run.
// It buffers at most noRecordPeekBytes and replays them ahead of the unread body; an unreadable,
// oversized, or different 400 body is reported as not a no-record answer.
func noRecordBody(response *http.Response, format string) (io.Reader, bool) {
	if response.StatusCode != http.StatusBadRequest || format != "json" {
		return response.Body, false
	}
	peeked, err := io.ReadAll(io.LimitReader(response.Body, noRecordPeekBytes+1))
	if err != nil || len(peeked) > noRecordPeekBytes {
		return response.Body, false
	}
	var answer struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(peeked, &answer) != nil || !strings.EqualFold(strings.TrimSpace(answer.Message), noRecordMessage) {
		return response.Body, false
	}
	return io.MultiReader(bytes.NewReader(peeked), response.Body), true
}

// checkStatus rejects partial/error responses and classifies whether the source attempt may retry.
func checkStatus(response *http.Response, now time.Time) error {
	status := response.StatusCode
	if status == http.StatusNotFound {
		// A distinct code lets operators and smoke checks tell an unpublished file from other source errors.
		return &Failure{Code: "SOURCE_NOT_FOUND", RetryAfter: retryAfter(response.Header.Get("Retry-After"), now)}
	}
	if status != http.StatusOK {
		return &Failure{Code: "SOURCE_HTTP", Retryable: status == 408 || status == 429 || status >= 500 && status <= 599, RetryAfter: retryAfter(response.Header.Get("Retry-After"), now)}
	}
	return nil
}

// checkRepresentation rejects encoded or wrongly typed documents before allocating temporary storage.
func checkRepresentation(response *http.Response, format string) error {
	encoding := strings.TrimSpace(response.Header.Get("Content-Encoding"))
	if encoding != "" && !strings.EqualFold(encoding, "identity") {
		return &Failure{Code: "SOURCE_ENCODING"}
	}
	media, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil {
		return &Failure{Code: "SOURCE_CONTENT_TYPE", Cause: err}
	}
	allowed := media == "application/octet-stream"
	switch format {
	case "csv":
		allowed = allowed || media == "text/csv" || media == "application/csv" || media == "text/plain"
	case "json":
		allowed = allowed || media == "application/json" || strings.HasPrefix(media, "application/") && strings.HasSuffix(media, "+json") || media == "text/plain"
	case "zip":
		allowed = allowed || media == "application/zip" || media == "application/x-zip-compressed"
	default:
		return &Failure{Code: "SOURCE_FORMAT"}
	}
	if !allowed {
		return &Failure{Code: "SOURCE_CONTENT_TYPE"}
	}
	return nil
}

// retryAfter preserves server-requested backoff; the owning context bounds the wait.
func retryAfter(value string, now time.Time) time.Duration {
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds < 0 {
			return 0
		}
		if seconds > int64((1<<63-1)/time.Second) {
			return time.Duration(1<<63 - 1)
		}
		return time.Duration(seconds) * time.Second
	}
	if date, err := http.ParseTime(value); err == nil {
		delay := date.Sub(now)
		if delay < 0 {
			return 0
		}
		return delay
	}
	return 0
}
