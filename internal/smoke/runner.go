// Package smoke verifies a deployed service against its real sources and processor.
package smoke

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	bseEvent     = "daily-bhavcopy"
	nsdlEvent    = "nsdl-bond-data"
	smokeSource  = "urn:data-fetch-service:smoke"
	scheduleName = "data-fetch-service-daily-bhavcopy"
	scheduleCron = "cron(0 20 ? * MON-FRI *)"
	scheduleZone = "Asia/Kolkata"
	notFoundCode = "SOURCE_NOT_FOUND"
	maxBodyBytes = 2 << 20
)

var (
	isinPattern = regexp.MustCompile(`^[A-Z0-9]{12}$`)
	idPattern   = regexp.MustCompile(`^[A-Za-z0-9-]{1,64}$`)
	nsdlJobs    = []string{"isin-details", "instrument-details", "coupon-details", "redemptions", "listings", "credit-ratings"}
)

// HTTPClient calls the deployed service API.
type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

// Queue publishes one message body to the deployed ingress queue.
type Queue interface {
	Send(context.Context, string) error
}

// Objects reports whether a key exists in the private artifact bucket.
type Objects interface {
	Exists(context.Context, string) (bool, error)
}

// Schedule is the deployed state of one EventBridge Scheduler schedule.
type Schedule struct {
	Expression string
	Timezone   string
	State      string
}

// Schedules reads a deployed schedule by name.
type Schedules interface {
	Get(context.Context, string) (Schedule, error)
}

// Config holds the explicit smoke inputs; nothing is defaulted from mocks.
type Config struct {
	BaseURL          string
	NSDLISIN         string
	FallbackWeekdays int
	PollInterval     time.Duration
	Timeout          time.Duration
	Commit           string
	SuiteID          string
}

// Runner executes the smoke suite against real deployed dependencies.
type Runner struct {
	Config    Config
	HTTP      HTTPClient
	Queue     Queue
	Objects   Objects
	Schedules Schedules
	Now       func() time.Time
	Sleep     func(context.Context, time.Duration) error
}

// Candidate records one attempted BSE date and its outcome.
type Candidate struct {
	Date      string `json:"date"`
	RunID     string `json:"run_id,omitempty"`
	Phase     string `json:"phase,omitempty"`
	ErrorCode string `json:"error_code,omitempty"`
}

// Report is the workflow artifact describing every smoke outcome.
type Report struct {
	Commit           string      `json:"commit"`
	SuiteID          string      `json:"suite_id"`
	StartedAt        time.Time   `json:"started_at"`
	BSECandidates    []Candidate `json:"bse_candidates"`
	BSESelectedDate  string      `json:"bse_selected_date,omitempty"`
	TodayUnavailable bool        `json:"bse_today_unavailable"`
	BSEFile          string      `json:"bse_file,omitempty"`
	NSDLRunID        string      `json:"nsdl_run_id,omitempty"`
	NSDLFiles        []string    `json:"nsdl_files,omitempty"`
	ForcedRunID      string      `json:"forced_run_id,omitempty"`
	ScheduleVerified bool        `json:"schedule_verified"`
	Passed           bool        `json:"passed"`
	Failure          string      `json:"failure,omitempty"`
}

type runView struct {
	RunID       string `json:"run_id"`
	Phase       string `json:"phase"`
	LatestRunID string `json:"latest_run_id"`
	LatestPhase string `json:"latest_phase"`
}

type jobRecord struct {
	JobID  string `json:"job_id"`
	Status string `json:"status"`
	Result *struct {
		Filename string `json:"filename"`
		Artifact struct {
			Key string `json:"key"`
		} `json:"artifact"`
	} `json:"result"`
	ErrorCode string `json:"error_code"`
}

// Preflight rejects missing or unusable inputs before any production call is made.
func (r *Runner) Preflight() error {
	target, err := url.Parse(r.Config.BaseURL)
	switch {
	case err != nil || target.Scheme != "https" || target.Host == "" || target.Path != "" || target.RawQuery != "":
		return errors.New("smoke preflight: API base URL must be an https origin")
	case !isinPattern.MatchString(r.Config.NSDLISIN):
		return errors.New("smoke preflight: SMOKE_NSDL_ISIN must be a 12-character ISIN")
	case r.Config.FallbackWeekdays < 3:
		return errors.New("smoke preflight: SMOKE_BSE_FALLBACK_WEEKDAYS must be at least 3")
	case r.Config.PollInterval <= 0 || r.Config.Timeout <= r.Config.PollInterval:
		return errors.New("smoke preflight: poll interval and timeout must be positive and ordered")
	case !idPattern.MatchString(r.Config.SuiteID) || r.Config.Commit == "":
		return errors.New("smoke preflight: suite ID and commit are required")
	case r.HTTP == nil || r.Queue == nil || r.Objects == nil || r.Schedules == nil || r.Now == nil || r.Sleep == nil:
		return errors.New("smoke preflight: API, queue, artifact, and schedule clients are required")
	}
	return nil
}

// Run executes every smoke check once and returns a report even when a check fails.
func (r *Runner) Run(ctx context.Context) (Report, error) {
	report := Report{Commit: r.Config.Commit, SuiteID: r.Config.SuiteID}
	if err := r.Preflight(); err != nil {
		report.Failure = err.Error()
		return report, err
	}
	report.StartedAt = r.Now().UTC()
	steps := []func(context.Context, *Report) error{r.schedule, r.bse, r.nsdl, r.forced}
	for _, step := range steps {
		if err := step(ctx, &report); err != nil {
			report.Failure = err.Error()
			return report, err
		}
	}
	report.Passed = true
	return report, nil
}

// CandidateDates returns the start date followed by the preceding weekdays, newest first.
func CandidateDates(start time.Time, fallbacks int) []time.Time {
	dates := []time.Time{start}
	for day := start.AddDate(0, 0, -1); len(dates) <= fallbacks; day = day.AddDate(0, 0, -1) {
		if day.Weekday() != time.Saturday && day.Weekday() != time.Sunday {
			dates = append(dates, day)
		}
	}
	return dates
}

// schedule verifies the deployed BSE schedule without waiting for it to fire.
func (r *Runner) schedule(ctx context.Context, report *Report) error {
	deployed, err := r.Schedules.Get(ctx, scheduleName)
	if err != nil {
		return fmt.Errorf("smoke schedule: %w", err)
	}
	if deployed.Expression != scheduleCron || deployed.Timezone != scheduleZone || deployed.State != "ENABLED" {
		return fmt.Errorf("smoke schedule: deployed %q %q %q differs from the agreed enabled weekday schedule", deployed.Expression, deployed.Timezone, deployed.State)
	}
	report.ScheduleVerified = true
	return nil
}

// bse tries today's IST date and falls back to earlier weekdays only when the source file is unpublished.
func (r *Runner) bse(ctx context.Context, report *Report) error {
	zone, err := time.LoadLocation(scheduleZone)
	if err != nil {
		return fmt.Errorf("smoke BSE: %w", err)
	}
	today := report.StartedAt.In(zone)
	for index, day := range CandidateDates(today, r.Config.FallbackWeekdays) {
		date := day.Format("2006-01-02")
		candidate := Candidate{Date: date}
		runID, err := r.manual(ctx, bseEvent, map[string]any{"exchangeName": "BSE", "run_date": date}, false, "bse-"+date)
		if err != nil {
			report.BSECandidates = append(report.BSECandidates, candidate)
			return fmt.Errorf("smoke BSE %s: %w", date, err)
		}
		view, err := r.await(ctx, runID)
		candidate.RunID, candidate.Phase = view.RunID, view.Phase
		if err != nil {
			report.BSECandidates = append(report.BSECandidates, candidate)
			return fmt.Errorf("smoke BSE %s: %w", date, err)
		}
		jobs, err := r.jobs(ctx, view.RunID)
		if err != nil {
			report.BSECandidates = append(report.BSECandidates, candidate)
			return fmt.Errorf("smoke BSE %s: %w", date, err)
		}
		if view.Phase == "failed" {
			candidate.ErrorCode = failureCode(jobs)
			report.BSECandidates = append(report.BSECandidates, candidate)
			if candidate.ErrorCode != notFoundCode {
				return fmt.Errorf("smoke BSE %s: run %s failed with %q", date, view.RunID, candidate.ErrorCode)
			}
			continue
		}
		report.BSECandidates = append(report.BSECandidates, candidate)
		want := "BSE_fgroup" + day.Format("02012006") + ".csv"
		files, err := r.files(ctx, view.RunID, jobs, 1)
		if err != nil {
			return fmt.Errorf("smoke BSE %s: %w", date, err)
		}
		if files[0] != want {
			return fmt.Errorf("smoke BSE %s: stored %q, want %q", date, files[0], want)
		}
		report.BSESelectedDate, report.BSEFile, report.TodayUnavailable = date, want, index > 0
		return nil
	}
	return fmt.Errorf("smoke BSE: no file was published for any of %d candidate dates", len(report.BSECandidates))
}

// nsdl sends a CloudEvent to the real ingress queue and validates all six ISIN-prefixed files.
func (r *Runner) nsdl(ctx context.Context, report *Report) error {
	id := r.Config.SuiteID + "-nsdl"
	event, err := json.Marshal(map[string]any{
		"specversion": "1.0", "id": id, "source": smokeSource, "type": "com.bondplatform.data.pull.requested.v1",
		"time": report.StartedAt.Format(time.RFC3339), "subject": "isin/" + r.Config.NSDLISIN, "datacontenttype": "application/json",
		"data": map[string]any{"schema_version": 1, "event_type": nsdlEvent, "inputs": map[string]string{"isin_code": r.Config.NSDLISIN}},
	})
	if err != nil {
		return fmt.Errorf("smoke NSDL: %w", err)
	}
	if err := r.Queue.Send(ctx, string(event)); err != nil {
		return fmt.Errorf("smoke NSDL: send to ingress: %w", err)
	}
	runID, err := r.resolve(ctx, id)
	if err != nil {
		return fmt.Errorf("smoke NSDL: %w", err)
	}
	report.NSDLRunID = runID
	files, err := r.completed(ctx, runID, len(nsdlJobs))
	if err != nil {
		return fmt.Errorf("smoke NSDL: %w", err)
	}
	for index, job := range nsdlJobs {
		if want := r.Config.NSDLISIN + "_" + job + ".json"; files[index] != want {
			return fmt.Errorf("smoke NSDL: stored %q, want %q", files[index], want)
		}
	}
	report.NSDLFiles = files
	return nil
}

// forced proves a forced manual run reaches the real processor despite deduplication.
func (r *Runner) forced(ctx context.Context, report *Report) error {
	runID, err := r.manual(ctx, nsdlEvent, map[string]any{"isin_code": r.Config.NSDLISIN}, true, "forced")
	if err != nil {
		return fmt.Errorf("smoke forced run: %w", err)
	}
	view, err := r.await(ctx, runID)
	report.ForcedRunID = view.RunID
	if err != nil {
		return fmt.Errorf("smoke forced run: %w", err)
	}
	if view.Phase != "completed" {
		return fmt.Errorf("smoke forced run: run %s ended %q, want completed", view.RunID, view.Phase)
	}
	var page struct {
		Items []struct {
			Status  int    `json:"status"`
			Code    string `json:"code"`
			Receipt *struct {
				Status string `json:"status"`
			} `json:"receipt"`
		} `json:"items"`
	}
	if _, err := r.call(ctx, http.MethodGet, "/v1/runs/"+view.RunID+"/delivery?limit=100", "", nil, &page); err != nil {
		return fmt.Errorf("smoke forced run: %w", err)
	}
	for _, item := range page.Items {
		if item.Status == http.StatusAccepted && item.Code == "ACCEPTED" && item.Receipt != nil {
			return nil
		}
	}
	return fmt.Errorf("smoke forced run: run %s has no recorded processor HTTP 202", view.RunID)
}

// completed waits for a run and requires a successful terminal phase with validated files.
func (r *Runner) completed(ctx context.Context, runID string, count int) ([]string, error) {
	view, err := r.await(ctx, runID)
	if err != nil {
		return nil, err
	}
	jobs, err := r.jobs(ctx, view.RunID)
	if err != nil {
		return nil, err
	}
	if view.Phase == "failed" {
		return nil, fmt.Errorf("run %s failed with %q", view.RunID, failureCode(jobs))
	}
	return r.files(ctx, view.RunID, jobs, count)
}

// manual submits one fresh manual run with a suite-unique idempotency key.
func (r *Runner) manual(ctx context.Context, event string, inputs map[string]any, force bool, label string) (string, error) {
	body, err := json.Marshal(map[string]any{"inputs": inputs, "force": force})
	if err != nil {
		return "", err
	}
	var result struct {
		RunID string `json:"run_id"`
	}
	status, err := r.call(ctx, http.MethodPost, "/v1/events/"+event+"/runs", r.Config.SuiteID+"-"+label, body, &result)
	if err != nil {
		return "", err
	}
	if status != http.StatusAccepted || result.RunID == "" {
		return "", fmt.Errorf("manual admission returned HTTP %d", status)
	}
	return result.RunID, nil
}

// resolve polls request lookup until the queued CloudEvent has been admitted.
func (r *Runner) resolve(ctx context.Context, id string) (string, error) {
	path := "/v1/requests/" + url.PathEscape(smokeSource) + "/" + url.PathEscape(id)
	deadline := r.Now().Add(r.Config.Timeout)
	for {
		var resolution struct {
			RunID string `json:"run_id"`
		}
		status, err := r.call(ctx, http.MethodGet, path, "", nil, &resolution)
		if err == nil && status == http.StatusOK && resolution.RunID != "" {
			return resolution.RunID, nil
		}
		if err != nil && status != http.StatusNotFound {
			return "", err
		}
		if err := r.pause(ctx, deadline, "request "+id+" was not admitted"); err != nil {
			return "", err
		}
	}
}

// await polls a run, following automatic retry children, until the chain is terminal.
func (r *Runner) await(ctx context.Context, runID string) (runView, error) {
	deadline := r.Now().Add(r.Config.Timeout)
	current := runView{RunID: runID}
	for {
		var view runView
		if _, err := r.call(ctx, http.MethodGet, "/v1/runs/"+current.RunID, "", nil, &view); err != nil {
			return current, err
		}
		current.Phase = view.Phase
		if view.LatestRunID != "" && view.LatestRunID != current.RunID {
			current = runView{RunID: view.LatestRunID, Phase: view.LatestPhase}
			continue
		}
		if current.Phase == "completed" || current.Phase == "skipped_unchanged" || current.Phase == "failed" {
			return current, nil
		}
		if err := r.pause(ctx, deadline, "run "+current.RunID+" did not finish"); err != nil {
			return current, err
		}
	}
}

// pause waits one poll interval and fails once the bounded deadline has passed.
func (r *Runner) pause(ctx context.Context, deadline time.Time, what string) error {
	if !r.Now().Before(deadline) {
		return fmt.Errorf("%s within %s", what, r.Config.Timeout)
	}
	return r.Sleep(ctx, r.Config.PollInterval)
}

// jobs reads every per-job result record of a run, ignoring attempt records.
func (r *Runner) jobs(ctx context.Context, runID string) ([]jobRecord, error) {
	var jobs []jobRecord
	cursor := ""
	for page := 0; page < 20; page++ {
		var body struct {
			Items      []jobRecord `json:"items"`
			NextCursor string      `json:"next_cursor"`
		}
		path := "/v1/runs/" + runID + "/pulls?limit=100"
		if cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		if _, err := r.call(ctx, http.MethodGet, path, "", nil, &body); err != nil {
			return nil, err
		}
		for _, item := range body.Items {
			if item.JobID != "" && item.Status != "" {
				jobs = append(jobs, item)
			}
		}
		if cursor = body.NextCursor; cursor == "" {
			return jobs, nil
		}
	}
	return nil, fmt.Errorf("run %s has an unbounded pull record listing", runID)
}

// files requires count completed jobs whose artifacts and manifest exist in the private bucket.
func (r *Runner) files(ctx context.Context, runID string, jobs []jobRecord, count int) ([]string, error) {
	names := make([]string, 0, count)
	order := map[string]int{}
	for index, job := range nsdlJobs {
		order[job] = index
	}
	sorted := make([]string, len(nsdlJobs))
	for _, job := range jobs {
		if job.Status != "completed" || job.Result == nil || job.Result.Filename == "" || !strings.HasPrefix(job.Result.Artifact.Key, "runs/") {
			return nil, fmt.Errorf("run %s job %s is %q without a stored artifact", runID, job.JobID, job.Status)
		}
		if found, err := r.Objects.Exists(ctx, job.Result.Artifact.Key); err != nil || !found {
			return nil, fmt.Errorf("run %s artifact for job %s is not readable: %v", runID, job.JobID, err)
		}
		if index, known := order[job.JobID]; known && count == len(nsdlJobs) {
			sorted[index] = job.Result.Filename
		}
		names = append(names, job.Result.Filename)
	}
	if len(names) != count {
		return nil, fmt.Errorf("run %s stored %d files, want %d", runID, len(names), count)
	}
	if found, err := r.Objects.Exists(ctx, "runs/"+runID+"/manifest.json"); err != nil || !found {
		return nil, fmt.Errorf("run %s manifest is not readable: %v", runID, err)
	}
	if count == len(nsdlJobs) {
		return sorted, nil
	}
	return names, nil
}

// failureCode returns the single source failure shared by failed jobs, or a mixed marker.
func failureCode(jobs []jobRecord) string {
	code := ""
	for _, job := range jobs {
		if job.Status == "completed" || job.ErrorCode == "GROUP_CANCELED" {
			continue
		}
		if code != "" && code != job.ErrorCode {
			return "MIXED_FAILURES"
		}
		code = job.ErrorCode
	}
	if code == "" {
		return "UNKNOWN_FAILURE"
	}
	return code
}

// call performs one bounded JSON request and decodes a 2xx body into out.
func (r *Runner) call(ctx context.Context, method, path, key string, body []byte, out any) (int, error) {
	request, err := http.NewRequestWithContext(ctx, method, r.Config.BaseURL+path, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	response, err := r.HTTP.Do(request)
	if err != nil {
		return 0, fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(response.Body, maxBodyBytes))
	if err != nil {
		return response.StatusCode, fmt.Errorf("%s %s: %w", method, path, err)
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return response.StatusCode, fmt.Errorf("%s %s returned HTTP %d", method, path, response.StatusCode)
	}
	if err := json.Unmarshal(data, out); err != nil {
		return response.StatusCode, fmt.Errorf("%s %s returned invalid JSON", method, path)
	}
	return response.StatusCode, nil
}
