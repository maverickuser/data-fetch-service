package smoke

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"
)

const isin = "INE121A07QY9"

// fakeService imitates the deployed API, queue, bucket, and scheduler for one suite.
type fakeService struct {
	t            *testing.T
	now          time.Time
	bse          map[string]string // run_date -> failure code, "" for success
	runs         map[string]*fakeRun
	keys         map[string]string
	requests     map[string]string
	missing      map[string]bool
	schedule     Schedule
	scheduleErr  error
	sendErr      error
	existsErr    error
	transportErr map[string]error
	status       map[string]int
	raw          map[string]string
	pending      int
	unresolved   int
	deliveryCode string
	nsdlRename   string
	extraPages   bool
	sleeps       int
	forcedPhase  string
	retryChild   bool
	bsePhase     string
	slowChild    int
}

type fakeRun struct {
	phase string
	jobs  []map[string]any
	force bool
	polls int
	child string
	bse   bool
}

func newFake(t *testing.T, now time.Time) *fakeService {
	return &fakeService{t: t, now: now, bse: map[string]string{}, runs: map[string]*fakeRun{}, keys: map[string]string{}, requests: map[string]string{}, missing: map[string]bool{},
		schedule: Schedule{Expression: scheduleCron, Timezone: scheduleZone, State: "ENABLED"}, transportErr: map[string]error{}, status: map[string]int{}, raw: map[string]string{}, deliveryCode: "ACCEPTED", forcedPhase: "completed"}
}

func (f *fakeService) runner() *Runner {
	return &Runner{Config: Config{BaseURL: "https://fetch.kagent.app", NSDLISIN: isin, FallbackWeekdays: 3, PollInterval: time.Second, Timeout: 10 * time.Second, Commit: "abc", SuiteID: "smoke-1"},
		HTTP: f, Queue: f, Objects: f, Schedules: f, Now: func() time.Time { return f.now }, Sleep: func(context.Context, time.Duration) error {
			f.sleeps++
			f.now = f.now.Add(time.Second)
			return nil
		}}
}

func (f *fakeService) Get(context.Context, string) (Schedule, error) {
	return f.schedule, f.scheduleErr
}

func (f *fakeService) Exists(_ context.Context, key string) (bool, error) {
	return !f.missing[key], f.existsErr
}

func (f *fakeService) Send(_ context.Context, body string) error {
	if f.sendErr != nil {
		return f.sendErr
	}
	var event struct {
		ID, Source, Type string
		Data             struct {
			EventType string            `json:"event_type"`
			Inputs    map[string]string `json:"inputs"`
		}
	}
	if err := json.Unmarshal([]byte(body), &event); err != nil || event.Source != smokeSource || event.Data.EventType != nsdlEvent || event.Data.Inputs["isin_code"] != isin {
		f.t.Fatal(body, err)
	}
	f.requests[event.ID] = f.nsdlRun(false)
	return nil
}

func (f *fakeService) nsdlRun(force bool) string {
	id := fmt.Sprintf("run_%d", len(f.runs)+1)
	run := &fakeRun{phase: "completed", force: force}
	if force {
		run.phase = f.forcedPhase
	}
	for _, job := range nsdlJobs {
		name := isin + "_" + job + ".json"
		if job == f.nsdlRename {
			name = "wrong.json"
		}
		run.jobs = append(run.jobs, map[string]any{"job_id": job, "status": "completed", "result": map[string]any{"filename": name, "artifact": map[string]string{"key": "runs/" + id + "/raw/" + job + "/1/" + name}}})
	}
	f.runs[id] = run
	return id
}

func (f *fakeService) Do(request *http.Request) (*http.Response, error) {
	path := request.URL.Path
	for prefix, err := range f.transportErr {
		if strings.HasPrefix(path, prefix) {
			return nil, err
		}
	}
	for prefix, status := range f.status {
		if strings.HasPrefix(path, prefix) {
			return reply(status, map[string]string{"error": "X"}), nil
		}
	}
	longest := ""
	for prefix := range f.raw {
		if strings.HasPrefix(path, prefix) && len(prefix) > len(longest) {
			longest = prefix
		}
	}
	if longest != "" {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(f.raw[longest]))}, nil
	}
	parts := strings.Split(strings.TrimPrefix(path, "/v1/"), "/")
	switch {
	case request.Method == http.MethodPost && parts[0] == "events":
		var body struct {
			Inputs map[string]string `json:"inputs"`
			Force  bool              `json:"force"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			f.t.Fatal(err)
		}
		key := request.Header.Get("Idempotency-Key")
		if key == "" || f.keys[key] != "" {
			f.t.Fatalf("idempotency key %q missing or reused", key)
		}
		var id string
		if parts[1] == nsdlEvent {
			id = f.nsdlRun(body.Force)
		} else {
			id = fmt.Sprintf("run_%d", len(f.runs)+1)
			date, _ := time.Parse("2006-01-02", body.Inputs["run_date"])
			name := "BSE_fgroup" + date.Format("02012006") + ".csv"
			run := &fakeRun{phase: "completed", jobs: []map[string]any{{"job_id": "debt-bhavcopy", "status": "completed", "result": map[string]any{"filename": name, "artifact": map[string]string{"key": "runs/" + id + "/raw/debt-bhavcopy/1/" + name}}}}}
			if code, failed := f.bse[body.Inputs["run_date"]]; failed {
				run.phase, run.jobs = "failed", []map[string]any{{"job_id": "debt-bhavcopy", "status": "failed", "error_code": code}}
			} else if body.Inputs["exchangeName"] != "BSE" {
				f.t.Fatal(body.Inputs)
			}
			run.bse = true
			if f.bsePhase != "" {
				run.phase = f.bsePhase
			}
			if f.retryChild {
				child := id + "c"
				f.runs[child] = run
				run = &fakeRun{phase: "failed", child: child}
			}
			f.runs[id] = run
		}
		f.keys[key] = id
		return reply(http.StatusAccepted, map[string]any{"run_id": id, "status": "queued", "reused": false}), nil
	case parts[0] == "requests":
		source, _ := url.PathUnescape(parts[1])
		if f.unresolved > 0 || source != smokeSource || f.requests[parts[2]] == "" {
			f.unresolved--
			return reply(http.StatusNotFound, map[string]string{"error": "NOT_FOUND"}), nil
		}
		return reply(http.StatusOK, map[string]string{"run_id": f.requests[parts[2]]}), nil
	case parts[0] == "runs" && len(parts) == 2:
		run := f.runs[parts[1]]
		run.polls++
		if run.polls <= f.pending {
			return reply(http.StatusOK, map[string]string{"run_id": parts[1], "phase": "pulling", "latest_run_id": parts[1], "latest_phase": "pulling"}), nil
		}
		latest, phase := parts[1], run.phase
		if run.child != "" && f.slowChild > 0 {
			// The link is durable before the child becomes readable.
			f.slowChild--
			return reply(http.StatusOK, map[string]string{"run_id": parts[1], "phase": run.phase, "child_run_id": run.child, "latest_run_id": latest, "latest_phase": phase}), nil
		}
		if run.child != "" {
			latest, phase = run.child, f.runs[run.child].phase
		}
		return reply(http.StatusOK, map[string]string{"run_id": parts[1], "phase": run.phase, "child_run_id": run.child, "latest_run_id": latest, "latest_phase": phase}), nil
	case parts[0] == "runs" && parts[2] == "pulls":
		items := []any{map[string]any{"attempt": 1, "status_code": 200}}
		if f.extraPages && request.URL.Query().Get("cursor") == "" {
			return reply(http.StatusOK, map[string]any{"items": items, "next_cursor": "next"}), nil
		}
		for _, job := range f.runs[parts[1]].jobs {
			items = append(items, job)
		}
		return reply(http.StatusOK, map[string]any{"items": items, "next_cursor": ""}), nil
	case parts[0] == "runs" && parts[2] == "delivery":
		if run := f.runs[parts[1]]; !run.force && !(run.bse && run.phase == "completed") {
			f.t.Fatal("delivery read for a run without a handoff")
		}
		return reply(http.StatusOK, map[string]any{"items": []any{map[string]any{"attempt": 1}, map[string]any{"status": 202, "code": f.deliveryCode, "receipt": map[string]string{"status": "ACCEPTED"}}}}), nil
	}
	f.t.Fatalf("unexpected %s %s", request.Method, path)
	return nil, nil
}

func reply(status int, body any) *http.Response {
	data, _ := json.Marshal(body)
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(string(data)))}
}

func ist(year int, month time.Month, day, hour int) time.Time {
	zone, _ := time.LoadLocation(scheduleZone)
	return time.Date(year, month, day, hour, 0, 0, 0, zone)
}

func TestSuitePassesAgainstHealthyDeployment(t *testing.T) {
	fake := newFake(t, ist(2026, 9, 21, 21))
	fake.pending, fake.unresolved, fake.extraPages = 2, 2, true
	report, err := fake.runner().Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wantFiles := []string{isin + "_isin-details.json", isin + "_instrument-details.json", isin + "_coupon-details.json", isin + "_redemptions.json", isin + "_listings.json", isin + "_credit-ratings.json"}
	if report.BSEHandoff != "accepted" {
		t.Fatal(report.BSEHandoff)
	}
	if !report.Passed || report.Failure != "" || !report.ScheduleVerified || report.TodayUnavailable || report.BSESelectedDate != "2026-09-21" || report.BSEFile != "BSE_fgroup21092026.csv" || !reflect.DeepEqual(report.NSDLFiles, wantFiles) || report.NSDLRunID == "" || report.ForcedRunID == "" || report.Commit != "abc" {
		t.Fatalf("%+v", report)
	}
	if len(fake.keys) != 2 || fake.keys["smoke-1-bse-2026-09-21"] == "" || fake.keys["smoke-1-forced"] == "" || fake.sleeps == 0 {
		t.Fatal(fake.keys, fake.sleeps)
	}
}

func TestBSEFallsBackOnlyWhenTheFileIsUnpublished(t *testing.T) {
	// Monday 21 September with a holiday on the preceding Friday: Sunday/Saturday are skipped.
	fake := newFake(t, ist(2026, 9, 21, 21))
	fake.bse["2026-09-21"], fake.bse["2026-09-18"] = notFoundCode, notFoundCode
	report, err := fake.runner().Run(context.Background())
	if err != nil || !report.Passed || !report.TodayUnavailable || report.BSESelectedDate != "2026-09-17" || report.BSEFile != "BSE_fgroup17092026.csv" || len(report.BSECandidates) != 3 {
		t.Fatalf("%+v %v", report, err)
	}
	if report.BSECandidates[0].ErrorCode != notFoundCode || report.BSECandidates[2].Phase != "completed" || fake.keys["smoke-1-bse-2026-09-18"] == fake.keys["smoke-1-bse-2026-09-21"] {
		t.Fatal(report.BSECandidates)
	}

	exhausted := newFake(t, ist(2026, 9, 21, 21))
	for _, date := range []string{"2026-09-21", "2026-09-18", "2026-09-17", "2026-09-16"} {
		exhausted.bse[date] = notFoundCode
	}
	report, err = exhausted.runner().Run(context.Background())
	if err == nil || report.Passed || len(report.BSECandidates) != 4 || !strings.Contains(report.Failure, "4 candidate dates") {
		t.Fatalf("%+v %v", report, err)
	}

	for _, code := range []string{"SOURCE_HTTP", "SOURCE_FORMAT", ""} {
		other := newFake(t, ist(2026, 9, 21, 21))
		other.bse["2026-09-21"] = code
		report, err = other.runner().Run(context.Background())
		if err == nil || len(report.BSECandidates) != 1 || len(other.keys) != 1 || report.BSECandidates[0].ErrorCode == notFoundCode {
			t.Fatalf("%q: %+v %v", code, report, err)
		}
	}
}

func TestCandidateDatesSkipWeekendsAcrossBoundaries(t *testing.T) {
	format := func(dates []time.Time) string {
		out := []string{}
		for _, date := range dates {
			out = append(out, date.Format("2006-01-02"))
		}
		return strings.Join(out, ",")
	}
	for name, test := range map[string]struct {
		start     time.Time
		fallbacks int
		want      string
	}{
		"sunday start":   {ist(2026, 9, 20, 9), 3, "2026-09-20,2026-09-18,2026-09-17,2026-09-16"},
		"monday start":   {ist(2026, 9, 21, 9), 3, "2026-09-21,2026-09-18,2026-09-17,2026-09-16"},
		"month boundary": {ist(2026, 10, 1, 9), 3, "2026-10-01,2026-09-30,2026-09-29,2026-09-28"},
		"year boundary":  {ist(2027, 1, 1, 9), 4, "2027-01-01,2026-12-31,2026-12-30,2026-12-29,2026-12-28"},
	} {
		if got := format(CandidateDates(test.start, test.fallbacks)); got != test.want {
			t.Fatalf("%s: %s", name, got)
		}
	}
}

func TestSuiteUsesIndiaDateCapturedOnce(t *testing.T) {
	// 20:00 UTC on the 21st is already the 22nd in India.
	fake := newFake(t, time.Date(2026, 9, 21, 20, 0, 0, 0, time.UTC))
	report, err := fake.runner().Run(context.Background())
	if err != nil || report.BSESelectedDate != "2026-09-22" {
		t.Fatalf("%+v %v", report, err)
	}
}

func TestSuiteFollowsAutomaticRetryChild(t *testing.T) {
	for _, slow := range []int{0, 3} {
		fake := newFake(t, ist(2026, 9, 21, 21))
		fake.retryChild, fake.slowChild = true, slow
		report, err := fake.runner().Run(context.Background())
		if err != nil || !strings.HasSuffix(report.BSECandidates[0].RunID, "c") || fake.slowChild != 0 {
			t.Fatalf("%d: %+v %v", slow, report, err)
		}
	}
	stuck := newFake(t, ist(2026, 9, 21, 21))
	stuck.retryChild, stuck.slowChild = true, 1000
	if _, err := stuck.runner().Run(context.Background()); err == nil || !strings.Contains(err.Error(), "did not start") {
		t.Fatal(err)
	}
}

func TestBSEReportsWhetherAHandoffWasObserved(t *testing.T) {
	fake := newFake(t, ist(2026, 9, 21, 21))
	fake.bsePhase = "skipped_unchanged"
	report, err := fake.runner().Run(context.Background())
	if err != nil || !report.Passed || report.BSEHandoff != "skipped_unchanged" || report.BSEFile != "BSE_fgroup21092026.csv" {
		t.Fatalf("%+v %v", report, err)
	}
	// Every job completed but delivery failed: not a missing file, so no date fallback.
	failed := newFake(t, ist(2026, 9, 21, 21))
	failed.bsePhase = "failed"
	report, err = failed.runner().Run(context.Background())
	if err == nil || len(report.BSECandidates) != 1 || report.BSECandidates[0].ErrorCode != "UNKNOWN_FAILURE" {
		t.Fatalf("%+v %v", report, err)
	}
}

func TestPreflightRejectsMissingInputs(t *testing.T) {
	for name, change := range map[string]func(*Runner){
		"http base":     func(r *Runner) { r.Config.BaseURL = "http://fetch.kagent.app" },
		"base path":     func(r *Runner) { r.Config.BaseURL = "https://fetch.kagent.app/v1" },
		"bad url":       func(r *Runner) { r.Config.BaseURL = "https://%zz" },
		"isin":          func(r *Runner) { r.Config.NSDLISIN = "" },
		"fallbacks":     func(r *Runner) { r.Config.FallbackWeekdays = 2 },
		"poll":          func(r *Runner) { r.Config.PollInterval = 0 },
		"timeout":       func(r *Runner) { r.Config.Timeout = time.Second },
		"suite":         func(r *Runner) { r.Config.SuiteID = "bad id" },
		"commit":        func(r *Runner) { r.Config.Commit = "" },
		"queue missing": func(r *Runner) { r.Queue = nil },
		"clock missing": func(r *Runner) { r.Now = nil },
	} {
		fake := newFake(t, ist(2026, 9, 21, 21))
		runner := fake.runner()
		change(runner)
		report, err := runner.Run(context.Background())
		if err == nil || report.Passed || report.Failure == "" || len(fake.keys) != 0 || !strings.HasPrefix(err.Error(), "smoke preflight") {
			t.Fatalf("%s: %+v %v", name, report, err)
		}
	}
}

func TestSuiteFailsWithDiagnostics(t *testing.T) {
	broken := errors.New("unavailable")
	for name, test := range map[string]struct {
		change func(*fakeService, *Runner)
		want   string
	}{
		"schedule read":       {func(f *fakeService, _ *Runner) { f.scheduleErr = broken }, "smoke schedule"},
		"schedule disabled":   {func(f *fakeService, _ *Runner) { f.schedule.State = "DISABLED" }, "differs"},
		"schedule expression": {func(f *fakeService, _ *Runner) { f.schedule.Expression = "rate(1 day)" }, "differs"},
		"admission transport": {func(f *fakeService, _ *Runner) { f.transportErr["/v1/events/"+bseEvent] = broken }, "smoke BSE"},
		"admission rejected":  {func(f *fakeService, _ *Runner) { f.status["/v1/events/"+bseEvent] = 400 }, "HTTP 400"},
		"admission replay":    {func(f *fakeService, _ *Runner) { f.raw["/v1/events/"+bseEvent] = `{"run_id":""}` }, "HTTP 200"},
		"run read":            {func(f *fakeService, _ *Runner) { f.status["/v1/runs/run_1"] = 503 }, "HTTP 503"},
		"run invalid json":    {func(f *fakeService, _ *Runner) { f.raw["/v1/runs/run_1"] = `{` }, "invalid JSON"},
		"poll deadline":       {func(f *fakeService, _ *Runner) { f.pending = 1000 }, "did not finish"},
		"sleep canceled": {func(f *fakeService, r *Runner) {
			f.pending = 1000
			r.Sleep = func(context.Context, time.Duration) error { return context.Canceled }
		}, "context canceled"},
		"pull records":    {func(f *fakeService, _ *Runner) { f.status["/v1/runs/run_1/pulls"] = 422 }, "HTTP 422"},
		"unbounded pulls": {func(f *fakeService, _ *Runner) { f.raw["/v1/runs/run_1/pulls"] = `{"items":[],"next_cursor":"again"}` }, "unbounded"},
		"artifact missing": {func(f *fakeService, _ *Runner) {
			f.missing["runs/run_1/raw/debt-bhavcopy/1/BSE_fgroup21092026.csv"] = true
		}, "not readable"},
		"manifest missing": {func(f *fakeService, _ *Runner) { f.missing["runs/run_1/manifest.json"] = true }, "manifest is not readable"},
		"artifact access":  {func(f *fakeService, _ *Runner) { f.existsErr = broken }, "not readable"},
		"no stored file":   {func(f *fakeService, _ *Runner) { f.raw["/v1/runs/run_1/pulls"] = `{"items":[]}` }, "stored 0 files"},
		"job not stored": {func(f *fakeService, _ *Runner) {
			f.raw["/v1/runs/run_1/pulls"] = `{"items":[{"job_id":"debt-bhavcopy","status":"completed"}]}`
		}, "without a stored artifact"},
		"wrong BSE name": {func(f *fakeService, _ *Runner) {
			f.raw["/v1/runs/run_1/pulls"] = `{"items":[{"job_id":"debt-bhavcopy","status":"completed","result":{"filename":"fgroup21092026.csv","artifact":{"key":"runs/run_1/x"}}}]}`
		}, `want "BSE_fgroup21092026.csv"`},
		"queue send":        {func(f *fakeService, _ *Runner) { f.sendErr = broken }, "send to ingress"},
		"never admitted":    {func(f *fakeService, _ *Runner) { f.unresolved = 1000 }, "was not admitted"},
		"lookup failure":    {func(f *fakeService, _ *Runner) { f.status["/v1/requests/"] = 503 }, "HTTP 503"},
		"NSDL wrong name":   {func(f *fakeService, _ *Runner) { f.nsdlRename = "listings" }, `stored "wrong.json"`},
		"NSDL run read":     {func(f *fakeService, _ *Runner) { f.status["/v1/runs/run_2"] = 503 }, "smoke NSDL"},
		"NSDL pulls":        {func(f *fakeService, _ *Runner) { f.status["/v1/runs/run_2/pulls"] = 503 }, "smoke NSDL"},
		"forced admission":  {func(f *fakeService, _ *Runner) { f.status["/v1/events/"+nsdlEvent] = 503 }, "smoke forced run"},
		"forced run read":   {func(f *fakeService, _ *Runner) { f.status["/v1/runs/run_3"] = 503 }, "smoke forced run"},
		"forced skipped":    {func(f *fakeService, _ *Runner) { f.forcedPhase = "skipped_unchanged" }, "want completed"},
		"delivery read":     {func(f *fakeService, _ *Runner) { f.status["/v1/runs/run_3/delivery"] = 503 }, "HTTP 503"},
		"BSE delivery read": {func(f *fakeService, _ *Runner) { f.status["/v1/runs/run_1/delivery"] = 503 }, "smoke BSE"},
		"forced pulls":      {func(f *fakeService, _ *Runner) { f.status["/v1/runs/run_3/pulls"] = 503 }, "smoke forced run"},
		"forced files":      {func(f *fakeService, _ *Runner) { f.missing["runs/run_3/manifest.json"] = true }, "object is missing"},
		"no recorded 202":   {func(f *fakeService, _ *Runner) { f.deliveryCode = "INVALID_RECEIPT" }, "no recorded processor HTTP 202"},
	} {
		fake := newFake(t, ist(2026, 9, 21, 21))
		runner := fake.runner()
		test.change(fake, runner)
		report, err := runner.Run(context.Background())
		if err == nil || report.Passed || report.Failure != err.Error() || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("%s: %+v %v", name, report, err)
		}
	}
}

func TestNSDLFailureReportsItsSourceCode(t *testing.T) {
	fake := newFake(t, ist(2026, 9, 21, 21))
	runner := fake.runner()
	fake.raw["/v1/runs/run_2"] = `{"run_id":"run_2","phase":"failed","latest_run_id":"run_2","latest_phase":"failed"}`
	fake.raw["/v1/runs/run_2/pulls"] = `{"items":[{"job_id":"listings","status":"failed","error_code":"SOURCE_HTTP"},{"job_id":"redemptions","status":"canceled","error_code":"GROUP_CANCELED"},{"job_id":"isin-details","status":"failed","error_code":"SOURCE_FORMAT"}]}`
	if _, err := runner.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "MIXED_FAILURES") {
		t.Fatal(err)
	}
}

func TestCallRejectsInvalidRequestsAndBodies(t *testing.T) {
	fake := newFake(t, ist(2026, 9, 21, 21))
	runner := fake.runner()
	if _, err := runner.call(context.Background(), "bad method", "/v1/runs/x", "", nil, nil); err == nil {
		t.Fatal("invalid method accepted")
	}
	runner.HTTP = clientFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(failingReader{})}, nil
	})
	if _, err := runner.call(context.Background(), http.MethodGet, "/v1/runs/x", "", nil, &struct{}{}); err == nil {
		t.Fatal("unreadable body accepted")
	}
	if _, err := runner.manual(context.Background(), bseEvent, map[string]any{"bad": make(chan int)}, false, "x"); err == nil {
		t.Fatal("unencodable inputs accepted")
	}
}

type clientFunc func(*http.Request) (*http.Response, error)

func (f clientFunc) Do(request *http.Request) (*http.Response, error) { return f(request) }

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }
