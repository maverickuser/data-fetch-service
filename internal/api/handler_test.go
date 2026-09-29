package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/admission"
	"github.com/maverickuser/data-fetch-service/internal/config"
	"github.com/maverickuser/data-fetch-service/internal/domain"
	"github.com/maverickuser/data-fetch-service/internal/state"
)

type apiFake struct {
	result                admission.Result
	err, scanErr, readErr error
	event, key            string
	force                 bool
	inputs                map[string]any
	view                  state.RunView
	coord                 state.Coordination
	page                  state.Page
	records               map[string]state.Object
	prefix, token         string
	limit                 int32
}

func (f *apiFake) Manual(_ context.Context, event, key string, inputs map[string]any, force bool) (admission.Result, error) {
	f.event, f.key, f.inputs, f.force = event, key, inputs, force
	return f.result, f.err
}
func (f *apiFake) ReadRun(context.Context, string) (state.RunView, error) { return f.view, f.err }
func (f *apiFake) History(context.Context, string, string, int32) (state.HistoryPage, error) {
	return state.HistoryPage{Transitions: []state.Transition{{RunID: "run", Sequence: 1, Phase: domain.Queued}}}, f.err
}
func (f *apiFake) RequestResolution(context.Context, string) (state.Resolution, error) {
	return state.Resolution{RunID: "run"}, f.err
}
func (f *apiFake) Load(context.Context, string) (state.Coordination, string, error) {
	return f.coord, "etag", f.err
}
func (f *apiFake) Scan(_ context.Context, prefix, token string, limit int32) (state.Page, error) {
	f.prefix, f.token, f.limit = prefix, token, limit
	return f.page, f.scanErr
}
func (f *apiFake) Read(_ context.Context, key string, _ time.Time) (state.Object, error) {
	return f.records[key], f.readErr
}

func handlerFixture(t *testing.T) (*Handler, *apiFake) {
	t.Helper()
	raw, err := os.ReadFile("../../config/events.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(raw)
	if err != nil {
		t.Fatal(err)
	}
	f := &apiFake{result: admission.Result{RunID: "run", Status: domain.Queued, StatusURL: "/v1/runs/run"}, view: state.RunView{RunID: "run", Phase: domain.Queued}, coord: state.Coordination{ActiveRunID: "run"}, records: map[string]state.Object{}}
	return &Handler{Service: f, Coordinator: f, Store: f, Config: cfg, Now: func() time.Time { return time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC) }}, f
}

func call(h *Handler, method, path, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Idempotency-Key", "request-key")
	w := httptest.NewRecorder()
	h.Routes().ServeHTTP(w, r)
	return w
}

func TestManualHTTPContract(t *testing.T) {
	h, f := handlerFixture(t)
	w := call(h, "POST", "/v1/events/daily-bhavcopy/runs", `{"inputs":{"exchangeName":"BSE"},"force":true}`)
	if w.Code != 202 || w.Header().Get("Location") != "/v1/runs/run" || f.event != "daily-bhavcopy" || f.key != "request-key" || !f.force || f.inputs["exchangeName"] != "BSE" {
		t.Fatal(w.Code, w.Body.String(), f)
	}
	f.result.CompletedReplay = true
	if w := call(h, "POST", "/v1/events/daily-bhavcopy/runs", `{"inputs":{}}`); w.Code != 200 {
		t.Fatal(w.Code)
	}
	for _, body := range []string{`{`, `{}`, `{"inputs":null}`, `{"inputs":{},"url":"https://evil"}`, `{"inputs":{}} {}`} {
		if w := call(h, "POST", "/v1/events/x/runs", body); w.Code != 400 {
			t.Fatal(body, w.Code)
		}
	}
	if w := call(h, "POST", "/v1/events/x/runs", `{"inputs":{"x":"`+strings.Repeat("x", 65536)+`"}}`); w.Code != 413 {
		t.Fatal(w.Code)
	}
	r := httptest.NewRequest("POST", "/v1/events/x/runs", strings.NewReader(`{"inputs":{}}`))
	r.Header.Set("Idempotency-Key", strings.Repeat("x", 257))
	w = httptest.NewRecorder()
	h.Routes().ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatal(w.Code)
	}
}

func TestReadRoutesAndErrorStatuses(t *testing.T) {
	h, f := handlerFixture(t)
	for _, path := range []string{"/v1/events/daily-bhavcopy", "/v1/runs/run", "/v1/runs/run/history", "/v1/runs/run/pulls", "/v1/runs/run/delivery", "/v1/requests/urn%3Atest/event", "/v1/events/daily-bhavcopy/active?inputs=" + url.QueryEscape(`{"exchangeName":"BSE"}`)} {
		if w := call(h, "GET", path, ""); w.Code != 200 {
			t.Fatal(path, w.Code, w.Body.String())
		}
	}
	if f.prefix != "runs/run/delivery/" {
		t.Fatal(f.prefix)
	}
	f.coord.ActiveRunID = ""
	if w := call(h, "GET", "/v1/events/daily-bhavcopy/active?inputs="+url.QueryEscape(`{"exchangeName":"BSE"}`), ""); w.Code != 404 {
		t.Fatal(w.Code)
	}
	for _, path := range []string{"/v1/events/missing", "/v1/events/missing/runs"} {
		if w := call(h, "GET", path, ""); w.Code != 404 {
			t.Fatal(path, w.Code)
		}
	}
	for _, path := range []string{"/v1/runs/run/history?limit=0", "/v1/runs/run/pulls?limit=101", "/v1/events/daily-bhavcopy/active?inputs=bad", "/v1/events/daily-bhavcopy/active?inputs=%7B%7D"} {
		if w := call(h, "GET", path, ""); w.Code != 400 {
			t.Fatal(path, w.Code)
		}
	}
	for _, tc := range []struct {
		err    error
		status int
	}{{admission.ErrInvalid, 400}, {state.ErrNotFound, 404}, {state.ErrExpired, 410}, {state.ErrIntegrity, 409}, {errors.New("secret provider info"), 503}} {
		f.err = tc.err
		if w := call(h, "POST", "/v1/events/x/runs", `{"inputs":{}}`); w.Code != tc.status || strings.Contains(w.Body.String(), "secret") {
			t.Fatal(w.Code, w.Body.String())
		}
		for _, path := range []string{"/v1/runs/run", "/v1/runs/run/history", "/v1/runs/run/pulls", "/v1/requests/urn%3Atest/event", "/v1/events/daily-bhavcopy/active?inputs=" + url.QueryEscape(`{"exchangeName":"BSE"}`)} {
			if w := call(h, "GET", path, ""); w.Code != tc.status {
				t.Fatal(path, w.Code)
			}
		}
	}
	f.err = nil
	f.view.Snapshot = json.RawMessage(`{`)
	if w := call(h, "GET", "/v1/runs/run", ""); w.Code != 500 {
		t.Fatal(w.Code)
	}
}

func TestMetadataPagesAndFailures(t *testing.T) {
	h, f := handlerFixture(t)
	f.page = state.Page{Keys: []string{"record"}, NextToken: "next"}
	f.records["record"] = state.Object{Data: []byte(`{"attempt":1}`)}
	if w := call(h, "GET", "/v1/runs/run/pulls?limit=1&cursor=old", ""); w.Code != 200 || f.limit != 1 || f.token != "old" || !strings.Contains(w.Body.String(), "next") {
		t.Fatal(w.Code, w.Body.String())
	}
	f.readErr = errors.New("read unavailable")
	if w := call(h, "GET", "/v1/runs/run/delivery", ""); w.Code != 503 {
		t.Fatal(w.Code)
	}
	f.readErr = nil
	f.scanErr = errors.New("scan unavailable")
	if w := call(h, "GET", "/v1/runs/run/pulls", ""); w.Code != 503 {
		t.Fatal(w.Code)
	}
}

func TestMetadataAggregateResponseBudget(t *testing.T) {
	h, f := handlerFixture(t)
	f.page = state.Page{Keys: []string{"large"}}
	f.records["large"] = state.Object{Data: []byte(strings.Repeat("x", (1<<20)+1))}
	if response := call(h, "GET", "/v1/runs/run/pulls", ""); response.Code != 422 {
		t.Fatal(response.Code)
	}
	if response := call(h, "GET", "/v1/events/daily-bhavcopy/active?inputs="+strings.Repeat("x", (16<<10)+1), ""); response.Code != 400 {
		t.Fatal(response.Code)
	}
}

func TestDateListingCursorAndBudgets(t *testing.T) {
	h, f := handlerFixture(t)
	f.page = state.Page{Keys: []string{"listing"}, NextToken: "opaque"}
	f.records["listing"] = state.Object{Data: []byte(`{"run_id":"run"}`)}
	w := call(h, "GET", "/v1/events/daily-bhavcopy/runs?limit=1", "")
	if w.Code != 200 || f.limit != 1 || f.prefix != "listings/daily-bhavcopy/2026-09-29/" {
		t.Fatal(w.Code, w.Body.String(), f.prefix)
	}
	var body struct {
		Next string `json:"next_cursor"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if w := call(h, "GET", "/v1/events/daily-bhavcopy/runs?limit=1&cursor="+body.Next, ""); w.Code != 200 || f.token != "opaque" {
		t.Fatal(w.Code, f.token)
	}
	if w := call(h, "GET", "/v1/events/nsdl-bond-data/runs?cursor="+body.Next, ""); w.Code != 400 {
		t.Fatal("filter cursor reused", w.Code)
	}
	f.page.NextToken = ""
	if w := call(h, "GET", "/v1/events/daily-bhavcopy/runs?from=2026-09-28&to=2026-09-29&limit=1", ""); w.Code != 200 || !strings.Contains(w.Body.String(), "next_cursor") {
		t.Fatal(w.Code)
	}
	for _, query := range []string{"limit=bad", "from=bad", "from=2026-09-29&to=2026-09-28", "from=2026-08-01&to=2026-09-29", "cursor=%%%", "cursor=" + base64.RawURLEncoding.EncodeToString([]byte(`{"Version":2}`)), "cursor=" + strings.Repeat("x", 8193)} {
		if w := call(h, "GET", "/v1/events/daily-bhavcopy/runs?"+query, ""); w.Code != 400 {
			t.Fatal(query, w.Code)
		}
	}
	f.page = state.Page{}
	if w := call(h, "GET", "/v1/events/daily-bhavcopy/runs", ""); w.Code != 200 {
		t.Fatal(w.Code)
	}
	f.scanErr = errors.New("scan")
	if w := call(h, "GET", "/v1/events/daily-bhavcopy/runs", ""); w.Code != 503 {
		t.Fatal(w.Code)
	}
}

func TestListingExpiredAndCorruptRecords(t *testing.T) {
	for _, scenario := range []string{"expired listing", "missing listing", "read failure", "corrupt listing", "expired run", "run failure"} {
		t.Run(scenario, func(t *testing.T) {
			h, f := handlerFixture(t)
			f.page = state.Page{Keys: []string{"listing"}}
			f.records["listing"] = state.Object{Data: []byte(`{"run_id":"run"}`)}
			expected := 200
			switch scenario {
			case "expired listing":
				f.readErr = state.ErrExpired
			case "missing listing":
				f.readErr = state.ErrNotFound
			case "read failure":
				f.readErr = errors.New("failed")
				expected = 503
			case "corrupt listing":
				f.records["listing"] = state.Object{Data: []byte("{")}
				expected = 503
			case "expired run":
				f.err = state.ErrExpired
			case "run failure":
				f.err = errors.New("failed")
				expected = 503
			}
			if w := call(h, "GET", "/v1/events/daily-bhavcopy/runs", ""); w.Code != expected {
				t.Fatal(w.Code, w.Body.String())
			}
		})
	}
}
