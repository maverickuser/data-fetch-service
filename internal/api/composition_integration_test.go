//go:build integration

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/admission"
	"github.com/maverickuser/data-fetch-service/internal/domain"
	"github.com/maverickuser/data-fetch-service/internal/events"
	"github.com/maverickuser/data-fetch-service/internal/state"
)

type integrationObjects struct {
	mu         sync.Mutex
	objects    map[string]state.Object
	version    int
	now        time.Time
	firstReads int
	barrier    chan struct{}
}

func (m *integrationObjects) Get(ctx context.Context, key string) (state.Object, error) {
	m.mu.Lock()
	if m.barrier != nil && strings.HasSuffix(key, "/intent.json") && m.firstReads < 2 {
		m.firstReads++
		if m.firstReads == 2 {
			close(m.barrier)
		}
		barrier := m.barrier
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return state.Object{}, ctx.Err()
		case <-barrier:
			return state.Object{}, state.ErrNotFound
		}
	}
	defer m.mu.Unlock()
	o, ok := m.objects[key]
	if !ok {
		return state.Object{}, state.ErrNotFound
	}
	o.Data = append([]byte(nil), o.Data...)
	return o, nil
}
func (m *integrationObjects) Put(_ context.Context, key string, data []byte, match string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	old, exists := m.objects[key]
	if (match == "" && exists) || (match != "" && (!exists || old.ETag != match)) {
		return "", state.ErrConflict
	}
	m.version++
	etag := fmt.Sprint(m.version)
	m.objects[key] = state.Object{Data: append([]byte(nil), data...), ETag: etag, Modified: m.now}
	return etag, nil
}
func (m *integrationObjects) List(_ context.Context, prefix, token string, limit int32) (state.Page, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := []string{}
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) && k > token {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	page := state.Page{Keys: keys}
	if len(keys) > int(limit) {
		page.Keys = keys[:limit]
		page.NextToken = page.Keys[len(page.Keys)-1]
	}
	return page, nil
}

type integrationQueue struct {
	mu   sync.Mutex
	runs []string
	fail bool
}

func (q *integrationQueue) Publish(_ context.Context, queue, run string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.runs = append(q.runs, queue+":"+run)
	if q.fail {
		return fmt.Errorf("lost queue reply")
	}
	return nil
}

func integrationHandler(t *testing.T, objects *integrationObjects, queue *integrationQueue, now time.Time, id string) (*Handler, *admission.Service) {
	t.Helper()
	unit, _ := handlerFixture(t)
	store := state.New(objects)
	clock := func() time.Time { return now }
	coord := state.NewCoordinator(store, clock)
	service := &admission.Service{Config: unit.Config, Coordinator: coord, Publisher: queue, Now: clock, NewID: func() string { return id }}
	return &Handler{Service: service, Store: store, Coordinator: coord, Config: unit.Config, Now: clock}, service
}

func TestHTTPCompositionConcurrentFirstRequestAcrossMidnight(t *testing.T) {
	// Both first reads see no intent. Server clocks straddle midnight in Asia/Kolkata.
	before := time.Date(2026, 9, 21, 18, 29, 59, 0, time.UTC)
	after := before.Add(2 * time.Second)
	objects := &integrationObjects{objects: map[string]state.Object{}, now: after, barrier: make(chan struct{})}
	queue := &integrationQueue{}
	h1, _ := integrationHandler(t, objects, queue, before, "candidate-1")
	h2, _ := integrationHandler(t, objects, queue, after, "candidate-2")
	results := make(chan *httptest.ResponseRecorder, 2)
	var wg sync.WaitGroup
	for _, handler := range []*Handler{h1, h2} {
		wg.Go(func() {
			results <- call(handler, "POST", "/v1/events/daily-bhavcopy/runs", `{"inputs":{"exchangeName":"BSE"}}`)
		})
	}
	wg.Wait()
	close(results)
	chosen := ""
	for response := range results {
		if response.Code != 202 {
			t.Fatal(response.Code, response.Body.String())
		}
		var result admission.Result
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if chosen == "" {
			chosen = result.RunID
		}
		if chosen != result.RunID {
			t.Fatal("requests diverged")
		}
	}
	view, err := h1.Coordinator.ReadRun(context.Background(), chosen)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot events.Snapshot
	if err := json.Unmarshal(view.Snapshot, &snapshot); err != nil {
		t.Fatal(err)
	}
	date := "2026-09-21"
	if chosen == "candidate-2" {
		date = "2026-09-22"
	}
	if snapshot.Inputs["run_date"] != date {
		t.Fatal(snapshot.Inputs)
	}
	// The same key with a real change remains a conflict, even after race recovery.
	if response := call(h1, "POST", "/v1/events/daily-bhavcopy/runs", `{"inputs":{"exchangeName":"BSE"},"force":true}`); response.Code != 409 {
		t.Fatal(response.Code, response.Body.String())
	}
}

func TestHTTPCompositionTimeoutRecoveryReadsAndPagination(t *testing.T) {
	now := time.Date(2026, 9, 21, 14, 30, 0, 0, time.UTC)
	objects := &integrationObjects{objects: map[string]state.Object{}, now: now}
	queue := &integrationQueue{fail: true}
	h, service := integrationHandler(t, objects, queue, now, "run-1")
	body := `{"inputs":{"exchangeName":"BSE"}}`
	if response := call(h, "POST", "/v1/events/daily-bhavcopy/runs", body); response.Code != 503 {
		t.Fatal(response.Code)
	}
	queue.fail = false
	response := call(h, "POST", "/v1/events/daily-bhavcopy/runs", body)
	if response.Code != 202 {
		t.Fatal(response.Code, response.Body.String())
	}
	for _, path := range []string{"/v1/runs/run-1", "/v1/runs/run-1/history", "/v1/runs/run-1/pulls", "/v1/runs/run-1/delivery", "/v1/requests/urn%3Abond-platform%3Amanual/request-key", "/v1/events/daily-bhavcopy/active?inputs=" + url.QueryEscape(`{"exchangeName":"BSE"}`)} {
		if r := call(h, "GET", path, ""); r.Code != 200 {
			t.Fatal(path, r.Code, r.Body.String())
		}
	}
	service.NewID = func() string { return "run-2" }
	r := httptest.NewRequest("POST", "/v1/events/daily-bhavcopy/runs", strings.NewReader(`{"inputs":{"exchangeName":"BSE2"}}`))
	r.Header.Set("Idempotency-Key", "second")
	w := httptest.NewRecorder()
	h.Routes().ServeHTTP(w, r)
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	first := call(h, "GET", "/v1/events/daily-bhavcopy/runs?limit=1", "")
	var page struct {
		Items []state.RunView `json:"items"`
		Next  string          `json:"next_cursor"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Next == "" {
		t.Fatal(first.Body.String())
	}
	firstID := page.Items[0].RunID
	second := call(h, "GET", "/v1/events/daily-bhavcopy/runs?limit=1&cursor="+page.Next, "")
	if err := json.Unmarshal(second.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].RunID == firstID || page.Next != "" {
		t.Fatal(second.Body.String())
	}
	snapshot, err := h.Coordinator.ReadRun(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	var saved events.Snapshot
	if err := json.Unmarshal(snapshot.Snapshot, &saved); err != nil {
		t.Fatal(err)
	}
	coord := service.Coordinator.(*state.Coordinator)
	key := "coordination/" + saved.ExecutionKey + ".json"
	lease, err := coord.ClaimDispatched(context.Background(), key, "run-1", "pull", "worker", now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := coord.Commit(context.Background(), key, lease, state.Transition{RunID: "run-1", Sequence: 2, Phase: domain.Failed, At: now}); err != nil {
		t.Fatal(err)
	}
	if result := call(h, "POST", "/v1/events/daily-bhavcopy/runs", body); result.Code != 200 {
		t.Fatal(result.Code, result.Body.String())
	}
}

func TestHTTPCompositionExternalRaceAcrossConfigVersions(t *testing.T) {
	now := time.Date(2026, 9, 21, 14, 30, 0, 0, time.UTC)
	objects := &integrationObjects{objects: map[string]state.Object{}, now: now, barrier: make(chan struct{})}
	queue := &integrationQueue{}
	_, first := integrationHandler(t, objects, queue, now, "first")
	_, second := integrationHandler(t, objects, queue, now, "second")
	changed := second.Config.Events[0].Inputs["run_date"]
	changed.Default = "2026-09-23"
	second.Config.Events[0].Inputs["run_date"] = changed
	second.Config.DeploymentCommit = "new-version"
	event := events.Normalized{EventID: "same", Source: "urn:test", Type: events.PullRequested, EventType: "daily-bhavcopy", OccurredAt: now, Inputs: map[string]any{"exchangeName": "BSE"}}
	results := make(chan admission.Result, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, service := range []*admission.Service{first, second} {
		wg.Go(func() { result, err := service.External(context.Background(), event); results <- result; errs <- err })
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	run := ""
	for result := range results {
		if run == "" {
			run = result.RunID
		}
		if result.RunID != run {
			t.Fatal("external replay diverged")
		}
	}
	view, err := first.Coordinator.ReadRun(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot events.Snapshot
	if err := json.Unmarshal(view.Snapshot, &snapshot); err != nil {
		t.Fatal(err)
	}
	if !snapshot.Event.OccurredAt.Equal(now) {
		t.Fatal("producer timestamp changed")
	}
}
