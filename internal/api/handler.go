// Package api exposes bounded manual admission and read-only run resources.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/admission"
	"github.com/maverickuser/data-fetch-service/internal/config"
	"github.com/maverickuser/data-fetch-service/internal/domain"
	"github.com/maverickuser/data-fetch-service/internal/events"
	"github.com/maverickuser/data-fetch-service/internal/state"
)

// Handler shares durable admission with SQS; read routes never perform recovery.
type Handler struct {
	Service          ManualAdmission
	Recovery         FullRerunAdmission
	DeliveryRecovery DeliveryRetryAdmission
	Store            Records
	Coordinator      Reader
	Config           config.Config
	Now              func() time.Time
}

// FullRerunAdmission creates a linked run from one terminal admitted snapshot.
type FullRerunAdmission interface {
	FullRerun(context.Context, string, string, *bool) (admission.Result, error)
}

// DeliveryRetryAdmission starts delivery against retained source files.
type DeliveryRetryAdmission interface {
	DeliveryRetry(context.Context, string, string, bool) (admission.Result, error)
}

// ManualAdmission is the application boundary used by the HTTP adapter.
type ManualAdmission interface {
	Manual(context.Context, string, string, map[string]any, bool) (admission.Result, error)
}

// Reader exposes immutable run reads and current execution authority without mutation.
type Reader interface {
	ReadRun(context.Context, string) (state.RunView, error)
	History(context.Context, string, string, int32) (state.HistoryPage, error)
	RequestResolution(context.Context, string) (state.Resolution, error)
	Load(context.Context, string) (state.Coordination, string, error)
}

// Records supports bounded metadata/listing reads for the REST projection.
type Records interface {
	Scan(context.Context, string, string, int32) (state.Page, error)
	Read(context.Context, string, time.Time) (state.Object, error)
}

// Routes binds manual admission, full rerun, and bounded read resources.
func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/events/{event_type}/runs", h.manual)
	mux.HandleFunc("POST /v1/runs/{run_id}/reruns", h.fullRerun)
	mux.HandleFunc("POST /v1/runs/{run_id}/delivery-retries", h.deliveryRetry)
	mux.HandleFunc("GET /v1/events/{event_type}", h.event)
	mux.HandleFunc("GET /v1/events/{event_type}/active", h.active)
	mux.HandleFunc("GET /v1/events/{event_type}/runs", h.listRuns)
	mux.HandleFunc("GET /v1/runs/{run_id}", h.run)
	mux.HandleFunc("GET /v1/runs/{run_id}/history", h.history)
	mux.HandleFunc("GET /v1/runs/{run_id}/pulls", h.records)
	mux.HandleFunc("GET /v1/runs/{run_id}/delivery", h.records)
	mux.HandleFunc("GET /v1/requests/{producer}/{event_id}", h.request)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(r.URL.RawQuery) > 16<<10 {
			h.fail(w, admission.ErrInvalid)
			return
		}
		if _, err := url.ParseQuery(r.URL.RawQuery); err != nil {
			h.fail(w, admission.ErrInvalid)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

// deliveryRetry accepts only an explicit processor-profile choice and idempotency key.
func (h *Handler) deliveryRetry(w http.ResponseWriter, r *http.Request) {
	if h.DeliveryRecovery == nil {
		h.fail(w, admission.ErrInvalid)
		return
	}
	var body struct {
		UseCurrent bool `json:"use_current_processor_config"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil && err != io.EOF {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			h.write(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "BODY_TOO_LARGE"})
			return
		}
		h.fail(w, admission.ErrInvalid)
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		h.fail(w, admission.ErrInvalid)
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if len(key) > 256 {
		h.fail(w, admission.ErrInvalid)
		return
	}
	result, err := h.DeliveryRecovery.DeliveryRetry(r.Context(), r.PathValue("run_id"), key, body.UseCurrent)
	if err != nil {
		h.fail(w, err)
		return
	}
	status := http.StatusAccepted
	if result.CompletedReplay {
		status = http.StatusOK
	}
	w.Header().Set("Location", result.StatusURL)
	h.write(w, status, result)
}

// fullRerun accepts only an optional force choice and an idempotency key for a terminal run.
func (h *Handler) fullRerun(w http.ResponseWriter, r *http.Request) {
	if h.Recovery == nil {
		h.fail(w, admission.ErrInvalid)
		return
	}
	var body struct {
		Force *bool `json:"force"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil && err != io.EOF {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			h.write(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "BODY_TOO_LARGE"})
			return
		}
		h.fail(w, admission.ErrInvalid)
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		h.fail(w, admission.ErrInvalid)
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if len(key) > 256 {
		h.fail(w, admission.ErrInvalid)
		return
	}
	result, err := h.Recovery.FullRerun(r.Context(), r.PathValue("run_id"), key, body.Force)
	if err != nil {
		h.fail(w, err)
		return
	}
	status := http.StatusAccepted
	if result.CompletedReplay {
		status = http.StatusOK
	}
	w.Header().Set("Location", result.StatusURL)
	h.write(w, status, result)
}

// manual rejects unknown/unbounded input and returns the durable run reference.
func (h *Handler) manual(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Inputs map[string]any `json:"inputs"`
		Force  bool           `json:"force"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			h.write(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "BODY_TOO_LARGE"})
			return
		}
		h.fail(w, admission.ErrInvalid)
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF || body.Inputs == nil {
		h.fail(w, admission.ErrInvalid)
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if len(key) > 256 {
		h.fail(w, admission.ErrInvalid)
		return
	}
	result, err := h.Service.Manual(r.Context(), r.PathValue("event_type"), key, body.Inputs, body.Force)
	if err != nil {
		h.fail(w, err)
		return
	}
	status := http.StatusAccepted
	if result.CompletedReplay {
		status = http.StatusOK
	}
	w.Header().Set("Location", result.StatusURL)
	h.write(w, status, result)
}

// event exposes one configured event definition and its deployment revision.
func (h *Handler) event(w http.ResponseWriter, r *http.Request) {
	def, ok := h.Config.Event(r.PathValue("event_type"))
	if !ok {
		h.fail(w, state.ErrNotFound)
		return
	}
	h.write(w, http.StatusOK, map[string]any{"event": def, "config_revision": h.Config.Revision()})
}

// run returns the pinned snapshot and status reconstructed from immutable history.
func (h *Handler) run(w http.ResponseWriter, r *http.Request) {
	view, err := h.Coordinator.ReadRun(r.Context(), r.PathValue("run_id"))
	if err != nil {
		h.fail(w, err)
		return
	}
	h.write(w, http.StatusOK, view)
}

// history requires an unexpired run and returns one bounded transition page.
func (h *Handler) history(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("run_id")
	if _, err := h.Coordinator.ReadRun(r.Context(), id); err != nil {
		h.fail(w, err)
		return
	}
	limit, err := pageLimit(r)
	if err != nil {
		h.fail(w, err)
		return
	}
	page, err := h.Coordinator.History(r.Context(), id, r.URL.Query().Get("cursor"), limit)
	if err != nil {
		h.fail(w, err)
		return
	}
	h.write(w, http.StatusOK, map[string]any{"items": page.Transitions, "next_cursor": page.NextToken})
}

// request resolves the URL-encoded producer/event identity without admitting work.
func (h *Handler) request(w http.ResponseWriter, r *http.Request) {
	key, err := domain.RequestKey(r.PathValue("producer"), r.PathValue("event_id"))
	if err != nil {
		h.fail(w, admission.ErrInvalid)
		return
	}
	receipt, err := h.Coordinator.RequestResolution(r.Context(), key)
	if err != nil {
		h.fail(w, err)
		return
	}
	h.write(w, http.StatusOK, receipt)
}

// active normalizes event inputs to find the matching execution authority.
func (h *Handler) active(w http.ResponseWriter, r *http.Request) {
	var inputs map[string]any
	raw := r.URL.Query().Get("inputs")
	if len(raw) > 65536 || json.Unmarshal([]byte(raw), &inputs) != nil || inputs == nil {
		h.fail(w, admission.ErrInvalid)
		return
	}
	snapshot, err := events.BuildSnapshot(h.Config, events.Normalized{EventID: "lookup", Source: "urn:lookup", EventType: r.PathValue("event_type"), Inputs: inputs, OccurredAt: h.Now()}, "lookup")
	if err != nil {
		h.fail(w, admission.ErrInvalid)
		return
	}
	var resolved events.Snapshot
	if err := json.Unmarshal(snapshot, &resolved); err != nil {
		h.fail(w, err)
		return
	}
	current, _, err := h.Coordinator.Load(r.Context(), "coordination/"+resolved.ExecutionKey+".json")
	if err != nil {
		h.fail(w, err)
		return
	}
	if current.ActiveRunID == "" {
		h.fail(w, state.ErrNotFound)
		return
	}
	h.write(w, http.StatusOK, map[string]string{"run_id": current.ActiveRunID})
}

// records returns bounded stored job/delivery metadata, never source response objects.
func (h *Handler) records(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("run_id")
	if _, err := h.Coordinator.ReadRun(r.Context(), id); err != nil {
		h.fail(w, err)
		return
	}
	limit, err := pageLimit(r)
	if err != nil {
		h.fail(w, err)
		return
	}
	kind := "pulls"
	if strings.HasSuffix(r.URL.Path, "/delivery") {
		kind = "delivery"
	}
	page, err := h.Store.Scan(r.Context(), "runs/"+id+"/"+kind+"/", r.URL.Query().Get("cursor"), limit)
	if err != nil {
		h.fail(w, err)
		return
	}
	records := []json.RawMessage{}
	remaining := 1 << 20
	for _, key := range page.Keys {
		object, err := h.Store.Read(r.Context(), key, h.Now())
		if err != nil {
			h.fail(w, err)
			return
		}
		remaining -= len(object.Data)
		if remaining < 0 {
			h.write(w, http.StatusUnprocessableEntity, map[string]string{"error": "RESPONSE_TOO_LARGE_REDUCE_LIMIT"})
			return
		}
		records = append(records, json.RawMessage(object.Data))
	}
	h.write(w, http.StatusOK, map[string]any{"items": records, "next_cursor": page.NextToken})
}

// pageLimit constrains each read to at most 100 records.
func pageLimit(r *http.Request) (int32, error) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return 25, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > 100 {
		return 0, admission.ErrInvalid
	}
	return int32(n), nil
}

// fail returns stable error categories without leaking storage or configuration internals.
func (h *Handler) fail(w http.ResponseWriter, err error) {
	status, code := http.StatusServiceUnavailable, "UNAVAILABLE"
	switch {
	case errors.Is(err, admission.ErrInvalid):
		status, code = http.StatusBadRequest, "INVALID_REQUEST"
	case errors.Is(err, state.ErrNotFound):
		status, code = http.StatusNotFound, "NOT_FOUND"
	case errors.Is(err, state.ErrExpired):
		status, code = http.StatusGone, "EXPIRED"
	case errors.Is(err, state.ErrIntegrity), errors.Is(err, state.ErrConflict):
		status, code = http.StatusConflict, "REQUEST_CONFLICT"
	}
	h.write(w, status, map[string]string{"error": code})
}

// write encodes before committing status; response write failures end the response naturally.
func (h *Handler) write(w http.ResponseWriter, status int, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		status = http.StatusInternalServerError
		data = []byte(`{"error":"ENCODING_FAILED"}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(data); err != nil {
		return
	}
}
