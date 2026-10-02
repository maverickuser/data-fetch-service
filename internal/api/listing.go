package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/admission"
	"github.com/maverickuser/data-fetch-service/internal/state"
)

// listingCursor binds the scan position to its original UTC date range and event filter.
type listingCursor struct {
	Version                     int
	Event, From, To, Day, Token string
}

// listRuns scans bounded UTC date partitions with a filter-bound continuation cursor.
func (h *Handler) listRuns(w http.ResponseWriter, r *http.Request) {
	event := r.PathValue("event_type")
	if _, ok := h.Config.Event(event); !ok {
		h.fail(w, state.ErrNotFound)
		return
	}
	limit, err := pageLimit(r)
	if err != nil {
		h.fail(w, err)
		return
	}
	query := r.URL.Query()
	from, to := query.Get("from"), query.Get("to")
	if from == "" {
		from = h.Now().UTC().Format(time.DateOnly)
	}
	if to == "" {
		to = from
	}
	start, e1 := time.Parse(time.DateOnly, from)
	end, e2 := time.Parse(time.DateOnly, to)
	if e1 != nil || e2 != nil || end.Before(start) || end.Sub(start) >= 30*24*time.Hour {
		h.fail(w, admission.ErrInvalid)
		return
	}
	cursor := listingCursor{Version: 1, Event: event, From: from, To: to, Day: from}
	if raw := query.Get("cursor"); raw != "" {
		if len(raw) > 8192 {
			h.fail(w, admission.ErrInvalid)
			return
		}
		data, err := base64.RawURLEncoding.DecodeString(raw)
		if err != nil || json.Unmarshal(data, &cursor) != nil || cursor.Version != 1 || cursor.Event != event || cursor.From != from || cursor.To != to {
			h.fail(w, admission.ErrInvalid)
			return
		}
	}
	day, err := time.Parse(time.DateOnly, cursor.Day)
	if err != nil || day.Before(start) || day.After(end) {
		h.fail(w, admission.ErrInvalid)
		return
	}
	items := []state.RunView{}
	next := ""
	remaining := limit
	// At most 30 list calls and 100 projections per request, including sparse partitions.
	for !day.After(end) && remaining > 0 {
		page, err := h.Store.Scan(r.Context(), "listings/"+event+"/"+day.Format(time.DateOnly)+"/", cursor.Token, remaining)
		if err != nil {
			h.fail(w, err)
			return
		}
		remaining -= int32(len(page.Keys))
		for _, key := range page.Keys {
			object, err := h.Store.Read(r.Context(), key, h.Now())
			if errors.Is(err, state.ErrExpired) || errors.Is(err, state.ErrNotFound) {
				continue
			}
			if err != nil {
				h.fail(w, err)
				return
			}
			var listing state.Listing
			if err := json.Unmarshal(object.Data, &listing); err != nil {
				h.fail(w, err)
				return
			}
			view, err := h.Coordinator.ReadRun(r.Context(), listing.RunID)
			if errors.Is(err, state.ErrExpired) || errors.Is(err, state.ErrNotFound) {
				continue
			}
			if err != nil {
				h.fail(w, err)
				return
			}
			view.Snapshot = nil
			items = append(items, view)
		}
		cursor.Token = page.NextToken
		if cursor.Token != "" {
			cursor.Day = day.Format(time.DateOnly)
			next = encodeCursor(cursor)
			break
		}
		day = day.AddDate(0, 0, 1)
		cursor.Day = day.Format(time.DateOnly)
	}
	if next == "" && !day.After(end) {
		next = encodeCursor(cursor)
	}
	h.write(w, http.StatusOK, map[string]any{"items": items, "next_cursor": next})
}

// encodeCursor preserves filter/version and provider position without exposing S3 keys.
func encodeCursor(cursor listingCursor) string {
	data, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(data)
}
