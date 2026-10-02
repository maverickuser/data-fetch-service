// Package events normalizes supported transports into the service event model.
package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

const PullRequested = "com.bondplatform.data.pull.requested.v1"

// CloudEvent is the structured JSON CloudEvents 1.0 envelope accepted on ingress.
type CloudEvent struct {
	SpecVersion     string          `json:"specversion"`
	ID              string          `json:"id"`
	Source          string          `json:"source"`
	Type            string          `json:"type"`
	Time            time.Time       `json:"time"`
	DataContentType string          `json:"datacontenttype"`
	Subject         string          `json:"subject,omitempty"`
	Data            json.RawMessage `json:"data"`
}

// Data is the business event payload after CloudEvents envelope validation.
type Data struct {
	SchemaVersion int            `json:"schema_version"`
	EventType     string         `json:"event_type"`
	Inputs        map[string]any `json:"inputs"`
}

// Normalized is the stable identity and payload used by admission logic.
type Normalized struct {
	EventID    string
	Source     string
	Type       string
	Subject    string
	OccurredAt time.Time
	EventType  string
	Inputs     map[string]any
	Original   json.RawMessage
	Force      bool
}

// ParseCloudEvent validates a structured JSON CloudEvent and its business data.
func ParseCloudEvent(payload []byte) (Normalized, error) {
	var event CloudEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return Normalized{}, fmt.Errorf("decode CloudEvent: %w", err)
	}
	if event.SpecVersion != "1.0" || strings.TrimSpace(event.ID) == "" || strings.TrimSpace(event.Source) == "" || event.Type != PullRequested {
		return Normalized{}, errors.New("CloudEvent requires specversion 1.0, id, source, and type")
	}
	if _, err := url.ParseRequestURI(event.Source); err != nil {
		return Normalized{}, fmt.Errorf("invalid event source: %w", err)
	}
	if event.DataContentType != "application/json" {
		return Normalized{}, errors.New("CloudEvent datacontenttype must be application/json")
	}
	if event.Time.IsZero() {
		return Normalized{}, errors.New("CloudEvent time is required")
	}
	var data Data
	if err := json.Unmarshal(event.Data, &data); err != nil {
		return Normalized{}, fmt.Errorf("decode event data: %w", err)
	}
	if data.SchemaVersion != 1 || data.EventType == "" || data.Inputs == nil {
		return Normalized{}, errors.New("event data requires schema_version 1, event_type, and inputs")
	}
	return Normalized{EventID: event.ID, Source: event.Source, Type: event.Type, Subject: event.Subject, OccurredAt: event.Time, EventType: data.EventType, Inputs: data.Inputs, Original: append(json.RawMessage(nil), payload...)}, nil
}

// ValidateInputString validates a required string input without normalizing it.
func ValidateInputString(inputs map[string]any, name string) (string, error) {
	value, ok := inputs[name].(string)
	if !ok || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("input %s must be a nonblank string", name)
	}
	return value, nil
}
