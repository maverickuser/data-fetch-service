package events

import (
	"encoding/json"
	"fmt"
	"time"
)

// RuleMapping binds an approved native rule or event signature to configured inputs.
type RuleMapping struct {
	RuleARN      string
	Account      string
	Region       string
	Source       string
	DetailType   string
	EventType    string
	Inputs       map[string]any
	DetailFields map[string]string // Input name to top-level detail field name.
}

// Record preserves each SQS transport identifier and independent decoding outcome.
type Record struct {
	MessageID string
	Event     Normalized
	Err       error
}

// ParseSQS decodes records independently; one invalid record does not hide valid ones.
func ParseSQS(payload []byte, mappings []RuleMapping) ([]Record, error) {
	var batch struct {
		Records []struct {
			MessageID string `json:"messageId"`
			Body      string `json:"body"`
		} `json:"Records"`
	}
	if err := json.Unmarshal(payload, &batch); err != nil {
		return nil, err
	}
	if len(batch.Records) == 0 {
		return nil, fmt.Errorf("SQS records required")
	}
	out := make([]Record, 0, len(batch.Records))
	for _, record := range batch.Records {
		event, err := ParseTransport([]byte(record.Body), mappings)
		if record.MessageID == "" {
			err = fmt.Errorf("SQS messageId required")
		}
		out = append(out, Record{record.MessageID, event, err})
	}
	return out, nil
}

// ParseTransport accepts CloudEvents or explicitly mapped native EventBridge events.
func ParseTransport(payload []byte, mappings []RuleMapping) (Normalized, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return Normalized{}, err
	}
	if _, ok := envelope["specversion"]; ok {
		return ParseCloudEvent(payload)
	}
	var native struct {
		ID         string         `json:"id"`
		Source     string         `json:"source"`
		Time       time.Time      `json:"time"`
		Account    string         `json:"account"`
		Region     string         `json:"region"`
		DetailType string         `json:"detail-type"`
		Resources  []string       `json:"resources"`
		Detail     map[string]any `json:"detail"`
	}
	if err := json.Unmarshal(payload, &native); err != nil {
		return Normalized{}, err
	}
	if native.Time.IsZero() {
		return Normalized{}, fmt.Errorf("native event time required")
	}
	scheduled := native.Source == "aws.events" && native.DetailType == "Scheduled Event"
	var selected *RuleMapping
	for i := range mappings {
		m := &mappings[i]
		match := false
		if scheduled && m.RuleARN != "" {
			for _, resource := range native.Resources {
				if m.RuleARN == resource {
					match = true
				}
			}
		} else if !scheduled && m.RuleARN == "" {
			match = m.Account == native.Account && m.Region == native.Region && m.Source == native.Source && m.DetailType == native.DetailType
		}
		if match {
			if selected != nil {
				return Normalized{}, fmt.Errorf("ambiguous native event mapping")
			}
			selected = m
		}
	}
	if selected == nil || selected.EventType == "" {
		return Normalized{}, fmt.Errorf("unmapped native event")
	}
	id, source := native.ID, fmt.Sprintf("urn:aws:eventbridge:%s:%s:%s", native.Account, native.Region, native.Source)
	if scheduled {
		id, source = native.Time.UTC().Format(time.RFC3339Nano), selected.RuleARN
	}
	if id == "" {
		return Normalized{}, fmt.Errorf("native event id required")
	}
	inputs := make(map[string]any)
	for k, v := range selected.Inputs {
		inputs[k] = v
	}
	for input, field := range selected.DetailFields {
		value, ok := native.Detail[field]
		if !ok {
			return Normalized{}, fmt.Errorf("missing detail field %s", field)
		}
		inputs[input] = value
	}
	return Normalized{EventID: id, Source: source, Type: PullRequested, OccurredAt: native.Time, EventType: selected.EventType, Inputs: inputs, Original: append(json.RawMessage(nil), payload...)}, nil
}
