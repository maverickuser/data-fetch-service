package events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/config"
	"gopkg.in/yaml.v3"
)

// LoadMappings validates repository-managed native producer mappings before consumption.
func LoadMappings(data []byte, cfg config.Config) ([]RuleMapping, error) {
	var document struct {
		Mappings []RuleMapping `yaml:"mappings"`
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&document); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("one mapping document required")
	}
	seen := map[string]bool{}
	for _, m := range document.Mappings {
		def, ok := cfg.Event(m.EventType)
		if !ok {
			return nil, fmt.Errorf("mapping references unknown event")
		}
		if m.RuleARN != "" {
			if !strings.HasPrefix(m.RuleARN, "arn:") || !strings.Contains(m.RuleARN, ":events:") || len(m.DetailFields) > 0 {
				return nil, fmt.Errorf("invalid scheduled rule mapping")
			}
		} else if m.Account == "" || m.Region == "" || m.Source == "" || m.DetailType == "" {
			return nil, fmt.Errorf("native mapping requires complete signature")
		}
		identity, _ := json.Marshal([]string{m.RuleARN, m.Account, m.Region, m.Source, m.DetailType})
		if m.RuleARN != "" {
			identity = []byte(m.RuleARN)
		}
		if seen[string(identity)] {
			return nil, fmt.Errorf("duplicate native mapping")
		}
		seen[string(identity)] = true
		for name, value := range m.Inputs {
			if _, ok := def.Inputs[name]; !ok {
				return nil, fmt.Errorf("mapping input undeclared")
			}
			if _, ok := value.(string); !ok {
				return nil, fmt.Errorf("mapping inputs must be strings")
			}
		}
		for name, field := range m.DetailFields {
			if _, ok := def.Inputs[name]; !ok || field == "" {
				return nil, fmt.Errorf("invalid mapped detail field")
			}
			if _, duplicate := m.Inputs[name]; duplicate {
				return nil, fmt.Errorf("duplicate mapped input")
			}
		}
		for name, input := range def.Inputs {
			_, fixed := m.Inputs[name]
			_, dynamic := m.DetailFields[name]
			if input.Required && input.Default == "" && !fixed && !dynamic {
				return nil, fmt.Errorf("mapping missing required input")
			}
		}
		if len(m.DetailFields) == 0 {
			def.Enabled = true
			if _, _, err := def.Resolve(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), m.Inputs); err != nil {
				return nil, fmt.Errorf("invalid fixed mapping inputs: %w", err)
			}
		}
	}
	return document.Mappings, nil
}
