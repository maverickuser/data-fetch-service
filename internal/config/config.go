// Package config loads and validates the versioned service configuration.
package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

var safeName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]*$`)

// Config is the complete effective service configuration after overlays.
type Config struct {
	DeploymentCommit string     `yaml:"deployment_commit,omitempty" json:"deployment_commit"`
	SchemaVersion    int        `yaml:"schema_version" json:"schema_version"`
	Environment      string     `yaml:"environment,omitempty" json:"environment,omitempty"`
	AWSRegion        string     `yaml:"aws_region,omitempty" json:"aws_region,omitempty"`
	ResourcePrefix   string     `yaml:"resource_prefix,omitempty" json:"resource_prefix,omitempty"`
	Defaults         Defaults   `yaml:"defaults" json:"defaults"`
	Processor        Processor  `yaml:"processor" json:"processor"`
	Events           []EventDef `yaml:"events" json:"events"`
}

// Defaults contains shared acquisition and retry limits.
type Defaults struct {
	PullConcurrency         int   `yaml:"pull_concurrency" json:"pull_concurrency"`
	RequestTimeoutSeconds   int   `yaml:"request_timeout_seconds" json:"request_timeout_seconds"`
	SourceMaxAttempts       int   `yaml:"source_max_attempts" json:"source_max_attempts"`
	PullExecutionMaxRetries int   `yaml:"pull_execution_max_retries" json:"pull_execution_max_retries"`
	MaxDownloadBytes        int64 `yaml:"max_download_bytes" json:"max_download_bytes"`
	MaxExtractedBytes       int64 `yaml:"max_extracted_bytes" json:"max_extracted_bytes"`
	MaxZipEntries           int   `yaml:"max_zip_entries" json:"max_zip_entries"`
	MaxCompressionRatio     int   `yaml:"max_compression_ratio" json:"max_compression_ratio"`
	MaxValidationTokenBytes int64 `yaml:"max_validation_token_bytes" json:"max_validation_token_bytes"`
}

// Processor configures the private downstream admission endpoint.
type Processor struct {
	Enabled               bool   `yaml:"enabled" json:"enabled"`
	URL                   string `yaml:"url" json:"url"`
	RequestTimeoutSeconds int    `yaml:"request_timeout_seconds" json:"request_timeout_seconds"`
	MaxAttempts           int    `yaml:"max_attempts" json:"max_attempts"`
}

// EventDef describes one supported pull event and its jobs.
type EventDef struct {
	ID          string              `yaml:"id" json:"id"`
	Enabled     bool                `yaml:"enabled" json:"enabled"`
	DatasetType string              `yaml:"dataset_type" json:"dataset_type"`
	Timezone    string              `yaml:"timezone" json:"timezone"`
	Inputs      map[string]InputDef `yaml:"inputs" json:"inputs"`
	Schedule    *Schedule           `yaml:"schedule,omitempty" json:"schedule,omitempty"`
	Jobs        []Job               `yaml:"jobs" json:"jobs"`
}

// InputDef declares one event input.
type InputDef struct {
	Type     string `yaml:"type" json:"type"`
	Required bool   `yaml:"required" json:"required"`
	Pattern  string `yaml:"pattern,omitempty" json:"pattern,omitempty"`
	Default  string `yaml:"default,omitempty" json:"default,omitempty"`
}

// Schedule describes an EventBridge schedule and its fixed inputs.
type Schedule struct {
	Enabled    bool              `yaml:"enabled" json:"enabled"`
	Expression string            `yaml:"expression" json:"expression"`
	Timezone   string            `yaml:"timezone" json:"timezone"`
	Inputs     map[string]string `yaml:"inputs" json:"inputs"`
}

// Job describes one source request and its persisted output.
type Job struct {
	ID       string   `yaml:"id" json:"id"`
	Request  Request  `yaml:"request" json:"request"`
	Response Response `yaml:"response" json:"response"`
}

// Request describes the fixed source request template.
type Request struct {
	Method      string              `yaml:"method" json:"method"`
	URLTemplate string              `yaml:"url_template" json:"url_template"`
	Variables   map[string]Variable `yaml:"variables,omitempty" json:"variables,omitempty"`
}

// Variable describes a safe template variable.
type Variable struct {
	Template string `yaml:"template" json:"template"`
}

// Response describes the source representation and output naming rules.
type Response struct {
	Format           string      `yaml:"format" json:"format"`
	Extraction       *Extraction `yaml:"extraction,omitempty" json:"extraction,omitempty"`
	FilenameTemplate string      `yaml:"filename_template" json:"filename_template"`
}

// Extraction selects one member from a ZIP response.
type Extraction struct {
	MemberPath       string `yaml:"member_path" json:"member_path"`
	Format           string `yaml:"format" json:"format"`
	FilenameTemplate string `yaml:"filename_template" json:"filename_template"`
}

// Load parses YAML and validates the complete effective configuration.
func Load(data []byte) (Config, error) {
	var cfg Config
	decoder := yaml.NewDecoder(strings.NewReader(string(data)))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return Config{}, errors.New("configuration must contain one YAML document")
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// LoadEffective merges a base event file with one environment overlay and validates the result.
func LoadEffective(base, overlay []byte) (Config, error) {
	var baseMap, overlayMap map[string]any
	if err := decodeMapping(base, &baseMap); err != nil {
		return Config{}, fmt.Errorf("decode base config: %w", err)
	}
	if err := decodeMapping(overlay, &overlayMap); err != nil {
		return Config{}, fmt.Errorf("decode environment overlay: %w", err)
	}
	mergeMaps(baseMap, overlayMap)
	merged, err := yaml.Marshal(baseMap)
	if err != nil {
		return Config{}, fmt.Errorf("marshal effective config: %w", err)
	}
	return Load(merged)
}

// decodeMapping rejects empty, multi-document, or non-JSON-compatible YAML mappings.
func decodeMapping(data []byte, target *map[string]any) error {
	d := yaml.NewDecoder(strings.NewReader(string(data)))
	if err := d.Decode(target); err != nil {
		return err
	}
	if *target == nil {
		return errors.New("expected nonempty mapping")
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return errors.New("expected one YAML document")
	}
	_, err := json.Marshal(target)
	return err
}

// mergeMaps recursively merges object keys; arrays and scalar values are replaced.
func mergeMaps(base, overlay map[string]any) {
	for key, value := range overlay {
		if nested, ok := value.(map[string]any); ok {
			if current, ok := base[key].(map[string]any); ok {
				mergeMaps(current, nested)
				continue
			}
		}
		base[key] = value
	}
}

// Validate rejects unsafe, incomplete, or internally inconsistent configuration.
func (c Config) Validate() error {
	if c.SchemaVersion != 1 {
		return fmt.Errorf("schema_version must be 1")
	}
	if c.Environment != "" && c.Environment != "prod" {
		return errors.New("only prod environment is supported")
	}
	if c.ResourcePrefix != "" && c.ResourcePrefix != "data-fetch-service" {
		return errors.New("invalid resource prefix")
	}
	if len(c.Events) == 0 {
		return errors.New("at least one event is required")
	}
	if c.Defaults.PullConcurrency < 1 || c.Defaults.RequestTimeoutSeconds < 1 || c.Defaults.SourceMaxAttempts < 1 || c.Defaults.PullExecutionMaxRetries < 0 {
		return errors.New("invalid retry or concurrency defaults")
	}
	if c.Defaults.MaxDownloadBytes < 1 || c.Defaults.MaxExtractedBytes < 1 || c.Defaults.MaxZipEntries < 1 || c.Defaults.MaxCompressionRatio < 1 || c.Defaults.MaxValidationTokenBytes < 1 {
		return errors.New("resource limits must be positive")
	}
	if c.Processor.RequestTimeoutSeconds < 1 || c.Processor.MaxAttempts < 1 {
		return errors.New("invalid processor limits")
	}
	if err := validateHTTPS(c.Processor.URL); err != nil {
		return fmt.Errorf("processor.url must be an HTTPS URL")
	}
	events := map[string]bool{}
	for _, event := range c.Events {
		if event.ID == "" || !safeName.MatchString(event.ID) || events[event.ID] {
			return fmt.Errorf("invalid or duplicate event id %q", event.ID)
		}
		events[event.ID] = true
		if (event.ID == "daily-bhavcopy" && event.DatasetType != "urn:bond-platform:dataset:bse-debt-trades") || (event.ID == "nsdl-bond-data" && event.DatasetType != "urn:bond-platform:dataset:nsdl-security") {
			return errors.New("dataset URN does not match event")
		}
		if !strings.HasPrefix(event.DatasetType, "urn:bond-platform:dataset:") {
			return fmt.Errorf("event %s has invalid dataset_type", event.ID)
		}
		if event.Timezone == "" || len(event.Jobs) == 0 {
			return fmt.Errorf("event %s requires timezone and jobs", event.ID)
		}
		if _, err := time.LoadLocation(event.Timezone); err != nil {
			return fmt.Errorf("event %s: %w", event.ID, err)
		}
		for name, input := range event.Inputs {
			if !safeName.MatchString(name) || (input.Type != "string" && input.Type != "date") {
				return fmt.Errorf("invalid input %s", name)
			}
			if _, err := regexp.Compile(input.Pattern); err != nil {
				return fmt.Errorf("invalid input pattern %s: %w", name, err)
			}
		}
		if event.Schedule != nil && event.Schedule.Enabled {
			if event.Schedule.Expression == "" {
				return errors.New("enabled schedule requires expression")
			}
			if _, err := time.LoadLocation(event.Schedule.Timezone); err != nil {
				return fmt.Errorf("schedule timezone: %w", err)
			}
			for name, input := range event.Inputs {
				if input.Required && input.Default == "" && event.Schedule.Inputs[name] == "" {
					return fmt.Errorf("schedule missing required input %s", name)
				}
			}
		}
		jobs := map[string]bool{}
		for _, job := range event.Jobs {
			if job.ID == "" || !safeName.MatchString(job.ID) || jobs[job.ID] {
				return fmt.Errorf("event %s has invalid or duplicate job id %q", event.ID, job.ID)
			}
			jobs[job.ID] = true
			if job.Request.Method != "GET" || job.Request.URLTemplate == "" {
				return fmt.Errorf("job %s/%s must define GET url_template", event.ID, job.ID)
			}
			if err := validateHTTPS(job.Request.URLTemplate); err != nil {
				return fmt.Errorf("job %s/%s has invalid URL template", event.ID, job.ID)
			}
			if job.Response.Format != "json" && job.Response.Format != "zip" && job.Response.Format != "csv" {
				return fmt.Errorf("job %s/%s has unsupported response format", event.ID, job.ID)
			}
			if _, isin := event.Inputs["isin_code"]; isin && job.Response.Format == "json" && !strings.Contains(job.Response.FilenameTemplate, "{isin_code}") {
				return fmt.Errorf("job %s/%s requires ISIN filename template", event.ID, job.ID)
			}
			if job.Response.Format == "zip" && (job.Response.Extraction == nil || job.Response.Extraction.MemberPath == "") {
				return fmt.Errorf("job %s/%s ZIP requires extraction member_path", event.ID, job.ID)
			}
			if job.Response.Format != "zip" && job.Response.Extraction != nil {
				return errors.New("extraction requires ZIP format")
			}
			if x := job.Response.Extraction; x != nil && x.Format != "csv" && x.Format != "json" {
				return errors.New("unsupported extracted format")
			}
		}
		if err := validateTemplates(event); err != nil {
			return fmt.Errorf("event %s: %w", event.ID, err)
		}
	}
	return nil
}

// validateHTTPS requires fixed HTTPS authority without credentials or fragments.
func validateHTTPS(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" || strings.ContainsAny(u.Host, "{}") {
		return errors.New("expected fixed HTTPS authority")
	}
	return nil
}

// Revision returns a deterministic SHA-256 revision of the effective config.
func (c Config) Revision() string {
	canonical, _ := json.Marshal(c)
	digest := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(digest[:])
}

// Event returns an event definition by its stable ID.
func (c Config) Event(id string) (EventDef, bool) {
	for _, event := range c.Events {
		if event.ID == id {
			return event, true
		}
	}
	return EventDef{}, false
}
