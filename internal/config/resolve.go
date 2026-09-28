package config

import (
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strings"
	"time"
	"unicode"
)

// ResolvedJob pins source and artifact names to the original logical event date.
type ResolvedJob struct {
	ID             string `json:"id"`
	URL            string `json:"url"`
	SourceFilename string `json:"source_filename"`
	Filename       string `json:"filename,omitempty"` // Empty until acquisition selects a response header or job-ID fallback.
	MemberPath     string `json:"member_path,omitempty"`
	Format         string `json:"format"`
}

// Resolve validates a fresh copy of inputs and resolves all jobs without I/O.
func (e EventDef) Resolve(trigger time.Time, supplied map[string]any) (map[string]string, []ResolvedJob, error) {
	if !e.Enabled || trigger.IsZero() {
		return nil, nil, fmt.Errorf("event disabled or missing trigger time")
	}
	inputs := make(map[string]string)
	for name := range supplied {
		if _, ok := e.Inputs[name]; !ok {
			return nil, nil, fmt.Errorf("undeclared input %s", name)
		}
	}
	for name, def := range e.Inputs {
		value := def.Default
		raw, suppliedValue := supplied[name]
		if suppliedValue {
			var ok bool
			value, ok = raw.(string)
			if !ok {
				return nil, nil, fmt.Errorf("input %s must be string", name)
			}
		}
		if def.Required && strings.TrimSpace(value) == "" {
			return nil, nil, fmt.Errorf("missing input %s", name)
		}
		if def.Type == "date" && value == "trigger_date" && !suppliedValue {
			value = ""
		}
		if def.Type == "date" {
			if suppliedValue && value == "" {
				return nil, nil, fmt.Errorf("input %s requires an ISO date", name)
			}
			d, err := ResolveLogicalDate(trigger, e.Timezone, value)
			if err != nil {
				return nil, nil, err
			}
			value = d.Format("2006-01-02")
		}
		if name == "isin_code" {
			value = strings.ToUpper(strings.TrimSpace(value))
		}
		if def.Required && strings.TrimSpace(value) == "" {
			return nil, nil, fmt.Errorf("missing input %s", name)
		}
		if def.Pattern != "" {
			rule, err := regexp.Compile(def.Pattern)
			if err != nil || !rule.MatchString(value) {
				return nil, nil, fmt.Errorf("input %s violates pattern", name)
			}
		}
		inputs[name] = value
	}
	date, err := ResolveLogicalDate(trigger, e.Timezone, inputs["run_date"])
	if err != nil {
		return nil, nil, err
	}
	jobs := make([]ResolvedJob, 0, len(e.Jobs))
	for _, job := range e.Jobs {
		resolved, err := resolveJob(job, inputs, date)
		if err != nil {
			return nil, nil, fmt.Errorf("job %s: %w", job.ID, err)
		}
		jobs = append(jobs, resolved)
	}
	return inputs, jobs, nil
}

// resolveJob expands declared variables and keeps path/query encoding independent.
func resolveJob(job Job, inputs map[string]string, date time.Time) (ResolvedJob, error) {
	values := make(map[string]string, len(inputs)+len(job.Request.Variables))
	for k, v := range inputs {
		values[k] = v
	}
	// Variables may refer to event inputs, not one another; this prevents cycles.
	for k, def := range job.Request.Variables {
		if _, exists := inputs[k]; exists {
			return ResolvedJob{}, fmt.Errorf("variable shadows input %s", k)
		}
		value, err := RenderTemplate(def.Template, inputs, date)
		if err != nil {
			return ResolvedJob{}, err
		}
		values[k] = value
	}
	u, err := url.Parse(job.Request.URLTemplate)
	if err != nil {
		return ResolvedJob{}, err
	}
	if err := validateHTTPS(job.Request.URLTemplate); err != nil {
		return ResolvedJob{}, err
	}
	parts := strings.Split(u.Path, "/")
	for i, part := range parts {
		parts[i], err = RenderTemplate(part, values, date)
		if err != nil {
			return ResolvedJob{}, err
		}
		if parts[i] == "." || parts[i] == ".." || strings.ContainsAny(parts[i], "/\\") {
			return ResolvedJob{}, fmt.Errorf("unsafe URL path segment")
		}
	}
	u.Path = strings.Join(parts, "/")
	u.RawPath = ""
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return ResolvedJob{}, err
	}
	for k, list := range query {
		if strings.ContainsAny(k, "{}") {
			return ResolvedJob{}, fmt.Errorf("query keys must be fixed")
		}
		for i, value := range list {
			list[i], err = RenderTemplate(value, values, date)
			if err != nil {
				return ResolvedJob{}, err
			}
		}
	}
	u.RawQuery = query.Encode()
	sourceFilename := path.Base(u.Path)
	if !safeFilename(sourceFilename, job.Response.Format) {
		sourceFilename = ""
	}
	format, name, member := job.Response.Format, job.Response.FilenameTemplate, ""
	if x := job.Response.Extraction; x != nil {
		format, name = x.Format, x.FilenameTemplate
		member, err = RenderTemplate(x.MemberPath, values, date)
		if err != nil {
			return ResolvedJob{}, err
		}
		if !safeMember(member) {
			return ResolvedJob{}, fmt.Errorf("unsafe ZIP member")
		}
		if name == "" {
			name = path.Base(member)
		}
	}
	if name == "" {
		name = sourceFilename
	}
	name, err = RenderTemplate(name, values, date)
	if err != nil {
		return ResolvedJob{}, err
	}
	if name != "" && !safeFilename(name, format) {
		return ResolvedJob{}, fmt.Errorf("unsafe output filename")
	}
	if strings.HasSuffix(strings.ToLower(u.Path), ".zip") && job.Response.Format != "zip" {
		return ResolvedJob{}, fmt.Errorf("ZIP URL requires extraction")
	}
	return ResolvedJob{ID: job.ID, URL: u.String(), SourceFilename: sourceFilename, Filename: name, MemberPath: member, Format: format}, nil
}

// safeMember rejects traversal, absolute paths and platform-specific separators.
func safeMember(value string) bool {
	return value != "" && value != "." && value != ".." && !strings.HasPrefix(value, "/") && !strings.HasPrefix(value, "../") && path.Clean(value) == value && !strings.ContainsAny(value, "\\:") && strings.IndexFunc(value, unicode.IsControl) < 0
}

// safeFilename requires a single safe filename with the declared content extension.
func safeFilename(value, format string) bool {
	return safeMember(value) && !strings.Contains(value, "/") && strings.EqualFold(path.Ext(value), "."+format)
}
