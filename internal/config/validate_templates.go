package config

import (
	"fmt"
	"strings"
	"time"
)

// validateTemplates rejects undeclared expressions before accepting configuration.
func validateTemplates(e EventDef) error {
	inputs := map[string]bool{}
	for name := range e.Inputs {
		inputs[name] = true
	}
	for _, job := range e.Jobs {
		values := map[string]bool{}
		for name := range inputs {
			values[name] = true
		}
		for name, variable := range job.Request.Variables {
			if inputs[name] || name == "run_date" || !safeName.MatchString(name) {
				return fmt.Errorf("invalid variable %s", name)
			}
			if err := validateExpression(variable.Template, inputs); err != nil {
				return err
			}
			values[name] = true
		}
		for _, value := range []string{job.Request.URLTemplate, job.Response.FilenameTemplate} {
			if err := validateExpression(value, values); err != nil {
				return err
			}
		}
		if x := job.Response.Extraction; x != nil {
			for _, value := range []string{x.MemberPath, x.FilenameTemplate} {
				if err := validateExpression(value, values); err != nil {
					return err
				}
				if value != "" && !safeMember(placeholder.ReplaceAllString(value, "x")) {
					return fmt.Errorf("unsafe ZIP/output template")
				}
			}
			if x.FilenameTemplate != "" && !safeFilename(placeholder.ReplaceAllString(x.FilenameTemplate, "x"), x.Format) {
				return fmt.Errorf("invalid extraction filename or extension")
			}
		}
		if job.Response.FilenameTemplate != "" && !safeFilename(placeholder.ReplaceAllString(job.Response.FilenameTemplate, "x"), job.Response.Format) {
			return fmt.Errorf("unsafe output template")
		}
	}
	if e.Schedule != nil && e.Schedule.Enabled {
		v := map[string]any{}
		for k, s := range e.Schedule.Inputs {
			v[k] = s
		}
		copy := e
		copy.Enabled = true
		if _, _, err := copy.Resolve(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), v); err != nil {
			return fmt.Errorf("invalid scheduled inputs: %w", err)
		}
	}
	return nil
}

// validateExpression permits only declared substitutions and supported date formats.
func validateExpression(value string, declared map[string]bool) error {
	for _, m := range placeholder.FindAllStringSubmatch(value, -1) {
		if !declared[m[1]] {
			return fmt.Errorf("undeclared template input %s", m[1])
		}
		if m[2] != "" && (m[1] != "run_date" || (m[2] != "ddMMyyyy" && m[2] != "yyyyMMdd" && m[2] != "yyyy-MM-dd")) {
			return fmt.Errorf("invalid template format %s", m[2])
		}
	}
	if strings.ContainsAny(placeholder.ReplaceAllString(value, ""), "{}") {
		return fmt.Errorf("invalid template expression")
	}
	return nil
}
