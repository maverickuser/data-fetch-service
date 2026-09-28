package config

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

var placeholder = regexp.MustCompile(`\{([A-Za-z_][A-Za-z0-9_]*)(?::([A-Za-z0-9-]+))?\}`)

// RenderTemplate resolves the small, non-executable filename/URL template language.
func RenderTemplate(template string, values map[string]string, runDate time.Time) (string, error) {
	var firstErr error
	result := placeholder.ReplaceAllStringFunc(template, func(token string) string {
		if firstErr != nil {
			return token
		}
		match := placeholder.FindStringSubmatch(token)
		name, format := match[1], match[2]
		if name == "run_date" {
			if format == "ddMMyyyy" {
				return runDate.Format("02012006")
			}
			if format == "yyyyMMdd" {
				return runDate.Format("20060102")
			}
			if format == "yyyy-MM-dd" || format == "" {
				return runDate.Format("2006-01-02")
			}
			firstErr = fmt.Errorf("unsupported run_date format %q", format)
			return token
		}
		if format != "" {
			firstErr = fmt.Errorf("format is not supported for %s", name)
			return token
		}
		value, ok := values[name]
		if !ok || strings.TrimSpace(value) == "" {
			firstErr = fmt.Errorf("template variable %q is not provided", name)
			return token
		}
		return value
	})
	if firstErr != nil {
		return "", firstErr
	}
	if strings.Contains(result, "{") || strings.Contains(result, "}") {
		return "", fmt.Errorf("unresolved template expression")
	}
	return result, nil
}

// ResolveLogicalDate resolves an explicit ISO date or derives it from trigger time.
func ResolveLogicalDate(trigger time.Time, timezone, explicit string) (time.Time, error) {
	location, err := time.LoadLocation(timezone)
	if err != nil {
		return time.Time{}, fmt.Errorf("load timezone %q: %w", timezone, err)
	}
	if explicit != "" {
		date, err := time.ParseInLocation("2006-01-02", explicit, location)
		if err != nil {
			return time.Time{}, fmt.Errorf("parse run_date: %w", err)
		}
		return date, nil
	}
	local := trigger.In(location)
	return time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, location), nil
}
