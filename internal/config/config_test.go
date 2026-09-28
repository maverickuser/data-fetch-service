package config

import (
	"os"
	"testing"
	"time"
)

func production(t *testing.T) Config {
	t.Helper()
	base, err := os.ReadFile("../../config/events.yaml")
	if err != nil {
		t.Fatal(err)
	}
	overlay, err := os.ReadFile("../../config/environments/prod.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadEffective(base, overlay)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestProductionAndRevision(t *testing.T) {
	c := production(t)
	if len(c.Events) != 2 || c.Environment != "prod" {
		t.Fatal("missing production configuration")
	}
	if _, ok := c.Event("missing"); ok {
		t.Fatal("unexpected event")
	}
	first := c.Revision()
	c.DeploymentCommit = "new-commit"
	if first == c.Revision() || len(first) != 71 {
		t.Fatal("revision must include commit")
	}
}

func TestInvalidConfiguration(t *testing.T) {
	cases := map[string]func(*Config){
		"schema":            func(c *Config) { c.SchemaVersion = 2 },
		"environment":       func(c *Config) { c.Environment = "dev" },
		"prefix":            func(c *Config) { c.ResourcePrefix = "other" },
		"empty":             func(c *Config) { c.Events = nil },
		"retry":             func(c *Config) { c.Defaults.SourceMaxAttempts = 0 },
		"resource":          func(c *Config) { c.Defaults.MaxDownloadBytes = 0 },
		"processor timeout": func(c *Config) { c.Processor.RequestTimeoutSeconds = 0 },
		"processor URL":     func(c *Config) { c.Processor.URL = "http://example.org" },
		"duplicate event":   func(c *Config) { c.Events = append(c.Events, c.Events[0]) },
		"dataset":           func(c *Config) { c.Events[0].DatasetType = "bad" },
		"jobs":              func(c *Config) { c.Events[0].Jobs = nil },
		"timezone":          func(c *Config) { c.Events[0].Timezone = "unknown/zone" },
		"input":             func(c *Config) { c.Events[0].Inputs["bad"] = InputDef{Type: "object"} },
		"pattern":           func(c *Config) { c.Events[0].Inputs["bad"] = InputDef{Type: "string", Pattern: "["} },
		"expression":        func(c *Config) { c.Events[0].Schedule.Expression = "" },
		"schedule zone":     func(c *Config) { c.Events[0].Schedule.Timezone = "unknown/zone" },
		"schedule input":    func(c *Config) { c.Events[0].Schedule.Inputs = nil },
		"duplicate job":     func(c *Config) { c.Events[0].Jobs = append(c.Events[0].Jobs, c.Events[0].Jobs[0]) },
		"method":            func(c *Config) { c.Events[0].Jobs[0].Request.Method = "POST" },
		"URL":               func(c *Config) { c.Events[0].Jobs[0].Request.URLTemplate = "https://%zz" },
		"relative URL":      func(c *Config) { c.Events[0].Jobs[0].Request.URLTemplate = "/relative" },
		"format":            func(c *Config) { c.Events[0].Jobs[0].Response.Format = "xml" },
		"filename":          func(c *Config) { c.Events[0].Jobs[0].Response.Extraction.FilenameTemplate = "" },
		"member":            func(c *Config) { c.Events[0].Jobs[0].Response.Extraction.MemberPath = "" },
		"extraction":        func(c *Config) { c.Events[0].Jobs[0].Response.Format = "json" },
		"member format":     func(c *Config) { c.Events[0].Jobs[0].Response.Extraction.Format = "xml" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			c := production(t)
			mutate(&c)
			if err := c.Validate(); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}

func TestYAMLBoundaries(t *testing.T) {
	for _, input := range []string{"", "null", "[", "x: 1\n---\nx: 2", "schema_version: 1\nschema_version: 2", "unknown: true", "schema_version: 2", "x: .nan"} {
		if _, err := Load([]byte(input)); err == nil {
			t.Fatalf("accepted %q", input)
		}
		if _, err := LoadEffective([]byte(input), []byte("environment: prod")); err == nil {
			t.Fatalf("accepted base %q", input)
		}
	}
	if _, err := LoadEffective([]byte("schema_version: 1"), []byte("[")); err == nil {
		t.Fatal("bad overlay")
	}
	base, err := os.ReadFile("../../config/events.yaml")
	if err != nil {
		t.Fatal(err)
	}
	c, err := LoadEffective(base, []byte("processor:\n  max_attempts: 4"))
	if err != nil || c.Processor.MaxAttempts != 4 {
		t.Fatal("nested merge", err)
	}
	if _, err := LoadEffective(base, []byte("processor:\n  extra: true")); err == nil {
		t.Fatal("unknown overlay field")
	}
}

func TestResolvedBSEAndNSDL(t *testing.T) {
	c := production(t)
	trigger := time.Date(2026, 9, 21, 14, 30, 0, 0, time.UTC)
	bse, _ := c.Event("daily-bhavcopy")
	inputs, jobs, err := bse.Resolve(trigger, map[string]any{"exchangeName": "BSE"})
	if err != nil {
		t.Fatal(err)
	}
	if inputs["run_date"] != "2026-09-21" || jobs[0].Filename != "BSE_fgroup21092026.csv" || jobs[0].URL != "https://www.bseindia.com/download/BhavCopy/Debt/DEBTBHAVCOPY21092026.zip" {
		t.Fatalf("wrong BSE result %+v %+v", inputs, jobs)
	}
	nsdl, _ := c.Event("nsdl-bond-data")
	inputs, jobs, err = nsdl.Resolve(trigger, map[string]any{"isin_code": " ine121a07qy9 "})
	if err != nil || len(jobs) != 6 || inputs["isin_code"] != "INE121A07QY9" {
		t.Fatalf("wrong NSDL result %v", err)
	}
	for _, j := range jobs {
		if j.Filename != "INE121A07QY9_"+j.ID+".json" {
			t.Fatal(j.Filename)
		}
	}
}

func TestResolveRejectsUnsafeInputs(t *testing.T) {
	for _, v := range []map[string]any{{}, {"exchangeName": 3}, {"exchangeName": "../bad"}, {"unknown": "x"}, {"exchangeName": "BSE", "run_date": "bad"}} {
		e := production(t).Events[0]
		if _, _, err := e.Resolve(time.Now(), v); err == nil {
			t.Fatal(v)
		}
	}
	e := production(t).Events[0]
	e.Enabled = false
	if _, _, err := e.Resolve(time.Now(), nil); err == nil {
		t.Fatal("disabled event")
	}
}
