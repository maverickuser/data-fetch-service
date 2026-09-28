package config

import (
	"net/url"
	"testing"
	"time"
)

func TestURLSubstitutionCannotInjectQueryParameters(t *testing.T) {
	job := Job{Request: Request{URLTemplate: "https://example.org/data?isin={isin}"}, Response: Response{Format: "json", FilenameTemplate: "data.json"}}
	result, err := resolveJob(job, map[string]string{"isin": "A&B=admin #fragment"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(result.URL)
	if err != nil {
		t.Fatal(err)
	}
	if u.Query().Get("isin") != "A&B=admin #fragment" || len(u.Query()) != 1 || u.Fragment != "" {
		t.Fatal(result.URL)
	}
}

func TestUnsafeJobResolution(t *testing.T) {
	cases := map[string]func(*Job){
		"shadow":              func(j *Job) { j.Request.Variables = map[string]Variable{"input": {Template: "x"}} },
		"missing variable":    func(j *Job) { j.Request.Variables = map[string]Variable{"other": {Template: "{missing}"}} },
		"invalid URL":         func(j *Job) { j.Request.URLTemplate = "https://%zz" },
		"insecure URL":        func(j *Job) { j.Request.URLTemplate = "http://example.org/data" },
		"missing path input":  func(j *Job) { j.Request.URLTemplate = "https://example.org/{missing}" },
		"traversal":           func(j *Job) { j.Request.URLTemplate = "https://example.org/{input}" },
		"bad query":           func(j *Job) { j.Request.URLTemplate = "https://example.org/data?a=%zz" },
		"dynamic query key":   func(j *Job) { j.Request.URLTemplate = "https://example.org/data?{input}=a" },
		"missing query value": func(j *Job) { j.Request.URLTemplate = "https://example.org/data?a={missing}" },
		"missing member":      func(j *Job) { j.Response.Extraction = &Extraction{MemberPath: "{missing}"} },
		"unsafe member":       func(j *Job) { j.Response.Extraction = &Extraction{MemberPath: "../data.csv"} },
		"missing filename":    func(j *Job) { j.Response.FilenameTemplate = "{missing}.json" },
		"unsafe filename":     func(j *Job) { j.Response.FilenameTemplate = "{input}.json" },
		"unextracted ZIP":     func(j *Job) { j.Request.URLTemplate = "https://example.org/data.zip" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			job := Job{Request: Request{URLTemplate: "https://example.org/data"}, Response: Response{Format: "json", FilenameTemplate: "data.json"}}
			mutate(&job)
			if _, err := resolveJob(job, map[string]string{"input": "../escape"}, time.Now()); err == nil {
				t.Fatal("unsafe job accepted")
			}
		})
	}
}

func TestInvalidTemplatesRejectedBeforeAdmission(t *testing.T) {
	cases := map[string]func(*EventDef){
		"variable shadows input": func(e *EventDef) { e.Jobs[0].Request.Variables = map[string]Variable{"exchangeName": {Template: "x"}} },
		"variable cycle":         func(e *EventDef) { e.Jobs[0].Request.Variables = map[string]Variable{"a": {Template: "{a}"}} },
		"undeclared URL":         func(e *EventDef) { e.Jobs[0].Request.URLTemplate = "https://example.org/{unknown}" },
		"bad format":             func(e *EventDef) { e.Jobs[0].Response.Extraction.MemberPath = "{run_date:unknown}.csv" },
		"bad expression":         func(e *EventDef) { e.Jobs[0].Response.Extraction.MemberPath = "{run_date" },
		"member traversal":       func(e *EventDef) { e.Jobs[0].Response.Extraction.MemberPath = "../data.csv" },
		"filename traversal":     func(e *EventDef) { e.Jobs[0].Response.FilenameTemplate = "../data.csv" },
		"schedule pattern":       func(e *EventDef) { e.Schedule.Inputs["exchangeName"] = "../escape" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			c := production(t)
			mutate(&c.Events[0])
			if err := c.Validate(); err == nil {
				t.Fatal("unsafe template accepted")
			}
		})
	}
}

func TestDateFormatsAndInvalidTimezone(t *testing.T) {
	date := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	value, err := RenderTemplate("{run_date:yyyyMMdd}", nil, date)
	if err != nil || value != "20260921" {
		t.Fatal(value, err)
	}
	if _, err := RenderTemplate("{missing}{also_missing}", nil, date); err == nil {
		t.Fatal("missing substitutions accepted")
	}
	e := production(t).Events[1]
	e.Timezone = "unknown/zone"
	if _, _, err := e.Resolve(date, map[string]any{"isin_code": "INE121A07QY9"}); err == nil {
		t.Fatal("invalid zone accepted")
	}
	e = production(t).Events[0]
	e.Jobs[0].Response.Extraction.FilenameTemplate = "../file.csv"
	if _, _, err := e.Resolve(date, map[string]any{"exchangeName": "BSE"}); err == nil {
		t.Fatal("invalid filename accepted")
	}
}

func TestReviewDateAndFilenameRegressions(t *testing.T) {
	date := time.Date(2026, 9, 21, 14, 30, 0, 0, time.UTC)
	e := production(t).Events[0]
	for _, value := range []string{"trigger_date", ""} {
		if _, _, err := e.Resolve(date, map[string]any{"exchangeName": "BSE", "run_date": value}); err == nil {
			t.Fatal("non-ISO date accepted")
		}
	}
	e.Inputs["run_date"] = InputDef{Type: "date", Required: true}
	if _, _, err := e.Resolve(date, map[string]any{"exchangeName": "BSE"}); err == nil {
		t.Fatal("missing required date accepted")
	}
	for _, name := range []string{"{isin_code}.csv", "dir/{isin_code}.json", "{isin_code}\t.json", "{isin_code}\u0085.json"} {
		c := production(t)
		c.Events[1].Jobs[0].Response.FilenameTemplate = name
		if err := c.Validate(); err == nil {
			t.Fatalf("invalid filename %q accepted", name)
		}
	}
	c := production(t)
	c.Events[0].Jobs[0].Response.Extraction.FilenameTemplate = "dir/file.csv"
	if err := c.Validate(); err == nil {
		t.Fatal("extraction subdirectory accepted")
	}
}

func TestOptionalFilenamePrecedence(t *testing.T) {
	c := production(t)
	e := &c.Events[0]
	e.Jobs[0].Response.Extraction.FilenameTemplate = ""
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	_, jobs, err := e.Resolve(time.Date(2026, 9, 21, 14, 30, 0, 0, time.UTC), map[string]any{"exchangeName": "BSE"})
	if err != nil || jobs[0].Filename != "fgroup21092026.csv" {
		t.Fatal(jobs, err)
	}
	for _, tc := range []struct{ url, name string }{
		{"https://example.org/Source%20File.CSV?date=1", "Source File.CSV"},
		{"https://example.org/api", ""},
	} {
		job := Job{ID: "prices", Request: Request{URLTemplate: tc.url}, Response: Response{Format: "csv"}}
		result, err := resolveJob(job, nil, time.Now())
		if err != nil || result.Filename != tc.name {
			t.Fatal(result, err)
		}
	}
}
