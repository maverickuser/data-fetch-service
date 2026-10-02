package config

import (
	"testing"
	"time"
)

func TestResolveRejectsEncodedPathSeparators(t *testing.T) {
	for _, source := range []string{"https://example.com/a%2Fb.json", "https://example.com/a%5Cb.json"} {
		if _, err := resolveJob(Job{Request: Request{URLTemplate: source}, Response: Response{Format: "json"}}, nil, time.Now()); err == nil {
			t.Fatal("encoded separator accepted", source)
		}
	}
	job, err := resolveJob(Job{Request: Request{URLTemplate: "https://example.com/a%252Fb.json"}, Response: Response{Format: "json"}}, nil, time.Now())
	if err != nil || job.SourceFilename != "a%2Fb.json" {
		t.Fatal(job, err)
	}
}
