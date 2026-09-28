package config

import (
	"testing"
	"time"
)

func TestRenderTemplateAndLogicalDate(t *testing.T) {
	date := time.Date(2026, 9, 21, 0, 0, 0, 0, time.FixedZone("IST", 19800))
	got, err := RenderTemplate("DEBTBHAVCOPY{run_date:ddMMyyyy}-{exchangeName}.zip", map[string]string{"exchangeName": "BSE"}, date)
	if err != nil || got != "DEBTBHAVCOPY21092026-BSE.zip" {
		t.Fatalf("template=%q error=%v", got, err)
	}
	resolved, err := ResolveLogicalDate(time.Date(2026, 9, 20, 19, 0, 0, 0, time.UTC), "Asia/Kolkata", "")
	if err != nil || resolved.Format("2006-01-02") != "2026-09-21" {
		t.Fatalf("date=%v error=%v", resolved, err)
	}
	explicit, err := ResolveLogicalDate(time.Now(), "Asia/Kolkata", "2026-01-01")
	if err != nil || explicit.Format("2006-01-02") != "2026-01-01" {
		t.Fatalf("explicit=%v error=%v", explicit, err)
	}
}

func TestRenderTemplateRejectsUnknownExpressions(t *testing.T) {
	date := time.Now()
	for _, input := range []string{"{missing}", "{run_date:bad}", "{exchangeName:bad}", "{unknown}", "literal {x"} {
		if _, err := RenderTemplate(input, map[string]string{"exchangeName": "BSE"}, date); err == nil {
			t.Fatalf("expected rejection for %s", input)
		}
	}
	if _, err := ResolveLogicalDate(time.Now(), "missing/zone", ""); err == nil {
		t.Fatal("expected timezone rejection")
	}
	if _, err := ResolveLogicalDate(time.Now(), "Asia/Kolkata", "bad"); err == nil {
		t.Fatal("expected date rejection")
	}
}
