package events

import "testing"

func TestParseCloudEvent(t *testing.T) {
	event, err := ParseCloudEvent([]byte(`{"specversion":"1.0","id":"evt-1","source":"urn:test","type":"com.bondplatform.data.pull.requested.v1","time":"2026-09-27T14:30:00Z","datacontenttype":"application/json","data":{"schema_version":1,"event_type":"daily-bhavcopy","inputs":{"exchangeName":"BSE"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	if event.EventID != "evt-1" || event.EventType != "daily-bhavcopy" || event.Inputs["exchangeName"] != "BSE" {
		t.Fatalf("unexpected event: %+v", event)
	}
}

func TestParseCloudEventRejectsInvalidEnvelope(t *testing.T) {
	cases := []string{
		"{",
		`{"specversion":"1.0","source":"urn:test","type":"com.bondplatform.data.pull.requested.v1","time":"2026-09-27T14:30:00Z","datacontenttype":"application/json","data":{}}`,
		`{"specversion":"1.0","id":"x","source":"urn:test","type":"com.bondplatform.data.pull.requested.v1","time":"2026-09-27T14:30:00Z","datacontenttype":"text/plain","data":{}}`,
		`{"specversion":"1.0","id":"x","source":"urn:test","type":"com.bondplatform.data.pull.requested.v1","datacontenttype":"application/json","data":{}}`,
		`{"specversion":"1.0","id":"x","source":"urn:test","type":"com.bondplatform.data.pull.requested.v1","time":"2026-09-27T14:30:00Z","datacontenttype":"application/json","data":"bad"}`,
		`{"specversion":"1.0","id":"x","source":"urn:test","type":"com.bondplatform.data.pull.requested.v1","time":"2026-09-27T14:30:00Z","datacontenttype":"application/json","data":{"schema_version":2,"event_type":"x","inputs":{}}}`,
	}
	for _, payload := range cases {
		if _, err := ParseCloudEvent([]byte(payload)); err == nil {
			t.Fatal("expected invalid envelope")
		}
	}
}

func TestValidateInputString(t *testing.T) {
	if got, err := ValidateInputString(map[string]any{"isin_code": "INE121A07QY9"}, "isin_code"); err != nil || got != "INE121A07QY9" {
		t.Fatalf("unexpected result %q/%v", got, err)
	}
	for _, inputs := range []map[string]any{{}, {"x": 3}, {"x": "  "}} {
		if _, err := ValidateInputString(inputs, "x"); err == nil {
			t.Fatal("expected invalid string")
		}
	}
}
