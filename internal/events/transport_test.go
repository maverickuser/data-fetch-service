package events

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/config"
)

const cloud = `{"specversion":"1.0","id":"evt-1","source":"urn:test","type":"com.bondplatform.data.pull.requested.v1","time":"2026-09-21T14:30:00Z","datacontenttype":"application/json","data":{"schema_version":1,"event_type":"daily-bhavcopy","inputs":{"exchangeName":"BSE"}}}`
const scheduled = `{"id":"transport-id","source":"aws.events","detail-type":"Scheduled Event","time":"2026-09-21T14:30:00Z","resources":["arn:aws:events:ap-south-1:123456789012:rule/bse"],"detail":{}}`
const native = `{"id":"native-1","account":"123","region":"ap-south-1","source":"bond-producer","detail-type":"ISIN","time":"2026-09-21T14:30:00Z","detail":{"isin":"INE121A07QY9"}}`

func TestNativeAndSQSTransports(t *testing.T) {
	mappings := []RuleMapping{{RuleARN: "arn:aws:events:ap-south-1:123456789012:rule/bse", EventType: "daily-bhavcopy", Inputs: map[string]any{"exchangeName": "BSE"}}, {Account: "123", Region: "ap-south-1", Source: "bond-producer", DetailType: "ISIN", EventType: "nsdl-bond-data", DetailFields: map[string]string{"isin_code": "isin"}}}
	for _, raw := range []string{cloud, scheduled, native} {
		e, err := ParseTransport([]byte(raw), mappings)
		if err != nil {
			t.Fatal(err)
		}
		if len(e.Original) == 0 || e.EventID == "transport-id" {
			t.Fatal("lost original or incorrect scheduled identity")
		}
	}
	payload, err := json.Marshal(map[string]any{"Records": []map[string]string{{"messageId": "one", "body": cloud}, {"messageId": "two", "body": "invalid"}, {"body": cloud}}})
	if err != nil {
		t.Fatal(err)
	}
	records, err := ParseSQS(payload, mappings)
	if err != nil || len(records) != 3 || records[0].Err != nil || records[1].Err == nil || records[2].Err == nil {
		t.Fatalf("bad partial batch %+v %v", records, err)
	}
	for _, raw := range []string{"{", "{}"} {
		if _, err := ParseSQS([]byte(raw), nil); err == nil {
			t.Fatal("invalid SQS")
		}
	}
	for _, raw := range []string{"{", "{}", `{"time":3}`, native} {
		if _, err := ParseTransport([]byte(raw), nil); err == nil {
			t.Fatal("invalid native")
		}
	}
	if _, err := ParseTransport([]byte(scheduled), append(mappings, mappings[0])); err == nil {
		t.Fatal("ambiguous mapping accepted")
	}
	mappings[1].DetailFields["isin_code"] = "missing"
	if _, err := ParseTransport([]byte(native), mappings); err == nil {
		t.Fatal("missing detail accepted")
	}
}

func eventConfig(t *testing.T) config.Config {
	t.Helper()
	b, err := os.ReadFile("../../config/events.yaml")
	if err != nil {
		t.Fatal(err)
	}
	c, err := config.Load(b)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestSnapshotPreservesLogicalDateAndIdentity(t *testing.T) {
	c := eventConfig(t)
	e, err := ParseCloudEvent([]byte(cloud))
	if err != nil {
		t.Fatal(err)
	}
	data, err := BuildSnapshot(c, e, "run_1")
	if err != nil {
		t.Fatal(err)
	}
	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Jobs[0].Filename != "BSE_fgroup21092026.csv" {
		t.Fatal(snapshot.Jobs)
	}
	e.Inputs["exchangeName"] = "changed"
	c.Events[0].Jobs[0].Request.URLTemplate = "changed"
	if snapshot.Inputs["exchangeName"] != "BSE" {
		t.Fatal("mutable snapshot")
	}
	e, err = ParseCloudEvent([]byte(cloud))
	if err != nil {
		t.Fatal(err)
	}
	e.Original = json.RawMessage(`{}`)
	second, err := BuildSnapshot(eventConfig(t), e, "run_2")
	if err != nil {
		t.Fatal(err)
	}
	var replay Snapshot
	if err := json.Unmarshal(second, &replay); err != nil {
		t.Fatal(err)
	}
	if snapshot.ExecutionKey != replay.ExecutionKey || snapshot.RequestKey != replay.RequestKey || snapshot.PayloadHash != replay.PayloadHash {
		t.Fatal("transport/run changed identities")
	}
	e.Inputs["run_date"] = "2026-09-20"
	second, err = BuildSnapshot(eventConfig(t), e, "run_3")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(second, &replay); err != nil {
		t.Fatal(err)
	}
	if snapshot.PayloadHash == replay.PayloadHash || snapshot.ExecutionKey == replay.ExecutionKey {
		t.Fatal("changed date must conflict")
	}
}

func TestSnapshotInvalidBoundaries(t *testing.T) {
	e, err := ParseCloudEvent([]byte(cloud))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := BuildSnapshot(eventConfig(t), e, ""); err == nil {
		t.Fatal("empty run")
	}
	c := eventConfig(t)
	c.SchemaVersion = 2
	if _, err := BuildSnapshot(c, e, "run"); err == nil {
		t.Fatal("bad config")
	}
	for _, mutate := range []func(*Normalized){func(e *Normalized) { e.EventType = "missing" }, func(e *Normalized) { e.Inputs = nil }, func(e *Normalized) { e.Source = "" }, func(e *Normalized) { e.OccurredAt = time.Time{} }, func(e *Normalized) { e.Original = json.RawMessage(`{`) }} {
		copy := e
		mutate(&copy)
		if _, err := BuildSnapshot(eventConfig(t), copy, "run"); err == nil {
			t.Fatal("bad event")
		}
	}
	if _, err := digest(make(chan int)); err == nil {
		t.Fatal("unsupported hash input")
	}
	bad := []byte(cloud)
	var obj map[string]any
	if err := json.Unmarshal(bad, &obj); err != nil {
		t.Fatal(err)
	}
	obj["source"] = "http://%zz"
	bad, err = json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseCloudEvent(bad); err == nil {
		t.Fatal("invalid source")
	}
}
