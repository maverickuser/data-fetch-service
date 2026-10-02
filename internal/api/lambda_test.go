package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/maverickuser/data-fetch-service/internal/telemetry"
)

func TestLambdaRequestAndResponseConversion(t *testing.T) {
	event := events.APIGatewayV2HTTPRequest{RawPath: "/v1/test", RawQueryString: "x=a%2Fb", Body: base64.StdEncoding.EncodeToString([]byte("body")), IsBase64Encoded: true, Headers: map[string]string{"X-Test": "yes"}, Cookies: []string{"a=1", "b=2"}}
	event.RequestContext.HTTP.Method = "POST"
	event.RequestContext.RequestID = "gateway-1"
	var logs bytes.Buffer
	adapter := Lambda{Telemetry: &telemetry.JSON{Output: &logs}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("x") != "a/b" || r.Header.Get("Cookie") != "a=1; b=2" || r.Header.Get("X-Test") != "yes" {
			t.Fatal(r)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(202)
		w.WriteHeader(500)
		if _, err := w.Write([]byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	})}
	response, err := adapter.Handle(context.Background(), event)
	if err != nil || response.StatusCode != 202 || response.Body != "{}" {
		t.Fatal(response, err)
	}
	if !strings.Contains(logs.String(), `"request_id":"gateway-1"`) || !strings.Contains(logs.String(), `"outcome":"completed"`) {
		t.Fatal(logs.String())
	}
	for _, mutate := range []func(*events.APIGatewayV2HTTPRequest){func(e *events.APIGatewayV2HTTPRequest) { e.Body = strings.Repeat("x", (128<<10)+1) }, func(e *events.APIGatewayV2HTTPRequest) { e.Body = "%%%" }, func(e *events.APIGatewayV2HTTPRequest) { e.RawPath = "relative" }, func(e *events.APIGatewayV2HTTPRequest) { e.RequestContext.HTTP.Method = "bad method" }} {
		copy := event
		mutate(&copy)
		response, err := adapter.Handle(context.Background(), copy)
		if err != nil || response.StatusCode < 400 {
			t.Fatal(response, err)
		}
	}
	for _, mode := range []string{"empty", "implicit", "overflow", "escaped overflow"} {
		adapter.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if mode == "escaped overflow" {
				if _, err := w.Write([]byte(strings.Repeat("\"", 4<<20))); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "implicit" {
				if _, err := w.Write([]byte("{}")); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "overflow" {
				if _, err := w.Write([]byte(strings.Repeat("x", (5<<20)+1))); err == nil {
					t.Fatal("unbounded response")
				}
			}
		})
		event.IsBase64Encoded = false
		event.Body = ""
		response, err := adapter.Handle(context.Background(), event)
		if err != nil {
			t.Fatal(err)
		}
		expected := 200
		if mode == "overflow" || mode == "escaped overflow" {
			expected = 502
		}
		if response.StatusCode != expected {
			t.Fatal(response)
		}
	}
	if !strings.Contains(logs.String(), `"outcome":"rejected"`) || !strings.Contains(logs.String(), `"outcome":"retry"`) {
		t.Fatal(logs.String())
	}
}

func TestLambdaTelemetryUsesDurableRunIdentity(t *testing.T) {
	var logs bytes.Buffer
	event := events.APIGatewayV2HTTPRequest{RawPath: "/v1/events/daily/runs"}
	event.RequestContext.HTTP.Method = "POST"
	adapter := Lambda{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"run_id":"run_123"}`))
	}), Telemetry: &telemetry.JSON{Output: &logs}}
	if response, err := adapter.Handle(context.Background(), event); err != nil || response.StatusCode != 200 {
		t.Fatal(response, err)
	}
	if !strings.Contains(logs.String(), `"correlation_id":"run_123"`) {
		t.Fatal(logs.String())
	}
	logs.Reset()
	event.RawPath = "/v1/runs/run_456/history"
	adapter.Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(404) })
	if response, err := adapter.Handle(context.Background(), event); err != nil || response.StatusCode != 404 {
		t.Fatal(response, err)
	}
	if !strings.Contains(logs.String(), `"run_id":"run_456"`) {
		t.Fatal(logs.String())
	}
}
