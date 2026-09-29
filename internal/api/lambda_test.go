package api

import (
	"context"
	"encoding/base64"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-lambda-go/events"
)

func TestLambdaRequestAndResponseConversion(t *testing.T) {
	event := events.APIGatewayV2HTTPRequest{RawPath: "/v1/test", RawQueryString: "x=a%2Fb", Body: base64.StdEncoding.EncodeToString([]byte("body")), IsBase64Encoded: true, Headers: map[string]string{"X-Test": "yes"}, Cookies: []string{"a=1", "b=2"}}
	event.RequestContext.HTTP.Method = "POST"
	adapter := Lambda{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
}
