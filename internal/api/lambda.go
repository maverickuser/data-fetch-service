package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/maverickuser/data-fetch-service/internal/telemetry"
)

// Lambda converts API Gateway HTTP API events into the same tested HTTP routes.
type Lambda struct {
	Handler   http.Handler
	Telemetry telemetry.Recorder
}

// Handle preserves encoded paths/query values and returns JSON responses to API Gateway.
func (l Lambda) Handle(ctx context.Context, event events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	response, err := l.handle(ctx, event)
	if l.Telemetry != nil {
		outcome := "completed"
		code := ""
		if response.StatusCode >= 500 || err != nil {
			outcome, code = "retry", "SERVER_ERROR"
		} else if response.StatusCode >= 400 {
			outcome, code = "rejected", "CLIENT_ERROR"
		}
		var result struct {
			RunID string `json:"run_id"`
		}
		if response.StatusCode < 400 {
			_ = json.Unmarshal([]byte(response.Body), &result)
		}
		if result.RunID == "" {
			parts := strings.Split(event.RawPath, "/")
			if len(parts) >= 4 && parts[1] == "v1" && parts[2] == "runs" {
				result.RunID = parts[3]
			}
		}
		l.Telemetry.Record(telemetry.Entry{Component: "api", Operation: "request", Outcome: outcome, RunID: result.RunID, RequestID: event.RequestContext.RequestID, ErrorCode: code})
	}
	return response, err
}

// handle preserves the existing API Gateway conversion and bounded response contract.
func (l Lambda) handle(ctx context.Context, event events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	body := event.Body
	if len(body) > 128<<10 {
		return gatewayError(413), nil
	}
	if event.IsBase64Encoded {
		decoded, err := base64.StdEncoding.DecodeString(body)
		if err != nil {
			return gatewayError(400), nil
		}
		body = string(decoded)
	}
	if !strings.HasPrefix(event.RawPath, "/") {
		return gatewayError(400), nil
	}
	target := "https://data-fetch-service" + event.RawPath
	if event.RawQueryString != "" {
		target += "?" + event.RawQueryString
	}
	request, err := http.NewRequestWithContext(ctx, event.RequestContext.HTTP.Method, target, strings.NewReader(body))
	if err != nil {
		return gatewayError(400), nil
	}
	for key, value := range event.Headers {
		request.Header.Set(key, value)
	}
	if len(event.Cookies) > 0 {
		request.Header.Set("Cookie", strings.Join(event.Cookies, "; "))
	}
	writer := &gatewayWriter{headers: make(http.Header)}
	l.Handler.ServeHTTP(writer, request)
	if writer.overflow {
		return gatewayError(502), nil
	}
	headers := make(map[string]string)
	for key, value := range writer.headers {
		headers[key] = strings.Join(value, ",")
	}
	if writer.status == 0 {
		writer.status = 200
	}
	response := events.APIGatewayV2HTTPResponse{StatusCode: writer.status, Headers: headers, Body: writer.body.String()}
	encoded, err := json.Marshal(response)
	if err != nil || len(encoded) > 6<<20 {
		return gatewayError(502), nil
	}
	return response, nil
}

// gatewayError emits a bounded adapter-level response before routing is possible.
func gatewayError(status int) events.APIGatewayV2HTTPResponse {
	return events.APIGatewayV2HTTPResponse{StatusCode: status, Headers: map[string]string{"Content-Type": "application/json"}, Body: `{"error":"INVALID_GATEWAY_REQUEST_OR_RESPONSE"}`}
}

type gatewayWriter struct {
	headers  http.Header
	status   int
	body     bytes.Buffer
	overflow bool
}

// Header exposes response headers to the HTTP router.
func (w *gatewayWriter) Header() http.Header { return w.headers }

// WriteHeader retains only the first HTTP status.
func (w *gatewayWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

// Write bounds the Lambda response below API Gateway's synchronous payload limit.
func (w *gatewayWriter) Write(data []byte) (int, error) {
	if w.body.Len()+len(data) > 5<<20 {
		w.overflow = true
		return 0, fmt.Errorf("response exceeds Lambda budget")
	}
	if w.status == 0 {
		w.status = 200
	}
	return w.body.Write(data)
}
