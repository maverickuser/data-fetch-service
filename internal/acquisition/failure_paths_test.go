package acquisition

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestGuardedClientRedirectAndDialPolicy(t *testing.T) {
	client := GuardedClient(resolverStub{addresses: []netip.Addr{netip.MustParseAddr("8.8.8.8")}}, &dialerStub{})
	transport := client.Transport.(*http.Transport)
	if transport.Proxy != nil || !transport.DisableCompression {
		t.Fatal("unsafe transport")
	}
	if _, err := transport.DialContext(context.Background(), "tcp", "example.com:443"); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		url   string
		hops  int
		valid bool
	}{{"https://example.com", 1, true}, {"http://example.com", 1, false}, {"https://example.com", 5, false}} {
		u, err := url.Parse(test.url)
		if err != nil {
			t.Fatal(err)
		}
		err = client.CheckRedirect(&http.Request{URL: u}, make([]*http.Request, test.hops))
		if (err == nil) != test.valid {
			t.Fatal(test, err)
		}
	}
	failure := errors.New("DNS or dial failed")
	for _, test := range []struct {
		target   string
		resolver resolverStub
		dialer   *dialerStub
	}{
		{"bad", resolverStub{}, &dialerStub{}},
		{"example.com:443", resolverStub{}, &dialerStub{}},
		{"example.com:443", resolverStub{err: failure}, &dialerStub{}},
		{"example.com:443", resolverStub{addresses: []netip.Addr{netip.MustParseAddr("8.8.8.8")}}, &dialerStub{err: failure}},
	} {
		if _, err := guardedDial(context.Background(), test.resolver, test.dialer, "tcp", test.target); err == nil {
			t.Fatal(test)
		}
	}
}

func TestDownloadSetupAndTransportFailures(t *testing.T) {
	limits := Limits{MaxDownloadBytes: 5, RequestTimeout: time.Second}
	client := clientFunc(func(*http.Request) (*http.Response, error) { return nil, io.ErrUnexpectedEOF })
	for _, test := range []struct {
		source string
		limits Limits
		budget *Budget
	}{
		{"https://example.com", Limits{}, NewBudget(5)},
		{"%", limits, NewBudget(5)},
		{"https://example.com", limits, NewBudget(5)},
	} {
		if _, _, err := DownloadSource(context.Background(), client, test.budget, test.limits, test.source, "csv", t.TempDir(), time.Now()); err == nil {
			t.Fatal(test)
		}
	}
	for _, test := range []struct {
		budget *Budget
		dir    string
	}{{NewBudget(1), t.TempDir()}, {NewBudget(5), t.TempDir() + "/missing"}} {
		body := &trackedBody{Reader: strings.NewReader("hello")}
		client := clientFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, ContentLength: 5, Header: http.Header{"Content-Type": {"text/csv"}}, Body: body}, nil
		})
		if _, _, err := DownloadSource(context.Background(), client, test.budget, limits, "https://example.com", "csv", test.dir, time.Now()); err == nil || !body.closed || test.budget.used != 0 {
			t.Fatal(err)
		}
	}
	f := &Failure{Code: "SOURCE_READ", Cause: io.ErrUnexpectedEOF}
	if f.Error() != "SOURCE_READ" || !errors.Is(f, io.ErrUnexpectedEOF) {
		t.Fatal(f)
	}
}

func TestResponseMediaAndBackoff(t *testing.T) {
	now := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		format, media string
		valid         bool
	}{{"zip", "application/zip", true}, {"zip", "application/x-zip-compressed", true}, {"json", "application/problem+json", true}, {"json", "", false}, {"xml", "application/octet-stream", false}, {"csv", "application/octet-stream", true}} {
		err := checkResponse(&http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {test.media}}}, test.format, now)
		if (err == nil) != test.valid {
			t.Fatal(test, err)
		}
	}
	for _, test := range []struct {
		value string
		want  time.Duration
	}{{"-1", 0}, {"1", time.Second}, {"bad", 0}, {now.Add(-time.Hour).Format(http.TimeFormat), 0}, {now.Add(10 * time.Second).Format(http.TimeFormat), 10 * time.Second}, {now.Add(time.Hour).Format(http.TimeFormat), time.Hour}} {
		if got := retryAfter(test.value, now); got != test.want {
			t.Fatal(test, got)
		}
	}
}

func TestUploadFailureAndCancellation(t *testing.T) {
	store := storageFunc(func(_ context.Context, _ string, r io.Reader) (Artifact, error) {
		_, err := io.Copy(io.Discard, r)
		return Artifact{}, err
	})
	limits := Limits{MaxExtractedBytes: 10, MaxTokenBytes: 10, MaxJSONDepth: 2}
	for _, test := range []struct {
		format string
		limits Limits
	}{{"json", Limits{}}, {"xml", limits}, {"json", limits}} {
		if _, err := UploadValidated(context.Background(), store, "key", test.format, io.NopCloser(strings.NewReader("{}")), test.limits); err == nil {
			t.Fatal(test)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := UploadValidated(ctx, store, "key", "json", io.NopCloser(strings.NewReader("{}")), limits); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestJSONTruncatedStructuresAndLateIOFailure(t *testing.T) {
	for _, raw := range []string{`{`, `[`, `{"a"`, `{"a":1`, `{"a":1,`, `[0,`, `{"a":"x"`, `{"a":1,"b"`} {
		if err := ValidateJSON(strings.NewReader(raw), 100, 10); err == nil {
			t.Fatal(raw)
		}
	}
	if err := ValidateJSON(io.MultiReader(strings.NewReader("0 "), brokenReader{}), 100, 10); err == nil {
		t.Fatal("late read failure ignored")
	}
	if _, err := filename("", "%", "", "job", "csv"); err == nil {
		t.Fatal("bad URL")
	}
	if _, err := filename("", "https://example.com", "", "../job", "csv"); err == nil {
		t.Fatal("unsafe fallback")
	}
}
