package acquisition

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strings"
	"testing"
)

func TestValidationRegressions(t *testing.T) {
	if err := ValidateCSV(strings.NewReader(strings.Repeat(" ", 600)+"<html>error</html>"), 1024); err == nil {
		t.Fatal("HTML beyond sniff window accepted")
	}
	for _, input := range []string{`[[]]`, `[[0]]`, `{"a":{}}`} {
		if err := ValidateJSON(strings.NewReader(input), 100, 1); err == nil {
			t.Fatal("depth accepted", input)
		}
	}
	for _, input := range []string{`[]`, `[0]`, `{"a":0}`} {
		if err := ValidateJSON(strings.NewReader(input), 100, 1); err != nil {
			t.Fatal(input, err)
		}
	}
	if err := ValidateCSV(strings.NewReader("\xef\xbb\xbf<html>error</html>"), 100); err == nil {
		t.Fatal("BOM HTML accepted")
	}
	if err := ValidateCSV(strings.NewReader("\xef\xbb\xbf\"a\",b\n1,2\n"), 100); err != nil {
		t.Fatal(err)
	}
}

func TestFilenamePrecedenceAndDecodedSegments(t *testing.T) {
	for _, test := range []struct{ explicit, url, header, want string }{
		{"chosen.csv", "https://example.com/url.csv", "", "chosen.csv"},
		{"", "https://example.com/a%20b.csv", "attachment; filename=header.csv", "a b.csv"},
		{"", "https://example.com/a%2Fb.csv", "attachment; filename=header.csv", "header.csv"},
		{"", "https://example.com/a%5Cb.csv", "", "job.csv"},
		{"", "https://example.com/a%252Fb.csv", "", "a%2Fb.csv"},
		{"", "https://example.com/api", "attachment; filename=old.csv; filename*=UTF-8''new.csv", "new.csv"},
		{"", "https://example.com/api", "attachment; filename=../bad.csv", "job.csv"},
	} {
		got, err := filename(test.explicit, test.url, test.header, "job", "csv")
		if err != nil || got != test.want {
			t.Fatal(test, got, err)
		}
	}
	if _, err := filename("../bad.csv", "https://example.com/a.csv", "", "job", "csv"); err == nil {
		t.Fatal("unsafe explicit name")
	}
}

type resolverStub struct {
	addresses []netip.Addr
	err       error
}

func (r resolverStub) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	return r.addresses, r.err
}

type dialerStub struct {
	targets []string
	err     error
}

func (d *dialerStub) DialContext(_ context.Context, _, target string) (net.Conn, error) {
	d.targets = append(d.targets, target)
	return nil, d.err
}

func TestGuardedDialRejectsWholeMixedAnswer(t *testing.T) {
	for _, address := range []string{"127.0.0.1", "10.0.0.1", "169.254.169.254", "192.88.99.2", "3fff::1", "::ffff:127.0.0.1", "2002::1", "64:ff9b::1"} {
		dialer := &dialerStub{}
		_, err := guardedDial(context.Background(), resolverStub{addresses: []netip.Addr{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr(address)}}, dialer, "tcp", "example.com:443")
		if !errors.Is(err, ErrUnsafeDestination) || len(dialer.targets) != 0 {
			t.Fatal(address, err, dialer.targets)
		}
	}
	dialer := &dialerStub{}
	_, err := guardedDial(context.Background(), resolverStub{addresses: []netip.Addr{netip.MustParseAddr("8.8.8.8")}}, dialer, "tcp", "example.com:443")
	if err != nil || len(dialer.targets) != 1 || dialer.targets[0] != "8.8.8.8:443" {
		t.Fatal(err, dialer.targets)
	}
}

func TestBudgetCancellationAndRelease(t *testing.T) {
	budget := NewBudget(10)
	release, err := budget.Acquire(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := budget.Acquire(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	release()
	release()
	next, err := budget.Acquire(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	next()
	if _, err := budget.Acquire(context.Background(), 11); err == nil {
		t.Fatal("oversized reservation")
	}
}
