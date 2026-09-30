package acquisition

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"time"
)

var ErrUnsafeDestination = errors.New("unsafe source destination")

// Resolver pins a fresh DNS answer to the actual dialed IP on every new connection.
type Resolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

// Dialer is injectable for deterministic DNS-rebinding and address-policy tests.
type Dialer interface {
	DialContext(context.Context, string, string) (net.Conn, error)
}

var blockedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("192.88.99.0/24"), netip.MustParsePrefix("3fff::/20"),
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("100.64.0.0/10"), netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"), netip.MustParsePrefix("198.18.0.0/15"), netip.MustParsePrefix("198.51.100.0/24"), netip.MustParsePrefix("203.0.113.0/24"), netip.MustParsePrefix("240.0.0.0/4"), netip.MustParsePrefix("2001::/23"), netip.MustParsePrefix("2001:db8::/32"), netip.MustParsePrefix("2002::/16"),
}

// publicAddress excludes private, special-use and transition addresses from source dialing.
func publicAddress(address netip.Addr) bool {
	address = address.Unmap()
	if !address.IsGlobalUnicast() || address.IsPrivate() || address.IsLoopback() || address.IsLinkLocalUnicast() {
		return false
	}
	if address.Is6() && !netip.MustParsePrefix("2000::/3").Contains(address) {
		return false
	}
	for _, prefix := range blockedPrefixes {
		if prefix.Contains(address) {
			return false
		}
	}
	return true
}

// GuardedClient disables proxy/compression shortcuts and validates every redirect and dial.
func GuardedClient(resolver Resolver, dialer Dialer) *http.Client {
	transport := &http.Transport{Proxy: nil, DisableCompression: true, ForceAttemptHTTP2: true, MaxIdleConns: 20, MaxIdleConnsPerHost: 3, IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: 10 * time.Second, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	transport.DialContext = func(ctx context.Context, network, target string) (net.Conn, error) {
		return guardedDial(ctx, resolver, dialer, network, target)
	}
	return &http.Client{Transport: transport, CheckRedirect: func(request *http.Request, via []*http.Request) error {
		if len(via) >= 5 {
			return fmt.Errorf("redirect limit exceeded: %w", ErrUnsafeDestination)
		}
		return sourceURL(request.URL)
	}}
}

// guardedDial resolves and checks the entire answer before dialing any chosen IP.
func guardedDial(ctx context.Context, resolver Resolver, dialer Dialer, network, target string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return nil, ErrUnsafeDestination
	}
	addresses, err := resolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	if len(addresses) == 0 {
		return nil, fmt.Errorf("source DNS returned no addresses")
	}
	for _, address := range addresses {
		if !publicAddress(address) {
			return nil, ErrUnsafeDestination
		}
	}
	var last error
	for _, address := range addresses {
		connection, err := dialer.DialContext(ctx, network, net.JoinHostPort(address.String(), port))
		if err == nil {
			return connection, nil
		}
		last = err
	}
	return nil, last
}

// sourceURL admits configured HTTPS URLs without credentials or fragments.
func sourceURL(u *url.URL) error {
	if u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
		return ErrUnsafeDestination
	}
	return nil
}
