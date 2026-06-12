package ssrfguard_test

import (
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/source/ssrfguard"
)

// TestBlocked is the pure classifier matrix — the unit-testable core of the
// guard. No network, so it is parallel-safe and independent of the process-
// global toggle.
func TestBlocked(t *testing.T) {
	t.Parallel()
	cases := []struct {
		addr string
		want bool
	}{
		// --- must block: loopback ---
		{"127.0.0.1", true},
		{"127.5.6.7", true},
		{"::1", true},
		// --- must block: cloud metadata / link-local (every encoding) ---
		{"169.254.169.254", true},
		{"::ffff:169.254.169.254", true}, // IPv4-mapped IPv6
		{"64:ff9b::a9fe:a9fe", true},     // NAT64 (DNS64)
		{"2002:a9fe:a9fe::", true},       // 6to4
		{"::a9fe:a9fe", true},            // deprecated v4-compatible
		{"fe80::1", true},                // IPv6 link-local
		// --- must block: RFC1918 + ULA ---
		{"10.0.0.5", true},
		{"172.16.0.1", true},
		{"192.168.1.1", true},
		{"fc00::1", true},
		{"fd12:3456::1", true},
		// --- must block: ranges netip misses ---
		{"100.64.0.1", true},      // CGNAT
		{"198.18.0.1", true},      // benchmark
		{"192.0.0.192", true},     // IETF protocol assignments
		{"255.255.255.255", true}, // broadcast
		{"0.0.0.0", true},         // unspecified / this-network
		{"0.1.2.3", true},         // 0.0.0.0/8
		{"224.0.0.1", true},       // multicast
		// --- must allow: public ---
		{"8.8.8.8", false},
		{"1.1.1.1", false},
		{"140.82.112.3", false}, // github.com-ish
		{"2606:4700:4700::1111", false},
		{"64:ff9b::808:808", false}, // NAT64 wrapping a PUBLIC v4 (8.8.8.8) — allowed
	}
	for _, tc := range cases {
		t.Run(tc.addr, func(t *testing.T) {
			t.Parallel()
			a, err := netip.ParseAddr(tc.addr)
			if err != nil {
				t.Fatalf("parse %q: %v", tc.addr, err)
			}
			if got := ssrfguard.Blocked(a); got != tc.want {
				t.Errorf("Blocked(%s) = %v; want %v", tc.addr, got, tc.want)
			}
		})
	}
}

// TestWrapTransport_GatesDialByToggle is the end-to-end block proof: a guarded
// client GETs an httptest server (which binds to 127.0.0.1, a blocked address).
// With the guard ON the dial is rejected before connecting; with it OFF the
// request succeeds — proving both the dial-time block and the opt-in default.
// NOT parallel: it flips the process-global toggle (reset via Cleanup).
func TestWrapTransport_GatesDialByToggle(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { ssrfguard.Restrict(false) })

	tr := http.DefaultTransport.(*http.Transport).Clone()
	ssrfguard.WrapTransport(tr)
	client := &http.Client{Transport: tr}

	// Guard ON: the loopback dial must be refused with the block error.
	ssrfguard.Restrict(true)
	if _, err := client.Get(srv.URL); err == nil {
		t.Fatal("guard ON: expected the loopback dial to be blocked, got success")
	} else if !strings.Contains(err.Error(), "blocked") {
		t.Fatalf("guard ON: expected a block error, got %v", err)
	}

	// Guard OFF (default): the same request must succeed.
	ssrfguard.Restrict(false)
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("guard OFF: expected success, got %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("guard OFF: status %d", resp.StatusCode)
	}
}
