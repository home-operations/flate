// Package ssrfguard is an opt-in SSRF egress guard for flate's outbound source
// fetches. When flate renders UNTRUSTED input (e.g. konflate rendering a fork
// PR), a fork can place attacker-chosen URLs in the tree — kustomize remote
// resources/bases and GitRepository/OCIRepository/HelmRepository/Bucket
// spec.url — which flate would otherwise fetch server-side from inside the
// cluster, reaching cloud metadata (169.254.169.254), in-cluster services, and
// RFC1918/loopback hosts.
//
// The guard installs a net.Dialer.Control hook on every fetch transport (via
// WrapTransport, wired into source.NewHTTPTransport, the helm getter, and the
// kustomize remote-fetch client). Control runs at dial time, AFTER DNS
// resolution, for every connection INCLUDING redirect targets, so one hook
// closes both DNS-rebinding and redirect SSRF without a CheckRedirect.
//
// It is OFF by default and gated by a single process-global toggle (Restrict):
// flate normally renders TRUSTED repos that legitimately fetch from private/LAN
// hosts (self-hosted Gitea, in-cluster registries), so an always-on private
// block would break the common case. A consumer rendering untrusted input
// (konflate fork mode, or the `flate --restrict-egress` flag) turns it on. The
// toggle is process-global, not per-render, because go-git v5 installs its HTTPS
// transport on a process-global protocol map with no per-clone hook — so a
// per-render git egress policy is impossible, and a process-global toggle is the
// honest model. Set it once at process init; last writer wins.
//
// Boundaries (deliberately NOT covered):
//   - A configured HTTP proxy is a trust boundary the operator owns: with
//     Transport.Proxy set, the dial (and thus Control) sees the proxy IP, not
//     the CONNECT target.
//   - SSH git (ssh://) dials via ssh.Dial, outside this HTTP transport; SSH to a
//     metadata/RFC1918 endpoint is implausible and out of scope.
//   - This blocks reaching INTERNAL addresses; full egress-deny (incl. public
//     hosts) is the consumer's NetworkPolicy job.
package ssrfguard

import (
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"sync/atomic"
	"syscall"
	"time"
)

// restrict is the process-global on/off toggle, read at dial time by control.
var restrict atomic.Bool

// Restrict enables or disables the egress guard process-wide. The orchestrator
// sets it from Config.RestrictEgress; tests reset it. Safe for concurrent use,
// but it is a single process-level setting (last writer wins) — see the package
// doc on why per-render is not possible.
func Restrict(on bool) { restrict.Store(on) }

// Restricted reports whether the guard is currently enabled.
func Restricted() bool { return restrict.Load() }

// Ranges netip.Addr.IsPrivate does NOT cover but that are not legitimate fetch
// targets: carrier-grade NAT, the IETF protocol-assignments /24 (holds
// 192.0.0.192), and the benchmarking range. Plus the IPv6 transition prefixes
// that can embed a private/metadata IPv4 (NAT64 is realistic on DNS64 clusters —
// flate's deployment target).
var (
	cgnat       = netip.MustParsePrefix("100.64.0.0/10")
	ietfProto   = netip.MustParsePrefix("192.0.0.0/24")
	benchmark   = netip.MustParsePrefix("198.18.0.0/15")
	nat64       = netip.MustParsePrefix("64:ff9b::/96")
	sixToFour   = netip.MustParsePrefix("2002::/16")
	teredo      = netip.MustParsePrefix("2001::/32")
	broadcastV4 = netip.AddrFrom4([4]byte{255, 255, 255, 255})
)

// Blocked reports whether a resolved address must not be dialed. It is pure (no
// I/O) and is the unit-testable core of the guard. The standard SSRF set
// (loopback, RFC1918+ULA, link-local incl. 169.254.169.254, multicast,
// unspecified) plus the ranges and IPv6-embedded-IPv4 forms net/netip misses.
func Blocked(a netip.Addr) bool {
	a = a.Unmap()
	switch {
	case !a.IsValid(),
		a.IsLoopback(),
		a.IsPrivate(),          // RFC1918 + IPv6 ULA fc00::/7
		a.IsLinkLocalUnicast(), // 169.254.0.0/16 + fe80::/10
		a.IsLinkLocalMulticast(),
		a.IsMulticast(),
		a.IsUnspecified(),
		a == broadcastV4,
		a.Is4() && a.As4()[0] == 0, // 0.0.0.0/8 "this network"
		cgnat.Contains(a), ietfProto.Contains(a), benchmark.Contains(a):
		return true
	}
	return embeddedV4Blocked(a)
}

// embeddedV4Blocked extracts the IPv4 address embedded in an IPv6 transition
// address (NAT64, 6to4, Teredo, or deprecated v4-compatible) and re-checks it,
// so e.g. a DNS64 cluster resolving an attacker host to 64:ff9b::a9fe:a9fe
// (= 169.254.169.254) is still blocked. Returns false for plain IPv6.
func embeddedV4Blocked(a netip.Addr) bool {
	if !a.Is6() {
		return false
	}
	b := a.As16()
	var v4 netip.Addr
	switch {
	case nat64.Contains(a):
		v4 = netip.AddrFrom4([4]byte(b[12:16]))
	case sixToFour.Contains(a):
		v4 = netip.AddrFrom4([4]byte(b[2:6]))
	case teredo.Contains(a):
		v4 = netip.AddrFrom4([4]byte{^b[12], ^b[13], ^b[14], ^b[15]})
	case isV4Compatible(b):
		v4 = netip.AddrFrom4([4]byte(b[12:16]))
	default:
		return false
	}
	return Blocked(v4)
}

// isV4Compatible matches the deprecated ::a.b.c.d form: the top 96 bits are zero
// and the low 32 are non-zero (:: and ::1 are already handled as unspecified /
// loopback, and ::ffff:a.b.c.d is unmapped to IPv4 before this point).
func isV4Compatible(b [16]byte) bool {
	for _, x := range b[:12] {
		if x != 0 {
			return false
		}
	}
	return b[12]|b[13]|b[14]|b[15] != 0
}

// blockedError is the dial error returned for a guarded address.
type blockedError struct {
	address string
	reason  string
}

func (e *blockedError) Error() string {
	if e.reason != "" {
		return fmt.Sprintf("egress to %s blocked: %s", e.address, e.reason)
	}
	return fmt.Sprintf("egress to %s blocked by --restrict-egress (private/loopback/link-local/metadata address)", e.address)
}

// control is the net.Dialer.Control hook. When the guard is off it is a no-op
// (the dial proceeds, behaviorally identical to an unguarded transport). When on
// it rejects a dial to a Blocked address. address is the already-resolved
// ip:port for each connection attempt.
func control(_, address string, _ syscall.RawConn) error {
	if !restrict.Load() {
		return nil
	}
	ap, err := netip.ParseAddrPort(address)
	if err != nil {
		return &blockedError{address: address, reason: err.Error()}
	}
	if Blocked(ap.Addr()) {
		return &blockedError{address: address}
	}
	return nil
}

// WrapTransport installs the guard's Control hook on tr's dialer. It mirrors
// http.DefaultTransport's dialer settings (Timeout/KeepAlive) so that with the
// guard off the transport behaves identically to an unwrapped one. Idempotent
// and safe to call on every transport flate builds.
func WrapTransport(tr *http.Transport) {
	tr.DialContext = (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
		Control:   control,
	}).DialContext
}
