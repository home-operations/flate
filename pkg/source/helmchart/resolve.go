package helmchart

import (
	"fmt"
	"regexp"
	"time"

	"github.com/home-operations/flate/pkg/source"
)

// A HelmRepository HTTP chart resolution (the concrete version, tarball digest,
// and absolute .tgz URL that index.yaml resolves a chart name+version to) is
// persisted in a dedicated "helm-resolve:" slot's source.SlotMeta sidecar — the
// same freshness-gated marker pattern the OCI fetcher uses for tag→digest
// resolution (pkg/source/oci/marker.go). The chart .tgz itself stays in the
// content-addressed blob store; this slot records only the small pointer, so a
// warm run skips the live index.yaml fetch within spec.interval.

// chartDigestRE matches a well-formed Helm chart tarball digest. Helm index
// digests are bare hex (normalizeChartDigest strips any "sha256:" prefix), so —
// unlike the OCI "<algo>:<hex>" digestRE — only the hex body is validated. A
// malformed digest is treated as a missing marker so the resolution rebuilds.
var chartDigestRE = regexp.MustCompile(`^[a-fA-F0-9]{32,}$`)

// chartResolution is what the index supplies and the blob path consumes: the
// concrete chart version, its tarball sha256 (bare hex; may be "" for a
// digest-less mutable index entry), and the absolute .tgz URL.
type chartResolution struct {
	Version  string
	Digest   string
	ChartURL string
}

// valid reports whether r carries a usable resolution: a concrete version and
// URL, plus either no digest (digest-less index) or a well-formed bare-hex one.
func (r chartResolution) valid() bool {
	if r.Version == "" || r.ChartURL == "" {
		return false
	}
	return r.Digest == "" || chartDigestRE.MatchString(r.Digest)
}

// readResolveFresh returns the slot's recorded resolution only when the sidecar
// was written within maxAge (spec.interval) and is well-formed. maxAge <= 0
// disables the gate (always stale → re-fetch the index).
func readResolveFresh(slotDir string, maxAge time.Duration) (chartResolution, bool) {
	m, ok := source.ReadSlotMetaFresh(slotDir, maxAge)
	if !ok {
		return chartResolution{}, false
	}
	r := chartResolution{Version: m.ChartVersion, Digest: m.ChartDigest, ChartURL: m.ChartURL}
	if !r.valid() {
		return chartResolution{}, false
	}
	return r, true
}

// writeResolve records r in the slot's meta sidecar, preserving any other
// fields (none today on a resolve slot — it carries only the Chart* triple).
func writeResolve(slotDir string, r chartResolution) error {
	return source.UpdateSlotMeta(slotDir, func(m *source.SlotMeta) {
		m.ChartVersion = r.Version
		m.ChartDigest = r.Digest
		m.ChartURL = r.ChartURL
	})
}

// persistResolve writes r into the resolve slot atomically (stage on a cache
// hit, write, commit), mirroring oci.persistOCIResolve. A no-op when the
// resolution is incomplete, so a failed resolve never poisons the slot.
func persistResolve(slot *source.Slot, r chartResolution) error {
	if slot == nil || !r.valid() {
		return nil
	}
	if slot.Exists {
		if err := slot.StageRefresh(); err != nil {
			return fmt.Errorf("helm resolve stage: %w", err)
		}
	}
	if err := writeResolve(slot.Path, r); err != nil {
		return fmt.Errorf("helm resolve write: %w", err)
	}
	if err := slot.Commit(); err != nil {
		return fmt.Errorf("helm resolve commit: %w", err)
	}
	return nil
}
