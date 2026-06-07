package diff

// DefaultStripAttrs is the default set of metadata annotation/label keys
// dropped before diffing — the noise `flate diff` strips out of the box,
// exported so SDK consumers normalize identically (and don't drift from a
// hand-copied list). A trailing "/" is a prefix match (see
// manifest.StripResourceAttributes): "checksum/" covers checksum/config,
// checksum/secret, checksum/secrets, and every other checksum/<x> a chart
// emits, in one entry.
//
// These rotate on every Helm chart bump and carry no review-relevant
// signal, so leaving them in surfaces a spurious change on every resource
// a touched chart renders. Some are genuinely per-render random — e.g.
// matrix-synapse's `registration_shared_secret | default (randAlphaNum
// 24)`, which sprig draws from crypto/rand (not seedable; Helm exposes no
// funcMap hook to override it) — so flate cannot make them stable and
// stripping is what keeps a diff churn-free.
//
// Treat as read-only; copy before mutating.
var DefaultStripAttrs = []string{
	"helm.sh/chart",
	"checksum/",
	"app.kubernetes.io/version",
	"chart",
}

// DefaultStripFields is the default set of dotted spec field-paths
// deleted before diffing — the same noise-suppression rationale as
// DefaultStripAttrs, but for volatile values a chart templates into the
// spec rather than metadata. The TrueCharts common library emits
//
//	spec.restic.unlock: {{ now | date "20060102150405" }}
//
// on every volsync ReplicationSource, so two diff sides rendered a second
// apart (cold cache, or across machines) would otherwise show a spurious
// `~ spec.restic.unlock` per backed-up app. Deleting the leaf keeps the
// diff churn-free; raw `flate build` output still mirrors Helm.
//
// Treat as read-only; copy before mutating.
var DefaultStripFields = []string{
	"spec.restic.unlock",
}
