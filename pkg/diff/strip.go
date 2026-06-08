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
// a touched chart renders. A `checksum/<x>` annotation also rotates
// whenever the hashed ConfigMap/Secret content changes — but the diff
// already shows that underlying change directly, so the annotation is
// duplicate noise worth stripping regardless. (The per-render-random
// component some checksums fold in — e.g. matrix-synapse's
// `registration_shared_secret | default (randAlphaNum 24)` — is now made
// reproducible by seeded rendering in pkg/helm/deterministic; this strip
// stays for the legitimate content- and version-driven rotations.)
//
// Treat as read-only; copy before mutating.
var DefaultStripAttrs = []string{
	"helm.sh/chart",
	"checksum/",
	"app.kubernetes.io/version",
	"chart",
}

// DefaultStripFields is the default set of dotted spec field-paths deleted
// before diffing — for volatile values a chart templates into the spec
// rather than metadata. It is empty: the one field it used to carry, the
// TrueCharts common library's
//
//	spec.restic.unlock: {{ now | date "20060102150405" }}
//
// on every volsync ReplicationSource, was a per-render timestamp. Seeded
// rendering (pkg/helm/deterministic) now pins `now` to a fixed clock, so
// the field renders identically on both diff sides; stripping it would
// only hide a genuine change. The slice stays exported (non-nil) so
// `--strip-field` and SDK consumers have a base set to extend.
//
// Treat as read-only; copy before mutating.
var DefaultStripFields = []string{}
