package diff

const helmChartLabel = "helm.sh/chart"

// DefaultStripAttrs contains chart and checksum metadata keys whose changes
// duplicate version or content differences. A trailing slash matches a prefix.
// Treat the slice as read-only; clone before mutation.
//
//nolint:gochecknoglobals // The SDK exposes shared default strip attributes.
var DefaultStripAttrs = []string{
	helmChartLabel,
	"checksum/",
	"app.kubernetes.io/version",
	"chart",
}

// DefaultStripFields contains dotted spec paths excluded from diffs.
// Treat the slice as read-only; clone before mutation.
//
//nolint:gochecknoglobals // The SDK exposes shared default strip fields.
var DefaultStripFields = []string{}
