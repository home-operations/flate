package diff

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

// normalizeDocs clones each Doc's manifest and rewrites fields that
// should not participate verbatim in human-facing diffs: the listed
// annotation/label keys (chart-bump noise like helm.sh/chart,
// checksum/config, …) and ConfigMap.binaryData values (opaque base64
// blobs whose verbatim diff is gibberish to a reviewer and
// pathologically expensive for dyff to render on large hook payloads
// like kube-prometheus-stack's CRD upgrade bundle). Deep-copies so
// the original tree (shared with other consumers in the same
// orchestrator run) is untouched.
func normalizeDocs(docs []Doc, attrs, fields []string, hook func(map[string]any)) []Doc {
	if len(attrs) == 0 && len(fields) == 0 && hook == nil && !docsContainBinaryData(docs) {
		return docs
	}
	out := make([]Doc, len(docs))
	for i, d := range docs {
		out[i] = Doc{Manifest: normalizeManifest(d.Manifest, attrs, fields, hook), Parent: d.Parent}
	}
	return out
}

// normalizeManifest returns a normalized deep copy of m: strip the listed
// annotation/label keys and spec field-paths, redact ConfigMap binaryData,
// then apply the optional consumer hook. Returns nil for a nil input (an
// added/removed side of a pair has no manifest). The original is never
// mutated — it's shared with other consumers in the same orchestrator run.
func normalizeManifest(m map[string]any, attrs, fields []string, hook func(map[string]any)) map[string]any {
	if m == nil {
		return nil
	}
	c := manifest.DeepCopyMap(m)
	manifest.StripResourceAttributes(c, attrs)
	manifest.StripResourceFields(c, fields)
	redactBinaryData(c)
	if hook != nil {
		hook(c)
	}
	return c
}

// docsContainBinaryData reports whether any doc is a ConfigMap
// carrying a non-empty binaryData field — the only shape
// redactBinaryData would touch. Used so the zero-input fast path in
// normalizeDocs stays allocation-free when neither strip attrs nor
// binary payloads are present.
func docsContainBinaryData(docs []Doc) bool {
	for _, d := range docs {
		if _, ok := configMapBinaryData(d.Manifest); ok {
			return true
		}
	}
	return false
}

// redactBinaryData rewrites each ConfigMap.binaryData value to a
// content-derived summary. binaryData is, by Kubernetes convention,
// opaque bytes; the useful review signal is "did the content change"
// not "which base64 character flipped." Hash-prefix summaries
// preserve that signal while keeping the diff legible.
func redactBinaryData(doc map[string]any) {
	binaryData, ok := configMapBinaryData(doc)
	if !ok {
		return
	}
	for k, v := range binaryData {
		binaryData[k] = binaryDataSummary(v)
	}
}

// configMapBinaryData returns a manifest's binaryData map when it is a
// ConfigMap carrying one — the only shape redactBinaryData rewrites.
func configMapBinaryData(m map[string]any) (map[string]any, bool) {
	if manifest.DocKind(m) != manifest.KindConfigMap {
		return nil, false
	}
	bd, ok := m["binaryData"].(map[string]any)
	return bd, ok
}

// binaryDataSummary returns a stable, content-derived placeholder for
// a single binaryData value. base64-decode is the happy path
// (binaryData is spec'd as base64); the trim handles YAML's trailing
// newline on multi-line scalars. On decode failure we still produce a
// content hash over the raw string so unequal-but-malformed values
// don't collapse to a single summary.
func binaryDataSummary(v any) string {
	s, ok := v.(string)
	if !ok {
		return "<redacted binary data>"
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(s))
	if err != nil {
		sum := sha256.Sum256([]byte(s))
		return fmt.Sprintf("<redacted binary data: %d base64 chars sha256:%s>", len(s), hex.EncodeToString(sum[:8]))
	}
	sum := sha256.Sum256(decoded)
	return fmt.Sprintf("<redacted binary data: %d bytes sha256:%s>", len(decoded), hex.EncodeToString(sum[:8]))
}
