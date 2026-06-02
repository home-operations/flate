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
func normalizeDocs(docs []Doc, attrs []string) []Doc {
	if len(attrs) == 0 && !docsContainBinaryData(docs) {
		return docs
	}
	out := make([]Doc, len(docs))
	for i, d := range docs {
		copyDoc := manifest.DeepCopyMap(d.Manifest)
		manifest.StripResourceAttributes(copyDoc, attrs)
		redactBinaryData(copyDoc)
		out[i] = Doc{Manifest: copyDoc, Parent: d.Parent}
	}
	return out
}

// docsContainBinaryData reports whether any doc is a ConfigMap
// carrying a non-empty binaryData field — the only shape
// redactBinaryData would touch. Used so the zero-input fast path in
// normalizeDocs stays allocation-free when neither strip attrs nor
// binary payloads are present.
func docsContainBinaryData(docs []Doc) bool {
	for _, d := range docs {
		if manifest.DocKind(d.Manifest) != manifest.KindConfigMap {
			continue
		}
		if _, ok := d.Manifest["binaryData"].(map[string]any); ok {
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
	if manifest.DocKind(doc) != manifest.KindConfigMap {
		return
	}
	binaryData, ok := doc["binaryData"].(map[string]any)
	if !ok {
		return
	}
	for k, v := range binaryData {
		binaryData[k] = binaryDataSummary(v)
	}
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
