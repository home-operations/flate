package diff

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

// TestNormalize_StripAttrsRemovesNoise pins that Options.StripAttrs is
// applied before the dyff comparison: rotating a stripped annotation
// without changing anything else yields zero diffs. This is the
// chart-bump noise filter — `helm.sh/chart` changes on every chart
// upgrade and would otherwise produce one entry per rendered
// resource even with the K8s-aware dyff backend.
func TestNormalize_StripAttrsRemovesNoise(t *testing.T) {
	mk := func(chartLabel string) []Doc {
		return []Doc{{
			Manifest: map[string]any{
				"kind": "Deployment",
				"metadata": map[string]any{
					"name":      "x",
					"namespace": "ns",
					"annotations": map[string]any{
						"helm.sh/chart": chartLabel,
					},
				},
			},
		}}
	}
	diffs, err := Run(mk("myapp-1.2.3"), mk("myapp-1.2.4"),
		Options{StripAttrs: []string{"helm.sh/chart"}})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, len(diffs), 0) // stripped annotation produces no entries

	// Sanity: without --strip-attr, the same change DOES surface as
	// a diff — proves the strip is what's suppressing the entry.
	diffs, err = Run(mk("myapp-1.2.3"), mk("myapp-1.2.4"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, len(diffs), 1) // control: unstripped change surfaces
}

// TestNormalize_RedactsConfigMapBinaryData locks the ConfigMap.binaryData
// redaction: each value is replaced with a content-derived summary
// before dyff sees the manifest, so a chart bump that only rotates a
// 200 KB binary hook payload (kube-prometheus-stack's CRD upgrade
// bundle is the canonical case) still surfaces as a one-line "this
// changed" diff instead of two walls of base64. Sibling text fields
// in `data` keep their normal line-by-line diff.
func TestNormalize_RedactsConfigMapBinaryData(t *testing.T) {
	mk := func(blob, data string) []Doc {
		return []Doc{{
			Manifest: map[string]any{
				"kind": "ConfigMap",
				"metadata": map[string]any{
					"name": "binary", "namespace": "ns",
				},
				"binaryData": map[string]any{"payload.bin": blob},
				"data":       map[string]any{"visible": data},
			},
		}}
	}

	diffs, err := Run(mk("QUFBQQ==", "same"), mk("QkJCQg==", "same"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) != 1 {
		t.Fatalf("binaryData-only change should surface as a redacted diff, got %d diff(s)", len(diffs))
	}
	body := diffs[0].Diff
	if !strings.Contains(body, "@@ binaryData.payload.bin @@") {
		t.Errorf("expected binaryData path to remain; got:\n%s", body)
	}
	if !strings.Contains(body, "redacted binary data") {
		t.Errorf("expected redaction marker; got:\n%s", body)
	}
	if strings.Contains(body, "QUFBQQ==") || strings.Contains(body, "QkJCQg==") {
		t.Errorf("raw binaryData leaked into diff body:\n%s", body)
	}

	diffs, err = Run(mk("QUFBQQ==", "old"), mk("QkJCQg==", "new"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) != 1 {
		t.Fatalf("mixed data + binaryData change should surface as one diff, got %d diff(s)", len(diffs))
	}
	body = diffs[0].Diff
	if strings.Contains(body, "QUFBQQ==") || strings.Contains(body, "QkJCQg==") {
		t.Errorf("raw binaryData leaked into diff body:\n%s", body)
	}
	if !strings.Contains(body, "@@ data.visible @@") {
		t.Errorf("expected visible data change to remain; got:\n%s", body)
	}
}
