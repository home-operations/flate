package diff

import (
	"strings"
	"testing"
)

func renderDiff(t *testing.T, left, right []Doc, opts Options) string {
	t.Helper()
	out, err := RenderDocs(left, right, opts)
	if err != nil {
		t.Fatalf("RenderDocs: %v", err)
	}
	return string(out)
}

// TestNormalize_StripAttrsRemovesNoise pins that Options.StripAttrs is
// applied before the diff: rotating a stripped annotation without changing
// anything else yields no diff. This is the chart-bump noise filter —
// `helm.sh/chart` rotates on every chart upgrade.
func TestNormalize_StripAttrsRemovesNoise(t *testing.T) {
	mk := func(chartLabel string) []Doc {
		return []Doc{{
			Manifest: map[string]any{
				"kind": "Deployment",
				"metadata": map[string]any{
					"name":        "x",
					"namespace":   "ns",
					"annotations": map[string]any{"helm.sh/chart": chartLabel},
				},
			},
		}}
	}
	if s := renderDiff(t, mk("myapp-1.2.3"), mk("myapp-1.2.4"),
		Options{Format: FormatDiff, StripAttrs: []string{"helm.sh/chart"}}); s != "" {
		t.Errorf("stripped annotation should produce no diff, got:\n%s", s)
	}
	// Control: without --strip-attr the same change DOES surface.
	if s := renderDiff(t, mk("myapp-1.2.3"), mk("myapp-1.2.4"), Options{Format: FormatDiff}); s == "" {
		t.Error("control: unstripped annotation change should surface as a diff")
	}
}

// TestNormalize_RedactsConfigMapBinaryData locks the ConfigMap.binaryData
// redaction: each value is replaced with a content-derived summary before
// the diff, so a rotated binary hook payload surfaces as a one-line
// "this changed" rather than two walls of base64.
func TestNormalize_RedactsConfigMapBinaryData(t *testing.T) {
	mk := func(blob, data string) []Doc {
		return []Doc{{
			Manifest: map[string]any{
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "binary", "namespace": "ns"},
				"binaryData": map[string]any{"payload.bin": blob},
				"data":       map[string]any{"visible": data},
			},
		}}
	}

	s := renderDiff(t, mk("QUFBQQ==", "same"), mk("QkJCQg==", "same"), Options{Format: FormatDiff})
	if s == "" {
		t.Fatal("binaryData-only change should surface as a redacted diff")
	}
	if !strings.Contains(s, "redacted binary data") {
		t.Errorf("expected redaction marker; got:\n%s", s)
	}
	if strings.Contains(s, "QUFBQQ==") || strings.Contains(s, "QkJCQg==") {
		t.Errorf("raw binaryData leaked into diff body:\n%s", s)
	}

	s = renderDiff(t, mk("QUFBQQ==", "old"), mk("QkJCQg==", "new"), Options{Format: FormatDiff})
	if strings.Contains(s, "QUFBQQ==") || strings.Contains(s, "QkJCQg==") {
		t.Errorf("raw binaryData leaked into diff body:\n%s", s)
	}
	if !strings.Contains(s, "visible") {
		t.Errorf("expected visible data change to remain; got:\n%s", s)
	}
}
