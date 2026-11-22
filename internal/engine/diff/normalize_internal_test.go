package diff

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func renderDiff(t *testing.T, left, right []Doc, optsValue *Options) string {
	t.Helper()
	opts := *optsValue
	out, _, err := renderDocuments(left, right, new(opts))
	assert.FatalIf(t, err != nil, "renderDocuments: %v", err)
	return string(out)
}

func TestComparison_NormalizeBeforePair(t *testing.T) {
	t.Parallel()
	for _, format := range []string{"changes", "json", "diff", "html", "github"} {
		t.Run(format, func(t *testing.T) {
			t.Parallel()
			testComparisonNormalizeBeforePairCase(t, &format)
		},
		)
	}
}

// TestNormalize_StripAttrsRemovesNoise pins that Options.StripAttrs is
// applied before the diff: rotating a stripped annotation without changing
// anything else yields no diff. This is the chart-bump noise filter —
// `helm.sh/chart` rotates on every chart upgrade.
func TestNormalize_StripAttrsRemovesNoise(t *testing.T) {
	t.Parallel()
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
	{
		s := renderDiff(
			t,
			mk("myapp-1.2.3"),
			mk("myapp-1.2.4"),
			new(Options{Format: FormatDiff, StripAttrs: []string{"helm.sh/chart"}}),
		)
		assert.ErrorIf(t, s != "", "stripped annotation should produce no diff, got:\n%s", s)
	}
	// Control: without --strip-attr the same change DOES surface.
	{
		s := renderDiff(t, mk("myapp-1.2.3"), mk("myapp-1.2.4"), new(Options{Format: FormatDiff}))
		assert.ErrorIf(t, s == "", "%s", "control: unstripped annotation change should surface as a diff")
	}
}

// TestNormalize_StripsChecksumPrefixOnPodTemplate pins the fix for the
// matrix-synapse churn: a pod-template `checksum/secrets` annotation
// (sha256 the chart recomputes every render from a randAlphaNum value)
// must be normalized away by the "checksum/" prefix entry, so a rotated
// hash yields no diff. The plural key previously slipped past the
// exact-match list.
func TestNormalize_StripsChecksumPrefixOnPodTemplate(t *testing.T) {
	t.Parallel()
	mk := func(sum string) []Doc {
		return []Doc{{
			Manifest: map[string]any{
				"kind":     "Deployment",
				"metadata": map[string]any{"name": "synapse", "namespace": "matrix"},
				"spec": map[string]any{
					"template": map[string]any{
						"metadata": map[string]any{
							"annotations": map[string]any{"checksum/secrets": sum},
						},
					},
				},
			},
		}}
	}
	{
		s := renderDiff(
			t,
			mk("105231a7"),
			mk("3d60fcd0"),
			new(Options{Format: FormatDiff, StripAttrs: []string{"checksum/"}}),
		)
		assert.ErrorIf(
			t,
			s != "",
			"rotated checksum/secrets should produce no diff under checksum/ prefix, got:\n%s",
			s,
		)
	}
	// Control: without the strip the same change DOES surface.
	{
		s := renderDiff(t, mk("105231a7"), mk("3d60fcd0"), new(Options{Format: FormatDiff}))
		assert.ErrorIf(t, s == "", "%s", "control: unstripped checksum/secrets change should surface as a diff")
	}
}

// TestNormalize_StripsSpecFieldPath pins Options.StripFields: a volatile
// spec field a chart stamps from the render clock — volsync's
// spec.restic.unlock = {{ now }} — is deleted before the diff, so a
// rotated timestamp yields no diff, while a genuinely-changed sibling
// spec field still surfaces.
func TestNormalize_StripsSpecFieldPath(t *testing.T) {
	t.Parallel()
	mk := func(unlock, repo string) []Doc {
		return []Doc{{
			Manifest: map[string]any{
				"apiVersion": "volsync.backube/v1alpha1",
				"kind":       "ReplicationSource",
				"metadata":   map[string]any{"name": "app-config", "namespace": "app"},
				"spec": map[string]any{
					"sourcePVC": "app-config",
					"restic": map[string]any{
						"unlock":     unlock,
						"repository": repo,
					},
				},
			},
		}}
	}
	fields := []string{"spec.restic.unlock"}

	// Only the {{ now }}-derived unlock rotated → stripped → no diff.
	{
		s := renderDiff(
			t,
			mk("20260606112230", "app-restic"),
			mk("20260606112233", "app-restic"),
			new(Options{Format: FormatDiff, StripFields: fields}),
		)
		assert.ErrorIf(t, s != "", "rotated spec.restic.unlock should produce no diff under StripFields, got:\n%s", s)
	}
	// Control: without the strip, the rotated unlock DOES surface.
	{
		s := renderDiff(
			t,
			mk("20260606112230", "app-restic"),
			mk("20260606112233", "app-restic"),
			new(Options{Format: FormatDiff}),
		)
		assert.ErrorIf(t, s == "", "%s", "control: unstripped spec.restic.unlock change should surface as a diff")
	}
	// A genuinely-changed sibling field still surfaces even with unlock stripped.
	{
		s := renderDiff(
			t,
			mk("20260606112230", "app-restic"),
			mk("20260606112233", "other-restic"),
			new(Options{Format: FormatDiff, StripFields: fields}),
		)
		assert.ErrorIf(t, s == "", "%s", "a real spec.restic.repository change must still surface under StripFields")
	}
}

// TestNormalize_RedactsConfigMapBinaryData locks the ConfigMap.binaryData
// redaction: each value is replaced with a content-derived summary before
// the diff, so a rotated binary hook payload surfaces as a one-line
// "this changed" rather than two walls of base64.
func TestNormalize_RedactsConfigMapBinaryData(t *testing.T) {
	t.Parallel()
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

	s := renderDiff(t, mk("QUFBQQ==", "same"), mk("QkJCQg==", "same"), new(Options{Format: FormatDiff}))
	assert.FatalIf(t, s == "", "%s", "binaryData-only change should surface as a redacted diff")
	assert.ErrorIf(t, !strings.Contains(s, "redacted binary data"), "expected redaction marker; got:\n%s", s)
	assert.ErrorIf(
		t,
		strings.Contains(s, "QUFBQQ==") || strings.Contains(s, "QkJCQg=="),
		"raw binaryData leaked into diff body:\n%s",
		s,
	)

	s = renderDiff(t, mk("QUFBQQ==", "old"), mk("QkJCQg==", "new"), new(Options{Format: FormatDiff}))
	assert.ErrorIf(
		t,
		strings.Contains(s, "QUFBQQ==") || strings.Contains(s, "QkJCQg=="),
		"raw binaryData leaked into diff body:\n%s",
		s,
	)
	assert.ErrorIf(t, !strings.Contains(s, "visible"), "expected visible data change to remain; got:\n%s", s)
}

func testComparisonNormalizeBeforePairCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	left, right := cm("old", "ns", "owner", "same"), cm("new", "ns", "owner", "same")
	before := []Doc{left, right}
	for i := range before {
		before[i].Manifest = manifest.DeepCopyMap(before[i].Manifest)
	}
	calls := map[string]int{}
	opts := Options{Format: Format((*testCaseInput)), Normalize: func(m map[string]any) {
		meta := m["metadata"].(map[string]any)
		calls[meta["name"].(string)]++
		meta["name"] = "normalized"
		m["data"].(map[string]any)["k"] = "normalized"
	}}
	switch *testCaseInput {
	case "changes":
		comparison := prepare([]Doc{left}, []Doc{right}, new(opts))
		changes, warnings := publicChanges(comparison), comparison.Warnings
		assert.Equal(t, len(changes), 0)
		assert.Equal(t, len(warnings), 0)
	case "json":
		got := jsonChanges(t, []Doc{left}, []Doc{right}, new(opts))
		assert.ErrorIf(
			t,
			len(got) != 0,
			"normalized identities should produce zero JSON changes, got %d: %+v",
			len(got),
			got,
		)
	default:
		out := renderDiff(t, []Doc{left}, []Doc{right}, new(opts))
		assert.ErrorIf(t, out != "", "normalized identities should pair without a diff, got:\n%s", out)
	}
	assert.Diff(t, calls, map[string]int{"old": 1, "new": 1})
	assert.Diff(t, []Doc{left, right}, before)
}

func TestComparison_GeneratorNormalization(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		normalize func(map[string]any)
		fields    []string
		pairs     int
	}{
		{"data only", func(m map[string]any) { delete(m, "data") }, nil, 1},
		{"name", func(m map[string]any) {
			meta := m["metadata"].(map[string]any)
			meta["name"] = "normalized-" + meta["name"].(string)
		}, nil, 2},
		{"namespace", func(m map[string]any) { m["metadata"].(map[string]any)["namespace"] = "normalized" }, nil, 2},
		{"kind", func(m map[string]any) { m["kind"] = "Secret" }, nil, 2},
		{"group", func(m map[string]any) { m["apiVersion"] = "example.test/v1" }, nil, 2},
		{"version only", func(m map[string]any) { m["apiVersion"] = "v2" }, nil, 1},
		{"deleted identity", nil, []string{"metadata"}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			old := withGeneratorHash(t, new(cm("settings", "apps", "owner", "before")))
			current := withGeneratorHash(t, new(cm("settings", "apps", "owner", "after")))
			before := []map[string]any{manifest.DeepCopyMap(old.Manifest), manifest.DeepCopyMap(current.Manifest)}
			calls := 0
			opts := Options{StripFields: tc.fields, Normalize: func(m map[string]any) {
				calls++
				if tc.normalize != nil {
					tc.normalize(m)
				}
			}}
			got := prepare([]Doc{old}, []Doc{current}, &opts)
			assert.Equal(t, len(got.Pairs), tc.pairs)
			assert.Equal(t, calls, 2)
			assert.Diff(t, []map[string]any{old.Manifest, current.Manifest}, before)
			if tc.pairs == 1 && tc.name != "deleted identity" {
				assert.Equal(t, got.Pairs[0].Resource.Name, manifest.DocIdentity(current.Manifest).Name)
			}
		})
	}
}

func TestComparison_ProducerLabels(t *testing.T) {
	t.Parallel()
	left, right := ncm("greeting", "k", "before"), ncm("greeting", "k", "after")
	right.Parent.Kind, right.Parent.Name = manifest.KindKustomization, "head"
	left.Parent = right.Parent
	left.Parent.Name = "base"
	keys := []string{
		"kustomize.toolkit.fluxcd.io/name", "kustomize.toolkit.fluxcd.io/namespace",
		"resourceset.fluxcd.controlplane.io/name", "resourceset.fluxcd.controlplane.io/namespace",
		"helm.toolkit.fluxcd.io/name", "helm.toolkit.fluxcd.io/namespace",
	}
	for i, doc := range []Doc{left, right} {
		value := []string{"old", "new"}[i]
		labels := map[string]any{"custom": value}
		for _, key := range keys {
			labels[key] = value
		}
		doc.Manifest["metadata"].(map[string]any)["labels"] = labels
		doc.Manifest["spec"] = map[string]any{"template": map[string]any{
			"metadata": map[string]any{"labels": map[string]any{keys[0]: value}},
		}}
	}
	before, after := manifest.DeepCopyMap(left.Manifest), manifest.DeepCopyMap(right.Manifest)
	for _, move := range []bool{true, false} {
		name := "same producer"
		if move {
			name = "different producers"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			current := right
			if !move {
				current.Parent = left.Parent
			}
			changes := jsonChanges(t, []Doc{left}, []Doc{current}, &Options{})
			assert.Equal(t, len(changes), 1)
			change := changes[0]
			assert.Equal(t, change.ProducerChange != nil, move)
			for i, got := range []map[string]any{change.Old, change.New} {
				want := []map[string]any{before, after}[i]
				if move {
					want = manifest.DeepCopyMap(want)
					want["metadata"].(map[string]any)["labels"] = map[string]any{"custom": []string{"old", "new"}[i]}
				}
				assert.Diff(t, got, want)
			}
			assert.Diff(t, left.Manifest, before)
			assert.Diff(t, right.Manifest, after)
		})
	}
}
