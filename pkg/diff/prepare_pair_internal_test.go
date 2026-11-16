package diff

import (
	"errors"
	"math"
	"strings"
	"testing"

	"sigs.k8s.io/kustomize/api/hasher"
	"sigs.k8s.io/kustomize/kyaml/yaml"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func preparedCMDoc(parent *Parent, id manifest.NamedResource, dataVal, chart string) Doc {
	meta := map[string]any{"name": id.Name}
	if id.Namespace != "" {
		meta["namespace"] = id.Namespace
	}
	if chart != "" {
		meta["labels"] = map[string]any{"helm.sh/chart": chart}
	}
	return Doc{
		Parent: *parent,
		Manifest: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": meta,
			"data":     map[string]any{"k": dataVal},
		},
	}
}

func TestPair_DuplicateDocumentsWarnAndLastWins(t *testing.T) {
	t.Parallel()
	parent := Parent{NamedResource: testutil.KustomizationID("flux-system", "apps"), Path: "./apps"}
	for _, side := range []string{"left", "right"} {
		t.Run(side, func(t *testing.T) { t.Parallel(); pairDuplicateDocumentsWarnAndLastWinsSide(t, &parent, &side) })
	}
}

func pairDuplicateDocumentsWarnAndLastWinsSide(t *testing.T, parent *Parent, side *string) {
	t.Helper()

	first := preparedCMDoc(parent, manifest.NamedResource{Name: "first", Namespace: "ns"}, "shadowed", "first-1")
	last := preparedCMDoc(parent, manifest.NamedResource{Name: "last", Namespace: "ns"}, "retained", "last-2")
	last.Parent.Path = "new/apps"
	last.Manifest["apiVersion"] = "v2"
	other := preparedCMDoc(parent, manifest.NamedResource{Name: "other", Namespace: "ns"}, "other-side", "other-3")
	foreign := preparedCMDoc(parent, manifest.NamedResource{Name: "foreign", Namespace: "ns"}, "separate-group", "")
	foreign.Manifest["apiVersion"] = "example.test/v1"
	left, right := []Doc{first, last, foreign}, []Doc{other}
	if (*side) == "right" {
		left, right = right, left
	}
	result := prepare(
		left,
		right,
		&Options{
			StripAttrs: []string{"helm.sh/chart", "checksum/", "app.kubernetes.io/version", "chart"},
			Normalize: func(m map[string]any) {
				meta := m["metadata"].(map[string]any)
				meta["name"], meta["namespace"] = "normalized", "normalized-ns"
				apiVersion := "v3"
				if group := manifest.DocIdentity(m).Group; group != "" {
					apiVersion = group + "/" + apiVersion
				}
				m["apiVersion"] = apiVersion
			},
		},
	)
	pairs, warnings := result.Pairs, result.Warnings
	assert.FatalIf(t, len(pairs) != 2, "expected two paired resources, got %d", len(pairs))
	assert.Equal(
		t,
		pairs[1].Resource,
		manifest.NamedResource{
			Group:     "example.test",
			Kind:      "ConfigMap",
			Namespace: "normalized-ns",
			Name:      "normalized",
		},
	)
	foreignDoc, absent := pairs[1].Old, pairs[1].New
	if (*side) == "right" {
		foreignDoc, absent = absent, foreignDoc
	}
	assert.FatalIf(
		t,
		absent != nil || foreignDoc["data"].(map[string]any)["k"] != "separate-group",
		"%v",
		"cross-group duplicate changed pairing",
	)
	retained, counterpart := pairs[0].Old, pairs[0].New
	if (*side) == "right" {
		retained, counterpart = counterpart, retained
	}
	if retained["data"].(map[string]any)["k"] != "retained" ||
		counterpart["data"].(map[string]any)["k"] != "other-side" {
		t.Fatalf("expected last document paired with other side, got %+v", pairs[0])
	}
	retainedChart, counterpartChart := pairs[0].OldChart, pairs[0].NewChart
	wantParent := other.Parent
	if (*side) == "right" {
		retainedChart, counterpartChart = counterpartChart, retainedChart
		wantParent = last.Parent
	}
	assert.Equal(t, retainedChart, "last-2")
	assert.Equal(t, counterpartChart, "other-3")
	assert.Equal(t, pairs[0].Parent, wantParent)
	assert.Equal(t, pairs[0].APIVersion, "v3")
	assert.Equal(t, pairs[0].Resource.Name, "normalized")
	assert.Equal(t, chartLabel(retained), "")
	assert.Equal(t, chartLabel(counterpart), "")
	warningSide := SideOrig
	if (*side) == "right" {
		warningSide = SideCurrent
	}
	assert.Diff(t, warnings, []manifest.Warning{
		{
			Resource: manifest.NamedResource{Kind: "ConfigMap", Namespace: "normalized-ns", Name: "normalized"},
			Category: manifest.WarnDuplicateDocument,
			Message:  "duplicate document shadows an earlier document",
			Detail: []string{
				"side: " + string(warningSide),
				"parent: " + parent.String(),
				"parent path: new/apps",
				"apiVersion: v2",
			},
		},
	})
}

func TestPair_EntryComparisonPreservesCurrentParent(t *testing.T) {
	t.Parallel()
	old := Parent{NamedResource: testutil.KustomizationID("apps", "old"), Path: "entry"}
	current := Parent{NamedResource: testutil.KustomizationID("apps", "current"), Path: "entry"}
	key := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", manifest.NamedResource{}).Named()
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "payload", Namespace: "apps"}
	result := prepare(
		[]Doc{preparedCMDoc(&old, id, "before", "")},
		[]Doc{preparedCMDoc(&current, id, "after", "")},
		&Options{producerKeys: [2]map[manifest.NamedResource]manifest.NamedResource{
			{old.NamedResource: key}, {current.NamedResource: key},
		}},
	)
	assert.Equal(t, len(result.Pairs), 1)
	assert.Equal(t, len(result.Warnings), 0)
	entry := result.Pairs[0]
	assert.Equal(t, entry.Parent, current)
	assert.Equal(t, entry.status(), "changed")
	assert.Equal(t, entry.Old["data"].(map[string]any)["k"], any("before"))
	assert.Equal(t, entry.New["data"].(map[string]any)["k"], any("after"))
	t.Run("generated", func(t *testing.T) {
		t.Parallel()
		before := withGeneratorHash(t, new(preparedCMDoc(&old, id, "before", "")))
		after := withGeneratorHash(t, new(preparedCMDoc(&current, id, "after", "")))
		result := prepare(
			[]Doc{before},
			[]Doc{after},
			&Options{producerKeys: [2]map[manifest.NamedResource]manifest.NamedResource{
				{old.NamedResource: key}, {current.NamedResource: key},
			}},
		)
		assert.Equal(t, len(result.Pairs), 1)
		assert.Equal(t, result.Pairs[0].Parent, current)
		assert.Equal(t, result.Pairs[0].Resource, manifest.DocIdentity(after.Manifest))
	})
}

func withGeneratorHash(t *testing.T, docValue *Doc) Doc {
	t.Helper()
	doc := *docValue
	node, err := yaml.FromMap(doc.Manifest)
	assert.NoError(t, err)
	hash, err := new(hasher.Hasher).Hash(node)
	assert.NoError(t, err)
	doc.Manifest = manifest.DeepCopyMap(doc.Manifest)
	meta := doc.Manifest["metadata"].(map[string]any)
	meta["name"] = meta["name"].(string) + "-" + hash
	return doc
}

func TestPair_GeneratorRename(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindConfigMap, manifest.KindSecret} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			parent := Parent{NamedResource: testutil.KustomizationID("apps", "render"), Path: "old"}
			id := manifest.NamedResource{Name: "app-values", Namespace: "apps"}
			old := preparedCMDoc(&parent, id, "before", "app-1")
			current := preparedCMDoc(&parent, id, "after", "app-2")
			old.Manifest["kind"], current.Manifest["kind"] = kind, kind
			if kind == manifest.KindSecret {
				old.Manifest["type"], current.Manifest["type"] = "Opaque", "Opaque"
			}
			old, current = withGeneratorHash(t, new(old)), withGeneratorHash(t, new(current))
			current.Parent.Path = "current"
			current.Manifest["apiVersion"] = "v2"
			before := []Doc{old, current}
			before[0].Manifest = manifest.DeepCopyMap(old.Manifest)
			before[1].Manifest = manifest.DeepCopyMap(current.Manifest)
			got := prepare([]Doc{old}, []Doc{current}, &Options{StripAttrs: []string{"helm.sh/chart"}})
			assert.Equal(t, len(got.Pairs), 1)
			assert.Equal(t, len(got.Warnings), 0)
			if len(got.Pairs) != 1 {
				t.FailNow()
			}
			entry := got.Pairs[0]
			assert.Equal(t, entry.status(), "changed")
			assert.Equal(t, entry.Resource, manifest.DocIdentity(current.Manifest))
			assert.Equal(t, entry.Parent, current.Parent)
			assert.Equal(t, entry.APIVersion, "v2")
			assert.Equal(t, entry.OldPos, 0)
			assert.Equal(t, entry.NewPos, 0)
			assert.Equal(t, entry.OldChart, "app-1")
			assert.Equal(t, entry.NewChart, "app-2")
			assert.Equal(t, chartLabel(entry.Old), "")
			assert.Equal(t, chartLabel(entry.New), "")
			assert.Diff(t, []Doc{old, current}, before)
		})
	}
}

func TestPair_GeneratorBoundaries(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		edit  func(*Doc, *Doc)
		pairs int
	}{
		{"unique", func(_, _ *Doc) {}, 1},
		{"namespace", func(_, current *Doc) { current.Manifest["metadata"].(map[string]any)["namespace"] = "elsewhere" }, 2},
		{"parent", func(_, current *Doc) { current.Parent.Name = "another" }, 2},
		{"parent namespace", func(_, current *Doc) { current.Parent.Namespace = "elsewhere" }, 2},
		{"parent group", func(_, current *Doc) { current.Parent.Group = "example.test" }, 2},
		{"foreign group", func(old, current *Doc) {
			old.Manifest["apiVersion"], current.Manifest["apiVersion"] = "example.test/v1", "example.test/v1"
		}, 2},
		{"kind", func(_, current *Doc) { current.Manifest["kind"] = "Secret" }, 2},
		{"false suffix", func(old, current *Doc) {
			for _, doc := range []*Doc{old, current} {
				doc.Manifest["data"].(map[string]any)["k"] = "patched"
			}
		}, 2},
		{"one valid side", func(_, current *Doc) { current.Manifest["data"].(map[string]any)["k"] = "patched" }, 2},
		{"later name prefix", func(_, current *Doc) {
			meta := current.Manifest["metadata"].(map[string]any)
			meta["name"] = "prefix-" + meta["name"].(string)
		}, 2},
		{"later name suffix", func(_, current *Doc) {
			meta := current.Manifest["metadata"].(map[string]any)
			meta["name"] = meta["name"].(string) + "-suffix"
		}, 2},
		{"absent segment", func(old, current *Doc) {
			old.Manifest["metadata"].(map[string]any)["name"] = "before"
			current.Manifest["metadata"].(map[string]any)["name"] = "after"
		}, 2},
		{"empty segment", func(old, current *Doc) {
			old.Manifest["metadata"].(map[string]any)["name"] = "before-"
			current.Manifest["metadata"].(map[string]any)["name"] = "after-"
		}, 2},
		{"empty shorter name", func(old, _ *Doc) {
			meta := old.Manifest["metadata"].(map[string]any)
			name := meta["name"].(string)
			meta["name"] = name[strings.LastIndexByte(name, '-'):]
		}, 2},
		{"wrong suffix length", func(_, current *Doc) {
			meta := current.Manifest["metadata"].(map[string]any)
			name := meta["name"].(string)
			meta["name"] = name[:len(name)-1]
		}, 2},
		{"disabled hashes", func(old, current *Doc) {
			old.Manifest["metadata"].(map[string]any)["name"] = "app-values"
			current.Manifest["metadata"].(map[string]any)["name"] = "app-values"
		}, 1},
		{"conversion error", func(_, current *Doc) { current.Manifest["invalid"] = generatorEncodingError{} }, 2},
		{"hash error", func(_, current *Doc) { current.Manifest["data"] = map[string]any{"invalid": math.Inf(1)} }, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			parent := Parent{NamedResource: testutil.KustomizationID("apps", "render")}
			id := manifest.NamedResource{Name: "app-values", Namespace: "apps"}
			old := withGeneratorHash(t, new(preparedCMDoc(&parent, id, "before", "")))
			current := withGeneratorHash(t, new(preparedCMDoc(&parent, id, "after", "")))
			tc.edit(&old, &current)
			got := prepare([]Doc{old}, []Doc{current}, &Options{})
			assert.Equal(t, len(got.Pairs), tc.pairs)
			assert.Equal(t, len(got.Warnings), 0)
			if tc.pairs == 1 {
				assert.Equal(t, got.Pairs[0].status(), "changed")
			}
		})
	}
}

func TestPair_GeneratorHashInputs(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, kind, field string
		editType          bool
		pairs             int
	}{
		{"binary data", manifest.KindConfigMap, "binaryData", false, 1},
		{"secret type", manifest.KindSecret, "data", false, 1},
		{"string data", manifest.KindSecret, "stringData", false, 1},
		{"patched secret type", manifest.KindSecret, "data", true, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			old := preparedCMDoc(&Parent{}, manifest.NamedResource{Name: "settings"}, "c3ludGhldGlj", "")
			old.Manifest["kind"] = tc.kind
			if tc.kind == manifest.KindSecret {
				old.Manifest["type"] = "Opaque"
			}
			if tc.field != "data" {
				old.Manifest[tc.field] = old.Manifest["data"]
				delete(old.Manifest, "data")
			}
			current := old
			current.Manifest = manifest.DeepCopyMap(old.Manifest)
			current.Manifest[tc.field].(map[string]any)["k"] = "c3ludGhldGljLTI="
			old, current = withGeneratorHash(t, new(old)), withGeneratorHash(t, new(current))
			if tc.editType {
				current.Manifest["type"] = "kubernetes.io/basic-auth"
			}
			before := []map[string]any{manifest.DeepCopyMap(old.Manifest), manifest.DeepCopyMap(current.Manifest)}
			got := prepare([]Doc{old}, []Doc{current}, &Options{})
			assert.Equal(t, len(got.Pairs), tc.pairs)
			assert.Diff(t, []map[string]any{old.Manifest, current.Manifest}, before)
			if tc.field == "binaryData" {
				assert.Equal(t, got.leftCopied, true)
				assert.Equal(t, got.rightCopied, true)
				assert.Equal(
					t,
					strings.HasPrefix(
						got.Pairs[0].Old["binaryData"].(map[string]any)["k"].(string),
						"<redacted binary data:",
					),
					true,
				)
			}
		})
	}
}

func TestPair_GeneratorAmbiguityAndExactPrecedence(t *testing.T) {
	t.Parallel()
	parent := Parent{NamedResource: testutil.KustomizationID("apps", "render")}
	id := manifest.NamedResource{Name: "settings"}
	first := withGeneratorHash(t, new(preparedCMDoc(&parent, id, "before", "")))
	second := withGeneratorHash(t, new(preparedCMDoc(&parent, id, "other", "")))
	current := withGeneratorHash(t, new(preparedCMDoc(&parent, id, "after", "")))
	invalid := first
	invalid.Manifest = manifest.DeepCopyMap(first.Manifest)
	invalid.Manifest["data"].(map[string]any)["k"] = "patched"
	authored := preparedCMDoc(&parent, id, "authored", "")
	for _, tc := range []struct {
		name         string
		old, current []Doc
		statuses     map[string]int
		warnings     int
	}{
		{"unique beside authored", []Doc{first, authored}, []Doc{current}, map[string]int{"changed": 1, "removed": 1}, 0},
		{"ambiguous old", []Doc{first, second, authored}, []Doc{current}, map[string]int{"removed": 3, "added": 1}, 0},
		{"ambiguous current", []Doc{first}, []Doc{second, current, authored}, map[string]int{"removed": 1, "added": 3}, 0},
		{"exact precedence", []Doc{first, second}, []Doc{first, current}, map[string]int{"": 1, "changed": 1}, 0},
		{"exact beside ambiguity", []Doc{first, second, current}, []Doc{first, withGeneratorHash(t, new(preparedCMDoc(&parent, id, "fourth", "")))}, map[string]int{"": 1, "removed": 2, "added": 1}, 0},
		{"duplicate invalid winner", []Doc{first, invalid}, []Doc{current}, map[string]int{"removed": 1, "added": 1}, 1},
		{"duplicate replacement valid", []Doc{invalid, first}, []Doc{current}, map[string]int{"changed": 1}, 1},
		{"duplicate valid winner", []Doc{first, first}, []Doc{current}, map[string]int{"changed": 1}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			first := prepare(tc.old, tc.current, &Options{})
			second := prepare(tc.old, tc.current, &Options{})
			statuses := map[string]int{}
			for _, p := range first.Pairs {
				statuses[p.status()]++
			}
			assert.Diff(t, statuses, tc.statuses)
			assert.Equal(t, len(first.Warnings), tc.warnings)
			assert.Diff(t, second.Pairs, first.Pairs)
			assert.Diff(t, second.Warnings, first.Warnings)
			var output []byte
			for run := range 2 {
				got, warnings, err := RenderDocs(tc.old, tc.current, Options{Format: FormatDiff})
				assert.NoError(t, err)
				assert.Diff(t, warnings, first.Warnings)
				if run == 0 {
					output = got
				} else {
					assert.Diff(t, got, output)
				}
			}
		})
	}
}

type generatorEncodingError struct{}

func (generatorEncodingError) MarshalYAML() (any, error) {
	return nil, errors.New("cannot encode synthetic test value")
}

func TestPair_CanonicalParentOrder(t *testing.T) {
	t.Parallel()
	parent := Parent{NamedResource: testutil.KustomizationID("apps", "render")}
	id := manifest.NamedResource{Name: "same"}
	old, current := preparedCMDoc(&parent, id, "before", ""), preparedCMDoc(&parent, id, "after", "")
	opts := Options{producerKeys: [2]map[manifest.NamedResource]manifest.NamedResource{
		{parent.NamedResource: testutil.KustomizationID("apps", "z")},
		{parent.NamedResource: testutil.KustomizationID("apps", "a")},
	}}
	first := prepare([]Doc{old}, []Doc{current}, &opts)
	second := prepare([]Doc{old}, []Doc{current}, &opts)
	assert.Equal(t, len(first.Pairs), 2)
	assert.Equal(t, first.Pairs[0].status(), "added")
	assert.Equal(t, first.Pairs[1].status(), "removed")
	assert.Diff(t, second.Pairs, first.Pairs)
}
