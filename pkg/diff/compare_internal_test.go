package diff

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func selectedSnapshot(parentValue *Parent, docs []Doc, unavailable store.StatusInfo) Snapshot {
	parent := *parentValue
	id := parent.NamedResource
	var obj manifest.BaseManifest = &manifest.HelmRelease{Name: parent.Name, Namespace: parent.Namespace}
	if parent.Kind == manifest.KindKustomization {
		ks := &manifest.Kustomization{Name: parent.Name, Namespace: parent.Namespace}
		ks.Path = parent.Path
		obj = ks
	}
	batch := make([]map[string]any, 0, len(docs))
	for _, doc := range docs {
		batch = append(batch, doc.Manifest)
	}
	result := Snapshot{
		Manifests: []manifest.DocumentBatch{testutil.DocumentBatch(obj, batch)},
	}
	if unavailable.Status != "" {
		result.Unavailable = map[manifest.NamedResource]store.StatusInfo{id: unavailable}
	}
	return result
}

func TestCompare_OptionalBatchIdentities(t *testing.T) {
	t.Parallel()
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "config"}
	left := []Doc{cmDoc(&Parent{}, id, "old", "")}
	right := []Doc{cmDoc(&Parent{}, id, "new", "")}
	want, _, err := RenderDocs(left, right, Options{Format: FormatJSON})
	assert.NoError(t, err)
	expected, err := json.MarshalIndent(struct {
		Changes    []Change      `json:"changes"`
		Suppressed []Suppression `json:"suppressed"`
	}{[]Change{{
		APIVersion: "v1", Kind: manifest.KindConfigMap, Namespace: "apps", Name: "config",
		Status: StatusChanged, Old: left[0].Manifest, New: right[0].Manifest,
	}}, []Suppression{}}, "", "  ")
	assert.NoError(t, err)
	assert.Diff(t, string(want), string(expected)+"\n")
	for _, tc := range []struct {
		name       string
		identities []manifest.NamedResource
	}{
		{name: "omitted"},
		{name: "empty", identities: []manifest.NamedResource{}},
		{name: "complete zero identity", identities: []manifest.NamedResource{{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			groups := []Group{{
				Orig: Snapshot{Manifests: []manifest.DocumentBatch{{
					Documents: []map[string]any{left[0].Manifest}, Identities: tc.identities,
				}}},
				Current: Snapshot{Manifests: []manifest.DocumentBatch{{
					Documents: []map[string]any{right[0].Manifest}, Identities: tc.identities,
				}}},
			}}
			before, err := json.Marshal(
				[2][]manifest.DocumentBatch{groups[0].Orig.Manifests, groups[0].Current.Manifests},
			)
			assert.NoError(t, err)
			comparison, err := Compare(groups, Options{Format: FormatJSON})
			assert.NoError(t, err)
			outputID := id
			if len(tc.identities) != 0 {
				outputID = tc.identities[0]
			}
			assert.Diff(t, comparison.Groups[0].Output, [2][]manifest.NamedResource{{outputID}, {outputID}})
			var first, second bytes.Buffer
			assert.NoError(t, comparison.Write(&first, nil))
			assert.NoError(t, comparison.Write(&second, nil))
			assert.Diff(t, first.String(), string(want))
			assert.Diff(t, second.String(), first.String())
			after, err := json.Marshal(
				[2][]manifest.DocumentBatch{groups[0].Orig.Manifests, groups[0].Current.Manifests},
			)
			assert.NoError(t, err)
			assert.Diff(t, after, before)
		})
	}
	for _, tc := range []struct {
		name    string
		omitted int
		reverse bool
	}{
		{name: "mixed orig", omitted: 0},
		{name: "mixed current", omitted: 1},
		{name: "mixed orig reversed", omitted: 0, reverse: true},
		{name: "mixed current reversed", omitted: 1, reverse: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testCompareMixedBatchIdentities(t, tc.omitted, tc.reverse)
		})
	}
}

func TestCompare_InvalidBatchIdentities(t *testing.T) {
	t.Parallel()
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "config"}
	doc := cmDoc(&Parent{}, id, "value", "").Manifest
	for _, side := range []Side{SideOrig, SideCurrent} {
		for _, tc := range []struct {
			name       string
			documents  []map[string]any
			identities []manifest.NamedResource
			suppressed bool
		}{
			{name: "too few", documents: []map[string]any{doc, doc}, identities: []manifest.NamedResource{id}},
			{name: "too many", documents: []map[string]any{doc}, identities: []manifest.NamedResource{id, id}},
			{name: "no documents", identities: []manifest.NamedResource{id}},
			{name: "suppressed too few", documents: []map[string]any{doc, doc}, identities: []manifest.NamedResource{id}, suppressed: true},
			{name: "suppressed too many", documents: []map[string]any{doc}, identities: []manifest.NamedResource{id, id}, suppressed: true},
			{name: "suppressed no documents", identities: []manifest.NamedResource{id}, suppressed: true},
		} {
			t.Run(string(side)+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				invalid := Snapshot{Manifests: []manifest.DocumentBatch{{}, {
					Documents: tc.documents, Identities: tc.identities,
				}}}
				selectedErr := errors.New("partial render")
				group := Group{Orig: invalid, SelectedErr: selectedErr}
				var notes []Suppression
				if tc.suppressed {
					parent := &manifest.ResourceSet{Name: "unavailable", Namespace: "apps"}
					invalid.Manifests[1].Parent = parent
					subject := manifest.DiagnosticIdentity{
						ID:   parent.Named(),
						Path: "clusters/apps",
						Resource: manifest.NamedResource{
							Group: manifest.FluxOperatorDomain,
							Kind:  "FluxInstance",
							Name:  "cluster",
						},
					}
					invalid.Subjects = map[manifest.NamedResource]manifest.DiagnosticIdentity{parent.Named(): subject}
					invalid.Unavailable = map[manifest.NamedResource]store.StatusInfo{
						parent.Named(): {Status: store.StatusFailed, Message: "failed"},
					}
					group.Orig = invalid
					group.Current = Snapshot{
						Manifests: []manifest.DocumentBatch{{Parent: parent, Documents: []map[string]any{doc}}},
					}
					notes = []Suppression{
						{Parent: objectParent(parent), Subject: subject, Side: side, Count: 1, Reason: "failed"},
					}
				}
				if side == SideCurrent {
					group.Orig, group.Current = group.Current, group.Orig
				}
				before, err := json.Marshal([2][]manifest.DocumentBatch{group.Orig.Manifests, group.Current.Manifests})
				assert.NoError(t, err)
				comparison, err := Compare([]Group{group}, Options{Format: FormatJSON})
				assert.FatalIf(t, !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) ||
					!errors.Is(err, selectedErr), "classified or selected error lost: %v", err)
				message := fmt.Sprintf("%s batch 1: identity count %d does not match document count %d",
					side, len(tc.identities), len(tc.documents))
				assert.ErrorIf(t, !strings.Contains(err.Error(), message), "input error lacks context: %v", err)
				assert.Diff(t, comparison.Groups[0].Output, [2][]manifest.NamedResource{})
				assert.Diff(t, comparison.Groups[0].Suppressed, notes)
				var out, second bytes.Buffer
				assert.NoError(t, comparison.Write(&out, nil))
				assert.NoError(t, comparison.Write(&second, nil))
				assert.Diff(t, second.String(), out.String())
				if tc.suppressed {
					displayed := notes[0]
					displayed.Parent = Parent{NamedResource: displayed.Subject.Resource, Path: displayed.Subject.Path}
					want, marshalErr := json.MarshalIndent(struct {
						Changes    []Change      `json:"changes"`
						Suppressed []Suppression `json:"suppressed"`
					}{[]Change{}, []Suppression{displayed}}, "", "  ")
					assert.NoError(t, marshalErr)
					assert.Diff(t, out.String(), string(want)+"\n")
				} else {
					assert.Diff(t, out.String(), "{\n  \"changes\": [],\n  \"suppressed\": []\n}\n")
				}
				after, err := json.Marshal([2][]manifest.DocumentBatch{group.Orig.Manifests, group.Current.Manifests})
				assert.NoError(t, err)
				assert.Diff(t, after, before)
			})
		}
	}
}

func TestCompare_SelectedErrorsRetained(t *testing.T) {
	t.Parallel()
	selectedErr := errors.New("shared teardown failure")
	valid := selectedSnapshot(new(helmReleaseParent()), hrDocs(1), store.StatusInfo{})
	for _, tc := range []struct {
		name    string
		orig    Snapshot
		current Snapshot
	}{
		{name: "empty snapshots"},
		{name: "selected snapshots", orig: valid, current: valid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			comparison1, preparationErr1 := Compare(
				[]Group{{Orig: tc.orig, Current: tc.current, SelectedErr: selectedErr}},
				Options{Format: FormatJSON},
			)
			var comparisonOutput1 bytes.Buffer
			writeErr1 := comparison1.Write(&comparisonOutput1, nil)
			_, _, _, docErr := comparisonOutput1.Bytes(), comparison1.Groups[0].Warnings, comparison1.Groups[0].Suppressed, errors.Join(
				preparationErr1,
				writeErr1,
			)
			_, _, imageErr := CompareImages(tc.orig, tc.current, selectedErr, false)
			for _, err := range []error{docErr, imageErr} {
				assert.ErrorIf(t, !errors.Is(err, selectedErr), "selected error lost: %v", err)
			}
		})
	}
	unavailable := selectedSnapshot(
		new(helmReleaseParent()),
		nil,
		store.StatusInfo{Status: store.StatusFailed, Message: "failed"},
	)
	comparison2, preparationErr2 := Compare(
		[]Group{{Orig: valid, Current: unavailable, SelectedErr: selectedErr}},
		Options{Format: "invalid"},
	)
	var comparisonOutput2 bytes.Buffer
	writeErr2 := comparison2.Write(&comparisonOutput2, nil)
	_, warnings, suppressed, err := comparisonOutput2.Bytes(), comparison2.Groups[0].Warnings, comparison2.Groups[0].Suppressed, errors.Join(
		preparationErr2,
		writeErr2,
	)
	assert.ErrorIf(t, !errors.Is(err, selectedErr), "format error discarded selected error: %v", err)
	assert.ErrorIf(t, len(warnings) != 0 ||
		!slices.Equal(
			suppressed,
			[]Suppression{{Parent: helmReleaseParent(), Side: SideCurrent, Count: 1, Reason: "failed"}},
		), "format error lost structured suppression: %+v; warnings: %+v", suppressed, warnings)
}

func TestCompare_SuppressionSubject(t *testing.T) {
	t.Parallel()
	for _, applier := range []manifest.NamedResource{{}, {Group: manifest.FluxOperatorDomain, Kind: "FluxInstance", Namespace: "flux-system", Name: "cluster"}} {
		for _, side := range []Side{SideOrig, SideCurrent, SideBoth} {
			t.Run(string(side)+"/"+applier.Name, func(t *testing.T) {
				t.Parallel()
				testCompareSuppressionSubjectCase(
					t,
					&testCompareSuppressionSubjectCaseInputs{applier: &applier, side: &side},
				)
			},
			)
		}
	}
}

func TestCompare_OptionsAndSnapshotsRemainImmutable(t *testing.T) {
	t.Parallel()
	for _, side := range []Side{SideOrig, SideCurrent} {
		t.Run(string(side), func(t *testing.T) {
			t.Parallel()
			left := selectedSnapshot(
				new(kustomizationParent()),
				[]Doc{
					cmDoc(
						new(kustomizationParent()),
						manifest.NamedResource{Name: "config", Namespace: "apps"},
						"old",
						"",
					),
				},
				store.StatusInfo{},
			)
			moved := kustomizationParent()
			moved.Path = "././apps/a"
			right := selectedSnapshot(
				new(moved),
				[]Doc{cmDoc(new(moved), manifest.NamedResource{Name: "config", Namespace: "apps"}, "new", "")},
				store.StatusInfo{},
			)
			unavailable := map[manifest.NamedResource]store.StatusInfo{
				kustomizationParent().NamedResource: {Status: store.StatusFailed, Message: "failed"},
			}
			if side == SideOrig {
				left.Unavailable = unavailable
			} else {
				right.Unavailable = unavailable
			}
			backing := []Suppression{
				{Parent: helmReleaseParent(), Side: SideOrig, Count: 1, Reason: "supplied"},
				{Reason: "untouched"},
			}
			before := slices.Clone(backing)
			originalLeft := manifest.DeepCopyMap(left.Manifests[0].Documents[0])
			originalRight := manifest.DeepCopyMap(right.Manifests[0].Documents[0])
			selectedErr := errors.New("selected failure")
			normalized := 0
			comparison3, preparationErr3 := Compare(
				[]Group{{Orig: left, Current: right, SelectedErr: selectedErr}},
				Options{
					Format: FormatJSON, Suppressed: backing[:1],
					Normalize: func(doc map[string]any) { normalized++; doc["kind"] = "Changed" },
				},
			)
			var comparisonOutput3 bytes.Buffer
			writeErr3 := comparison3.Write(&comparisonOutput3, nil)
			out, warnings, suppressed, err := comparisonOutput3.Bytes(), comparison3.Groups[0].Warnings, comparison3.Groups[0].Suppressed, errors.Join(
				preparationErr3,
				writeErr3,
			)
			assert.FatalIf(t, !errors.Is(err, selectedErr), "selected error lost: %v", err)
			var document struct {
				Suppressed []Suppression `json:"suppressed"`
			}
			{
				unmarshalErr3 := json.Unmarshal(out, &document)
				assert.NoError(t, unmarshalErr3)
			}
			moved.Path = "./apps/a"
			want := Suppression{Parent: moved, Side: side, Count: 1, Reason: "failed"}
			assert.ErrorIf(
				t,
				len(document.Suppressed) != 2 || document.Suppressed[0] != backing[0] ||
					document.Suppressed[1] != want,
				"supplied or moved producer notes differ: %+v",
				document.Suppressed,
			)
			assert.ErrorIf(
				t,
				!slices.Equal(suppressed, document.Suppressed) || len(warnings) != 0,
				"structured suppressions = %+v; warnings = %+v",
				suppressed,
				warnings,
			)
			images, notes, err := CompareImages(left, right, selectedErr, true)
			assert.ErrorIf(
				t,
				!errors.Is(err, selectedErr) || len(images) != 0 || !slices.Equal(notes, []Suppression{want}),
				"image suppression or selected error differs: %v, %+v, %v",
				images,
				notes,
				err,
			)
			_, directWarnings := Changes(nil, nil, Options{Suppressed: backing[:1]})
			assert.ErrorIf(
				t,
				len(directWarnings) != 0,
				"structured changes constructed presentation warnings: %+v",
				directWarnings,
			)
			assert.ErrorIf(
				t,
				!slices.Equal(backing, before) || !reflect.DeepEqual(left.Manifests[0].Documents[0], originalLeft) ||
					!reflect.DeepEqual(
						right.Manifests[0].Documents[0],
						originalRight,
					) || len(left.Manifests) != 1 || len(right.Manifests) != 1 ||
					normalized != 0,
				"%s",
				"comparison modified caller data or normalized withheld documents",
			)
		})
	}
}

func TestCompareImages_HealthyProducerContributionsAndCounts(t *testing.T) {
	t.Parallel()
	left := selectedSnapshot(new(helmReleaseParent()), hrDocs(2), store.StatusInfo{})
	right := selectedSnapshot(
		new(helmReleaseParent()),
		hrDocs(1),
		store.StatusInfo{Status: store.StatusFailed, Message: "failed"},
	)
	left.Manifests[0].Documents[0]["spec"] = map[string]any{
		"containers": []any{
			map[string]any{"image": "ghcr.io/demo/shared:v1"},
			map[string]any{"image": "ghcr.io/demo/shared:v1"},
		},
	}
	left.Manifests[0].Documents[1]["spec"] = map[string]any{
		"initContainers": []any{map[string]any{"image": "ghcr.io/demo/withheld:v1"}},
	}
	right.Manifests[0].Documents[0]["spec"] = map[string]any{
		"ephemeralContainers": []any{map[string]any{"image": "ghcr.io/demo/stale:v1"}},
	}
	other := &manifest.HelmRelease{Name: "healthy", Namespace: "monitoring"}
	left.Manifests = append(
		left.Manifests,
		testutil.DocumentBatch(
			other,
			[]map[string]any{{"containers": []any{map[string]any{"image": "ghcr.io/demo/removed:v1"}}}},
		),
	)
	right.Manifests = append(
		right.Manifests,
		testutil.DocumentBatch(other, []map[string]any{{"containers": []any{
			map[string]any{
				"image": "ghcr.io/demo/shared:v1",
			},
			map[string]any{"image": "ghcr.io/demo/new:v2"},
			map[string]any{"image": "ghcr.io/demo/new:v2"},
		}, "unencoded": func() {}}}),
	)
	selectedErr := errors.New("partial render")
	for _, tc := range []struct {
		name           string
		reverse        bool
		includeRemoved bool
		want           []string
	}{
		{name: "additions", want: []string{"ghcr.io/demo/new:v2", "ghcr.io/demo/shared:v1"}},
		{name: "including removals", includeRemoved: true, want: []string{"ghcr.io/demo/new:v2", "ghcr.io/demo/removed:v1", "ghcr.io/demo/shared:v1"}},
		{name: "reversed additions", reverse: true, want: []string{"ghcr.io/demo/removed:v1"}},
		{name: "reversed including removals", reverse: true, includeRemoved: true, want: []string{"ghcr.io/demo/new:v2", "ghcr.io/demo/removed:v1", "ghcr.io/demo/shared:v1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			orig, current, side := left, right, SideCurrent
			if tc.reverse {
				orig, current, side = right, left, SideOrig
			}
			images, notes, err := CompareImages(orig, current, selectedErr, tc.includeRemoved)
			assert.ErrorIf(
				t,
				!slices.Equal(images, tc.want) || !errors.Is(err, selectedErr),
				"images=%v err=%v; want %v and selected error",
				images,
				err,
				tc.want,
			)
			wantSuppression := Suppression{Parent: helmReleaseParent(), Side: side, Count: 2, Reason: "failed"}
			assert.ErrorIf(
				t,
				!slices.Equal(notes, []Suppression{wantSuppression}),
				"suppression must count documents: %+v",
				notes,
			)
		})
	}
}

func TestCompareImages_DeclaredAndFullReferences(t *testing.T) {
	t.Parallel()
	left := Snapshot{Manifests: []manifest.DocumentBatch{testutil.DocumentBatch(nil, []map[string]any{
		{
			"kind": "UnknownResource",
			"spec": map[string]any{"imageName": "ghcr.io/demo/declared:v1", "image": "nginx"},
			"data": map[string]any{
				"tool":    "ghcr.io/demo/config:v1",
				"record":  "apiserver_request:burnrate1d",
				"address": "0.0.0.0:80",
			},
		},
	})}}
	right := Snapshot{Manifests: []manifest.DocumentBatch{testutil.DocumentBatch(nil, []map[string]any{
		{
			"kind": "UnknownResource",
			"spec": map[string]any{"imageName": "ghcr.io/demo/declared:v2", "image": "nginx"},
			"data": map[string]any{
				"tool":    "ghcr.io/demo/config:v2",
				"record":  "apiserver_request:burnrate6h",
				"address": "127.0.0.1:9234",
			},
		},
	})}}
	junk := Snapshot{Manifests: []manifest.DocumentBatch{testutil.DocumentBatch(nil, []map[string]any{{
		"kind": "UnknownResource",
		"spec": map[string]any{"imageName": "ghcr.io/demo/declared:v1", "image": "nginx"},
		"data": map[string]any{"tool": "ghcr.io/demo/config:v1", "record": "count:up0", "address": "127.0.0.1:9234"},
	}})}}
	for _, tc := range []struct {
		name           string
		reverse        bool
		includeRemoved bool
		junkOnly       bool
		want           []string
	}{
		{name: "additions", want: []string{"ghcr.io/demo/config:v2", "ghcr.io/demo/declared:v2"}},
		{name: "including removals", includeRemoved: true, want: []string{"ghcr.io/demo/config:v1", "ghcr.io/demo/config:v2", "ghcr.io/demo/declared:v1", "ghcr.io/demo/declared:v2"}},
		{name: "reversed additions", reverse: true, want: []string{"ghcr.io/demo/config:v1", "ghcr.io/demo/declared:v1"}},
		{name: "reversed including removals", reverse: true, includeRemoved: true, want: []string{"ghcr.io/demo/config:v1", "ghcr.io/demo/config:v2", "ghcr.io/demo/declared:v1", "ghcr.io/demo/declared:v2"}},
		{name: "junk additions", junkOnly: true},
		{name: "junk including removals", junkOnly: true, includeRemoved: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			orig, current := left, right
			if tc.junkOnly {
				current = junk
			}
			if tc.reverse {
				orig, current = current, orig
			}
			images, notes, err := CompareImages(orig, current, nil, tc.includeRemoved)
			assert.FatalIf(
				t,
				err != nil || !slices.Equal(images, tc.want) || len(notes) != 0,
				"images=%v notes=%v err=%v; want %v",
				images,
				notes,
				err,
				tc.want,
			)
		})
	}
}

func TestCompare_EmptyOneSidedOutputDoesNotDisclose(t *testing.T) {
	t.Parallel()
	for _, unavailable := range []store.StatusInfo{
		{Status: store.StatusFailed, Message: "failed"},
		{Status: store.StatusReady, Reason: store.ReasonSkipped, Message: "missing source"},
	} {
		left := selectedSnapshot(new(helmReleaseParent()), nil, store.StatusInfo{})
		right := selectedSnapshot(new(helmReleaseParent()), hrDocs(1), unavailable)
		comparison4, preparationErr4 := Compare(
			[]Group{{Orig: left, Current: right, SelectedErr: nil}},
			Options{Format: FormatDiff},
		)
		var comparisonOutput4 bytes.Buffer
		writeErr4 := comparison4.Write(&comparisonOutput4, nil)
		out, _, _, err := comparisonOutput4.Bytes(), comparison4.Groups[0].Warnings, comparison4.Groups[0].Suppressed, errors.Join(
			preparationErr4,
			writeErr4,
		)
		assert.ErrorIf(
			t,
			err != nil || len(out) != 0,
			"one unavailable side without healthy documents: out=%q err=%v",
			out,
			err,
		)
	}
}

func TestCompare_PreservesDocumentFormattingAndNormalization(t *testing.T) {
	t.Parallel()
	left := []Doc{
		cmDoc(new(helmReleaseParent()), manifest.NamedResource{Name: "config", Namespace: "apps"}, "old", "chart-1"),
	}
	right := []Doc{
		cmDoc(new(helmReleaseParent()), manifest.NamedResource{Name: "config", Namespace: "apps"}, "new", "chart-2"),
	}
	selected := []Snapshot{
		selectedSnapshot(new(helmReleaseParent()), left, store.StatusInfo{}),
		selectedSnapshot(new(helmReleaseParent()), right, store.StatusInfo{}),
	}
	before := manifest.DeepCopyMap(left[0].Manifest)
	opts := Options{
		Format: FormatJSON, StripAttrs: DefaultStripAttrs, StripFields: DefaultStripFields,
		Normalize: func(doc map[string]any) { doc["metadata"].(map[string]any)["namespace"] = "normalized" },
	}
	want, _, err := RenderDocs(left, right, opts)
	assert.NoError(t, err)
	comparison5, preparationErr5 := Compare([]Group{{Orig: selected[0], Current: selected[1], SelectedErr: nil}}, opts)
	var comparisonOutput5 bytes.Buffer
	writeErr5 := comparison5.Write(&comparisonOutput5, nil)
	got, _, _, err := comparisonOutput5.Bytes(), comparison5.Groups[0].Warnings, comparison5.Groups[0].Suppressed, errors.Join(
		preparationErr5,
		writeErr5,
	)
	assert.ErrorIf(
		t,
		err != nil || !bytes.Equal(got, want),
		"selected formatting differs: got %q want %q err=%v",
		got,
		want,
		err,
	)
	assert.ErrorIf(
		t,
		!reflect.DeepEqual(left[0].Manifest, before),
		"%s",
		"normalization mutated selected manifests",
	)
}

type testCompareSuppressionSubjectCaseInputs struct {
	applier *manifest.NamedResource
	side    *Side
}

func testCompareSuppressionSubjectCase(t *testing.T, testCaseInput *testCompareSuppressionSubjectCaseInputs) {
	t.Helper()

	root := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "cluster", (*testCaseInput.applier))
	subject := manifest.DiagnosticFor(root.Named(), root)
	key := manifest.NamedResource{Name: "paired-layer"}
	failed := Snapshot{
		Subjects: map[manifest.NamedResource]manifest.DiagnosticIdentity{root.Named(): subject},
		Unavailable: map[manifest.NamedResource]store.StatusInfo{
			key: {Status: store.StatusFailed, Message: "missing input"},
		},
		ProducerKeys: map[manifest.NamedResource]manifest.NamedResource{root.Named(): key},
	}
	healthy := selectedSnapshot(new(helmReleaseParent()), hrDocs(1), store.StatusInfo{})
	healthy.ProducerKeys = map[manifest.NamedResource]manifest.NamedResource{helmReleaseParent().NamedResource: key}
	orig, current := healthy, failed
	switch *testCaseInput.side {
	case SideOrig:
		orig, current = failed, healthy
	case SideCurrent:
	case SideBoth:
		orig = failed
	}
	comparison6, preparationErr6 := Compare(
		[]Group{{Orig: orig, Current: current, SelectedErr: nil}},
		Options{Format: FormatJSON},
	)
	var comparisonOutput6 bytes.Buffer
	writeErr6 := comparison6.Write(&comparisonOutput6, nil)
	out, _, notes, err := comparisonOutput6.Bytes(), comparison6.Groups[0].Warnings, comparison6.Groups[0].Suppressed, errors.Join(
		preparationErr6,
		writeErr6,
	)
	assert.FatalIf(
		t,
		err != nil || len(notes) != 1 || notes[0].Subject != subject || notes[0].Side != (*testCaseInput.side),
		"suppression lost captured subject: notes=%+v err=%v",
		notes,
		err,
	)
	wantParent, wantCount := helmReleaseParent(), 1
	if (*testCaseInput.side) == SideBoth {
		wantParent, wantCount = Parent{NamedResource: key}, 0
	}
	if notes[0].Parent != wantParent || notes[0].Count != wantCount ||
		strings.Contains(notes[0].String(), subject.String()) {
		t.Fatalf("suppression changed execution identity or lost its label: %+v", notes[0])
	}
	var rendered struct {
		Suppressed []Suppression `json:"Suppressed"`
	}
	{
		unmarshalErr := json.Unmarshal(
			out,
			&rendered,
		)
		assert.FatalIf(t, unmarshalErr != nil || len(rendered.Suppressed) != 1 ||
			rendered.Suppressed[0].Parent != (Parent{NamedResource: subject.Resource, Path: subject.Path}), "JSON lost display attribution: %s; err=%v", out, unmarshalErr)
	}
	images, imageNotes, err := CompareImages(orig, current, nil, true)
	assert.FatalIf(
		t,
		err != nil || len(images) != 0 || !slices.Equal(imageNotes, notes),
		"images lost suppression context: images=%v notes=%+v err=%v",
		images,
		imageNotes,
		err,
	)
}

func testCompareMixedBatchIdentities(t *testing.T, omitted int, reverse bool) {
	t.Helper()
	parents := [2]*manifest.ResourceSet{
		{Name: "a-derived", Namespace: "policy"},
		{Name: "b-reporting", Namespace: "reporting"},
	}
	ids := [2]manifest.NamedResource{
		{Kind: manifest.KindConfigMap, Namespace: "policy", Name: "derived"},
		{Kind: manifest.KindConfigMap, Namespace: "emitter", Name: "supplied"},
	}
	var snapshots [2]Snapshot
	expected := make([]Change, 0, len(parents))
	var backing [2][]manifest.NamedResource
	for producer, parent := range parents {
		var docs [2]map[string]any
		for side := range snapshots {
			value := "old"
			if side == 1 {
				value = "new"
			}
			docs[side] = cmDoc(&Parent{}, manifest.NamedResource{Name: ids[producer].Name}, value, "").Manifest
			identities := []manifest.NamedResource{ids[producer]}
			if producer == 0 && side == omitted {
				identities = nil
			}
			if producer == 1 {
				backing[side] = []manifest.NamedResource{ids[producer], {Name: "untouched"}}
				identities = backing[side][:1]
			}
			snapshots[side].Manifests = append(snapshots[side].Manifests, manifest.DocumentBatch{
				Parent: parent, Documents: []map[string]any{docs[side]}, Identities: identities,
			})
		}
		expected = append(expected, Change{
			Parent: Parent{NamedResource: parent.Named()}, APIVersion: "v1",
			Kind: manifest.KindConfigMap, Name: ids[producer].Name,
			Status: StatusChanged, Old: docs[0], New: docs[1],
		})
	}
	if reverse {
		for side := range snapshots {
			slices.Reverse(snapshots[side].Manifests)
		}
	}
	before, err := json.Marshal([2][]manifest.DocumentBatch{snapshots[0].Manifests, snapshots[1].Manifests})
	assert.NoError(t, err)
	wantBytes, err := json.MarshalIndent(struct {
		Changes    []Change      `json:"changes"`
		Suppressed []Suppression `json:"suppressed"`
	}{expected, []Suppression{}}, "", "  ")
	assert.NoError(t, err)
	comparison, err := Compare([]Group{{Orig: snapshots[0], Current: snapshots[1]}}, Options{Format: FormatJSON})
	assert.NoError(t, err)
	want := [2][]manifest.NamedResource{{ids[0], ids[1]}, {ids[0], ids[1]}}
	assert.Diff(t, comparison.Groups[0].Output, want)
	var first, second bytes.Buffer
	assert.NoError(t, comparison.Write(&first, nil))
	assert.NoError(t, comparison.Write(&second, nil))
	assert.Diff(t, first.String(), string(wantBytes)+"\n")
	assert.Diff(t, second.String(), first.String())
	assert.Diff(t, comparison.Groups[0].Output, want)
	after, err := json.Marshal([2][]manifest.DocumentBatch{snapshots[0].Manifests, snapshots[1].Manifests})
	assert.NoError(t, err)
	assert.Diff(t, after, before)
	for _, identities := range backing {
		assert.Diff(t, identities, []manifest.NamedResource{ids[1], {Name: "untouched"}})
	}
}
