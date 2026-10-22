package diff

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func selectedSnapshot(parent Parent, docs []Doc, unavailable store.StatusInfo) Snapshot {
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
		Objects:   []manifest.BaseManifest{obj},
		Manifests: map[manifest.NamedResource][]map[string]any{id: batch},
	}
	if unavailable.Status != "" {
		result.Unavailable = map[manifest.NamedResource]store.StatusInfo{id: unavailable}
	}
	return result
}

func TestCompare_SelectedErrorsRetained(t *testing.T) {
	t.Parallel()
	selectedErr := errors.New("shared teardown failure")
	valid := selectedSnapshot(hrParent, hrDocs(1), store.StatusInfo{})
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
			_, _, _, docErr := Compare(tc.orig, tc.current, selectedErr, Options{Format: FormatJSON})
			_, _, imageErr := CompareImages(tc.orig, tc.current, selectedErr, false)
			for _, err := range []error{docErr, imageErr} {
				if !errors.Is(err, selectedErr) {
					t.Errorf("selected error lost: %v", err)
				}
			}
		})
	}
	unavailable := selectedSnapshot(hrParent, nil, store.StatusInfo{Status: store.StatusFailed, Message: "failed"})
	_, warnings, suppressed, err := Compare(valid, unavailable, selectedErr, Options{Format: "invalid"})
	if !errors.Is(err, selectedErr) {
		t.Errorf("format error discarded selected error: %v", err)
	}
	if len(warnings) != 0 || !slices.Equal(suppressed, []Suppression{{Parent: hrParent, Side: SideCurrent, Count: 1, Reason: "failed"}}) {
		t.Errorf("format error lost structured suppression: %+v; warnings: %+v", suppressed, warnings)
	}
}

func TestCompare_UnavailableProducerPolicy(t *testing.T) {
	t.Parallel()
	failedOrig := store.StatusInfo{Status: store.StatusFailed, Message: "orig build failed"}
	failedCurrent := store.StatusInfo{Status: store.StatusFailed, Message: "current build failed"}
	skipped := store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonSkipped, Message: "missing source"}
	suspended := store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonSuspended, Message: "operator stopped reconciliation"}
	unchanged := store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonUnchanged, Message: "unchanged"}
	st := store.New()
	st.UpdateFailure(hrParent.NamedResource, &manifest.DependencyFailedError{Parent: hrParent.NamedResource, Failed: []manifest.NamedResource{{Name: "source"}}})
	blocked, _ := st.GetStatus(hrParent.NamedResource)
	selectedErr := errors.New("scoped render failure")
	cases := []struct {
		name          string
		left, right   store.StatusInfo
		leftN, rightN int
		withheld      bool
		side          Side
		count         int
		reason        string
		keepError     bool
	}{
		{name: "current failure with stale output", right: failedCurrent, leftN: 3, rightN: 1, withheld: true, side: SideCurrent, count: 3, reason: failedCurrent.Message, keepError: true},
		{name: "orig failure", left: failedOrig, leftN: 1, rightN: 2, withheld: true, side: SideOrig, count: 2, reason: failedOrig.Message, keepError: true},
		{name: "out-of-scope failure stays nonfatal", right: failedCurrent, leftN: 2, withheld: true, side: SideCurrent, count: 2, reason: failedCurrent.Message},
		{name: "current soft skip", right: skipped, leftN: 2, rightN: 1, withheld: true, side: SideCurrent, count: 2, reason: "missing source"},
		{name: "both failed", left: failedOrig, right: failedCurrent, leftN: 2, rightN: 1, withheld: true, side: SideBoth, count: 3, reason: "failed on both sides: orig: orig build failed; current: current build failed", keepError: true},
		{name: "both failed without documents", left: failedOrig, right: failedCurrent, withheld: true, side: SideBoth, reason: "failed on both sides: orig: orig build failed; current: current build failed", keepError: true},
		{name: "current suspended", right: suspended, leftN: 2, rightN: 1, withheld: true, side: SideCurrent, count: 2, reason: "suspended on current"},
		{name: "orig suspended", left: suspended, leftN: 1, rightN: 2, withheld: true, side: SideOrig, count: 2, reason: "suspended on orig"},
		{name: "suspension without healthy documents", right: suspended, rightN: 1, withheld: true, side: SideCurrent, reason: "suspended on current"},
		{name: "both suspended", left: suspended, right: suspended, leftN: 2, rightN: 1, withheld: true, side: SideBoth, count: 3, reason: "suspended on orig; suspended on current"},
		{name: "both suspended without documents", left: suspended, right: suspended, withheld: true, side: SideBoth, reason: "suspended on orig; suspended on current"},
		{name: "failure and suspension", left: failedOrig, right: suspended, leftN: 2, rightN: 1, withheld: true, side: SideBoth, count: 3, reason: "orig: orig build failed; suspended on current", keepError: true},
		{name: "suspension and failure", left: suspended, right: failedCurrent, leftN: 1, rightN: 2, withheld: true, side: SideBoth, count: 3, reason: "suspended on orig; current: current build failed", keepError: true},
		{name: "skip and suspension", left: skipped, right: suspended, leftN: 1, rightN: 2, withheld: true, side: SideBoth, count: 3, reason: "orig: missing source; suspended on current"},
		{name: "both skipped control", left: skipped, right: skipped, leftN: 1, rightN: 1},
		{name: "failure and skip control", left: failedOrig, right: skipped, leftN: 1, rightN: 1, keepError: true},
		{name: "skip and failure control", left: skipped, right: failedCurrent, leftN: 1, rightN: 1, keepError: true},
		{name: "unchanged is available", left: unchanged, right: unchanged, leftN: 1, rightN: 1},
		{name: "blocked is failed", right: blocked, left: suspended, leftN: 1, rightN: 2, withheld: true, side: SideBoth, count: 3, reason: "suspended on orig; current: " + blocked.Message, keepError: true},
		{name: "empty typed skip", right: store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonSkipped}, leftN: 1, withheld: true, side: SideCurrent, count: 1},
		{name: "nested skip", right: store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonSkipped, Message: "source OCIRepository.source.toolkit.fluxcd.io/ns/source skipped: skipped: missing auth"}, leftN: 1, withheld: true, side: SideCurrent, count: 1, reason: "source OCIRepository.source.toolkit.fluxcd.io/ns/source skipped: skipped: missing auth"},
		{name: "boundary whitespace", right: store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonSkipped, Message: "source OCIRepository.source.toolkit.fluxcd.io/ns/source skipped:   missing auth"}, leftN: 1, withheld: true, side: SideCurrent, count: 1, reason: "source OCIRepository.source.toolkit.fluxcd.io/ns/source skipped:   missing auth"},
		{name: "misleading skip", right: store.StatusInfo{Status: store.StatusReady, Message: "skipped: missing source"}, leftN: 1, rightN: 1},
		{name: "misleading suspension", right: store.StatusInfo{Status: store.StatusReady, Message: "suspended"}, leftN: 1, rightN: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			left, right := hrDocs(tc.leftN), hrDocs(tc.rightN)
			for _, doc := range left {
				doc.Manifest["data"] = map[string]any{"k": "ghcr.io/demo/orig:v1"}
			}
			for _, doc := range right {
				doc.Manifest["data"] = map[string]any{"k": "ghcr.io/demo/current:v2"}
			}
			selected := []Snapshot{selectedSnapshot(hrParent, left, tc.left), selectedSnapshot(hrParent, right, tc.right)}
			var runErr error
			if tc.keepError {
				runErr = selectedErr
			}
			out, warnings, suppressed, err := Compare(selected[0], selected[1], runErr, Options{Format: FormatJSON})
			if errors.Is(err, selectedErr) != tc.keepError {
				t.Errorf("selected error = %v; want preserved=%t", err, tc.keepError)
			}
			var document struct {
				Changes    []Change      `json:"changes"`
				Suppressed []Suppression `json:"suppressed"`
			}
			if err := json.Unmarshal(out, &document); err != nil {
				t.Fatal(err)
			}
			if tc.withheld {
				want := Suppression{Parent: hrParent, Side: tc.side, Count: tc.count, Reason: tc.reason}
				if len(document.Changes) != 0 || len(document.Suppressed) != 1 || document.Suppressed[0] != want {
					t.Fatalf("withheld output = %+v; want note %+v and no changes", document, want)
				}
				if !slices.Equal(suppressed, []Suppression{want}) || len(warnings) != 0 {
					t.Errorf("structured suppressions = %+v; warnings = %+v", suppressed, warnings)
				}
			} else if len(document.Changes) == 0 || len(document.Suppressed) != 0 || len(warnings) != 0 || len(suppressed) != 0 {
				t.Fatalf("control must remain comparable: %+v", document)
			}
			images, notes, err := CompareImages(selected[0], selected[1], runErr, true)
			if errors.Is(err, selectedErr) != tc.keepError || !slices.Equal(notes, suppressed) {
				t.Errorf("image disclosure/error differs: %+v / %v", notes, err)
			}
			if tc.withheld != (len(images) == 0) {
				t.Errorf("image withholding=%t; images=%v", tc.withheld, images)
			}
		})
	}
}

func TestCompare_OptionsAndSnapshotsRemainImmutable(t *testing.T) {
	t.Parallel()
	left := selectedSnapshot(ksParent, []Doc{cmDoc(ksParent, manifest.NamedResource{Name: "config", Namespace: "apps"}, "old", "")}, store.StatusInfo{})
	moved := ksParent
	moved.Path = "other/apps"
	right := selectedSnapshot(moved, nil, store.StatusInfo{Status: store.StatusFailed, Message: "failed"})
	id := left.Objects[0].Named()
	backing := []Suppression{{Parent: hrParent, Side: SideOrig, Count: 1, Reason: "supplied"}, {Reason: "untouched"}}
	before := slices.Clone(backing)
	original := manifest.DeepCopyMap(left.Manifests[id][0])
	normalized := 0
	out, warnings, suppressed, err := Compare(left, right, nil, Options{
		Format: FormatJSON, Suppressed: backing[:1],
		Normalize: func(doc map[string]any) { normalized++; doc["kind"] = "Changed" },
	})
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Suppressed []Suppression `json:"suppressed"`
	}
	if err := json.Unmarshal(out, &document); err != nil {
		t.Fatal(err)
	}
	want := Suppression{Parent: moved, Side: SideCurrent, Count: 1, Reason: "failed"}
	if len(document.Suppressed) != 2 || document.Suppressed[0] != backing[0] || document.Suppressed[1] != want {
		t.Errorf("supplied or moved producer notes differ: %+v", document.Suppressed)
	}
	if !slices.Equal(suppressed, document.Suppressed) || len(warnings) != 0 {
		t.Errorf("structured suppressions = %+v; warnings = %+v", suppressed, warnings)
	}
	_, directWarnings := Changes(nil, nil, Options{Suppressed: backing[:1]})
	if len(directWarnings) != 0 {
		t.Errorf("structured changes constructed presentation warnings: %+v", directWarnings)
	}
	if !slices.Equal(backing, before) || !reflect.DeepEqual(left.Manifests[id][0], original) || len(left.Objects) != 1 || normalized != 0 {
		t.Error("comparison modified caller data or normalized withheld documents")
	}
}

func TestCompareImages_HealthyProducerContributionsAndCounts(t *testing.T) {
	t.Parallel()
	left := selectedSnapshot(hrParent, hrDocs(2), store.StatusInfo{})
	right := selectedSnapshot(hrParent, hrDocs(1), store.StatusInfo{Status: store.StatusFailed, Message: "failed"})
	left.Manifests[hrID][0]["data"] = map[string]any{"images": []any{"ghcr.io/demo/shared:v1", "ghcr.io/demo/shared:v1"}}
	left.Manifests[hrID][1]["data"] = map[string]any{"image": "ghcr.io/demo/withheld:v1"}
	right.Manifests[hrID][0]["data"] = map[string]any{"image": "ghcr.io/demo/stale:v1"}
	other := &manifest.HelmRelease{Name: "healthy", Namespace: "monitoring"}
	left.Objects = append(left.Objects, other)
	right.Objects = append(right.Objects, other)
	left.Manifests[other.Named()] = []map[string]any{{"image": "ghcr.io/demo/removed:v1"}}
	right.Manifests[other.Named()] = []map[string]any{{"images": []any{"ghcr.io/demo/shared:v1", "ghcr.io/demo/new:v2", "ghcr.io/demo/new:v2"}, "unencoded": func() {}}}
	selectedErr := errors.New("partial render")
	for _, includeRemoved := range []bool{false, true} {
		images, notes, err := CompareImages(left, right, selectedErr, includeRemoved)
		want := []string{"ghcr.io/demo/new:v2", "ghcr.io/demo/shared:v1"}
		if includeRemoved {
			want = []string{"ghcr.io/demo/new:v2", "ghcr.io/demo/removed:v1", "ghcr.io/demo/shared:v1"}
		}
		if !slices.Equal(images, want) || !errors.Is(err, selectedErr) {
			t.Errorf("images=%v err=%v; want %v and selected error", images, err, want)
		}
		wantSuppression := Suppression{Parent: hrParent, Side: SideCurrent, Count: 2, Reason: "failed"}
		if !slices.Equal(notes, []Suppression{wantSuppression}) {
			t.Errorf("suppression must count documents: %+v", notes)
		}
	}
}

func TestCompare_EmptyOneSidedOutputDoesNotDisclose(t *testing.T) {
	t.Parallel()
	for _, unavailable := range []store.StatusInfo{
		{Status: store.StatusFailed, Message: "failed"},
		{Status: store.StatusReady, Reason: store.ReasonSkipped, Message: "missing source"},
	} {
		left := selectedSnapshot(hrParent, nil, store.StatusInfo{})
		right := selectedSnapshot(hrParent, hrDocs(1), unavailable)
		out, _, _, err := Compare(left, right, nil, Options{Format: FormatDiff})
		if err != nil || len(out) != 0 {
			t.Errorf("one unavailable side without healthy documents: out=%q err=%v", out, err)
		}
	}
}

func TestCompare_PreservesDocumentFormattingAndNormalization(t *testing.T) {
	t.Parallel()
	left := []Doc{cmDoc(hrParent, manifest.NamedResource{Name: "config", Namespace: "apps"}, "old", "chart-1")}
	right := []Doc{cmDoc(hrParent, manifest.NamedResource{Name: "config", Namespace: "apps"}, "new", "chart-2")}
	selected := []Snapshot{selectedSnapshot(hrParent, left, store.StatusInfo{}), selectedSnapshot(hrParent, right, store.StatusInfo{})}
	before := manifest.DeepCopyMap(left[0].Manifest)
	opts := Options{Format: FormatJSON, StripAttrs: DefaultStripAttrs, StripFields: DefaultStripFields,
		Normalize: func(doc map[string]any) { doc["metadata"].(map[string]any)["namespace"] = "normalized" }}
	want, _, err := RenderDocs(left, right, opts)
	if err != nil {
		t.Fatal(err)
	}
	got, _, _, err := Compare(selected[0], selected[1], nil, opts)
	if err != nil || !bytes.Equal(got, want) {
		t.Errorf("selected formatting differs: got %q want %q err=%v", got, want, err)
	}
	if !reflect.DeepEqual(left[0].Manifest, before) {
		t.Error("normalization mutated selected manifests")
	}
}
