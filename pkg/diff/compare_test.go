package diff

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/store"
)

func selectedResult(parent Parent, docs []Doc, unavailable store.StatusInfo) *orchestrator.Result {
	id := manifest.NamedResource{Kind: parent.Kind, Namespace: parent.Namespace, Name: parent.Name}
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
	result := &orchestrator.Result{
		Objects: []manifest.BaseManifest{obj}, Counts: map[string]int{parent.Kind: 1},
		Manifests: map[manifest.NamedResource][]map[string]any{id: batch},
	}
	if unavailable.Status != "" {
		result.Unavailable = map[manifest.NamedResource]store.StatusInfo{id: unavailable}
	}
	return result
}

func TestCompare_SelectedInputsAndErrors(t *testing.T) {
	selectedErr := errors.New("shared teardown failure")
	valid := selectedResult(hrParent, hrDocs(1), store.StatusInfo{})
	cases := []struct {
		name        string
		selected    []*orchestrator.Result
		selectedErr error
		inputError  bool
	}{
		{name: "initialization error", selectedErr: selectedErr},
		{name: "no snapshots", inputError: true},
		{name: "one snapshot", selected: []*orchestrator.Result{valid}, inputError: true},
		{name: "nil snapshot", selected: []*orchestrator.Result{valid, nil}, selectedErr: selectedErr, inputError: true},
		{name: "raw snapshot", selected: []*orchestrator.Result{valid, {Manifests: valid.Manifests}}, inputError: true},
		{name: "selected error survives", selected: []*orchestrator.Result{valid, valid}, selectedErr: selectedErr},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, docErr := Compare(tc.selected, tc.selectedErr, Options{Format: FormatJSON})
			_, _, imageErr := CompareImages(tc.selected, tc.selectedErr, false)
			for _, err := range []error{docErr, imageErr} {
				if errors.Is(err, manifest.ErrInput) != tc.inputError {
					t.Errorf("input error = %v; want %t", err, tc.inputError)
				}
				if tc.inputError && !errors.Is(err, manifest.ErrFlux) {
					t.Errorf("input error must wrap ErrFlux: %v", err)
				}
				if tc.selectedErr != nil && !errors.Is(err, tc.selectedErr) {
					t.Errorf("selected error lost: %v", err)
				}
			}
		})
	}
	_, err := Compare([]*orchestrator.Result{valid, valid}, selectedErr, Options{Format: "invalid"})
	if !errors.Is(err, selectedErr) {
		t.Errorf("format error discarded selected error: %v", err)
	}
}

func TestCompare_UnavailableProducerPolicy(t *testing.T) {
	failedOrig := store.StatusInfo{Status: store.StatusFailed, Message: "orig build failed"}
	failedCurrent := store.StatusInfo{Status: store.StatusFailed, Message: "current build failed"}
	skipped := store.StatusInfo{Status: store.StatusReady, Message: store.SkippedPrefix + " missing source"}
	suspended := store.StatusInfo{Status: store.StatusReady, Message: store.MsgSuspended}
	unchanged := store.StatusInfo{Status: store.StatusReady, Message: store.MsgUnchanged}
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
		{name: "both failed", left: failedOrig, right: failedCurrent, leftN: 2, rightN: 1, withheld: true, side: sideBoth, count: 3, reason: "failed on both sides: orig: orig build failed; current: current build failed", keepError: true},
		{name: "both failed without documents", left: failedOrig, right: failedCurrent, withheld: true, side: sideBoth, reason: "failed on both sides: orig: orig build failed; current: current build failed", keepError: true},
		{name: "current suspended", right: suspended, leftN: 2, rightN: 1, withheld: true, side: SideCurrent, count: 2, reason: "suspended on current"},
		{name: "orig suspended", left: suspended, leftN: 1, rightN: 2, withheld: true, side: SideOrig, count: 2, reason: "suspended on orig"},
		{name: "suspension without healthy documents", right: suspended, rightN: 1, withheld: true, side: SideCurrent, reason: "suspended on current"},
		{name: "both suspended", left: suspended, right: suspended, leftN: 2, rightN: 1, withheld: true, side: sideBoth, count: 3, reason: "suspended on orig; suspended on current"},
		{name: "both suspended without documents", left: suspended, right: suspended, withheld: true, side: sideBoth, reason: "suspended on orig; suspended on current"},
		{name: "failure and suspension", left: failedOrig, right: suspended, leftN: 2, rightN: 1, withheld: true, side: sideBoth, count: 3, reason: "orig: orig build failed; suspended on current", keepError: true},
		{name: "suspension and failure", left: suspended, right: failedCurrent, leftN: 1, rightN: 2, withheld: true, side: sideBoth, count: 3, reason: "suspended on orig; current: current build failed", keepError: true},
		{name: "skip and suspension", left: skipped, right: suspended, leftN: 1, rightN: 2, withheld: true, side: sideBoth, count: 3, reason: "orig: missing source; suspended on current"},
		{name: "both skipped control", left: skipped, right: skipped, leftN: 1, rightN: 1},
		{name: "failure and skip control", left: failedOrig, right: skipped, leftN: 1, rightN: 1, keepError: true},
		{name: "skip and failure control", left: skipped, right: failedCurrent, leftN: 1, rightN: 1, keepError: true},
		{name: "unchanged is available", left: unchanged, right: unchanged, leftN: 1, rightN: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			left, right := hrDocs(tc.leftN), hrDocs(tc.rightN)
			for _, doc := range left {
				doc.Manifest["data"] = map[string]any{"k": "ghcr.io/demo/orig:v1"}
			}
			for _, doc := range right {
				doc.Manifest["data"] = map[string]any{"k": "ghcr.io/demo/current:v2"}
			}
			selected := []*orchestrator.Result{selectedResult(hrParent, left, tc.left), selectedResult(hrParent, right, tc.right)}
			var runErr error
			if tc.keepError {
				runErr = selectedErr
			}
			out, err := Compare(selected, runErr, Options{Format: FormatJSON})
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
			} else if len(document.Changes) == 0 || len(document.Suppressed) != 0 {
				t.Fatalf("control must remain comparable: %+v", document)
			}
			images, notes, err := CompareImages(selected, runErr, true)
			if errors.Is(err, selectedErr) != tc.keepError || !slices.Equal(notes, document.Suppressed) {
				t.Errorf("image disclosure/error differs: %+v / %v", notes, err)
			}
			if tc.withheld != (len(images) == 0) {
				t.Errorf("image withholding=%t; images=%v", tc.withheld, images)
			}
		})
	}
}

func TestCompare_OptionsAndSnapshotsRemainImmutable(t *testing.T) {
	left := selectedResult(hrParent, hrDocs(1), store.StatusInfo{})
	right := selectedResult(hrParent, nil, store.StatusInfo{Status: store.StatusFailed, Message: "failed"})
	backing := []Suppression{{Parent: ksParent, Side: SideOrig, Count: 1, Reason: "supplied"}, {Reason: "untouched"}}
	before := slices.Clone(backing)
	original := manifest.DeepCopyMap(left.Manifests[hrID][0])
	normalized := 0
	out, err := Compare([]*orchestrator.Result{left, right}, nil, Options{
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
	if len(document.Suppressed) != 2 || document.Suppressed[0] != backing[0] {
		t.Errorf("supplied notes were not retained: %+v", document.Suppressed)
	}
	if !slices.Equal(backing, before) || !reflect.DeepEqual(left.Manifests[hrID][0], original) || len(left.Objects) != 1 || normalized != 0 {
		t.Error("comparison modified caller data or normalized withheld documents")
	}
}

func TestCompareImages_HealthyProducerContributionsAndCounts(t *testing.T) {
	left := selectedResult(hrParent, hrDocs(2), store.StatusInfo{})
	right := selectedResult(hrParent, hrDocs(1), store.StatusInfo{Status: store.StatusFailed, Message: "failed"})
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
		images, notes, err := CompareImages([]*orchestrator.Result{left, right}, selectedErr, includeRemoved)
		want := []string{"ghcr.io/demo/new:v2", "ghcr.io/demo/shared:v1"}
		if includeRemoved {
			want = []string{"ghcr.io/demo/new:v2", "ghcr.io/demo/removed:v1", "ghcr.io/demo/shared:v1"}
		}
		if !slices.Equal(images, want) || !errors.Is(err, selectedErr) {
			t.Errorf("images=%v err=%v; want %v and selected error", images, err, want)
		}
		if len(notes) != 1 || notes[0].Count != 2 {
			t.Errorf("suppression must count documents: %+v", notes)
		}
	}
}

func TestCompare_EmptyOneSidedOutputDoesNotDisclose(t *testing.T) {
	for _, unavailable := range []store.StatusInfo{
		{Status: store.StatusFailed, Message: "failed"},
		{Status: store.StatusReady, Message: store.SkippedPrefix + " missing source"},
	} {
		left := selectedResult(hrParent, nil, store.StatusInfo{})
		right := selectedResult(hrParent, hrDocs(1), unavailable)
		out, err := Compare([]*orchestrator.Result{left, right}, nil, Options{Format: FormatDiff})
		if err != nil || len(out) != 0 {
			t.Errorf("one unavailable side without healthy documents: out=%q err=%v", out, err)
		}
	}
}

func TestCompare_PreservesDocumentFormattingAndNormalization(t *testing.T) {
	left := []Doc{cmDoc(hrParent, "config", "apps", "old", "chart-1")}
	right := []Doc{cmDoc(hrParent, "config", "apps", "new", "chart-2")}
	selected := []*orchestrator.Result{selectedResult(hrParent, left, store.StatusInfo{}), selectedResult(hrParent, right, store.StatusInfo{})}
	before := manifest.DeepCopyMap(left[0].Manifest)
	for _, format := range []Format{FormatHuman, FormatGitHub, FormatDiff, FormatHTML, FormatBrief, FormatGitLab, FormatGitea, FormatJSON} {
		t.Run(string(format), func(t *testing.T) {
			opts := Options{Format: format, StripAttrs: DefaultStripAttrs, StripFields: DefaultStripFields,
				Normalize: func(doc map[string]any) { doc["metadata"].(map[string]any)["namespace"] = "normalized" }}
			want, err := RenderDocs(left, right, opts)
			if err != nil {
				t.Fatal(err)
			}
			got, err := Compare(selected, nil, opts)
			if err != nil || !bytes.Equal(got, want) {
				t.Errorf("selected %s formatting differs: got %q want %q err=%v", format, got, want, err)
			}
			if !reflect.DeepEqual(left[0].Manifest, before) {
				t.Error("normalization mutated selected manifests")
			}
		})
	}
}
