package diff

import (
	"bytes"
	"encoding/json"
	"errors"
	"html"
	"maps"
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
		Manifests: []manifest.DocumentBatch{{Parent: obj, Documents: batch}},
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

func TestCompare_SuppressionSubject(t *testing.T) {
	t.Parallel()
	for _, applier := range []manifest.NamedResource{{}, {Group: manifest.FluxOperatorDomain, Kind: "FluxInstance", Namespace: "flux-system", Name: "cluster"}} {
		for _, side := range []Side{SideOrig, SideCurrent, SideBoth} {
			t.Run(string(side)+"/"+applier.Name, func(t *testing.T) {
				t.Parallel()
				root := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "cluster", applier)
				subject := manifest.DiagnosticFor(root.Named(), root)
				key := manifest.NamedResource{Name: "paired-layer"}
				failed := Snapshot{
					Subjects:     map[manifest.NamedResource]manifest.DiagnosticIdentity{root.Named(): subject},
					Unavailable:  map[manifest.NamedResource]store.StatusInfo{key: {Status: store.StatusFailed, Message: "missing input"}},
					ProducerKeys: map[manifest.NamedResource]manifest.NamedResource{root.Named(): key},
				}
				healthy := selectedSnapshot(hrParent, hrDocs(1), store.StatusInfo{})
				healthy.ProducerKeys = map[manifest.NamedResource]manifest.NamedResource{hrParent.NamedResource: key}
				orig, current := healthy, failed
				switch side {
				case SideOrig:
					orig, current = failed, healthy
				case SideBoth:
					orig = failed
				}
				out, _, notes, err := Compare(orig, current, nil, Options{Format: FormatJSON})
				if err != nil || len(notes) != 1 || notes[0].Subject != subject || notes[0].Side != side {
					t.Fatalf("suppression lost captured subject: notes=%+v err=%v", notes, err)
				}
				wantParent, wantCount := hrParent, 1
				if side == SideBoth {
					wantParent, wantCount = Parent{NamedResource: key}, 0
				}
				if notes[0].Parent != wantParent || notes[0].Count != wantCount || !bytes.Contains([]byte(notes[0].String()), []byte(subject.String())) {
					t.Fatalf("suppression changed execution identity or lost its label: %+v", notes[0])
				}
				var rendered struct{ Suppressed []Suppression }
				if err := json.Unmarshal(out, &rendered); err != nil || len(rendered.Suppressed) != 1 || rendered.Suppressed[0].Parent != (Parent{NamedResource: subject.Resource, Path: subject.Path}) {
					t.Fatalf("JSON lost display attribution: %s; err=%v", out, err)
				}
				images, imageNotes, err := CompareImages(orig, current, nil, true)
				if err != nil || len(images) != 0 || !slices.Equal(imageNotes, notes) {
					t.Fatalf("images lost suppression context: images=%v notes=%+v err=%v", images, imageNotes, err)
				}
			})
		}
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
		{name: "current failure with stale output", right: failedCurrent, leftN: 3, rightN: 1, withheld: true, side: SideCurrent, count: 3, reason: failedCurrent.String(), keepError: true},
		{name: "orig failure", left: failedOrig, leftN: 1, rightN: 2, withheld: true, side: SideOrig, count: 2, reason: failedOrig.String(), keepError: true},
		{name: "out-of-scope failure stays nonfatal", right: failedCurrent, leftN: 2, withheld: true, side: SideCurrent, count: 2, reason: failedCurrent.String()},
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
		{name: "blocked is failed", right: blocked, left: suspended, leftN: 1, rightN: 2, withheld: true, side: SideBoth, count: 3, reason: "suspended on orig; current: " + blocked.String(), keepError: true},
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
				doc.Manifest["spec"] = map[string]any{"containers": []any{map[string]any{"image": "ghcr.io/demo/orig:v1"}}}
			}
			for _, doc := range right {
				doc.Manifest["spec"] = map[string]any{"containers": []any{map[string]any{"image": "ghcr.io/demo/current:v2"}}}
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
	for _, side := range []Side{SideOrig, SideCurrent} {
		t.Run(string(side), func(t *testing.T) {
			t.Parallel()
			left := selectedSnapshot(ksParent, []Doc{cmDoc(ksParent, manifest.NamedResource{Name: "config", Namespace: "apps"}, "old", "")}, store.StatusInfo{})
			moved := ksParent
			moved.Path = "././apps/a"
			right := selectedSnapshot(moved, []Doc{cmDoc(moved, manifest.NamedResource{Name: "config", Namespace: "apps"}, "new", "")}, store.StatusInfo{})
			unavailable := map[manifest.NamedResource]store.StatusInfo{ksParent.NamedResource: {Status: store.StatusFailed, Message: "failed"}}
			if side == SideOrig {
				left.Unavailable = unavailable
			} else {
				right.Unavailable = unavailable
			}
			backing := []Suppression{{Parent: hrParent, Side: SideOrig, Count: 1, Reason: "supplied"}, {Reason: "untouched"}}
			before := slices.Clone(backing)
			originalLeft := manifest.DeepCopyMap(left.Manifests[0].Documents[0])
			originalRight := manifest.DeepCopyMap(right.Manifests[0].Documents[0])
			selectedErr := errors.New("selected failure")
			normalized := 0
			out, warnings, suppressed, err := Compare(left, right, selectedErr, Options{
				Format: FormatJSON, Suppressed: backing[:1],
				Normalize: func(doc map[string]any) { normalized++; doc["kind"] = "Changed" },
			})
			if !errors.Is(err, selectedErr) {
				t.Fatalf("selected error lost: %v", err)
			}
			var document struct {
				Suppressed []Suppression `json:"suppressed"`
			}
			if err := json.Unmarshal(out, &document); err != nil {
				t.Fatal(err)
			}
			moved.Path = "./apps/a"
			want := Suppression{Parent: moved, Side: side, Count: 1, Reason: "failed"}
			if len(document.Suppressed) != 2 || document.Suppressed[0] != backing[0] || document.Suppressed[1] != want {
				t.Errorf("supplied or moved producer notes differ: %+v", document.Suppressed)
			}
			if !slices.Equal(suppressed, document.Suppressed) || len(warnings) != 0 {
				t.Errorf("structured suppressions = %+v; warnings = %+v", suppressed, warnings)
			}
			images, notes, err := CompareImages(left, right, selectedErr, true)
			if !errors.Is(err, selectedErr) || len(images) != 0 || !slices.Equal(notes, []Suppression{want}) {
				t.Errorf("image suppression or selected error differs: %v, %+v, %v", images, notes, err)
			}
			_, directWarnings := Changes(nil, nil, Options{Suppressed: backing[:1]})
			if len(directWarnings) != 0 {
				t.Errorf("structured changes constructed presentation warnings: %+v", directWarnings)
			}
			if !slices.Equal(backing, before) || !reflect.DeepEqual(left.Manifests[0].Documents[0], originalLeft) ||
				!reflect.DeepEqual(right.Manifests[0].Documents[0], originalRight) || len(left.Manifests) != 1 || len(right.Manifests) != 1 || normalized != 0 {
				t.Error("comparison modified caller data or normalized withheld documents")
			}
		})
	}
}

func TestCompareImages_HealthyProducerContributionsAndCounts(t *testing.T) {
	t.Parallel()
	left := selectedSnapshot(hrParent, hrDocs(2), store.StatusInfo{})
	right := selectedSnapshot(hrParent, hrDocs(1), store.StatusInfo{Status: store.StatusFailed, Message: "failed"})
	left.Manifests[0].Documents[0]["spec"] = map[string]any{"containers": []any{map[string]any{"image": "ghcr.io/demo/shared:v1"}, map[string]any{"image": "ghcr.io/demo/shared:v1"}}}
	left.Manifests[0].Documents[1]["spec"] = map[string]any{"initContainers": []any{map[string]any{"image": "ghcr.io/demo/withheld:v1"}}}
	right.Manifests[0].Documents[0]["spec"] = map[string]any{"ephemeralContainers": []any{map[string]any{"image": "ghcr.io/demo/stale:v1"}}}
	other := &manifest.HelmRelease{Name: "healthy", Namespace: "monitoring"}
	left.Manifests = append(left.Manifests, manifest.DocumentBatch{Parent: other, Documents: []map[string]any{{"containers": []any{map[string]any{"image": "ghcr.io/demo/removed:v1"}}}}})
	right.Manifests = append(right.Manifests, manifest.DocumentBatch{Parent: other, Documents: []map[string]any{{"containers": []any{
		map[string]any{"image": "ghcr.io/demo/shared:v1"}, map[string]any{"image": "ghcr.io/demo/new:v2"}, map[string]any{"image": "ghcr.io/demo/new:v2"},
	}, "unencoded": func() {}}}})
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
			if !slices.Equal(images, tc.want) || !errors.Is(err, selectedErr) {
				t.Errorf("images=%v err=%v; want %v and selected error", images, err, tc.want)
			}
			wantSuppression := Suppression{Parent: hrParent, Side: side, Count: 2, Reason: "failed"}
			if !slices.Equal(notes, []Suppression{wantSuppression}) {
				t.Errorf("suppression must count documents: %+v", notes)
			}
		})
	}
}

func TestCompareImages_DeclaredAndFullReferences(t *testing.T) {
	t.Parallel()
	left := Snapshot{Manifests: []manifest.DocumentBatch{{Documents: []map[string]any{{
		"kind": "UnknownResource",
		"spec": map[string]any{"imageName": "ghcr.io/demo/declared:v1", "image": "nginx"},
		"data": map[string]any{"tool": "ghcr.io/demo/config:v1", "record": "apiserver_request:burnrate1d", "address": "0.0.0.0:80"},
	}}}}}
	right := Snapshot{Manifests: []manifest.DocumentBatch{{Documents: []map[string]any{{
		"kind": "UnknownResource",
		"spec": map[string]any{"imageName": "ghcr.io/demo/declared:v2", "image": "nginx"},
		"data": map[string]any{"tool": "ghcr.io/demo/config:v2", "record": "apiserver_request:burnrate6h", "address": "127.0.0.1:9234"},
	}}}}}
	junk := Snapshot{Manifests: []manifest.DocumentBatch{{Documents: []map[string]any{{
		"kind": "UnknownResource",
		"spec": map[string]any{"imageName": "ghcr.io/demo/declared:v1", "image": "nginx"},
		"data": map[string]any{"tool": "ghcr.io/demo/config:v1", "record": "count:up0", "address": "127.0.0.1:9234"},
	}}}}}
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
			if err != nil || !slices.Equal(images, tc.want) || len(notes) != 0 {
				t.Fatalf("images=%v notes=%v err=%v; want %v", images, notes, err, tc.want)
			}
		})
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

func TestCompare_OwnerlessDocuments(t *testing.T) {
	t.Parallel()
	owner := &manifest.Kustomization{Name: "actual", Namespace: "apps"}
	left := selectedSnapshot(Parent{NamedResource: owner.Named()}, nil, store.StatusInfo{})
	right := selectedSnapshot(Parent{NamedResource: owner.Named()}, nil, store.StatusInfo{Status: store.StatusFailed, Message: "real driver failed"})
	makeDoc := func(name, image string) map[string]any {
		return map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": name, "namespace": "apps"}, "spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"image": image}}}}}}
	}
	left.Manifests = append(left.Manifests, manifest.DocumentBatch{Documents: []map[string]any{makeDoc("changed", "example.test/app:v1"), makeDoc("removed", "example.test/removed:v1")}})
	right.Manifests = append(right.Manifests, manifest.DocumentBatch{Documents: []map[string]any{makeDoc("changed", "example.test/app:v2"), makeDoc("added", "example.test/added:v1")}})
	out, _, _, err := Compare(left, right, nil, Options{Format: FormatJSON})
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Changes    []Change      `json:"changes"`
		Suppressed []Suppression `json:"suppressed"`
	}
	if err := json.Unmarshal(out, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Changes) != 3 || len(result.Suppressed) != 0 {
		t.Fatalf("ownerless changes suppressed: %s", out)
	}
	statuses := map[string]ChangeStatus{}
	for _, changed := range result.Changes {
		if changed.Parent != (Parent{}) || changed.Kind != "Deployment" || changed.Namespace != "apps" {
			t.Fatalf("ownerless identity changed: %+v", changed)
		}
		statuses[changed.Name] = changed.Status
	}
	if statuses["changed"] != StatusChanged || statuses["removed"] != StatusRemoved || statuses["added"] != StatusAdded {
		t.Fatalf("changes = %v", statuses)
	}
	images, notes, err := CompareImages(left, right, nil, true)
	if err != nil || len(notes) != 0 || !slices.Equal(images, []string{"example.test/added:v1", "example.test/app:v1", "example.test/app:v2", "example.test/removed:v1"}) {
		t.Fatalf("images = %v, notes = %v, err = %v", images, notes, err)
	}
	out, _, _, err = Compare(left, right, nil, Options{Format: FormatDiff})
	if err != nil || bytes.Contains(out, []byte("Kustomization")) || !bytes.Contains(out, []byte("Deployment apps/changed")) {
		t.Fatalf("ownerless heading = %q, err = %v", out, err)
	}
}

func TestCompare_EntryParentDisplay(t *testing.T) {
	t.Parallel()
	for _, applier := range []manifest.NamedResource{{}, {Group: manifest.FluxOperatorDomain, Kind: "FluxInstance", Namespace: "apps", Name: "cluster"}} {
		t.Run(applier.Name, func(t *testing.T) {
			t.Parallel()
			root := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "cluster", applier)
			subject := manifest.DiagnosticFor(root.Named(), root)
			parent := Parent{NamedResource: root.Named(), Path: "cluster"}
			before := cmDoc(parent, manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "changed"}, "before", "")
			after := cmDoc(parent, manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "changed"}, "after", "")
			left := selectedSnapshot(parent, []Doc{before, before}, store.StatusInfo{})
			right := selectedSnapshot(parent, []Doc{after, after}, store.StatusInfo{})
			left.Subjects = map[manifest.NamedResource]manifest.DiagnosticIdentity{root.Named(): subject}
			right.Subjects = maps.Clone(left.Subjects)
			originalLeft := manifest.DeepCopyMap(before.Manifest)
			originalRight := manifest.DeepCopyMap(after.Manifest)
			originalSubjects := maps.Clone(left.Subjects)
			out, warnings, _, err := Compare(left, right, nil, Options{Format: FormatJSON})
			if err != nil {
				t.Fatal(err)
			}
			var result struct{ Changes []Change }
			if err := json.Unmarshal(out, &result); err != nil {
				t.Fatal(err)
			}
			if len(result.Changes) != 1 || result.Changes[0].Parent != (Parent{NamedResource: subject.Resource, Path: subject.Path}) || bytes.Contains(out, []byte(`\u0000`)) {
				t.Fatalf("JSON leaked execution attribution: %s", out)
			}
			if len(warnings) != 2 {
				t.Fatalf("duplicate warnings lost: %+v", warnings)
			}
			for _, warning := range warnings {
				if !slices.Contains(warning.Detail, "parent: "+subject.String()) {
					t.Fatalf("warning lost captured parent: %+v", warning)
				}
			}
			for _, format := range []Format{FormatHTML, FormatDiff} {
				out, _, _, err := Compare(left, right, nil, Options{Format: format})
				if err != nil {
					t.Fatal(err)
				}
				want := "--- ConfigMap changed (cluster)\n+++ ConfigMap changed (cluster)\n"
				if format == FormatHTML {
					label := `entry path "cluster"`
					if applier != (manifest.NamedResource{}) {
						label = `FluxInstance apps/cluster (entry path "cluster")`
					}
					want = `<div class="rparent">` + html.EscapeString(label+" (cluster)") + `</div>`
				}
				if !bytes.Contains(out, []byte(want)) {
					t.Errorf("%s missing heading %q", format, want)
				}
			}
			if !reflect.DeepEqual(left.Manifests[0].Documents[0], originalLeft) || !reflect.DeepEqual(right.Manifests[0].Documents[0], originalRight) {
				t.Error("display projection changed snapshot documents")
			}
			if !maps.Equal(left.Subjects, originalSubjects) || !maps.Equal(right.Subjects, originalSubjects) {
				t.Error("display projection changed snapshot subjects")
			}
			if left.Manifests[0].Parent.Named() != root.Named() || right.Manifests[0].Parent.Named() != root.Named() {
				t.Fatal("display projection changed raw snapshot parents")
			}
		})
	}
}
