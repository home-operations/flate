package loader

import (
	"context"
	"errors"
	"io/fs"
	"maps"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestQualify_ContentFingerprint(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"immutable", "revision", "digest", "shaping", "content", "family_only", "mutable", "empty", "unknown", "changed", "error", "canceled", "callback_canceled", "callback_deadline", "bootstrap"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testQualifyContentFingerprintCase(t, &mode)
		},
		)
	}
}

func TestFilter_EqualSelectedSources(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"equal", "authored_file", "reader_reuse", "comparison_policy", "missing_bootstrap", "ownership_only", "payload_edit", "declaration_edit", "artifact_replaced", "declaration_replaced", "equal_labels", "unavailable", "unpaired_pinned"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testFilterEqualSelectedSourcesCase(t, &mode)
		},
		)
	}
}

func TestFilter_CurrentSideEvidenceAmbiguity(t *testing.T) {
	t.Parallel()
	source := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
	owner := &manifest.Kustomization{
		Name:      "apps",
		Namespace: "flux-system",
		Path:      "apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: source.Named().Kind, Name: source.Name},
	}
	head := store.New()
	head.AddObjects([]manifest.BaseManifest{source, owner})
	current := &store.SourceArtifact{
		LocalPath:  "/head",
		LocalRoot:  "/head",
		InputAlias: true,
		FS:         fstest.MapFS{"apps/cm.yaml": {Data: []byte("B\n")}},
	}
	head.SetArtifact(source.Named(), current)
	head.SetArtifact(manifest.BootstrapSourceID, current)
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		source.Named(): {
			File: "flux/source.yaml",
			Productions: []manifest.Production{
				{Source: manifest.BootstrapSourceID, File: "flux/source.yaml", Path: "/head/flux/source.yaml"},
			},
		},
		owner.Named(): {
			File: "flux/ks.yaml",
			Refs: []manifest.NamedResource{source.Named()},
			Productions: []manifest.Production{
				{Owner: owner.Named(), Source: source.Named(), File: "apps/cm.yaml", Path: "/head/apps/cm.yaml"},
			},
		},
	}, Claims: map[string][]manifest.NamedResource{"apps/": {owner.Named()}}, Readers: map[string][]manifest.FileReader{"apps/cm.yaml": {{Reader: owner.Named(), Source: source.Named()}}}}
	head.SetDiscoveryIndex(index)
	selectedNames := []string{source.Named().String(), owner.Named().String()}
	slices.Sort(selectedNames)
	var unqualified [2][]string
	for i, payload := range []string{"B\n", "A\n"} {
		before := store.New()
		before.AddObjects([]manifest.BaseManifest{source, owner})
		input := &store.SourceArtifact{
			LocalPath: "/base",
			LocalRoot: "/base",
			FS:        fstest.MapFS{"apps/cm.yaml": {Data: []byte("A\n")}},
		}
		before.SetArtifact(manifest.BootstrapSourceID, input)
		before.SetArtifact(
			source.Named(),
			&store.SourceArtifact{
				LocalPath: "/selected",
				LocalRoot: "/base",
				Key:       "committed",
				Content:   "committed",
				FS:        fstest.MapFS{"apps/cm.yaml": {Data: []byte(payload)}},
			},
		)
		paths, err := change.Detect(t.Context(), input, current)
		assert.NoError(t, err)
		assert.Diff(t, paths.Paths(), []string{"apps/cm.yaml"})
		unqualified[i] = newAdmission(
			t,
			admissionConfig{changes: paths, index: index, inventory: head, root: ""},
		).KeepNames()
		assert.Diff(t, unqualified[i], selectedNames)
		evidence, err := change.Qualify(t.Context(), paths, before, head)
		assert.NoError(t, err)
		got := newAdmission(t, admissionConfig{changes: evidence, index: index, inventory: head, root: ""}).KeepNames()
		want := []string{}
		if i == 1 {
			want = selectedNames
		}
		assert.Diff(t, got, want)
		t.Logf(
			"same current Store=%p/index=%p/current artifact=%p; bootstrap delta=%v; peer selected payload=%q; unqualified keep=%v; required paired keep=%v",
			head,
			index,
			current,
			paths.Paths(),
			payload,
			unqualified[i],
			got,
		)
	}
	assert.Diff(t, unqualified[0], unqualified[1])
}

func TestFilter_PrimaryRolesAndCycles(t *testing.T) {
	t.Parallel()
	st := store.New()
	source := &manifest.GitRepository{Name: "shared", Namespace: "apps"}
	a := &manifest.Kustomization{
		Name:      "a",
		Namespace: "apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: source.Named().Kind, Name: source.Name},
		PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []manifest.SubstituteReference{
				{Kind: manifest.KindConfigMap, Name: "from-b", Optional: true},
			},
		},
	}
	b := a.Clone()
	b.Name = "b"
	b.PostBuild = &kustomizev1.PostBuild{
		SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "from-a", Optional: true}},
	}
	peer := a.Clone()
	peer.Name, peer.PostBuild = "unrelated", nil
	st.AddObjects([]manifest.BaseManifest{source, a, b, peer})
	fromA := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "from-a"}
	fromB := fromA
	fromB.Name = "from-b"
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		a.Named(): {File: "a.yaml"}, b.Named(): {File: "b.yaml"}, peer.Named(): {File: "peer.yaml"},
		fromA: {Productions: []manifest.Production{{Owner: a.Named(), Source: manifest.BootstrapSourceID}}},
		fromB: {Productions: []manifest.Production{{Owner: b.Named(), Source: manifest.BootstrapSourceID}}},
	}}
	filter := newAdmission(
		t,
		admissionConfig{changes: change.NewSet([]string{"a.yaml"}), index: index, inventory: st, root: "/repo"},
	)
	assert.Diff(
		t,
		filter.keep,
		map[manifest.NamedResource]role{
			a.Named():      primary,
			b.Named():      primary,
			fromA:          primary,
			fromB:          primary,
			source.Named(): kept,
		},
	)
	assert.Equal(t, filter.ShouldReconcile(peer.Named()), false)

	c := a.Clone()
	c.Name = "c"
	c.PostBuild = &kustomizev1.PostBuild{
		SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: fromA.Name, Optional: true},
		},
	}
	fromC := fromA
	fromC.Name = "from-c"
	b = b.Clone()
	b.PostBuild = &kustomizev1.PostBuild{
		SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: fromC.Name, Optional: true},
		},
	}
	st.AddObjects([]manifest.BaseManifest{b, c})
	next := *index
	next.Records = maps.Clone(index.Records)
	next.Records[c.Named()] = manifest.DiscoveryRecord{File: "c.yaml"}
	next.Records[fromC] = manifest.DiscoveryRecord{
		Productions: []manifest.Production{{Owner: c.Named(), Source: manifest.BootstrapSourceID}},
	}
	filter = newAdmission(
		t,
		admissionConfig{changes: change.NewSet([]string{"a.yaml"}), index: &next, inventory: st, root: "/repo"},
	)
	assert.Diff(
		t,
		filter.keep,
		map[manifest.NamedResource]role{
			a.Named():      primary,
			b.Named():      primary,
			c.Named():      primary,
			fromA:          primary,
			fromB:          primary,
			fromC:          primary,
			source.Named(): kept,
		},
	)
	assert.Equal(t, filter.ShouldReconcile(peer.Named()), false)
}

func TestAddEmitted_RetainedInputsRemainUnconsumed(t *testing.T) {
	t.Parallel()
	st := store.New()
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	target := &manifest.ConfigMap{Name: "values", Namespace: "apps"}
	consumer := &manifest.HelmRelease{
		Name:       "consumer",
		Namespace:  "apps",
		ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindConfigMap, Name: target.Name}},
	}
	st.AddObjects([]manifest.BaseManifest{producer, consumer})
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		consumer.Named(): {File: "consumer.yaml"},
		target.Named():   {Productions: []manifest.Production{{Owner: producer.Named()}}},
	}}
	f := newAdmission(
		t,
		admissionConfig{changes: change.NewSet([]string{"consumer.yaml"}), index: index, inventory: st, root: "/repo"},
	)
	assert.Equal(t, f.ShouldReconcile(producer.Named()), true)
	var reads store.ReadSet
	reads.Init(st, consumer.Named(), nil)
	defer reads.Close()
	f.AddEmitted(manifest.NamedResource{}, store.Child{Obj: consumer}, &reads)
	_, _, next := st.GetEmission(producer.Named())
	defer next.Finish()
	next.Children = []store.Child{{Obj: target}}
	next.Complete = true
	st.RecordEmitted(producer.Named(), &next, nil)
	assert.Equal(t, reads.Valid(), true)
	assert.Equal(t, reads.GetObject(target.Named()), manifest.BaseManifest(target))
}

func TestFilter_ProducedOrderingTarget(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"kustomization", "helmrelease", "emitted"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
			ancestor := testutil.KustomizationID("apps", "ancestor")
			declaration := testutil.KustomizationID("apps", "raw")
			target := testutil.KustomizationID("apps", "generated-raw")
			var consumer manifest.BaseManifest = &manifest.Kustomization{Name: "consumer", Namespace: "apps", DependsOn: []kustomizev1.DependencyReference{{Name: target.Name}}}
			if mode == "helmrelease" {
				target = testutil.HelmReleaseID("apps", "generated-raw")
				consumer = &manifest.HelmRelease{
					Name:      "consumer",
					Namespace: "apps",
					DependsOn: []helmv2.DependencyReference{{Name: target.Name}},
				}
			}
			sibling := testutil.KustomizationID("apps", "sibling")
			emitter := testutil.KustomizationID("apps", "emitter")
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				consumer.Named(): {File: "consumer.yaml"},
				emitter:          {File: "emitter.yaml"},
				producer.Named(): {File: "producer.yaml", Parent: ancestor},
				target: {
					Productions: []manifest.Production{
						{Owner: producer.Named(), Declaration: declaration, Authored: declaration},
					},
				},
				sibling: {Productions: []manifest.Production{{Owner: producer.Named()}}},
			}}
			st := store.New()
			st.AddObject(producer)
			changed := "consumer.yaml"
			if mode == "emitted" {
				changed = "emitter.yaml"
			} else {
				st.AddObject(consumer)
			}
			f := newAdmission(
				t,
				admissionConfig{changes: change.NewSet([]string{changed}), index: index, inventory: st, root: "/repo"},
			)
			want := map[manifest.NamedResource]role{
				consumer.Named(): primary,
				target:           kept,
				producer.Named(): kept,
				declaration:      kept,
				ancestor:         kept,
			}
			if mode == "emitted" {
				assert.Equal(t, f.ShouldReconcile(consumer.Named()), false)
				var reads store.ReadSet
				reads.Init(st, emitter, nil)
				defer reads.Close()
				added := f.AddEmitted(emitter, store.Child{Obj: consumer}, &reads)
				assert.Equal(t, slices.IsSortedFunc(added, manifest.NamedResource.Compare), true)
				assert.Equal(t, len(added), len(want))
				assert.Equal(t, reads.Valid(), true)
				want[emitter] = primary
			}
			assert.Diff(t, f.keep, want)
			assert.Equal(t, f.ShouldReconcile(sibling), false)
			assert.Equal(t, st.GetObject(target), nil)
		})
	}
}

func TestAddEmitted_CapturedAbsentOrderingTarget(t *testing.T) {
	t.Parallel()
	emitter := testutil.KustomizationID("apps", "emitter")
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	target := &manifest.Kustomization{Name: "target", Namespace: "apps"}
	child := &manifest.Kustomization{
		Name:      "child",
		Namespace: "apps",
		DependsOn: []kustomizev1.DependencyReference{{Name: target.Name}},
	}
	st := store.New()
	st.AddObject(producer)
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		emitter:        {File: "emitter.yaml"},
		target.Named(): {Productions: []manifest.Production{{Owner: producer.Named()}}},
	}}
	f := newAdmission(
		t,
		admissionConfig{changes: change.NewSet([]string{"emitter.yaml"}), index: index, inventory: st, root: "/repo"},
	)
	var reads store.ReadSet
	reads.Init(st, emitter, nil)
	defer reads.Close()
	assert.Equal(t, reads.GetObject(target.Named()), nil)
	assert.Equal(t, reads.Valid(), true)
	st.AddObject(target)
	assert.Equal(t, reads.GetObject(target.Named()), nil)
	f.AddEmitted(emitter, store.Child{Obj: child}, &reads)
	assert.Diff(
		t,
		f.keep,
		map[manifest.NamedResource]role{
			emitter:          primary,
			child.Named():    primary,
			target.Named():   kept,
			producer.Named(): kept,
		},
	)
	assert.Equal(t, reads.Valid(), false)

	fresh := newAdmission(
		t,
		admissionConfig{changes: change.NewSet([]string{"emitter.yaml"}), index: index, inventory: st, root: "/repo"},
	)
	var current store.ReadSet
	current.Init(st, emitter, nil)
	defer current.Close()
	fresh.AddEmitted(emitter, store.Child{Obj: child}, &current)
	assert.Diff(t, fresh.keep, map[manifest.NamedResource]role{emitter: primary, child.Named(): primary})
	assert.Equal(t, current.Valid(), true)
}

func testQualifyContentFingerprintCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	st := [2]*store.Store{store.New(), store.New()}
	source := &manifest.GitRepository{Name: "selected", Namespace: "apps"}
	id := source.Named()
	if (*testCaseInput) == "bootstrap" {
		id = manifest.BootstrapSourceID
	}
	var readers [2]*relationReader
	for i := range st {
		readers[i] = &relationReader{FS: fstest.MapFS{"file": {Data: []byte{byte(i)}}}}
		art := &store.SourceArtifact{
			FS:        readers[i],
			LocalPath: []string{"/a", "/b"}[i],
			Key:       "family",
			Content:   "generation",
			Revision:  "commit",
			Digest:    "digest",
		}
		if (*testCaseInput) == "content" {
			art.Content += string(rune('a' + i))
		}
		if (*testCaseInput) == "family_only" {
			art.Content = ""
		}
		if (*testCaseInput) == "revision" {
			art.Revision += string(rune('a' + i))
		}
		if (*testCaseInput) == "digest" {
			art.Digest += string(rune('a' + i))
		}
		if (*testCaseInput) == "shaping" {
			art.Key += string(rune('a' + i))
		}
		if (*testCaseInput) == "mutable" || (*testCaseInput) == "empty" ||
			(*testCaseInput) == "unknown" ||
			(*testCaseInput) == "changed" ||
			(*testCaseInput) == "error" ||
			(*testCaseInput) ==
				"callback_canceled" ||
			(*testCaseInput) ==
				"callback_deadline" {
			art.Key, art.Content = "", ""
			if (*testCaseInput) != "unknown" {
				art.ContentFingerprint = func(context.Context, fs.FS) (string, error) {
					if (*testCaseInput) == "callback_canceled" {
						return "", context.Canceled
					}
					if (*testCaseInput) == "callback_deadline" {
						return "", context.DeadlineExceeded
					}
					if (*testCaseInput) == "error" {
						return "", fs.ErrPermission
					}
					if (*testCaseInput) == "empty" {
						return "", nil
					}
					if (*testCaseInput) == "changed" {
						return string(rune('a' + i)), nil
					}
					return "selected bytes", nil
				}
			}
		}
		st[i].AddObject(source)
		st[i].SetArtifact(id, art)
	}
	testQualifyContentFingerprintCaseCompletion(t, &testQualifyContentFingerprintCaseCompletionState{testCaseInput: &testCaseInput, st: &st, id: &id, readers: &readers})
}

func testFilterEqualSelectedSourcesCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	source := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
	owner := &manifest.Kustomization{
		Name:      "apps",
		Namespace: "flux-system",
		Path:      "apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: source.Named().Kind, Name: source.Name},
	}
	selected := []*store.SourceArtifact{
		{LocalPath: "/head", LocalRoot: "/head", InputAlias: true, FS: fstest.MapFS{
			"apps/cm.yaml": {Data: []byte("same\n")}, "other.txt": {Data: []byte("also same\n")},
		}},
		{LocalPath: "/selected", LocalRoot: "/base", Key: "committed", Content: "committed", FS: fstest.MapFS{
			"apps/cm.yaml": {Data: []byte("same\n")}, "other.txt": {Data: []byte("also same\n")},
		}},
	}
	if (*testCaseInput) == "authored_file" {
		for _, input := range selected {
			input.FS.(fstest.MapFS)["flux/source.yaml"] = &fstest.MapFile{
				Data: []byte("selected declaration\n"),
			}
		}
	}
	if (*testCaseInput) == "payload_edit" || (*testCaseInput) == "ownership_only" {
		selected[1].FS.(fstest.MapFS)["apps/cm.yaml"].Data = []byte("changed\n")
	}
	if (*testCaseInput) == "equal_labels" {
		selected[1].LocalPath = selected[0].LocalPath
	}
	if (*testCaseInput) == "comparison_policy" {
		for _, input := range selected {
			input.FS.(fstest.MapFS)["nested/extra.txt"] = &fstest.MapFile{Data: []byte("same\n")}
			input.FS = &relationReader{FS: input.FS}
		}
	}
	if (*testCaseInput) == "reader_reuse" {
		for _, input := range selected {
			input.FS = &relationReader{FS: input.FS}
		}
	}
	delta, err := change.Detect(t.Context(), selected[0], selected[1])
	assert.NoError(t, err)
	var wantDelta []string
	if (*testCaseInput) == "payload_edit" || (*testCaseInput) == "ownership_only" {
		wantDelta = []string{"apps/cm.yaml"}
	}
	assert.Diff(t, delta.Paths(), wantDelta)
	t.Logf("complete selected filesystem comparison: apps/cm.yaml and other.txt; delta=%v", delta.Paths())
	var expectedOpens [2]int64
	if (*testCaseInput) == "reader_reuse" {
		for i, input := range selected {
			expectedOpens[i] = input.FS.(*relationReader).opens.Swap(0)
		}
	}
	stores := []*store.Store{store.New(), store.New()}
	roots := []string{"/head", "/base"}
	if (*testCaseInput) == "comparison_policy" {
		roots[1] = "/head/nested"
	}
	indexes := make([]*manifest.DiscoveryIndex, 2)
	for i, st := range stores {
		st.AddObjects([]manifest.BaseManifest{source, owner})
		st.SetArtifact(source.Named(), selected[i])
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: roots[i], FS: fstest.MapFS{
			"apps/cm.yaml": {Data: []byte(roots[i])},
		}})
		indexes[i] = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			source.Named(): {
				File: "flux/source.yaml",
				Productions: []manifest.Production{
					{
						Source: manifest.BootstrapSourceID,
						File:   "flux/source.yaml",
						Path:   roots[i] + "/flux/source.yaml",
					},
				},
			},
			owner.Named(): {
				File:   "flux/ks.yaml",
				Pinned: i == 1,
				Refs:   []manifest.NamedResource{source.Named()},
				Productions: []manifest.Production{
					{
						Owner:  owner.Named(),
						Source: source.Named(),
						File:   "apps/cm.yaml",
						Path:   selected[i].LocalPath + "/apps/cm.yaml",
					},
				},
			},
		}, Claims: map[string][]manifest.NamedResource{}, Readers: map[string][]manifest.FileReader{}}
		if (*testCaseInput) == "authored_file" {
			record := indexes[i].Records[source.Named()]
			record.Path, record.File = roots[i]+"/bootstrap/source.yaml", "bootstrap/source.yaml"
			record.Productions = []manifest.Production{
				{
					Owner:    owner.Named(),
					Authored: source.Named(),
					Source:   source.Named(),
					File:     "flux/source.yaml",
					Path:     selected[i].LocalPath + "/flux/source.yaml",
				},
			}
			indexes[i].Records[source.Named()] = record
		}
		coordinate := "apps/cm.yaml"
		if i == 1 {
			coordinate = "../selected/apps/cm.yaml"
		}
		indexes[i].Claims[filepath.ToSlash(filepath.Dir(coordinate))+"/"] = []manifest.NamedResource{
			owner.Named(),
		}
		indexes[i].Readers[coordinate] = []manifest.FileReader{{Reader: owner.Named(), Source: source.Named()}}
		if (*testCaseInput) == "payload_edit" {
			indexes[i].Claims = nil
			indexes[i].Readers = nil
		}
		if (*testCaseInput) == "ownership_only" {
			record := indexes[i].Records[owner.Named()]
			record.Productions = []manifest.Production{
				{Source: manifest.BootstrapSourceID, File: "flux/ks.yaml", Path: roots[i] + "/flux/ks.yaml"},
			}
			indexes[i].Records[owner.Named()] = record
		}
	}
	if (*testCaseInput) == "comparison_policy" || (*testCaseInput) == "reader_reuse" {
		for i, st := range stores {
			st.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{LocalPath: roots[i], FS: selected[i].FS},
			)
		}
	}
	if (*testCaseInput) == "reader_reuse" {
		other := *source
		other.Name = "other"
		for i, st := range stores {
			st.AddObject(&other)
			art := *selected[i]
			art.LocalPath = "/equal"
			st.SetArtifact(other.Named(), &art)
		}
	}
	testFilterEqualSelectedSourcesCaseCompletion(t, &testFilterEqualSelectedSourcesCaseCompletionState{testCaseInput: &testCaseInput, source: &source, owner: &owner, selected: &selected, err: &err, expectedOpens: &expectedOpens, stores: &stores, roots: &roots, indexes: &indexes})
}

type testQualifyContentFingerprintCaseCompletionState struct {
	testCaseInput **string
	st            *[2]*store.Store
	id            *manifest.NamedResource
	readers       *[2]*relationReader
}

func testQualifyContentFingerprintCaseCompletion(t *testing.T, scope *testQualifyContentFingerprintCaseCompletionState) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if (*(*scope.testCaseInput)) == "canceled" {
		cancel()
	}
	changes := change.NewSet(nil)
	if (*(*scope.testCaseInput)) == "bootstrap" {
		changes = nil
	}
	qualified, err := change.Qualify(ctx, changes, (*scope.st)[0], (*scope.st)[1])
	if (*(*scope.testCaseInput)) == "error" || (*(*scope.testCaseInput)) == "canceled" ||
		(*(*scope.testCaseInput)) == "callback_canceled" ||
		(*(*scope.testCaseInput)) == "callback_deadline" {
		want := fs.ErrPermission
		if (*(*scope.testCaseInput)) == "canceled" || (*(*scope.testCaseInput)) == "callback_canceled" {
			want = context.Canceled
		}
		if (*(*scope.testCaseInput)) == "callback_deadline" {
			want = context.DeadlineExceeded
		}
		assert.FatalIf(t, !errors.Is(err, want), "change.Qualify error = %v, want %v", err, want)
		assert.Equal(t, errors.Is(err, manifest.ErrInput), (*(*scope.testCaseInput)) == "error")
		assert.Equal(t, errors.Is(err, manifest.ErrFlux), (*(*scope.testCaseInput)) == "error")
		return
	}
	assert.NoError(t, err)
	var paths []string
	if (*(*scope.testCaseInput)) != "immutable" && (*(*scope.testCaseInput)) != "mutable" {
		paths = []string{"file"}
	}
	assert.Diff(t, sourceChanges(qualified, (*scope.id)).Paths(), paths)
	for _, reader := range *scope.readers {
		assert.Equal(t, reader.opens.Load() == 0, paths == nil)
	}
}

type testFilterEqualSelectedSourcesCaseCompletionState struct {
	testCaseInput **string
	source        **manifest.GitRepository
	owner         **manifest.Kustomization
	selected      *[]*store.SourceArtifact
	err           *error
	expectedOpens *[2]int64
	stores        *[]*store.Store
	roots         *[]string
	indexes       *[]*manifest.DiscoveryIndex
}

func testFilterEqualSelectedSourcesCaseCompletion(t *testing.T, scope *testFilterEqualSelectedSourcesCaseCompletionState) {
	t.Helper()
	var changes *change.Set
	paths := []string{"apps/cm.yaml"}
	if (*(*scope.testCaseInput)) == "payload_edit" || (*(*scope.testCaseInput)) == "ownership_only" {
		paths = nil
	}
	if (*(*scope.testCaseInput)) == "authored_file" {
		paths = []string{"bootstrap/source.yaml"}
	}
	if (*(*scope.testCaseInput)) == "declaration_edit" {
		paths = []string{"flux/source.yaml"}
	}
	if (*(*scope.testCaseInput)) == "unpaired_pinned" {
		(*scope.selected)[0].FS = nil
		paths = []string{"../selected/apps/cm.yaml"}
	}
	if (*(*scope.testCaseInput)) == "unavailable" {
		(*scope.selected)[1].FS = nil
	}
	supplied := change.NewSet(paths)
	if (*(*scope.testCaseInput)) == "missing_bootstrap" {
		(*scope.stores)[0].SetArtifact(manifest.BootstrapSourceID, nil)
		_, qualifyErr := change.Qualify(t.Context(), nil, (*scope.stores)[0], (*scope.stores)[1])
		assert.Equal(t, errors.Is(qualifyErr, manifest.ErrInput), true)
		return
	}
	compareInput := supplied
	if (*(*scope.testCaseInput)) == "comparison_policy" || (*(*scope.testCaseInput)) == "reader_reuse" {
		compareInput = nil
	}
	changes, (*scope.err) = change.Qualify(t.Context(), compareInput, (*scope.stores)[0], (*scope.stores)[1])
	assert.NoError(t, (*scope.err))
	assert.Diff(t, supplied.Paths(), change.NewSet(paths).Paths())
	assert.Equal(t, sourceChanges(supplied, (*scope.source).Named()) == nil, true)
	if (*(*scope.testCaseInput)) == "reader_reuse" {
		for i, input := range *scope.selected {
			assert.Equal(t, input.FS.(*relationReader).opens.Load(), (*scope.expectedOpens)[i])
		}
	}
	if (*(*scope.testCaseInput)) == "comparison_policy" {
		assert.Equal(t, sourceChanges(changes, (*scope.source).Named()).Len(), 0)
		assert.Equal(t, changes.Contains("nested/extra.txt"), true)
	}
	if (*(*scope.testCaseInput)) == "artifact_replaced" {
		for i, st := range *scope.stores {
			replacement := *(*scope.selected)[i]
			replacement.Content = "replacement"
			replacement.FS = fstest.MapFS{"apps/cm.yaml": {Data: []byte("replacement\n")}}
			st.SetArtifact((*scope.source).Named(), &replacement)
		}
	}
	if (*(*scope.testCaseInput)) == "declaration_replaced" {
		replacement := *(*scope.source)
		replacement.URL = "git://fixture.invalid/replaced"
		for _, st := range *scope.stores {
			st.AddObject(&replacement)
		}
	}
	var kept [2][]string
	for i := range *scope.stores {
		kept[i] = newAdmission(
			t,
			admissionConfig{changes: changes, index: (*scope.indexes)[i], inventory: (*scope.stores)[i], root: (*scope.roots)[i]},
		).KeepNames()
		t.Logf("side %d exact kept identities: %v", i, kept[i])
		want := []string{}
		if (*(*scope.testCaseInput)) == "payload_edit" || (*(*scope.testCaseInput)) == "ownership_only" ||
			(*(*scope.testCaseInput)) == "declaration_edit" ||
			(*(*scope.testCaseInput)) ==
				"authored_file" ||
			i == 0 &&
				((*(*scope.testCaseInput)) == "artifact_replaced" || (*(*scope.testCaseInput)) == "declaration_replaced" || (*(*scope.testCaseInput)) == "unavailable") {
			want = []string{(*scope.source).Named().String(), (*scope.owner).Named().String()}
			slices.Sort(want)
		}
		assert.Diff(t, kept[i], want)
	}
	if (*(*scope.testCaseInput)) == "equal" || (*(*scope.testCaseInput)) == "equal_labels" ||
		(*(*scope.testCaseInput)) == "payload_edit" ||
		(*(*scope.testCaseInput)) == "declaration_edit" ||
		(*(*scope.testCaseInput)) ==
			"authored_file" {
		assert.Diff(t, kept[0], kept[1])
	}
}
