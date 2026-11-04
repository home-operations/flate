package loader

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
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
		configureFingerprint(&testCaseInput, art, i)
		st[i].AddObject(source)
		st[i].SetArtifact(id, art)
	}
	testQualifyContentFingerprintCaseCompletion(
		t,
		&testQualifyContentFingerprintCaseCompletionState{
			testCaseInput: &testCaseInput,
			st:            &st,
			id:            &id,
			readers:       &readers,
		},
	)
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
	configureSelectedSources(testCaseInput, selected)
	delta, err := change.Detect(t.Context(), selected[0], selected[1], manifest.WorkScope{})
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
	populateSelectedIndexes(
		&selectedIndexInput{
			mode:     testCaseInput,
			source:   source,
			owner:    owner,
			selected: selected,
			roots:    roots,
			stores:   stores,
			indexes:  indexes,
		},
	)
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
	testFilterEqualSelectedSourcesCaseCompletion(
		t,
		&testFilterEqualSelectedSourcesCaseCompletionState{
			testCaseInput: &testCaseInput,
			source:        &source,
			owner:         &owner,
			selected:      &selected,
			err:           &err,
			expectedOpens: &expectedOpens,
			stores:        &stores,
			roots:         &roots,
			indexes:       &indexes,
		},
	)
}

type testQualifyContentFingerprintCaseCompletionState struct {
	testCaseInput **string
	st            *[2]*store.Store
	id            *manifest.NamedResource
	readers       *[2]*relationReader
}

func testQualifyContentFingerprintCaseCompletion(
	t *testing.T,
	scope *testQualifyContentFingerprintCaseCompletionState,
) {
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
	qualified, err := change.Qualify(ctx, changes, [2]*store.Store{scope.st[0], scope.st[1]}, manifest.WorkScope{})
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

func testFilterEqualSelectedSourcesCaseCompletion(
	t *testing.T,
	scope *testFilterEqualSelectedSourcesCaseCompletionState,
) {
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
		_, qualifyErr := change.Qualify(
			t.Context(),
			nil,
			[2]*store.Store{(*scope.stores)[0], (*scope.stores)[1]},
			manifest.WorkScope{},
		)
		assert.Equal(t, errors.Is(qualifyErr, manifest.ErrInput), true)
		return
	}
	compareInput := supplied
	if (*(*scope.testCaseInput)) == "comparison_policy" || (*(*scope.testCaseInput)) == "reader_reuse" {
		compareInput = nil
	}
	changes, (*scope.err) = change.Qualify(
		t.Context(),
		compareInput,
		[2]*store.Store{(*scope.stores)[0], (*scope.stores)[1]},
		manifest.WorkScope{},
	)
	assert.NoError(t, (*scope.err))
	assert.Diff(t, supplied.Paths(), change.NewSet(paths).Paths())
	assert.Equal(t, sourceChanges(supplied, (*scope.source).Named()) == nil, true)
	if (*(*scope.testCaseInput)) == "reader_reuse" {
		for i, input := range *scope.selected {
			assert.Equal(t, input.FS.(*relationReader).opens.Load(), scope.expectedOpens[i])
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
	assertSelectedSourceNames(t, scope, changes)
}

func configureFingerprint(mode **string, art *store.SourceArtifact, i int) {
	if (*(*mode)) != "mutable" && (*(*mode)) != "empty" &&
		(*(*mode)) != "unknown" &&
		(*(*mode)) != "changed" &&
		(*(*mode)) != "error" &&
		(*(*mode)) !=
			"callback_canceled" &&
		(*(*mode)) !=
			"callback_deadline" {
		return
	}
	art.Key, art.Content = "", ""
	if (*(*mode)) == "unknown" {
		return
	}
	art.ContentFingerprint = func(context.Context, fs.FS) (string, error) {
		if (*(*mode)) == "callback_canceled" {
			return "", context.Canceled
		}
		if (*(*mode)) == "callback_deadline" {
			return "", context.DeadlineExceeded
		}
		if (*(*mode)) == "error" {
			return "", fs.ErrPermission
		}
		if (*(*mode)) == "empty" {
			return "", nil
		}
		if (*(*mode)) == "changed" {
			return string(rune('a' + i)), nil
		}
		return "selected bytes", nil
	}
}

func configureSelectedSources(testCaseInput *string, selected []*store.SourceArtifact) {
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
}

type selectedIndexInput struct {
	mode     *string
	source   *manifest.GitRepository
	owner    *manifest.Kustomization
	selected []*store.SourceArtifact
	roots    []string
	stores   []*store.Store
	indexes  []*manifest.DiscoveryIndex
}

func populateSelectedIndexes(fixture *selectedIndexInput) {
	for i, st := range fixture.stores {
		st.AddObjects([]manifest.BaseManifest{fixture.source, fixture.owner})
		st.SetArtifact(fixture.source.Named(), fixture.selected[i])
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: fixture.roots[i], FS: fstest.MapFS{
			"apps/cm.yaml": {Data: []byte(fixture.roots[i])},
		}})
		fixture.indexes[i] = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			fixture.source.Named(): {
				File: "flux/source.yaml",
				Productions: []manifest.Production{
					{
						Source: manifest.BootstrapSourceID,
						File:   "flux/source.yaml",
						Path:   fixture.roots[i] + "/flux/source.yaml",
					},
				},
			},
			fixture.owner.Named(): {
				File:   "flux/ks.yaml",
				Pinned: i == 1,
				Refs:   []manifest.NamedResource{fixture.source.Named()},
				Productions: []manifest.Production{
					{
						Owner:  fixture.owner.Named(),
						Source: fixture.source.Named(),
						File:   "apps/cm.yaml",
						Path:   fixture.selected[i].LocalPath + "/apps/cm.yaml",
					},
				},
			},
		}, Claims: map[string][]manifest.NamedResource{}, Readers: map[string][]manifest.FileReader{}}
		if (*fixture.mode) == "authored_file" {
			record := fixture.indexes[i].Records[fixture.source.Named()]
			record.Path, record.File = fixture.roots[i]+"/bootstrap/source.yaml", "bootstrap/source.yaml"
			record.Productions = []manifest.Production{
				{
					Owner:    fixture.owner.Named(),
					Authored: fixture.source.Named(),
					Source:   fixture.source.Named(),
					File:     "flux/source.yaml",
					Path:     fixture.selected[i].LocalPath + "/flux/source.yaml",
				},
			}
			fixture.indexes[i].Records[fixture.source.Named()] = record
		}
		coordinate := "apps/cm.yaml"
		if i == 1 {
			coordinate = "../selected/apps/cm.yaml"
		}
		fixture.indexes[i].Claims[filepath.ToSlash(filepath.Dir(coordinate))+"/"] = []manifest.NamedResource{
			fixture.owner.Named(),
		}
		fixture.indexes[i].Readers[coordinate] = []manifest.FileReader{
			{Reader: fixture.owner.Named(), Source: fixture.source.Named()},
		}
		if (*fixture.mode) == "payload_edit" {
			fixture.indexes[i].Claims = nil
			fixture.indexes[i].Readers = nil
		}
		if (*fixture.mode) == "ownership_only" {
			record := fixture.indexes[i].Records[fixture.owner.Named()]
			record.Productions = []manifest.Production{
				{Source: manifest.BootstrapSourceID, File: "flux/ks.yaml", Path: fixture.roots[i] + "/flux/ks.yaml"},
			}
			fixture.indexes[i].Records[fixture.owner.Named()] = record
		}
	}
}

func assertSelectedSourceNames(
	t *testing.T,
	scope *testFilterEqualSelectedSourcesCaseCompletionState,
	changes *change.Set,
) {
	t.Helper()
	var kept [2][]string
	for i := range *scope.stores {
		kept[i] = newAdmission(
			t,
			admissionConfig{
				changes:   changes,
				index:     (*scope.indexes)[i],
				inventory: (*scope.stores)[i],
				root:      (*scope.roots)[i],
			},
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
