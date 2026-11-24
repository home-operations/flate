package loader

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/change/inputprojection"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/filtertest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestReaderFinalizer_ChartDependencies(t *testing.T) {
	t.Parallel()
	for _, missing := range []bool{false, true} {
		name := "present"
		if missing {
			name = "missing"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(
				t,
				root,
				"parent/Chart.yaml",
				"apiVersion: v2\nname: parent\nversion: 1.0.0\ndependencies:\n- name: child\n  version: 1.0.0\n  repository: file://../child\n",
			)
			if !missing {
				testutil.WriteFile(t, root, "child/Chart.yaml", "apiVersion: v2\nname: child\nversion: 1.0.0\n")
				testutil.WriteFile(
					t,
					root,
					"child/templates/cm.yaml",
					"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: child\n",
				)
			}
			st := store.New()
			admitForTest(st, root)
			l := New(st)
			l.mountSources()
			rf := readerFinalizer{
				loader:         l,
				repoRoot:       root,
				charts:         map[string]chartReaders{},
				templateBodies: map[string][][]byte{},
			}
			inputs, err := rf.chartNames(t.Context(), filepath.Join(root, "parent"), "parent")
			assert.NoError(t, err)
			if missing {
				assert.Diff(t, inputs.missing, []string{"child"})
			} else {
				assert.Equal(t, slices.Contains(inputs.names, "child/templates/cm.yaml"), true)
				assert.Equal(t, len(rf.templateBodies[filepath.Join(root, "parent")]), 1)
			}
			assert.Equal(t, inputs.remote, false)
		})
	}
	t.Run("malformed_local_repository", func(t *testing.T) {
		t.Parallel()
		testChartDependencyChange(t, chartDependencyChange{
			file: "repository.yaml", namespace: "charts", before: true, after: true, malformedLocal: true,
		})
	})
}

func TestChartFiles_PartialDependencies(t *testing.T) {
	t.Parallel()
	input := fstest.MapFS{
		"parent/Chart.yaml": {Data: []byte(`apiVersion: v2
name: parent
version: 1.0.0
dependencies:
- name: invalid
  version: 1.0.0
  repository: file://../../outside
- name: missing
  version: 1.0.0
  repository: file://../missing
- name: present
  version: 1.0.0
  repository: file://../present
- name: remote
  version: 1.0.0
  repository: https://example.invalid
`)},
		"present/Chart.yaml":        {Data: []byte("apiVersion: v2\nname: present\nversion: 1.0.0\n")},
		"present/templates/cm.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: child}\n")},
	}
	files, bodies, err := chartFiles(t.Context(), fileInput{fs: input, name: "parent"}, "parent", true)
	assert.NoError(t, err)
	assert.Diff(t, files.names, []string{
		"parent/Chart.yaml",
		"parent/.helmignore",
		"present/.helmignore",
		"present/Chart.yaml",
		"present/templates/cm.yaml",
	})
	assert.Diff(t, files.missing, []string{"missing"})
	assert.Equal(t, files.remote, true)
	assert.Diff(t, bodies, [][]byte{input["present/templates/cm.yaml"].Data})
}

func TestFilter_ChartDependencyRepositoryInputs(t *testing.T) {
	t.Parallel()
	for _, tc := range []chartDependencyChange{
		{name: "full_repository_edit", file: "repository.yaml", namespace: "charts", before: true, after: true, selected: true, full: true},
		{name: "full_secret_edit", file: "secret.yaml", namespace: "charts", before: true, after: true, selected: true, full: true},
		{name: "repository_edit", file: "repository.yaml", namespace: "charts", before: true, after: true, selected: true},
		{name: "repository_creation", file: "repository.yaml", namespace: "charts", after: true, selected: true},
		{name: "repository_deletion", file: "repository.yaml", namespace: "charts", before: true, selected: true},
		{name: "secret_edit", file: "secret.yaml", namespace: "charts", before: true, after: true, selected: true},
		{name: "secret_creation", file: "secret.yaml", namespace: "charts", after: true, selected: true},
		{name: "secret_deletion", file: "secret.yaml", namespace: "charts", before: true, selected: true},
		{name: "certificate_edit", file: "certificate.yaml", namespace: "charts", before: true, after: true, selected: true},
		{name: "certificate_creation", file: "certificate.yaml", namespace: "charts", after: true, selected: true},
		{name: "certificate_deletion", file: "certificate.yaml", namespace: "charts", before: true, selected: true},
		{name: "other_namespace", file: "secret.yaml", namespace: "other", before: true, after: true},
		{name: "sibling_edit", file: "child/Chart.yaml", namespace: "charts", local: true, before: true, after: true, selected: true},
		{name: "sibling_creation", file: "child/Chart.yaml", namespace: "charts", local: true, after: true, selected: true},
		{name: "sibling_deletion", file: "child/Chart.yaml", namespace: "charts", local: true, before: true, selected: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testChartDependencyChange(t, tc)
		})
	}
}

type chartDependencyChange struct {
	name, file, namespace string
	before, after         bool
	selected              bool
	local                 bool
	full                  bool
	malformedLocal        bool
}

func testChartDependencyChange(t *testing.T, tc chartDependencyChange) {
	t.Helper()
	var loaders [2]*Loader
	var stores [2]*store.Store
	var indexes [2]*manifest.DiscoveryIndex
	for side := range loaders {
		root := t.TempDir()
		files := map[string]string{
			"flux/release.yaml":     "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: release, namespace: apps}\nspec:\n  chart:\n    spec:\n      chart: parent\n      sourceRef: {kind: GitRepository, name: input, namespace: charts}\n",
			"parent/Chart.yaml":     "apiVersion: v2\nname: parent\nversion: 1.0.0\ndependencies:\n- name: child\n  version: 1.0.0\n  repository: https://repo.example\n",
			"flux/repository.yaml":  "apiVersion: source.toolkit.fluxcd.io/v1\nkind: HelmRepository\nmetadata: {name: declared, namespace: " + tc.namespace + "}\nspec: {url: https://repo.example, secretRef: {name: auth}, certSecretRef: {name: tls}}\n",
			"flux/secret.yaml":      "apiVersion: v1\nkind: Secret\nmetadata: {name: auth, namespace: " + tc.namespace + "}\nstringData: {password: before}\n",
			"flux/certificate.yaml": "apiVersion: v1\nkind: Secret\nmetadata: {name: tls, namespace: " + tc.namespace + "}\nstringData: {ca.crt: before}\n",
		}
		changedPath := "flux/" + tc.file
		if tc.local {
			changedPath = tc.file
			files["parent/Chart.yaml"] = strings.ReplaceAll(
				files["parent/Chart.yaml"],
				"https://repo.example",
				"file://../child",
			)
			files[changedPath] = "apiVersion: v2\nname: child\nversion: 1.0.0\ndescription: before\n"
		}
		if tc.malformedLocal {
			files["parent/Chart.yaml"] = strings.ReplaceAll(
				files["parent/Chart.yaml"], "https://repo.example", "file://%zz/child",
			)
		}
		switch {
		case !tc.before && side == 0 || !tc.after && side == 1:
			delete(files, changedPath)
		case side == 1 && tc.file == "repository.yaml":
			files[changedPath] = strings.ReplaceAll(files[changedPath], "repo.example", "repo.changed")
		case side == 1:
			files[changedPath] = strings.ReplaceAll(files[changedPath], "before", "after")
		}
		input := fstest.MapFS{}
		for name, body := range files {
			testutil.WriteFile(t, root, name, body)
			input[name] = &fstest.MapFile{Data: []byte(body)}
		}
		st := store.New()
		admitForTest(st, root)
		st.SetArtifact(
			manifest.BootstrapSourceID,
			&store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, LocalRoot: root, FS: input},
		)
		source := &manifest.GitRepository{Name: "input", Namespace: "charts"}
		st.AddObject(source)
		st.SetArtifact(
			source.Named(),
			&store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, LocalRoot: root, FS: input},
		)
		l := New(st)
		l.SourceRoot = root
		_, err := l.Load(t.Context(), filepath.Join(root, "flux"), manifest.NamedResource{}, nil)
		assert.NoError(t, err)
		assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
		if tc.full || tc.malformedLocal {
			assertChartDependencyReaders(t, l, tc.full)
		}
		loaders[side], stores[side], indexes[side] = l, st, l.Index
	}
	changes, err := change.Qualify(t.Context(), nil, change.SourcePair{Stores: stores}, manifest.WorkScope{})
	assert.NoError(t, err)
	changes, err = changes.WithReaders(
		t.Context(),
		indexes,
		[2]map[manifest.NamedResource]inputprojection.Projection{},
		manifest.WorkScope{},
	)
	assert.NoError(t, err)
	id := testutil.HelmReleaseID("apps", "release")
	for side, l := range loaders {
		l.Options.Changes = changes
		l.side, l.sideKnown = manifest.TreeSide(side), true
		configureSelection(t, l, selection.Request{Changed: !tc.full})
		proposal, err := prepareSelection(t.Context(), t, l, selection.PrepareRequest{Invocation: true})
		assert.NoError(t, err)
		assert.ErrorIf(
			t,
			slices.Contains(proposal.Selected, id) != tc.selected,
			"side %d selected=%v, want release selection %v",
			side,
			proposal.Selected,
			tc.selected,
		)
		assert.Equal(t, len(proposal.Request.History), 0)
		proposal.Retire()
		assert.Equal(t, len(l.Store.Warnings()), 0)
		assert.Equal(t, l.Store.GetArtifact(id, store.ArtifactContent) == nil, true)
	}
}

func assertChartDependencyReaders(t *testing.T, l *Loader, want bool) {
	t.Helper()
	release := testutil.HelmReleaseID("apps", "release")
	repository := (&manifest.HelmRepository{Name: "declared", Namespace: "charts"}).Named()
	occurrence, found := l.Occurrence(release)
	assert.Equal(t, found, true)
	assert.ErrorIf(t, slices.ContainsFunc(occurrence.Refs, func(ref manifest.Reference) bool {
		return ref.NamedResource == repository && ref.ContentInput()
	}) != want, "release repository relation: got %v, want presence %t", occurrence.Refs, want)
	for _, file := range []string{"flux/repository.yaml", "flux/secret.yaml"} {
		readers := l.Index.Readers[file]
		assert.ErrorIf(t, slices.ContainsFunc(readers, func(reader manifest.FileReader) bool {
			return reader.Reader == release
		}) != want, "%s readers: got %v, want release presence %t", file, readers, want)
	}
}

func TestReaderFinalizer_RepositoryProjection(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"indexed", "live", "emitted"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			l := New(st)
			l.SourceProvider = filtertest.CheckoutProvider(st, l.SourceProvider)
			l.Index.Readers = map[string][]manifest.FileReader{}
			release := &manifest.HelmRelease{Name: "release", Namespace: "apps"}
			repository := &manifest.HelmRepository{
				Name:      "repo",
				Namespace: "apps",
				URL:       "https://repo.example",
				SecretRef: &manifest.LocalObjectReference{Name: "auth"},
			}
			secret := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "auth"}
			l.Index.Records[release.Named()] = manifest.DiscoveryRecord{Declaration: release, File: "release.yaml"}
			l.Index.Records[repository.Named()] = manifest.DiscoveryRecord{
				Declaration: repository,
				Builder:     "repo",
				File:        "repository.yaml",
			}
			l.Index.Records[secret] = manifest.DiscoveryRecord{File: "secret.yaml"}
			unrelated := repository.WithNamespace("other")
			l.Index.Records[unrelated.Named()] = manifest.DiscoveryRecord{
				Declaration: unrelated,
				Builder:     "repo",
				File:        "other-repository.yaml",
			}
			occurrence := compactOccurrence(release, manifest.TemplateProjection{})
			occurrence.object = release
			original := occurrence.facts
			l.locations = map[manifest.NamedResource]authoredOccurrence{release.Named(): occurrence}
			if name == "live" {
				st.AddObject(release)
				st.AddObject(repository)
			}
			if name == "emitted" {
				l.locations = nil
				l.Index.Records[release.Named()] = manifest.DiscoveryRecord{Declaration: release, Builder: "rendered"}
			}
			rf := readerFinalizer{
				loader:       l,
				objects:      map[manifest.NamedResource]manifest.BaseManifest{release.Named(): release},
				repositories: map[string][]manifest.NamedResource{},
				visited:      map[manifest.NamedResource]struct{}{},
			}
			assert.NoError(t, rf.observeRepositories(t.Context(), release.Named(), release.Namespace, false))
			refs := slices.Collect(l.References(release.Named()))
			assert.Equal(t, slices.ContainsFunc(refs, func(ref manifest.Reference) bool {
				return ref.NamedResource == repository.Named() && ref.ContentInput() && ref.Gate == 0
			}), true)
			assert.Equal(t, len(original.refs), 0)
			assert.Equal(t, len(l.Index.Readers["repository.yaml"]), 1)
			assert.Equal(t, len(l.Index.Readers["secret.yaml"]), 1)
			assert.Equal(t, len(l.Index.Readers["other-repository.yaml"]), 0)
			view, found := l.Occurrence(release.Named())
			assert.Equal(t, found, true)
			assert.Equal(t, slices.Contains(view.Reverse, repository.Named()), true)
			for _, file := range []string{"repository.yaml", "secret.yaml"} {
				l.Options.Changes = detectChanges(t, []string{file}, []string{file})
				configureSelection(t, l, selection.Request{Changed: true})
				proposal, err := prepareSelection(t.Context(), t, l, selection.PrepareRequest{Invocation: true})
				assert.NoError(t, err)
				assert.Equal(t, slices.Contains(proposal.Needed, release.Named()), true)
				assert.Equal(t, slices.Contains(proposal.Needed, unrelated.Named()), false)
				assert.Equal(t, len(proposal.Request.History), 0)
			}
		})
	}
}
