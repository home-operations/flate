package loader

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/source/gitrepo"
	"github.com/home-operations/flate/internal/source/gittree"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func testAdmitGraphProjectionCase4(t *testing.T, testCaseInput *struct {
	name   string
	hrDir  string
	pinned bool
	files  map[string]string
	scans  []string
	want   []string
	wantKS []string
	alias  bool
},
) {
	t.Helper()

	root := t.TempDir()
	for file, body := range testCaseInput.files {
		testutil.WriteFile(t, root, file, body)
	}
	hrDir := "apps"
	if _, ok := testCaseInput.files["base/kustomization.yaml"]; ok {
		hrDir = "base"
	}
	if testCaseInput.hrDir != "" {
		hrDir = testCaseInput.hrDir
	}
	hrDoc := `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: app}
spec:
  chartRef: {kind: OCIRepository, name: chart}
  valuesFrom: [{kind: ConfigMap, name: values}, {kind: Secret, name: optional, optional: true}]
`
	testutil.WriteFile(t, root, hrDir+"/hr.yaml", hrDoc)
	l := loaderFixture(store.New(), nil)
	l.SourceRoot = root
	admitForTest(l.Store, root)
	if testCaseInput.alias {
		location, err := gitrepo.Locate(t.Context(), root, root)
		assert.NoError(t, err)
		input, err := gittree.AdmitDirectory(t.Context(), location)
		assert.NoError(t, err)
		l.Store.SetArtifact(manifest.BootstrapSourceID, input)
		view, _, err := gittree.New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			gittree.ViewOptions{Mode: gittree.Source, Current: input.FS},
		)
		assert.NoError(t, err)
		id := manifest.NamedResource{
			Group:     "source.toolkit.fluxcd.io",
			Kind:      manifest.KindGitRepository,
			Namespace: "flux-system",
			Name:      "cluster",
		}
		l.Store.AddObject(
			&manifest.GitRepository{Name: id.Name, Namespace: id.Namespace, URL: "file://" + root},
		)
		l.Store.SetArtifact(
			id,
			&store.SourceArtifact{
				Kind:       id.Kind,
				LocalRoot:  root,
				LocalPath:  filepath.Join(filepath.Dir(root), ".flate-sources", "alias", id.Namespace, id.Name),
				InputAlias: true,
				FS:         view,
			},
		)
	}
	if testCaseInput.name == "self_claim_transformer_fallback" {
		for _, namespace := range []string{"flux-system", "myns"} {
			source := &manifest.GitRepository{Name: "cluster", Namespace: namespace}
			l.Store.AddObject(source)
			l.Store.SetArtifact(
				source.Named(),
				&store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: os.DirFS(root)},
			)
		}
	}
	if testCaseInput.pinned {
		artifact := t.TempDir()
		testutil.WriteFile(t, artifact, "apps/kustomization.yaml", "resources: [hr.yaml]\n")
		testutil.WriteFile(t, artifact, "apps/hr.yaml", hrDoc)
		l.Store.SetArtifact(
			manifest.NamedResource{
				Group:     "source.toolkit.fluxcd.io",
				Kind:      manifest.KindGitRepository,
				Namespace: "flux-system",
				Name:      "cluster",
			},
			&store.SourceArtifact{
				LocalRoot: root,
				Key:       "pinned",
				Content:   "pinned",
				FS:        os.DirFS(artifact),
				LocalPath: artifact,
			},
		)
		l.documents = readerForTest(root)
		l.documents.views[artifact] = os.DirFS(artifact)
		l.documents.read(filepath.Join(artifact, "apps"))
	}
	loadProjectionScans(t, l, root, testCaseInput)
	l.Admit(root)
	if testCaseInput.pinned || testCaseInput.alias {
		ks := l.Store.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization)[0]
		artifact := l.Store.GetArtifact(ks.SourceID(), store.ArtifactContent).(*store.SourceArtifact)
		{
			_, err := loadForTest(t.Context(), t, l, filepath.Join(artifact.LocalPath, "apps"))
			assert.NoError(t, err)
		}
	}
	var original, snapshot *manifest.Kustomization
	var raw manifest.DiscoveryRecord
	if len(testCaseInput.wantKS) != 0 {
		id := testutil.KustomizationID("flux-system", "myapp")
		var r manifest.DiscoveryRecord
		//nolint:gocritic // The iterator supplies filtered value snapshots through its callback.
		for _, record := range l.Index.Files() {
			if record.Authored == id {
				r = record
				break
			}
		}
		authored := l.authoredObject(r.Path, r.Authored)
		assert.FatalIf(
			t,
			authored.Doc == nil || manifest.DocIdentity(authored.Doc) != r.Authored,
			"projection lost authored document: %v",
			authored.Doc,
		)
		original, _ = authored.Obj.(*manifest.Kustomization)
		assert.FatalIf(t, original == nil, "%s", "authored Kustomization missing")
		snapshot = original.Clone()
		raw = r
	}
	testAdmitGraphProjectionCase4Completion(
		t,
		&testAdmitGraphProjectionCase4CompletionState{
			testCaseInput: &testCaseInput,
			root:          &root,
			l:             &l,
			original:      &original,
			snapshot:      &snapshot,
			raw:           &raw,
		},
	)
}

type testAdmitGraphProjectionCase4CompletionState struct {
	testCaseInput **struct {
		name   string
		hrDir  string
		pinned bool
		files  map[string]string
		scans  []string
		want   []string
		wantKS []string
		alias  bool
	}
	root     *string
	l        **Loader
	original **manifest.Kustomization
	snapshot **manifest.Kustomization
	raw      *manifest.DiscoveryRecord
}

func testAdmitGraphProjectionCase4Completion(t *testing.T, scope *testAdmitGraphProjectionCase4CompletionState) {
	t.Helper()
	(*scope.l).Admit((*scope.root))
	if (*scope.original) != nil {
		assertProjectionAuthored(t, scope)
	}
	if (*scope.testCaseInput).name == "self_claim" {
		id := testutil.KustomizationID("", "owner")
		{
			_, ok := (*scope.l).Index.Lookup(id)
			assert.FatalIf(
				t,
				!ok || (*scope.l).Store.GetObject(id) != nil,
				"%s",
				"namespace-less owner must remain indexed without independent publication",
			)
		}
		{
			err := (*scope.l).FinalizeIndex(t.Context(), (*scope.root), true)
			assert.NoError(t, err)
		}
		id.Namespace = "media"
		{
			_, ok := (*scope.l).Index.Lookup(id)
			assert.FatalIf(t, ok, "%s", "own coverage invented a second Kustomization identity")
		}
	}
	if (*scope.testCaseInput).pinned {
		{
			err := (*scope.l).FinalizeIndex(t.Context(), (*scope.root), true)
			assert.NoError(t, err)
		}
		r, _ := (*scope.l).Index.Lookup(testutil.HelmReleaseID("apps", "app"))
		readers := (*scope.l).Index.Readers[r.File]
		assert.FatalIf(
			t,
			len(readers) != 1 || readers[0].Reader != testutil.KustomizationID("flux-system", "pinned"),
			"committed source file %q readers = %v",
			r.File,
			readers,
		)
	}
	if (*scope.testCaseInput).name == "cross_tree" {
		ks, ok := (*scope.l).Store.Get[*manifest.Kustomization](testutil.KustomizationID("games", "games"))
		assert.FatalIf(t, !ok, "%s", "projected Kustomization missing")
		var deps []manifest.NamedResource
		for ref := range manifest.References(ks, manifest.ReferenceRelations) {
			if ref.Use == "dependsOn" {
				deps = append(deps, ref.NamedResource)
			}
		}
		assert.FatalIf(
			t,
			len(deps) != 1 || deps[0].Namespace != "games",
			"projected dependsOn namespace: %+v",
			deps,
		)
	}
	for _, ns := range (*scope.testCaseInput).want {
		id := testutil.HelmReleaseID(ns, "app")
		r, ok := (*scope.l).Index.Lookup(id)
		assert.FatalIf(t, !ok || len(r.Refs) != 3, "%s record=%+v present=%t", id, r, ok)
		for _, ref := range r.Refs {
			assert.FatalIf(t, ref.Namespace != ns, "%s ref=%s", id, ref)
		}
		assert.FatalIf(t, (*scope.l).Store.GetObject(id) != nil, "%s admitted before promotion", id)
	}
	{
		_, ok := (*scope.l).Index.Lookup(
			testutil.HelmReleaseID("", "app"),
		)
		assert.FatalIf(t, ok &&
			!slices.Contains((*scope.testCaseInput).want, ""), "%s", "stale raw consumer record")
	}
	{
		_, ok := (*scope.l).Index.Lookup(testutil.HelmReleaseID("flux-system", "app"))
		assert.FatalIf(t, ok, "%s", "ancestor fallback retained a stale consumer identity")
	}
	{
		_, ok := (*scope.l).Index.Lookup(testutil.HelmReleaseID("inner", "app"))
		assert.FatalIf(t, ok, "%s", "inner base namespace must not override its overlay")
	}
	if (*scope.testCaseInput).name == "inactive_namespace" {
		{
			_, ok := (*scope.l).Index.Lookup(testutil.HelmReleaseID("inactive", "app"))
			assert.FatalIf(t, ok, "%s", "inactive owner projected a second consumer namespace")
		}
	}
}

func loadProjectionScans(t *testing.T, l *Loader, root string, testCaseInput *struct {
	name   string
	hrDir  string
	pinned bool
	files  map[string]string
	scans  []string
	want   []string
	wantKS []string
	alias  bool
},
) {
	t.Helper()
	for _, scan := range testCaseInput.scans {
		{
			_, err := l.Load(t.Context(), filepath.Join(root, scan), manifest.NamedResource{}, nil)
			assert.NoError(t, err)
		}
		if testCaseInput.name == "named_ready" && scan == "flux" {
			l.Admit(root)
			if _, ok := l.Index.Lookup(testutil.HelmReleaseID("", "app")); !ok {
				t.Fatalf("unresolved source records before loading its path: %+v", l.Index.Records)
			}
			source := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
			l.Store.AddObject(source)
			l.Store.SetArtifact(
				source.Named(),
				&store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: os.DirFS(root)},
			)
		}
	}
}

func assertProjectionAuthored(t *testing.T, scope *testAdmitGraphProjectionCase4CompletionState) {
	t.Helper()
	{
		diff := cmp.Diff(
			(*scope.original),
			(*scope.snapshot),
			cmp.AllowUnexported(manifest.Kustomization{}),
			cmpopts.IgnoreFields(manifest.Kustomization{}, "projection"),
		)
		assert.FatalIf(t, diff != "", "authored object mutated (-want +got): %s", diff)
	}
	{
		diff := cmp.Diff(
			(*scope.l).authoredObject(scope.raw.Path, scope.raw.Authored).Obj,
			(*scope.snapshot),
			cmp.AllowUnexported(manifest.Kustomization{}),
			cmpopts.IgnoreFields(manifest.Kustomization{}, "projection"),
		)
		assert.FatalIf(t, diff != "", "authored object mutated (-want +got): %s", diff)
	}
	assert.Diff(t, (*scope.l).recordNamespaces(new((*scope.raw)), "myns"), (*scope.testCaseInput).wantKS)
	got, want := map[manifest.NamedResource][]manifest.NamedResource{}, map[manifest.NamedResource][]manifest.NamedResource{}
	//nolint:gocritic // The iterator supplies filtered value snapshots through its callback.
	for id, r := range (*scope.l).Index.Files() {
		if r.Path == scope.raw.Path {
			assert.Equal(t, r.Authored, scope.raw.Authored)
			got[id] = r.Refs
		}
	}
	for _, ns := range (*scope.testCaseInput).wantKS {
		id := testutil.KustomizationID(ns, "myapp")
		want[id] = []manifest.NamedResource{
			{
				Group:     "source.toolkit.fluxcd.io",
				Kind:      manifest.KindGitRepository,
				Namespace: ns,
				Name:      "cluster",
			},
		}
	}
	assert.Diff(t, got, want)
}
