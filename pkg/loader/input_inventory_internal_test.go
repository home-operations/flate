package loader

import (
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestFinalizeIndex_InputInventory(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, body, file string
	}{
		{"normalized base", "bases: [../shared]\n", "shared/cm.yaml"},
		{"component", "components: [../component]\n", "component/value.txt"},
		{"JSON patch", "patchesJson6902: [{path: ../inputs/value}]\n", "inputs/value"},
		{"strategic patch", "patchesStrategicMerge: [../inputs/value]\n", "inputs/value"},
		{"replacement", "replacements: [{path: ../inputs/value}]\n", "inputs/value"},
		{"configuration", "configurations: [../inputs/value]\n", "inputs/value"},
		{"CRD", "crds: [../inputs/value]\n", "inputs/value"},
		{"schema", "openapi: {path: ../inputs/value}\n", "inputs/value"},
		{"inline configuration", "generators:\n- |\n    apiVersion: builtin\n    kind: ConfigMapGenerator\n    metadata: {name: input}\n    files: [key=../inputs/value]\n", "inputs/value"},
		{"file configuration", "validators: [../plugin.yaml]\n", "inputs/value"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "apps/kustomization.yaml", tc.body)
			testutil.WriteFile(t, root, "shared/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "shared/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input}\n")
			testutil.WriteFile(
				t,
				root,
				"component/kustomization.yaml",
				"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nsecretGenerator: [{name: input, files: [value.txt]}]\n",
			)
			testutil.WriteFile(t, root, "component/value.txt", "component data")
			testutil.WriteFile(t, root, "inputs/value", "terminal data")
			testutil.WriteFile(
				t,
				root,
				"plugin.yaml",
				"apiVersion: builtin\nkind: SecretGenerator\nmetadata: {name: input}\nenvs: [../inputs/value]\n",
			)
			s := store.New()
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./apps"}
			s.AddObject(owner)
			idx := rendererFixture(t, New(s), root)
			assert.Diff(
				t,
				idx.Readers[tc.file],
				[]manifest.FileReader{{Reader: owner.Named(), Source: manifest.BootstrapSourceID}},
			)
			assert.Diff(t, ownersForTest(idx, tc.file), []manifest.NamedResource{owner.Named()})
			{
				assert.FatalIf(t, len(idx.Readers["inputs/unrelated"]) != 0, "%s", "unrelated sibling gained readers")
			}
		})
	}
}

func TestFinalizeIndex_InputCoordinatesAndAdmission(t *testing.T) {
	t.Parallel()
	for _, filename := range manifest.KustomizeBuilderFilenames {
		t.Run(filename, func(t *testing.T) {
			t.Parallel()
			testFinalizeIndexInputCoordinatesAndAdmissionCase(
				t,
				&filename,
			)
		},
		)
	}
}

func TestLoader_InputRolesDualResourceOutside(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: []\npatches: [{path: ../inputs/dual.yaml}]\n")
	testutil.WriteFile(t, root, "inputs/kustomization.yaml", "resources: [dual.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"inputs/dual.yaml",
		testutil.KustomizationYAML("dual", "authored", "  path: ./hidden\n"),
	)
	st := store.New()
	admitForTest(st, root)
	l := New(st)
	l.SourceRoot = root
	{
		_, err := l.Load(t.Context(), root)
		assert.NoError(t, err)
	}
	l.Admit(root)
	id := testutil.KustomizationID("authored", "dual")
	{
		_, ok := l.Index.Lookup(
			id,
		)
		assert.FatalIf(t, !ok || l.candidates[id].Obj == nil || st.GetObject(id) == nil ||
			len(
				l.documents.cache[filepath.Join(root, "inputs/dual.yaml")].objects,
			) != 1, "%s", "terminal role suppressed a separately declared resource")
	}
	file := filepath.Join(root, "inputs/dual.yaml")
	admitted := l.AdmitsRoot(l.IndexedObject(id, l.Index.Records[id]).Obj, file)
	assert.Equal(t, admitted, true)
	assert.Equal(t, l.terminalInput(file), false)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	assert.Equal(t, l.AdmitsRoot(l.IndexedObject(id, l.Index.Records[id]).Obj, file), admitted)
	assert.Equal(t, l.terminalInput(file), false)
}

func TestFinalizeIndex_AdmissionAndConcurrentQueries(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [target]\n")
	testutil.WriteFile(t, root, "target/kustomization.yaml", "resources: [../declarations, ../missing, broken.yaml]\n")
	testutil.WriteFile(t, root, "target/broken.yaml", "invalid: [\n")
	testutil.WriteFile(t, root, "declarations/kustomization.yaml", "resources: [peer.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"declarations/peer.yaml",
		testutil.KustomizationYAML("peer", "apps", "  path: ./target\n"),
	)
	st := store.New()
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "./target"}
	st.AddObject(parent)
	admitForTest(st, root)
	l := New(st)
	l.SourceRoot = root
	_, err := loadForTest(t.Context(), t, l, root)
	assert.NoError(t, err)
	peer, missing := testutil.KustomizationID("apps", "peer"), testutil.HelmReleaseID("apps", "missing")
	peerFile, missingFile := filepath.Join(root, "declarations/peer.yaml"), filepath.Join(root, "missing/release.yaml")
	l.Index.Records[missing] = manifest.DiscoveryRecord{
		Authored: missing,
		File:     "missing/release.yaml",
		Path:     missingFile,
	}
	l.Index.Readers = map[string][]manifest.FileReader{
		"missing": {{Reader: parent.Named(), Source: manifest.BootstrapSourceID}},
	}
	before := map[manifest.NamedResource]bool{
		peer:    l.AdmitsRoot(l.IndexedObject(peer, l.Index.Records[peer]).Obj, peerFile),
		missing: l.AdmitsRoot(l.IndexedObject(missing, l.Index.Records[missing]).Obj, missingFile),
	}
	assert.Equal(t, before[peer], true)
	assert.Equal(t, before[missing], false)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	assert.Diff(t, l.Index.Claims["target/"], []manifest.NamedResource{parent.Named(), peer})
	assert.Diff(
		t,
		l.Index.Readers["missing"],
		[]manifest.FileReader{
			{Reader: parent.Named(), Source: manifest.BootstrapSourceID},
			{Reader: peer, Source: manifest.BootstrapSourceID},
		},
	)
	assert.Equal(t, len(retainedErrorsForTest(l)), 1)
	assert.Equal(t, l.scopes == nil, true)
	frozen := snapshotIndexForTest(l.Index)
	for id, record := range frozen.Records {
		record.Inputs = slices.Clone(record.Inputs)
		record.ContentRoots = maps.Clone(record.ContentRoots)
		for source, roots := range record.ContentRoots {
			record.ContentRoots[source] = slices.Clone(roots)
		}
		frozen.Records[id] = record
	}
	type queryResult struct {
		admitted          map[manifest.NamedResource]bool
		owners, ancestors []manifest.NamedResource
		readers           []manifest.FileReader
	}
	ready := make(chan struct{}, 2)
	start := make(chan struct{})
	results := make(chan queryResult, 2)
	var workers sync.WaitGroup
	for range 2 {
		workers.Go(func() {
			ready <- struct{}{}
			<-start
			owners, ancestors := l.Index.OwnersOfFile("target/deleted.yaml")
			var readers []manifest.FileReader
			for reader := range l.Index.ReadersOfFile("missing/release.yaml") {
				readers = append(readers, reader)
			}
			results <- queryResult{admitted: map[manifest.NamedResource]bool{peer: l.AdmitsRoot(l.IndexedObject(peer, l.Index.Records[peer]).Obj, peerFile), missing: l.AdmitsRoot(l.IndexedObject(missing, l.Index.Records[missing]).Obj, missingFile)}, owners: owners, ancestors: ancestors, readers: readers}
		})
	}
	for range 2 {
		<-ready
	}
	close(start)
	workers.Wait()
	close(results)
	for result := range results {
		assert.Diff(t, result.admitted, before)
		assert.Diff(t, result.owners, []manifest.NamedResource{parent.Named(), peer})
		assert.Equal(t, len(result.ancestors), 0)
		assert.Diff(t, result.readers, frozen.Readers["missing"])
	}
	{
		diff := cmp.Diff(frozen, *l.Index, cmpopts.EquateErrors(), cmp.Comparer(manifest.Equal))
		assert.FatalIf(t, diff != "", "frozen index changed (-want +got): %s", diff)
	}
}

func TestOccurrences_InputSourceViews(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"mounted", "alias", "pinned"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			local := filepath.Join(root, ".artifacts", mode)
			input := fstest.MapFS{
				"entry/nested/kustomization.yaml": {
					Data: []byte("namePrefix: pre-\nnameSuffix: -post\nresources: [../../shared]\n"),
				},
				"shared/kustomization.yaml": {
					Data: []byte("resources: [resource.yaml]\npatches: [{path: ../inputs/patch}]\n"),
				},
				"shared/resource.yaml": {
					Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: mounted}\n"),
				},
			}
			testutil.WriteFile(t, local, "shared/kustomization.yaml", "patches: [{path: ../inputs/host-only}]\n")
			s := store.New()
			source := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
			s.AddObject(source)
			owner := &manifest.Kustomization{
				Name:      "owner",
				Namespace: "apps",
				Path:      "./entry",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{
					Kind:      manifest.KindGitRepository,
					Name:      source.Name,
					Namespace: source.Namespace,
				},
			}
			s.AddObject(owner)
			art := &store.SourceArtifact{LocalRoot: local, LocalPath: local, FS: input}
			if mode == "alias" {
				art.InputAlias, art.LocalRoot = true, filepath.Join(root, "subtree")
			}
			if mode == "pinned" {
				art.Key, art.Content = "pinned", "pinned"
			}
			s.SetArtifact(source.Named(), art)
			l := New(s)
			l.SourceRoot = root
			admitForTest(s, root)
			_, err := l.Load(t.Context(), filepath.Join(local, "entry"))
			assert.NoError(t, err)
			occurrences, err := l.Occurrences(t.Context(), filepath.Join(local, "entry"), "projected")
			assert.NoError(t, err)
			{
				assert.FatalIf(t, len(occurrences) != 1 || occurrences[0].Identity.Name != "pre-mounted-post" ||
					occurrences[0].Identity.Namespace != "projected", "source-view occurrence projection: %+v", occurrences)
			}
			wantSource, wantFile := local, "shared/resource.yaml"
			if mode == "alias" {
				wantSource, wantFile = root, "subtree/shared/resource.yaml"
			}
			assert.Equal(t, occurrences[0].Declaration, OccurrenceID{Source: wantSource, File: wantFile, Document: 0})
			assert.Equal(t, occurrences[0].object.Named().Namespace, "")
			assert.Equal(t, occurrences[0].object.Named().Name, "mounted")
			repeated, err := l.Occurrences(t.Context(), filepath.Join(local, "entry"), "projected")
			assert.NoError(t, err)
			assert.Equal(t, slices.Equal(repeated, occurrences), true)
			assert.Equal(t, len(l.scopes.projections[0])+len(l.scopes.projections[1]), 1)
			repeated[0].Declaration.File = "caller-owned"
			assert.Equal(t, occurrences[0].Declaration.File, wantFile)
		})
	}
}

func testFinalizeIndexInputCoordinatesAndAdmissionCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	parent := t.TempDir()
	root := filepath.Join(parent, "source")
	testutil.WriteFile(
		t,
		parent,
		"outside.yaml",
		testutil.KustomizationYAML("outside", "authored", "  path: ./hidden\n"),
	)
	testutil.WriteFile(
		t,
		root,
		"inputs/absolute.yaml",
		testutil.KustomizationYAML("absolute", "authored", "  path: ./hidden\n"),
	)
	testutil.WriteFile(
		t,
		root,
		"inputs/escaped.yaml",
		testutil.KustomizationYAML("escaped", "authored", "  path: ./hidden\n"),
	)
	body := fmt.Sprintf(
		"resources: [%q, ../inputs/escaped.yaml, ../../outside.yaml]\npatches: [{path: ../inputs/missing}, {path: patch.yaml}]\n",
		filepath.ToSlash(filepath.Join(root, "inputs/absolute.yaml")),
	)
	testutil.WriteFile(t, root, "apps/"+(*testCaseInput), body)
	s := store.New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./apps"}
	s.AddObject(owner)
	sibling := &manifest.Kustomization{Name: "sibling", Namespace: "apps", Path: "./hidden"}
	s.AddObject(sibling)
	testutil.WriteFile(
		t,
		root,
		"apps/patch.yaml",
		testutil.KustomizationYAML("sibling", "apps", "  path: ./hidden\n"),
	)
	l := New(s)
	l.Index.Records[sibling.Named()] = manifest.DiscoveryRecord{
		File: "apps/patch.yaml",
		Path: filepath.Join(root, "apps/patch.yaml"),
	}
	l.SourceRoot = root
	admitForTest(s, root)
	l.Admit(root)
	admitted := l.AdmitsRoot(
		l.IndexedObject(sibling.Named(), l.Index.Records[sibling.Named()]).Obj,
		filepath.Join(root, "apps/patch.yaml"),
	)
	assert.Equal(t, admitted, false)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	idx := l.Index
	assert.Equal(
		t,
		l.AdmitsRoot(
			l.IndexedObject(sibling.Named(), l.Index.Records[sibling.Named()]).Obj,
			filepath.Join(root, "apps/patch.yaml"),
		),
		admitted,
	)
	{
		assert.FatalIf(t, l.AdmitsRoot(
			l.IndexedObject(sibling.Named(), l.Index.Records[sibling.Named()]).Obj,
			filepath.Join(root, "apps/patch.yaml"),
		), "%s", "file-role input counted as a listed structural resource")
	}
	for _, file := range []string{"apps/" + (*testCaseInput), "inputs/absolute.yaml", "inputs/escaped.yaml", "inputs/missing", "apps/patch.yaml"} {
		assert.Diff(
			t,
			idx.Readers[file],
			[]manifest.FileReader{{Reader: owner.Named(), Source: manifest.BootstrapSourceID}},
		)
	}
	for _, name := range []string{"absolute", "escaped", "outside"} {
		{
			assert.FatalIf(t, s.GetObject(testutil.KustomizationID("authored", name)) != nil ||
				len(
					idx.Records[testutil.KustomizationID("authored", name)].Productions,
				) != 0, "dependency coordinate broadened object admission: %s", name)
		}
	}
	{
		assert.FatalIf(t, len(idx.Readers["../outside.yaml"]) != 0, "%s", "out-of-source resource gained a reader")
	}
}
