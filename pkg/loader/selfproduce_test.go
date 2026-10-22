package loader

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/internal/kustpath"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func cmID(ns string) manifest.NamedResource {
	return manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: ns, Name: "cluster-settings"}
}

func TestFinalizeIndex_EmptyPathRequiresSource(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, path, source string
		produces           bool
	}{
		{name: "stub"},
		{name: "source_root", source: "flux-system", produces: true},
		{name: "explicit_root", path: "./", produces: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "namespace: apps\nresources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cluster-settings}\n")
			testutil.WriteFile(t, root, "flux/nested/raw.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: loose, namespace: apps}\n")
			st := store.New()
			ks := &manifest.Kustomization{
				Name: "root", Namespace: "flux-system", Path: tc.path,
				SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: tc.source, Namespace: "flux-system"},
			}
			st.AddObject(ks)
			l := loaderFixture(st, nil)
			l.Options.WipeSecrets = false
			idx := rendererFixture(t, l, root)
			var want []manifest.NamedResource
			if tc.produces {
				want = []manifest.NamedResource{ks.Named()}
			}
			if got := renderers(idx, cmID("apps")); !slices.Equal(got, want) {
				t.Fatalf("ProducedBy = %v, want %v", got, want)
			}
			if got := renderers(idx, manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "loose"}); !slices.Equal(got, want) {
				t.Fatalf("loose manifest producers = %v, want %v", got, want)
			}
			if got := ownersForTest(idx, "apps/cm.yaml"); !slices.Equal(got, want) {
				t.Fatalf("OwnersOfFile = %v, want %v", got, want)
			}
		})
	}
}

// The bjw-s/onedr0p topology: a root KS with a BARE spec.path (no
// kustomization.yaml) whose per-namespace subdir bases each stamp their own
// `namespace:` and pull in a shared substitutions component defining a
// namespace-LESS ConfigMap. The index must attribute that ConfigMap — in
// EACH group's resolved namespace — back to the root KS whose own render
// emits it, resolving the bare-dir → subdir-base → component → namespace
// chain that no path-prefix index can see.
func TestFinalizeIndex_BareDirComponentNamespace(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, target, local, namespace string }{
		{"bare", "", "", ""},
		{"target", "target", "", "target"},
		{"local", "", "namespace: local\n", "local"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			testutil.WriteFile(t, dir, "kubernetes/apps/flux-system/kustomization.yaml",
				"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: flux-system\ncomponents:\n  - ../../components/substitutions\n")
			testutil.WriteFile(t, dir, "kubernetes/apps/default/kustomization.yaml",
				"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: default\ncomponents:\n  - ../../components/substitutions\n")
			testutil.WriteFile(t, dir, "kubernetes/components/substitutions/kustomization.yaml",
				"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources:\n  - ./cluster-settings.yaml\n")
			testutil.WriteFile(t, dir, "kubernetes/components/substitutions/cluster-settings.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cluster-settings\ndata:\n  CLUSTER_NAME: home\n"+
					"---\napiVersion: foreign.example/v1\nkind: ConfigMap\nmetadata: {name: foreign-config}\ndata: {VALUE: foreign}\n"+
					"---\napiVersion: foreign.example/v1\nkind: Secret\nmetadata: {name: foreign-secret}\nstringData: {VALUE: foreign}\n")

			testutil.WriteFile(t, dir, "kubernetes/apps/bare/kustomization.yaml", tc.local+"resources: [child.yaml]\nconfigMapGenerator:\n  - name: generated\n    literals: [CLUSTER_NAME=home]\nsecretGenerator:\n  - name: generated-secret\n    literals: [KEY=value]\n")
			testutil.WriteFile(t, dir, "kubernetes/apps/bare/child.yaml", testutil.KustomizationYAML("consumer", "", "  path: ./leaf\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: generated}, {kind: Secret, name: generated-secret}]\n"))
			testutil.WriteFile(t, dir, "leaf/kustomization.yaml", "resources: []\n")
			s := store.New()
			clusterApps := &manifest.Kustomization{
				Name:      "cluster-apps",
				Namespace: "flux-system",
				Path:      "./kubernetes/apps",
			}
			clusterApps.TargetNamespace = tc.target
			s.AddObject(clusterApps)

			admitForTest(s, dir)
			l := loaderFixture(s, nil)
			l.SourceRoot = dir
			if _, err := loadForTest(t.Context(), t, l, filepath.Join(dir, "kubernetes/apps/bare")); err != nil {
				t.Fatal(err)
			}
			if err := l.FinalizeIndex(t.Context(), dir); err != nil {
				t.Fatal(err)
			}
			idx := l.Index
			for _, id := range []manifest.NamedResource{
				{Kind: manifest.KindConfigMap, Name: "foreign-config"},
				{Kind: manifest.KindSecret, Name: "foreign-secret"},
			} {
				for _, ns := range []string{"", "flux-system", "default", "target"} {
					id.Namespace = ns
					if _, ok := idx.Lookup(id); ok {
						t.Fatalf("foreign data lookalike indexed: %s", id)
					}
				}
			}

			// Produced in EVERY group's namespace (the namespace transformer stamps
			// the component's namespace-less ConfigMap per base) — not just the first.
			namespaces := []string{"flux-system", "default"}
			if tc.target != "" {
				namespaces = []string{tc.target}
			}
			for _, ns := range namespaces {
				if got := renderers(idx, cmID(ns)); !slices.Contains(got, clusterApps.Named()) {
					t.Errorf("ProducedBy(ConfigMap/%s/cluster-settings) = %v, want to contain cluster-apps", ns, got)
				}
			}
			secret := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: tc.namespace, Name: "generated-secret"}
			if got := renderers(idx, secret); !slices.Contains(got, clusterApps.Named()) {
				t.Errorf("generated secret %s owners=%v", secret, got)
			}
			generated := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: tc.namespace, Name: "generated"}
			if got := renderers(idx, generated); !slices.Contains(got, clusterApps.Named()) {
				t.Errorf("generated ConfigMap %s owners=%v", generated, got)
			}
			consumer := testutil.KustomizationID(tc.namespace, "consumer")
			record, ok := idx.Lookup(consumer)
			if !ok {
				t.Fatalf("consumer %s not indexed: records=%+v", consumer, idx.Records)
			}
			assert.Diff(t, record.Refs, []manifest.NamedResource{generated})
			obj, ok := candidateForTest(t, l, consumer).(*manifest.Kustomization)
			if !ok {
				t.Fatalf("consumer %s not stored", consumer)
			}
			refs := 0
			for ref := range manifest.References(obj, manifest.ReferenceRelations) {
				if ref.Use == "substituteFrom" {
					refs++
					assert.Equal(t, ref.Namespace, tc.namespace)
					if _, ok := idx.Lookup(ref.NamedResource); !ok {
						t.Errorf("consumer input %s not indexed", ref.NamedResource)
					}
				}
			}
			assert.Equal(t, refs, 2)
			for _, id := range []manifest.NamedResource{generated, secret, consumer} {
				wrong := id
				wrong.Namespace = "flux-system"
				if _, ok := idx.Lookup(wrong); ok {
					t.Errorf("parent namespace leaked into %s", wrong)
				}
			}
			// A namespace no base stamps is not attributed.
			if got := renderers(idx, cmID("kube-system")); len(got) != 0 {
				t.Errorf("ProducedBy(ConfigMap/kube-system/cluster-settings) = %v, want empty", got)
			}
		})
	}
}

// A substituteFrom ConfigMap defined OUTSIDE the KS's own render subtree —
// i.e. produced by a different KS — must NOT be attributed to this KS, so its
// dependency edge survives and a real failure stays loud. Here the root KS's
// spec.path holds only its own kustomization with no component, so it produces
// nothing.
func TestFinalizeIndex_NonProducerNotAttributed(t *testing.T) {

	dir := t.TempDir()
	testutil.WriteFile(t, dir, "kubernetes/apps/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: flux-system\nresources: []\n")

	s := store.New()
	ks := &manifest.Kustomization{
		Name:      "cluster-apps",
		Namespace: "flux-system",
		Path:      "./kubernetes/apps",
	}
	s.AddObject(ks)

	idx := rendererFixture(t, loaderFixture(s, nil), dir)
	if got := renderers(idx, cmID("flux-system")); len(got) != 0 {
		t.Errorf("ProducedBy = %v, want empty (KS produces no cluster-settings)", got)
	}
	t.Run("nonproducer_allocations", func(t *testing.T) {
		b := &selfProduceBuilder{}
		if got := testing.AllocsPerRun(10, func() {
			b.recordProducer(producerDeclaration{obj: ks}, nil)
		}); got != 0 {
			t.Fatalf("nonproducer preparation allocations=%v, want 0", got)
		}
	})
}

// Nil index is safe — collectDeps falls back to always-add (the pre-index
// behavior) when no repoRoot produced an index.
func TestDiscoveryIndex_NilSafe(t *testing.T) {
	t.Parallel()
	var idx *manifest.DiscoveryIndex
	if got := declarationOwnersForTest(idx, manifest.NamedResource{}); got != nil {
		t.Errorf("nil index OwnersOfProducer = %v, want nil", got)
	}
	if got := renderers(idx, cmID("flux-system")); got != nil {
		t.Errorf("nil index ProducedBy = %v, want nil", got)
	}
	if _, ok := emissionParentForTest(idx, "apps/base/app-a/ks.yaml"); ok {
		t.Errorf("nil index EmissionParentByFile = ok, want absent")
	}
	if got := ownersForTest(idx, "apps/base/postgres/cluster.yaml"); got != nil {
		t.Errorf("nil index OwnersOfFile = %v, want nil", got)
	}
}

func TestFinalizeIndex_OwnersOfFileResourcesEscape(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, overlay string
		mixed         bool
	}{
		{"explicit", "apps/homelab", false},
		{"shallow", "apps/homelab/nested", false},
		{"deep", "apps/homelab/deep/nested", false},
		{"mixed", "apps/homelab/deep/nested", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			base, err := filepath.Rel(tc.overlay, "apps/base/postgres")
			assert.NoError(t, err)
			testutil.WriteFile(t, dir, tc.overlay+"/kustomization.yaml", "resources: ["+filepath.ToSlash(base)+"]\n")
			if tc.mixed {
				testutil.WriteFile(t, dir, "apps/homelab/sibling.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: sibling}\n")
			}
			testutil.WriteFile(t, dir, "apps/base/postgres/kustomization.yaml",
				"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - cluster.yaml\npatches: [{path: ../../../inputs/patch}]\n")
			testutil.WriteFile(t, dir, "apps/base/postgres/cluster.yaml",
				"apiVersion: postgresql.cnpg.io/v1\nkind: Cluster\nmetadata:\n  name: postgres\n")

			s := store.New()
			var want []manifest.NamedResource
			for i := range 2 {
				ks := &manifest.Kustomization{Name: fmt.Sprintf("apps-%d", i), Namespace: "flux-system", Path: "./apps/homelab"}
				s.AddObject(ks)
				want = append(want, ks.Named())
			}

			idx := rendererFixture(t, loaderFixture(s, nil), dir)

			for _, file := range []string{
				tc.overlay + "/kustomization.yaml",
				"apps/base/postgres/kustomization.yaml",
				"apps/base/postgres/cluster.yaml",
				"inputs/patch",
			} {
				if got := idx.Readers[file]; !slices.Equal(got, want) {
					t.Errorf("Readers[%s] = %v, want %v", file, got, want)
				}
			}
			filter := change.NewFilter(change.NewSet([]string{"apps/base/postgres/cluster.yaml"}), idx, s)
			for _, id := range want {
				if !filter.ShouldReconcile(id) {
					t.Errorf("changed shared base did not select %s", id)
				}
			}
		})
	}
}

// TestFinalizeIndex_OwnersOfFileDirectFileEscape: an overlay lists an
// existing resource file directly, by a path escaping its own directory. Even
// though the escape-rejecting policy keeps it from being opened, it must still
// be attributed to the owning KS — the direct-file twin of the dir escape in
// #833. Otherwise changed-only mode omits the KS when that file is the only edit.
func TestFinalizeIndex_OwnersOfFileDirectFileEscape(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "apps/homelab/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - ../base/postgres/cluster.yaml\n")
	testutil.WriteFile(t, dir, "apps/base/postgres/cluster.yaml",
		"apiVersion: postgresql.cnpg.io/v1\nkind: Cluster\nmetadata:\n  name: postgres\n")

	s := store.New()
	ks := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "./apps/homelab"}
	s.AddObject(ks)

	idx := rendererFixture(t, loaderFixture(s, nil), dir)

	if got := ownersForTest(idx, "apps/base/postgres/cluster.yaml"); !slices.Contains(got, ks.Named()) {
		t.Errorf("OwnersOfFile(escaped file cluster.yaml) = %v, want to contain apps", got)
	}
}

type missingResourceFS struct {
	fs.FS
	missing              string
	statError            error
	opens, stats, probes int
}

func (f *missingResourceFS) Open(name string) (fs.File, error) {
	if name == f.missing {
		f.opens++
	} else if strings.HasPrefix(name, f.missing+"/") {
		f.probes++
	}
	return f.FS.Open(name)
}

func (f *missingResourceFS) Stat(name string) (fs.FileInfo, error) {
	if name == f.missing {
		f.stats++
		if f.statError != nil {
			return nil, f.statError
		}
	}
	return fs.Stat(f.FS, name)
}

func TestFinalizeIndex_OwnersOfFileDanglingResource(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		statError error
		present   bool
		preload   bool
		wantOpens int
	}{
		{name: "cached_file", present: true, preload: true},
		{name: "unscanned_file", present: true, wantOpens: 4},
		{name: "absent"},
		{name: "stat_permission", statError: fs.ErrPermission, wantOpens: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			testutil.WriteFile(t, dir, "apps/homelab/kustomization.yaml", "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources: [../base/postgres]\n")
			testutil.WriteFile(t, dir, "apps/base/postgres/kustomization.yaml", "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources: [cluster.yaml]\n")
			s := store.New()
			ks := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "./apps/homelab"}
			other := &manifest.Kustomization{Name: "other", Namespace: "flux-system", Path: "./apps/homelab", TargetNamespace: "second"}
			s.AddObject(ks)
			s.AddObject(other)
			input := &missingResourceFS{FS: os.DirFS(dir), missing: "apps/base/postgres/cluster.yaml", statError: tc.statError}
			s.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{Kind: manifest.KindGitRepository, LocalPath: dir, LocalRoot: dir, FS: input})
			l := loaderFixture(s, nil)
			l.SourceRoot = dir
			if tc.present {
				testutil.WriteFile(t, dir, input.missing, "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: postgres, namespace: authored}\n")
			}
			if tc.preload {
				l.documents.mountSources(s)
				_, err := l.loadFile(filepath.Join(dir, input.missing))
				assert.NoError(t, err)
				input.opens = 0
			}
			if err := l.FinalizeIndex(t.Context(), dir); err != nil {
				t.Fatal(err)
			}
			if err := l.FinalizeIndex(t.Context(), dir); err != nil {
				t.Fatal(err)
			}
			for _, owner := range []manifest.NamedResource{ks.Named(), other.Named()} {
				if got := ownersForTest(l.Index, input.missing); !slices.Contains(got, owner) {
					t.Errorf("OwnersOfFile(cluster.yaml) = %v, want %v", got, owner)
				}
			}
			if input.opens != tc.wantOpens {
				t.Fatalf("resource decode opens=%d, want %d", input.opens, tc.wantOpens)
			}
			wantStats := 1
			if tc.preload {
				wantStats = 0
			}
			if input.stats != wantStats {
				t.Fatalf("resource stats across owners and index freezes=%d, want %d", input.stats, wantStats)
			}
			wantProbes := 0
			if tc.statError != nil {
				wantProbes = len(manifest.KustomizeBuilderFilenames)
			}
			if input.probes != wantProbes {
				t.Fatalf("kustomization probes beneath a resource=%d, want %d", input.probes, wantProbes)
			}
			s.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{Kind: manifest.KindGitRepository, LocalPath: dir, LocalRoot: dir, FS: input})
			if err := l.FinalizeIndex(t.Context(), dir); err != nil {
				t.Fatal(err)
			}
			if input.stats != wantStats+1 {
				t.Fatalf("resource stats after source replacement=%d, want %d", input.stats, wantStats+1)
			}
		})
	}
}

// TestFinalizeIndex_OwnersOfFileDanglingDirectory: the directory
// form of the dangling case. A resources: entry pointing at a directory
// that no longer exists records only the directory path, but change
// detection reports the deleted files individually — OwnersOfFile must
// resolve them through the recorded parent directory.
func TestFinalizeIndex_OwnersOfFileDanglingDirectory(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "apps/homelab/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - ../base/postgres\n")
	// A peer overlay directly references a deleted file beneath the deleted
	// directory: its exact-key owner must not mask the directory owner.
	testutil.WriteFile(t, dir, "apps/other/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - ../base/postgres/cluster.yaml\n")

	s := store.New()
	ks := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "./apps/homelab"}
	other := &manifest.Kustomization{Name: "other", Namespace: "flux-system", Path: "./apps/other"}
	s.AddObject(ks)
	s.AddObject(other)

	idx := rendererFixture(t, loaderFixture(s, nil), dir)

	for _, file := range []string{
		"apps/base/postgres/kustomization.yaml",
		"apps/base/postgres/nested/cluster.yaml",
	} {
		if got := ownersForTest(idx, file); !slices.Contains(got, ks.Named()) {
			t.Errorf("OwnersOfFile(%s under deleted dir) = %v, want to contain apps", file, got)
		}
	}
	// Owned by both: other by its exact entry, apps through the deleted dir.
	if got := ownersForTest(idx, "apps/base/postgres/cluster.yaml"); !slices.Contains(got, ks.Named()) || !slices.Contains(got, other.Named()) {
		t.Errorf("OwnersOfFile(deleted cluster.yaml) = %v, want to contain both apps and other", got)
	}
	if got := ownersForTest(idx, "apps/base/redis/cm.yaml"); got != nil {
		t.Errorf("OwnersOfFile(unrelated file) = %v, want nil", got)
	}
}

func TestFinalizeIndex_EmissionParentByFile(t *testing.T) {
	t.Parallel()
	child := testutil.KustomizationID("flux-system", "child")
	parent := testutil.KustomizationID("flux-system", "parent")
	other := testutil.KustomizationID("flux-system", "other")
	for _, tc := range []struct {
		name                         string
		emitters                     []manifest.NamedResource
		wantParent                   manifest.NamedResource
		strict, repeated, graphAdded bool
		nonStructural                bool
	}{
		{name: "unique owner", emitters: []manifest.NamedResource{parent}, wantParent: parent},
		{name: "non structural record", emitters: []manifest.NamedResource{parent}, nonStructural: true},
		{name: "missing owner"},
		{name: "ambiguous owners", emitters: []manifest.NamedResource{other, parent}},
		{name: "self owner", emitters: []manifest.NamedResource{child}},
		{name: "self and another owner are ambiguous", emitters: []manifest.NamedResource{child, parent}},
		{name: "strict parent wins", emitters: []manifest.NamedResource{parent}, wantParent: other, strict: true},
		{name: "repeated owner", emitters: []manifest.NamedResource{parent}, wantParent: parent, repeated: true},
		{name: "graph added record", emitters: []manifest.NamedResource{parent}, wantParent: parent, graphAdded: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			file := "objects/ks.yaml"
			path := filepath.Join(root, file)
			body := testutil.KustomizationYAML("child", "flux-system", "  path: ./apps/${ENV}/child\n")
			id := child
			if tc.nonStructural {
				id = cmID("flux-system")
				body += "---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: cluster-settings, namespace: flux-system}\n"
			}
			if tc.repeated {
				body += "---\n" + testutil.KustomizationYAML("peer", "flux-system", "  path: ./apps/${ENV}/peer\n")
			}
			testutil.WriteFile(t, root, file, body)
			testutil.WriteFile(t, root, "render/kustomization.yaml", "resources: [../objects]\n")
			testutil.WriteFile(t, root, "objects/kustomization.yaml", "resources: [ks.yaml]\n")
			if tc.strict {
				testutil.WriteFile(t, root, "kustomization.yaml", "resources: []\n")
			}
			newLoader := func() *Loader {
				t.Helper()
				st := store.New()
				for _, owner := range tc.emitters {
					st.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace, Path: "./render"})
				}
				if tc.strict {
					st.AddObject(&manifest.Kustomization{Name: other.Name, Namespace: other.Namespace, Path: "."})
				}
				l := New(st)
				admitForTest(st, root)
				l.mountSources()
				objects, err := l.documents.parseFile(path, manifest.ParseDocOptions{})
				assert.NoError(t, err)
				for _, obj := range objects {
					if _, ok := obj.(*manifest.Kustomization); ok && st.GetObject(obj.Named()) == nil {
						st.AddObject(obj)
					}
				}
				if !tc.graphAdded {
					l.Index.Records[id] = manifest.DiscoveryRecord{Authored: id, Path: path, File: file}
				}
				return l
			}
			l := newLoader()
			_, exists := l.Index.Lookup(id)
			assert.Equal(t, exists, !tc.graphAdded)
			if err := l.FinalizeIndex(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			want := manifest.DiscoveryRecord{Authored: id, Path: path, File: file, Parent: tc.wantParent}
			for _, owner := range tc.emitters {
				production := manifest.Production{Owner: owner, Authored: id, File: file, Source: manifest.BootstrapSourceID}
				if !id.KindInfo().Runnable || l.Store.GetObject(id) != nil {
					production.Path = path
				}
				want.Productions = append(want.Productions, production)
			}
			if len(want.Productions) == 0 {
				want.Productions = []manifest.Production{{Authored: id, Path: path, File: file, Source: manifest.BootstrapSourceID}}
			}
			assert.Diff(t, l.Index.Records[id], want)
			if tc.repeated {
				peer := testutil.KustomizationID("flux-system", "peer")
				assert.Equal(t, l.Index.Records[peer].Parent, parent)
			}
			wantOwners := tc.emitters
			if tc.strict {
				wantOwners = []manifest.NamedResource{other, parent}
			}
			owners, ancestors := l.Index.OwnersOfFile(file)
			assert.Diff(t, owners, wantOwners)
			assert.Equal(t, len(ancestors), 0)
			snapshot := snapshotIndexForTest(l.Index)
			if err := l.FinalizeIndex(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, l.Index.Records, snapshot.Records)
			assert.Diff(t, l.Index.Readers, snapshot.Readers)
			owners, ancestors = l.Index.OwnersOfFile(file)
			assert.Diff(t, owners, wantOwners)
			assert.Equal(t, len(ancestors), 0)
			fresh := newLoader()
			if err := fresh.FinalizeIndex(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, *fresh.Index, snapshot)
		})
	}
}

func secretID(ns, name string) manifest.NamedResource {
	return manifest.NamedResource{Kind: manifest.KindSecret, Namespace: ns, Name: name}
}

// The discovery-time producer scan rides the same self-produce walk: an in-repo
// ExternalSecret / SealedSecret under a KS spec.path is recorded as
// target-Secret → producer, with the SAME effective-namespace resolution the
// ConfigMap path uses — an enclosing `namespace:` wins over the file's own.
func TestFinalizeIndex_RecordsProducers(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "apps/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: secure\nresources:\n  - ./es.yaml\n  - ./sealed.yaml\n  - ./explicit.yaml\n  - ./obc.yaml\n")
	// ExternalSecret, no namespace in file → inherits `secure`; explicit target.
	testutil.WriteFile(t, dir, "apps/es.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata:\n  name: app-creds\nspec:\n  target:\n    name: app-values\n")
	// SealedSecret with spec.template.metadata.name.
	testutil.WriteFile(t, dir, "apps/sealed.yaml",
		"apiVersion: bitnami.com/v1alpha1\nkind: SealedSecret\nmetadata:\n  name: db\nspec:\n  template:\n    metadata:\n      name: db-secret\n")
	// ExternalSecret carrying its own namespace — the transformer (secure)
	// overrides it, exactly as kustomize does.
	testutil.WriteFile(t, dir, "apps/explicit.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata:\n  name: own\n  namespace: ignored\nspec:\n  target:\n    name: own-values\n")
	// ObjectBucketClaim: its provisioner materializes a Secret AND a ConfigMap
	// both named after the claim, in the (transformer-resolved) namespace.
	testutil.WriteFile(t, dir, "apps/obc.yaml",
		"apiVersion: objectbucket.io/v1alpha1\nkind: ObjectBucketClaim\nmetadata:\n  name: media-bucket\nspec:\n  bucketName: media\n  storageClassName: ceph-bucket\n")

	s := store.New()
	s.AddObject(&manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path: "./apps",
	})

	idx := rendererFixture(t, loaderFixture(s, nil), dir)

	want := map[manifest.NamedResource]string{ // target → producer name
		secretID("secure", "app-values"): "app-creds",
		secretID("secure", "db-secret"):  "db",
		secretID("secure", "own-values"): "own",
		// The OBC produces BOTH a Secret and a ConfigMap named after the claim.
		secretID("secure", "media-bucket"):                                        "media-bucket",
		{Kind: manifest.KindConfigMap, Namespace: "secure", Name: "media-bucket"}: "media-bucket",
	}
	for target, prodName := range want {
		declarations := testutil.Declarations(s.Productions(target, idx))
		if len(declarations) != 1 {
			t.Errorf("Producer(%v) missing; want %q", target, prodName)
			continue
		}
		got := declarations[0]
		if got.Name != prodName || got.Namespace != target.Namespace {
			t.Errorf("Producer(%v).Name = %q, want %q", target, got.Name, prodName)
		}
	}
}

// Caveat-as-test: the scan reads the producer's RAW target name; it does not
// apply a kustomize namePrefix/nameSuffix (the walker ignores those
// directives), so a prefixed target is recorded — and would be looked up —
// under the un-prefixed name. Producer-inference then misses the real
// (prefixed) Secret and the consumer falls back to fail-loud / the flag.
func TestFinalizeIndex_NamePrefixNotFollowed(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "apps/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: secure\nnamePrefix: prod-\nresources:\n  - ./es.yaml\n")
	testutil.WriteFile(t, dir, "apps/es.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata:\n  name: app-creds\nspec:\n  target:\n    name: app-values\n")

	s := store.New()
	s.AddObject(&manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path: "./apps",
	})

	idx := rendererFixture(t, loaderFixture(s, nil), dir)

	if declarations := testutil.Declarations(s.Productions(secretID("secure", "app-values"), idx)); len(declarations) == 0 {
		t.Error("producer not recorded under its raw target name")
	}
	// The materialized Secret would be prod-app-values; the scan does NOT
	// record that — pinning the documented namePrefix coverage gap.
	if declarations := testutil.Declarations(s.Productions(secretID("secure", "prod-app-values"), idx)); len(declarations) > 0 {
		t.Error("scan unexpectedly followed namePrefix; the caveat test is stale")
	}
}

// An ExternalSecret that statically declares its output keys (target.template.data
// here) materializes a stand-in placeholder Secret in the store, so a downstream
// substituteFrom / valuesFrom resolves those keys to ..PLACEHOLDER_<key>.. rather
// than empty — flate's SOPS shape, applied to a secret whose keys are knowable
// but whose values are not. The scan never clobbers a real Secret already
// occupying the target id; a dataFrom-only producer declares nothing, so none is
// synthesized and it stays genuinely unreadable.
func TestFinalizeIndex_SynthesizesPlaceholderSecret(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "apps/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: secure\nresources:\n  - ./declared.yaml\n  - ./datafrom.yaml\n  - ./preexisting.yaml\n")
	testutil.WriteFile(t, dir, "apps/declared.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata:\n  name: app\nspec:\n  target:\n    name: app-secret\n    template:\n      data:\n        HOST: \"{{ .h }}\"\n        TOKEN: \"{{ .t }}\"\n")
	testutil.WriteFile(t, dir, "apps/datafrom.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata:\n  name: dyn\nspec:\n  target:\n    name: dyn-secret\n  dataFrom:\n    - extract:\n        key: vault/dyn\n")
	testutil.WriteFile(t, dir, "apps/preexisting.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata:\n  name: pre\nspec:\n  target:\n    name: pre-secret\n    template:\n      data:\n        K: \"{{ .k }}\"\n")

	s := store.New()
	s.AddObject(&manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path: "./apps",
	})
	existing := &manifest.Secret{Name: "pre-secret", Namespace: "secure", StringData: map[string]any{"K": "real"}}
	s.AddObject(existing)

	testutil.WriteFile(t, dir, "implicit/deep/declared.yaml", "apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: deferred}\nspec: {target: {name: deferred-secret, template: {data: {KEY: placeholder}}}}\n")
	deferred := &manifest.Kustomization{Name: "deferred", Namespace: "flux-system", Path: "./implicit", TargetNamespace: "secure"}
	s.AddObject(deferred)
	idx := rendererFixture(t, loaderFixture(s, nil), dir)
	assert.Diff(t, idx.Readers["implicit/deep/declared.yaml"], []manifest.NamedResource{deferred.Named()})
	if s.GetObject(secretID("secure", "deferred-secret")) != nil || len(idx.Records[secretID("secure", "deferred-secret")].Productions) != 0 {
		t.Fatal("implicit producer was registered or synthesized before rendering")
	}

	got, _ := s.GetObject(secretID("secure", "app-secret")).(*manifest.Secret)
	if got == nil {
		t.Fatal("app-secret was not synthesized from its declared keys")
	}
	for _, k := range []string{"HOST", "TOKEN"} {
		if want := manifest.ValuePlaceholder(k); got.StringData[k] != want {
			t.Errorf("app-secret stringData[%s] = %v, want %q", k, got.StringData[k], want)
		}
	}
	if obj := s.GetObject(secretID("secure", "dyn-secret")); obj != nil {
		t.Errorf("dataFrom-only ExternalSecret must NOT be synthesized; got %#v", obj)
	}
	if obj := s.GetObject(secretID("secure", "pre-secret")); obj != manifest.BaseManifest(existing) {
		t.Errorf("a real Secret on the target id must not be clobbered; got %#v", obj)
	}
}

func TestFinalizeIndex_MaterializesSopsSecret(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "apps/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: flux-system\nresources:\n  - ./secret.sops.yaml\n  - ./plain.yaml\n")
	testutil.WriteFile(t, dir, "apps/secret.sops.yaml",
		"apiVersion: v1\nkind: Secret\nmetadata:\n  name: cluster-secrets\nstringData:\n  SECRET_DOMAIN: ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]\nsops:\n  mac: ENC[AES256_GCM,data:a,type:str]\n  version: 3.9.0\n")
	testutil.WriteFile(t, dir, "apps/plain.yaml",
		"apiVersion: v1\nkind: Secret\nmetadata:\n  name: plain-secret\nstringData:\n  TOKEN: hunter2\n")

	s := store.New()
	s.AddObject(&manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path: "./apps",
	})

	testutil.WriteFile(t, dir, "generated/secret.yaml", "apiVersion: v1\nkind: Secret\nmetadata: {name: deferred-secret}\nstringData: {TOKEN: 'ENC[AES256_GCM,data:x]'}\nsops: {mac: encrypted}\n")
	s.AddObject(&manifest.Kustomization{Name: "generated", Namespace: "flux-system", Path: "./generated", TargetNamespace: "flux-system"})
	testutil.WriteFile(t, dir, "generated/nested/kustomization.yaml", "namespace: nested\nresources: [secret.yaml, producer.yaml]\nconfigMapGenerator: [{name: generated-data, files: [payload=../../inputs/payload.yaml]}]\n")
	testutil.WriteFile(t, dir, "generated/nested/secret.yaml", "apiVersion: v1\nkind: Secret\nmetadata: {name: nested-secret}\nstringData: {TOKEN: 'ENC[AES256_GCM,data:x]'}\nsops: {mac: encrypted}\n")
	testutil.WriteFile(t, dir, "generated/nested/producer.yaml", "apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: nested-producer}\nspec: {target: {name: nested-produced}, data: [{secretKey: KEY, remoteRef: {key: hidden}}]}\n")
	testutil.WriteFile(t, dir, "inputs/payload.yaml", testutil.KustomizationYAML("terminal", "authored", "  path: ./hidden\n"))
	idx := rendererFixture(t, loaderFixture(s, nil), dir)
	for _, file := range []string{"generated/nested/secret.yaml", "generated/nested/producer.yaml", "inputs/payload.yaml"} {
		assert.Diff(t, idx.Readers[file], []manifest.NamedResource{testutil.KustomizationID("flux-system", "generated")})
	}
	generated, _ := s.Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "generated-data"})
	if generated == nil || generated.Data["payload"] != testutil.KustomizationYAML("terminal", "authored", "  path: ./hidden\n") || s.GetObject(testutil.KustomizationID("authored", "terminal")) != nil {
		t.Fatalf("implicit generator data was lost or admitted as a resource: generated=%+v terminal=%+v", generated, s.GetObject(testutil.KustomizationID("authored", "terminal")))
	}
	if obj := s.GetObject(secretID("flux-system", "deferred-secret")); obj != nil {
		t.Errorf("generated Secret materialized before rendering: %#v", obj)
	}
	for _, name := range []string{"nested-secret", "nested-produced"} {
		for _, namespace := range []string{"nested", "flux-system"} {
			if obj := s.GetObject(secretID(namespace, name)); obj != nil {
				t.Fatalf("implicit-boundary input materialized early: %v", obj)
			}
		}
	}

	got, _ := s.GetObject(secretID("flux-system", "cluster-secrets")).(*manifest.Secret)
	if got == nil {
		t.Fatal("SOPS cluster-secrets was not materialized into the store")
	}
	if want := manifest.ValuePlaceholder("SECRET_DOMAIN"); got.StringData["SECRET_DOMAIN"] != want {
		t.Errorf("materialized SECRET_DOMAIN = %v, want %q", got.StringData["SECRET_DOMAIN"], want)
	}
	if obj := s.GetObject(secretID("flux-system", "plain-secret")); obj != nil {
		t.Errorf("a cleartext Secret must not be materialized; got %#v", obj)
	}
}

// The placeholder Secret is a wipe-mode stand-in (a stand-in for a value flate
// can't read), so --no-wipe-secrets suppresses synthesis entirely — the secret
// is left genuinely absent, exactly as it was before this feature.
func TestFinalizeIndex_NoSynthesisWithoutWipe(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "apps/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: secure\nresources:\n  - ./es.yaml\n")
	testutil.WriteFile(t, dir, "apps/es.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata:\n  name: app\nspec:\n  target:\n    name: app-secret\n    template:\n      data:\n        TOKEN: \"{{ .t }}\"\n")

	s := store.New()
	s.AddObject(&manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path: "./apps",
	})

	l := loaderFixture(s, nil)
	l.Options.WipeSecrets = false
	rendererFixture(t, l, dir)

	if obj := s.GetObject(secretID("secure", "app-secret")); obj != nil {
		t.Errorf("wipeSecrets=false must suppress synthesis; got %#v", obj)
	}
}

func TestFinalizeIndex_ProducerOwners(t *testing.T) {
	t.Parallel()
	for _, namespace := range []string{"", "authored"} {
		t.Run("namespace_"+namespace, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			s := store.New()
			var want []manifest.NamedResource
			for _, name := range []string{"b", "a"} {
				ks := &manifest.Kustomization{Name: name, Namespace: "flux-system", Path: "./" + name}
				s.AddObject(ks)
				want = append(want, ks.Named())
				testutil.WriteFile(t, root, name+"/kustomization.yaml", "namespace: effective\nresources: [../shared, missing.yaml]\n")
			}
			testutil.WriteFile(t, root, "shared/kustomization.yaml", "resources: [producer.yaml]\n")
			testutil.WriteFile(t, root, "shared/producer.yaml", "apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: producer, namespace: '"+namespace+"'}\nspec:\n  target: {name: values}\n  dataFrom: [{extract: {key: values}}]\n")
			idx := rendererFixture(t, loaderFixture(s, nil), root)
			slices.SortFunc(want, manifest.NamedResource.Compare)
			for _, ns := range []string{namespace, "effective"} {
				id := manifest.NamedResource{Group: "external-secrets.io", Kind: "ExternalSecret", Namespace: ns, Name: "producer"}
				got := declarationOwnersForTest(idx, id)
				slices.SortFunc(got, manifest.NamedResource.Compare)
				if !slices.Equal(got, want) {
					t.Fatalf("declaration %s owners = %v, want %v", id, got, want)
				}
				got[0] = manifest.NamedResource{}
				fresh := declarationOwnersForTest(idx, id)
				slices.SortFunc(fresh, manifest.NamedResource.Compare)
				if !slices.Equal(fresh, want) {
					t.Fatal("caller mutated index")
				}
			}
			effective := manifest.NamedResource{Group: "external-secrets.io", Kind: "ExternalSecret", Namespace: "effective", Name: "producer"}
			if r := idx.Records[effective]; r.Authored.Namespace != namespace || r.Authored.Name != "producer" || r.File != "shared/producer.yaml" {
				t.Fatalf("producer lost authored file proof: %+v", r)
			}
			if len(declarationOwnersForTest(idx, manifest.NamedResource{Group: "external-secrets.io", Kind: "ExternalSecret", Namespace: "foreign", Name: "producer"})) != 0 {
				t.Fatal("foreign namespace inherited declaration owners")
			}
			if got := renderers(idx, manifest.NamedResource{Kind: "ConfigMap", Namespace: "effective", Name: "values"}); len(got) != 0 {
				t.Fatalf("producer ownership changed self-substitution admission: %v", got)
			}
		})
	}
}

func TestFinalizeIndex_ProducerOwnersMultipleTargets(t *testing.T) {
	t.Parallel()
	for _, namespace := range []string{"", "apps"} {
		t.Run("namespace_"+namespace, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			s := store.New()
			ks := &manifest.Kustomization{Name: "owner", Namespace: "flux-system", Path: "./values"}
			s.AddObject(ks)
			testutil.WriteFile(t, root, "values/kustomization.yaml", "namespace: apps\nresources: [bucket.yaml]\n")
			testutil.WriteFile(t, root, "values/bucket.yaml", "apiVersion: objectbucket.io/v1alpha1\nkind: ObjectBucketClaim\nmetadata: {name: bucket, namespace: '"+namespace+"'}\nspec: {generateBucketName: bucket, storageClassName: bucket}\n")
			idx := rendererFixture(t, loaderFixture(s, nil), root)
			for _, kind := range []string{manifest.KindSecret, manifest.KindConfigMap} {
				target := manifest.NamedResource{Kind: kind, Namespace: "apps", Name: "bucket"}
				if declarations := testutil.Declarations(s.Productions(target, idx)); len(declarations) == 0 {
					t.Fatalf("missing %s target", kind)
				}
			}
			for _, ns := range []string{namespace, "apps"} {
				producer := manifest.NamedResource{Group: "objectbucket.io", Kind: "ObjectBucketClaim", Namespace: ns, Name: "bucket"}
				if got := declarationOwnersForTest(idx, producer); !slices.Equal(got, []manifest.NamedResource{ks.Named()}) {
					t.Fatalf("repeated targets duplicate declaration owner: %v", got)
				}
			}
		})
	}
}

func TestSelfProduceBuilder_SecretPrecedence(t *testing.T) {
	t.Parallel()
	for _, order := range []string{"placeholder first", "SOPS first", "two SOPS"} {
		t.Run(order, func(t *testing.T) {
			t.Parallel()
			id := secretID("apps", "input")
			b := &selfProduceBuilder{store: store.New(), wipeSecrets: true, secrets: map[manifest.NamedResource]*manifest.Secret{}}
			first := &manifest.Secret{Name: id.Name, Namespace: id.Namespace, Data: map[string]any{"first": "..PLACEHOLDER_first.."}}
			second := &manifest.Secret{Name: id.Name, Namespace: id.Namespace, StringData: map[string]any{"second": "..PLACEHOLDER_second.."}}
			want := first
			switch order {
			case "placeholder first":
				b.synthesizePlaceholderSecret(id, []string{"first"})
				want = second
				b.materializeSecret(second, "apps", "apps")
			case "SOPS first":
				b.materializeSecret(first, "apps", "apps")
				b.synthesizePlaceholderSecret(id, []string{"second"})
			case "two SOPS":
				want = second
				b.materializeSecret(first, "apps", "apps")
				b.materializeSecret(second, "apps", "apps")
			}
			assert.Diff(t, b.secrets[id], want)
		})
	}
}

func TestFinalizeIndex_MixedKustomizationDataFile(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"mixed", "mixed_producer", "duplicate_record", "duplicate_store_object"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "flux/ks.yaml", testutil.KustomizationYAML("owner", "flux-system", "  {path: ./apps}\n"))
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "namespace: apps\nresources: [bundle.yaml]\n")
			bundle := testutil.KustomizationYAML("child", "apps", "  {path: ./child}\n")
			if mode == "mixed_producer" {
				bundle += "---\napiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: producer}\nspec: {target: {name: values}}\n"
			} else {
				bundle += `---
apiVersion: v1
kind: ConfigMap
metadata: {name: values, namespace: apps}
data: {value: fixture}
`
			}
			testutil.WriteFile(t, root, "apps/bundle.yaml", bundle)
			if strings.HasPrefix(mode, "duplicate") {
				testutil.WriteFile(t, root, "flux/shadow.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: values, namespace: apps}\ndata: {value: shadow}\n")
			}
			l := loaderFixture(store.New(), nil)
			var existing *manifest.ConfigMap
			if mode == "duplicate_store_object" {
				existing = &manifest.ConfigMap{Name: "values", Namespace: "apps", Data: map[string]any{"value": "shadow"}}
				l.Store.AddObject(existing)
			}
			l.PreferExisting = true
			l.SourceRoot = root
			for _, dir := range []string{"flux", "apps"} {
				if _, err := loadForTest(t.Context(), t, l, filepath.Join(root, dir)); err != nil {
					t.Fatal(err)
				}
			}
			if err := l.FinalizeIndex(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			idx := l.Index
			owner := testutil.KustomizationID("flux-system", "owner")
			child := testutil.KustomizationID("apps", "child")
			values := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "values"}
			if existing != nil && l.Store.GetObject(values) != existing {
				t.Fatal("pre-existing data object was replaced")
			}
			if record, _ := l.Index.Lookup(child); record.Parent != owner {
				t.Fatalf("child parent = %s, want %s", record.Parent, owner)
			}
			if mode == "mixed_producer" {
				if declarations := testutil.Declarations(l.Store.Productions(secretID("apps", "values"), idx)); len(declarations) == 0 {
					t.Fatal("mixed-file raw producer is missing")
				}
			} else if record, _ := l.Index.Lookup(values); !slices.Contains(testutil.ProductionOwners(record), owner) {
				t.Fatalf("mixed-file data renderers = %v, want %s", testutil.ProductionOwners(record), owner)
			}
		})
	}
}

func TestLoader_DotDotPaths(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		path     string
		accepted bool
	}{
		{path: "..foo", accepted: true},
		{path: "..foo/bar", accepted: true},
		{path: "..foo/../../outside"},
		{path: "a/../../x"},
		{path: "./../x"},
		{path: ".."},
		{path: "../x"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			t.Parallel()
			root := filepath.Join(t.TempDir(), "repo")
			dir := filepath.Join(root, tc.path)
			testutil.WriteFile(t, dir, "kustomization.yaml", "configMapGenerator:\n- name: settings\n  literals: [key=value]\n")
			documents := readerForTest(root)
			documents.read(dir)

			l := New(store.New())
			l.documents = documents
			var wantRoots []string
			if tc.accepted {
				if _, err := l.Load(t.Context(), dir); err != nil {
					t.Fatal(err)
				}
				wantRoots = []string{tc.path}
			}
			if got := l.standaloneRoots(root, nil); !slices.Equal(got, wantRoots) {
				t.Errorf("standaloneRoots = %v, want %v", got, wantRoots)
			}

			w := graphWalker{documents: documents, visited: map[graphContext]struct{}{}}
			var files []string
			if !w.walk(graphVisit{root: root, dir: tc.path}, func(v graphVisit) bool {
				files = append(files, v.file)
				return true
			}) {
				t.Fatal("walk stopped before completing")
			}
			var wantFiles []string
			if tc.accepted {
				wantFiles = []string{filepath.Join(dir, "kustomization.yaml")}
			}
			if !slices.Equal(files, wantFiles) {
				t.Errorf("walk files = %v, want %v", files, wantFiles)
			}

			wantRef := ""
			if tc.accepted {
				wantRef = tc.path
			}
			if got, ok := kustpath.Resolve(".", tc.path, kustpath.Transformer); ok != tc.accepted || got != wantRef {
				t.Errorf("kustpath.Resolve = (%q, %v), want (%q, %v)", got, ok, wantRef, tc.accepted)
			}
			if got, ok := kustomizeinput.Local(".", ".", tc.path); ok != tc.accepted || got != wantRef {
				t.Errorf("kustomizeinput.Local = (%q, %v), want (%q, %v)", got, ok, wantRef, tc.accepted)
			}
		})
	}
}

func TestGraphWalker_RootCoordinates(t *testing.T) {
	t.Parallel()
	for _, dir := range []string{"", ".", "./apps/..", "apps//child/../app", "/app", "https://example.com/app"} {
		t.Run(dir, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, filepath.Join(root, dir), "kustomization.yaml", "resources: []\n")
			w := graphWalker{documents: readerForTest(root), visited: map[graphContext]struct{}{}}
			context := graphContext{root: root, dir: dir}
			count := 0
			if !w.walk(graphVisit{graphContext: context}, func(v graphVisit) bool {
				assert.Equal(t, v.graphContext, context)
				count++
				return true
			}) {
				t.Fatal("walk stopped before completing")
			}
			assert.Equal(t, count, 1)
			if _, ok := w.visited[context]; !ok {
				t.Fatal("original graph context was not visited")
			}
		})
	}
}

func TestFinalizeIndex_ArtifactGenerators(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, resources, scan string
		component             bool
	}{
		{"generator_first", "generator.yaml, consumer.yaml", ".", false},
		{"consumer_first", "consumer.yaml, generator.yaml", ".", false},
		{"component_full", "consumer.yaml", ".", true},
		{"component_changed_only", "consumer.yaml", "apps", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: ["+tc.resources+"]\n")
			generatorFile := "apps/generator.yaml"
			if tc.component {
				generatorFile = "shared/generator.yaml"
				testutil.WriteFile(t, root, "shared/kustomization.yaml", "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [generator.yaml]\n")
			}
			testutil.WriteFile(t, root, generatorFile, "apiVersion: source.extensions.fluxcd.io/v1beta1\nkind: ArtifactGenerator\nmetadata: {name: generator, namespace: authored}\nspec:\n  pathPattern: '@repo/apps/{app}'\n  artifacts: [{name: 'app-{app}'}]\n")
			testutil.WriteFile(t, root, "apps/consumer.yaml", testutil.KustomizationYAML("consumer", "authored", "  path: ./leaf\n  sourceRef: {kind: ExternalArtifact, name: app-api}\n"))
			s := store.New()
			for _, ns := range []string{"apps", "staging"} {
				owner := &manifest.Kustomization{Name: ns, Namespace: "flux-system", Path: "./apps", TargetNamespace: ns}
				if tc.component {
					owner.Components = []string{"../shared"}
				}
				s.AddObject(owner)
			}
			l := loaderFixture(s, nil)
			l.SourceRoot = root
			admitForTest(s, root)
			if _, err := l.Load(t.Context(), filepath.Join(root, tc.scan)); err != nil {
				t.Fatal(err)
			}
			if err := l.FinalizeIndex(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			idx := l.Index
			filter := change.NewFilter(change.NewSet([]string{generatorFile}), idx, s)
			assert.Diff(t, idx.Readers[generatorFile], []manifest.NamedResource{testutil.KustomizationID("flux-system", "apps"), testutil.KustomizationID("flux-system", "staging")})
			for _, ns := range []string{"apps", "staging"} {
				owner := testutil.KustomizationID("flux-system", ns)
				if !filter.ShouldReconcile(owner) {
					t.Fatal("changed declaration did not select its contributing owner")
				}
				generator := manifest.NamedResource{Group: "source.extensions.fluxcd.io", Kind: "ArtifactGenerator", Namespace: ns, Name: "generator"}
				obj := s.GetObject(generator)
				assert.Equal(t, obj != nil, !tc.component)
				if obj != nil && (!manifest.KindOf(obj).Discovery || manifest.KindOf(obj).Runnable) {
					t.Fatalf("generator admission = %v", obj)
				}
				for _, name := range []string{"app-api"} {
					target := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: ns, Name: name}
					r, ok := idx.Lookup(target)
					if !ok || r.Path != "" || s.GetObject(target) != nil || s.GetArtifact(target) != nil {
						t.Fatalf("prediction supplied content: %+v", r)
					}
					want := manifest.Production{Owner: testutil.KustomizationID("flux-system", ns), Declaration: generator, Authored: manifest.NamedResource{Group: generator.Group, Kind: generator.Kind, Namespace: "authored", Name: generator.Name}, Source: manifest.BootstrapSourceID, File: generatorFile}
					assert.Diff(t, r.Productions, []manifest.Production{want})
				}
				if r, ok := idx.Lookup(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: ns, Name: "generator"}); ok && len(r.Productions) != 0 {
					t.Fatal("generator name became an output")
				}
			}
		})
	}
}

func TestFinalizeIndex_ArtifactSourceViews(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	s := store.New()
	l := loaderFixture(s, nil)
	l.SourceRoot = root
	admitForTest(s, root)
	for _, name := range []string{"first", "second"} {
		view := filepath.Join(root, "views", name)
		testutil.WriteFile(t, view, "apps/kustomization.yaml", "namespace: apps\nresources: [generator.yaml, consumer.yaml]\n")
		testutil.WriteFile(t, view, "apps/generator.yaml", "apiVersion: source.extensions.fluxcd.io/v1beta1\nkind: ArtifactGenerator\nmetadata: {name: generator}\nspec:\n  pathPattern: '@repo/{app}'\n  artifacts: [{name: 'app-{app}'}, {name: common}]\n")
		testutil.WriteFile(t, view, "apps/consumer.yaml", testutil.KustomizationYAML("consumer", "", "  path: ./leaf\n  sourceRef: {kind: ExternalArtifact, name: app-"+name+"}\n"))
		source := &manifest.GitRepository{Name: name, Namespace: "flux-system"}
		s.AddObject(source)
		s.SetArtifact(source.Named(), &store.SourceArtifact{LocalRoot: root, LocalPath: view, Key: "pin-" + name, FS: os.DirFS(view)})
		s.AddObject(&manifest.Kustomization{Name: name, Namespace: "flux-system", Path: "./apps", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: name}})
		if _, err := l.Load(t.Context(), view); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.FinalizeIndex(t.Context(), root); err != nil {
		t.Fatal(err)
	}
	idx := l.Index
	generator := manifest.NamedResource{Group: "source.extensions.fluxcd.io", Kind: "ArtifactGenerator", Namespace: "apps", Name: "generator"}
	for _, name := range []string{"first", "second"} {
		target := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: "app-" + name}
		r, _ := idx.Lookup(target)
		var scoped []manifest.Production
		for _, p := range r.Productions {
			if p.Owner != (manifest.NamedResource{}) {
				scoped = append(scoped, p)
			}
		}
		want := manifest.Production{Owner: testutil.KustomizationID("flux-system", name), Declaration: generator, Authored: manifest.NamedResource{Group: generator.Group, Kind: generator.Kind, Name: generator.Name}, Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: name}, File: "views/" + name + "/apps/generator.yaml"}
		assert.Diff(t, scoped, []manifest.Production{want})
	}
	common, _ := idx.Lookup(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: "common"})
	var files []string
	for _, p := range common.Productions {
		if p.Owner != (manifest.NamedResource{}) {
			files = append(files, p.File)
		}
	}
	slices.Sort(files)
	assert.Diff(t, files, []string{"views/first/apps/generator.yaml", "views/second/apps/generator.yaml"})
}

func BenchmarkSelfProduceBuilder_ArtifactJoin(b *testing.B) {
	obj := &manifest.RawObject{APIVersion: "source.extensions.fluxcd.io/v1beta1", Kind: "ArtifactGenerator", Name: "generator", Namespace: "apps", Spec: map[string]any{"pathPattern": "@repo/{app}", "artifacts": []any{map[string]any{"name": "app-{app}"}}}}
	consumers := make([]manifest.BaseManifest, 100)
	for i := range consumers {
		consumers[i] = &manifest.Kustomization{Namespace: "apps", Name: fmt.Sprintf("consumer-%d", i), SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindExternalArtifact, Name: fmt.Sprintf("app-%d", i)}}
	}
	declaration := producerDeclaration{obj: obj, authored: obj.Named(), visit: graphVisit{source: manifest.BootstrapSourceID, coordinate: "generator.yaml"}}
	reader := documentReader{cache: map[string]document{"consumers.yaml": {objects: consumers}}}
	scope := []graphVisit{{file: "consumers.yaml", readable: true}}
	s := store.New()
	b.ReportAllocs()
	for b.Loop() {
		builder := selfProduceBuilder{idx: &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}, store: s, reader: reader}
		builder.joinProductions([]producerDeclaration{declaration}, scope)
	}
}

func TestFinalizeIndex_ArtifactScopeIsolation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, template string
		split          bool
		want           bool
	}{
		{"before", "old-{app}", false, false},
		{"after", "app-{app}", false, true},
		{"disjoint_coverage", "app-{app}", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			consumerDir := "producer"
			resources := "generator.yaml, consumer.yaml"
			if tc.split {
				consumerDir, resources = "consumer", "generator.yaml"
			}
			testutil.WriteFile(t, root, "producer/kustomization.yaml", "namespace: apps\nresources: ["+resources+"]\n")
			testutil.WriteFile(t, root, "producer/generator.yaml", "apiVersion: source.extensions.fluxcd.io/v1beta1\nkind: ArtifactGenerator\nmetadata: {name: generator}\nspec:\n  pathPattern: '@repo/{app}'\n  artifacts: [{name: '"+tc.template+"'}]\n")
			if tc.split {
				testutil.WriteFile(t, root, "consumer/kustomization.yaml", "namespace: apps\nresources: [consumer.yaml]\n")
			}
			testutil.WriteFile(t, root, consumerDir+"/consumer.yaml", testutil.KustomizationYAML("consumer", "", "  path: ./leaf\n  sourceRef: {kind: ExternalArtifact, name: app-api}\n"))
			s := store.New()
			owner := &manifest.Kustomization{Name: "producer", Namespace: "flux-system", Path: "./producer"}
			s.AddObject(owner)
			if tc.split {
				s.AddObject(&manifest.Kustomization{Name: "other", Namespace: "flux-system", Path: "./consumer"})
			}
			l := loaderFixture(s, nil)
			l.SourceRoot = root
			admitForTest(s, root)
			if _, err := l.Load(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			if err := l.FinalizeIndex(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			target := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: "app-api"}
			got := slices.ContainsFunc(s.Productions(target, l.Index), func(p manifest.Production) bool { return p.Owner == owner.Named() })
			if got != tc.want {
				t.Fatalf("prediction = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestSelfProduceBuilder_ArtifactDocumentVisit(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: document, namespace: apps}\nspec:\n  sourceRef: {kind: ExternalArtifact, name: app-document}\nresources: [generator.yaml, consumer.yaml]\n")
	testutil.WriteFile(t, root, "generator.yaml", "apiVersion: source.extensions.fluxcd.io/v1beta1\nkind: ArtifactGenerator\nmetadata: {name: generator, namespace: apps}\nspec:\n  pathPattern: '@repo/{app}'\n  artifacts: [{name: 'app-{app}'}]\n")
	testutil.WriteFile(t, root, "consumer.yaml", testutil.KustomizationYAML("consumer", "apps", "  sourceRef: {kind: ExternalArtifact, name: app-api}\n"))
	reader := readerForTest(root)
	b := selfProduceBuilder{idx: &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}, store: store.New(), reader: reader, emittedFiles: map[string][]manifest.NamedResource{}}
	w := graphWalker{documents: reader, visited: map[graphContext]struct{}{}}
	var scope []graphVisit
	var declarations []producerDeclaration
	if !w.walk(graphVisit{root: root, dir: ".", source: manifest.BootstrapSourceID}, func(v graphVisit) bool {
		scope = append(scope, v)
		if v.document == nil && v.readable {
			declarations = b.recordProduced(v, nil, declarations)
		}
		return true
	}) {
		t.Fatal("walk stopped before completing")
	}
	b.joinProductions(declarations, scope)
	for name, want := range map[string]int{"app-api": 1, "app-document": 0} {
		id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: name}
		record, _ := b.idx.Lookup(id)
		assert.Equal(t, len(record.Productions), want)
	}
}

func TestFinalizeIndex_UntraversedBuilder(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "evidence/kustomization.yaml", "configMapGenerator:\n- name: untraversed\n  literals: [value=hidden]\n")
	testutil.WriteFile(t, root, "selected/kustomization.yaml", "configMapGenerator:\n- name: selected\n  literals: [value=visible]\n")
	l := New(store.New())
	l.SourceRoot = root
	admitForTest(l.Store, root)
	l.mountSources()
	l.documents.read(filepath.Join(root, "evidence"))
	_, err := l.Load(t.Context(), filepath.Join(root, "selected"))
	assert.NoError(t, err)
	if err := l.FinalizeIndex(t.Context(), root); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"untraversed", "selected"} {
		id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: name}
		_, indexed := l.Index.Lookup(id)
		if indexed != (name == "selected") {
			t.Fatalf("generator %s indexed = %v", name, indexed)
		}
	}
}
