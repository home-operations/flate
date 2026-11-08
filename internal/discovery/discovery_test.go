package discovery_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/discovery"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRun_SmallTree(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	testutil.WriteFileAt(
		t,
		filepath.Join(dir, "flux", "parent.yaml"),
		testutil.KustomizationYAML(
			"parent",
			"flux-system",
			"  path: ./apps\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n  interval: 10m\n",
		),
	)
	testutil.WriteFileAt(
		t,
		filepath.Join(dir, "apps", "child.yaml"),
		testutil.KustomizationYAML(
			"child",
			"flux-system",
			"  path: ./apps/leaf\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n  interval: 10m\n",
		),
	)
	testutil.WriteFileAt(t, filepath.Join(dir, "apps", "leaf", "kustomization.yaml"), `resources: []
`)

	st := store.New()
	res, err := discovery.Run(context.Background(), discovery.Config{
		Trees: testTrees(t),
		Path:  dir, Store: st,
	})
	assert.FatalIf(t, err != nil, "Run: %v", err)
	wantRoot, _ := filepath.EvalSymlinks(dir)
	assert.ErrorIf(t, res.RepoRoot != wantRoot,
		"RepoRoot = %q, want %q", res.RepoRoot, wantRoot)

	parent := testutil.KustomizationID("flux-system", "parent")
	child := testutil.KustomizationID("flux-system", "child")
	for _, id := range []manifest.NamedResource{parent, child} {
		{
			_, ok := filesForTest(res.Index)[id]
			assert.ErrorIf(t, !ok, "SourceFiles missing %s", id)
		}
		assert.ErrorIf(t, st.GetObject(id) == nil, "discovery-only root omitted Kustomization %s", id)
	}

	{
		got := parentsForTest(res.Index)[child]
		assert.ErrorIf(t, got != parent, "ParentOf[child] = %v, want %v", got, parent)
	}

	// Synthetic bootstrap GR should be Ready so KSes resolve their
	// sourceRef without an explicit GitRepository file in the tree.
	bootstrap := manifest.BootstrapSourceID
	assert.ErrorIf(t, st.GetObject(bootstrap) == nil, "bootstrap GitRepository not seeded")
}

func TestRun_NamespaceIndependentOfDiscoveryDepth(t *testing.T) {
	t.Parallel()
	for _, repo := range []struct {
		name, url, ref string
	}{
		{"none", "", ""},
		{"no_ref", "https://example.invalid/self.git", ""},
		{"non_matching_ref", "https://example.invalid/x.git", "  ref: {branch: main}\n"},
	} {
		t.Run(repo.name, func(t *testing.T) {
			t.Parallel()
			testRunNamespaceIndependentOfDiscoveryDepthCase(
				t,
				&repo,
			)
		},
		)
	}
}

func TestRun_IndexesParentBeforeHelmReleaseArrival(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(
		t,
		dir,
		"flux/apps.yaml",
		testutil.KustomizationYAML(
			"apps",
			"flux-system",
			"  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
		),
	)
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources: [release.yaml, producer.yaml]\n")
	release := `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: demo
  namespace: apps
spec:
  chart:
    spec:
      chart: charts/demo
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`
	testutil.WriteFile(t, dir, "apps/release.yaml", release)
	testutil.WriteFile(t, dir, "orphan.yaml", strings.Replace(release, "name: demo", "name: orphan", 1))
	testutil.WriteFile(t, dir, "apps/producer.yaml", `apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata: {name: values, namespace: apps}
spec: {target: {name: values}}
`)

	st := store.New()
	foreign := &manifest.RawObject{
		APIVersion: "foreign.example/v1",
		Kind:       manifest.KindHelmRelease,
		Namespace:  "apps",
		Name:       "demo",
	}
	st.AddObject(foreign)
	res, err := discovery.Run(
		t.Context(),
		discovery.Config{Trees: testTrees(t), Path: dir, Store: st},
	)
	assert.FatalIf(t, err != nil, "%v", err)
	hr := testutil.HelmReleaseID("apps", "demo")
	parent := testutil.KustomizationID("flux-system", "apps")
	assert.FatalIf(t, st.GetObject(hr) != nil, "%v", "parent-owned HelmRelease must wait for rendering")
	{
		_, indexed := filesForTest(res.Index)[hr]
		assert.FatalIf(t, !indexed, "%v", "HelmRelease source identity must be indexed")
	}
	{
		got := parentsForTest(res.Index)[hr]
		assert.ErrorIf(t, got != parent, "ParentOf[%s] = %v, want %v", hr, got, parent)
	}
	secret := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "values"}
	producer := manifest.NamedResource{
		Group:     "external-secrets.io",
		Kind:      "ExternalSecret",
		Namespace: "apps",
		Name:      "values",
	}
	{
		declarations := testutil.Declarations(
			st.Productions(secret, nil),
		)
		assert.FatalIf(t, !slices.Equal(
			declarations,
			[]manifest.NamedResource{producer},
		), "declarations before arrival: %v", declarations)
	}
	foreignSecret := secret
	foreignSecret.Group = "foreign.example"
	{
		declarations := testutil.Declarations(st.Productions(foreignSecret, nil))
		assert.FatalIf(t, len(declarations) > 0, "%v", "foreign secret borrowed the core producer")
	}
	assert.FatalIf(t, st.GetObject(foreign.Named()) != foreign, "%v", "discovery replaced the foreign HelmRelease")
	{
		_, ok := res.Index.Lookup(foreign.Named())
		assert.FatalIf(t, ok, "%v", "foreign HelmRelease borrowed the real declaration record")
	}
	orphan := testutil.HelmReleaseID("apps", "orphan")
	{
		record, ok := res.Index.Lookup(
			orphan,
		)
		assert.ErrorIf(t, !ok || record.Parent != (manifest.NamedResource{}) ||
			st.GetObject(orphan) == nil, "%v", "standalone declaration was not promoted as a static root")
	}
}

// TestRun_AliasesNonDefaultNamespaceBootstrap pins issue #199: a
// Kustomization whose sourceRef points at a GitRepository in a
// non-`flux-system` namespace (typical of the flux-operator /
// FluxInstance pattern, where Flux runs in `gitops-system` and the
// root GitRepository is created out-of-band by the operator) must
// have that GitRepository aliased to the working tree so depwait
// resolves it. Without the fix, every consumer fails with
// `dependency not found: GitRepository.source.toolkit.fluxcd.io/gitops-system/gitops-system`.
func TestRun_AliasesNonDefaultNamespaceBootstrap(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	testutil.WriteFileAt(
		t,
		filepath.Join(dir, "flux", "cluster.yaml"),
		testutil.KustomizationYAML(
			"flux-repositories",
			"gitops-system",
			"  path: ./apps\n  sourceRef:\n    kind: GitRepository\n    name: gitops-system\n    namespace: gitops-system\n  interval: 1h\n",
		),
	)
	testutil.WriteFileAt(t, filepath.Join(dir, "apps", "kustomization.yaml"), "resources: []\n")

	st := store.New()
	{
		_, err := discovery.Run(context.Background(), discovery.Config{
			Trees: testTrees(t),
			Path:  dir, Store: st,
		})
		assert.FatalIf(t, err != nil, "Run: %v", err)
	}

	aliased := manifest.NamedResource{
		Group: "source.toolkit.fluxcd.io",
		Kind:  manifest.KindGitRepository, Namespace: "gitops-system", Name: "gitops-system",
	}
	assert.ErrorIf(
		t,
		st.GetObject(aliased) == nil,
		"expected GitRepository.source.toolkit.fluxcd.io/gitops-system/gitops-system to be aliased; alias scoped only to flux-system would leave this case broken",
	)
	assert.ErrorIf(
		t,
		st.GetArtifact(aliased, store.ArtifactContent) == nil,
		"aliased GitRepository should have a SourceArtifact so depwait resolves",
	)
	info, ok := st.GetStatus(aliased)
	assert.ErrorIf(
		t,
		!ok || info.Status != store.StatusReady,
		"aliased GitRepository should be Ready; got ok=%v info=%+v",
		ok,
		info,
	)
}

func TestRun_AdmittedScanCoordinates(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindGitRepository, manifest.KindOCIRepository, manifest.KindBucket} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			assertAdmittedScanCoordinates(t, filepath.Join(t.TempDir(), "virtual"), kind, "")
		})
	}
	t.Run("outside-root", func(t *testing.T) {
		t.Parallel()
		parent := t.TempDir()
		root := filepath.Join(parent, "virtual")
		input := &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: fstest.MapFS{}}
		{
			_, err := discovery.Run(t.Context(), discovery.Config{
				Trees: testTrees(t),
				Input: input, Path: filepath.Join(parent, "outside"), Store: store.New(),
			})
			assert.FatalIf(t, err == nil, "%v", "scan outside admitted root succeeded")
		}
		{
			_, err := discovery.Plan(
				t.Context(),
				discovery.Config{Input: input, Path: filepath.Join(parent, "outside")},
			)
			assert.FatalIf(t, err == nil, "%v", "entry plan escaped the admitted root")
		}
	})
}

//nolint:paralleltest // This test changes process-global state.
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRun_AdmittedRelativeScanCoordinates(t *testing.T) {
	t.Run("unchanged_entry_construction", testSessionUnchangedEntryConstruction)
	t.Chdir(t.TempDir())
	cwd, err := os.Getwd()
	assert.FatalIf(t, err != nil, "%v", err)
	for _, kind := range []string{manifest.KindGitRepository, manifest.KindOCIRepository, manifest.KindBucket} {
		t.Run(kind, func(t *testing.T) {
			assertAdmittedScanCoordinates(t, filepath.Join(cwd, "virtual"), kind, "virtual")
		})
	}
}

func assertAdmittedScanCoordinates(t *testing.T, root, kind, scan string) {
	t.Helper()
	{
		_, err := os.Stat(root)
		assert.FatalIf(t, !errors.Is(err, fs.ErrNotExist), "virtual label exists on disk: %v", err)
	}
	body := testutil.KustomizationYAML(
		"root",
		"flux-system",
		"  path: .\n  sourceRef: {kind: "+kind+", name: missing}\n",
	)
	input := &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: fstest.MapFS{"ks.yaml": {Data: []byte(body)}}}
	source := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      kind,
		Namespace: "flux-system",
		Name:      "missing",
	}
	wantIDs := []string{manifest.BootstrapSourceID.String(), testutil.KustomizationID("flux-system", "root").String()}
	if kind == manifest.KindGitRepository {
		wantIDs = append(wantIDs, source.String())
	}
	slices.Sort(wantIDs)
	run := func(scan string) store.SourceArtifact {
		t.Helper()
		st := store.New()
		result, err := discovery.Run(
			t.Context(),
			discovery.Config{Trees: testTrees(t), Input: input, Path: scan, Store: st},
		)
		assert.FatalIf(t, err != nil, "Run with Path %q: %v", scan, err)
		assert.FatalIf(
			t,
			result.RepoRoot != root || st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent) != input,
			"bootstrap root = %q, artifact = %+v",
			result.RepoRoot,
			st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent),
		)

		var ids []string
		for kind := range manifest.Kinds() {
			for _, obj := range st.ListObjects(kind.Group, kind.Kind) {
				ids = append(ids, obj.Named().String())
			}
		}
		slices.Sort(ids)
		assert.Diff(t, ids, wantIDs)
		alias := input
		if kind == manifest.KindGitRepository {
			status, present := st.GetStatus(source)
			assert.FatalIf(
				t,
				!present || status.Status != store.StatusReady || status.String() != "local entry source",
				"Git readiness = %+v",
				status,
			)
			var ok bool
			alias, ok = st.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
			assert.FatalIf(
				t,
				!ok || alias.LocalRoot != root || alias.LocalPath == "" || !alias.InputAlias,
				"source artifact = %+v",
				alias,
			)
		} else {
			assert.FatalIf(
				t,
				st.GetObject(source) != nil || st.GetArtifact(source, store.ArtifactContent) != nil,
				"%v",
				"missing non-Git source became available",
			)
		}
		got, err := fs.ReadFile(alias.FS, "ks.yaml")
		assert.FatalIf(t, err != nil || string(got) != body, "admitted alias bytes = %q, %v", got, err)
		{
			diff := cmp.Diff(
				&store.SourceArtifact{
					LocalPath: root,
					LocalRoot: root,
					FS:        fstest.MapFS{"ks.yaml": {Data: []byte(body)}},
				},
				input,
				cmpopts.IgnoreFields(store.SourceArtifact{}, "material"),
			)
			assert.ErrorIf(t, diff != "", "admitted input mismatch (-want +got):\n%s", diff)
		}
		aliasCopy := *alias
		aliasCopy.FS = nil
		aliasCopy.ContentFingerprint = nil
		return aliasCopy
	}
	{
		diff := cmp.Diff(run(root), run(scan), cmpopts.IgnoreFields(store.SourceArtifact{}, "material"))
		assert.ErrorIf(t, diff != "", "scan coordinate mismatch (-want +got):\n%s", diff)
	}
}

func TestRun_BootstrapOCIArtifactRoot(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindGitRepository, manifest.KindOCIRepository, manifest.KindBucket} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			testutil.WriteFile(
				t,
				dir,
				"entry/ks.yaml",
				testutil.KustomizationYAML(
					"parent",
					"apps",
					"  path: ./apps/x\n  sourceRef: {kind: "+kind+", name: source}\n",
				),
			)
			testutil.WriteFile(t, dir, "apps/x/kustomization.yaml", "resources: [cm.yaml, child.yaml]\n")
			testutil.WriteFile(
				t,
				dir,
				"apps/x/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload, namespace: apps}\ndata: {K: value}\n",
			)
			testutil.WriteFile(
				t,
				dir,
				"apps/x/child.yaml",
				testutil.KustomizationYAML(
					"child",
					"apps",
					"  path: ./consumer\n  sourceRef: {kind: "+kind+", name: source}\n",
				),
			)
			testutil.WriteFile(t, dir, "consumer/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t,
				dir,
				"consumer/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: result}\n",
			)
			testutil.WriteFile(
				t,
				dir,
				"sibling/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated}\n",
			)
			st := store.New()
			res, err := discovery.Run(
				t.Context(),
				discovery.Config{
					Trees:    testTrees(t),
					Path:     filepath.Join(dir, "entry"),
					RepoRoot: dir,
					Store:    st,
				},
			)
			assert.NoError(t, err)
			source := manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      kind,
				Namespace: "apps",
				Name:      "source",
			}
			parent := testutil.KustomizationID("apps", "parent")
			payload := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "payload"}
			if kind != manifest.KindGitRepository {
				assert.FatalIf(
					t,
					st.GetObject(source) != nil || st.GetArtifact(source, store.ArtifactContent) != nil,
					"%v",
					"missing non-Git source aliased",
				)
				{
					ownership := res.Index.OwnersOfFile("apps/x/cm.yaml")
					owners := ownership.Owners
					assert.FatalIf(t, len(owners) != 0, "missing source claimed owners: %v", owners)
				}
				{
					producers := st.Productions(payload, res.Index)
					assert.FatalIf(t, len(producers) != 0, "missing source claimed producers: %v", producers)
				}
				{
					readers := slices.Collect(res.Index.ReadersOfFile("apps/x/cm.yaml"))
					assert.FatalIf(t, len(readers) != 0, "missing source claimed readers: %v", readers)
				}
				return
			}
			art, ok := st.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
			assert.FatalIf(t, !ok || art.LocalRoot != dir, "Git source = %+v", art)
			{
				ownership2 := res.Index.OwnersOfFile("apps/x/cm.yaml")
				owners := ownership2.Owners
				assert.FatalIf(t, !slices.Contains(owners, parent), "Git owners = %v", owners)
			}
			{
				producers := testutil.Owners(st.Productions(payload, res.Index))
				assert.FatalIf(t, !slices.Contains(producers, parent), "Git producers = %v", producers)
			}
			{
				readers := slices.Collect(
					res.Index.ReadersOfFile("apps/x/cm.yaml"),
				)
				assert.FatalIf(t, !slices.ContainsFunc(
					readers,
					func(r manifest.FileReader) bool { return r.Reader == parent },
				), "Git readers = %v", readers)
			}
			{
				ownership3 := res.Index.OwnersOfFile("sibling/cm.yaml")
				owners := ownership3.Owners
				assert.FatalIf(t, len(owners) != 0, "unrelated owners = %v", owners)
			}
		})
	}
}

// TestRun_LoadsResourceSetAndDeepRSIP pins that discovery loads a
// ResourceSet and the RSIPs behind a Kustomization's spec.path into the
// store WITHOUT expanding the RS — expansion is now a run-time concern
// owned by the ResourceSet controller (a first-class DAG node), not
// discovery. Discovery must still walk the parent KS's path so the RSIP
// is seeded; the RS-emitted child Kustomization is NOT produced here.
func TestRun_LoadsResourceSetAndDeepRSIP(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Root: a parent KS that points at apps/, plus an RS at the same
	// level. The RS selects RSIPs by label in its own namespace.
	testutil.WriteFileAt(
		t,
		filepath.Join(dir, "flux", "parent.yaml"),
		testutil.KustomizationYAML(
			"parent",
			"flux-system",
			"  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n  interval: 10m\n",
		),
	)
	testutil.WriteFileAt(t, filepath.Join(dir, "flux", "rs.yaml"), `---
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: late-rs, namespace: flux-system}
spec:
  inputsFrom:
    - apiVersion: fluxcd.controlplane.io/v1
      kind: ResourceSetInputProvider
      selector:
        matchLabels: {role: db}
  resourcesTemplate: |
    ---
    apiVersion: kustomize.toolkit.fluxcd.io/v1
    kind: Kustomization
    metadata: {name: child-<< index (index inputs "provider") "name" >>, namespace: flux-system}
    spec:
      path: ./child
      sourceRef: {kind: GitRepository, name: flux-system}
`)
	// RSIP lives BEHIND the parent KS's path — discovery must expand
	// that path so the RSIP is loaded into the store for the run-time
	// RS controller to resolve.
	testutil.WriteFileAt(t, filepath.Join(dir, "apps", "rsip.yaml"), `---
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSetInputProvider
metadata:
  name: rsip
  namespace: flux-system
  labels: {role: db}
spec:
  type: Static
  defaultValues:
    user: alice
`)

	st := store.New()
	res, err := discovery.Run(context.Background(), discovery.Config{
		Trees: testTrees(t),
		Path:  dir, Store: st,
	})
	assert.FatalIf(t, err != nil, "Run: %v", err)

	// The RS itself is loaded into the store as a node for the run phase.
	rsID := manifest.NamedResource{
		Group:     "fluxcd.controlplane.io",
		Kind:      manifest.KindResourceSet,
		Namespace: "flux-system",
		Name:      "late-rs",
	}
	{
		_, ok := res.Index.Lookup(rsID)
		assert.ErrorIf(t, !ok, "expected ResourceSet %s loaded into store", rsID)
	}
	// The deep RSIP behind the parent KS's path is loaded — discovery
	// walked the spec.path even though it no longer expands the RS.
	rsipID := manifest.NamedResource{
		Group:     "fluxcd.controlplane.io",
		Kind:      manifest.KindResourceSetInputProvider,
		Namespace: "flux-system",
		Name:      "rsip",
	}
	{
		_, ok := res.Index.Lookup(rsipID)
		assert.ErrorIf(t, !ok, "expected ResourceSetInputProvider %s loaded into store (parent KS path walked)", rsipID)
	}
	// Discovery does NOT expand the RS anymore — the child Kustomization
	// is produced at run time by the RS controller, not here.
	child := testutil.KustomizationID("flux-system", "child-rsip")
	assert.ErrorIf(
		t,
		st.GetObject(child) != nil,
		"discovery should not pre-expand the RS; %s must be produced at run time",
		child,
	)
}

func TestRun_RequiresStoreAndLoader(t *testing.T) {
	t.Parallel()
	{
		_, err := discovery.Run(
			context.Background(),
			discovery.Config{Trees: testTrees(t), Path: t.TempDir()},
		)
		assert.ErrorIf(t, err == nil, "%v", "Run with nil Store/Loader: want error, got nil")
	}
}

func testRunNamespaceIndependentOfDiscoveryDepthCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		url string
		ref string
	},
) {
	t.Helper()

	for _, placement := range []string{"deep", "clusters"} {
		t.Run(placement, func(t *testing.T) {
			t.Parallel()
			testRunNamespaceIndependentOfDiscoveryDepthCaseCase(
				t,
				testRunNamespaceIndependentOfDiscoveryDepthCaseCaseInputs{
					testCaseInput: &testCaseInput,
					placement:     &placement,
				},
			)
		},
		)
	}
}

type testRunNamespaceIndependentOfDiscoveryDepthCaseCaseInputs struct {
	testCaseInput **struct {
		name string
		url  string
		ref  string
	}
	placement *string
}

func testRunNamespaceIndependentOfDiscoveryDepthCaseCase(
	t *testing.T,
	testCaseInput testRunNamespaceIndependentOfDiscoveryDepthCaseCaseInputs,
) {
	t.Helper()

	root := t.TempDir()
	testutil.GitInit(t, root)
	for _, file := range []struct {
		path, name, namespace, target, extra string
	}{
		{"flux/a.yaml", "a", "flux-system", "./apps", ""},
		{"flux/c.yaml", "c", "flux-system", "./clusters", ""},
		{"apps/team/x.yaml", "x", "", "./xout", ""},
		{"clusters/c2.yaml", "c2", "flux-system", "./deep", ""},
		{(*testCaseInput.placement) + "/b.yaml", "b", "flux-system", "./apps/team", "  targetNamespace: team\n"},
	} {
		testutil.WriteFile(
			t,
			root,
			file.path,
			testutil.KustomizationYAML(
				file.name,
				file.namespace,
				"  path: "+file.target+"\n"+file.extra+"  sourceRef: {kind: GitRepository, name: flux-system}\n",
			),
		)
	}
	if (*testCaseInput.testCaseInput).name != "none" {
		testutil.WriteFile(
			t,
			root,
			"flux/repo.yaml",
			fmt.Sprintf(`apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec:
  url: %s
%s`, (*testCaseInput.testCaseInput).url, (*testCaseInput.testCaseInput).ref),
		)
	}
	st := store.New()
	result, err := discovery.Run(t.Context(), discovery.Config{
		Trees: testTrees(t),
		Path:  filepath.Join(root, "flux"), Store: st,
		SelfURLs: []string{"https://example.invalid/self.git"},
	})
	assert.FatalIf(t, err != nil, "%v", err)
	admitted := map[manifest.NamedResource]struct{}{}
	for _, ks := range st.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization) {
		if !ks.Named().ReportHidden() {
			admitted[ks.Named()] = struct{}{}
		}
	}
	for id, record := range result.Index.Files() { //nolint:gocritic // The iterator supplies independent value snapshots.
		if id.Group == manifest.FluxKustomizeDomain && id.Kind == manifest.KindKustomization &&
			record.Builder != "" {
			admitted[id] = struct{}{}
			assert.Equal(t, st.GetObject(id) == nil, true)
			assert.Equal(t, record.Parent, result.Binding.Producer)
		}
	}
	var ids []string
	if len(admitted) > 0 {
		ids = make([]string, 0, len(admitted))
	}
	for id := range admitted {
		ids = append(ids, id.NamespacedName())
	}
	slices.Sort(ids)
	assert.Diff(
		t,
		ids,
		[]string{"flux-system/a", "flux-system/b", "flux-system/c", "flux-system/c2", "team/x"},
	)
}
