package discovery_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	"github.com/home-operations/flate/pkg/values"
)

func TestRun_CrossTreeBaseEmissionGate(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	testutil.WriteFile(
		t,
		dir,
		"cluster/cluster-apps.yaml",
		testutil.KustomizationYAML(
			"cluster-apps",
			"flux-system",
			"  path: ./apps/test\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n",
		),
	)
	testutil.WriteFile(t, dir, "cluster/kustomization.yaml", "resources: [cluster-apps.yaml, ../self/ks.yaml]\n")
	testutil.WriteFile(t, dir, "apps/test/kustomization.yaml", "resources:\n  - ./app-a\n")
	testutil.WriteFile(t, dir, "apps/test/app-a/kustomization.yaml", "resources:\n  - ../../base/app-a\n")
	testutil.WriteFile(t, dir, "apps/base/app-a/kustomization.yaml", "resources:\n  - ks.yaml\n")
	// ${VAR} path — unresolvable at discovery, so app-a's own walk can't follow
	// it; only cluster-apps emits its file, giving an unambiguous gate.
	testutil.WriteFile(
		t,
		dir,
		"apps/base/app-a/ks.yaml",
		testutil.KustomizationYAML(
			"app-a",
			"flux-system",
			"  path: ./apps/${CLUSTER_ENV}/app-a\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n",
		),
	)

	// A self-emitting KS: its spec.path (./self) holds its own ks.yaml.
	testutil.WriteFile(t, dir, "self/kustomization.yaml", "resources:\n  - ks.yaml\n")
	testutil.WriteFile(
		t,
		dir,
		"self/ks.yaml",
		testutil.KustomizationYAML(
			"selfish",
			"flux-system",
			"  path: ./self\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n",
		),
	)

	st := store.New()
	res, err := discovery.Run(
		context.Background(),
		discovery.Config{
			Trees:       testTrees(t),
			Path:        filepath.Join(dir, "cluster"),
			RepoRoot:    dir,
			Store:       st,
			WipeSecrets: true,
		},
	)
	assert.FatalIf(t, err != nil, "Run: %v", err)

	appA := testutil.KustomizationID("flux-system", "app-a")
	clusterApps := testutil.KustomizationID("flux-system", "cluster-apps")
	{
		got := parentsForTest(res.Index)[appA]
		assert.ErrorIf(t, got != clusterApps, "ParentOf[app-a] = %v, want cluster-apps (cross-tree emission gate)", got)
	}

	selfish := testutil.KustomizationID("flux-system", "selfish")
	{
		got := parentsForTest(res.Index)[selfish]
		if got != res.Binding.Producer {
			t.Errorf("ParentOf[selfish] = %v, want entry %v",
				got,
				res.Binding.Producer,
			)
		}
	}
}

func TestRun_SymlinkEntryResolved(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	target := filepath.Join(dir, "real")
	{
		err := os.MkdirAll(target, 0o750)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	got, err := discovery.Run(t.Context(), discovery.Config{Trees: testTrees(t), Path: link, Store: store.New()})
	assert.FatalIf(t, err != nil, "%v", err)
	want, _ := filepath.EvalSymlinks(target)
	if got.RepoRoot != want {
		t.Errorf("RepoRoot = %q, want %q", got.RepoRoot, want)
	}
	entries, err := discovery.Plan(t.Context(), discovery.Config{Path: link})
	assert.FatalIf(t, err != nil, "%v", err)
	assert.FatalIf(
		t,
		len(entries) != 1 || entries[0].Path != want || entries[0].Input.LocalPath != want,
		"entry plan changed resolved source/scan bounds: %+v",
		entries,
	)
}

func TestRun_NoRepositoryUsesScanRoot(t *testing.T) {
	t.Parallel()
	for _, layout := range []string{"absent", "empty git directory"} {
		t.Run(layout, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			if layout == "empty git directory" {
				{
					err := os.Mkdir(filepath.Join(dir, ".git"), 0o750)
					assert.FatalIf(t, err != nil, "%v", err)
				}
			}
			st := store.New()
			result, err := discovery.Run(t.Context(), discovery.Config{Trees: testTrees(t), Path: dir, Store: st})
			assert.FatalIf(t, err != nil, "%v", err)
			artifact := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
			if result.RepoRoot != dir || artifact.LocalPath != dir {
				t.Errorf("no-repository roots = %q, %q; want %q",
					result.RepoRoot,
					artifact.LocalPath,
					dir,
				)
			}
		})
	}
}

// TestRun_AliasesURLMatchedInTreeGitRepository pins the Zariel/
// home-ops pattern: a GitRepository CR defined IN the tree
// whose spec.url points at the same remote the working tree
// itself clones from. Real Flux uses these with SOPS-decrypted
// SSH deploy keys; flate runs offline and can't materialize the
// key, so the fetch fails on a placeholder credential.
//
// Entry resolution detects that
// the URL matches the working tree's .git/config remote and
// overrides the artifact with a working-tree alias so the
// dependent KSes proceed against local files rather than the
// failed-fetch source.
func TestRun_AliasesURLMatchedInTreeGitRepository(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	repo := testutil.GitInit(t, dir)
	testutil.WriteFile(t, dir, ".git/config", `[core]
	repositoryformatversion = 0
[remote "origin"]
	url = git@github.com:Example/home-ops.git
`)
	{
		err := repo.Storer.SetReference(
			plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName("main")),
		)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	testutil.WriteFileAt(t, filepath.Join(dir, "k8s", "flux", "cluster.yaml"), "---\n"+
		`apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata:
  name: home-kubernetes
  namespace: flux-system
spec:
  url: ssh://git@github.com/example/home-ops.git
  ref:
    branch: main
  secretRef:
    name: github-deploy-key
`+"---\n"+
		testutil.KustomizationYAML("cluster", "flux-system", `  path: ./k8s/apps
  sourceRef:
    kind: GitRepository
    name: home-kubernetes
  interval: 1h
`))
	testutil.WriteFileAt(t, filepath.Join(dir, "k8s", "apps", "kustomization.yaml"), "resources: []\n")

	committed := testutil.GitCommitAll(
		t,
		repo,
		"fixture",
		&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}},
	)

	testutil.WriteFile(t, dir, "k8s/apps/kustomization.yaml", "resources: []\nnamePrefix: current-\n")
	testutil.GitCommitAll(
		t,
		repo,
		"current",
		&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(1, 0)}},
	)
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reverse_%t", reverse), func(t *testing.T) {
			t.Parallel()
			testRunAliasesURLMatchedInTreeGitRepositoryCase(
				t,
				testRunAliasesURLMatchedInTreeGitRepositoryCaseInputs{
					dir:       &dir,
					committed: &committed,
					reverse:   &reverse,
				},
			)
		},
		)
	}
}

// TestRun_LeavesUnmatchedInTreeGitRepository covers the
// negative case: an in-tree GitRepository whose URL points at a
// different remote (a real shared-infra source) must NOT get
// aliased to the working tree — that would silently render the
// wrong files. The store should still have the GitRepository
// from file-load, but without an aliased SourceArtifact.
func TestRun_LeavesUnmatchedInTreeGitRepository(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	repo := testutil.GitInit(t, dir)
	{
		_, err := repo.CreateRemote(
			&config.RemoteConfig{Name: "origin", URLs: []string{"git@github.com:Example/home-ops.git"}},
		)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	testutil.WriteFileAt(t, filepath.Join(dir, "k8s", "flux", "shared-infra.yaml"), "---\n"+
		`apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata:
  name: shared-infra
  namespace: flux-system
spec:
  url: https://github.com/upstream/shared-infra.git
  ref:
    branch: main
`+"---\n"+
		testutil.KustomizationYAML("cluster", "flux-system", `  path: ./k8s/apps
  sourceRef:
    kind: GitRepository
    name: shared-infra
  interval: 1h
`))
	testutil.WriteFileAt(t, filepath.Join(dir, "k8s", "apps", "kustomization.yaml"), "resources: []\n")

	st := store.New()
	{
		_, err := discovery.Run(context.Background(), discovery.Config{
			Trees: testTrees(t),
			Path:  dir, Store: st, WipeSecrets: true,
		})
		assert.FatalIf(t, err != nil, "Run: %v", err)
	}

	id := manifest.NamedResource{
		Group: "source.toolkit.fluxcd.io",
		Kind:  manifest.KindGitRepository, Namespace: "flux-system", Name: "shared-infra",
	}
	assert.ErrorIf(
		t,
		st.GetArtifact(id, store.ArtifactContent) != nil,
		"non-matching upstream GitRepository must NOT receive a working-tree alias artifact",
	)
}

// Generated data MUST use the render namespace, independent of the owning
// Kustomization's metadata.namespace, so dependency lookups address its output.
func TestRun_ComponentGeneratorLookupData(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	testutil.WriteFileAt(
		t,
		filepath.Join(dir, "flux", "ks.yaml"),
		testutil.KustomizationYAML(
			"cluster-config",
			"flux-system",
			"  path: ./cluster\n  sourceRef: {kind: GitRepository, name: flux-system}\n  interval: 10m\n",
		),
	)
	testutil.WriteFileAt(t, filepath.Join(dir, "cluster", "kustomization.yaml"), `apiVersion: kustomize.config.k8s.io/v1
kind: Kustomization
components:
  - ../components/cluster-settings
`)
	testutil.WriteFileAt(
		t,
		filepath.Join(dir, "components", "cluster-settings", "kustomization.yaml"),
		`apiVersion: kustomize.config.k8s.io/v1alpha1
kind: Component
configMapGenerator:
  - name: cluster-settings
    literals:
      - DOMAIN=example.com
      - TIMEZONE=UTC
`,
	)

	network := new(source.Network)
	defer network.Close()
	inputs := source.NewHTTPInputs(cachetest.New(t, "", cache.New), network, task.NewBounded(2))
	inputs.Close()
	st := store.New()
	{
		_, err := discovery.Run(t.Context(), discovery.Config{
			Trees: testTrees(t),
			Path:  dir, Store: st, WipeSecrets: true, RemoteInputs: &inputs,
		})
		assert.FatalIf(t, err != nil, "Run: %v", err)
	}

	cmID := manifest.NamedResource{
		Kind: manifest.KindConfigMap, Name: "cluster-settings",
	}
	cm, _ := values.NewStoreProvider(st, true).Data(cmID).(*manifest.ConfigMap)
	assert.FatalIf(t, st.GetObject(cmID) != nil, "%v", "generator lookup entered canonical inventory")
	assert.FatalIf(t, cm == nil, "ConfigMap %s unavailable through generator lookup", cmID)
	{
		got, _ := cm.Data["DOMAIN"].(string)
		assert.ErrorIf(t, got != "example.com", "DOMAIN = %q, want example.com", got)
	}
	{
		got, _ := cm.Data["TIMEZONE"].(string)
		assert.ErrorIf(t, got != "UTC", "TIMEZONE = %q, want UTC", got)
	}
}

// TestRun_KRMIgnoreFileScopesInitialScanOnly pins the override's reach:
// Config.KRMIgnoreFile filters the --path scan, while spec.path targets
// followed afterwards still honor their own .krmignore rather than the
// override (whose patterns are anchored at the scan root).
func TestRun_KRMIgnoreFileScopesInitialScanOnly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ks := func(name, path string) string {
		return testutil.KustomizationYAML(
			name,
			"flux-system",
			"  path: "+path+"\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n  interval: 10m\n",
		) + "---\n" +
			fmt.Sprintf(
				"apiVersion: source.extensions.fluxcd.io/v1beta1\nkind: ArtifactGenerator\nmetadata: {name: %s, namespace: flux-system, uid: %s}\nspec:\n  sources: [{alias: repo, kind: GitRepository, name: flux-system}]\n  artifacts: [{name: %s-output}]\n",
				name,
				name,
				name,
			)
	}
	testutil.WriteFileAt(t, filepath.Join(dir, "cluster", "keep.yaml"), ks("keep", "./apps"))
	testutil.WriteFileAt(t, filepath.Join(dir, "cluster", "drop.yaml"), ks("drop", "./apps"))
	testutil.WriteFileAt(t, filepath.Join(dir, "cluster", ".krmignore"), "keep.yaml\n")
	testutil.WriteFileAt(t, filepath.Join(dir, "override.krmignore"), "drop.yaml\n")
	testutil.WriteFileAt(t, filepath.Join(dir, "apps", "leaf.yaml"), ks("leaf", "./apps/leaf"))
	testutil.WriteFileAt(t, filepath.Join(dir, "apps", "hidden.yaml"), ks("hidden", "./apps/leaf"))
	testutil.WriteFileAt(
		t,
		filepath.Join(dir, "apps", "hidden-nested", "hidden.yaml"),
		ks("nested-hidden", "./apps/leaf"),
	)
	testutil.WriteFileAt(t, filepath.Join(dir, "apps", ".krmignore"), "hidden.yaml\nhidden-nested/\n")
	testutil.WriteFileAt(t, filepath.Join(dir, "apps", "leaf", "kustomization.yaml"), "resources: []\n")

	testutil.WriteFile(t, dir, "cluster/kustomization.yaml", "resources: [keep.yaml]\n")
	st := store.New()
	result, err := discovery.Run(t.Context(), discovery.Config{
		Trees: testTrees(t),
		Path:  filepath.Join(dir, "cluster"), RepoRoot: dir, Store: st, WipeSecrets: true,
		KRMIgnoreFile: filepath.Join(dir, "override.krmignore"),
	})
	assert.FatalIf(t, err != nil, "Run: %v", err)
	id := func(name string) manifest.NamedResource {
		return testutil.KustomizationID("flux-system", name)
	}
	for name, want := range map[string]bool{"keep": true, "drop": false, "leaf": true, "hidden": false, "nested-hidden": false} {
		{
			_, got := result.Index.Lookup(id(name))
			assert.ErrorIf(t, got != want, "index has Kustomization %s = %v, want %v", name, got, want)
		}
		generator := manifest.NamedResource{
			Group:     "source.extensions.fluxcd.io",
			Kind:      "ArtifactGenerator",
			Namespace: "flux-system",
			Name:      name,
		}
		_, present := result.Index.Lookup(generator)
		assert.ErrorIf(t, present != want, "index has ArtifactGenerator %s = %v, want %v", name, present, want)
		if want {
			record, _ := result.Index.Lookup(generator)
			assert.Diff(
				t,
				record.Refs,
				[]manifest.NamedResource{
					{
						Group:     manifest.SourceDomain,
						Kind:      manifest.KindGitRepository,
						Namespace: "flux-system",
						Name:      "flux-system",
					},
				},
			)
		}
		output := manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindExternalArtifact,
			Namespace: "flux-system",
			Name:      name + "-output",
		}
		productions := st.Productions(output, result.Index)
		{
			got := slices.ContainsFunc(
				productions,
				func(p manifest.Production) bool { return p.Declaration == generator },
			)
			assert.ErrorIf(
				t,
				got != want,
				"ArtifactGenerator %s output productions = %+v, want declaration %t",
				name,
				productions,
				want,
			)
		}
	}
}

func TestRun_GraphOwnedOrphans(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, resource, namespace string }{
		{"sibling", "../shared/demo", ""},
		{"direct", "../shared/demo/bundle.yaml", ""},
		{"explicit", "../shared/demo", "demo"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			testutil.WriteFile(
				t,
				dir,
				"flux/ks.yaml",
				testutil.KustomizationYAML(
					"owner",
					"flux-system",
					"  {path: ./cluster/per-cluster, sourceRef: {kind: GitRepository, name: flux-system}}\n",
				),
			)
			testutil.WriteFile(
				t,
				dir,
				"cluster/per-cluster/kustomization.yaml",
				"namespace: demo\nresources: ["+tc.resource+"]\n",
			)
			testutil.WriteFile(
				t,
				dir,
				"cluster/shared/demo/kustomization.yaml",
				"namespace: demo\nresources: [bundle.yaml]\n",
			)
			ns := ""
			if tc.namespace != "" {
				ns = ", namespace: " + tc.namespace
			}
			testutil.WriteFile(t, dir, "cluster/shared/demo/bundle.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: demo`+ns+`}
spec: {chart: {spec: {chart: demo, sourceRef: {kind: HelmRepository, name: demo}}}}
---
apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmRepository
metadata: {name: demo`+ns+`}
spec: {url: "https://charts.example.invalid", interval: 1h}
`)
			testutil.WriteFile(t, dir, "standalone.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: standalone, namespace: demo}
data: {value: fixture}
`)
			st := store.New()
			res, err := discovery.Run(
				t.Context(),
				discovery.Config{Trees: testTrees(t), Path: dir, RepoRoot: dir, Store: st, WipeSecrets: true},
			)
			assert.FatalIf(t, err != nil, "%v", err)
			raw := testutil.HelmReleaseID("demo", "demo")
			{
				_, ok := res.Index.Lookup(raw)
				assert.ErrorIf(t, !ok, "%v", "projected release must remain indexed")
			}
			assert.ErrorIf(t, st.GetObject(raw) != nil, "graph-owned release promoted: %s", raw)
			repos := st.ListAs[*manifest.HelmRepository](manifest.SourceDomain, manifest.KindHelmRepository)
			assert.ErrorIf(
				t,
				len(repos) != 1 || repos[0].Name != "demo" || repos[0].Namespace != "demo",
				"repositories = %+v, want only demo/demo",
				repos,
			)
			standalone := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "demo", Name: "standalone"}
			record, indexed := res.Index.Lookup(standalone)
			if !indexed || st.GetObject(standalone) == nil || res.Binding != (discovery.Binding{}) {
				t.Fatalf("standalone ConfigMap ownership = %+v, published=%v", record, st.GetObject(standalone))
			}
			assert.Equal(t, len(testutil.Owners(st.Productions(standalone, res.Index))), 0)
		})
	}
}

func TestRun_SecretPreparationPreservesPrecedence(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"entry-owned", "preexisting", "graph-owned"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testRunSecretPreparationPreservesPrecedenceCase(
				t,
				&scenario,
			)
		},
		)
	}
}

type testRunAliasesURLMatchedInTreeGitRepositoryCaseInputs struct {
	dir       *string
	committed *plumbing.Hash
	reverse   *bool
}

func testRunAliasesURLMatchedInTreeGitRepositoryCase(
	t *testing.T,
	testCaseInput testRunAliasesURLMatchedInTreeGitRepositoryCaseInputs,
) {
	t.Helper()

	st := store.New()
	pinned := &manifest.GitRepository{
		Name:      "committed",
		Namespace: "flux-system",
		URL:       "https://github.com/example/home-ops.git",
		Reference: &sourcev1.GitRepositoryRef{Commit: testCaseInput.committed.String()},
	}
	st.AddObject(pinned)
	objects := make([]manifest.BaseManifest, 0, 6)
	want := []string{
		(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "home-kubernetes"}).String(),
	}
	for _, kind := range []string{manifest.KindGitRepository, manifest.KindOCIRepository, manifest.KindBucket} {
		sourceID := manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      kind,
			Namespace: "other",
			Name:      "missing",
		}
		if kind == manifest.KindGitRepository {
			want = append(want, sourceID.String())
		}
		for _, name := range []string{"one", "two"} {
			objects = append(
				objects,
				&manifest.Kustomization{
					Name:      kind + name,
					Namespace: "other",
					Path:      "./k8s/apps",
					SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: kind, Name: sourceID.Name},
				},
			)
		}
	}
	if *testCaseInput.reverse {
		slices.Reverse(objects)
	}
	st.AddObjects(objects)
	{
		_, err := discovery.Run(
			t.Context(),
			discovery.Config{Trees: testTrees(t), Path: (*testCaseInput.dir), Store: st, WipeSecrets: true},
		)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	id := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "flux-system",
		Name:      "home-kubernetes",
	}
	artifact, ok := st.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
	assert.FatalIf(
		t,
		!ok || !artifact.InputAlias || !strings.HasPrefix(artifact.URL, "file://"),
		"matched source artifact = %+v",
		artifact,
	)
	{
		info, ok := st.GetStatus(id)
		assert.FatalIf(t, !ok || info.Status != store.StatusReady, "matched source status = %+v, %t", info, ok)
	}
	{
		artifact, ok := st.GetArtifact(pinned.Named(), store.ArtifactContent).(*store.SourceArtifact)
		assert.FatalIf(t, !ok ||
			artifact.InputAlias ||
			artifact.Key == "" ||
			st.GetObject(pinned.Named()) != pinned, "committed source artifact = %+v", artifact)
	}
	slices.Sort(want)
	want = append(want, (*testCaseInput.dir))
	warnings := st.Warnings()
	assert.FatalIf(
		t,
		len(warnings) != 1 || warnings[0].Category != manifest.WarnBootstrapAliases || warnings[0].Count != 1 ||
			!slices.Equal(warnings[0].Detail, want),
		"combined alias warning = %+v, want %v",
		warnings,
		want,
	)
}

func testRunSecretPreparationPreservesPrecedenceCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	dir := t.TempDir()
	testutil.WriteFile(
		t,
		dir,
		"flux/ks.yaml",
		testutil.KustomizationYAML(
			"owner",
			"flux-system",
			"  {path: ./apps, sourceRef: {kind: GitRepository, name: flux-system}}\n",
		),
	)
	resources := "producer.yaml"
	if (*testCaseInput) == "graph-owned" {
		resources += ", ../secret.yaml"
	}
	entry := "flux/ks.yaml"
	if (*testCaseInput) == "entry-owned" {
		entry += ", secret.yaml"
	}
	testutil.WriteFile(t, dir, "kustomization.yaml", "resources: ["+entry+"]\n")
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "namespace: secure\nresources: ["+resources+"]\n")
	testutil.WriteFile(t, dir, "apps/producer.yaml", `apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata: {name: producer, namespace: secure}
spec:
 target: {name: app-secret}
 data: [{secretKey: HOST, remoteRef: {key: fixture}}]
`)
	const authored = `apiVersion: v1
kind: Secret
metadata: {name: app-secret, namespace: secure}
stringData: {HOST: fixture-value}
`
	testutil.WriteFile(t, dir, "secret.yaml", authored)
	st := store.New()
	id := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "secure", Name: "app-secret"}
	preexisting := &manifest.Secret{
		Name:       id.Name,
		Namespace:  id.Namespace,
		StringData: map[string]any{"HOST": "preexisting-value"},
	}
	if (*testCaseInput) == "preexisting" {
		st.AddObject(preexisting)
	}
	result, err := discovery.Run(
		t.Context(),
		discovery.Config{Trees: testTrees(t), Path: dir, RepoRoot: dir, Store: st, WipeSecrets: true},
	)
	assert.FatalIf(t, err != nil, "%v", err)
	secret, ok := st.GetObject(id).(*manifest.Secret)
	if (*testCaseInput) != "preexisting" {
		assert.FatalIf(
			t,
			st.GetObject(id) != nil,
			"%s Secret published before rendering: %+v", (*testCaseInput), secret,
		)
	} else {
		assert.FatalIf(
			t,
			!ok || secret != preexisting || secret.StringData["HOST"] != "preexisting-value",
			"preexisting Secret replaced: %+v",
			secret,
		)
	}
	if (*testCaseInput) == "graph-owned" {
		lookup, ok := values.NewStoreProvider(st, true).Data(id).(*manifest.Secret)
		assert.FatalIf(
			t,
			!ok || lookup.StringData["HOST"] != manifest.ValuePlaceholder("HOST"),
			"lookup Secret = %+v, want declared-key placeholder",
			lookup,
		)
	}
	if (*testCaseInput) == "entry-owned" {
		declared, ok := result.Index.Records[id].Declaration.(*manifest.Secret)
		assert.FatalIf(
			t,
			!ok || declared.StringData["HOST"] != "fixture-value" || declared == secret,
			"prepared Secret = %+v, published=%+v",
			declared,
			secret,
		)
		if _, indexed := result.Index.Lookup(
			id,
		); !indexed ||
			!slices.Contains(testutil.Owners(st.Productions(id, result.Index)), result.Binding.Producer) {
			t.Fatalf("prepared Secret lost entry ownership: %+v", result.Index.Records[id])
		}
	}
	data, err := os.ReadFile(filepath.Join(dir, "secret.yaml"))
	assert.NoError(t, err)
	assert.Equal(t, string(data), authored)
	assert.Diff(t, preexisting.StringData, map[string]any{"HOST": "preexisting-value"})
	declarations := testutil.Declarations(st.Productions(id, nil))
	assert.FatalIf(t, !slices.Equal(
		declarations,
		[]manifest.NamedResource{
			{Group: "external-secrets.io", Kind: "ExternalSecret", Namespace: "secure", Name: "producer"},
		},
	), "declarations=%v", declarations)
}
