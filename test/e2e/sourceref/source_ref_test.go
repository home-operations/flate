// Package sourceref tests source selection, pinned trees and discovery.
package sourceref_test

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SourceRef_IgnoreNegation(t *testing.T) {
	const crd = `apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: network-attachment-definitions.k8s.cni.cncf.io
spec:
  group: k8s.cni.cncf.io
  scope: Namespaced
  names:
    plural: network-attachment-definitions
    singular: network-attachment-definition
    kind: NetworkAttachmentDefinition
  versions:
    - name: v1
      served: true
      storage: true
      schema:
        openAPIV3Schema:
          type: object
          properties:
            spec:
              type: object
              properties:
                config:
                  type: string
`
	sourceRoot := t.TempDir()
	repo := testutil.GitInit(t, sourceRoot)
	testutil.WriteFile(t, sourceRoot, "artifacts/networks-crd.yaml", crd)
	testutil.WriteFile(
		t,
		sourceRoot,
		"artifacts/excluded.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: excluded}\n",
	)
	commit := testutil.GitCommitAll(t, repo, "source", &gogit.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
	})
	assert.NoError(
		t,
		repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.7.7"), commit)),
	)
	installSourceRefTransport(t, repo.Storer)
	want, err := testutil.DecodeDocs(strings.NewReader(crd))
	assert.NoError(t, err)
	want[0]["metadata"].(map[string]any)["labels"] = map[string]any{
		"kustomize.toolkit.fluxcd.io/name":      "multus-crd",
		"kustomize.toolkit.fluxcd.io/namespace": "flux-system",
	}
	for _, tc := range []struct {
		name, path, ignore string
		excluded           bool
	}{
		{name: "omitted", ignore: "    /*\n    !artifacts/networks-crd.yaml\n"},
		{name: "explicit", path: "  path: ./artifacts\n", ignore: "    /*\n    !artifacts/networks-crd.yaml\n"},
		{name: "excluded", ignore: "    /*\n", excluded: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			testutil.GitInit(t, root)
			testutil.WriteFile(t, root, "flux/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: upstream, namespace: flux-system}
spec:
  interval: 10m
  url: git://fixture.invalid/cluster
  ref: {tag: v1.7.7}
  ignore: |
`+tc.ignore)
			testutil.WriteFile(t, root, "flux/consumer.yaml", testutil.KustomizationYAML("multus-crd", "flux-system",
				"  interval: 10m\n"+tc.path+"  sourceRef: {kind: GitRepository, name: upstream}\n"))
			args := []string{
				"build", "ks", "multus-crd", "--path", filepath.Join(root, "flux"),
				"--concurrency", "2", "--git-depth", "0", "--skip-kinds", "", "--cache-dir", t.TempDir(),
			}
			out, _ := harness.RequireCLIOK(t, args...)
			again, _ := harness.RequireCLIOK(t, args...)
			assert.Equal(t, again, out)
			docs, err := testutil.DecodeDocs(strings.NewReader(out))
			assert.NoError(t, err)
			if tc.excluded {
				assert.Equal(t, len(docs), 0)
			} else {
				assert.Diff(t, docs, want)
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SourceRef_EmptyPathMatchesRoot(t *testing.T) {
	fixture := func(t *testing.T, field string) (string, *store.Store, *discovery.Result) {
		t.Helper()
		root := t.TempDir()
		testutil.GitInit(t, root)
		testutil.WriteFile(
			t,
			root,
			"flux/root.yaml",
			testutil.KustomizationYAML(
				"root",
				"flux-system",
				"",
			)+field+`  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  targetNamespace: apps
  postBuild: {substitute: {VALUE: source-root}}
`,
		)
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: [cm.yaml, child.yaml]\n")
		testutil.WriteFile(t, root, "cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: root-owned}
data: {value: '${VALUE}'}
`)
		testutil.WriteFile(
			t,
			root,
			"child.yaml",
			testutil.KustomizationYAML(
				"child",
				"apps",
				"  path: ./leaf\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
			),
		)
		testutil.WriteFile(t, root, "leaf/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(t, root, "leaf/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: leaf-owned}\n")
		st := store.New()
		res, err := discovery.Run(t.Context(), discovery.Config{
			Path: filepath.Join(root, "flux"), RepoRoot: root, Store: st,
		})
		assert.NoError(t, err)
		return root, st, res
	}
	root, _, want := fixture(t, "  path: ./\n")
	wantOutput := harness.RunCLIStdout(t, "build", "all", "--path", filepath.Join(root, "flux"),
		"--concurrency", "2", "--cache-dir", t.TempDir())
	wantDocs, err := testutil.DecodeDocs(strings.NewReader(wantOutput))
	assert.NoError(t, err)
	for _, doc := range wantDocs {
		name, _ := manifest.DocMetadata(doc)
		if manifest.DocKind(doc) == manifest.KindKustomization && name == "root" {
			delete(doc["spec"].(map[string]any), "path")
		}
	}
	for _, path := range []struct{ name, field string }{
		{name: "explicit", field: "  path: ./\n"},
		{name: "omitted"},
	} {
		t.Run(
			path.name,
			func(t *testing.T) {
				e2ESourceRefEmptyPathMatchesRootName(t, &fixture, &path, struct {
					want     **discovery.Result
					wantDocs *[]map[string]any
				}{want: &want, wantDocs: &wantDocs})
			},
		)
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SourceRef_LastOwnerLocalChart(t *testing.T) {
	var roots [2]string
	for i, value := range []string{"before", "after"} {
		root := t.TempDir()
		roots[i] = root
		testutil.GitInit(t, root)
		testutil.WriteFile(t, root, "flux/release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: orphan, namespace: apps}
spec:
  interval: 10m
  chart:
    spec:
      chart: charts/demo
      sourceRef: {kind: GitRepository, name: cluster}
`)
		testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 1.0.0\n")
		testutil.WriteFile(
			t,
			root,
			"charts/demo/templates/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: chart-rendered, namespace: apps}\ndata: {value: "+value+"}\n",
		)
		testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"apps/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: owned, namespace: apps}\n",
		)
		if i == 0 {
			testutil.WriteFile(
				t,
				root,
				"flux/owner.yaml",
				testutil.KustomizationYAML(
					"owner",
					"apps",
					"  path: ./apps\n  sourceRef: {kind: GitRepository, name: cluster}\n",
				),
			)
		}
		out := harness.RunCLIStdout(
			t,
			"build",
			"all",
			"--path",
			filepath.Join(root, "flux"),
			"--concurrency",
			"2",
			"--cache-dir",
			t.TempDir(),
		)
		assert.FatalIf(
			t,
			!strings.Contains(out, "name: chart-rendered") || !strings.Contains(out, "value: "+value),
			"full render lost the local chart on %s side:\n%s",
			value,
			out,
		)
	}
	for _, tc := range []struct {
		name, value     string
		current, before int
	}{
		{name: "owner removed", value: "after", current: 1, before: 0},
		{name: "reversed", value: "before", current: 0, before: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := harness.RunCLIStdout(
				t,
				"build",
				"all",
				"--path",
				filepath.Join(roots[tc.current], "flux"),
				"--path-orig",
				filepath.Join(roots[tc.before], "flux"),
				"--concurrency",
				"2",
				"--cache-dir",
				t.TempDir(),
			)
			assert.FatalIf(
				t,
				!strings.Contains(out, "name: chart-rendered") || !strings.Contains(out, "value: "+tc.value),
				"changed-only render lost the local chart:\n%s",
				out,
			)
			out = harness.RunCLIStdout(
				t,
				"diff",
				"all",
				"--path",
				filepath.Join(roots[tc.current], "flux"),
				"--path-orig",
				filepath.Join(roots[tc.before], "flux"),
				"--concurrency",
				"2",
				"--cache-dir",
				t.TempDir(),
				"-o",
				"diff",
			)
			assert.FatalIf(t, !strings.Contains(out, "-  value: "+[]string{"before", "after"}[tc.before]) ||
				!strings.Contains(out, "+  value: "+tc.value), "comparison lost one side's local chart:\n%s", out)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SourceRef_PatchedStubKustomizationKeepsParentPath(t *testing.T) {
	for _, concurrency := range []string{"2", "8"} {
		t.Run("concurrency_"+concurrency, func(t *testing.T) {
			root := t.TempDir()
			testutil.GitInit(t, root)
			testutil.WriteFile(t, root, "flux/kustomization.yaml", `resources: [root.yaml, tmpl.yaml]
patches:
  - target:
      group: kustomize.toolkit.fluxcd.io
      version: v1
      kind: Kustomization
      name: tmpl
    patch: |-
      - op: add
        path: /spec/path
        value: ./apps
      - op: add
        path: /spec/sourceRef
        value: {kind: GitRepository, name: flux-system}
`)
			testutil.WriteFile(
				t,
				root,
				"flux/root.yaml",
				testutil.KustomizationYAML(
					"root",
					"flux-system",
					"  interval: 1m\n  prune: true\n  path: ./flux\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
				),
			)
			testutil.WriteFile(
				t,
				root,
				"flux/tmpl.yaml",
				testutil.KustomizationYAML("tmpl", "flux-system", "  {interval: 1m, prune: true}\n"),
			)
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"apps/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: apps-cm, namespace: apps}\n",
			)
			testutil.WriteFile(t, root, "unrelated/broken.yaml", "invalid: [\n")
			out, _ := harness.RequireCLIOK(t, "build", "all", "--path", filepath.Join(root, "flux"),
				"--concurrency", concurrency, "--cache-dir", t.TempDir())
			docs, err := testutil.DecodeDocs(strings.NewReader(out))
			assert.NoError(t, err)
			assert.Equal(t, len(docs), 3)
			seen := map[manifest.NamedResource]bool{}
			for _, doc := range docs {
				obj, _, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{DefaultNamespace: ""})
				assert.NoError(t, err)
				seen[obj.Named()] = true
				if ks, ok := obj.(*manifest.Kustomization); ok && ks.Name == "tmpl" {
					assert.Equal(t, ks.Path, "./apps")
					assert.Equal(t, ks.SourceRef.Name, "flux-system")
				}
			}
			assert.Diff(t, seen, map[manifest.NamedResource]bool{
				testutil.KustomizationID("flux-system", "root"):                    true,
				testutil.KustomizationID("flux-system", "tmpl"):                    true,
				{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "apps-cm"}: true,
			})
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SourceRef_NonHEADTag(t *testing.T) {
	root, repo, older, _ := sourceRefFixture(t)
	commit, err := repo.CommitObject(older)
	assert.NoError(t, err)
	tree, err := commit.Tree()
	assert.NoError(t, err)
	tree.Entries = append(tree.Entries, object.TreeEntry{Name: "module", Mode: filemode.Submodule, Hash: older})
	slices.SortFunc(tree.Entries, func(a, b object.TreeEntry) int { return strings.Compare(a.Name, b.Name) })
	encoded := repo.Storer.NewEncodedObject()
	assert.NoError(t, tree.Encode(encoded))
	commit.TreeHash, err = repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	encoded = repo.Storer.NewEncodedObject()
	assert.NoError(t, commit.Encode(encoded))
	pinned, err := repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	assert.NoError(
		t,
		repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), pinned)),
	)
	for _, verb := range []string{"build", "test"} {
		var previous string
		for range 2 {
			out, stderr := harness.RequireCLIOK(
				t,
				verb,
				"all",
				"--path",
				root+"/flux",
				"--concurrency=2",
				"--cache-dir",
				t.TempDir(),
			)
			assert.FatalIf(t, strings.Count(stderr, "[SkippedSubmodule]") != 1 || !strings.Contains(stderr, "module") ||
				!strings.Contains(stderr, "flux-system/cluster") ||
				strings.Contains(out, "skipped submodule"), "submodule destination: stdout=%s stderr=%s", out, stderr)
			assert.FatalIf(
				t,
				verb == "build" && (!strings.Contains(out, "value: v1.0.0") || strings.Contains(out, "value: v2.0.0")) ||
					verb == "test" && !strings.Contains(out, "passed"),
				"pinned product: %s",
				out,
			)
			assert.FatalIf(t, previous != "" && stderr != previous, "%v", "submodule warning changed")
			previous = stderr
		}
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SourceRef_PinnedSourceDiscoveredAfterConsumer(t *testing.T) {
	for _, tt := range []struct {
		name, appsFile, sourcesFile, sourcesName, sourceRefName, sourceURL string
		noRef                                                              bool
	}{
		{name: "apps_first/flux-system", appsFile: "a.yaml", sourcesFile: "z.yaml", sourcesName: "z-sources", sourceRefName: "flux-system"},
		{name: "apps_first/cluster", appsFile: "a.yaml", sourcesFile: "z.yaml", sourcesName: "z-sources", sourceRefName: "cluster"},
		{name: "sources_first/flux-system", appsFile: "z.yaml", sourcesFile: "a.yaml", sourcesName: "a-sources", sourceRefName: "flux-system"},
		{name: "sources_first/cluster", appsFile: "z.yaml", sourcesFile: "a.yaml", sourcesName: "a-sources", sourceRefName: "cluster"},
		{name: "unpinned/no_ref", appsFile: "a.yaml", sourcesFile: "z.yaml", sourcesName: "z-sources", sourceRefName: "flux-system", noRef: true},
		{name: "unpinned/non_matching", appsFile: "a.yaml", sourcesFile: "z.yaml", sourcesName: "z-sources", sourceRefName: "flux-system", sourceURL: "git://fixture.invalid/other"},
	} {
		t.Run(tt.name, func(t *testing.T) { e2ESourceRefPinnedSourceDiscoveredAfterConsumerName(t, &tt) })
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SourceRef_HeldErrorScope(t *testing.T) {
	for _, tt := range []struct {
		name, kind, sourceName, url, ref    string
		unreadable, bothHeld, reversedPaths bool
	}{
		{name: "oci", kind: "OCIRepository", sourceName: "second"},
		{name: "bootstrap", kind: "GitRepository", sourceName: "flux-system"},
		{name: "resolved_unavailable_tag", kind: "GitRepository", sourceName: "second", url: "git://fixture.invalid/cluster", ref: "  ref: {tag: unavailable}\n"},
		{name: "non_matching_url", kind: "GitRepository", sourceName: "second", url: "git://fixture.invalid/other"},
		{name: "non_decode_error", kind: "GitRepository", sourceName: "second", unreadable: true},
		{name: "two_held", kind: "GitRepository", sourceName: "second", bothHeld: true},
		{name: "two_held_path_order", kind: "GitRepository", sourceName: "second", bothHeld: true, reversedPaths: true},
	} {
		t.Run(tt.name, func(t *testing.T) { e2ESourceRefHeldErrorScopeName(t, &tt) })
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SourceRef_HeldErrorAcrossNamespaces(t *testing.T) {
	for _, tt := range []struct {
		name, betaURL string
		workingTree   bool
	}{
		{name: "external_source", betaURL: "git://fixture.invalid/other"},
		{name: "working_tree_source", betaURL: "git://fixture.invalid/cluster", workingTree: true},
	} {
		t.Run(tt.name, func(t *testing.T) { e2ESourceRefHeldErrorAcrossNamespacesName(t, &tt) })
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SourceRef_HeldErrorRetainedByEmptyPath(t *testing.T) {
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	{
		_, err := repo.CreateRemote(&config.RemoteConfig{
			Name: "origin", URLs: []string{"git://fixture.invalid/cluster"},
		})
		assert.FatalIf(t, err != nil, "%v", err)
	}
	writeKS := func(file, name, path, sourceName string) {
		t.Helper()
		pathField := ""
		if path != "" {
			pathField = "  path: " + path + "\n"
		}
		testutil.WriteFile(
			t,
			root,
			"flux/"+file,
			testutil.KustomizationYAML(
				name,
				"flux-system",
				"  interval: 10m\n"+pathField+"  sourceRef: {kind: GitRepository, name: "+sourceName+", namespace: flux-system}\n",
			),
		)
	}
	writeKS("a-x.yaml", "a-x", "./", "pinned")
	writeKS("b-z.yaml", "b-z", "", "flux-system")
	writeKS("z-sources.yaml", "z-sources", "./sources", "flux-system")
	testutil.WriteFile(t, root, "sources/kustomization.yaml", "resources: [repo.yaml]\n")
	testutil.WriteFile(t, root, "sources/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: pinned, namespace: flux-system}
spec:
  interval: 10m
  url: git://fixture.invalid/cluster
  ref: {tag: v1.0.0}
`)
	testutil.WriteFile(
		t,
		root,
		"apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {value: pinned}\n",
	)
	testutil.GitCommitAll(
		t,
		repo,
		"init",
		&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
	)
	head, err := repo.Head()
	assert.NoError(t, err)
	assert.NoError(t, repo.Storer.SetReference(
		plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), head.Hash()),
	))
	harness.MutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: pinned", "value: newer")
	testutil.GitCommitAll(
		t,
		repo,
		"init",
		&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
	)
	testutil.WriteFile(t, root, "apps/cm.yaml", "metadata: {name: broken\n")
	_, stderr, code := harness.RunCLIBuffers("build", "ks", "b-z", "--path", filepath.Join(root, "flux"),
		"--concurrency", "2", "--cache-dir", t.TempDir())
	assert.FatalIf(
		t,
		code != 1 || !strings.Contains(stderr, "apps/cm.yaml") || !strings.Contains(stderr, "b-z"),
		"expected empty-path owner failure: exit=%d stderr=%s",
		code,
		stderr,
	)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SourceRef_HeldErrorSurvivesNamespaceInheritance(t *testing.T) {
	for _, tt := range []struct {
		name, namespace, wantNamespace        string
		pinned, external, namespaceFreeSource bool
	}{
		{name: "explicit_source_namespace", namespace: ", namespace: flux-system", wantNamespace: "flux-system"},
		{name: "implicit_source_namespace", wantNamespace: "shifted"},
		{name: "pinned_inherited_namespace", wantNamespace: "shifted", pinned: true},
		{name: "external_source_in_other_inherited_namespace", wantNamespace: "shifted", external: true, namespaceFreeSource: true},
		{name: "pinned_namespace_free_source", wantNamespace: "shifted", pinned: true, namespaceFreeSource: true},
	} {
		t.Run(tt.name, func(t *testing.T) { e2ESourceRefHeldErrorSurvivesNamespaceInheritanceName(t, &tt) })
	}
}
