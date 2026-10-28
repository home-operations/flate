package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"testing/synctest"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/format/index"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func TestProjectEntryReaders_PreservesSideEvidence(t *testing.T) {
	t.Parallel()
	authored := testutil.KustomizationID("apps", "owner")
	internal := manifest.NewEntryKustomization(manifest.BootstrapSourceID, ".", manifest.NamedResource{}).Named()
	other := testutil.KustomizationID("apps", "other")
	for _, pair := range [][2]manifest.NamedResource{{authored, internal}, {internal, authored}} {
		t.Run(pair[0].NamespacedName(), func(t *testing.T) {
			t.Parallel()
			from, to := pair[0], pair[1]
			readers := []manifest.FileReader{
				{Reader: from, Source: manifest.BootstrapSourceID, Exact: true},
				{Reader: other, Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "other"}},
			}
			index := &manifest.DiscoveryIndex{Readers: map[string][]manifest.FileReader{"removed.yaml": readers}}
			projected := projectEntryReaders(index, discovery.Binding{Producer: from}, discovery.Binding{Producer: to})
			assert.Diff(t, index.Readers["removed.yaml"], readers)
			want := append([]manifest.FileReader{}, readers...)
			want[0].Reader = to
			assert.Diff(t, projected.Readers["removed.yaml"], want)

			changes := change.NewSet([]string{"removed.yaml"})
			assert.NoError(t, changes.CollectInputs(t.Context(), fstest.MapFS{"removed.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: removed}\n")}}, "removed.yaml", true))
			paired := changes.WithReaders(projected, &manifest.DiscoveryIndex{})
			st := store.New()
			local := &manifest.Kustomization{Name: to.Name, Namespace: to.Namespace}
			st.AddObject(local)
			root := t.TempDir()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
			filter := admissiontest.Filter(t, admissiontest.Config{Changes: paired, Index: &manifest.DiscoveryIndex{}, Inventory: st, Root: root})
			assert.Equal(t, filter.ShouldReconcile(to), true)
			assert.Equal(t, filter.ShouldReconcile(from), false)
			assert.Diff(t, changes.Paths(), []string{"removed.yaml"})
			assert.Diff(t, index.Readers["removed.yaml"], readers)
			projected.Readers["removed.yaml"][0].Exact = false
			assert.Equal(t, index.Readers["removed.yaml"][0].Exact, true)
		})
	}
}

// writeCluster lays out a two-app Flux cluster under dir: app-a renders
// ConfigMap cfg-a (data k=aVal), app-b renders ConfigMap cfg-b (data
// k=bVal). The two per-app Kustomizations are independent, so a change to
// one's rendered output must not pull the other into changed-only scope.
func writeCluster(t *testing.T, dir, aVal, bVal string) {
	t.Helper()
	ks := func(name string) string {
		return testutil.KustomizationYAML(name, "flux-system", "  path: ./apps/"+name+"\n"+
			"  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n")
	}
	cm := func(name, val string) string {
		return "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: " + name + "}\ndata: {k: " + val + "}\n"
	}
	testutil.WriteFile(t, dir, "kustomization.yaml", "resources: [ks-a.yaml, ks-b.yaml]\n")
	testutil.WriteFile(t, dir, "ks-a.yaml", ks("app-a"))
	testutil.WriteFile(t, dir, "ks-b.yaml", ks("app-b"))
	testutil.WriteFile(t, dir, "apps/app-a/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "apps/app-a/cm.yaml", cm("cfg-a", aVal))
	testutil.WriteFile(t, dir, "apps/app-b/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "apps/app-b/cm.yaml", cm("cfg-b", bVal))
}

// configMapNames returns the set of ConfigMap names across every parent's
// rendered manifests in a Result — what each side of the comparison
// actually rendered.
func configMapNames(res *Result) map[string]bool {
	out := map[string]bool{}
	for _, docs := range outputByParent(res.Manifests) {
		for _, m := range docs {
			if m["kind"] == "ConfigMap" {
				if meta, ok := m["metadata"].(map[string]any); ok {
					if name, ok := meta["name"].(string); ok {
						out[name] = true
					}
				}
			}
		}
	}
	return out
}

// Changed-only rendering must follow a nested child's effective namespace on
// both sides without selecting an unchanged sibling.
func TestRenderTrees_ChangedOnlyAndDiff(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, target, namespace, query string
		remote                         bool
	}{
		{"bare", "", "", "", false},
		{"target", "targetNamespace: flux-system, ", "flux-system", "", false},
		{"markerless remote", "targetNamespace: flux-system, ", "flux-system", "", true},
		{"markerless remote without submodules", "targetNamespace: flux-system, ", "flux-system", "?submodules=false", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			baseDir := t.TempDir()
			headDir := t.TempDir()
			writeCluster(t, baseDir, "v1", "same")
			writeCluster(t, headDir, "v2", "same")
			remote := ""
			if tc.remote {
				options := &git.CommitOptions{Author: &object.Signature{Name: "test", Email: "test@example.test", When: time.Unix(0, 0)}}
				module := t.TempDir()
				testutil.WriteFile(t, module, "kustomization.yaml", "resources: [cm.yaml]\n")
				testutil.WriteFile(t, module, "cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: from-submodule}\n")
				pin := testutil.GitCommitAll(t, testutil.GitInit(t, module), "module", options)
				root := t.TempDir()
				resources := "resources: [child.yaml, lib]\n"
				if tc.query != "" {
					resources = "resources: [child.yaml]\n"
					module = filepath.Join(module, "unavailable")
				}
				testutil.WriteFile(t, root, "kustomization.yaml", resources)
				testutil.WriteFile(t, root, "child.yaml", testutil.KustomizationYAML("consumer-a", "", "  path: ./consumer\n  sourceRef: {kind: GitRepository, name: flux-system}\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: render-values}]\n"))
				testutil.WriteFile(t, root, ".gitmodules", "[submodule \"lib\"]\n\tpath = lib\n\turl = file://"+module+"\n")
				repo := testutil.GitInit(t, root)
				testutil.GitCommitAll(t, repo, "remote", options)
				idx, err := repo.Storer.Index()
				assert.NoError(t, err)
				idx.Entries = append(idx.Entries, &index.Entry{Name: "lib", Mode: filemode.Submodule, Hash: pin})
				assert.NoError(t, repo.Storer.SetIndex(idx))
				wt, err := repo.Worktree()
				assert.NoError(t, err)
				_, err = wt.Commit("submodule", options)
				assert.NoError(t, err)
				remote = "file://" + root + tc.query
			}
			for i, dir := range []string{baseDir, headDir} {
				testutil.WriteFile(t, dir, "ks-a.yaml", testutil.KustomizationYAML("app-a", "flux-system", "  {path: ./apps/app-a, "+tc.target+"sourceRef: {kind: GitRepository, name: flux-system}}\n"))
				if i == 1 && tc.target != "" {
					testutil.WriteFile(t, dir, "apps/app-a/kustomization.yaml", "namespace: dirty\nresources: [cm.yaml]\n")
				} else if i == 1 {
					testutil.WriteFile(t, dir, "apps/app-a/kustomization.yaml", "resources: [cm.yaml]\ncommonAnnotations: {revision: head}\n")
				}
				testutil.WriteFile(t, dir, "apps/app-a/cm.yaml", testutil.KustomizationYAML("consumer-a", "", "  path: ./consumer\n  sourceRef: {kind: GitRepository, name: flux-system}\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: render-values}]\n"))
				if tc.remote {
					control := fmt.Sprintf("resources: [%q]\n", remote)
					if i == 1 {
						control += "# head\n"
					}
					testutil.WriteFile(t, dir, "apps/app-a/kustomization.yaml", control)
					if err := os.Remove(filepath.Join(dir, "apps/app-a/cm.yaml")); err != nil {
						t.Fatal(err)
					}
				}
				testutil.WriteFile(t, dir, "kustomization.yaml", "resources: [ks-a.yaml, ks-b.yaml, inputs]\n")
				testutil.WriteFile(t, dir, "inputs/kustomization.yaml", "resources: [values.yaml]\n")
				testutil.WriteFile(t, dir, "inputs/values.yaml", fmt.Sprintf("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: render-values, namespace: flux-system}\ndata: {VALUE: v%d}\n", i+1))
				testutil.WriteFile(t, dir, "consumer/kustomization.yaml", "resources:\n- cm.yaml\n")
				testutil.WriteFile(t, dir, "consumer/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cfg-a}\ndata: {k: '${VALUE}'}\n")
			}

			if tc.remote {
				for side, root := range []string{baseDir, headDir} {
					var first []byte
					for run := range 2 {
						rendered, err := RenderTrees(t.Context(), []Tree{{RepoRoot: root, Path: root, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}}, Config{CacheDir: t.TempDir(), Concurrency: 2})
						assert.NoError(t, err)
						result := rendered[0].Result
						assert.Equal(t, configMapNames(result)["from-submodule"], tc.query == "")
						byParent := outputByParent(result.Manifests)
						consumer := testutil.KustomizationID("flux-system", "consumer-a")
						if len(byParent[consumer]) != 1 || !configMapNames(result)["cfg-b"] {
							t.Fatalf("full remote reachability: %+v", result)
						}
						assert.Equal(t, byParent[consumer][0]["data"].(map[string]any)["k"], any(fmt.Sprintf("v%d", side+1)))
						body, err := json.Marshal(result.Manifests)
						assert.NoError(t, err)
						if run == 0 {
							first = body
						} else {
							assert.Equal(t, string(body), string(first))
						}
					}
				}
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			renderedTrees1, err := RenderTrees(ctx, []Tree{{RepoRoot: baseDir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}}, {RepoRoot: headDir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}}, Config{WipeSecrets: true,
				Network:     new(source.Network),
				Concurrency: 2})
			base, head := renderedTrees1[0], renderedTrees1[1]
			if (err != nil) != (tc.namespace == "") {
				t.Fatalf("RenderTrees: %v, want blocked=%t", err, tc.namespace == "")
			}
			if base.Result == nil || head.Result == nil {
				t.Fatalf("both sides must render: base=%v head=%v", base.Result, head.Result)
			}

			for _, side := range []Rendered{base, head} {
				children := 0
				for _, obj := range side.Store().ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization) {
					if obj.Name == "consumer-a" {
						children++
						assert.Equal(t, obj.Namespace, tc.namespace)
						assert.Equal(t, obj.SourceID().Namespace, tc.namespace)
					}
				}
				assert.Equal(t, children, 1)
			}

			consumer := testutil.KustomizationID(tc.namespace, "consumer-a")
			for i, side := range []Rendered{base, head} {
				if tc.remote {
					assert.Equal(t, configMapNames(side.Result)["from-submodule"], tc.query == "")
				}
				if tc.namespace == "" {
					missing := manifest.NamedResource{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindGitRepository, Name: "flux-system"}
					if side.Err == nil || !slices.Contains(side.Result.Blocked[consumer], missing) || configMapNames(side.Result)["cfg-a"] {
						t.Fatalf("side %d bare child: error=%v blockers=%v manifests=%v", i, side.Err, side.Result.Blocked, side.Result.Manifests)
					}
				} else {
					values := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: tc.namespace, Name: "render-values"}
					obj, ok := side.Store().Get[*manifest.ConfigMap](values)
					if side.Err != nil || len(side.Result.Blocked) != 0 || !ok || obj.Data["VALUE"] != fmt.Sprintf("v%d", i+1) {
						t.Fatalf("side %d producer %s: error=%v blockers=%v object=%v", i, values, side.Err, side.Result.Blocked, obj)
					}
				}
				if configMapNames(side.Result)["cfg-b"] {
					t.Fatalf("side %d rendered unchanged sibling", i)
				}
			}
			if tc.namespace == "" {
				return
			}

			for i, rendered := range []Rendered{base, head} {
				obj, ok := rendered.Store().Get[*manifest.Kustomization](consumer)
				if !ok {
					t.Fatalf("side %d lost effective consumer identity", i)
				}
				owners := testutil.Owners(rendered.Store().Productions(consumer, nil))
				if !slices.Contains(owners, testutil.KustomizationID("flux-system", "app-a")) {
					t.Fatalf("side %d owners=%v", i, owners)
				}
				ns := obj.Namespace
				assert.Equal(t, ns, "flux-system")
				docs := outputByParent(rendered.Result.Manifests)[consumer]
				if len(docs) != 1 {
					t.Fatalf("side %d consumer documents = %d", i, len(docs))
				}
				assert.Equal(t, docs[0]["data"].(map[string]any)["k"], any(fmt.Sprintf("v%d", i+1)))
				_, ns = manifest.DocMetadata(docs[0])
				assert.Equal(t, ns, "")
			}

			selected, selectedErr, _ := Select([]Rendered{base, head}, err,
				[]manifest.NamedResource{testutil.KustomizationID("", ""), testutil.HelmReleaseID("", "")}, nil)
			body, _, _, err := diff.Compare(
				diff.Snapshot{Subjects: selected[0].Subjects, Manifests: selected[0].Manifests, ProducerKeys: selected[0].ProducerKeys, Unavailable: selected[0].Unavailable},
				diff.Snapshot{Subjects: selected[1].Subjects, Manifests: selected[1].Manifests, ProducerKeys: selected[1].ProducerKeys, Unavailable: selected[1].Unavailable},
				selectedErr, diff.Options{Format: diff.FormatJSON})
			if err != nil {
				t.Fatal(err)
			}
			var output struct {
				Changes []diff.Change `json:"changes"`
			}
			if err := json.Unmarshal(body, &output); err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, len(output.Changes), 2)
			changes := slices.DeleteFunc(output.Changes, func(change diff.Change) bool {
				if change.Name == "render-values" {
					assert.Equal(t, change.Parent.NamedResource, manifest.NamedResource{})
					assert.Equal(t, change.Parent.Path, ".")
					assert.Equal(t, change.Status, diff.StatusChanged)
					return true
				}
				return false
			})
			if len(changes) != 1 {
				t.Fatalf("want exactly 1 change (cfg-a), got %d: %+v", len(changes), changes)
			}
			c := changes[0]
			if c.Name != "cfg-a" || c.Kind != "ConfigMap" || c.Status != diff.StatusChanged {
				t.Errorf("change = %+v; want ConfigMap cfg-a changed", c)
			}
		})
	}
}

func TestRenderTrees_ChangedOnlyAndDiff_SelectorRelabel(t *testing.T) {
	roots := [2]string{t.TempDir(), t.TempDir()}
	trees := make([]Tree, len(roots))
	for side, root := range roots {
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: [set.yaml, provider-a.yaml, provider-b.yaml]\n")
		testutil.WriteFile(t, root, "set.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: matrix, namespace: apps}
spec:
  inputsFrom:
    - kind: ResourceSetInputProvider
      selector:
        matchLabels: {app: selected}
  resourcesTemplate: |
    apiVersion: v1
    kind: ConfigMap
    metadata: {name: 'cfg-<< inputs.name >>', namespace: apps}
    data: {value: '<< inputs.value >>'}
`)
		for _, name := range []string{"a", "b"} {
			label := "selected"
			if side == 1 && name == "a" {
				label = "other"
			}
			testutil.WriteFile(t, root, "provider-"+name+".yaml", fmt.Sprintf(`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSetInputProvider
metadata:
  name: provider-%s
  namespace: apps
  labels: {app: %s}
spec:
  type: Static
  defaultValues: {name: %s, value: stable}
`, name, label, name))
		}
		trees[side] = Tree{RepoRoot: root, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)}}
	}

	render := func(trees []Tree) []Rendered {
		t.Helper()
		runs, err := RenderTrees(t.Context(), trees, Config{CacheDir: t.TempDir(), Concurrency: 2})
		assert.NoError(t, err)
		assert.Equal(t, len(runs), len(trees))
		for _, run := range runs {
			assert.NoError(t, run.Err)
			if run.Result == nil {
				t.Fatal("missing render result")
			}
			assert.Equal(t, len(run.Result.Blocked), 0)
			assert.Equal(t, len(run.Result.Failed), 0)
		}
		return runs
	}
	compare := func(runs []Rendered) []byte {
		t.Helper()
		selected, selectedErr, remainder := Select(runs, nil, []manifest.NamedResource{
			testutil.KustomizationID("", ""),
			{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet},
		}, nil)
		assert.NoError(t, selectedErr)
		assert.NoError(t, remainder)
		body, _, _, err := diff.Compare(
			diff.Snapshot{Subjects: selected[0].Subjects, Manifests: selected[0].Manifests, ProducerKeys: selected[0].ProducerKeys, Unavailable: selected[0].Unavailable},
			diff.Snapshot{Subjects: selected[1].Subjects, Manifests: selected[1].Manifests, ProducerKeys: selected[1].ProducerKeys, Unavailable: selected[1].Unavailable},
			selectedErr, diff.Options{Format: diff.FormatJSON})
		assert.NoError(t, err)
		return body
	}

	full := []Rendered{render(trees[:1])[0], render(trees[1:])[0]}
	for _, run := range full {
		assert.Equal(t, run.filter.Enabled(), false)
	}
	assert.Diff(t, configMapNames(full[0].Result), map[string]bool{"cfg-a": true, "cfg-b": true})
	assert.Diff(t, configMapNames(full[1].Result), map[string]bool{"cfg-b": true})
	want := compare(full)
	var first []byte
	for run := range 2 {
		paired := render(trees)
		for _, side := range paired {
			assert.Equal(t, side.filter.Enabled(), true)
			assert.Diff(t, side.cfg.ExternalChanges.Paths(), []string{"provider-a.yaml"})
		}
		assert.Diff(t, configMapNames(paired[0].Result), map[string]bool{"cfg-a": true, "cfg-b": true})
		assert.Diff(t, configMapNames(paired[1].Result), map[string]bool{"cfg-b": true})
		body := compare(paired)
		assert.Diff(t, string(body), string(want))
		var output struct {
			Changes []diff.Change `json:"changes"`
		}
		assert.NoError(t, json.Unmarshal(body, &output))
		assert.Equal(t, len(output.Changes), 2)
		var deleted []string
		for _, change := range output.Changes {
			if change.Kind == manifest.KindConfigMap {
				assert.Equal(t, change.Status, diff.StatusRemoved)
				deleted = append(deleted, change.Name)
			} else {
				assert.Equal(t, change.Kind, manifest.KindResourceSetInputProvider)
				assert.Equal(t, change.Name, "provider-a")
				assert.Equal(t, change.Status, diff.StatusChanged)
			}
		}
		assert.Diff(t, deleted, []string{"cfg-a"})
		if run == 0 {
			first = body
		} else {
			assert.Diff(t, string(body), string(first))
		}
	}
}

func TestRenderTrees_MultipleValuesFromKeys(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		keys    []string
		changed bool
	}{
		{"unchanged then changed", []string{"stable.yaml", "changed.yaml"}, true},
		{"changed then unchanged", []string{"changed.yaml", "stable.yaml"}, true},
		{"all unchanged", []string{"stable.yaml", "changed.yaml"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			roots := [2]string{t.TempDir(), t.TempDir()}
			trees := make([]Tree, 2)
			for side, root := range roots {
				value := "old"
				if side == 1 && tc.changed {
					value = "new"
				}
				testutil.WriteFile(t, root, "flux/values.yaml", fmt.Sprintf("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: values, namespace: apps}\ndata:\n  stable.yaml: 'stable: same'\n  changed.yaml: 'changed: %s'\n  unused: '%d'\n", value, side))
				release := "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: app, namespace: apps}\nspec:\n  interval: 1m\n  chart: {spec: {chart: charts/demo, sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}}}\n  valuesFrom:\n"
				for _, key := range tc.keys {
					release += "  - {kind: ConfigMap, name: values, valuesKey: " + key + "}\n"
				}
				testutil.WriteFile(t, root, "flux/release.yaml", release)
				testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 1.0.0\n")
				testutil.WriteFile(t, root, "charts/demo/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\ndata:\n  changed: {{ .Values.changed | quote }}\n  stable: {{ .Values.stable | quote }}\n")
				trees[side] = Tree{RepoRoot: root, Path: filepath.Join(root, "flux"), ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)}, ExternalChanges: change.NewSet([]string{"flux/values.yaml"})}
			}
			runs, err := RenderTrees(t.Context(), trees, Config{CacheDir: t.TempDir(), Concurrency: 2})
			assert.NoError(t, err)
			assert.Equal(t, len(runs), 2)
			app := testutil.HelmReleaseID("apps", "app")
			for side, run := range runs {
				assert.NoError(t, run.Err)
				assert.Equal(t, len(run.Result.Warnings), 0)
				assert.Equal(t, len(run.Result.ClusterWarnings), 0)
				assert.Equal(t, len(run.Result.Blocked), 0)
				_, selected, dirty, _ := run.filter.Membership(app)
				assert.Equal(t, selected, tc.changed)
				assert.Equal(t, dirty, tc.changed)
				docs := outputByParent(run.Result.Manifests)[app]
				if !tc.changed {
					assert.Equal(t, len(docs), 0)
					continue
				}
				assert.Equal(t, len(docs), 1)
				if len(docs) == 1 {
					value := "old"
					if side == 1 {
						value = "new"
					}
					assert.Diff(t, docs[0]["data"], map[string]any{"changed": value, "stable": "same"})
				}
			}
		})
	}
}

func TestRenderTrees_ParentIntervalExternalChart(t *testing.T) {
	t.Parallel()
	roots := [2]string{t.TempDir(), t.TempDir()}
	trees := make([]Tree, 2)
	for side, root := range roots {
		testutil.WriteFile(t, root, "flux/parent.yaml", testutil.KustomizationYAML("parent", "apps", fmt.Sprintf("  interval: %dm\n  path: ./apps/child\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n", side+1)))
		testutil.WriteFile(t, root, "apps/child/kustomization.yaml", "resources: [release.yaml]\n")
		testutil.WriteFile(t, root, "apps/child/release.yaml", "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: child, namespace: apps}\nspec:\n  interval: 1m\n  chart: {spec: {chart: charts/demo, sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}}}\n")
		testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 1.0.0\n")
		testutil.WriteFile(t, root, "charts/demo/templates/cm.yaml", fmt.Sprintf("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\ndata: {value: v%d}\n", side+1))
		trees[side] = Tree{RepoRoot: root, Path: filepath.Join(root, "flux"), ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)}}
	}
	changes, err := change.Detect(t.Context(), testDirectoryInput(t, roots[0]), testDirectoryInput(t, roots[1]))
	assert.NoError(t, err)
	assert.Diff(t, changes.Paths(), []string{"charts/demo/templates/cm.yaml", "flux/parent.yaml"})
	child := testutil.HelmReleaseID("apps", "child")
	parent := testutil.KustomizationID("apps", "parent")
	full := make([]Rendered, 2)
	for side, tree := range trees {
		runs, err := RenderTrees(t.Context(), []Tree{tree}, Config{CacheDir: t.TempDir(), Concurrency: 2})
		assert.NoError(t, err)
		assert.Equal(t, len(runs), 1)
		full[side] = runs[0]
	}
	delta := func(runs []Rendered) []byte {
		t.Helper()
		for side, run := range runs {
			assert.NoError(t, run.Err)
			assert.Equal(t, len(run.Result.Warnings), 0)
			assert.Equal(t, len(run.Result.ClusterWarnings), 0)
			assert.Equal(t, len(run.Result.Blocked), 0)
			docs := outputByParent(run.Result.Manifests)[child]
			assert.Equal(t, len(docs), 1)
			if len(docs) == 1 {
				assert.Diff(t, docs[0]["data"], map[string]any{"value": fmt.Sprintf("v%d", side+1)})
			}
		}
		selected, selectedErr, err := Select(runs, nil, []manifest.NamedResource{testutil.KustomizationID("", ""), testutil.HelmReleaseID("", "")}, nil)
		assert.NoError(t, err)
		body, warnings, suppressed, err := diff.Compare(
			diff.Snapshot{Subjects: selected[0].Subjects, Manifests: selected[0].Manifests, ProducerKeys: selected[0].ProducerKeys, Unavailable: selected[0].Unavailable},
			diff.Snapshot{Subjects: selected[1].Subjects, Manifests: selected[1].Manifests, ProducerKeys: selected[1].ProducerKeys, Unavailable: selected[1].Unavailable},
			selectedErr, diff.Options{Format: diff.FormatJSON})
		assert.NoError(t, err)
		assert.Equal(t, len(warnings), 0)
		assert.Equal(t, len(suppressed), 0)
		return body
	}
	want := delta(full)
	var expected struct {
		Changes []diff.Change `json:"changes"`
	}
	assert.NoError(t, json.Unmarshal(want, &expected))
	assert.Equal(t, len(expected.Changes) > 0, true)
	assert.Equal(t, slices.ContainsFunc(expected.Changes, func(c diff.Change) bool {
		return c.Parent.NamedResource == child && c.Kind == "ConfigMap" && c.Name == "output" && c.Status == diff.StatusChanged
	}), true)
	t.Logf("independent full delta: %s", want)
	var first [2][]byte
	for run := range 2 {
		for side := range trees {
			trees[side].ExternalChanges = changes
		}
		runs, err := RenderTrees(t.Context(), trees, Config{CacheDir: t.TempDir(), Concurrency: 2})
		assert.NoError(t, err)
		assert.Equal(t, len(runs), 2)
		assert.Equal(t, string(delta(runs)), string(want))
		for side, rendered := range runs {
			needed, selected, dirty, _ := rendered.filter.Membership(parent)
			assert.Equal(t, needed, true)
			assert.Equal(t, selected, true)
			t.Logf("run %d side %d parent membership: needed=%t selected=%t dirty=%t", run, side, needed, selected, dirty)
			_, selected, dirty, _ = rendered.filter.Membership(child)
			assert.Equal(t, selected, true)
			assert.Equal(t, dirty, true)
			assert.Equal(t, rendered.Store().GetObject(child) != nil, true)
			body, err := json.Marshal(rendered.Result.Manifests)
			assert.NoError(t, err)
			if run == 0 {
				first[side] = body
			} else {
				assert.Equal(t, string(body), string(first[side]))
			}
		}
	}
}

func TestRenderTrees_OptionalInputRoutes(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"directory_0300", "committed_0301"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			before, after := t.TempDir(), t.TempDir()
			for _, root := range []string{before, after} {
				testutil.WriteFile(t, root, "flux/cluster.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec: {url: https://example.test/cluster.git}
`)
				testutil.WriteFile(t, root, "flux/app.yaml", testutil.KustomizationYAML("app", "flux-system", "  path: ./apps/app\n  sourceRef: {kind: GitRepository, name: cluster}\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: optional, optional: true}]\n"))
				testutil.WriteFile(t, root, "flux/stable.yaml", testutil.KustomizationYAML("stable", "flux-system", "  path: ./apps/stable\n  sourceRef: {kind: GitRepository, name: cluster}\n  dependsOn: [{name: app}]\n"))
				for _, name := range []string{"app", "stable"} {
					testutil.WriteFile(t, root, "apps/"+name+"/kustomization.yaml", "resources: [output.yaml]\n")
					testutil.WriteFile(t, root, "apps/"+name+"/output.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+", namespace: flux-system}\ndata: {value: '${VALUE:=old}'}\n")
				}
			}
			owner := cachetest.New(t, "", sourcecache.New)
			var base string
			var repo *git.Repository
			options := &git.CommitOptions{Author: &object.Signature{Name: "test", Email: "test@example.test", When: time.Unix(0, 0)}}
			if route == "committed_0301" {
				repo = testutil.GitInit(t, after)
				base = testutil.GitCommitAll(t, repo, "base", options).String()
			}
			testutil.WriteFile(t, after, "flux/optional.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: optional, namespace: flux-system}\ndata: {VALUE: new}\n")
			var inputs *baseline.Result
			var err error
			if repo != nil {
				testutil.GitCommitAll(t, repo, "optional input appears", options)
				inputs, err = baseline.AutoResolve(t.Context(), after, base, owner)
			} else {
				inputs, err = baseline.ResolvePaths(t.Context(), after, before)
			}
			assert.NoError(t, err)
			if repo != nil && inputs.Changes == nil {
				t.Fatal("committed resolver returned no change set")
			}
			app := testutil.KustomizationID("flux-system", "app")
			stable := testutil.KustomizationID("flux-system", "stable")
			cluster := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "cluster"}
			fullApp := make([][]map[string]any, 2)
			for side, input := range []*store.SourceArtifact{inputs.BaseInput, inputs.Source} {
				renderedSingleton1, err := RenderTrees(t.Context(), []Tree{{Path: input.LocalPath, RepoRoot: input.LocalPath, Input: input, SelfURLs: []string{"https://example.test/cluster.git"}, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}}, Config{Cache: owner, Concurrency: 2})
				if len(renderedSingleton1) != 1 {
					t.Fatalf("missing render: %v", err)
				}
				result := renderedSingleton1[0].Result
				assert.NoError(t, err)
				fullApp[side] = outputByParent(result.Manifests)[app]
				if len(fullApp[side]) != 1 {
					t.Fatalf("side %d full app documents = %d", side, len(fullApp[side]))
				}
				want := "${VALUE:=old}"
				if side == 1 {
					want = "new"
				}
				assert.Diff(t, fullApp[side][0]["data"], map[string]any{"value": want})
			}
			cfg := treeConfig{Cache: owner, Concurrency: 2, BaseInput: inputs.BaseInput, Input: inputs.Source, ExternalChanges: inputs.Changes}
			baseTree := Tree{RepoRoot: inputs.BaseInput.LocalPath, Path: inputs.BaseScanPath, SelfURLs: []string{"https://example.test/cluster.git"}}
			headTree := Tree{RepoRoot: inputs.Source.LocalPath, Path: inputs.ScanPath, SelfURLs: []string{"https://example.test/cluster.git"}}
			var first [2][]byte
			for run := range 2 {
				renderedTrees2, err := RenderTrees(t.Context(), []Tree{{RepoRoot: baseTree.RepoRoot, Path: baseTree.Path, SelfURLs: baseTree.SelfURLs, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}, Input: cfg.BaseInput, BaseInput: cfg.Input, ExternalChanges: cfg.ExternalChanges}, {RepoRoot: headTree.RepoRoot, Path: headTree.Path, SelfURLs: headTree.SelfURLs, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Input: cfg.Input, BaseInput: cfg.BaseInput, ExternalChanges: cfg.ExternalChanges}}, cfg.Config)
				base, head := renderedTrees2[0], renderedTrees2[1]
				assert.NoError(t, err)
				for side, rendered := range []Rendered{base, head} {
					if rendered.Result == nil || rendered.Orchestrator == nil {
						t.Fatalf("side %d missing render result", side)
					}
					for _, id := range []manifest.NamedResource{app, cluster} {
						info, _ := rendered.Store().GetStatus(id)
						if info.Outcome() != store.OutcomePassed {
							t.Errorf("side %d required %s was not reconciled: %+v", side, id, info)
						}
					}
					_, dispatched := rendered.Store().GetStatus(stable)
					assert.Equal(t, dispatched, false)
					byParent := outputByParent(rendered.Result.Manifests)
					assert.Equal(t, len(byParent[stable]), 0)
					assert.Diff(t, byParent[app], fullApp[side])
					type producerBatch struct {
						Parent    manifest.NamedResource
						Documents []map[string]any
					}
					var batches []producerBatch
					for _, batch := range rendered.Result.Manifests {
						var parent manifest.NamedResource
						if batch.Parent != nil {
							parent = batch.Parent.Named()
						}
						batches = append(batches, producerBatch{parent, batch.Documents})
					}
					body, err := json.Marshal(batches)
					assert.NoError(t, err)
					if run == 0 {
						first[side] = body
					} else {
						assert.Equal(t, string(body), string(first[side]))
					}
				}
			}
		})
	}
}

func TestRenderTrees_ResourceSetGeneratedData(t *testing.T) {
	t.Parallel()

	baseDir, headDir := t.TempDir(), t.TempDir()
	for i, dir := range []string{baseDir, headDir} {
		writeCluster(t, dir, "unused", "same")
		testutil.WriteFile(t, dir, "apps/app-a/cm.yaml", fmt.Sprintf(`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: z, namespace: flux-system}
spec:
  inputs: [{name: one}]
  resources:
    - apiVersion: v1
      kind: ConfigMap
      metadata: {name: cm-<< inputs.name >>, namespace: flux-system}
      data: {key: v%d}
    - apiVersion: v1
      kind: Secret
      metadata: {name: secret-<< inputs.name >>, namespace: default}
      stringData: {key: same}
---
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: a, namespace: flux-system}
spec:
  inputs: [{name: one}]
  resources:
    - apiVersion: v1
      kind: ConfigMap
      metadata: {name: cm-<< inputs.name >>, namespace: flux-system}
      data: {key: losing-explicit}
    - apiVersion: v1
      kind: ConfigMap
      metadata: {name: cm-<< inputs.name >>}
      data: {key: losing-omitted}
`, i+1))
	}
	renderedTrees3, err := RenderTrees(t.Context(), []Tree{{RepoRoot: baseDir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}}, {RepoRoot: headDir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}}, Config{Concurrency: 2, Network: new(source.Network)})
	base, head := renderedTrees3[0], renderedTrees3[1]
	assert.NoError(t, err)
	parent := testutil.KustomizationID("flux-system", "app-a")
	for i, side := range []Rendered{base, head} {
		if side.Result == nil || side.Err != nil {
			t.Fatalf("side %d incomplete: result=%v err=%v", i, side.Result, side.Err)
		}
		if len(outputByParent(side.Result.Manifests)[testutil.KustomizationID("flux-system", "app-b")]) != 0 || configMapNames(side.Result)["cfg-b"] {
			t.Fatalf("side %d rendered unchanged app-b: %v", i, side.Result.Manifests)
		}
		id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "cm-one"}
		cm, ok := side.Store().Get[*manifest.ConfigMap](id)
		if !ok {
			t.Fatal("missing shared ConfigMap")
		}
		assert.Diff(t, cm.Data, map[string]any{"key": fmt.Sprintf("v%d", i+1)})
		winner := manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "flux-system", Name: "z"}
		assert.Equal(t, side.Store().EmissionOwner(id), winner)
		assert.Equal(t, len(side.Result.Warnings), 1)
		if len(side.Result.Warnings) == 1 {
			assert.Equal(t, side.Result.Warnings[0].Message, "conflicting producers; using "+winner.String())
		}
		identities := make([]string, 0, len(outputByParent(side.Result.Manifests)[parent]))
		for _, doc := range outputByParent(side.Result.Manifests)[parent] {
			name, namespace := manifest.DocMetadata(doc)
			identities = append(identities, manifest.DocKind(doc)+"/"+namespace+"/"+name)
			switch manifest.DocKind(doc) {
			case manifest.KindConfigMap:
				assert.Diff(t, doc["data"], map[string]any{"key": fmt.Sprintf("v%d", i+1)})
			case manifest.KindSecret:
				assert.Diff(t, doc["stringData"], map[string]any{"key": "same"})
			}
		}
		slices.Sort(identities)
		assert.Diff(t, identities, []string{"ConfigMap/flux-system/cm-one", "ResourceSet/flux-system/a", "ResourceSet/flux-system/z", "Secret/default/secret-one"})
	}
	selected, selectedErr, _ := Select([]Rendered{base, head}, err,
		[]manifest.NamedResource{testutil.KustomizationID("", ""), testutil.HelmReleaseID("", "")}, nil)
	body, _, _, err := diff.Compare(
		diff.Snapshot{Subjects: selected[0].Subjects, Manifests: selected[0].Manifests, ProducerKeys: selected[0].ProducerKeys, Unavailable: selected[0].Unavailable},
		diff.Snapshot{Subjects: selected[1].Subjects, Manifests: selected[1].Manifests, ProducerKeys: selected[1].ProducerKeys, Unavailable: selected[1].Unavailable},
		selectedErr, diff.Options{Format: diff.FormatJSON})
	assert.NoError(t, err)
	var output struct {
		Changes []diff.Change `json:"changes"`
	}
	assert.NoError(t, json.Unmarshal(body, &output))
	generatedChanges := 0
	for _, c := range output.Changes {
		if c.Name == "cfg-b" || c.Name == "secret-one" {
			t.Fatalf("unchanged document in diff: %+v", c)
		}
		if c.Kind == manifest.KindConfigMap && c.Namespace == "flux-system" && c.Name == "cm-one" {
			generatedChanges++
			assert.Equal(t, c.Parent.Name, parent.Name)
			assert.Equal(t, c.Parent.Namespace, parent.Namespace)
			assert.Equal(t, c.Status, diff.StatusChanged)
			assert.Diff(t, c.Old["data"], map[string]any{"key": "v1"})
			assert.Diff(t, c.New["data"], map[string]any{"key": "v2"})
		}
	}
	assert.Equal(t, generatedChanges, 1)
}

// writeHomeOpsCluster lays out a cluster-template-style tree under dir
// with NO .git: the Flux entry point is kubernetes/flux/cluster/ks.yaml
// (cluster-apps → ./kubernetes/apps, repo-ROOT-relative), and the app's
// ConfigMap carries val. This is konflate's extracted-tree shape — the
// caller renders it via Tree{RepoRoot: dir, Path: dir/kubernetes/flux/cluster}.
func writeHomeOpsCluster(t *testing.T, dir, val string) {
	t.Helper()
	testutil.WriteFile(t, dir, "kubernetes/flux/cluster/ks.yaml",
		testutil.KustomizationYAML("cluster-apps", "flux-system", "  path: ./kubernetes/apps\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
	testutil.WriteFile(t, dir, "kubernetes/apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "kubernetes/apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo, namespace: default}\ndata: {k: "+val+"}\n")
}

// TestRenderTrees_RepoRootRelativeNoGit is the konflate clusterPath
// regression at the SDK level: two extracted trees with NO .git, the Flux
// entry point a subdir (kubernetes/flux/cluster), and repo-root-relative
// spec.path (./kubernetes/apps). With per-side Tree{RepoRoot, Path} the
// change in kubernetes/apps/cm.yaml — outside the scanned entry-point dir —
// is detected and rendered, and diff.Changes reports exactly it. (Before
// the explicit-root model, the .git-less subdir collapsed the anchor and
// the change detection both failed: empty diff / "no changes detected".)
func TestRenderTrees_RepoRootRelativeNoGit(t *testing.T) {
	baseDir := t.TempDir()
	headDir := t.TempDir()
	writeHomeOpsCluster(t, baseDir, "v1")
	writeHomeOpsCluster(t, headDir, "v2") // only the ConfigMap data differs

	entry := func(root string) string { return root + "/kubernetes/flux/cluster" }
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	renderedTrees4, err := RenderTrees(ctx, []Tree{{RepoRoot: baseDir, Path: entry(baseDir), ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}}, {RepoRoot: headDir, Path: entry(headDir), ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}}, Config{WipeSecrets: true, Concurrency: 4})
	base, head := renderedTrees4[0], renderedTrees4[1]
	if err != nil {
		t.Fatalf("RenderTrees: %v", err)
	}
	if base.Result == nil || head.Result == nil {
		t.Fatalf("both sides must render: base=%v head=%v", base.Result, head.Result)
	}
	selected, selectedErr, _ := Select([]Rendered{base, head}, err,
		[]manifest.NamedResource{testutil.KustomizationID("", ""), testutil.HelmReleaseID("", "")}, nil)
	body, _, _, err := diff.Compare(
		diff.Snapshot{Subjects: selected[0].Subjects, Manifests: selected[0].Manifests, ProducerKeys: selected[0].ProducerKeys, Unavailable: selected[0].Unavailable},
		diff.Snapshot{Subjects: selected[1].Subjects, Manifests: selected[1].Manifests, ProducerKeys: selected[1].ProducerKeys, Unavailable: selected[1].Unavailable},
		selectedErr, diff.Options{Format: diff.FormatJSON})
	if err != nil {
		t.Fatal(err)
	}
	var output struct {
		Changes []diff.Change `json:"changes"`
	}
	if err := json.Unmarshal(body, &output); err != nil {
		t.Fatal(err)
	}
	changes := output.Changes
	if len(changes) != 1 {
		t.Fatalf("want exactly 1 change (demo ConfigMap), got %d: %+v", len(changes), changes)
	}
	if c := changes[0]; c.Name != "demo" || c.Kind != "ConfigMap" || c.Status != diff.StatusChanged {
		t.Errorf("change = %+v; want ConfigMap demo changed", c)
	}
}

func TestRenderTrees_OwnCacheCloses(t *testing.T) {
	for _, headOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("head_only_%t", headOnly), func(t *testing.T) {
			baseDir, headDir, root := t.TempDir(), t.TempDir(), t.TempDir()
			writeCluster(t, baseDir, "v1", "same")
			writeCluster(t, headDir, "v2", "same")
			if _, err := RenderTrees(t.Context(), []Tree{{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}, RepoRoot: baseDir}, {ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, RepoRoot: headDir}},
				Config{CacheDir: root, Concurrency: 2, HeadOnly: headOnly}); err != nil {
				t.Fatal(err)
			}
			res, err := sourcecache.Sweep(t.Context(), root, sourcecache.SweepOpts{})
			if err != nil || len(res.Errors) != 0 {
				t.Fatalf("own cache retained lock: result=%+v err=%v", res, err)
			}
			if _, err := RenderTrees(t.Context(), []Tree{{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}}, {ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}},
				Config{CacheDir: root, Concurrency: 2, HeadOnly: headOnly}); !errors.Is(err, manifest.ErrInput) {
				t.Fatalf("missing input error = %v", err)
			}
			res, err = sourcecache.Sweep(t.Context(), root, sourcecache.SweepOpts{})
			if err != nil || len(res.Errors) != 0 {
				t.Fatalf("input failure retained owner: result=%+v err=%v", res, err)
			}
		})
	}
}

func TestRenderTrees_SameDirectoryKeepsFullRender(t *testing.T) {
	for _, workers := range []int{2, 4} {
		for _, tc := range []struct {
			name       string
			base, head bool
		}{
			{name: "directories"},
			{name: "supplied", base: true, head: true},
			{name: "supplied_base", base: true},
			{name: "supplied_head", head: true},
		} {
			for _, external := range []string{"automatic", "nonempty", "empty"} {
				t.Run(fmt.Sprintf("%s/%s/workers_%d", tc.name, external, workers), func(t *testing.T) {
					root := t.TempDir()
					writeCluster(t, root, "same", "same")
					alias := filepath.Join(t.TempDir(), "linked")
					assert.NoError(t, os.Symlink(root, alias))
					cfg := treeConfig{CacheDir: t.TempDir(), Concurrency: workers, Tree: Tree{}}
					if tc.base || tc.head {
						resolved, err := baseline.ResolvePaths(t.Context(), alias, root)
						assert.NoError(t, err)
						if tc.base {
							cfg.BaseInput = resolved.BaseInput
						}
						if tc.head {
							cfg.Input = resolved.Source
						}
					}
					switch external {
					case "nonempty":
						cfg.ExternalChanges = change.NewSet([]string{"apps/app-a/cm.yaml"})
					case "empty":
						cfg.ExternalChanges = change.NewSet(nil)
					}
					renderedTrees7, err := RenderTrees(t.Context(), []Tree{{RepoRoot: root, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}, BaseInput: cfg.Input, ExternalChanges: cfg.ExternalChanges, Input: cfg.BaseInput}, {RepoRoot: alias, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Input: cfg.Input, BaseInput: cfg.BaseInput, ExternalChanges: cfg.ExternalChanges}}, cfg.Config)
					base, head := renderedTrees7[0], renderedTrees7[1]
					assert.NoError(t, err)
					full := tc.base == tc.head && external == "automatic"
					for side, rendered := range []Rendered{base, head} {
						if rendered.Result == nil {
							t.Fatalf("side %d has no result", side)
						}
						assert.NoError(t, rendered.Err)
						assert.Equal(t, len(rendered.Result.Failed), 0)
						names := configMapNames(rendered.Result)
						assert.Equal(t, names["cfg-a"], full || external == "nonempty")
						assert.Equal(t, names["cfg-b"], full)
						warnings := rendered.Result.Warnings
						if external == "nonempty" {
							assert.Equal(t, len(warnings), 0)
							continue
						}
						if len(warnings) != 1 || warnings[0].Category != manifest.WarnPathConfig {
							t.Fatalf("side %d warnings = %v", side, warnings)
						}
						message := "no changes detected"
						if full {
							message = "resolve to the same root"
						}
						assert.Equal(t, strings.Contains(warnings[0].Message, message), true)
					}
				})
			}
		}
	}
	t.Run("scan_paths", func(t *testing.T) {
		for _, mode := range []string{"directories", "supplied", "head_only"} {
			t.Run(mode, func(t *testing.T) {
				root := t.TempDir()
				for _, name := range []string{"a", "b"} {
					testutil.WriteFile(t, root, "scans/"+name+"/ks.yaml", testutil.KustomizationYAML("app-"+name, "flux-system", "  path: ./apps/"+name+"\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
					testutil.WriteFile(t, root, "apps/"+name+"/kustomization.yaml", "resources: [cm.yaml]\n")
					testutil.WriteFile(t, root, "apps/"+name+"/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cfg-"+name+"}\n")
				}
				cfg := treeConfig{CacheDir: t.TempDir(), Concurrency: 2, HeadOnly: mode == "head_only", Tree: Tree{}}
				if mode != "directories" {
					input, _, err := baseline.AdmitDirectory(t.Context(), root, root)
					assert.NoError(t, err)
					cfg.BaseInput, cfg.Input = input, input
				}
				renderedTrees8, err := RenderTrees(t.Context(), []Tree{{RepoRoot: root, Path: filepath.Join(root, "scans", "a"), ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}, Input: cfg.BaseInput, BaseInput: cfg.Input, ExternalChanges: cfg.ExternalChanges}, {RepoRoot: root, Path: filepath.Join(root, "scans", "b"), ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Input: cfg.Input, BaseInput: cfg.BaseInput, ExternalChanges: cfg.ExternalChanges}}, cfg.Config)
				base, head := renderedTrees8[0], renderedTrees8[1]
				assert.NoError(t, err)
				for side, rendered := range []Rendered{base, head} {
					if cfg.HeadOnly && side == 0 {
						assert.Equal(t, rendered == (Rendered{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}}), true)
						continue
					}
					if rendered.Result == nil {
						t.Fatalf("side %d has no result", side)
					}
					assert.NoError(t, rendered.Err)
					name, other := "a", "b"
					if side == 1 {
						name, other = other, name
					}
					assert.Diff(t, configMapNames(rendered.Result), map[string]bool{"cfg-" + name: true})
					foreign := testutil.KustomizationID("flux-system", "app-"+other)
					if rendered.Store().GetObject(foreign) != nil {
						t.Fatalf("side %d admitted the other scan's Kustomization as an orphan", side)
					}
				}
			})
		}
	})
}

func TestRenderTrees_SuppliedViews(t *testing.T) {
	for _, headOnly := range []bool{false, true} {
		for _, scenario := range []string{"differing_options", "filtered_addition", "filtered_deletion", "explicit_changes", "explicit_empty"} {
			t.Run(fmt.Sprintf("%s/head_only_%t", scenario, headOnly), func(t *testing.T) {
				root := t.TempDir()
				writeCluster(t, root, "same", "same")
				for _, name := range []string{"ks-a.yaml", "ks-b.yaml"} {
					data, err := os.ReadFile(filepath.Join(root, name))
					assert.NoError(t, err)
					testutil.WriteFile(t, root, "flux/"+name, string(data))
				}
				input, _, err := baseline.AdmitDirectory(t.Context(), root, root)
				assert.NoError(t, err)
				cfg := treeConfig{CacheDir: t.TempDir(), Concurrency: 2, HeadOnly: headOnly, Tree: Tree{}}
				for side := range 2 {
					current := input.FS
					filtered := scenario == "filtered_addition" && side == 0 || scenario == "filtered_deletion" && side == 1
					if filtered {
						current, _, err = gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{
							Mode: gittree.Source, Current: current, Ignore: "flux/ks-a.yaml\napps/app-a/\n",
							Sparse: []string{"flux/ks-b.yaml", "apps/app-b"},
						})
						assert.NoError(t, err)
					}
					opts := gittree.ViewOptions{Mode: gittree.Direct, Current: current}
					if scenario == "differing_options" && side == 1 {
						opts.Ignore, opts.Sparse = "flux/ks-a.yaml\napps/app-a/\n", []string{"flux/ks-b.yaml", "apps/app-b"}
					}
					view, key, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, opts)
					assert.NoError(t, err)
					assert.Equal(t, key, "")
					if view.Location() == nil || view.Location().ContentRoot == "" || view.Location().ContentRoot != input.LocalPath {
						t.Fatalf("side %d location = %+v, want content root %s", side, view.Location(), input.LocalPath)
					}
					for _, name := range []string{"flux/ks-a.yaml", "apps/app-a/kustomization.yaml", "apps/app-a/cm.yaml", "flux/ks-b.yaml", "apps/app-b/cm.yaml"} {
						body, readErr := fs.ReadFile(view, name)
						if filtered && (name == "flux/ks-a.yaml" || strings.HasPrefix(name, "apps/app-a/")) {
							assert.Equal(t, errors.Is(readErr, fs.ErrNotExist), true)
						} else {
							assert.NoError(t, readErr)
							want, err := os.ReadFile(filepath.Join(root, name))
							assert.NoError(t, err)
							assert.Equal(t, string(body), string(want))
						}
					}
					artifact := *input
					artifact.FS, artifact.Key = view, key
					if side == 0 {
						cfg.BaseInput = &artifact
					} else {
						cfg.Input = &artifact
					}
				}
				switch scenario {
				case "explicit_changes":
					cfg.ExternalChanges = change.NewSet([]string{"apps/app-a/cm.yaml"})
				case "explicit_empty":
					cfg.ExternalChanges = change.NewSet(nil)
				}
				renderedTrees9, err := RenderTrees(t.Context(), []Tree{{RepoRoot: root, Path: filepath.Join(root, "flux"), ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}, Input: cfg.BaseInput, BaseInput: cfg.Input, ExternalChanges: cfg.ExternalChanges}, {RepoRoot: root, Path: filepath.Join(root, "flux"), ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Input: cfg.Input, BaseInput: cfg.BaseInput, ExternalChanges: cfg.ExternalChanges}}, cfg.Config)
				base, head := renderedTrees9[0], renderedTrees9[1]
				assert.NoError(t, err)
				for side, rendered := range []Rendered{base, head} {
					if headOnly && side == 0 {
						assert.Equal(t, rendered == (Rendered{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}}), true)
						continue
					}
					if rendered.Result == nil {
						t.Fatalf("side %d has no result", side)
					}
					assert.NoError(t, rendered.Err)
					assert.Equal(t, len(rendered.Result.Failed), 0)
					wantA := scenario == "explicit_changes" || scenario == "filtered_addition" && side == 1 || scenario == "filtered_deletion" && side == 0
					names := configMapNames(rendered.Result)
					assert.Equal(t, names["cfg-a"], wantA)
					assert.Equal(t, names["cfg-b"], false)
					filtered := scenario == "filtered_addition" && side == 0 || scenario == "filtered_deletion" && side == 1
					if filtered {
						id := testutil.KustomizationID("flux-system", "app-a")
						assert.Equal(t, rendered.Store().GetObject(id) == nil, true)
						for _, obj := range rendered.Store().ListObjects("", manifest.KindConfigMap) {
							if obj.Named().Name == "cfg-a" {
								t.Fatal("filtered side admitted cfg-a as an orphan seed")
							}
						}
					}
					warnings := rendered.Result.Warnings
					if scenario == "differing_options" || scenario == "explicit_empty" {
						if len(warnings) != 1 || warnings[0].Category != manifest.WarnPathConfig || !strings.Contains(warnings[0].Message, "no changes detected") {
							t.Fatalf("side %d warnings = %v", side, warnings)
						}
					} else {
						assert.Equal(t, len(warnings), 0)
					}
				}
			})
		}
	}
	t.Run("same_input_override", func(t *testing.T) {
		for _, empty := range []bool{false, true} {
			t.Run(fmt.Sprintf("empty_%t", empty), func(t *testing.T) {
				root := t.TempDir()
				writeCluster(t, root, "same", "same")
				input, _, err := baseline.AdmitDirectory(t.Context(), root, root)
				assert.NoError(t, err)
				paths := []string{"apps/app-a/cm.yaml"}
				if empty {
					paths = nil
				}
				cfg := treeConfig{CacheDir: t.TempDir(), Concurrency: 2, Input: input, BaseInput: input, ExternalChanges: change.NewSet(paths)}
				var previous [2]map[manifest.NamedResource][]map[string]any
				for run := range 2 {
					renderedTrees1, err := RenderTrees(t.Context(), []Tree{{RepoRoot: root, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}, ExternalChanges: cfg.ExternalChanges, Input: cfg.BaseInput, BaseInput: cfg.Input}, {RepoRoot: root, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Input: cfg.Input, BaseInput: cfg.BaseInput, ExternalChanges: cfg.ExternalChanges}}, cfg.Config)
					base, head := renderedTrees1[0], renderedTrees1[1]
					assert.NoError(t, err)
					for side, rendered := range []Rendered{base, head} {
						if run == 0 && side == 0 {
							directCfg := cfg
							directCfg.Path = root
							direct, err := newTestOrchestrator(t, directCfg.Tree, directCfg.Config)
							assert.NoError(t, err)
							result, err := direct.render(t.Context())
							assert.NoError(t, err)
							assert.Diff(t, outputByParent(result.Manifests), outputByParent(rendered.Result.Manifests))
						}
						want := map[string]bool{}
						if !empty {
							want["cfg-a"] = true
						}
						assert.Diff(t, configMapNames(rendered.Result), want)
						if run == 1 {
							assert.Diff(t, outputByParent(rendered.Result.Manifests), previous[side])
						}
						previous[side] = outputByParent(rendered.Result.Manifests)
					}
				}
			})
		}
	})

}

func TestRenderTrees_HeadOnly(t *testing.T) {
	for _, headFailure := range []bool{false, true} {
		t.Run(fmt.Sprintf("head_failure_%t", headFailure), func(t *testing.T) {
			baseInput := &store.SourceArtifact{LocalPath: filepath.Join(t.TempDir(), "base")}
			headInput := &store.SourceArtifact{LocalPath: filepath.Join(t.TempDir(), "head")}
			before := fstest.MapFS{
				"flux/hr.yaml":                       {Data: []byte("apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: demo, namespace: apps}\nspec:\n  interval: 10m\n  chart:\n    spec:\n      chart: charts/demo\n      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n")},
				"charts/demo/Chart.yaml":             {Data: []byte("apiVersion: v2\nname: demo\nversion: 1.0.0\n")},
				"charts/demo/templates/cm.yaml":      {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: surviving}\n")},
				"charts/demo/templates/deleted.yaml": {Data: []byte("{{ fail \"baseline reconciliation ran\" }}\n")},
			}
			after := make(fstest.MapFS, len(before)-1)
			for name, file := range before {
				if name != "charts/demo/templates/deleted.yaml" {
					after[name] = file
				}
			}
			if headFailure {
				after["charts/demo/templates/cm.yaml"] = &fstest.MapFile{Data: []byte("{{ fail \"head reconciliation failed\" }}\n")}
			}
			baseInput.FS, headInput.FS = before, after
			cfg := Config{Concurrency: 2, HeadOnly: true}
			baseTree := Tree{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}, Input: baseInput, RepoRoot: baseInput.LocalPath, Path: filepath.Join(baseInput.LocalPath, "flux")}
			headTree := Tree{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Input: headInput, RepoRoot: headInput.LocalPath, Path: filepath.Join(headInput.LocalPath, "flux")}
			runs, err := RenderTrees(t.Context(), []Tree{baseTree, headTree}, cfg)
			if len(runs) != 2 {
				t.Fatalf("missing results: %v", err)
			}
			base, head := runs[0], runs[1]
			if base != (Rendered{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}}) || head.Orchestrator == nil || head.Result == nil {
				t.Fatalf("head-only completion = base=%+v head=%+v err=%v", base, head, err)
			}
			if headFailure {
				if !errors.Is(err, manifest.ErrFlux) || !strings.Contains(err.Error(), "head reconciliation failed") || len(head.Result.Failed) != 1 {
					t.Fatalf("head failure lost partial result: %+v %v", head.Result, err)
				}
			} else {
				assert.NoError(t, err)
				assert.Diff(t, configMapNames(head.Result), map[string]bool{"surviving": true})
				cfg.HeadOnly = false
				_, err = RenderTrees(t.Context(), []Tree{baseTree, headTree}, cfg)
				if err == nil || !strings.Contains(err.Error(), "baseline reconciliation ran") {
					t.Fatalf("baseline failure control did not reconcile: %v", err)
				}
			}
		})
	}
}

type joinedTreeFS struct {
	fs.FS
	payloadPath string
	armed       atomic.Bool
	once        sync.Once
	acquire     func() error
	err         error
}

func (f *joinedTreeFS) Open(name string) (fs.File, error) {
	if f.payloadPath != "" && (name != f.payloadPath || !f.armed.Load()) {
		return f.FS.Open(name)
	}
	f.once.Do(func() { f.err = f.acquire() })
	return nil, f.err
}

func TestRenderTrees_CancellationJoinsBeforeCacheClose(t *testing.T) {
	t.Run("discovery_failure", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			release := make(chan struct{})
			failure := errors.New("baseline discovery failed")
			baseInput := &store.SourceArtifact{LocalPath: t.TempDir(), FS: &joinedTreeFS{acquire: func() error {
				<-release
				return failure
			}}, Warnings: []manifest.Warning{{Category: manifest.WarnBaselineStatusUnavailable, Message: "base warning"}}}
			headInput := &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}, Warnings: []manifest.Warning{{Category: manifest.WarnBaselineStatusUnavailable, Message: "head warning"}}}
			var base, head Rendered
			done := make(chan error, 1)
			go func() {
				var err error
				runs, runErr := RenderTrees(t.Context(), []Tree{
					{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}, Input: baseInput, ExternalChanges: change.NewSet(nil)},
					{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Input: headInput, ExternalChanges: change.NewSet(nil)},
				}, Config{Concurrency: 2, HeadOnly: true})
				err = runErr
				if len(runs) != 2 {
					t.Errorf("missing diagnostic sides: %v", err)
				} else {
					base, head = runs[0], runs[1]
				}
				done <- err
			}()
			synctest.Wait()
			close(release)
			err := <-done
			if !errors.Is(err, failure) || strings.Count(err.Error(), failure.Error()) != 1 {
				t.Fatalf("discovery cause lost or duplicated: %v", err)
			}
			for _, side := range []Rendered{base, head} {
				if side.Orchestrator != nil || side.Result == nil || len(side.Result.Manifests)+len(side.Result.Failed) != 0 {
					t.Fatalf("failed preparation exposed content: %+v", side)
				}
			}
		})
	})
	for _, workers := range []int{2, 16} {
		for _, headOnly := range []bool{false, true} {
			for _, tc := range []struct {
				backing  string
				injected bool
			}{{"memory", true}, {"disk", true}, {"disk", false}} {
				t.Run(fmt.Sprintf("workers_%d/head_only_%t/%s/injected_%t", workers, headOnly, tc.backing, tc.injected), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						root := ""
						if tc.backing == "disk" {
							root = t.TempDir()
						}
						owner, err := sourcecache.New(t.Context(), root)
						assert.NoError(t, err)
						externalShare := true
						t.Cleanup(func() {
							if externalShare {
								assert.NoError(t, owner.Close())
							}
						})
						ctx, cancel := context.WithCancel(t.Context())
						defer cancel()
						release := make(chan struct{})
						defer close(release)
						active := 2
						if headOnly {
							active = 1
						}
						wantOrigErrors := 1
						if headOnly {
							wantOrigErrors = 0
						}
						entered, canceled, completed := make(chan struct{}, 2), make(chan struct{}, 2), make(chan struct{}, 2)
						inputs := [2]*store.SourceArtifact{}
						for i := range inputs {
							input := &joinedTreeFS{payloadPath: "app/payload.txt", FS: fstest.MapFS{
								"kustomization.yaml":     {Data: []byte("resources: [flux.yaml]\n")},
								"flux.yaml":              {Data: []byte(testutil.KustomizationYAML("owner", "apps", "  path: ./app\n  sourceRef: {kind: GitRepository, name: cluster}\n"))},
								"app/kustomization.yaml": {Data: []byte("configMapGenerator: [{name: held, files: [payload.txt]}]\n")},
								"app/payload.txt":        {Data: fmt.Appendf(nil, "fixture-%d", i)},
							}}
							input.acquire = func() error {
								_, _, _, err := owner.Blob(ctx, sourcecache.BlobKey{Content: fmt.Sprintf("held-input-%d", i)}, sourcecache.Policy{},
									func(_ context.Context, _ sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
										entered <- struct{}{}
										<-ctx.Done()
										canceled <- struct{}{}
										<-release
										completed <- struct{}{}
										return sourcecache.ProducerResult{}, ctx.Err()
									})
								return err
							}
							inputs[i] = &store.SourceArtifact{LocalPath: t.TempDir(), FS: input}
						}
						cfg := treeConfig{CacheDir: root, Concurrency: workers, HeadOnly: headOnly,
							StatusListener: func(id manifest.TreeID, resource manifest.DiagnosticIdentity, status store.StatusInfo) {
								if resource.ID == testutil.KustomizationID("apps", "owner") && status.String() == "resolving dependencies" {
									input := inputs[id.Side].FS.(*joinedTreeFS)
									input.once.Do(func() { input.err = input.acquire() })
								}
							}, BaseInput: inputs[0], Input: inputs[1], ExternalChanges: change.NewSet([]string{"app/payload.txt"})}
						if tc.injected {
							cfg.Cache = owner
						}
						done := make(chan error, 1)
						go func() {
							_, err := RenderTrees(ctx, []Tree{{RepoRoot: inputs[0].LocalPath, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}, Input: cfg.BaseInput, BaseInput: cfg.Input, ExternalChanges: cfg.ExternalChanges}, {RepoRoot: inputs[1].LocalPath, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Input: cfg.Input, BaseInput: cfg.BaseInput, ExternalChanges: cfg.ExternalChanges}}, cfg.Config)
							done <- err
						}()
						for range active {
							<-entered
						}
						if !tc.injected {
							assert.NoError(t, owner.Close())
							externalShare = false
						}
						cancel()
						for range active {
							<-canceled
						}
						synctest.Wait()
						select {
						case err := <-done:
							t.Fatalf("render returned before acquisition cleanup: %v", err)
						default:
						}
						if root != "" {
							res, err := sourcecache.Sweep(t.Context(), root, sourcecache.SweepOpts{})
							if err == nil && len(res.Errors) == 0 {
								t.Fatal("backing released before held acquisitions joined")
							}
						}
						key := sourcecache.BlobKey{Content: "backing-survives"}
						_, _, _, err = owner.Blob(t.Context(), key, sourcecache.Policy{MaxAge: time.Hour}, func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
							_, err := io.WriteString(w, "live")
							return sourcecache.ProducerResult{}, err
						})
						assert.NoError(t, err)

						for range active {
							release <- struct{}{}
						}
						if err := <-done; !errors.Is(err, context.Canceled) || strings.Count(err.Error(), "orig snapshot:") != wantOrigErrors || strings.Count(err.Error(), "current snapshot:") != 1 {
							t.Fatalf("joined cancellation lost a side: %v", err)
						}
						for range active {
							<-completed
						}
						if tc.injected {
							_, _, _, err = owner.Blob(t.Context(), key, sourcecache.Policy{MaxAge: time.Hour}, nil)
							assert.NoError(t, err)

							assert.NoError(t, owner.Close())
							externalShare = false
						}
						var reacquired atomic.Int64
						_, _, _, err = owner.Blob(ctx, sourcecache.BlobKey{Content: "post-close-canceled"}, sourcecache.Policy{}, func(context.Context, sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
							reacquired.Add(1)
							return sourcecache.ProducerResult{}, nil
						})
						if !errors.Is(err, context.Canceled) || reacquired.Load() != 0 {
							t.Fatalf("canceled run acquired after owner close: calls=%d error=%v", reacquired.Load(), err)
						}
						if root != "" {
							res, err := sourcecache.Sweep(t.Context(), root, sourcecache.SweepOpts{})
							if err != nil || len(res.Errors) != 0 {
								t.Fatalf("joined render retained owner: %+v %v", res, err)
							}
						}
					})
				})
			}
		}
	}
	t.Run("http-bootstrap", func(t *testing.T) {
		for _, tc := range []struct{ headOnly, injected bool }{{false, false}, {false, true}, {true, false}, {true, true}} {
			t.Run(fmt.Sprintf("head_only_%t/injected_%t", tc.headOnly, tc.injected), func(t *testing.T) {
				active, wantOrigErrors := 2, 1
				if tc.headOnly {
					active, wantOrigErrors = 1, 0
				}
				entered, completed := make(chan struct{}, 2), make(chan struct{}, 2)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusOK)
					w.(http.Flusher).Flush()
					entered <- struct{}{}
					<-r.Context().Done()
					completed <- struct{}{}
				}))
				defer server.Close()
				var trees [2]Tree
				for i := range trees {
					root := t.TempDir()
					testutil.WriteFile(t, root, "flux.yaml", testutil.KustomizationYAML("owner", "apps", "  path: ./apps\n  interval: 5m\n"))
					testutil.WriteFile(t, root, "apps/kustomization.yaml", fmt.Sprintf("configMapGenerator: [{name: remote, envs: [%s/%d]}]\n", server.URL, i))
					trees[i] = Tree{RepoRoot: root}
				}
				cfg := treeConfig{CacheDir: t.TempDir(), Concurrency: 2, HeadOnly: tc.headOnly, Tree: Tree{}}
				if tc.injected {
					var err error
					cfg.Cache, err = sourcecache.New(t.Context(), cfg.CacheDir)
					assert.NoError(t, err)
					t.Cleanup(func() { assert.NoError(t, cfg.Cache.Close()) })
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				go func() {
					_, err := RenderTrees(ctx, []Tree{{RepoRoot: trees[0].RepoRoot, Path: trees[0].Path, SelfURLs: trees[0].SelfURLs, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}, Input: cfg.BaseInput, BaseInput: cfg.Input, ExternalChanges: cfg.ExternalChanges}, {RepoRoot: trees[1].RepoRoot, Path: trees[1].Path, SelfURLs: trees[1].SelfURLs, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Input: cfg.Input, BaseInput: cfg.BaseInput, ExternalChanges: cfg.ExternalChanges}}, cfg.Config)
					done <- err
				}()
				for range active {
					<-entered
				}
				cancel()
				if err := <-done; !errors.Is(err, context.Canceled) || errors.Is(err, fs.ErrClosed) || strings.Count(err.Error(), "orig snapshot:") != wantOrigErrors || strings.Count(err.Error(), "current snapshot:") != 1 {
					t.Fatalf("HTTP bootstrap teardown lost a side or closed backing early: %v", err)
				}
				for range active {
					<-completed
				}
				if tc.injected {
					_, _, _, err := cfg.Cache.Blob(t.Context(), sourcecache.BlobKey{Content: "after-http-bootstrap"}, sourcecache.Policy{}, func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
						_, err := io.WriteString(w, "live")
						return sourcecache.ProducerResult{}, err
					})
					assert.NoError(t, err)

					assert.NoError(t, cfg.Cache.Close())
				}
				res, err := sourcecache.Sweep(t.Context(), cfg.CacheDir, sourcecache.SweepOpts{})
				if err != nil || len(res.Errors) != 0 {
					t.Fatalf("HTTP bootstrap retained backing after both sides joined: %+v %v", res, err)
				}
			})
		}
	})
}

type countedNodeController struct {
	reconcileController
	calls, admitted map[manifest.NamedResource]*atomic.Int64
}

type reconcileController interface {
	Owns(manifest.NamedResource) bool
	ReconcileNode(context.Context, manifest.NamedResource, manifest.DrainLevel) *base.Execution
	PreviousRender(context.Context, manifest.BaseManifest, *store.ReadSet) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool)
}

func (c countedNodeController) ReconcileNode(ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel) *base.Execution {
	c.calls[id].Add(1)
	execution := c.reconcileController.ReconcileNode(ctx, id, level)
	if execution != nil && execution.Reads.Admitted() {
		c.admitted[id].Add(1)
	}
	return execution
}

type countedChartFetcher struct {
	chart string
	calls atomic.Int64
}

func (f *countedChartFetcher) Fetch(context.Context, manifest.BaseManifest, *store.ReadSet, sourcecache.Policy) (*store.SourceArtifact, error) {
	f.calls.Add(1)
	const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	return &store.SourceArtifact{Kind: manifest.KindOCIRepository, LocalPath: f.chart, FS: os.DirFS(f.chart),
		Digest: digest, Revision: "1.0.0@" + digest, Key: digest}, nil
}

func TestRender_DeclarationOnlyExcludesUnrelatedWork(t *testing.T) {
	roots := [2]string{t.TempDir(), t.TempDir()}
	for side, root := range roots {
		writeCluster(t, root, "same", "same")
		testutil.WriteFile(t, root, "ks-a.yaml", testutil.KustomizationYAML("app-a", "flux-system", "  interval: "+[]string{"1h", "2h"}[side]+"\n  path: ./apps/app-a\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: [ks-a.yaml, ks-b.yaml, chart.yaml, release.yaml]\n")
		testutil.WriteFile(t, root, "chart.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: unrelated-chart, namespace: flux-system}\nspec: {url: 'oci://fixture.invalid/chart', ref: {tag: 1.0.0}}\n")
		testutil.WriteFile(t, root, "release.yaml", "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: unrelated-release, namespace: flux-system}\nspec: {interval: 1h, chartRef: {kind: OCIRepository, name: unrelated-chart}}\n")
	}
	chart := t.TempDir()
	testutil.WriteFile(t, chart, "fixture/Chart.yaml", "apiVersion: v2\nname: fixture\nversion: 1.0.0\n")
	testutil.WriteFile(t, chart, "fixture/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: from-chart}\n")
	inputs := [2]*store.SourceArtifact{testDirectoryInput(t, roots[0]), testDirectoryInput(t, roots[1])}
	changes, err := change.Detect(t.Context(), inputs[0], inputs[1])
	assert.NoError(t, err)
	assert.Diff(t, changes.Paths(), []string{"ks-a.yaml"})
	for side, root := range roots {
		for _, changedOnly := range []bool{false, true} {
			t.Run(fmt.Sprintf("side_%d/changed_%t", side, changedOnly), func(t *testing.T) {
				tree := Tree{Path: root, RepoRoot: root, Input: inputs[side], ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)}}
				if changedOnly {
					tree.BaseInput, tree.ExternalChanges = inputs[1-side], changes
				}
				o, err := newTestOrchestrator(t, tree, Config{Concurrency: 2})
				assert.NoError(t, err)
				fetcher := &countedChartFetcher{chart: chart}
				o.src.Fetchers[manifest.KindOCIRepository] = fetcher
				assert.NoError(t, o.bootstrap(t.Context()))
				ids := []manifest.NamedResource{o.entryBinding.Producer, testutil.KustomizationID("flux-system", "app-a"), testutil.KustomizationID("flux-system", "app-b"), {Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Namespace: "flux-system", Name: "unrelated-release"}}
				calls, admitted := map[manifest.NamedResource]*atomic.Int64{}, map[manifest.NamedResource]*atomic.Int64{}
				for _, id := range ids {
					calls[id], admitted[id] = new(atomic.Int64), new(atomic.Int64)
				}
				o.controllers.Kustomization = countedNodeController{o.ksc, calls, admitted}
				o.controllers.HelmRelease = countedNodeController{o.hrc, calls, admitted}
				runErr := o.run(t.Context())
				assert.NoError(t, runErr)
				for _, id := range ids {
					t.Logf("work %s calls=%d admitted=%d", id, calls[id].Load(), admitted[id].Load())
				}
				t.Logf("chart acquisitions=%d", fetcher.calls.Load())
				if changedOnly {
					assert.Equal(t, admitted[ids[2]].Load(), int64(0))
					assert.Equal(t, admitted[ids[3]].Load(), int64(0))
					assert.Equal(t, fetcher.calls.Load(), int64(0))
					assert.Equal(t, o.store.GetArtifact(ids[2], store.ArtifactContent), store.Artifact(nil))
					assert.Equal(t, o.store.GetArtifact(ids[3], store.ArtifactContent), store.Artifact(nil))
				} else {
					assert.Equal(t, admitted[ids[2]].Load(), int64(1))
					if admitted[ids[3]].Load() < 1 {
						t.Fatal("full-render control did not reconcile the release")
					}
					assert.Equal(t, fetcher.calls.Load(), int64(1))
				}
				assert.Equal(t, admitted[ids[0]].Load(), int64(1))
				assert.Equal(t, admitted[ids[1]].Load(), int64(1))
				result, err := o.collectResult(runErr)
				assert.NoError(t, err)
				var interval any
				for _, batch := range result.Manifests {
					for _, doc := range batch.Documents {
						metadata, _ := doc["metadata"].(map[string]any)
						if doc["kind"] == manifest.KindKustomization && metadata["name"] == "app-a" {
							interval = doc["spec"].(map[string]any)["interval"]
						}
					}
				}
				assert.Equal(t, interval, any([]string{"1h", "2h"}[side]))
			})
		}
	}
}

func TestProjectEntryReaders_ZeroBinding(t *testing.T) {
	t.Parallel()
	entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, ".", manifest.NamedResource{}).Named()
	index := &manifest.DiscoveryIndex{Readers: map[string][]manifest.FileReader{"root.yaml": {{Reader: entry, Source: manifest.BootstrapSourceID}}}}
	for _, pair := range [][2]discovery.Binding{{{}, {Producer: entry}}, {{Producer: entry}, {}}, {{}, {}}} {
		assert.Equal(t, projectEntryReaders(index, pair[0], pair[1]), index)
	}
}

func TestRenderTrees_EntryQualificationTransitions(t *testing.T) {
	for _, pair := range [][2]string{{"discovery", "discovery"}, {"discovery", "package"}, {"package", "discovery"}, {"authored", "discovery"}, {"discovery", "authored"}} {
		t.Run(pair[0]+" to "+pair[1], func(t *testing.T) {
			var trees []Tree
			for side, qualification := range pair {
				root := t.TempDir()
				testutil.WriteFile(t, root, "Taskfile.yaml", "version: '3'\ntasks: {build: {cmds: [true]}}\n")
				for _, name := range []string{"a", "b"} {
					testutil.WriteFile(t, root, "clusters/"+name+"/ks.yaml", testutil.KustomizationYAML(name, "apps", "  path: ./workload/"+name+"\n"))
					testutil.WriteFile(t, root, "workload/"+name+"/kustomization.yaml", "resources: [cm.yaml]\n")
					testutil.WriteFile(t, root, "workload/"+name+"/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+"}\ndata: {side: '"+fmt.Sprint(side)+"'}\n")
				}
				if qualification == "package" {
					testutil.WriteFile(t, root, "kustomization.yaml", "resources: [clusters/a/ks.yaml, clusters/b/ks.yaml]\n")
				}
				if qualification == "authored" {
					assert.NoError(t, os.Remove(filepath.Join(root, "Taskfile.yaml")))
					testutil.WriteFile(t, root, "owner.yaml", testutil.KustomizationYAML("root-owner", "apps", "  path: ./\n"))
				}
				trees = append(trees, Tree{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)}, Path: root, RepoRoot: root})
			}
			var previous []byte
			for range 2 {
				runs, err := RenderTrees(t.Context(), trees, Config{Concurrency: 2})
				assert.NoError(t, err)
				assert.Equal(t, len(runs), 2)
				for side, run := range runs {
					qualified := pair[side] != "discovery"
					assert.Equal(t, run.entryBinding != (discovery.Binding{}), qualified)
					assert.Diff(t, configMapNames(run.Result), map[string]bool{"a": true, "b": true})
				}
				selected, err, _ := Select(runs, nil, []manifest.NamedResource{testutil.KustomizationID("", "")}, nil)
				assert.NoError(t, err)
				assert.Equal(t, len(selected), 2)
				for side, selection := range selected {
					wantKeys := 0
					if pair[side] != "discovery" {
						wantKeys = 1
					}
					assert.Equal(t, len(selection.ProducerKeys), wantKeys)
					assert.Equal(t, len(selection.Unavailable), 0)
				}
				body, _, notes, err := diff.Compare(
					diff.Snapshot{Subjects: selected[0].Subjects, Manifests: selected[0].Manifests, ProducerKeys: selected[0].ProducerKeys, Unavailable: selected[0].Unavailable},
					diff.Snapshot{Subjects: selected[1].Subjects, Manifests: selected[1].Manifests, ProducerKeys: selected[1].ProducerKeys, Unavailable: selected[1].Unavailable},
					nil, diff.Options{Format: diff.FormatJSON})
				assert.NoError(t, err)
				assert.Equal(t, len(notes), 0)
				if previous != nil {
					assert.Equal(t, string(body), string(previous))
				}
				previous = body
			}
		})
	}
}
