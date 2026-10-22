package orchestrator_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

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
func configMapNames(res *orchestrator.Result) map[string]bool {
	out := map[string]bool{}
	for _, docs := range res.Manifests {
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
	for _, tc := range []struct{ name, target, namespace string }{
		{"bare", "", ""},
		{"target", "targetNamespace: flux-system, ", "flux-system"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			baseDir := t.TempDir()
			headDir := t.TempDir()
			writeCluster(t, baseDir, "v1", "same")
			writeCluster(t, headDir, "v2", "same")
			for i, dir := range []string{baseDir, headDir} {
				testutil.WriteFile(t, dir, "ks-a.yaml", testutil.KustomizationYAML("app-a", "flux-system", "  {path: ./apps/app-a, "+tc.target+"sourceRef: {kind: GitRepository, name: flux-system}}\n"))
				if i == 1 && tc.target != "" {
					testutil.WriteFile(t, dir, "apps/app-a/kustomization.yaml", "namespace: dirty\nresources: [cm.yaml]\n")
				} else if i == 1 {
					testutil.WriteFile(t, dir, "apps/app-a/kustomization.yaml", "resources: [cm.yaml]\ncommonAnnotations: {revision: head}\n")
				}
				testutil.WriteFile(t, dir, "apps/app-a/cm.yaml", testutil.KustomizationYAML("consumer-a", "", "  path: ./consumer\n  sourceRef: {kind: GitRepository, name: flux-system}\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: render-values}]\n"))
				testutil.WriteFile(t, dir, "inputs/values.yaml", fmt.Sprintf("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: render-values, namespace: flux-system}\ndata: {VALUE: v%d}\n", i+1))
				testutil.WriteFile(t, dir, "consumer/kustomization.yaml", "resources:\n- cm.yaml\n")
				testutil.WriteFile(t, dir, "consumer/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cfg-a}\ndata: {k: '${VALUE}'}\n")
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			base, head, err := orchestrator.RenderTrees(ctx,
				orchestrator.Tree{RepoRoot: baseDir},
				orchestrator.Tree{RepoRoot: headDir},
				orchestrator.Config{
					WipeSecrets: true,
					Network:     new(source.Network),
					Concurrency: 2,
				})
			if (err != nil) != (tc.namespace == "") {
				t.Fatalf("RenderTrees: %v, want blocked=%t", err, tc.namespace == "")
			}
			if base.Result == nil || head.Result == nil {
				t.Fatalf("both sides must render: base=%v head=%v", base.Result, head.Result)
			}

			for _, side := range []orchestrator.Rendered{base, head} {
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
			for i, side := range []orchestrator.Rendered{base, head} {
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

			for i, rendered := range []orchestrator.Rendered{base, head} {
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
				docs := rendered.Result.Manifests[consumer]
				if len(docs) != 1 {
					t.Fatalf("side %d consumer documents = %d", i, len(docs))
				}
				assert.Equal(t, docs[0]["data"].(map[string]any)["k"], any(fmt.Sprintf("v%d", i+1)))
				_, ns = manifest.DocMetadata(docs[0])
				assert.Equal(t, ns, "")
			}

			selected, selectedErr, _ := orchestrator.Select([]orchestrator.Rendered{base, head}, err,
				[]manifest.NamedResource{testutil.KustomizationID("", ""), testutil.HelmReleaseID("", "")}, nil)
			body, err := diff.Compare(
				diff.Snapshot{Objects: selected[0].Objects, Manifests: selected[0].Manifests, Unavailable: selected[0].Unavailable},
				diff.Snapshot{Objects: selected[1].Objects, Manifests: selected[1].Manifests, Unavailable: selected[1].Unavailable},
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
				t.Fatalf("want exactly 1 change (cfg-a), got %d: %+v", len(changes), changes)
			}
			c := changes[0]
			if c.Name != "cfg-a" || c.Kind != "ConfigMap" || c.Status != diff.StatusChanged {
				t.Errorf("change = %+v; want ConfigMap cfg-a changed", c)
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
	base, head, err := orchestrator.RenderTrees(t.Context(),
		orchestrator.Tree{RepoRoot: baseDir},
		orchestrator.Tree{RepoRoot: headDir},
		orchestrator.Config{Concurrency: 2, Network: new(source.Network)})
	assert.NoError(t, err)
	parent := testutil.KustomizationID("flux-system", "app-a")
	for i, side := range []orchestrator.Rendered{base, head} {
		if side.Result == nil || side.Err != nil {
			t.Fatalf("side %d incomplete: result=%v err=%v", i, side.Result, side.Err)
		}
		if len(side.Result.Manifests[testutil.KustomizationID("flux-system", "app-b")]) != 0 || configMapNames(side.Result)["cfg-b"] {
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
		identities := make([]string, 0, len(side.Result.Manifests[parent]))
		for _, doc := range side.Result.Manifests[parent] {
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
	selected, selectedErr, _ := orchestrator.Select([]orchestrator.Rendered{base, head}, err,
		[]manifest.NamedResource{testutil.KustomizationID("", ""), testutil.HelmReleaseID("", "")}, nil)
	body, err := diff.Compare(
		diff.Snapshot{Objects: selected[0].Objects, Manifests: selected[0].Manifests, Unavailable: selected[0].Unavailable},
		diff.Snapshot{Objects: selected[1].Objects, Manifests: selected[1].Manifests, Unavailable: selected[1].Unavailable},
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
	base, head, err := orchestrator.RenderTrees(ctx,
		orchestrator.Tree{RepoRoot: baseDir, Path: entry(baseDir)},
		orchestrator.Tree{RepoRoot: headDir, Path: entry(headDir)},
		orchestrator.Config{WipeSecrets: true, Concurrency: 4})
	if err != nil {
		t.Fatalf("RenderTrees: %v", err)
	}
	if base.Result == nil || head.Result == nil {
		t.Fatalf("both sides must render: base=%v head=%v", base.Result, head.Result)
	}
	selected, selectedErr, _ := orchestrator.Select([]orchestrator.Rendered{base, head}, err,
		[]manifest.NamedResource{testutil.KustomizationID("", ""), testutil.HelmReleaseID("", "")}, nil)
	body, err := diff.Compare(
		diff.Snapshot{Objects: selected[0].Objects, Manifests: selected[0].Manifests, Unavailable: selected[0].Unavailable},
		diff.Snapshot{Objects: selected[1].Objects, Manifests: selected[1].Manifests, Unavailable: selected[1].Unavailable},
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
	baseDir, headDir, root := t.TempDir(), t.TempDir(), t.TempDir()
	writeCluster(t, baseDir, "v1", "same")
	writeCluster(t, headDir, "v2", "same")
	if _, _, err := orchestrator.RenderTrees(t.Context(), orchestrator.Tree{RepoRoot: baseDir}, orchestrator.Tree{RepoRoot: headDir},
		orchestrator.Config{CacheDir: root, Concurrency: 2}); err != nil {
		t.Fatal(err)
	}
	res, err := sourcecache.Sweep(t.Context(), root, sourcecache.SweepOpts{})
	if err != nil || len(res.Errors) != 0 {
		t.Fatalf("own cache retained lock: result=%+v err=%v", res, err)
	}
	if _, _, err := orchestrator.RenderTrees(t.Context(), orchestrator.Tree{}, orchestrator.Tree{},
		orchestrator.Config{CacheDir: root, Concurrency: 2}); !errors.Is(err, manifest.ErrInput) {
		t.Fatalf("missing input error = %v", err)
	}
	res, err = sourcecache.Sweep(t.Context(), root, sourcecache.SweepOpts{})
	if err != nil || len(res.Errors) != 0 {
		t.Fatalf("input failure retained owner: result=%+v err=%v", res, err)
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
					cfg := orchestrator.Config{CacheDir: t.TempDir(), Concurrency: workers}
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
					base, head, err := orchestrator.RenderTrees(t.Context(),
						orchestrator.Tree{RepoRoot: root}, orchestrator.Tree{RepoRoot: alias}, cfg)
					assert.NoError(t, err)
					full := tc.base == tc.head && external == "automatic"
					for side, rendered := range []orchestrator.Rendered{base, head} {
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
		for _, mode := range []string{"directories", "supplied"} {
			t.Run(mode, func(t *testing.T) {
				root := t.TempDir()
				for _, name := range []string{"a", "b"} {
					testutil.WriteFile(t, root, "scans/"+name+"/ks.yaml", testutil.KustomizationYAML("app-"+name, "flux-system", "  path: ./apps/"+name+"\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
					testutil.WriteFile(t, root, "apps/"+name+"/kustomization.yaml", "resources: [cm.yaml]\n")
					testutil.WriteFile(t, root, "apps/"+name+"/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cfg-"+name+"}\n")
				}
				cfg := orchestrator.Config{CacheDir: t.TempDir(), Concurrency: 2}
				if mode == "supplied" {
					input, _, err := baseline.AdmitDirectory(t.Context(), root, root)
					assert.NoError(t, err)
					cfg.BaseInput, cfg.Input = input, input
				}
				base, head, err := orchestrator.RenderTrees(t.Context(),
					orchestrator.Tree{RepoRoot: root, Path: filepath.Join(root, "scans", "a")},
					orchestrator.Tree{RepoRoot: root, Path: filepath.Join(root, "scans", "b")}, cfg)
				assert.NoError(t, err)
				for side, rendered := range []orchestrator.Rendered{base, head} {
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
	for _, workers := range []int{2, 4} {
		for _, scenario := range []string{"differing_options", "filtered_addition", "filtered_deletion", "explicit_changes", "explicit_empty"} {
			t.Run(fmt.Sprintf("%s/workers_%d", scenario, workers), func(t *testing.T) {
				root := t.TempDir()
				writeCluster(t, root, "same", "same")
				input, _, err := baseline.AdmitDirectory(t.Context(), root, root)
				assert.NoError(t, err)
				cfg := orchestrator.Config{CacheDir: t.TempDir(), Concurrency: workers}
				for side := range 2 {
					current := input.FS
					filtered := scenario == "filtered_addition" && side == 0 || scenario == "filtered_deletion" && side == 1
					if filtered {
						current, _, err = gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{
							Mode: gittree.Source, Current: current, Ignore: "ks-a.yaml\napps/app-a/\n",
							Sparse: []string{"ks-b.yaml", "apps/app-b"},
						})
						assert.NoError(t, err)
					}
					opts := gittree.ViewOptions{Mode: gittree.Direct, Current: current}
					if scenario == "differing_options" && side == 1 {
						opts.Ignore, opts.Sparse = "ks-a.yaml\napps/app-a/\n", []string{"ks-b.yaml", "apps/app-b"}
					}
					view, key, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, opts)
					assert.NoError(t, err)
					assert.Equal(t, key, "")
					if view.Location() == nil || view.Location().ContentRoot == "" || view.Location().ContentRoot != input.LocalPath {
						t.Fatalf("side %d location = %+v, want content root %s", side, view.Location(), input.LocalPath)
					}
					for _, name := range []string{"ks-a.yaml", "apps/app-a/kustomization.yaml", "apps/app-a/cm.yaml", "ks-b.yaml", "apps/app-b/cm.yaml"} {
						body, readErr := fs.ReadFile(view, name)
						if filtered && (name == "ks-a.yaml" || strings.HasPrefix(name, "apps/app-a/")) {
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
				base, head, err := orchestrator.RenderTrees(t.Context(),
					orchestrator.Tree{RepoRoot: root}, orchestrator.Tree{RepoRoot: root}, cfg)
				assert.NoError(t, err)
				for side, rendered := range []orchestrator.Rendered{base, head} {
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
}

type joinedTreeFS struct {
	once    sync.Once
	acquire func() error
	err     error
}

func (f *joinedTreeFS) Open(_ string) (fs.File, error) {
	f.once.Do(func() { f.err = f.acquire() })
	return nil, f.err
}

func TestRenderTrees_CancellationJoinsBeforeCacheClose(t *testing.T) {
	for _, workers := range []int{2, 4} {
		for _, tc := range []struct {
			backing  string
			injected bool
		}{{"memory", true}, {"disk", true}, {"disk", false}} {
			t.Run(fmt.Sprintf("%d/%s/injected_%t", workers, tc.backing, tc.injected), func(t *testing.T) {
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
					entered, canceled, completed := make(chan struct{}, 2), make(chan struct{}, 2), make(chan struct{}, 2)
					inputs := [2]*store.SourceArtifact{}
					for i := range inputs {
						input := &joinedTreeFS{}
						input.acquire = func() error {
							reader, _, _, err := owner.Blob(ctx, sourcecache.BlobKey{Content: fmt.Sprintf("held-input-%d", i)}, sourcecache.Policy{},
								func(ctx context.Context, _ sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
									entered <- struct{}{}
									<-ctx.Done()
									canceled <- struct{}{}
									<-release
									completed <- struct{}{}
									return sourcecache.ProducerResult{}, ctx.Err()
								})
							if reader != nil {
								err = errors.Join(err, reader.Close())
							}
							return err
						}
						inputs[i] = &store.SourceArtifact{LocalPath: t.TempDir(), FS: input}
					}
					cfg := orchestrator.Config{CacheDir: root, Concurrency: workers, BaseInput: inputs[0], Input: inputs[1], ExternalChanges: change.NewSet(nil)}
					if tc.injected {
						cfg.Cache = owner
					}
					done := make(chan error, 1)
					go func() {
						_, _, err := orchestrator.RenderTrees(ctx,
							orchestrator.Tree{RepoRoot: inputs[0].LocalPath}, orchestrator.Tree{RepoRoot: inputs[1].LocalPath},
							cfg)
						done <- err
					}()
					for range 2 {
						<-entered
					}
					if !tc.injected {
						assert.NoError(t, owner.Close())
						externalShare = false
					}
					cancel()
					for range 2 {
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
					reader, _, _, err := owner.Blob(t.Context(), key, sourcecache.Policy{MaxAge: time.Hour}, func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
						_, err := io.WriteString(w, "live")
						return sourcecache.ProducerResult{}, err
					})
					assert.NoError(t, err)
					assert.NoError(t, reader.Close())
					release <- struct{}{}
					release <- struct{}{}
					if err := <-done; !errors.Is(err, context.Canceled) || strings.Count(err.Error(), "base tree:") != 1 || strings.Count(err.Error(), "head tree:") != 1 {
						t.Fatalf("joined cancellation lost a side: %v", err)
					}
					for range 2 {
						<-completed
					}
					if tc.injected {
						reader, _, _, err = owner.Blob(t.Context(), key, sourcecache.Policy{MaxAge: time.Hour}, nil)
						assert.NoError(t, err)
						assert.NoError(t, reader.Close())
						assert.NoError(t, owner.Close())
						externalShare = false
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
