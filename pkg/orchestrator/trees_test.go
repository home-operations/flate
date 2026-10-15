package orchestrator_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

// writeCluster lays out a two-app Flux cluster under dir: app-a renders
// ConfigMap cfg-a (data k=aVal), app-b renders ConfigMap cfg-b (data
// k=bVal). The two per-app Kustomizations are independent, so a change to
// one's rendered output must not pull the other into changed-only scope.
func writeCluster(t *testing.T, dir, aVal, bVal string) {
	t.Helper()
	ks := func(name string) string {
		return "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\n" +
			"metadata: {name: " + name + ", namespace: flux-system}\n" +
			"spec:\n  path: ./apps/" + name + "\n" +
			"  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"
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
		for _, workers := range []int{2, 4} {
			t.Run(tc.name+"/workers"+fmt.Sprint(workers), func(t *testing.T) {
				baseDir := t.TempDir()
				headDir := t.TempDir()
				writeCluster(t, baseDir, "v1", "same")
				writeCluster(t, headDir, "v2", "same")
				for i, dir := range []string{baseDir, headDir} {
					testutil.WriteFile(t, dir, "ks-a.yaml", "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: app-a, namespace: flux-system}\nspec: {path: ./apps/app-a, "+tc.target+"sourceRef: {kind: GitRepository, name: flux-system}}\n")
					if i == 1 && tc.target != "" {
						testutil.WriteFile(t, dir, "apps/app-a/kustomization.yaml", "namespace: dirty\nresources: [cm.yaml]\n")
					} else if i == 1 {
						testutil.WriteFile(t, dir, "apps/app-a/kustomization.yaml", "resources: [cm.yaml]\ncommonAnnotations: {revision: head}\n")
					}
					testutil.WriteFile(t, dir, "apps/app-a/cm.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: consumer-a}
spec:
  path: ./consumer
  sourceRef: {kind: GitRepository, name: flux-system}
  postBuild:
    substituteFrom: [{kind: ConfigMap, name: render-values}]
`)
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
						Concurrency: workers,
					})
				if (err != nil) != (tc.namespace == "") {
					t.Fatalf("RenderTrees: %v, want blocked=%t", err, tc.namespace == "")
				}
				if base.Result == nil || head.Result == nil {
					t.Fatalf("both sides must render: base=%v head=%v", base.Result, head.Result)
				}

				for _, side := range []orchestrator.Rendered{base, head} {
					children := 0
					for _, obj := range side.Store().ListAs[*manifest.Kustomization](manifest.KindKustomization) {
						if obj.Name == "consumer-a" {
							children++
							assert.Equal(t, obj.Namespace, tc.namespace)
							assert.Equal(t, obj.SourceID().Namespace, tc.namespace)
						}
					}
					assert.Equal(t, children, 1)
				}

				consumer := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: tc.namespace, Name: "consumer-a"}
				for i, side := range []orchestrator.Rendered{base, head} {
					if tc.namespace == "" {
						missing := manifest.NamedResource{Kind: manifest.KindGitRepository, Name: "flux-system"}
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
					owner, emitted := rendered.Store().EmittedParent(consumer)
					if !emitted || owner != (manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "app-a"}) {
						t.Fatalf("side %d consumer owner = %s, emitted=%t", i, owner, emitted)
					}
					_, ns := manifest.DocMetadata(obj.Contents)
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
					[]manifest.NamedResource{{Kind: manifest.KindKustomization}, {Kind: manifest.KindHelmRelease}}, nil)
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
}

// TestRenderTrees_StopsButStoreReadable confirms the contract the CLI and
// SDK consumers rely on: both orchestrators are Stopped on return, yet
// their Stores stay readable for post-render doc gathering.
func TestRenderTrees_StopsButStoreReadable(t *testing.T) {
	baseDir := t.TempDir()
	headDir := t.TempDir()
	writeCluster(t, baseDir, "v1", "same")
	writeCluster(t, headDir, "v2", "same")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	base, head, err := orchestrator.RenderTrees(ctx,
		orchestrator.Tree{RepoRoot: baseDir},
		orchestrator.Tree{RepoRoot: headDir},
		orchestrator.Config{WipeSecrets: true, Concurrency: 4})
	if err != nil {
		t.Fatalf("RenderTrees: %v", err)
	}
	// Store still answers after the internal Stop.
	id := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "app-a"}
	if base.Store().GetObject(id) == nil {
		t.Errorf("base Store unreadable after RenderTrees Stop: %s missing", id)
	}
	if head.Store().GetObject(id) == nil {
		t.Errorf("head Store unreadable after RenderTrees Stop: %s missing", id)
	}
}

// writeHomeOpsCluster lays out a cluster-template-style tree under dir
// with NO .git: the Flux entry point is kubernetes/flux/cluster/ks.yaml
// (cluster-apps → ./kubernetes/apps, repo-ROOT-relative), and the app's
// ConfigMap carries val. This is konflate's extracted-tree shape — the
// caller renders it via Tree{RepoRoot: dir, Path: dir/kubernetes/flux/cluster}.
func writeHomeOpsCluster(t *testing.T, dir, val string) {
	t.Helper()
	testutil.WriteFile(t, dir, "kubernetes/flux/cluster/ks.yaml",
		"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\n"+
			"metadata: {name: cluster-apps, namespace: flux-system}\n"+
			"spec:\n  path: ./kubernetes/apps\n"+
			"  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n")
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
		[]manifest.NamedResource{{Kind: manifest.KindKustomization}, {Kind: manifest.KindHelmRelease}}, nil)
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
	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			baseDir, headDir, root := t.TempDir(), t.TempDir(), t.TempDir()
			writeCluster(t, baseDir, "v1", "same")
			writeCluster(t, headDir, "v2", "same")
			if _, _, err := orchestrator.RenderTrees(t.Context(), orchestrator.Tree{RepoRoot: baseDir}, orchestrator.Tree{RepoRoot: headDir},
				orchestrator.Config{CacheDir: root, Concurrency: workers}); err != nil {
				t.Fatal(err)
			}
			res, err := sourcecache.Sweep(t.Context(), root, sourcecache.SweepOpts{})
			if err != nil || len(res.Errors) != 0 {
				t.Fatalf("own cache retained lock: result=%+v err=%v", res, err)
			}
			if _, _, err := orchestrator.RenderTrees(t.Context(), orchestrator.Tree{}, orchestrator.Tree{},
				orchestrator.Config{CacheDir: root, Concurrency: workers}); !errors.Is(err, manifest.ErrInput) {
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
	root := t.TempDir()
	writeCluster(t, root, "same", "same")
	alias := filepath.Join(t.TempDir(), "linked")
	assert.NoError(t, os.Symlink(root, alias))
	base, head, err := orchestrator.RenderTrees(t.Context(),
		orchestrator.Tree{RepoRoot: root}, orchestrator.Tree{RepoRoot: alias},
		orchestrator.Config{CacheDir: t.TempDir(), Concurrency: 2})
	assert.NoError(t, err)
	for _, rendered := range []orchestrator.Rendered{base, head} {
		if names := configMapNames(rendered.Result); !names["cfg-a"] || !names["cfg-b"] {
			t.Fatalf("same-directory comparison lost full render: %v", names)
		}
		if len(rendered.Result.Warnings) != 1 || rendered.Result.Warnings[0].Category != manifest.WarnPathConfig {
			t.Fatalf("same-directory warning changed: %v", rendered.Result.Warnings)
		}
	}
}

type joinedTreeFS struct {
	fs.FS
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (f *joinedTreeFS) Open(name string) (fs.File, error) {
	f.once.Do(func() {
		close(f.entered)
		<-f.release
	})
	return f.FS.Open(name)
}

func TestRenderTrees_CancellationJoinsBeforeCacheClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	inputs := [2]*store.SourceArtifact{}
	gates := [2]*joinedTreeFS{}
	for i := range inputs {
		gates[i] = &joinedTreeFS{FS: fstest.MapFS{"cm.yaml": &fstest.MapFile{Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input}\n")}}, entered: make(chan struct{}), release: release}
		inputs[i] = &store.SourceArtifact{LocalPath: t.TempDir(), FS: gates[i]}
	}
	root := t.TempDir()
	done := make(chan error, 1)
	go func() {
		_, _, err := orchestrator.RenderTrees(ctx,
			orchestrator.Tree{RepoRoot: inputs[0].LocalPath}, orchestrator.Tree{RepoRoot: inputs[1].LocalPath},
			orchestrator.Config{CacheDir: root, Concurrency: 2, BaseInput: inputs[0], Input: inputs[1], ExternalChanges: change.NewSet(nil)})
		done <- err
	}()
	for _, gate := range gates {
		select {
		case <-gate.entered:
		case err := <-done:
			t.Fatalf("render ended before both readers started: %v", err)
		case <-ctx.Done():
			t.Fatal("both readers did not start")
		}
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("render returned before its readers joined: %v", err)
	default:
	}
	res, err := sourcecache.Sweep(t.Context(), root, sourcecache.SweepOpts{})
	if err == nil && len(res.Errors) == 0 {
		t.Fatal("cache owner released while readers were still running")
	}
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("render cancellation: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("render did not join released readers")
	}
	res, err = sourcecache.Sweep(t.Context(), root, sourcecache.SweepOpts{})
	if err != nil || len(res.Errors) != 0 {
		t.Fatalf("joined render retained owner: result=%+v err=%v", res, err)
	}
}
