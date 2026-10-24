package orchestrator_test

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestRenderTrees_AdmittedViews(t *testing.T) {
	for _, scenario := range []string{"equal_labels", "synthetic_labels", "authored_pins", "committed_detection", "unavailable_host_decoy"} {
		t.Run(scenario, func(t *testing.T) {
			baseRoot, headRoot := t.TempDir(), t.TempDir()
			cfg := orchestrator.Config{CacheDir: t.TempDir(), Concurrency: 2}
			changed := testutil.KustomizationID("flux-system", "app-a")
			same := changed
			same.Name = "app-b"
			const url = "git://fixture.invalid/cluster"
			switch scenario {
			case "equal_labels":
				writeCluster(t, baseRoot, "A", "same")
				writeCluster(t, headRoot, "B", "same")
				var err error
				cfg.BaseInput, _, err = baseline.AdmitDirectory(t.Context(), baseRoot, baseRoot)
				assert.NoError(t, err)
				cfg.Input, _, err = baseline.AdmitDirectory(t.Context(), headRoot, headRoot)
				assert.NoError(t, err)
				cfg.Input.LocalPath, cfg.Input.LocalRoot = baseRoot, baseRoot
				headRoot = baseRoot
			case "synthetic_labels":
				for i, root := range []string{baseRoot, headRoot} {
					writeCluster(t, root, []string{"A", "B"}[i], "same")
					files := fstest.MapFS{}
					for _, file := range []string{"ks-a.yaml", "ks-b.yaml", "apps/app-a/kustomization.yaml", "apps/app-a/cm.yaml", "apps/app-b/kustomization.yaml", "apps/app-b/cm.yaml"} {
						body, err := os.ReadFile(filepath.Join(root, file))
						assert.NoError(t, err)
						files[file] = &fstest.MapFile{Data: body}
					}
					label := filepath.Join(root, ".flate-base", "synthetic")
					input := &store.SourceArtifact{LocalPath: label, LocalRoot: label, FS: files}
					if i == 0 {
						cfg.BaseInput, baseRoot = input, label
					} else {
						cfg.Input, headRoot = input, label
					}
				}
			case "authored_pins", "committed_detection":
				headRoot = baseRoot
				repo := testutil.GitInit(t, headRoot)
				var baseHash plumbing.Hash
				for i, value := range []string{"A", "B", "C"} {
					writeCluster(t, headRoot, value, "same")
					body, err := os.ReadFile(filepath.Join(headRoot, "ks-a.yaml"))
					assert.NoError(t, err)
					testutil.WriteFile(t, headRoot, "ks-a.yaml", strings.Replace(string(body), "name: flux-system, namespace: flux-system", "name: cluster, namespace: flux-system", 1))
					ref := "v1"
					if i == 2 {
						ref = "v2"
					}
					testutil.WriteFile(t, headRoot, "source.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: flux-system}\nspec: {url: "+url+", ref: {tag: "+ref+"}}\n")
					hash := testutil.GitCommitAll(t, repo, value, &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}})
					if i == 0 {
						baseHash = hash
					}
					if i < 2 {
						assert.NoError(t, repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName(fmt.Sprintf("v%d", i+1)), hash)))
					}
				}
				cfg.Cache = cachetest.New(t, "", sourcecache.New)
				selected, err := baseline.AutoResolve(t.Context(), headRoot, baseHash.String(), cfg.Cache)
				assert.NoError(t, err)
				cfg.BaseInput, cfg.Input = selected.BaseInput, selected.Source
				if scenario == "authored_pins" {
					cfg.ExternalChanges = selected.Changes
				}
				baseRoot = selected.BaseInput.LocalPath
			case "unavailable_host_decoy":
				writeCluster(t, headRoot, "host-decoy", "same")
				body, err := os.ReadFile(filepath.Join(headRoot, "ks-a.yaml"))
				assert.NoError(t, err)
				testutil.WriteFile(t, headRoot, "ks-a.yaml", strings.Replace(string(body), "name: flux-system, namespace: flux-system", "name: unavailable, namespace: flux-system", 1))
				files := fstest.MapFS{}
				for _, file := range []string{"ks-a.yaml", "ks-b.yaml", "apps/app-b/kustomization.yaml", "apps/app-b/cm.yaml"} {
					body, err := os.ReadFile(filepath.Join(headRoot, file))
					assert.NoError(t, err)
					files[file] = &fstest.MapFile{Data: body}
				}
				cfg.Path, cfg.Input = headRoot, &store.SourceArtifact{LocalPath: headRoot, LocalRoot: headRoot, FS: files}
				cfg.ExternalChanges = change.NewSet([]string{"ks-a.yaml"})
				o, err := orchestrator.New(t.Context(), cfg)
				assert.NoError(t, err)
				t.Cleanup(func() {
					if err := o.Stop(); err != nil {
						t.Errorf("Stop: %v", err)
					}
				})
				o.WithFetcher(manifest.KindGitRepository, nil)
				o.Store().SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: headRoot, FS: os.DirFS(headRoot)})
				assert.NoError(t, o.Bootstrap(t.Context()))
				if o.Store().GetArtifact(manifest.BootstrapSourceID) != cfg.Input {
					t.Fatal("preseeded input replaced supplied input")
				}
				source := &manifest.GitRepository{Name: "unavailable", Namespace: "flux-system"}
				o.Store().AddObject(source)
				o.Store().SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: headRoot})
				o.Store().UpdateStatus(source.Named(), store.StatusReady, "", "fixture unavailable payload")
				result, err := o.Render(t.Context())
				if err == nil || result == nil || result.Failed[changed].Status != store.StatusFailed {
					t.Fatalf("selected producer must fail after discovery: result=%+v err=%v", result, err)
				}
				if len(outputByParent(result.Manifests)[changed]) != 0 || len(outputByParent(result.Manifests)[same]) != 0 {
					t.Fatalf("unavailable producer or unchanged sibling emitted: %v", result.Manifests)
				}
				for _, cm := range o.Store().ListObjects("", manifest.KindConfigMap) {
					if cm.Named().Name == "cfg-a" {
						t.Fatal("unavailable source admitted the host decoy ConfigMap")
					}
				}
				return
			}
			base, head, err := orchestrator.RenderTrees(t.Context(),
				orchestrator.Tree{Path: baseRoot, SelfURLs: []string{url}},
				orchestrator.Tree{Path: headRoot, SelfURLs: []string{url}}, cfg)
			assert.NoError(t, err)
			for side, rendered := range []orchestrator.Rendered{base, head} {
				docs := outputByParent(rendered.Result.Manifests)[changed]
				if len(docs) != 1 || len(outputByParent(rendered.Result.Manifests)[same]) != 0 {
					t.Fatalf("side %d lost producer ownership or unchanged selection: %v", side, rendered.Result.Manifests)
				}
				name, ns := manifest.DocMetadata(docs[0])
				if name != "cfg-a" {
					t.Fatalf("side %d producer emitted %q, want cfg-a", side, name)
				}
				assert.Equal(t, ns, "")
				obj, _, _, err := manifest.ParseDoc(docs[0], manifest.ParseDocOptions{DefaultNamespace: ""})
				assert.NoError(t, err)
				owners := testutil.Owners(rendered.Store().Productions(obj.Named(), nil))
				if !slices.Contains(owners, changed) {
					t.Fatalf("side %d owners=%v want %s", side, owners, changed)
				}
				if value := docs[0]["data"].(map[string]any)["k"]; value != []string{"A", "B"}[side] {
					t.Fatalf("side %d read %v, want selected source bytes", side, value)
				}
			}
		})
	}
}
