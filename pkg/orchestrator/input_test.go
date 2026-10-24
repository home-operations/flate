package orchestrator_test

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"testing/synctest"
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

type scopedPeerReader struct {
	fs.FS
	unrelated atomic.Int64
}

func (r *scopedPeerReader) ReadDir(name string) ([]fs.DirEntry, error) {
	if name == "clusters/b" {
		r.unrelated.Add(1)
	}
	return fs.ReadDir(r.FS, name)
}

func TestRenderTrees_AdmittedViews(t *testing.T) {
	for _, scenario := range []string{"scoped_peer", "equal_selected_alias", "selected_payload_only", "source_declaration_equal", "equal_labels", "synthetic_labels", "authored_pins", "committed_detection", "equal_selected_pin", "unavailable_host_decoy"} {
		t.Run(scenario, func(t *testing.T) {
			baseRoot, headRoot := t.TempDir(), t.TempDir()
			cfg := orchestrator.Config{CacheDir: t.TempDir(), Concurrency: 2}
			changed := testutil.KustomizationID("flux-system", "app-a")
			same := changed
			same.Name = "app-b"
			const url = "git://fixture.invalid/cluster"
			switch scenario {
			case "scoped_peer":
				for _, missing := range []bool{false, true} {
					for _, synthetic := range []bool{false, true} {
						t.Run(fmt.Sprintf("missing_%t/synthetic_%t", missing, synthetic), func(t *testing.T) {
							roots := [2]string{t.TempDir(), t.TempDir()}
							inputs := [2]*store.SourceArtifact{}
							for side, root := range roots {
								writeCluster(t, root, []string{"A", "B"}[side], "same")
								testutil.WriteFile(t, root, "clusters/b/broken.yaml", "invalid: [\n")
								if side == 1 || !missing {
									body, err := os.ReadFile(filepath.Join(root, "ks-a.yaml"))
									assert.NoError(t, err)
									testutil.WriteFile(t, root, "clusters/a/ks.yaml", string(body))
								}
								input, _, err := baseline.AdmitDirectory(t.Context(), root, root)
								assert.NoError(t, err)
								if synthetic {
									files := fstest.MapFS{}
									assert.NoError(t, fs.WalkDir(input.FS, ".", func(name string, entry fs.DirEntry, err error) error {
										if err != nil {
											return err
										}
										if !entry.IsDir() {
											data, err := fs.ReadFile(input.FS, name)
											if err != nil {
												return err
											}
											files[name] = &fstest.MapFile{Data: data}
										}
										return nil
									}))
									input.FS = files
									input.LocalPath = filepath.Join(root, "label")
									input.LocalRoot = input.LocalPath
									roots[side] = input.LocalPath
								}
								inputs[side] = input
							}
							var peerMap fstest.MapFS
							peerReader := &scopedPeerReader{FS: inputs[0].FS}
							if synthetic {
								peerMap = inputs[0].FS.(fstest.MapFS)
								inputs[0].FS = peerReader
							}
							cache := cachetest.New(t, "", sourcecache.New)
							var previous map[manifest.NamedResource][]map[string]any
							for run := range 2 {
								peerReader.unrelated.Store(0)
								directCfg := orchestrator.Config{Path: filepath.Join(roots[1], "clusters/a"), Input: inputs[1], BaseInput: inputs[0], Cache: cache, Concurrency: 2}
								direct, err := orchestrator.New(t.Context(), directCfg)
								assert.NoError(t, err)
								assert.NoError(t, direct.Bootstrap(t.Context()))
								assert.NoError(t, direct.Bootstrap(t.Context()))
								result, err := direct.Render(t.Context())
								assert.NoError(t, err)
								assert.Diff(t, configMapNames(result), map[string]bool{"cfg-a": true})
								if synthetic {
									assert.Equal(t, peerReader.unrelated.Load(), int64(1))
								}
								foreign := testutil.KustomizationID("flux-system", "app-b")
								assert.Equal(t, direct.Store().GetObject(foreign) == nil, true)
								if missing {
									if synthetic {
										peerMap["clusters/a"] = &fstest.MapFile{Mode: fs.ModeDir | 0o755}
									} else {
										assert.NoError(t, os.MkdirAll(filepath.Join(roots[0], "clusters/a"), 0o755))
									}
								}
								pairedCfg := directCfg
								pairedCfg.Path = ""
								_, head, err := orchestrator.RenderTrees(t.Context(), orchestrator.Tree{Path: filepath.Join(roots[0], "clusters/a")}, orchestrator.Tree{Path: filepath.Join(roots[1], "clusters/a")}, pairedCfg)
								assert.NoError(t, err)
								assert.Diff(t, outputByParent(result.Manifests), outputByParent(head.Result.Manifests))
								if run == 1 {
									assert.Diff(t, outputByParent(result.Manifests), previous)
								}
								previous = outputByParent(result.Manifests)
							}
						})
					}
				}
				return
			case "equal_selected_alias", "selected_payload_only", "source_declaration_equal":
				repo := testutil.GitInit(t, headRoot)
				var old plumbing.Hash
				for i, value := range []string{"A", "B"} {
					writeCluster(t, headRoot, value, "same")
					for _, file := range []string{"ks-a.yaml", "ks-b.yaml"} {
						body, err := os.ReadFile(filepath.Join(headRoot, file))
						assert.NoError(t, err)
						testutil.WriteFile(t, headRoot, file, strings.ReplaceAll(string(body), "name: flux-system, namespace: flux-system", "name: cluster, namespace: flux-system"))
					}
					ref := ", ref: {branch: selected}"
					if scenario == "selected_payload_only" {
						ref = ""
					}
					interval := "1m"
					if scenario == "source_declaration_equal" && i == 1 {
						interval = "2m"
					}
					testutil.WriteFile(t, headRoot, "source.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: flux-system}\nspec: {url: "+url+", interval: "+interval+ref+"}\n")
					hash := testutil.GitCommitAll(t, repo, value, &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}})
					if i == 0 {
						old = hash
					} else {
						assert.NoError(t, repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewBranchReferenceName("selected"), hash)))
					}
				}
				cfg.Cache = cachetest.New(t, "", sourcecache.New)
				selected, err := baseline.AutoResolve(t.Context(), headRoot, old.String(), cfg.Cache)
				assert.NoError(t, err)
				cfg.Input, cfg.BaseInput = selected.Source, selected.BaseInput
				cfg.ExternalChanges = selected.Changes
				if scenario == "selected_payload_only" {
					cfg.ExternalChanges = change.NewSet(nil)
				}
				baseRoot = cfg.BaseInput.LocalPath
				var previous [2]map[manifest.NamedResource][]map[string]any
				for run := range 2 {
					base, head, err := orchestrator.RenderTrees(t.Context(),
						orchestrator.Tree{Path: baseRoot, SelfURLs: []string{url}}, orchestrator.Tree{Path: headRoot, SelfURLs: []string{url}}, cfg)
					assert.NoError(t, err)
					source := (&manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}).Named()
					left := base.Store().GetArtifact(source).(*store.SourceArtifact)
					right := head.Store().GetArtifact(source).(*store.SourceArtifact)
					payload, err := change.Detect(t.Context(), left, right)
					assert.NoError(t, err)
					if scenario != "selected_payload_only" {
						assert.Equal(t, payload.Len(), 0)
						assert.Equal(t, left.InputAlias, false)
						assert.Equal(t, right.InputAlias, true)
					}
					for side, rendered := range []orchestrator.Rendered{base, head} {
						want := map[string]bool{}
						if scenario != "equal_selected_alias" {
							want["cfg-a"] = true
						}
						if scenario == "source_declaration_equal" {
							want["cfg-b"] = true
						}
						assert.Diff(t, configMapNames(rendered.Result), want)
						for id, docs := range outputByParent(rendered.Result.Manifests) {
							for _, doc := range docs {
								name, _ := manifest.DocMetadata(doc)
								value := "same"
								if name == "cfg-a" {
									value = "B"
									if scenario == "selected_payload_only" && side == 0 {
										value = "A"
									}
								}
								assert.Diff(t, doc, map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": name, "labels": map[string]any{"kustomize.toolkit.fluxcd.io/name": id.Name, "kustomize.toolkit.fluxcd.io/namespace": id.Namespace}}, "data": map[string]any{"k": value}})
								assert.Equal(t, id == changed || id == same, true)
							}
						}
						if scenario == "selected_payload_only" {
							for _, warning := range rendered.Result.Warnings {
								if strings.Contains(warning.Message, "output will be empty") {
									t.Fatal(warning)
								}
							}
						}
						if run == 0 && scenario == "source_declaration_equal" {
							directCfg := cfg
							directCfg.Input, directCfg.BaseInput = []*store.SourceArtifact{cfg.BaseInput, cfg.Input}[side], []*store.SourceArtifact{cfg.Input, cfg.BaseInput}[side]
							directCfg.Path, directCfg.SelfURLs = directCfg.Input.LocalPath, []string{url}
							direct, err := orchestrator.New(t.Context(), directCfg)
							assert.NoError(t, err)
							result, err := direct.Render(t.Context())
							assert.NoError(t, err)
							assert.Diff(t, outputByParent(result.Manifests), outputByParent(rendered.Result.Manifests))
						}
						if run == 1 {
							assert.Diff(t, outputByParent(rendered.Result.Manifests), previous[side])
						}
						previous[side] = outputByParent(rendered.Result.Manifests)
						logged := map[string][]map[string]any{}
						for id, docs := range outputByParent(rendered.Result.Manifests) {
							logged[id.String()] = docs
						}
						bytes, err := json.Marshal(logged)
						assert.NoError(t, err)
						t.Logf("run=%d side=%d exact selected output=%s", run, side, bytes)
					}
				}
				return
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
			case "authored_pins", "committed_detection", "equal_selected_pin":
				headRoot = baseRoot
				repo := testutil.GitInit(t, headRoot)
				var baseHash plumbing.Hash
				for i, value := range []string{"A", "B", "C"} {
					writeCluster(t, headRoot, value, "same")
					body, err := os.ReadFile(filepath.Join(headRoot, "ks-a.yaml"))
					assert.NoError(t, err)
					testutil.WriteFile(t, headRoot, "ks-a.yaml", strings.Replace(string(body), "name: flux-system, namespace: flux-system", "name: cluster, namespace: flux-system", 1))
					ref := "v1"
					if i == 2 && scenario != "equal_selected_pin" {
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
				if scenario == "authored_pins" || scenario == "equal_selected_pin" {
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
				o.Store().UpdateStatus(source.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: "fixture unavailable payload"})
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
			if scenario == "equal_selected_pin" {
				for side, rendered := range []orchestrator.Rendered{base, head} {
					if len(outputByParent(rendered.Result.Manifests)[changed]) != 0 || len(outputByParent(rendered.Result.Manifests)[same]) != 0 {
						t.Fatalf("side %d rendered dirty checkout changes behind equal selected pin: %v", side, rendered.Result.Manifests)
					}
					selected := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "cluster"}
					art, ok := rendered.Store().GetArtifact(selected).(*store.SourceArtifact)
					if !ok {
						t.Fatalf("side %d has no selected pinned source", side)
					}
					body, err := fs.ReadFile(art.FS, "apps/app-a/cm.yaml")
					assert.NoError(t, err)
					assert.Equal(t, string(body), "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cfg-a}\ndata: {k: A}\n")
				}
				return
			}
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

type preparationReader struct {
	fs.FS
	parsed       atomic.Int64
	fingerprints atomic.Int64
}

func (r *preparationReader) Open(name string) (fs.File, error) {
	if name == "ks-a.yaml" {
		r.parsed.Add(1)
	}
	if name == "fingerprint.txt" {
		r.fingerprints.Add(1)
	}
	return r.FS.Open(name)
}

func TestRenderTrees_PreparesEachSideOnce(t *testing.T) {
	t.Parallel()
	roots := [2]string{t.TempDir(), t.TempDir()}
	var readers [2]*preparationReader
	inputs := [2]*store.SourceArtifact{}
	for side, root := range roots {
		writeCluster(t, root, []string{"before", "after"}[side], "same")
		for _, name := range []string{"ks-a.yaml", "ks-b.yaml"} {
			body, err := os.ReadFile(filepath.Join(root, name))
			assert.NoError(t, err)
			testutil.WriteFile(t, root, name, strings.ReplaceAll(string(body), "name: flux-system, namespace: flux-system", "name: selected, namespace: flux-system"))
		}
		testutil.WriteFile(t, root, "fingerprint.txt", "selected content evidence\n")
		readers[side] = &preparationReader{FS: os.DirFS(root)}
		inputs[side] = &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: readers[side]}
	}
	base, head, err := orchestrator.RenderTrees(t.Context(), orchestrator.Tree{RepoRoot: roots[0]}, orchestrator.Tree{RepoRoot: roots[1]}, orchestrator.Config{BaseInput: inputs[0], Input: inputs[1], ExternalChanges: change.NewSet([]string{"apps/app-a/cm.yaml"}), Concurrency: 2})
	assert.NoError(t, err)
	for side, rendered := range []orchestrator.Rendered{base, head} {
		assert.Equal(t, readers[side].fingerprints.Load(), int64(2))
		assert.Equal(t, readers[side].parsed.Load(), int64(3))
		assert.Diff(t, configMapNames(rendered.Result), map[string]bool{"cfg-a": true})
		t.Logf("side=%d declaration reads=%d selected evidence reads=%d", side, readers[side].parsed.Load(), readers[side].fingerprints.Load())
	}
}

type pairedBarrierReader struct {
	fs.FS
	entered    chan struct{}
	release    chan struct{}
	blocked    atomic.Bool
	inputOpens atomic.Int64
}

func (r *pairedBarrierReader) Open(name string) (fs.File, error) {
	if name == "apps/app-a/render-input.env" {
		r.inputOpens.Add(1)
	}
	if name == "block.env" && r.entered != nil && r.blocked.CompareAndSwap(false, true) {
		close(r.entered)
		<-r.release
	}
	return r.FS.Open(name)
}

func TestRenderTrees_JoinsPreparationBeforeDispatch(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		roots := [2]string{t.TempDir(), t.TempDir()}
		var readers [2]*pairedBarrierReader
		var inputs [2]*store.SourceArtifact
		entered, release := make(chan struct{}), make(chan struct{})
		for side, root := range roots {
			writeCluster(t, root, []string{"before", "after"}[side], "same")
			for _, name := range []string{"ks-a.yaml", "ks-b.yaml"} {
				body, err := os.ReadFile(filepath.Join(root, name))
				assert.NoError(t, err)
				testutil.WriteFile(t, root, name, strings.ReplaceAll(string(body), "name: flux-system, namespace: flux-system", "name: selected, namespace: flux-system"))
			}
			testutil.WriteFile(t, root, "block.env", "BLOCK=once\n")
			testutil.WriteFile(t, root, "apps/app-a/render-input.env", "VALUE=selected\n")
			testutil.WriteFile(t, root, "apps/app-a/kustomization.yaml", "resources: [cm.yaml]\nconfigMapGenerator: [{name: generated, envs: [render-input.env]}]\n")
			readers[side] = &pairedBarrierReader{FS: os.DirFS(root)}
			if side == 1 {
				readers[side].entered, readers[side].release = entered, release
			}
			inputs[side] = &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: readers[side]}
		}
		done := make(chan error, 1)
		go func() {
			_, _, err := orchestrator.RenderTrees(t.Context(), orchestrator.Tree{RepoRoot: roots[0]}, orchestrator.Tree{RepoRoot: roots[1]}, orchestrator.Config{BaseInput: inputs[0], Input: inputs[1], ExternalChanges: change.NewSet([]string{"apps/app-a/cm.yaml"}), Concurrency: 2})
			done <- err
		}()
		<-entered
		synctest.Wait()
		assert.Equal(t, readers[0].inputOpens.Load(), int64(7))
		close(release)
		assert.NoError(t, <-done)
		for side, reader := range readers {
			assert.Equal(t, reader.inputOpens.Load(), int64(10))
			t.Logf("side=%d render input opens=%d", side, reader.inputOpens.Load())
		}
	})
}
