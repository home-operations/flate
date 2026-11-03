package orchestrator

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

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRenderTrees_AdmittedViews(t *testing.T) {
	for _, scenario := range []string{"scoped_peer", "equal_selected_alias", "selected_payload_only", "source_declaration_equal", "equal_labels", "synthetic_labels", "authored_pins", "committed_detection", "equal_selected_pin", "unavailable_host_decoy"} {
		t.Run(scenario, func(t *testing.T) {
			testRenderTreesAdmittedViewsCase(t, &scenario)
		},
		)
	}
}

func TestRenderTrees_EntrySubstitutionSelectsChangedChild(t *testing.T) {
	t.Parallel()
	roots := [2]string{t.TempDir(), t.TempDir()}
	for side, root := range roots {
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: [root.yaml, child.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"root.yaml",
			testutil.KustomizationYAML(
				"root",
				"flux-system",
				"  path: ./\n  postBuild:\n    substitute: {LOCATION: "+[]string{"before", "after"}[side]+"}\n",
			),
		)
		testutil.WriteFile(
			t,
			root,
			"child.yaml",
			testutil.KustomizationYAML("child", "flux-system", "  path: ./workloads/${LOCATION}\n"),
		)
		for _, value := range []string{"before", "after"} {
			testutil.WriteFile(t, root, "workloads/"+value+"/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"workloads/"+value+"/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload}\ndata: {value: "+value+"}\n",
			)
		}
	}
	runs, err := RenderTrees(t.Context(), []Tree{
		{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}, Path: roots[0]},
		{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Path: roots[1]},
	}, Config{Concurrency: 2})
	assert.NoError(t, err)
	child := testutil.KustomizationID("flux-system", "child")
	for side, run := range runs {
		want := []string{"before", "after"}[side]
		obj, ok := run.store.Get[*manifest.Kustomization](child)
		{
			assert.FatalIf(t, !ok, "side %d: child declaration missing", side)
		}
		assert.Equal(t, obj.Path, "./workloads/"+want)
		docs := outputByParent(run.Result.Manifests)[child]
		{
			assert.FatalIf(t, len(docs) != 1, "side %d: shaped child not rendered: %v", side, docs)
		}
		assert.Equal(t, docs[0]["data"].(map[string]any)["value"], any(want))
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
			testutil.WriteFile(
				t,
				root,
				name,
				strings.ReplaceAll(
					string(body),
					"name: flux-system, namespace: flux-system",
					"name: selected, namespace: flux-system",
				),
			)
		}
		testutil.WriteFile(t, root, "fingerprint.txt", "selected content evidence\n")
		readers[side] = &preparationReader{FS: os.DirFS(root)}
		inputs[side] = &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: readers[side]}
	}
	renderedTrees3, err := RenderTrees(
		t.Context(),
		[]Tree{
			{
				RepoRoot:        roots[0],
				ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
				Input:           inputs[0],
				BaseInput:       inputs[1],
				ExternalChanges: change.NewSet([]string{"apps/app-a/cm.yaml"}),
			},
			{
				RepoRoot:        roots[1],
				ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
				Input:           inputs[1],
				BaseInput:       inputs[0],
				ExternalChanges: change.NewSet([]string{"apps/app-a/cm.yaml"}),
			},
		},
		Config{Concurrency: 2},
	)
	base, head := renderedTrees3[0], renderedTrees3[1]
	assert.NoError(t, err)
	for side, rendered := range []Rendered{base, head} {
		assert.Equal(t, readers[side].fingerprints.Load(), int64(2))
		// Mutable source proofs MUST retain two validations, alongside one census,
		// one fingerprint, one required build, and seven stat-only opens.
		assert.Equal(t, readers[side].parsed.Load(), int64(12))
		assert.Diff(t, configMapNames(rendered.Result), map[string]bool{"cfg-a": true})
		t.Logf(
			"side=%d declaration reads=%d selected evidence reads=%d",
			side,
			readers[side].parsed.Load(),
			readers[side].fingerprints.Load(),
		)
	}
}

func testRenderTreesAdmittedViewsCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	baseRoot, headRoot := t.TempDir(), t.TempDir()
	cfg := treeConfig{CacheDir: t.TempDir(), Concurrency: 2, Tree: Tree{}}
	changed := testutil.KustomizationID("flux-system", "app-a")
	same := changed
	same.Name = "app-b"
	const url = "git://fixture.invalid/cluster"
	if returned := testRenderTreesAdmittedViewsCaseSegment(t, &testRenderTreesAdmittedViewsCaseSegmentInputs{testCaseInput: &testCaseInput, baseRoot: &baseRoot, headRoot: &headRoot, cfg: &cfg, changed: &changed, same: &same}); returned {
		return
	}
	renderedTrees1, err := RenderTrees(
		t.Context(),
		[]Tree{
			{
				Path:            baseRoot,
				SelfURLs:        []string{url},
				ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
				Input:           cfg.BaseInput,
				BaseInput:       cfg.Input,
				ExternalChanges: cfg.ExternalChanges,
			},
			{
				Path:            headRoot,
				SelfURLs:        []string{url},
				ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
				BaseInput:       cfg.BaseInput,
				ExternalChanges: cfg.ExternalChanges,
				Input:           cfg.Input,
			},
		},
		cfg.Config,
	)
	base, head := renderedTrees1[0], renderedTrees1[1]
	assert.NoError(t, err)
	if (*testCaseInput) == "equal_selected_pin" {
		for side, rendered := range []Rendered{base, head} {
			if len(outputByParent(rendered.Result.Manifests)[changed]) != 0 ||
				len(outputByParent(rendered.Result.Manifests)[same]) != 0 {
				t.Fatalf(
					"side %d rendered dirty checkout changes behind equal selected pin: %v",
					side,
					rendered.Result.Manifests,
				)
			}
			selected := manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindGitRepository,
				Namespace: "flux-system",
				Name:      "cluster",
			}
			art, ok := rendered.Store().GetArtifact(selected, store.ArtifactContent).(*store.SourceArtifact)
			{
				assert.FatalIf(t, !ok, "side %d has no selected pinned source", side)
			}
			body, err := fs.ReadFile(art.FS, "apps/app-a/cm.yaml")
			assert.NoError(t, err)
			assert.Equal(
				t,
				string(body),
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cfg-a}\ndata: {k: A}\n",
			)
		}
		return
	}
	for side, rendered := range []Rendered{base, head} {
		docs := outputByParent(rendered.Result.Manifests)[changed]
		if len(docs) != 1 || len(outputByParent(rendered.Result.Manifests)[same]) != 0 {
			t.Fatalf(
				"side %d lost producer ownership or unchanged selection: %v",
				side,
				rendered.Result.Manifests,
			)
		}
		name, ns := manifest.DocMetadata(docs[0])
		{
			assert.FatalIf(t, name != "cfg-a", "side %d producer emitted %q, want cfg-a", side, name)
		}
		assert.Equal(t, ns, "")
		obj, _, _, err := manifest.ParseDoc(docs[0], manifest.ParseDocOptions{DefaultNamespace: ""})
		assert.NoError(t, err)
		owners := testutil.Owners(rendered.Store().Productions(obj.Named(), nil))
		{
			assert.FatalIf(
				t,
				!slices.Contains(owners, changed),
				"side %d owners=%v want %s",
				side,
				owners,
				changed,
			)
		}
		{
			{
				value := docs[0]["data"].(map[string]any)["k"]
				assert.FatalIf(
					t,
					value != []string{"A", "B"}[side],
					"side %d read %v, want selected source bytes",
					side,
					value,
				)
			}
		}
	}
}

type testRenderTreesAdmittedViewsCaseCaseInputs struct {
	missing   *bool
	synthetic *bool
}

func testRenderTreesAdmittedViewsCaseCase(t *testing.T, testCaseInput testRenderTreesAdmittedViewsCaseCaseInputs) {
	t.Helper()

	roots := [2]string{t.TempDir(), t.TempDir()}
	inputs := [2]*store.SourceArtifact{}
	testRenderTreesAdmittedViewsCaseCaseSegment(t, &testCaseInput, &roots, &inputs)
	peerReader := &scopedPeerReader{FS: inputs[0].FS}
	if *testCaseInput.synthetic {
		inputs[0].FS = peerReader
	}
	cache := cachetest.New(t, "", sourcecache.New)
	var previous map[manifest.NamedResource][]map[string]any
	for run := range 2 {
		peerReader.unrelated.Store(0)
		directCfg := treeConfig{
			Cache:       cache,
			Concurrency: 2,
			Path:        filepath.Join(roots[1], "clusters/a"),
			Input:       inputs[1],
			BaseInput:   inputs[0],
		}
		direct, err := newTestOrchestrator(t, directCfg.Tree, directCfg.Config)
		assert.NoError(t, err)
		assert.NoError(t, direct.bootstrap(t.Context()))
		assert.NoError(t, direct.bootstrap(t.Context()))
		result, err := direct.render(t.Context())
		assert.NoError(t, err)
		assert.Diff(t, configMapNames(result), map[string]bool{"cfg-a": true})
		if *testCaseInput.synthetic {
			assert.Equal(t, peerReader.unrelated.Load(), int64(1))
		}
		foreign := testutil.KustomizationID("flux-system", "app-b")
		assert.Equal(t, direct.Store().GetObject(foreign) == nil, true)
		pairedCfg := directCfg
		pairedCfg.Path = ""
		renderedTrees1, err := RenderTrees(
			t.Context(),
			[]Tree{
				{
					Path:            filepath.Join(roots[0], "clusters/a"),
					ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
					ExternalChanges: pairedCfg.ExternalChanges,
					Input:           pairedCfg.BaseInput,
					BaseInput:       pairedCfg.Input,
				},
				{
					Path:            filepath.Join(roots[1], "clusters/a"),
					ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
					Input:           pairedCfg.Input,
					BaseInput:       pairedCfg.BaseInput,
					ExternalChanges: pairedCfg.ExternalChanges,
				},
			},
			pairedCfg.Config,
		)
		_, head := renderedTrees1[0], renderedTrees1[1]
		assert.NoError(t, err)
		assert.Diff(t, outputByParent(result.Manifests), outputByParent(head.Result.Manifests))
		if run == 1 {
			assert.Diff(t, outputByParent(result.Manifests), previous)
		}
		previous = outputByParent(result.Manifests)
	}
}

func testRenderTreesAdmittedViewsCaseCaseSegment(t *testing.T, testCaseInput *testRenderTreesAdmittedViewsCaseCaseInputs, roots *[2]string, inputs *[2]*store.SourceArtifact) {
	t.Helper()
	for side, root := range *roots {
		writeCluster(t, root, []string{"A", "B"}[side], "same")
		testutil.WriteFile(t, root, "clusters/b/broken.yaml", "invalid: [\n")
		if side == 1 || !(*(*testCaseInput).missing) {
			body, err := os.ReadFile(filepath.Join(root, "ks-a.yaml"))
			assert.NoError(t, err)
			testutil.WriteFile(t, root, "clusters/a/ks.yaml", string(body))
		} else {
			testutil.WriteFile(t, root, "clusters/a/kustomization.yaml", "resources: []\n")
		}
		input, _, err := baseline.AdmitDirectory(t.Context(), root, root)
		assert.NoError(t, err)
		if *(*testCaseInput).synthetic {
			files := fstest.MapFS{}
			assert.NoError(
				t,
				fs.WalkDir(
					input.FS,
					".",
					func(name string, entry fs.DirEntry, err error) error {
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
					},
				),
			)
			input.FS = files
			input.LocalPath = filepath.Join(root, "label")
			input.LocalRoot = input.LocalPath
			(*roots)[side] = input.LocalPath
		}
		(*inputs)[side] = input
	}
}

type testRenderTreesAdmittedViewsCaseSegmentInputs struct {
	testCaseInput **string
	baseRoot      *string
	headRoot      *string
	cfg           *treeConfig
	changed       *manifest.NamedResource
	same          *manifest.NamedResource
}

func testRenderTreesAdmittedViewsCaseSegment(t *testing.T, statementInput *testRenderTreesAdmittedViewsCaseSegmentInputs) bool {
	t.Helper()
	const url = "git://fixture.invalid/cluster"
	switch *(*statementInput.testCaseInput) {
	case "scoped_peer":
		for _, missing := range []bool{false, true} {
			for _, synthetic := range []bool{false, true} {
				t.Run(fmt.Sprintf("missing_%t/synthetic_%t", missing, synthetic), func(t *testing.T) {
					t.Parallel()
					testRenderTreesAdmittedViewsCaseCase(
						t,
						testRenderTreesAdmittedViewsCaseCaseInputs{missing: &missing, synthetic: &synthetic},
					)
				},
				)
			}
		}
		return true
	case "equal_selected_alias", "selected_payload_only", "source_declaration_equal":
		repo := testutil.GitInit(t, (*statementInput.headRoot))
		var old plumbing.Hash
		for i, value := range []string{"A", "B"} {
			writeCluster(t, (*statementInput.headRoot), value, "same")
			testutil.WriteFile(
				t,
				(*statementInput.headRoot),
				"kustomization.yaml",
				"resources: [ks-a.yaml, ks-b.yaml, source.yaml]\n",
			)
			for _, file := range []string{"ks-a.yaml", "ks-b.yaml"} {
				body, err := os.ReadFile(filepath.Join((*statementInput.headRoot), file))
				assert.NoError(t, err)
				testutil.WriteFile(
					t,
					(*statementInput.headRoot),
					file,
					strings.ReplaceAll(
						string(body),
						"name: flux-system, namespace: flux-system",
						"name: cluster, namespace: flux-system",
					),
				)
			}
			ref := ", ref: {branch: selected}"
			if (*(*statementInput.testCaseInput)) == "selected_payload_only" {
				ref = ""
			}
			interval := "1m"
			if (*(*statementInput.testCaseInput)) == "source_declaration_equal" && i == 1 {
				interval = "2m"
			}
			testutil.WriteFile(
				t,
				(*statementInput.headRoot),
				"source.yaml",
				"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: flux-system}\nspec: {url: "+url+", interval: "+interval+ref+"}\n",
			)
			hash := testutil.GitCommitAll(
				t,
				repo,
				value,
				&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}},
			)
			if i == 0 {
				old = hash
			} else {
				assert.NoError(
					t,
					repo.Storer.SetReference(
						plumbing.NewHashReference(plumbing.NewBranchReferenceName("selected"), hash),
					),
				)
			}
		}
		(*statementInput.cfg).Cache = cachetest.New(t, "", sourcecache.New)
		selected, err := baseline.AutoResolve(t.Context(), (*statementInput.headRoot), old.String(), (*statementInput.cfg).Cache)
		assert.NoError(t, err)
		(*statementInput.cfg).Input, (*statementInput.cfg).BaseInput = selected.Source, selected.BaseInput
		(*statementInput.cfg).ExternalChanges = selected.Changes
		if (*(*statementInput.testCaseInput)) == "selected_payload_only" {
			(*statementInput.cfg).ExternalChanges = change.NewSet(nil)
		}
		(*statementInput.baseRoot) = (*statementInput.cfg).BaseInput.LocalPath
		var previous [2]map[manifest.NamedResource][]map[string]any
		for run := range 2 {
			renderedTrees2, err := RenderTrees(
				t.Context(),
				[]Tree{
					{
						Path:            (*statementInput.baseRoot),
						SelfURLs:        []string{url},
						ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
						Input:           (*statementInput.cfg).BaseInput,
						BaseInput:       (*statementInput.cfg).Input,
						ExternalChanges: (*statementInput.cfg).ExternalChanges,
					},
					{
						Path:            (*statementInput.headRoot),
						SelfURLs:        []string{url},
						ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
						ExternalChanges: (*statementInput.cfg).ExternalChanges,
						Input:           (*statementInput.cfg).Input,
						BaseInput:       (*statementInput.cfg).BaseInput,
					},
				},
				(*statementInput.cfg).Config,
			)
			base, head := renderedTrees2[0], renderedTrees2[1]
			assert.NoError(t, err)
			source := (&manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}).Named()
			left := base.Store().GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
			right := head.Store().GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
			payload, err := change.Detect(t.Context(), left, right)
			assert.NoError(t, err)
			if (*(*statementInput.testCaseInput)) != "selected_payload_only" {
				assert.Equal(t, payload.Len(), 0)
				assert.Equal(t, left.InputAlias, false)
				assert.Equal(t, right.InputAlias, true)
			}
			for side, rendered := range []Rendered{base, head} {
				want := map[string]bool{}
				if (*(*statementInput.testCaseInput)) != "equal_selected_alias" {
					want["cfg-a"] = true
				}
				if (*(*statementInput.testCaseInput)) == "source_declaration_equal" {
					want["cfg-b"] = true
				}
				assert.Diff(t, configMapNames(rendered.Result), want)
				for id, docs := range outputByParent(rendered.Result.Manifests) {
					if id == rendered.entryBinding.Producer {
						var declarations []map[string]any
						for _, file := range []string{"ks-a.yaml", "ks-b.yaml", "source.yaml"} {
							body, err := fs.ReadFile(rendered.cfg.Input.FS, file)
							assert.NoError(t, err)
							parsed, err := testutil.DecodeDocs(strings.NewReader(string(body)))
							assert.NoError(t, err)
							declarations = append(declarations, parsed...)
						}
						assert.Diff(t, docs, declarations)
						continue
					}
					for _, doc := range docs {
						name, _ := manifest.DocMetadata(doc)
						value := "same"
						if name == "cfg-a" {
							value = "B"
							if (*(*statementInput.testCaseInput)) == "selected_payload_only" && side == 0 {
								value = "A"
							}
						}
						assert.Diff(
							t,
							doc,
							map[string]any{
								"apiVersion": "v1",
								"kind":       "ConfigMap",
								"metadata": map[string]any{
									"name": name,
									"labels": map[string]any{
										"kustomize.toolkit.fluxcd.io/name":      id.Name,
										"kustomize.toolkit.fluxcd.io/namespace": id.Namespace,
									},
								},
								"data": map[string]any{"k": value},
							},
						)
						assert.Equal(t, id == (*statementInput.changed) || id == (*statementInput.same), true)
					}
				}
				if (*(*statementInput.testCaseInput)) == "selected_payload_only" {
					{
						warningValues := rendered.Result.Warnings
						for warningIndex := range warningValues {
							warning := warningValues[warningIndex]

							{
								assert.FatalIf(
									t,
									strings.Contains(warning.Message, "output will be empty"),
									"%v",
									warning,
								)
							}
						}
					}
				}
				if run == 0 && (*(*statementInput.testCaseInput)) == "source_declaration_equal" {
					directCfg := (*statementInput.cfg)
					directCfg.Input, directCfg.BaseInput = []*store.SourceArtifact{(*statementInput.cfg).BaseInput, (*statementInput.cfg).Input}[side], []*store.SourceArtifact{(*statementInput.cfg).Input, (*statementInput.cfg).BaseInput}[side]
					directCfg.Path, directCfg.SelfURLs = directCfg.Input.LocalPath, []string{url}
					direct, err := newTestOrchestrator(t, directCfg.Tree, directCfg.Config)
					assert.NoError(t, err)
					result, err := direct.render(t.Context())
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
		return true
	case "equal_labels":
		writeCluster(t, (*statementInput.baseRoot), "A", "same")
		writeCluster(t, (*statementInput.headRoot), "B", "same")
		var err error
		(*statementInput.cfg).BaseInput, _, err = baseline.AdmitDirectory(t.Context(), (*statementInput.baseRoot), (*statementInput.baseRoot))
		assert.NoError(t, err)
		(*statementInput.cfg).Input, _, err = baseline.AdmitDirectory(t.Context(), (*statementInput.headRoot), (*statementInput.headRoot))
		assert.NoError(t, err)
		(*statementInput.cfg).Input.LocalPath, (*statementInput.cfg).Input.LocalRoot = (*statementInput.baseRoot), (*statementInput.baseRoot)
		(*statementInput.headRoot) = (*statementInput.baseRoot)
	case "synthetic_labels":
		for i, root := range []string{(*statementInput.baseRoot), (*statementInput.headRoot)} {
			writeCluster(t, root, []string{"A", "B"}[i], "same")
			files := fstest.MapFS{}
			for _, file := range []string{"kustomization.yaml", "ks-a.yaml", "ks-b.yaml", "apps/app-a/kustomization.yaml", "apps/app-a/cm.yaml", "apps/app-b/kustomization.yaml", "apps/app-b/cm.yaml"} {
				body, err := os.ReadFile(filepath.Join(root, file))
				assert.NoError(t, err)
				files[file] = &fstest.MapFile{Data: body}
			}
			label := filepath.Join(root, ".flate-base", "synthetic")
			input := &store.SourceArtifact{LocalPath: label, LocalRoot: label, FS: files}
			if i == 0 {
				(*statementInput.cfg).BaseInput, (*statementInput.baseRoot) = input, label
			} else {
				(*statementInput.cfg).Input, (*statementInput.headRoot) = input, label
			}
		}
	case "authored_pins", "committed_detection", "equal_selected_pin":
		(*statementInput.headRoot) = (*statementInput.baseRoot)
		repo := testutil.GitInit(t, (*statementInput.headRoot))
		var baseHash plumbing.Hash
		for i, value := range []string{"A", "B", "C"} {
			writeCluster(t, (*statementInput.headRoot), value, "same")
			testutil.WriteFile(
				t,
				(*statementInput.headRoot),
				"kustomization.yaml",
				"resources: [ks-a.yaml, ks-b.yaml, source.yaml]\n",
			)
			body, err := os.ReadFile(filepath.Join((*statementInput.headRoot), "ks-a.yaml"))
			assert.NoError(t, err)
			testutil.WriteFile(
				t,
				(*statementInput.headRoot),
				"ks-a.yaml",
				strings.Replace(
					string(body),
					"name: flux-system, namespace: flux-system",
					"name: cluster, namespace: flux-system",
					1,
				),
			)
			ref := "v1"
			if i == 2 && (*(*statementInput.testCaseInput)) != "equal_selected_pin" {
				ref = "v2"
			}
			testutil.WriteFile(
				t,
				(*statementInput.headRoot),
				"source.yaml",
				"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: flux-system}\nspec: {url: "+url+", ref: {tag: "+ref+"}}\n",
			)
			hash := testutil.GitCommitAll(
				t,
				repo,
				value,
				&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}},
			)
			if i == 0 {
				baseHash = hash
			}
			if i < 2 {
				assert.NoError(
					t,
					repo.Storer.SetReference(
						plumbing.NewHashReference(plumbing.NewTagReferenceName(fmt.Sprintf("v%d", i+1)), hash),
					),
				)
			}
		}
		(*statementInput.cfg).Cache = cachetest.New(t, "", sourcecache.New)
		selected, err := baseline.AutoResolve(t.Context(), (*statementInput.headRoot), baseHash.String(), (*statementInput.cfg).Cache)
		assert.NoError(t, err)
		(*statementInput.cfg).BaseInput, (*statementInput.cfg).Input = selected.BaseInput, selected.Source
		if (*(*statementInput.testCaseInput)) == "authored_pins" || (*(*statementInput.testCaseInput)) == "equal_selected_pin" {
			(*statementInput.cfg).ExternalChanges = selected.Changes
		}
		(*statementInput.baseRoot) = selected.BaseInput.LocalPath
	case "unavailable_host_decoy":
		writeCluster(t, (*statementInput.headRoot), "host-decoy", "same")
		body, err := os.ReadFile(filepath.Join((*statementInput.headRoot), "ks-a.yaml"))
		assert.NoError(t, err)
		testutil.WriteFile(
			t,
			(*statementInput.headRoot),
			"ks-a.yaml",
			strings.Replace(
				string(body),
				"name: flux-system, namespace: flux-system",
				"name: unavailable, namespace: flux-system",
				1,
			),
		)
		files := fstest.MapFS{}
		for _, file := range []string{"kustomization.yaml", "ks-a.yaml", "ks-b.yaml", "apps/app-b/kustomization.yaml", "apps/app-b/cm.yaml"} {
			body, localErr := os.ReadFile(filepath.Join((*statementInput.headRoot), file))
			assert.NoError(t, localErr)
			files[file] = &fstest.MapFile{Data: body}
		}
		(*statementInput.cfg).Path, (*statementInput.cfg).Input = (*statementInput.headRoot), &store.SourceArtifact{
			LocalPath: (*statementInput.headRoot),
			LocalRoot: (*statementInput.headRoot),
			FS:        files,
		}
		(*statementInput.cfg).ExternalChanges = change.NewSet([]string{"ks-a.yaml"})
		o, err := newTestOrchestrator(t, (*statementInput.cfg).Tree, (*statementInput.cfg).Config)
		assert.NoError(t, err)
		t.Cleanup(func() {
			o.stop()
		})
		delete(o.src.Fetchers, manifest.KindGitRepository)
		o.Store().
			SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: (*statementInput.headRoot), FS: os.DirFS((*statementInput.headRoot))})
		assert.NoError(t, o.bootstrap(t.Context()))
		{
			assert.FatalIf(
				t,
				o.Store().GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent) != (*statementInput.cfg).Input,
				"%v",
				"preseeded input replaced supplied input",
			)
		}
		source := &manifest.GitRepository{Name: "unavailable", Namespace: "flux-system"}
		o.Store().AddObject(source)
		o.Store().SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: (*statementInput.headRoot)})
		o.Store().
			UpdateStatus(source.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: "fixture unavailable payload"})
		result, err := o.collectResult(o.run(t.Context()))
		{
			assert.FatalIf(
				t,
				err == nil || result == nil || result.Failed[(*statementInput.changed)].Status != store.StatusFailed,
				"selected producer must fail after discovery: result=%+v err=%v",
				result,
				err,
			)
		}
		if len(outputByParent(result.Manifests)[(*statementInput.changed)]) != 0 ||
			len(outputByParent(result.Manifests)[(*statementInput.same)]) != 0 {
			t.Fatalf("unavailable producer or unchanged sibling emitted: %v", result.Manifests)
		}
		for _, cm := range o.Store().ListObjects("", manifest.KindConfigMap) {
			{
				assert.FatalIf(
					t,
					cm.Named().Name == "cfg-a",
					"%v",
					"unavailable source admitted the host decoy ConfigMap",
				)
			}
		}
		return true
	}
	return false
}
