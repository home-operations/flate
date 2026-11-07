package orchestrator

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
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
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

type testRenderTreesAdmittedViewsCaseSegmentInputs struct {
	testCaseInput **string
	baseRoot      *string
	headRoot      *string
	cfg           *treeConfig
	changed       *manifest.NamedResource
	same          *manifest.NamedResource
}

func testRenderTreesAdmittedViewsCaseSegment(
	t *testing.T,
	statementInput *testRenderTreesAdmittedViewsCaseSegmentInputs,
) bool {
	t.Helper()
	switch *(*statementInput.testCaseInput) {
	case "scoped_peer":
		scopedPeerAdmittedViews(t)
		return true
	case "equal_selected_alias", "selected_payload_only", "source_declaration_equal":
		selectedAliasAdmittedViews(t, statementInput)
		return true
	case "equal_labels":
		writeCluster(t, (*statementInput.baseRoot), "A", "same")
		writeCluster(t, (*statementInput.headRoot), "B", "same")
		var err error
		statementInput.cfg.BaseInput, _, err = baseline.AdmitDirectory(
			t.Context(),
			(*statementInput.baseRoot),
			(*statementInput.baseRoot), manifest.WorkScope{},
		)
		assert.NoError(t, err)
		statementInput.cfg.Input, _, err = baseline.AdmitDirectory(
			t.Context(),
			(*statementInput.headRoot),
			(*statementInput.headRoot), manifest.WorkScope{},
		)
		assert.NoError(t, err)
		statementInput.cfg.Input.LocalPath, statementInput.cfg.Input.LocalRoot = (*statementInput.baseRoot), (*statementInput.baseRoot)
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
				statementInput.cfg.BaseInput, (*statementInput.baseRoot) = input, label
			} else {
				statementInput.cfg.Input, (*statementInput.headRoot) = input, label
			}
		}
	case "authored_pins", "committed_detection", "equal_selected_pin":
		pinnedSourceAdmittedViews(t, statementInput)
	case "unavailable_host_decoy":
		unavailableSourceAdmittedViews(t, statementInput)
		return true
	}
	return false
}

func scopedPeerAdmittedViews(t *testing.T) {
	t.Helper()
	for _, missing := range []bool{false, true} {
		for _, synthetic := range []bool{false, true} {
			t.Run(fmt.Sprintf("missing_%t/synthetic_%t", missing, synthetic), func(t *testing.T) {
				t.Parallel()
				testRenderTreesAdmittedViewsCaseCase(
					t,
					testRenderTreesAdmittedViewsCaseCaseInputs{missing: &missing, synthetic: &synthetic},
				)
			})
		}
	}
}

func selectedAliasAdmittedViews(t *testing.T, statementInput *testRenderTreesAdmittedViewsCaseSegmentInputs) {
	t.Helper()
	old := selectedAliasRepository(t, statementInput)
	statementInput.cfg.Cache = cachetest.New(t, "", sourcecache.New)
	selected, err := baseline.AutoResolve(
		t.Context(),
		(*statementInput.headRoot),
		old.String(),
		statementInput.cfg.Cache,
	)
	assert.NoError(t, err)
	statementInput.cfg.Input, statementInput.cfg.BaseInput = selected.Source, selected.BaseInput
	statementInput.cfg.ExternalChanges = selected.Changes
	if (*(*statementInput.testCaseInput)) == "selected_payload_only" {
		statementInput.cfg.ExternalChanges = change.NewSet(nil)
	}
	(*statementInput.baseRoot) = statementInput.cfg.BaseInput.LocalPath
	var previous [2]map[manifest.NamedResource][]map[string]any
	for run := range 2 {
		renderedTrees2, err := renderTreesForTest(t.Context(), t,
			[]Tree{
				{
					Path:            (*statementInput.baseRoot),
					SelfURLs:        []string{admittedViewsURL},
					ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
					Input:           statementInput.cfg.BaseInput,
					BaseInput:       statementInput.cfg.Input,
					ExternalChanges: statementInput.cfg.ExternalChanges,
				},
				{
					Path:            (*statementInput.headRoot),
					SelfURLs:        []string{admittedViewsURL},
					ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
					ExternalChanges: statementInput.cfg.ExternalChanges,
					Input:           statementInput.cfg.Input,
					BaseInput:       statementInput.cfg.BaseInput,
				},
			},
			statementInput.cfg.Config,
		)
		base, head := renderedTrees2[0], renderedTrees2[1]
		assert.NoError(t, err)
		source := (&manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}).Named()
		left := base.Store().GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
		right := head.Store().GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
		payload, err := change.Detect(t.Context(), left, right, manifest.WorkScope{})
		assert.NoError(t, err)
		if (*(*statementInput.testCaseInput)) != "selected_payload_only" {
			assert.Equal(t, payload.Len(), 0)
			assert.Equal(t, left.InputAlias, false)
			assert.Equal(t, right.InputAlias, true)
		}
		for side, rendered := range []Rendered{base, head} {
			assertSelectedAliasConfigMaps(t, statementInput, &rendered)
			assertSelectedAliasDocuments(t, statementInput, &rendered, side)
			if (*(*statementInput.testCaseInput)) == "selected_payload_only" {
				{
					warningValues := rendered.Result.Warnings
					for warningIndex := range warningValues {
						warning := warningValues[warningIndex]
						assert.FatalIf(t, strings.Contains(warning.Message, "output will be empty"), "%v", warning)
					}
				}
			}
			if run == 0 && (*(*statementInput.testCaseInput)) == "source_declaration_equal" {
				directCfg := (*statementInput.cfg)
				directCfg.Input, directCfg.BaseInput = []*store.SourceArtifact{statementInput.cfg.BaseInput, statementInput.cfg.Input}[side], []*store.SourceArtifact{statementInput.cfg.Input, statementInput.cfg.BaseInput}[side]
				directCfg.Path, directCfg.SelfURLs = directCfg.Input.LocalPath, []string{admittedViewsURL}
				direct, err := newTestOrchestrator(t, directCfg.Tree, directCfg.Config)
				assert.NoError(t, err)
				result, err := direct.render(t.Context(), t)
				assert.NoError(t, err)
				assert.Diff(t, outputByParent(result.Manifests), outputByParent(rendered.Result.Manifests))
			}
			if run == 1 {
				assert.Diff(t, outputByParent(rendered.Result.Manifests), previous[side])
			}
			previous[side] = outputByParent(rendered.Result.Manifests)
			logSelectedAliasOutput(t, &rendered, run, side)
		}
	}
}

func pinnedSourceAdmittedViews(t *testing.T, statementInput *testRenderTreesAdmittedViewsCaseSegmentInputs) {
	t.Helper()
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
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: flux-system}\nspec: {url: "+admittedViewsURL+", ref: {tag: "+ref+"}}\n",
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
	statementInput.cfg.Cache = cachetest.New(t, "", sourcecache.New)
	selected, err := baseline.AutoResolve(
		t.Context(),
		(*statementInput.headRoot),
		baseHash.String(),
		statementInput.cfg.Cache,
	)
	assert.NoError(t, err)
	statementInput.cfg.BaseInput, statementInput.cfg.Input = selected.BaseInput, selected.Source
	if (*(*statementInput.testCaseInput)) == "authored_pins" ||
		(*(*statementInput.testCaseInput)) == "equal_selected_pin" {
		statementInput.cfg.ExternalChanges = selected.Changes
	}
	(*statementInput.baseRoot) = selected.BaseInput.LocalPath
}

func unavailableSourceAdmittedViews(t *testing.T, statementInput *testRenderTreesAdmittedViewsCaseSegmentInputs) {
	t.Helper()
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
	statementInput.cfg.Path, statementInput.cfg.Input = (*statementInput.headRoot), &store.SourceArtifact{
		LocalPath: (*statementInput.headRoot),
		LocalRoot: (*statementInput.headRoot),
		FS:        files,
	}
	statementInput.cfg.ExternalChanges = change.NewSet([]string{"ks-a.yaml"})
	o, err := newTestOrchestrator(t, statementInput.cfg.Tree, statementInput.cfg.Config)
	assert.NoError(t, err)
	t.Cleanup(func() {
		o.stop()
	})
	delete(o.src.Fetchers, manifest.KindGitRepository)
	o.Store().
		SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: (*statementInput.headRoot), FS: os.DirFS((*statementInput.headRoot))})
	assert.NoError(t, o.bootstrap(t.Context()))
	assert.FatalIf(
		t,
		o.Store().GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent) != statementInput.cfg.Input,
		"%v",
		"preseeded input replaced supplied input",
	)
	source := &manifest.GitRepository{Name: "unavailable", Namespace: "flux-system"}
	o.Store().AddObject(source)
	o.Store().SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: (*statementInput.headRoot)})
	o.Store().
		UpdateStatus(source.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: "fixture unavailable payload"})
	result, err := o.collectResult(o.run(t.Context()))
	t.Cleanup(result.Close)
	assert.FatalIf(
		t,
		err == nil || result == nil || result.Failed[(*statementInput.changed)].Status != store.StatusFailed,
		"selected producer must fail after discovery: result=%+v err=%v",
		result,
		err,
	)
	if len(outputByParent(result.Manifests)[(*statementInput.changed)]) != 0 ||
		len(outputByParent(result.Manifests)[(*statementInput.same)]) != 0 {
		t.Fatalf("unavailable producer or unchanged sibling emitted: %v", result.Manifests)
	}
	for _, cm := range o.Store().ListObjects("", manifest.KindConfigMap) {
		assert.FatalIf(t, cm.Named().Name == "cfg-a", "%v", "unavailable source admitted the host decoy ConfigMap")
	}
}

const admittedViewsURL = "git://fixture.invalid/cluster"

func selectedAliasRepository(
	t *testing.T,
	statementInput *testRenderTreesAdmittedViewsCaseSegmentInputs,
) plumbing.Hash {
	t.Helper()
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
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: flux-system}\nspec: {url: "+admittedViewsURL+", interval: "+interval+ref+"}\n",
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
				repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewBranchReferenceName("selected"), hash)),
			)
		}
	}
	return old
}

func logSelectedAliasOutput(t *testing.T, renderedValue *Rendered, run, side int) {
	t.Helper()
	rendered := *renderedValue
	logged := map[string][]map[string]any{}
	for id, docs := range outputByParent(rendered.Result.Manifests) {
		logged[id.String()] = docs
	}
	bytes, err := json.Marshal(logged)
	assert.NoError(t, err)
	t.Logf("run=%d side=%d exact selected output=%s", run, side, bytes)
}

func assertSelectedAliasConfigMaps(
	t *testing.T,
	statementInput *testRenderTreesAdmittedViewsCaseSegmentInputs,
	renderedValue *Rendered,
) {
	t.Helper()
	rendered := *renderedValue
	want := map[string]bool{}
	if (*(*statementInput.testCaseInput)) != "equal_selected_alias" {
		want["cfg-a"] = true
	}
	if (*(*statementInput.testCaseInput)) == "source_declaration_equal" {
		want["cfg-b"] = true
	}
	assert.Diff(t, configMapNames(rendered.Result), want)
}

func assertSelectedAliasDocuments(
	t *testing.T,
	statementInput *testRenderTreesAdmittedViewsCaseSegmentInputs,
	renderedValue *Rendered,
	side int,
) {
	t.Helper()
	rendered := *renderedValue
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
			metadata := manifest.DocMetadata(doc)
			name := metadata.Name
			value := selectedAliasValue(name, side, statementInput)
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
}

func selectedAliasValue(name string, side int, statementInput *testRenderTreesAdmittedViewsCaseSegmentInputs) string {
	value := "same"
	if name == "cfg-a" {
		value = "B"
		if (*(*statementInput.testCaseInput)) == "selected_payload_only" && side == 0 {
			value = "A"
		}
	}
	return value
}
