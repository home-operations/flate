package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func (f *countedProducerFS) Open(name string) (fs.File, error) {
	if name == "kubernetes/components/cluster-settings/cluster-settings.yaml" {
		f.reads.Add(1)
	}
	return f.FS.Open(name)
}

// TestOrchestrator_ChangedOnlyKeepsResourcesEscapeOwner reproduces #833
// end-to-end: a Flux Kustomization's overlay pulls a base in from OUTSIDE
// its own spec.path via a `resources:` escape, and the only edit is to a
// plain resource under that base. The prefix-claim index alone attributes
// the file to no owner; the SelfProduceIndex file attribution must reach
// the change filter through buildChangeFilter so the owning KS is kept.
// Exercises the real wiring (SelfProduceIndex.OwnersOfFile is nil-safe, so
// a nil selfProduce would silently disable the fallback and the unit tests
// with a fake FileOwnerLookup would still pass).
//
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestOrchestrator_ChangedOnlyKeepsResourcesEscapeOwner(t *testing.T) {
	writeTree := func(dir, value string) {
		testutil.WriteFile(
			t,
			dir,
			"clusters/prod/flux-system/apps.yaml",
			testutil.KustomizationYAML(
				"apps",
				"flux-system",
				"  interval: 10m\n  path: ./apps/prod\n  prune: true\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n    namespace: flux-system\n",
			),
		)
		testutil.WriteFile(t, dir, "apps/prod/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../base/postgres
`)
		testutil.WriteFile(t, dir, "apps/base/postgres/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - cluster.yaml
`)
		testutil.WriteFile(t, dir, "apps/base/postgres/cluster.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: postgres
  namespace: default
data:
  replicas: "`+value+`"
`)
	}
	dir, origDir := t.TempDir(), t.TempDir()
	writeTree(origDir, "1")
	writeTree(dir, "2")

	o, err := newTestOrchestrator(
		t,
		Tree{Path: dir, BaseInput: testDirectoryInput(t, origDir)},
		Config{},
	)
	assert.FatalIf(t, err != nil, "New: %v", err)
	t.Cleanup(func() {
		o.stop()
	})
	{
		err := o.bootstrap(context.Background())
		assert.FatalIf(t, err != nil, "Bootstrap: %v", err)
	}
	f := o.filter
	assert.FatalIf(t, f == nil, "%v", "expected non-nil change filter when PathOrig differs from Path")
	apps := testutil.KustomizationID("flux-system", "apps")
	assert.FatalIf(t, !f.ShouldReconcile(apps),
		"KS reading the changed file via a resources: escape must be kept; keep=%v", f.KeepNames())
}

func TestRenderTrees_IndependentInvocations(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	origDir := t.TempDir()
	testutil.WriteFile(
		t,
		dir,
		"ks.yaml",
		testutil.KustomizationYAML(
			"apps",
			"flux-system",
			"  path: ./apps\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n    namespace: flux-system\n",
		),
	)
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: hello
data: {k: v}
`)
	// Baseline with different content forces changed-only mode, which
	// makes buildChangeFilter actually construct a filter we can pin
	// across the two Bootstrap calls.
	testutil.WriteFile(
		t,
		origDir,
		"ks.yaml",
		testutil.KustomizationYAML(
			"apps",
			"flux-system",
			"  path: ./other\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n    namespace: flux-system\n",
		),
	)

	tree := Tree{
		ID:        manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
		Path:      dir,
		BaseInput: testDirectoryInput(t, origDir),
	}
	first, err := RenderTrees(t.Context(), []Tree{tree}, Config{Concurrency: 2})
	assert.NoError(t, err)
	second, err := RenderTrees(t.Context(), []Tree{tree}, Config{Concurrency: 2})
	assert.NoError(t, err)
	assert.FatalIf(t, len(first) != 1 || len(second) != 1, "invocation results: %v %v", first, second)
	for _, run := range []Rendered{first[0], second[0]} {
		assert.FatalIf(t, run.filter == nil, "%v", "changed-only filter missing")
		assert.FatalIf(
			t,
			run.Store().GetObject(testutil.KustomizationID("flux-system", "apps")) == nil,
			"%v",
			"apps Kustomization missing",
		)
	}
	assert.Equal(
		t,
		len(first[0].Store().ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization)),
		len(second[0].Store().ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization)),
	)
	assert.Equal(
		t,
		len(first[0].Store().ListObjects("", manifest.KindConfigMap)),
		len(second[0].Store().ListObjects("", manifest.KindConfigMap)),
	)
	assert.Diff(t, first[0].filter.KeepNames(), second[0].filter.KeepNames())
	assert.Diff(t, canonicalBatches(first[0].Result), canonicalBatches(second[0].Result))
}

func writeDigestTrackingRelease(t *testing.T, root, rel, text string) {
	t.Helper()
	testutil.WriteFile(t, root, rel, `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: flux-instance, namespace: flux-system}
spec:
  suspend: true
  chartRef: {kind: OCIRepository, name: fixture}
  values:
    instance:
      kustomize:
        patches:
          - target: {name: helm-controller}
            patch: "--feature-gates=DisableChartDigestTracking=`+text+`"
`)
}

func writeDigestTrackingTree(t *testing.T, root string) *store.SourceArtifact {
	t.Helper()
	testutil.WriteFile(
		t,
		root,
		"flux/ks.yaml",
		testutil.KustomizationYAML(
			"apps",
			"flux-system",
			"  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n  postBuild:\n    substitute: {GATE: \"true\"}\n",
		),
	)
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [hr.yaml]\n")
	testutil.WriteFile(t, root, "chart/Chart.yaml", "apiVersion: v2\nname: fixture\nversion: 0.1.0\n")
	testutil.WriteFile(t, root, "chart/templates/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: active
  labels:
    helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | quote }}
data:
  marker: {{ .Values.marker | quote }}
`)
	files := map[string]string{}
	for _, file := range []string{"chart/Chart.yaml", "chart/templates/cm.yaml"} {
		body, err := os.ReadFile(filepath.Join(root, file))
		assert.NoError(t, err)
		files[file] = string(body)
	}
	registry := newInlineChartRegistry(t, []map[string]string{files}, false)
	testutil.WriteFile(
		t,
		root,
		"flux/source.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: fixture, namespace: flux-system}\nspec:\n  url: "+registry.url+"\n  insecure: true\n  ref: {tag: 0.1.0}\n  layerSelector: {mediaType: application/vnd.cncf.helm.chart.content.v1.tar+gzip, operation: copy}\n",
	)
	writeDigestTrackingActive(t, root, "stable")
	digest := "sha256:" + strings.Repeat("abcdef0123456789", 4)
	return &store.SourceArtifact{FS: os.DirFS(root), LocalPath: root, Revision: "0.1.0@" + digest, Digest: digest}
}

func writeDigestTrackingActive(t *testing.T, root, marker string) {
	t.Helper()
	testutil.WriteFile(t, root, "flux/active.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: active, namespace: flux-system}
spec:
  chartRef: {kind: OCIRepository, name: fixture}
  values: {marker: `+marker+`}
`)
}

func assertDigestTrackingOutput(t *testing.T, run Rendered, disabled bool, marker string) {
	t.Helper()
	result := run.Result
	assert.FatalIf(t, result == nil, "%v", "render result unavailable")
	docs := outputByParent(result.Manifests)[testutil.HelmReleaseID("flux-system", "active")]
	assert.Equal(t, len(docs), 1)
	want := "fixture-0.1.0"
	if !disabled {
		artifact := run.Store().GetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "fixture"}, store.ArtifactContent).(*store.SourceArtifact)
		want += "_" + strings.TrimPrefix(artifact.Digest, "sha256:")[:12]
	}
	assert.Equal(t, docs[0]["metadata"].(map[string]any)["labels"].(map[string]any)["helm.sh/chart"], any(want))
	assert.Equal(t, docs[0]["data"].(map[string]any)["marker"], any(marker))
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestOrchestrator_BootstrapChartDigestTracking(t *testing.T) {
	for _, covered := range []bool{false, true} {
		for _, disabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("covered_%t/disabled_%t", covered, disabled), func(t *testing.T) {
				root := t.TempDir()
				input := writeDigestTrackingTree(t, root)
				rel := "apps/hr.yaml"
				if !covered {
					rel = "flux/hr.yaml"
					testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: []\n")
				}
				if !covered {
					testutil.WriteFile(
						t,
						root,
						"flux/kustomization.yaml",
						"resources: [ks.yaml, hr.yaml, active.yaml, source.yaml]\n",
					)
				}
				writeDigestTrackingRelease(t, root, rel, "true")
				o, err := newTestOrchestrator(
					t,
					Tree{Path: filepath.Join(root, "flux"), RepoRoot: root, Input: input},
					Config{
						Concurrency: 2,
						HelmOptions: helm.Options{DisableChartDigestTracking: disabled},
					},
				)
				assert.NoError(t, err)
				t.Cleanup(func() { o.stop() })
				assert.NoError(t, o.bootstrap(t.Context()))
				assert.Equal(t, o.hrc.Options.DisableChartDigestTracking, disabled)
				id := testutil.HelmReleaseID("flux-system", "flux-instance")
				assert.Equal(t, o.store.GetObject(id) == nil, true)
				_, indexed := o.index.Lookup(id)
				assert.Equal(t, indexed, true)
				writeDigestTrackingRelease(t, root, rel, "false")
				assert.NoError(t, o.bootstrap(t.Context()))
				assert.Equal(t, o.hrc.Options.DisableChartDigestTracking, disabled)
			})
		}
	}
}

func TestRenderTrees_ChartDigestTrackingFailureRetry(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	input := writeDigestTrackingTree(t, root)
	writeDigestTrackingRelease(t, root, "apps/hr.yaml", "true")
	tree := Tree{
		ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
		Path:     filepath.Join(root, "flux"),
		RepoRoot: root,
		Input:    input,
	}
	tree.BaseInput = &store.SourceArtifact{
		LocalPath: filepath.Join(root, "absent"),
		FS:        os.DirFS(filepath.Join(root, "absent")),
	}
	cfg := Config{CacheDir: t.TempDir(), Concurrency: 2}
	_, err := RenderTrees(t.Context(), []Tree{tree}, cfg)
	assert.FatalIf(t, err == nil, "%v", "expected unavailable baseline failure")
	writeDigestTrackingRelease(t, root, "apps/hr.yaml", "false")
	tree.BaseInput = nil
	runs, err := RenderTrees(t.Context(), []Tree{tree}, cfg)
	assert.NoError(t, err)
	assert.FatalIf(t, len(runs) != 1 || runs[0].Orchestrator == nil, "retry lost render: %v", runs)
	assert.Equal(t, runs[0].hrc.Options.DisableChartDigestTracking, false)
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestOrchestrator_ChartDigestTrackingRenderedBoundary(t *testing.T) {
	for _, producer := range []string{"file", "kustomization", "resourceset"} {
		t.Run(producer, func(t *testing.T) {
			testOrchestratorChartDigestTrackingRenderedBoundaryCase(
				t,
				&producer,
			)
		},
		)
	}
}

func TestRenderTrees_ChartDigestTrackingIndependent(t *testing.T) {
	t.Parallel()

	baseRoot, headRoot := t.TempDir(), t.TempDir()
	baseInput := writeDigestTrackingTree(t, baseRoot)
	headInput := writeDigestTrackingTree(t, headRoot)
	writeDigestTrackingRelease(t, baseRoot, "apps/hr.yaml", "false")
	writeDigestTrackingRelease(t, headRoot, "apps/hr.yaml", "true")
	writeDigestTrackingActive(t, baseRoot, "base")
	writeDigestTrackingActive(t, headRoot, "head")
	owner := cachetest.New(t, "", sourcecache.New)
	for _, disabled := range []bool{false, true} {
		var previous []byte
		for range 2 {
			renderedTrees1, err := RenderTrees(
				t.Context(),
				[]Tree{
					{
						Path:      filepath.Join(baseRoot, "flux"),
						RepoRoot:  baseRoot,
						ID:        manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
						BaseInput: headInput,
						Input:     baseInput,
					},
					{
						Path:      filepath.Join(headRoot, "flux"),
						RepoRoot:  headRoot,
						ID:        manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
						Input:     headInput,
						BaseInput: baseInput,
					},
				},
				Config{
					Cache: owner, Concurrency: 2,
					HelmOptions: helm.Options{DisableChartDigestTracking: disabled},
				},
			)
			base, head := renderedTrees1[0], renderedTrees1[1]
			assert.NoError(t, err)
			assertDigestTrackingOutput(t, base, disabled, "base")
			assertDigestTrackingOutput(t, head, disabled, "head")
			output, err := json.Marshal([][]manifest.DocumentBatch{base.Result.Manifests, head.Result.Manifests})
			assert.NoError(t, err)
			if previous != nil {
				assert.Equal(t, string(output), string(previous))
			}
			previous = output
		}
	}
}

func TestRenderTrees_InjectedCacheRemainsCallerOwned(t *testing.T) {
	t.Parallel()

	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			t.Parallel()

			root := ""
			if backing == "disk" {
				root = t.TempDir()
			}
			c, err := sourcecache.New(t.Context(), root)
			assert.FatalIf(t, err != nil, "%v", err)
			t.Cleanup(func() { _ = c.Close() })
			calls := 0
			key := sourcecache.BlobKey{Content: "blob-owner-test"}
			produce := func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
				calls++
				_, localErr := io.WriteString(w, "held")
				return sourcecache.ProducerResult{}, localErr
			}
			_, _, _, err = c.Blob(t.Context(), key, sourcecache.Policy{MaxAge: time.Hour}, produce)
			assert.FatalIf(t, err != nil, "%v", err)
			baseDir, headDir := t.TempDir(), t.TempDir()
			for _, dir := range []string{baseDir, headDir} {
				writeSubstitutionCluster(t, dir, "values", "value")
				testutil.WriteFile(t, dir, "kustomization.yaml", "resources: [flux.yaml, values.yaml]\n")
				testutil.WriteFile(
					t,
					dir,
					"values.yaml",
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: values, namespace: apps}\ndata: {VALUE: stable}\n",
				)
			}
			configuredRoot := t.TempDir()
			renderedTrees2, err := RenderTrees(
				t.Context(),
				[]Tree{
					{RepoRoot: baseDir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}},
					{RepoRoot: headDir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}},
				},
				Config{Cache: c, CacheDir: configuredRoot, Concurrency: 2},
			)
			base, head := renderedTrees2[0], renderedTrees2[1]
			assert.FatalIf(t, err != nil, "%v", err)
			assert.FatalIf(t, base.cache != c || head.cache != c || base.hrc.Helm == nil || head.hrc.Helm == nil ||
				base.ksc.Trees == nil ||
				head.ksc.Trees == nil, "%v", "diff sides did not wire both renderers through the selected owner")
			entries, err := os.ReadDir(configuredRoot)
			assert.FatalIf(
				t,
				err != nil || len(entries) != 0,
				"diff sides created independent cache backing: entries=%v err=%v",
				entries,
				err,
			)
			reader, _, _, err := c.Blob(t.Context(), key, sourcecache.Policy{MaxAge: time.Hour}, produce)
			assert.FatalIf(t, err != nil, "%v", err)
			data, err := fs.ReadFile(reader, "content")
			assert.FatalIf(
				t,
				err != nil || string(data) != "held" || calls != 1,
				"caller cache after both sides stop: data=%q calls=%d err=%v",
				data,
				calls,
				err,
			)
			assert.NoError(t, os.Remove(filepath.Join(baseDir, "app", "cm.yaml")))
			renderedTrees3, err := RenderTrees(
				t.Context(),
				[]Tree{
					{RepoRoot: baseDir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}},
					{RepoRoot: headDir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}},
				},
				Config{Cache: c, Concurrency: 2},
			)
			base, head = renderedTrees3[0], renderedTrees3[1]
			{
				_, ok := errors.AsType[*FailuresError](
					err,
				)
				assert.FatalIf(
					t,
					!ok || base.Result == nil || len(base.Result.Failed) == 0 || head.Result == nil || len(head.Result.Manifests) == 0 ||
						head.Err != nil,
					"resource failure canceled healthy sibling: base=%+v head=%+v err=%v",
					base,
					head,
					err,
				)
			}
			for _, side := range []Rendered{base, head} {
				{
					_, innerLocalErr := side.inputs.Acquire(
						t.Context(),
						"://closed",
						sourcecache.RetryConfig{},
						nil,
					)
					assert.FatalIf(t, !errors.Is(
						innerLocalErr,
						fs.ErrClosed,
					), "partial side left TreeCache open: %v", innerLocalErr)
				}
			}
			_, _, _, err = c.Blob(t.Context(), key, sourcecache.Policy{MaxAge: time.Hour}, nil)
			assert.NoError(t, err)
		})
	}
}

func testOrchestratorChartDigestTrackingRenderedBoundaryCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	root := t.TempDir()
	input := writeDigestTrackingTree(t, root)
	switch *testCaseInput {
	case "file":
		writeDigestTrackingRelease(t, root, "apps/hr.yaml", "true")
	case "kustomization":
		writeDigestTrackingRelease(t, root, "apps/hr.yaml", "${GATE}")
	default:
		testutil.WriteFile(t, root, "apps/hr.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: instance, namespace: flux-system}
spec:
  inputs: [{gate: "true"}]
  resourcesTemplate: |
    apiVersion: helm.toolkit.fluxcd.io/v2
    kind: HelmRelease
    metadata: {name: flux-instance, namespace: flux-system}
    spec:
      suspend: true
      chartRef: {kind: OCIRepository, name: fixture}
      values:
        instance:
          kustomize:
            patches:
              - patch: "DisableChartDigestTracking=<< inputs.gate >>"
`)
	}
	owner := cachetest.New(t, "", sourcecache.New)
	for _, disabled := range []bool{false, true} {
		var previous []byte
		for range 2 {
			o, err := newTestOrchestrator(
				t,
				Tree{Path: filepath.Join(root, "flux"), RepoRoot: root, Input: input},
				Config{
					Cache: owner, Concurrency: 2,
					HelmOptions: helm.Options{DisableChartDigestTracking: disabled},
				},
			)
			assert.NoError(t, err)
			t.Cleanup(func() { o.stop() })
			assert.NoError(t, o.bootstrap(t.Context()))
			assert.Equal(t, o.hrc.Options.DisableChartDigestTracking, disabled)
			res, err := o.render(t.Context())
			assert.NoError(t, err)
			assertDigestTrackingOutput(t, Rendered{Orchestrator: o, Result: res}, disabled, "stable")
			hr, ok := o.store.Get[*manifest.HelmRelease](testutil.HelmReleaseID("flux-system", "flux-instance"))
			assert.FatalIf(t, !ok, "%v", "matching rendered release missing")
			instance := hr.Values["instance"].(map[string]any)
			patches := instance["kustomize"].(map[string]any)["patches"].([]any)
			assert.Equal(
				t,
				strings.Contains(
					patches[0].(map[string]any)["patch"].(string),
					"DisableChartDigestTracking=true",
				),
				true,
			)
			assert.Equal(t, o.hrc.Options.DisableChartDigestTracking, disabled)
			output, err := json.Marshal(res.Manifests)
			assert.NoError(t, err)
			if previous != nil {
				assert.Equal(t, string(output), string(previous))
			}
			previous = output
		}
	}
}
