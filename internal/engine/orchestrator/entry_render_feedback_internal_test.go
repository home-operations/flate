package orchestrator

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/testfixture"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRenderTrees_EntryRenderFeedback(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, edit, marker string
		renders            uint64
		policyOnly         bool
	}{
		{name: "omission", edit: "edit-omit", marker: "before", renders: 1},
		{name: "convergence", marker: "after", renders: 2},
		{name: "longer chain", edit: "edit-converge", marker: "after", renders: 3},
		{name: "cycle", edit: "edit-cycle", marker: "middle", renders: 2},
		{name: "policy only", marker: "before", renders: 1, policyOnly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := testfixture.Materialize(t, "flux-instance-aggregate-values", "generator-values")
			step := slices.IndexFunc(fixture.Plan.Steps, func(step testfixture.Step) bool { return step.ID == tc.edit })
			if step >= 0 {
				fixture.Edit(t, fixture.Plan.Steps[step])
			}
			if tc.policyOnly {
				const template = "charts/bootstrap/templates/instance.yaml"
				original, err := os.ReadFile(filepath.Join(fixture.Repo, template))
				assert.NoError(t, err)
				policy := strings.Replace(string(original),
					`- op: replace
                    path: /spec/values/marker
                    value: {{ default "after" .Values.result | quote }}`,
					`- op: add
                    path: /spec/upgrade
                    value: {remediation: {retries: 3}}`, 1)
				assert.FatalIf(t, policy == string(original), "missing feedback patch")
				testutil.WriteFile(t, fixture.Repo, template, policy)
			}
			const path = "kubernetes/apps"
			runs, err := renderTreesForTest(t.Context(), t, []Tree{{
				ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
				RepoRoot: fixture.Repo,
				Path:     filepath.Join(fixture.Repo, path),
				SelfURLs: []string{"https://example.invalid/fixture"},
			}}, Config{Concurrency: 2, RecordWork: true})
			assert.Equal(t, err != nil, tc.edit == "edit-cycle")
			assert.FatalIf(t, len(runs) != 1 || runs[0].Result == nil, "missing render result: %v", runs)
			run := &runs[0]
			release := testutil.HelmReleaseID("flux-system", "flux-instance")
			assert.Equal(t, workCounts(run.Result.Work, release, manifest.WorkHelm).Attempts, tc.renders)
			assert.Equal(t, run.Err != nil, tc.edit == "edit-cycle")
			if tc.policyOnly {
				effective, ok := run.store.GetDeclaration(release).(*manifest.HelmRelease)
				assert.FatalIf(t, !ok || effective.Upgrade == nil || effective.Upgrade.Remediation == nil,
					"missing patched generator policy")
				assert.Equal(t, effective.Upgrade.Remediation.Retries, 3)
			}
			entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, path, manifest.NamedResource{}).Named()
			assert.Equal(t, run.entryBinding.Key, entry)
			assert.Equal(t, run.entryBinding.Producer == entry, tc.edit == "edit-omit")
			docs := outputByParent(run.Result.Manifests)[release]
			for _, doc := range docs {
				if doc["kind"] != manifest.KindConfigMap {
					continue
				}
				data, _ := doc["data"].(map[string]any)
				assert.Equal(t, data["marker"], any(tc.marker))
				return
			}
			t.Fatal("missing marker output")
		})
	}
}

func TestRenderTrees_EntryQualificationLatePrivateDependencies(t *testing.T) {
	t.Parallel()
	fixture := testfixture.Materialize(t, "flux-instance-aggregate-values", "generator-values")
	root := fixture.Repo
	const entry = "kubernetes/apps"
	const ns = entry + "/ns/"
	testutil.WriteFile(t, root, ns+"kustomization.yaml", "resources: [source.yaml, values.yaml, generator.yaml]\n")
	assert.NoError(t, os.Remove(filepath.Join(root, ns+"instance-release.yaml")))
	testutil.WriteFile(t, root, ns+"values.yaml", testutil.KustomizationYAML("values", "flux-system", `  path: ./values
  sourceRef: {kind: GitRepository, name: bootstrap}
`))
	testutil.WriteFile(
		t,
		root,
		ns+"generator.yaml",
		testutil.KustomizationYAML("generator", "flux-system", `  path: ./generator
  sourceRef: {kind: GitRepository, name: bootstrap}
  dependsOn: [{name: values}]
`),
	)
	testutil.WriteFile(t, root, "values/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "values/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: feedback-values, namespace: flux-system}
data: {values.yaml: 'marker: from-prerequisite'}
`)
	testutil.WriteFile(t, root, "generator/kustomization.yaml", "resources: [release.yaml]\n")
	testutil.WriteFile(t, root, "generator/release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: flux-instance, namespace: flux-system}
spec:
  interval: 10m
  chart:
    spec: {chart: charts/bootstrap, sourceRef: {kind: GitRepository, name: bootstrap}}
  valuesFrom: [{kind: ConfigMap, name: feedback-values}]
  values: {area: kubernetes, directory: apps}
`)
	runs, err := renderTreesForTest(t.Context(), t, []Tree{{
		ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
		RepoRoot: root,
		Path:     filepath.Join(root, entry),
		SelfURLs: []string{"https://example.invalid/fixture"},
	}}, Config{Concurrency: 2, RecordWork: true})
	assert.NoError(t, err)
	assert.FatalIf(t, len(runs) != 1 || runs[0].Result == nil, "missing render result: %v", runs)
	run := &runs[0]
	assert.Equal(t, run.entryBinding.Producer, testutil.KustomizationID("flux-system", "sync"))
	releaseID := testutil.HelmReleaseID("flux-system", "flux-instance")
	docs := outputByParent(run.Result.Manifests)[releaseID]
	for _, doc := range docs {
		if manifest.DocKind(doc) == manifest.KindConfigMap {
			data, _ := doc["data"].(map[string]any)
			assert.Equal(t, data["marker"], any("from-prerequisite"))
			return
		}
	}
	t.Fatal("missing marker output")
}

func TestRenderTrees_GeneratedRootCoverageDiff(t *testing.T) {
	t.Parallel()
	const entry = "kubernetes/apps/media"
	const rootTemplate = "charts/bootstrap/templates/instance.yaml"
	const plexTemplate = "charts/plex/templates/config.yaml"
	const siblingValue = "kubernetes/apps/outside/workload/value.yaml"
	const self = "https://example.invalid/fixture"
	for _, tc := range []struct {
		name  string
		order [2]int
	}{
		{name: "forward", order: [2]int{0, 1}},
		{name: "reverse", order: [2]int{1, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var trees [2]Tree
			for i, value := range []string{"before", "after"} {
				fixture := testfixture.Materialize(t, "flux-instance-root", "descendant")
				fixture.Edit(t, fixture.Plan.Steps[0])
				if i == 1 {
					template, err := os.ReadFile(filepath.Join(fixture.Repo, rootTemplate))
					assert.NoError(t, err)
					testutil.WriteFile(
						t,
						fixture.Repo,
						rootTemplate,
						strings.Replace(string(template), "retries: 2", "retries: 3", 1),
					)
				}
				source := testDirectoryInput(t, fixture.Repo)
				testutil.WriteFile(t, fixture.Repo, plexTemplate, `apiVersion: v1
kind: ConfigMap
metadata: {name: plex, namespace: media}
data: {value: `+value+"}\n")
				testutil.WriteFile(t, fixture.Repo, siblingValue, `apiVersion: v1
kind: ConfigMap
metadata: {name: outside, namespace: media}
data: {value: `+value+"}\n")
				trees[i] = Tree{
					RepoRoot: fixture.Repo,
					Path:     filepath.Join(fixture.Repo, entry),
					Input:    source,
					SelfURLs: []string{self},
				}
			}
			var pair [2]Tree
			for side, version := range tc.order {
				pair[side] = trees[version]
				pair[side].ID = manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)}
				pair[side].BaseInput = trees[1-version].Input
				pair[side].ExternalChanges = detectChanges(
					t,
					[]string{rootTemplate, plexTemplate, siblingValue},
					[]string{rootTemplate, plexTemplate, siblingValue},
				)
			}
			runs, err := renderTreesForTest(t.Context(), t, pair[:], Config{
				Concurrency: 2, Selection: selection.Request{Changed: true}, RecordWork: true,
			})
			assert.NoError(t, err)
			assert.FatalIf(t, len(runs) != 2, "missing comparison result: %v", runs)
			key := manifest.NewEntryKustomization(manifest.BootstrapSourceID, entry, manifest.NamedResource{}).Named()
			for side, version := range tc.order {
				run := &runs[side]
				assert.Equal(t, run.entryBinding.Key, key)
				assert.Equal(t, run.entryBinding.Producer, testutil.KustomizationID("flux-system", "sync"))
				assert.Equal(t, run.entryBinding.Descendant, entry)
				entryProjection := run.entryBinding.Projection()
				_, listed := entryProjection.Documents[testutil.KustomizationID("media", "plex")]
				assert.Equal(t, listed, true)
				assert.Diff(t, configMapNames(run.Result), map[string]bool{"plex": true})
				plex := testutil.HelmReleaseID("media", "plex")
				effective, ok := run.store.GetDeclaration(plex).(*manifest.HelmRelease)
				assert.FatalIf(
					t,
					!ok || effective.Upgrade == nil || effective.Upgrade.Remediation == nil,
					"missing patched Plex policy",
				)
				assert.Equal(t, effective.Upgrade.Remediation.Retries, 2+version)
				docs := outputByParent(run.Result.Manifests)[plex]
				assert.FatalIf(t, len(docs) != 1, "missing Plex emission: %v", docs)
				assert.Diff(t, docs[0]["data"], map[string]any{"value": []string{"before", "after"}[version]})
			}
		})
		t.Run("sync overlap "+tc.name, func(t *testing.T) {
			t.Parallel()
			testGeneratedRootOverlapDiff(t, tc.order, self)
		})
	}
}

func TestRenderTrees_GeneratedRootIdentityScope(t *testing.T) {
	t.Parallel()
	const apps = "kubernetes/apps"
	const media = apps + "/media"
	fixture := testfixture.Materialize(t, "flux-instance-scoped-workloads", "workloads")
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: remote}\ndata: {from: remote}\n"))
	}))
	t.Cleanup(remote.Close)
	testutil.WriteFile(t, fixture.Repo, apps+"/kustomization.yaml", `resources: [media, sibling]
configMapGenerator:
- name: hashed
  behavior: merge
  literals: [FROM=root]
patches:
- target: {kind: ConfigMap, name: relocated}
  patch: |-
    - op: replace
      path: /metadata/namespace
      value: relocated-by-root
- target: {kind: ConfigMap, name: renamed}
  patch: |-
    - op: replace
      path: /metadata/name
      value: renamed-by-root
- target: {kind: ConfigMap, name: collision}
  patch: |-
    apiVersion: v1
    kind: ConfigMap
    metadata: {name: collision}
    $patch: delete
- target: {kind: ConfigMap, name: sibling-collision}
  patch: |-
    - op: replace
      path: /metadata/name
      value: collision
    - op: copy
      from: /metadata/annotations
      path: /data
- target:
    kind: ConfigMap
    name: shared
    annotationSelector: '!config.kubernetes.io/origin'
  patch: |-
    - op: replace
      path: /data/from
      value: patched-by-root
- target: {kind: ConfigMap, name: sibling}
  patch: |-
    apiVersion: v1
    kind: ConfigMap
    metadata:
      name: sibling
      annotations:
        config.kubernetes.io/origin: 'path: media/local.yaml'
`)
	testutil.WriteFile(t, fixture.Repo, media+"/kustomization.yaml", `resources:
- flux-instance.yaml
- local.yaml
- ../shared.yaml
- `+remote.URL+`/remote.yaml
configMapGenerator:
- name: hashed
  literals: [FROM=media]
`)
	testutil.WriteFile(t, fixture.Repo, media+"/local.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: collision}
data: {from: media}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: renamed}
data: {from: media}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: relocated, namespace: media}
data: {from: media}
`)
	testutil.WriteFile(t, fixture.Repo, apps+"/shared.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: shared}
data: {from: shared}
`)
	testutil.WriteFile(t, fixture.Repo, apps+"/sibling/kustomization.yaml", "resources: [config.yaml]\n")
	testutil.WriteFile(t, fixture.Repo, apps+"/sibling/config.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: sibling-collision
  annotations: {from: sibling}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: sibling}
data: {from: sibling}
`)
	input := Tree{
		ID:       manifest.TreeID{Cluster: "."},
		RepoRoot: fixture.Repo,
		Path:     filepath.Join(fixture.Repo, media),
		Input:    testDirectoryInput(t, fixture.Repo),
		SelfURLs: []string{"https://github.com/example/fleet.git"},
	}
	runs, err := renderTreesForTest(t.Context(), t, []Tree{input}, Config{Concurrency: 2})
	assert.NoError(t, err)
	assert.FatalIf(t, len(runs) != 1 || runs[0].Result == nil, "missing render result: %v", runs)
	run := &runs[0]
	root := testutil.KustomizationID("flux-system", "flux-system")
	assert.Equal(t, run.entryBinding.Producer, root)
	assert.Equal(t, run.entryBinding.Descendant, media)
	data := make(map[string]any)
	for _, doc := range outputByParent(run.Result.Manifests)[root] {
		if manifest.DocKind(doc) == manifest.KindConfigMap {
			data[manifest.DocIdentity(doc).Name] = doc["data"]
		}
	}
	assert.Diff(t, data, map[string]any{
		"collision": map[string]any{
			"from": "sibling",
			"internal.config.kubernetes.io/previousKinds":      "ConfigMap",
			"internal.config.kubernetes.io/previousNames":      "sibling-collision",
			"internal.config.kubernetes.io/previousNamespaces": "default",
		},
		"shared": map[string]any{"from": "patched-by-root"},
		"remote": map[string]any{"from": "remote"},
	})
	input.Path = filepath.Join(fixture.Repo, apps)
	full, err := renderTreesForTest(t.Context(), t, []Tree{input}, Config{Concurrency: 2})
	assert.NoError(t, err)
	assert.FatalIf(t, len(full) != 1 || full[0].Result == nil, "missing equal-path result: %v", full)
	equal := make(map[string]any)
	for _, doc := range outputByParent(full[0].Result.Manifests)[root] {
		id := manifest.DocIdentity(doc)
		switch {
		case id.Name == "relocated":
			equal["namespace"] = id.Namespace
		case strings.HasPrefix(id.Name, "hashed-"):
			equal["generated"] = doc["data"]
		}
	}
	assert.Diff(t, equal, map[string]any{
		"namespace": "relocated-by-root",
		"generated": map[string]any{"FROM": "root"},
	})
}

func TestReview_GeneratedRootRepairsStandaloneDocument(t *testing.T) {
	t.Parallel()
	const apps = "kubernetes/apps"
	const media = apps + "/media"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: repaired, namespace: media}
spec: placeholder
`))
	}))
	t.Cleanup(server.Close)
	for _, tc := range []struct {
		name, path, resources string
		literal               bool
	}{
		{name: "equal", path: apps, resources: "flux-instance.yaml, "},
		{name: "descendant", path: media, resources: "flux-instance.yaml, "},
		{name: "literal", path: media, literal: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := testfixture.Materialize(t, "flux-instance-scoped-workloads", "workloads")
			testutil.WriteFile(t, fixture.Repo, media+"/kustomization.yaml",
				"resources: ["+tc.resources+server.URL+"/release.yaml]\n",
			)
			testutil.WriteFile(t, fixture.Repo, apps+"/kustomization.yaml", `resources: [media]
patches:
- target: {kind: HelmRelease, name: repaired}
  patch: |-
    - op: replace
      path: /spec
      value:
        suspend: true
        chartRef: {kind: OCIRepository, name: unused}
`)
			runs, err := renderTreesForTest(t.Context(), t, []Tree{{
				ID:       manifest.TreeID{Cluster: "."},
				RepoRoot: fixture.Repo,
				Path:     filepath.Join(fixture.Repo, tc.path),
				Input:    testDirectoryInput(t, fixture.Repo),
				SelfURLs: []string{"https://github.com/example/fleet.git"},
			}}, Config{Concurrency: 2})
			if tc.literal {
				assert.FatalIf(t, err == nil || !strings.Contains(err.Error(), "HelmRelease decode:"),
					"literal package lost its document error: %v", err)
				return
			}
			assert.NoError(t, err)
			assert.FatalIf(t, len(runs) != 1 || runs[0].Result == nil, "missing render result: %v", runs)
			root := testutil.KustomizationID("flux-system", "flux-system")
			assert.Equal(t, runs[0].entryBinding.Producer, root)
			for _, doc := range outputByParent(runs[0].Result.Manifests)[root] {
				if manifest.DocIdentity(doc) == testutil.HelmReleaseID("media", "repaired") {
					assert.Diff(t, doc["spec"], map[string]any{
						"suspend":  true,
						"chartRef": map[string]any{"kind": "OCIRepository", "name": "unused"},
					})
					return
				}
			}
			t.Fatal("accepted root output omitted the repaired HelmRelease")
		})
	}
}

func testGeneratedRootOverlapDiff(t *testing.T, order [2]int, self string) {
	t.Helper()
	const syncPath = "clusters/main"
	var pair [2]Tree
	for side, version := range order {
		fixture := testfixture.Materialize(t, "flux-instance-authored-parent", "sync-overlap")
		fixture.Edit(t, fixture.Plan.Steps[0])
		if version == 1 {
			testutil.WriteFile(t, fixture.Repo, syncPath+"/kustomization.yaml",
				`apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - apps.yaml
  - cluster-info.yaml
  - flux-instance.yaml
  - instance-release.yaml
`,
			)
		}
		pair[side] = Tree{
			ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)},
			RepoRoot: fixture.Repo,
			Path:     filepath.Join(fixture.Repo, syncPath),
			Input:    testDirectoryInput(t, fixture.Repo),
			SelfURLs: []string{self},
		}
	}
	pair[0].BaseInput, pair[1].BaseInput = pair[1].Input, pair[0].Input
	runs, err := renderTreesForTest(t.Context(), t, pair[:], Config{Concurrency: 2})
	assert.NoError(t, err)
	assert.FatalIf(t, len(runs) != 2, "missing overlap comparison: %v", runs)
	projected, errs := Select(runs, err, []manifest.NamedResource{
		{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "flux-system"},
	}, nil)
	assert.NoError(t, errs.Selected)
	assert.NoError(t, errs.Remaining)
	assert.FatalIf(t, len(projected) != 2, "missing projected overlap comparison: %v", projected)
	for side, version := range order {
		assertGeneratedRootOverlap(t, runs[side].Result, projected[side], version)
	}
}

func assertGeneratedRootOverlap(t *testing.T, run *Result, result *selection.Result, version int) {
	t.Helper()
	id := manifest.NamedResource{
		Group: "monitoring.coreos.com", Kind: "PrometheusRule", Namespace: "flux-system", Name: "flux-instance-rules",
	}
	winner := [2]string{"sync", "flux-instance"}[version]
	for _, batches := range [][]manifest.DocumentBatch{run.Manifests, result.Manifests} {
		var parents, labels []string
		for _, batch := range batches {
			for _, doc := range batch.Documents {
				if manifest.DocIdentity(doc) != id {
					continue
				}
				meta, _ := doc["metadata"].(map[string]any)
				stamp, _ := meta["labels"].(map[string]any)
				label, _ := stamp[manifest.FluxKustomizeDomain+"/name"].(string)
				parents = append(parents, batch.Parent.Named().Name)
				labels = append(labels, label)
			}
		}
		assert.Diff(t, parents, []string{winner})
		assert.Diff(t, labels, []string{winner})
	}
	warnings := 0
	for i := range run.Warnings {
		warning := &run.Warnings[i]
		if warning.Resource == id && warning.Category == manifest.WarnConflictingProducers {
			warnings += warning.Count
		}
	}
	assert.Equal(t, warnings, 1-version)
}

func TestRenderTrees_GeneratedRootCoversBareDescendant(t *testing.T) {
	t.Parallel()
	const apps = "kubernetes/apps"
	const media = apps + "/media"
	for _, path := range []string{apps, media, media + "/plain"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			fixture := testfixture.Materialize(t, "flux-instance-scoped-workloads", "workloads")
			testutil.WriteFile(t, fixture.Repo, media+"/kustomization.yaml",
				"resources: [flux-instance.yaml, plain/deploy.yaml]\n",
			)
			testutil.WriteFile(t, fixture.Repo, apps+"/kustomization.yaml", `resources: [media, sibling]
patches:
- target: {kind: Deployment, name: plain}
  patch: |-
    - op: add
      path: /metadata/annotations
      value: {review: accepted-root}
`)

			var probes atomic.Int32
			if path != media+"/plain" {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					probes.Add(1)
					_, _ = w.Write([]byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: sentinel}\n"))
				}))
				t.Cleanup(server.Close)
				testutil.WriteFile(t, fixture.Repo, "kustomization.yaml",
					"resources: [kubernetes/apps, "+server.URL+"/sentinel.yaml]\n")
			}
			runs, err := renderTreesForTest(t.Context(), t, []Tree{{
				ID:       manifest.TreeID{Cluster: "."},
				RepoRoot: fixture.Repo,
				Path:     filepath.Join(fixture.Repo, path),
				Input:    testDirectoryInput(t, fixture.Repo),
				SelfURLs: []string{"https://github.com/example/fleet.git"},
			}}, Config{Concurrency: 2})
			assert.NoError(t, err)
			assert.FatalIf(t, len(runs) != 1 || runs[0].Result == nil, "missing result: %v", runs)
			root := testutil.KustomizationID("flux-system", "flux-system")
			assert.Equal(t, runs[0].entryBinding.Producer, root)
			assert.Equal(t, probes.Load(), int32(0))
			for _, batch := range runs[0].Result.Manifests {
				for _, doc := range batch.Documents {
					id := manifest.DocIdentity(doc)
					if id.Kind == "Deployment" && id.Name == "plain" {
						metadata := doc["metadata"].(map[string]any)
						annotations, _ := metadata["annotations"].(map[string]any)
						assert.Equal(t, annotations["review"], any("accepted-root"))
						return
					}
				}
			}
			t.Fatal("plain Deployment absent")
		})
	}
}
