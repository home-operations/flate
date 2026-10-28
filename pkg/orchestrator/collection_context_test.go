package orchestrator

import (
	"fmt"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"path/filepath"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestRenderTrees_SharedHelmContext(t *testing.T) {
	root := t.TempDir()
	const selfURL = "https://fixture.invalid/cluster.git"
	testutil.WriteFile(t, root, "shared/source.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: flux-system, namespace: flux-system}\nspec: {url: "+selfURL+"}\n")
	testutil.WriteFile(t, root, "shared/app.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared-app, namespace: apps}\ndata: {shared: content}\n")
	testutil.WriteFile(t, root, "charts/shared/Chart.yaml", "apiVersion: v2\nname: shared\nversion: 1.0.0\n")
	testutil.WriteFile(t, root, "charts/shared/values.yaml", "value: default\n")
	testutil.WriteFile(t, root, "charts/shared/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: chart-output, namespace: apps}\ndata: {value: {{ .Values.value | quote }}}\n")
	for i, entry := range []string{"alpha", "omega"} {
		testutil.WriteFile(t, root, entry+"/kustomization.yaml", "resources: [release.yaml, ../shared/source.yaml, ../shared/app.yaml]\n")
		testutil.WriteFile(t, root, entry+"/release.yaml", fmt.Sprintf(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: workload, namespace: apps}
spec:
  chart:
    spec:
      chart: charts/shared
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  values: {value: %s, unused: stale}
`, []string{"first", "second"}[i]))
	}
	owner := cachetest.New(t, "", sourcecache.New)
	cfg := Config{Cache: owner, Network: new(source.Network), HelmClient: helm.NewClient(owner), Concurrency: 2}
	t.Cleanup(cfg.Network.Close)
	runs, err := RenderTrees(t.Context(), []Tree{{ID: manifest.TreeID{Cluster: "alpha", Side: manifest.TreeHead}, RepoRoot: root, Path: filepath.Join(root, "alpha"), SelfURLs: []string{selfURL}}, {ID: manifest.TreeID{Cluster: "omega", Side: manifest.TreeHead}, RepoRoot: root, Path: filepath.Join(root, "omega"), SelfURLs: []string{selfURL}}}, cfg)
	assert.NoError(t, err)
	if len(runs) != 2 {
		t.Fatalf("collection lost clusters: %+v", runs)
	}
	id := testutil.HelmReleaseID("apps", "workload")
	var warnings [][]manifest.Warning
	for i, entry := range []string{"alpha", "omega"} {
		run := runs[i]
		assert.Equal(t, run.ID, manifest.TreeID{Cluster: manifest.ClusterID(entry), Side: manifest.TreeHead})
		assert.Equal(t, run.cache == cfg.Cache, true)
		assert.Equal(t, run.cfg.Network == cfg.Network, true)
		assert.Equal(t, run.hrc.Helm == cfg.HelmClient, true)
		explicit, err := RenderTrees(t.Context(), []Tree{{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Path: filepath.Join(root, entry), RepoRoot: root, SelfURLs: []string{selfURL}}}, cfg)
		assert.NoError(t, err)
		if len(explicit) != 1 {
			t.Fatalf("entry lost completed result: %+v", explicit)
		}
		if delta := cmp.Diff(run.Result, explicit[0].Result, cmpopts.IgnoreUnexported(manifest.Kustomization{}, manifest.HelmRelease{})); delta != "" {
			t.Fatal(delta)
		}
		for _, obj := range explicit[0].Store().ListObjects("", "") {
			got, gotOK := run.Store().GetStatus(obj.Named())
			want, wantOK := explicit[0].Store().GetStatus(obj.Named())
			assert.Equal(t, gotOK, wantOK)
			assert.Equal(t, got, want)
		}
		docs := outputByParent(run.Result.Manifests)[id]
		if len(docs) != 1 || docs[0]["data"].(map[string]any)["value"] != []string{"first", "second"}[i] {
			t.Fatalf("cluster %s lost Helm values: %v", entry, docs)
		}
		stale := slices.DeleteFunc(slices.Clone(run.Result.Warnings), func(w manifest.Warning) bool { return w.Category != manifest.WarnStaleValues })
		if len(stale) != 1 || stale[0].Resource != id || stale[0].Count != 1 {
			t.Fatalf("cluster %s lost warning ownership: %+v", entry, run.Result.Warnings)
		}
		warnings = append(warnings, stale)
	}
	assert.Diff(t, warnings[0], warnings[1])
	if runs[0].Store() == runs[1].Store() || runs[0].tasks == runs[1].tasks {
		t.Fatal("clusters shared mutable execution state")
	}
	first, firstOK := runs[0].Store().Get[*manifest.HelmRelease](id)
	second, secondOK := runs[1].Store().Get[*manifest.HelmRelease](id)
	if !firstOK || !secondOK || first == second {
		t.Fatal("clusters shared their HelmRelease object")
	}
	updated := first.Clone()
	updated.Values["value"] = "changed"
	runs[0].Store().AddObject(updated)
	assert.Equal(t, second.Values["value"], any("second"))
	assert.Equal(t, outputByParent(runs[1].Result.Manifests)[id][0]["data"].(map[string]any)["value"], any("second"))
}

func TestRenderTrees_ClusterSecretAndDependencyIsolation(t *testing.T) {
	root := t.TempDir()
	for _, entry := range []string{"alpha", "omega"} {
		testutil.WriteFile(t, root, entry+"/kustomization.yaml", "resources: [objects.yaml]\n")
		objects := testutil.KustomizationYAML("shared", "apps", "  path: ./shared/app\n") + "---\n" +
			testutil.KustomizationYAML("read-secret", "apps", "  path: ./shared/secret\n  postBuild: {substituteFrom: [{kind: Secret, name: credentials}]}\n") + "---\n" +
			testutil.KustomizationYAML("wait-ready", "apps", "  path: ./shared/dependent\n  dependsOn: [{name: ready}]\n")
		if entry == "omega" {
			objects += "---\napiVersion: v1\nkind: Secret\nmetadata: {name: credentials, namespace: apps}\nstringData: {TOKEN: omega-only}\n---\n" +
				testutil.KustomizationYAML("ready", "apps", "  path: ./shared/ready\n")
		}
		testutil.WriteFile(t, root, entry+"/objects.yaml", objects)
	}
	for _, name := range []string{"app", "secret", "dependent"} {
		testutil.WriteFile(t, root, "shared/"+name+"/kustomization.yaml", "resources: [cm.yaml]\n")
		value := "stable"
		if name == "secret" {
			value = "${TOKEN}"
		}
		testutil.WriteFile(t, root, "shared/"+name+"/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+"-output, namespace: apps}\ndata: {value: '"+value+"'}\n")
	}
	testutil.WriteFile(t, root, "shared/ready/kustomization.yaml", "resources: []\n")
	runs, err := RenderTrees(t.Context(), []Tree{{ID: manifest.TreeID{Cluster: "alpha", Side: manifest.TreeHead}, RepoRoot: root, Path: filepath.Join(root, "alpha")}, {ID: manifest.TreeID{Cluster: "omega", Side: manifest.TreeHead}, RepoRoot: root, Path: filepath.Join(root, "omega")}}, Config{Concurrency: 2})
	if err == nil || len(runs) != 2 || runs[0].Orchestrator == nil || runs[1].Orchestrator == nil {
		t.Fatalf("collection lost isolated partial results: %+v %v", runs, err)
	}
	assert.Equal(t, runs[0].ID.Cluster, manifest.ClusterID("alpha"))
	assert.Equal(t, runs[1].ID.Cluster, manifest.ClusterID("omega"))
	secret := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "credentials"}
	ready, readSecret, waitReady := testutil.KustomizationID("apps", "ready"), testutil.KustomizationID("apps", "read-secret"), testutil.KustomizationID("apps", "wait-ready")
	if runs[0].Store().GetObject(secret) != nil || runs[0].Store().GetObject(ready) != nil || !slices.Contains(runs[0].Result.Blocked[waitReady], ready) {
		t.Fatalf("another cluster satisfied missing inputs: failed=%v blocked=%v", runs[0].Result.Failed, runs[0].Result.Blocked)
	}
	if len(outputByParent(runs[0].Result.Manifests)[waitReady]) != 0 {
		t.Fatalf("missing dependency emitted %s", waitReady)
	}
	if docs := outputByParent(runs[0].Result.Manifests)[readSecret]; len(docs) != 0 || !slices.Contains(runs[0].Result.Blocked[readSecret], secret) {
		t.Fatalf("another cluster satisfied the required Secret: docs=%v blocked=%v", docs, runs[0].Result.Blocked)
	}
	for _, w := range runs[1].Result.Warnings {
		if w.Category == manifest.WarnUnresolvedSubstitution {
			t.Fatalf("missing-secret warning crossed clusters: %+v", w)
		}
	}
	for _, id := range []manifest.NamedResource{readSecret, waitReady} {
		status, ok := runs[1].Store().GetStatus(id)
		if !ok || status.Status != store.StatusReady {
			t.Fatalf("healthy cluster lost %s: %+v", id, status)
		}
	}
	assert.NoError(t, runs[1].Err)
	assert.Equal(t, len(runs[1].Result.Failed), 0)
	assert.Equal(t, outputByParent(runs[1].Result.Manifests)[readSecret][0]["data"].(map[string]any)["value"], any("omega-only"))
}
