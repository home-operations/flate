package orchestrator

import (
	"fmt"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestRender_GeneratedValuesAndParentGates(t *testing.T) {
	var generatedName string
	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			dir := t.TempDir()
			testutil.WriteGeneratedValuesCluster(t, dir)
			o, err := New(Config{Path: dir, RepoRoot: dir, CacheDir: t.TempDir(), WipeSecrets: true, Concurrency: workers})
			if err != nil {
				t.Fatal(err)
			}
			if err := o.Bootstrap(t.Context()); err != nil {
				t.Fatal(err)
			}
			a := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "flux-system", Name: "a"}
			if o.store.GetObject(a) != nil || parentsForTest(o.index)[a] != ksID("apps") {
				t.Fatalf("raw HR arrived or parent gate missing: %s parent=%s", a, parentsForTest(o.index)[a])
			}
			res, err := o.Render(t.Context())
			if err != nil || len(res.Failed) != 0 || len(res.Blocked) != 0 {
				t.Fatalf("Render err=%v failures=%v blockers=%v", err, res.Failed, res.Blocked)
			}
			for _, name := range []string{"a", "b"} {
				id := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "flux-system", Name: name}
				hr, ok := o.store.Get[*manifest.HelmRelease](id)
				if !ok {
					t.Fatalf("HR missing: %s", id)
				}
				if name == "a" {
					if len(hr.ValuesFrom) != 1 || !strings.HasPrefix(hr.ValuesFrom[0].Name, "generated-values-") {
						t.Fatalf("HR did not retain rewritten reference: %+v", hr)
					}
					ref := hr.ValuesFrom[0].Name
					if generatedName != "" && ref != generatedName {
						t.Fatalf("generated reference varies by entry or concurrency: %q != %q", ref, generatedName)
					}
					generatedName = ref
					cmID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: id.Namespace, Name: ref}
					cm, ok := o.store.Get[*manifest.ConfigMap](cmID)
					if !ok || cm.Data["values.yaml"] != "greeting: hello\nreplicas: 2\n" {
						t.Fatalf("generated values unavailable: %+v", cm)
					}
				}
				renderedID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: id.Namespace, Name: name + "-rendered"}
				rendered, ok := o.store.Get[*manifest.ConfigMap](renderedID)
				if !ok || rendered.Data["greeting"] != "hello" || rendered.Data["replicas"] != "2" {
					t.Fatalf("effective chart values differ: %+v", rendered)
				}
				if info, _ := o.store.GetStatus(id); info.Status != store.StatusReady {
					t.Fatalf("HR status=%+v", info)
				}
			}
			if info, _ := o.store.GetStatus(ksID("apps")); info.Status != store.StatusReady {
				t.Fatalf("enclosing KS status=%+v", info)
			}
		})
	}
}

func TestRenderTrees_ExcludedOrderingAndPromotion(t *testing.T) {
	for _, broken := range []bool{false, true} {
		t.Run(fmt.Sprintf("broken_source_%v", broken), func(t *testing.T) {
			baseDir, headDir := t.TempDir(), t.TempDir()
			testutil.WriteFilteredOrderingCluster(t, baseDir, "v1", broken)
			testutil.WriteFilteredOrderingCluster(t, headDir, "v2", broken)
			base, head, err := RenderTrees(t.Context(), Tree{RepoRoot: baseDir}, Tree{RepoRoot: headDir},
				Config{WipeSecrets: true, Concurrency: 4, CacheDir: t.TempDir()})
			if (err != nil) != broken {
				t.Fatalf("RenderTrees error=%v, want failure=%v", err, broken)
			}
			for _, side := range []Rendered{base, head} {
				if side.Result == nil {
					t.Fatal("snapshot result missing")
				}
				st := side.Store()
				selected := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "flux-system", Name: "selected"}
				for _, id := range []manifest.NamedResource{
					ksID("monitor-a"), ksID("monitor-b"),
					{Kind: manifest.KindHelmRelease, Namespace: "flux-system", Name: "monitor-a"},
					{Kind: manifest.KindHelmRelease, Namespace: "flux-system", Name: "monitor-b"},
					{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "excluded-values"},
				} {
					if (id.Kind != manifest.KindKustomization && st.GetObject(id) != nil) || side.filter.ShouldReconcile(id) {
						t.Errorf("excluded sibling or ordering target admitted: %s", id)
					}
				}
				excludedSource := manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "excluded-source"}
				if info, _ := st.GetStatus(excludedSource); !store.IsUnchanged(info) || side.filter.ShouldReconcile(excludedSource) {
					t.Fatalf("discovered excluded source was activated: %+v", info)
				}
				if st.GetArtifact(excludedSource) != nil {
					t.Fatal("excluded source was fetched")
				}
				if broken {
					source := manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "selected-source"}
					if st.GetObject(source) == nil || side.Result.Failed[source].Status != store.StatusFailed || side.Result.Failed[selected].Status != store.StatusFailed {
						t.Fatalf("selected source failure hidden: objects=%v failures=%v", st.GetObject(source), side.Result.Failed)
					}
					continue
				}
				if side.Err != nil || len(side.Result.Failed) != 0 || len(side.Result.Blocked) != 0 {
					t.Fatalf("excluded chain contaminated snapshot: error=%v failures=%v", side.Err, side.Result.Failed)
				}
				if st.GetArtifact(ksID("selected")) == nil {
					t.Fatal("selected KS was skipped instead of reconciled")
				}
				chart := manifest.NamedResource{Kind: manifest.KindHelmChart, Namespace: "flux-system", Name: "shared-chart"}
				if st.GetObject(chart) == nil || !side.filter.ShouldReconcile(chart) {
					t.Fatalf("required shared chart was not admitted: %s", chart)
				}
				otherSource := manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "selected-source"}
				if info, _ := st.GetStatus(otherSource); !store.IsUnchanged(info) || side.filter.ShouldReconcile(otherSource) {
					t.Fatalf("excluded source sibling was activated: %+v", info)
				}
				if st.GetArtifact(otherSource) != nil {
					t.Fatal("excluded source sibling was fetched")
				}
				for _, id := range []manifest.NamedResource{
					selected, ksID("selected"),
					{Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "shared"},
				} {
					if st.GetObject(id) == nil || !side.filter.ShouldReconcile(id) {
						t.Fatalf("selected source or consumer missing: %s", id)
					}
					if info, _ := st.GetStatus(id); info.Status != store.StatusReady {
						t.Fatalf("selected resource not Ready: %s %+v", id, info)
					}
				}
				if st.GetObject(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "selected-values"}) == nil {
					t.Fatal("required values data was not rendered")
				}
			}
			if !broken {
				for i, side := range []Rendered{base, head} {
					batches := canonicalBatches(side.Result)
					if len(batches) != 2 || batches[0].Parent != (manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "flux-system", Name: "selected"}) || batches[1].Parent != ksID("selected") {
						t.Fatalf("unexpected selected producer batches: %+v", batches)
					}
					if len(batches[0].Documents) != 1 || len(batches[1].Documents) != 2 {
						t.Fatalf("unexpected selected document order: %+v", batches)
					}
					cm := batches[0].Documents[0]
					meta, _ := cm["metadata"].(map[string]any)
					data, _ := cm["data"].(map[string]any)
					want := "v1"
					if i == 1 {
						want = "v2"
					}
					if cm["kind"] != manifest.KindConfigMap || meta["name"] != "selected-rendered" || data["greeting"] != want {
						t.Fatalf("rendered chart values = %+v, want %s", cm, want)
					}
					hrDoc, valuesDoc := batches[1].Documents[0], batches[1].Documents[1]
					valuesMeta, _ := valuesDoc["metadata"].(map[string]any)
					hrMeta, _ := hrDoc["metadata"].(map[string]any)
					spec, _ := hrDoc["spec"].(map[string]any)
					values, _ := spec["values"].(map[string]any)
					if valuesDoc["kind"] != manifest.KindConfigMap || valuesMeta["name"] != "selected-values" || hrDoc["kind"] != manifest.KindHelmRelease || hrMeta["name"] != "selected" || values["greeting"] != want {
						t.Fatalf("selected producer document order or values differ: %+v", batches[1].Documents)
					}
				}
			}
		})
	}
}

func TestRenderTrees_UnknownOrderingTargetStillFails(t *testing.T) {
	baseDir, headDir := t.TempDir(), t.TempDir()
	testutil.WriteFilteredOrderingCluster(t, baseDir, "v1", false)
	testutil.WriteFilteredOrderingCluster(t, headDir, "v2", false)
	for _, dir := range []string{baseDir, headDir} {
		testutil.WriteFile(t, dir, "flux/selected.yaml", ksYAML("selected", "apps/selected", "unknown"))
	}
	base, head, err := RenderTrees(t.Context(), Tree{RepoRoot: baseDir}, Tree{RepoRoot: headDir},
		Config{WipeSecrets: true, Concurrency: 2, CacheDir: t.TempDir()})
	if err == nil {
		t.Fatal("selected unknown ordering target was ignored")
	}
	for _, side := range []Rendered{base, head} {
		info := side.Result.Failed[ksID("selected")]
		if info.Status != store.StatusFailed || !strings.Contains(info.Message, "unknown: dependency not found") {
			t.Fatalf("missing target diagnosis lost: %+v", info)
		}
	}
}
