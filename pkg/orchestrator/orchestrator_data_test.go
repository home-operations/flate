package orchestrator

import (
	"fmt"
	"slices"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestOrchestrator_ParentOf(t *testing.T) {
	t.Parallel()
	id := testutil.HelmReleaseID("apps", "child")
	structural, rendered := testutil.KustomizationID("flux-system", "structural"), testutil.KustomizationID("flux-system", "rendered")
	for _, tc := range []struct {
		name                       string
		structural, rendered, want bool
	}{
		{name: "none"}, {name: "structural", structural: true, want: true},
		{name: "rendered", rendered: true, want: true},
		{name: "precedence", structural: true, rendered: true, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o := &Orchestrator{store: store.New(), index: indexForTest(nil)}
			if tc.structural {
				o.index.Records[id] = manifest.DiscoveryRecord{Parent: structural}
			}
			if tc.rendered {
				publishFixture(t, o.store, rendered, []manifest.NamedResource{id})
			}
			got, ok := o.parentOf(id)
			want := rendered
			if tc.structural {
				want = structural
			}
			if ok != tc.want || (ok && got != want) {
				t.Fatalf("parent = %s, %t, want %s, %t", got, ok, want, tc.want)
			}
		})
	}
}

func TestOrchestrator_RequiredDataDependencies_FileOwners(t *testing.T) {
	t.Run("namespace_apps", func(t *testing.T) {
		root := t.TempDir()
		for _, owner := range []string{"a", "b", "c"} {
			testutil.WriteFile(t, root, owner+".yaml", testutil.KustomizationYAML(owner, "flux-system", "  interval: 10m\n  path: ./"+owner+"\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
			ns := "apps"
			if owner == "c" {
				ns = "other"
			}
			testutil.WriteFile(t, root, owner+"/kustomization.yaml", "namespace: "+ns+"\nresources: [../shared, missing.yaml]\n")
		}
		testutil.WriteFile(t, root, "shared/kustomization.yaml", "resources: [input.yaml, declaration.yaml]\n")
		testutil.WriteFile(t, root, "shared/input.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input, namespace: apps}\ndata: {values.yaml: 'greeting: hello'}\n")
		testutil.WriteFile(t, root, "shared/declaration.yaml", "apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: declaration, namespace: apps}\nspec:\n  target: {name: generated}\n  data: [{secretKey: values.yaml, remoteRef: {key: input}}]\n")
		o, err := New(t.Context(), Config{Path: root, RepoRoot: root, CacheDir: t.TempDir(), WipeSecrets: true, Concurrency: 2})
		if err != nil {
			t.Fatal(err)
		}
		res, _ := o.Render(t.Context())
		if res == nil || len(res.Failed) != 3 || o.filter.Enabled() {
			t.Fatalf("full-mode pre-emission fixture failed: %+v", res)
		}
		hr := &manifest.HelmRelease{Name: "consumer", Namespace: "apps"}
		hr.ValuesFrom = []helmv2.ValuesReference{{Kind: "ConfigMap", Name: "input", Optional: true}, {Kind: "Secret", Name: "generated"}, {Kind: "ConfigMap", Name: "input"}}
		o.store.AddObject(hr)
		got := o.requiredDataDependencies(hr.Named())
		for _, id := range []manifest.NamedResource{
			{Kind: "ConfigMap", Namespace: "apps", Name: "input"},
			{Kind: "Secret", Namespace: "apps", Name: "generated"},
			{Group: "external-secrets.io", Kind: "ExternalSecret", Namespace: "apps", Name: "declaration"}, testutil.KustomizationID("flux-system", "a"), testutil.KustomizationID("flux-system", "b"), testutil.KustomizationID("flux-system", "c"),
		} {
			if !slices.Contains(got, id) {
				t.Fatalf("known prerequisite missing: %s in %v", id, got)
			}
		}
		clone := hr.Clone()
		clone.ValuesFrom = []helmv2.ValuesReference{{Kind: "ConfigMap", Name: "input", Optional: true}}
		o.store.AddObject(clone)
		gotInput := o.requiredDataDependencies(hr.Named())
		want := 4
		if len(gotInput) != want || !slices.IsSortedFunc(got, manifest.NamedResource.Compare) || len(slices.Compact(slices.Clone(got))) != len(got) {
			t.Fatalf("ownership/dedup/sort violated: all=%v input=%v", got, gotInput)
		}
		gotInput[0] = manifest.NamedResource{}
		if next := o.requiredDataDependencies(hr.Named()); len(next) != want || slices.Contains(next, manifest.NamedResource{}) {
			t.Fatal("returned slice aliases stored state")
		}
		if len(o.requiredDataDependencies(testutil.KustomizationID("flux-system", "absent"))) != 0 {
			t.Fatal("missing canonical consumer invents refs")
		}
	})
}

func TestOrchestrator_RequiredDataDependencies_Lifecycle(t *testing.T) {
	root := t.TempDir()
	testutil.WriteGeneratedValuesCluster(t, root)
	o, err := New(t.Context(), Config{Path: root, RepoRoot: root, CacheDir: t.TempDir(), Concurrency: 2, WipeSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	id := testutil.HelmReleaseID("flux-system", "a")
	var fresh []manifest.NamedResource
	for range 2 {
		if _, err := o.Render(t.Context()); err != nil {
			t.Fatal(err)
		}
		deps := o.requiredDataDependencies(id)
		if !slices.Contains(deps, testutil.KustomizationID("flux-system", "apps")) || len(deps) < 2 {
			t.Fatalf("generated input ownership missing: %v", deps)
		}
		if fresh == nil {
			fresh = deps
		} else if !slices.Equal(fresh, deps) {
			t.Fatalf("replay changed authored lookup: %v %v", fresh, deps)
		}
	}
	hr, _ := o.store.Get[*manifest.HelmRelease](id)
	artifact := o.store.GetArtifact(id)
	if artifact == nil {
		t.Fatal("fresh render has no artifact")
	}
	clone := hr.Clone()
	clone.ValuesFrom = []helmv2.ValuesReference{{Kind: "Secret", Name: "new", Optional: true}}
	clone.Suspend = true
	o.store.AddObject(clone)
	if _, err := o.Render(t.Context()); err != nil {
		t.Fatal(err)
	}
	want := manifest.NamedResource{Kind: "Secret", Namespace: id.Namespace, Name: "new"}
	if got := o.requiredDataDependencies(id); !slices.Equal(got, []manifest.NamedResource{want}) || len(fresh) < 2 {
		t.Fatalf("suspended canonical replacement changed slice or refs: %v", got)
	}
	clone = clone.Clone()
	clone.Suspend = false
	clone.ValuesFrom[0].Optional = false
	o.store.AddObject(clone)
	o.store.UpdateStatus(id, store.StatusFailed, "", "required input missing")
	_, retained := o.store.GetEmission(id)
	if o.store.GetArtifact(id) != nil || len(retained.Manifests) != len(artifact.(*store.Emission).Manifests) || o.store.FailedResources()[id].Status != store.StatusFailed {
		t.Fatal("stale artifact weakened selected failure")
	}
	if got := o.requiredDataDependencies(id); !slices.Equal(got, []manifest.NamedResource{want}) {
		t.Fatalf("failed reconcile changed canonical admission: %v", got)
	}
}

func TestOrchestrator_RequiredDataDependencies_Skipped(t *testing.T) {
	for _, mode := range []string{"suspended", "unchanged"} {
		t.Run(mode, func(t *testing.T) {
			root, orig := t.TempDir(), t.TempDir()
			for _, dir := range []string{root, orig} {
				testutil.WriteFile(t, dir, "consumer.yaml", fmt.Sprintf(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: consumer, namespace: apps}
spec:
  suspend: %t
  chartRef: {kind: OCIRepository, name: absent}
  valuesFrom: [{kind: ConfigMap, name: input, optional: true}]
`, mode == "suspended"))
				testutil.WriteFile(t, dir, "producer.yaml", testutil.KustomizationYAML("producer", "flux-system", "  path: ./values\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
				testutil.WriteFile(t, dir, "values/kustomization.yaml", "resources: [input.yaml]\n")
				testutil.WriteFile(t, dir, "values/input.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input, namespace: apps}\ndata: {values.yaml: 'greeting: hello'}\n")
			}
			cfg := Config{Path: root, RepoRoot: root, CacheDir: t.TempDir(), Concurrency: 2}
			if mode == "unchanged" {
				cfg.PathOrig = orig
			}
			o, err := New(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := o.Render(t.Context()); err != nil {
				t.Fatal(err)
			}
			id := testutil.HelmReleaseID("apps", "consumer")
			before, _ := o.store.GetStatus(id)
			if before.Outcome() != store.OutcomeSuspended && before.Outcome() != store.OutcomeUnchanged || o.store.GetArtifact(id) != nil {
				t.Fatalf("consumer was rendered: %+v", before)
			}
			got := o.requiredDataDependencies(id)
			if !slices.Contains(got, testutil.KustomizationID("flux-system", "producer")) || !slices.Contains(got, manifest.NamedResource{Kind: "ConfigMap", Namespace: "apps", Name: "input"}) {
				t.Fatalf("skipped consumer lost authored refs: %v", got)
			}
			after, _ := o.store.GetStatus(id)
			if before.Status != after.Status || before.Message != after.Message || !slices.Equal(before.BlockedBy(), after.BlockedBy()) || o.store.GetArtifact(id) != nil {
				t.Fatal("lookup activated skipped consumer")
			}
		})
	}
}

func TestOrchestrator_ChildrenByParent(t *testing.T) {
	o, err := New(t.Context(), Config{Path: t.TempDir(), CacheDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	id := func(name string) manifest.NamedResource {
		return testutil.KustomizationID("apps", name)
	}
	parent, other, first, second := id("parent"), id("other"), id("a"), id("b")
	setParentsForTest(o, map[manifest.NamedResource]manifest.NamedResource{first: parent, second: parent})
	publishFixture(t, o.store, parent, []manifest.NamedResource{second})
	publishFixture(t, o.store, other, []manifest.NamedResource{first})
	got := o.childrenByParent()
	if !slices.Equal(got[parent], []manifest.NamedResource{first, second}) ||
		!slices.Equal(got[other], []manifest.NamedResource{first}) {
		t.Fatalf("ownership snapshot = %v", got)
	}
	got[parent][0] = other
	delete(got, other)
	fresh := o.childrenByParent()
	if !slices.Equal(fresh[parent], []manifest.NamedResource{first, second}) ||
		!slices.Equal(fresh[other], []manifest.NamedResource{first}) {
		t.Fatalf("caller mutated ownership: %v", fresh)
	}
}

func TestOrchestrator_RequiredDataDependencies_Parents(t *testing.T) {
	o, err := New(t.Context(), Config{Path: t.TempDir(), CacheDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	hr := &manifest.HelmRelease{Name: "consumer", Namespace: "apps"}
	hr.ValuesFrom = []helmv2.ValuesReference{{Kind: "Secret", Name: "input", Optional: true}}
	o.store.AddObject(hr)
	input := manifest.NamedResource{Kind: "Secret", Namespace: "apps", Name: "input"}
	structural, rendered := testutil.KustomizationID("flux-system", "structural"), testutil.KustomizationID("flux-system", "rendered")
	setParentsForTest(o, map[manifest.NamedResource]manifest.NamedResource{input: structural})
	publishFixture(t, o.store, rendered, []manifest.NamedResource{input})
	want := []manifest.NamedResource{input, structural, rendered}
	slices.SortFunc(want, manifest.NamedResource.Compare)
	if got := o.requiredDataDependencies(hr.Named()); !slices.Equal(got, want) {
		t.Fatalf("input lost a known parent: want %v, got %v", want, got)
	}
}

func TestOrchestrator_RequiredDataDependencies_ChangedOnlyProducers(t *testing.T) {
	root := t.TempDir()
	o, err := New(t.Context(), Config{Path: root, RepoRoot: root, CacheDir: t.TempDir(), Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	owner := &manifest.Kustomization{Name: "owner", Namespace: "flux-system", Path: "./values"}
	hr := &manifest.HelmRelease{Name: "consumer", Namespace: "apps"}
	hr.ValuesFrom = []helmv2.ValuesReference{{Kind: manifest.KindConfigMap, Name: "input", Optional: true}}
	o.store.AddObject(owner)
	o.store.AddObject(hr)
	input := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input"}
	testutil.WriteFile(t, root, "values/kustomization.yaml", "resources: [input.yaml]\n")
	testutil.WriteFile(t, root, "values/input.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input, namespace: apps}\ndata: {values.yaml: 'greeting: hello'}\n")
	admitted, _, err := baseline.AdmitDirectory(t.Context(), root, root)
	assert.NoError(t, err)
	o.store.SetArtifact(manifest.BootstrapSourceID, admitted)
	l := loader.New(o.store)
	l.Index = indexForTest(map[manifest.NamedResource]string{input: "values/input.yaml", hr.Named(): "consumer.yaml"})
	l.FinalizeIndex(root)
	o.index = l.Index
	foreign := input
	foreign.Group = "example.test"
	foreignOwner := &manifest.Kustomization{Name: "foreign-owner", Namespace: "flux-system", Path: "./foreign"}
	o.store.AddObject(foreignOwner)
	o.index.Records[foreign] = manifest.DiscoveryRecord{File: "foreign/input.yaml", Productions: testutil.ProductionEvidence([]manifest.NamedResource{foreignOwner.Named()})}
	o.filter = change.NewFilter(change.NewSet([]string{"consumer.yaml"}), o.index, o.store)
	assert.Diff(t, testutil.Owners(o.store.Productions(input, o.index)), []manifest.NamedResource{owner.Named()})
	want := []manifest.NamedResource{input, owner.Named()}
	slices.SortFunc(want, manifest.NamedResource.Compare)
	assert.Diff(t, o.requiredDataDependencies(hr.Named()), want)
	assert.Diff(t, testutil.Owners(o.store.Productions(foreign, o.index)), []manifest.NamedResource{foreignOwner.Named()})
}

func TestOrchestrator_RequiredDataDependencies_MissingDirectoryReaders(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [../missing]\n")
	st := store.New()
	admitted, _, err := baseline.AdmitDirectory(t.Context(), root, root)
	assert.NoError(t, err)
	st.SetArtifact(manifest.BootstrapSourceID, admitted)
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps", Path: "./apps"}
	st.AddObject(producer)
	l := loader.New(st)
	l.FinalizeIndex(root)
	input := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input"}
	l.Index.Records[input] = manifest.DiscoveryRecord{File: "missing/nested/input.yaml"}
	consumer := &manifest.HelmRelease{Name: "consumer", Namespace: "apps"}
	consumer.ValuesFrom = []helmv2.ValuesReference{{Kind: manifest.KindConfigMap, Name: "input"}}
	st.AddObject(consumer)
	o := &Orchestrator{store: st, index: l.Index}
	want := []manifest.NamedResource{input, producer.Named()}
	if got := o.requiredDataDependencies(consumer.Named()); !slices.Equal(got, want) {
		t.Fatalf("missing-directory readers = %v, want %v", got, want)
	}
}
