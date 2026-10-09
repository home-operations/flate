package orchestrator

import (
	"fmt"
	"slices"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestOrchestrator_ParentOf(t *testing.T) {
	id := manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "child", Namespace: "apps"}
	structural, rendered := ksID("structural"), ksID("rendered")
	for _, tc := range []struct {
		name                       string
		structural, rendered, want bool
	}{
		{name: "none"}, {name: "structural", structural: true, want: true},
		{name: "rendered", rendered: true, want: true},
		{name: "precedence", structural: true, rendered: true, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := &Orchestrator{parentOf: map[manifest.NamedResource]manifest.NamedResource{}, rendered: newRenderedSet()}
			if tc.structural {
				o.parentOf[id] = structural
			}
			if tc.rendered {
				o.rendered.MarkRenderedBatch(rendered, []manifest.NamedResource{id})
			}
			got, ok := o.ParentOf(id)
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
	for _, namespace := range []string{"", "apps", "foreign"} {
		t.Run("namespace_"+namespace, func(t *testing.T) {
			root := t.TempDir()
			for _, owner := range []string{"a", "b"} {
				testutil.WriteFile(t, root, owner+".yaml", fmt.Sprintf(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: %s, namespace: flux-system}
spec:
  interval: 10m
  path: ./%s
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`, owner, owner))
				testutil.WriteFile(t, root, owner+"/kustomization.yaml", "namespace: apps\nresources: [../shared, missing.yaml]\n")
			}
			testutil.WriteFile(t, root, "shared/kustomization.yaml", "resources: [input.yaml, declaration.yaml]\n")
			testutil.WriteFile(t, root, "shared/input.yaml", "apiVersion: v1\nkind: Secret\nmetadata: {name: input, namespace: '"+namespace+"'}\nstringData: {values.yaml: 'greeting: hello'}\n")
			testutil.WriteFile(t, root, "shared/declaration.yaml", "apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: declaration, namespace: apps}\nspec:\n  target: {name: generated}\n  data: [{secretKey: values.yaml, remoteRef: {key: input}}]\n")
			o, err := New(Config{Path: root, RepoRoot: root, CacheDir: t.TempDir(), WipeSecrets: true, Concurrency: 2})
			if err != nil {
				t.Fatal(err)
			}
			res, _ := o.Render(t.Context())
			if res == nil || len(res.Failed) != 2 || o.Filter().Enabled() {
				t.Fatalf("full-mode pre-emission fixture failed: %+v", res)
			}
			hr := &manifest.HelmRelease{Name: "consumer", Namespace: "apps"}
			hr.ValuesFrom = []helmv2.ValuesReference{{Kind: "Secret", Name: "input", Optional: true}, {Kind: "Secret", Name: "generated"}, {Kind: "Secret", Name: "input"}}
			o.store.AddObject(hr)
			got := o.RequiredDataDependencies(hr.Named())
			for _, id := range []manifest.NamedResource{
				{Kind: "Secret", Namespace: "apps", Name: "input"},
				{Kind: "Secret", Namespace: "apps", Name: "generated"},
				{Kind: "ExternalSecret", Namespace: "apps", Name: "declaration"}, ksID("a"), ksID("b"),
			} {
				if !slices.Contains(got, id) {
					t.Fatalf("known prerequisite missing: %s in %v", id, got)
				}
			}
			clone := hr.Clone()
			clone.ValuesFrom = []helmv2.ValuesReference{{Kind: "Secret", Name: "input", Optional: true}}
			o.store.AddObject(clone)
			gotInput := o.RequiredDataDependencies(hr.Named())
			want := 3
			if namespace == "foreign" {
				want = 1
			}
			if len(gotInput) != want || !slices.IsSortedFunc(got, manifest.NamedResource.Compare) || len(slices.Compact(slices.Clone(got))) != len(got) {
				t.Fatalf("ownership/dedup/sort violated: all=%v input=%v", got, gotInput)
			}
			gotInput[0] = manifest.NamedResource{}
			if next := o.RequiredDataDependencies(hr.Named()); len(next) != want || slices.Contains(next, manifest.NamedResource{}) {
				t.Fatal("returned slice aliases stored state")
			}
			if len(o.RequiredDataDependencies(ksID("absent"))) != 0 {
				t.Fatal("missing canonical consumer invents refs")
			}
		})
	}
}

func TestOrchestrator_RequiredDataDependencies_Lifecycle(t *testing.T) {
	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteGeneratedValuesCluster(t, root)
			o, err := New(Config{Path: root, RepoRoot: root, CacheDir: t.TempDir(), Concurrency: workers, WipeSecrets: true})
			if err != nil {
				t.Fatal(err)
			}
			id := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "flux-system", Name: "a"}
			var fresh []manifest.NamedResource
			for range 2 {
				if _, err := o.Render(t.Context()); err != nil {
					t.Fatal(err)
				}
				deps := o.RequiredDataDependencies(id)
				if !slices.Contains(deps, ksID("apps")) || len(deps) < 2 {
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
			if got := o.RequiredDataDependencies(id); !slices.Equal(got, []manifest.NamedResource{want}) || len(fresh) < 2 {
				t.Fatalf("suspended canonical replacement changed slice or refs: %v", got)
			}
			clone = clone.Clone()
			clone.Suspend = false
			clone.ValuesFrom[0].Optional = false
			o.store.AddObject(clone)
			o.store.UpdateStatus(id, store.StatusFailed, "required input missing")
			if o.store.GetArtifact(id) != artifact || o.store.FailedResources()[id].Status != store.StatusFailed {
				t.Fatal("stale artifact weakened selected failure")
			}
			if got := o.RequiredDataDependencies(id); !slices.Equal(got, []manifest.NamedResource{want}) {
				t.Fatalf("failed reconcile changed canonical admission: %v", got)
			}
		})
	}
}

func TestOrchestrator_RequiredDataDependencies_Skipped(t *testing.T) {
	for _, workers := range []int{2, 4} {
		for _, mode := range []string{"suspended", "unchanged"} {
			t.Run(fmt.Sprintf("%d/%s", workers, mode), func(t *testing.T) {
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
					testutil.WriteFile(t, dir, "producer.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: producer, namespace: flux-system}
spec:
  path: ./values
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
					testutil.WriteFile(t, dir, "values/kustomization.yaml", "resources: [input.yaml]\n")
					testutil.WriteFile(t, dir, "values/input.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input, namespace: apps}\ndata: {values.yaml: 'greeting: hello'}\n")
				}
				cfg := Config{Path: root, RepoRoot: root, CacheDir: t.TempDir(), Concurrency: workers}
				if mode == "unchanged" {
					cfg.PathOrig = orig
				}
				o, err := New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := o.Render(t.Context()); err != nil {
					t.Fatal(err)
				}
				id := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "consumer"}
				before, _ := o.store.GetStatus(id)
				if !store.IsSuspended(before) && !store.IsUnchanged(before) || o.store.GetArtifact(id) != nil {
					t.Fatalf("consumer was rendered: %+v", before)
				}
				got := o.RequiredDataDependencies(id)
				if !slices.Contains(got, ksID("producer")) || !slices.Contains(got, manifest.NamedResource{Kind: "ConfigMap", Namespace: "apps", Name: "input"}) {
					t.Fatalf("skipped consumer lost authored refs: %v", got)
				}
				after, _ := o.store.GetStatus(id)
				if before != after || o.store.GetArtifact(id) != nil {
					t.Fatal("lookup activated skipped consumer")
				}
			})
		}
	}
}

func TestOrchestrator_ChildrenByParent(t *testing.T) {
	o, err := New(Config{Path: t.TempDir(), CacheDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	id := func(name string) manifest.NamedResource {
		return manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: name}
	}
	parent, other, first, second := id("parent"), id("other"), id("a"), id("b")
	o.parentOf = map[manifest.NamedResource]manifest.NamedResource{first: parent, second: parent}
	o.rendered.MarkRenderedBatch(parent, []manifest.NamedResource{second})
	o.rendered.MarkRenderedBatch(other, []manifest.NamedResource{first})
	got := o.ChildrenByParent()
	if !slices.Equal(got[parent], []manifest.NamedResource{first, second}) ||
		!slices.Equal(got[other], []manifest.NamedResource{first}) {
		t.Fatalf("ownership snapshot = %v", got)
	}
	got[parent][0] = other
	delete(got, other)
	fresh := o.ChildrenByParent()
	if !slices.Equal(fresh[parent], []manifest.NamedResource{first, second}) ||
		!slices.Equal(fresh[other], []manifest.NamedResource{first}) {
		t.Fatalf("caller mutated ownership: %v", fresh)
	}
}

func TestOrchestrator_RequiredDataDependencies_Parents(t *testing.T) {
	o, err := New(Config{Path: t.TempDir(), CacheDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	hr := &manifest.HelmRelease{Name: "consumer", Namespace: "apps"}
	hr.ValuesFrom = []helmv2.ValuesReference{{Kind: "Secret", Name: "input", Optional: true}}
	o.store.AddObject(hr)
	input := manifest.NamedResource{Kind: "Secret", Namespace: "apps", Name: "input"}
	structural, rendered := ksID("structural"), ksID("rendered")
	o.parentOf = map[manifest.NamedResource]manifest.NamedResource{input: structural}
	o.rendered.MarkRenderedBatch(rendered, []manifest.NamedResource{input})
	want := []manifest.NamedResource{input, structural, rendered}
	slices.SortFunc(want, manifest.NamedResource.Compare)
	if got := o.RequiredDataDependencies(hr.Named()); !slices.Equal(got, want) {
		t.Fatalf("input lost a known parent: want %v, got %v", want, got)
	}
}

func TestOrchestrator_RequiredDataDependencies_ChangedOnlyProducers(t *testing.T) {
	root := t.TempDir()
	o, err := New(Config{Path: root, RepoRoot: root, CacheDir: t.TempDir(), Concurrency: 2})
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
	// The change filter can retain file ownership absent from the canonical indexes.
	o.filter = change.NewFilter(change.NewSet([]string{"consumer.yaml"}), map[manifest.NamedResource]string{input: "values/input.yaml", hr.Named(): "consumer.yaml"}, root, o.store)
	assert.Diff(t, o.filter.ProducersFor(input), []manifest.NamedResource{owner.Named()})
	want := []manifest.NamedResource{input, owner.Named()}
	slices.SortFunc(want, manifest.NamedResource.Compare)
	assert.Diff(t, o.RequiredDataDependencies(hr.Named()), want)
}
