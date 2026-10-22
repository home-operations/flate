package base

import (
	"context"
	"errors"
	"github.com/home-operations/flate/pkg/values"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/depwait"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
)

func BenchmarkCollectDeps(b *testing.B) {
	id := testutil.KustomizationID("apps", "target")
	ks := &manifest.Kustomization{Name: "consumer", Namespace: "apps", DependsOn: []kustomizev1.DependencyReference{{Name: id.Name, Namespace: id.Namespace}}}
	st := store.New()
	c := gateExecution(New(st, nil), manifest.NamedResource{})
	c.Configure(Options{Filter: change.NewFilter(change.NewSet([]string{"target.yaml"}), &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: {File: "target.yaml"}}}, st)})
	b.ReportAllocs()
	for b.Loop() {

		c.dependencies(ks, manifest.GateInputs)
	}
}

func TestDependencies_Kustomization_SubstitutionProducers(t *testing.T) {
	t.Parallel()
	consumer := testutil.KustomizationID("apps", "consumer")
	producer := testutil.KustomizationID("apps", "producer")
	cm := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	for _, tc := range []struct {
		name                    string
		ref                     manifest.SubstituteReference
		producers, owners, want []manifest.NamedResource
	}{
		{name: "required ConfigMap", ref: manifest.SubstituteReference{Kind: manifest.KindConfigMap, Name: "settings"}, producers: []manifest.NamedResource{producer}, want: []manifest.NamedResource{cm, producer}},
		{name: "Owners-only ConfigMap in full mode", ref: manifest.SubstituteReference{Kind: manifest.KindConfigMap, Name: "settings"}, owners: []manifest.NamedResource{producer}, want: []manifest.NamedResource{cm, producer}},
		{name: "optional ConfigMap", ref: manifest.SubstituteReference{Kind: manifest.KindConfigMap, Name: "settings", Optional: true}, producers: []manifest.NamedResource{producer}},
		{name: "required Secret", ref: manifest.SubstituteReference{Kind: manifest.KindSecret, Name: "settings"}, producers: []manifest.NamedResource{producer}, want: []manifest.NamedResource{producer}},
		{name: "optional Secret", ref: manifest.SubstituteReference{Kind: manifest.KindSecret, Name: "settings", Optional: true}, producers: []manifest.NamedResource{producer}},
		{name: "unknown Secret", ref: manifest.SubstituteReference{Kind: manifest.KindSecret, Name: "settings"}},
		{name: "missing ConfigMap", ref: manifest.SubstituteReference{Kind: manifest.KindConfigMap, Name: "settings"}, want: []manifest.NamedResource{cm}},
		{name: "self producer", ref: manifest.SubstituteReference{Kind: manifest.KindConfigMap, Name: "settings"}, producers: []manifest.NamedResource{consumer}},
		{name: "empty name", ref: manifest.SubstituteReference{Kind: manifest.KindConfigMap}, producers: []manifest.NamedResource{producer}},
		{name: "unknown kind", ref: manifest.SubstituteReference{Kind: "Junk", Name: "settings"}, producers: []manifest.NamedResource{producer}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			id := manifest.NamedResource{Kind: tc.ref.Kind, Namespace: consumer.Namespace, Name: tc.ref.Name}
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: {Productions: testutil.ProductionEvidence(append(slices.Clone(tc.producers), tc.owners...))}}}
			c := gateExecution(New(store.New(), nil), manifest.NamedResource{})
			c.Configure(Options{Index: index})
			ks := &manifest.Kustomization{Name: consumer.Name, Namespace: consumer.Namespace, PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{tc.ref}}}
			var got []manifest.NamedResource
			for _, dep := range c.dependenciesForTest(ks) {
				got = append(got, dep.NamedResource)
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("dependencies = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestResolve_SubstitutionProducers(t *testing.T) {
	t.Parallel()
	consumer := testutil.KustomizationID("apps", "consumer")
	producer := testutil.KustomizationID("apps", "producer")
	cm := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	for _, tc := range []struct {
		name                    string
		ref                     manifest.SubstituteReference
		producers, owners, want []manifest.NamedResource
	}{
		{name: "required ConfigMap", ref: manifest.SubstituteReference{Kind: manifest.KindConfigMap, Name: "settings"}, producers: []manifest.NamedResource{producer}, want: []manifest.NamedResource{cm, producer}},
		{name: "Owners-only ConfigMap in full mode", ref: manifest.SubstituteReference{Kind: manifest.KindConfigMap, Name: "settings"}, owners: []manifest.NamedResource{producer}, want: []manifest.NamedResource{cm, producer}},
		{name: "optional ConfigMap", ref: manifest.SubstituteReference{Kind: manifest.KindConfigMap, Name: "settings", Optional: true}, producers: []manifest.NamedResource{producer}},
		{name: "required Secret", ref: manifest.SubstituteReference{Kind: manifest.KindSecret, Name: "settings"}, producers: []manifest.NamedResource{producer}, want: []manifest.NamedResource{producer}},
		{name: "optional Secret", ref: manifest.SubstituteReference{Kind: manifest.KindSecret, Name: "settings", Optional: true}, producers: []manifest.NamedResource{producer}},
		{name: "unknown Secret", ref: manifest.SubstituteReference{Kind: manifest.KindSecret, Name: "settings"}},
		{name: "missing ConfigMap", ref: manifest.SubstituteReference{Kind: manifest.KindConfigMap, Name: "settings"}, want: []manifest.NamedResource{cm}},
		{name: "self producer", ref: manifest.SubstituteReference{Kind: manifest.KindConfigMap, Name: "settings"}, producers: []manifest.NamedResource{consumer}},
		{name: "empty name", ref: manifest.SubstituteReference{Kind: manifest.KindConfigMap}, producers: []manifest.NamedResource{producer}},
		{name: "unknown kind", ref: manifest.SubstituteReference{Kind: "Junk", Name: "settings"}, producers: []manifest.NamedResource{producer}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			id := manifest.NamedResource{Kind: tc.ref.Kind, Namespace: consumer.Namespace, Name: tc.ref.Name}
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: {Productions: testutil.ProductionEvidence(append(slices.Clone(tc.producers), tc.owners...))}}}
			c := gateExecution(New(store.New(), nil), manifest.NamedResource{})
			c.Configure(Options{Index: index})
			ks := &manifest.Kustomization{Name: consumer.Name, Namespace: consumer.Namespace, PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{tc.ref}}}

			c.Reads.Init(c.Store, ks.Named(), nil)
			inputs := c.DataInputs(t.Context(), ks, values.NewStoreProvider(&c.Reads, false, nil))
			err := c.AwaitData(t.Context(), DataWait{Inputs: inputs, Resume: func(context.Context, manifest.DrainLevel, []manifest.NamedResource) error { return nil }}, manifest.DrainNone)
			blocked, waiting := errors.AsType[*depwait.ErrBlocked](err)
			var want []manifest.NamedResource
			for _, supplier := range append(slices.Clone(tc.producers), tc.owners...) {
				if supplier != consumer && tc.ref.Name != "" && (tc.ref.Kind == manifest.KindConfigMap || tc.ref.Kind == manifest.KindSecret) {
					want = append(want, supplier)
				}
			}
			if len(want) > 0 {
				if !waiting {
					t.Fatalf("supplier was not bound: %v", err)
				}
				for _, supplier := range want {
					if !slices.Contains(blocked.Deps, supplier) {
						t.Fatalf("suppliers=%v want=%v", blocked.Deps, want)
					}
				}
			} else if tc.ref.Kind == manifest.KindConfigMap && tc.ref.Name != "" && !tc.ref.Optional && !slices.Contains(tc.producers, consumer) {
				if !waiting || !slices.Contains(blocked.Deps, cm) {
					t.Fatalf("required input did not wait: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}

		})
	}
}

func TestCollectDeps_Allocations(t *testing.T) {
	id := testutil.KustomizationID("apps", "target")
	ks := &manifest.Kustomization{Name: "consumer", Namespace: "apps", DependsOn: []kustomizev1.DependencyReference{{Name: id.Name, Namespace: id.Namespace}}}
	st := store.New()
	c := gateExecution(New(st, nil), manifest.NamedResource{})
	c.Configure(Options{Filter: change.NewFilter(change.NewSet([]string{"target.yaml"}), &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: {File: "target.yaml"}}}, st)})
	allocs := testing.AllocsPerRun(100, func() { c.dependencies(ks, manifest.GateInputs) })
	if allocs > 3 {
		t.Fatalf("dependency collection allocated %g times, want at most 3", allocs)
	}
}

func TestCollectDeps_NoParentNoExtraDep(t *testing.T) {
	t.Parallel()
	child := &manifest.Kustomization{Name: "karma", Namespace: "observability"}
	c := New(store.New(), nil)
	deps := gateExecution(c, child.Named()).dependencies(child, manifest.GateInputs)
	if len(deps) != 0 {
		t.Errorf("expected no deps for KS without sourceRef/dependsOn/parent; got %+v", deps)
	}
}

func TestSubstitutionAvailability_ObservesMaterializers(t *testing.T) {
	t.Parallel()
	consumer := testutil.KustomizationID("apps", "consumer")
	producer := testutil.KustomizationID("apps", "producer")
	for _, tc := range []struct {
		name, kind            string
		optional, self, known bool
		want                  schedule.Kind
	}{
		{name: "required ConfigMap", kind: manifest.KindConfigMap, known: true, want: schedule.Blocked},
		{name: "optional ConfigMap", kind: manifest.KindConfigMap, optional: true, known: true, want: schedule.Blocked},
		{name: "required Secret", kind: manifest.KindSecret, known: true, want: schedule.Blocked},
		{name: "optional Secret", kind: manifest.KindSecret, optional: true, known: true, want: schedule.Blocked},
		{name: "unknown Secret", kind: manifest.KindSecret, want: schedule.Terminal},
		{name: "unknown ConfigMap", kind: manifest.KindConfigMap, want: schedule.Blocked},
		{name: "self produced", kind: manifest.KindConfigMap, known: true, self: true, want: schedule.Terminal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			id := manifest.NamedResource{Kind: tc.kind, Namespace: "apps", Name: "settings"}
			owner := producer
			if tc.self {
				owner = consumer
			}
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
			if tc.known {
				index.Records[id] = manifest.DiscoveryRecord{Productions: []manifest.Production{{Owner: owner}}}
			}
			c := New(store.New(), nil)
			c.Configure(Options{Index: index})
			ks := &manifest.Kustomization{Name: consumer.Name, Namespace: consumer.Namespace, PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: tc.kind, Name: id.Name, Optional: tc.optional}}}}
			execution := gateExecution(c, consumer)
			if deps := execution.authoredDependenciesForTest(ks); len(deps) != 0 {
				t.Fatalf("data became authored readiness gate: %v", deps)
			}
			input := values.NewStoreProvider(&execution.Reads, false, c.SubstituteFrom).Lookup(id)
			w := depwait.Waiter{Store: &execution.Reads, Parent: consumer}
			got := w.Available(t.Context(), depwait.DataDependency{NamedResource: id, Input: input, Optional: tc.optional, AllowMissing: tc.kind == manifest.KindSecret}, manifest.DrainNone)
			if got.Kind != tc.want {
				t.Fatalf("availability=%+v want=%v", got, tc.want)
			}
			if tc.known && !tc.self && !slices.Equal(got.Blocked, []manifest.NamedResource{producer}) {
				t.Fatalf("materializer was not observed: %+v", got)
			}
			for required := range execution.Reads.Required() {
				t.Fatalf("materializer became required cause: %s", required)
			}
		})
	}
}

func TestSubstitutionAvailability_UnselectedMaterializer(t *testing.T) {
	t.Parallel()
	consumerObj := &manifest.Kustomization{
		Name: "cluster-apps", Namespace: "flux-system",
		Path: "kubernetes/apps",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "cluster-settings"},
		}},
	}
	producerObj := &manifest.Kustomization{
		Name: "cluster-vars", Namespace: "flux-system",
		Path: "kubernetes/flux/vars",
	}
	consumer := consumerObj.Named()
	producer := producerObj.Named()
	// discovery-indexed CM (Namespace="") — kustomize's namespace
	// directive hasn't run yet at file-walk time.
	cmObj := &manifest.ConfigMap{Name: "cluster-settings", Namespace: ""}
	cmID := cmObj.Named()

	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{consumer: {File: "kubernetes/flux/cluster-apps.yaml"}, producer: {File: "kubernetes/flux/cluster-vars.yaml"}, cmID: {File: "kubernetes/flux/vars/cluster-settings.yaml", Productions: []manifest.Production{{Owner: producer, Authored: cmID, Path: "kubernetes/flux/vars/cluster-settings.yaml", File: "kubernetes/flux/vars/cluster-settings.yaml"}}}}, Claims: map[string][]manifest.NamedResource{"kubernetes/apps/": {consumer}, "kubernetes/flux/vars/": {producer}}}
	fixture := store.New()
	fixture.AddObjects([]manifest.BaseManifest{consumerObj, producerObj, cmObj})
	fixture.SetDiscoveryIndex(index)
	f := change.NewFilter(change.NewSet([]string{"kubernetes/apps/changed.yaml"}), index, fixture)

	c := New(store.New(), nil)
	c.Configure(Options{Filter: f, Index: index})
	execution := gateExecution(c, consumerObj.Named())
	input := values.NewStoreProvider(&execution.Reads, false, c.SubstituteFrom).Lookup(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: consumer.Namespace, Name: cmID.Name})
	if !slices.Equal(input.Materializers, []manifest.NamedResource{producer}) || !input.Offline {
		t.Fatalf("missing unselected materializer evidence: %+v", input)
	}
	if deps := execution.authoredDependenciesForTest(consumerObj); len(deps) != 0 {
		t.Fatalf("materializer became required gate: %v", deps)
	}

}

func TestCollectDeps_SkipsSelfProducer(t *testing.T) {
	t.Parallel()
	selfObj := &manifest.Kustomization{
		Name: "app", Namespace: "foo",
		Path: "kubernetes/foo/app",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "settings"},
		}},
	}
	self := selfObj.Named()
	cmObj := &manifest.ConfigMap{Name: "settings", Namespace: self.Namespace}
	cmID := cmObj.Named()

	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{self: {File: "kubernetes/foo/app/ks.yaml"}, cmID: {File: "kubernetes/foo/app/settings.yaml", Productions: testutil.ProductionEvidence([]manifest.NamedResource{self})}}, Claims: map[string][]manifest.NamedResource{"kubernetes/foo/app/": {self}}}
	fixture := store.New()
	fixture.AddObjects([]manifest.BaseManifest{selfObj, cmObj})
	fixture.SetDiscoveryIndex(index)
	f := change.NewFilter(change.NewSet([]string{"kubernetes/foo/app/changed.yaml"}), index, fixture)

	c := New(store.New(), nil)
	c.Configure(Options{Filter: f, Index: index})
	if owners := c.Store.Productions(cmID, index); len(owners) != 1 || owners[0].Owner != self {
		t.Fatalf("missing self evidence: %v", owners)
	}
	deps := gateExecution(c, selfObj.Named()).authoredDependenciesForTest(selfObj)

	for _, d := range deps {
		if d.NamedResource == self {
			t.Errorf("self-producer must not appear as its own dep; got %+v", deps)
		}
	}
}

func TestSubstitutionAvailability_RuntimeDeclaration(t *testing.T) {
	t.Parallel()

	s := store.New()
	c := New(s, nil)
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	s.AddObject(producer)
	declaration := &manifest.RawObject{APIVersion: "external-secrets.io/v1", Kind: "ExternalSecret", Name: "declaration", Namespace: "apps", Spec: map[string]any{"target": map[string]any{"name": "credentials", "template": map[string]any{"data": map[string]any{"X": "remote"}}}}}
	c.Emit(producer.Named(), &store.Emission{Children: []store.Child{{Obj: declaration}}, Complete: true}, nil)
	consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindSecret, Name: "credentials"}}}}
	execution := gateExecution(c, consumer.Named())
	target := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: consumer.Namespace, Name: "credentials"}
	input := values.NewStoreProvider(&execution.Reads, true, c.SubstituteFrom).Lookup(target)
	if !input.Generated || input.Offline || len(input.Materializers) != 0 {
		t.Fatalf("runtime declaration evidence=%+v", input)
	}
	if input.Data == nil {
		t.Fatal("declared keys did not retain placeholder data")
	}
	if value, _, err := input.Data.Value("X"); err != nil || value != "..PLACEHOLDER_X.." {
		t.Fatalf("placeholder=%q error=%v", value, err)
	}
	if deps := execution.authoredDependenciesForTest(consumer); len(deps) != 0 {
		t.Fatalf("runtime declaration became required gate: %v", deps)
	}

}

func TestCollectDeps_DeclarationAvailabilityUsesCommonGate(t *testing.T) {
	t.Parallel()

	parent := testutil.KustomizationID("flux-system", "cluster-apps")
	child := &manifest.Kustomization{Name: "karma", Namespace: "observability"}
	for _, name := range []string{"predicted", "provisional seed", "file evidence", "self declared", "accepted supporter", "self declared with accepted supporter"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			index := parentIndexForTest(map[manifest.NamedResource]manifest.NamedResource{child.Named(): parent})
			if name != "predicted" {
				r := index.Records[child.Named()]
				r.Productions = []manifest.Production{{Source: manifest.BootstrapSourceID}}
				index.Records[child.Named()] = r
			}
			if name == "file evidence" {
				r := index.Records[child.Named()]
				r.Productions = []manifest.Production{{Owner: manifest.NamedResource{Kind: manifest.KindKustomization, Name: "file-owner"}, Path: "child.yaml"}}
				index.Records[child.Named()] = r
			}
			if strings.HasPrefix(name, "self declared") {
				r := index.Records[child.Named()]
				r.Productions = []manifest.Production{{Owner: child.Named(), Source: manifest.BootstrapSourceID}}
				index.Records[child.Named()] = r
			}
			st := store.New()
			if strings.Contains(name, "accepted supporter") {
				st.AddObject(child)
				st.SetDiscoveryIndex(index)
				other := &manifest.Kustomization{Name: "other", Namespace: child.Namespace}
				st.AddObject(other)
				st.RecordEmitted(other.Named(), captureEmission(st, other.Named(), &store.Emission{Children: []store.Child{{Obj: child}}, Complete: true}), nil)
			}
			c := New(st, nil)
			c.Configure(Options{Index: index})
			deps := gateExecution(c, child.Named()).dependencies(child, manifest.GateInputs)
			if len(deps) != 0 {
				t.Fatalf("structural declaration owners leaked into authored dependencies: %v", deps)
			}
		})
	}
}

func TestCollectDeps_OrderingFilter(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                      string
		active, selected, indexed bool
		present                   bool
		status                    store.Status
		retain                    bool
		noData                    bool
	}{
		{name: "unfiltered missing", retain: true},
		{name: "no data", retain: true, noData: true},
		{name: "known file excluded", active: true, indexed: true},
		{name: "render generated excluded", active: true, present: true},
		{name: "unknown retained", active: true, retain: true},
		{name: "selected ready", active: true, selected: true, present: true, status: store.StatusReady, retain: true},
		{name: "selected failed", active: true, selected: true, present: true, status: store.StatusFailed, retain: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			target := &manifest.Kustomization{Name: "target", Namespace: "apps", Path: "./target"}
			if tc.present {
				st.AddObject(target)
				st.UpdateStatus(target.Named(), store.StatusInfo{Status: tc.status, Reason: "", Message: "fixture"})
			}
			var changes *change.Set
			if tc.active {
				files := []string{"consumer.yaml"}
				if tc.selected {
					files = append(files, "target.yaml")
				}
				changes = change.NewSet(files)
			}
			consumer := &manifest.Kustomization{
				Name: "consumer", Namespace: "apps",
				DependsOn: []kustomizev1.DependencyReference{{Name: target.Name, Namespace: target.Namespace, ReadyExpr: "dep.spec.suspend == false"}, {Name: target.Name, Namespace: target.Namespace, ReadyExpr: "true"}},
				SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "source", Namespace: "apps"},
				PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "optional", Optional: true}, {Kind: manifest.KindConfigMap, Name: "ignored"}, {Kind: "Unsupported", Name: "unsupported"}, {Kind: manifest.KindConfigMap, Name: "self"}, {Kind: manifest.KindConfigMap, Name: "values"}}},
			}
			if tc.noData {
				consumer.PostBuild = nil
			}
			filter := change.NewFilter(changes, &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{consumer.Named(): {File: "consumer.yaml"}, target.Named(): {File: "target.yaml"}}}, st)
			parent := testutil.KustomizationID("apps", "parent")
			producers := []manifest.NamedResource{testutil.KustomizationID("apps", "second"), testutil.KustomizationID("apps", "first")}
			slices.SortFunc(producers, manifest.NamedResource.Compare)
			c := New(st, nil)
			c.Configure(Options{Index: &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{consumer.Named(): {Productions: []manifest.Production{{Owner: parent}}}, {Kind: manifest.KindConfigMap, Namespace: "apps", Name: "values"}: {Productions: testutil.ProductionEvidence(producers)}, {Kind: manifest.KindConfigMap, Namespace: "apps", Name: "self"}: {Productions: testutil.ProductionEvidence([]manifest.NamedResource{consumer.Named()})}}}, Filter: filter, Existence: indexedOrderingDeps{target.Named(): tc.indexed}, SubstituteFrom: map[manifest.NamedResource]manifest.BaseManifest{{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "ignored"}: &manifest.ConfigMap{}}})
			size := len(filter.KeepNames())
			deps := gateExecution(c, consumer.Named()).authoredDependenciesForTest(consumer)
			var want []manifest.DependencyRef
			if tc.retain {
				want = append(want, manifest.DependencyRef{NamedResource: target.Named(), ReadyExpr: new("dep.spec.suspend == false")}, manifest.DependencyRef{NamedResource: target.Named(), ReadyExpr: new("true")})
			}
			want = append(want, manifest.DependencyRef{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "source"})
			assert.Diff(t, deps, want)
			if len(filter.KeepNames()) != size || len(consumer.DependsOn) != 2 {
				t.Error("ordering collection must preserve keep and input edges")
			}
		})
	}
}
