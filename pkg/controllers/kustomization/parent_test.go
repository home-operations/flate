package kustomization

import (
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/schedule"

	sourcecache "github.com/home-operations/flate/pkg/source/cache"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestCollectDeps_AppendsStructuralParent(t *testing.T) {
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
			want := parent
			if strings.Contains(name, "accepted supporter") {
				st.AddObject(child)
				st.SetDiscoveryIndex(index)
				other := &manifest.Kustomization{Name: "other", Namespace: child.Namespace}
				st.AddObject(other)
				st.RecordEmitted(other.Named(), &store.Emission{Children: []store.Child{{Obj: child}}, Complete: true})
				want = other.Named()
			}
			c := New(st, nil, nil, false)
			c.Configure(Options{Index: index}, sourcecache.RetryConfig{})
			deps := c.newExecution(child.Named()).collectDeps(child)
			var count int
			for _, d := range deps {
				if d.NamedResource == want {
					count++
				}
			}
			if count != 1 || len(deps) != 1 {
				t.Fatalf("dependency %s occurrences=%d in deps %v", want, count, deps)
			}
		})
	}
}

func TestCollectDeps_NoParentNoExtraDep(t *testing.T) {
	t.Parallel()
	child := &manifest.Kustomization{Name: "karma", Namespace: "observability"}
	c := New(store.New(), nil, nil, false)
	deps := c.newExecution(child.Named()).collectDeps(child)
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
			c := New(store.New(), nil, nil, false)
			c.Configure(Options{Index: index}, sourcecache.RetryConfig{})
			ks := &manifest.Kustomization{Name: consumer.Name, Namespace: consumer.Namespace, PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: tc.kind, Name: id.Name, Optional: tc.optional}}}}
			execution := c.newExecution(consumer)
			if deps := execution.collectDeps(ks); len(deps) != 0 {
				t.Fatalf("data became authored readiness gate: %v", deps)
			}
			input := execution.data.Lookup(id)
			w := depwait.Waiter{Store: &execution.run.Reads, Parent: consumer}
			got := w.Available(t.Context(), depwait.DataDependency{NamedResource: id, Input: input, Optional: tc.optional, AllowMissing: tc.kind == manifest.KindSecret}, manifest.DrainNone)
			if got.Kind != tc.want {
				t.Fatalf("availability=%+v want=%v", got, tc.want)
			}
			if tc.known && !tc.self && !slices.Equal(got.Blocked, []manifest.NamedResource{producer}) {
				t.Fatalf("materializer was not observed: %+v", got)
			}
			for required := range execution.run.Reads.Required() {
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
	f := change.NewFilter(change.NewSet([]string{"kubernetes/apps/changed.yaml"}), index, controllertest.MapLister{consumer: consumerObj, producer: producerObj, cmID: cmObj})

	c := New(store.New(), nil, nil, false)
	c.Configure(Options{Filter: f, Index: index}, sourcecache.RetryConfig{})
	execution := c.newExecution(consumerObj.Named())
	input := execution.data.Lookup(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: consumer.Namespace, Name: cmID.Name})
	if !slices.Equal(input.Materializers, []manifest.NamedResource{producer}) || !input.Offline {
		t.Fatalf("missing unselected materializer evidence: %+v", input)
	}
	if deps := execution.collectDeps(consumerObj); len(deps) != 0 {
		t.Fatalf("materializer became required gate: %v", deps)
	}

}

// TestCollectDeps_SkipsSelfProducer pins the bjw-s self-substitute
// pattern: a KS that produces its own substituteFrom ConfigMap must
// NOT be appended as a dep on itself (would self-loop in depwait).
// See #418.
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
	f := change.NewFilter(change.NewSet([]string{"kubernetes/foo/app/changed.yaml"}), index, controllertest.MapLister{self: selfObj, cmID: cmObj})

	c := New(store.New(), nil, nil, false)
	c.Configure(Options{Filter: f, Index: index}, sourcecache.RetryConfig{})
	if owners := c.Store.Productions(cmID, index); len(owners) != 1 || owners[0].Owner != self {
		t.Fatalf("missing self evidence: %v", owners)
	}
	deps := c.newExecution(selfObj.Named()).collectDeps(selfObj)

	for _, d := range deps {
		if d.NamedResource == self {
			t.Errorf("self-producer must not appear as its own dep; got %+v", deps)
		}
	}
}

func TestSubstitutionAvailability_RuntimeDeclaration(t *testing.T) {
	t.Parallel()

	s := store.New()
	c := New(s, nil, nil, true)
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	s.AddObject(producer)
	declaration := &manifest.RawObject{APIVersion: "external-secrets.io/v1", Kind: "ExternalSecret", Name: "declaration", Namespace: "apps", Spec: map[string]any{"target": map[string]any{"name": "credentials", "template": map[string]any{"data": map[string]any{"X": "remote"}}}}}
	c.Emit(producer.Named(), &store.Emission{Children: []store.Child{{Obj: declaration}}, Complete: true})
	consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindSecret, Name: "credentials"}}}}
	execution := c.newExecution(consumer.Named())
	target := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: consumer.Namespace, Name: "credentials"}
	input := execution.data.Lookup(target)
	if !input.Generated || input.Offline || len(input.Materializers) != 0 {
		t.Fatalf("runtime declaration evidence=%+v", input)
	}
	if input.Data == nil {
		t.Fatal("declared keys did not retain placeholder data")
	}
	if value, _, err := input.Data.Value("X"); err != nil || value != "..PLACEHOLDER_X.." {
		t.Fatalf("placeholder=%q error=%v", value, err)
	}
	if deps := execution.collectDeps(consumer); len(deps) != 0 {
		t.Fatalf("runtime declaration became required gate: %v", deps)
	}

}

func TestReconcile_OptionalMaterializerAbsence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		status   store.Status
		required bool
	}{
		{name: "failed optional", status: store.StatusFailed},
		{name: "exhausted optional", status: store.StatusPending},
		{name: "required cause only", status: store.StatusFailed, required: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, s, _ := newControllerWithFixture(t)
			producer := &manifest.Kustomization{Name: "optional-owner", Namespace: "flux-system"}
			s.AddObject(producer)
			s.UpdateStatus(producer.Named(), tc.status, "", "optional materializer unavailable")
			optional := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: producer.Namespace, Name: "optional"}
			required := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: producer.Namespace, Name: "required"}
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{optional: {Productions: []manifest.Production{{Owner: producer.Named()}}}}}
			c.Store.SetDiscoveryIndex(index)
			ks := &manifest.Kustomization{Name: "reader", Namespace: producer.Namespace, Path: "./apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: optional.Kind, Name: optional.Name, Optional: true}}}}
			if tc.required {
				ks.PostBuild.SubstituteFrom = append(ks.PostBuild.SubstituteFrom, manifest.SubstituteReference{Kind: required.Kind, Name: required.Name})
			}
			s.AddObject(ks)
			assessment := c.ReconcileNode(t.Context(), ks.Named(), manifest.DrainNone)
			defer assessment.Close()
			if tc.status == store.StatusPending || tc.required {
				if assessment.Outcome() != schedule.Blocked || s.GetArtifact(ks.Named()) != nil {
					t.Fatal("reader did not wait for its unresolved input")
				}
				assessment.Settle(t.Context(), manifest.DrainForce)
			}
			info, _ := s.GetStatus(ks.Named())
			if tc.required {
				if info.Status != store.StatusFailed || !slices.Equal(info.BlockedBy(), []manifest.NamedResource{required}) {
					t.Fatalf("required diagnostic causes=%+v blocked=%v", info, info.BlockedBy())
				}
			} else {
				art, _ := s.GetArtifact(ks.Named()).(*store.Emission)
				if info.Status != store.StatusReady || art == nil || len(art.Manifests) != 1 {
					t.Fatalf("absent optional input prevented render: status=%+v artifact=%+v", info, art)
				}
			}
			for id := range assessment.Reads.Required() {
				if id == optional || id == producer.Named() {
					t.Fatalf("optional input entered required footprint: %s", id)
				}
			}
		})
	}
}

func TestReconcile_OwnerDoesNotEraseReadyExpr(t *testing.T) {
	t.Parallel()
	for _, expr := range []string{"false", "true"} {
		t.Run(expr, func(t *testing.T) {
			t.Parallel()
			c, s, _ := newControllerWithFixture(t)
			owner := &manifest.Kustomization{Name: "owner", Namespace: "flux-system"}
			child := &manifest.Kustomization{
				Name: "child", Namespace: owner.Namespace, Path: "./apps",
				DependsOn: []kustomizev1.DependencyReference{{Name: owner.Name, ReadyExpr: expr}},
			}
			s.AddObject(owner)
			c.Emit(owner.Named(), &store.Emission{Children: []store.Child{{Obj: child}}, Complete: true})
			s.UpdateStatus(owner.Named(), store.StatusReady, "", "")
			info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: child.Named(), ReconcileNode: c.ReconcileNode})
			output, _ := s.GetArtifact(child.Named()).(*store.Emission)
			rendered := s.GetObject(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "hello"})
			if expr == "false" {
				if info.Status != store.StatusFailed || !info.IsDependencyFailure() || !slices.Equal(info.BlockedBy(), []manifest.NamedResource{owner.Named()}) || output != nil && len(output.Manifests) != 0 || rendered != nil {
					t.Fatalf("false owner gate permitted output: status=%+v artifact=%+v rendered=%v", info, output, rendered)
				}
			} else if info.Status != store.StatusReady || output == nil || len(output.Manifests) != 1 || rendered == nil {
				t.Fatalf("true owner gate prevented output: status=%+v artifact=%+v rendered=%v", info, output, rendered)
			}
		})
	}
}
