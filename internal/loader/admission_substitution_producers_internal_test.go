package loader

import (
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/change"
	"github.com/home-operations/flate/internal/filtertest"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

// TestFilter_SubstituteFromConfigMapKeepsProducerKustomization pins
// issue #418: a kept Kustomization with a non-Optional
// postBuild.substituteFrom ConfigMap must pull the producer KS that
// renders that CM into keep, even when the producer's own files
// didn't change. Without this, changed-only mode fails reconcile
// with "ConfigMap/flux-system/cluster-settings: dependency not found"
// because the producer never runs and the CM never materializes.
//
// The producer joins keep as ancestor-only — NOT primary — so its
// render output for unrelated siblings can't cascade into changed-
// only scope (the #204 keep-cascade rationale).
func TestFilter_SubstituteFromConfigMapKeepsProducerKustomization(t *testing.T) {
	t.Parallel()
	clusterAppsObj := &manifest.Kustomization{
		Name: "cluster-apps", Namespace: "flux-system",
		Path: "kubernetes/apps",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "cluster-settings"},
		}},
	}
	clusterVarsObj := &manifest.Kustomization{
		Name: "cluster-vars", Namespace: "flux-system",
		Path: "kubernetes/flux/vars",
		// The CM lives in a shared Component referenced by
		// cluster-vars — ownersOf returns cluster-vars for any
		// file under the resolved component path.
		Components: []string{"../../components/cluster-settings"},
	}
	ntfyObj := &manifest.Kustomization{
		Name: "ntfy", Namespace: "communication",
		Path: "kubernetes/apps/communication/ntfy/app",
	}
	hrObj := &manifest.HelmRelease{Name: "ntfy", Namespace: "communication"}
	// rawSettings is the discovery-indexed pre-render form:
	// Namespace="" because kustomize's namespace directive hasn't
	// run yet at file-walk time.
	rawSettings := &manifest.ConfigMap{Name: "cluster-settings", Namespace: ""}
	rawSettingsID := rawSettings.Named()

	clusterApps := clusterAppsObj.Named()
	clusterVars := clusterVarsObj.Named()
	ntfy := ntfyObj.Named()
	hr := hrObj.Named()
	// renderedSettings is the form the consumer KS waits on once
	// kustomize's namespace directive has stamped flux-system onto
	// the rendered CM.
	renderedSettings := manifest.NamedResource{
		Kind:      manifest.KindConfigMap,
		Namespace: "flux-system",
		Name:      "cluster-settings",
	}

	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{"kubernetes/apps/communication/ntfy/app/helmrelease.yaml"}),
			index: new(filterFixture{files: map[manifest.NamedResource]string{
				clusterApps:   "kubernetes/flux/cluster-apps.yaml",
				clusterVars:   "kubernetes/flux/cluster-vars.yaml",
				ntfy:          "kubernetes/apps/communication/ntfy/ks.yaml",
				hr:            "kubernetes/apps/communication/ntfy/app/helmrelease.yaml",
				rawSettingsID: "kubernetes/components/cluster-settings/cluster-settings.yaml",
			}, root: "", objects: filtertest.MapLister{
				clusterApps:   clusterAppsObj,
				clusterVars:   clusterVarsObj,
				ntfy:          ntfyObj,
				hr:            hrObj,
				rawSettingsID: rawSettings,
			}}).index(),
			inventory: filtertest.MapLister{
				clusterApps:   clusterAppsObj,
				clusterVars:   clusterVarsObj,
				ntfy:          ntfyObj,
				hr:            hrObj,
				rawSettingsID: rawSettings,
			},
			root: "",
		},
	)

	// The leaf HR's file changed, so ntfy + the cluster-apps
	// ancestor are pulled in; the rendered ConfigMap is a
	// substituteFrom dep of cluster-apps; and cluster-vars is the
	// producer of that ConfigMap.
	for _, id := range []manifest.NamedResource{clusterApps, ntfy, hr, renderedSettings, clusterVars} {
		if !f.Reconcile(id) {
			t.Errorf("expected %s in keep; keep=%v", id, f.KeepNames())
		}
	}
	// The producer is ancestor-only: AddEmitted from cluster-vars
	// to a sentinel child must reject (producer is NOT primary).
	sentinel := testutil.KustomizationID("unrelated", "sentinel")
	f.AddEmitted(clusterVars, new(store.Child{Obj: refObj(sentinel)}), f.objs)
	if f.Reconcile(sentinel) {
		t.Errorf(
			"producer cluster-vars must be ancestor-only (NOT primary) — AddEmitted leaked sentinel into keep; keep=%v",
			f.KeepNames(),
		)
	}
}

// TestFilter_AddEmittedKeepsSubstituteFromProducerDependencyOnly
// covers the runtime AddEmitted path: a primary parent KS emits a
// child KS at render time, and that child has a substituteFrom CM
// produced by an unchanged third KS. The producer must land in keep
// (so the CM materializes) but stay dependency-only — its render
// output for unrelated siblings is unchanged, so promoting it would
// re-introduce the #204 cascade. See #418.
func TestFilter_AddEmittedKeepsSubstituteFromProducerDependencyOnly(t *testing.T) {
	t.Parallel()
	parent := testutil.KustomizationID("flux-system", "parent-apps")
	childObj := &manifest.Kustomization{
		Name: "child-app", Namespace: "apps",
		Path: "kubernetes/apps/child",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "shared-settings"},
		}},
	}
	child := childObj.Named()
	producerObj := &manifest.Kustomization{
		Name: "settings-producer", Namespace: "apps",
		Path: "kubernetes/apps/settings",
	}
	producer := producerObj.Named()
	// CM with namespace="" mirrors the discovery pre-render form.
	cmID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "", Name: "shared-settings"}
	cmObj := &manifest.ConfigMap{Name: cmID.Name, Namespace: cmID.Namespace}

	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{"kubernetes/parent/parent.yaml"}),
			index: new(filterFixture{files: map[manifest.NamedResource]string{
				parent:   "kubernetes/parent/parent.yaml",
				producer: "kubernetes/apps/settings/ks.yaml",
				cmID:     "kubernetes/apps/settings/cm.yaml",
			}, root: "", objects: filtertest.MapLister{producer: producerObj, cmID: cmObj}}).index(),
			inventory: filtertest.MapLister{
				parent:   &manifest.Kustomization{Name: parent.Name, Namespace: parent.Namespace},
				producer: producerObj, cmID: cmObj,
			},
			root: "",
		},
	)

	if !f.Reconcile(parent) {
		t.Fatalf("parent must be primary from direct file change; keep=%v", f.KeepNames())
	}
	if f.Reconcile(producer) {
		t.Fatalf("precondition: producer must NOT be in keep before AddEmitted runs; keep=%v", f.KeepNames())
	}

	added := f.AddEmitted(parent, new(store.Child{Obj: childObj}), f.objs)

	data := cmID
	data.Namespace = child.Namespace
	for _, id := range []manifest.NamedResource{child, data, producer} {
		if !f.Reconcile(id) {
			t.Errorf("expected %s in keep; keep=%v", id, f.KeepNames())
		}
		assert.ErrorIf(t, !slices.Contains(added, id), "AddEmitted must return %s; added=%v", id, added)
	}
	// Producer is dependency-only: AddEmitted from producer to a
	// sentinel child must reject because producer is not primary.
	sentinel := testutil.KustomizationID("unrelated", "sentinel")
	f.AddEmitted(producer, new(store.Child{Obj: refObj(sentinel)}), f.objs)
	if f.Reconcile(sentinel) {
		t.Errorf(
			"producer must be dependency-only (NOT primary) — AddEmitted leaked sentinel into keep; keep=%v",
			f.KeepNames(),
		)
	}
}

// Dependency-only producers must be returned so callers can refire their arrivals.
func TestFilter_AddEmittedReturnsSubstituteFromProducer(t *testing.T) {
	t.Parallel()
	parent := testutil.KustomizationID("flux-system", "parent-apps")
	childObj := &manifest.Kustomization{
		Name: "child-app", Namespace: "apps",
		Path: "kubernetes/apps/child",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindSecret, Name: "shared-settings", Optional: true},
		}},
	}
	child := childObj.Named()
	producerObj := &manifest.Kustomization{
		Name: "settings-producer", Namespace: "apps",
		Path: "kubernetes/apps/settings",
	}
	producer := producerObj.Named()
	dataID := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "", Name: "shared-settings"}
	dataObj := &manifest.Secret{Name: dataID.Name, Namespace: dataID.Namespace}

	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{"kubernetes/parent/parent.yaml"}),
			index: new(filterFixture{files: map[manifest.NamedResource]string{
				parent:   "kubernetes/parent/parent.yaml",
				producer: "kubernetes/apps/settings/ks.yaml",
				dataID:   "kubernetes/apps/settings/secret.yaml",
			}, root: "", objects: filtertest.MapLister{producer: producerObj, dataID: dataObj}}).index(),
			inventory: filtertest.MapLister{
				parent:   &manifest.Kustomization{Name: parent.Name, Namespace: parent.Namespace},
				producer: producerObj, dataID: dataObj,
			},
			root: "",
		},
	)

	added := f.AddEmitted(parent, new(store.Child{Obj: childObj}), f.objs)

	data := dataID
	data.Namespace = child.Namespace
	for _, id := range []manifest.NamedResource{child, data, producer} {
		if !f.Reconcile(id) {
			t.Errorf("expected %s in keep; keep=%v", id, f.KeepNames())
		}
		assert.ErrorIf(t, !slices.Contains(added, id), "AddEmitted must return %s; added=%v", id, added)
	}
	sentinel := testutil.KustomizationID("unrelated", "sentinel")
	if added := f.AddEmitted(
		producer,
		new(store.Child{Obj: refObj(sentinel)}),
		f.objs,
	); len(added) != 0 ||
		f.Reconcile(sentinel) {
		t.Errorf("dependency-only producer must reject unrelated child; added=%v, keep=%v", added, f.KeepNames())
	}
}

// Optional producer inputs must retain the complete chain as dependency-only.
func TestFilter_ChainedSubstituteFromProducers(t *testing.T) {
	t.Parallel()
	aObj := &manifest.Kustomization{
		Name: "consumer", Namespace: "flux-system",
		Path: "kubernetes/consumer",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "cm-a", Optional: true},
		}},
	}
	bObj := &manifest.Kustomization{
		Name: "producer-b", Namespace: "flux-system",
		Path: "kubernetes/b",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindSecret, Name: "cm-b", Optional: true},
		}},
	}
	cObj := &manifest.Kustomization{
		Name: "producer-c", Namespace: "flux-system",
		Path: "kubernetes/c",
	}
	a, b, c := aObj.Named(), bObj.Named(), cObj.Named()

	cmA := &manifest.ConfigMap{Name: "cm-a", Namespace: ""}
	cmAID := cmA.Named()
	cmB := &manifest.Secret{Name: "cm-b", Namespace: ""}
	cmBID := cmB.Named()

	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{"kubernetes/consumer/file.yaml"}),
			index: new(filterFixture{files: map[manifest.NamedResource]string{
				a:     "kubernetes/consumer/ks.yaml",
				b:     "kubernetes/b/ks.yaml",
				c:     "kubernetes/c/ks.yaml",
				cmAID: "kubernetes/b/cm-a.yaml",
				cmBID: "kubernetes/c/cm-b.yaml",
			}, root: "", objects: filtertest.MapLister{a: aObj, b: bObj, c: cObj, cmAID: cmA, cmBID: cmB}}).index(),
			inventory: filtertest.MapLister{a: aObj, b: bObj, c: cObj, cmAID: cmA, cmBID: cmB},
			root:      "",
		},
	)

	if !f.Reconcile(a) {
		t.Fatalf("consumer A must be in keep (file change); keep=%v", f.KeepNames())
	}
	if !f.Reconcile(b) {
		t.Errorf("producer B (renders cm-a) must be in keep; keep=%v", f.KeepNames())
	}
	if !f.Reconcile(c) {
		t.Errorf("chained producer C (renders cm-b consumed by B) must be in keep; keep=%v", f.KeepNames())
	}
}

// TestFilter_SelfProducerSkippedInResolve pins the bjw-s
// self-substitute pattern: a KS may produce its own
// postBuild.substituteFrom CM. The BFS must NOT enqueue the KS as
// its own ancestor (would infinite-loop or self-edge). See #418.
func TestFilter_SelfProducerSkippedInResolve(t *testing.T) {
	t.Parallel()
	selfObj := &manifest.Kustomization{
		Name: "self", Namespace: "flux-system",
		Path: "kubernetes/self",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "self-settings"},
		}},
	}
	self := selfObj.Named()
	// CM lives at spec.path of self — so ownersOf returns self.
	cmObj := &manifest.ConfigMap{Name: "self-settings", Namespace: ""}
	cmID := cmObj.Named()

	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{"kubernetes/self/changed.yaml"}),
			index: new(filterFixture{files: map[manifest.NamedResource]string{
				self: "kubernetes/self/ks.yaml",
				cmID: "kubernetes/self/cm.yaml",
			}, root: "", objects: filtertest.MapLister{self: selfObj, cmID: cmObj}}).index(),
			inventory: filtertest.MapLister{self: selfObj, cmID: cmObj},
			root:      "",
		},
	)

	if !f.Reconcile(self) {
		t.Errorf("self KS must be in keep; keep=%v", f.KeepNames())
	}
	// Sanity: no infinite loop happened (we made it here). Also
	// confirm self only appears once in the producer index — not as
	// its own ancestor via enqueueAncestor.
	prods := f.productionOwners(
		manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "self-settings"},
	)
	assert.ErrorIf(
		t,
		!slices.Contains(prods, self),
		"producers index should still contain self for cm-by-name lookup; got=%v",
		prods,
	)
}

// TestFilter_ProducerByNameDoesNotLeakAcrossNamespaces pins the
// asymmetric producer-byName rule, mirroring
// TestFilter_KeepByNameDoesNotLeakAcrossNamespaces. A producer KS
// renders a CM in namespace ns1; an unrelated producer in ns2
// renders a same-named CM in ns2. A Productions query on
// CM/ns1/shared MUST return only the ns1 producer — never the ns2
// one. See #418.
func TestFilter_ProducerByNameDoesNotLeakAcrossNamespaces(t *testing.T) {
	t.Parallel()
	// Both producer KSes spec a CM named "shared". The CMs land at
	// distinct on-disk paths, so ownersOf attributes each producer
	// only to its own CM file.
	aObj := &manifest.Kustomization{
		Name: "producer-a", Namespace: "ns1",
		Path: "p1",
	}
	bObj := &manifest.Kustomization{
		Name: "producer-b", Namespace: "ns2",
		Path: "p2",
	}
	a, b := aObj.Named(), bObj.Named()

	// The discovery-indexed CMs are in their respective on-disk
	// paths but have explicit namespace already (so they land in
	// producersByID only — NOT producersByName).
	cmNS1 := &manifest.ConfigMap{Name: "shared", Namespace: "ns1"}
	cmNS1ID := cmNS1.Named()
	cmNS2 := &manifest.ConfigMap{Name: "shared", Namespace: "ns2"}
	cmNS2ID := cmNS2.Named()

	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{"unrelated.yaml"}),
			index: new(filterFixture{files: map[manifest.NamedResource]string{
				a:       "p1/ks.yaml",
				b:       "p2/ks.yaml",
				cmNS1ID: "p1/cm.yaml",
				cmNS2ID: "p2/cm.yaml",
			}, root: "", objects: filtertest.MapLister{a: aObj, b: bObj, cmNS1ID: cmNS1, cmNS2ID: cmNS2}}).index(),
			inventory: filtertest.MapLister{a: aObj, b: bObj, cmNS1ID: cmNS1, cmNS2ID: cmNS2},
			root:      "",
		},
	)

	gotNS1 := f.productionOwners(cmNS1ID)
	assert.ErrorIf(
		t,
		!slices.Equal(gotNS1, []manifest.NamedResource{a}),
		"production owners(CM/ns1/shared) leaked across namespaces: got %v, want [%v]",
		gotNS1,
		a,
	)
	gotNS2 := f.productionOwners(cmNS2ID)
	assert.ErrorIf(
		t,
		!slices.Equal(gotNS2, []manifest.NamedResource{b}),
		"production owners(CM/ns2/shared) leaked across namespaces: got %v, want [%v]",
		gotNS2,
		b,
	)
}

func TestFilter_SubstitutionGeneratorProducers(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, kind string
		optional   bool
		emptyName  bool
	}{
		{name: "ConfigMap", kind: manifest.KindConfigMap},
		{name: "Secret", kind: manifest.KindSecret},
		{name: "optional ConfigMap", kind: manifest.KindConfigMap, optional: true},
		{name: "optional Secret", kind: manifest.KindSecret, optional: true},
		{name: "empty optional ConfigMap", kind: manifest.KindConfigMap, optional: true, emptyName: true},
		{name: "empty optional Secret", kind: manifest.KindSecret, optional: true, emptyName: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFilterSubstitutionGeneratorProducersCase(t, &tc)
		},
		)
	}
	t.Run(
		"qualified_declarations",
		func(t *testing.T) { t.Parallel(); testFilterSubstitutionGeneratorProducersQualifiedDeclarations(t) },
	)
}

func testFilterSubstitutionGeneratorProducersCase(
	t *testing.T,
	testCaseInput *struct {
		name      string
		kind      string
		optional  bool
		emptyName bool
	},
) {
	t.Helper()

	name := "settings"
	if testCaseInput.emptyName {
		name = ""
	}
	consumer := &manifest.Kustomization{
		Name:      "consumer",
		Namespace: "apps",
		Path:      "app",
		PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []manifest.SubstituteReference{
				{Kind: testCaseInput.kind, Name: name, Optional: testCaseInput.optional},
			},
		},
	}
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps", Path: "data"}
	data := manifest.NamedResource{Kind: testCaseInput.kind, Namespace: "apps", Name: name}
	index := &manifest.DiscoveryIndex{
		Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			consumer.Named(): {File: "flux/consumer.yaml"},
			producer.Named(): {File: "flux/producer.yaml"},
			data: {
				File:        "data/kustomization.yaml",
				Productions: testutil.ProductionEvidence([]manifest.NamedResource{producer.Named()}),
			},
		},
		Claims: map[string][]manifest.NamedResource{"app/": {consumer.Named()}, "data/": {producer.Named()}},
	}
	f := newAdmission(
		t,
		admissionConfig{
			changes:   change.NewSet([]string{"app/cm.yaml"}),
			index:     index,
			inventory: filtertest.MapLister{consumer.Named(): consumer, producer.Named(): producer},
			root:      "",
		},
	)
	for _, id := range []manifest.NamedResource{data, producer.Named()} {
		if got := f.Reconcile(id); got == testCaseInput.emptyName {
			t.Errorf("%s retained = %t, empty reference name = %t", id, got, testCaseInput.emptyName)
		}
	}
	sentinel := testutil.KustomizationID("apps", "unrelated")
	if added := f.AddEmitted(
		producer.Named(),
		new(store.Child{Obj: refObj(sentinel)}),
		f.objs,
	); len(added) != 0 ||
		f.Reconcile(sentinel) {
		t.Errorf(
			"dependency-only producer must reject unrelated child; added=%v, keep=%v",
			added,
			f.KeepNames(),
		)
	}
}

func testFilterSubstitutionGeneratorProducersQualifiedDeclarations(t *testing.T) {
	t.Helper()

	stores := [2]*store.Store{store.New(), store.New()}
	indexes := [2]*manifest.DiscoveryIndex{}
	roots := [2]string{t.TempDir(), t.TempDir()}
	source := &manifest.GitRepository{Name: "selected", Namespace: "apps"}
	producer := &manifest.Kustomization{
		Name:      "producer",
		Namespace: "apps",
		Path:      "data",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: source.Named().Kind, Name: source.Name},
	}
	consumer := &manifest.Kustomization{
		Name:      "consumer",
		Namespace: "apps",
		PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []manifest.SubstituteReference{
				{Kind: manifest.KindConfigMap, Name: "settings"},
				{Kind: manifest.KindSecret, Name: "secret"},
			},
		},
	}
	for i, st := range stores {
		st.AddObjects([]manifest.BaseManifest{source, producer, consumer})
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: roots[i], FS: fstest.MapFS{}})
		mounted := filepath.Join(roots[i], ".mount")
		st.SetArtifact(
			source.Named(),
			&store.SourceArtifact{
				LocalPath:  mounted,
				LocalRoot:  filepath.Join(roots[i], "subtree"),
				InputAlias: true,
				FS: fstest.MapFS{
					"data/kustomization.yaml": {
						Data: []byte(
							"namespace: apps\nresources: [producer.yaml]\nconfigMapGenerator:\n- name: settings\n  literals: [value=" + []string{"before", "after"}[i] + "]\n",
						),
					},
					"data/producer.yaml": {
						Data: []byte(
							"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: generated}\nspec: {target: {name: secret}}\n",
						),
					},
				},
			},
		)
		l := New(st)
		l.SourceRoot = roots[i]
		_, err := l.Load(t.Context(), filepath.Join(mounted, "data"), manifest.NamedResource{}, nil)
		assert.NoError(t, err)
		l.FinalizeIndex(t.Context(), roots[i], true)
		indexes[i] = l.Index
		for _, target := range []manifest.NamedResource{{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}, {Kind: manifest.KindSecret, Namespace: "apps", Name: "secret"}} {
			evidence := st.Productions(target, l.Index)
			assert.FatalIf(t, len(evidence) == 0, "missing evidence for %s", target)
			for i := range evidence {
				p := evidence[i]
				if p.File != "" {
					assert.Equal(t, p.Source, source.Named())
				}
				if p.Path != "" {
					assert.Equal(t, filepath.IsAbs(p.Path), true)
				}
			}
		}
	}
	changes, err := change.Qualify(
		t.Context(),
		change.NewSet(nil),
		[2]*store.Store{stores[0], stores[1]},
		manifest.WorkScope{},
	)
	assert.NoError(t, err)
	for i, st := range stores {
		filter := newAdmission(
			t,
			admissionConfig{changes: changes, index: indexes[i], inventory: st, root: roots[i]},
		)
		assert.Equal(t, filter.Reconcile(consumer.Named()), true)
		assert.Equal(t, filter.Reconcile(producer.Named()), true)
		assert.Equal(
			t,
			filter.Reconcile(
				manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"},
			),
			true,
		)
	}
}
