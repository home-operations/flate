package loader

import (
	"slices"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/filtertest"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestFilter_ReverseContentInputs(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		obj      manifest.BaseManifest
		dep      manifest.NamedResource
		inactive bool
	}{
		{"auth", &manifest.GitRepository{Name: "repo", Namespace: "apps", SecretRef: &manifest.LocalObjectReference{Name: "input"}}, manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "input"}, false},
		{"provider", &manifest.ResourceSet{Name: "set", Namespace: "apps", InputsFrom: []fluxopv1.InputProviderReference{{Name: "input"}}}, manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSetInputProvider, Namespace: "apps", Name: "input"}, false},
		{"provider_auth", &manifest.ResourceSetInputProvider{Name: "tags", Namespace: "effective", Type: fluxopv1.InputProviderOCIArtifactTag, SecretRef: &manifest.LocalObjectReference{Name: "input"}}, manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "effective", Name: "input"}, false},
		{"provider_cert", &manifest.ResourceSetInputProvider{Name: "tags", Namespace: "effective", Type: fluxopv1.InputProviderOCIArtifactTag, CertSecretRef: &manifest.LocalObjectReference{Name: "input"}}, manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "effective", Name: "input"}, false},
		{"pre_baked_provider", &manifest.ResourceSetInputProvider{Name: "tags", Namespace: "apps", Type: fluxopv1.InputProviderOCIArtifactTag, SecretRef: &manifest.LocalObjectReference{Name: "input"}, Status: fluxopv1.ResourceSetInputProviderStatus{ExportedInputs: []fluxopv1.ResourceSetInput{{}}}}, manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "input"}, true},
		{"static_provider", &manifest.ResourceSetInputProvider{Name: "tags", Namespace: "apps", Type: fluxopv1.InputProviderStatic, SecretRef: &manifest.LocalObjectReference{Name: "input"}}, manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "input"}, true},
		{"unsupported_provider", &manifest.ResourceSetInputProvider{Name: "tags", Namespace: "apps", Type: fluxopv1.InputProviderACRArtifactTag, SecretRef: &manifest.LocalObjectReference{Name: "input"}}, manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "input"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			objects := filtertest.MapLister{tc.obj.Named(): tc.obj}
			index := new(filterFixture{
				files:   map[manifest.NamedResource]string{tc.obj.Named(): "consumer.yaml", tc.dep: "input.yaml"},
				objects: objects,
			}).index()
			f := newAdmission(
				t,
				admissionConfig{
					changes:   change.NewSet([]string{"input.yaml"}),
					index:     index,
					inventory: objects,
					root:      "",
				},
			)
			assert.Equal(t, f.ShouldReconcile(tc.dep), true)
			assert.Equal(t, f.ShouldReconcile(tc.obj.Named()), !tc.inactive)
		})
	}
}

func TestFilter_ReferenceSnapshotUnchanged(t *testing.T) {
	t.Parallel()
	ks := &manifest.Kustomization{
		Name:      "app",
		Namespace: "apps",
		PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []manifest.SubstituteReference{
				{Kind: manifest.KindConfigMap, Name: "optional", Optional: true},
			},
		},
	}
	source := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "cluster",
	}
	untouched := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "sentinel"}
	backing := []manifest.NamedResource{source, untouched, untouched, untouched}
	want := slices.Clone(backing)
	objects := filtertest.MapLister{ks.Named(): ks}
	index := new(filterFixture{
		files:   map[manifest.NamedResource]string{ks.Named(): "app.yaml", source: "source.yaml"},
		refs:    map[manifest.NamedResource][]manifest.NamedResource{ks.Named(): backing[:1]},
		objects: objects,
	}).index()
	newAdmission(
		t,
		admissionConfig{changes: change.NewSet([]string{"source.yaml"}), index: index, inventory: objects, root: ""},
	)
	assert.Diff(t, backing, want)
}

func TestFilter_ArtifactMaterializerDependencyOnly(t *testing.T) {
	t.Parallel()
	target := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindExternalArtifact,
		Namespace: "apps",
		Name:      "app-api",
	}
	producer := &manifest.Kustomization{
		Name:      "producer",
		Namespace: "apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindExternalArtifact, Name: target.Name},
	}
	consumer := &manifest.Kustomization{
		Name:      "consumer",
		Namespace: "apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindExternalArtifact, Name: target.Name},
	}
	other := &manifest.Kustomization{Name: "other", Namespace: "apps"}
	declaration := manifest.NamedResource{
		Group:     "source.extensions.fluxcd.io",
		Kind:      "ArtifactGenerator",
		Namespace: "apps",
		Name:      "generator",
	}
	otherTarget := target
	otherTarget.Name = "app-unrelated"
	idx := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		consumer.Named(): {File: "consumer.yaml"},
		producer.Named(): {File: "producer.yaml"},
		other.Named():    {File: "other.yaml"},
		target: {
			Productions: []manifest.Production{
				{Owner: producer.Named(), Declaration: declaration},
				{Owner: producer.Named(), Declaration: declaration},
				{Owner: other.Named(), Path: "artifact.yaml", File: "artifact.yaml"},
			},
		},
		otherTarget: {Productions: []manifest.Production{{Owner: other.Named(), Declaration: declaration}}},
	}}
	objects := filtertest.MapLister{consumer.Named(): consumer, producer.Named(): producer, other.Named(): other}
	f := newAdmission(
		t,
		admissionConfig{changes: change.NewSet([]string{"consumer.yaml"}), index: idx, inventory: objects, root: ""},
	)
	if !f.ShouldReconcile(producer.Named()) || f.ShouldReconcile(other.Named()) || f.ShouldReconcile(otherTarget) {
		t.Fatalf("production closure: %v", f.KeepNames())
	}
	assert.Diff(t, f.productionOwners(target), []manifest.NamedResource{producer.Named()})
	sentinel := &manifest.Kustomization{Name: "sentinel", Namespace: "apps"}
	f.AddEmitted(producer.Named(), store.Child{Obj: sentinel}, objects)
	{
		assert.FatalIf(
			t,
			f.ShouldReconcile(sentinel.Named()),
			"%s",
			"dependency-only materializer included unrelated output",
		)
	}
	child := &manifest.Kustomization{Name: "runtime", Namespace: "apps", SourceRef: consumer.SourceRef}
	f.AddEmitted(consumer.Named(), store.Child{Obj: child}, objects)
	{
		assert.FatalIf(
			t,
			!f.ShouldReconcile(child.Named()) || !f.ShouldReconcile(producer.Named()),
			"%s",
			"runtime content dependency lost its producer",
		)
	}
}

func TestFilter_DeclarationSourceChange(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, file string
		declared   bool
		otherScope bool
		pinned     bool
		want       bool
	}{
		{name: "declaration", file: "generator.yaml", declared: true, want: true},
		{name: "ordinary_production", file: "generator.yaml"},
		{name: "unrelated_declaration", file: "other.yaml", declared: true},
		{name: "other_namespace", file: "generator.yaml", declared: true, otherScope: true},
		{name: "fixed_pin", file: "../pinned/generator.yaml", declared: true, pinned: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source := manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindExternalArtifact,
				Namespace: "apps",
				Name:      "generated",
			}
			consumer := &manifest.Kustomization{
				Name:      "consumer",
				Namespace: "apps",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: source.Kind, Name: source.Name},
			}
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
			declaration := manifest.NamedResource{
				Group:     "example.com",
				Kind:      "Generator",
				Namespace: "apps",
				Name:      "declaration",
			}
			production := manifest.Production{Owner: owner.Named(), File: tc.file, Source: manifest.BootstrapSourceID}
			if tc.declared {
				production.Declaration = declaration
			}
			target := source
			if tc.otherScope {
				target.Namespace = "other"
			}
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				consumer.Named(): {File: "consumer.yaml", Refs: []manifest.NamedResource{source}},
				owner.Named():    {File: "owner.yaml", Pinned: tc.pinned},
				target:           {Productions: []manifest.Production{production}},
			}}
			objects := filtertest.MapLister{consumer.Named(): consumer, owner.Named(): owner}
			assert.Diff(t, index.Records[consumer.Named()].Refs, []manifest.NamedResource{source})
			assert.Diff(t, index.Records[target].Productions, []manifest.Production{production})
			filter := newAdmission(
				t,
				admissionConfig{
					changes:   change.NewSet([]string{"generator.yaml"}),
					index:     index,
					inventory: objects,
					root:      "",
				},
			)
			assert.Equal(t, filter.ShouldReconcile(consumer.Named()), tc.want)
			assert.Equal(t, filter.ShouldReconcile(owner.Named()), tc.want)
		})
	}
}

func TestFilter_ProducerInputClosure(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                               string
		chain, cycle, promotion, ownerOnly bool
		foreign, fixed, projected          bool
		want                               bool
	}{
		{name: "generic_declaration", want: true},
		{name: "chained_declaration", chain: true, want: true},
		{name: "cycles_and_duplicates", chain: true, cycle: true, want: true},
		{name: "dependency_promotion", promotion: true, want: true},
		{name: "owner_is_not_declaration", ownerOnly: true},
		{name: "projected_namespace", projected: true, want: true},
		{name: "foreign_same_name", foreign: true},
		{name: "fixed_coordinate", fixed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFilterProducerInputClosureCase(t, &tc)
		},
		)
	}
}

func testFilterProducerInputClosureCase(t *testing.T, testCaseInput *struct {
	name      string
	chain     bool
	cycle     bool
	promotion bool
	ownerOnly bool
	foreign   bool
	fixed     bool
	projected bool
	want      bool
},
) {
	t.Helper()

	input := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "input",
	}
	declaration := manifest.NamedResource{
		Group:     "example.test",
		Kind:      "Producer",
		Namespace: "apps",
		Name:      "declaration",
	}
	owner := testutil.KustomizationID("apps", "owner")
	output := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindExternalArtifact,
		Namespace: "apps",
		Name:      "generated",
	}
	consumer := testutil.KustomizationID("apps", "consumer")
	unrelated := testutil.KustomizationID("apps", "unrelated")
	independent := declaration
	independent.Name = "independent"
	independentOutput := output
	independentOutput.Name = "independent-output"
	production := manifest.Production{
		Owner:       owner,
		Declaration: declaration,
		Source:      manifest.BootstrapSourceID,
		File:        "declaration.yaml",
	}
	if testCaseInput.projected {
		production.Authored = declaration
		production.Authored.Namespace = ""
	}
	if testCaseInput.fixed {
		production.File = "../fixed/declaration.yaml"
	}
	target := output
	if testCaseInput.foreign {
		target.Namespace = "other"
	}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		input:       {File: "input.yaml"},
		declaration: {File: "declaration.yaml", Refs: []manifest.NamedResource{input}},
		owner:       {File: "owner.yaml"},
		target:      {Productions: []manifest.Production{production}},
		consumer:    {File: "consumer.yaml", Refs: []manifest.NamedResource{output}},
		unrelated:   {File: "unrelated.yaml", Refs: []manifest.NamedResource{independentOutput}},
		independent: {File: "independent.yaml"},
		independentOutput: {
			Productions: []manifest.Production{
				{
					Owner:       independent,
					Declaration: independent,
					Source:      manifest.BootstrapSourceID,
					File:        "independent.yaml",
				},
			},
		},
	}}
	r := index.Records[declaration]
	r.Refs = append(r.Refs, independent)
	index.Records[declaration] = r
	if testCaseInput.ownerOnly {
		r := index.Records[declaration]
		r.Refs = []manifest.NamedResource{independent}
		index.Records[declaration] = r
		r = index.Records[owner]
		r.Refs = []manifest.NamedResource{input}
		index.Records[owner] = r
	}
	leaf := consumer
	if testCaseInput.chain || testCaseInput.promotion {
		next := declaration
		next.Name = "next"
		nextOutput := output
		nextOutput.Name = "next-output"
		if testCaseInput.promotion {
			seed := testutil.KustomizationID("apps", "seed")
			index.Records[seed] = manifest.DiscoveryRecord{
				File: "seed.yaml",
				Refs: []manifest.NamedResource{declaration},
			}
			index.Readers = map[string][]manifest.FileReader{
				"input.yaml": {{Reader: seed, Source: manifest.BootstrapSourceID}},
			}
			r := index.Records[declaration]
			r.Refs = []manifest.NamedResource{nextOutput, independent}
			index.Records[declaration] = r
			index.Records[next] = manifest.DiscoveryRecord{
				File: "next.yaml",
				Refs: []manifest.NamedResource{input},
			}
		} else {
			index.Records[next] = manifest.DiscoveryRecord{
				File: "next.yaml",
				Refs: []manifest.NamedResource{output},
			}
			leaf = testutil.KustomizationID("apps", "leaf")
			index.Records[leaf] = manifest.DiscoveryRecord{
				File: "leaf.yaml",
				Refs: []manifest.NamedResource{nextOutput},
			}
		}
		index.Records[nextOutput] = manifest.DiscoveryRecord{
			Productions: []manifest.Production{
				{Owner: owner, Declaration: next, Source: manifest.BootstrapSourceID, File: "next.yaml"},
			},
		}
		if testCaseInput.cycle {
			r := index.Records[declaration]
			r.Refs = append(r.Refs, nextOutput, nextOutput)
			index.Records[declaration] = r
			r = index.Records[target]
			r.Productions = append(r.Productions, production)
			index.Records[target] = r
		}
	}
	objects := make(filtertest.MapLister)
	for id := range index.Records {
		if len(index.Records[id].Refs) == 0 {
			objects[id] = refObj(id)
		}
	}
	filter := newAdmission(
		t,
		admissionConfig{
			changes:   change.NewSet([]string{"input.yaml"}),
			index:     index,
			inventory: objects,
			root:      "",
		},
	)
	assert.Equal(t, filter.ShouldReconcile(consumer), testCaseInput.want)
	assert.Equal(t, filter.ShouldReconcile(leaf), testCaseInput.want)
	assert.Equal(t, filter.ShouldReconcile(unrelated), false)
	assert.Equal(t, filter.ShouldReconcile(declaration), !testCaseInput.ownerOnly)
	if testCaseInput.projected {
		assert.Equal(t, filter.ShouldReconcile(production.Authored), false)
	}
}
