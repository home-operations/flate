package base

import (
	"fmt"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/values"
	"github.com/home-operations/flate/pkg/manifest"
)

type dependencyFixture struct {
	object   *manifest.Kustomization
	document map[string]any
}

func emissionDependency(t *testing.T, name, spec string) dependencyFixture {
	t.Helper()
	docs, err := testutil.SplitDocs([]byte(testutil.KustomizationYAML(name, "apps", spec)))
	assert.FatalIf(t, err != nil, "%v", err)
	parsed, err := manifest.ParseDoc(docs[0], manifest.ParseDocOptions{})
	obj := parsed.Object
	assert.FatalIf(t, err != nil, "%v", err)
	return dependencyFixture{object: obj.(*manifest.Kustomization), document: docs[0]}
}

func TestEmit_DependencySubstitutions(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name, want   string
		declarations []string
	}{
		{name: "zero-options-default-only", want: "dev-config"},
		{name: "supplied-render-only", declarations: []string{"prod-"}, want: "prod-config"},
		{name: "conflicting-disabled-substitution", declarations: []string{"prod-", "other-"}, want: "dev-config"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			testEmitDependencySubstitutionsCase(t, &tt)
		},
		)
	}
}

func TestEmit_DependencySubstitutionsConcurrent(t *testing.T) {
	t.Parallel()
	st := store.New()
	emissionDependencyResult1 := emissionDependency(t, "supplier", "  postBuild: {substitute: {PREFIX: 'prod-'}}\n")
	supplier, _ := emissionDependencyResult1.object, emissionDependencyResult1.document
	st.AddObject(supplier)
	d := values.NewDependencySubstitutions(
		st.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization),
	)
	controllers := make([]*Controller, 2)
	parents := make([]*manifest.Kustomization, 2)
	children := make([]*manifest.Kustomization, 2)
	docs := make([]map[string]any, 2)
	for i := range 2 {
		controllers[i] = New(st, nil)
		controllers[i].Configure(Options{DependencySubstitutions: d})
		emissionDependencyResult2 := emissionDependency(t, fmt.Sprintf("parent-%d", i), "  path: ./children\n")
		parents[i], _ = emissionDependencyResult2.object, emissionDependencyResult2.document
		emissionDependencyResult3 := emissionDependency(
			t,
			fmt.Sprintf("child-%d", i),
			"  dependsOn: [{name: '${PREFIX:-dev-}config'}]\n",
		)
		children[i], docs[i] = emissionDependencyResult3.object, emissionDependencyResult3.document
		st.AddObject(parents[i])
	}
	var ready, done sync.WaitGroup
	ready.Add(2)
	done.Add(2)
	start := make(chan struct{})
	for i := range 2 {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			(&Execution{Controller: controllers[i]}).emit(t.Context(), parents[i].Named(), &store.Emission{
				Children: []store.Child{{Obj: children[i], Doc: docs[i]}}, Complete: true,
			}, nil)
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()
	for _, child := range children {
		got, ok := st.Get[*manifest.Kustomization](child.Named())
		assert.FatalIf(t, !ok || got.DependsOn[0].Name != "prod-config", "concurrent emitted child=%+v", got)
		assert.Equal(t, child.DependsOn[0].Name, "${PREFIX:-dev-}config")
	}
}

func testEmitDependencySubstitutionsCase(t *testing.T, testCaseInput *struct {
	name string

	want         string
	declarations []string
},
) {
	t.Helper()

	st := store.New()
	c := New(st, nil)
	emissionDependencyResult4 := emissionDependency(t, "parent", "  path: ./children\n")
	parent, parentDoc := emissionDependencyResult4.object, emissionDependencyResult4.document
	st.AddObject(parent)
	for i, declaration := range testCaseInput.declarations {
		emissionDependencyResult5 := emissionDependency(
			t,
			fmt.Sprintf("supplier-%d", i),
			fmt.Sprintf("  postBuild: {substitute: {PREFIX: %q, NS: resolved}}\n", declaration),
		)
		supplier, _ := emissionDependencyResult5.object, emissionDependencyResult5.document
		st.AddObject(supplier)
	}
	if len(testCaseInput.declarations) != 0 {
		c.Configure(
			Options{
				DependencySubstitutions: values.NewDependencySubstitutions(
					st.ListAs[*manifest.Kustomization](
						manifest.FluxKustomizeDomain,
						manifest.KindKustomization,
					),
				),
			},
		)
	}
	emissionDependencyResult6 := emissionDependency(
		t,
		"later-supplier",
		"  postBuild: {substitute: {PREFIX: 'later-'}}\n",
	)
	later, _ := emissionDependencyResult6.object, emissionDependencyResult6.document
	st.AddObject(later)
	emissionDependencyResult7 := emissionDependency(t, "literal", "  dependsOn: [{name: plain}]\n")
	literal, literalDoc := emissionDependencyResult7.object, emissionDependencyResult7.document
	emissionDependencyResult8 := emissionDependency(
		t,
		"child",
		"  path: ./app\n  dependsOn: [{name: '${PREFIX:-dev-}config', namespace: '${NS}', readyExpr: 'true'}]\n",
	)
	child, childDoc := emissionDependencyResult8.object, emissionDependencyResult8.document
	childDoc["metadata"].(map[string]any)["annotations"] = map[string]any{
		"kustomize.toolkit.fluxcd.io/substitute": "disabled",
	}
	child.Annotations = map[string]string{"kustomize.toolkit.fluxcd.io/substitute": "disabled"}
	before := child.Clone()
	emissionDependencyResult9 := emissionDependency(t, "sibling", "  dependsOn: [{name: '${PREFIX:-dev-}config'}]\n")
	sibling, siblingDoc := emissionDependencyResult9.object, emissionDependencyResult9.document
	docs := []map[string]any{literalDoc, childDoc, siblingDoc}
	next := &store.Emission{
		Children: []store.Child{
			{Obj: literal, Doc: literalDoc},
			{Obj: child, Doc: childDoc},
			{Obj: sibling, Doc: siblingDoc},
			{},
			{Obj: parent, Doc: parentDoc},
		},
		Manifests: testutil.DocumentOccurrences(
			docs,
		),
		Complete:    true,
		Fingerprint: manifest.Fingerprint("inputs"),
	}
	arrivals := 0
	st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, payload any) {
		if id != child.Named() || payload == nil {
			return
		}
		arrivals++
		{
			got := payload.(*manifest.Kustomization).DependsOn[0].Name
			assert.ErrorIf(
				t,
				got != testCaseInput.want,
				"first child arrival dependency=%q, want %q",
				got,
				testCaseInput.want,
			)
		}
	}, false)
	(&Execution{Controller: c}).emit(t.Context(), parent.Named(), next, nil)
	assert.Equal(t, arrivals, 1)
	got, ok := st.Get[*manifest.Kustomization](child.Named())
	assert.FatalIf(
		t,
		!ok || got.DependsOn[0].Name != testCaseInput.want || got.DependsOn[0].Namespace != "${NS}" ||
			got.DependsOn[0].ReadyExpr != "true",
		"admitted child=%+v",
		got,
	)
	{
		diff := cmp.Diff(
			before,
			child,
			cmp.AllowUnexported(manifest.Kustomization{}),
			cmpopts.IgnoreFields(manifest.Kustomization{}, "projection"),
		)
		assert.FatalIf(t, diff != "", "caller object/document mutated (-want +got):\n%s", diff)
	}
	assert.FatalIf(t, next.Footprint() == nil || next.Children[1].Obj != got ||
		sibling.DependsOn[0].Name != "${PREFIX:-dev-}config", "%v", "accepted caller handoff or immutable input changed")
	_, _, accepted := st.GetEmission(parent.Named())
	{
		diff := cmp.Diff(docs, testutil.DocumentMaps(accepted.Manifests))
		assert.FatalIf(t, diff != "", "authoritative documents changed (-want +got):\n%s", diff)
	}
	assert.Equal(t, accepted.Children[0].Obj, manifest.BaseManifest(literal))
	{
		diff := cmp.Diff(childDoc, accepted.Children[1].Doc)
		assert.FatalIf(t, diff != "", "child document changed (-want +got):\n%s", diff)
	}
	{
		fingerprintDedupResult10, fingerprintDedupResult10Err := newExecution(
			c,
			parent.Named(),
		).FingerprintDedup(t.Context(), parent.Named(), "inputs", &store.Emission{})
		_, handled, err := fingerprintDedupResult10.Fingerprint, fingerprintDedupResult10.Reused, fingerprintDedupResult10Err
		assert.FatalIf(t, !handled ||
			err != nil, "normalized replay: handled=%v err=%v", handled, err)
	}
	replayed, _ := st.Get[*manifest.Kustomization](child.Named())
	assert.Equal(t, replayed.DependsOn[0].Name, testCaseInput.want)
	output, matched := st.GetArtifact(parent.Named(), store.ArtifactContent).(*store.Emission).Replay(
		manifest.Fingerprint("inputs"),
	)
	assert.Equal(t, matched, true)
	assert.Diff(t, testutil.DocumentMaps(output.Manifests), docs)
	assert.Diff(t, output.Children[1].Doc, childDoc)
	assert.FatalIf(t, st.GetObject(parent.Named()) != parent, "%v", "self child replaced parent")
}
