package base

import (
	"fmt"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
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
		{name: "zero-options-default-only", want: "${PREFIX:-dev-}config"},
		{name: "supplied-render-only", declarations: []string{"prod-"}, want: "${PREFIX:-dev-}config"},
		{name: "conflicting-disabled-substitution", declarations: []string{"prod-", "other-"}, want: "${PREFIX:-dev-}config"},
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
	emissionDependencyResult1 := emissionDependency(
		t,
		"supplier",
		"  postBuild: {substitute: {PREFIX: 'prod-', NS: shared}}\n",
	)
	supplier, _ := emissionDependencyResult1.object, emissionDependencyResult1.document
	st.AddObject(supplier)
	controllers := make([]*Controller, 2)
	parents := make([]*manifest.Kustomization, 2)
	children := make([]*manifest.Kustomization, 2)
	docs := make([]map[string]any, 2)
	for i := range 2 {
		controllers[i] = New(st, nil)
		controllers[i].Configure(Options{})
		emissionDependencyResult2 := emissionDependency(
			t,
			fmt.Sprintf("parent-%d", i),
			"  path: ./children\n  postBuild: {substitute: {PREFIX: 'prod-', NS: shared}}\n",
		)
		parents[i], _ = emissionDependencyResult2.object, emissionDependencyResult2.document
		emissionDependencyResult3 := emissionDependency(
			t,
			fmt.Sprintf("child-%d", i),
			"  dependsOn: [{name: '${PREFIX:-dev-}config', namespace: '${NS}'}]\n",
		)
		children[i], docs[i] = emissionDependencyResult3.object, emissionDependencyResult3.document
		st.AddObject(parents[i])
	}
	var ready, done sync.WaitGroup
	ready.Add(2)
	done.Add(2)
	start := make(chan struct{})
	results := make(chan error, 2)
	for i := range 2 {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			output := []manifest.DocOccurrence{{Document: manifest.DeepCopyMap(docs[i])}}
			if err := kustomize.PrepareShape(&kustomize.RenderRequest{Spec: parents[i]}, output).
				Apply(t.Context(), kustomize.ShapeOptions{}); err != nil {
				results <- err
				return
			}
			parsed, err := manifest.ParseDoc(output[0].Document, manifest.ParseDocOptions{})
			if err != nil {
				results <- err
				return
			}
			results <- (&Execution{Controller: controllers[i]}).emit(t.Context(), parents[i].Named(), &store.Emission{
				Children: []store.Child{{Obj: parsed.Object, Doc: parsed.Document}}, Complete: true,
			}, nil)
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()
	for range 2 {
		assert.NoError(t, <-results)
	}
	for _, child := range children {
		got, ok := st.Get[*manifest.Kustomization](child.Named())
		assert.FatalIf(t, !ok || got.DependsOn[0].Name != "prod-config", "concurrent emitted child=%+v", got)
		assert.Equal(t, got.DependsOn[0].Namespace, "shared")
		assert.Equal(t, child.DependsOn[0].Name, "${PREFIX:-dev-}config")
		assert.Equal(t, child.DependsOn[0].Namespace, "${NS}")
	}
}

func TestEmit_ParentDependencyNamespace(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, parentSpec, dependency, namespace, wantName, wantNamespace, disabled string
	}{
		{name: "enabled", parentSpec: "  postBuild: {substitute: {DEP: ready, TEAM: shared}}\n", dependency: "${DEP}", namespace: "${TEAM}", wantName: "ready", wantNamespace: "shared"},
		{name: "disabled annotation", parentSpec: "  postBuild: {substitute: {DEP: ready, TEAM: shared}}\n", dependency: "${DEP}", namespace: "${TEAM}", wantName: "${DEP}", wantNamespace: "${TEAM}", disabled: "annotations"},
		{name: "disabled label", parentSpec: "  postBuild: {substitute: {DEP: ready, TEAM: shared}}\n", dependency: "${DEP}", namespace: "${TEAM}", wantName: "${DEP}", wantNamespace: "${TEAM}", disabled: "labels"},
		{name: "inactive", dependency: "${DEP:-ready}", namespace: "${TEAM:-shared}", wantName: "${DEP:-ready}", wantNamespace: "${TEAM:-shared}"},
		{name: "always", parentSpec: "  postBuild: {substituteStrategy: Always}\n", dependency: "${DEP:-ready}", namespace: "${TEAM:-shared}", wantName: "ready", wantNamespace: "shared"},
		{name: "escaped", parentSpec: "  postBuild: {substitute: {DEP: ready, TEAM: shared}}\n", dependency: "$${DEP:-ready}", namespace: "$${TEAM:-shared}", wantName: "${DEP:-ready}", wantNamespace: "${TEAM:-shared}"},
		{name: "literal", dependency: "ready", namespace: "shared", wantName: "ready", wantNamespace: "shared"},
		{name: "namespace default", dependency: "ready", wantName: "ready"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			parent := emissionDependency(t, "parent", "  path: ./children\n"+tc.parentSpec)
			child := emissionDependency(t, "child", fmt.Sprintf(
				"  path: ./app\n  dependsOn: [{name: %q, namespace: %q, readyExpr: 'true'}]\n",
				tc.dependency, tc.namespace,
			))
			if tc.disabled != "" {
				child.document["metadata"].(map[string]any)[tc.disabled] = map[string]any{
					"kustomize.toolkit.fluxcd.io/substitute": "disabled",
				}
			}
			before := manifest.Fingerprint(child.document)
			docs := []manifest.DocOccurrence{{Document: manifest.DeepCopyMap(child.document)}}
			assert.NoError(t, kustomize.PrepareShape(&kustomize.RenderRequest{Spec: parent.object}, docs).
				Apply(t.Context(), kustomize.ShapeOptions{}))
			st.AddObject(parent.object, parent.document)
			c := New(st, nil)
			c.Configure(Options{})
			e := &Execution{Controller: c}
			parsed := e.ParseChildren(parent.object, docs, manifest.ParseDocOptions{})
			assert.Equal(t, len(parsed.Failures), 0)
			assert.FatalIf(t, len(parsed.Children) != 1, "prepared children=%+v", parsed.Children)
			prepared := parsed.Children[0].Obj.(*manifest.Kustomization)
			assert.Equal(t, prepared.DependsOn[0].Name, tc.wantName)
			assert.Equal(t, prepared.DependsOn[0].Namespace, tc.wantNamespace)
			wantNamespace := tc.wantNamespace
			if wantNamespace == "" {
				wantNamespace = "apps"
			}
			for ref := range manifest.References(prepared, manifest.ReferenceRelations) {
				if ref.Use == "dependsOn" {
					assert.Equal(t, ref.NamedResource, testutil.KustomizationID(wantNamespace, tc.wantName))
				}
			}
			arrivals := 0
			st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, payload any) {
				if id == prepared.Named() {
					arrivals++
					got := payload.(*manifest.Kustomization)
					assert.Equal(t, got.DependsOn[0].Name, tc.wantName)
					assert.Equal(t, got.DependsOn[0].Namespace, tc.wantNamespace)
				}
			}, false)
			next := &store.Emission{
				Children:    parsed.Children,
				Manifests:   docs,
				Complete:    true,
				Fingerprint: manifest.Fingerprint("inputs"),
			}
			authoritative := manifest.Fingerprint(docs)
			assert.NoError(t, e.emit(t.Context(), parent.object.Named(), next, nil))
			assert.Equal(t, arrivals, 1)
			got, ok := st.Get[*manifest.Kustomization](prepared.Named())
			assert.Equal(t, ok, true)
			assert.Equal(t, got.DependsOn[0].Name, tc.wantName)
			assert.Equal(t, got.DependsOn[0].Namespace, tc.wantNamespace)
			assert.Equal(t, got.DependsOn[0].ReadyExpr, "true")
			assert.Equal(t, manifest.Fingerprint(child.document), before)
			assert.Equal(t, child.object.DependsOn[0].Name, tc.dependency)
			assert.Equal(t, child.object.DependsOn[0].Namespace, tc.namespace)
			assert.Equal(t, manifest.Fingerprint(next.Manifests), authoritative)
			_, _, accepted := st.GetEmission(parent.object.Named())
			assert.Equal(t, manifest.Fingerprint(accepted.Manifests), authoritative)
			assert.Diff(t, accepted.Children[0].Doc, docs[0].Document)
			accepted.Finish()
			replay, err := newExecution(
				c,
				parent.object.Named(),
			).FingerprintDedup(t.Context(), parent.object.Named(), "inputs", &store.Emission{})
			assert.NoError(t, err)
			assert.Equal(t, replay.Reused, true)
			replayed, _ := st.Get[*manifest.Kustomization](prepared.Named())
			assert.Equal(t, replayed.DependsOn[0].Name, tc.wantName)
			assert.Equal(t, replayed.DependsOn[0].Namespace, tc.wantNamespace)
		})
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
		assert.Equal(t, payload.(*manifest.Kustomization).DependsOn[0].Namespace, "${NS}")
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
	assert.Equal(t, replayed.DependsOn[0].Namespace, "${NS}")
	output, matched := st.GetArtifact(parent.Named(), store.ArtifactContent).(*store.Emission).Replay(
		manifest.Fingerprint("inputs"),
	)
	assert.Equal(t, matched, true)
	assert.Diff(t, testutil.DocumentMaps(output.Manifests), docs)
	assert.Diff(t, output.Children[1].Doc, childDoc)
	assert.FatalIf(t, st.GetObject(parent.Named()) != parent, "%v", "self child replaced parent")
}
