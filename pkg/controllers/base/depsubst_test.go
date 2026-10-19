package base

import (
	"fmt"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

func emissionDependency(t *testing.T, name, spec string) *manifest.Kustomization {
	t.Helper()
	docs, err := manifest.SplitDocs([]byte(testutil.KustomizationYAML(name, "apps", spec)))
	if err != nil {
		t.Fatal(err)
	}
	obj, _, _, err := manifest.ParseDoc(docs[0], manifest.ParseDocOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return obj.(*manifest.Kustomization)
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
			st := store.New()
			c := New(st, nil, "test")
			parent := emissionDependency(t, "parent", "  path: ./children\n")
			st.AddObject(parent)
			for i, declaration := range tt.declarations {
				st.AddObject(emissionDependency(t, fmt.Sprintf("supplier-%d", i), fmt.Sprintf("  postBuild: {substitute: {PREFIX: %q, NS: resolved}}\n", declaration)))
			}
			if len(tt.declarations) != 0 {
				c.Configure(Options{DependencySubstitutions: values.NewDependencySubstitutions(st.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization))})
			}
			st.AddObject(emissionDependency(t, "later-supplier", "  postBuild: {substitute: {PREFIX: 'later-'}}\n"))
			literal := emissionDependency(t, "literal", "  dependsOn: [{name: plain}]\n")
			child := emissionDependency(t, "child", "  path: ./app\n  dependsOn: [{name: '${PREFIX:-dev-}config', namespace: '${NS}', readyExpr: 'true'}]\n")
			child.Contents["metadata"].(map[string]any)["annotations"] = map[string]any{"kustomize.toolkit.fluxcd.io/substitute": "disabled"}
			before := child.Clone()
			sibling := emissionDependency(t, "sibling", "  dependsOn: [{name: '${PREFIX:-dev-}config'}]\n")
			docs := []map[string]any{literal.Contents, child.Contents, sibling.Contents}
			next := &store.Emission{
				Children: []store.Child{
					{Obj: literal, Doc: literal.Contents},
					{Obj: child, Doc: child.Contents},
					{Obj: sibling, Doc: sibling.Contents},
					{},
					{Obj: parent, Doc: parent.Contents},
				},
				Manifests: docs, Complete: true, Fingerprint: manifest.Fingerprint("inputs"), Render: []byte("retained"),
			}
			arrivals := 0
			st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, payload any) {
				if id != child.Named() || payload == nil {
					return
				}
				arrivals++
				if got := payload.(*manifest.Kustomization).DependsOn[0].Name; got != tt.want {
					t.Errorf("first child arrival dependency=%q, want %q", got, tt.want)
				}
			}, false)
			c.Emit(parent.Named(), next)
			assert.Equal(t, arrivals, 1)
			got, ok := st.Get[*manifest.Kustomization](child.Named())
			if !ok || got.DependsOn[0].Name != tt.want || got.DependsOn[0].Namespace != "${NS}" || got.DependsOn[0].ReadyExpr != "true" {
				t.Fatalf("admitted child=%+v", got)
			}
			if diff := cmp.Diff(before, child); diff != "" {
				t.Fatalf("caller object/document mutated (-want +got):\n%s", diff)
			}
			if next.Children[1].Obj != child || next.Children[2].Obj != sibling || sibling.DependsOn[0].Name != "${PREFIX:-dev-}config" {
				t.Fatal("caller emission/children mutated")
			}
			_, accepted := st.GetEmission(parent.Named())
			if diff := cmp.Diff(docs, accepted.Manifests); diff != "" {
				t.Fatalf("authoritative documents changed (-want +got):\n%s", diff)
			}
			assert.Equal(t, string(accepted.Render), "retained")
			assert.Equal(t, accepted.Children[0].Obj, manifest.BaseManifest(literal))
			if diff := cmp.Diff(child.Contents, accepted.Children[1].Doc); diff != "" {
				t.Fatalf("child document changed (-want +got):\n%s", diff)
			}
			if _, handled, err := c.FingerprintDedup(parent.Named(), "inputs"); !handled || err != nil {
				t.Fatalf("normalized replay: handled=%v err=%v", handled, err)
			}
			replayed, _ := st.Get[*manifest.Kustomization](child.Named())
			assert.Equal(t, replayed.DependsOn[0].Name, tt.want)
			if st.GetObject(parent.Named()) != parent {
				t.Fatal("self child replaced parent")
			}
		})
	}
}

func TestEmit_DependencySubstitutionsConcurrent(t *testing.T) {
	t.Parallel()
	st := store.New()
	st.AddObject(emissionDependency(t, "supplier", "  postBuild: {substitute: {PREFIX: 'prod-'}}\n"))
	d := values.NewDependencySubstitutions(st.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization))
	controllers := make([]*Controller, 2)
	parents := make([]*manifest.Kustomization, 2)
	children := make([]*manifest.Kustomization, 2)
	for i := range 2 {
		controllers[i] = New(st, nil, "test")
		controllers[i].Configure(Options{DependencySubstitutions: d})
		parents[i] = emissionDependency(t, fmt.Sprintf("parent-%d", i), "  path: ./children\n")
		children[i] = emissionDependency(t, fmt.Sprintf("child-%d", i), "  dependsOn: [{name: '${PREFIX:-dev-}config'}]\n")
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
			controllers[i].Emit(parents[i].Named(), &store.Emission{
				Children: []store.Child{{Obj: children[i], Doc: children[i].Contents}}, Complete: true,
			})
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()
	for _, child := range children {
		got, ok := st.Get[*manifest.Kustomization](child.Named())
		if !ok || got.DependsOn[0].Name != "prod-config" {
			t.Fatalf("concurrent emitted child=%+v", got)
		}
		assert.Equal(t, child.DependsOn[0].Name, "${PREFIX:-dev-}config")
	}
}
