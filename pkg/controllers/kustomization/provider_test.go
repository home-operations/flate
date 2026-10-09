package kustomization

import (
	"errors"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	"github.com/home-operations/flate/pkg/values"
)

func TestCollectDeps_AcceptedSubstitutionExactIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, kind, namespace, ref string
		accepted                   bool
	}{
		{"exact", "ConfigMap", "apps", "external", true},
		{"wrong kind", "Secret", "apps", "external", false},
		{"wrong namespace", "ConfigMap", "other", "external", false},
		{"wrong name", "ConfigMap", "apps", "other", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := store.New()
			c := New(st, task.NewBounded(2), nil, true)
			source := &manifest.ConfigMap{Name: "external", Namespace: "apps"}
			supplied := map[manifest.NamedResource]manifest.BaseManifest{{Kind: tc.kind, Namespace: tc.namespace, Name: tc.ref}: source}
			c.Configure(Options{SubstituteFrom: supplied, Substitute: map[string]string{"VALUE": "overlay"}})
			ks := &manifest.Kustomization{Name: "consumer", Namespace: "apps", PostBuildSubstituteFrom: []manifest.SubstituteReference{{Kind: "ConfigMap", Name: "external"}}}
			deps := c.collectDeps(ks)
			if (len(deps) == 0) != tc.accepted {
				t.Fatalf("deps=%v, accepted=%v", deps, tc.accepted)
			}
		})
	}
}

func TestSubstitutionProvider_AcceptedSourceAndFallback(t *testing.T) {
	st := store.New()
	external := &manifest.Secret{Name: "external", Namespace: "apps", StringData: map[string]any{"VALUE": "supplied"}}
	other := &manifest.Secret{Name: "other", Namespace: "apps", StringData: map[string]any{"VALUE": "repository"}}
	st.AddObject(other)
	c := New(st, task.NewBounded(2), nil, true)
	c.Configure(Options{SubstituteFrom: map[manifest.NamedResource]manifest.BaseManifest{external.Named(): external}})
	st.AddObject(&manifest.Secret{Name: external.Name, Namespace: external.Namespace, StringData: map[string]any{"VALUE": "late"}})
	if c.substitutionProvider.Secret("apps", "external") != external || c.substitutionProvider.Secret("apps", "other") != other || c.substitutionProvider.Secret("other", "external") != nil {
		t.Fatal("provider lost exact frozen precedence or fallback")
	}
	ks := &manifest.Kustomization{Name: "consumer", Namespace: "apps", PostBuildSubstituteFrom: []manifest.SubstituteReference{{Kind: "Secret", Name: "external"}}}
	if got := values.UnreadableSubstituteSecrets(ks, c.substitutionProvider); len(got) != 0 {
		t.Fatalf("readable supplied Secret reported unreadable: %v", got)
	}
	if got := values.NewStoreProvider(st).Secret("apps", "external").StringData["VALUE"]; got != "late" {
		t.Fatal("ordinary provider used supplied values")
	}
}

func TestReconcile_StrictSubstitutionClassification(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\ndata: {value: '${MISSING}'}\n")
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root})
	c := New(st, task.NewBounded(2), kustomize.NewTreeCache(), false)
	c.Configure(Options{StrictSubstitutions: true})
	docs, err := manifest.SplitDocs([]byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: apps}\nspec: {path: app, postBuild: {}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	obj, err := manifest.ParseDoc(docs[0], manifest.ParseDocOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ks := obj.(*manifest.Kustomization)
	st.AddObject(ks)
	err = c.reconcile(t.Context(), ks)
	if !errors.Is(err, manifest.ErrFlux) || !errors.Is(err, manifest.ErrInvalidSubstituteReference) {
		t.Fatalf("strict engine failure lost classification: %v", err)
	}
}
