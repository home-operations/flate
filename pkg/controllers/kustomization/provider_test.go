package kustomization

import (
	"errors"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	"github.com/home-operations/flate/pkg/values"
)

func TestCollectDeps_AcceptedSubstitutionExactIdentity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, group, kind, namespace, ref string
		accepted                          bool
	}{
		{"exact", "", "ConfigMap", "apps", "external", true},
		{"wrong group", "example.test", "ConfigMap", "apps", "external", false},
		{"wrong kind", "", "Secret", "apps", "external", false},
		{"wrong namespace", "", "ConfigMap", "other", "external", false},
		{"wrong name", "", "ConfigMap", "apps", "other", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			c := New(st, task.NewBounded(2), nil, true)
			source := &manifest.ConfigMap{Name: "external", Namespace: "apps"}
			supplied := map[manifest.NamedResource]manifest.BaseManifest{{Group: tc.group, Kind: tc.kind, Namespace: tc.namespace, Name: tc.ref}: source}
			c.Configure(Options{SubstituteFrom: supplied, Substitute: map[string]string{"VALUE": "overlay"}}, sourcecache.RetryConfig{})
			ks := &manifest.Kustomization{Name: "consumer", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: "ConfigMap", Name: "external"}}}}
			var resolved depwait.Resolution
			err := c.Require(&depwait.Waiter{Parent: ks.Named(), Declaration: ks, Supplied: c.substitutionSources}, manifest.ReconcileAttempt{Drain: manifest.DrainCascade}, &resolved)
			if (err == nil) != tc.accepted || (resolved.Inputs.Data(source.Named()) == source) != tc.accepted {
				t.Fatalf("resolution=%+v err=%v accepted=%v", resolved, err, tc.accepted)
			}
		})
	}
}

func TestSubstitutionProvider_AcceptedSourceAndFallback(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ external, late manifest.BaseManifest }{
		{&manifest.ConfigMap{Name: "external", Namespace: "apps", Data: map[string]any{"VALUE": "supplied"}},
			&manifest.ConfigMap{Name: "external", Namespace: "apps", Data: map[string]any{"VALUE": "late"}}},
		{&manifest.Secret{Name: "external", Namespace: "apps", StringData: map[string]any{"VALUE": "supplied"}},
			&manifest.Secret{Name: "external", Namespace: "apps", StringData: map[string]any{"VALUE": "late"}}},
	} {
		t.Run(tc.external.Named().Kind, func(t *testing.T) {
			t.Parallel()
			external := tc.external
			st := store.New()
			other := external.WithNamespace("other")
			st.AddObject(other)
			c := New(st, task.NewBounded(2), nil, true)
			c.Configure(Options{SubstituteFrom: map[manifest.NamedResource]manifest.BaseManifest{external.Named(): external}}, sourcecache.RetryConfig{})
			st.AddObject(tc.late)
			missing := manifest.NamedResource{Kind: external.Named().Kind, Namespace: "apps", Name: "absent"}
			ks := &manifest.Kustomization{Name: "consumer", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: external.Named().Kind, Name: "external"}}}}
			var resolved depwait.Resolution
			err := c.Require(&depwait.Waiter{Parent: ks.Named(), Declaration: ks, Supplied: c.substitutionSources}, manifest.ReconcileAttempt{}, &resolved)
			assert.NoError(t, err)
			if resolved.Inputs.Data(external.Named()) != external.(manifest.Data) || resolved.Inputs.Data(missing) != nil {
				t.Fatal("lost exact supplied precedence or frozen absence")
			}
			if got := values.UnreadableSubstituteSecrets(ks, resolved.Inputs); len(got) != 0 {
				t.Fatalf("readable supplied source reported unreadable: %v", got)
			}
			if got, _, err := values.NewStoreProvider(st, true).Data(external.Named()).Value("VALUE"); err != nil || got != "late" {
				t.Fatal("ordinary provider used supplied values")
			}
		})
	}
}

func TestReconcile_StrictSubstitutionClassification(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\ndata: {value: '${MISSING}'}\n")
	st := store.New()
	location, err := gitrepo.Locate(t.Context(), root, "")
	assert.NoError(t, err)
	input, err := gittree.AdmitDirectory(t.Context(), location)
	assert.NoError(t, err)
	st.SetArtifact(manifest.BootstrapSourceID, input)
	owner := cachetest.New(t, "", sourcecache.New)
	tasks := task.NewBounded(2)
	network := new(source.Network)
	t.Cleanup(network.Close)
	inputs := source.NewHTTPInputs(owner, network, tasks)
	t.Cleanup(inputs.Close)
	trees := kustomize.NewTreeCache(owner, &inputs, tasks)
	t.Cleanup(trees.Close)
	c := New(st, tasks, trees, false)
	c.Configure(Options{StrictSubstitutions: true}, sourcecache.RetryConfig{})
	docs, err := manifest.SplitDocs([]byte(testutil.KustomizationYAML("consumer", "apps", "  {path: app, postBuild: {substituteStrategy: Always}}\n")))
	if err != nil {
		t.Fatal(err)
	}
	obj, _, _, err := manifest.ParseDoc(docs[0], manifest.ParseDocOptions{DefaultNamespace: ""})
	if err != nil {
		t.Fatal(err)
	}
	ks := obj.(*manifest.Kustomization)
	st.AddObject(ks)
	_, err = c.reconcile(t.Context(), ks, manifest.ReconcileAttempt{Drain: manifest.DrainNone}, false)
	if !errors.Is(err, manifest.ErrFlux) || !errors.Is(err, manifest.ErrInvalidSubstituteReference) {
		t.Fatalf("strict engine failure lost classification: %v", err)
	}
}
