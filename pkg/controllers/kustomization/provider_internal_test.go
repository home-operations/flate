package kustomization

import (
	"context"
	"errors"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	"github.com/home-operations/flate/pkg/values"
)

func TestSubstitutionProvider_ExactIdentity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, kind, namespace, ref string
		accepted                   bool
	}{
		{"exact", manifest.KindConfigMap, "apps", "external", true},
		{"wrong kind", manifest.KindSecret, "apps", "external", false},
		{"wrong namespace", manifest.KindConfigMap, "other", "external", false},
		{"wrong name", manifest.KindConfigMap, "apps", "other", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			var source manifest.BaseManifest = &manifest.ConfigMap{Name: tc.ref, Namespace: tc.namespace, Data: map[string]any{"VALUE": "supplied"}}
			if tc.kind == manifest.KindSecret {
				source = &manifest.Secret{
					Name:       tc.ref,
					Namespace:  tc.namespace,
					StringData: map[string]any{"VALUE": "supplied"},
				}
			}
			st.AddDeclaration(
				store.Declaration{
					Obj:    source,
					Doc:    testutil.ObjectDocument(source),
					Origin: manifest.SuppliedDeclaration,
				},
			)
			c := New(st, task.NewBounded(2), nil)
			c.Configure(Options{}, sourcecache.RetryConfig{})
			execution := c.newExecution(testutil.KustomizationID("apps", "consumer"))
			defer execution.run.Close()
			input := execution.data.Lookup(
				manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "external"},
			)
			assert.FatalIf(t, (input.Data != nil) != tc.accepted, "input=%+v accepted=%t", input, tc.accepted)
		})
	}
}

func TestSubstitutionProvider_AcceptedSourceAndFallback(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ external, late manifest.BaseManifest }{
		{
			&manifest.ConfigMap{Name: "external", Namespace: "apps", Data: map[string]any{"VALUE": "supplied"}},
			&manifest.ConfigMap{Name: "external", Namespace: "apps", Data: map[string]any{"VALUE": "late"}},
		},
		{
			&manifest.Secret{Name: "external", Namespace: "apps", StringData: map[string]any{"VALUE": "supplied"}},
			&manifest.Secret{Name: "external", Namespace: "apps", StringData: map[string]any{"VALUE": "late"}},
		},
	} {
		t.Run(tc.external.Named().Kind, func(t *testing.T) {
			t.Parallel()
			external := tc.external
			st := store.New()
			other := external.WithNamespace("other")
			st.AddObject(other)
			c := New(st, task.NewBounded(2), nil)
			st.AddDeclaration(
				store.Declaration{
					Obj:    external,
					Doc:    testutil.ObjectDocument(external),
					Origin: manifest.SuppliedDeclaration,
				},
			)
			c.Configure(Options{}, sourcecache.RetryConfig{})
			st.AddObject(tc.late)
			execution := c.newExecution(testutil.KustomizationID("apps", "consumer"))
			defer execution.run.Close()
			provider := values.NewStoreProvider(&execution.run.Reads)
			missing := manifest.NamedResource{Kind: external.Named().Kind, Namespace: "apps", Name: "absent"}
			assert.FatalIf(t, provider.Data(external.Named()) != tc.late.(manifest.Data) ||
				provider.Data(other.Named()) != other.(manifest.Data) ||
				provider.Data(missing) != nil, "%v", "provider lost repository precedence or exact identity")
			assert.FatalIf(
				t,
				!provider.Lookup(external.Named()).Data.HasKeys(),
				"%v",
				"readable supplied source reported unreadable",
			)

			{
				valueResult, err := values.NewStoreProvider(st).
					Data(external.Named()).
					Value("VALUE")
				got := valueResult.Value
				assert.FatalIf(t, err != nil ||
					got != "late", "%v", "ordinary provider used supplied values")
			}
		})
	}
}

//nolint:paralleltest // This test changes process-global state.
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestSubstitutionProvider_SuppliedLookupAllocations(t *testing.T) {
	for _, source := range []manifest.BaseManifest{
		&manifest.ConfigMap{Name: "external", Namespace: "apps", Data: map[string]any{"VALUE": "supplied"}},
		&manifest.Secret{Name: "external", Namespace: "apps", StringData: map[string]any{"VALUE": "supplied"}},
	} {
		t.Run(source.Named().Kind, func(t *testing.T) {
			st := store.New()
			c := New(st, task.NewBounded(2), nil)
			st.AddDeclaration(
				store.Declaration{
					Obj:    source,
					Doc:    testutil.ObjectDocument(source),
					Origin: manifest.SuppliedDeclaration,
				},
			)
			c.Configure(Options{}, sourcecache.RetryConfig{})
			execution := c.newExecution(testutil.KustomizationID("apps", "consumer"))
			defer execution.run.Close()
			provider := execution.data
			id := source.Named()
			assert.FatalIf(t, provider.Data(id) == nil, "%v", "provider lost supplied identity")
			{
				allocs := testing.AllocsPerRun(100, func() { provider.Data(id) })
				assert.FatalIf(t, allocs != 0, "supplied lookup allocated %g times, want 0", allocs)
			}
		})
	}
}

func TestReconcile_StrictSubstitutionClassification(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"app/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\ndata: {value: '${MISSING}'}\n",
	)
	st := store.New()
	location, err := gitrepo.Locate(t.Context(), root, "")
	assert.NoError(t, err)
	input, err := gittree.AdmitDirectory(t.Context(), location)
	assert.NoError(t, err)
	st.SetArtifact(manifest.BootstrapSourceID, input)
	owner := cachetest.New(t, "", sourcecache.New)
	tasks := task.NewBounded(2)
	trees := kustomize.NewTreeCache(owner, nil, tasks)
	t.Cleanup(trees.Close)
	c := New(st, tasks, trees)
	c.Configure(Options{StrictSubstitutions: true}, sourcecache.RetryConfig{})
	docs, err := testutil.SplitDocs(
		[]byte(
			testutil.KustomizationYAML("consumer", "apps", "  {path: app, postBuild: {substituteStrategy: Always}}\n"),
		),
	)
	assert.FatalIf(t, err != nil, "%v", err)
	parsed, err := manifest.ParseDoc(docs[0], manifest.ParseDocOptions{DefaultNamespace: ""})
	obj := parsed.Object
	assert.FatalIf(t, err != nil, "%v", err)
	ks := obj.(*manifest.Kustomization)
	st.AddObject(ks)
	tasks.Go(
		t.Context(),
		"reconcile",
		func(ctx context.Context) { err = c.newExecution(ks.Named()).reconcile(ctx, ks, manifest.DrainNone) },
	)
	tasks.BlockTillDone()
	assert.FatalIf(
		t,
		!errors.Is(err, manifest.ErrFlux) || !errors.Is(err, manifest.ErrInvalidSubstituteReference),
		"strict engine failure lost classification: %v",
		err,
	)
}
