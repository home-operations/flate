package discovery

import (
	"path/filepath"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/engine/selection"

	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestBegin_OrdinarySupplierCensus(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(
		t,
		root,
		"flux/consumer.yaml",
		testutil.KustomizationYAML(
			"consumer",
			"apps",
			"  path: ./consumer\n  postBuild: {substituteFrom: [{kind: ConfigMap, name: hello}]}\n",
		),
	)
	testutil.WriteFile(
		t,
		root,
		"flux/supplier.yaml",
		testutil.KustomizationYAML("supplier", "apps", "  path: ./supplier\n"),
	)
	testutil.WriteFile(t, root, "flux/unused.yaml", testutil.KustomizationYAML("unused", "apps", "  path: ./unused\n"))
	testutil.WriteFile(t, root, "supplier/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"supplier/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {value: supplied}\n",
	)
	testutil.WriteFile(t, root, "consumer/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"consumer/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: result, namespace: apps}\ndata: {value: '${value}'}\n",
	)
	testutil.WriteFile(t, root, "unused/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"unused/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unused, namespace: apps}\n",
	)
	st := store.New()
	l := loader.New(st)
	consumer := testutil.KustomizationID("apps", "consumer")
	configureSelection(t, l, selection.Request{Roots: []manifest.NamedResource{consumer}})
	configureSelection(t, l, selection.Request{Roots: []manifest.NamedResource{consumer}})
	supplier := testutil.KustomizationID("apps", "supplier")
	unused := testutil.KustomizationID("apps", "unused")
	data := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "hello"}
	_, err := beginTest(
		t.Context(),
		testConfig(t, Config{
			Path:      filepath.Join(root, "flux"),
			RepoRoot:  root,
			Store:     st,
			Loader:    l,
			Selection: selection.Request{Roots: []manifest.NamedResource{consumer}},
		}),
	)
	assert.NoError(t, err)
	record, ok := l.Index.Lookup(data)
	assert.Equal(t, ok, true)
	assert.Equal(t, record.File, "supplier/cm.yaml")
	assert.Equal(
		t,
		slices.ContainsFunc(
			record.Productions,
			func(p manifest.Production) bool { return p.Materializer(data) == supplier },
		),
		true,
	)
	assert.Equal(t, st.GetObject(data), nil)
	assert.Equal(t, len(selectedNames(l)), 0)
	proposal, err := prepareSelection(t.Context(), t, l, selection.PrepareRequest{Invocation: true})
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, supplier), true)
	assert.Equal(t, slices.Contains(proposal.Ordinary, supplier), true)
	assert.Equal(t, slices.Contains(proposal.Needed, unused), false)
	assert.Equal(t, slices.Contains(proposal.Selected, supplier), false)
	committed, err := commitSelection(t.Context(), l, proposal, selection.Construction)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	assert.Equal(t, len(selectedNames(l)), 0)
	assert.Equal(t, l.Options.Session.Snapshot().OrdinaryDemand(supplier), true)
	assert.Equal(t, st.GetObject(unused), nil)
	assert.Equal(t, st.GetArtifact(unused, store.ArtifactContent), nil)
}
