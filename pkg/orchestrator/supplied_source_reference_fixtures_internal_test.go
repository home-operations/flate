package orchestrator

import (
	"fmt"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/change"
	"github.com/home-operations/flate/internal/loader"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func testSuppliedSourceReference(t *testing.T, kind, mode string) {
	t.Helper()
	suppliedSourceReferenceFixtureResult1 := suppliedSourceReferenceFixture(t, kind)
	root, artifact, inputs := suppliedSourceReferenceFixtureResult1.root, suppliedSourceReferenceFixtureResult1.artifact, suppliedSourceReferenceFixtureResult1.declarations
	tree := Tree{Path: root, RepoRoot: root}
	if mode == "changed" {
		tree.ExternalChanges = change.NewSet([]string{"consumer.yaml"})
	}
	id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: kind, Namespace: "supply-only", Name: "external"}
	cfg := Config{Concurrency: 2, Supply: inputs}
	if mode == "selected all" {
		for info := range manifest.Kinds() {
			if info.Render {
				cfg.Selection.Roots = append(
					cfg.Selection.Roots,
					manifest.NamedResource{Group: info.Group, Kind: info.Kind},
				)
			}
		}
	}
	if mode == "selected reference" {
		cfg.Selection.Roots = []manifest.NamedResource{id}
	}
	o, err := newTestOrchestrator(t, tree, cfg)
	assert.NoError(t, err)
	fetcher := &countedChartFetcher{chart: artifact}
	charts := &sharedRepositoryFetcher{calls: map[manifest.NamedResource]int{}}
	if kind == manifest.KindHelmRepository || kind == manifest.KindHelmChart {
		o.src.Fetchers[manifest.KindHelmChart] = charts
	} else {
		o.src.Fetchers[kind] = fetcher
	}
	result, err := o.render(t.Context(), t)
	assert.NoError(t, err)
	if mode == "selected reference" {
		if len(result.Manifests) != 0 || fetcher.calls.Load() != 0 || len(charts.calls) != 0 {
			t.Fatal("reference produced output or acquired a source")
		}
	} else if kind == manifest.KindOCIRepository || kind == manifest.KindBucket {
		if fetcher.calls.Load() != 1 {
			t.Fatalf("acquisitions=%d", fetcher.calls.Load())
		}
	}
	assertSuppliedSourceReference(t, o, result, mode)
}

type suppliedReferenceFixture struct {
	root         string
	artifact     string
	declarations []store.Declaration
}

func suppliedSourceReferenceFixture(t *testing.T, kind string) suppliedReferenceFixture {
	t.Helper()
	root, artifact := t.TempDir(), t.TempDir()
	testutil.WriteFile(t, artifact, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		artifact,
		"app/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: from-source, namespace: apps}\n",
	)
	spec := "{url: 'oci://fixture.invalid/chart', ref: {tag: latest}}"
	consumer := testutil.KustomizationYAML("consumer",
		"apps",
		"  path: ./app\n  sourceRef: {kind: "+kind+", name: external, namespace: supply-only}\n")
	if kind == manifest.KindBucket {
		spec = "{endpoint: fixture.invalid, bucketName: fixture}"
	}
	if kind == manifest.KindHelmRepository || kind == manifest.KindHelmChart {
		spec = "{url: 'https://fixture.invalid/charts'}"
		chart := "chartRef: {kind: HelmChart, name: external, namespace: supply-only}"
		if kind == manifest.KindHelmRepository {
			chart = "chartRef: {kind: HelmChart, name: chart, namespace: supply-only}"
		} else {
			spec = "{chart: demo, version: 1.0.0, sourceRef: {kind: HelmRepository, name: repository, namespace: supply-only}}"
		}
		consumer = "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: consumer, " +
			"namespace: apps}\nspec: {interval: 1h, " + chart + "}\n"
	}
	testutil.WriteFile(t, root, "consumer.yaml", consumer)
	stream := fmt.Sprintf("apiVersion: source.toolkit.fluxcd.io/v1\nkind: %s\nmetadata: {name: external, namespa"+
		"ce: supply-only}\nspec: %s\n---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: %s\nm"+
		"etadata: {name: unused, namespace: supply-only}\nspec: %s\n",
		kind,
		spec,
		kind,
		spec)
	if kind == manifest.KindHelmChart {
		stream += "---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: HelmRepository\nmetadata: {name: " +
			"repository, namespace: supply-only}\nspec: {url: 'https://fixture.invalid/charts'}\n"
	}
	if kind == manifest.KindHelmRepository {
		stream += "---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: HelmChart\nmetadata: {name: chart" +
			", namespace: supply-only}\nspec: {chart: demo, version: 1.0.0, sourceRef: {kind: Helm" +
			"Repository, name: external}}\n"
	}
	inputs, err := loader.LoadDocuments(t.Context(), strings.NewReader(stream), "supply.yaml")
	assert.NoError(t, err)
	return suppliedReferenceFixture{root: root, artifact: artifact, declarations: inputs}
}
