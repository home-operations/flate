package orchestrator

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/engine/schedule"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:paralleltest // The orchestrator fixture changes process-wide source policy.
func TestRender_FluxInstanceGeneratorSource(t *testing.T) {
	for _, tc := range []struct {
		name, ref, material string
		provider            store.SourceProvider
	}{
		{name: "checkout", ref: "main", material: "checkout", provider: store.ProviderAlias},
		{name: "digest", ref: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", material: "upstream"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, upstream := t.TempDir(), t.TempDir()
			checkoutTree := fluxInstanceGeneratorFixture{
				ref: tc.ref, name: "{namespace}-{app}", copyGlob: "base/**", material: "checkout",
			}
			writeFluxInstanceGeneratorTree(t, root, checkoutTree)
			upstreamTree := checkoutTree
			upstreamTree.material = "upstream"
			if tc.provider != store.ProviderFetch {
				upstreamTree.name, upstreamTree.copyGlob = "{app}", "base/cert-manager/**"
			}
			writeFluxInstanceGeneratorTree(t, upstream, upstreamTree)
			o, err := newTestOrchestrator(t, Tree{Path: filepath.Join(root, "entry"), RepoRoot: root},
				Config{Concurrency: 2})
			assert.NoError(t, err)
			fetcher := &generatedOCIFetcher{root: upstream}
			o.src.Fetchers[manifest.KindOCIRepository] = fetcher
			assert.NoError(t, o.bootstrap(t.Context()))
			assert.NoError(t, o.run(t.Context()))
			assert.Equal(t, fetcher.calls.Load() != 0, tc.provider == store.ProviderFetch)
			selected := assertFluxInstanceGeneratorSource(t, o.store, tc.provider, tc.material)
			id := testutil.KustomizationID("flux-system", "consumer")
			docs := outputByParent(o.collectManifests())[id]
			assert.FatalIf(t, len(docs) != 1, "consumer documents: %v", docs)
			assert.Diff(t, docs[0]["data"], map[string]any{"value": tc.material})

			nextRoot := t.TempDir()
			checkoutTree.material = tc.material + "-updated"
			writeFluxInstanceGeneratorTree(t, nextRoot, checkoutTree)
			nextInput := testDirectoryInput(t, nextRoot)
			next := selected.WithSourceView(nextInput.FS)
			next.Content = nextInput.Content
			o.store.SetArtifact(fluxInstanceTestSourceID(), next)
			o.tasks.Go(t.Context(), "generator", func(ctx context.Context) {
				run := o.agc.ReconcileNode(ctx, fluxInstanceTestGeneratorID(), manifest.DrainCascade, schedule.Result{})
				defer run.Close()
				assert.Equal(t, run.Outcome(), schedule.Terminal)
			})
			o.tasks.BlockTillDone()
			assertFluxInstanceGeneratorSource(t, o.store, tc.provider, checkoutTree.material)
		})
	}
}

func TestRenderTrees_ChangedOnlyAndDiff_FluxInstanceGeneratorSource(t *testing.T) {
	t.Parallel()
	materials := [2]string{"before", "after"}
	trees := make([]Tree, len(materials))
	for side, material := range materials {
		root := t.TempDir()
		writeFluxInstanceGeneratorTree(t, root, fluxInstanceGeneratorFixture{
			ref: "main", name: "{namespace}-{app}", copyGlob: "base/**", material: material,
		})
		trees[side] = Tree{
			Path: filepath.Join(root, "entry"), RepoRoot: root,
			ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)},
		}
	}
	for side := range trees {
		trees[side].Input = testDirectoryInput(t, trees[side].RepoRoot)
		trees[side].BaseInput = trees[0].Input
	}
	results, err := renderTreesForTest(t.Context(), t, trees, Config{
		Concurrency: 2, Selection: selection.Request{Changed: true},
	})
	assert.NoError(t, err)
	assert.Equal(t, len(results), len(trees))
	for side := range results {
		result := &results[side]
		assert.NoError(t, result.Err)
		assertFluxInstanceGeneratorSource(t, result.store, store.ProviderAlias, materials[side])
		docs := outputByParent(result.Result.Manifests)[testutil.KustomizationID("flux-system", "consumer")]
		assert.FatalIf(t, len(docs) != 1, "consumer documents on side %d: %v", side, docs)
		assert.Diff(t, docs[0]["data"], map[string]any{"value": materials[side]})
	}
}

func fluxInstanceTestGeneratorID() manifest.NamedResource {
	return manifest.NamedResource{
		Group: manifest.SourceExtensionsDomain, Kind: manifest.KindArtifactGenerator,
		Namespace: "flux-system", Name: "apps",
	}
}

func fluxInstanceTestSourceID() manifest.NamedResource {
	return manifest.NamedResource{
		Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "sync",
	}
}

type fluxInstanceGeneratorFixture struct {
	ref, name, copyGlob, material string
}

func writeFluxInstanceGeneratorTree(t *testing.T, root string, fixture fluxInstanceGeneratorFixture) {
	t.Helper()
	testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [apps, checkout.yaml]\n")
	testutil.WriteFile(t, root, "entry/checkout.yaml", testutil.KustomizationYAML("checkout", "flux-system",
		"  path: ./base/cert-manager\n  sourceRef: {kind: OCIRepository, name: sync}\n"))
	testutil.WriteFile(
		t,
		root,
		"entry/apps/kustomization.yaml",
		"resources: [release.yaml, generator.yaml, consumer.yaml]\n",
	)
	testutil.WriteFile(t, root, "entry/apps/release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: instance, namespace: flux-system}
spec:
  interval: 1m
  chart:
    spec:
      chart: chart
      sourceRef: {kind: GitRepository, name: flux-system}
`)
	testutil.WriteFile(t, root, "chart/Chart.yaml", "apiVersion: v2\nname: instance\nversion: 1.0.0\n")
	testutil.WriteFile(t, root, "chart/templates/instance.yaml", fmt.Sprintf(`apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: instance, namespace: flux-system}
spec:
  distribution: {version: 2.6.x}
  sync: {name: sync, kind: OCIRepository, url: oci://127.0.0.1:0/sync, ref: %q, path: ./entry/apps}
`, fixture.ref))
	testutil.WriteFile(
		t,
		root,
		"entry/apps/generator.yaml",
		fmt.Sprintf(`apiVersion: source.extensions.fluxcd.io/v1beta1
kind: ArtifactGenerator
metadata: {name: apps, namespace: flux-system}
spec:
  sources: [{alias: repo, kind: OCIRepository, name: sync}]
  pathPattern: '@repo/apps/{namespace}/{app}'
  artifacts:
    - name: %q
      copy: [{from: '@repo/%s', to: '@artifact/'}]
`, fixture.name, fixture.copyGlob),
	)
	testutil.WriteFile(t, root, "entry/apps/consumer.yaml", testutil.KustomizationYAML("consumer", "flux-system",
		"  path: ./cert-manager\n  sourceRef: {kind: ExternalArtifact, name: cert-manager-clusterissuer}\n"))
	testutil.WriteFile(t, root, "apps/cert-manager/clusterissuer/marker.txt", "app path")
	testutil.WriteFile(t, root, "base/cert-manager/kustomization.yaml", "resources: [payload.yaml]\n")
	testutil.WriteFile(t, root, "base/cert-manager/payload.yaml", fluxInstanceGeneratorPayload(fixture.material))
}

func fluxInstanceGeneratorPayload(material string) string {
	return "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: copied}\ndata: {value: " + material + "}\n"
}

func assertFluxInstanceGeneratorSource(
	t *testing.T, st *store.Store, provider store.SourceProvider, material string,
) *store.SourceArtifact {
	t.Helper()
	id := fluxInstanceTestGeneratorID()
	object, document, emission := st.GetEmission(id)
	defer emission.Finish()
	assert.FatalIf(t, object == nil || document == nil, "missing generator declaration")
	spec := document["spec"].(map[string]any)
	assert.Diff(t, object.(*manifest.RawObject).Spec, spec)
	assert.Diff(t, spec["artifacts"], []any{map[string]any{
		"name": "{namespace}-{app}", "copy": []any{map[string]any{"from": "@repo/base/**", "to": "@artifact/"}},
	}})
	artifact, _ := st.GetArtifact(fluxInstanceTestSourceID(), store.ArtifactContent).(*store.SourceArtifact)
	assert.FatalIf(t, !artifact.Available(), "sync source is unreadable")
	assert.Equal(t, artifact.Provider, provider)
	data, err := fs.ReadFile(artifact.FS, "base/cert-manager/payload.yaml")
	assert.NoError(t, err)
	assert.Equal(t, string(data), fluxInstanceGeneratorPayload(material))
	output := manifest.NamedResource{
		Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact,
		Namespace: "flux-system", Name: "cert-manager-clusterissuer",
	}
	targets := manifest.ProducerTargets(object, output)
	assert.Equal(t, len(targets), 1)
	assert.Equal(t, targets[0].NamedResource, output)
	generated, _ := st.GetArtifact(output, store.ArtifactContent).(*store.SourceArtifact)
	assert.FatalIf(t, !generated.Available(), "generated artifact %s is unreadable", output)
	got, err := fs.ReadFile(generated.FS, "cert-manager/payload.yaml")
	assert.NoError(t, err)
	assert.Equal(t, string(got), fluxInstanceGeneratorPayload(material))
	return artifact
}
