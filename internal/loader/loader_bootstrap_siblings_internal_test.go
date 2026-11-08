package loader

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/selection"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestLoader_DiscoveryPicksUpBootstrapSiblingKS(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "repositories/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ./source.yaml
generators: ["# flux-entry.yaml"]
`)
	testutil.WriteFile(t, dir, "repositories/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata:
  name: chart
  namespace: flux-system
spec:
  url: oci://example.com/chart
  ref:
    tag: 1.0.0
`)
	testutil.WriteFile(
		t,
		dir,
		"repositories/# flux-entry.yaml",
		testutil.KustomizationYAML(
			"flux-entry-repos",
			"flux-system",
			"  path: ./repositories\n  sourceRef:\n    kind: GitRepository\n    name: cluster\n  interval: 10m\n",
		),
	)
	testutil.WriteFile(t, dir, "repositories/stray-hr.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: stray
  namespace: flux-system
spec:
  chartRef:
    kind: OCIRepository
    name: chart
    namespace: flux-system
`)

	s := store.New()
	l := loaderFixture(s, nil)
	l.SourceRoot = dir
	{
		_, err := loadForTest(context.Background(), t, l, dir)
		assert.FatalIf(t, err != nil, "Load: %v", err)
	}

	ksID := testutil.KustomizationID("flux-system", "flux-entry-repos")
	assert.ErrorIf(t, s.GetObject(ksID) != nil ||
		selection.Root(
			rootAtForTest(
				l,
				l.IndexedObject(ksID, l.Index.Records[ksID]).Obj,
				filepath.Join(dir, "repositories", "# flux-entry.yaml"),
			),
		).Admitted, "%s", "unlisted declaration became runnable")
	{
		got, want := filesForTest(l.Index)[ksID], "repositories/# flux-entry.yaml"
		assert.ErrorIf(t, got != want, "SourceFiles[KS] = %q; want %q", got, want)
	}

	hrID := testutil.HelmReleaseID("flux-system", "stray")
	assert.ErrorIf(
		t,
		s.GetObject(hrID) != nil,
		"sibling HR must NOT be picked up by the bootstrap scan (scope is Flux KS only)",
	)
	{
		_, ok := l.Index.Lookup(hrID)
		assert.ErrorIf(t, ok, "sibling HR must NOT be recorded in Existence either — it's not in the kustomize graph")
	}
}

// TestLoader_BootstrapSiblingScanDedupesWithResources guards the
// "in BOTH places" edge case: a Flux KS file listed in resources AND
// sitting beside the kustomization.yaml must load exactly once. The
// dedup key is the resolved absolute path on each side.
func TestLoader_BootstrapSiblingScanDedupesWithResources(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ./entry.yaml
`)
	testutil.WriteFile(
		t,
		dir,
		"entry.yaml",
		testutil.KustomizationYAML(
			"entry",
			"flux-system",
			"  path: ./\n  sourceRef:\n    kind: GitRepository\n    name: cluster\n  interval: 10m\n",
		),
	)
	s := store.New()
	l := loaderFixture(s, nil)
	l.SourceRoot = dir
	{
		_, err := loadForTest(context.Background(), t, l, dir)
		assert.FatalIf(t, err != nil, "Load: %v", err)
	}
	{
		got := len(s.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization))
		assert.ErrorIf(t, got != 1, "expected exactly 1 Kustomization (no double-load), got %d", got)
	}
}

// TestLoader_BootstrapSiblingHonorsKrmignore guards the ignore
// integration: a Flux KS sibling that matches a .krmignore pattern
// must NOT be loaded. Otherwise the bootstrap scan would silently
// override the user's explicit "don't look here" directive — the rest
// of the loader honors .krmignore for ad-hoc and resource paths.
func TestLoader_BootstrapSiblingHonorsKrmignore(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, ".krmignore", "flux-entry.yaml\n")
	testutil.WriteFile(t, dir, "kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources: []
`)
	testutil.WriteFile(
		t,
		dir,
		"flux-entry.yaml",
		testutil.KustomizationYAML(
			"ignored",
			"flux-system",
			"  path: ./\n  sourceRef:\n    kind: GitRepository\n    name: cluster\n  interval: 10m\n",
		),
	)
	s := store.New()
	l := loaderFixture(s, nil)
	l.SourceRoot = dir
	{
		_, err := loadForTest(context.Background(), t, l, dir)
		assert.FatalIf(t, err != nil, "Load: %v", err)
	}
	{
		got := len(s.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization))
		assert.ErrorIf(t, got != 0, "ignored bootstrap-sibling KS must NOT load, got %d in Store", got)
	}
}
