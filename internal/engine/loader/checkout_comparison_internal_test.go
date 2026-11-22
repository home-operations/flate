package loader

import (
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/filtertest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestSourceFacts_CheckoutProvider(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, source string
		provider     store.SourceProvider
		extent       store.SourceExtent
		checkout     bool
	}{
		{name: "fleet checkout", source: "fleet", provider: store.ProviderAlias, checkout: true},
		{name: "flux-system checkout", source: "flux-system", provider: store.ProviderAlias, checkout: true},
		{name: "flux-system external", source: "flux-system", provider: store.ProviderFetch},
		{name: "flux-system composed", source: "flux-system", provider: store.ProviderInput, extent: store.ExtentComposed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			source := &manifest.GitRepository{
				Name: tc.source, Namespace: "flux-system", URL: "https://fixture.invalid/source",
			}
			id := source.Named()
			st.AddObject(source)
			st.SetArtifact(id, &store.SourceArtifact{
				Provider: tc.provider, Extent: tc.extent, LocalPath: root, LocalRoot: root, FS: fstest.MapFS{},
			})
			l := New(st)
			l.SourceRoot = root
			l.Options.Changes = detectChanges(t, []string{"apps/changed.yaml"}, []string{"apps/changed.yaml"})
			l.SourceProvider = func(source manifest.NamedResource, reads *store.ReadSet, index *manifest.DiscoveryIndex) store.SourceProvider {
				assert.Equal(t, source, id)
				assert.Equal(t, index, l.Index)
				return reads.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact).Provider
			}
			var reads store.ReadSet
			reads.Init(st, manifest.NamedResource{}, nil)
			defer reads.Retire()
			facts := l.SourceFacts(id, &reads)
			assert.Equal(t, facts.Paired, false)
			assert.Equal(t, facts.External, tc.provider == store.ProviderFetch)
			if tc.checkout {
				assert.Equal(t, facts.Delta, l.Options.Changes)
				assert.Diff(t, facts.Files, []selection.SourceFile{
					{Path: "apps/changed.yaml", Indexed: "apps/changed.yaml"},
				})
			} else {
				assert.Equal(t, facts.Delta, (*change.Set)(nil))
				assert.Equal(t, len(facts.Files), 0)
			}
		})
	}
}

func TestSourceFacts_SuppliedCheckoutComparison(t *testing.T) {
	t.Parallel()
	st := store.New()
	l := New(st)
	l.SourceRoot = t.TempDir()
	l.Options.Changes = detectChanges(t, []string{"changed.yaml"}, []string{"changed.yaml"})
	checkoutProvider := filtertest.CheckoutProvider(st, l.SourceProvider)
	l.SourceProvider = checkoutProvider
	var reads store.ReadSet
	reads.Init(st, manifest.NamedResource{}, nil)
	defer reads.Retire()

	facts := l.SourceFacts(manifest.BootstrapSourceID, &reads)
	assert.Equal(t, facts.Input, (*store.SourceArtifact)(nil))
	assert.Equal(t, facts.Path, "")
	assert.Equal(t, facts.Delta, l.Options.Changes)
	assert.Diff(t, facts.Files, []selection.SourceFile{{Path: "changed.yaml", Indexed: "changed.yaml"}})

	l.SourceProvider = func(
		manifest.NamedResource, *store.ReadSet, *manifest.DiscoveryIndex,
	) store.SourceProvider {
		return store.ProviderGenerator
	}
	facts = l.SourceFacts(manifest.BootstrapSourceID, &reads)
	assert.Equal(t, facts.Path, "")
	assert.Equal(t, facts.External, false)
	assert.Equal(t, facts.PendingGeneration, true)
	assert.Equal(t, facts.Delta, (*change.Set)(nil))
	assert.Equal(t, len(facts.Files), 0)

	l.SourceProvider = checkoutProvider
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{Provider: store.ProviderInput})
	facts = l.SourceFacts(manifest.BootstrapSourceID, nil)
	assert.Equal(t, facts.Path, "")
	assert.Equal(t, facts.Delta, l.Options.Changes)
	assert.Diff(t, facts.Files, []selection.SourceFile{{Path: "changed.yaml", Indexed: "changed.yaml"}})
}
