package discovery

import (
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestAdmission_ConstructionViewReuse(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{
		"payload/file": {Data: []byte("initial")},
	}}
	d := newDiscoverer(
		&Config{Path: root, Input: input, Store: store.New(), SelfURLs: []string{"https://example.test/self"}},
	)
	t.Cleanup(d.Close)
	_, err := d.resolveTestSources(t.Context(), false)
	assert.NoError(t, err)
	var previous fs.FS
	for _, content := range []string{"initial", "replacement"} {
		if content == "replacement" {
			replaced := *input
			replaced.Provider, replaced.Content = store.ProviderInput, content
			replaced.FS = fstest.MapFS{"payload/file": {Data: []byte(content)}}
			d.cfg.Store.SetArtifact(manifest.BootstrapSourceID, &replaced)
			_, err = d.resolveTestSources(t.Context(), false)
			assert.NoError(t, err)
		}
		var first fs.FS
		for range 2 {
			parsed, parseErr := manifest.ParseDoc(
				retainedApplicationDocument("FluxInstance", "."),
				manifest.ParseDocOptions{},
			)
			assert.NoError(t, parseErr)
			application, valid := manifest.ApplicationFor(parsed.Object)
			assert.Equal(t, valid, true)
			generatedSource := testGeneratedSync(t, parsed.Object).Objects[0]
			_, resolveErr := d.resolveEntrySource(t.Context(), d.admission(), application.Source, &entrySourceGroup{
				object: generatedSource, entry: true, projected: true,
			})
			assert.Equal(t, resolveErr == nil || errors.Is(resolveErr, errPendingSources), true)
			assert.NoError(t, constructTestSources(t.Context(), d))
			material := d.resolvedSources.snapshot()[application.Source].artifact
			assert.FatalIf(t, material == nil, "projected source lost local material")
			data, readErr := fs.ReadFile(material.FS, "payload/file")
			assert.NoError(t, readErr)
			assert.Equal(t, string(data), content)
			if first == nil {
				first = material.FS
			}
			assert.Equal(t, material.FS == first, true)
		}
		assert.Equal(t, first == previous, false)
		previous = first
	}
}
