package discovery

import (
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRun_DiscoveryOnlyEntry(t *testing.T) {
	t.Parallel()
	const root = "/virtual/discovery-only"
	input := fstest.MapFS{
		"Taskfile.yaml":        {Data: []byte("version: '3'\ntasks: {build: {cmds: [true]}}\n")},
		"clusters/a/sync.yaml": {Data: []byte(testutil.KustomizationYAML("a", "apps", "  path: ./clusters/a\n"))},
		"clusters/b/sync.yaml": {Data: []byte(testutil.KustomizationYAML("b", "apps", "  path: ./clusters/b\n"))},
	}
	st := store.New()
	observed := &entryGeneratorFS{FS: input}
	result, err := Run(
		t.Context(),
		testConfig(
			t,
			Config{
				Input: &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: observed},
				Path:  root,
				Store: st,
			},
		),
	)
	assert.NoError(t, err)
	assert.Equal(t, result.Binding, Binding{})
	assert.Equal(t, observed.calls.Load(), int64(0))
	for _, name := range []string{"a", "b"} {
		assert.FatalIf(
			t,
			st.GetObject(testutil.KustomizationID("apps", name)) == nil,
			"cluster %s was not discovered",
			name,
		)
	}
	for _, obj := range st.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization) {
		if obj.Named().ReportHidden() {
			t.Fatalf("discovery-only entry published %s", obj.Named())
		}
	}
}
