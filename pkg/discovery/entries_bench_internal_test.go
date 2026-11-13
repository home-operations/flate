package discovery

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func BenchmarkPlan_RepeatedIdentity(b *testing.B) {
	for _, tc := range []struct{ name, projection string }{
		{name: "untransformed"},
		{name: "transformed", projection: "namespace: apps\nnamePrefix: pre-\nnameSuffix: -post\n"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			root := b.TempDir()
			for _, dir := range []string{"alpha", "omega"} {
				testutil.WriteFile(b, root, dir+"/kustomization.yaml", tc.projection+"resources: [objects.yaml]\n")
				testutil.WriteFile(
					b,
					root,
					dir+"/objects.yaml",
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: same}\ndata: {key: equal}\n---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: "+dir+"}\nspec: {url: oci://fixture.invalid/chart}\n",
				)
			}
			b.ReportAllocs()
			for b.Loop() {
				entries, err := Plan(b.Context(), Config{Path: root})
				assert.NoError(b, err)
				// The owner ruling restores #1379's multi-entry benchmark assertions.
				assert.FatalIf(b, len(entries) != 2, "entries: got %d, want 2", len(entries))
				for i, dir := range []string{"alpha", "omega"} {
					assert.Equal(b, entries[i].Path, filepath.Join(root, dir))
				}
			}
		})
	}
}
