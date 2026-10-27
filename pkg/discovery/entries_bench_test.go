package discovery

import (
	"testing"

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
				testutil.WriteFile(b, root, dir+"/objects.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: same}\ndata: {key: equal}\n---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: "+dir+"}\nspec: {url: oci://fixture.invalid/chart}\n")
			}
			b.ReportAllocs()
			for b.Loop() {
				entries, err := Plan(b.Context(), Config{Path: root, WipeSecrets: true})
				if err != nil {
					b.Fatal(err)
				}
				if len(entries) != 1 {
					b.Fatalf("entries: got %d, want 1", len(entries))
				}
				if entries[0].Path != root {
					b.Fatalf("entry: got %+v, want path %q", entries[0], root)
				}
			}
		})
	}
}
