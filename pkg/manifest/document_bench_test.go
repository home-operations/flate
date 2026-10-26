package manifest

import "testing"

func BenchmarkRenderDocument_Kustomization(b *testing.B) {
	k := &Kustomization{Name: "app", Namespace: "flux-system"}
	k.Path = "./apps"
	for b.Loop() {
		if _, err := k.RenderDocument(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkObjectDocument_DesiredSpec(b *testing.B) {
	for _, tc := range []struct {
		kind, version, spec string
	}{
		{KindKustomization, FluxKustomizeDomain + "/v1", "path: ./apps\npostBuild: {substitute: {releaseVersion: v1}}"},
		{KindHelmRelease, HelmReleaseDomain + "/v2", "chartRef: {kind: OCIRepository, name: chart}\nvalues: {nested: {integer: 42, releaseVersion: v1}}"},
	} {
		b.Run(tc.kind, func(b *testing.B) {
			docs, err := SplitDocs([]byte("apiVersion: " + tc.version + "\nkind: " + tc.kind + "\nmetadata: {name: app, namespace: flux-system}\nspec: {}\n"))
			if err != nil {
				b.Fatal(err)
			}
			specs, err := SplitDocs([]byte(tc.spec))
			if err != nil {
				b.Fatal(err)
			}
			docs[0]["spec"] = specs[0]
			obj, _, _, err := ParseDoc(docs[0], ParseDocOptions{})
			if err != nil {
				b.Fatal(err)
			}
			id := obj.Named()
			for b.Loop() {
				if _, err := ObjectDocument(id, obj); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
