package manifest

import (
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
)

func TestEffectiveSourceID_Defaults(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		ref  kustomizev1.CrossNamespaceSourceReference
		want NamedResource
	}{
		{name: "omitted", want: BootstrapSourceID},
		{name: "missing kind", ref: kustomizev1.CrossNamespaceSourceReference{Name: "source"}, want: BootstrapSourceID},
		{name: "missing name", ref: kustomizev1.CrossNamespaceSourceReference{Kind: KindOCIRepository}, want: BootstrapSourceID},
		{
			name: "explicit bootstrap",
			ref: kustomizev1.CrossNamespaceSourceReference{
				Kind: BootstrapSourceID.Kind, Namespace: BootstrapSourceID.Namespace, Name: BootstrapSourceID.Name,
			},
			want: BootstrapSourceID,
		},
		{
			name: "default namespace",
			ref:  kustomizev1.CrossNamespaceSourceReference{Kind: KindGitRepository, Name: "source"},
			want: NamedResource{Group: SourceDomain, Kind: KindGitRepository, Namespace: "apps", Name: "source"},
		},
		{
			name: "explicit namespace and group",
			ref: kustomizev1.CrossNamespaceSourceReference{
				APIVersion: "custom.example/v1", Kind: KindOCIRepository, Namespace: "sources", Name: "source",
			},
			want: NamedResource{Group: "custom.example", Kind: KindOCIRepository, Namespace: "sources", Name: "source"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ks := &Kustomization{Namespace: "apps", Path: "./apps", SourceRef: tc.ref}
			authored := ks.SourceID()
			assert.Equal(t, ks.EffectiveSourceID(), tc.want)
			assert.Equal(t, ks.SourceID(), authored)
			assert.Equal(t, ks.SourceRef, tc.ref)
			source, path, present := ks.SourcePath()
			assert.Equal(t, source, authored)
			assert.Equal(t, path, "./apps")
			assert.Equal(t, present, true)
		})
	}
}
