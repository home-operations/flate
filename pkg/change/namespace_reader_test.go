package change

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestFilter_SharedFileReaderNamespace(t *testing.T) {
	t.Parallel()
	st := store.New()
	target := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "credentials"}
	owner := &manifest.Kustomization{Name: "owner", Namespace: "flux-system"}
	owner.TargetNamespace = "apps"
	reader := &manifest.Kustomization{Name: "same-namespace", Namespace: "flux-system"}
	reader.TargetNamespace = "apps"
	unrelated := &manifest.Kustomization{Name: "foreign-namespace", Namespace: "flux-system"}
	unrelated.TargetNamespace = "other"
	unknown := &manifest.Kustomization{Name: "unknown-namespace", Namespace: "flux-system"}
	consumer := &manifest.HelmRelease{Name: "consumer", Namespace: "apps", ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindSecret, Name: target.Name}}}
	st.AddObjects([]manifest.BaseManifest{owner, reader, unrelated, unknown, consumer})
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		consumer.Named(): {File: "consumer.yaml"},
		target: {Productions: []manifest.Production{{Owner: owner.Named(), Source: manifest.BootstrapSourceID,
			Path: "/repo/common/secret.yaml", File: "common/secret.yaml"}}},
	}, Readers: map[string][]manifest.FileReader{"common/secret.yaml": {{Reader: reader.Named(), Source: manifest.BootstrapSourceID}, {Reader: unrelated.Named(), Source: manifest.BootstrapSourceID}, {Reader: unknown.Named(), Source: manifest.BootstrapSourceID}}}}
	f := NewFilter(NewSet([]string{"consumer.yaml"}), index, st, "/repo")
	for _, id := range []manifest.NamedResource{consumer.Named(), target, owner.Named(), reader.Named(), unknown.Named()} {
		assert.Equal(t, f.ShouldReconcile(id), true)
	}
	assert.Equal(t, f.ShouldReconcile(unrelated.Named()), false)
}
