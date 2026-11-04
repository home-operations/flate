package manifest

import (
	"errors"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
)

func TestResolve_NamespacePrecedence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, forced, authored, fallback, want string }{
		{"forced", "forced", "authored", "fallback", "forced"},
		{"authored", "", "authored", "fallback", "authored"},
		{"fallback", "", "", "fallback", "fallback"},
		{"empty", "", "", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			id := NamedResource{
				Group:     FluxKustomizeDomain,
				Kind:      KindKustomization,
				Namespace: tc.authored,
				Name:      "app",
			}
			want := NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Namespace: tc.want, Name: "app"}
			assert.Equal(t, Resolve(id, tc.forced, tc.fallback), want)
			assert.Equal(
				t,
				id,
				NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Namespace: tc.authored, Name: "app"},
			)
		})
	}
}

func TestNamedResource_BindLocal(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{KindGitRepository, KindOCIRepository, KindBucket} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			testNamedResourceBindLocalCase(t, &kind)
		},
		)
	}
	for _, id := range []NamedResource{
		{Group: "wrong.group", Kind: KindGitRepository, Namespace: "other", Name: "entry"},
		{Group: SourceDomain, Kind: "Unknown", Namespace: "other", Name: "entry"},
		{Group: SourceDomain, Kind: KindBucket, Name: "entry"},
		{Group: SourceDomain, Kind: KindOCIRepository, Namespace: "other"},
		{Kind: KindConfigMap, Namespace: "other", Name: "entry"},
	} {
		assert.Equal(t, id.CanBindLocal(), false)
		{
			obj, err := id.BindLocal(
				"/chosen/root",
			)
			assert.FatalIf(t, obj != nil || !errors.Is(err, ErrInput) ||
				!errors.Is(err, ErrFlux), "invalid identity %s: %v, %v", id, obj, err)
		}
	}
}

func TestNamedResource_Labels(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		id         NamedResource
		kind, full string
	}{
		{"core", NamedResource{Kind: "ConfigMap", Namespace: "ns", Name: "shared"}, "ConfigMap", "ConfigMap/ns/shared"},
		{"foreign group", NamedResource{Group: "example.test", Kind: "ConfigMap", Namespace: "ns", Name: "shared"}, "ConfigMap.example.test", "ConfigMap.example.test/ns/shared"},
		{"cluster", NamedResource{Group: "example.test", Kind: "Widget", Name: "shared"}, "Widget.example.test", "Widget.example.test/shared"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.id.KindLabel(), tc.kind)
			assert.Equal(t, tc.id.String(), tc.full)
		})
	}
}

func testNamedResourceBindLocalCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	id := NamedResource{Group: SourceDomain, Kind: (*testCaseInput), Namespace: "other", Name: "entry"}
	assert.Equal(t, id.CanBindLocal(), true)
	obj, err := id.BindLocal("/chosen/root")
	assert.NoError(t, err)
	assert.Equal(t, obj.Named(), id)
	assert.Equal(t, KindOf(obj).APIVersion(), sourcev1.GroupVersion.String())
	switch *testCaseInput {
	case KindGitRepository:
		{
			g, ok := obj.(*GitRepository)
			assert.FatalIf(t, !ok || g.URL != "file:///chosen/root" ||
				g.Provider != sourcev1.GitProviderGeneric, "decoded Git identity: %#v", obj)
		}
	case KindOCIRepository:
		{
			o, ok := obj.(*OCIRepository)
			assert.FatalIf(t, !ok || o.URL != "file:///chosen/root" ||
				o.Provider != sourcev1.GenericOCIProvider, "decoded OCI identity: %#v", obj)
		}
	case KindBucket:
		{
			b, ok := obj.(*Bucket)
			assert.FatalIf(t, !ok || b.Endpoint != "file:///chosen/root" || b.BucketName != id.Name ||
				b.Provider != sourcev1.BucketProviderGeneric, "decoded bucket identity: %#v", obj)
		}
	}
	doc := map[string]any{
		"apiVersion": sourcev1.GroupVersion.String(),
		"kind":       (*testCaseInput),
		"metadata":   map[string]any{"name": id.Name, "namespace": id.Namespace},
	}
	_, _, _, err = ParseDoc(doc, defaultParseDocOptions())
	{
		assert.FatalIf(
			t,
			!errors.Is(err, ErrInput) || !errors.Is(err, ErrFlux),
			"authored missing spec: %v",
			err,
		)
	}
}
