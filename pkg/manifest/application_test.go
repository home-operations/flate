package manifest

import (
	"maps"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
)

func TestEntryApplication_Kustomization(t *testing.T) {
	t.Parallel()
	source := NamedResource{Group: SourceDomain, Kind: KindGitRepository, Name: "source", Namespace: "sources"}
	for _, tc := range []struct {
		name   string
		object *Kustomization
		source NamedResource
		valid  bool
	}{
		{name: "authored", object: &Kustomization{Name: "entry", Namespace: "apps", Path: "./apps", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: source.Kind, Name: source.Name, Namespace: source.Namespace}}, source: source, valid: true},
		{name: "implicit source", object: &Kustomization{Name: "owner", Path: "."}, source: BootstrapSourceID, valid: true},
		{name: "fragment", object: &Kustomization{Name: "fragment"}},
		{name: "internal", object: NewEntryKustomization(BootstrapSourceID, ".", NamedResource{})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			application, valid := tc.object.EntryApplication()
			assert.Equal(t, valid, tc.valid)
			if tc.valid {
				assert.Equal(t, application.Source, tc.source)
				assert.Equal(t, application.Path, tc.object.Path)
				assert.Equal(t, application.Declaration, BaseManifest(nil))
			}
		})
	}
}

func TestEntryApplication_FluxInstance(t *testing.T) {
	t.Parallel()
	secret := &meta.LocalObjectReference{Name: "credentials"}
	for _, tc := range []struct {
		name     string
		sync     map[string]any
		expected BaseManifest
	}{
		{name: "git namespace default", sync: map[string]any{"kind": KindGitRepository, "url": "https://fixture.invalid/self", "ref": "refs/heads/main", "path": "./clusters", "pullSecret": "credentials", "provider": "github"},
			expected: NewGitRepository(sourcev1.GitRepository{Name: "control", Namespace: "control", Spec: sourcev1.GitRepositorySpec{URL: "https://fixture.invalid/self", Reference: &sourcev1.GitRepositoryRef{Name: "refs/heads/main"}, SecretRef: secret, Provider: "github"}})},
		{name: "oci explicit name", sync: map[string]any{"name": "custom", "kind": KindOCIRepository, "url": "oci://fixture.invalid/self", "ref": "latest", "path": "./clusters", "pullSecret": "credentials", "provider": "aws"},
			expected: NewOCIRepository(sourcev1.OCIRepository{Name: "custom", Namespace: "control", Spec: sourcev1.OCIRepositorySpec{URL: "oci://fixture.invalid/self", Reference: &sourcev1.OCIRepositoryRef{Tag: "latest"}, SecretRef: secret, Provider: "aws"}})},
		{name: "bucket", sync: map[string]any{"kind": KindBucket, "url": "s3.fixture.invalid", "ref": "cluster", "path": "./clusters", "pullSecret": "credentials"},
			expected: NewBucket(sourcev1.Bucket{Name: "control", Namespace: "control", Spec: sourcev1.BucketSpec{Endpoint: "s3.fixture.invalid", BucketName: "cluster", SecretRef: secret}})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			object := &RawObject{APIVersion: FluxOperatorDomain + "/v1", Kind: "FluxInstance", Namespace: "control", Name: "flux", Spec: map[string]any{"sync": tc.sync}}
			before := DeepCopyMap(object.Spec)
			application, valid := object.EntryApplication()
			assert.Equal(t, valid, true)
			assert.Equal(t, application.Source, tc.expected.Named())
			assert.Equal(t, application.Path, "./clusters")
			if !Equal(application.Declaration, tc.expected) {
				t.Fatalf("source projection differs: got %+v, want %+v", application.Declaration, tc.expected)
			}
			source, path, present := object.SourcePath()
			assert.Equal(t, source, application.Source)
			assert.Equal(t, path, application.Path)
			assert.Equal(t, present, true)
			assert.Diff(t, object.Spec, before)
		})
	}
	for _, missing := range []string{"kind", "url", "ref", "path"} {
		t.Run("missing "+missing, func(t *testing.T) {
			t.Parallel()
			sync := map[string]any{"kind": KindGitRepository, "url": "https://fixture.invalid/self", "ref": "refs/heads/main", "path": "./clusters"}
			delete(sync, missing)
			object := &RawObject{APIVersion: FluxOperatorDomain + "/v1", Kind: "FluxInstance", Namespace: "control", Spec: map[string]any{"sync": sync}}
			_, valid := object.EntryApplication()
			assert.Equal(t, valid, false)
			sync = maps.Clone(sync)
			sync[missing] = []string{"malformed"}
			object.Spec = map[string]any{"sync": sync}
			_, valid = object.EntryApplication()
			assert.Equal(t, valid, false)
		})
	}
	for _, tc := range []struct{ name, group, kind, sourceKind string }{
		{"foreign group", "example.test", "FluxInstance", KindGitRepository},
		{"foreign kind", FluxOperatorDomain, "Other", KindGitRepository},
		{"unsupported source", FluxOperatorDomain, "FluxInstance", KindHelmRepository},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			object := &RawObject{APIVersion: tc.group + "/v1", Kind: tc.kind, Namespace: "control", Spec: map[string]any{"sync": map[string]any{"kind": tc.sourceKind, "url": "https://fixture.invalid/self", "ref": "main", "path": "."}}}
			_, valid := object.EntryApplication()
			assert.Equal(t, valid, false)
		})
	}
}
