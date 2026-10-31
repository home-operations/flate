package manifest

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"maps"
	"testing"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
			expected: NewGitRepository(sourcev1.GitRepository{Name: "control", Namespace: "control", Spec: sourcev1.GitRepositorySpec{Interval: metav1.Duration{Duration: time.Minute}, URL: "https://fixture.invalid/self", Reference: &sourcev1.GitRepositoryRef{Name: "refs/heads/main"}, SecretRef: secret, Provider: "github"}})},
		{name: "oci explicit name", sync: map[string]any{"name": "custom", "kind": KindOCIRepository, "url": "oci://fixture.invalid/self", "ref": "latest", "path": "./clusters", "pullSecret": "credentials", "provider": "aws"},
			expected: NewOCIRepository(sourcev1.OCIRepository{Name: "custom", Namespace: "control", Spec: sourcev1.OCIRepositorySpec{Interval: &metav1.Duration{Duration: time.Minute}, URL: "oci://fixture.invalid/self", Reference: &sourcev1.OCIRepositoryRef{Tag: "latest"}, SecretRef: secret, Provider: "aws"}})},
		{name: "bucket", sync: map[string]any{"kind": KindBucket, "url": "s3.fixture.invalid", "ref": "cluster", "path": "./clusters", "pullSecret": "credentials"},
			expected: NewBucket(sourcev1.Bucket{Name: "control", Namespace: "control", Spec: sourcev1.BucketSpec{Interval: metav1.Duration{Duration: time.Minute}, Endpoint: "s3.fixture.invalid", BucketName: "cluster", SecretRef: secret}})},
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

func TestSyncProjection_SourceContract(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, kind, syncName, interval, provider, secret string
		wantSpec                                         map[string]any
	}{
		{name: "git defaults", kind: KindGitRepository, wantSpec: map[string]any{
			"interval": "1m0s", "url": "https://fixture.invalid/self", "ref": map[string]any{"name": "main"},
		}},
		{name: "oci explicit", kind: KindOCIRepository, syncName: "custom", interval: "3m", provider: "aws", secret: "credentials", wantSpec: map[string]any{
			"interval": "3m0s", "url": "https://fixture.invalid/self", "ref": map[string]any{"tag": "main"},
			"provider": "aws", "secretRef": map[string]any{"name": "credentials"},
		}},
		{name: "bucket explicit", kind: KindBucket, interval: "5m", provider: "gcp", secret: "credentials", wantSpec: map[string]any{
			"interval": "5m0s", "endpoint": "https://fixture.invalid/self", "bucketName": "main",
			"provider": "gcp", "secretRef": map[string]any{"name": "credentials"},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sync := map[string]any{"kind": tc.kind, "url": "https://fixture.invalid/self", "ref": "main", "path": "./clusters"}
			for key, value := range map[string]string{"name": tc.syncName, "interval": tc.interval, "provider": tc.provider, "pullSecret": tc.secret} {
				if value != "" {
					sync[key] = value
				}
			}
			object := &RawObject{
				APIVersion: FluxOperatorDomain + "/v1", Kind: "FluxInstance", Namespace: "control",
				Spec: map[string]any{
					"sync": sync, "distribution": map[string]any{"version": "2.x"},
					"kustomize": map[string]any{"patches": []any{map[string]any{
						"target": map[string]any{"kind": tc.kind},
						"patch":  "- op: replace\n  path: /spec/interval\n  value: 7m",
					}}},
				},
			}
			before := DeepCopyMap(object.Spec)
			projection, err := object.SyncProjection()
			assert.NoError(t, err)
			if projection == nil {
				t.Fatal("missing sync projection")
			}
			name := cmp.Or(tc.syncName, "control")
			documents := bytes.Split(projection.Documents, []byte("\n---\n"))
			assert.Equal(t, len(documents), 2)
			var source, root map[string]any
			assert.NoError(t, json.Unmarshal(documents[0], &source))
			assert.NoError(t, json.Unmarshal(documents[1], &root))
			metadata := map[string]any{"name": name, "namespace": "control"}
			assert.Diff(t, source["metadata"], metadata)
			assert.Equal(t, source["apiVersion"], any(sourcev1.GroupVersion.String()))
			assert.Equal(t, source["kind"], any(tc.kind))
			assert.Diff(t, source["spec"], tc.wantSpec)
			assert.Diff(t, root["metadata"], metadata)
			assert.Equal(t, root["apiVersion"], any(kustomizev1.GroupVersion.String()))
			assert.Equal(t, root["kind"], any(KindKustomization))
			assert.Diff(t, root["spec"], map[string]any{
				"interval": "10m0s", "path": "./clusters", "prune": true,
				"sourceRef": map[string]any{"kind": tc.kind, "name": name},
			})
			assert.Equal(t, projection.Namespace, "control")
			assert.Equal(t, len(projection.Patches), 1)
			assert.Equal(t, projection.Patches[0].Target.Kind, tc.kind)
			assert.Equal(t, len(projection.Transformers), 2)
			for i, kind := range []string{"AnnotationsTransformer", "LabelTransformer"} {
				var transformer map[string]any
				assert.NoError(t, json.Unmarshal(projection.Transformers[i], &transformer))
				assert.Equal(t, transformer["kind"], any(kind))
			}
			var labels map[string]any
			assert.NoError(t, json.Unmarshal(projection.Transformers[1], &labels))
			assert.Equal(t, labels["labels"].(map[string]any)["app.kubernetes.io/version"], any("2.x"))
			application, valid := object.EntryApplication()
			assert.Equal(t, valid, true)
			assert.Equal(t, application.Source, DocIdentity(source))
			parsed, _, _, err := ParseDoc(source, ParseDocOptions{})
			assert.NoError(t, err)
			if !Equal(application.Declaration, parsed) {
				t.Fatalf("evidence source differs from projected source: %+v versus %+v", application.Declaration, parsed)
			}
			projection.Patches[0].Target.Kind = "changed"
			again, err := object.SyncProjection()
			assert.NoError(t, err)
			assert.Equal(t, again.Patches[0].Target.Kind, tc.kind)
			assert.Diff(t, object.Spec, before)
		})
	}
}

func TestSyncProjection_OptionalAndInvalidSync(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		spec map[string]any
		fail bool
	}{
		{name: "no sync", spec: map[string]any{"distribution": map[string]any{"version": "2.x"}}},
		{name: "null sync", spec: map[string]any{"sync": nil}},
		{name: "malformed sync", spec: map[string]any{"sync": []any{"invalid"}}, fail: true},
		{name: "incomplete sync", spec: map[string]any{"sync": map[string]any{"kind": KindGitRepository}}, fail: true},
		{name: "unsupported source", spec: map[string]any{"sync": map[string]any{"kind": KindHelmRepository, "url": "https://fixture.invalid", "ref": "main", "path": "."}}, fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			object := &RawObject{APIVersion: FluxOperatorDomain + "/v1", Kind: "FluxInstance", Namespace: "control", Spec: tc.spec}
			projection, err := object.SyncProjection()
			if tc.fail {
				assert.Equal(t, errors.Is(err, ErrInput), true)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, projection, (*SyncProjection)(nil))
		})
	}
}
