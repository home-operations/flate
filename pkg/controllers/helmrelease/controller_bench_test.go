package helmrelease

import (
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func BenchmarkHelmReleaseFingerprint_SourceIdentity(b *testing.B) {
	hr := &manifest.HelmRelease{Name: "podinfo", Namespace: "apps", Values: map[string]any{"replicas": 2}}
	const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	for _, tc := range []struct {
		name   string
		source *store.SourceArtifact
	}{
		{"other sources", nil},
		{"direct OCI", &store.SourceArtifact{Digest: digest, Revision: "6.15.0@" + digest}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			var identity any
			if tc.source != nil {
				identity = struct{ Digest, Revision string }{tc.source.Digest, tc.source.Revision}
			}
			c := base.New(store.New(), nil, "test")
			b.ReportAllocs()
			for b.Loop() {
				if fp, _, _, _ := c.FingerprintDedup(hr.Named(), hr.Content(identity, nil)); fp == "" {
					b.Fatal("empty fingerprint")
				}
			}
		})
	}
}

func BenchmarkHelmReleaseFingerprint_SourceLookup(b *testing.B) {
	hr := &manifest.HelmRelease{Name: "podinfo", Namespace: "apps", Values: map[string]any{"replicas": 2},
		Chart: manifest.HelmChart{Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "podinfo"}}}
	const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	st := store.New()
	st.SetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "podinfo"}, &store.SourceArtifact{Digest: digest, Revision: "6.15.0@" + digest})
	resolver := helm.NewStoreSourceResolver(st)
	c := base.New(st, nil, "test")
	b.ReportAllocs()
	for b.Loop() {
		source := resolver.LocalSourceArtifact(hr.ChartSource())
		identity := struct{ Digest, Revision string }{source.Digest, source.Revision}
		if fp, _, _, _ := c.FingerprintDedup(hr.Named(), hr.Content(identity, nil)); fp == "" {
			b.Fatal("empty fingerprint")
		}
	}
}

func BenchmarkCollectHRDeps(b *testing.B) {
	id := testutil.HelmReleaseID("apps", "target")
	hr := &manifest.HelmRelease{Name: "consumer", Namespace: "apps", DependsOn: []helmv2.DependencyReference{{Name: id.Name, Namespace: id.Namespace}}}
	st := store.New()
	c := &Controller{Controller: base.New(st, nil, "helmrelease")}
	c.Configure(ReconcileOptions{Filter: change.NewFilter(change.NewSet([]string{"target.yaml"}), &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: {File: "target.yaml"}}}, st)})
	b.ReportAllocs()
	for b.Loop() {
		c.collectDeps(hr)
	}
}

func BenchmarkHelmReleaseEmission_Children(b *testing.B) {
	for _, tc := range []struct {
		name    string
		doc     map[string]any
		publish bool
	}{
		{"source", map[string]any{"apiVersion": "source.toolkit.fluxcd.io/v1", "kind": "GitRepository", "metadata": map[string]any{"name": "repo", "namespace": "apps"}, "spec": map[string]any{"url": "https://example.com/repo", "ref": map[string]any{"branch": "main"}}}, true},
		{"raw", map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "service", "namespace": "apps"}}, true},
		{"source_replay", map[string]any{"apiVersion": "source.toolkit.fluxcd.io/v1", "kind": "GitRepository", "metadata": map[string]any{"name": "repo", "namespace": "apps"}, "spec": map[string]any{"url": "https://example.com/repo", "ref": map[string]any{"branch": "main"}}}, false},
		{"raw_replay", map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "service", "namespace": "apps"}}, false},
	} {
		b.Run(tc.name, func(b *testing.B) {
			st := store.New()
			c := &Controller{Controller: base.New(st, nil, "helmrelease")}
			parent := testutil.HelmReleaseID("apps", "parent")
			docs := []map[string]any{tc.doc}
			b.ReportAllocs()
			for b.Loop() {
				publishDocuments(c, parent, docs, manifest.ParseDocOptions{WipeSecrets: c.WipeSecrets, DefaultNamespace: ""})
			}
		})
	}
}
