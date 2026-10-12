package helmrelease

import (
	"testing"

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
			var identity string
			if tc.source != nil {
				identity = tc.source.Digest + "\x00" + tc.source.Revision
			}
			b.ReportAllocs()
			for b.Loop() {
				if fp := helmReleaseFingerprint(hr, identity, nil); fp == "" {
					b.Fatal("empty fingerprint")
				}
			}
		})
	}
}

func BenchmarkHelmReleaseFingerprint_SourceLookup(b *testing.B) {
	hr := &manifest.HelmRelease{Name: "podinfo", Namespace: "apps", Values: map[string]any{"replicas": 2},
		Chart: manifest.HelmChart{RepoKind: manifest.KindOCIRepository, RepoNamespace: "apps", RepoName: "podinfo"}}
	const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	st := store.New()
	st.SetArtifact(manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "podinfo"}, &store.SourceArtifact{Digest: digest, Revision: "6.15.0@" + digest})
	resolver := helm.NewStoreSourceResolver(st)
	b.ReportAllocs()
	for b.Loop() {
		source := resolver.LocalSourceArtifact(hr.Chart.RepoKind, hr.Chart.RepoNamespace, hr.Chart.RepoName)
		identity := source.Digest + "\x00" + source.Revision
		if fp := helmReleaseFingerprint(hr, identity, nil); fp == "" {
			b.Fatal("empty fingerprint")
		}
	}
}

func BenchmarkCollectHRDeps(b *testing.B) {
	id := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "target"}
	hr := &manifest.HelmRelease{Name: "consumer", Namespace: "apps", DependsOn: []manifest.DependencyRef{{NamedResource: id}}}
	st := store.New()
	c := &Controller{Controller: base.New(st, nil, "helmrelease")}
	c.SetFilter(change.NewFilter(change.NewSet([]string{"target.yaml"}), &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: {File: "target.yaml"}}}, st))
	b.ReportAllocs()
	for b.Loop() {
		c.OrderingDeps(hr.DependsOn)
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
			parent := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "parent"}
			docs := []map[string]any{tc.doc}
			b.ReportAllocs()
			for b.Loop() {
				c.Emit(parent, docs, c.WipeSecrets, tc.publish)
			}
		})
	}
}
