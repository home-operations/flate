package helmrelease

import (
	"testing"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
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
			b.ReportAllocs()
			for b.Loop() {
				if fp := helmReleaseFingerprint(hr, tc.source); fp == "" {
					b.Fatal("empty fingerprint")
				}
			}
		})
	}
}

func BenchmarkCollectHRDeps(b *testing.B) {
	id := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "target"}
	hr := &manifest.HelmRelease{Name: "consumer", Namespace: "apps", DependsOn: []manifest.DependencyRef{{NamedResource: id}}}
	st := store.New()
	c := &Controller{Controller: base.New(st, nil, "helmrelease")}
	c.SetFilter(change.NewFilter(change.NewSet([]string{"target.yaml"}), map[manifest.NamedResource]string{id: "target.yaml"}, "", st))
	b.ReportAllocs()
	for b.Loop() {
		c.collectHRDeps(hr)
	}
}
