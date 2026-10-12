package helm

import (
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/pkg/manifest"
)

func TestPrepare_ChartReferenceNamespace(t *testing.T) {
	for _, tc := range []struct {
		name      string
		kind      string
		inline    bool
		namespace string
	}{
		{name: "inline relative", kind: manifest.KindHelmRepository, inline: true},
		{name: "inline explicit", kind: manifest.KindHelmRepository, inline: true, namespace: "other"},
		{name: "direct OCI relative", kind: manifest.KindOCIRepository},
		{name: "direct OCI explicit", kind: manifest.KindOCIRepository, namespace: "other"},
		{name: "HelmChart relative", kind: manifest.KindHelmChart},
		{name: "HelmChart explicit", kind: manifest.KindHelmChart, namespace: "other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ref := map[string]any{"kind": tc.kind, "name": "source"}
			if tc.namespace != "" {
				ref["namespace"] = tc.namespace
			}
			spec := map[string]any{"chartRef": ref}
			if tc.inline {
				spec = map[string]any{"chart": map[string]any{"spec": map[string]any{"chart": "podinfo", "sourceRef": ref}}}
			}
			obj, err := manifest.ParseDoc(map[string]any{
				"apiVersion": "helm.toolkit.fluxcd.io/v2", "kind": manifest.KindHelmRelease,
				"metadata": map[string]any{"name": "app", "namespace": "authored"}, "spec": spec,
			}, manifest.ParseDocOptions{})
			if err != nil {
				t.Fatal(err)
			}
			hr := obj.(*manifest.HelmRelease)
			hr.Namespace = "effective"
			wantNS := tc.namespace
			if wantNS == "" {
				wantNS = "effective"
			}
			lookupCalls := 0
			lookup := func(namespace, name string) *manifest.HelmChartSource {
				lookupCalls++
				if namespace != wantNS || name != "source" {
					t.Fatalf("lookup = %s/%s, want %s/source", namespace, name, wantNS)
				}
				return &manifest.HelmChartSource{Name: name, Namespace: namespace, Chart: "podinfo", Version: "1.0.0", SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: manifest.KindHelmRepository, Name: "resolved"}}
			}
			got, err := Prepare(hr, lookup, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if got.Chart.RepoNamespace != wantNS {
				t.Fatalf("prepared namespace = %q, want %q", got.Chart.RepoNamespace, wantNS)
			}
			if tc.kind == manifest.KindHelmChart {
				if lookupCalls != 1 || got.Chart.RepoName != "resolved" {
					t.Fatalf("resolved chart = %+v, lookups = %d", got.Chart, lookupCalls)
				}
				prepared, err := Prepare(got, lookup, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				if lookupCalls != 1 || prepared.Chart != got.Chart {
					t.Fatalf("repeated prepare chart = %+v, lookups = %d; want %+v", prepared.Chart, lookupCalls, got.Chart)
				}
			} else if lookupCalls != 0 {
				t.Fatalf("unexpected chart lookups = %d", lookupCalls)
			}
			if tc.namespace == "" && hr.Chart.RepoNamespace != "authored" {
				t.Fatalf("prepare mutated authored chart: %+v", hr.Chart)
			}
		})
	}
}
