package output

import (
	"fmt"
	"maps"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

func BenchmarkPrepareReport_ManyClusters(b *testing.B) {
	for _, clusters := range []int{1, 8, 32, 128} {
		b.Run(fmt.Sprintf("clusters=%d", clusters), func(b *testing.B) {
			report := Report{Groups: make([]ReportGroup, clusters)}
			before := make([][]map[manifest.NamedResource]manifest.DiagnosticIdentity, clusters)
			for i := range report.Groups {
				group := &report.Groups[i]
				group.Cluster = fmt.Sprintf("cluster-%d", i)
				group.Subjects = make(map[manifest.NamedResource]manifest.DiagnosticIdentity)
				group.Models = []Model{
					{Context: "orig snapshot", Subjects: make(map[manifest.NamedResource]manifest.DiagnosticIdentity)},
					{Context: "current snapshot", Subjects: make(map[manifest.NamedResource]manifest.DiagnosticIdentity)},
				}
				for j := range 4 {
					raw := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: fmt.Sprintf("source-%d", j)}
					owner := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: "apps", Name: fmt.Sprintf("chart-%d", j)}
					group.Subjects[raw] = manifest.DiagnosticIdentity{ID: raw, Resource: owner}
					for k := range group.Models {
						captured := owner
						captured.Name = fmt.Sprintf("snapshot-%d-chart-%d", k, j)
						group.Models[k].Subjects[raw] = manifest.DiagnosticIdentity{ID: raw, Resource: captured}
						group.Models[k].Primary = append(group.Models[k].Primary, Primary{ID: raw, Msg: "failed"})
					}
					owner.Name = fmt.Sprintf("warning-chart-%d", j)
					report.Warnings = append(report.Warnings, Warning{Group: i, Warning: manifest.Warning{
						Resource: raw, Subject: manifest.DiagnosticIdentity{ID: raw, Resource: owner}, Message: "captured",
					}})
				}
				before[i] = append(before[i], maps.Clone(group.Subjects))
				for _, model := range group.Models {
					before[i] = append(before[i], maps.Clone(model.Subjects))
				}
			}
			b.ReportAllocs()
			for b.Loop() {
				report.Prepare(nil)
			}
			for i, group := range report.Groups {
				if len(group.Names.Subjects) != 4 || !maps.Equal(group.Subjects, before[i][0]) {
					b.Fatal("preparation changed the group inventory or identities")
				}
				for j, model := range group.Models {
					if len(model.Names.Subjects) != 4 || !maps.Equal(model.Subjects, before[i][j+1]) {
						b.Fatal("preparation changed the snapshot inventory or identities")
					}
				}
			}
		})
	}
}

func BenchmarkPrepareReport_NoOverrides(b *testing.B) {
	for _, clusters := range []int{1, 8, 32, 128} {
		b.Run(fmt.Sprintf("clusters=%d", clusters), func(b *testing.B) {
			report, _ := benchmarkNamingFixture(clusters)
			b.ReportAllocs()
			for b.Loop() {
				report.Prepare(nil)
			}
		})
	}
}

func BenchmarkPrepareNames_NoOverrides(b *testing.B) {
	for _, clusters := range []int{1, 8, 32, 128} {
		b.Run(fmt.Sprintf("clusters=%d", clusters), func(b *testing.B) {
			report, inventory := benchmarkNamingFixture(clusters)
			names := make([]string, clusters)
			for i, group := range report.Groups {
				names[i] = group.Cluster
			}
			b.ReportAllocs()
			for b.Loop() {
				PrepareNames(names, inventory, nil)
			}
		})
	}
}

func benchmarkNamingFixture(clusters int) (Report, [][]manifest.NamedResource) {
	report := Report{Groups: make([]ReportGroup, clusters)}
	inventory := make([][]manifest.NamedResource, clusters)
	for i := range report.Groups {
		group := &report.Groups[i]
		group.Cluster = fmt.Sprintf("cluster-%d", i)
		group.Models = []Model{{}}
		for j := range 200 {
			id := manifest.NamedResource{Group: "example.io", Kind: fmt.Sprintf("Kind%d", j%5), Namespace: "apps", Name: fmt.Sprintf("resource-%d", j)}
			inventory[i] = append(inventory[i], id)
			group.Models[0].Primary = append(group.Models[0].Primary, Primary{ID: id, Msg: "failed"})
		}
	}
	return report, inventory
}
