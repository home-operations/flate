package helmrelease

import (
	"context"
	"slices"
	"testing"
	"testing/fstest"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/filtertest"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
)

func TestGeneratedObjects_PublishBeforeSourceWait(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ kind, replacement string }{
		{manifest.KindHelmChart, "failure"},
		{manifest.KindOCIRepository, "failure"},
		{manifest.KindHelmChart, manifest.KindOCIRepository},
		{manifest.KindHelmChart, "chartRef"},
		{manifest.KindOCIRepository, "chartRef"},
	} {
		t.Run(tc.kind+"/"+tc.replacement, func(t *testing.T) {
			t.Parallel()
			template := &helmv2.HelmChartTemplate{Kind: tc.kind, Spec: helmv2.HelmChartTemplateSpec{Chart: "app", Version: "1.0.0", SourceRef: &helmv2.CrossNamespaceObjectReference{Kind: manifest.KindHelmRepository, Name: "late", Namespace: "sources"}}}
			if tc.kind == manifest.KindOCIRepository {
				template.Spec = helmv2.HelmChartTemplateSpec{URL: "oci://registry.example/charts/app"}
			}
			hr := (&manifest.HelmRelease{Name: "app", Namespace: "apps", HelmReleaseSpec: helmv2.HelmReleaseSpec{Chart: template}}).WithNamespace("apps").(*manifest.HelmRelease)
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{hr.Named(): {File: "release.yaml"}}}
			filterLoader := admissiontest.New(t, admissiontest.Config{Changes: change.NewSet([]string{"release.yaml"}), Index: index, Inventory: filtertest.MapLister{}})
			admissiontest.Commit(t, filterLoader, nil)
			filter := filterLoader.Options.Filter
			c, st := newTestController(t, filterLoader)
			reconcile := func(level manifest.DrainLevel) *base.Execution {
				var run *base.Execution
				c.Tasks.Go(t.Context(), "release", func(ctx context.Context) {
					run = c.ReconcileNode(ctx, hr.Named(), level)
				})
				c.Tasks.BlockTillDone()
				return run
			}
			st.AddObject(hr)
			c.KeepEmitted(t.Context(), manifest.NamedResource{}, store.Child{Obj: hr}, nil)
			id := hr.ChartSource()
			arrivals := 0
			stop := st.AddListener(store.EventObjectAdded, func(got manifest.NamedResource, value any) {
				if got == id && value != nil {
					arrivals++
					if !filter.ShouldReconcile(got) {
						t.Error("generated source arrived before keep-set admission")
					}
				}
			}, false)
			defer stop()
			first := reconcile(manifest.DrainNone)
			defer first.Close()
			if first.Outcome() != schedule.Blocked || st.GetDeclaration(id) == nil || arrivals != 1 {
				t.Fatalf("first outcome=%v source=%v arrivals=%d", first.Outcome(), st.GetDeclaration(id), arrivals)
			}
			_, _, partial := st.GetEmission(hr.Named())
			defer partial.Finish()
			if len(partial.Children) != 1 || !partial.Children[0].Generated || len(partial.Manifests) != 0 || !st.ReportHidden(id) {
				t.Fatalf("provisional inventory=%+v", partial)
			}
			if _, ready := st.GetStatus(id); ready {
				t.Fatal("parent assigned readiness to its generated source")
			}
			st.SetArtifact(id, &store.SourceArtifact{Key: "chart", Content: "chart-generation", Revision: "1.0.0@sha256:abcdef12345675476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8", FS: fstest.MapFS{
				"chart/Chart.yaml":            &fstest.MapFile{Data: []byte("apiVersion: v2\nname: app\nversion: 1.0.0\n")},
				"chart/templates/output.yaml": &fstest.MapFile{Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\ndata: {value: complete}\n")},
			}})
			st.UpdateStatus(id, store.StatusInfo{Status: store.StatusReady})
			second := reconcile(manifest.DrainNone)
			defer second.Close()
			if info, _ := st.GetStatus(hr.Named()); info.Status != store.StatusReady {
				t.Fatalf("render after source arrival: %+v", info)
			}
			_, _, completed := st.GetEmission(hr.Named())
			defer completed.Finish()
			if len(completed.Manifests) != 1 || !slices.ContainsFunc(completed.Children, func(child store.Child) bool { return child.Generated && child.Obj.Named() == id }) {
				t.Fatal("complete render lost the current generated source")
			}
			if tc.replacement != "failure" {
				next := hr.Clone()
				if tc.replacement == "chartRef" {
					next.HelmReleaseSpec.Chart = nil
					next.ChartRef = &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "replacement"}
				} else {
					next.HelmReleaseSpec.Chart = &helmv2.HelmChartTemplate{Kind: manifest.KindOCIRepository, Spec: helmv2.HelmChartTemplateSpec{URL: "oci://registry.example/charts/replacement"}}
				}
				next = next.WithNamespace(hr.Namespace).(*manifest.HelmRelease)
				sibling := hr.Clone()
				sibling.Name = "sibling"
				st.AddObject(sibling)
				st.AddObject(next)
				replacement := next.ChartSource()
				if tc.replacement == "chartRef" {
					st.AddObject(manifest.NewOCIRepository(sourcev1.OCIRepository{Name: replacement.Name, Namespace: replacement.Namespace, Spec: sourcev1.OCIRepositorySpec{URL: "oci://registry.example/charts/replacement"}}))
					st.UpdateStatus(replacement, store.StatusInfo{Status: store.StatusPending})
				}
				withdrawn := make(chan struct{}, 1)
				stopWithdrawal := st.AddListener(store.EventObjectAdded, func(got manifest.NamedResource, _ any) {
					if got == id && st.GetObject(id) == nil {
						if info, _ := st.GetStatus(replacement); info.Status == store.StatusReady || st.GetArtifact(replacement, store.ArtifactContent) != nil {
							t.Error("obsolete source withdrew after replacement settled")
						}
						withdrawn <- struct{}{}
					}
				}, false)
				defer stopWithdrawal()
				blocked := reconcile(manifest.DrainNone)
				defer blocked.Close()
				select {
				case <-withdrawn:
				default:
					t.Fatal("obsolete generated source did not withdraw before the replacement wait")
				}
				if !blocked.Reads.Valid() {
					blocked = reconcile(manifest.DrainNone)
					defer blocked.Close()
				}
				if blocked.Outcome() != schedule.Blocked {
					info, _ := st.GetStatus(hr.Named())
					t.Fatalf("replacement outcome=%v status=%#v readsValid=%t admitted=%t source=%v", blocked.Outcome(), info, blocked.Reads.Valid(), blocked.Reads.Admitted(), replacement)
				}
				assert.Diff(t, blocked.Blocked, []manifest.NamedResource{replacement})
				assert.Equal(t, filter.ShouldReconcile(next.Named()), true)
				assert.Equal(t, filter.ShouldReconcile(sibling.Named()), false)
				_, _, waiting := st.GetEmission(hr.Named())
				defer waiting.Finish()
				if waiting.Complete || len(waiting.Failures) != 0 {
					t.Fatal("source transition forced completion or failure")
				}
				for _, child := range completed.Children {
					if child.Generated {
						continue
					}
					if !slices.ContainsFunc(waiting.Children, func(current store.Child) bool { return current.Obj.Named() == child.Obj.Named() }) || st.GetObject(child.Obj.Named()) == nil {
						t.Fatal("blocked replacement discarded ordinary partial inventory")
					}
				}
				assert.Diff(t, testutil.DocumentMaps(waiting.Manifests), testutil.DocumentMaps(completed.Manifests))
				if st.GetObject(id) != nil || st.HasGeneratedObjects(hr.Named()) != (tc.replacement != "chartRef") {
					t.Fatal("blocked replacement retained obsolete generated membership")
				}
				return
			}
			st.UpdateStatus(id, store.StatusInfo{Status: store.StatusFailed, Message: "source failed"})
			failed := reconcile(manifest.DrainForce)
			defer failed.Close()
			if info, _ := st.GetStatus(hr.Named()); info.Status != store.StatusFailed {
				t.Fatalf("source failure was hidden by fingerprint replay: %+v", info)
			}
			if st.GetObject(id) == nil {
				t.Fatal("failed render discarded its current generated source")
			}
		})
	}
}
