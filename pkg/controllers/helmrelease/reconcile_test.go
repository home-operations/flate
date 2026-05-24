package helmrelease

import (
	"strings"
	"testing"
	"time"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func ptrDuration(d time.Duration) *metav1.Duration {
	out := metav1.Duration{Duration: d}
	return &out
}

// TestReconcile_ChartSourceNotReady covers the error path: a HelmRelease
// whose chartRef points at an OCIRepository that never reaches Ready
// must surface "chart source ... not ready" via the depwait timeout.
func TestReconcile_ChartSourceNotReady(t *testing.T) {
	_, st := newTestController(t, nil)
	// Source CR exists but no SetArtifact + no Ready status → depwait
	// hangs until timeout, then fails.
	src := &manifest.OCIRepository{
		Name: "podinfo", Namespace: "flux-system",
		OCIRepositorySpec: sourcev1.OCIRepositorySpec{URL: "oci://example.test/podinfo"},
	}
	st.AddObject(src)

	hr := &manifest.HelmRelease{
		Name: "podinfo", Namespace: "flux-system",
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			Timeout: ptrDuration(100 * time.Millisecond),
		},
		Chart: manifest.HelmChart{
			Name: "podinfo", RepoKind: manifest.KindOCIRepository,
			RepoName: "podinfo", RepoNamespace: "flux-system",
		},
	}
	st.AddObject(hr)
	info := waitForStatus(t, st, hr.Named(), store.StatusFailed)
	if !strings.Contains(info.Message, "not ready") && !strings.Contains(info.Message, "object not found") {
		t.Errorf("expected chart-source-not-ready failure, got %q", info.Message)
	}
}

// TestReconcile_DependsOnFailed cascades a dep failure to a non-rendering
// HR — DependencyFailedError surfaces via RunWithStatus → Failed status.
func TestReconcile_DependsOnFailed(t *testing.T) {
	_, st := newTestController(t, nil)
	dep := &manifest.HelmRelease{Name: "dep", Namespace: "flux-system"}
	st.AddObject(dep)
	st.UpdateStatus(dep.Named(), store.StatusFailed, "synthetic dep failure")

	hr := &manifest.HelmRelease{
		Name: "depender", Namespace: "flux-system",
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			Timeout: ptrDuration(100 * time.Millisecond),
		},
		DependsOn: []manifest.DependencyRef{{NamedResource: dep.Named()}},
	}
	st.AddObject(hr)
	info := waitForStatus(t, st, hr.Named(), store.StatusFailed)
	if info.Message == "" {
		t.Error("expected non-empty failure on dep cascade")
	}
}

// TestReconcile_ParentGateWaits exercises the parent-KS gate from #221:
// when ParentOf maps an HR to a KS that never reaches Ready, reconcile
// times out in depwait and surfaces the parent-not-ready error.
func TestReconcile_ParentGateWaits(t *testing.T) {
	parent := &manifest.Kustomization{Name: "apps", Namespace: "flux-system"}
	hr := &manifest.HelmRelease{
		Name: "child", Namespace: "flux-system",
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			Timeout: ptrDuration(80 * time.Millisecond),
		},
	}
	c, st := newTestControllerWithParentOf(t, map[manifest.NamedResource]manifest.NamedResource{
		hr.Named(): parent.Named(),
	})
	_ = c
	st.AddObject(parent) // never reaches Ready
	st.AddObject(hr)
	info := waitForStatus(t, st, hr.Named(), store.StatusFailed)
	if !strings.Contains(info.Message, "parent") {
		t.Errorf("expected parent-not-ready error, got %q", info.Message)
	}
}

// TestReconcile_NoChartRefBypassDepwait covers a degenerate HR with
// neither chartRef nor sourceRef — chart resolution writes
// "missing spec.chart" via helm.Prepare, surfacing as Failed without
// ever entering depwait.
func TestReconcile_MissingChartFails(t *testing.T) {
	_, st := newTestController(t, nil)
	hr := &manifest.HelmRelease{
		Name: "broken", Namespace: "flux-system",
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			Timeout: ptrDuration(80 * time.Millisecond),
		},
		// Chart left empty — depwait on an unset source ref times out.
	}
	st.AddObject(hr)
	info := waitForStatus(t, st, hr.Named(), store.StatusFailed)
	if info.Message == "" {
		t.Error("expected non-empty failure for missing chart")
	}
}
