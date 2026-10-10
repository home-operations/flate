package helmrelease

import (
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestReconcile_OCIChartDigestTracking(t *testing.T) {
	const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	for _, tc := range []struct {
		name, pin string
		disable   bool
	}{
		{"tag", "tag", false}, {"digest", "digest", false},
		{"tag tracking off", "tag", true}, {"digest tracking off", "digest", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, st := newTestController(t, nil)
			c.Options.DisableChartDigestTracking = tc.disable
			dir := t.TempDir()
			testutil.WriteFile(t, dir, "Chart.yaml", "apiVersion: v2\nname: podinfo\nversion: 6.15.0\n")
			testutil.WriteFile(t, dir, "templates/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: podinfo
  labels:
    helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | quote }}
data:
  version: {{ .Chart.Version | quote }}
`)
			ref := map[string]any{"tag": "6.15.0"}
			revision := "6.15.0@" + digest
			if tc.pin == "digest" {
				ref = map[string]any{"digest": digest}
				revision = digest
			}
			src, err := manifest.ParseOCIRepository(map[string]any{
				"apiVersion": "source.toolkit.fluxcd.io/v1", "kind": "OCIRepository",
				"metadata": map[string]any{"name": "podinfo", "namespace": "apps"},
				"spec":     map[string]any{"url": "oci://example.test/podinfo", "ref": ref},
			})
			if err != nil {
				t.Fatal(err)
			}
			obj, err := manifest.ParseDoc(map[string]any{
				"apiVersion": "helm.toolkit.fluxcd.io/v2", "kind": "HelmRelease",
				"metadata": map[string]any{"name": "podinfo", "namespace": "apps"},
				"spec":     map[string]any{"chartRef": map[string]any{"kind": "OCIRepository", "name": "podinfo"}},
			}, manifest.ParseDocOptions{})
			if err != nil {
				t.Fatal(err)
			}
			hr := obj.(*manifest.HelmRelease)
			original := hr.Clone()
			st.AddObject(src)
			st.SetArtifact(src.Named(), &store.SourceArtifact{
				Kind: manifest.KindOCIRepository, LocalPath: dir, Revision: revision, Digest: digest,
			})
			st.UpdateStatus(src.Named(), store.StatusReady, "recorded artifact")
			st.AddObject(hr)
			if info := dispatchToFixpoint(t, c, st, hr.Named()); info.Status != store.StatusReady {
				t.Fatalf("reconcile: %+v", info)
			}
			art := st.GetArtifact(hr.Named()).(*store.HelmReleaseArtifact)
			version := "6.15.0"
			if !tc.disable {
				version += "+ff3d3e14728f"
			}
			if got := renderedConfigMapValue(art.Manifests, "version"); got != version {
				t.Fatalf("rendered chart version = %q, want %s", got, version)
			}
			md := art.Manifests[0]["metadata"].(map[string]any)
			labels := md["labels"].(map[string]any)
			if got := labels["helm.sh/chart"]; got != "podinfo-"+strings.ReplaceAll(version, "+", "_") {
				t.Fatalf("rendered chart label = %v", got)
			}
			if diff := cmp.Diff(original, hr); diff != "" {
				t.Fatalf("stored release mutated (-want +got):\n%s", diff)
			}
			if info := dispatchToFixpoint(t, c, st, hr.Named()); info.Status != store.StatusReady {
				t.Fatalf("repeat reconcile: %+v", info)
			}
			if st.GetArtifact(hr.Named()) != art {
				t.Fatal("unchanged inputs did not deduplicate")
			}
			sharedPrefix := digest[:len(digest)-1] + "9"
			st.SetArtifact(src.Named(), &store.SourceArtifact{
				Kind: manifest.KindOCIRepository, LocalPath: dir, Revision: "6.15.0@" + sharedPrefix, Digest: sharedPrefix,
			})
			if info := dispatchToFixpoint(t, c, st, hr.Named()); info.Status != store.StatusReady {
				t.Fatalf("shared-prefix digest reconcile: %+v", info)
			}
			sameVersion := st.GetArtifact(hr.Named()).(*store.HelmReleaseArtifact)
			if sameVersion.Fingerprint == art.Fingerprint {
				t.Fatal("full digests sharing twelve characters reused controller fingerprint")
			}
			if diff := cmp.Diff(art.Manifests, sameVersion.Manifests); diff != "" {
				t.Fatalf("same visible identity changed output (-want +got):\n%s", diff)
			}
			const next = "sha256:abcdef12345675476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
			st.SetArtifact(src.Named(), &store.SourceArtifact{
				Kind: manifest.KindOCIRepository, LocalPath: dir, Revision: "6.15.0@" + next, Digest: next,
			})
			if info := dispatchToFixpoint(t, c, st, hr.Named()); info.Status != store.StatusReady {
				t.Fatalf("changed digest reconcile: %+v", info)
			}
			updated := st.GetArtifact(hr.Named()).(*store.HelmReleaseArtifact)
			version = "6.15.0"
			if !tc.disable {
				version += "+abcdef123456"
			}
			if got := renderedConfigMapValue(updated.Manifests, "version"); got != version {
				t.Fatalf("changed digest version = %q", got)
			}
			if art.Fingerprint == updated.Fingerprint {
				t.Fatal("changed digest reused controller fingerprint")
			}
			st.SetArtifact(src.Named(), &store.SourceArtifact{
				Kind: manifest.KindOCIRepository, LocalPath: dir, Revision: "latest@" + next, Digest: next,
			})
			if info := dispatchToFixpoint(t, c, st, hr.Named()); info.Status != store.StatusReady {
				t.Fatalf("changed revision reconcile: %+v", info)
			}
			revised := st.GetArtifact(hr.Named()).(*store.HelmReleaseArtifact)
			if revised.Fingerprint == updated.Fingerprint {
				t.Fatal("changed revision reused controller fingerprint")
			}
			if diff := cmp.Diff(updated.Manifests, revised.Manifests); diff != "" {
				t.Fatalf("same visible version changed output (-want +got):\n%s", diff)
			}
			st.SetArtifact(src.Named(), &store.SourceArtifact{
				Kind: manifest.KindOCIRepository, LocalPath: dir, Revision: "invalid", Digest: next,
			})
			if info := dispatchToFixpoint(t, c, st, hr.Named()); info.Status != store.StatusFailed {
				t.Fatalf("invalid changed revision bypassed validation: %+v", info)
			}
		})
	}
}

func ptrDuration(d time.Duration) *metav1.Duration {
	out := metav1.Duration{Duration: d}
	return &out
}

// TestReconcile_ChartSourceNotReady covers the error path: a HelmRelease
// whose chartRef points at an OCIRepository that never reaches Ready
// must surface "chart source ... not ready" via the depwait timeout.
func TestReconcile_ChartSourceNotReady(t *testing.T) {
	c, st := newTestController(t, nil)
	// Source CR exists but no SetArtifact + no Ready status → depwait
	// hangs until timeout, then fails.
	src := &manifest.OCIRepository{
		Name: "podinfo", Namespace: "flux-system",
		URL: "oci://example.test/podinfo",
	}
	st.AddObject(src)

	hr := &manifest.HelmRelease{
		Name: "podinfo", Namespace: "flux-system",
		Timeout: ptrDuration(100 * time.Millisecond),
		Chart: manifest.HelmChart{
			Name: "podinfo", RepoKind: manifest.KindOCIRepository,
			RepoName: "podinfo", RepoNamespace: "flux-system",
		},
	}
	st.AddObject(hr)
	info := dispatchToFixpoint(t, c, st, hr.Named())
	if info.Status != store.StatusFailed {
		t.Fatalf("status = %+v, want StatusFailed", info)
	}
	if !strings.Contains(info.Message, "not ready") && !strings.Contains(info.Message, "object not found") {
		t.Errorf("expected chart-source-not-ready failure, got %q", info.Message)
	}
}

// TestReconcile_DependsOnFailed cascades a dep failure to a non-rendering
// HR — DependencyFailedError surfaces via RunWithStatus → Failed status.
func TestReconcile_DependsOnFailed(t *testing.T) {
	c, st := newTestController(t, nil)
	dep := &manifest.HelmRelease{Name: "dep", Namespace: "flux-system"}
	st.AddObject(dep)
	st.UpdateStatus(dep.Named(), store.StatusFailed, "synthetic dep failure")

	hr := &manifest.HelmRelease{
		Name: "depender", Namespace: "flux-system",
		Timeout:   ptrDuration(100 * time.Millisecond),
		DependsOn: []manifest.DependencyRef{{NamedResource: dep.Named()}},
	}
	st.AddObject(hr)
	info := dispatchToFixpoint(t, c, st, hr.Named())
	if info.Status != store.StatusFailed {
		t.Fatalf("status = %+v, want StatusFailed", info)
	}
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
		Timeout: ptrDuration(80 * time.Millisecond),
	}
	c, st := newTestControllerWithParentOf(t, map[manifest.NamedResource]manifest.NamedResource{
		hr.Named(): parent.Named(),
	})
	st.AddObject(parent) // never reaches Ready
	st.AddObject(hr)
	info := dispatchToFixpoint(t, c, st, hr.Named())
	if info.Status != store.StatusFailed {
		t.Fatalf("status = %+v, want StatusFailed", info)
	}
	if !strings.Contains(info.Message, "parent") {
		t.Errorf("expected parent-not-ready error, got %q", info.Message)
	}
}

// TestReconcile_ParentGateViaResolverFunc verifies the parent gate
// works when ParentOf is a closure (the production wiring path —
// orchestrator combines the pre-built path-prefix index with the
// renderedSet to support both file-loaded and render-emitted HRs).
// Locks the contract that any resolver implementation is honored,
// not just the static-map shape.
func TestReconcile_ParentGateViaResolverFunc(t *testing.T) {
	parent := &manifest.Kustomization{Name: "apps", Namespace: "flux-system"}
	hr := &manifest.HelmRelease{
		Name: "child", Namespace: "flux-system",
		Timeout: ptrDuration(80 * time.Millisecond),
	}
	// Resolver returns the parent for the HR only — closes over
	// captured state, the production shape (which combines a map +
	// a renderedSet lookup).
	resolver := func(id manifest.NamedResource) (manifest.NamedResource, bool) {
		if id == hr.Named() {
			return parent.Named(), true
		}
		return manifest.NamedResource{}, false
	}
	c, st := newTestControllerWithOptions(t, ReconcileOptions{ParentOf: resolver})
	st.AddObject(parent) // never reaches Ready
	st.AddObject(hr)
	info := dispatchToFixpoint(t, c, st, hr.Named())
	if info.Status != store.StatusFailed {
		t.Fatalf("status = %+v, want StatusFailed", info)
	}
	if !strings.Contains(info.Message, "parent") {
		t.Errorf("expected parent-not-ready error via resolver, got %q", info.Message)
	}
}

// TestReconcile_NoChartRefBypassDepwait covers a degenerate HR with
// neither chartRef nor sourceRef — chart resolution writes
// TestIsFluxSourceKind enumerates the chart-render dispatch matrix:
// source CRs go through AddObject (so the source controller picks
// them up and writes a status); other kinds go through AddRendered.
// Pins the fix for the m00nwtchr report where tofu-controller's
// chart-rendered OCIRepository/aws-package surfaced as "FAILED
// (no status reported)" because the HR controller used to
// AddRendered every doc unconditionally.
func TestIsFluxSourceKind(t *testing.T) {
	cases := []struct {
		name string
		obj  manifest.BaseManifest
		want bool
	}{
		{"GitRepository", &manifest.GitRepository{}, true},
		{"OCIRepository", &manifest.OCIRepository{}, true},
		{"HelmRepository", &manifest.HelmRepository{}, true},
		{"Bucket", &manifest.Bucket{}, true},
		{"HelmChartSource", &manifest.HelmChartSource{}, true},
		{"ExternalArtifact", &manifest.ExternalArtifact{}, true},
		{"ConfigMap is not a source", &manifest.ConfigMap{}, false},
		{"Secret is not a source", &manifest.Secret{}, false},
		{"Kustomization is not a source", &manifest.Kustomization{}, false},
		{"HelmRelease is not a source", &manifest.HelmRelease{}, false},
		{"RawObject is not a source", &manifest.RawObject{Kind: "Deployment"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isFluxSourceKind(tc.obj); got != tc.want {
				t.Errorf("isFluxSourceKind(%T) = %v, want %v", tc.obj, got, tc.want)
			}
		})
	}
}

// "missing spec.chart" via helm.Prepare, surfacing as Failed without
// ever entering depwait.
func TestReconcile_MissingChartFails(t *testing.T) {
	c, st := newTestController(t, nil)
	hr := &manifest.HelmRelease{
		Name: "broken", Namespace: "flux-system",
		Timeout: ptrDuration(80 * time.Millisecond),
		// Chart left empty — depwait on an unset source ref times out.
	}
	st.AddObject(hr)
	info := dispatchToFixpoint(t, c, st, hr.Named())
	if info.Status != store.StatusFailed {
		t.Fatalf("status = %+v, want StatusFailed", info)
	}
	if info.Message == "" {
		t.Error("expected non-empty failure for missing chart")
	}
}
