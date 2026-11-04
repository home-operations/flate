package helmrelease

import (
	"errors"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/go-cmp/cmp/cmpopts"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestReconcile_OCIChartDigestTracking(t *testing.T) {
	t.Parallel()
	for _, entry := range []string{"chartRef", "template"} {
		for _, tc := range []struct {
			name, pin       string
			disable         bool
			targetNamespace string
		}{
			{"tag", "tag", false, "release"},
			{"digest", "digest", false, "release"},
			{"tag tracking off", "tag", true, "release"},
			{"digest tracking off", "digest", true, "release"},
			{"HR namespace fallback", "tag", false, ""},
		} {
			t.Run(entry+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				testReconcileOCIChartDigestTrackingCase(
					t,
					testReconcileOCIChartDigestTrackingCaseInputs{entry: &entry, tc: &tc},
				)
			},
			)
		}
	}
}

// A chart source that stays Pending fails with typed dependency provenance at force drain.
func TestReconcile_ChartSourceNotReady(t *testing.T) {
	t.Parallel()
	c, st := newTestController(t, nil)
	// The source exists without an artifact or Ready condition.
	src := &manifest.OCIRepository{
		Name: "podinfo", Namespace: "flux-system",
		URL: "oci://example.test/podinfo",
	}
	st.AddObject(src)

	hr := &manifest.HelmRelease{
		Name:      "podinfo",
		Namespace: "flux-system",
		Chart: manifest.HelmChart{
			Name: "podinfo",
			Source: manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindOCIRepository,
				Namespace: "flux-system",
				Name:      "podinfo",
			},
		},
		ChartRef: &helmv2.CrossNamespaceSourceReference{
			Kind:      manifest.KindOCIRepository,
			Name:      "podinfo",
			Namespace: "flux-system",
		},
	}
	st.AddObject(hr)
	err := c.newExecution(hr.Named()).requireChartForTest(t.Context(), hr, manifest.DrainForce)
	assert.FatalIf(
		t,
		!errors.Is(err, manifest.ErrInput) || errors.Is(err, manifest.ErrObjectNotFound),
		"chart gate category = %v",
		err,
	)
	info := reconcileFixture(t, c, hr.Named())
	assert.FatalIf(t, info.Status != store.StatusFailed, "status = %+v, want StatusFailed", info)
	if info.Message != "dependencies failed: "+src.Named().String()+": not ready" ||
		!slices.Equal(info.BlockedBy(), []manifest.NamedResource{src.Named()}) {
		t.Errorf("chart failure = %+v; blockers = %v", info, info.BlockedBy())
	}
}

// TestReconcile_DependsOnFailed cascades a dep failure to a non-rendering
// DependencyFailedError MUST retain failure provenance in the terminal status.
func TestReconcile_DependsOnFailed(t *testing.T) {
	t.Parallel()
	c, st := newTestController(t, nil)
	dep := &manifest.HelmRelease{Name: "dep", Namespace: "flux-system"}
	st.AddObject(dep)
	st.UpdateStatus(
		dep.Named(),
		store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "synthetic dep failure"},
	)

	hr := &manifest.HelmRelease{
		Name: "depender", Namespace: "flux-system",
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			DependsOn: []helmv2.DependencyReference{{Name: dep.Name, Namespace: dep.Namespace}},
			Chart: &helmv2.HelmChartTemplate{
				Spec: helmv2.HelmChartTemplateSpec{
					Chart: "demo",
					SourceRef: &helmv2.CrossNamespaceObjectReference{
						Kind: manifest.KindHelmRepository,
						Name: "repository",
					},
				},
			},
		},
	}
	st.AddObject(hr)
	info := reconcileFixture(t, c, hr.Named())
	assert.FatalIf(t, info.Status != store.StatusFailed, "status = %+v, want StatusFailed", info)
	assert.FatalIf(
		t,
		!info.IsDependencyFailure() || !slices.Equal(info.BlockedBy(), []manifest.NamedResource{dep.Named()}),
		"dependsOn failure mixed with chart-source readiness: %+v",
		info,
	)
}

// A structural parent that stays Pending fails with typed dependency provenance.
func TestReconcile_ParentGateWaits(t *testing.T) {
	t.Parallel()
	parent := &manifest.Kustomization{Name: "apps", Namespace: "flux-system"}
	hr := &manifest.HelmRelease{
		Name: "child", Namespace: "flux-system",
	}
	c, st := newTestControllerWithParentOf(t, map[manifest.NamedResource]manifest.NamedResource{
		hr.Named(): parent.Named(),
	})
	st.AddObject(parent) // never reaches Ready
	st.AddObject(hr)
	err := c.newExecution(hr.Named()).reconcile(t.Context(), hr, manifest.DrainForce)
	assert.FatalIf(
		t,
		!errors.Is(err, manifest.ErrInput) || errors.Is(err, manifest.ErrObjectNotFound),
		"parent gate category = %v",
		err,
	)
	info := reconcileFixture(t, c, hr.Named())
	assert.FatalIf(t, info.Status != store.StatusFailed, "status = %+v, want StatusFailed", info)
	if info.Message != "dependencies failed: "+parent.Named().String()+": not ready" ||
		!slices.Equal(info.BlockedBy(), []manifest.NamedResource{parent.Named()}) {
		t.Errorf("parent failure = %+v; blockers = %v", info, info.BlockedBy())
	}
}

// TestReconcile_ParentGateViaResolverFunc verifies the parent gate
// works when ParentOf is a closure (the production wiring path —
// orchestrator combines the pre-built path-prefix index with the
// Store render ownership to support both file-loaded and render-emitted HRs).
// Locks the contract that any resolver implementation is honored,
// not just the static-map shape.
func TestReconcile_ParentGateViaResolverFunc(t *testing.T) {
	t.Parallel()
	parent := &manifest.Kustomization{Name: "apps", Namespace: "flux-system"}
	hr := &manifest.HelmRelease{
		Name: "child", Namespace: "flux-system",
	}
	// Resolver returns the parent for the HR only — closes over
	// captured state, the production shape (which combines a map +
	// a Store render ownership lookup).
	c, st := newTestControllerWithOptions(
		t,
		&base.Options{
			Index: parentIndexForTest(map[manifest.NamedResource]manifest.NamedResource{hr.Named(): parent.Named()}),
		},
	)
	st.AddObject(parent) // never reaches Ready
	st.AddObject(hr)
	info := reconcileFixture(t, c, hr.Named())
	assert.FatalIf(t, info.Status != store.StatusFailed, "status = %+v, want StatusFailed", info)
	if info.Message != "dependencies failed: "+parent.Named().String()+": not ready" ||
		!slices.Equal(info.BlockedBy(), []manifest.NamedResource{parent.Named()}) {
		t.Errorf("parent resolver failure = %+v; blockers = %v", info, info.BlockedBy())
	}
}

// A missing chart must fail before entering dependency waits.
func TestReconcile_MissingChartFails(t *testing.T) {
	t.Parallel()
	c, st := newTestController(t, nil)
	hr := &manifest.HelmRelease{
		Name: "broken", Namespace: "flux-system",
		// An unset chart source fails the dependency gate.
	}
	st.AddObject(hr)
	info := reconcileFixture(t, c, hr.Named())
	assert.FatalIf(t, info.Status != store.StatusFailed, "status = %+v, want StatusFailed", info)
	assert.ErrorIf(t, info.Message == "", "%v", "expected non-empty failure for missing chart")
}

type testReconcileOCIChartDigestTrackingCaseInputs struct {
	entry *string
	tc    *struct {
		name string

		pin             string
		disable         bool
		targetNamespace string
	}
}

func testReconcileOCIChartDigestTrackingCase(
	t *testing.T,
	testCaseInput testReconcileOCIChartDigestTrackingCaseInputs,
) {
	t.Helper()
	const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"

	c, st := newTestController(t, nil)
	c.Options.DisableChartDigestTracking = testCaseInput.tc.disable
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "chart/Chart.yaml", "apiVersion: v2\nname: podinfo\nversion: 6.15.0\n")
	testutil.WriteFile(t, dir, "chart/templates/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: podinfo
  labels:
    helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | quote }}
data:
  version: {{ .Chart.Version | quote }}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: explicit
  namespace: {{ .Release.Namespace }}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: authored, namespace: other}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: workload}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata: {name: cluster}
---
apiVersion: foreign.example/v1
kind: ClusterRole
metadata: {name: foreign}
---
apiVersion: nvidia.com/v1
kind: ClusterPolicy
metadata: {name: cluster-policy}
---
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata: {name: issuer}
`)
	ref := map[string]any{"tag": "6.15.0"}
	revision := "6.15.0@" + digest
	if testCaseInput.tc.pin == "digest" {
		ref = map[string]any{"digest": digest}
		revision = digest
	}
	parsed, err := manifest.ParseDoc(map[string]any{
		"apiVersion": "source.toolkit.fluxcd.io/v1", "kind": "OCIRepository",
		"metadata": map[string]any{"name": "podinfo", "namespace": "apps"},
		"spec":     map[string]any{"url": "oci://example.test/podinfo", "ref": ref},
	}, manifest.ParseDocOptions{DefaultNamespace: ""})
	obj := parsed.Object
	assert.FatalIf(t, err != nil, "%v", err)
	src := obj.(*manifest.OCIRepository)
	releaseSpec := map[string]any{
		"targetNamespace": testCaseInput.tc.targetNamespace,
		"chartRef":        map[string]any{"kind": "OCIRepository", "name": "podinfo"},
	}
	if (*testCaseInput.entry) == "template" {
		delete(releaseSpec, "chartRef")
		releaseSpec["chart"] = map[string]any{
			"kind": "OCIRepository",
			"spec": map[string]any{"url": "oci://example.test/podinfo", "ref": ref},
		}
	}
	parsed2, parsedErr := manifest.ParseDoc(map[string]any{
		"apiVersion": "helm.toolkit.fluxcd.io/v2", "kind": "HelmRelease",
		"metadata": map[string]any{"name": "podinfo", "namespace": "apps"},
		"spec":     releaseSpec,
	}, manifest.ParseDocOptions{DefaultNamespace: ""})
	obj, err = parsed2.Object, parsedErr
	assert.FatalIf(t, err != nil, "%v", err)
	hr := obj.(*manifest.HelmRelease)
	if (*testCaseInput.entry) == "template" {
		generated, err := manifest.GeneratedObjects(hr)
		assert.NoError(t, err)
		src = generated[0].(*manifest.OCIRepository)
	}
	original := hr.Clone()
	st.AddObject(src)
	st.SetArtifact(src.Named(), &store.SourceArtifact{
		Kind:      manifest.KindOCIRepository,
		FS:        os.DirFS(dir),
		LocalPath: dir,
		Revision:  revision,
		Digest:    digest,
	})
	st.UpdateStatus(
		src.Named(),
		store.StatusInfo{Status: store.StatusReady, Reason: "", Message: "recorded artifact"},
	)
	st.AddObject(hr)
	{
		info := reconcileFixture(t, c, hr.Named())
		assert.FatalIf(t, info.Status != store.StatusReady, "reconcile: %+v", info)
	}
	art := st.GetArtifact(hr.Named(), store.ArtifactContent).(*store.Emission)
	retained := make([]map[string]any, len(art.Manifests))
	if len(art.Manifests) != 8 {
		t.Fatalf("chart documents = %d", len(art.Manifests))
	}
	for i, doc := range art.Manifests {
		retained[i] = manifest.DeepCopyMap(doc.Document)
		metadata := manifest.DocMetadata(doc.Document)
		name, ns := metadata.Name, metadata.Namespace
		want := hr.ReleaseNamespace()
		switch name {
		case "cluster":
			want = ""
		case "authored":
			want = "other"
		}
		assert.FatalIf(t, ns != want, "%s namespace = %q, want %q", name, ns, want)
		identityNamespace := want
		assert.FatalIf(t, st.GetObject(
			manifest.NamedResource{
				Group: schema.FromAPIVersionAndKind(
					manifest.DocAPIVersion(doc.Document),
					manifest.DocKind(doc.Document),
				).Group,
				Kind:      manifest.DocKind(doc.Document),
				Namespace: identityNamespace,
				Name:      name,
			},
		) == nil, "chart child %s/%s missing", identityNamespace, name)
	}
	version := "6.15.0"
	if !testCaseInput.tc.disable {
		version += "+ff3d3e14728f"
	}
	{
		got := renderedConfigMapValue(testutil.DocumentMaps(art.Manifests), "version")
		assert.FatalIf(t, got != version, "rendered chart version = %q, want %s", got, version)
	}
	for _, doc := range art.Manifests {
		if manifest.DocMetadata(doc.Document).Name == "podinfo" {
			md := doc.Document["metadata"].(map[string]any)
			labels := md["labels"].(map[string]any)
			{
				got := labels["helm.sh/chart"]
				assert.FatalIf(
					t,
					got != "podinfo-"+strings.ReplaceAll(version, "+", "_"),
					"rendered chart label = %v",
					got,
				)
			}
		}
	}
	{
		diff := cmp.Diff(original, hr, cmpopts.IgnoreUnexported(manifest.HelmRelease{}))
		assert.FatalIf(t, diff != "", "stored release mutated (-want +got):\n%s", diff)
	}
	st.DeleteObject(
		manifest.NamedResource{
			Kind:      manifest.KindConfigMap,
			Namespace: hr.ReleaseNamespace(),
			Name:      "podinfo",
		},
	)
	st.DeleteObject(
		manifest.NamedResource{
			Group:     "nvidia.com",
			Kind:      "ClusterPolicy",
			Namespace: hr.ReleaseNamespace(),
			Name:      "cluster-policy",
		},
	)
	st.DeleteObject(
		manifest.NamedResource{
			Group:     "cert-manager.io",
			Kind:      "ClusterIssuer",
			Namespace: hr.ReleaseNamespace(),
			Name:      "issuer",
		},
	)
	var authority store.ReadSet
	authority.Init(st, manifest.NamedResource{Name: "artifact-reader"}, nil)
	authority.GetArtifact(hr.Named(), store.ArtifactContent)
	var repeatedEvents atomic.Int64
	stopEvents := make([]store.Unsubscribe, 0, 3)
	for _, kind := range []store.EventKind{store.EventObjectAdded, store.EventStatusUpdated, store.EventArtifactUpdated} {
		stopEvents = append(stopEvents, st.AddListener(kind, func(id manifest.NamedResource, _ any) {
			if id == hr.Named() {
				repeatedEvents.Add(1)
			}
		}, false))
	}

	{
		info := reconcileFixture(t, c, hr.Named())
		assert.FatalIf(t, info.Status != store.StatusReady, "repeat reconcile: %+v", info)
	}
	for _, stop := range stopEvents {
		stop()
	}
	current := st.GetArtifact(hr.Named(), store.ArtifactContent).(*store.Emission)
	if current.Fingerprint != art.Fingerprint || !authority.Valid() || repeatedEvents.Load() != 0 {
		t.Fatalf(
			"unchanged output authority changed: valid=%t events=%d",
			authority.Valid(),
			repeatedEvents.Load(),
		)
	}
	assert.Diff(t, testutil.DocumentMaps(current.Manifests), retained)
	authority.Retire()

	{
		delta := cmp.Diff(retained, testutil.DocumentMaps(art.Manifests))
		assert.FatalIf(t, delta != "", "replay changed retained documents (-want +got):\n%s", delta)
	}
	assert.FatalIf(t, st.GetObject(
		manifest.NamedResource{
			Kind:      manifest.KindConfigMap,
			Namespace: hr.ReleaseNamespace(),
			Name:      "podinfo",
		},
	) == nil, "%v", "replay lost release namespace identity")
	assert.FatalIf(t, st.GetObject(
		manifest.NamedResource{
			Group:     "nvidia.com",
			Kind:      "ClusterPolicy",
			Namespace: hr.ReleaseNamespace(),
			Name:      "cluster-policy",
		},
	) == nil, "%v", "replay lost defaulted chart child")
	assert.FatalIf(t, st.GetObject(
		manifest.NamedResource{
			Group:     "cert-manager.io",
			Kind:      "ClusterIssuer",
			Namespace: hr.ReleaseNamespace(),
			Name:      "issuer",
		},
	) == nil, "%v", "replay lost cluster issuer")
	for _, ns := range []string{"", "wrong"} {
		assert.FatalIf(t, st.GetObject(
			manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: ns, Name: "podinfo"},
		) != nil, "replay published chart child in namespace %q", ns)
	}
	sharedPrefix := digest[:len(digest)-1] + "9"
	st.SetArtifact(src.Named(), &store.SourceArtifact{
		Kind:      manifest.KindOCIRepository,
		FS:        os.DirFS(dir),
		LocalPath: dir,
		Revision:  "6.15.0@" + sharedPrefix,
		Digest:    sharedPrefix,
	})
	{
		info := reconcileFixture(t, c, hr.Named())
		assert.FatalIf(t, info.Status != store.StatusReady, "shared-prefix digest reconcile: %+v", info)
	}
	sameVersion := st.GetArtifact(hr.Named(), store.ArtifactContent).(*store.Emission)
	assert.FatalIf(
		t,
		sameVersion.Fingerprint == art.Fingerprint,
		"%v",
		"full digests sharing twelve characters reused controller fingerprint",
	)
	{
		diff := cmp.Diff(art.Manifests, sameVersion.Manifests)
		assert.FatalIf(t, diff != "", "same visible identity changed output (-want +got):\n%s", diff)
	}
	const next = "sha256:abcdef12345675476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	st.SetArtifact(src.Named(), &store.SourceArtifact{
		Kind:      manifest.KindOCIRepository,
		FS:        os.DirFS(dir),
		LocalPath: dir,
		Revision:  "6.15.0@" + next,
		Digest:    next,
	})
	{
		info := reconcileFixture(t, c, hr.Named())
		assert.FatalIf(t, info.Status != store.StatusReady, "changed digest reconcile: %+v", info)
	}
	updated := st.GetArtifact(hr.Named(), store.ArtifactContent).(*store.Emission)
	version = "6.15.0"
	if !testCaseInput.tc.disable {
		version += "+abcdef123456"
	}
	{
		got := renderedConfigMapValue(testutil.DocumentMaps(updated.Manifests), "version")
		assert.FatalIf(t, got != version, "changed digest version = %q", got)
	}
	assert.FatalIf(
		t,
		art.Fingerprint == updated.Fingerprint,
		"%v",
		"changed digest reused controller fingerprint",
	)
	st.SetArtifact(src.Named(), &store.SourceArtifact{
		Kind:      manifest.KindOCIRepository,
		FS:        os.DirFS(dir),
		LocalPath: dir,
		Revision:  "latest@" + next,
		Digest:    next,
	})
	{
		info := reconcileFixture(t, c, hr.Named())
		assert.FatalIf(t, info.Status != store.StatusReady, "changed revision reconcile: %+v", info)
	}
	revised := st.GetArtifact(hr.Named(), store.ArtifactContent).(*store.Emission)
	assert.FatalIf(
		t,
		revised.Fingerprint == updated.Fingerprint,
		"%v",
		"changed revision reused controller fingerprint",
	)
	{
		diff := cmp.Diff(updated.Manifests, revised.Manifests)
		assert.FatalIf(t, diff != "", "same visible version changed output (-want +got):\n%s", diff)
	}
	st.SetArtifact(src.Named(), &store.SourceArtifact{
		Kind:      manifest.KindOCIRepository,
		FS:        os.DirFS(dir),
		LocalPath: dir,
		Revision:  "invalid",
		Digest:    next,
	})
	{
		info := reconcileFixture(t, c, hr.Named())
		assert.FatalIf(
			t,
			info.Status != store.StatusFailed,
			"invalid changed revision bypassed validation: %+v",
			info,
		)
	}
}
