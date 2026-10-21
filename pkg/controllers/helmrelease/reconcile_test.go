package helmrelease

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	"github.com/home-operations/flate/pkg/values"
)

func TestReconcile_OCIChartDigestTracking(t *testing.T) {
	t.Parallel()
	const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	for _, tc := range []struct {
		name, pin       string
		disable         bool
		targetNamespace string
	}{
		{"tag", "tag", false, "release"}, {"digest", "digest", false, "release"},
		{"tag tracking off", "tag", true, "release"}, {"digest tracking off", "digest", true, "release"},
		{"HR namespace fallback", "tag", false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
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
			if tc.pin == "digest" {
				ref = map[string]any{"digest": digest}
				revision = digest
			}
			obj, _, _, err := manifest.ParseDoc(map[string]any{
				"apiVersion": "source.toolkit.fluxcd.io/v1", "kind": "OCIRepository",
				"metadata": map[string]any{"name": "podinfo", "namespace": "apps"},
				"spec":     map[string]any{"url": "oci://example.test/podinfo", "ref": ref},
			}, manifest.ParseDocOptions{DefaultNamespace: ""})
			if err != nil {
				t.Fatal(err)
			}
			src := obj.(*manifest.OCIRepository)
			obj, _, _, err = manifest.ParseDoc(map[string]any{
				"apiVersion": "helm.toolkit.fluxcd.io/v2", "kind": "HelmRelease",
				"metadata": map[string]any{"name": "podinfo", "namespace": "apps"},
				"spec":     map[string]any{"targetNamespace": tc.targetNamespace, "chartRef": map[string]any{"kind": "OCIRepository", "name": "podinfo"}},
			}, manifest.ParseDocOptions{DefaultNamespace: ""})
			if err != nil {
				t.Fatal(err)
			}
			hr := obj.(*manifest.HelmRelease)
			original := hr.Clone()
			st.AddObject(src)
			st.SetArtifact(src.Named(), &store.SourceArtifact{
				Kind: manifest.KindOCIRepository, FS: os.DirFS(dir), LocalPath: dir, Revision: revision, Digest: digest,
			})
			st.UpdateStatus(src.Named(), store.StatusReady, "", "recorded artifact")
			st.AddObject(hr)
			if info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: hr.Named(), ReconcileNode: c.ReconcileNode}); info.Status != store.StatusReady {
				t.Fatalf("reconcile: %+v", info)
			}
			art := st.GetArtifact(hr.Named()).(*store.Emission)
			retained := make([]map[string]any, len(art.Manifests))
			if len(art.Manifests) != 8 {
				t.Fatalf("chart documents = %d", len(art.Manifests))
			}
			for i, doc := range art.Manifests {
				retained[i] = manifest.DeepCopyMap(doc)
				name, ns := manifest.DocMetadata(doc)
				want := hr.ReleaseNamespace()
				switch name {
				case "cluster":
					want = ""
				case "authored":
					want = "other"
				}
				if ns != want {
					t.Fatalf("%s namespace = %q, want %q", name, ns, want)
				}
				identityNamespace := want
				if st.GetObject(manifest.NamedResource{Group: schema.FromAPIVersionAndKind(manifest.DocAPIVersion(doc), manifest.DocKind(doc)).Group, Kind: manifest.DocKind(doc), Namespace: identityNamespace, Name: name}) == nil {
					t.Fatalf("chart child %s/%s missing", identityNamespace, name)
				}
			}
			version := "6.15.0"
			if !tc.disable {
				version += "+ff3d3e14728f"
			}
			if got := renderedConfigMapValue(art.Manifests, "version"); got != version {
				t.Fatalf("rendered chart version = %q, want %s", got, version)
			}
			for _, doc := range art.Manifests {
				if name, _ := manifest.DocMetadata(doc); name == "podinfo" {
					md := doc["metadata"].(map[string]any)
					labels := md["labels"].(map[string]any)
					if got := labels["helm.sh/chart"]; got != "podinfo-"+strings.ReplaceAll(version, "+", "_") {
						t.Fatalf("rendered chart label = %v", got)
					}
				}
			}
			if diff := cmp.Diff(original, hr); diff != "" {
				t.Fatalf("stored release mutated (-want +got):\n%s", diff)
			}
			st.DeleteObject(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: hr.ReleaseNamespace(), Name: "podinfo"})
			st.DeleteObject(manifest.NamedResource{Group: "nvidia.com", Kind: "ClusterPolicy", Namespace: hr.ReleaseNamespace(), Name: "cluster-policy"})
			st.DeleteObject(manifest.NamedResource{Group: "cert-manager.io", Kind: "ClusterIssuer", Namespace: hr.ReleaseNamespace(), Name: "issuer"})
			if info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: hr.Named(), ReconcileNode: c.ReconcileNode}); info.Status != store.StatusReady {
				t.Fatalf("repeat reconcile: %+v", info)
			}
			if st.GetArtifact(hr.Named()) != art {
				t.Fatal("unchanged inputs did not deduplicate")
			}
			if delta := cmp.Diff(retained, art.Manifests); delta != "" {
				t.Fatalf("replay changed retained documents (-want +got):\n%s", delta)
			}
			if st.GetObject(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: hr.ReleaseNamespace(), Name: "podinfo"}) == nil {
				t.Fatal("replay lost release namespace identity")
			}
			if st.GetObject(manifest.NamedResource{Group: "nvidia.com", Kind: "ClusterPolicy", Namespace: hr.ReleaseNamespace(), Name: "cluster-policy"}) == nil {
				t.Fatal("replay lost defaulted chart child")
			}
			if st.GetObject(manifest.NamedResource{Group: "cert-manager.io", Kind: "ClusterIssuer", Namespace: hr.ReleaseNamespace(), Name: "issuer"}) == nil {
				t.Fatal("replay lost cluster issuer")
			}
			for _, ns := range []string{"", "wrong"} {
				if st.GetObject(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: ns, Name: "podinfo"}) != nil {
					t.Fatalf("replay published chart child in namespace %q", ns)
				}
			}
			sharedPrefix := digest[:len(digest)-1] + "9"
			st.SetArtifact(src.Named(), &store.SourceArtifact{
				Kind: manifest.KindOCIRepository, FS: os.DirFS(dir), LocalPath: dir, Revision: "6.15.0@" + sharedPrefix, Digest: sharedPrefix,
			})
			if info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: hr.Named(), ReconcileNode: c.ReconcileNode}); info.Status != store.StatusReady {
				t.Fatalf("shared-prefix digest reconcile: %+v", info)
			}
			sameVersion := st.GetArtifact(hr.Named()).(*store.Emission)
			if sameVersion.Fingerprint == art.Fingerprint {
				t.Fatal("full digests sharing twelve characters reused controller fingerprint")
			}
			if diff := cmp.Diff(art.Manifests, sameVersion.Manifests); diff != "" {
				t.Fatalf("same visible identity changed output (-want +got):\n%s", diff)
			}
			const next = "sha256:abcdef12345675476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
			st.SetArtifact(src.Named(), &store.SourceArtifact{
				Kind: manifest.KindOCIRepository, FS: os.DirFS(dir), LocalPath: dir, Revision: "6.15.0@" + next, Digest: next,
			})
			if info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: hr.Named(), ReconcileNode: c.ReconcileNode}); info.Status != store.StatusReady {
				t.Fatalf("changed digest reconcile: %+v", info)
			}
			updated := st.GetArtifact(hr.Named()).(*store.Emission)
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
				Kind: manifest.KindOCIRepository, FS: os.DirFS(dir), LocalPath: dir, Revision: "latest@" + next, Digest: next,
			})
			if info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: hr.Named(), ReconcileNode: c.ReconcileNode}); info.Status != store.StatusReady {
				t.Fatalf("changed revision reconcile: %+v", info)
			}
			revised := st.GetArtifact(hr.Named()).(*store.Emission)
			if revised.Fingerprint == updated.Fingerprint {
				t.Fatal("changed revision reused controller fingerprint")
			}
			if diff := cmp.Diff(updated.Manifests, revised.Manifests); diff != "" {
				t.Fatalf("same visible version changed output (-want +got):\n%s", diff)
			}
			st.SetArtifact(src.Named(), &store.SourceArtifact{
				Kind: manifest.KindOCIRepository, FS: os.DirFS(dir), LocalPath: dir, Revision: "invalid", Digest: next,
			})
			if info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: hr.Named(), ReconcileNode: c.ReconcileNode}); info.Status != store.StatusFailed {
				t.Fatalf("invalid changed revision bypassed validation: %+v", info)
			}
		})
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
		Name: "podinfo", Namespace: "flux-system",
		Chart: manifest.HelmChart{
			Name: "podinfo", RepoGroup: manifest.SourceDomain, RepoKind: manifest.KindOCIRepository,
			RepoName: "podinfo", RepoNamespace: "flux-system",
		},
	}
	st.AddObject(hr)
	err := c.awaitChartSource(t.Context(), hr, manifest.DrainForce)
	if !errors.Is(err, manifest.ErrInput) || errors.Is(err, manifest.ErrObjectNotFound) {
		t.Fatalf("chart gate category = %v", err)
	}
	info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: hr.Named(), ReconcileNode: c.ReconcileNode})
	if info.Status != store.StatusFailed {
		t.Fatalf("status = %+v, want StatusFailed", info)
	}
	if info.Message != "dependencies failed: "+src.Named().String()+": not ready" || !slices.Equal(info.BlockedBy(), []manifest.NamedResource{src.Named()}) {
		t.Errorf("chart failure = %+v; blockers = %v", info, info.BlockedBy())
	}
}

// TestReconcile_DependsOnFailed cascades a dep failure to a non-rendering
// HR — DependencyFailedError surfaces via RunWithStatusOutcome → Failed status.
func TestReconcile_DependsOnFailed(t *testing.T) {
	t.Parallel()
	c, st := newTestController(t, nil)
	dep := &manifest.HelmRelease{Name: "dep", Namespace: "flux-system"}
	st.AddObject(dep)
	st.UpdateStatus(dep.Named(), store.StatusFailed, "", "synthetic dep failure")

	hr := &manifest.HelmRelease{
		Name: "depender", Namespace: "flux-system",
		DependsOn: []helmv2.DependencyReference{{Name: dep.Name, Namespace: dep.Namespace}},
	}
	st.AddObject(hr)
	info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: hr.Named(), ReconcileNode: c.ReconcileNode})
	if info.Status != store.StatusFailed {
		t.Fatalf("status = %+v, want StatusFailed", info)
	}
	if info.Message == "" {
		t.Error("expected non-empty failure on dep cascade")
	}
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
	err := c.reconcile(t.Context(), hr, manifest.DrainForce)
	if !errors.Is(err, manifest.ErrInput) || errors.Is(err, manifest.ErrObjectNotFound) {
		t.Fatalf("parent gate category = %v", err)
	}
	info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: hr.Named(), ReconcileNode: c.ReconcileNode})
	if info.Status != store.StatusFailed {
		t.Fatalf("status = %+v, want StatusFailed", info)
	}
	if info.Message != "dependencies failed: "+parent.Named().String()+": not ready" || !slices.Equal(info.BlockedBy(), []manifest.NamedResource{parent.Named()}) {
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
	c, st := newTestControllerWithOptions(t, ReconcileOptions{Index: parentIndexForTest(map[manifest.NamedResource]manifest.NamedResource{hr.Named(): parent.Named()})})
	st.AddObject(parent) // never reaches Ready
	st.AddObject(hr)
	info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: hr.Named(), ReconcileNode: c.ReconcileNode})
	if info.Status != store.StatusFailed {
		t.Fatalf("status = %+v, want StatusFailed", info)
	}
	if info.Message != "dependencies failed: "+parent.Named().String()+": not ready" || !slices.Equal(info.BlockedBy(), []manifest.NamedResource{parent.Named()}) {
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
	info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: hr.Named(), ReconcileNode: c.ReconcileNode})
	if info.Status != store.StatusFailed {
		t.Fatalf("status = %+v, want StatusFailed", info)
	}
	if info.Message == "" {
		t.Error("expected non-empty failure for missing chart")
	}
}

func TestReconcile_SuspensionAfterRefresh(t *testing.T) {
	t.Parallel()
	for _, site := range []struct {
		name, message    string
		parent, retained bool
	}{
		{"parent", "waiting for parent KS", true, false},
		{"dependencies_after_parent", "resolving dependencies", true, false},
		{"dependencies_without_parent", "resolving dependencies", false, false},
		{"pre_render", "awaiting pre-render references", false, false},
		{"pre_render_retained", "awaiting pre-render references", true, true},
	} {
		t.Run(site.name, func(t *testing.T) {
			t.Parallel()
			for _, suspended := range []bool{false, true} {
				t.Run(fmt.Sprintf("suspended=%t", suspended), func(t *testing.T) {
					t.Parallel()
					parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
					parents := map[manifest.NamedResource]manifest.NamedResource{}
					id := testutil.HelmReleaseID("apps", "release")
					if site.parent {
						parents[id] = parent.Named()
					}
					c, st := newTestControllerWithParentOf(t, parents)
					st.AddObject(parent)
					st.UpdateStatus(parent.Named(), store.StatusReady, "", "")
					dep := &manifest.HelmRelease{Name: "dependency", Namespace: "apps"}
					st.AddObject(dep)
					st.UpdateStatus(dep.Named(), store.StatusReady, "", "")
					dir := t.TempDir()
					testutil.WriteFile(t, dir, "Chart.yaml", "apiVersion: v2\nname: refresh\nversion: 1.0.0\n")
					testutil.WriteFile(t, dir, "templates/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: {{ .Values.output }}, namespace: apps}
data: {greeting: hi}
`)
					src := &manifest.OCIRepository{Name: "chart", Namespace: "apps", URL: "oci://example.test/chart"}
					st.AddObject(src)
					st.SetArtifact(src.Named(), &store.SourceArtifact{Kind: manifest.KindOCIRepository, FS: os.DirFS(dir), LocalPath: dir, Revision: "1.0.0@sha256:abcdef12345675476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8", Digest: "sha256:abcdef12345675476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"})
					st.UpdateStatus(src.Named(), store.StatusReady, "", "")
					doc := map[string]any{
						"apiVersion": "helm.toolkit.fluxcd.io/v2", "kind": manifest.KindHelmRelease,
						"metadata": map[string]any{"name": id.Name, "namespace": id.Namespace},
						"spec": map[string]any{
							"chartRef":  map[string]any{"kind": manifest.KindOCIRepository, "name": src.Name},
							"dependsOn": []any{map[string]any{"name": dep.Name}},
							"values":    map[string]any{"output": "stale-output"},
						},
					}
					if site.message == "awaiting pre-render references" {
						input := &manifest.ConfigMap{Name: "values", Namespace: "apps", Data: map[string]any{"values.yaml": "{}"}}
						st.AddObject(input)
						doc["spec"].(map[string]any)["valuesFrom"] = []any{map[string]any{"kind": manifest.KindConfigMap, "name": input.Name}}
					}
					obj, _, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{DefaultNamespace: ""})
					if err != nil {
						t.Fatal(err)
					}
					hr := obj.(*manifest.HelmRelease)
					original := hr.Clone()
					st.AddObject(hr)
					st.UpdateStatus(id, store.StatusPending, "", "")
					var retained *store.Emission
					if site.retained {
						retained = &store.Emission{Fingerprint: "prior", Manifests: []map[string]any{{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "prior"}}}}
						st.SetArtifact(id, retained)
					}
					replacement := manifest.DeepCopyMap(doc)
					spec := replacement["spec"].(map[string]any)
					spec["suspend"] = suspended
					spec["values"].(map[string]any)["output"] = "hr-refresh-output"
					var observations, parentEvents, renders atomic.Int64
					var fired, published atomic.Bool
					unsub := st.AddListener(store.EventStatusUpdated, func(other manifest.NamedResource, payload any) {
						info, ok := payload.(store.StatusInfo)
						if other != id || !ok {
							return
						}
						if info.Message == "rendering chart" {
							renders.Add(1)
						}
						if info.Message == "waiting for parent KS" {
							parentEvents.Add(1)
						}
						if info.Message != site.message {
							return
						}
						observations.Add(1)
						if fired.Swap(true) {
							return
						}
						if site.parent {
							publishDocuments(c, parent.Named(), []map[string]any{replacement}, manifest.ParseDocOptions{WipeSecrets: false, DefaultNamespace: ""})
						} else {
							fresh := hr.Clone()
							fresh.Suspend = suspended
							fresh.Values["output"] = "hr-refresh-output"
							st.AddObject(fresh)
						}
						published.Store(true)
					}, false)
					t.Cleanup(unsub)
					c.Tasks.Go(t.Context(), "refresh", func(ctx context.Context) {
						if blocked := c.ReconcileNode(ctx, id, manifest.DrainNone); len(blocked) != 0 {
							t.Errorf("blocked on %v", blocked)
						}
					})
					c.Tasks.BlockTillDone()
					if observations.Load() != 1 || !published.Load() || c.Tasks.Failures() != 0 {
						t.Fatalf("observations=%d published=%t panics=%d", observations.Load(), published.Load(), c.Tasks.Failures())
					}
					if site.parent && parentEvents.Load() != 1 || !site.parent && parentEvents.Load() != 0 {
						t.Fatalf("parent refresh events = %d", parentEvents.Load())
					}
					fresh, ok := st.Get[*manifest.HelmRelease](id)
					if !ok || fresh == hr || fresh.Suspend != suspended || fresh.Values["output"] != "hr-refresh-output" {
						t.Fatalf("canonical replacement = %+v", fresh)
					}
					if diff := cmp.Diff(original, hr); diff != "" {
						t.Fatalf("original mutated (-want +got):\n%s", diff)
					}
					if site.parent {
						if owners := testutil.Owners(st.Productions(id, nil)); !slices.Contains(owners, parent.Named()) {
							t.Fatalf("missing independent contributor: %v", owners)
						}
					}
					info, _ := st.GetStatus(id)
					art, _ := st.GetArtifact(id).(*store.Emission)
					output := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "hr-refresh-output"}
					if suspended {
						if info.Outcome() != store.OutcomeSuspended || info.Message != "suspended" {
							t.Errorf("status = %+v, want Ready/suspended", info)
						}
						_, inventory := st.GetEmission(id)
						if art != retained || renders.Load() != 0 || st.GetObject(output) != nil || len(inventory.Children) != 0 {
							t.Errorf("suspended replacement rendered: artifact=%+v output=%v", art, st.GetObject(output))
						}
					} else if info.Status != store.StatusReady || art == nil || art == retained || renders.Load() != 1 || len(art.Manifests) != 1 || st.GetObject(output) == nil {
						t.Fatalf("active replacement did not render %s: status=%+v artifact=%+v", output, info, art)
					}
				})
			}
		})
	}
}

type renderBarrierFS struct {
	fs.FS
	tasks   *task.Service
	once    sync.Once
	started chan struct{}
	resume  <-chan struct{}
}

func (f *renderBarrierFS) Open(name string) (fs.File, error) {
	if name == "Chart.yaml" {
		f.once.Do(func() {
			close(f.started)
			f.tasks.YieldSlot(func() { <-f.resume })
		})
	}
	return f.FS.Open(name)
}

func TestReconcile_ParentUpdateDuringRender(t *testing.T) {
	t.Parallel()

	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			t.Parallel()

			for _, rescue := range []bool{false, true} {
				t.Run(fmt.Sprintf("rescue_%t", rescue), func(t *testing.T) {
					t.Parallel()

					st := store.New()
					tasks := task.NewBounded(workers)
					owner, err := sourcecache.New(t.Context(), "")
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = owner.Close() })
					client, err := helm.NewClient(owner)
					if err != nil {
						t.Fatal(err)
					}
					client.SetSourceResolver(helm.NewStoreSourceResolver(st))
					c := New(base.New(st, tasks, "helmrelease"), client, helm.Options{}, false)
					c.Configure(ReconcileOptions{})
					c.Start(t.Context())
					started, resume, rendered := make(chan struct{}), make(chan struct{}), make(chan struct{})
					release := sync.OnceFunc(func() { close(resume) })
					t.Cleanup(func() { release(); tasks.BlockTillDone(); c.Close() })
					parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
					if rescue {
						parent.PostBuild = &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindSecret, Name: "unreadable"}}}
					}
					st.AddObject(parent)
					dir := t.TempDir()
					testutil.WriteFile(t, dir, "Chart.yaml", "apiVersion: v2\nname: binding\nversion: 1.0.0\n")
					testutil.WriteFile(t, dir, "templates/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: binding}
data:
  output: {{ .Values.output | quote }}
  required: {{ required "required value" .Values.required | quote }}
`)
					src := &manifest.OCIRepository{Name: "chart", Namespace: "apps", URL: "oci://example.test/chart"}
					st.AddObject(src)
					source := &store.SourceArtifact{
						Kind:      manifest.KindOCIRepository,
						FS:        &renderBarrierFS{FS: os.DirFS(dir), tasks: tasks, started: started, resume: resume},
						LocalPath: dir, Revision: "1.0.0@sha256:abcdef12345675476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8",
						Digest: "sha256:abcdef12345675476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8",
					}
					st.SetArtifact(src.Named(), source)
					st.UpdateStatus(src.Named(), store.StatusReady, "", "")
					required := "present"
					if rescue {
						required = ""
					}
					docA := map[string]any{
						"apiVersion": "helm.toolkit.fluxcd.io/v2", "kind": manifest.KindHelmRelease,
						"metadata": map[string]any{"name": "release", "namespace": "apps"},
						"spec": map[string]any{
							"chartRef": map[string]any{"kind": manifest.KindOCIRepository, "name": src.Name},
							"values":   map[string]any{"output": "A", "required": required},
						},
					}
					docB := manifest.DeepCopyMap(docA)
					docB["spec"].(map[string]any)["values"].(map[string]any)["output"] = "B"
					publish := func(doc map[string]any) {
						publishDocuments(c, parent.Named(), []map[string]any{doc}, manifest.ParseDocOptions{DefaultNamespace: "apps"})
					}
					publish(docA)
					st.UpdateStatus(parent.Named(), store.StatusReady, "", "")
					id := testutil.HelmReleaseID("apps", "release")
					hr, ok := st.Get[*manifest.HelmRelease](id)
					if !ok {
						t.Fatal("parent did not emit input A")
					}
					prepared, err := helm.Prepare(hr, client.Resolver().HelmChart, values.NewStoreProvider(st, false), client.ValuesCache())
					if err != nil {
						t.Fatal(err)
					}
					wantFingerprint := manifest.Fingerprint(prepared.Content(struct{ Digest, Revision string }{source.Digest, source.Revision}, nil))
					var accepted atomic.Pointer[store.Emission]
					unsubscribe := st.AddListener(store.EventArtifactUpdated, func(other manifest.NamedResource, payload any) {
						if emission, ok := payload.(*store.Emission); other == id && ok && emission.Complete {
							accepted.Store(emission)
						}
					}, false)
					t.Cleanup(unsubscribe)
					tasks.Go(t.Context(), "parent", func(context.Context) {
						defer release()
						reached := false
						tasks.YieldSlot(func() {
							select {
							case <-started:
								reached = true
							case <-rendered:
							}
						})
						if !reached {
							t.Error("render never reached chart filesystem")
							return
						}
						publish(docB)
					})
					tasks.Go(t.Context(), "render", func(ctx context.Context) {
						defer close(rendered)
						if blocked := c.ReconcileNode(ctx, id, manifest.DrainNone); len(blocked) != 0 {
							t.Errorf("blocked on %v", blocked)
						}
					})
					tasks.BlockTillDone()
					if tasks.Failures() != 0 {
						t.Fatalf("task failures = %d", tasks.Failures())
					}
					emission := accepted.Load()
					if emission == nil || !emission.Complete || len(emission.Manifests) != 1 || emission.Fingerprint != wantFingerprint {
						t.Fatalf("accepted A emission = %+v; fingerprint want %s", emission, wantFingerprint)
					}
					if got := renderedConfigMapValue(emission.Manifests, "output"); got != "A" {
						t.Fatalf("rendered output = %q, want A", got)
					}
					if rescue {
						if got := renderedConfigMapValue(emission.Manifests, "required"); got != "..PLACEHOLDER_required.." {
							t.Fatalf("rescued value = %q", got)
						}
						if !slices.ContainsFunc(st.Warnings(), func(w manifest.Warning) bool {
							return w.Resource == id && w.Category == manifest.WarnUnresolvedSubstitution && slices.Contains(w.Detail, "required")
						}) {
							t.Fatal("best-effort rescue warning missing")
						}
					}
					current, ok := st.Get[*manifest.HelmRelease](id)
					if !ok || current.Values["output"] != "B" {
						t.Fatalf("current input is not B: %+v", current)
					}
					if st.GetArtifact(id) != nil {
						t.Error("input B incorrectly accepts the output rendered from A")
					}
					publish(docA)
					if st.GetArtifact(id) != emission {
						t.Error("restoring exact input A did not expose the captured emission")
					}
					publish(docB)
					if st.GetArtifact(id) != nil {
						t.Error("restored input B incorrectly accepts A's emission")
					}
					if hr.Values["output"] != "A" || hr.Values["required"] != required {
						t.Fatal("render mutated input A")
					}
				})
			}
		})
	}
}
