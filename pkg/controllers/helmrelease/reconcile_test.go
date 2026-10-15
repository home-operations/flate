package helmrelease

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestReconcile_OCIChartDigestTracking(t *testing.T) {
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
			st.UpdateStatus(src.Named(), store.StatusReady, "recorded artifact")
			st.AddObject(hr)
			if info := dispatchToFixpoint(t, c, st, hr.Named()); info.Status != store.StatusReady {
				t.Fatalf("reconcile: %+v", info)
			}
			art := st.GetArtifact(hr.Named()).(*store.HelmReleaseArtifact)
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
				if st.GetObject(manifest.NamedResource{Kind: manifest.DocKind(doc), Namespace: identityNamespace, Name: name}) == nil {
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
			st.DeleteObject(manifest.NamedResource{Kind: "ClusterPolicy", Namespace: hr.ReleaseNamespace(), Name: "cluster-policy"})
			st.DeleteObject(manifest.NamedResource{Kind: "ClusterIssuer", Namespace: hr.ReleaseNamespace(), Name: "issuer"})
			if info := dispatchToFixpoint(t, c, st, hr.Named()); info.Status != store.StatusReady {
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
			if st.GetObject(manifest.NamedResource{Kind: "ClusterPolicy", Namespace: hr.ReleaseNamespace(), Name: "cluster-policy"}) == nil {
				t.Fatal("replay lost defaulted chart child")
			}
			if st.GetObject(manifest.NamedResource{Kind: "ClusterIssuer", Namespace: hr.ReleaseNamespace(), Name: "issuer"}) == nil {
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
				Kind: manifest.KindOCIRepository, FS: os.DirFS(dir), LocalPath: dir, Revision: "6.15.0@" + next, Digest: next,
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
				Kind: manifest.KindOCIRepository, FS: os.DirFS(dir), LocalPath: dir, Revision: "latest@" + next, Digest: next,
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
				Kind: manifest.KindOCIRepository, FS: os.DirFS(dir), LocalPath: dir, Revision: "invalid", Digest: next,
			})
			if info := dispatchToFixpoint(t, c, st, hr.Named()); info.Status != store.StatusFailed {
				t.Fatalf("invalid changed revision bypassed validation: %+v", info)
			}
		})
	}
}

// A chart source that stays Pending fails with typed dependency provenance at force drain.
func TestReconcile_ChartSourceNotReady(t *testing.T) {
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
			Name: "podinfo", RepoKind: manifest.KindOCIRepository,
			RepoName: "podinfo", RepoNamespace: "flux-system",
		},
	}
	st.AddObject(hr)
	err := c.awaitChartSource(t.Context(), hr, manifest.DrainForce)
	if !errors.Is(err, manifest.ErrInput) || errors.Is(err, manifest.ErrObjectNotFound) {
		t.Fatalf("chart gate category = %v", err)
	}
	info := dispatchToFixpoint(t, c, st, hr.Named())
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
	c, st := newTestController(t, nil)
	dep := &manifest.HelmRelease{Name: "dep", Namespace: "flux-system"}
	st.AddObject(dep)
	st.UpdateStatus(dep.Named(), store.StatusFailed, "synthetic dep failure")

	hr := &manifest.HelmRelease{
		Name: "depender", Namespace: "flux-system",
		DependsOn: []helmv2.DependencyReference{{Name: dep.Name, Namespace: dep.Namespace}},
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

// A structural parent that stays Pending fails with typed dependency provenance.
func TestReconcile_ParentGateWaits(t *testing.T) {
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
	info := dispatchToFixpoint(t, c, st, hr.Named())
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
	parent := &manifest.Kustomization{Name: "apps", Namespace: "flux-system"}
	hr := &manifest.HelmRelease{
		Name: "child", Namespace: "flux-system",
	}
	// Resolver returns the parent for the HR only — closes over
	// captured state, the production shape (which combines a map +
	// a Store render ownership lookup).
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
	if info.Message != "dependencies failed: "+parent.Named().String()+": not ready" || !slices.Equal(info.BlockedBy(), []manifest.NamedResource{parent.Named()}) {
		t.Errorf("parent resolver failure = %+v; blockers = %v", info, info.BlockedBy())
	}
}

// A missing chart must fail before entering dependency waits.
func TestReconcile_MissingChartFails(t *testing.T) {
	c, st := newTestController(t, nil)
	hr := &manifest.HelmRelease{
		Name: "broken", Namespace: "flux-system",
		// An unset chart source fails the dependency gate.
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

func TestReconcile_SuspensionAfterRefresh(t *testing.T) {
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
			for _, suspended := range []bool{false, true} {
				t.Run(fmt.Sprintf("suspended=%t", suspended), func(t *testing.T) {
					parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
					parents := map[manifest.NamedResource]manifest.NamedResource{}
					id := manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "release", Namespace: "apps"}
					if site.parent {
						parents[id] = parent.Named()
					}
					c, st := newTestControllerWithParentOf(t, parents)
					st.AddObject(parent)
					st.UpdateStatus(parent.Named(), store.StatusReady, "")
					dep := &manifest.HelmRelease{Name: "dependency", Namespace: "apps"}
					st.AddObject(dep)
					st.UpdateStatus(dep.Named(), store.StatusReady, "")
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
					st.UpdateStatus(src.Named(), store.StatusReady, "")
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
					st.UpdateStatus(id, store.StatusPending, "")
					var retained *store.HelmReleaseArtifact
					if site.retained {
						retained = &store.HelmReleaseArtifact{Fingerprint: "prior", Manifests: []map[string]any{{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "prior"}}}}
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
							c.Emit(parent.Named(), []map[string]any{replacement}, manifest.ParseDocOptions{WipeSecrets: false, DefaultNamespace: ""}, true)
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
						if owner, ok := st.EmittedParent(id); !ok || owner != parent.Named() {
							t.Fatalf("replacement owner = %v, found=%t", owner, ok)
						}
					}
					info, _ := st.GetStatus(id)
					art, _ := st.GetArtifact(id).(*store.HelmReleaseArtifact)
					output := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "hr-refresh-output"}
					if suspended {
						if info.Status != store.StatusReady || info.Message != store.MsgSuspended {
							t.Errorf("status = %+v, want Ready/suspended", info)
						}
						if art != retained || renders.Load() != 0 || st.GetObject(output) != nil || len(st.EmittedChildrenByParent()[id]) != 0 {
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
