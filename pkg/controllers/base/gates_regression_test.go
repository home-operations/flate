package base

import (
	"context"
	"errors"
	"fmt"
	"github.com/home-operations/flate/pkg/values"
	"slices"
	"sync/atomic"
	"testing"
	"testing/fstest"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

type gatePromotion struct {
	promote func(manifest.NamedResource) bool
}

func TestFingerprintDedup_ChangedWorkBecomesPending(t *testing.T) {
	t.Parallel()
	s := store.New()
	hr := &manifest.HelmRelease{Name: "release", Namespace: "apps"}
	s.AddObject(hr)
	s.UpdateStatus(hr.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: "completed"})
	c := gateExecution(New(s, task.NewBounded(2)), hr.Named())
	_, handled, err := c.FingerprintDedup(hr.Named(), "changed")
	info, _ := s.GetStatus(hr.Named())
	if err != nil || handled || info.Status != store.StatusPending {
		t.Fatalf("admission=%t/%v, status=%+v", handled, err, info)
	}
}

func TestEmit_ApplyingProgress(t *testing.T) {
	s := store.New()
	parent := &manifest.ResourceSet{Name: "parent", Namespace: "apps"}
	s.AddObject(parent)
	s.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "rendering"})
	c := New(s, task.NewBounded(2))
	_, _, next := s.GetEmission(parent.Named())
	defer next.Finish()
	next.Complete = true
	c.Emit(parent.Named(), &next, nil)
	info, _ := s.GetStatus(parent.Named())
	assert.Equal(t, info.String(), "applying 0 objects")
	first := info
	publication := func(t *testing.T, want string) {
		t.Helper()
		if allocations := testing.AllocsPerRun(100, func() {
			s.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "rendering"})
			c.Emit(parent.Named(), &next, nil)
		}); allocations != 0 {
			t.Fatalf("applying publication allocates %g times", allocations)
		}
		info, _ := s.GetStatus(parent.Named())
		assert.Equal(t, info.String(), want)
	}
	t.Run("first", func(t *testing.T) { publication(t, "applying 0 objects") })
	if allocations := testing.AllocsPerRun(100, func() {
		c.Emit(parent.Named(), &next, nil)
	}); allocations != 0 {
		t.Fatalf("repeated publication allocates %g times", allocations)
	}
	next.Manifests = []map[string]any{{"kind": "ConfigMap"}}
	c.Emit(parent.Named(), &next, nil)
	info, _ = s.GetStatus(parent.Named())
	assert.Equal(t, info.String(), "applying 1 objects")
	t.Run("changed", func(t *testing.T) { publication(t, "applying 1 objects") })
	assert.Equal(t, first.String(), "applying 0 objects")
}

func TestRequireRefresh_PreparedSource(t *testing.T) {
	t.Parallel()
	for _, replaced := range []bool{false, true} {
		name := "prepared"
		if replaced {
			name = "replaced"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			repository := &manifest.HelmRepository{Name: "charts", Namespace: "apps", URL: "https://example.test/charts"}
			s.AddObject(repository)
			s.UpdateStatus(repository.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
			canonical := &manifest.HelmRelease{Name: "release", Namespace: "apps", Chart: manifest.HelmChart{Name: "chart", Version: "1.0.0", Source: repository.Named()}}
			s.AddObject(canonical)
			prepared := canonical.Clone()
			source := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: "apps", Name: "materialized"}
			prepared.Chart.Source = source
			want := canonical
			if replaced {
				want = canonical.Clone()
				want.Values = map[string]any{"current": true}
				s.AddObject(want)
			}
			c := gateExecution(New(s, task.NewBounded(2)), canonical.Named())
			got, err := c.RequireRefresh(t.Context(), prepared, manifest.GateArtifacts, manifest.DrainNone)
			if got != want || canonical.Chart.Source.Kind != manifest.KindHelmRepository || prepared.Chart.Source.Kind != manifest.KindHelmChart {
				t.Fatalf("canonical=%v prepared=%v returned=%v", canonical, prepared, got)
			}
			if blocked, ok := errors.AsType[*depwait.ErrBlocked](err); !ok || !slices.Equal(blocked.Deps, []manifest.NamedResource{source}) {
				t.Fatalf("prepared source gate=%v, want %s", err, source)
			}
		})
	}
}

func (*gatePromotion) IsFileIndexed(manifest.NamedResource) bool { return true }
func (p *gatePromotion) Promote(_ context.Context, _ *store.ReadSet, id manifest.NamedResource) bool {
	return p.promote(id)
}

func TestRequireRefresh_DataPromotionRefreshesCanonical(t *testing.T) {
	t.Parallel()
	s := store.New()
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	s.AddObject(parent)
	s.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusReady})
	canonical := &manifest.Kustomization{Name: "consumer", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{
		SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "original"}},
	}}
	s.AddObject(canonical)
	fresh := canonical.Clone()
	fresh.PostBuild.SubstituteFrom[0].Name = "replacement"
	original := &manifest.ConfigMap{Name: "original", Namespace: "apps"}
	replacement := &manifest.ConfigMap{Name: "replacement", Namespace: "apps"}
	s.AddObject(replacement)
	c := New(s, task.NewBounded(2))
	c.Configure(Options{
		Index: parentIndexForTest(map[manifest.NamedResource]manifest.NamedResource{canonical.Named(): parent.Named()}),
		Existence: &gatePromotion{promote: func(id manifest.NamedResource) bool {
			if id != original.Named() {
				return false
			}
			s.AddObject(original)
			s.AddObject(fresh)
			return true
		}},
	})
	first := gateExecution(c, canonical.Named())
	_ = first.Reads.GetObject(canonical.Named())
	_ = first.DataInputs(t.Context(), canonical, values.NewStoreProvider(&first.Reads, false, nil))
	if first.Reads.Valid() {
		t.Fatal("promotion did not invalidate the captured declaration")
	}
	second := gateExecution(c, canonical.Named())
	got, _ := second.Reads.Get[*manifest.Kustomization](canonical.Named())
	assert.Equal(t, got, fresh)
	inputs := second.DataInputs(t.Context(), got, values.NewStoreProvider(&second.Reads, false, nil))
	assert.Equal(t, len(inputs), 1)
	assert.Equal(t, inputs[0].NamedResource, replacement.Named())
	assert.Equal(t, inputs[0].Input.Data, manifest.Data(replacement))
}

func TestRequireRefresh_ChangedCanonicalReferences(t *testing.T) {
	t.Parallel()
	for _, suspended := range []bool{false, true} {
		name := "active"
		if suspended {
			name = "suspended"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			child := &manifest.Kustomization{Name: "child", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "stale"}}}}
			fresh := child.Clone()
			fresh.Suspend, fresh.TargetNamespace = suspended, "target"
			fresh.PostBuild.SubstituteFrom[0].Name = "current"
			s.AddObject(child)
			index := parentIndexForTest(map[manifest.NamedResource]manifest.NamedResource{child.Named(): parent.Named()})
			promotion := &gatePromotion{promote: func(id manifest.NamedResource) bool {
				if id != parent.Named() {
					return false
				}
				s.AddObject(parent)
				s.RecordEmitted(parent.Named(), &store.Emission{Children: []store.Child{{Obj: fresh}}, Complete: true}, nil)
				s.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
				return true
			}}
			c := New(s, task.NewBounded(2))
			c.Configure(Options{Index: index, Existence: promotion})
			first := gateExecution(c, child.Named())
			_, _ = first.RequireRefresh(t.Context(), child, manifest.GateParents, manifest.DrainNone)
			if first.Reads.Valid() {
				t.Fatal("parent publication did not invalidate the captured attempt")
			}
			second := gateExecution(c, child.Named())
			got, err := second.RequireRefresh(t.Context(), fresh, manifest.GateParents, manifest.DrainNone)
			if got != fresh || child.TargetNamespace != "" || child.PostBuild.SubstituteFrom[0].Name != "stale" {
				t.Fatalf("canonical=%v, original=%v", got, child)
			}
			if suspended {
				info, _ := s.GetStatus(child.Named())
				if !errors.Is(err, errSuspended) || info.Outcome() != store.OutcomeSuspended {
					t.Fatalf("error=%v, status=%+v", err, info)
				}
				return
			}
			assert.NoError(t, err)
			inputs := second.DataInputs(t.Context(), got, values.NewStoreProvider(&second.Reads, false, nil))
			assert.Equal(t, len(inputs), 1)
			assert.Equal(t, inputs[0].Name, "current")
		})
	}
}

func TestEmit_LatePreflight(t *testing.T) {
	t.Parallel()
	for _, complete := range []bool{false, true} {
		name := "incomplete"
		if complete {
			name = "complete"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			child := &manifest.ExternalArtifact{Name: "child", Namespace: "apps"}
			s.AddObject(parent)
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{parent.Named(): {File: "parent.yaml"}}}
			filter := change.NewFilter(change.NewSet([]string{"parent.yaml"}), index, s, "")
			var reject atomic.Bool
			c := New(s, task.NewBounded(2))
			c.Configure(Options{Filter: filter, Index: index, PreflightFailure: func(manifest.NamedResource) (string, bool) { return "cycle", reject.Load() }})
			execution := gateExecution(c, parent.Named())
			_, handled, err := execution.FingerprintDedup(parent.Named(), "new content")
			if handled || err != nil {
				t.Fatalf("admission=%t/%v", handled, err)
			}
			_, _, next := s.GetEmission(parent.Named())
			defer next.Finish()
			next.Children, next.Complete = []store.Child{{Obj: child}}, complete
			reject.Store(true)
			s.SetPreflight(parent.Named(), "cycle")
			payloads := map[manifest.NamedResource]*store.SourceArtifact{child.Named(): {FS: fstest.MapFS{}}}
			if err := execution.Emit(parent.Named(), &next, payloads); err == nil {
				t.Fatalf("publication error=%v", err)
			}
			if s.GetObject(child.Named()) != nil || s.GetArtifact(child.Named()) != nil || s.GetArtifact(parent.Named()) != nil || filter.ShouldReconcile(child.Named()) {
				t.Fatal("rejected publication admitted children or expanded selection")
			}
		})
	}
}

func TestStamp_CommonMetadataIntroducesHook(t *testing.T) {
	t.Parallel()
	h := &manifest.HelmRelease{Name: "owner", Namespace: "apps", CommonMetadata: &helmv2.CommonMetadata{Labels: map[string]string{manifest.HelmReleaseDomain + "/name": "common"}, Annotations: map[string]string{"helm.sh/hook": ""}}}
	doc := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "hook"}}
	Stamp(h.EmissionPolicy(), []map[string]any{doc})
	metadata := doc["metadata"].(map[string]any)
	assert.Diff(t, metadata["labels"], map[string]any{manifest.HelmReleaseDomain + "/name": "common"})
	assert.Diff(t, metadata["annotations"], map[string]any{"helm.sh/hook": ""})
	assert.Diff(t, h.CommonMetadata.Labels, map[string]string{manifest.HelmReleaseDomain + "/name": "common"})
}

func TestEmit_ProducerNamespacePolicy(t *testing.T) {
	t.Parallel()
	for _, producer := range []manifest.BaseManifest{
		&manifest.Kustomization{Name: "owner", Namespace: "apps", TargetNamespace: "target"},
		&manifest.HelmRelease{Name: "owner", Namespace: "apps", TargetNamespace: "target"},
		&manifest.ResourceSet{Name: "owner", Namespace: "target"},
	} {
		t.Run(producer.Named().Kind, func(t *testing.T) {
			t.Parallel()
			c := New(store.New(), task.NewBounded(2))
			docs := []map[string]any{
				{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "data"}},
				{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": "cluster"}},
			}
			children, visible := c.ParseChildren(producer, docs, manifest.ParseDocOptions{DefaultNamespace: "wrong"})
			if len(children) != 2 || len(visible) != 2 {
				t.Fatalf("children=%v, visible=%v", children, visible)
			}
			if children[0].Obj.Named().Namespace != "target" || children[1].Obj.Named().Namespace != "" {
				t.Fatalf("child identities=%v", children)
			}
			_, ns := manifest.DocMetadata(visible[0])
			policy := producer.(emissionProducer).EmissionPolicy()
			if (ns == "") != policy.InternalNamespace {
				t.Fatalf("visible namespace=%q, policy=%+v", ns, policy)
			}
			_, clusterNamespace := manifest.DocMetadata(visible[1])
			if clusterNamespace != "" || children[0].Doc["metadata"] == nil {
				t.Fatal("cluster scope or document binding changed")
			}
			_, authoredNamespace := manifest.DocMetadata(docs[0])
			if authoredNamespace != "" {
				t.Fatal("borrowed document changed during namespace admission")
			}
		})
	}
}

func TestEmit_KindExclusionPolicy(t *testing.T) {
	t.Parallel()
	for _, producer := range []manifest.BaseManifest{
		&manifest.Kustomization{Name: "owner", Namespace: "apps"},
		&manifest.HelmRelease{Name: "owner", Namespace: "apps"},
		&manifest.ResourceSet{Name: "owner", Namespace: "apps"},
	} {
		t.Run(producer.Named().Kind, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			s.AddObject(producer)
			c := New(s, task.NewBounded(2))
			c.Configure(Options{SkipKinds: []string{"ConfigMap", "Kustomization", "GitRepository"}})
			docs, err := manifest.SplitDocs([]byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: data, namespace: apps}\n---\napiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: nested, namespace: apps}\nspec: {path: ., sourceRef: {kind: GitRepository, name: repo}}\n---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: repo, namespace: apps}\nspec: {url: https://example.test/repo}\n---\napiVersion: v1\nkind: ServiceAccount\nmetadata: {name: visible, namespace: apps}\n"))
			assert.NoError(t, err)
			children, visible := c.ParseChildren(producer, docs, manifest.ParseDocOptions{})
			if len(visible) != 1 || manifest.DocKind(visible[0]) != "ServiceAccount" {
				t.Fatalf("visible=%v", visible)
			}
			wantChildren := 4
			if producer.(emissionProducer).EmissionPolicy().ExcludeChildren {
				wantChildren = 1
			}
			if len(children) != wantChildren {
				t.Fatalf("children=%d, want %d", len(children), wantChildren)
			}
			_, _, next := s.GetEmission(producer.Named())
			defer next.Finish()
			next.Children, next.Manifests, next.Complete = children, visible, true
			c.Emit(producer.Named(), &next, nil)
			accepted, _ := s.GetArtifact(producer.Named()).(*store.Emission)
			if accepted == nil || len(accepted.Manifests) != 1 {
				t.Fatalf("accepted=%v", accepted)
			}
			for _, child := range children {
				if s.GetObject(child.Obj.Named()) == nil {
					t.Fatalf("internal child %s was hidden", child.Obj.Named())
				}
			}
		})
	}
}

func TestRequireRefresh_FailedParentRetainsInputFailures(t *testing.T) {
	t.Parallel()
	for _, level := range []manifest.DrainLevel{manifest.DrainNone, manifest.DrainCascade} {
		t.Run(fmt.Sprint(level), func(t *testing.T) {
			t.Parallel()
			s := store.New()
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			child := &manifest.Kustomization{Name: "child", Namespace: "apps", DependsOn: []kustomizev1.DependencyReference{{Name: "missing"}}}
			s.AddObject(parent)
			s.AddObject(child)
			s.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "render failed"})
			s.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{FS: fstest.MapFS{}})
			c := New(s, task.NewBounded(2))
			c.Configure(Options{Index: parentIndexForTest(map[manifest.NamedResource]manifest.NamedResource{child.Named(): parent.Named()})})
			execution := gateExecution(c, child.Named())
			_, err := execution.RequireRefresh(t.Context(), child, manifest.GateInputs, level)
			missing := parent.Named()
			missing.Name = "missing"
			if level == manifest.DrainNone {
				if blocked, ok := errors.AsType[*depwait.ErrBlocked](err); !ok || !slices.Equal(blocked.Deps, []manifest.NamedResource{missing}) {
					t.Fatalf("gate=%v, want missing input blocked before terminal failure", err)
				}
			} else if failed, ok := errors.AsType[*manifest.DependencyFailedError](err); !ok || !slices.Equal(failed.Failed, []manifest.NamedResource{missing, parent.Named()}) {
				t.Fatalf("gate=%v, want both input and parent failures", err)
			}
		})
	}
}

func TestRequireRefresh_ArtifactReferences(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		kind       string
		apiVersion string
		spec       map[string]any
		dependency manifest.BaseManifest
		stage      manifest.GateStage
		parent     bool
	}{
		{
			name: "generator source", kind: "ArtifactGenerator", apiVersion: "source.extensions.fluxcd.io/v1beta1",
			spec:       map[string]any{"sources": []any{map[string]any{"alias": "repo", "kind": manifest.KindGitRepository, "name": "repo", "namespace": "other"}}},
			dependency: &manifest.GitRepository{Name: "repo", Namespace: "other"}, stage: manifest.GateArtifacts, parent: true,
		},
		{
			name: "source materializer", kind: manifest.KindExternalArtifact, apiVersion: "source.toolkit.fluxcd.io/v1",
			spec:       map[string]any{"sourceRef": map[string]any{"apiVersion": "source.extensions.fluxcd.io/v1beta1", "kind": "ArtifactGenerator", "name": "generator"}},
			dependency: &manifest.RawObject{APIVersion: "source.extensions.fluxcd.io/v1beta1", Kind: "ArtifactGenerator", Name: "generator", Namespace: "apps"}, stage: manifest.GateInputs,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			obj, _, _, err := manifest.ParseDoc(map[string]any{
				"apiVersion": tc.apiVersion, "kind": tc.kind,
				"metadata": map[string]any{"name": "consumer", "namespace": "apps"}, "spec": tc.spec,
			}, manifest.ParseDocOptions{})
			assert.NoError(t, err)
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			s.AddObjects([]manifest.BaseManifest{obj, parent, tc.dependency})
			s.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusPending})
			s.UpdateStatus(tc.dependency.Named(), store.StatusInfo{Status: store.StatusPending})
			c := New(s, task.NewBounded(2))
			c.Configure(Options{Index: parentIndexForTest(map[manifest.NamedResource]manifest.NamedResource{obj.Named(): parent.Named()})})
			if tc.parent {
				first := gateExecution(c, obj.Named())
				defer first.Close()
				_, err := first.RequireRefresh(t.Context(), obj, manifest.GateParents, manifest.DrainNone)
				blocked, ok := errors.AsType[*depwait.ErrBlocked](err)
				if !ok || !slices.Equal(blocked.Deps, []manifest.NamedResource{parent.Named()}) {
					t.Fatalf("parent gate=%v", err)
				}
				s.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusReady})
			}
			c.Emit(parent.Named(), &store.Emission{Children: []store.Child{{Obj: obj}}, Complete: true}, nil)
			first := gateExecution(c, obj.Named())
			defer first.Close()
			canonical, err := first.RequireRefresh(t.Context(), obj, tc.stage, manifest.DrainNone)
			assert.Equal(t, canonical, obj)
			blocked, ok := errors.AsType[*depwait.ErrBlocked](err)
			if !ok || !slices.Equal(blocked.Deps, []manifest.NamedResource{tc.dependency.Named()}) {
				t.Fatalf("reference gate=%v", err)
			}
			s.SetArtifact(tc.dependency.Named(), &store.SourceArtifact{FS: fstest.MapFS{}})
			s.UpdateStatus(tc.dependency.Named(), store.StatusInfo{Status: store.StatusReady})
			if first.Reads.Valid() {
				t.Fatal("dependency transition retained a stale assessment")
			}
			fresh := gateExecution(c, obj.Named())
			defer fresh.Close()
			canonical, err = fresh.RequireRefresh(t.Context(), obj, tc.stage, manifest.DrainNone)
			assert.NoError(t, err)
			assert.Equal(t, canonical, obj)
		})
	}
}
